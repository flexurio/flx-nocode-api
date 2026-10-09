use std::collections::HashSet;
use std::sync::Arc;
use once_cell::sync::Lazy;
use serde_json::Value;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

use crate::log::{log_output, log_output_lazy};
use crate::model::{ParamJoin, TableSchema};
use crate::AppState;
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV, Expr as QE, Join as QJ, JoinKind as QJK};
use crate::helpers::{escape_like, is_supported_operator, operator_query};

// Env vars are loaded once at process start (dotenv in main()) and never change
// afterward, so resolve these once instead of on every GET request.
static LIMIT_DEFAULT_ENV: Lazy<i32> = Lazy::new(|| {
    std::env::var("LIMIT_DEFAULT").ok().and_then(|s| s.parse().ok()).unwrap_or(200)
});
static LIMIT_MAX_ENV: Lazy<i32> = Lazy::new(|| {
    std::env::var("LIMIT_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(2000)
});

/// Query parameters with a fixed meaning on every GET route. They are processed
/// even when `get.parameters` does not list them and are never treated as filters.
pub const RESERVED_PARAMS: &[&str] = &[
    "page", "limit", "sort", "ascending", "search", "count", "with_count", "redis",
    "fields", "filter", "after",
];

/// Maximum nesting depth of a `?filter=` JSON tree (`and`/`or` groups).
pub const FILTER_MAX_DEPTH: usize = 5;
/// Maximum number of leaf conditions in a `?filter=` JSON tree.
pub const FILTER_MAX_LEAVES: usize = 50;

fn is_reserved(name: &str) -> bool { RESERVED_PARAMS.contains(&name) }

/// Result of `build_read_query`: the AST query plus what the executor needs to
/// run an optional COUNT and to produce the next keyset cursor.
#[derive(Debug, Clone)]
pub struct ReadQueryPlan {
    pub query: QQ,
    pub count_query: Option<QQ>,
    /// `(field as used in ORDER BY, asc)` — the active sort, used for cursors.
    pub sort_keys: Vec<(String, bool)>,
    pub limit: u32,
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

fn to_val(s: &str) -> QV {
    if s.eq_ignore_ascii_case("true") { return QV::Bool(true); }
    if s.eq_ignore_ascii_case("false") { return QV::Bool(false); }
    if let Ok(i) = s.parse::<i64>() { return QV::I64(i); }
    if let Ok(f) = s.parse::<f64>() { return QV::F64(f); }
    QV::Str(s.to_string())
}

fn json_to_qv(v: &Value) -> QV {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() { QV::I64(i) }
            else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
        }
        Value::Bool(b) => QV::Bool(*b),
        Value::String(s) => QV::Str(s.clone()),
        Value::Null => QV::Null,
        other => QV::Str(other.to_string()),
    }
}

/// Parse a list value: JSON array string (`[1,"a"]`) or comma separated (`1,a`).
fn parse_list(raw: &str) -> Vec<QV> {
    let t = raw.trim();
    if t.starts_with('[') && t.ends_with(']') {
        if let Ok(arr) = serde_json::from_str::<Vec<Value>>(t) {
            return arr.iter().map(json_to_qv).collect();
        }
    }
    if t.contains(',') {
        return t.split(',').map(|s| to_val(s.trim())).collect();
    }
    vec![to_val(t)]
}

// ---------------------------------------------------------------------------
// Column / operator key parsing
// ---------------------------------------------------------------------------

/// A column reference resolved from a `col.op` / `col->path.op` key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColRef {
    /// SQL expression used in the AST (dialect compiled for JSON paths).
    pub expr: String,
    /// Normalised identity used for allow-list matching:
    /// `table.col` or `table.col->path`.
    pub key: String,
    pub json_path: Option<String>,
}

fn valid_json_path(p: &str) -> bool {
    !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

fn valid_column_ident(c: &str) -> bool {
    !c.is_empty() && c.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Compile `col->path` into a dialect specific JSON extraction expression.
pub fn json_path_expr(dialect: &str, col: &str, path: &str) -> String {
    match dialect {
        "postgres" => {
            if path.contains('.') {
                let parts = path.split('.').collect::<Vec<_>>().join(",");
                format!("{} #>> '{{{}}}'", col, parts)
            } else {
                format!("{}->>'{}'", col, path)
            }
        }
        "mysql" => format!("JSON_UNQUOTE(JSON_EXTRACT({},'$.{}'))", col, path),
        "sqlite" => format!("json_extract({},'$.{}')", col, path),
        "mssql" => format!("JSON_VALUE({},'$.{}')", col, path),
        _ => format!("{}.{}", col, path),
    }
}

/// Parse a parameter key into `(column, canonical operator, operator symbol)`.
///
/// * `strict = false` mirrors the lenient legacy behaviour of
///   `split_column_operator` (unknown suffix => `=`), used for declared params.
/// * `strict = true` rejects unknown operators (used for `?filter=` leaves).
pub fn parse_param_key(key: &str, table: &str, dialect: &str, strict: bool) -> Result<(ColRef, String, String), String> {
    let key = key.trim();
    if key.is_empty() { return Err("empty filter key".into()); }

    let (col_part, path, op_sym): (String, Option<String>, String) = if let Some((c, rest)) = key.split_once("->") {
        let (p, op) = match rest.rsplit_once('.') {
            Some((p, op)) if is_supported_operator(op) => (p.to_string(), op.to_string()),
            // `meta->a.b` with no operator suffix: whole rest is the path, op = eq
            _ => (rest.to_string(), "eq".to_string()),
        };
        if p != "*" && !valid_json_path(&p) {
            return Err(format!("invalid JSON path '{}' in '{}'", p, key));
        }
        (c.to_string(), Some(p), op)
    } else {
        match key.rsplit_once('.') {
            Some((c, op)) if is_supported_operator(op) => (c.to_string(), None, op.to_string()),
            Some((c, op)) => {
                if strict { return Err(format!("unsupported operator '{}' in '{}'", op, key)); }
                (c.to_string(), None, op.to_string())
            }
            None => (key.to_string(), None, "eq".to_string()),
        }
    };

    if !valid_column_ident(&col_part) {
        return Err(format!("invalid column name '{}'", col_part));
    }
    let qualified = if col_part.contains('.') { col_part } else { format!("{}.{}", table, col_part) };
    let canonical = {
        let o = operator_query(&op_sym);
        if o.is_empty() { "=".to_string() } else { o }
    };
    let col = match &path {
        Some(p) => ColRef {
            expr: json_path_expr(dialect, &qualified, p),
            key: format!("{}->{}", qualified, p),
            json_path: Some(p.clone()),
        },
        None => ColRef { expr: qualified.clone(), key: qualified, json_path: None },
    };
    Ok((col, canonical, op_sym))
}

/// Build one leaf filter from a canonical operator and the raw request value.
/// Returns `None` when the value is empty for an operator that needs one.
pub fn build_leaf_filter(column: String, operator: &str, raw: &str) -> Option<QF> {
    let v = raw.trim();
    let f = match operator {
        "isnull" => {
            if matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no") { QF::IsNotNull(column) } else { QF::IsNull(column) }
        }
        "notnull" => {
            if matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no") { QF::IsNull(column) } else { QF::IsNotNull(column) }
        }
        _ if v.is_empty() => return None,
        "is" => {
            if v.eq_ignore_ascii_case("NULL") { QF::IsNull(column) }
            else if v.eq_ignore_ascii_case("NOT NULL") { QF::IsNotNull(column) }
            else { QF::Eq(column, to_val(v)) }
        }
        "=" => {
            let t = v;
            if (t.starts_with('[') && t.ends_with(']')) || t.contains(',') {
                let vs = parse_list(t);
                if vs.len() == 1 { QF::Eq(column, vs.into_iter().next().unwrap()) } else { QF::In(column, vs) }
            } else { QF::Eq(column, to_val(t)) }
        }
        "<>" => QF::Ne(column, to_val(v)),
        "<" => QF::Lt(column, to_val(v)),
        "<=" => QF::Lte(column, to_val(v)),
        ">" => QF::Gt(column, to_val(v)),
        ">=" => QF::Gte(column, to_val(v)),
        // `like` has always been case-insensitive on this read path; keep it.
        "like" | "ilike" => QF::ILike(column, wrap_like(raw)),
        "nlike" => QF::NotLike(column, wrap_like(raw)),
        "startswith" => QF::Like(column, format!("{}%", escape_like(raw))),
        "endswith" => QF::Like(column, format!("%{}", escape_like(raw))),
        "contains" => QF::Like(column, format!("%{}%", escape_like(raw))),
        "in" => QF::In(column, parse_list(v)),
        "nin" => QF::NotIn(column, parse_list(v)),
        "between" => {
            let vs = parse_list(v);
            if vs.len() == 2 {
                let mut it = vs.into_iter();
                QF::Between(column, it.next().unwrap(), it.next().unwrap())
            } else { QF::Eq(column, to_val(v)) }
        }
        _ => QF::Eq(column, to_val(v)),
    };
    Some(f)
}

/// Legacy `like` semantics: wrap in `%..%` unless the caller already supplied
/// wildcards (split_column_operator used to wrap unconditionally; a value that
/// already starts and ends with `%` is left alone to avoid `%%x%%`).
fn wrap_like(raw: &str) -> String {
    if raw.starts_with('%') && raw.ends_with('%') && raw.len() >= 2 { raw.to_string() } else { format!("%{}%", raw) }
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn bare(name: &str) -> &str { name.rsplit('.').next().unwrap_or(name).trim() }

/// `(bare column name, alias)` for a configured select column such as
/// `t.col`, `col`, `t.col AS x`, `COUNT(x) as cnt`.
fn column_names(c: &str) -> (String, Option<String>) {
    let s = c.trim();
    let lower = s.to_lowercase();
    if let Some(pos) = lower.rfind(" as ") {
        let left = s[..pos].trim();
        let alias = s[pos + 4..].trim();
        let base = bare(left).to_string();
        return (base, if alias.is_empty() { None } else { Some(alias.to_string()) });
    }
    (bare(s).to_string(), None)
}

fn is_text_type(t: &str) -> bool {
    let t = t.trim().to_ascii_lowercase();
    ["varchar", "char", "text", "string", "nvarchar", "nchar", "enum", "json", "uuid", "clob"]
        .iter().any(|p| t.starts_with(p))
        || t.contains("text") || t.contains("char")
}

fn is_numeric_type(t: &str) -> bool {
    let t = t.trim().to_ascii_lowercase();
    ["int", "bigint", "smallint", "tinyint", "mediumint", "integer", "decimal", "numeric", "float",
     "double", "real", "number", "serial", "bigserial", "money"]
        .iter().any(|p| t.starts_with(p))
        || t.contains("int")
}

/// Build the `?search=` OR group over PK + indexed columns, honouring column types.
pub fn build_search_filter(table_schema: &TableSchema, term: &str) -> Option<QF> {
    let term = term.trim();
    if term.is_empty() { return None; }
    let mut cols: Vec<String> = Vec::new();
    for c in table_schema.primary_key.columns.iter() { cols.push(c.clone()); }
    for idx in table_schema.indexes.iter() { for c in idx.columns.iter() { cols.push(c.clone()); } }

    let mut seen: HashSet<String> = HashSet::new();
    let mut ors: Vec<QF> = Vec::with_capacity(cols.len());
    let numeric_term = to_val(term);
    let term_is_number = matches!(numeric_term, QV::I64(_) | QV::F64(_));
    let has_types = !table_schema.columns.is_empty();

    for column in cols {
        let b = bare(&column).to_string();
        if b.is_empty() || !seen.insert(b.clone()) { continue; }
        let qualified = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, b) };
        let type_data = table_schema.columns.iter().find(|c| c.name == b).map(|c| c.type_data.as_str());
        match type_data {
            Some(t) if is_text_type(t) => ors.push(QF::ILike(qualified, format!("%{}%", term))),
            Some(t) if is_numeric_type(t) => {
                if term_is_number { ors.push(QF::Eq(qualified, numeric_term.clone())); }
            }
            Some(_) => { /* date/bool/binary: skip */ }
            None if !has_types => ors.push(QF::ILike(qualified, format!("%{}%", term))),
            None => { /* unknown column with a typed schema: skip */ }
        }
    }
    if ors.is_empty() { None } else { Some(QF::Or(ors)) }
}

/// Intersect the configured select columns with a client `?fields=` list.
pub fn select_fields(table_schema: &TableSchema, fields: &str) -> Vec<String> {
    let configured = &table_schema.get.columns;
    let wanted: HashSet<String> = fields.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    if wanted.is_empty() { return configured.clone(); }
    let pks: HashSet<String> = table_schema.primary_key.columns.iter().map(|c| bare(c).to_string()).collect();

    let mut out: Vec<String> = Vec::new();
    let mut matched_any = false;
    let mut pk_present: HashSet<String> = HashSet::new();
    for c in configured.iter() {
        let (base, alias) = column_names(c);
        let is_pk = pks.contains(&base) || alias.as_ref().map(|a| pks.contains(a)).unwrap_or(false);
        let hit = wanted.contains(&base) || alias.as_ref().map(|a| wanted.contains(a)).unwrap_or(false);
        if hit { matched_any = true; }
        if hit || is_pk {
            if is_pk { pk_present.insert(base.clone()); if let Some(a) = &alias { pk_present.insert(a.clone()); } }
            out.push(c.clone());
        }
    }
    if !matched_any { return configured.clone(); }
    for pk in table_schema.primary_key.columns.iter() {
        let b = bare(pk);
        if !pk_present.contains(b) {
            out.push(if pk.contains('.') { pk.clone() } else { format!("{}.{}", table_schema.table, b) });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// `?filter=` JSON
// ---------------------------------------------------------------------------

struct FilterCtx<'a> {
    table: &'a str,
    dialect: &'a str,
    declared: &'a HashSet<String>,
    leaves: usize,
    touches_deleted_at: bool,
}

fn column_allowed(declared: &HashSet<String>, col: &ColRef) -> bool {
    if declared.contains(&col.key) { return true; }
    if col.json_path.is_some() {
        if let Some((base, _)) = col.key.split_once("->") {
            return declared.contains(&format!("{}->*", base));
        }
    }
    false
}

fn leaf_value_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn parse_filter_node(v: &Value, ctx: &mut FilterCtx, depth: usize) -> Result<Option<QF>, String> {
    if depth > FILTER_MAX_DEPTH {
        return Err(format!("filter nesting exceeds max depth {}", FILTER_MAX_DEPTH));
    }
    match v {
        Value::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for it in items { if let Some(f) = parse_filter_node(it, ctx, depth + 1)? { parts.push(f); } }
            Ok(if parts.is_empty() { None } else if parts.len() == 1 { parts.pop() } else { Some(QF::And(parts)) })
        }
        Value::Object(map) => {
            let mut parts: Vec<QF> = Vec::with_capacity(map.len());
            for (k, val) in map {
                let kl = k.trim().to_ascii_lowercase();
                if kl == "and" || kl == "or" {
                    let items = val.as_array().ok_or_else(|| format!("'{}' must be an array", kl))?;
                    let mut group = Vec::with_capacity(items.len());
                    for it in items { if let Some(f) = parse_filter_node(it, ctx, depth + 1)? { group.push(f); } }
                    if group.is_empty() { continue; }
                    parts.push(if kl == "and" { QF::And(group) } else { QF::Or(group) });
                    continue;
                }
                ctx.leaves += 1;
                if ctx.leaves > FILTER_MAX_LEAVES {
                    return Err(format!("filter has more than {} conditions", FILTER_MAX_LEAVES));
                }
                let (col, op, _sym) = parse_param_key(k, ctx.table, ctx.dialect, true)?;
                if col.json_path.as_deref() == Some("*") {
                    return Err(format!("wildcard JSON path not allowed in filter key '{}'", k));
                }
                if !column_allowed(ctx.declared, &col) {
                    return Err(format!("column '{}' is not allowed in filter", k));
                }
                if col.key.contains("deleted_at") { ctx.touches_deleted_at = true; }
                let f = match (val, op.as_str()) {
                    (Value::Null, "=") | (Value::Null, "is") => QF::IsNull(col.expr),
                    (Value::Null, "<>") => QF::IsNotNull(col.expr),
                    _ => match build_leaf_filter(col.expr, &op, &leaf_value_string(val)) {
                        Some(f) => f,
                        None => return Err(format!("filter key '{}' has an empty value", k)),
                    },
                };
                parts.push(f);
            }
            Ok(if parts.is_empty() { None } else if parts.len() == 1 { parts.pop() } else { Some(QF::And(parts)) })
        }
        Value::String(s) => {
            // Allow a JSON-encoded string (double encoded by some clients).
            let parsed: Value = serde_json::from_str(s).map_err(|e| format!("invalid filter JSON: {}", e))?;
            if parsed.is_string() { return Err("invalid filter JSON".into()); }
            parse_filter_node(&parsed, ctx, depth)
        }
        _ => Err("filter must be a JSON object or array".into()),
    }
}

/// Set of column identities (`table.col`, `table.col->path`, `table.col->*`)
/// that appear anywhere in `get.parameters` (any operator).
pub fn declared_columns(table_schema: &TableSchema, dialect: &str) -> HashSet<String> {
    let mut set = HashSet::new();
    for param in &table_schema.get.parameters {
        let clean = param.trim_start_matches('*');
        if is_reserved(clean) || clean.contains("paramjoin") { continue; }
        for part in clean.split('|') {
            if let Ok((col, _, _)) = parse_param_key(part, &table_schema.table, dialect, false) {
                set.insert(col.key);
            }
        }
    }
    set
}

/// Parse a `?filter=` JSON value into an AST filter, enforcing the allow-list,
/// depth and leaf limits. Returns `(filter, touches_deleted_at)`.
pub fn parse_filter_json(table_schema: &TableSchema, dialect: &str, raw: &Value) -> Result<(Option<QF>, bool), String> {
    let declared = declared_columns(table_schema, dialect);
    let mut ctx = FilterCtx { table: &table_schema.table, dialect, declared: &declared, leaves: 0, touches_deleted_at: false };
    let f = parse_filter_node(raw, &mut ctx, 1)?;
    Ok((f, ctx.touches_deleted_at))
}

// ---------------------------------------------------------------------------
// Cursor pagination
// ---------------------------------------------------------------------------

pub fn encode_cursor(vals: &[Value]) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(vals).unwrap_or_default())
}

pub fn decode_cursor(s: &str) -> Result<Vec<Value>, String> {
    let bytes = URL_SAFE_NO_PAD.decode(s.trim())
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(s.trim()))
        .map_err(|_| "invalid cursor".to_string())?;
    let v: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid cursor".to_string())?;
    match v {
        Value::Array(a) => Ok(a),
        _ => Err("invalid cursor".into()),
    }
}

/// Keyset predicate for sort keys `(field, asc)` positioned after `vals`:
/// `(k1 > v1) OR (k1 = v1 AND k2 > v2) OR ...` (direction per key).
pub fn keyset_filter(keys: &[(String, bool)], vals: &[QV]) -> Result<QF, String> {
    if keys.is_empty() || keys.len() != vals.len() {
        return Err("cursor does not match the active sort".into());
    }
    if vals.iter().any(|v| matches!(v, QV::Null)) {
        return Err("cursor contains a null sort value".into());
    }
    let cmp = |i: usize| -> QF {
        let (k, asc) = &keys[i];
        if *asc { QF::Gt(k.clone(), vals[i].clone()) } else { QF::Lt(k.clone(), vals[i].clone()) }
    };
    if keys.len() == 1 { return Ok(cmp(0)); }
    let mut ors = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        let mut ands: Vec<QF> = (0..i).map(|j| QF::Eq(keys[j].0.clone(), vals[j].clone())).collect();
        ands.push(cmp(i));
        ors.push(if ands.len() == 1 { ands.pop().unwrap() } else { QF::And(ands) });
    }
    Ok(QF::Or(ors))
}

/// Build the cursor for the row that ends the current page, or `None` when a
/// sort key value is missing from the row.
pub fn next_cursor_from_row(row: &Value, keys: &[(String, bool)]) -> Option<String> {
    let obj = row.as_object()?;
    let mut vals = Vec::with_capacity(keys.len());
    for (k, _) in keys {
        let v = obj.get(bare(k)).or_else(|| obj.get(k.as_str()))?;
        if v.is_null() { return None; }
        vals.push(v.clone());
    }
    Some(encode_cursor(&vals))
}

// ---------------------------------------------------------------------------
// Query planning (pure; unit-testable without a database)
// ---------------------------------------------------------------------------

fn param_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::String(s) => matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
        Value::Number(n) => n.as_i64().map(|x| x != 0).unwrap_or(false),
        _ => false,
    }
}

#[allow(clippy::collapsible_if)]
pub fn build_read_query(
    table_schema: &TableSchema,
    params_map: &serde_json::Map<String, Value>,
    dialect: &str,
) -> Result<ReadQueryPlan, String> {
    let limit_default_env: i32 = *LIMIT_DEFAULT_ENV;
    let limit_max_env: i32 = *LIMIT_MAX_ENV;

    let mut i_limit_ast = limit_default_env;
    let mut i_page_ast = 1i32;
    let mut order_col_ast = table_schema.get.order_by.clone().join(", ");
    let mut order_type_ast = "ASC".to_string();
    let mut is_deleted_at = true;
    let mut fields_param: Option<String> = None;
    let mut after_param: Option<String> = None;

    let mut filters: Vec<QF> = Vec::with_capacity(params_map.len());
    let mut paramjoins_ast: Vec<ParamJoin> = Vec::with_capacity(4);

    // Identify param joins first
    for (k, v) in params_map {
        if k.contains("paramjoin") {
            if let Some(s) = v.as_str() {
                paramjoins_ast.push(ParamJoin { name: k.replace(".eq", ""), value: s.to_string() });
            }
        }
    }

    // ---- Reserved params: processed before (and independent of) the allow-list.
    if let Some(v) = params_map.get("page") {
        i_page_ast = param_str(v).parse::<i32>().ok().filter(|v| *v > 0).unwrap_or(1);
    }
    if let Some(v) = params_map.get("limit") {
        i_limit_ast = param_str(v).parse::<i32>().ok().map(|v| v.clamp(1, limit_max_env)).unwrap_or(limit_default_env);
    }
    if let Some(v) = params_map.get("sort") {
        let value_str = param_str(v);
        if !value_str.is_empty() {
            order_col_ast = value_str
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ' || *c == '_' || *c == '-')
                .collect();
        }
    }
    if let Some(v) = params_map.get("ascending") {
        order_type_ast = if param_str(v).eq_ignore_ascii_case("true") { "ASC".into() } else { "DESC".into() };
    }
    if let Some(v) = params_map.get("search") {
        if let Some(f) = build_search_filter(table_schema, &param_str(v)) { filters.push(f); }
    }
    if let Some(v) = params_map.get("fields") {
        let s = param_str(v);
        if !s.trim().is_empty() { fields_param = Some(s); }
    }
    if let Some(v) = params_map.get("after") {
        let s = param_str(v);
        if !s.trim().is_empty() { after_param = Some(s); }
    }
    if let Some(v) = params_map.get("filter") {
        let (f, touches_deleted) = parse_filter_json(table_schema, dialect, v)?;
        if touches_deleted { is_deleted_at = false; }
        if let Some(f) = f { filters.push(f); }
    }

    // ---- Declared parameters (allow-list): exact name match.
    for param in &table_schema.get.parameters {
        let clean_param = param.trim_start_matches('*');
        if is_reserved(clean_param) || clean_param.contains("paramjoin") { continue; }

        if clean_param.contains("->*") {
            // Wildcard JSON path declaration: `meta->*.eq` accepts `meta->anykey.eq`.
            let Some((prefix, suffix)) = clean_param.split_once("->*") else { continue };
            let prefix = format!("{}->", prefix);
            for (k, v) in params_map {
                if !k.starts_with(&prefix) || !k.ends_with(suffix) || is_reserved(k) { continue; }
                let (col, op, _) = match parse_param_key(k, &table_schema.table, dialect, true) { Ok(x) => x, Err(_) => continue };
                if col.json_path.as_deref() == Some("*") { continue; }
                if let Some(f) = build_leaf_filter(col.expr, &op, &param_str(v)) { filters.push(f); }
            }
            continue;
        }

        let Some(value) = params_map.get(clean_param) else { continue };
        let value_str = param_str(value);
        if clean_param.contains("deleted_at") { is_deleted_at = false; }

        if clean_param.contains('|') {
            let mut ors: Vec<QF> = Vec::with_capacity(clean_param.matches('|').count() + 1);
            for part in clean_param.split('|') {
                let (col, op, _) = parse_param_key(part, &table_schema.table, dialect, false)?;
                if col.json_path.as_deref() == Some("*") { continue; }
                if let Some(f) = build_leaf_filter(col.expr, &op, &value_str) { ors.push(f); }
            }
            if !ors.is_empty() { filters.push(QF::Or(ors)); }
        } else {
            let (col, op, _) = parse_param_key(clean_param, &table_schema.table, dialect, false)?;
            if col.json_path.as_deref() == Some("*") { continue; }
            if let Some(f) = build_leaf_filter(col.expr, &op, &value_str) { filters.push(f); }
        }
    }

    // default deleted_at IS NULL if not requested otherwise
    if is_deleted_at {
        filters.push(QF::IsNull(format!("{}.deleted_at", table_schema.table)));
    }

    // projection
    let select_columns = match &fields_param {
        Some(f) => select_fields(table_schema, f),
        None => table_schema.get.columns.clone(),
    };
    let mut q = QQ::from(table_schema.table.clone()).select(select_columns);

    // where
    if !filters.is_empty() {
        q = q.r#where(QF::And(filters));
    }

    // Apply where_clause from schema (raw, trusted conditions) to the WHERE clause.
    for wc in &table_schema.get.where_clause {
        if !wc.trim().is_empty() { q.where_raw.push(wc.clone()); }
    }

    // Order By allow-list
    let mut allowed_unqualified: HashSet<String> = HashSet::new();
    for c in table_schema.get.columns.iter() {
        let (base, alias) = column_names(c);
        if !base.is_empty() { allowed_unqualified.insert(base); }
        if let Some(a) = alias { allowed_unqualified.insert(a); }
    }
    for idx in table_schema.indexes.iter() {
        for c in idx.columns.iter() {
            let b = bare(c);
            if !b.is_empty() { allowed_unqualified.insert(b.to_string()); }
        }
    }
    for j in table_schema.get.join_tables.iter() {
        for c in j.columns.iter() {
            let b = bare(c);
            if !b.is_empty() { allowed_unqualified.insert(b.to_string()); }
        }
    }
    for c in table_schema.primary_key.columns.iter() {
        let b = bare(c);
        if !b.is_empty() { allowed_unqualified.insert(b.to_string()); }
    }

    let global_asc = order_type_ast.eq_ignore_ascii_case("ASC");
    let mut any_order = false;
    for token in order_col_ast.split(',') {
        let raw = token.trim();
        if raw.is_empty() { continue; }
        let mut col_str = raw;
        let mut asc_opt: Option<bool> = None;
        if let Some(stripped) = raw.strip_prefix('-') {
            col_str = stripped.trim();
            asc_opt = Some(false);
        } else if let Some((name, dir)) = raw.rsplit_once(' ') {
            let d = dir.trim().to_ascii_lowercase();
            if d == "asc" || d == "desc" {
                col_str = name.trim();
                asc_opt = Some(d == "asc");
            }
        }
        if !allowed_unqualified.contains(bare(col_str)) { continue; }
        q = q.order_by(col_str.to_string(), asc_opt.unwrap_or(global_asc));
        any_order = true;
    }
    if !any_order {
        for col in table_schema.get.order_by.iter() {
            let col_trim = col.trim();
            if col_trim.is_empty() { continue; }
            if allowed_unqualified.contains(bare(col_trim)) || allowed_unqualified.contains(col_trim) {
                q = q.order_by(col_trim.to_string(), global_asc);
            }
        }
    }

    // Cursor pagination needs a deterministic order: fall back to PK asc. The PK
    // order is only materialised in the SQL when a cursor is actually consumed.
    let mut sort_keys: Vec<(String, bool)> = q.sort.iter().map(|s| (s.field.clone(), s.asc)).collect();
    if sort_keys.is_empty() {
        sort_keys = table_schema.primary_key.columns.iter()
            .map(|c| (if c.contains('.') { c.clone() } else { format!("{}.{}", table_schema.table, bare(c)) }, true))
            .collect();
        if after_param.is_some() {
            for (k, asc) in &sort_keys { q = q.order_by(k.clone(), *asc); }
        }
    }

    // JOINs (with safe paramjoin replacements)
    for j in &table_schema.get.join_tables {
        let mut logical = j.logical.clone();
        for pj in &paramjoins_ast {
            let safe_val: String = pj.value.chars().filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.').collect();
            logical = logical.replace(&pj.name, &safe_val);
        }
        let parse_col_eq = |s: &str| -> Option<(String, String)> {
            let (l, r) = s.split_once('=')?;
            let lhs = l.trim();
            let rhs = r.trim();
            if lhs.is_empty() || rhs.is_empty() { return None; }
            if !lhs.contains('.') || !rhs.contains('.') { return None; }
            Some((lhs.to_string(), rhs.to_string()))
        };
        if let Some((lhs, rhs)) = parse_col_eq(&logical) {
            if j.type_join.eq_ignore_ascii_case("left") {
                q = q.join_left_expr(j.table.clone(), QE::ColEq(lhs, rhs));
            } else {
                q = q.join_inner_expr(j.table.clone(), QE::ColEq(lhs, rhs));
            }
        } else {
            let kind = if j.type_join.eq_ignore_ascii_case("left") { QJK::Left } else { QJK::Inner };
            q.joins.push(QJ { kind, table: j.table.clone(), on: logical, on_expr: None });
        }
    }

    // GROUP BY
    if !table_schema.get.column_groups.is_empty() {
        q = q.group_by(table_schema.get.column_groups.clone());
    }
    // HAVING
    if !table_schema.get.having.is_empty() {
        let hv = table_schema.get.having.iter().cloned().map(QE::Raw).collect::<Vec<_>>();
        q = q.having_expr(hv);
    }

    // Total count is opt-in (`?count=true` / `?with_count=true`).
    let want_count = params_map.get("count").or_else(|| params_map.get("with_count")).map(truthy).unwrap_or(false);
    let count_query = if want_count && q.group_by.is_empty() {
        let mut cq = q.clone();
        cq.projection.clear();
        cq.sort.clear();
        cq.having_exprs.clear();
        cq.aggs.clear();
        cq.limit = None;
        cq.offset = None;
        Some(cq.agg_count_all("cnt"))
    } else { None };

    // Keyset pagination (after the COUNT clone so totals ignore the cursor).
    if let Some(cur) = &after_param {
        let vals = decode_cursor(cur)?.iter().map(json_to_qv).collect::<Vec<_>>();
        let ks = keyset_filter(&sort_keys, &vals)?;
        q.filter = Some(match q.filter.take() {
            Some(QF::And(mut fs)) => { fs.push(ks); QF::And(fs) }
            Some(other) => QF::And(vec![other, ks]),
            None => ks,
        });
        i_page_ast = 1;
    }

    // pagination
    let offset_ast = (i_page_ast - 1) * i_limit_ast;
    q = q.limit(i_limit_ast as u32).offset(offset_ast.max(0) as u32);

    Ok(ReadQueryPlan { query: q, count_query, sort_keys, limit: i_limit_ast.max(1) as u32 })
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Returns `(rows, total, next_cursor)`. `next_cursor` is `Some` when the page
/// is full and the last row carries all active sort-key values.
#[allow(clippy::collapsible_if)]
pub async fn fetch_dynamic_data(
    state: &AppState,
    route: &str,
    table_schema: &Arc<TableSchema>,
    params_map: &serde_json::Map<String, Value>,
) -> Result<(Vec<Value>, usize, Option<String>), String> {
    let plan = build_read_query(table_schema, params_map, state.db_type.as_str())?;
    let q = plan.query;

    // log query
    log_output_lazy("DEBUG", "DATA READ", route, || format!("Query: {:?}", q), true);

    let rows = match state.store.query(&q).await {
        Ok(rs) => rs,
        Err(e) => return Err(format!("Error NCO-GET(AST) route {}: {}. Query : {:?}", route, e, q)),
    };

    let next_cursor = if rows.len() as u32 >= plan.limit {
        rows.last().and_then(|r| next_cursor_from_row(r, &plan.sort_keys))
    } else { None };

    // Embed child details if configured and rows are present
    let mut enriched_rows = rows;
    if !table_schema.details.is_empty() && !enriched_rows.is_empty() {
        for detail in &table_schema.details {
            if detail.field.is_empty()
                || detail.target_table.is_empty()
                || detail.foreign_key_column.is_empty()
            {
                continue;
            }

            let parent_ids: Vec<QV> = enriched_rows
                .iter()
                .filter_map(|r| r.get("id"))
                .map(|v| match v {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() { QV::I64(i) } else { QV::F64(n.as_f64().unwrap_or(0.0)) }
                    }
                    Value::String(s) => {
                        if let Ok(i) = s.parse::<i64>() { QV::I64(i) } else { QV::Str(s.clone()) }
                    }
                    _ => QV::Null,
                })
                .collect();

            if !parent_ids.is_empty() {
                let detail_query = QQ::from(detail.target_table.clone())
                    .select(vec!["*".to_string()])
                    .r#where(QF::In(detail.foreign_key_column.clone(), parent_ids));

                if let Ok(detail_rows) = state.store.query(&detail_query).await {
                    let mut detail_groups: std::collections::HashMap<String, Vec<Value>> =
                        std::collections::HashMap::new();
                    for d_row in detail_rows {
                        if let Some(fk_val) = d_row.get(&detail.foreign_key_column) {
                            let fk_key = match fk_val {
                                Value::Number(n) => n.to_string(),
                                Value::String(s) => s.clone(),
                                _ => fk_val.to_string(),
                            };
                            detail_groups.entry(fk_key).or_default().push(d_row);
                        }
                    }

                    for row in &mut enriched_rows {
                        if let Some(parent_id_val) = row.get("id") {
                            let parent_key = match parent_id_val {
                                Value::Number(n) => n.to_string(),
                                Value::String(s) => s.clone(),
                                _ => parent_id_val.to_string(),
                            };
                            let items = detail_groups.get(&parent_key).cloned().unwrap_or_default();
                            if let Some(row_obj) = row.as_object_mut() {
                                row_obj.insert(detail.field.clone(), Value::Array(items));
                            }
                        }
                    }
                }
            }
        }
    }

    let total = match plan.count_query {
        Some(cq) => match state.store.query(&cq).await {
            Ok(crows) => crows
                .first()
                .and_then(|r| r.get("cnt"))
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
                .unwrap_or(0)
                .max(0) as usize,
            Err(e) => {
                log_output("ERROR", "DATA READ COUNT", route, format!("count failed: {}", e), false);
                enriched_rows.len()
            }
        },
        None => enriched_rows.len(),
    };

    Ok((enriched_rows, total, next_cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, Index, PrimaryKey};
    use serde_json::json;

    fn schema(params: &[&str], columns: &[&str]) -> TableSchema {
        let mut t = TableSchema::default();
        t.table = "orders".into();
        t.primary_key = PrimaryKey { columns: vec!["id".into()] };
        t.get.columns = columns.iter().map(|s| s.to_string()).collect();
        t.get.parameters = params.iter().map(|s| s.to_string()).collect();
        t.columns = vec![
            Column { name: "id".into(), type_data: "integer".into(), ..Default::default() },
            Column { name: "code".into(), type_data: "varchar(50)".into(), ..Default::default() },
            Column { name: "qty".into(), type_data: "int".into(), ..Default::default() },
            Column { name: "created_at".into(), type_data: "datetime".into(), ..Default::default() },
        ];
        t.indexes = vec![Index { name: "ix".into(), columns: vec!["code".into(), "qty".into(), "created_at".into()], unique: false }];
        t
    }

    fn params(pairs: &[(&str, Value)]) -> serde_json::Map<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn leaf(key: &str, val: &str) -> QF {
        let (col, op, _) = parse_param_key(key, "orders", "postgres", false).unwrap();
        build_leaf_filter(col.expr, &op, val).unwrap()
    }

    // --- operator parsing ---

    #[test]
    fn op_ne_and_neq() {
        assert!(matches!(leaf("qty.ne", "5"), QF::Ne(c, QV::I64(5)) if c == "orders.qty"));
        assert!(matches!(leaf("qty.neq", "5"), QF::Ne(_, QV::I64(5))));
    }

    #[test]
    fn op_in_and_nin() {
        match leaf("qty.in", "1,2,3") { QF::In(_, v) => assert_eq!(v.len(), 3), f => panic!("{:?}", f) }
        match leaf("qty.in", "[1,\"a\"]") { QF::In(_, v) => assert_eq!(v.len(), 2), f => panic!("{:?}", f) }
        match leaf("qty.nin", "1,2") { QF::NotIn(_, v) => assert_eq!(v.len(), 2), f => panic!("{:?}", f) }
    }

    #[test]
    fn op_null_variants() {
        assert!(matches!(leaf("code.notnull", "true"), QF::IsNotNull(_)));
        assert!(matches!(leaf("code.isnotnull", ""), QF::IsNotNull(_)));
        assert!(matches!(leaf("code.isnull", ""), QF::IsNull(_)));
        assert!(matches!(leaf("code.is", "NOT NULL"), QF::IsNotNull(_)));
    }

    #[test]
    fn op_like_family() {
        assert!(matches!(leaf("code.like", "ab"), QF::ILike(_, p) if p == "%ab%"));
        assert!(matches!(leaf("code.ilike", "ab"), QF::ILike(_, p) if p == "%ab%"));
        assert!(matches!(leaf("code.nlike", "ab"), QF::NotLike(_, p) if p == "%ab%"));
        assert!(matches!(leaf("code.notlike", "ab"), QF::NotLike(_, p) if p == "%ab%"));
    }

    #[test]
    fn op_startswith_endswith_contains_escape() {
        assert!(matches!(leaf("code.startswith", "a%b"), QF::Like(_, p) if p == "a\\%b%"));
        assert!(matches!(leaf("code.endswith", "a_b"), QF::Like(_, p) if p == "%a\\_b"));
        assert!(matches!(leaf("code.contains", "a\\b"), QF::Like(_, p) if p == "%a\\\\b%"));
    }

    #[test]
    fn escape_like_escapes_all_metachars() {
        assert_eq!(escape_like("100%_x\\"), "100\\%\\_x\\\\");
        assert_eq!(escape_like("plain"), "plain");
    }

    #[test]
    fn strict_parse_rejects_unknown_operator() {
        assert!(parse_param_key("qty.foo", "orders", "postgres", true).is_err());
        assert!(parse_param_key("qty.gte", "orders", "postgres", true).is_ok());
        assert!(parse_param_key("qty; drop", "orders", "postgres", true).is_err());
    }

    // --- search ---

    #[test]
    fn search_skips_integer_columns_for_text_term() {
        let t = schema(&[], &["id", "code"]);
        let f = build_search_filter(&t, "abc").unwrap();
        match f {
            QF::Or(fs) => {
                assert_eq!(fs.len(), 1);
                assert!(matches!(&fs[0], QF::ILike(c, _) if c == "orders.code"));
            }
            f => panic!("{:?}", f),
        }
    }

    #[test]
    fn search_adds_eq_on_numeric_columns_for_numeric_term() {
        let t = schema(&[], &["id", "code"]);
        let f = build_search_filter(&t, "42").unwrap();
        match f {
            QF::Or(fs) => {
                assert_eq!(fs.len(), 3, "id eq, code ilike, qty eq");
                assert!(fs.iter().any(|f| matches!(f, QF::Eq(c, QV::I64(42)) if c == "orders.id")));
                assert!(!fs.iter().any(|f| matches!(f, QF::ILike(c, _) if c == "orders.id")));
            }
            f => panic!("{:?}", f),
        }
    }

    // --- fields ---

    #[test]
    fn fields_intersection_keeps_pk_and_drops_unknown() {
        let t = schema(&[], &["orders.id", "orders.code", "orders.qty AS quantity", "orders.created_at"]);
        let cols = select_fields(&t, "quantity,bogus,code");
        assert_eq!(cols, vec!["orders.id", "orders.code", "orders.qty AS quantity"]);
    }

    #[test]
    fn fields_empty_intersection_falls_back_to_configured() {
        let t = schema(&[], &["orders.id", "orders.code"]);
        assert_eq!(select_fields(&t, "nope"), t.get.columns);
    }

    #[test]
    fn fields_adds_pk_when_not_configured() {
        let t = schema(&[], &["orders.code", "orders.qty"]);
        assert_eq!(select_fields(&t, "code"), vec!["orders.code", "orders.id"]);
    }

    // --- reserved params without declaration ---

    #[test]
    fn reserved_params_work_without_declaration() {
        let t = schema(&[], &["orders.id", "orders.code"]);
        let p = params(&[("page", json!("3")), ("limit", json!("10")), ("sort", json!("-code")), ("search", json!("x"))]);
        let plan = build_read_query(&t, &p, "postgres").unwrap();
        assert_eq!(plan.query.limit, Some(10));
        assert_eq!(plan.query.offset, Some(20));
        assert_eq!(plan.query.sort.len(), 1);
        assert!(!plan.query.sort[0].asc);
        assert!(matches!(&plan.query.filter, Some(QF::And(fs)) if fs.len() == 2));
    }

    #[test]
    fn undeclared_filter_param_is_ignored() {
        let t = schema(&["code.eq"], &["orders.id", "orders.code"]);
        let p = params(&[("qty.gt", json!("1")), ("code.eq", json!("A"))]);
        let plan = build_read_query(&t, &p, "postgres").unwrap();
        match plan.query.filter.unwrap() {
            QF::And(fs) => {
                assert_eq!(fs.len(), 2); // code eq + deleted_at is null
                assert!(matches!(&fs[0], QF::Eq(c, QV::Str(s)) if c == "orders.code" && s == "A"));
            }
            f => panic!("{:?}", f),
        }
    }

    // --- filter JSON ---

    #[test]
    fn filter_json_nested_groups() {
        let t = schema(&["code.eq", "qty.gt"], &["orders.id"]);
        let v = json!({"or":[{"code.eq":"open"},{"and":[{"qty.gt":5},{"code.like":"a%"}]}]});
        let (f, _) = parse_filter_json(&t, "postgres", &v).unwrap();
        match f.unwrap() {
            QF::Or(fs) => {
                assert_eq!(fs.len(), 2);
                assert!(matches!(&fs[0], QF::Eq(_, QV::Str(s)) if s == "open"));
                assert!(matches!(&fs[1], QF::And(inner) if inner.len() == 2));
            }
            f => panic!("{:?}", f),
        }
    }

    #[test]
    fn filter_json_accepts_string_encoded_and_rejects_undeclared_column() {
        let t = schema(&["code.eq"], &["orders.id"]);
        let ok = json!("{\"code.ne\":\"x\"}");
        assert!(parse_filter_json(&t, "postgres", &ok).unwrap().0.is_some());
        let bad = json!({"qty.gt": 1});
        let err = parse_filter_json(&t, "postgres", &bad).unwrap_err();
        assert!(err.contains("not allowed"), "{}", err);
        let bad_op = json!({"code.foo": 1});
        assert!(parse_filter_json(&t, "postgres", &bad_op).is_err());
    }

    #[test]
    fn filter_json_rejects_excessive_depth_and_leaves() {
        let t = schema(&["qty.gt"], &["orders.id"]);
        let mut v = json!({"qty.gt": 1});
        for _ in 0..6 { v = json!({"and": [v]}); }
        assert!(parse_filter_json(&t, "postgres", &v).unwrap_err().contains("depth"));
        let many: Vec<Value> = (0..51).map(|i| json!({"qty.gt": i})).collect();
        assert!(parse_filter_json(&t, "postgres", &json!({"or": many})).unwrap_err().contains("conditions"));
    }

    #[test]
    fn filter_json_null_value_maps_to_is_null() {
        let t = schema(&["code.eq"], &["orders.id"]);
        let (f, _) = parse_filter_json(&t, "postgres", &json!({"code.eq": null})).unwrap();
        assert!(matches!(f, Some(QF::IsNull(_))));
    }

    // --- cursor ---

    #[test]
    fn cursor_round_trip() {
        let vals = vec![json!("2024-01-01"), json!(42)];
        let c = encode_cursor(&vals);
        assert_eq!(decode_cursor(&c).unwrap(), vals);
        assert!(decode_cursor("!!notb64").is_err());
    }

    #[test]
    fn keyset_single_column() {
        let f = keyset_filter(&[("orders.id".into(), true)], &[QV::I64(10)]).unwrap();
        assert!(matches!(f, QF::Gt(c, QV::I64(10)) if c == "orders.id"));
        let f = keyset_filter(&[("orders.id".into(), false)], &[QV::I64(10)]).unwrap();
        assert!(matches!(f, QF::Lt(_, _)));
        assert!(keyset_filter(&[("a".into(), true)], &[]).is_err());
    }

    #[test]
    fn keyset_two_columns_row_expansion() {
        let f = keyset_filter(
            &[("orders.created_at".into(), false), ("orders.id".into(), true)],
            &[QV::Str("2024".into()), QV::I64(7)],
        ).unwrap();
        match f {
            QF::Or(ors) => {
                assert_eq!(ors.len(), 2);
                assert!(matches!(&ors[0], QF::Lt(c, _) if c == "orders.created_at"));
                match &ors[1] {
                    QF::And(ands) => {
                        assert!(matches!(&ands[0], QF::Eq(c, _) if c == "orders.created_at"));
                        assert!(matches!(&ands[1], QF::Gt(c, QV::I64(7)) if c == "orders.id"));
                    }
                    f => panic!("{:?}", f),
                }
            }
            f => panic!("{:?}", f),
        }
    }

    #[test]
    fn after_param_adds_keyset_and_ignores_page() {
        let t = schema(&[], &["orders.id", "orders.code"]);
        let cur = encode_cursor(&[json!(5)]);
        let p = params(&[("after", json!(cur)), ("page", json!("9")), ("limit", json!("2"))]);
        let plan = build_read_query(&t, &p, "postgres").unwrap();
        assert_eq!(plan.query.offset, Some(0));
        assert_eq!(plan.sort_keys, vec![("orders.id".to_string(), true)]);
        assert_eq!(plan.query.sort.len(), 1, "PK order materialised when cursor used");
        match plan.query.filter.unwrap() {
            QF::And(fs) => assert!(fs.iter().any(|f| matches!(f, QF::Gt(c, QV::I64(5)) if c == "orders.id"))),
            f => panic!("{:?}", f),
        }
        let row = json!({"id": 9, "code": "x"});
        assert_eq!(next_cursor_from_row(&row, &plan.sort_keys), Some(encode_cursor(&[json!(9)])));
    }

    // --- JSON path ---

    #[test]
    fn json_path_compiles_per_dialect() {
        assert_eq!(json_path_expr("postgres", "orders.meta", "key"), "orders.meta->>'key'");
        assert_eq!(json_path_expr("postgres", "orders.meta", "a.b"), "orders.meta #>> '{a,b}'");
        assert_eq!(json_path_expr("mysql", "orders.meta", "a.b"), "JSON_UNQUOTE(JSON_EXTRACT(orders.meta,'$.a.b'))");
        assert_eq!(json_path_expr("sqlite", "orders.meta", "k"), "json_extract(orders.meta,'$.k')");
        assert_eq!(json_path_expr("mssql", "orders.meta", "k"), "JSON_VALUE(orders.meta,'$.k')");
    }

    #[test]
    fn json_path_param_allow_listed_with_wildcard() {
        let t = schema(&["meta->*.eq"], &["orders.id"]);
        let p = params(&[("meta->color.eq", json!("red")), ("meta->x.gt", json!("1"))]);
        let plan = build_read_query(&t, &p, "mysql").unwrap();
        match plan.query.filter.unwrap() {
            QF::And(fs) => {
                assert_eq!(fs.len(), 2, "only `.eq` on meta accepted + deleted_at");
                assert!(matches!(&fs[0], QF::Eq(c, QV::Str(s)) if c == "JSON_UNQUOTE(JSON_EXTRACT(orders.meta,'$.color'))" && s == "red"));
            }
            f => panic!("{:?}", f),
        }
        assert!(parse_param_key("meta->a;b.eq", "orders", "mysql", true).is_err());
        // filter JSON honours the wildcard declaration too
        let (f, _) = parse_filter_json(&t, "postgres", &json!({"meta->size.gte": 3})).unwrap();
        assert!(matches!(f, Some(QF::Gte(c, QV::I64(3))) if c == "orders.meta->>'size'"));
        assert!(parse_filter_json(&t, "postgres", &json!({"other->k.eq": 1})).is_err());
    }
}
