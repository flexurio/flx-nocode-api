use std::collections::HashSet;
use std::sync::Arc;
use once_cell::sync::Lazy;
use serde_json::Value;

use crate::log::log_output;
use crate::model::{ParamJoin, TableSchema};
use crate::AppState;
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV, Expr as QE, Join as QJ, JoinKind as QJK};
use crate::helpers::split_column_operator;

// Env vars are loaded once at process start (dotenv in main()) and never change
// afterward, so resolve these once instead of on every GET request.
static LIMIT_DEFAULT_ENV: Lazy<i32> = Lazy::new(|| {
    std::env::var("LIMIT_DEFAULT").ok().and_then(|s| s.parse().ok()).unwrap_or(200)
});
static LIMIT_MAX_ENV: Lazy<i32> = Lazy::new(|| {
    std::env::var("LIMIT_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(2000)
});

#[allow(clippy::collapsible_if)]
pub async fn fetch_dynamic_data(
    state: &AppState,
    route: &str,
    table_schema: &Arc<TableSchema>,
    params_map: &serde_json::Map<String, Value>,
) -> Result<(Vec<Value>, usize), String> {
    // Build AST query
    // Tuneable limits via env to control memory usage per request
    let limit_default_env: i32 = *LIMIT_DEFAULT_ENV;
    let limit_max_env: i32 = *LIMIT_MAX_ENV;

    let mut i_limit_ast = limit_default_env;
    let mut i_page_ast = 1i32;
    let mut order_col_ast = table_schema.get.order_by.clone().join(", ");
    let mut order_type_ast = "ASC".to_string();
    let mut is_deleted_at = true;
    
    // Pre-allocate with estimated capacity
    let mut filters: Vec<QF> = Vec::with_capacity(params_map.len());
    // collect paramjoin values if provided
    let mut paramjoins_ast: Vec<ParamJoin> = Vec::with_capacity(4);

    // Identify param joins first
    for (k, v) in params_map {
        if k.contains("paramjoin") {
            if let Some(s) = v.as_str() {
                paramjoins_ast.push(ParamJoin { name: k.replace(".eq", ""), value: s.to_string() });
            }
        }
    }

    // helper to parse value into QV
    fn to_val(s: &str) -> QV {
        if s.eq_ignore_ascii_case("true") { return QV::Bool(true); }
        if s.eq_ignore_ascii_case("false") { return QV::Bool(false); }
        if let Ok(i) = s.parse::<i64>() { return QV::I64(i); }
        if let Ok(f) = s.parse::<f64>() { return QV::F64(f); }
        QV::Str(s.to_string())
    }

    // Filter processing based on schema allowlist (table_schema.get.parameters)
    // BUT since we are in repo, we trust caller has validated required params. 
    // We strictly iterate over schema-allowed params to build filters.
    for param in &table_schema.get.parameters {
        // Handle "*param" syntax stripper
        let clean_param = param.trim_start_matches('*');
        
        if let Some(value) = params_map.get(clean_param) {
            let value_str = value.as_str().unwrap_or("").to_string();
            
            if clean_param.contains("deleted_at") { is_deleted_at = false; }
            
            match clean_param {
                "page" => {
                    i_page_ast = value_str.parse::<i32>().ok().filter(|v| *v > 0).unwrap_or(1);
                }
                "sort" => {
                    if !value_str.is_empty() {
                        let sanitized: String = value_str
                            .chars()
                            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ' || *c == '_' || *c == '-')
                            .collect();
                        order_col_ast = sanitized;
                    }
                }
                "ascending" => {
                    order_type_ast = if value_str.eq_ignore_ascii_case("true") { "ASC".into() } else { "DESC".into() };
                }
                "limit" => {
                    i_limit_ast = value_str
                        .parse::<i32>()
                        .ok()
                        .map(|v| v.clamp(1, limit_max_env))
                        .unwrap_or(limit_default_env);
                }
                p if p.contains("paramjoin") => {
                    // handled via join logical substitution below
                }
                "search" => {
                    let v = value_str;
                    if !v.is_empty() {
                        // Pre-calculate capacity for OR filters
                        let pk_count = table_schema.primary_key.columns.len();
                        let idx_count: usize = table_schema.indexes.iter()
                            .map(|idx| idx.columns.len())
                            .sum();
                        let mut ors: Vec<QF> = Vec::with_capacity(pk_count + idx_count);
                        
                        // primary key columns
                        for column in table_schema.primary_key.columns.iter() {
                            let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                            ors.push(QF::ILike(col, format!("%{}%", v)));
                        }
                        // indexed columns
                        for index in table_schema.indexes.iter() {
                            for column in index.columns.iter() {
                                let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                ors.push(QF::ILike(col, format!("%{}%", v)));
                            }
                        }
                        if !ors.is_empty() { filters.push(QF::Or(ors)); }
                    }
                }
                p if p.contains('|') => {
                    if value_str.is_empty() { continue; }
                    // OR across multiple columns
                    let parts_count = p.matches('|').count() + 1;
                    let mut ors: Vec<QF> = Vec::with_capacity(parts_count); 
                    for part in p.split('|') {
                        let (column, operator, val) = split_column_operator(part, &table_schema.table, &value_str);
                        let f = match operator.as_str() {
                            "=" => QF::Eq(column, to_val(&val)),
                            "<" => QF::Lt(column, to_val(&val)),
                            "<=" => QF::Lte(column, to_val(&val)),
                            ">" => QF::Gt(column, to_val(&val)),
                            ">=" => QF::Gte(column, to_val(&val)),
                            "like" => QF::ILike(column, val),
                            "nin" => {
                                // nin: support JSON array or comma-separated
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                                        let vs = arr.into_iter().map(|x| match x {
                                            serde_json::Value::Number(n) => {
                                                if let Some(i) = n.as_i64() { QV::I64(i) }
                                                else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
                                            }
                                            serde_json::Value::Bool(b) => QV::Bool(b),
                                            serde_json::Value::String(s) => QV::Str(s),
                                            serde_json::Value::Null => QV::Null,
                                            other => QV::Str(other.to_string()),
                                        }).collect::<Vec<QV>>();
                                        QF::NotIn(column, vs)
                                    } else { QF::NotIn(column, vec![to_val(&val)]) }
                                } else if value_str.contains(',') {
                                    let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                    QF::NotIn(column, vs)
                                } else { QF::NotIn(column, vec![to_val(&val)]) }
                            }
                            "between" => {
                                // between: support JSON [a,b] or 'a,b'
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                                        if arr.len() == 2 {
                                            let a = &arr[0];
                                            let b = &arr[1];
                                            let to_qv = |v: &serde_json::Value| match v {
                                                serde_json::Value::Number(n) => {
                                                    if let Some(i) = n.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
                                                }
                                                serde_json::Value::Bool(b) => QV::Bool(*b),
                                                serde_json::Value::String(s) => QV::Str(s.clone()),
                                                serde_json::Value::Null => QV::Null,
                                                other => QV::Str(other.to_string()),
                                            };
                                            QF::Between(column, to_qv(a), to_qv(b))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    } else { QF::Eq(column, to_val(&val)) }
                                } else if value_str.contains(',') {
                                    let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                    let a = parts.next().unwrap_or_default();
                                    let b = parts.next().unwrap_or_default();
                                    QF::Between(column, to_val(&a), to_val(&b))
                                } else { QF::Eq(column, to_val(&val)) }
                            }
                            "is" => {
                                if val.eq_ignore_ascii_case("NULL") { QF::IsNull(column) }
                                else if val.eq_ignore_ascii_case("NOT NULL") { QF::IsNotNull(column) }
                                else { QF::Eq(column, to_val(&val)) }
                            }
                            _ => QF::Eq(column, to_val(&val)),
                        };
                        ors.push(f);
                    }
                    if !ors.is_empty() { filters.push(QF::Or(ors)); }
                }
                _ => {
                    if value_str.is_empty() { continue; }
                    let (column, operator, val) = split_column_operator(clean_param, &table_schema.table, &value_str);
                    if operator == "is" {
                        if value_str.eq_ignore_ascii_case("NULL") {
                            filters.push(QF::IsNull(column));
                        } else if value_str.eq_ignore_ascii_case("NOT NULL") {
                            filters.push(QF::IsNotNull(column));
                        } else {
                            filters.push(QF::Eq(column, to_val(&val)));
                        }
                    } else {
                        // Support IN via comma-separated list or JSON array string when operator is equality
                        let f = match operator.as_str() {
                            "=" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                                        let vs = arr
                                            .into_iter()
                                            .map(|x| match x {
                                                serde_json::Value::Number(n) => {
                                                    if let Some(i) = n.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
                                                }
                                                serde_json::Value::Bool(b) => QV::Bool(b),
                                                serde_json::Value::String(s) => QV::Str(s),
                                                serde_json::Value::Null => QV::Null,
                                                other => QV::Str(other.to_string()),
                                            })
                                            .collect::<Vec<QV>>();
                                        QF::In(column, vs)
                                    } else {
                                        QF::Eq(column, to_val(&val))
                                    }
                                } else if value_str.contains(',') {
                                    let vs = value_str
                                        .split(',')
                                        .map(|s| to_val(s.trim()))
                                        .collect::<Vec<QV>>();
                                    QF::In(column, vs)
                                } else {
                                    QF::Eq(column, to_val(&val))
                                }
                            }
                            "<" => QF::Lt(column, to_val(&val)),
                            "<=" => QF::Lte(column, to_val(&val)),
                            ">" => QF::Gt(column, to_val(&val)),
                            ">=" => QF::Gte(column, to_val(&val)),
                            "like" => QF::ILike(column, val),
                            "nin" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                                        let vs = arr.into_iter().map(|x| match x {
                                            serde_json::Value::Number(n) => {
                                                if let Some(i) = n.as_i64() { QV::I64(i) }
                                                else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
                                            }
                                            serde_json::Value::Bool(b) => QV::Bool(b),
                                            serde_json::Value::String(s) => QV::Str(s),
                                            serde_json::Value::Null => QV::Null,
                                            other => QV::Str(other.to_string()),
                                        }).collect::<Vec<QV>>();
                                        QF::NotIn(column, vs)
                                    } else { QF::NotIn(column, vec![to_val(&val)]) }
                                } else if value_str.contains(',') {
                                    let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                    QF::NotIn(column, vs)
                                } else { QF::NotIn(column, vec![to_val(&val)]) }
                            }
                            "between" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                                        if arr.len() == 2 {
                                            let a = &arr[0];
                                            let b = &arr[1];
                                            let to_qv = |v: &serde_json::Value| match v {
                                                serde_json::Value::Number(n) => {
                                                    if let Some(i) = n.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
                                                }
                                                serde_json::Value::Bool(b) => QV::Bool(*b),
                                                serde_json::Value::String(s) => QV::Str(s.clone()),
                                                serde_json::Value::Null => QV::Null,
                                                other => QV::Str(other.to_string()),
                                            };
                                            QF::Between(column, to_qv(a), to_qv(b))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    } else { QF::Eq(column, to_val(&val)) }
                                } else if value_str.contains(',') {
                                    let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                    let a = parts.next().unwrap_or_default();
                                    let b = parts.next().unwrap_or_default();
                                    QF::Between(column, to_val(&a), to_val(&b))
                                } else { QF::Eq(column, to_val(&val)) }
                            }
                            _ => QF::Eq(column, to_val(&val)),
                        };
                        filters.push(f);
                    }
                }
            }
        }
    }

    // default deleted_at IS NULL if not requested otherwise
    if is_deleted_at {
        filters.push(QF::IsNull(format!("{}.deleted_at", table_schema.table)));
    }

    // projection
    let select_columns = table_schema.get.columns.clone();
    let mut q = QQ::from(table_schema.table.clone()).select(select_columns);

    // where
    if !filters.is_empty() {
        q = q.r#where(QF::And(filters));
    }

    // Apply where_clause from schema (raw, trusted conditions) to the WHERE clause.
    // Previously these were (incorrectly) emitted as HAVING and silently overwritten
    // by the schema `having` block below.
    if !table_schema.get.where_clause.is_empty() {
        for wc in &table_schema.get.where_clause {
            if !wc.trim().is_empty() {
                q.where_raw.push(wc.clone());
            }
        }
    }

    // Order By logic (same as original)
    let mut allowed_unqualified: HashSet<String> = HashSet::new();
    for c in table_schema.get.columns.iter() {
        let s = c.trim();
        if let Some((left, right)) = s.to_lowercase().split_once(" as ") {
            if let Some((_l, alias_actual)) = s.split_once(" as ") {
                allowed_unqualified.insert(alias_actual.trim().to_string());
            } else if let Some((_l, alias_actual)) = s.split_once(" AS ") {
                allowed_unqualified.insert(alias_actual.trim().to_string());
            } else {
                allowed_unqualified.insert(right.trim().to_string());
            }
            let left_orig = &s[..left.len()];
            let base = left_orig.rsplit('.').next().unwrap_or(left_orig).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        } else {
            let base = s.rsplit('.').next().unwrap_or(s).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
    }
    for idx in table_schema.indexes.iter() {
        for c in idx.columns.iter() {
            let base = c.rsplit('.').next().unwrap_or(c).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
    }
    for j in table_schema.get.join_tables.iter() {
        for c in j.columns.iter() {
            let base = c.rsplit('.').next().unwrap_or(c).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
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
        let unqualified = col_str.rsplit('.').next().unwrap_or(col_str).trim();
        if !allowed_unqualified.contains(unqualified) { continue; }
        let asc = asc_opt.unwrap_or(global_asc);
        q = q.order_by(col_str.to_string(), asc);
        any_order = true;
    }

    if !any_order {
        for col in table_schema.get.order_by.iter() {
            let col_trim = col.trim();
            if col_trim.is_empty() { continue; }
            let unqualified = col_trim.rsplit('.').next().unwrap_or(col_trim);
            if allowed_unqualified.contains(unqualified) || allowed_unqualified.contains(col_trim) {
                q = q.order_by(col_trim.to_string(), global_asc);
            }
        }
    }

    // JOINs (with safe paramjoin replacements)
    if !table_schema.get.join_tables.is_empty() {
        for j in &table_schema.get.join_tables {
            let mut logical = j.logical.clone();
            for pj in &paramjoins_ast {
                let safe_val: String = pj
                    .value
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                    .collect();
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
    }

    // GROUP BY
    if !table_schema.get.column_groups.is_empty() {
        q = q.group_by(table_schema.get.column_groups.clone());
    }

    // HAVING
    if !table_schema.get.having.is_empty() {
        let hv = table_schema
            .get
            .having
            .iter()
            .cloned()
            .map(QE::Raw)
            .collect::<Vec<_>>();
        q = q.having_expr(hv);
    }

    // Total count is opt-in (`?count=true`). By default we skip the extra COUNT(*)
    // query for performance and report the number of rows returned in this page.
    let want_count = params_map.get("count").map(|v| match v {
        Value::Bool(b) => *b,
        Value::String(s) => matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
        Value::Number(n) => n.as_i64().map(|x| x != 0).unwrap_or(false),
        _ => false,
    }).unwrap_or(false);

    // Build the COUNT query from the same filters/joins/where_raw BEFORE pagination.
    // GROUP BY queries are skipped (counting groups needs a subquery the AST can't
    // express yet); for those we fall back to the page row count.
    let count_query = if want_count && q.group_by.is_empty() {
        let mut cq = q.clone();
        cq.projection.clear();
        cq.sort.clear();
        cq.having_exprs.clear();
        cq.aggs.clear();
        cq.limit = None;
        cq.offset = None;
        Some(cq.agg_count_all("cnt"))
    } else {
        None
    };

    // pagination
    let offset_ast = (i_page_ast - 1) * i_limit_ast;
    q = q.limit(i_limit_ast as u32).offset(offset_ast.max(0) as u32);

    // log query
    log_output("DEBUG", "DATA READ", route, format!("Query: {:?}", q), true);

    let rows = match state.store.query(&q).await {
        Ok(rs) => rs,
        Err(e) => return Err(format!("Error NCO-GET(AST) route {}: {}. Query : {:?}", route, e, q)),
    };

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

            let parent_ids: Vec<crate::storage::ast::Val> = enriched_rows
                .iter()
                .filter_map(|r| r.get("id"))
                .map(|v| match v {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            crate::storage::ast::Val::I64(i)
                        } else {
                            crate::storage::ast::Val::F64(n.as_f64().unwrap_or(0.0))
                        }
                    }
                    Value::String(s) => {
                        if let Ok(i) = s.parse::<i64>() {
                            crate::storage::ast::Val::I64(i)
                        } else {
                            crate::storage::ast::Val::Str(s.clone())
                        }
                    }
                    _ => crate::storage::ast::Val::Null,
                })
                .collect();

            if !parent_ids.is_empty() {
                let detail_query = crate::storage::ast::Query::from(detail.target_table.clone())
                    .select(vec!["*".to_string()])
                    .r#where(crate::storage::ast::Filter::In(
                        detail.foreign_key_column.clone(),
                        parent_ids,
                    ));

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
                            let items = detail_groups
                                .get(&parent_key)
                                .cloned()
                                .unwrap_or_default();
                            if let Some(row_obj) = row.as_object_mut() {
                                row_obj.insert(detail.field.clone(), Value::Array(items));
                            }
                        }
                    }
                }
            }
        }
    }

    let total = match count_query {
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

    Ok((enriched_rows, total))
}
