use actix_web::{web::Data, HttpResponse, Responder};
use serde_json::{json, Value};

use crate::{
    AppState, auth::{check_access, get_user_info_from_token}, log::log_output, model::{
        TableSchema, WebResponse,
    }
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// NCO-VALIDATE
pub async fn check_table_design(
    state: Data<AppState>,
    route: String,
    table_schema_in: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if state.require_auth && !state.route_publics.contains(&route){
        let claims = match get_user_info_from_token(&req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };

        if let Err(e) = check_access(&claims, &req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // get table schema from table_schemas where table = route
    // let table_schema = filter_table_schema(&table_schemas, route.clone()).await; -- Use passed schema
    let table_schema = table_schema_in.as_ref().clone(); // Clone for validation mutation or just deref? validate_table_design takes value
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Check table schema
    match validate_table_design(&table_schema) {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Table validated".to_string(),
            total_data: 1,
            data: json!(table_schema),
        }),
        Err(errors) => HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Schema validation failed".to_string(),
            total_data: 0,
            data: json!({ "errors": errors }),
        }),
    }
}

pub fn validate_table_design(design: &TableSchema) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    // Check if table exists
    if design.table.is_empty() {
        errors.push("Table name cannot be empty".to_string());
    }

    // Check primary key
    if design.primary_key.columns.is_empty() {
        errors.push("Primary key columns cannot be empty".to_string());
    } else {
        for pk_col in &design.primary_key.columns {
            if !design.columns.iter().any(|col| col.name == *pk_col) {
                errors.push(format!("Primary key column '{}' does not exist in columns", pk_col));
            }
        }
    }

    // Check columns
    if design.columns.is_empty() {
        errors.push("Columns cannot be empty".to_string());
    } else {
        // Validasi type_data
        let allowed_types = [
            "char", "varchar", "text", "longtext", "mediumtext", "tinytext",
            "int", "tinyint", "smallint", "mediumint", "bigint",
            "float", "double", "decimal",
            "date", "datetime", "timestamp", "time", "year",
            "blob", "longblob", "mediumblob", "tinyblob",
            "json", "boolean", "bool", "enum"
        ];

        for col in &design.columns {
            let lower_type = col.type_data.to_lowercase();
            let is_valid = allowed_types.iter().any(|&t| lower_type.starts_with(t));
            
            if !is_valid {
                 errors.push(format!(
                    "Column '{}' has invalid type_data '{}'. Allowed types are: {:?}", 
                    col.name, col.type_data, allowed_types
                ));
            }
        }
    }

    // Check indexes
    for index in &design.indexes {
        for index_col in &index.columns {
            if !design.columns.iter().any(|col| col.name == *index_col) {
                errors.push(format!("Index column '{}' does not exist in columns", index_col));
            }
            if design.primary_key.columns.contains(index_col) {
                errors.push(format!("Primary key column '{}' should not be indexed", index_col));
            }
        }
    }

    // Check GET parameters
    let required_params = ["search", "page", "sort", "ascending", "limit"];
    let has_required_params = required_params
        .iter()
        .all(|p| design.get.parameters.contains(&p.to_string()));

    if !has_required_params && design.get.enable_method {
         errors.push("GET parameters must contain search, page, sort, ascending, limit".to_string());
    }

    for param in &design.get.parameters {
        if !required_params.contains(&param.as_str()) && !param.contains("deleted_at") {
             let parts: Vec<&str> = param.split('.').collect();
             let (table, param_name) = if parts.len() >= 2 {
                 (parts[0], parts[parts.len() - 2])
             } else {
                 (design.table.as_str(), parts[0])
             };

             let is_col_ok = if table == design.table {
                 design.columns.iter().any(|col| col.name == param_name)
             } else {
                 design.get.join_tables.iter()
                     .filter(|jt| jt.table == table)
                     .any(|jt| jt.columns.contains(&param_name.to_string()))
             };

             if !is_col_ok {
                 errors.push(format!("GET parameter '{}' does not exist in columns or joined tables", param));
             } else if !design.primary_key.columns.contains(&param_name.to_string()) {
                 let in_index = design.indexes.iter().any(|idx| idx.columns.contains(&param_name.to_string()));
                 if !in_index {
                      errors.push(format!("GET parameter '{}' is not indexed (and not PK)", param));
                 }
             }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

// ── Startup configuration linter (NCO-LINT) ───────────────────────────────────
//
// Pure, DB-free checks over every loaded entity schema. Runs once at boot
// (see `startup::run_config_lint`) so semantic config mistakes surface as a
// clear startup failure instead of a runtime SQL error or a silent no-op.

/// Result of [`lint_schemas`] / [`lint_raw_entity`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LintReport {
    /// Fatal problems: in strict mode the process refuses to start.
    pub errors: Vec<String>,
    /// Suspicious but tolerated configuration.
    pub warnings: Vec<String>,
}

impl LintReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    fn error(&mut self, route: &str, msg: impl AsRef<str>) {
        self.errors.push(format!("[{}] {}", route, msg.as_ref()));
    }

    fn warn(&mut self, route: &str, msg: impl AsRef<str>) {
        self.warnings.push(format!("[{}] {}", route, msg.as_ref()));
    }

    /// Append another report's findings.
    pub fn merge(&mut self, other: LintReport) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }
}

/// Operator suffixes understood by `helpers::split_column_operator` /
/// `helpers::operator_query`. Anything else silently degrades to `=` at
/// runtime, which is exactly the kind of mistake the linter must catch.
pub const SUPPORTED_PARAM_OPERATORS: &[&str] = crate::helpers::SUPPORTED_OPERATORS;

/// GET parameters with special meaning (not column filters).
pub const RESERVED_GET_PARAMS: &[&str] = &["search", "page", "sort", "ascending", "limit", "redis"];

/// Trigger events after `normalize_trigger_event`.
pub const SUPPORTED_TRIGGER_EVENTS: &[&str] = &["update", "create", "delete", "status_change", "any"];

/// Top-level keys an entity JSON file may contain.
pub const KNOWN_TOP_LEVEL_KEYS: &[&str] = &[
    "table", "primary_key", "foreign_keys", "columns", "indexes", "details",
    "action_triggers", "triggers", "redis", "get", "post", "put", "del", "patch", "trace",
    "locked_when", "state_machine", "auto_generate", "seed", "collate", "position",
];

/// Keys a `columns[]` object may contain (mirrors `model::Column` + aliases).
pub const KNOWN_COLUMN_KEYS: &[&str] = &[
    "name", "auto_increment", "nullable", "type_data", "function", "function_endpoint",
    "function_endpoint_path", "encrypt", "collate", "default", "default_value",
    // flattened `model::FieldRules`
    "enum", "values", "enum_values", "pattern", "min", "max", "min_length", "max_length",
    "email", "url", "message",
];

/// `^[A-Za-z_][A-Za-z0-9_]*$` without pulling in a regex at startup.
pub fn is_valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Outcome of resolving a column reference against a schema set.
enum ColRef {
    /// Resolved to a column of the entity itself (or skipped: expression).
    Ok,
    /// Unqualified / self-qualified column that the entity does not define.
    MissingLocal(String),
    /// `other_table.col` where the qualifier is not the entity: the table is
    /// loaded but has no such column, or the qualifier is unknown altogether.
    UnknownQualified(String),
}

/// Strip `table.` qualifier / ` as alias` / trailing `*` / ORDER BY direction.
fn normalize_column_ref(raw: &str) -> Option<(Option<String>, String)> {
    let mut s = raw.trim();
    if s.is_empty() || s.contains('(') {
        return None; // expression / aggregate: cannot be linted statically
    }
    // "col as alias" / "col AS alias"
    let lower = s.to_ascii_lowercase();
    if let Some(pos) = lower.find(" as ") {
        s = s[..pos].trim();
    }
    // ORDER BY: "-col", "col desc", "col ASC"
    if let Some((name, dir)) = s.rsplit_once(' ') {
        let d = dir.trim().to_ascii_lowercase();
        if d == "asc" || d == "desc" {
            s = name.trim();
        }
    }
    let s = s.trim_start_matches('-').trim_start_matches('*').trim_end_matches('*').trim();
    if s.is_empty() {
        return None;
    }
    match s.rsplit_once('.') {
        Some((q, c)) => Some((Some(q.trim().to_string()), c.trim().to_string())),
        None => Some((None, s.to_string())),
    }
}

struct LintCtx<'a> {
    schemas: &'a HashMap<String, Arc<TableSchema>>,
    /// Every route key and every `table` value, plus a map table→schema.
    by_table: HashMap<String, &'a TableSchema>,
}

impl<'a> LintCtx<'a> {
    fn find_schema(&self, name: &str) -> Option<&'a TableSchema> {
        if let Some(s) = self.schemas.get(name) {
            return Some(s.as_ref());
        }
        self.by_table.get(name).copied()
    }

    fn has_column(schema: &TableSchema, col: &str) -> bool {
        schema.columns.iter().any(|c| c.name == col)
    }

    fn resolve(&self, route: &str, schema: &TableSchema, raw: &str) -> ColRef {
        let (qualifier, col) = match normalize_column_ref(raw) {
            Some(x) => x,
            None => return ColRef::Ok,
        };
        match qualifier {
            None => {
                if Self::has_column(schema, &col) {
                    ColRef::Ok
                } else {
                    ColRef::MissingLocal(col)
                }
            }
            Some(q) if q == schema.table || q == route => {
                if Self::has_column(schema, &col) {
                    ColRef::Ok
                } else {
                    ColRef::MissingLocal(col)
                }
            }
            Some(q) => {
                // Join alias: "join_tables[].table" may be "tbl" or "tbl alias".
                let is_join_alias = schema.get.join_tables.iter().any(|jt| {
                    jt.table
                        .split_whitespace()
                        .any(|tok| tok == q)
                });
                match self.find_schema(&q) {
                    Some(other) if Self::has_column(other, &col) => ColRef::Ok,
                    Some(_) => ColRef::UnknownQualified(format!(
                        "'{}.{}' (table '{}' is loaded but has no column '{}')",
                        q, col, q, col
                    )),
                    None if is_join_alias => ColRef::Ok,
                    None => ColRef::UnknownQualified(format!(
                        "'{}.{}' (unknown table/alias '{}')",
                        q, col, q
                    )),
                }
            }
        }
    }

    /// Column reference that must be local: ERROR when missing locally,
    /// WARN when it points at an unknown join table.
    fn check_col(
        &self,
        report: &mut LintReport,
        route: &str,
        schema: &TableSchema,
        what: &str,
        raw: &str,
    ) {
        match self.resolve(route, schema, raw) {
            ColRef::Ok => {}
            ColRef::MissingLocal(c) => report.error(
                route,
                format!("{} '{}' refers to column '{}' which is not defined in columns", what, raw, c),
            ),
            ColRef::UnknownQualified(d) => {
                report.warn(route, format!("{} references {}", what, d))
            }
        }
    }
}

fn check_identifier(report: &mut LintReport, route: &str, what: &str, value: &str) {
    if !is_valid_identifier(value) {
        report.error(
            route,
            format!(
                "{} '{}' is not a valid SQL identifier (expected ^[A-Za-z_][A-Za-z0-9_]*$)",
                what, value
            ),
        );
    }
}

fn is_text_type(type_data: &str) -> bool {
    let t = type_data.trim().to_ascii_lowercase();
    t.starts_with("varchar") || t.starts_with("char") || t.ends_with("text") || t.starts_with("nvarchar")
}

/// A string default that DDL would emit raw and that is neither quoted, a
/// function/keyword, nor numeric/boolean — on a text column it is almost
/// certainly a forgotten pair of quotes.
fn default_looks_like_bare_literal(default: &str) -> bool {
    let d = default.trim();
    if d.is_empty() {
        return false;
    }
    if d.starts_with('\'') || d.starts_with('"') || d.contains('(') {
        return false;
    }
    let lower = d.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "null" | "true" | "false" | "current_timestamp" | "current_date" | "current_time" | "now"
    ) {
        return false;
    }
    d.parse::<f64>().is_err()
}

fn lint_trigger_actions(
    ctx: &LintCtx<'_>,
    report: &mut LintReport,
    route: &str,
    trigger_name: &str,
    actions: &[crate::model::TriggerAction],
) {
    for action in actions {
        let label = action
            .name
            .as_deref()
            .filter(|n| !n.is_empty())
            .unwrap_or(action.action_type.as_str());
        for (field, value) in [
            ("target_table", action.target_table.as_str()),
            ("detail_table", action.detail_table.as_deref().unwrap_or("")),
        ] {
            if value.is_empty() {
                continue;
            }
            check_identifier(
                report,
                route,
                &format!("action_triggers '{}' action '{}' {}", trigger_name, label, field),
                value,
            );
            if ctx.find_schema(value).is_none() {
                report.warn(
                    route,
                    format!(
                        "action_triggers '{}' action '{}' {} '{}' is not a loaded entity table",
                        trigger_name, label, field, value
                    ),
                );
            }
        }
        if let Some(nested) = &action.actions {
            lint_trigger_actions(ctx, report, route, trigger_name, nested);
        }
    }
}

fn lint_one(ctx: &LintCtx<'_>, report: &mut LintReport, route: &str, schema: &TableSchema) {
    // ── Identifiers (SQL-injection guard) ────────────────────────────────
    check_identifier(report, route, "table", &schema.table);
    for col in &schema.columns {
        check_identifier(report, route, "column", &col.name);
    }
    for pk in &schema.primary_key.columns {
        check_identifier(report, route, "primary_key column", pk);
    }
    for fk in &schema.foreign_keys {
        check_identifier(report, route, "foreign_key column", &fk.column);
        check_identifier(report, route, "foreign_key reference_table", &fk.reference_table);
        check_identifier(report, route, "foreign_key reference_column", &fk.reference_column);
    }
    for idx in &schema.indexes {
        if !idx.name.is_empty() {
            check_identifier(report, route, "index name", &idx.name);
        }
        for c in &idx.columns {
            check_identifier(report, route, &format!("index '{}' column", idx.name), c);
        }
    }

    // ── Duplicate columns ────────────────────────────────────────────────
    let mut seen = HashSet::new();
    for col in &schema.columns {
        if !seen.insert(col.name.as_str()) {
            report.error(route, format!("duplicate column name '{}'", col.name));
        }
    }

    // ── Column existence ─────────────────────────────────────────────────
    for pk in &schema.primary_key.columns {
        ctx.check_col(report, route, schema, "primary_key", pk);
    }
    for fk in &schema.foreign_keys {
        ctx.check_col(report, route, schema, "foreign_key", &fk.column);
    }
    for idx in &schema.indexes {
        for c in &idx.columns {
            ctx.check_col(report, route, schema, &format!("index '{}'", idx.name), c);
        }
    }
    for c in &schema.get.columns {
        ctx.check_col(report, route, schema, "get.columns", c);
    }
    for c in &schema.post.columns {
        ctx.check_col(report, route, schema, "post.columns", c);
    }
    for c in &schema.put.columns {
        ctx.check_col(report, route, schema, "put.columns", c);
    }
    for c in &schema.get.order_by {
        ctx.check_col(report, route, schema, "get.order_by", c);
    }

    // ── GET parameters ───────────────────────────────────────────────────
    if schema.get.enable_method && schema.get.parameters.is_empty() {
        report.warn(
            route,
            "get.enable_method is true but get.parameters is empty: page/limit/sort query params will be ignored",
        );
    }
    for raw_param in &schema.get.parameters {
        let param = raw_param.trim().trim_start_matches('*');
        if param.is_empty() {
            report.error(route, "get.parameters contains an empty entry");
            continue;
        }
        if RESERVED_GET_PARAMS.contains(&param) || param.contains("paramjoin") {
            continue;
        }
        for part in param.split('|') {
            let segs: Vec<&str> = part.split('.').collect();
            if segs.len() < 2 {
                // bare column: runtime falls back to `table.col = value`
                ctx.check_col(report, route, schema, "get.parameters", part);
                continue;
            }
            let op = segs[segs.len() - 1];
            let col = segs[segs.len() - 2];
            if !SUPPORTED_PARAM_OPERATORS.contains(&op) {
                report.error(
                    route,
                    format!(
                        "get.parameters '{}' uses unknown operator '{}' (supported: {})",
                        raw_param,
                        op,
                        SUPPORTED_PARAM_OPERATORS.join(", ")
                    ),
                );
            }
            let col_ref = if segs.len() >= 3 {
                format!("{}.{}", segs[segs.len() - 3], col)
            } else {
                col.to_string()
            };
            ctx.check_col(report, route, schema, "get.parameters", &col_ref);
        }
    }

    // ── Triggers ─────────────────────────────────────────────────────────
    for trig in &schema.action_triggers {
        let ev = crate::nocode::trigger_engine::normalize_trigger_event(&trig.event);
        if !SUPPORTED_TRIGGER_EVENTS.contains(&ev.as_str()) {
            report.error(
                route,
                format!(
                    "action_triggers '{}' has unknown event '{}' (supported: on_update, on_create/insert, on_delete, on_status_change, any)",
                    trig.name, trig.event
                ),
            );
        }
        if let Some(cond) = &trig.condition {
            if !cond.field.is_empty() && !LintCtx::has_column(schema, &cond.field) {
                report.warn(
                    route,
                    format!(
                        "action_triggers '{}' condition.field '{}' is not a column of this entity",
                        trig.name, cond.field
                    ),
                );
            }
        }
        lint_trigger_actions(ctx, report, route, &trig.name, &trig.actions);
    }

    // ── Foreign keys → other schemas ─────────────────────────────────────
    for fk in &schema.foreign_keys {
        match ctx.find_schema(&fk.reference_table) {
            None => report.warn(
                route,
                format!(
                    "foreign_key '{}' reference_table '{}' is not a loaded entity (external table?)",
                    fk.column, fk.reference_table
                ),
            ),
            Some(other) if !LintCtx::has_column(other, &fk.reference_column) => report.warn(
                route,
                format!(
                    "foreign_key '{}' reference_column '{}' does not exist in loaded entity '{}'",
                    fk.column, fk.reference_column, fk.reference_table
                ),
            ),
            Some(_) => {}
        }
    }

    // ── Details (header/detail documents) ────────────────────────────────
    for d in &schema.details {
        let label = if d.field.is_empty() { d.target_table.as_str() } else { d.field.as_str() };
        if d.target_table.is_empty() {
            report.error(route, format!("details '{}' has empty target_table", label));
            continue;
        }
        check_identifier(report, route, &format!("details '{}' target_table", label), &d.target_table);
        if !d.foreign_key_column.is_empty() {
            check_identifier(
                report,
                route,
                &format!("details '{}' foreign_key_column", label),
                &d.foreign_key_column,
            );
        }
        let target = match ctx.find_schema(&d.target_table) {
            Some(t) => t,
            None => {
                report.error(
                    route,
                    format!(
                        "details '{}' target_table '{}' is not a loaded entity (routes.json)",
                        label, d.target_table
                    ),
                );
                continue;
            }
        };
        if d.foreign_key_column.is_empty() {
            report.error(route, format!("details '{}' has empty foreign_key_column", label));
        } else if !LintCtx::has_column(target, &d.foreign_key_column) {
            report.error(
                route,
                format!(
                    "details '{}' foreign_key_column '{}' does not exist in target '{}'",
                    label, d.foreign_key_column, d.target_table
                ),
            );
        }
        if let Some(pk) = d.parent_key_column.as_deref().filter(|p| !p.is_empty()) {
            check_identifier(report, route, &format!("details '{}' parent_key_column", label), pk);
            if !LintCtx::has_column(schema, pk) {
                report.error(
                    route,
                    format!(
                        "details '{}' parent_key_column '{}' is not a column of this entity",
                        label, pk
                    ),
                );
            }
        }
        for c in &d.columns {
            let stripped = c.trim().trim_end_matches('*');
            if stripped.is_empty() || stripped.contains('(') {
                continue;
            }
            if !LintCtx::has_column(target, stripped) {
                report.error(
                    route,
                    format!(
                        "details '{}' column '{}' does not exist in target '{}'",
                        label, c, d.target_table
                    ),
                );
            }
        }
    }

    // ── State machine / locked_when ──────────────────────────────────────
    if let Some(sm) = &schema.state_machine {
        if sm.field.is_empty() {
            report.error(route, "state_machine.field is empty");
        } else if !LintCtx::has_column(schema, &sm.field) {
            report.error(
                route,
                format!("state_machine.field '{}' is not a column of this entity", sm.field),
            );
        }
    }
    if let Some(lw) = &schema.locked_when {
        for field in lw.get_conditions().keys() {
            if !LintCtx::has_column(schema, field) {
                report.error(
                    route,
                    format!("locked_when field '{}' is not a column of this entity", field),
                );
            }
        }
        for c in lw.get_except_columns() {
            if !LintCtx::has_column(schema, c) {
                report.warn(
                    route,
                    format!("locked_when.except_columns '{}' is not a column of this entity", c),
                );
            }
        }
    }

    // ── Column defaults ──────────────────────────────────────────────────
    for col in &schema.columns {
        if let Some(def) = &col.default {
            if is_text_type(&col.type_data) && default_looks_like_bare_literal(def) {
                report.warn(
                    route,
                    format!(
                        "column '{}' ({}) default '{}' is an unquoted string: DDL will treat it as an identifier/expression; use \"'{}'\"",
                        col.name, col.type_data, def, def
                    ),
                );
            }
        }
    }
}

/// Lint every loaded entity schema. Pure: no DB, no filesystem.
///
/// `routes` drives iteration order (deterministic output) and detects routes
/// without a schema; schemas not listed in `routes` are linted afterwards.
pub fn lint_schemas(schemas: &HashMap<String, Arc<TableSchema>>, routes: &[String]) -> LintReport {
    let mut report = LintReport::default();

    let mut by_table: HashMap<String, &TableSchema> = HashMap::with_capacity(schemas.len());
    let mut table_owner: HashMap<&str, &str> = HashMap::with_capacity(schemas.len());

    let mut order: Vec<&String> = routes.iter().collect();
    let mut extra: Vec<&String> = schemas.keys().filter(|k| !routes.contains(k)).collect();
    extra.sort();
    order.extend(extra);

    // Duplicate `table` across routes (and build lookup).
    for route in &order {
        if let Some(s) = schemas.get(*route) {
            if s.table.is_empty() {
                continue;
            }
            match table_owner.get(s.table.as_str()) {
                Some(first) => report.error(
                    route,
                    format!("table '{}' is already declared by route '{}'", s.table, first),
                ),
                None => {
                    table_owner.insert(s.table.as_str(), route.as_str());
                    by_table.insert(s.table.clone(), s.as_ref());
                }
            }
        }
    }

    let ctx = LintCtx { schemas, by_table };

    for route in order {
        match schemas.get(route) {
            None => report.error(route, "route is listed in routes.json but no entity schema was loaded"),
            Some(s) => {
                if s.table.is_empty() {
                    report.error(route, "table name is empty");
                    continue;
                }
                lint_one(&ctx, &mut report, route, s);
            }
        }
    }

    report
}

/// Lint the raw JSON of one entity file for unknown keys (typos such as
/// `action_trigger` or `nullabel` deserialize silently into defaults).
pub fn lint_raw_entity(route: &str, raw: &Value) -> LintReport {
    let mut report = LintReport::default();
    let obj = match raw.as_object() {
        Some(o) => o,
        None => {
            report.error(route, "entity JSON root is not an object");
            return report;
        }
    };
    for key in obj.keys() {
        if !KNOWN_TOP_LEVEL_KEYS.contains(&key.as_str()) {
            report.warn(route, format!("unknown top-level key '{}' is ignored", key));
        }
    }
    if let Some(cols) = obj.get("columns").and_then(|c| c.as_array()) {
        for (i, col) in cols.iter().enumerate() {
            if let Some(co) = col.as_object() {
                let name = co.get("name").and_then(|n| n.as_str()).unwrap_or("");
                for key in co.keys() {
                    if !KNOWN_COLUMN_KEYS.contains(&key.as_str()) {
                        report.warn(
                            route,
                            format!("columns[{}] '{}' has unknown key '{}' which is ignored", i, name, key),
                        );
                    }
                }
            }
        }
    }
    report
}

pub async fn validate_api_formula(formula: &str, body: &Value, auth_token: Option<&str>) -> Result<(), String> {
    if !formula.starts_with("API:") {
        return Ok(());
    }

    // Check for suffix |operator:response_path:request_variable
    // Operator can be: eq, neq, in (formerly EXISTS)
    let (base_formula, validation_rule) = match formula.find('|') {
        Some(idx) => {
            let rule = &formula[idx + 1..];
            (&formula[..idx], Some(rule))
        },
        None => (formula, None),
    };

    let parts: Vec<&str> = base_formula.splitn(3, ':').collect();
    // API:METHOD:URL
    if parts.len() < 3 {
        return Ok(()); 
    }

    let method = parts[1].to_uppercase();
    let url_formula = parts[2];

    match crate::database::state::build_url_from_formula(url_formula, body) {
        Ok(url) => {
            // log_output untuk debug
            log_output("DEBUG", "API VALIDATION","URL", url.to_string(), true);
            let client = reqwest::Client::new();
            let mut builder = match method.as_str() {
                "GET" => client.get(&url),
                "POST" => client.post(&url).json(body),
                "PUT" => client.put(&url).json(body),
                "DELETE" => client.delete(&url),
                _ => client.get(&url),
            };

            if let Some(token) = auth_token {
                builder = builder.header("Authorization", token);
            }

            match builder.send().await {
                Ok(res) => {
                    if !res.status().is_success() {
                        let status = res.status();
                        let msg = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        return Err(format!("Validation failed (API {}): {}", status, msg));
                    }
                    
                    // Operator Check Logic
                    if let Some(rule_str) = validation_rule {
                        let rule_parts: Vec<&str> = rule_str.splitn(3, ':').collect();
                        if rule_parts.len() == 3 {
                            let operator = rule_parts[0];
                            let resp_path = rule_parts[1];
                            let req_path = rule_parts[2];

                            let resp_json: Value = res.json().await.map_err(|e| format!("Failed to parse response JSON: {}", e))?;
                            
                            // Get value from response path
                            let resp_val_opt = crate::database::state::get_by_path_value(&resp_json, resp_path);

                            // Get value from request body to check
                            let req_key = req_path.strip_prefix("request.").unwrap_or(req_path);
                            let req_val_opt = crate::database::state::get_by_path_value(body, req_key);

                            // Handle unwrapping based on operator requirements
                            // 'eq', 'neq' need single values. 'in' needs array from response.

                            match operator {
                                "eq" => {
                                     let resp_val = resp_val_opt.ok_or_else(|| format!("Validation failed: Response path '{}' not found", resp_path))?;
                                     let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                     if resp_val != req_val {
                                         return Err(format!("Validation failed: Response '{:?}' != Request '{:?}'", resp_val, req_val));
                                     }
                                },
                                "neq" => {
                                     let resp_val = resp_val_opt.ok_or_else(|| format!("Validation failed: Response path '{}' not found", resp_path))?;
                                     let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                     if resp_val == req_val {
                                         return Err(format!("Validation failed: Response '{:?}' == Request '{:?}'", resp_val, req_val));
                                     }
                                },
                                "in" => {
                                    // Response must be array
                                    let target_array = match resp_val_opt {
                                        Some(Value::Array(arr)) => arr,
                                        _ => return Err(format!("Validation failed: Response path '{}' is not an array for 'in' operator", resp_path)),
                                    };
                                    let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                    
                                    let found = target_array.iter().any(|item| item == req_val);
                                    if !found {
                                        return Err(format!("Validation failed: Value '{:?}' not found in allowed list", req_val));
                                    }
                                },
                                _ => {
                                    return Err(format!("Unknown validation operator: {}", operator));
                                }
                            }
                        }
                    }

                    Ok(())
                },
                Err(e) => {
                    Err(format!("Error calling validation API: {}", e))
                }
            }
        },
        Err(e) => {
            Err(format!("Error building validation URL: {}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, Index, PrimaryKey, TableSchema};

    fn make_valid_schema() -> TableSchema {
        TableSchema {
            table: "test_table".to_string(),
            primary_key: PrimaryKey {
                columns: vec!["id".to_string()],
            },
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_data: "bigint".to_string(),
                    ..Default::default()
                },
                Column {
                    name: "name".to_string(),
                    type_data: "varchar(255)".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn test_valid_schema_passes_validation() {
        let schema = make_valid_schema();
        assert!(validate_table_design(&schema).is_ok());
    }

    #[test]
    fn test_empty_table_name_fails() {
        let mut schema = make_valid_schema();
        schema.table = "".to_string();
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Table name cannot be empty")));
    }

    #[test]
    fn test_empty_primary_key_fails() {
        let mut schema = make_valid_schema();
        schema.primary_key.columns = vec![];
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Primary key columns cannot be empty")));
    }

    #[test]
    fn test_pk_column_not_in_columns_list_fails() {
        let mut schema = make_valid_schema();
        schema.primary_key.columns = vec!["nonexistent_col".to_string()];
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("does not exist in columns")),
            "Expected error about missing PK column, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_empty_columns_list_fails() {
        let schema = TableSchema {
            table: "t".to_string(),
            primary_key: PrimaryKey { columns: vec![] },
            columns: vec![],
            ..Default::default()
        };
        let result = validate_table_design(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_column_type_fails() {
        let mut schema = make_valid_schema();
        schema.columns.push(Column {
            name: "bad_col".to_string(),
            type_data: "totally_fake_type".to_string(),
            ..Default::default()
        });
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("invalid type_data")),
            "Expected type validation error, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_all_allowed_column_types_pass() {
        let base_types = [
            "varchar(255)",
            "int",
            "bigint",
            "tinyint",
            "smallint",
            "mediumint",
            "text",
            "longtext",
            "datetime",
            "date",
            "timestamp",
            "boolean",
            "bool",
            "decimal(10,2)",
            "float",
            "double",
            "json",
            "blob",
            "enum",
        ];
        for t in &base_types {
            let mut schema = make_valid_schema();
            schema.columns.push(Column {
                name: "test_col".to_string(),
                type_data: t.to_string(),
                ..Default::default()
            });
            let result = validate_table_design(&schema);
            assert!(
                result.is_ok(),
                "Type '{}' should be valid but got errors: {:?}",
                t,
                result.err()
            );
        }
    }

    #[test]
    fn test_index_on_nonexistent_column_fails() {
        let mut schema = make_valid_schema();
        schema.indexes = vec![Index {
            name: "idx_missing".to_string(),
            columns: vec!["missing_col".to_string()],
            unique: false,
        }];
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("does not exist in columns")),
            "Expected index column error, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_index_on_pk_column_should_error() {
        let mut schema = make_valid_schema();
        // Index on the primary key column — should produce a warning/error
        schema.indexes = vec![Index {
            name: "idx_id".to_string(),
            columns: vec!["id".to_string()],
            unique: false,
        }];
        let result = validate_table_design(&schema);
        assert!(result.is_err(), "PK column should not be explicitly indexed");
    }

    #[test]
    fn test_multiple_errors_accumulated() {
        let schema = TableSchema {
            table: "".to_string(),
            primary_key: PrimaryKey { columns: vec![] },
            columns: vec![],
            ..Default::default()
        };
        let result = validate_table_design(&schema);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.len() >= 2, "Expected multiple accumulated errors, got: {:?}", errors);
    }

    #[test]
    fn test_composite_primary_key_valid() {
        let schema = TableSchema {
            table: "order_items".to_string(),
            primary_key: PrimaryKey {
                columns: vec!["order_id".to_string(), "product_id".to_string()],
            },
            columns: vec![
                Column {
                    name: "order_id".to_string(),
                    type_data: "bigint".to_string(),
                    ..Default::default()
                },
                Column {
                    name: "product_id".to_string(),
                    type_data: "bigint".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(validate_table_design(&schema).is_ok());
    }

    #[test]
    fn test_get_disabled_skip_parameter_checks() {
        // When GET is not enabled, parameter checks should be skipped
        let mut schema = make_valid_schema();
        schema.get.enable_method = false;
        schema.get.parameters = vec![]; // no standard params
        assert!(validate_table_design(&schema).is_ok());
    }
}

#[cfg(test)]
mod lint_tests {
    use super::*;
    use crate::model::TableSchema;
    use serde_json::json;

    fn schema(v: Value) -> Arc<TableSchema> {
        Arc::new(serde_json::from_value(v).expect("valid TableSchema JSON"))
    }

    fn order_json() -> Value {
        json!({
            "table": "sales_order",
            "primary_key": {"columns": ["id"]},
            "columns": [
                {"name": "id", "type_data": "bigint", "auto_increment": true},
                {"name": "so_number", "type_data": "varchar(30)"},
                {"name": "status", "type_data": "varchar(20)", "default": "'draft'"},
                {"name": "total", "type_data": "decimal(18,2)", "default": "0"},
                {"name": "created_at", "type_data": "datetime", "default": "CURRENT_TIMESTAMP"}
            ],
            "indexes": [{"name": "idx_so_number", "columns": ["so_number"], "unique": true}],
            "foreign_keys": [],
            "details": [{
                "field": "items",
                "target_table": "sales_order_item",
                "foreign_key_column": "sales_order_id",
                "columns": ["product_id", "qty"]
            }],
            "action_triggers": [{
                "name": "on_ship",
                "event": "on_status_change",
                "condition": {"field": "status", "to": "SHIPPED"},
                "actions": [{"type": "update", "target_table": "sales_order_item", "set": {"qty": 1}}]
            }],
            "state_machine": {"field": "status", "transitions": []},
            "locked_when": {"status": ["SHIPPED"], "except_columns": ["status"]},
            "get": {
                "enable_method": true,
                "columns": ["sales_order.id", "so_number as number", "COUNT(id) as n"],
                "parameters": ["search", "page", "limit", "sort", "ascending", "*so_number.eq", "status.eq|so_number.like", "sales_order.total.gte"],
                "order_by": ["-created_at", "so_number DESC"]
            },
            "post": {"enable_method": true, "columns": ["so_number*", "status"]},
            "put": {"enable_method": true, "columns": ["status*"]}
        })
    }

    fn item_json() -> Value {
        json!({
            "table": "sales_order_item",
            "primary_key": {"columns": ["id"]},
            "columns": [
                {"name": "id", "type_data": "bigint", "auto_increment": true},
                {"name": "sales_order_id", "type_data": "bigint"},
                {"name": "product_id", "type_data": "bigint"},
                {"name": "qty", "type_data": "int"}
            ],
            "foreign_keys": [{
                "column": "sales_order_id", "reference_table": "sales_order",
                "reference_column": "id", "on_delete": "cascade", "on_update": "cascade"
            }],
            "get": {"enable_method": true, "parameters": ["page", "limit"]}
        })
    }

    fn two() -> (HashMap<String, Arc<TableSchema>>, Vec<String>) {
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(order_json()));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        (m, vec!["sales_order".to_string(), "sales_order_item".to_string()])
    }

    #[test]
    fn valid_schemas_produce_no_errors() {
        let (m, r) = two();
        let rep = lint_schemas(&m, &r);
        assert!(rep.errors.is_empty(), "unexpected errors: {:?}", rep.errors);
        assert!(rep.warnings.is_empty(), "unexpected warnings: {:?}", rep.warnings);
    }

    #[test]
    fn invalid_identifier_is_error() {
        let mut v = order_json();
        v["columns"][1]["name"] = json!("so-number; DROP TABLE x");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(
            rep.errors.iter().any(|e| e.contains("not a valid SQL identifier")),
            "{:?}",
            rep.errors
        );
    }

    #[test]
    fn unknown_parameter_operator_is_error() {
        let mut v = order_json();
        v["get"]["parameters"] = json!(["page", "status.equals", "so_number.regex"]);
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        let ops: Vec<_> = rep.errors.iter().filter(|e| e.contains("unknown operator")).collect();
        assert_eq!(ops.len(), 2, "{:?}", rep.errors);
        assert!(ops[0].contains("'equals'"));
        assert!(ops[1].contains("'regex'"));
    }

    #[test]
    fn missing_parameter_column_is_error() {
        let mut v = order_json();
        v["get"]["parameters"] = json!(["ghost.eq"]);
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(rep.errors.iter().any(|e| e.contains("column 'ghost'")), "{:?}", rep.errors);
    }

    #[test]
    fn bad_trigger_event_is_error() {
        let mut v = order_json();
        v["action_triggers"][0]["event"] = json!("on_approve");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(rep.errors.iter().any(|e| e.contains("unknown event 'on_approve'")), "{:?}", rep.errors);
    }

    #[test]
    fn trigger_unknown_target_table_is_warning() {
        let mut v = order_json();
        v["action_triggers"][0]["actions"][0]["target_table"] = json!("ledger");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert!(rep.warnings.iter().any(|w| w.contains("target_table 'ledger'")), "{:?}", rep.warnings);
    }

    #[test]
    fn details_target_missing_is_error() {
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(order_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string()]);
        assert!(
            rep.errors.iter().any(|e| e.contains("target_table 'sales_order_item' is not a loaded entity")),
            "{:?}",
            rep.errors
        );
    }

    #[test]
    fn details_fk_column_missing_in_target_is_error() {
        let mut v = order_json();
        v["details"][0]["foreign_key_column"] = json!("order_id");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(rep.errors.iter().any(|e| e.contains("foreign_key_column 'order_id'")), "{:?}", rep.errors);
    }

    #[test]
    fn duplicate_table_across_routes_is_error() {
        let mut dup = item_json();
        dup["table"] = json!("sales_order");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(order_json()));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        m.insert("orders_v2".to_string(), schema(dup));
        let rep = lint_schemas(
            &m,
            &["sales_order".to_string(), "sales_order_item".to_string(), "orders_v2".to_string()],
        );
        assert!(rep.errors.iter().any(|e| e.contains("already declared by route 'sales_order'")), "{:?}", rep.errors);
    }

    #[test]
    fn duplicate_column_and_missing_state_field_are_errors() {
        let mut v = order_json();
        v["columns"].as_array_mut().unwrap().push(json!({"name": "status", "type_data": "varchar(5)"}));
        v["state_machine"]["field"] = json!("state");
        let mut m = HashMap::new();
        m.insert("sales_order".to_string(), schema(v));
        m.insert("sales_order_item".to_string(), schema(item_json()));
        let rep = lint_schemas(&m, &["sales_order".to_string(), "sales_order_item".to_string()]);
        assert!(rep.errors.iter().any(|e| e.contains("duplicate column name 'status'")), "{:?}", rep.errors);
        assert!(rep.errors.iter().any(|e| e.contains("state_machine.field 'state'")), "{:?}", rep.errors);
    }

    #[test]
    fn warnings_for_empty_params_external_fk_and_bare_default() {
        let mut v = item_json();
        v["get"]["parameters"] = json!([]);
        v["foreign_keys"][0]["reference_table"] = json!("external_crm");
        v["columns"].as_array_mut().unwrap().push(json!({"name": "note", "type_data": "varchar(50)", "default": "none"}));
        let mut m = HashMap::new();
        m.insert("sales_order_item".to_string(), schema(v));
        let rep = lint_schemas(&m, &["sales_order_item".to_string()]);
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert!(rep.warnings.iter().any(|w| w.contains("page/limit/sort")), "{:?}", rep.warnings);
        assert!(rep.warnings.iter().any(|w| w.contains("reference_table 'external_crm'")), "{:?}", rep.warnings);
        assert!(rep.warnings.iter().any(|w| w.contains("default 'none' is an unquoted string")), "{:?}", rep.warnings);
    }

    #[test]
    fn route_without_schema_is_error() {
        let m: HashMap<String, Arc<TableSchema>> = HashMap::new();
        let rep = lint_schemas(&m, &["ghost".to_string()]);
        assert_eq!(rep.errors.len(), 1);
        assert!(rep.errors[0].contains("no entity schema was loaded"));
    }

    #[test]
    fn raw_entity_unknown_keys_are_warnings() {
        let raw = json!({
            "table": "t",
            "action_trigger": [],
            "position": {"x": 1},
            "columns": [{"name": "id", "type_data": "int", "nullabel": true, "default_value": null}]
        });
        let rep = lint_raw_entity("t", &raw);
        assert!(rep.errors.is_empty());
        assert_eq!(rep.warnings.len(), 2, "{:?}", rep.warnings);
        assert!(rep.warnings[0].contains("'action_trigger'"));
        assert!(rep.warnings[1].contains("'nullabel'"));
    }

    #[test]
    fn identifier_regex_equivalent() {
        assert!(is_valid_identifier("_a1"));
        assert!(is_valid_identifier("Table_9"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("9abc"));
        assert!(!is_valid_identifier("a b"));
        assert!(!is_valid_identifier("a;"));
        assert!(!is_valid_identifier("naïve"));
    }
}
