use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::sync::Arc;

use crate::log::log_output;
use anyhow::{anyhow, Result};

pub struct QueryConverter {
    pub datetime_now: String,
}

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
    pub db_type: String,
    pub secret: String,
    pub encrypt_key: String,
    pub query_converter: QueryConverter,
    pub whitelist_ips: Vec<String>,
    pub route_publics: Vec<String>,
}

/// Simple cross-DB parameter type for binding values safely.
#[derive(Debug, Clone)]
pub enum DbParam {
    I64(i64),
    F64(f64),
    Str(String),
    Bool(bool),
    Null,
}

#[async_trait::async_trait]
pub trait DbRepository: Send + Sync {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error>;
    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error>;

    // Parameterized variants for safer queries. SQL must contain placeholders
    // appropriate for the target DB (MySQL/SQLite: `?`, Postgres: `$1,$2,...`).
    async fn query_with_params(
        &self,
        sql: &str,
        _params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        // Default fallback calls non-parameterized method (not recommended).
        // Implementations for each DB override this to bind safely.
        self.query(sql).await
    }

    async fn get_total_rows_with_params(
        &self,
        sql: &str,
        _params: Vec<DbParam>,
    ) -> Result<i32, anyhow::Error> {
        self.get_total_rows(sql).await
    }

    // Transaction support
    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error>;
}

#[async_trait::async_trait]
pub trait DbTransaction: Send + Sync {
    async fn query_with_params(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error>;
    async fn commit(self: Box<Self>) -> Result<(), anyhow::Error>;
    async fn rollback(self: Box<Self>) -> Result<(), anyhow::Error>;
}

pub async fn execute_sql_formula(
    db: &Arc<dyn DbRepository>,
    sql: String,
    body: &serde_json::Value,
    route: &str,
) -> Result<Vec<Value>, anyhow::Error> {
    // Build parameterized SQL and params safely, then execute.
    match build_sql_and_params_from_formula(&sql, body) {
        Ok((built_sql, params)) => {
            log_output("QUERY", "AFTER", route, built_sql.clone(), true);
            match db.query_with_params(&built_sql, params).await {
                Ok(rows) => {
                    log_output(
                        "SUCCESS",
                        "AFTER",
                        route,
                        "SQL formula executed successfully".to_string(),
                        true,
                    );
                    Ok(rows)
                }
                Err(err) => {
                    log_output(
                        "ERROR",
                        "AFTER",
                        route,
                        format!("Error executing SQL query: {}", err),
                        false,
                    );
                    Err(err)
                }
            }
        }
        Err(e) => {
            log_output(
                "ERROR",
                "AFTER",
                route,
                format!("Error building SQL formula: {}", e),
                false,
            );
            Err(e)
        }
    }
}

pub async fn execute_sql_formula_with_transaction(
    tx: &mut Box<dyn DbTransaction>,
    sql: String,
    body: &serde_json::Value,
    route: &str,
) -> Result<(), anyhow::Error> {
    // Build parameterized SQL and params safely, then execute within transaction.
    match build_sql_and_params_from_formula(&sql, body) {
        Ok((built_sql, params)) => {
            log_output("QUERY", "AFTER", route, built_sql.clone(), true);
            match tx.query_with_params(&built_sql, params).await {
                Ok(_) => {
                    log_output(
                        "SUCCESS",
                        "AFTER",
                        route,
                        "SQL formula executed successfully in transaction".to_string(),
                        true,
                    );
                    Ok(())
                }
                Err(err) => {
                    log_output(
                        "ERROR",
                        "AFTER",
                        route,
                        format!("Error executing SQL query in transaction: {}", err),
                        false,
                    );
                    Err(err)
                }
            }
        }
        Err(e) => {
            log_output(
                "ERROR",
                "AFTER",
                route,
                format!("Error building SQL formula: {}", e),
                false,
            );
            Err(e)
        }
    }
}

#[allow(dead_code)]
pub fn concat_column_values(values: Vec<Value>, column_name: &str, separator: &str) -> String {
    let mut result = Vec::new();

    for value in values {
        if let Value::Object(obj) = value {
            if let Some(v) = obj.get(column_name) {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "".to_string(),
                    _ => "".to_string(),
                };
                result.push(s);
            }
        }
    }

    result.join(separator)
}

#[allow(dead_code)]
pub fn sanitize_sql_input(input: String) -> String {
    // More comprehensive SQL injection prevention
    let dangerous_patterns = [
        "--", "/*", "*/", ";", "\\", "xp_", "sp_", "exec", "execute", "select", "insert", "update",
        "delete", "drop", "create", "alter", "union",
    ];

    let mut sanitized = input
        .replace("'", "''") // Proper SQL escape for single quotes
        .replace("\"", "\"\"") // Escape double quotes
        .replace("\u{0000}", "") // Remove null bytes
        .replace("\r", "") // Remove carriage returns
        .replace("\n", " "); // Replace newlines with spaces

    // Remove dangerous SQL keywords (case insensitive)
    for pattern in dangerous_patterns {
        sanitized = sanitized.replace(&pattern.to_lowercase(), "");
        sanitized = sanitized.replace(&pattern.to_uppercase(), "");
    }

    // Limit length to prevent buffer overflow attacks
    if sanitized.len() > 1000 {
        sanitized.truncate(1000);
    }

    sanitized.trim().to_string()
}

pub fn convert_to_sql(input: &str) -> String {
    // Pastikan formatnya seperti products[1].price
    let re = match regex::Regex::new(r"^(\w+)\[(\d+)\]\.(\w+)$") {
        Ok(re) => re,
        Err(_) => return "".to_string(),
    };

    if let Some(captures) = re.captures(input) {
        let table = &captures[1];
        let id = &captures[2];
        let field = &captures[3];

        format!("(SELECT {} FROM {} WHERE id = {})", field, table, id)
    } else {
        "".to_string()
    }
}

// Precompiled regexes for performance
static RE_NESTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\{(\w+)\[\s*\{([^}]+)\}\s*\]\.(\w+)\}").unwrap());
static RE_PLAIN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{(\w+)\[(\d+)\]\.(\w+)\}").unwrap());
static RE_REQ: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{request\.([^}]+)\}").unwrap());
static RE_LEFTOVER: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{[^}]+\}").unwrap());
static RE_IDENT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

/// Build parameterized SQL and params from a "SQL:" formula string.
/// Supported placeholders:
/// - {request.field} -> bound as a parameter
/// - {table[<id>].col} or {table[{request.id}].col} -> expanded to subselect
pub fn build_sql_and_params_from_formula(
    sql_formula: &str,
    body: &serde_json::Value,
) -> Result<(String, Vec<DbParam>)> {
    // Strip optional prefix
    let mut sql = sql_formula
        .trim()
        .strip_prefix("SQL:")
        .unwrap_or(sql_formula)
        .to_string();
    let mut params: Vec<DbParam> = Vec::new();

    // Helper: get nested value by dotted path, e.g. "user.id" or "items.0.price"
    fn get_by_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
        let mut cur = root;
        for part in path.split('.') {
            if let Ok(idx) = part.parse::<usize>() {
                match cur {
                    serde_json::Value::Array(arr) => cur = arr.get(idx)?,
                    _ => return None,
                }
            } else {
                match cur {
                    serde_json::Value::Object(map) => cur = map.get(part)?,
                    _ => return None,
                }
            }
        }
        Some(cur)
    }

    // 1) Resolve nested subselects that include an inner {request.*}, deterministically.
    // Example: {products[{request.product_id}].price}
    while let Some(cap) = RE_NESTED.captures(&sql) {
        let table = cap.get(1).unwrap().as_str();
        let inner = cap.get(2).unwrap().as_str(); // e.g., request.product_id
        let field = cap.get(3).unwrap().as_str();

        // Validate identifiers strictly
        if !RE_IDENT.is_match(table) || !RE_IDENT.is_match(field) {
            return Err(anyhow!(
                "Invalid identifier in formula: {}.{}",
                table,
                field
            ));
        }

        // resolve inner value from body with dotted path support
        let key = inner.strip_prefix("request.").unwrap_or(inner);
        // Prefer numeric id, fallback to NULL
        let id_param = match get_by_path(body, key) {
            Some(serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    DbParam::I64(i)
                } else if let Some(f) = n.as_f64() {
                    DbParam::F64(f)
                } else {
                    DbParam::Null
                }
            }
            Some(serde_json::Value::String(s)) => {
                if let Ok(i) = s.parse::<i64>() {
                    DbParam::I64(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    DbParam::F64(f)
                } else {
                    DbParam::Null
                }
            }
            Some(serde_json::Value::Bool(b)) => {
                if *b {
                    DbParam::I64(1)
                } else {
                    DbParam::I64(0)
                }
            }
            _ => DbParam::Null,
        };
        // Parameterize the id inside subselect
        let sub = format!("(SELECT {} FROM {} WHERE id = ?)", field, table);
        params.push(id_param);
        sql = RE_NESTED.replace(&sql, sub.as_str()).to_string();
    }

    // 2) Resolve plain subselects with numeric id: {table[123].field}
    sql = RE_PLAIN
        .replace_all(&sql, |caps: &regex::Captures| {
            let table = &caps[1];
            let id = &caps[2];
            let field = &caps[3];
            if !RE_IDENT.is_match(table) || !RE_IDENT.is_match(field) {
                return String::from("{__INVALID_IDENT__}");
            }
            // Parameterize numeric id as well
            // We'll convert this literal to a placeholder and append the param below
            format!(
                "(SELECT {} FROM {} WHERE id = {{__ID_PLACEHOLDER__:{}}})",
                field, table, id
            )
        })
        .to_string();

    if sql.contains("{__INVALID_IDENT__}") {
        return Err(anyhow!("Invalid identifier in plain subselect formula"));
    }

    // Post-process RE_PLAIN replacements: find markers and turn them into '?' with params
    // This avoids running regex with a closure that captures external vec
    if sql.contains("{__ID_PLACEHOLDER__:") {
        let mut out = String::with_capacity(sql.len());
        let mut i = 0;
        while let Some(start) = sql[i..].find("{__ID_PLACEHOLDER__:") {
            let abs_start = i + start;
            out.push_str(&sql[i..abs_start]);
            // find closing '}'
            if let Some(end_rel) = sql[abs_start..].find('}') {
                let content = &sql[abs_start + "{__ID_PLACEHOLDER__:".len()..abs_start + end_rel];
                // parse number safely
                if let Ok(n) = content.parse::<i64>() {
                    params.push(DbParam::I64(n));
                } else if let Ok(f) = content.parse::<f64>() {
                    params.push(DbParam::F64(f));
                } else {
                    return Err(anyhow!(
                        "Invalid numeric id in plain subselect: {}",
                        content
                    ));
                }
                out.push('?');
                i = abs_start + end_rel + 1;
            } else {
                // malformed marker, copy rest and break
                return Err(anyhow!("Malformed ID placeholder in formula"));
            }
        }
        if i < sql.len() {
            out.push_str(&sql[i..]);
        }
        sql = out;
    }

    // 3) Bind remaining {request.*} placeholders as parameters (left-to-right, single pass).
    if RE_REQ.is_match(&sql) {
        let mut new_sql = String::with_capacity(sql.len());
        let mut last = 0usize;
        for cap in RE_REQ.captures_iter(&sql) {
            let full = cap.get(0).unwrap();
            let key = cap.get(1).unwrap().as_str();

            // push preceding literal SQL
            new_sql.push_str(&sql[last..full.start()]);
            // push placeholder
            new_sql.push('?');
            last = full.end();

            // Infer type directly from JSON value (with dotted path)
            match get_by_path(body, key) {
                Some(serde_json::Value::Null) | None => params.push(DbParam::Null),
                Some(serde_json::Value::Bool(b)) => params.push(DbParam::Bool(*b)),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        params.push(DbParam::I64(i));
                    } else if let Some(f) = n.as_f64() {
                        params.push(DbParam::F64(f));
                    } else {
                        params.push(DbParam::Str(n.to_string()));
                    }
                }
                Some(serde_json::Value::String(s)) => {
                    // try numeric first to reduce type mismatch on numeric columns
                    if let Ok(i) = s.parse::<i64>() {
                        params.push(DbParam::I64(i));
                    } else if let Ok(f) = s.parse::<f64>() {
                        params.push(DbParam::F64(f));
                    } else {
                        params.push(DbParam::Str(s.clone()));
                    }
                }
                Some(_) => params.push(DbParam::Str(String::new())),
            }
        }
        // push tail
        new_sql.push_str(&sql[last..]);
        sql = new_sql;
    }

    // 4) Remove any leftover braces content defensively (shouldn't remain).
    if RE_LEFTOVER.is_match(&sql) {
        return Err(anyhow!(
            "Unresolved placeholder(s) remain in formula: {}",
            sql
        ));
    }

    Ok((sql, params))
}

/// Legacy replacement used in older code paths. Prefer `build_sql_and_params_from_formula`.
#[allow(dead_code)]
pub fn formula_replace(mut string_formula: String, body: &serde_json::Value) -> String {
    // 1) Resolve nested subselects first
    while let Some(cap) = RE_NESTED.captures(&string_formula) {
        let table = cap.get(1).unwrap().as_str();
        let inner = cap.get(2).unwrap().as_str();
        let field = cap.get(3).unwrap().as_str();
        // support dotted path
        let key = inner.strip_prefix("request.").unwrap_or(inner);
        let raw = {
            // local helper mirrors get_by_path for this legacy path
            fn get<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
                let mut cur = root;
                for part in path.split('.') {
                    if let Ok(idx) = part.parse::<usize>() {
                        match cur {
                            serde_json::Value::Array(arr) => cur = arr.get(idx)?,
                            _ => return None,
                        }
                    } else {
                        match cur {
                            serde_json::Value::Object(map) => cur = map.get(part)?,
                            _ => return None,
                        }
                    }
                }
                Some(cur)
            }
            get(body, key)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default()
        };
        let id_num: i64 = raw.parse::<i64>().unwrap_or(0);
        let sub = format!("(SELECT {} FROM {} WHERE id = {})", field, table, id_num);
        string_formula = RE_NESTED.replace(&string_formula, sub.as_str()).to_string();
    }

    // 2) Resolve plain subselects
    string_formula = RE_PLAIN
        .replace_all(&string_formula, |caps: &regex::Captures| {
            let table = &caps[1];
            let id = &caps[2];
            let field = &caps[3];
            format!("(SELECT {} FROM {} WHERE id = {})", field, table, id)
        })
        .to_string();

    // 3) Replace request.* inline (legacy, non-parameterized)
    string_formula = RE_REQ
        .replace_all(&string_formula, |caps: &regex::Captures| {
            let key = &caps[1];
            body.get(key)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default()
        })
        .to_string();

    string_formula
}
