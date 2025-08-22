use std::sync::Arc;
use serde_json::Value;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::{log::log_output};

pub struct QueryConvertor {
    pub datetime_now: String,    
}

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
    pub db_type: String,
    pub secret: String,
    pub encrypt_key: String,
    pub query_convertor: QueryConvertor,
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
    async fn query_with_params(&self, sql: &str, _params: Vec<DbParam>) -> Result<Vec<Value>, anyhow::Error> {
        // Default fallback calls non-parameterized method (not recommended).
        // Implementations for each DB override this to bind safely.
        self.query(sql).await
    }

    async fn get_total_rows_with_params(&self, sql: &str, _params: Vec<DbParam>) -> Result<i32, anyhow::Error> {
        self.get_total_rows(sql).await
    }
}


pub async fn execute_sql_formula(db: &Arc<dyn DbRepository>, sql: String, body: &serde_json::Value, route: &str) {
    // Build parameterized SQL and params safely, then execute.
    let (built_sql, params) = build_sql_and_params_from_formula(&sql, body);
    log_output("QUERY", "POST", route, built_sql.clone(), true);
    match db.query_with_params(&built_sql, params).await {
        Ok(_) => println!("AFTER POST SQL query executed successfully"),
        Err(err) => println!("Error executing SQL query: {}", err),
    }
}


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
    input
        .replace("'", "`")       // escape single quotes (SQL standard)
        .replace("--", "")        // remove SQL comment syntax
        .replace(";", "")         // prevent query stacking
        .replace("\"", "")        // remove double quotes
        .replace("\\", "")        // prevent backslash escape (esp. in MySQL)
        .replace("/*", "")        // remove block comment start
        .replace("*/", "")        // remove block comment end
        .replace("#", "")         // MySQL comment
        .replace("`", "")         // MySQL identifier escape
        .replace(" OR ", " ")     // remove logic operators
        .replace(" or ", " ")
        .replace(" AND ", " ")
        .replace(" and ", " ")
        .replace("=", "")         // remove equal signs
        .replace("(", "")         // remove open parenthesis
        .replace(")", "")         // remove close parenthesis
        .replace("%", "")         // remove wildcards in LIKE
        .replace("_", "")         // remove underscore wildcard
        .replace("\u{0000}", "")  // remove null byte
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
static RE_NESTED: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{(\w+)\[\s*\{([^}]+)\}\s*\]\.(\w+)\}").unwrap());
static RE_PLAIN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{(\w+)\[(\d+)\]\.(\w+)\}").unwrap());
static RE_REQ: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{request\.([^}]+)\}").unwrap());
static RE_LEFTOVER: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{[^}]+\}").unwrap());

/// Build parameterized SQL and params from a "SQL:" formula string.
/// Supported placeholders:
/// - {request.field} -> bound as a parameter
/// - {table[<id>].col} or {table[{request.id}].col} -> expanded to subselect
pub fn build_sql_and_params_from_formula(sql_formula: &str, body: &serde_json::Value) -> (String, Vec<DbParam>) {
    // Strip optional prefix
    let mut sql = sql_formula.trim().strip_prefix("SQL:").unwrap_or(sql_formula).to_string();
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

        // resolve inner value from body with dotted path support
        let key = inner.strip_prefix("request.").unwrap_or(inner);
        // Prefer numeric id, fallback to 0
        let id_num: i64 = match get_by_path(body, key) {
            Some(serde_json::Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
            Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
            Some(serde_json::Value::Bool(b)) => if *b { 1 } else { 0 },
            Some(_) | None => 0,
        };
        let sub = format!("(SELECT {} FROM {} WHERE id = {})", field, table, id_num);
        sql = RE_NESTED.replace(&sql, sub.as_str()).to_string();
    }

    // 2) Resolve plain subselects with numeric id: {table[123].field}
    sql = RE_PLAIN
        .replace_all(&sql, |caps: &regex::Captures| {
            let table = &caps[1];
            let id = &caps[2];
            let field = &caps[3];
            format!("(SELECT {} FROM {} WHERE id = {})", field, table, id)
        })
        .to_string();

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
                    if let Some(i) = n.as_i64() { params.push(DbParam::I64(i)); }
                    else if let Some(f) = n.as_f64() { params.push(DbParam::F64(f)); }
                    else { params.push(DbParam::Str(n.to_string())); }
                }
                Some(serde_json::Value::String(s)) => {
                    // try numeric first to reduce type mismatch on numeric columns
                    if let Ok(i) = s.parse::<i64>() { params.push(DbParam::I64(i)); }
                    else if let Ok(f) = s.parse::<f64>() { params.push(DbParam::F64(f)); }
                    else { params.push(DbParam::Str(s.clone())); }
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
        sql = RE_LEFTOVER.replace_all(&sql, "").to_string();
    }

    (sql, params)
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
            body
                .get(key)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default()
        })
        .to_string();

    string_formula
}