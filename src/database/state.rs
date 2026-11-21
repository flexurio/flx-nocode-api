use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::sync::Arc;

use crate::{auth::ClaimsConverter, log::log_output};
use anyhow::{anyhow, Result};

pub struct QueryConverter {
    pub datetime_now: String,
}

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
    pub require_auth: bool,
    pub db_type: crate::model::DbType,
    // Kept for API compatibility; JWT signing/verification now uses the cached
    // JWT_ENCODING_KEY / JWT_DECODING_KEY statics in auth.rs (built once from the
    // same SECRET_KEY env var) instead of rebuilding keys from this field per call.
    #[allow(dead_code)]
    pub secret: String,
    pub encrypt_key: String,
    pub query_converter: QueryConverter,
    pub whitelist_ips: Vec<String>,
    pub converter_token: ClaimsConverter,
    /// Backend-agnostic data store adapter (initially SQL-backed). Use this for new code paths.
    pub store: Arc<dyn crate::storage::traits::DataStore>,
    pub is_cachedb: bool, // whether store supports caching (e.g. Redis)
    pub write_queue_enabled: bool, // enable Redis write queue for POST/PUT/DELETE
    pub default_collate: String, // default collation for tables (e.g. utf8mb4_bin)
    /// L1 page cache: key = `flx:c2:<route>:v<version>:<sorted params>`,
    /// value = the already-serialized JSON body (see [`CachedPage`]).
    pub l1_cache: moka::future::Cache<String, CachedPage>,
}

impl AppState {
    /// Is `route` public (no JWT required)? Read from the live registry so a
    /// `routes.json` edit takes effect on reload.
    pub fn is_public_route(&self, route: &str) -> bool {
        crate::registry::is_public(route)
    }

    /// `rules.json` of the configuration currently in service.
    pub fn rules(&self) -> Value {
        crate::registry::current().rules_raw.clone()
    }
}

/// A cached GET page. The body is stored **serialized** so a cache hit costs one
/// `Bytes` ref-count clone instead of a `serde_json::Value` deep clone plus a
/// full re-serialization. The ETag lets clients get `304 Not Modified`.
#[derive(Clone, Debug)]
pub struct CachedPage {
    pub body: actix_web::web::Bytes,
    pub etag: String,
    pub next_cursor: Option<String>,
    pub total_data: i32,
}

impl CachedPage {
    /// Serialize a response once; returns `None` only if serialization fails.
    pub fn from_response(resp: &crate::model::WebResponse, next_cursor: Option<String>) -> Option<Self> {
        let body = serde_json::to_vec(resp).ok()?;
        let etag = Self::etag_for(&body);
        Some(Self {
            body: actix_web::web::Bytes::from(body),
            etag,
            next_cursor,
            total_data: resp.total_data,
        })
    }

    /// Strong ETag: deterministic across processes (SipHash with fixed keys) so
    /// every replica answers the same `If-None-Match`.
    pub fn etag_for(body: &[u8]) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        body.hash(&mut h);
        format!("\"{:016x}-{}\"", h.finish(), body.len())
    }

    /// Approximate memory weight for the L1 byte budget.
    pub fn weight(&self) -> u32 {
        (self.body.len() + self.etag.len() + self.next_cursor.as_ref().map(|c| c.len()).unwrap_or(0) + 64)
            .min(u32::MAX as usize) as u32
    }

    /// L2 (Redis) wire format: `<cursor>\n<total>\n<json body>`.
    pub fn encode(&self) -> String {
        let mut s = String::with_capacity(self.body.len() + 32);
        s.push_str(self.next_cursor.as_deref().unwrap_or(""));
        s.push('\n');
        s.push_str(&self.total_data.to_string());
        s.push('\n');
        // Body is JSON produced by serde_json, therefore valid UTF-8.
        s.push_str(std::str::from_utf8(&self.body).unwrap_or("{}"));
        s
    }

    pub fn decode(raw: String) -> Option<Self> {
        let (cursor, rest) = raw.split_once('\n')?;
        let (total, body) = rest.split_once('\n')?;
        if body.is_empty() {
            return None;
        }
        let body_bytes = body.as_bytes().to_vec();
        let etag = Self::etag_for(&body_bytes);
        Some(Self {
            body: actix_web::web::Bytes::from(body_bytes),
            etag,
            next_cursor: if cursor.is_empty() { None } else { Some(cursor.to_string()) },
            total_data: total.parse().unwrap_or(0),
        })
    }
}

/// Build the cache key for a GET page. `params` must already exclude control
/// parameters such as `redis`.
pub fn build_cache_key(route: &str, version: u64, params: &serde_json::Map<String, Value>) -> String {
    let prefix = crate::database::redis::build_key_prefix("c2", route);
    let mut keys: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    keys.sort();
    let mut key = String::with_capacity(prefix.len() + 16 + keys.iter().map(|k| k.len() + 1).sum::<usize>());
    key.push_str(&prefix);
    key.push_str(":v");
    key.push_str(&version.to_string());
    if !keys.is_empty() {
        key.push(':');
        key.push_str(&keys.join("&"));
    }
    key
}

/// Invalidate every cached GET page of `route` (L1 on this process, L2 and the
/// L1 of other replicas through the shared version key). O(1), never scans.
pub async fn invalidate_route(state: &AppState, route: &str) {
    crate::database::redis::bump_route_version(route, state.is_cachedb).await;
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

/// Connection-pool snapshot exported on `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    pub size: u32,
    pub idle: u32,
    pub max: u32,
}

#[async_trait::async_trait]
pub trait DbRepository: Send + Sync {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error>;

    /// Pool occupancy for observability; `None` when the backend has no pool.
    fn pool_stats(&self) -> Option<PoolStats> {
        None
    }

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
    /// Run a statement that returns no rows (UPDATE/DELETE/INSERT) and report
    /// the number of rows the database actually touched.
    async fn execute(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<u64, anyhow::Error>;
    async fn commit(self: Box<Self>) -> Result<(), anyhow::Error>;
    async fn rollback(self: Box<Self>) -> Result<(), anyhow::Error>;
}

/// Execute a parameterized SQL formula string within a generic TxStore transaction.
/// This mirrors `execute_sql_formula_with_transaction` but targets the new storage abstraction.
pub async fn execute_sql_formula_with_txstore(
    tx: &mut Box<dyn crate::storage::traits::TxStore>,
    sql: String,
    body: &serde_json::Value,
    route: &str,
    claims: Option<&crate::auth::Claims>,
) -> Result<(), anyhow::Error> {
    match build_sql_and_params_from_formula_with_claims(&sql, body, claims) {
        Ok((built_sql, params)) => {
            log_output("QUERY", "build_sql_and_params_from_formula", route, built_sql.clone(), true);
            crate::log::log_output_lazy("INFO", "FORMULA_PARAMS", route, || format!("Params for formula '{}': {:?}", built_sql, params), true);
            crate::log::log_output_lazy("BODY", "build_sql_and_params_from_formula", route, || format!("{:?}", body), true);
            // Log params explicitly to verify only placeholders become params
            log_output(
                "PARAMS",
                "build_sql_and_params_from_formula",
                route,
                format!("{:?}", params),
                true,
            );
            match tx.raw_sql(&built_sql, params).await {
                Ok(_) => {
                    log_output(
                        "SUCCESS",
                        "AFTER",
                        route,
                        "SQL formula executed successfully in txstore".to_string(),
                        true,
                    );
                    Ok(())
                }
                Err(err) => {
                    log_output(
                        "ERROR",
                        "AFTER",
                        route,
                        format!("Error executing SQL query in txstore: {}", err),
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

/// Convert '?' placeholders into dialect-specific placeholders while skipping string literals.
/// - postgres: $1, $2, ...
/// - mssql: @P1, @P2, ...
/// - others: unchanged
pub fn rehydrate_placeholders(sql: &str, dialect: &str) -> String {
    match dialect {
        "postgres" | "mssql" => {
            let bytes = sql.as_bytes();
            let mut out = String::with_capacity(sql.len());
            let mut i = 0;
            let mut in_str = false;
            let mut idx = 1usize;
            while i < bytes.len() {
                let c = bytes[i] as char;
                if c == '\'' { // handle single-quoted strings and escaped ''
                    out.push(c);
                    if in_str {
                        if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                            out.push('\'');
                            i += 2;
                            continue;
                        } else {
                            in_str = false;
                            i += 1;
                            continue;
                        }
                    } else {
                        in_str = true;
                        i += 1;
                        continue;
                    }
                }
                if !in_str && c == '?' {
                    match dialect {
                        "postgres" => {
                            out.push('$');
                            out.push_str(&idx.to_string());
                        }
                        "mssql" => {
                            out.push_str("@P");
                            out.push_str(&idx.to_string());
                        }
                        _ => out.push('?'),
                    }
                    idx += 1;
                    i += 1;
                    continue;
                }
                out.push(c);
                i += 1;
            }
            out
        }
        _ => sql.to_string(),
    }
}

// Precompiled regexes for performance
static RE_NESTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\{(\w+)\[\s*\{([^}]+)\}\s*\]\.(\w+)\}").unwrap());
static RE_PLAIN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{(\w+)\[(\d+)\]\.(\w+)\}").unwrap());
static RE_FORMULA_TOKEN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\{(?:__NUM__:(\d+)|request\.([^}]+)|user\.([^}]+))\}").unwrap()
});
static RE_URL_TOKEN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\{(?:request\.([^}]+)|user\.([^}]+))\}").unwrap());
static RE_LEFTOVER: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{[^}]+\}").unwrap());
static RE_IDENT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

/// Resolve a `{user.<key>}` placeholder into a bound [`DbParam`].
///
/// Supported keys:
/// - `id` -> `claims.id` (canonical integer bound as `I64`, otherwise `Str`; `Null` if missing/empty)
/// - `name` -> `claims.nm` (`Str`; `Null` if missing/empty)
/// - `custom` -> `claims.cs` (`Str` to preserve leading zeros / NIP; `Null` if missing/empty)
/// - `roles` -> role names from `claims.rl` without `/bitmask` suffix (`Str`, comma-separated; `""` if missing)
/// - `has_role:<role>` -> `I64(1)` if caller has `<role>`, else `I64(0)`
pub fn resolve_user_placeholder_param(
    key: &str,
    claims: Option<&crate::auth::Claims>,
) -> Result<DbParam> {
    let key = key.trim();
    match key {
        "id" => match claims.map(|c| c.id.trim()).filter(|s| !s.is_empty()) {
            None => Ok(DbParam::Null),
            Some(s) => {
                if let Ok(i) = s.parse::<i64>()
                    && i.to_string() == s
                {
                    Ok(DbParam::I64(i))
                } else {
                    Ok(DbParam::Str(s.to_string()))
                }
            }
        },
        "name" => match claims.map(|c| c.nm.as_str()).filter(|s| !s.trim().is_empty()) {
            None => Ok(DbParam::Null),
            Some(s) => Ok(DbParam::Str(s.to_string())),
        },
        "custom" => match claims.map(|c| c.cs.as_str()).filter(|s| !s.trim().is_empty()) {
            None => Ok(DbParam::Null),
            Some(s) => Ok(DbParam::Str(s.to_string())),
        },
        "roles" => Ok(DbParam::Str(
            claims.map(|c| c.role_names_csv()).unwrap_or_default(),
        )),
        _ => {
            if let Some(role) = key.strip_prefix("has_role:") {
                let role = role.trim();
                if role.is_empty() {
                    return Err(anyhow!("Empty role name in {{user.has_role:}} placeholder"));
                }
                let has = claims.is_some_and(|c| c.has_role(role));
                Ok(DbParam::I64(if has { 1 } else { 0 }))
            } else {
                Err(anyhow!("Unknown user placeholder: {{user.{}}}", key))
            }
        }
    }
}

/// Resolve a `{user.<key>}` placeholder into a string value (for URL / template interpolation).
/// Returns `Ok(None)` when the claim is absent or empty (except `roles` -> `Some("")` and `has_role:*` -> `Some("1"|"0")`).
pub fn resolve_user_placeholder_str(
    key: &str,
    claims: Option<&crate::auth::Claims>,
) -> Result<Option<String>> {
    let key = key.trim();
    match key {
        "id" => Ok(claims
            .map(|c| c.id.trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string)),
        "name" => Ok(claims
            .map(|c| c.nm.as_str())
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)),
        "custom" => Ok(claims
            .map(|c| c.cs.as_str())
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)),
        "roles" => Ok(Some(
            claims.map(|c| c.role_names_csv()).unwrap_or_default(),
        )),
        _ => {
            if let Some(role) = key.strip_prefix("has_role:") {
                let role = role.trim();
                if role.is_empty() {
                    return Err(anyhow!("Empty role name in {{user.has_role:}} placeholder"));
                }
                let has = claims.is_some_and(|c| c.has_role(role));
                Ok(Some(if has { "1" } else { "0" }.to_string()))
            } else {
                Err(anyhow!("Unknown user placeholder: {{user.{}}}", key))
            }
        }
    }
}

// Helper: get nested value by dotted path, e.g. "user.id" or "items.0.price"
pub fn get_by_path_value<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
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

/// Build parameterized SQL and params from a "SQL:" formula string.
/// Supported placeholders:
/// - `{request.field}` -> bound as a parameter
/// - `{user.id}`, `{user.name}`, `{user.custom}`, `{user.roles}`, `{user.has_role:<role>}` -> bound as a parameter
/// - `{table[<id>].col}` or `{table[{request.id}].col}` or `{table[{user.id}].col}` -> expanded to subselect
pub fn build_sql_and_params_from_formula(
    sql_formula: &str,
    body: &serde_json::Value,
) -> Result<(String, Vec<DbParam>)> {
    build_sql_and_params_from_formula_with_claims(sql_formula, body, None)
}

/// Same as [`build_sql_and_params_from_formula`], also resolving `{user.*}` placeholders
/// from the authenticated caller's JWT claims as bound parameters (never string interpolation).
pub fn build_sql_and_params_from_formula_with_claims(
    sql_formula: &str,
    body: &serde_json::Value,
    claims: Option<&crate::auth::Claims>,
) -> Result<(String, Vec<DbParam>)> {
    // Strip optional prefix
    let mut sql = sql_formula
        .trim()
        .strip_prefix("SQL:")
        .unwrap_or(sql_formula)
        .to_string();

    // log_output SQL
    log_output("QUERY", "BEFORE/AFTER", "build_sql_and_params_from_formula", sql.clone(), true);
    let mut params: Vec<DbParam> = Vec::new();

    // 1) Expand nested subselects: {table[{request.x}].col} or {table[{user.x}].col}
    //    Expand structurally without pushing params yet so the single left-to-right pass
    //    in step 3 binds every `?` in exact positional order.
    while let Some(cap) = RE_NESTED.captures(&sql) {
        crate::log::log_output_lazy("INFO", "CAP", "build_sql_and_params_from_formula", || format!("{:?}", cap), true);
        let table = cap.get(1).unwrap().as_str();
        let inner = cap.get(2).unwrap().as_str().trim(); // e.g., request.product_id or user.id
        let field = cap.get(3).unwrap().as_str();

        crate::log::log_output_lazy("INFO", "NESTED", "build_sql_and_params_from_formula", || format!("table:{}, inner: {} field: {}", table, inner, field), true);

        if !RE_IDENT.is_match(table) || !RE_IDENT.is_match(field) {
            return Err(anyhow!(
                "Invalid identifier in formula: {}.{}",
                table,
                field
            ));
        }

        let inner_token = if inner.starts_with("request.") || inner.starts_with("user.") {
            format!("{{{}}}", inner)
        } else {
            format!("{{request.{}}}", inner)
        };
        let sub = format!("(SELECT {} FROM {} WHERE id = {})", field, table, inner_token);
        sql = RE_NESTED.replace(&sql, sub.as_str()).to_string();
    }

    // 2) Expand plain subselects with numeric id: {table[123].field}
    sql = RE_PLAIN
        .replace_all(&sql, |caps: &regex::Captures| {
            let table = &caps[1];
            let id = &caps[2];
            let field = &caps[3];
            if !RE_IDENT.is_match(table) || !RE_IDENT.is_match(field) {
                return String::from("{__INVALID_IDENT__}");
            }
            format!(
                "(SELECT {} FROM {} WHERE id = {{__NUM__:{}}})",
                field, table, id
            )
        })
        .to_string();

    if sql.contains("{__INVALID_IDENT__}") {
        return Err(anyhow!("Invalid identifier in plain subselect formula"));
    }

    // 3) Single left-to-right pass binding {__NUM__:*}, {request.*}, and {user.*} as `?` params.
    if RE_FORMULA_TOKEN.is_match(&sql) {
        let mut new_sql = String::with_capacity(sql.len());
        let mut last = 0usize;
        for cap in RE_FORMULA_TOKEN.captures_iter(&sql) {
            let full = cap.get(0).unwrap();
            new_sql.push_str(&sql[last..full.start()]);
            new_sql.push('?');
            last = full.end();

            if let Some(num_m) = cap.get(1) {
                let raw = num_m.as_str();
                if let Ok(n) = raw.parse::<i64>() {
                    params.push(DbParam::I64(n));
                } else if let Ok(f) = raw.parse::<f64>() {
                    params.push(DbParam::F64(f));
                } else {
                    return Err(anyhow!("Invalid numeric id in plain subselect: {}", raw));
                }
            } else if let Some(req_m) = cap.get(2) {
                let key = req_m.as_str();
                match get_by_path_value(body, key) {
                    Some(serde_json::Value::Null) | None => params.push(DbParam::Null),
                    Some(&serde_json::Value::Bool(b)) => params.push(DbParam::Bool(b)),
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
            } else if let Some(user_m) = cap.get(3) {
                let param = resolve_user_placeholder_param(user_m.as_str(), claims)?;
                params.push(param);
            }
        }
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

/// Build a URL string from an "API:" formula string by interpolating `{request.*}` variables.
pub fn build_url_from_formula(
    url_formula: &str,
    body: &serde_json::Value,
) -> Result<String> {
    build_url_from_formula_with_claims(url_formula, body, None)
}

/// Build a URL string from an "API:" formula string by interpolating `{request.*}` and `{user.*}` variables.
pub fn build_url_from_formula_with_claims(
    url_formula: &str,
    body: &serde_json::Value,
    claims: Option<&crate::auth::Claims>,
) -> Result<String> {
    // Helper to get nested value by dotted path
    fn get_by_path_str(root: &serde_json::Value, path: &str) -> String {
        let mut cur = root;
        for part in path.split('.') {
            if let Ok(idx) = part.parse::<usize>() {
                match cur {
                    serde_json::Value::Array(arr) => {
                        if let Some(val) = arr.get(idx) {
                            cur = val;
                        } else {
                            return "".to_string();
                        }
                    }
                    _ => return "".to_string(),
                }
            } else {
                match cur {
                    serde_json::Value::Object(map) => {
                        if let Some(val) = map.get(part) {
                            cur = val;
                        } else {
                            return "".to_string();
                        }
                    }
                    _ => return "".to_string(),
                }
            }
        }
        match cur {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "".to_string(),
            _ => cur.to_string(),
        }
    }

    if !RE_URL_TOKEN.is_match(url_formula) {
        return Ok(url_formula.to_string());
    }

    let mut new_url = String::with_capacity(url_formula.len());
    let mut last = 0usize;
    for cap in RE_URL_TOKEN.captures_iter(url_formula) {
        let full = cap.get(0).unwrap();
        new_url.push_str(&url_formula[last..full.start()]);

        let val = if let Some(req_m) = cap.get(1) {
            get_by_path_str(body, req_m.as_str())
        } else if let Some(user_m) = cap.get(2) {
            resolve_user_placeholder_str(user_m.as_str(), claims)?.unwrap_or_default()
        } else {
            String::new()
        };
        new_url.push_str(&urlencoding::encode(&val));
        last = full.end();
    }
    new_url.push_str(&url_formula[last..]);

    Ok(new_url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Claims;
    use serde_json::json;

    fn sample_claims() -> Claims {
        Claims {
            id: "42".to_string(),
            nm: "Siti Aminah".to_string(),
            exp: 9999999999,
            at: 0,
            rl: "plant_qa_manager/127,capa_reviewer".to_string(),
            cs: "00198765".to_string(),
            jti: None,
            typ: None,
        }
    }

    #[test]
    fn sql_formula_binds_user_placeholders_in_exact_positional_order() {
        let claims = sample_claims();
        let body = json!({ "id": 105 });
        let formula = "SQL:SELECT (pic_nip = {user.custom}) AND (prepared_by <> {user.custom}) AS is_valid FROM plant_capas WHERE id = {request.id}";

        let (sql, params) =
            build_sql_and_params_from_formula_with_claims(formula, &body, Some(&claims))
                .expect("formula should compile");

        assert_eq!(
            sql,
            "SELECT (pic_nip = ?) AND (prepared_by <> ?) AS is_valid FROM plant_capas WHERE id = ?"
        );
        assert_eq!(params.len(), 3);
        match &params[0] {
            DbParam::Str(s) => assert_eq!(s, "00198765"),
            other => panic!("expected Str for user.custom (preserve leading zeros), got {:?}", other),
        }
        match &params[1] {
            DbParam::Str(s) => assert_eq!(s, "00198765"),
            other => panic!("expected Str for user.custom, got {:?}", other),
        }
        match &params[2] {
            DbParam::I64(n) => assert_eq!(*n, 105),
            other => panic!("expected I64 for request.id, got {:?}", other),
        }
    }

    #[test]
    fn sql_formula_binds_all_user_fields_and_has_role() {
        let claims = sample_claims();
        let body = json!({ "dept_id": 9 });
        let formula = "SQL:INSERT INTO audit(uid, uname, nip, roles, is_qa, is_admin, dept_head) VALUES ({user.id}, {user.name}, {user.custom}, {user.roles}, {user.has_role:plant_qa_manager}, {user.has_role:superadmin}, {departments[{request.dept_id}].head_nip})";

        let (sql, params) =
            build_sql_and_params_from_formula_with_claims(formula, &body, Some(&claims))
                .expect("formula should compile");

        assert_eq!(
            sql,
            "INSERT INTO audit(uid, uname, nip, roles, is_qa, is_admin, dept_head) VALUES (?, ?, ?, ?, ?, ?, (SELECT head_nip FROM departments WHERE id = ?))"
        );
        assert_eq!(params.len(), 7);
        assert!(matches!(&params[0], DbParam::I64(42)));
        assert!(matches!(&params[1], DbParam::Str(s) if s == "Siti Aminah"));
        assert!(matches!(&params[2], DbParam::Str(s) if s == "00198765"));
        assert!(matches!(&params[3], DbParam::Str(s) if s == "plant_qa_manager,capa_reviewer"));
        assert!(matches!(&params[4], DbParam::I64(1)));
        assert!(matches!(&params[5], DbParam::I64(0)));
        assert!(matches!(&params[6], DbParam::I64(9)));
    }

    #[test]
    fn sql_formula_user_placeholders_fail_closed_when_unauthenticated() {
        let body = json!({ "id": 1 });
        let formula = "SQL:SELECT {user.id}, {user.name}, {user.custom}, {user.roles}, {user.has_role:plant_qa_manager} WHERE id = {request.id}";

        let (sql, params) =
            build_sql_and_params_from_formula_with_claims(formula, &body, None)
                .expect("formula should compile");

        assert_eq!(sql, "SELECT ?, ?, ?, ?, ? WHERE id = ?");
        assert!(matches!(&params[0], DbParam::Null));
        assert!(matches!(&params[1], DbParam::Null));
        assert!(matches!(&params[2], DbParam::Null));
        assert!(matches!(&params[3], DbParam::Str(s) if s.is_empty()));
        assert!(matches!(&params[4], DbParam::I64(0)));
        assert!(matches!(&params[5], DbParam::I64(1)));
    }

    #[test]
    fn sql_formula_prevents_sql_injection_via_claims() {
        let mut claims = sample_claims();
        claims.cs = "' OR '1'='1".to_string();
        let body = json!({ "id": 5 });
        let formula = "SQL:SELECT (pic_nip = {user.custom}) AS is_valid FROM plant_capas WHERE id = {request.id}";

        let (sql, params) =
            build_sql_and_params_from_formula_with_claims(formula, &body, Some(&claims))
                .expect("formula should compile");

        assert_eq!(sql, "SELECT (pic_nip = ?) AS is_valid FROM plant_capas WHERE id = ?");
        assert!(!sql.contains("OR"));
        assert!(matches!(&params[0], DbParam::Str(s) if s == "' OR '1'='1"));
    }

    #[test]
    fn sql_formula_rejects_unknown_or_empty_user_placeholder() {
        let claims = sample_claims();
        let body = json!({});
        assert!(build_sql_and_params_from_formula_with_claims(
            "SQL:SELECT {user.unknown}",
            &body,
            Some(&claims),
        )
        .is_err());
        assert!(build_sql_and_params_from_formula_with_claims(
            "SQL:SELECT {user.has_role:}",
            &body,
            Some(&claims),
        )
        .is_err());
    }

    #[test]
    fn sql_formula_supports_user_inside_nested_subselect() {
        let claims = sample_claims();
        let body = json!({});
        let formula = "SQL:SELECT {employees[{user.id}].department_id} AS dept";
        let (sql, params) =
            build_sql_and_params_from_formula_with_claims(formula, &body, Some(&claims))
                .expect("nested user subselect should compile");
        assert_eq!(sql, "SELECT (SELECT department_id FROM employees WHERE id = ?) AS dept");
        assert!(matches!(&params[0], DbParam::I64(42)));
    }

    #[test]
    fn url_formula_interpolates_request_and_user_placeholders() {
        let claims = sample_claims();
        let body = json!({ "id": 77 });
        let url = build_url_from_formula_with_claims(
            "http://internal/check?id={request.id}&nip={user.custom}&qa={user.has_role:plant_qa_manager}&name={user.name}",
            &body,
            Some(&claims),
        )
        .expect("url should build");
        assert_eq!(
            url,
            "http://internal/check?id=77&nip=00198765&qa=1&name=Siti%20Aminah"
        );
    }
}
