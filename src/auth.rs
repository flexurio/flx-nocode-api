use std::env;
use std::sync::atomic::{AtomicBool, Ordering};

use actix_web::{web, HttpResponse};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde_json::Value;

use crate::{database::state::DbParam, helpers::get_client_ip, AppState};

// The app's own JWT signing secret (SECRET_KEY) is read once from the environment
// at process start (main.rs) and never changes for the process lifetime, so the
// derived encoding/decoding keys and validation config are cached here instead of
// being rebuilt on every login / every authenticated request.
static JWT_SECRET_BYTES: Lazy<Vec<u8>> =
    Lazy::new(|| env::var("SECRET_KEY").unwrap_or_default().into_bytes());
static JWT_ENCODING_KEY: Lazy<EncodingKey> =
    Lazy::new(|| EncodingKey::from_secret(&JWT_SECRET_BYTES));
static JWT_DECODING_KEY: Lazy<DecodingKey> =
    Lazy::new(|| DecodingKey::from_secret(&JWT_SECRET_BYTES));
static JWT_VALIDATION: Lazy<Validation> = Lazy::new(|| {
    let mut v = Validation::new(Algorithm::HS256);
    v.validate_exp = true;
    v.leeway = 30; // Allow 30 seconds clock skew
    v
});

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub id: String,
    pub nm: String,
    pub exp: usize,
    pub at: usize,
    pub rl: String,
    pub cs: String,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ClaimsConverter {
    pub id: String,
    pub nm: String,
    pub exp: String,
    pub at: String,
    pub rl: String,
    pub cs: String,
}

impl Claims {
    pub fn get_roles(&self) -> Vec<String> {
        self.rl.split(",").map(|s| s.to_string()).collect()
    }
}

// set default Claims
impl Default for Claims {
    fn default() -> Self {
        Claims {
            id: "".to_string(),
            nm: "route_publics".to_string(),
            exp: 0,
            at: 0,
            rl: "*/127".to_string(),
            cs: "".to_string(),
        }
    }
}


// set default ClaimsConverter
impl Default for ClaimsConverter {
    fn default() -> Self {
        ClaimsConverter {
            id:"id".to_string(),
            nm:"nm".to_string(),
            exp:"exp".to_string(),
            at:"at".to_string(),
            rl:"rl".to_string(),
            cs:"cs".to_string(),
        }
    }
}

// Middleware untuk verifikasi token dengan optimized validation
pub fn validate_token(
    req: &actix_web::HttpRequest,
    state: &web::Data<AppState>,
) -> Result<Claims, Box<HttpResponse>> {
    // Fast path: check IP whitelist and public routes first
    if is_ip_whitelisted(req, &state.whitelist_ips) ||
        state.route_publics.contains(req.path()) {
        return Ok(Claims::default());
    }

    if !state.require_auth {
        return Ok(Claims::default());
    }

    // Extract Authorization header once
    let auth_header = match req.headers().get("Authorization") {
        Some(header) => match header.to_str() {
            Ok(auth_str) if auth_str.starts_with("Bearer ") => auth_str,
            _ => {
                return Err(Box::new(HttpResponse::Unauthorized().json("Invalid Authorization header format")))
            }
        },
        None => return Err(Box::new(HttpResponse::Unauthorized().json("Missing Authorization header"))),
    };

    // When converter_token is set, the token is issued by an external/third-party
    // identity provider. We cryptographically verify it using an operator-provided
    // key (CONVERTER_JWT_SECRET or CONVERTER_JWT_PUBLIC_KEY). Verification is
    // mandatory: if no key is configured the request is rejected (fail closed),
    // unless the operator explicitly opts out via CONVERTER_JWT_INSECURE_SKIP_VERIFY.
    if state.converter_token != ClaimsConverter::default() {
        validate_converter_token(auth_header, &state.converter_token.exp)?;
        return extract_token_claims_no_validation(auth_header, (*state).clone())
            .ok_or_else(|| Box::new(HttpResponse::Unauthorized().json("Failed to extract converter token claims")));
    }

    match decode::<Claims>(
        auth_header.trim_start_matches("Bearer "),
        &JWT_DECODING_KEY,
        &JWT_VALIDATION,
    ) {
        Ok(token_data) => Ok(token_data.claims),
        Err(e) => {
            eprintln!("Token validation failed: {}", e);
            Err(Box::new(HttpResponse::Unauthorized().json("Invalid or expired token")))
        }
    }
}

// ---------------- Converter (external IDP) JWT verification ----------------

/// Verification configuration for externally-issued (converter_token) JWTs,
/// loaded once from environment variables.
struct ConverterVerifyConfig {
    /// Key used to verify the signature; `None` means no key was configured.
    decoding_key: Option<DecodingKey>,
    /// Allowed signing algorithms (non-empty when `decoding_key` is set).
    algorithms: Vec<Algorithm>,
    /// Expected `iss` values, if enforcement is desired.
    issuer: Option<Vec<String>>,
    /// Expected `aud` values, if enforcement is desired.
    audience: Option<Vec<String>>,
    /// Operator explicitly accepts unverified tokens (signature delegated to an
    /// upstream gateway). Only honored when no `decoding_key` is configured.
    insecure_skip_verify: bool,
}

static CONVERTER_VERIFY: Lazy<ConverterVerifyConfig> = Lazy::new(load_converter_verify_config);
static CONVERTER_INSECURE_WARNED: AtomicBool = AtomicBool::new(false);
static CONVERTER_MISCONFIG_WARNED: AtomicBool = AtomicBool::new(false);

fn parse_jwt_alg(s: &str) -> Option<Algorithm> {
    match s.trim().to_ascii_uppercase().as_str() {
        "HS256" => Some(Algorithm::HS256),
        "HS384" => Some(Algorithm::HS384),
        "HS512" => Some(Algorithm::HS512),
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "ES256" => Some(Algorithm::ES256),
        "ES384" => Some(Algorithm::ES384),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        "EDDSA" => Some(Algorithm::EdDSA),
        _ => None,
    }
}

fn split_csv_env(name: &str) -> Option<Vec<String>> {
    let raw = env::var(name).ok()?;
    let items: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if items.is_empty() { None } else { Some(items) }
}

fn load_converter_verify_config() -> ConverterVerifyConfig {
    let issuer = split_csv_env("CONVERTER_JWT_ISSUER");
    let audience = split_csv_env("CONVERTER_JWT_AUDIENCE");
    let insecure_skip_verify = env::var("CONVERTER_JWT_INSECURE_SKIP_VERIFY")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let explicit_alg = env::var("CONVERTER_JWT_ALG").ok().and_then(|s| parse_jwt_alg(&s));

    // Symmetric secret (HS*) takes precedence if provided.
    if let Ok(secret) = env::var("CONVERTER_JWT_SECRET")
        && !secret.is_empty() {
            let alg = explicit_alg.unwrap_or(Algorithm::HS256);
            return ConverterVerifyConfig {
                decoding_key: Some(DecodingKey::from_secret(secret.as_bytes())),
                algorithms: vec![alg],
                issuer,
                audience,
                insecure_skip_verify,
            };
        }

    // Asymmetric public key (PEM). Allow `\n`-escaped single-line env values.
    if let Ok(pem_raw) = env::var("CONVERTER_JWT_PUBLIC_KEY")
        && !pem_raw.trim().is_empty() {
            let pem = pem_raw.replace("\\n", "\n");
            let alg = explicit_alg.unwrap_or(Algorithm::RS256);
            let key_res = match alg {
                Algorithm::ES256 | Algorithm::ES384 => DecodingKey::from_ec_pem(pem.as_bytes()),
                Algorithm::EdDSA => DecodingKey::from_ed_pem(pem.as_bytes()),
                _ => DecodingKey::from_rsa_pem(pem.as_bytes()),
            };
            match key_res {
                Ok(key) => {
                    return ConverterVerifyConfig {
                        decoding_key: Some(key),
                        algorithms: vec![alg],
                        issuer,
                        audience,
                        insecure_skip_verify,
                    };
                }
                Err(e) => {
                    // Configured but invalid: fail closed (no usable key).
                    eprintln!("ERROR: CONVERTER_JWT_PUBLIC_KEY could not be parsed as {:?} PEM: {}", alg, e);
                }
            }
        }

    ConverterVerifyConfig {
        decoding_key: None,
        algorithms: vec![],
        issuer,
        audience,
        insecure_skip_verify,
    }
}

/// Verify an externally-issued JWT (converter_token mode).
/// `auth_header` is the raw `Authorization` value (may include the `Bearer ` prefix).
fn validate_converter_token(auth_header: &str, exp_field: &str) -> Result<(), Box<HttpResponse>> {
    let token = auth_header.trim_start_matches("Bearer ").trim();
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Box::new(HttpResponse::Unauthorized().json("Invalid token structure")));
    }

    let cfg = &*CONVERTER_VERIFY;

    // Preferred path: cryptographically verify the signature.
    if let Some(key) = &cfg.decoding_key {
        let mut validation = Validation::new(cfg.algorithms[0]);
        validation.algorithms = cfg.algorithms.clone();
        validation.validate_exp = true;
        validation.leeway = 30;
        if let Some(iss) = &cfg.issuer {
            validation.set_issuer(iss.as_slice());
        }
        match &cfg.audience {
            Some(aud) => validation.set_audience(aud.as_slice()),
            None => validation.validate_aud = false,
        }
        return match decode::<Value>(token, key, &validation) {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Converter JWT verification failed: {}", e);
                Err(Box::new(HttpResponse::Unauthorized().json("Invalid or expired token")))
            }
        };
    }

    // Explicit opt-out: operator delegates signature checks to an upstream gateway.
    if cfg.insecure_skip_verify {
        if !CONVERTER_INSECURE_WARNED.swap(true, Ordering::Relaxed) {
            eprintln!(
                "WARNING: converter_token signature verification is DISABLED \
                 (CONVERTER_JWT_INSECURE_SKIP_VERIFY=true). Tokens are accepted without \
                 cryptographic verification — ensure an upstream gateway validates them."
            );
        }
        // Best-effort expiry check from the unverified payload.
        if let Ok(decoded) = URL_SAFE_NO_PAD.decode(parts[1])
            && let Ok(json) = serde_json::from_slice::<Value>(&decoded)
            && let Some(exp) = json.get(exp_field).and_then(|v| v.as_u64())
            && exp > 0
        {
            let now = Utc::now().timestamp() as u64;
            if now > exp + 30 {
                return Err(Box::new(HttpResponse::Unauthorized().json("Token expired")));
            }
        }
        return Ok(());
    }

    // Fail closed: converter mode active but no verification key and no explicit opt-out.
    if !CONVERTER_MISCONFIG_WARNED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "ERROR: converter_token mode is active but no verification key is configured. \
             Set CONVERTER_JWT_SECRET (HS*) or CONVERTER_JWT_PUBLIC_KEY (RS*/ES*/EdDSA PEM), \
             or set CONVERTER_JWT_INSECURE_SKIP_VERIFY=true to accept unverified tokens. \
             Rejecting all converter_token requests until configured."
        );
    }
    Err(Box::new(HttpResponse::Unauthorized().json("Token verification not configured")))
}

// Handler untuk login dan generate token
pub async fn create_token(
    id_user: String,
    name: String,
    state: web::Data<AppState>,
    roles: String,
) -> String {
    let expiration = Utc::now()
        .checked_add_signed(Duration::days(1))
        .expect("valid timestamp")
        .timestamp() as usize;

    // Prefer the correctly-spelled env var, fall back to the legacy typo for
    // backward compatibility with existing deployments.
    let query = env::var("CUSTOM_JWT_QUERY").or_else(|_| env::var("CUSTOME_JWT_QUERY"));
    let mut addjwt = String::new();
    if let Ok(mut sql_query) = query {
        sql_query = sql_query.to_lowercase();

        if !sql_query.is_empty() {
            // Replace the placeholder with `?` so query_with_params can bind it safely.
            // This prevents SQL injection: the value is passed as a bound parameter,
            // never interpolated into the query string.
            sql_query = sql_query.replace("{:?}", "?");

            // Optimize: Add timeout to prevent hanging on slow queries
            let query_future = state.db.query_with_params(&sql_query, vec![DbParam::Str(id_user.clone())]);
            addjwt = match tokio::time::timeout(std::time::Duration::from_millis(500), query_future).await {
                Ok(Ok(results)) => results
                    .first()
                    .and_then(|value| value.as_str().map(|s| s.to_string()))
                    .unwrap_or_default(),
                Ok(Err(e)) => {
                    eprintln!("Error executing custom JWT query: {}", e);
                    String::new()
                }
                Err(_) => {
                    eprintln!("Custom JWT query timeout after 500ms");
                    String::new()
                }
            };
        }
    }

    let claims = Claims {
        id: id_user,
        nm: name,
        exp: expiration,
        at: Utc::now().timestamp() as usize,
        rl: roles,
        cs: addjwt,
    };

    match encode(&Header::default(), &claims, &JWT_ENCODING_KEY) {
        Ok(token) => token,
        Err(e) => {
            eprintln!("Failed to create JWT token: {}", e);
            String::new()
        }
    }
}

// Function untuk mengekstrak claims dari token
fn extract_token_claims(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token = token.trim_start_matches("Bearer ");

    // Decode token dan ekstrak claims
    let token_data = decode::<Claims>(token, &JWT_DECODING_KEY, &JWT_VALIDATION)?;

    Ok(token_data.claims)
}

// function JWWT Decoder tanpa validasi key
fn extract_token_claims_no_validation(token: &str, state: web::Data<AppState>) -> Option<Claims> {
    let token = token.trim_start_matches("Bearer ");

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload_b64 = parts[1];

    let decoded = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let json: Value = serde_json::from_slice(&decoded).ok()?;

    // read converter_token from state
    let converter = &state.converter_token;
    Some(Claims {
        id: json.get(&converter.id).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        nm: json.get(&converter.nm).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        exp: json.get(&converter.exp).and_then(|v| v.as_u64()).unwrap_or(0) as usize,
        at: json.get(&converter.at).and_then(|v| v.as_u64()).unwrap_or(0) as usize,
        rl: json.get(&converter.rl).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        cs: json.get(&converter.cs).and_then(|v| v.as_str()).unwrap_or("converter_token").to_string(),
    })
}

fn is_ip_whitelisted(req: &actix_web::HttpRequest, whitelist: &[String]) -> bool {
    let ip = get_client_ip(req);
    whitelist.contains(&ip)
}

// Contoh penggunaan
pub fn get_user_info_from_token(
    req: &actix_web::HttpRequest,
    state: web::Data<AppState>,
) -> Result<Claims, bool> {
    // 1. Fast path: check if claims were already validated & inserted into request extensions by AuthMiddleware
    use actix_web::HttpMessage;
    if let Some(claims) = req.extensions().get::<Claims>() {
        return Ok(claims.clone());
    }

    if is_ip_whitelisted(req, &state.whitelist_ips) {
        println!("IP is whitelisted, returning default claims.");
        // Anda bisa sesuaikan isi Claims berikut sesuai kebutuhan
        return Ok(Claims::default());
    }

    let auth_str = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .filter(|s| s.starts_with("Bearer "));

    if let Some(auth) = auth_str {
        if state.converter_token != ClaimsConverter::default() {
            if let Some(claims) = extract_token_claims_no_validation(auth, state.clone()) {
                return Ok(claims);
            } else {
                return Err(false);
            }
        }
        return match extract_token_claims(auth) {
            Ok(claims) => Ok(claims),
            Err(_) => Err(false),
        };
    }

    Err(false)
}

#[derive(Deserialize, Debug, Clone)]
pub struct Rule {
    #[serde(rename = "match")]
    pub endpoint: String,
    pub allows: Vec<AllowDef>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AllowDef {
    pub method: String,
        #[allow(dead_code)] // parsed for config validation; not consulted at runtime yet
        pub permission_id: String,
    #[serde(rename = "if")]
    pub condition: Option<Conditions>,
    #[serde(default)]
    #[allow(dead_code)] // read by `allowed_fields_in`; flagged until services are wired
    pub allowed_fields: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Conditions {
    Or { or: Vec<Conditions> },
    And { and: Vec<Conditions> },
    Eq { eq: (String, String) },
}

#[derive(Deserialize)]
struct RulesFile {
    #[serde(default)]
    rule: Vec<Rule>,
}

fn load_rules() -> &'static [Rule] {
    use once_cell::sync::Lazy;
    // Cache rules at first access; the file is read once for the process lifetime.
    // Supports three on-disk shapes (most-current first):
    //   1) `{ "role": [...], "rule": [Rule, ...] }`  (current schema)
    //   2) `[Rule, ...]`                              (legacy flat list)
    //   3) `Rule`                                     (legacy single rule)
    static RULES: Lazy<Vec<Rule>> = Lazy::new(|| {
        // Reuse the single CONFIG_LOCATION source of truth (also backed by LOC_CONFIG)
        // instead of re-reading the env var here, so both never risk diverging.
        let path = format!("{}/rules.json", crate::config::CONFIG_LOCATION.as_str());
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        if let Ok(file) = serde_json::from_str::<RulesFile>(&content) {
            return file.rule;
        }
        if let Ok(rules) = serde_json::from_str::<Vec<Rule>>(&content) {
            return rules;
        }
        if let Ok(rule) = serde_json::from_str::<Rule>(&content) {
            return vec![rule];
        }
        Vec::new()
    });
    &RULES
}

fn evaluate_condition(condition: &Conditions, claims: &Claims) -> bool {
    match condition {
        Conditions::Or { or } => or.iter().any(|c| evaluate_condition(c, claims)),
        Conditions::And { and } => and.iter().all(|c| evaluate_condition(c, claims)),
        Conditions::Eq { eq } => {
            let (field, value) = eq;
            if field == "$user.role" {
                 // Check if any of the user's roles match the value
                 // claims.get_roles() returns Vec<String> like ["admin", "user/1"]
                 // We need to match precise role or basic role
                 return claims.get_roles().iter().any(|r| {
                     let parts: Vec<&str> = r.split('/').collect();
                     parts[0] == value
                 });
            }
            // Add other field checks if needed, e.g., $user.id
            if field == "$user.id" {
                return &claims.id == value;
            }
            false
        }
    }
}

/// Normalise a route pattern or request path into its non-empty segments.
/// Trailing/duplicate slashes are ignored, so `/banks/1/` == `/banks/1`.
fn path_segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

/// `true` when `pattern` (e.g. `/banks/{id}`) matches `path` (e.g. `/banks/42`).
/// A `{param}` segment matches any single segment; other segments must be equal.
fn route_matches(pattern: &str, path: &str) -> bool {
    let route_parts: Vec<&str> = path_segments(pattern).collect();
    let path_parts: Vec<&str> = path_segments(path).collect();
    if route_parts.len() != path_parts.len() {
        return false;
    }
    route_parts
        .iter()
        .zip(path_parts.iter())
        .all(|(r, p)| (r.starts_with('{') && r.ends_with('}')) || r == p)
}

/// Shared rule-matching core used by both `check_access` and the
/// `allowed_fields` enforcement helpers. Finds the rule whose `match` pattern
/// fits `current_path`, then the `allows[]` entry for `current_method`
/// (case-insensitive), then evaluates its `if` condition against `claims`.
/// Returns the matched allow entry on success.
fn match_allow<'a>(
    rules: &'a [Rule],
    claims: &Claims,
    current_path: &str,
    current_method: &str,
) -> Result<&'a AllowDef, String> {
    let rule = rules
        .iter()
        .find(|r| route_matches(&r.endpoint, current_path))
        .ok_or_else(|| format!("Rule not defined for endpoint: {}", current_path))?;

    let allow = rule
        .allows
        .iter()
        .find(|a| a.method.trim().eq_ignore_ascii_case(current_method.trim()))
        .ok_or_else(|| format!("Method {} not allowed for this rule", current_method))?;

    if let Some(cond) = &allow.condition
        && !evaluate_condition(cond, claims)
    {
        return Err("Access denied by rule condition".to_string());
    }
    Ok(allow)
}

pub fn evaluate_access(rules: &[Rule], claims: &Claims, current_path: &str, current_method: &str) -> Result<(), String> {
    match_allow(rules, claims, current_path, current_method).map(|_| ())
}

pub fn check_access(claims: &Claims, req: &actix_web::HttpRequest) -> Result<(), String> {
    if claims.cs == "converter_token" {
        return Ok(());
    }

    let rules = load_rules();
    let current_path = req.path();
    let current_method = req.method().as_str();

    evaluate_access(rules, claims, current_path, current_method)
}

// ---------------------------------------------------------------------------
// allowed_fields enforcement
// ---------------------------------------------------------------------------

// NOTE: the `#[allow(dead_code)]` markers below can be dropped once the
// services call `enforce_allowed_fields` / `retain_fields_on`.

/// Keys that are never subject to `allowed_fields` filtering (primary key).
const ALWAYS_ALLOWED_FIELDS: &[&str] = &["id"];

fn is_always_allowed(key: &str) -> bool {
    ALWAYS_ALLOWED_FIELDS.iter().any(|k| k.eq_ignore_ascii_case(key))
}

/// Pure variant of [`allowed_fields_for`] over an explicit rule set, so tests can
/// inject rules without touching the on-disk `rules.json`.
/// `None` = no restriction (no matching rule/allow, or `allowed_fields` empty).
pub fn allowed_fields_in(
    rules: &[Rule],
    claims: &Claims,
    current_path: &str,
    current_method: &str,
) -> Option<Vec<String>> {
    match match_allow(rules, claims, current_path, current_method) {
        Ok(allow) if !allow.allowed_fields.is_empty() => Some(allow.allowed_fields.clone()),
        _ => None,
    }
}

/// Return the `allowed_fields` of the `rules.json` allow entry that matched this
/// request (same matching as [`check_access`]: path pattern with `{param}`
/// segments, trailing slashes ignored, method case-insensitive, `if` condition).
///
/// `None` means **no restriction applies**: converter tokens, no matching rule,
/// access denied (callers must still run `check_access` first), or an empty
/// `allowed_fields` list. `Some(fields)` means only those fields (plus `id`)
/// may be written or returned.
pub fn allowed_fields_for(claims: &Claims, req: &actix_web::HttpRequest) -> Option<Vec<String>> {
    if claims.cs == "converter_token" {
        return None;
    }
    allowed_fields_in(load_rules(), claims, req.path(), req.method().as_str())
}

/// Strict write-side check over an already-resolved restriction.
/// `body` may be an object or an array of objects (bulk insert).
fn enforce_fields_on(allowed: &[String], body: &Value) -> Result<(), String> {
    let check_obj = |map: &serde_json::Map<String, Value>| -> Result<(), String> {
        for key in map.keys() {
            if is_always_allowed(key) || allowed.iter().any(|f| f == key) {
                continue;
            }
            return Err(format!("field '{}' is not permitted for this role", key));
        }
        Ok(())
    };
    match body {
        Value::Object(map) => check_obj(map),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .try_for_each(check_obj),
        _ => Ok(()),
    }
}

/// Enforce `allowed_fields` on a POST/PUT/PATCH request body (403 semantics).
///
/// If a restriction applies to this request (see [`allowed_fields_for`]), any
/// top-level key of the body object (or of each object in a body array) that is
/// neither in `allowed_fields` nor the primary key `id` yields
/// `Err("field '<k>' is not permitted for this role")`. The body is not
/// modified. Returns `Ok(())` for other methods or when no restriction applies.
/// Call it right after `check_access` and before the service adds audit fields
/// such as `created_by_id`.
pub fn enforce_allowed_fields(
    claims: &Claims,
    req: &actix_web::HttpRequest,
    body: &mut Value,
) -> Result<(), String> {
    let method = req.method().as_str();
    if !(method.eq_ignore_ascii_case("POST")
        || method.eq_ignore_ascii_case("PUT")
        || method.eq_ignore_ascii_case("PATCH"))
    {
        return Ok(());
    }
    match allowed_fields_for(claims, req) {
        Some(allowed) => enforce_fields_on(&allowed, body),
        None => Ok(()),
    }
}

/// Read-side projection over an already-resolved restriction.
pub fn retain_fields_on(allowed: &[String], data: &mut Value) {
    let keep = |key: &str| is_always_allowed(key) || allowed.iter().any(|f| f == key);
    match data {
        Value::Object(map) => map.retain(|k, _| keep(k)),
        Value::Array(items) => {
            for item in items.iter_mut() {
                if let Value::Object(map) = item {
                    map.retain(|k, _| keep(k));
                }
            }
        }
        _ => {}
    }
}

/// Project a GET response down to the `allowed_fields` of the matched rule.
///
/// If a restriction applies (see [`allowed_fields_for`]), every object in
/// `data` (a single object or an array of objects) keeps only the allowed
/// fields plus `id`; everything else is removed in place. No-op when no
/// restriction applies. Because the result is role-specific, a filtered
/// response must not be written to or served from the shared response cache.


#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_rules() -> Vec<Rule> {
        vec![
            Rule {
                endpoint: "/api/test".to_string(),
                allows: vec![
                    AllowDef {
                        method: "GET".to_string(),
                        permission_id: "perm1".to_string(),
                        condition: None,
                        allowed_fields: vec![],
                    },
                    AllowDef {
                        method: "POST".to_string(),
                         permission_id: "perm2".to_string(),
                        condition: Some(Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) }),
                        allowed_fields: vec![],
                    }
                ],
            },
            Rule {
                endpoint: "/api/items/{id}".to_string(),
                allows: vec![
                    AllowDef {
                        method: "DELETE".to_string(),
                         permission_id: "perm3".to_string(),
                        condition: Some(Conditions::Or { or: vec![
                            Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) },
                            Conditions::Eq { eq: ("$user.id".to_string(), "123".to_string()) }
                        ]}),
                        allowed_fields: vec![],
                    }
                ]
            }
        ]
    }

    fn create_claims(role: &str, id: &str) -> Claims {
        let mut claims = Claims {
            id: id.to_string(),
            ..Claims::default()
        };
        // Assuming format "role/permission"
        // If role doesn't contain '/', append dummy permission
        if role.contains('/') {
            claims.rl = role.to_string();
        } else {
            claims.rl = format!("{}/1", role);
        }
        claims
    }

    #[test]
    fn test_access_allowed_no_condition() {
        let rules = create_test_rules();
        let claims = create_claims("user", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/test", "GET");
        assert!(result.is_ok());
    }

    #[test]
    fn test_access_denied_condition_fail() {
        let rules = create_test_rules();
        let claims = create_claims("user", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/test", "POST");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Access denied by rule condition");
    }

    #[test]
    fn test_access_allowed_condition_pass() {
        let rules = create_test_rules();
        let claims = create_claims("admin", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/test", "POST");
        assert!(result.is_ok());
    }

    #[test]
    fn test_access_undefined_endpoint() {
        let rules = create_test_rules();
        let claims = create_claims("admin", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/undefined", "GET");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Rule not defined"));
    }

    #[test]
    fn test_access_method_not_allowed() {
        let rules = create_test_rules();
        let claims = create_claims("admin", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/test", "DELETE");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Method DELETE not allowed"));
    }

    #[test]
    fn test_access_parameter_matching() {
        let rules = create_test_rules();
        let claims = create_claims("admin", "1");
        
        let result = evaluate_access(&rules, &claims, "/api/items/999", "DELETE");
        assert!(result.is_ok(), "Admin should be able to delete item 999");
    }

    #[test]
    fn test_access_complex_condition() {
        let rules = create_test_rules();
        let claims = create_claims("user", "123");
        
        let result = evaluate_access(&rules, &claims, "/api/items/888", "DELETE");
        assert!(result.is_ok(), "User 123 should be able to delete via OR condition");

        let claims_fail = create_claims("user", "999");
        let result_fail = evaluate_access(&rules, &claims_fail, "/api/items/888", "DELETE");
        assert!(result_fail.is_err());
    }

    // --- allowed_fields ---

    fn field_rules() -> Vec<Rule> {
        let json = serde_json::json!({
            "role": ["admin"],
            "rule": [
                {
                    "match": "/banks/{id}",
                    "allows": [
                        {
                            "method": "put",
                            "permission_id": "banks_edit",
                            "allowed_fields": ["name", "bank_type_id", "foto"],
                            "if": { "or": [
                                { "eq": ["$user.role", "admin"] },
                                { "eq": ["$user.role", "manager"] }
                            ] }
                        },
                        {
                            "method": "GET",
                            "permission_id": "banks_get_one",
                            "allowed_fields": []
                        }
                    ]
                },
                {
                    "match": "/banks",
                    "allows": [
                        {
                            "method": "GET",
                            "permission_id": "banks_get",
                            "allowed_fields": ["name", "foto"]
                        },
                        {
                            "method": "POST",
                            "permission_id": "banks_add",
                            "allowed_fields": ["name"]
                        }
                    ]
                }
            ]
        });
        serde_json::from_value::<RulesFile>(json).unwrap().rule
    }

    #[test]
    fn test_allowed_fields_in_matched_rule() {
        let rules = field_rules();
        let claims = create_claims("admin", "1");
        assert_eq!(
            allowed_fields_in(&rules, &claims, "/banks/7", "PUT"),
            Some(vec!["name".to_string(), "bank_type_id".to_string(), "foto".to_string()])
        );
    }

    #[test]
    fn test_allowed_fields_in_method_case_insensitive_and_trailing_slash() {
        let rules = field_rules();
        let claims = create_claims("manager", "1");
        // rule method is lowercase "put"; request method uppercase; trailing slash on path
        assert!(allowed_fields_in(&rules, &claims, "/banks/7/", "PUT").is_some());
        assert!(allowed_fields_in(&rules, &claims, "/banks//7", "Put").is_some());
        assert!(allowed_fields_in(&rules, &claims, "/banks/", "get").is_some());
    }

    #[test]
    fn test_allowed_fields_in_param_matches_single_segment_only() {
        let rules = field_rules();
        let claims = create_claims("admin", "1");
        // `{id}` must not swallow two segments
        assert_eq!(allowed_fields_in(&rules, &claims, "/banks/7/extra", "PUT"), None);
        assert!(evaluate_access(&rules, &claims, "/banks/7/extra", "PUT").is_err());
    }

    #[test]
    fn test_allowed_fields_in_none_when_empty_list_or_no_rule_or_denied() {
        let rules = field_rules();
        let claims = create_claims("admin", "1");
        // empty allowed_fields → no restriction
        assert_eq!(allowed_fields_in(&rules, &claims, "/banks/7", "GET"), None);
        // no rule for endpoint → no restriction
        assert_eq!(allowed_fields_in(&rules, &claims, "/accounts", "GET"), None);
        // method not in allows → no restriction
        assert_eq!(allowed_fields_in(&rules, &claims, "/banks/7", "DELETE"), None);
        // condition fails → None (check_access is the one that denies)
        let user = create_claims("user", "1");
        assert_eq!(allowed_fields_in(&rules, &user, "/banks/7", "PUT"), None);
        assert!(evaluate_access(&rules, &user, "/banks/7", "PUT").is_err());
    }

    #[test]
    fn test_enforce_fields_rejects_unlisted_key() {
        let allowed = vec!["name".to_string(), "foto".to_string()];
        let body = serde_json::json!({ "id": 3, "name": "BCA", "balance": 100 });
        let err = enforce_fields_on(&allowed, &body).unwrap_err();
        assert_eq!(err, "field 'balance' is not permitted for this role");
    }

    #[test]
    fn test_enforce_fields_accepts_listed_keys_and_id() {
        let allowed = vec!["name".to_string(), "foto".to_string()];
        let body = serde_json::json!({ "id": 3, "name": "BCA", "foto": null });
        assert!(enforce_fields_on(&allowed, &body).is_ok());
        // bulk array body: each object checked
        let bulk = serde_json::json!([{ "name": "a" }, { "name": "b", "secret": 1 }]);
        assert_eq!(
            enforce_fields_on(&allowed, &bulk).unwrap_err(),
            "field 'secret' is not permitted for this role"
        );
        // non-object body: nothing to enforce
        assert!(enforce_fields_on(&allowed, &Value::String("x".into())).is_ok());
    }

    #[test]
    fn test_retain_fields_filters_array_and_object() {
        let allowed = vec!["name".to_string()];
        let mut arr = serde_json::json!([
            { "id": 1, "name": "BCA", "balance": 5 },
            { "id": 2, "name": "BNI", "balance": 9, "owner": "x" }
        ]);
        retain_fields_on(&allowed, &mut arr);
        assert_eq!(arr, serde_json::json!([{ "id": 1, "name": "BCA" }, { "id": 2, "name": "BNI" }]));

        let mut obj = serde_json::json!({ "id": 1, "name": "BCA", "balance": 5 });
        retain_fields_on(&allowed, &mut obj);
        assert_eq!(obj, serde_json::json!({ "id": 1, "name": "BCA" }));

        let mut scalar = Value::Null;
        retain_fields_on(&allowed, &mut scalar);
        assert_eq!(scalar, Value::Null);
    }

    #[test]
    fn test_enforce_allowed_fields_skips_non_write_methods() {
        // Even with a body full of unknown keys, GET/DELETE are never field-enforced.
        let req = actix_web::test::TestRequest::get().uri("/banks").to_http_request();
        let claims = create_claims("admin", "1");
        let mut body = serde_json::json!({ "anything": 1 });
        assert!(enforce_allowed_fields(&claims, &req, &mut body).is_ok());
        assert_eq!(body, serde_json::json!({ "anything": 1 }));
    }

    #[test]
    fn test_converter_token_has_no_field_restriction() {
        let req = actix_web::test::TestRequest::put().uri("/banks/1").to_http_request();
        let claims = Claims { cs: "converter_token".to_string(), ..Claims::default() };
        assert_eq!(allowed_fields_for(&claims, &req), None);
        let mut body = serde_json::json!({ "secret": 1 });
        assert!(enforce_allowed_fields(&claims, &req, &mut body).is_ok());
        let mut data = serde_json::json!([{ "id": 1, "secret": 1 }]);
        if let Some(allowed) = allowed_fields_for(&claims, &req) {
            retain_fields_on(&allowed, &mut data);
        }
        assert_eq!(data, serde_json::json!([{ "id": 1, "secret": 1 }]));
    }

    // --- Claims ---

    #[test]
    fn test_claims_get_roles_single() {
        let claims = Claims {
            rl: "admin/1".to_string(),
            ..Claims::default()
        };
        let roles = claims.get_roles();
        assert_eq!(roles, vec!["admin/1"]);
    }

    #[test]
    fn test_claims_get_roles_multiple() {
        let claims = Claims {
            rl: "admin/1,editor/2,viewer/3".to_string(),
            ..Claims::default()
        };
        let roles = claims.get_roles();
        assert_eq!(roles, vec!["admin/1", "editor/2", "viewer/3"]);
    }

    #[test]
    fn test_claims_get_roles_empty() {
        let claims = Claims {
            rl: "".to_string(),
            ..Claims::default()
        };
        let roles = claims.get_roles();
        assert_eq!(roles, vec![""], "Empty string should produce one empty role");
    }

    #[test]
    fn test_claims_default_values() {
        let claims = Claims::default();
        assert_eq!(claims.id, "");
        assert_eq!(claims.nm, "route_publics");
        assert_eq!(claims.exp, 0);
        assert_eq!(claims.at, 0);
        assert_eq!(claims.rl, "*/127");
        assert_eq!(claims.cs, "");
    }

    #[test]
    fn test_claims_converter_default_values() {
        let conv = ClaimsConverter::default();
        assert_eq!(conv.id, "id");
        assert_eq!(conv.nm, "nm");
        assert_eq!(conv.exp, "exp");
        assert_eq!(conv.at, "at");
        assert_eq!(conv.rl, "rl");
        assert_eq!(conv.cs, "cs");
    }

    #[test]
    fn test_claims_converter_equality() {
        let c1 = ClaimsConverter::default();
        let c2 = ClaimsConverter::default();
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_claims_converter_inequality_when_different() {
        let c = ClaimsConverter {
            id: "user_id".to_string(),
            ..Default::default()
        };
        assert_ne!(c, ClaimsConverter::default());
    }

    // --- evaluate_condition ---

    #[test]
    fn test_condition_eq_user_role_matches() {
        let claims = create_claims("admin", "1");
        let cond = Conditions::Eq {
            eq: ("$user.role".to_string(), "admin".to_string()),
        };
        assert!(evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_eq_user_role_no_match() {
        let claims = create_claims("user", "1");
        let cond = Conditions::Eq {
            eq: ("$user.role".to_string(), "admin".to_string()),
        };
        assert!(!evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_eq_user_id_matches() {
        let claims = create_claims("user", "42");
        let cond = Conditions::Eq {
            eq: ("$user.id".to_string(), "42".to_string()),
        };
        assert!(evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_and_all_true() {
        let claims = create_claims("admin", "42");
        let cond = Conditions::And {
            and: vec![
                Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) },
                Conditions::Eq { eq: ("$user.id".to_string(), "42".to_string()) },
            ],
        };
        assert!(evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_and_one_false() {
        let claims = create_claims("user", "42");
        let cond = Conditions::And {
            and: vec![
                Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) },
                Conditions::Eq { eq: ("$user.id".to_string(), "42".to_string()) },
            ],
        };
        assert!(!evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_or_one_true() {
        let claims = create_claims("user", "42");
        let cond = Conditions::Or {
            or: vec![
                Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) },
                Conditions::Eq { eq: ("$user.id".to_string(), "42".to_string()) },
            ],
        };
        assert!(evaluate_condition(&cond, &claims));
    }

    #[test]
    fn test_condition_or_all_false() {
        let claims = create_claims("user", "99");
        let cond = Conditions::Or {
            or: vec![
                Conditions::Eq { eq: ("$user.role".to_string(), "admin".to_string()) },
                Conditions::Eq { eq: ("$user.id".to_string(), "42".to_string()) },
            ],
        };
        assert!(!evaluate_condition(&cond, &claims));
    }
}
