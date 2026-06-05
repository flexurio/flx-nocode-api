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
) -> Result<(), HttpResponse> {
    // Fast path: check IP whitelist and public routes first
    if is_ip_whitelisted(req, &state.whitelist_ips) ||
        state.route_publics.contains(&req.path().to_string()) {
        return Ok(());
    }

    if !state.require_auth {
        return Ok(());
    }

    // Extract Authorization header once
    let auth_header = match req.headers().get("Authorization") {
        Some(header) => match header.to_str() {
            Ok(auth_str) if auth_str.starts_with("Bearer ") => auth_str,
            _ => {
                return Err(HttpResponse::Unauthorized().json("Invalid Authorization header format"))
            }
        },
        None => return Err(HttpResponse::Unauthorized().json("Missing Authorization header")),
    };

    // When converter_token is set, the token is issued by an external/third-party
    // identity provider. We cryptographically verify it using an operator-provided
    // key (CONVERTER_JWT_SECRET or CONVERTER_JWT_PUBLIC_KEY). Verification is
    // mandatory: if no key is configured the request is rejected (fail closed),
    // unless the operator explicitly opts out via CONVERTER_JWT_INSECURE_SKIP_VERIFY.
    if state.converter_token != ClaimsConverter::default() {
        return validate_converter_token(auth_header, &state.converter_token.exp);
    }

    // Create validation config once
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.leeway = 30; // Allow 30 seconds clock skew

    match decode::<Claims>(
        auth_header.trim_start_matches("Bearer "),
        &DecodingKey::from_secret(state.secret.as_ref()),
        &validation,
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Token validation failed: {}", e);
            Err(HttpResponse::Unauthorized().json("Invalid or expired token"))
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
fn validate_converter_token(auth_header: &str, exp_field: &str) -> Result<(), HttpResponse> {
    let token = auth_header.trim_start_matches("Bearer ").trim();
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(HttpResponse::Unauthorized().json("Invalid token structure"));
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
                Err(HttpResponse::Unauthorized().json("Invalid or expired token"))
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
                return Err(HttpResponse::Unauthorized().json("Token expired"));
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
    Err(HttpResponse::Unauthorized().json("Token verification not configured"))
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

    match encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.secret.as_ref()),
    ) {
        Ok(token) => token,
        Err(e) => {
            eprintln!("Failed to create JWT token: {}", e);
            String::new()
        }
    }
}

// Function untuk mengekstrak claims dari token
fn extract_token_claims(token: &str, secret: &[u8]) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token = token.trim_start_matches("Bearer ");

    // Decode token dan ekstrak claims
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret),
        &Validation::new(Algorithm::HS256),
    )?;

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
        return match extract_token_claims(auth, state.secret.as_ref()) {
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
    #[allow(dead_code)]
    pub permission_id: String,
    #[serde(rename = "if")]
    pub condition: Option<Conditions>,
    #[serde(default)]
    #[allow(dead_code)]
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
        let loc = std::env::var("LOC_CONFIG").unwrap_or_else(|_| "config".to_string());
        let path = format!("{}/rules.json", loc);
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

pub fn evaluate_access(rules: &[Rule], claims: &Claims, current_path: &str, current_method: &str) -> Result<(), String> {
    // Find matching rule
    // Simple matching for now: exact match or parameter match {id}
    let matched_rule = rules.iter().find(|r| {
        let route_parts: Vec<&str> = r.endpoint.split('/').filter(|s| !s.is_empty()).collect();
        let path_parts: Vec<&str> = current_path.split('/').filter(|s| !s.is_empty()).collect();

        if route_parts.len() != path_parts.len() {
            return false;
        }

        route_parts.iter().zip(path_parts.iter()).all(|(r_part, p_part)| {
            r_part.starts_with('{') && r_part.ends_with('}') || r_part == p_part
        })
    });

    match matched_rule {
        Some(rule) => {
            // Check method
            let allow = rule.allows.iter().find(|a| a.method.eq_ignore_ascii_case(current_method));
            
            match allow {
                Some(a) => {
                    // Check condition
                    if let Some(cond) = &a.condition {
                        if evaluate_condition(cond, claims) {
                            Ok(())
                        } else {
                            Err("Access denied by rule condition".to_string())
                        }
                    } else {
                        Ok(())
                    }
                },
                None => Err(format!("Method {} not allowed for this rule", current_method)),
            }
        },
        None => Err(format!("Rule not defined for endpoint: {}", current_path)),
    }
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
        let mut c = ClaimsConverter::default();
        c.id = "user_id".to_string();
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
