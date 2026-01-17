use std::{env};

use actix_web::{web, HttpResponse};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde_json::Value;

use crate::{helpers::get_client_ip, AppState};

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
    // Fast path: check IP whitelist first to avoid expensive token operations
    if is_ip_whitelisted(req, &state.whitelist_ips) || 
        state.route_publics.contains(&req.path().to_string()) ||
        state.converter_token != ClaimsConverter::default() {
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

    let query = env::var("CUSTOME_JWT_QUERY");
    let mut addjwt = String::new();
    if let Ok(mut sql_query) = query {
        sql_query = sql_query.to_lowercase();

        if !sql_query.is_empty() {
            sql_query = sql_query.replace("{:?}", &id_user);

            // Optimize: Add timeout to prevent hanging on slow queries
            let query_future = state.db.query(&sql_query);
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
    req: actix_web::HttpRequest,
    state: web::Data<AppState>,
) -> Result<Claims, bool> {
    if is_ip_whitelisted(&req, &state.whitelist_ips) {
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

fn get_permissions(value: i8) -> Vec<&'static str> {
    // 1 = DELETE / DELETE
    // 2 = WRITE / ADD
    // 4 = READ / SHOW
    // 8 = EXECUTE
    // 16 = OPEN/CLOSE
    // 32 = EXPORT
    // 64 = APPROVE/REJECT

    let mut permissions = Vec::new();

    if value & 1 != 0 {
        permissions.push("delete");
    }
    if value & 2 != 0 {
        permissions.push("write");
    }
    if value & 4 != 0 {
        permissions.push("read");
    }
    if value & 8 != 0 {
        permissions.push("execute");
    }
    if value & 16 != 0 {
        permissions.push("open/close");
    }
    if value & 32 != 0 {
        permissions.push("export");
    }
    if value & 64 != 0 {
        permissions.push("approve/reject");
    }

    permissions
}

pub fn check_access(claims: &Claims, route: &str, permission: &str) -> bool {
    if claims.cs == "converter_token" {
        return true;
    }
    // from claims.roles get rl where ep = route
    let mut role = 0_i8;
    for r in claims.get_roles().iter() {
        // split r by "/"
        let route_rol = r.split("/").collect::<Vec<&str>>();

        // Cek route spesifik atau wildcard
        if !(route_rol[0] == route || route_rol[0] == "*") {
            continue;
        }
        if let Some(val) = route_rol.get(1).and_then(|s| s.parse::<i8>().ok()) {
            role = val;
        }
    }

    let access = get_permissions(role);
    access.contains(&permission.to_lowercase().as_str())
}
