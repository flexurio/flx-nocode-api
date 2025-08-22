use std::env;

use actix_web::{web, HttpResponse};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub id: i64,
    pub nm: String,
    pub exp: usize,
    pub at: usize,
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
            id: 0,
            nm: "route_publics".to_string(),
            exp: 0,
            at: 0,
            rl: "*/127".to_string(),
            cs: "".to_string(),
        }
    }
}

// Middleware untuk verifikasi token
pub fn validate_token(
    req: actix_web::HttpRequest,
    state: web::Data<AppState>,
) -> Result<web::Data<AppState>, HttpResponse> {
    if is_ip_whitelisted(&req, &state.whitelist_ips) {
        // Anda bisa sesuaikan isi Claims berikut sesuai kebutuhan
        return Ok(state);
    }


    println!("Validating token...");

     // Cek apakah ada header Authorization

    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let mut validation = Validation::new(Algorithm::HS256);
                validation.validate_exp = true;
                match decode::<Claims>(
                    auth_str.trim_start_matches("Bearer "),
                    &DecodingKey::from_secret(state.secret.as_ref()),
                    &validation,
                ) {
                    Ok(_) => return Ok(state), // Token valid, lanjutkan
                    Err(_) => return Err(HttpResponse::Unauthorized().json("Invalid token")),
                }
            }
        }
    }
    Err(HttpResponse::Unauthorized().json("Missing or invalid Authorization header"))
}


// Handler untuk login dan generate token
pub async fn create_token(id_user: i64, name: String, state: web::Data<AppState>, roles: String) -> String {
    let expiration = Utc::now()
        .checked_add_signed(Duration::days(1))
        .expect("valid timestamp")
        .timestamp() as usize;

    let query = env::var("CUSTOME_JWT_QUERY");
    let mut addjwt = "".to_string();
    if query.is_ok() {
        let mut sql_query = query.unwrap();
        sql_query = sql_query.to_lowercase();

        if !sql_query.is_empty() {

            sql_query = sql_query.replace("{:?}", &id_user.to_string());

            addjwt = state.db.query(&sql_query).await.unwrap().first()
                .and_then(|value| value.as_str().map(|s| s.to_string()))
                .unwrap_or_default();

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

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.secret.as_ref()),
    )
    .unwrap()
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

fn is_ip_whitelisted(req: &actix_web::HttpRequest, whitelist: &[String]) -> bool {
    if let Some(peer_addr) = req.peer_addr() {
    let ip = peer_addr.ip().to_string();
    whitelist.contains(&ip)
    } else {
        false
    }
}

// Contoh penggunaan
pub fn get_user_info_from_token(
    req: actix_web::HttpRequest,
    state: web::Data<AppState>,
) -> Result<Claims, bool> {
    if is_ip_whitelisted(&req, &state.whitelist_ips) {
        println!("IP is whitelisted, returning default claims.");
        // Anda bisa sesuaikan isi Claims berikut sesuai kebutuhan
        return Ok(Claims {
            id: 0,
            nm: "whitelisted".to_string(),
            exp: 0,
            at: 0,
            rl: "*/127".to_string(),
            cs: "".to_string(),
        });
    }


    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                match extract_token_claims(auth_str, state.secret.as_ref()) {
                    Ok(claims) => return Ok(claims),
                    Err(_) => return Err(false),
                }
            }
        }
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
    // from claims.roles get rl where ep = route
    let mut role = 0_i8;
    for r in claims.get_roles().iter() {
        // split r by "/"
        let route_rol = r.split("/").collect::<Vec<&str>>();

        // Cek route spesifik atau wildcard
        if route_rol[0] == route || route_rol[0] == "*" {
            if let Ok(val) = route_rol[1].parse::<i8>() {
                role = val;
            }
        }
    }

    let access = get_permissions(role);
    access.contains(&permission.to_lowercase().as_str())
}
