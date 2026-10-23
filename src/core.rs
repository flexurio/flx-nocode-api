use std::error::Error;

use actix_multipart::Multipart;
use actix_web::{
    HttpResponse, Responder,
    web::{self, Data},
};
use base64::{self, Engine};
use rand::RngExt;
use serde_json::{Value, json};

use crate::rate_limit::{RL_WINDOW_LOGIN, RL_WINDOW_LOGIN_FAIL};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
use crate::storage::sql_store::SqlStore;
use crate::{
    AppState,
    auth::create_token,
    crypt::{decrypt, hash_password, hash_password_async, is_argon2_hash, verify_password_async},
    helpers::{get_client_ip, multipart_to_json},
    log::log_output,
    model::WebResponse,
};
// removed unused import: DataStore trait not needed in scope for method calls on trait objects
// removed unused import: DataStore trait not needed in scope for method calls on trait objects

// Precomputed Argon2 hash used for a constant-time dummy verification when a
// user is not found, to reduce login user-enumeration via timing differences.
static DUMMY_PASSWORD_HASH: once_cell::sync::Lazy<String> =
    once_cell::sync::Lazy::new(|| hash_password("flx-dummy-credential-for-timing"));

// Env vars are loaded once at process start (dotenv in main()) and never change
// afterward, so resolve login rate-limit settings once instead of on every
// login attempt. Two distinct defaults (10 vs 3) are preserved for
// RATE_LIMIT_LOGIN_PER_MIN to match the pre-existing behavior at each call site.
static RATE_LIMIT_LOGIN_PER_MIN: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| {
    std::env::var("RATE_LIMIT_LOGIN_PER_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(10)
});
static RATE_LIMIT_LOGIN_PER_MIN_BASE: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| {
    std::env::var("RATE_LIMIT_LOGIN_PER_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(3)
});
static RATE_LIMIT_LOGIN_FAIL_USER: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| {
    std::env::var("RATE_LIMIT_LOGIN_FAIL_USER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::cmp::max(5_i64, *RATE_LIMIT_LOGIN_PER_MIN_BASE))
});
static RATE_LIMIT_LOGIN_FAIL_IP: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| {
    std::env::var("RATE_LIMIT_LOGIN_FAIL_IP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::cmp::max(20_i64, RATE_LIMIT_LOGIN_PER_MIN_BASE.saturating_mul(5)))
});

pub async fn login(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {
    // Rate limit by IP (fixed window) — allow disabling with 0 or -1
    let ip_key = get_client_ip(&req);
    let limit_i64: i64 = *RATE_LIMIT_LOGIN_PER_MIN;
    if limit_i64 > 0 {
        let limit = (limit_i64.min(u32::MAX as i64)) as u32;
        if !RL_WINDOW_LOGIN.check_and_increment(&format!("login:{}", ip_key), limit) {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many login attempts".into(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }
    // Parse Authorization Basic header safely
    let Some(hdr) = req.headers().get("Authorization") else {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Missing Authorization".into(),
            total_data: 0,
            data: Value::Null,
        });
    };
    let Ok(hstr) = hdr.to_str() else {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Invalid Authorization".into(),
            total_data: 0,
            data: Value::Null,
        });
    };
    let Some(b64) = hstr.strip_prefix("Basic ") else {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Expect Basic".into(),
            total_data: 0,
            data: Value::Null,
        });
    };
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64) else {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Invalid base64".into(),
            total_data: 0,
            data: Value::Null,
        });
    };
    let Ok(pair) = String::from_utf8(bytes) else {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Invalid credentials".into(),
            total_data: 0,
            data: Value::Null,
        });
    };
    let mut parts = pair.splitn(2, ":");
    let email = parts.next().unwrap_or("");
    let pass_in = parts.next().unwrap_or("");

    // Query via DataStore; for SQL we can still use SqlStore preview in debug
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    // DB-specific cast for password so we always read it as string (skip casts for Mongo)
    let pass_expr: &str = match state.db_type.as_str() {
        "mysql" | "postgres" => "CAST(password as CHAR(255)) as password",
        "sqlite" => "CAST(password as TEXT) as password",
        "mssql" => "CAST(password as NVARCHAR(255)) as password",
        "mongodb" => "password",
        _ => "password",
    };
    let q_user = QQ::from("flx_users")
        .select(["id", "name", pass_expr]) // columns needed for auth
        .r#where(QF::And(vec![
            QF::Eq("email".into(), QV::Str(email.to_string())),
            QF::Eq("enabled".into(), QV::Bool(true)),
        ]))
        .limit(1);
    if *crate::ISDEBUG {
        if state.db_type == crate::model::DbType::Mongodb {
            log_output(
                "QUERY",
                "POST",
                "login",
                "AST (mongo) flx_users by email".to_string(),
                true,
            );
        } else {
            let (sql_dbg, _params_dbg) = ds.preview_sql(&q_user);
            log_output("QUERY", "POST", "login", sql_dbg.clone(), true);
        }
    } else {
        log_output(
            "QUERY",
            "POST",
            "login",
            format!("AST flx_users where email=? (db={})", state.db_type),
            true,
        );
    }
    let rows = match state.store.query(&q_user).await {
        Ok(rows) => rows,
        Err(e) => {
            // Distinguish "database unreachable" from "credentials invalid": silently
            // treating a DB error as an empty result set makes every outage look like
            // a wrong password to the caller and to anyone reading auth failure logs.
            log_output(
                "ERROR",
                "core.rs/login",
                "flx_users-query",
                format!("Login query failed: {}", e),
                true,
            );
            return HttpResponse::ServiceUnavailable().json(WebResponse {
                success: false,
                message: "Service temporarily unavailable".into(),
                total_data: 0,
                data: Value::Null,
            });
        }
    };
    let (password_db, id_user_str, name) = if let Some(row0) = rows.first() {
        let password = row0
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
            .replace(" ", "");
        // Accept id either as number or string; normalize to String for JWT
        let id = row0
            .get("id")
            .and_then(|v| {
                if let Some(n) = v.as_i64() {
                    Some(n.to_string())
                } else if let Some(s) = v.as_str() {
                    Some(s.to_string())
                } else if let Some(obj) = v.as_object() {
                    obj.get("$oid")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        let name = row0
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (password, id, name)
    } else {
        ("".to_string(), String::new(), "".to_string())
    };

    // Verify credentials. New credentials are stored as one-way Argon2 hashes.
    // Legacy deployments may still hold AES-encrypted (reversible) passwords, so
    // we transparently verify those and opportunistically re-hash on success.
    let user_found = !id_user_str.is_empty();
    let stored_is_legacy = !password_db.is_empty() && !is_argon2_hash(&password_db);
    let password_ok = if is_argon2_hash(&password_db) {
        verify_password_async(pass_in.to_string(), password_db.clone()).await
    } else if stored_is_legacy {
        // Legacy reversible format: compare against the decrypted value.
        pass_in == decrypt(state.encrypt_key.clone(), password_db.clone())
    } else {
        // No stored credential (user absent/disabled). Run a dummy verify so the
        // response time is comparable to the valid-user path.
        let _ = verify_password_async(pass_in.to_string(), DUMMY_PASSWORD_HASH.clone()).await;
        false
    };

    if !password_ok {
        // Apply per-user and per-IP failure rate limits within a 5-minute window.
        // Any of these env values <= 0 disables that specific limiter.
        // Defaults if env not set: user=5 per 5 min, ip=20 per 5 min
        let fail_user_limit_i64: i64 = *RATE_LIMIT_LOGIN_FAIL_USER;
        let fail_ip_limit_i64: i64 = *RATE_LIMIT_LOGIN_FAIL_IP;

        let mut over_user = false;
        let mut over_ip = false;
        let user_key = email.to_lowercase();
        if fail_user_limit_i64 > 0 {
            let limit = (fail_user_limit_i64.min(u32::MAX as i64)) as u32;
            over_user = !RL_WINDOW_LOGIN_FAIL
                .check_and_increment(&format!("loginfail:user:{}", user_key), limit);
        }
        if fail_ip_limit_i64 > 0 {
            let limit = (fail_ip_limit_i64.min(u32::MAX as i64)) as u32;
            over_ip = !RL_WINDOW_LOGIN_FAIL
                .check_and_increment(&format!("loginfail:ip:{}", ip_key), limit);
        }

        if over_user || over_ip {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many login attempts".into(),
                total_data: 0,
                data: Value::Null,
            });
        }

        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Login Failed".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Opportunistic migration: now that we have a verified plaintext, replace a
    // legacy reversible credential with an Argon2 hash. Best-effort; failures are
    // logged but never block login. Skipped for MongoDB (uses the document store
    // path rather than the SQL helpers below).
    if user_found && stored_is_legacy && state.db_type != crate::model::DbType::Mongodb {
        let new_hash = hash_password_async(pass_in.to_string()).await;
        if !new_hash.is_empty() {
            let id_val = if let Ok(n) = id_user_str.parse::<i64>() {
                QV::I64(n)
            } else {
                QV::Str(id_user_str.clone())
            };
            let filter = QF::Eq("id".into(), id_val);
            let fields = [(
                "password".to_string(),
                crate::storage::sql_store::InsertValue::Param(
                    crate::database::state::DbParam::Str(new_hash),
                ),
            )];
            match ds.preview_update_with("flx_users", Some(&filter), &fields) {
                Ok((sql, params)) => {
                    let built = crate::database::state::rehydrate_placeholders(
                        &sql,
                        state.db_type.as_str(),
                    );
                    if let Err(e) = state.db.query_with_params(&built, params).await {
                        log_output(
                            "ERROR",
                            "core.rs/login",
                            "password-rehash",
                            format!(
                                "Failed to migrate password hash for user {}: {}",
                                id_user_str, e
                            ),
                            true,
                        );
                    } else {
                        log_output(
                            "INFO",
                            "core.rs/login",
                            "password-rehash",
                            format!(
                                "Migrated legacy password to Argon2 for user {}",
                                id_user_str
                            ),
                            true,
                        );
                    }
                }
                Err(e) => {
                    log_output(
                        "ERROR",
                        "core.rs/login",
                        "password-rehash",
                        format!(
                            "Failed to build rehash update for user {}: {}",
                            id_user_str, e
                        ),
                        true,
                    );
                }
            }
        }
    }

    let roles_data = roles_for_user(&state, &id_user_str).await;

    let (refresh, _refresh_exp) = crate::auth::create_refresh_token(&id_user_str, &name);
    let token = create_token(id_user_str, name, state.clone(), roles_data).await;
    HttpResponse::Ok()
        // Access token stays in `data` (unchanged contract); the refresh token
        // travels in a header so existing clients keep working.
        .insert_header(("X-Refresh-Token", refresh))
        .insert_header(("X-Token-Expires-In", crate::auth::JWT_ACCESS_TTL_SECS.to_string()))
        .json(WebResponse {
            success: true,
            message: "Login Success".to_string(),
            total_data: 1,
            data: json!(token),
        })
}

/// Comma-separated `flx_roles.role` entries of a user, as embedded in the JWT.
pub async fn roles_for_user(state: &AppState, id_user_str: &str) -> String {
    let id_roles_filter = if let Ok(n) = id_user_str.parse::<i64>() {
        QV::I64(n)
    } else {
        QV::Str(id_user_str.to_string())
    };
    let q_roles = QQ::from("flx_roles")
        .select(["role"])
        .r#where(QF::Eq("id_users".into(), id_roles_filter));
    let roles_rows = state.store.query(&q_roles).await.unwrap_or_default();

    let mut result = String::with_capacity(roles_rows.len() * 20);
    let mut first = true;
    for v in roles_rows {
        if let Some(rl) = v.as_object().and_then(|o| o.get("role")) {
            let piece = match rl {
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::String(s) => s.clone(),
                _ => continue,
            };
            if !first {
                result.push(',');
            }
            first = false;
            result.push_str(&piece);
        }
    }
    result
}

/// `POST /token/refresh` — `Authorization: Bearer <refresh token>`.
/// Issues a new access token (roles re-read from the database, so role changes
/// apply) and rotates the refresh token (the presented one is revoked).
pub async fn refresh_token(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {
    let Some(hdr) = req.headers().get("Authorization").and_then(|h| h.to_str().ok()) else {
        return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Missing Authorization".into(), total_data: 0, data: Value::Null });
    };
    let claims = match crate::auth::verify_refresh_token(hdr) {
        Ok(c) => c,
        Err(msg) => {
            return HttpResponse::Unauthorized().json(WebResponse { success: false, message: msg.into(), total_data: 0, data: Value::Null });
        }
    };
    // The user must still exist and be enabled.
    let q_user = QQ::from("flx_users")
        .select(["id", "name"])
        .r#where(QF::And(vec![
            QF::Eq("id".into(), if let Ok(n) = claims.id.parse::<i64>() { QV::I64(n) } else { QV::Str(claims.id.clone()) }),
            QF::Eq("enabled".into(), QV::Bool(true)),
        ]))
        .limit(1);
    let rows = match state.store.query(&q_user).await {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::ServiceUnavailable().json(WebResponse { success: false, message: "Service temporarily unavailable".into(), total_data: 0, data: Value::Null });
        }
    };
    if rows.is_empty() {
        crate::auth::revoke(&claims);
        return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "User disabled or removed".into(), total_data: 0, data: Value::Null });
    }
    let name = rows[0].get("name").and_then(|v| v.as_str()).unwrap_or(&claims.nm).to_string();
    let roles = roles_for_user(&state, &claims.id).await;
    // Rotate: the old refresh token is dead from now on.
    crate::auth::revoke(&claims);
    let (new_refresh, _) = crate::auth::create_refresh_token(&claims.id, &name);
    let access = create_token(claims.id.clone(), name, state.clone(), roles).await;
    HttpResponse::Ok()
        .insert_header(("X-Refresh-Token", new_refresh))
        .insert_header(("X-Token-Expires-In", crate::auth::JWT_ACCESS_TTL_SECS.to_string()))
        .json(WebResponse { success: true, message: "Token refreshed".into(), total_data: 1, data: json!(access) })
}

/// `POST /logout` — revokes the presented access token; an optional
/// `X-Refresh-Token` header revokes the matching refresh token too.
pub async fn logout(req: actix_web::HttpRequest) -> impl Responder {
    use actix_web::HttpMessage;
    let mut revoked = 0;
    if let Some(claims) = req.extensions().get::<crate::auth::Claims>().cloned()
        && claims.jti.is_some()
    {
        crate::auth::revoke(&claims);
        revoked += 1;
    }
    if let Some(rt) = req.headers().get("X-Refresh-Token").and_then(|h| h.to_str().ok())
        && let Ok(rc) = crate::auth::verify_refresh_token(rt)
    {
        crate::auth::revoke(&rc);
        revoked += 1;
    }
    HttpResponse::Ok().json(WebResponse { success: true, message: "Logged out".into(), total_data: revoked, data: Value::Null })
}

// NCO-POST
pub async fn register(state: Data<AppState>, multipart: Multipart) -> impl Responder {
    let body = match multipart_to_json(multipart).await {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Invalid multipart: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    if body["email"] == "" || body["password"] == "" || body["name"] == "" || body["phone"] == "" {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Email and Password is required".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let password_value = &body["password"];
    let password = if let Some(s) = password_value.as_str() {
        s.to_string()
    } else {
        password_value.to_string()
    };

    let encrypt_password = hash_password_async(password).await;

    // Use DataStore insert with app-side timestamps for cross-DB compatibility
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let now = chrono::Local::now()
        .naive_local()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    let email = body["email"].to_string().trim_matches('"').to_string();
    let phone = body["phone"].to_string().trim_matches('"').to_string();
    let name = body["name"].to_string().trim_matches('"').to_string();

    // enabled type depends on DB: Postgres expects boolean, others accept 1/0
    let enabled_val = match state.db_type.as_str() {
        "postgres" | "mongodb" => Value::Bool(true),
        _ => Value::Number(1.into()),
    };

    let doc = json!({
        "email": email,
        "phone": phone,
        "password": encrypt_password,
        "name": name,
        "created_at": now,
        "updated_at": now,
        "enabled": enabled_val
    });

    if *crate::ISDEBUG {
        if state.db_type == crate::model::DbType::Mongodb {
            log_output(
                "QUERY",
                "POST",
                "register",
                "AST(mongo) insert flx_users".to_string(),
                true,
            );
        } else if let Ok((sql_dbg, params_dbg)) = ds.preview_insert("flx_users", &doc) {
            log_output("QUERY", "POST", "register", sql_dbg, true);
            log_output(
                "PARAM",
                "POST",
                "register",
                format!("{:?}", params_dbg),
                true,
            );
        }
    } else {
        log_output(
            "QUERY",
            "POST",
            "register",
            "AST insert flx_users".to_string(),
            true,
        );
    }

    match state.store.insert("flx_users", doc).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Register Success".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-POST: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }
}

// Handler to get all roles from state.rules
pub async fn get_roles(state: Data<AppState>) -> impl Responder {
    let rules = state.rules();
    let roles = rules["role"].as_array().cloned().unwrap_or_default();

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Roles retrieved successfully".to_string(),
        total_data: roles.len() as i32,
        data: json!(roles),
    })
}

// NCO-POST
pub async fn generate_users(
    state: Data<AppState>,
    schemas: &std::collections::HashMap<String, std::sync::Arc<crate::model::TableSchema>>,
) -> String {
    // MongoDB: no DDL. Seed collections and default data using DataStore.
    if state.db_type == crate::model::DbType::Mongodb {
        use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
        // Check if admin exists
        let q_admin = QQ::from("flx_users")
            .select(["id"]) // expect numeric id if we seed
            .r#where(QF::Eq("email".into(), QV::Str("admin".into())))
            .limit(1);

        let rows = state.store.query(&q_admin).await.unwrap_or_default();

        let mut id_user_str: String = rows
            .first()
            .and_then(|row| row.get("id"))
            .and_then(|v| {
                if let Some(n) = v.as_i64() {
                    Some(n.to_string())
                } else if let Some(s) = v.as_str() {
                    Some(s.to_string())
                } else if let Some(obj) = v.as_object() {
                    obj.get("$oid")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if id_user_str.is_empty() {
            // create random password, encrypt, then insert admin
            let random_pass = rand::rng().random_range(1000..9999).to_string();
            let encrypt_password = hash_password(&random_pass);

            println!("==========================================");
            println!("Your admin Password: {:?}", random_pass);
            println!("==========================================");

            let now_iso = chrono::Local::now().to_rfc3339();
            // Do not set explicit id; let Mongo generate _id, then capture it
            let user_doc = serde_json::json!({
                "email": "admin",
                "phone": "5758",
                "password": encrypt_password,
                "name": "Admin Flexurio",
                "created_at": now_iso,
                "updated_at": now_iso,
                "enabled": true
            });
            match state.store.insert("flx_users", user_doc).await {
                Ok(resp) => {
                    // Expect { inserted_id: ... }
                    if let Some(v) = resp.get("inserted_id") {
                        if let Some(s) = v.as_str() {
                            id_user_str = s.to_string();
                        } else if let Some(n) = v.as_i64() {
                            id_user_str = n.to_string();
                        } else if v.is_object() {
                            // Try $oid style
                            if let Some(oid) = v.get("$oid").and_then(|x| x.as_str()) {
                                id_user_str = oid.to_string();
                            }
                        }
                    }
                }
                Err(err) => {
                    // Do not terminate app; log and continue (duplicate key or other)
                    log_output(
                        "ERROR",
                        "POST",
                        "insert-flx_users-admin",
                        format!("Failed to insert admin user (continuing): {}", err),
                        true,
                    );
                    return id_user_str;
                }
            }

            // Insert default roles
            let role1 = serde_json::json!({
                "id_users": id_user_str,
                "endpoint": "flx_users",
                "role": 127,
                "created_at": now_iso
            });
            let role2 = serde_json::json!({
                "id_users": id_user_str,
                "endpoint": "flx_roles",
                "role": 127,
                "created_at": now_iso
            });
            if *crate::ISDEBUG {
                log_output(
                    "INSERT",
                    "POST",
                    "generate/mongodb/insert-flx_roles",
                    format!("{} | {}", role1, role2),
                    true,
                );
            }
            let _ = state.store.insert("flx_roles", role1).await;
            let _ = state.store.insert("flx_roles", role2).await;
        }
        id_user_str
    } else {
        // Create tables via DDL AST from Schema (Single Source of Truth)
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());

        // Ensure flx_users exists
        if let Some(schema) = schemas.get("flx_users") {
            // Apply default collate if needed (consistent with main.rs logic)
            let mut schema_with_collate = schema.as_ref().clone();
            if schema_with_collate.collate.trim().is_empty()
                && state.db_type == crate::model::DbType::Mysql
            {
                schema_with_collate.collate = state.default_collate.clone();
            }

            let (sql_create_table, sql_create_index) =
                crate::nocode::generate::generate_table(&ds, &schema_with_collate);

            // Log the Create Table query
            log_output(
                "QUERY",
                "BOOT",
                "generate/table/flx_users",
                format!("Executing SQL: {}", sql_create_table),
                true,
            );

            // Execute Create Table
            if let Err(e) = state.db.query(&sql_create_table).await {
                log_output(
                    "ERROR QUERY",
                    "POST",
                    "generate/table/flx_users",
                    format!("{} ~ ERROR : {}", sql_create_table, e),
                    true,
                );
            }

            // Execute Create Index
            for sql_idx in sql_create_index {
                if let Err(e) = state.db.query(&sql_idx).await {
                    // Start ignoring duplicate index errors similar to generate.rs if needed, or just log
                    log_output(
                        "ERROR QUERY",
                        "POST",
                        "generate/index/flx_users",
                        format!("{} ~ ERROR : {}", sql_idx, e),
                        true,
                    );
                }
            }
        } else {
            log_output(
                "ERROR",
                "BOOT",
                "generate_users",
                "Schema for flx_users not found!".to_string(),
                true,
            );
        }

        // Ensure flx_roles exists
        if let Some(schema) = schemas.get("flx_roles") {
            // Apply default collate if needed
            let mut schema_with_collate = schema.as_ref().clone();
            if schema_with_collate.collate.trim().is_empty()
                && state.db_type == crate::model::DbType::Mysql
            {
                schema_with_collate.collate = state.default_collate.clone();
            }

            let (sql_create_table, sql_create_index) =
                crate::nocode::generate::generate_table(&ds, &schema_with_collate);

            // Log the Create Table query
            log_output(
                "QUERY",
                "BOOT",
                "generate/table/flx_roles",
                format!("Executing SQL: {}", sql_create_table),
                true,
            );

            // Execute Create Table
            if let Err(e) = state.db.query(&sql_create_table).await {
                log_output(
                    "ERROR QUERY",
                    "POST",
                    "generate/table/flx_roles",
                    format!("{} ~ ERROR : {}", sql_create_table, e),
                    true,
                );
            }

            // Execute Create Index
            for sql_idx in sql_create_index {
                if let Err(e) = state.db.query(&sql_idx).await {
                    log_output(
                        "ERROR QUERY",
                        "POST",
                        "generate/index/flx_roles",
                        format!("{} ~ ERROR : {}", sql_idx, e),
                        true,
                    );
                }
            }
        } else {
            log_output(
                "ERROR",
                "BOOT",
                "generate_users",
                "Schema for flx_roles not found!".to_string(),
                true,
            );
        }

        // AST query to check if admin user exists: SELECT id FROM flx_users WHERE email='admin' LIMIT 1
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
        let q_admin = QQ::from("flx_users")
            .select(["id"])
            .r#where(QF::Eq("email".into(), QV::Str("admin".into())))
            .limit(1);
        let (sql_admin, params_admin) = ds.preview_sql(&q_admin);
        let built_admin =
            crate::database::state::rehydrate_placeholders(&sql_admin, state.db_type.as_str());
        println!("=========================================================");
        println!("ADMIN ID QUERY : {}", built_admin);
        println!("=========================================================");
        let mut id_user: i64 = match &state.db.query_with_params(&built_admin, params_admin).await {
            Ok(rows) => {
                if rows.is_empty() {
                    0
                } else {
                    rows[0].get("id").and_then(|v| v.as_i64()).unwrap_or(0)
                }
            }
            Err(err) => {
                log_output(
                    "ERROR QUERY",
                    "POST",
                    "generate/table/select-flx_users-admin",
                    format!(" ~ ERROR : {}", err),
                    true,
                );
                0
            }
        };
        log_output(
            "QUERY",
            "POST",
            "generate/table/users",
            sql_admin.to_string(),
            true,
        );

        if id_user == 0 {
            id_user = 1;
            // create string number
            let random_pass = rand::rng().random_range(1000..9999).to_string();
            let encrypt_password = hash_password(&random_pass);

            println!("==========================================");
            println!("Your admin Password: {:?}", random_pass);
            println!("==========================================");

            // Insert admin using AST insert_with for cross-db NOW()
            let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
            let now_fn = state.query_converter.datetime_now.clone();
            let insert_fields = [
                (
                    "id".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::I64(1),
                    ),
                ),
                (
                    "email".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::Str("admin".into()),
                    ),
                ),
                (
                    "phone".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::Str("5758".into()),
                    ),
                ),
                (
                    "password".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::Str(encrypt_password),
                    ),
                ),
                (
                    "name".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::Str("Admin Flexurio".into()),
                    ),
                ),
                (
                    "created_at".into(),
                    crate::storage::sql_store::InsertValue::Raw(now_fn.clone()),
                ),
                (
                    "updated_at".into(),
                    crate::storage::sql_store::InsertValue::Raw(now_fn.clone()),
                ),
                (
                    "enabled".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::Bool(true),
                    ),
                ),
                (
                    "email_verified".into(),
                    crate::storage::sql_store::InsertValue::Param(
                        crate::database::state::DbParam::I64(1),
                    ),
                ),
            ];
            let (sql_insert_admin, params_insert_admin) =
                match ds.preview_insert_with("flx_users", &insert_fields) {
                    Ok(v) => v,
                    Err(err) => {
                        log_output(
                            "ERROR",
                            "QUERY",
                            "generate/table/insert-flx_users-admin",
                            format!("Failed to build insert for admin user: {}", err),
                            true,
                        );
                        return id_user.to_string();
                    }
                };
            let built = crate::database::state::rehydrate_placeholders(
                &sql_insert_admin,
                state.db_type.as_str(),
            );
            // For MSSQL identity column, allow explicit ID insertion by toggling IDENTITY_INSERT
            let result = if state.db_type == crate::model::DbType::Mssql {
                let _ = state.db.query("SET IDENTITY_INSERT flx_users ON").await;
                let r = state
                    .db
                    .query_with_params(&built, params_insert_admin.clone())
                    .await;
                let _ = state.db.query("SET IDENTITY_INSERT flx_users OFF").await;
                r
            } else {
                state
                    .db
                    .query_with_params(&built, params_insert_admin.clone())
                    .await
            };
            if let Err(err) = &result {
                log_output(
                    "ERROR",
                    "QUERY",
                    "generate/table/insert-flx_users-admin",
                    format!(" ~ ERROR : {}, QUERY : {}", err, built),
                    true,
                );
                log_output(
                    "PARAM",
                    "QUERY",
                    "generate/table/insert-flx_users-admin",
                    format!(" ~ PARAM INSERT : {:?}", params_insert_admin),
                    true,
                );
            }

            // Insert default roles (two rows) with AST bulk insert
            if let Err(err) = generate_role_admin(
                &state,
                ds,
                id_user,
                vec!["flx_users".into(), "flx_roles".into()],
            )
            .await
            {
                log_output(
                    "ERROR",
                    "generate_users",
                    "generate_role_admin",
                    format!("Failed to generate roles: {}", err),
                    true,
                );
            }
        }

        "1".into()
    }
}

// create function generate flx_roles
pub async fn generate_role_admin(
    state: &AppState,
    ds: SqlStore,
    id_user: i64,
    _routes: Vec<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let now_fn = state.query_converter.datetime_now.clone();
    if state.db_type != crate::model::DbType::Mongodb {
        // Insert default roles (two rows) with AST bulk insert
        use crate::storage::sql_store::InsertValue as IV;
        let cols = vec![
            "id_users".into(),
            "endpoint".into(),
            "role".into(),
            "created_at".into(),
        ];

        let mut rows = Vec::new();
        // Just insert for every route provided in '_routes'
        if _routes.is_empty() {
            // Fallback if empty? Or just do nothing?
            // But usually it's flx_users and flx_roles at least.
        }
        for r in _routes {
            rows.push(vec![
                IV::Param(crate::database::state::DbParam::I64(id_user)),
                IV::Param(crate::database::state::DbParam::Str(r)),
                IV::Param(crate::database::state::DbParam::I64(127)),
                IV::Raw(now_fn.clone()),
            ]);
        }
        if let Ok((sql_roles_ins, params_roles_ins)) =
            ds.preview_insert_bulk("flx_roles", &cols, &rows)
        {
            let built_roles = crate::database::state::rehydrate_placeholders(
                &sql_roles_ins,
                state.db_type.as_str(),
            );
            // Properly await the async query and handle errors
            match state
                .db
                .query_with_params(&built_roles, params_roles_ins)
                .await
            {
                Ok(_) => {
                    log_output(
                        "INSERT",
                        "generate_role_admin",
                        "flx_roles",
                        format!("Role inserted for user {}", id_user),
                        true,
                    );
                }
                Err(err) => {
                    log_output(
                        "ERROR",
                        "generate_role_admin",
                        "flx_roles",
                        format!("Failed to insert role for user {}: {}", id_user, err),
                        true,
                    );
                    return Err(err.into());
                }
            }
        } else {
            log_output(
                "ERROR",
                "generate_role_admin",
                "flx_roles",
                format!("Failed to generate SQL for user {}", id_user),
                true,
            );
        }
    }
    Ok(())
}
