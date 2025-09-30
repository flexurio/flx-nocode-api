use std::{error::Error, fs::File, io, path::Path};

use actix_multipart::Multipart;
use actix_web::{
    web::{self, Data},
    HttpResponse, Responder,
};
use base64::{self, Engine};
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use zip::ZipArchive;

use crate::rate_limit::{RL_WINDOW_LOGIN, RL_WINDOW_LOGIN_FAIL};
use crate::{
    auth::create_token,
    crypt::{decrypt, encrypt},
    helpers::{get_client_ip, multipart_to_json},
    log::log_output,
    model::WebResponse,
    AppState,
};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
use crate::storage::sql_store::SqlStore;
use crate::storage::prelude::{Ddl, CreateTable, ColumnDef, ColumnType, TableConstraint, DefaultExpr, ForeignAction};
use crate::storage::traits::DataStore;

pub async fn login(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {
    // Rate limit by IP (fixed window) — allow disabling with 0 or -1
    let ip_key = get_client_ip(&req);
    let limit_i64: i64 = std::env::var("RATE_LIMIT_LOGIN_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
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

    // Query via DataStore (SqlStore adapter)
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    // DB-specific cast for password so we always read it as string
    let pass_expr: &str = match state.db_type.as_str() {
        "mysql" | "postgres" => "CAST(password as CHAR(255)) as password",
        "sqlite" => "CAST(password as TEXT) as password",
        "mssql" => "CAST(password as NVARCHAR(255)) as password",
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
        let (sql_dbg, _params_dbg) = ds.preview_sql(&q_user);
        log_output("QUERY", "POST", "login", sql_dbg.clone(), true);
    } else {
        log_output("QUERY", "POST", "login", format!("AST flx_users where email=? (db={})", state.db_type), true);
    }
    let rows = ds.query(&q_user).await.unwrap_or_default();
    let (password_db, id_user, name) = if let Some(row0) = rows.first() {
        let password = row0
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
            .replace(" ", "");
        let id = row0.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        let name = row0
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (password, id, name)
    } else {
        ("".to_string(), 0_i64, "".to_string())
    };

    let decrypt_password = decrypt(state.encrypt_key.clone(), password_db);

    if pass_in != decrypt_password {
        // Apply per-user and per-IP failure rate limits within a 5-minute window.
        // Any of these env values <= 0 disables that specific limiter.
        let base_per_min_i64: i64 = std::env::var("RATE_LIMIT_LOGIN_PER_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        // Defaults if env not set: user=5 per 5 min, ip=20 per 5 min
        let fail_user_limit_i64: i64 = std::env::var("RATE_LIMIT_LOGIN_FAIL_USER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| std::cmp::max(5_i64, base_per_min_i64));
        let fail_ip_limit_i64: i64 = std::env::var("RATE_LIMIT_LOGIN_FAIL_IP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| std::cmp::max(20_i64, base_per_min_i64.saturating_mul(5)));

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

    // Cross-DB: fetch endpoint & role and join in Rust via DataStore
    let q_roles = QQ::from("flx_roles")
        .select(["endpoint", "role"]) 
        .r#where(QF::Eq("id_users".into(), QV::I64(id_user)));
    log_output("QUERY", "core.rs/login", "flx_roles", format!("AST id_users=? ~ {}", id_user), true);
    let roles_rows = ds.query(&q_roles).await.unwrap_or_default();
    let roles_data = roles_rows
        .into_iter()
        .filter_map(|v| {
            let obj = v.as_object()?;
            let ep = obj.get("endpoint")?.as_str()?.to_string();
            let rl = obj
                .get("role")?
                .as_i64()
                .map(|n| n.to_string())
                .or_else(|| obj.get("role")?.as_str().map(|s| s.to_string()))?;
            Some(format!("{}/{}", ep, rl))
        })
        .collect::<Vec<_>>()
        .join(",");

    let token = create_token(id_user, name, state.clone(), roles_data);
    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Login Success".to_string(),
        total_data: 1,
        data: json!(token.await),
    })
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

    let encrypt_password = encrypt(state.encrypt_key.clone(), password);

    // Use DataStore (SqlStore) insert with app-side timestamps for cross-DB compatibility
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let now = chrono::Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string();
    let email = body["email"].to_string().trim_matches('"').to_string();
    let phone = body["phone"].to_string().trim_matches('"').to_string();
    let name = body["name"].to_string().trim_matches('"').to_string();

    // enabled type depends on DB: Postgres expects boolean, others accept 1/0
    let enabled_val = match state.db_type.as_str() {
        "postgres" => Value::Bool(true),
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
        if let Ok((sql_dbg, params_dbg)) = ds.preview_insert("flx_users", &doc) {
            log_output("QUERY", "POST", "register", sql_dbg, true);
            log_output("PARAM", "POST", "register", format!("{:?}", params_dbg), true);
        }
    } else {
        log_output("QUERY", "POST", "register", "AST insert flx_users".to_string(), true);
    }

    match ds.insert("flx_users", doc).await {
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

// NCO-POST
pub async fn generate_users(state: Data<AppState>) -> impl Responder {
    // Create tables via DDL AST (vendor-aware)
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());

    // flx_users definition simplified; use Raw types per dialect where necessary
    let users_cols: Vec<ColumnDef> = match state.db_type.as_str() {
        "mssql" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGINT IDENTITY(1,1)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: true },
            ColumnDef { name: "email".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "phone".into(), col_type: ColumnType::Raw("VARCHAR(15)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "password".into(), col_type: ColumnType::Raw("NVARCHAR(MAX)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "name".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "photo".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "email_verified".into(), col_type: ColumnType::Raw("BIT".into()), nullable: false, default: Some(DefaultExpr::Raw("0".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "enabled".into(), col_type: ColumnType::Raw("BIT".into()), nullable: true, default: Some(DefaultExpr::Raw("1".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: false, default: Some(DefaultExpr::Raw("SYSDATETIME()".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        "postgres" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGSERIAL".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: true },
            ColumnDef { name: "email".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "phone".into(), col_type: ColumnType::Raw("VARCHAR(15)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "password".into(), col_type: ColumnType::Raw("TEXT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "name".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "photo".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "email_verified".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: false, default: Some(DefaultExpr::Raw("false".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "enabled".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: true, default: Some(DefaultExpr::Raw("true".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: Some(DefaultExpr::Raw("CURRENT_TIMESTAMP".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        "sqlite" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("INTEGER PRIMARY KEY AUTOINCREMENT".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: false },
            ColumnDef { name: "email".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "phone".into(), col_type: ColumnType::Raw("VARCHAR(15)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "password".into(), col_type: ColumnType::Raw("TEXT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "name".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "photo".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "email_verified".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: false, default: Some(DefaultExpr::Raw("0".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "enabled".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: true, default: Some(DefaultExpr::Raw("1".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: Some(DefaultExpr::CurrentTimestamp), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        _ => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGINT AUTO_INCREMENT".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: true },
            ColumnDef { name: "email".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "phone".into(), col_type: ColumnType::Raw("VARCHAR(15)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "password".into(), col_type: ColumnType::Raw("LONGTEXT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "name".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "photo".into(), col_type: ColumnType::Raw("VARCHAR(50)".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "email_verified".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: false, default: Some(DefaultExpr::Raw("0".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "enabled".into(), col_type: ColumnType::Raw("BOOLEAN".into()), nullable: true, default: Some(DefaultExpr::Raw("1".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: Some(DefaultExpr::Now), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
    };

    let users_constraints = vec![
        TableConstraint::Unique { name: Some("idx_user_email".into()), columns: vec!["email".into()] },
        TableConstraint::Unique { name: Some("idx_user_phone".into()), columns: vec!["phone".into()] },
    ];

    let ddl_users = Ddl::CreateTable(CreateTable {
        if_not_exists: true,
        name: "flx_users".into(),
        columns: users_cols,
        constraints: users_constraints,
    });
    let sql_users = ds.preview_ddl(&ddl_users);
    if let Err(e) = state.db.query(&sql_users).await {
        log_output("ERROR QUERY", "POST", "generate/table/flx_users", format!("{} ~ ERROR : {}", sql_users, e), true);
    }

    // flx_roles
    let roles_cols: Vec<ColumnDef> = match state.db_type.as_str() {
        "mssql" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGINT IDENTITY(1,1)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: true },
            ColumnDef { name: "id_users".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "endpoint".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "role".into(), col_type: ColumnType::Raw("TINYINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: false, default: Some(DefaultExpr::Raw("SYSDATETIME()".into())), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME2".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        "postgres" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGSERIAL".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: true },
            ColumnDef { name: "id_users".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "endpoint".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "role".into(), col_type: ColumnType::Raw("SMALLINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: Some(DefaultExpr::CurrentTimestamp), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("TIMESTAMP".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        "sqlite" => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("INTEGER PRIMARY KEY AUTOINCREMENT".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: false },
            ColumnDef { name: "id_users".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "endpoint".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "role".into(), col_type: ColumnType::Raw("SMALLINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: Some(DefaultExpr::CurrentTimestamp), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
        _ => vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Raw("BIGINT AUTO_INCREMENT".into()), nullable: false, default: None, auto_increment: true, primary_key_inline: true },
            ColumnDef { name: "id_users".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "endpoint".into(), col_type: ColumnType::Raw("VARCHAR(100)".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "role".into(), col_type: ColumnType::Raw("TINYINT".into()), nullable: false, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: Some(DefaultExpr::Now), auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_at".into(), col_type: ColumnType::Raw("DATETIME".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "created_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "updated_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
            ColumnDef { name: "deleted_by_id".into(), col_type: ColumnType::Raw("BIGINT".into()), nullable: true, default: None, auto_increment: false, primary_key_inline: false },
        ],
    };

    let roles_constraints = vec![
        TableConstraint::Unique { name: Some("uq_role_user_endpoint".into()), columns: vec!["id_users".into(), "endpoint".into()] },
        TableConstraint::ForeignKey {
            name: Some("fk_roles_user".into()),
            columns: vec!["id_users".into()],
            ref_table: "flx_users".into(),
            ref_columns: vec!["id".into()],
            on_delete: Some(ForeignAction::Cascade),
            on_update: Some(ForeignAction::NoAction),
        },
    ];

    let ddl_roles = Ddl::CreateTable(CreateTable {
        if_not_exists: true,
        name: "flx_roles".into(),
        columns: roles_cols,
        constraints: roles_constraints,
    });
    let sql_roles = ds.preview_ddl(&ddl_roles);
    if let Err(e) = state.db.query(&sql_roles).await {
        log_output("ERROR QUERY", "POST", "generate/table/flx_roles", format!("{} ~ ERROR : {}", sql_roles, e), true);
    }

    // Query flx_users to check if admin user already exists
    let select_admin_sql = "SELECT id FROM flx_users WHERE email = 'admin'";
    let mut id_user: i64 = match &state.db.query(select_admin_sql).await {
        Ok(row) => {
            // check if row is empty
            if row.is_empty() {
                0
            } else {
                row[0].get("id").and_then(|v| v.as_i64()).unwrap_or(0)
            }
        }
        Err(err) => {
            log_output(
                "ERROR QUERY",
                "POST",
                "generate/table/select-flx_users-admin",
                select_admin_sql.to_string() + " ~ ERROR : " + &err.to_string(),
                true,
            );
            0
        },
    };
    log_output("QUERY", "POST", "generate/table/users", select_admin_sql.to_string(), true);

    if id_user == 0 {
        id_user = 1;
        // create string number
        let random_pass = rand::rng().random_range(1000..9999).to_string();
        let encrypt_password = encrypt(state.encrypt_key.clone(), random_pass.clone());

        println!("==========================================");
        println!("Your admin Password: {:?}", random_pass);
        println!("==========================================");

    // Insert admin using AST insert_with for cross-db NOW()
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        let now_fn = state.query_converter.datetime_now.clone();
        let (sql_insert_admin, params_insert_admin) = ds
            .preview_insert_with(
                "flx_users",
                &[
                    ("id".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::I64(1))),
                    ("email".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::Str("admin".into()))),
                    ("phone".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::Str("5758".into()))),
                    ("password".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::Str(encrypt_password))),
                    ("name".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::Str("Admin Flexurio".into()))),
                    ("created_at".into(), crate::storage::sql_store::InsertValue::Raw(now_fn.clone())),
                    ("updated_at".into(), crate::storage::sql_store::InsertValue::Raw(now_fn.clone())),
                    ("enabled".into(), crate::storage::sql_store::InsertValue::Param(crate::database::state::DbParam::Bool(true))),
                ],
            )
            .unwrap();
        let built = crate::database::state::rehydrate_placeholders(&sql_insert_admin, &state.db_type);
        // For MSSQL identity column, allow explicit ID insertion by toggling IDENTITY_INSERT
        let result = if state.db_type == "mssql" {
            let _ = state.db.query("SET IDENTITY_INSERT flx_users ON").await;
            let r = state.db.query_with_params(&built, params_insert_admin).await;
            let _ = state.db.query("SET IDENTITY_INSERT flx_users OFF").await;
            r
        } else {
            state.db.query_with_params(&built, params_insert_admin).await
        };
        if let Err(err) = &result {
            log_output(
                "ERROR QUERY",
                "POST",
                "generate/table/insert-flx_users-admin",
                sql_insert_admin.clone() + " ~ ERROR : " + err.to_string().as_str(),
                true,
            );
        }

        // Insert default roles (two rows) with AST bulk insert
        use crate::storage::sql_store::InsertValue as IV;
        let cols = vec!["id_users".into(), "endpoint".into(), "role".into(), "created_at".into()];
        let rows = vec![
            vec![IV::Param(crate::database::state::DbParam::I64(id_user)), IV::Param(crate::database::state::DbParam::Str("flx_users".into())), IV::Param(crate::database::state::DbParam::I64(127)), IV::Raw(now_fn.clone())],
            vec![IV::Param(crate::database::state::DbParam::I64(id_user)), IV::Param(crate::database::state::DbParam::Str("flx_roles".into())), IV::Param(crate::database::state::DbParam::I64(127)), IV::Raw(now_fn.clone())],
        ];
        if let Ok((sql_roles_ins, params_roles_ins)) = ds.preview_insert_bulk("flx_roles", &cols, &rows) {
            let built_roles = crate::database::state::rehydrate_placeholders(&sql_roles_ins, &state.db_type);
            let _ = state.db.query_with_params(&built_roles, params_roles_ins).await;
        }
    }

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Generate Table users".to_string(),
        total_data: 1,
        data: Value::Null,
    })
}

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
}

async fn get_latest_release() -> Result<String, Box<dyn Error + Send + Sync>> {
    let url = "https://api.github.com/repos/flexurio/flx-nocode-api/releases/latest";

    let client = Client::new();
    let resp = client
        .get(url)
        .header("User-Agent", "flexurio-client") // GitHub butuh user-agent
        .send()
        .await?
        .error_for_status()?
        .json::<Release>()
        .await?;

    Ok(resp.tag_name)
}

/// Download file zip dari URL dan simpan ke file lokal
async fn download_file(url: String, output: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let mut resp = client.get(url).send().await?.error_for_status()?;
    let mut out = File::create(output)?;
    while let Some(chunk) = resp.chunk().await? {
        use std::io::Write;
        out.write_all(&chunk)?;
    }
    Ok(())
}

/// Ekstrak file zip ke folder tujuan
fn extract_zip(zip_path: &str, target_dir: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let out_path = Path::new(target_dir).join(file.name());

        if file.is_dir() {
            std::fs::create_dir_all(&out_path)?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut outfile = File::create(&out_path)?;
            io::copy(&mut file, &mut outfile)?;
        }
    }

    Ok(())
}

// Function create directory & get config
pub(crate) async fn create_dir_and_get_config(conf: &str) -> Result<(), std::io::Error> {
    // If db directory already exists, skip
    if Path::new(conf).exists() {
        return Ok(());
    }

    // get latest version from github release
    let latest_version = get_latest_release()
        .await
        .unwrap_or_else(|_| "v1.0.0".to_string());
    let url = format!(
        "https://github.com/flexurio/flx-nocode-api/releases/download/{}/config.zip",
        latest_version
    );
    let zip_path = "config.zip";
    let extract_to = "."; // current working directory

    println!("Downloading...");
    download_file(url, zip_path)
        .await
        .map_err(std::io::Error::other)?;

    println!("Extracting...");
    extract_zip(zip_path, extract_to).map_err(std::io::Error::other)?;

    // move config to <conf>
    std::fs::rename(format!("{}/config", extract_to), conf)?;

    // Ensure config directory now exists
    if !Path::new(conf).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "config directory not found after extraction",
        ));
    }

    // Clean up zip file
    let _ = std::fs::remove_file(zip_path);

    Ok(())
}

// function download .env from latest release github
pub(crate) async fn download_env_file() -> Result<(), Box<dyn Error + Send + Sync>> {
    let latest_version = get_latest_release().await?;
    let url = format!(
        "https://github.com/flexurio/flx-nocode-api/releases/download/{}/env",
        latest_version
    );
    let output_path = ".env";

    println!("Downloading .env file from {}", url);
    download_file(url, output_path).await?;

    Ok(())
}
