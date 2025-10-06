use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::audit::{write_audit, AuditEntry};
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token},
    database::state::DbParam,
    helpers::filter_table_schema,
    log::log_output,
    model::{TableSchema, WebResponse},
    AppState,
};
use crate::storage::sql_store::SqlStore;
use chrono::Local;
use std::sync::Arc;

// NCO-PATCH
pub async fn process_sp(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let mut actor_id: Option<String> = None;
    if !state.route_publics.contains(&route) {
        let req_for_auth = req.clone();
        let claims = match get_user_info_from_token(req_for_auth, state.clone()) {
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

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
        actor_id = Some(claims.id);
    }
    // Rate-limit (allow disable with 0 or -1)
    let ip_key = crate::helpers::get_client_ip(&req);
    let limit_i64: i64 = std::env::var("RATE_LIMIT_MUTATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if limit_i64 > 0 {
        let limit = (limit_i64.min(u32::MAX as i64)) as u32;
        if !RL_WINDOW_MUTATE.check_and_increment(&format!("patch:{}:{}", route, ip_key), limit) {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".into(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }
    // Per-user limit (for non-public routes only)
    if !state.route_publics.contains(&route) {
        if let Some(ref uid) = actor_id {
            if limit_i64 > 0
                && !uid.is_empty()
                && !RL_WINDOW_MUTATE
                    .check_and_increment(&format!("patch:{}:user:{}", route, uid), limit_i64 as u32)
            {
                return HttpResponse::TooManyRequests().json(WebResponse {
                    success: false,
                    message: "Too many requests".into(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // get parameters value only allowed from table_schema.trace.parameters
    // loop every table_schema.trace.parameters

    let table_schema: TableSchema = filter_table_schema(&table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Build ordered parameters according to schema.patch.parameters
    let params_map = parameters.into_inner();
    let param_count = table_schema.patch.parameters.len();
    let mut param_sp: Vec<String> = Vec::with_capacity(param_count); // Pre-allocate
    let mut bind_params: Vec<DbParam> = Vec::with_capacity(param_count); // Pre-allocate

    for name in table_schema.patch.parameters.iter() {
        match params_map.get(name) {
            Some(v) => {
                let s = v.to_string().trim_matches('"').to_string();
                // Try to infer numeric types if the schema declares a column with the same name
                // Otherwise, fall back to best-effort parse
                if let Some(col) = table_schema.columns.iter().find(|c| c.name == *name) {
                    let t = col.type_data.to_lowercase();
                    if t.contains("int") {
                        if let Ok(n) = s.parse::<i64>() { bind_params.push(DbParam::I64(n)); } else { bind_params.push(DbParam::Str(s)); }
                    } else if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") {
                        if let Ok(f) = s.parse::<f64>() { bind_params.push(DbParam::F64(f)); } else { bind_params.push(DbParam::Str(s)); }
                    } else {
                        bind_params.push(DbParam::Str(s));
                    }
                } else {
                    // Not tied to a column; heuristic
                    if let Ok(n) = s.parse::<i64>() {
                        bind_params.push(DbParam::I64(n));
                    } else if let Ok(f) = s.parse::<f64>() {
                        bind_params.push(DbParam::F64(f));
                    } else {
                        bind_params.push(DbParam::Str(s));
                    }
                }
                param_sp.push("?".into());
            }
            None => {
                // Missing required parameter declared in schema
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Missing required parameter: {}", name),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // Use SqlStore to compile dialect-aware procedure call
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let param_count = param_sp.len();
    let (s_sql, compiled_params) = match ds.preview_call_procedure(&table_schema.patch.pre_process_sp, param_count, bind_params.clone()) {
        Ok((sql, params)) => {
            if *crate::ISDEBUG {
                log_output("QUERY", "PATCH(CALL)", route.as_str(), sql.clone(), true);
                log_output("PARAMS", "PATCH(CALL)", route.as_str(), format!("{:?}", params), true);
            }
            (sql, params)
        }
        Err(e) => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Unsupported or invalid procedure call: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    // MongoDB does not support raw SQL stored procedure execution; return explicit unsupported
    if state.db_type == "mongodb" {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "PATCH procedure execution is not supported for MongoDB".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Begin transaction via generic store
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    match tx.raw_sql(&s_sql, compiled_params).await {
        Ok(rows) => {
            tx.commit().await.ok();
            // Audit
            write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: actor_id.unwrap_or_default(),
                action: "PATCH",
                route: &route,
                id: None,
                ip: req
                    .clone()
                    .peer_addr()
                    .map(|a| a.ip().to_string())
                    .as_deref(),
            });
            // Respect return_mode
            let mode = table_schema.patch.return_mode.to_ascii_lowercase();
            if mode == "rows" {
                let total = rows.len() as i32;
                HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Processes executed".to_string(),
                    total_data: total,
                    data: Value::Array(rows),
                })
            } else if mode == "affected" {
                // tiberius/sqlx doesn't always return affected count in this path; use rows len if any
                let affected = rows.len() as i32;
                HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Processes executed".to_string(),
                    total_data: affected,
                    data: Value::Null,
                })
            } else {
                HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Processes executed".to_string(),
                    total_data: 1,
                    data: Value::Null,
                })
            }
        }

        Err(err) => {
            tx.rollback().await.ok();
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PATCH: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
