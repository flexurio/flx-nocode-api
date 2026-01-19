use actix_web::{web, HttpResponse};
use serde_json::Value;
use std::sync::Arc;
use chrono::Local;

use crate::AppState;
use crate::model::{TableSchema, WebResponse};
use crate::auth::{check_access, get_user_info_from_token};
use crate::database::state::DbParam;
use crate::nocode::repositories::data_patch_repo;
use crate::audit::{AuditEntry, write_audit};

pub async fn process_patch_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    req: &actix_web::HttpRequest,
) -> HttpResponse {
    
    // Auth Check
    let mut actor_id_opt: Option<String> = None;

    if state.require_auth && !state.route_publics.contains(&route.to_string()) {
        let claims = match get_user_info_from_token(req, state.clone()) {
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

        if let Err(e) = check_access(&claims, req) {
             return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
        actor_id_opt = Some(claims.id.clone());
    }

    // Schema Check
    if table_schema.table.is_empty() {
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: format!("Entity {} not found", route),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Parameter Logic
    let params_map = parameters.clone().into_inner();
    let param_count = table_schema.patch.parameters.len();
    let mut bind_params: Vec<DbParam> = Vec::with_capacity(param_count);

    for name in table_schema.patch.parameters.iter() {
        match params_map.get(name) {
            Some(v) => {
                let s = v.to_string().trim_matches('"').to_string();
                if let Some(col) = table_schema.columns.iter().find(|c| c.name == *name) {
                    let t = col.type_data.to_lowercase();
                    if t.contains("int") {
                        if let Ok(n) = s.parse::<i64>() { bind_params.push(DbParam::I64(n)); } else { bind_params.push(DbParam::Str(s)); }
                    } else if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") {
                        if let Ok(f) = s.parse::<f64>() { bind_params.push(DbParam::F64(f)); } else { bind_params.push(DbParam::Str(s)); }
                    } else {
                        bind_params.push(DbParam::Str(s));
                    }
                } else if let Ok(n) = s.parse::<i64>() {
                    bind_params.push(DbParam::I64(n));
                } else if let Ok(f) = s.parse::<f64>() {
                    bind_params.push(DbParam::F64(f));
                } else {
                    bind_params.push(DbParam::Str(s));
                }
            }
            None => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Missing required parameter: {}", name),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // Call Repo
    match data_patch_repo::execute_procedure(state, table_schema, route, bind_params, param_count).await {
        Ok((rows, count)) => {
            // Audit
            // Note: Patch operations often don't have a specific record ID they operate on in a RESTful sense
            // unless returned or passed. We use None for ID here as per original logic.
             write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: actor_id_opt.unwrap_or_default(),
                action: "PATCH",
                route,
                id: None,
                ip: Some(crate::helpers::get_client_ip(req)).as_deref(),
            });

            // Return Mode Logic
            let mode = table_schema.patch.return_mode.to_ascii_lowercase();
            if mode == "rows" {
                 HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Processes executed".to_string(),
                    total_data: rows.len() as i32,
                    data: Value::Array(rows),
                })
            } else if mode == "affected" {
                 HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Processes executed".to_string(),
                    total_data: count as i32,
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
        Err(e) => {
             // Repo error includes BadRequest logic if unsupported, but generic error format
             // The original code returned BadRequest for unsupported MongoDB/Invalid SP call, and InternalServerError for DB errors.
             // Here we wrap in generic error. We can differentiate if needed, but error message usually suffices.
             // If "Unsupported" is in message, maybe 400? For now 500 or 400 based on message check if we want strict compatibility,
             // but 500 is safe default for "failed execution".
             // Actually, original code returned 400 for unsupported.
             if e.contains("Unsupported") || e.contains("not supported") {
                 HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: e,
                    total_data: 0,
                    data: Value::Null,
                })
             } else {
                 HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: e,
                    total_data: 0,
                    data: Value::Null,
                })
             }
        }
    }
}
