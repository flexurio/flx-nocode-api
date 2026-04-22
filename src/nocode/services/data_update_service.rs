use actix_web::{web, HttpResponse};
use actix_multipart::Multipart;
use serde_json::Value;
use std::sync::Arc;
use chrono::Local;

use crate::AppState;
use crate::model::{TableSchema, WebResponse, ReferenceForeignKey};
use crate::auth::{check_access, get_user_info_from_token, Claims};
use crate::helpers::{multipart_to_json, get_client_ip};
use crate::crypt::{encrypt, is_encrypted_string};
use crate::storage::sql_store::InsertValue;
use crate::database::state::DbParam;
use crate::nocode::repositories::data_update_repo;
use crate::nocode::pk_utils::{dbparam_from_str_and_type, json_value_from_str_and_type};
use crate::audit::{AuditEntry, write_audit};

#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
pub async fn process_update_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    _ref_fks: &Arc<Vec<ReferenceForeignKey>>,
    multipart: Multipart,
    path: web::Path<String>,
    req: &actix_web::HttpRequest,
) -> HttpResponse {
    let id_raw = path.into_inner();
    
    // Auth Check
    let mut claims = Claims::default();
    let actor_id_opt: Option<String>;

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
    } else {
         claims.id = "0".to_string();
         actor_id_opt = Some("0".to_string());
    }

    // Multipart to JSON
    let mut body = match multipart_to_json(multipart).await {
        Ok(json) => json,
        Err(e) => {
             return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Failed to parse multipart data: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    // Queue Handling
    let isqueue = parameters
        .as_object()
        .and_then(|m| m.get("isqueue"))
        .map(|v| v.as_bool().unwrap_or(false) || v.as_str() == Some("true"))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
        // Enforce Actor ID if authenticated
         if state.require_auth && !state.route_publics.contains(&route.to_string()) {
              if let Some(map) = body.as_object_mut() { map.insert("__actor_id__".into(), serde_json::json!(claims.id)); }
         }
         
         let job = crate::nocode::consumer::WriteJob {
             route: route.to_string(),
             op: crate::nocode::consumer::WriteOpKind::Put { id: id_raw },
             body,
             headers: vec![],
             enqueued_at: chrono::Utc::now().to_rfc3339(),
             actor_id: actor_id_opt,
         };
         
         if state.write_queue_fast_ack {
             crate::nocode::consumer::enqueue_job_background(job, "UPDATE-HANDLER");
              return HttpResponse::Accepted().json(WebResponse {
                    success: true,
                    message: "Enqueued (async)".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
         } else {
             match crate::nocode::consumer::enqueue_job(&job).await {
                 Ok(_) => {
                     return HttpResponse::Accepted().json(WebResponse {
                        success: true,
                        message: "Enqueued".to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                 },
                 Err(e) => {
                      return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Queue error: {}", e),
                        total_data: 0,
                        data: Value::Null,
                    });
                 }
             }
         }
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

    // Prepare Update Fields
    let mut update_fields: Vec<(String, InsertValue)> = Vec::new();
    let mut patch_fields = serde_json::Map::new();
    let mut fk_checks: Vec<(String, String, String, String)> = Vec::new();
    let mut password_override: Option<String> = None;

    if let Some(body_obj) = body.as_object() {
        for column in table_schema.put.columns.iter() {
            let is_required_marker = column.ends_with('*');
            let clean_column = if is_required_marker {
                column.trim_end_matches('*')
            } else {
                column.as_str()
            };

            // Validate required fields marked with *
            if is_required_marker {
                let present = body_obj.get(clean_column)
                    .map(|v| v.to_string().replace('"', ""))
                    .map(|s| !s.trim().is_empty())
                    .unwrap_or(false);
                if !present {
                    return HttpResponse::BadRequest().json(WebResponse {
                        success: false,
                        message: format!("Missing required field: {}", clean_column),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }

            if let Some(value) = body_obj.get(clean_column) {
                let mut value_x = format!("{}", value).replace("\"", "").replace("null", "");
                
                if !value_x.is_empty() {
                    // Collect FK Checks
                    for fk in table_schema.foreign_keys.iter() {
                        if fk.column == clean_column {
                             fk_checks.push((clean_column.to_string(), fk.reference_table.clone(), fk.reference_column.clone(), value_x.clone()));
                        }
                    }

                    // Metadata Check
                    let col = match table_schema.columns.iter().find(|c| c.name == clean_column) {
                         Some(c) => c,
                         None => {
                             return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: format!("Unknown column '{}' for route '{}'", clean_column, route),
                                total_data: 0,
                                data: Value::Null,
                            });
                         }
                    };

                    // Encrypt
                    if col.encrypt {
                        let is_encrypted = is_encrypted_string(&value_x);
                        if !is_encrypted {
                            value_x = encrypt(state.encrypt_key.clone(), value_x.clone());
                        }
                        if route == "flx_users" && clean_column == "password" {
                            password_override = Some(value_x.clone());
                        }
                    }

                    // Type Conversion
                    if col.type_data.contains("int") || col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
                         if let Ok(n) = value_x.parse::<i64>() {
                            update_fields.push((clean_column.to_string(), InsertValue::Param(DbParam::I64(n))));
                            patch_fields.insert(clean_column.to_string(), serde_json::json!(n));
                         } else if let Ok(f) = value_x.parse::<f64>() {
                            update_fields.push((clean_column.to_string(), InsertValue::Param(DbParam::F64(f))));
                            patch_fields.insert(clean_column.to_string(), serde_json::json!(f));
                         } else {
                            update_fields.push((clean_column.to_string(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                            patch_fields.insert(clean_column.to_string(), serde_json::json!(value_x));
                         }
                    } else {
                        update_fields.push((clean_column.to_string(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                        patch_fields.insert(clean_column.to_string(), serde_json::json!(value_x));
                    }
                }
            }
        }
    }

    // Add updated_at/by
    update_fields.push(("updated_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())));
    if state.db_type == crate::model::DbType::Mongodb {
         patch_fields.insert("updated_at".to_string(), serde_json::json!(Local::now().to_rfc3339()));
    } else {
         // for SQL, inserted via Raw, but also good to have in patch_fields if we used it for Mongo logic
    }

    let created_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "updated_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or("int".to_string());
    update_fields.push((
        "updated_by_id".to_string(),
        InsertValue::Param(dbparam_from_str_and_type(&claims.id, &created_by_type)),
    ));
    patch_fields.insert(
        "updated_by_id".to_string(),
        json_value_from_str_and_type(&claims.id, &created_by_type),
    );

    // Extract Authorization header to forward to API validation
    let auth_token = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    // Call Repository
    match data_update_repo::perform_update(
        state,
        table_schema,
        route,
        &id_raw,
        update_fields,
        patch_fields,
        fk_checks,
        password_override,
        &body,
        auth_token
    ).await {
        Ok((msg, count)) => {
            // Audit
            write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: claims.id.clone(),
                action: "PUT",
                route,
                id: Some(&id_raw),
                ip: Some(get_client_ip(req)).as_deref(),
            });

            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: msg,
                total_data: count,
                data: Value::Null,
            })
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: e,
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
