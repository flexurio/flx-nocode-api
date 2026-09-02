use actix_web::{HttpResponse, web};
use chrono::Local;
use serde_json::Value;
use std::sync::Arc;

use super::web_err;
use crate::AppState;
use crate::audit::{AuditEntry, write_audit};
use crate::auth::{Claims, check_access, get_user_info_from_token};
use crate::crypt::{encrypt, is_encrypted_string};
use crate::database::state::DbParam;
use crate::helpers::get_client_ip;
use crate::model::{ReferenceForeignKey, TableSchema, WebResponse};
use crate::nocode::pk_utils::{
    dbparam_from_str_and_type, json_value_from_str_and_type, validate_pk_path,
};
use crate::nocode::repositories::data_update_repo;
use crate::storage::sql_store::InsertValue;

fn unauthorized(msg: impl Into<String>) -> HttpResponse {
    HttpResponse::Unauthorized().json(web_err(msg))
}

fn bad_request(msg: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(web_err(msg))
}

fn server_error(msg: impl Into<String>) -> HttpResponse {
    HttpResponse::InternalServerError().json(web_err(msg))
}

#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
pub async fn process_update_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    _ref_fks: &Arc<Vec<ReferenceForeignKey>>,
    mut body: Value,
    path: web::Path<String>,
    req: &actix_web::HttpRequest,
) -> HttpResponse {
    let id_raw = path.into_inner();

    // Auth Check — single mutable `claims` so the id propagates out of the if-block.
    let mut claims = Claims::default();
    let actor_id_opt: Option<String>;
    let auth_required = state.require_auth && !state.route_publics.contains(route);

    if auth_required {
        claims = match get_user_info_from_token(req, state.clone()) {
            Ok(c) => c,
            Err(_) => return unauthorized("Invalid token"),
        };
        if let Err(e) = check_access(&claims, req) {
            return unauthorized(format!("Unauthorized: {}", e));
        }
        actor_id_opt = Some(claims.id.clone());
    } else {
        claims.id = "0".to_string();
        actor_id_opt = Some("0".to_string());
    }

    // Queue Handling
    let isqueue = parameters
        .as_object()
        .and_then(|m| m.get("isqueue"))
        .map(|v| v.as_bool().unwrap_or(false) || v.as_str() == Some("true"))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
        if auth_required {
            if let Some(map) = body.as_object_mut() {
                map.insert("__actor_id__".into(), serde_json::json!(claims.id));
            }
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
        }
        return match crate::nocode::consumer::enqueue_job(&job).await {
            Ok(_) => HttpResponse::Accepted().json(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            }),
            Err(e) => server_error(format!("Queue error: {}", e)),
        };
    }

    // Schema Check
    if table_schema.table.is_empty() {
        return HttpResponse::FailedDependency()
            .json(web_err(format!("Entity {} not found", route)));
    }

    // Validate composite PK shape (only meaningful when the schema declares PK columns).
    if !table_schema.primary_key.columns.is_empty()
        && let Err(e) = validate_pk_path(&id_raw, table_schema.primary_key.columns.len())
    {
        return bad_request(e);
    }

    // Prepare Update Fields
    let mut update_fields: Vec<(String, InsertValue)> = Vec::new();
    let mut patch_fields = serde_json::Map::new();
    let mut fk_checks: Vec<(String, String, String, String, String)> = Vec::new();
    let mut password_override: Option<String> = None;

    if let Some(body_obj) = body.as_object() {
        for column in table_schema.put.columns.iter() {
            let is_required_marker = column.ends_with('*');
            let clean_column = if is_required_marker {
                column.trim_end_matches('*')
            } else {
                column.as_str()
            };

            // Validate required fields (treat Value::Null and whitespace-only as missing).
            if is_required_marker {
                let present = match body_obj.get(clean_column) {
                    None | Some(Value::Null) => false,
                    Some(Value::String(s)) => {
                        let t = s.trim();
                        !t.is_empty() && !t.eq_ignore_ascii_case("null")
                    }
                    Some(_) => true,
                };
                if !present {
                    return bad_request(format!("Missing required field: {}", clean_column));
                }
            }

            let Some(raw_value) = body_obj.get(clean_column) else {
                continue;
            };

            // Typed string extraction (avoid "null" leaking from Value::Null).
            let str_value: String = match raw_value {
                Value::Null => String::new(),
                Value::String(s) => s.trim().to_string(),
                v => v.to_string().trim().to_string(),
            };

            if str_value.is_empty() {
                continue;
            }

            // Metadata Check (moved ahead of FK checks so col.type_data is available below)
            let Some(col) = table_schema.columns.iter().find(|c| c.name == clean_column) else {
                return bad_request(format!(
                    "Unknown column '{}' for route '{}'",
                    clean_column, route
                ));
            };

            // Collect FK Checks
            for fk in table_schema.foreign_keys.iter() {
                if fk.column == clean_column {
                    fk_checks.push((
                        clean_column.to_string(),
                        fk.reference_table.clone(),
                        fk.reference_column.clone(),
                        str_value.clone(),
                        col.type_data.clone(),
                    ));
                }
            }

            // Encrypt
            let value_for_db = if col.encrypt && !is_encrypted_string(&str_value) {
                let enc = encrypt(state.encrypt_key.clone(), str_value.clone());
                if route == "flx_users" && clean_column == "password" {
                    password_override = Some(enc.clone());
                }
                enc
            } else {
                if col.encrypt && route == "flx_users" && clean_column == "password" {
                    password_override = Some(str_value.clone());
                }
                str_value.clone()
            };

            // Type-aware binding via shared helpers.
            let dbparam = dbparam_from_str_and_type(&value_for_db, &col.type_data);
            let json_val = json_value_from_str_and_type(&value_for_db, &col.type_data);
            update_fields.push((clean_column.to_string(), InsertValue::Param(dbparam)));
            patch_fields.insert(clean_column.to_string(), json_val);
        }
    }

    // In-memory Pre-processing of Details for Update
    let mut prepared_details: Vec<crate::nocode::repositories::data_create_repo::PreparedDetailBatch> = Vec::new();
    let skip_columns: std::collections::HashSet<&str> = [
        "created_at", "created_by_id", "updated_at", "updated_by_id", "deleted_at", "deleted_by_id",
    ].iter().copied().collect();

    for detail in &table_schema.details {
        if detail.field.is_empty() || detail.target_table.is_empty() {
            continue;
        }

        if let Some(items_val) = body.get(&detail.field) {
            let items_arr = match items_val {
                Value::Array(arr) => arr,
                Value::Null => continue,
                _ => return bad_request(format!("Field '{}' must be an array of objects", detail.field)),
            };

            let target_schema_opt = crate::config::SCHEMAS.0.get(&detail.target_table);

            if let Some(detail_schema) = target_schema_opt {
                let detail_post_cols: std::collections::HashSet<&str> = detail_schema
                    .post
                    .columns
                    .iter()
                    .map(|s| s.trim_end_matches('*'))
                    .collect();

                let mut detail_insert_cols: Vec<String> = detail_schema
                    .columns
                    .iter()
                    .filter(|c| {
                        !c.auto_increment
                            && !skip_columns.contains(c.name.as_str())
                            && (detail_post_cols.is_empty() || detail_post_cols.contains(c.name.as_str()))
                    })
                    .map(|c| c.name.clone())
                    .collect();

                if !detail_insert_cols.contains(&detail.foreign_key_column) {
                    detail_insert_cols.insert(0, detail.foreign_key_column.clone());
                }

                let fk_index = detail_insert_cols
                    .iter()
                    .position(|c| c == &detail.foreign_key_column)
                    .unwrap_or(0);

                let fk_col_def = detail_schema
                    .columns
                    .iter()
                    .find(|c| c.name == detail.foreign_key_column);
                let fk_type_data = fk_col_def
                    .map(|c| c.type_data.clone())
                    .unwrap_or_else(|| "int".to_string());

                let mut detail_rows: Vec<Vec<InsertValue>> = Vec::with_capacity(items_arr.len());
                let mut detail_response_items: Vec<serde_json::Map<String, Value>> =
                    Vec::with_capacity(items_arr.len());

                for (idx, item) in items_arr.iter().enumerate() {
                    let Some(item_obj) = item.as_object() else {
                        return bad_request(format!("Detail item #{} in '{}' must be an object", idx + 1, detail.field));
                    };

                    let mut row_values: Vec<InsertValue> = Vec::with_capacity(detail_insert_cols.len());
                    let mut item_resp_map = serde_json::Map::with_capacity(detail_insert_cols.len());

                    for col_name in &detail_insert_cols {
                        if col_name == &detail.foreign_key_column {
                            row_values.push(InsertValue::Param(DbParam::Null));
                            continue;
                        }

                        let col_def_opt = detail_schema.columns.iter().find(|c| &c.name == col_name);
                        let raw_val = item_obj.get(col_name);
                        let str_val = match raw_val {
                            None | Some(Value::Null) => String::new(),
                            Some(Value::String(s)) => s.trim().to_string(),
                            Some(v) => v.to_string().trim().to_string(),
                        };

                        if !str_val.is_empty() {
                            for fk in &detail_schema.foreign_keys {
                                if &fk.column == col_name {
                                    let td = col_def_opt.map(|c| c.type_data.clone()).unwrap_or_else(|| "text".to_string());
                                    fk_checks.push((col_name.clone(), fk.reference_table.clone(), fk.reference_column.clone(), str_val.clone(), td));
                                }
                            }
                        }

                        let encrypt_col = col_def_opt.map(|c| c.encrypt).unwrap_or(false);
                        let value_for_db = if encrypt_col && !str_val.is_empty() && !is_encrypted_string(&str_val) {
                            encrypt(state.encrypt_key.clone(), str_val.clone())
                        } else {
                            str_val.clone()
                        };

                        let type_lower = col_def_opt.map(|c| c.type_data.to_lowercase()).unwrap_or_default();
                        let is_int = type_lower.contains("int");
                        let is_float = type_lower.contains("float") || type_lower.contains("decimal");

                        if is_int {
                            if let Ok(n) = value_for_db.parse::<i64>() {
                                item_resp_map.insert(col_name.clone(), serde_json::json!(n));
                                row_values.push(InsertValue::Param(DbParam::I64(n)));
                            } else {
                                item_resp_map.insert(col_name.clone(), raw_val.cloned().unwrap_or(Value::Null));
                                row_values.push(InsertValue::Param(DbParam::Null));
                            }
                        } else if is_float {
                            if let Ok(f) = value_for_db.parse::<f64>() {
                                item_resp_map.insert(col_name.clone(), serde_json::json!(f));
                                row_values.push(InsertValue::Param(DbParam::F64(f)));
                            } else {
                                item_resp_map.insert(col_name.clone(), raw_val.cloned().unwrap_or(Value::Null));
                                row_values.push(InsertValue::Param(DbParam::Null));
                            }
                        } else {
                            item_resp_map.insert(col_name.clone(), raw_val.cloned().unwrap_or(Value::String(String::new())));
                            row_values.push(InsertValue::Param(DbParam::Str(value_for_db)));
                        }
                    }

                    detail_rows.push(row_values);
                    detail_response_items.push(item_resp_map);
                }

                prepared_details.push(crate::nocode::repositories::data_create_repo::PreparedDetailBatch {
                    field: detail.field.clone(),
                    target_table: detail.target_table.clone(),
                    foreign_key_column: detail.foreign_key_column.clone(),
                    fk_type_data,
                    columns: detail_insert_cols,
                    fk_index,
                    rows: detail_rows,
                    response_items: detail_response_items,
                });
            }
        }
    }

    // Add updated_at/by
    update_fields.push((
        "updated_at".to_string(),
        InsertValue::Raw(state.query_converter.datetime_now.clone()),
    ));
    if state.db_type == crate::model::DbType::Mongodb {
        patch_fields.insert(
            "updated_at".to_string(),
            serde_json::json!(Local::now().to_rfc3339()),
        );
    }

    let updated_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "updated_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or_else(|| "int".to_string());
    update_fields.push((
        "updated_by_id".to_string(),
        InsertValue::Param(dbparam_from_str_and_type(&claims.id, &updated_by_type)),
    ));
    patch_fields.insert(
        "updated_by_id".to_string(),
        json_value_from_str_and_type(&claims.id, &updated_by_type),
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
        prepared_details,
        &body,
        auth_token,
    )
    .await
    {
        Ok((msg, count, mut updated_data)) => {
            // Invalidate L1 in-memory cache and L2 Redis cache
            state.l1_cache.invalidate_all();
            if state.is_cachedb {
                let cache_prefix = crate::database::redis::build_key_prefix("public", route);
                let _ = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await;
            }

            if auth_required {
                let ip_opt = get_client_ip(req);
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "PUT",
                    route,
                    id: Some(&id_raw),
                    ip: Some(ip_opt.as_str()),
                });
            }

            if let Some(obj) = updated_data.as_object_mut() {
                if !obj.contains_key("id") {
                    obj.insert("id".to_string(), serde_json::Value::String(id_raw.clone()));
                }
            }

            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: msg,
                total_data: count,
                data: updated_data,
            })
        }
        Err(e) => server_error(e),
    }
}
