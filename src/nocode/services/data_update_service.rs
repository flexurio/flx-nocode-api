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
use crate::nocode::field_rules;
use crate::nocode::pk_utils::{
    bool_kind, dbparam_from_str_and_type, is_bool_type, json_value_from_str_and_type,
    json_value_from_value_and_type, validate_pk_path, dbparam_from_value_and_type,
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
    // Typed mapping: unique violation -> 409, FK -> 422, contention -> 503, SQL hidden.
    crate::error::ApiError::respond_message(&msg.into())
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
    let auth_required = state.require_auth && !state.is_public_route(route);

    if auth_required {
        claims = match get_user_info_from_token(req, state.clone()) {
            Ok(c) => c,
            Err(_) => return unauthorized("Invalid token"),
        };
        if let Err(e) = check_access(&claims, req) {
            return unauthorized(format!("Unauthorized: {}", e));
        }
        // rules.json `allowed_fields`: reject keys this role may not write (before any injection).
        if let Err(e) = crate::auth::enforce_allowed_fields(&claims, req, &mut body) {
            return HttpResponse::Forbidden().json(web_err(format!("Forbidden: {}", e)));
        }
        actor_id_opt = Some(claims.id.clone());
    } else {
        claims.id = "0".to_string();
        actor_id_opt = Some("0".to_string());
    }

    // Optimistic locking: `If-Match: <version>` (or body.version) must equal the
    // row's current `version` column, when the entity has one.
    if let Some(v) = req
        .headers()
        .get("if-match")
        .and_then(|h| h.to_str().ok())
        .map(|h| h.trim().trim_start_matches("W/").trim_matches('"').to_string())
        && !v.is_empty()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("__expected_version__".to_string(), Value::String(v));
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
            claims: if auth_required { Some(claims.clone()) } else { None },
        };

        // Acknowledge only after the job is durably in Redis (see create service).
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

            // Metadata Check (ahead of null/FK handling so col.nullable/type_data are available)
            let Some(col) = table_schema.columns.iter().find(|c| c.name == clean_column) else {
                return bad_request(format!(
                    "Unknown column '{}' for route '{}'",
                    clean_column, route
                ));
            };

            // Explicit JSON null: clear a nullable column, reject on NOT NULL.
            // (Empty strings from form posts keep the old "skip" behaviour below.)
            if raw_value.is_null() {
                if !col.nullable {
                    return bad_request(format!(
                        "Field '{}' cannot be null (column is NOT NULL)",
                        clean_column
                    ));
                }
                update_fields.push((clean_column.to_string(), InsertValue::Param(DbParam::Null)));
                patch_fields.insert(clean_column.to_string(), Value::Null);
                continue;
            }

            // Typed string extraction.
            let str_value: String = match raw_value {
                Value::String(s) => s.trim().to_string(),
                v => v.to_string().trim().to_string(),
            };

            if str_value.is_empty() {
                continue;
            }

            // Declarative column rules (enum/pattern/min/max/length/email/url).
            if let Err(e) = field_rules::validate_field_message(col, raw_value) {
                return bad_request(e);
            }

            // Boolean columns: bind a real boolean (bool/boolean) or 0/1 (bit/tinyint(1)).
            if is_bool_type(&col.type_data) {
                let dbparam = dbparam_from_value_and_type(raw_value, &col.type_data);
                if matches!(dbparam, DbParam::Str(_)) {
                    return bad_request(format!(
                        "Invalid field '{}': must be a boolean (true/false, 1/0, yes/no)",
                        clean_column
                    ));
                }
                update_fields.push((clean_column.to_string(), InsertValue::Param(dbparam)));
                patch_fields.insert(
                    clean_column.to_string(),
                    json_value_from_value_and_type(raw_value, &col.type_data),
                );
                continue;
            }

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
    let mut prepared_details: Vec<data_update_repo::PreparedDetailUpdateBatch> = Vec::new();
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

            let registry = crate::registry::current();
            let target_schema_opt = registry.schemas.get(&detail.target_table);

            if let Some(detail_schema) = target_schema_opt {
                let strategy = data_update_repo::DetailUpdateStrategy::from_opt(
                    detail.update_strategy.as_deref(),
                );
                let delete_missing = detail.delete_missing.unwrap_or(true);

                let pk_column = detail_schema
                    .primary_key
                    .columns
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "id".to_string());
                let pk_col_def = detail_schema.columns.iter().find(|c| c.name == pk_column);
                let pk_type_data = pk_col_def
                    .map(|c| c.type_data.clone())
                    .unwrap_or_else(|| "int".to_string());
                let pk_auto_increment = pk_col_def
                    .map(|c| c.auto_increment)
                    .unwrap_or(false);

                let has_created_at = detail_schema.columns.iter().any(|c| c.name == "created_at");
                let created_by_col = detail_schema.columns.iter().find(|c| c.name == "created_by_id");
                let has_updated_at = detail_schema.columns.iter().any(|c| c.name == "updated_at");
                let updated_by_col = detail_schema.columns.iter().find(|c| c.name == "updated_by_id");
                let has_deleted_at = detail_schema.columns.iter().any(|c| c.name == "deleted_at");
                let deleted_by_col = detail_schema.columns.iter().find(|c| c.name == "deleted_by_id");
                let has_deleted_by_id = deleted_by_col.is_some();
                let deleted_by_type = deleted_by_col
                    .map(|c| c.type_data.clone())
                    .unwrap_or_else(|| "int".to_string());

                let effective_type_delete = if !detail_schema.del.type_delete.is_empty() {
                    detail_schema.del.type_delete.as_str()
                } else if !table_schema.del.type_delete.is_empty() {
                    table_schema.del.type_delete.as_str()
                } else {
                    "hard"
                };
                let use_soft_delete =
                    effective_type_delete.eq_ignore_ascii_case("soft") && has_deleted_at;

                let detail_whitelist: std::collections::HashSet<&str> = detail
                    .columns
                    .iter()
                    .map(|s| s.trim().trim_end_matches('*'))
                    .filter(|s| !s.is_empty())
                    .collect();

                let detail_post_cols: std::collections::HashSet<&str> = detail_schema
                    .post
                    .columns
                    .iter()
                    .map(|s| s.trim_end_matches('*'))
                    .collect();

                let detail_put_cols: std::collections::HashSet<&str> = detail_schema
                    .put
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
                            && (detail_whitelist.is_empty() || detail_whitelist.contains(c.name.as_str()))
                    })
                    .map(|c| c.name.clone())
                    .collect();

                if !detail_insert_cols.contains(&detail.foreign_key_column) {
                    detail_insert_cols.insert(0, detail.foreign_key_column.clone());
                }
                if has_created_at && !detail_insert_cols.contains(&"created_at".to_string()) {
                    detail_insert_cols.push("created_at".to_string());
                }
                if created_by_col.is_some() && !detail_insert_cols.contains(&"created_by_id".to_string()) {
                    detail_insert_cols.push("created_by_id".to_string());
                }

                let detail_updatable_cols: Vec<String> = detail_schema
                    .columns
                    .iter()
                    .filter(|c| {
                        !c.auto_increment
                            && c.name != pk_column
                            && c.name != detail.foreign_key_column
                            && !skip_columns.contains(c.name.as_str())
                            && (if !detail_put_cols.is_empty() {
                                detail_put_cols.contains(c.name.as_str())
                            } else if !detail_post_cols.is_empty() {
                                detail_post_cols.contains(c.name.as_str())
                            } else {
                                true
                            })
                            && (detail_whitelist.is_empty() || detail_whitelist.contains(c.name.as_str()))
                    })
                    .map(|c| c.name.clone())
                    .collect();

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

                let mut insert_rows: Vec<Vec<InsertValue>> = Vec::new();
                let mut update_items: Vec<data_update_repo::PreparedDetailUpdateItem> = Vec::new();
                let mut seen_update_pks: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                for (idx, item) in items_arr.iter().enumerate() {
                    let Some(item_obj) = item.as_object() else {
                        return bad_request(format!("Detail item #{} in '{}' must be an object", idx + 1, detail.field));
                    };
                    if let Err(e) = field_rules::validate_body(&detail_schema.columns, item_obj) {
                        return bad_request(format!("Item #{} in '{}': {}", idx + 1, detail.field, e));
                    }

                    // Extract PK if upsert strategy is active and item supplies a non-empty PK
                    let upsert_pk_opt = if strategy == data_update_repo::DetailUpdateStrategy::Upsert {
                        item_obj.get(&pk_column).and_then(|v| match v {
                            Value::Null => None,
                            Value::String(s) => {
                                let t = s.trim();
                                if t.is_empty() { None } else { Some(t.to_string()) }
                            }
                            Value::Number(n) => Some(n.to_string()),
                            other => {
                                let s = other.to_string();
                                if s.is_empty() { None } else { Some(s) }
                            }
                        })
                    } else {
                        None
                    };

                    // Helper closure to build a full INSERT row for this item
                    let build_insert_row = |fk_checks: &mut Vec<(String, String, String, String, String)>| -> Result<Vec<InsertValue>, HttpResponse> {
                        let mut row_values: Vec<InsertValue> = Vec::with_capacity(detail_insert_cols.len());
                        for col_name in &detail_insert_cols {
                            if col_name == &detail.foreign_key_column {
                                row_values.push(InsertValue::Param(DbParam::Null));
                                continue;
                            }
                            if col_name == "created_at" {
                                row_values.push(InsertValue::Raw(state.query_converter.datetime_now.clone()));
                                continue;
                            }
                            if col_name == "created_by_id" {
                                let td = created_by_col.map(|c| c.type_data.as_str()).unwrap_or("int");
                                row_values.push(InsertValue::Param(dbparam_from_str_and_type(&claims.id, td)));
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
                            let bool_col = col_def_opt.and_then(|c| bool_kind(&c.type_data));
                            let is_int = bool_col.is_none() && type_lower.contains("int");
                            let is_float = type_lower.contains("float")
                                || type_lower.contains("double")
                                || type_lower.contains("decimal")
                                || type_lower.contains("numeric")
                                || type_lower.contains("money")
                                || type_lower.contains("real");

                            if bool_col.is_some() {
                                let td = col_def_opt.map(|c| c.type_data.as_str()).unwrap_or("bool");
                                let raw = raw_val.cloned().unwrap_or(Value::Null);
                                let param = if str_val.is_empty() { DbParam::Null } else { dbparam_from_value_and_type(&raw, td) };
                                if matches!(param, DbParam::Str(_)) {
                                    return Err(bad_request(format!(
                                        "Item #{} in '{}': Invalid field '{}': must be a boolean (true/false, 1/0, yes/no)",
                                        idx + 1, detail.field, col_name
                                    )));
                                }
                                row_values.push(InsertValue::Param(param));
                            } else if is_int {
                                if let Ok(n) = value_for_db.parse::<i64>() {
                                    row_values.push(InsertValue::Param(DbParam::I64(n)));
                                } else {
                                    row_values.push(InsertValue::Param(DbParam::Null));
                                }
                            } else if is_float {
                                if let Ok(f) = value_for_db.parse::<f64>() {
                                    row_values.push(InsertValue::Param(DbParam::F64(f)));
                                } else {
                                    row_values.push(InsertValue::Param(DbParam::Null));
                                }
                            } else if matches!(raw_val, Some(Value::Null))
                                && col_def_opt.map(|c| c.nullable).unwrap_or(false)
                            {
                                row_values.push(InsertValue::Param(DbParam::Null));
                            } else {
                                row_values.push(InsertValue::Param(DbParam::Str(value_for_db)));
                            }
                        }
                        Ok(row_values)
                    };

                    if let Some(pk_raw) = upsert_pk_opt {
                        if !seen_update_pks.insert(pk_raw.clone()) {
                            return bad_request(format!(
                                "Duplicate detail {} '{}' in '{}'",
                                pk_column, pk_raw, detail.field
                            ));
                        }

                        let mut set_fields: Vec<(String, InsertValue)> = Vec::new();
                        for col_name in &detail_updatable_cols {
                            let Some(raw_val) = item_obj.get(col_name) else {
                                continue;
                            };
                            let Some(col_def) = detail_schema.columns.iter().find(|c| &c.name == col_name) else {
                                continue;
                            };

                            if raw_val.is_null() {
                                if !col_def.nullable {
                                    return bad_request(format!(
                                        "Item #{} in '{}': Field '{}' cannot be null (column is NOT NULL)",
                                        idx + 1, detail.field, col_name
                                    ));
                                }
                                set_fields.push((col_name.clone(), InsertValue::Param(DbParam::Null)));
                                continue;
                            }

                            let str_val = match raw_val {
                                Value::String(s) => s.trim().to_string(),
                                v => v.to_string().trim().to_string(),
                            };

                            if !str_val.is_empty() {
                                for fk in &detail_schema.foreign_keys {
                                    if &fk.column == col_name {
                                        fk_checks.push((
                                            col_name.clone(),
                                            fk.reference_table.clone(),
                                            fk.reference_column.clone(),
                                            str_val.clone(),
                                            col_def.type_data.clone(),
                                        ));
                                    }
                                }
                            }

                            if is_bool_type(&col_def.type_data) {
                                let param = dbparam_from_value_and_type(raw_val, &col_def.type_data);
                                if matches!(param, DbParam::Str(_)) {
                                    return bad_request(format!(
                                        "Item #{} in '{}': Invalid field '{}': must be a boolean (true/false, 1/0, yes/no)",
                                        idx + 1, detail.field, col_name
                                    ));
                                }
                                set_fields.push((col_name.clone(), InsertValue::Param(param)));
                                continue;
                            }

                            let value_for_db = if col_def.encrypt && !str_val.is_empty() && !is_encrypted_string(&str_val) {
                                encrypt(state.encrypt_key.clone(), str_val.clone())
                            } else {
                                str_val.clone()
                            };
                            let param = dbparam_from_str_and_type(&value_for_db, &col_def.type_data);
                            set_fields.push((col_name.clone(), InsertValue::Param(param)));
                        }

                        if !set_fields.is_empty() {
                            if has_updated_at {
                                set_fields.push((
                                    "updated_at".to_string(),
                                    InsertValue::Raw(state.query_converter.datetime_now.clone()),
                                ));
                            }
                            if let Some(upd_by) = updated_by_col {
                                set_fields.push((
                                    "updated_by_id".to_string(),
                                    InsertValue::Param(dbparam_from_str_and_type(&claims.id, &upd_by.type_data)),
                                ));
                            }
                        }

                        let fallback_insert_row = if !pk_auto_increment {
                            match build_insert_row(&mut fk_checks) {
                                Ok(r) => Some(r),
                                Err(resp) => return resp,
                            }
                        } else {
                            None
                        };

                        let pk_param = dbparam_from_str_and_type(&pk_raw, &pk_type_data);
                        update_items.push(data_update_repo::PreparedDetailUpdateItem {
                            pk_raw,
                            pk_param,
                            set_fields,
                            fallback_insert_row,
                        });
                    } else {
                        match build_insert_row(&mut fk_checks) {
                            Ok(r) => insert_rows.push(r),
                            Err(resp) => return resp,
                        }
                    }
                }

                prepared_details.push(data_update_repo::PreparedDetailUpdateBatch {
                    field: detail.field.clone(),
                    target_table: detail.target_table.clone(),
                    foreign_key_column: detail.foreign_key_column.clone(),
                    parent_key_column: detail.parent_key_column.clone(),
                    fk_type_data,
                    pk_column,
                    pk_type_data,
                    pk_auto_increment,
                    strategy,
                    delete_missing,
                    use_soft_delete,
                    has_deleted_at,
                    has_deleted_by_id,
                    deleted_by_type,
                    insert_columns: detail_insert_cols,
                    fk_index,
                    insert_rows,
                    update_items,
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
        actor_id_opt.as_deref(),
        Some(claims.rl.as_str()),
    )
    .await
    {
        Ok((msg, count, mut updated_data)) => {
            // Invalidate cached GET pages (L1 + L2) in O(1) via the route version.
            crate::database::state::invalidate_route(state, route).await;
            for detail in &table_schema.details {
                if !detail.target_table.is_empty() && detail.target_table != route {
                    crate::database::state::invalidate_route(state, &detail.target_table).await;
                }
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
        Err(e) if e.starts_with(data_update_repo::NOT_FOUND_PREFIX) => {
            HttpResponse::NotFound().json(web_err(e))
        }
        Err(e) if e.starts_with(data_update_repo::CONFLICT_PREFIX) => {
            HttpResponse::Conflict().json(web_err(e))
        }
        Err(e) => server_error(e),
    }
}
