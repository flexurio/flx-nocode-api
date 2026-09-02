use crate::AppState;
use crate::auth::{check_access, get_user_info_from_token};
use crate::helpers::{find_column_match, get_client_ip};
use crate::model::{Column, TableSchema, WebResponse};
use actix_web::web;
use serde_json::Value;
use std::sync::Arc;
// use crate::log::log_output;
use super::web_err as err;
use crate::crypt::{encrypt, is_encrypted_string};
use crate::database::state::DbParam;
use crate::nocode::repositories::data_create_repo;
use crate::storage::sql_store::InsertValue;
use chrono::Local;
use std::collections::HashSet;

// Build (InsertValue, json) pair for the actor id, respecting the audit column type.
fn audit_actor_value(col: &Column, actor_id: &str) -> (InsertValue, Value) {
    let t = col.type_data.to_lowercase();
    if t.contains("int") {
        if let Ok(n) = actor_id.parse::<i64>() {
            return (InsertValue::Param(DbParam::I64(n)), serde_json::json!(n));
        }
    } else if (t.contains("float") || t.contains("decimal"))
        && let Ok(f) = actor_id.parse::<f64>()
    {
        return (InsertValue::Param(DbParam::F64(f)), serde_json::json!(f));
    }
    (
        InsertValue::Param(DbParam::Str(actor_id.to_string())),
        Value::String(actor_id.to_string()),
    )
}

#[allow(clippy::collapsible_if)]
pub async fn process_insert_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    mut body: Value,
    req: &actix_web::HttpRequest,
) -> Result<WebResponse, WebResponse> {
    // 1. Auth Check (Early)
    let mut actor_id_opt: Option<String> = None;

    if state.require_auth && !state.route_publics.contains(route) {
        let claims =
            get_user_info_from_token(req, state.clone()).map_err(|_| err("Invalid token"))?;
        check_access(&claims, req).map_err(|e| err(format!("Unauthorized: {}", e)))?;
        actor_id_opt = Some(claims.id.clone());
    }

    // 2. Handle Write Queue
    let isqueue = parameters
        .as_object()
        .and_then(|map| map.get("isqueue"))
        .map(|v| *v == Value::Bool(true) || *v == Value::String("true".to_string()))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
        // Add created_by_id if needed for queue
        if let Some(actor_id) = &actor_id_opt {
            if let Some(col) = table_schema
                .columns
                .iter()
                .find(|c| c.name == "created_by_id")
            {
                if let Some(map) = body.as_object_mut() {
                    let (_, json_val) = audit_actor_value(col, actor_id);
                    map.insert("created_by_id".into(), json_val);
                }
            }
        }

        let job = crate::nocode::consumer::WriteJob {
            route: route.to_string(),
            op: crate::nocode::consumer::WriteOpKind::Post,
            body,
            headers: vec![],
            enqueued_at: chrono::Utc::now().to_rfc3339(),
            actor_id: actor_id_opt.clone(),
        };

        if state.write_queue_fast_ack {
            crate::nocode::consumer::enqueue_job_background(job, "CREATE-HANDLER");
            return Ok(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        } else {
            return match crate::nocode::consumer::enqueue_job(&job).await {
                Ok(_) => Ok(WebResponse {
                    success: true,
                    message: "Enqueued".to_string(),
                    total_data: 0,
                    data: Value::Null,
                }),
                Err(e) => Err(err(format!("Queue error: {}", e))),
            };
        }
    }

    // 3. Validate Table Existence
    if table_schema.table.is_empty() {
        return Err(err(format!(
            "Entity {} on folder config/{}.json not found",
            route, route
        )));
    }

    // Pre-compute the cleaned post.columns names once (strip trailing '*').
    let post_column_names: HashSet<&str> = table_schema
        .post
        .columns
        .iter()
        .map(|s| s.trim_end_matches('*'))
        .collect();
    let post_columns_vec: Vec<&str> = post_column_names.iter().copied().collect();

    // 4. Validate Required Fields
    for post_col in &table_schema.post.columns {
        // Check if column is marked as required with *
        let is_required_marker = post_col.ends_with('*');
        let clean_col_name = if is_required_marker {
            post_col.trim_end_matches('*')
        } else {
            post_col.as_str()
        };

        let Some(col_def) = table_schema
            .columns
            .iter()
            .find(|c| c.name == clean_col_name)
        else {
            continue;
        };

        // get data type
        let data_type = col_def.type_data.to_ascii_lowercase();
        let is_datetime = data_type.contains("datetime")
            || data_type.contains("timestamp")
            || data_type.contains("date");

        // convert empty datetime -> null
        if is_datetime {
            if let Some(value) = body.get_mut(clean_col_name) {
                if let Some(s) = value.as_str() {
                    if s.trim().is_empty() || s.trim().eq_ignore_ascii_case("null") {
                        *value = Value::Null;
                    }
                }
            }
        }
        // Check if field is mandatory: either marked with * or column is not nullable and not auto_increment
        let is_mandatory = is_required_marker || (!col_def.nullable && !col_def.auto_increment);

        if is_mandatory {
            let present = match body.get(clean_col_name) {
                None | Some(Value::Null) => false,
                Some(Value::String(s)) => {
                    let t = s.trim();
                    !t.is_empty() && !t.eq_ignore_ascii_case("null")
                }
                Some(_) => true,
            };

            if !present {
                return Err(err(format!("Missing required field: {}", clean_col_name)));
            }
        }
    }

    // 5. Prepare Logic (Filter Columns, Encrypt, Build Insert Lists)
    let skip_columns: HashSet<&str> = [
        "created_at",
        "created_by_id",
        "updated_at",
        "updated_by_id",
        "deleted_at",
        "deleted_by_id",
    ]
    .iter()
    .copied()
    .collect();

    let mut filtered_columns: Vec<&Column> = table_schema
        .columns
        .iter()
        .filter(|col| {
            !col.auto_increment
                && !skip_columns.contains(col.name.as_str())
                && post_column_names.contains(col.name.as_str())
        })
        .collect();

    let mut insert_columns: Vec<&str> = filtered_columns.iter().map(|c| c.name.as_str()).collect();

    // explicit id check
    if let Some(col) = table_schema
        .columns
        .iter()
        .find(|c| c.name == "id" && !c.auto_increment)
    {
        if !post_column_names.contains("id") {
            insert_columns.push("id");
            filtered_columns.push(col);
        }
    }

    // Custom id generation tokens (hoisted out of the per-column loop).
    let function_id_split: Vec<String> = table_schema
        .columns
        .iter()
        .find(|c| c.name == "id" && !c.function.is_empty())
        .map(|c| c.function.split('/').map(|s| s.to_string()).collect())
        .unwrap_or_default();

    // Params collecting
    let mut fk_checks: Vec<(String, String, String, String, String)> =
        Vec::with_capacity(filtered_columns.len());
    let mut insert_fields: Vec<(String, InsertValue)> =
        Vec::with_capacity(filtered_columns.len() + 3);
    let mut doc_map = serde_json::Map::with_capacity(filtered_columns.len() + 3);

    // Loop through filtered columns to prepare header data
    for col in filtered_columns.iter() {
        let mut isformula = false;
        let (exists, matched_string) = find_column_match(&post_columns_vec, &col.name);

        if exists && col.name != "id" {
            let string_formula = matched_string.unwrap_or("").to_string();
            if string_formula.contains('=') {
                isformula = true;
                let rhs = string_formula.replace(&format!("{}=", col.name), "");
                let (frag, params) = build_formula_value_service(&rhs, &body);
                insert_fields.push((
                    col.name.clone(),
                    InsertValue::RawWithParams { sql: frag, params },
                ));
            }
        }

        // Skip if handled as formula, or if id is generated via function (handled in repo).
        if isformula || (col.name == "id" && !col.function.is_empty()) {
            continue;
        }

        let raw_value = body.get(&col.name);
        let type_lower = col.type_data.to_lowercase();
        let is_datetime = type_lower.contains("datetime")
            || type_lower.contains("timestamp")
            || type_lower.contains("date");
        let is_int = type_lower.contains("int");
        let is_float = type_lower.contains("float") || type_lower.contains("decimal");

        // Typed string extraction (avoids "null" leaking from Value::Null via to_string()).
        let str_value: String = match raw_value {
            None | Some(Value::Null) => String::new(),
            Some(Value::String(s)) => s.trim().to_string(),
            Some(v) => v.to_string().trim().to_string(),
        };

        // Datetime null/empty -> bind NULL.
        if is_datetime && (str_value.is_empty() || str_value.eq_ignore_ascii_case("null")) {
            doc_map.insert(col.name.clone(), Value::Null);
            insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Null)));
            continue;
        }

        // FK Checks (only when we have a real value).
        if !str_value.is_empty() {
            for fk in table_schema.foreign_keys.iter() {
                if fk.column == col.name {
                    fk_checks.push((
                        col.name.clone(),
                        fk.reference_table.clone(),
                        fk.reference_column.clone(),
                        str_value.clone(),
                        col.type_data.clone(),
                    ));
                }
            }
        }

        // Encrypt (after FK check uses plaintext).
        let value_for_db =
            if col.encrypt && !str_value.is_empty() && !is_encrypted_string(&str_value) {
                encrypt(state.encrypt_key.clone(), str_value.clone())
            } else {
                str_value.clone()
            };

        // Bind based on column type.
        if is_int {
            if let Ok(n) = value_for_db.parse::<i64>() {
                doc_map.insert(col.name.clone(), serde_json::json!(n));
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::I64(n))));
            } else if value_for_db.is_empty() && col.nullable {
                doc_map.insert(col.name.clone(), Value::Null);
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Null)));
            } else {
                doc_map.insert(
                    col.name.clone(),
                    raw_value.cloned().unwrap_or(Value::String(String::new())),
                );
                insert_fields.push((
                    col.name.clone(),
                    InsertValue::Param(DbParam::Str(value_for_db)),
                ));
            }
        } else if is_float {
            if let Ok(f) = value_for_db.parse::<f64>() {
                doc_map.insert(col.name.clone(), serde_json::json!(f));
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::F64(f))));
            } else if value_for_db.is_empty() && col.nullable {
                doc_map.insert(col.name.clone(), Value::Null);
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Null)));
            } else {
                doc_map.insert(
                    col.name.clone(),
                    raw_value.cloned().unwrap_or(Value::String(String::new())),
                );
                insert_fields.push((
                    col.name.clone(),
                    InsertValue::Param(DbParam::Str(value_for_db)),
                ));
            }
        } else {
            doc_map.insert(
                col.name.clone(),
                raw_value.cloned().unwrap_or(Value::String(String::new())),
            );
            insert_fields.push((
                col.name.clone(),
                InsertValue::Param(DbParam::Str(value_for_db)),
            ));
        }
    }

    // Inject created_by_id when actor is known and the column exists on the table.
    if let Some(actor_id) = &actor_id_opt {
        if let Some(col) = table_schema
            .columns
            .iter()
            .find(|c| c.name == "created_by_id")
        {
            if !doc_map.contains_key("created_by_id") {
                let (insert_val, json_val) = audit_actor_value(col, actor_id);
                doc_map.insert("created_by_id".into(), json_val);
                insert_fields.push(("created_by_id".into(), insert_val));
                if !insert_columns.contains(&"created_by_id") {
                    insert_columns.push("created_by_id");
                }
            }
        }
    }

    // 6. In-Memory Pre-Processing of Details (BEFORE starting DB transaction)
    let mut prepared_details: Vec<data_create_repo::PreparedDetailBatch> = Vec::new();

    for detail in &table_schema.details {
        if detail.field.is_empty() || detail.target_table.is_empty() {
            continue;
        }

        if let Some(items_val) = body.get(&detail.field) {
            let items_arr = match items_val {
                Value::Array(arr) => arr,
                Value::Null => continue,
                _ => return Err(err(format!("Field '{}' must be an array of objects", detail.field))),
            };

            let target_schema_opt = crate::config::SCHEMAS.0.get(&detail.target_table);

            if let Some(detail_schema) = target_schema_opt {
                let detail_post_cols: HashSet<&str> = detail_schema
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
                    let item_obj = item.as_object().ok_or_else(|| {
                        err(format!(
                            "Detail item #{} in '{}' must be a JSON object",
                            idx + 1,
                            detail.field
                        ))
                    })?;

                    // Validate detail item required fields
                    for post_col in &detail_schema.post.columns {
                        let is_required_marker = post_col.ends_with('*');
                        let clean_col_name = if is_required_marker {
                            post_col.trim_end_matches('*')
                        } else {
                            post_col.as_str()
                        };

                        if clean_col_name == detail.foreign_key_column {
                            continue; // FK is injected by backend
                        }

                        let Some(col_def) = detail_schema
                            .columns
                            .iter()
                            .find(|c| c.name == clean_col_name)
                        else {
                            continue;
                        };

                        let is_mandatory = is_required_marker
                            || (!col_def.nullable && !col_def.auto_increment);

                        if is_mandatory {
                            let present = match item_obj.get(clean_col_name) {
                                None | Some(Value::Null) => false,
                                Some(Value::String(s)) => {
                                    let t = s.trim();
                                    !t.is_empty() && !t.eq_ignore_ascii_case("null")
                                }
                                Some(_) => true,
                            };
                            if !present {
                                return Err(err(format!(
                                    "Item #{}: Missing required field '{}' in detail '{}'",
                                    idx + 1,
                                    clean_col_name,
                                    detail.field
                                )));
                            }
                        }
                    }

                    let mut row_values: Vec<InsertValue> =
                        Vec::with_capacity(detail_insert_cols.len());
                    let mut item_resp_map =
                        serde_json::Map::with_capacity(detail_insert_cols.len());

                    for col_name in &detail_insert_cols {
                        if col_name == &detail.foreign_key_column {
                            row_values.push(InsertValue::Param(DbParam::Null));
                            continue;
                        }

                        let col_def_opt = detail_schema
                            .columns
                            .iter()
                            .find(|c| &c.name == col_name);

                        let raw_val = item_obj.get(col_name);
                        let str_val = match raw_val {
                            None | Some(Value::Null) => String::new(),
                            Some(Value::String(s)) => s.trim().to_string(),
                            Some(v) => v.to_string().trim().to_string(),
                        };

                        // FK Check for detail item
                        if !str_val.is_empty() {
                            for fk in &detail_schema.foreign_keys {
                                if &fk.column == col_name {
                                    let td = col_def_opt
                                        .map(|c| c.type_data.clone())
                                        .unwrap_or_else(|| "text".to_string());
                                    fk_checks.push((
                                        col_name.clone(),
                                        fk.reference_table.clone(),
                                        fk.reference_column.clone(),
                                        str_val.clone(),
                                        td,
                                    ));
                                }
                            }
                        }

                        let encrypt_col = col_def_opt.map(|c| c.encrypt).unwrap_or(false);
                        let value_for_db = if encrypt_col
                            && !str_val.is_empty()
                            && !is_encrypted_string(&str_val)
                        {
                            encrypt(state.encrypt_key.clone(), str_val.clone())
                        } else {
                            str_val.clone()
                        };

                        let type_lower = col_def_opt
                            .map(|c| c.type_data.to_lowercase())
                            .unwrap_or_default();
                        let is_int = type_lower.contains("int");
                        let is_float =
                            type_lower.contains("float") || type_lower.contains("decimal");

                        if is_int {
                            if let Ok(n) = value_for_db.parse::<i64>() {
                                item_resp_map.insert(col_name.clone(), serde_json::json!(n));
                                row_values.push(InsertValue::Param(DbParam::I64(n)));
                            } else {
                                item_resp_map.insert(
                                    col_name.clone(),
                                    raw_val.cloned().unwrap_or(Value::Null),
                                );
                                row_values.push(InsertValue::Param(DbParam::Null));
                            }
                        } else if is_float {
                            if let Ok(f) = value_for_db.parse::<f64>() {
                                item_resp_map.insert(col_name.clone(), serde_json::json!(f));
                                row_values.push(InsertValue::Param(DbParam::F64(f)));
                            } else {
                                item_resp_map.insert(
                                    col_name.clone(),
                                    raw_val.cloned().unwrap_or(Value::Null),
                                );
                                row_values.push(InsertValue::Param(DbParam::Null));
                            }
                        } else {
                            item_resp_map.insert(
                                col_name.clone(),
                                raw_val.cloned().unwrap_or(Value::String(String::new())),
                            );
                            row_values.push(InsertValue::Param(DbParam::Str(value_for_db)));
                        }
                    }

                    detail_rows.push(row_values);
                    detail_response_items.push(item_resp_map);
                }

                prepared_details.push(data_create_repo::PreparedDetailBatch {
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

    // 7. Execute Repository
    let auth_token = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    match data_create_repo::perform_insert(
        state,
        table_schema,
        &body,
        &insert_columns,
        &filtered_columns,
        insert_fields,
        doc_map,
        fk_checks,
        function_id_split,
        prepared_details,
        route,
        auth_token,
    )
    .await
    {
        Ok((msg, _count, inserted_data)) => {
            // Invalidate L1 in-memory cache and L2 Redis cache
            state.l1_cache.invalidate_all();
            if state.is_cachedb {
                let cache_prefix = crate::database::redis::build_key_prefix("public", route);
                let _ = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await;
            }

            // Audit Log
            if let Some(actor) = &actor_id_opt {
                let ip_opt = get_client_ip(req);

                crate::audit::write_audit(&crate::audit::AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: actor.clone(),
                    action: "POST",
                    route,
                    id: None,
                    ip: Some(ip_opt.as_str()),
                });
            }

            Ok(WebResponse {
                success: true,
                message: msg,
                total_data: 1,
                data: inserted_data,
            })
        }
        Err(e) => Err(err(e)),
    }
}

// Validation helper (duplicated from repo/helper logic because it needs body access)
fn build_formula_value_service(raw: &str, body: &Value) -> (String, Vec<DbParam>) {
    let mut sql = raw.to_string();
    let mut params: Vec<DbParam> = Vec::new();
    let exprs = crate::helpers::extract_expressions(&sql);
    for expr in exprs.into_iter() {
        let needle = format!("{{{}}}", expr);
        if expr.contains('[') {
            let sub = crate::database::state::convert_to_sql(&expr);
            sql = sql.replace(&needle, &sub);
        } else if let Some(stripped) = expr.strip_prefix("request.") {
            let val = body
                .get(stripped)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default();
            if let Ok(n) = val.parse::<i64>() {
                params.push(DbParam::I64(n));
            } else if let Ok(f) = val.parse::<f64>() {
                params.push(DbParam::F64(f));
            } else {
                params.push(DbParam::Str(val));
            }
            sql = sql.replace(&needle, "?");
        } else {
            sql = sql.replace(&needle, "");
        }
    }
    (sql, params)
}
