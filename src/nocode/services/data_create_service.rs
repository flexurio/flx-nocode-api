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
use crate::nocode::field_rules;
use crate::nocode::pk_utils::{bool_kind, parse_bool_value, BoolKind};
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

/// A column must be supplied on POST only when nothing else can fill it: NOT NULL, no
/// `default`, not auto-increment, no generator `function`. Explicit `*` markers in
/// `post.columns` are handled by the caller and always win.
fn column_is_mandatory(col: &Column) -> bool {
    !col.nullable && col.default.is_none() && !col.auto_increment && col.function.is_empty()
}

/// Presence check used for required fields: absent, JSON null, "" and "null" count as missing.
fn value_is_present(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::String(s)) => {
            let t = s.trim();
            !t.is_empty() && !t.eq_ignore_ascii_case("null")
        }
        Some(_) => true,
    }
}

/// `state_machine.initial` enforcement for one record about to be created.
///
/// Opt-in: does nothing when the entity has no `state_machine` or no `initial`.
/// Otherwise the status field is filled with `initial` when absent / null / empty,
/// and any other value is rejected, so a record can only enter the workflow at its
/// first state and must then pass every transition guard (`roles`, `actor_column`,
/// `not_same_as`). The error message contains "Invalid" so it maps to HTTP 400.
pub fn enforce_initial_state(
    table_schema: &TableSchema,
    record: &mut serde_json::Map<String, Value>,
) -> Result<(), String> {
    let Some(sm) = &table_schema.state_machine else { return Ok(()) };
    let Some(initial) = sm.initial.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let given = match record.get(&sm.field) {
        None | Some(Value::Null) => None,
        // "" and "null" count as absent, same as `value_is_present` (CSV/XLSX cells).
        Some(Value::String(s)) => {
            Some(s.trim().to_string()).filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("null"))
        }
        Some(v) => Some(v.to_string()),
    };
    match given {
        None => {
            record.insert(sm.field.clone(), Value::String(initial.to_string()));
            Ok(())
        }
        Some(v) if v == initial => Ok(()),
        Some(v) => Err(format!(
            "Invalid initial state on '{}': new records must start in '{}' (got '{}')",
            sm.field, initial, v
        )),
    }
}

/// Bind a boolean-like column (`bool`/`boolean` -> Bool, `bit`/`tinyint(1)` -> 0/1).
fn bind_bool(col: &Column, raw: &Value, kind: BoolKind) -> Result<(InsertValue, Value), WebResponse> {
    let Some(b) = parse_bool_value(raw) else {
        return Err(err(format!(
            "Invalid field '{}': must be a boolean (true/false, 1/0, yes/no)",
            col.name
        )));
    };
    Ok(match kind {
        BoolKind::Native => (InsertValue::Param(DbParam::Bool(b)), Value::Bool(b)),
        BoolKind::Int01 => (
            InsertValue::Param(DbParam::I64(b as i64)),
            serde_json::json!(b as i64),
        ),
    })
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
    let mut claims_opt: Option<crate::auth::Claims> = None;

    if state.require_auth && !state.is_public_route(route) {
        let claims =
            get_user_info_from_token(req, state.clone()).map_err(|_| err("Invalid token"))?;
        check_access(&claims, req).map_err(|e| err(format!("Unauthorized: {}", e)))?;
        // rules.json `allowed_fields`: reject keys this role may not write (before any injection).
        crate::auth::enforce_allowed_fields(&claims, req, &mut body)
            .map_err(|e| err(format!("Forbidden: {}", e)))?;
        actor_id_opt = Some(claims.id.clone());
        claims_opt = Some(claims);
    }

    // 1b. state_machine.initial: a new record must start in the initial state
    //     (checked before queueing so a rejected job never enters the queue).
    if let Some(map) = body.as_object_mut() {
        enforce_initial_state(table_schema, map).map_err(err)?;
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
            headers: crate::nocode::audit_trail::forwarded_headers(req),
            enqueued_at: chrono::Utc::now().to_rfc3339(),
            actor_id: actor_id_opt.clone(),
            claims: claims_opt.clone(),
        };

        // Acknowledge only after the job is durably in Redis. The old "fast ack"
        // replied before LPUSH, so an enqueue failure silently lost the write.
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
        // Mandatory when marked with `*`, or when the DB cannot fill it itself
        // (NOT NULL without default / auto_increment / generator function).
        let is_mandatory = is_required_marker || column_is_mandatory(col_def);

        if is_mandatory && !value_is_present(body.get(clean_col_name)) {
            return Err(err(format!("Missing required field: {}", clean_col_name)));
        }
    }

    // 4b. Declarative column rules (enum/pattern/min/max/length/email/url) on provided fields.
    if let Some(body_obj) = body.as_object() {
        field_rules::validate_body(&table_schema.columns, body_obj).map_err(err)?;
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

        // Absent from the body (or explicit null on a NOT NULL column that has a default):
        // leave the column out of the INSERT so the DB default applies. Required
        // columns were already enforced in step 4, so nothing mandatory is skipped here.
        let Some(raw_value_ref) = raw_value else {
            continue;
        };
        if raw_value_ref.is_null() && !col.nullable && col.default.is_some() {
            continue;
        }

        let type_lower = col.type_data.to_lowercase();
        let is_datetime = type_lower.contains("datetime")
            || type_lower.contains("timestamp")
            || type_lower.contains("date");
        let is_bool = bool_kind(&col.type_data);
        let is_int = is_bool.is_none() && type_lower.contains("int");
        let is_float = type_lower.contains("float")
            || type_lower.contains("double")
            || type_lower.contains("decimal")
            || type_lower.contains("numeric")
            || type_lower.contains("money")
            || type_lower.contains("real");

        // Typed string extraction (avoids "null" leaking from Value::Null via to_string()).
        let str_value: String = match raw_value {
            None | Some(Value::Null) => String::new(),
            Some(Value::String(s)) => s.trim().to_string(),
            Some(v) => v.to_string().trim().to_string(),
        };

        // Boolean columns: JSON true/false (or "yes"/"1"/...) bind as a real boolean / 0|1.
        if let Some(kind) = is_bool {
            if str_value.is_empty() {
                doc_map.insert(col.name.clone(), Value::Null);
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Null)));
                continue;
            }
            let (param, json_val) = bind_bool(col, raw_value_ref, kind)?;
            doc_map.insert(col.name.clone(), json_val);
            insert_fields.push((col.name.clone(), param));
            continue;
        }

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

            let registry = crate::registry::current();
            let target_schema_opt = registry.schemas.get(&detail.target_table);

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

                let has_created_at = detail_schema.columns.iter().any(|c| c.name == "created_at");
                let created_by_col = detail_schema.columns.iter().find(|c| c.name == "created_by_id");

                if !detail_insert_cols.contains(&detail.foreign_key_column) {
                    detail_insert_cols.insert(0, detail.foreign_key_column.clone());
                }
                if has_created_at && !detail_insert_cols.contains(&"created_at".to_string()) {
                    detail_insert_cols.push("created_at".to_string());
                }
                if created_by_col.is_some()
                    && actor_id_opt.is_some()
                    && !detail_insert_cols.contains(&"created_by_id".to_string())
                {
                    detail_insert_cols.push("created_by_id".to_string());
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

                        if is_mandatory && !value_is_present(item_obj.get(clean_col_name)) {
                            return Err(err(format!(
                                "Item #{}: Missing required field '{}' in detail '{}'",
                                idx + 1,
                                clean_col_name,
                                detail.field
                            )));
                        }
                    }

                    // Declarative column rules on the detail item's provided fields.
                    if let Err(e) = field_rules::validate_body(&detail_schema.columns, item_obj) {
                        return Err(err(format!("Item #{} in '{}': {}", idx + 1, detail.field, e)));
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
                        if col_name == "created_at" {
                            row_values.push(InsertValue::Raw(state.query_converter.datetime_now.clone()));
                            continue;
                        }
                        if col_name == "created_by_id"
                            && let (Some(col_def), Some(actor_id)) = (created_by_col, actor_id_opt.as_deref())
                        {
                            let (insert_val, json_val) = audit_actor_value(col_def, actor_id);
                            item_resp_map.insert(col_name.clone(), json_val);
                            row_values.push(insert_val);
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
                        let bool_col = col_def_opt.and_then(|c| bool_kind(&c.type_data));
                        let is_int = bool_col.is_none() && type_lower.contains("int");
                        let is_float = type_lower.contains("float")
                            || type_lower.contains("double")
                            || type_lower.contains("decimal")
                            || type_lower.contains("numeric")
                            || type_lower.contains("money")
                            || type_lower.contains("real");

                        if let (Some(col_def), Some(kind)) = (col_def_opt, bool_col) {
                            match raw_val {
                                Some(rv) if !str_val.is_empty() => {
                                    let (param, json_val) = bind_bool(col_def, rv, kind)?;
                                    item_resp_map.insert(col_name.clone(), json_val);
                                    row_values.push(param);
                                }
                                _ => {
                                    item_resp_map.insert(col_name.clone(), Value::Null);
                                    row_values.push(InsertValue::Param(DbParam::Null));
                                }
                            }
                        } else if is_int {
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

    let change_reason = crate::nocode::audit_trail::extract_change_reason(req);
    let request_id = crate::nocode::audit_trail::extract_request_id(req);
    let actor_custom = claims_opt
        .as_ref()
        .map(|c| c.cs.as_str())
        .filter(|s| !s.is_empty());
    let audit_ctx = crate::nocode::audit_trail::AuditContext {
        action: "POST",
        actor_id: actor_id_opt.as_deref().unwrap_or("0"),
        actor_custom,
        reason: change_reason.as_deref(),
        request_id: request_id.as_deref(),
    };

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
        claims_opt.as_ref(),
        audit_ctx,
    )
    .await
    {
        Ok((msg, _count, inserted_data)) => {
            // Invalidate cached GET pages (L1 + L2) in O(1) via the route version.
            crate::database::state::invalidate_route(state, route).await;

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

#[cfg(test)]
mod initial_state_tests {
    use super::enforce_initial_state;
    use crate::model::{StateMachine, TableSchema};
    use serde_json::{json, Map, Value};

    fn schema(initial: Option<&str>) -> TableSchema {
        TableSchema {
            table: "plant_capas".into(),
            state_machine: Some(StateMachine {
                field: "status".into(),
                initial: initial.map(str::to_string),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn rec(status: Option<Value>) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("code".into(), json!("CAPA-1"));
        if let Some(s) = status {
            m.insert("status".into(), s);
        }
        m
    }

    #[test]
    fn no_initial_or_no_state_machine_is_unchanged() {
        // Opt-in: entities without `initial` keep today's behaviour.
        let mut r = rec(Some(json!("IMPLEMENTATION")));
        assert_eq!(enforce_initial_state(&schema(None), &mut r), Ok(()));
        assert_eq!(r["status"], "IMPLEMENTATION");

        let mut r = rec(None);
        assert_eq!(enforce_initial_state(&TableSchema::default(), &mut r), Ok(()));
        assert!(!r.contains_key("status"));
    }

    #[test]
    fn missing_or_empty_status_is_filled() {
        let s = schema(Some("DRAFT"));
        for v in [None, Some(Value::Null), Some(json!("")), Some(json!("  ")), Some(json!("null"))] {
            let mut r = rec(v);
            assert_eq!(enforce_initial_state(&s, &mut r), Ok(()));
            assert_eq!(r["status"], "DRAFT");
        }
    }

    #[test]
    fn initial_is_accepted_and_other_states_rejected() {
        let s = schema(Some("DRAFT"));
        let mut r = rec(Some(json!(" DRAFT ")));
        assert_eq!(enforce_initial_state(&s, &mut r), Ok(()));

        let mut r = rec(Some(json!("IMPLEMENTATION")));
        let e = enforce_initial_state(&s, &mut r).unwrap_err();
        assert!(e.starts_with("Invalid initial state on 'status'"), "{}", e);
        // Must map to HTTP 400, not 500.
        assert_eq!(crate::error::ApiError::from_message(&e).status, actix_web::http::StatusCode::BAD_REQUEST);
        // Case matters: state names are exact.
        let mut r = rec(Some(json!("draft")));
        assert!(enforce_initial_state(&s, &mut r).is_err());
    }
}
