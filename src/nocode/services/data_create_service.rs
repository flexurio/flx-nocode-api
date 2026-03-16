use std::sync::Arc;
use actix_web::web;
use actix_multipart::Multipart;
use serde_json::Value;
use crate::AppState;
use crate::model::{TableSchema, WebResponse, Column};
use crate::auth::{check_access, get_user_info_from_token};
use crate::helpers::{multipart_to_json, get_client_ip, find_column_match};
// use crate::log::log_output;
use crate::crypt::{encrypt, is_encrypted_string};
use crate::storage::sql_store::InsertValue;
use crate::database::state::DbParam;
use crate::nocode::repositories::data_create_repo;
use std::collections::HashSet;
use chrono::Local;

#[allow(clippy::collapsible_if)]
pub async fn process_insert_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    multipart: Multipart,
    req: &actix_web::HttpRequest,
) -> Result<WebResponse, WebResponse> {

    // 1. Auth Check (Early)
    let mut actor_id_opt: Option<String> = None;

    if state.require_auth && !state.route_publics.contains(&route.to_string()) {
        let claims = match get_user_info_from_token(req, state.clone()) {
             Ok(c) => c,
             Err(_) => {
                return Err(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                     data: Value::Null,
                });
             }
        };

        if let Err(e) = check_access(&claims, req) {
             return Err(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
        actor_id_opt = Some(claims.id.clone());
    }

    // 2. Parse Multipart
    let mut body = match multipart_to_json(multipart).await {
         Ok(json) => json,
         Err(e) => {
             return Err(WebResponse {
                 success: false,
                 message: format!("Failed to parse multipart data: {}", e),
                 total_data: 0,
                 data: Value::Null,
             });
         }
    };

    // 3. Handle Write Queue
    let isqueue = parameters
        .as_object()
        .and_then(|map| map.get("isqueue"))
        .map(|v| *v == Value::Bool(true) || *v == Value::String("true".to_string()))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
         // Add created_by_id if needed for queue
         if let Some(actor_id) = &actor_id_opt {
             if let Some(col) = table_schema.columns.iter().find(|c| c.name == "created_by_id") {
                  if col.type_data.contains("int") {
                     if let Ok(n) = actor_id.parse::<i64>() {
                         if let Some(map) = body.as_object_mut() { map.insert("created_by_id".into(), serde_json::json!(n)); }
                     }
                  } else if col.type_data.contains("float") || col.type_data.contains("decimal") {
                        if let Ok(f) = actor_id.parse::<f64>() {
                             if let Some(map) = body.as_object_mut() { map.insert("created_by_id".into(), serde_json::json!(f)); }
                        }
                  } else if let Some(map) = body.as_object_mut() { 
                       map.insert("created_by_id".into(), serde_json::json!(actor_id.clone())); 
                  }
             }
         }

         let job = crate::nocode::consumer::WriteJob {
             route: route.to_string(),
             op: crate::nocode::consumer::WriteOpKind::Post,
             body,
             headers: vec![],
             enqueued_at: chrono::Utc::now().to_rfc3339(),
             actor_id: actor_id_opt.clone(), // Use already validated actor_id
         };

         if state.write_queue_fast_ack {
             tokio::spawn(async move {
                 let _ = crate::nocode::consumer::enqueue_job(&job).await;
             });
             return Ok(WebResponse {
                 success: true,
                 message: "Enqueued".to_string(),
                 total_data: 0,
                 data: Value::Null,
             });
         } else {
             match crate::nocode::consumer::enqueue_job(&job).await {
                 Ok(_) => return Ok(WebResponse {
                     success: true,
                     message: "Enqueued".to_string(),
                     total_data: 0,
                     data: Value::Null,
                 }),
                 Err(e) => return Err(WebResponse {
                     success: false,
                     message: format!("Queue error: {}", e),
                     total_data: 0,
                     data: Value::Null,
                 }),
             }
         }
    }
    
    // 4. Validate Table Existence
    if table_schema.table.is_empty() {
        return Err(WebResponse {
            success: false,
            message: format!("Entity {} on folder config/{}.json not found", route, route),
            total_data: 0,
            data: Value::Null,
        });
    }

    // 5. Validate Required Fields
    for post_col in &table_schema.post.columns {
        // Check if column is marked as required with *
        let is_required_marker = post_col.ends_with('*');
        let clean_col_name = if is_required_marker {
            post_col.trim_end_matches('*')
        } else {
            post_col.as_str()
        };

        let Some(col_def) = table_schema.columns.iter().find(|c| c.name == clean_col_name) else { continue };

        // Check if field is mandatory: either marked with * or column is not nullable and not auto_increment
        let is_mandatory = is_required_marker || (!col_def.nullable && !col_def.auto_increment);

        if is_mandatory {
            let present = body
                .get(clean_col_name)
                .map(|v| v.to_string().replace('"', ""))
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);

            if !present {
                return Err(WebResponse {
                    success: false,
                    message: format!("Missing required field: {}", clean_col_name),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // 6. Prepare Logic (Filter Columns, Encrypt, Build Insert Lists)
    let skip_columns: HashSet<&str> = [
        "created_at", "created_by_id", "updated_at", "updated_by_id", "deleted_at", "deleted_by_id",
    ].iter().cloned().collect();

    // Helper to check if column is in post.columns (strips *)
    let col_in_post_columns = |col_name: &str| -> bool {
        table_schema.post.columns.iter().any(|post_col| {
            let clean_name = post_col.trim_end_matches('*');
            clean_name == col_name
        })
    };

    let mut filtered_columns: Vec<&Column> = Vec::with_capacity(table_schema.post.columns.len());
    filtered_columns.extend(
        table_schema
            .columns
            .iter()
            .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()) && col_in_post_columns(&col.name))
    );

    let mut insert_columns: Vec<&str> = Vec::with_capacity(filtered_columns.len() + 2);
    insert_columns.extend(
        filtered_columns
            .iter()
            .filter(|col| col_in_post_columns(&col.name))
            .map(|col| col.name.as_str())
    );
     // explicit id check
    if let Some(col) = table_schema.columns.iter().find(|c| c.name == "id" && !c.auto_increment) {
        insert_columns.push("id");
        filtered_columns.push(col);
    }
    
    // Params collecting
    let mut fk_checks: Vec<(String, String, String, String)> = Vec::with_capacity(filtered_columns.len());
    let mut insert_fields: Vec<(String, InsertValue)> = Vec::with_capacity(filtered_columns.len() + 3);
    let mut doc_map = serde_json::Map::with_capacity(filtered_columns.len() + 3);
    let mut function_id_split: Vec<String> = Vec::new();

    // Loop through filtered columns to prepare data
     for col in filtered_columns.iter() {
        if col.auto_increment { continue; }

        let mut isformula = false;
        // Strip * marker from post.columns for comparison
        let post_columns: Vec<&str> = table_schema.post.columns.iter()
            .map(|s| s.trim_end_matches('*'))
            .collect();
        let (exists, matched_string) = find_column_match(&post_columns, &col.name);

        if exists && col.name != "id" {
             let string_formula = matched_string.unwrap_or("").to_string();
             if string_formula.contains('=') {
                 isformula = true;
                  let rhs = string_formula.replace(&format!("{}=", col.name), "");
                 let (frag, params) = build_formula_value_service(&rhs, &body);
                 insert_fields.push((col.name.clone(), InsertValue::RawWithParams { sql: frag, params }));
             }
        }
        
        if col.name == "id" && !col.function.is_empty() {
             function_id_split = col.function.split("/").map(|s| s.to_string()).collect();
        }

        if !isformula && (col.name != "id" || col.function.is_empty()) {
             let mut value = body
                .get(&col.name)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default();

             // FK Checks
             for fk in table_schema.foreign_keys.iter() {
                if fk.column == col.name && !value.is_empty() {
                    fk_checks.push((col.name.clone(), fk.reference_table.clone(), fk.reference_column.clone(), value.clone()));
                }
             }

             // Encrypt
             if col.encrypt {
                 if !is_encrypted_string(&value) {
                     value = encrypt(state.encrypt_key.clone(), value);
                 }
             }

             // Bind
             if col.type_data.contains("int") || col.type_data.contains("float") {
                 if let Ok(n) = value.parse::<i64>() {
                     doc_map.insert(col.name.clone(), serde_json::json!(n));
                     insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::I64(n))));
                 } else if let Ok(f) = value.parse::<f64>() {
                     doc_map.insert(col.name.clone(), serde_json::json!(f));
                     insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::F64(f))));
                 } else {
                     doc_map.insert(col.name.clone(), serde_json::json!(body.get(&col.name).cloned().unwrap_or(Value::String(String::new()))));
                     insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Str(value))));
                 }
             } else {
                 doc_map.insert(col.name.clone(), body.get(&col.name).cloned().unwrap_or(Value::String(String::new())));
                 insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Str(value))));
             }
        }
     }


    // 7. Audit Log Preparation (before execution?) No, logic usually logs after success.
    // We execute now.
    
    // Extract Authorization header to forward to API validation
    let auth_token = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    // Call Repo
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
        route,
        auth_token
    ).await {
         Ok((msg, _count, inserted_id)) => {
             // Audit Log
             if let Some(actor) = &actor_id_opt {
                  // Audit Log
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
                 data: inserted_id, 
             })
         },
         Err(e) => Err(WebResponse {
             success: false,
             message: e,
             total_data: 0,
             data: Value::Null,
         })
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
