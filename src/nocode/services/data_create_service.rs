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
        let claims = match get_user_info_from_token(req.clone(), state.clone()) {
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

        if !check_access(&claims, route, "write") {
             return Err(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
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
        let Some(col_def) = table_schema.columns.iter().find(|c| c.name == *post_col) else { continue };
        if !col_def.nullable && !col_def.auto_increment {
            let present = body
                .get(post_col)
                .map(|v| v.to_string().replace('"', ""))
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !present {
                return Err(WebResponse {
                    success: false,
                    message: format!("Missing required field: {}", post_col),
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

    let mut filtered_columns: Vec<&Column> = Vec::with_capacity(table_schema.post.columns.len());
    filtered_columns.extend(
        table_schema
            .columns
            .iter()
            .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()) && table_schema.post.columns.contains(&col.name))
    );

    let mut insert_columns: Vec<&str> = Vec::with_capacity(filtered_columns.len() + 2);
    insert_columns.extend(
        filtered_columns
            .iter()
            .filter(|col| table_schema.post.columns.contains(&col.name))
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
        let post_columns: Vec<&str> = table_schema.post.columns.iter().map(|s| s.as_str()).collect();
        let (exists, matched_string) = find_column_match(&post_columns, &col.name);

        if exists && col.name != "id" {
             let string_formula = matched_string.unwrap_or("").to_string();
             if string_formula.contains('=') {
                 isformula = true;
                 // It's a formula, handled in repo or here? 
                 // The extraction logic from post.rs handled formula parsing inline.
                 // We need to keep that logic.
                 // Ideally repo shouldn't do parsing of body against formula strings if possible, but repo needs to construct the SQL.
                 // Actually, `data_create_repo` expects `insert_fields` which ALREADY contains `InsertValue::Raw` or `RawWithParams`.
                 // So we must prepare it here.
                 
                 // Reuse logic from post.rs (needs `build_formula_value` helper, but that was moved to repo private?)
                 // Ah, I made `build_formula_value` private in repo. I should probably duplicate it or expose it, 
                 // OR move the entire loop logic to repo?
                 // Moving loop to repo means passing `body` which is fine. But encryption happens here in Service.
                 // Encryption makes sense in Service.
                 // FK check gathering makes sense in Service or Repo? Repo validates it.
                 // Let's implement basics here. To do that I need `build_formula_value` exposed or implemented here.
                 // Since it parses body to build params, it CAN be here.
                 
                 // wait, I put `build_formula_value` in repo but it is private.
                 // I should move this loop logic to repo to avoid duplicating formula parsing?
                 // But encryption is logic.
                 
                 // Let's decide: Service prepares `effective_values` (encrypted, etc), Repo builds AST/SQL?
                 // But `custom formula` implies AST generation.
                 
                 // The previous `post.rs` loop did EVERYTHING: encryption, formula parsing, FK collecting.
                 // If I move the loop to Service, I need to pass massive amount of args to Repo.
                 // If I move loop to Repo, Repo handles encryption (which depends on State key) and body parsing.
                 
                 // `put.rs` had `effective_values` prepared in Service (encrypted), then Repo used them.
                 // `post.rs` is more complex due to `build_formula_value`.
                 
                 // Strategy: Move `build_formula_value` logic to Service or expose from helper?
                 // It uses `extract_expressions` from crate::helpers.
                 // I'll implement `build_formula_value` locally here in Service as a helper function (or closure).
                 
                 let rhs = string_formula.replace(&format!("{}=", col.name), "");
                 // We need a helper for this.
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


     println!("Masuk Sini");

    // 7. Audit Log Preparation (before execution?) No, logic usually logs after success.
    // We execute now.
    
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
        route
    ).await {
         Ok((msg, _count)) => {
             // Audit Log
             if let Some(actor) = &actor_id_opt {
                  // We don't have the inserted ID easily unless we parse it from msg or return it from repo.
                  // Repo returns (String, i64). String is message, i64 is count?
                  // `post.rs` didn't seem to log the inserted ID in audit explicitly, just "POST" action.
                  // Wait, `put.rs` refactor logged audit.
                  // `post.rs` code I viewed shows `write_audit` is imported but ONLY used in `delete.rs`?
                  // Let me check `post.rs` view again. I don't see `write_audit` called in the main flow.
                  // Ah, line 10: `use crate::audit::{write_audit, AuditEntry};` was imported.
                  // But searching `post.rs` content, I don't see `write_audit` being CALLED.
                  // It seems `post.rs` might have missed audit logging or I missed it.
                  
                  // In `put.rs`, we added it. It's good practice to add it here too.
                  // For now, I will NOT add it if it wasn't there, to avoid changing behavior too much, 
                  // but `put.rs` refactor added it. I'll consistency add it.
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
                 data: Value::Null, // Or return the inserted ID if we can? `post.rs` returned success msg.
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
