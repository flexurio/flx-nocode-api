use actix_multipart::Multipart;
use actix_web::{web::Data, HttpResponse, Responder};
use serde_json::Value;
use std::collections::HashSet;

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::{DbParam},
    helpers::{extract_expressions, filter_table_schema, find_column_match, multipart_to_json},
    log::log_output,
    model::{Column, TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::storage::ast::{Query as Q, Filter as F};

// NCO-POST
pub async fn insert(
    state: Data<AppState>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let mut claims = Claims::default();
    if !state.route_publics.contains(&route) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
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


        if !check_access(&claims, &route, "write") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let mut function_id_split: Vec<String> = Vec::new();

    let body = match multipart_to_json(multipart).await {
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

    // Rate-limit per IP for mutations
    let ip_key = get_client_ip(&req);
    let limit_i64: i64 = std::env::var("RATE_LIMIT_MUTATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if limit_i64 > 0
        && !RL_WINDOW_MUTATE
            .check_and_increment(&format!("post:{}:{}", route, ip_key), limit_i64 as u32)
    {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::Null,
        });
    }
    // Per-user limit (for non-public routes only)
    if !state.route_publics.contains(&route) {
        let user_key = claims.id.clone();
        if limit_i64 > 0
            && !user_key.is_empty()
            && !RL_WINDOW_MUTATE
                .check_and_increment(&format!("post:{}:user:{}", route, user_key), limit_i64 as u32)
        {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".into(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // Generate SQL query INSERT to table in variable route, from data structure table in table_schemas
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

    // Validate required fields (non-nullable columns listed in post.columns)
    for post_col in &table_schema.post.columns {
        if let Some(col_def) = table_schema.columns.iter().find(|c| c.name == *post_col) {
            if !col_def.nullable && !col_def.auto_increment {
                let present = body
                    .get(post_col)
                    .map(|v| v.to_string().replace('"', ""))
                    .map(|s| !s.trim().is_empty())
                    .unwrap_or(false);
                if !present {
                    return HttpResponse::BadRequest().json(WebResponse {
                        success: false,
                        message: format!("Missing required field: {}", post_col),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        }
    }

    let skip_columns: HashSet<&str> = [
        "created_at",
        "created_by_id",
        "updated_at",
        "updated_by_id",
        "deleted_at",
        "deleted_by_id",
    ]
    .iter()
    .cloned()
    .collect();

    // **1️⃣ Filter kolom yang valid untuk INSERT**
    let filtered_columns: Vec<&Column> = table_schema
        .columns
        .iter()
        .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()))
        .collect();

    // **2️⃣ Buat daftar kolom untuk INSERT**
    let mut insert_columns: Vec<&str> = filtered_columns
        .iter()
        .map(|col| col.name.as_str())
        .collect();

    // Helper: build formula with placeholders and collect params
    fn build_formula_value(raw: &str, body: &Value) -> (String, Vec<DbParam>) {
        // raw like: "col=CONCAT({request.x}, {table[1].f})". Caller will strip "col=".
        let mut sql = raw.to_string();
        let mut params: Vec<DbParam> = Vec::new();
        let exprs = extract_expressions(&sql);
        for expr in exprs.into_iter() {
            let needle = format!("{{{}}}", expr);
            if expr.contains('[') {
                // table lookup, convert to SQL subselect
                let sub = crate::database::state::convert_to_sql(&expr);
                sql = sql.replace(&needle, &sub);
            } else if let Some(stripped) = expr.strip_prefix("request.") {
                // bind request value
                let val = body
                    .get(stripped)
                    .map(|v| v.to_string().replace('"', "").replace("null", ""))
                    .unwrap_or_default();
                // Infer numeric
                if let Ok(n) = val.parse::<i64>() {
                    params.push(DbParam::I64(n));
                } else if let Ok(f) = val.parse::<f64>() {
                    params.push(DbParam::F64(f));
                } else {
                    params.push(DbParam::Str(val));
                }
                sql = sql.replace(&needle, "?");
            } else {
                // unknown placeholder, drop
                sql = sql.replace(&needle, "");
            }
        }
        (sql, params)
    }

    // Param list for INSERT
    let mut bind_params: Vec<DbParam> = Vec::new();

    // **3️⃣ Buat daftar nilai untuk INSERT** (fragment SQL per kolom)
    let mut doc_map = serde_json::Map::new();
    // Collect explicit (column, value) pairs for dialect-aware insert builder
    let mut insert_fields: Vec<(String, InsertValue)> = Vec::new();
    for col in filtered_columns.iter() {
        if col.auto_increment {
            continue;
        }

        let mut isformula = false;
        let post_columns: Vec<&str> = table_schema
            .post
            .columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        let (exists, matched_string) = find_column_match(&post_columns, &col.name);

        if exists && col.name != "id" {
            let string_formula = matched_string.unwrap_or("").to_string();
            if string_formula.contains('=') {
                isformula = true;
                // Remove leading "col=" part
                let rhs = string_formula.replace(&format!("{}=", col.name), "");
                let (frag, params) = build_formula_value(&rhs, &body);
                if params.is_empty() {
                    insert_fields.push((col.name.clone(), InsertValue::Raw(frag)));
                } else {
                    // Raw fragment with its own params (will be rebound per dialect later)
                    insert_fields.push((col.name.clone(), InsertValue::RawWithParams { sql: frag, params: params.clone() }));
                }
            }
        }

        if col.name == "id" && !col.function.is_empty() {
            function_id_split = col.function.split("/").map(|s| s.to_string()).collect();
        }

        if !isformula && (col.name != "id" || col.function.is_empty()) {
            // Ambil dari body dan bind sebagai param
            let mut value = body
                .get(&col.name)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default();

            // check if col.name is equal with foreign key column
            for fk in table_schema.foreign_keys.iter() {
                if fk.column == col.name {
                    // check if value is valid !
                    let isok = check_data_foreign_key(
                        &state,
                        fk.reference_table.clone(),
                        fk.reference_column.clone(),
                        value.clone(),
                    )
                    .await;
                    if !isok {
                        log_output(
                            "ERROR",
                            "CHECK FOREIGN KEY",
                            "DATA",
                            format!("Invalid foreign key value: {}", value),
                            false,
                        );
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!(
                                "Invalid foreign key value: {} from table {}",
                                value, fk.reference_table
                            ),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }
            }

            if col.encrypt {
                let is_encrypted = is_encrypted_string(value.as_str());
                if !is_encrypted {
                    value = encrypt(state.encrypt_key.clone(), value);
                }
            }

            // Do not force default "0"; required fields checked earlier. Keep empty if optional.

            // push param by type and mirror into doc_map for AST
            if col.type_data.contains("int") || col.type_data.contains("float") {
                if let Ok(n) = value.parse::<i64>() {
                    bind_params.push(DbParam::I64(n));
                    doc_map.insert(col.name.clone(), serde_json::json!(n));
                    insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::I64(n))));
                } else if let Ok(f) = value.parse::<f64>() {
                    bind_params.push(DbParam::F64(f));
                    doc_map.insert(col.name.clone(), serde_json::json!(f));
                    insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::F64(f))));
                } else {
                    bind_params.push(DbParam::Str(value.clone()));
                    // try number first; else string
                    doc_map.insert(col.name.clone(), serde_json::json!(body.get(&col.name).cloned().unwrap_or(Value::String(String::new()))));
                    insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Str(value))));
                }
            } else {
                bind_params.push(DbParam::Str(value.clone()));
                doc_map.insert(col.name.clone(), body.get(&col.name).cloned().unwrap_or(Value::String(String::new())));
                insert_fields.push((col.name.clone(), InsertValue::Param(DbParam::Str(value))));
            }
        }
    }

    // Note: we no longer build insert_values manually; using insert_fields per column

    // Begin transaction early for SQL backends so ID generation runs in the same TX
    let mut tx_opt: Option<Box<dyn crate::storage::traits::TxStore>> = None;
    if state.db_type != "mongodb" {
        match state.store.begin_tx().await {
            Ok(t) => tx_opt = Some(t),
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error starting transaction: {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    if !function_id_split.is_empty() {
        // Build parts without leading slash; join with '/'
        let mut parts: Vec<String> = Vec::new();
        for token in function_id_split.iter() {
            if token == "%Y" {
                parts.push(chrono::Utc::now().format("%Y").to_string());
            } else if token == "%m" {
                parts.push(chrono::Utc::now().format("%m").to_string());
            } else if token == "%d" {
                parts.push(chrono::Utc::now().format("%d").to_string());
            } else if token.contains("ID") {
                // Numeric suffix with zero-padding based on token, e.g. 000ID -> width 3
                let s_append = token.replace("ID", "");
                let len_id = s_append.len();
                let id_prefix = parts.join("/");

                // Find latest id by prefix. For Mongo, use simple sort by id DESC; for SQL, you can still sort.
                let like_pat = if id_prefix.is_empty() { "%".to_string() } else { format!("{}%", id_prefix) };
                // Build: SELECT id FROM table WHERE id ILIKE prefix% ORDER BY id DESC LIMIT 1
                let q_max = Q::from(table_schema.table.clone())
                    .select(["id"]).r#where(F::ILike("id".into(), like_pat))
                    .order_by("id", false).limit(1);
                let max_id: String = if state.db_type == "mongodb" {
                    match state.store.query(&q_max).await {
                        Ok(rows) if !rows.is_empty() => rows[0].get("id").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_else(|| "0".to_string()),
                        _ => "0".to_string(),
                    }
                } else {
                    // Debug SQL preview for SQL backends
                    if *crate::ISDEBUG {
                        let ds_dbg = SqlStore::new(state.db.clone(), state.db_type.clone());
                        let (sql_dbg, params_dbg) = ds_dbg.preview_sql(&q_max);
                        log_output("QUERY", "MAX-ID", route.as_str(), sql_dbg, true);
                        log_output("PARAMS", "MAX-ID", route.as_str(), format!("{:?}", params_dbg), true);
                    }
                    match tx_opt.as_mut().unwrap().query(&q_max).await {
                        Ok(rows) if !rows.is_empty() => rows[0].get("id").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_else(|| "0".to_string()),
                        _ => "0".to_string(),
                    }
                };
                // Extract numeric suffix (width = len_id) and increment
                let mut current_num: i64 = 0;
                if max_id.len() >= len_id {
                    let suffix = &max_id[max_id.len() - len_id..];
                    current_num = suffix.parse::<i64>().unwrap_or(0);
                }
                let next_num = current_num + 1;
                let next_str = format!("{:0width$}", next_num, width = len_id);
                parts.push(next_str);
            } else if token.starts_with('{') && token.ends_with('}') {
                // Placeholder, likely {request.field}
                let key = token.trim_start_matches('{').trim_end_matches('}');
                if let Some(stripped) = key.strip_prefix("request.") {
                    let val = body
                        .get(stripped)
                        .map(|v| v.to_string().replace('"', "").replace("null", ""))
                        .unwrap_or_default();
                    if !val.is_empty() {
                        parts.push(val);
                    }
                }
            } else if !token.is_empty() {
                // literal segment
                parts.push(token.clone());
            }
        }
        let id = parts.join("/");
        // after building from tokens, bind id into params and AST fields
        bind_params.push(DbParam::Str(id.clone()));
        // mirror to doc_map actual id value for AST path
        doc_map.insert("id".to_string(), serde_json::json!(id.clone()));
        insert_fields.push(("id".to_string(), InsertValue::Param(DbParam::Str(id))));
    }

    // **Tambahkan created_at** (app-side timestamp for AST)
    let now = Local::now().to_rfc3339();
    insert_columns.push("created_at");
    // always use raw expression for created_at to keep server-side clock consistent
    insert_fields.push(("created_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())));
    // keep doc for audit/debug
    doc_map.insert("created_at".to_string(), serde_json::json!(now.clone()));

    // **Tambahkan created_by_id**
    insert_columns.push("created_by_id");
    
    // get type data created_by_id from table_schema
    let created_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "created_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or("int".to_string());

    log_output("TYPE", "created_by_id", route.as_str(), created_by_type.clone(), true);

    if created_by_type.contains("int") {
        if let Ok(n) = claims.id.parse::<i64>() {
            bind_params.push(DbParam::I64(n));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(n));
            insert_fields.push(("created_by_id".to_string(), InsertValue::Param(DbParam::I64(n))));
        } else {
            bind_params.push(DbParam::Str(claims.id.clone()));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
            insert_fields.push(("created_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        }
    } else if created_by_type.contains("float")
        || created_by_type.contains("double")
        || created_by_type.contains("decimal")
        || created_by_type.contains("money")
    {
        if let Ok(n) = claims.id.parse::<f64>() {
            bind_params.push(DbParam::F64(n));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(n));
            insert_fields.push(("created_by_id".to_string(), InsertValue::Param(DbParam::F64(n))));
        } else {
            bind_params.push(DbParam::Str(claims.id.clone()));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
            insert_fields.push(("created_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        }
    } else {
        bind_params.push(DbParam::Str(claims.id.clone()));
        doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
        insert_fields.push(("created_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
    }

    // For MongoDB, directly insert JSON doc without SQL; for SQL, compile INSERT
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let (exec_sql, exec_params) = if state.db_type == "mongodb" {
        (String::new(), vec![])
    } else {
        match ds.preview_insert_with(&table_schema.table, &insert_fields) {
            Ok((sql_dbg, params_dbg)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "POST(AST)", route.as_str(), sql_dbg.clone(), true);
                    log_output("PARAMS", "POST(AST)", route.as_str(), format!("{:?}", params_dbg), true);
                }
                (sql_dbg, params_dbg)
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST INSERT: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    };

    // (moved) validation_data will be executed inside the transaction below (SQL only)
    // Run validate_data inside transaction for SQL backends
    if state.db_type != "mongodb" && table_schema.post.validate_data.contains("SQL:") {
        match crate::database::state::build_sql_and_params_from_formula(
            &table_schema.post.validate_data,
            &body,
        ) {
            Ok((built_sql, params)) => {
                match tx_opt.as_mut().unwrap().raw_sql(&built_sql, params).await {
                    Ok(row) => {
                        if !row.is_empty() {
                            let is_valid = row[0].get(0).and_then(|v| v.as_bool()).unwrap_or(true);
                            if !is_valid {
                                let _ = tx_opt.take().unwrap().rollback().await;
                                return HttpResponse::BadRequest().json(WebResponse {
                                    success: false,
                                    message: "Validation data from table is not valid. Please contact your administrator".to_string(),
                                    total_data: 0,
                                    data: Value::Null,
                                });
                            }
                        } else {
                            let _ = tx_opt.take().unwrap().rollback().await;
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: "Validation data from table is empty. Please contact your administrator".to_string(),
                                total_data: 0,
                                data: Value::Null,
                            });
                        }
                    }
                    Err(err) => {
                        let _ = tx_opt.take().unwrap().rollback().await;
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Error in validation_data: {}", err),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }
            }
            Err(e) => {
                let _ = tx_opt.take().unwrap().rollback().await;
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error building validation formula: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    if state.db_type != "mongodb" && table_schema.post.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
            tx_opt.as_mut().unwrap(),
            table_schema.post.pre_process,
            &body,
            route.as_str(),
        )
        .await
        {
            let _ = tx_opt.take().unwrap().rollback().await;
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    if state.db_type == "mongodb" {
        // Insert using document map
        let doc = Value::Object(doc_map.clone());
        match state.store.insert(&table_schema.table, doc).await {
            Ok(_) => {
                // Audit trail
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "POST",
                    route: &route,
                    id: None,
                    ip: Some(get_client_ip(&req)).as_deref(),
                });
                return HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Data inserted".to_string(),
                    total_data: 1,
                    data: Value::Null,
                });
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-POST (mongo): {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    let mut tx = tx_opt.take().unwrap();
    match tx.raw_sql(&exec_sql, exec_params).await {
        Ok(_) => {
            if state.db_type != "mongodb" && table_schema.post.post_process.contains("SQL:") {
                if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                    &mut tx,
                    table_schema.post.post_process,
                    &body,
                    route.as_str(),
                )
                .await
                {
                    let _ = tx.rollback().await;
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
            let _ = tx.commit().await;
            // Audit trail
            write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: claims.id.clone(),
                action: "POST",
                route: &route,
                id: None,
                ip: Some(get_client_ip(&req)).as_deref(),
            });
            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Data inserted".to_string(),
                total_data: 1,
                data: Value::Null,
            })
        }
        Err(err) => {
            let _ = tx.rollback().await;
            let mut err_message = err.to_string().to_lowercase();
            if err_message.contains("created_by_id") {
                err_message += format!(" \n id from token : {}", &claims.id).as_str();
            }
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err_message),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
