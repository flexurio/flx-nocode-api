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
    database::state::{execute_sql_formula, execute_sql_formula_with_transaction, DbParam},
    helpers::{extract_expressions, filter_table_schema, find_column_match, multipart_to_json},
    log::log_output,
    model::{Column, TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::SqlStore;

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
    let mut id: String = String::new();

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
    let mut has_formula = false;
    let mut doc_map = serde_json::Map::new();
    let mut insert_values: Vec<String> = Vec::new();
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
                has_formula = true;
                // Remove leading "col=" part
                let rhs = string_formula.replace(&format!("{}=", col.name), "");
                let (frag, mut params) = build_formula_value(&rhs, &body);
                insert_values.push(frag);
                bind_params.append(&mut params);
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

            // push placeholder and param by type and mirror into doc_map for AST
            if col.type_data.contains("int") || col.type_data.contains("float") {
                if let Ok(n) = value.parse::<i64>() {
                    bind_params.push(DbParam::I64(n));
                    doc_map.insert(col.name.clone(), serde_json::json!(n));
                } else if let Ok(f) = value.parse::<f64>() {
                    bind_params.push(DbParam::F64(f));
                    doc_map.insert(col.name.clone(), serde_json::json!(f));
                } else {
                    bind_params.push(DbParam::Str(value));
                    // try number first; else string
                    doc_map.insert(col.name.clone(), serde_json::json!(body.get(&col.name).cloned().unwrap_or(Value::String(String::new()))));
                }
                insert_values.push("?".into());
            } else {
                bind_params.push(DbParam::Str(value));
                doc_map.insert(col.name.clone(), body.get(&col.name).cloned().unwrap_or(Value::String(String::new())));
                insert_values.push("?".into());
            }
        }
    }

    // remove element "XXXAUTOINC" from insert_values
    insert_values.retain(|v| v != "XXXAUTOINC");

    if !function_id_split.is_empty() {
        // loop every function_id_split
        for function_id in function_id_split.iter() {
            if function_id == "%Y" {
                // get year from now with format YYYY
                let year = chrono::Utc::now().format("%Y").to_string();
                id.push('/');
                id.push_str(&year);
            } else if function_id == "%m" {
                // get month from now with format MM
                let month = chrono::Utc::now().format("%m").to_string();
                id.push('/');
                id.push_str(&month);
            } else if function_id == "%d" {
                // get day from now with format DD
                let day = chrono::Utc::now().format("%d").to_string();
                id.push('/');
                id.push_str(&day);
            } else if function_id.contains("ID") {
                let mut id_find = id.clone();
                id_find.remove(0);

                let s_append = function_id.replace("ID", "");
                let len_id = s_append.len();

                // get max id from table from column id with length len_id from left
                let s_sql_max_id = format!(
                    "SELECT COALESCE(MAX(id),0) as max_id FROM {} WHERE id like '%{}%' ",
                    table_schema.table, id_find
                );
                log_output("QUERY", "GET", route.as_str(), s_sql_max_id.clone(), true);
                let max_id: String = match &state.db.query(&s_sql_max_id).await {
                    Ok(row) => row[0]
                        .get("max_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    Err(_) => "0".to_string(),
                };

                // get n char from right
                let max_id_str: String = max_id.rsplit('/').next().unwrap_or("0").to_string();

                // remove leading zero
                let _ = max_id_str.trim_start_matches('0');

                let max_id: i64 = max_id_str.parse().unwrap_or(0) + 1;
                let max_id_str = format!("{:0>len$}", max_id, len = len_id);
                id.push('/');
                id.push_str(&max_id_str);
            } else if function_id.starts_with("{request.") && function_id.ends_with('}') {
                
                // get value from body
                let key = function_id
                    .trim_start_matches("{request.")
                    .trim_end_matches('}');
                let value = body
                    .get(key)
                    .map(|v| v.to_string().replace('"', "").replace("null", ""))
                    .unwrap_or_default();

                if value.is_empty() {
                    // return error if value is empty
                    return HttpResponse::BadRequest().json(WebResponse {
                        success: false,
                        message: format!("Missing value for {}", key),
                        total_data: 0,
                        data: Value::Null,
                    });
                }

                id.push('/');
                id.push_str(&value);
            } else {
                id.push('/');
                id.push_str(function_id);
            }
        }

        // remove first "/" from id
        id.remove(0);

        // remove id from insert_columns
        insert_columns.retain(|&x| x != "id");
        insert_columns.push("id");
    // add id parameter
    insert_values.push("?".into());
    bind_params.push(DbParam::Str(id.clone()));
    // mirror to doc_map actual id value for AST path
    doc_map.insert("id".to_string(), serde_json::json!(id));
    }

    // **Tambahkan created_at** (app-side timestamp for AST)
    let now = Local::now().to_rfc3339();
    insert_columns.push("created_at");
    if has_formula {
        insert_values.push(state.query_converter.datetime_now.clone());
    } else {
        insert_values.push("?".into());
        // bind for legacy path
        bind_params.push(DbParam::Str(now.clone()));
        // doc for AST path
        doc_map.insert("created_at".to_string(), serde_json::json!(now.clone()));
    }

    // **Tambahkan created_by_id**
    insert_columns.push("created_by_id");
    insert_values.push("?".into());
    
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
        } else {
            bind_params.push(DbParam::Str(claims.id.clone()));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else if created_by_type.contains("float")
        || created_by_type.contains("double")
        || created_by_type.contains("decimal")
        || created_by_type.contains("money")
    {
        if let Ok(n) = claims.id.parse::<f64>() {
            bind_params.push(DbParam::F64(n));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(n));
        } else {
            bind_params.push(DbParam::Str(claims.id.clone()));
            doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else {
        bind_params.push(DbParam::Str(claims.id.clone()));
        doc_map.insert("created_by_id".to_string(), serde_json::json!(claims.id.clone()));
    }

    // Determine execution SQL and params (AST when no formula, legacy otherwise)
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let (exec_sql, exec_params) = if !has_formula {
        let doc_val = serde_json::Value::Object(doc_map.clone());
        match ds.preview_insert(&table_schema.table, &doc_val) {
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
    } else {
        let s_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_schema.table,
            insert_columns.join(", "),
            insert_values.join(", ")
        );
        if *crate::ISDEBUG {
            log_output("QUERY", "POST(LEGACY)", route.as_str(), s_sql.clone(), true);
            log_output("PARAMS", "POST(LEGACY)", route.as_str(), format!("{:?}", bind_params), true);
        }
        (s_sql, bind_params.clone())
    };

    // check validation_data
    if table_schema.post.validate_data.contains("SQL:") {
        match execute_sql_formula(
            &state.db,
            table_schema.post.validate_data.clone(),
            &body,
            route.as_str(),
        )
        .await
        {
            Ok(row) => {
                // check data row
                if !row.is_empty() {
                    let is_valid = row[0].get(0).and_then(|v| v.as_bool()).unwrap_or(true);
                    if !is_valid {
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: "Validation data from table is not valid. Please contact your administrator".to_string(),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                } else {
                    return HttpResponse::BadRequest().json(WebResponse {
                        success: false,
                        message:
                            "Validation data from table is empty. Please contact your administrator"
                                .to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
            Err(err) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error in validation_data: {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // Begin transaction
    let mut transaction = match state.db.begin_transaction().await {
        Ok(tx) => tx,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    if table_schema.post.pre_process.contains("SQL:") {
        if let Err(err) = execute_sql_formula_with_transaction(
            &mut transaction,
            table_schema.post.pre_process,
            &body,
            route.as_str(),
        )
        .await
        {
            transaction.rollback().await.ok();
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    match transaction.query_with_params(&exec_sql, exec_params).await {
        Ok(_) => {
            if table_schema.post.post_process.contains("SQL:") {
                if let Err(err) = execute_sql_formula_with_transaction(
                    &mut transaction,
                    table_schema.post.post_process,
                    &body,
                    route.as_str(),
                )
                .await
                {
                    transaction.rollback().await.ok();
                    // Rollback transaction if post-process SQL fails
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
            transaction.commit().await.ok();
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
            transaction.rollback().await.ok();
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
