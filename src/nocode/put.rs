use actix_multipart::Multipart;
use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::{audit::{AuditEntry, write_audit}};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::{DbParam},
    helpers::{filter_table_schema, multipart_to_json},
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::storage::ast::{Filter as QF, Val as QV};

// NCO-PUT
pub async fn update(
    state: Data<AppState>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    multipart: Multipart,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;
    let reference_foreign_keys = &schemas.1;
    
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
    // Rate-limit per IP
    let ip_key = get_client_ip(&req);
    let limit_i64: i64 = std::env::var("RATE_LIMIT_MUTATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if limit_i64 > 0
        && !RL_WINDOW_MUTATE
            .check_and_increment(&format!("put:{}:{}", route, ip_key), limit_i64 as u32)
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
                .check_and_increment(&format!("put:{}:user:{}", route, user_key), limit_i64 as u32)
        {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".into(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }
    let id_raw: String = path.into_inner();

    // get body from request and compare with table_schemas.put.columns
    let table_schema = filter_table_schema(table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Collect update fields using expression-aware builder
    let mut update_fields: Vec<(String, InsertValue)> = Vec::new();
    let mut id_new = "".to_string();
    let mut password_override: Option<String> = None;
    let mut patch_fields = serde_json::Map::new(); // kept for special-case flx_users password-only path

    // loop every column in table_schemas.put.columns
    for column in table_schema.put.columns.iter() {
        // loop every key and value in body
        for (key, value) in body.as_object().unwrap_or(&serde_json::Map::new()).iter() {
            // check if key from body is equal to column
            if key == column {
                // convert value to string
                let mut value_x = format!("{}", value).replace("\"", "").replace("null", "");

                // check if value from body is not empty
                if !value_x.is_empty() {
                    // check jika ada kolom id maka id nya diganti. Sehingga perlu dipakai buat update foreign key
                    if key == "id" {
                        // convert value to string
                        id_new = value_x.clone();
                    }

                    // check if col.name is equal with foreign key column
                    for fk in table_schema.foreign_keys.iter() {
                        if fk.column == *column {
                            // check if value is valid !
                            let isok = check_data_foreign_key(
                                &state,
                                fk.reference_table.clone(),
                                fk.reference_column.clone(),
                                value_x.clone(),
                            )
                            .await;
                            if !isok {
                                log_output(
                                    "ERROR",
                                    "CHECK FOREIGN KEY",
                                    "DATA",
                                    format!("Invalid foreign key value: {}", value_x),
                                    false,
                                );
                                return HttpResponse::InternalServerError().json(WebResponse {
                                    success: false,
                                    message: format!(
                                        "Invalid foreign key value: {} from table {}",
                                        value_x, fk.reference_table
                                    ),
                                    total_data: 0,
                                    data: Value::Null,
                                });
                            }
                        }
                    }

                    // find column properties in table_schemas.columns (handle not found)
                    let col = match table_schema.columns.iter().find(|col| col.name == *column) {
                        Some(c) => c,
                        None => {
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: format!(
                                    "Unknown column '{}' for route '{}'",
                                    column, route
                                ),
                                total_data: 0,
                                data: Value::Null,
                            });
                        }
                    };

                    // check col.encrypt if true then encrypt value (and capture password override for flx_users)
                    if col.encrypt {
                        let is_encrypted = is_encrypted_string(value_x.clone().as_str());
                        if !is_encrypted {
                            value_x = encrypt(state.encrypt_key.clone(), value_x.clone());
                        }
                        if route == "flx_users" && column == "password" {
                            password_override = Some(value_x.clone());
                        }
                    }

                    // check if value from body is number
                    if col.type_data.contains("int") || col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
                        if let Ok(n) = value_x.parse::<i64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::I64(n))));
                            patch_fields.insert(column.clone(), serde_json::json!(n));
                        } else if let Ok(f) = value_x.parse::<f64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::F64(f))));
                            patch_fields.insert(column.clone(), serde_json::json!(f));
                        } else {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                            patch_fields.insert(column.clone(), serde_json::json!(value_x));
                        }
                    } else {
                        update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                        patch_fields.insert(column.clone(), serde_json::json!(value_x));
                    }
                }
            }
        }
    }

    // add updated_at/by into update_fields (server-side now expression)
    update_fields.push(("updated_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())));

    // get type data updated_by_id from table_schema
    let created_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "updated_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or("int".to_string());

    log_output("TYPE", "updated_by_id", route.as_str(), created_by_type.clone(), true);

    if created_by_type.contains("int") {
        if let Ok(n) = claims.id.parse::<i64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::I64(n))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else if created_by_type.contains("float") || 
        created_by_type.contains("double") || 
        created_by_type.contains("decimal") || 
        created_by_type.contains("money") {
        if let Ok(n) = claims.id.parse::<f64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::F64(n))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else {
        update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
    }
    
    // legacy set_clause kept only for logging; actual SQL compiled via AST

    // Compile AST update (SQL only). For MongoDB we'll use DataStore.update with patch_fields
    let (s_sql, params_compiled) = if state.db_type == "mongodb" {
        (String::new(), vec![])
    } else {
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        let filter = Some(QF::Eq("id".into(), QV::Str(id_raw.clone())));
        match ds.preview_update_with(&table_schema.table, filter.as_ref(), &update_fields) {
            Ok(pair) => pair,
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST UPDATE: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    };

    // Preview AST-style update for debug (filter id, patch keys from body + timestamps)
    if *crate::ISDEBUG && state.db_type != "mongodb" {
        log_output("QUERY", "PUT(AST)", route.as_str(), s_sql.clone(), true);
        log_output("PARAM", "PUT(AST)", route.as_str(), format!("{:?}", params_compiled), true);
    }

    // validation_data moved to run inside the transaction below
    // Begin transaction for SQL backends only
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

    // Run validate_data inside transaction (expects boolean in first column of first row)
    if state.db_type != "mongodb" && table_schema.put.validate_data.contains("SQL:") {
        match crate::database::state::build_sql_and_params_from_formula(
            &table_schema.put.validate_data,
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

    if state.db_type != "mongodb" && table_schema.put.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
            tx_opt.as_mut().unwrap(),
            table_schema.put.pre_process,
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

    // If this is a password-only update for flx_users, prefer DataStore for clarity and consistency
    if route == "flx_users" && password_override.is_some() && table_schema.put.columns.len() == 1 {
        let now = chrono::Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut doc = serde_json::json!({
            "password": password_override.unwrap(),
            "updated_at": now,
        });
        // updated_by_id from claims
        // detect numeric type for updated_by_id
        let created_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "updated_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
        if created_by_type.contains("int") {
            if let Ok(n) = claims.id.parse::<i64>() {
                doc["updated_by_id"] = serde_json::json!(n);
            } else {
                doc["updated_by_id"] = serde_json::json!(claims.id.clone());
            }
        } else if created_by_type.contains("float")
            || created_by_type.contains("double")
            || created_by_type.contains("decimal")
            || created_by_type.contains("money")
        {
            if let Ok(n) = claims.id.parse::<f64>() {
                doc["updated_by_id"] = serde_json::json!(n);
            } else {
                doc["updated_by_id"] = serde_json::json!(claims.id.clone());
            }
        } else {
            doc["updated_by_id"] = serde_json::json!(claims.id.clone());
        }

        // Build WHERE id = ? by calling update with Filter
        use crate::storage::ast::{Filter as QF, Val as QV};
        let filter = Some(QF::Eq("id".into(), QV::Str(id_raw.clone())));
        if state.db_type == "mongodb" {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_str(), "Mongo password update flx_users".to_string(), true);
            }
            match state.store.update("flx_users", filter, doc).await {
                Ok(_) => {
                    // Audit
                    write_audit(&AuditEntry {
                        at: Local::now().to_rfc3339(),
                        actor_id: claims.id.clone(),
                        action: "PUT",
                        route: &route,
                        id: Some(&id_raw),
                        ip: Some(get_client_ip(&req)).as_deref(),
                    });
                    return HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    });
                }
                Err(err) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT (mongo): {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        } else {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_str(), "AST password update flx_users".to_string(), true);
            }
            let mut tx = tx_opt.take().unwrap();
            match tx.update("flx_users", filter, doc).await {
                Ok(_) => {
                    let _ = tx.commit().await;
                    return HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    });
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        }
    }

    // MongoDB main update path (no transaction)
    if state.db_type == "mongodb" {
        // Ensure updated_at exists in patch_fields for Mongo (ISO timestamp)
        let now_iso = Local::now().to_rfc3339();
        patch_fields.insert("updated_at".to_string(), serde_json::json!(now_iso));
        // Build filter by id (attempt numeric, fallback to string)
        let filt_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let filter = Some(QF::Eq("id".into(), filt_val));
        match state.store.update(&table_schema.table, filter, Value::Object(patch_fields.clone())).await {
            Ok(_) => {
                // Audit
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "PUT",
                    route: &route,
                    id: Some(&id_raw),
                    ip: Some(get_client_ip(&req)).as_deref(),
                });
                return HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Data updated successfully".to_string(),
                    total_data: 1,
                    data: Value::Null,
                });
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-PUT (mongo): {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // SQL path below
    let mut tx = tx_opt.take().unwrap();
    match tx.raw_sql(&s_sql, params_compiled).await {
        Ok(_) => {
            if state.db_type != "mongodb" && table_schema.put.post_process.contains("SQL:") {
                if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                    &mut tx,
                    table_schema.put.post_process,
                    &body,
                    route.as_str(),
                )
                .await
                {
                    let _ = tx.rollback().await;
                    // Rollback transaction if post-process SQL fails
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }

            // jika id_new TIDAK SAMA dg "" maka ada perubahan nilai id
            if !id_new.is_empty() {
                if state.db_type != "mongodb" {
                    let (is_fk_ok, err_message) = crate::nocode::foreign_key::process_foreign_keys_delete_update_txstore(
                        "UPDATE", // "DELETE" or "UPDATE"
                        state.clone(),
                        route.clone(),
                        &mut tx,
                        reference_foreign_keys,
                        claims.id.clone(),
                        id_raw.clone(),
                        id_new, // for UPDATE                        
                    )
                    .await;

                    if !is_fk_ok {
                        let _ = tx.rollback().await;
                        return HttpResponse::InternalServerError().json(WebResponse {
                            success: false,
                            message: format!(
                                "Transaction rolled back due to foreign key failures: {}",
                                err_message
                            ),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }
            }

            // Commit transaction if all operations succeeded
            match tx.commit().await {
                Ok(_) => {
                    // Audit
                    write_audit(&AuditEntry {
                        at: Local::now().to_rfc3339(),
                        actor_id: claims.id.clone(),
                        action: "PUT",
                        route: &route,
                        id: Some(&id_raw),
                        ip: Some(get_client_ip(&req)).as_deref(),
                    });
                    HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    })
                }
                Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error committing transaction: {}", err),
                    total_data: 0,
                    data: Value::Null,
                }),
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PUT: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
