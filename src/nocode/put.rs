use crate::crypt::{encrypt_ref, is_encrypted_string};
use actix_multipart::Multipart;
use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use sonic_rs::{Value, json};
use crate::json_compat::{JsonValueTrait, JsonContainerTrait, value_from_f64};

use crate::{audit::{AuditEntry, write_audit}};
use crate::helpers::get_client_ip; // still used for logging if needed
// Global rate limiting now handled in main.rs (removed RL_WINDOW_MUTATE usage)
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    database::state::{DbParam},
    helpers::{filter_table_schema, multipart_to_json},
    log::log_output,
    model::{TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{InsertValue};
use crate::storage::ast::{Filter as QF, Val as QV};

// NCO-PUT
pub async fn update(
    state: Data<AppState>,
    route: Arc<str>,
    table_schemas: Arc<Vec<TableSchema>>,
    multipart: Multipart,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &table_schemas;
    
    let mut claims = Claims::default();
    if !state.route_publics.iter().any(|r| r == route.as_ref()) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: crate::constants::ERR_INVALID_TOKEN.to_string(),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        };

    if !check_access(&claims, route.as_ref(), "write") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
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
                data: Value::default(),
            });
        }
    };
    // Rate limiting removed (handled globally). Keep IP if needed.
    let _ip_key = get_client_ip(&req);
    let id_raw: String = path.into_inner();

    // get body from request and compare with table_schemas.put.columns
    let table_schema = filter_table_schema(table_schemas, route.as_ref());
    if table_schema.table.is_empty() {
    let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    // Collect update fields using expression-aware builder
    let mut update_fields: Vec<(String, InsertValue)> = Vec::new();
    let mut id_new = "".to_string();
    let mut password_override: Option<String> = None;
    let mut patch_fields = sonic_rs::Object::new(); // kept for special-case flx_users password-only path

    // loop every column in table_schemas.put.columns
    if let Some(body_obj) = body.as_object() {
        for column in table_schema.put.columns.iter() {
            for (key, value) in body_obj.iter() {
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
                                    data: Value::default(),
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
                                data: Value::default(),
                            });
                        }
                    };

                    // check col.encrypt if true then encrypt value (and capture password override for flx_users)
                    if col.encrypt {
                        if !is_encrypted_string(&value_x) {
                            value_x = encrypt_ref(&state.encrypt_key, &value_x);
                        }
                        if route.as_ref() == "flx_users" && column == "password" {
                            password_override = Some(value_x.clone());
                        }
                    }

                    // check if value from body is number
                    if col.type_data.contains("int") || col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
                        if let Ok(n) = value_x.parse::<i64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::I64(n))));
                            patch_fields.insert(column.as_str(), json!(n));
                        } else if let Ok(f) = value_x.parse::<f64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::F64(f))));
                            patch_fields.insert(column.as_str(), value_from_f64(f));
                        } else {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                            patch_fields.insert(column.as_str(), Value::from(value_x.as_str()));
                        }
                    } else {
                        update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                        patch_fields.insert(column.as_str(), Value::from(value_x.as_str()));
                    }
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

    log_output("TYPE", "updated_by_id", route.as_ref(), created_by_type.clone(), true);

    if created_by_type.contains("int") {
        if let Ok(n) = claims.id.parse::<i64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::I64(n))));
                            patch_fields.insert("updated_by_id", json!(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id", Value::from(claims.id.as_str()));
        }
    } else if created_by_type.contains("float") || 
        created_by_type.contains("double") || 
        created_by_type.contains("decimal") || 
        created_by_type.contains("money") {
        if let Ok(n) = claims.id.parse::<f64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::F64(n))));
            patch_fields.insert("updated_by_id", value_from_f64(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id", Value::from(claims.id.as_str()));
        }
    } else {
        update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
    patch_fields.insert("updated_by_id", Value::from(claims.id.as_str()));
    }
    
    // legacy set_clause kept only for logging; actual SQL compiled via AST

    // Compile AST update (SQL only). For MongoDB we'll use DataStore.update with patch_fields
    let (s_sql, params_compiled) = if state.db_type == "mongodb" {
        (String::new(), vec![])
    } else {
        // Prefer numeric id for filter when path id is numeric to ensure proper matching
        let filter = Some(QF::Eq(
            "id".into(),
            if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) }
        ));
        match state.sql_store.preview_update_with(&table_schema.table, filter.as_ref(), &update_fields) {
            Ok(pair) => pair,
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST UPDATE: {}", e),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    };

    // Preview AST-style update for debug (filter id, patch keys from body + timestamps)
    if *crate::ISDEBUG && state.db_type != "mongodb" {
    log_output("QUERY", "PUT(AST)", route.as_ref(), s_sql.clone(), true);
    log_output("PARAM", "PUT(AST)", route.as_ref(), format!("{:?}", params_compiled), true);
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
                    data: Value::default(),
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
                                    data: Value::default(),
                                });
                            }
                        } else {
                            let _ = tx_opt.take().unwrap().rollback().await;
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: "Validation data from table is empty. Please contact your administrator".to_string(),
                                total_data: 0,
                                data: Value::default(),
                            });
                        }
                    }
                    Err(err) => {
                        let _ = tx_opt.take().unwrap().rollback().await;
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Error in validation_data: {}", err),
                            total_data: 0,
                            data: Value::default(),
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
                    data: Value::default(),
                });
            }
        }
    }

    if state.db_type != "mongodb" && table_schema.put.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
            tx_opt.as_mut().unwrap(),
            &table_schema.put.pre_process,
            &body,
            route.as_ref(),
        )
        .await
        {
            let _ = tx_opt.take().unwrap().rollback().await;
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    // If this is a password-only update for flx_users, prefer DataStore for clarity and consistency
    if route.as_ref() == "flx_users" && password_override.is_some() && table_schema.put.columns.len() == 1 {
        let now = chrono::Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut doc_obj = sonic_rs::Object::new();
        if let Some(pass_tmp) = password_override {
            doc_obj.insert("password", json!(pass_tmp));
        }
        doc_obj.insert("updated_at", json!(now));
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
                doc_obj.insert("updated_by_id", json!(n));
            } else {
                doc_obj.insert("updated_by_id", json!(claims.id));
            }
        } else if created_by_type.contains("float")
            || created_by_type.contains("double")
            || created_by_type.contains("decimal")
            || created_by_type.contains("money")
        {
            if let Ok(n) = claims.id.parse::<f64>() {
                doc_obj.insert("updated_by_id", json!(n));
            } else {
                doc_obj.insert("updated_by_id", json!(claims.id));
            }
        } else {
            doc_obj.insert("updated_by_id", json!(claims.id));
        }

        let doc = Value::from(doc_obj);

        // Build WHERE id = ? by calling update with Filter
        use crate::storage::ast::{Filter as QF, Val as QV};
        let filter = Some(QF::Eq(
            "id".into(),
            if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) }
        ));
        if state.db_type == "mongodb" {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_ref(), "Mongo password update flx_users".to_string(), true);
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
                        data: Value::default(),
                    });
                }
                Err(err) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT (mongo): {}", err),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            }
        } else {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_ref(), "AST password update flx_users".to_string(), true);
            }
            let mut tx = tx_opt.take().unwrap();
            match tx.update("flx_users", filter, doc).await {
                Ok(_) => {
                    let _ = tx.commit().await;
                    return HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::default(),
                    });
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT: {}", err),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            }
        }
    }

    // MongoDB main update path (no transaction)
    if state.db_type == "mongodb" {
        // Ensure updated_at exists in patch_fields for Mongo (ISO timestamp)
        let now_iso = Local::now().to_rfc3339();
    patch_fields.insert("updated_at", json!(now_iso));
        // Build filter by id (attempt numeric, fallback to string)
        let filt_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let filter = Some(QF::Eq("id".into(), filt_val));
    match state.store.update(&table_schema.table, filter, Value::from(patch_fields.clone())).await {
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
                    data: Value::default(),
                });
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-PUT (mongo): {}", err),
                    total_data: 0,
                    data: Value::default(),
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
                    &table_schema.put.post_process,
                    &body,
                    route.as_ref(),
                )
                .await
                {
                    let _ = tx.rollback().await;
                    // Rollback transaction if post-process SQL fails
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            }

            // jika id_new TIDAK SAMA dg "" maka ada perubahan nilai id
            if !id_new.is_empty()
                && state.db_type != "mongodb" {
                    let (is_fk_ok, err_message) = crate::nocode::foreign_key::process_foreign_keys_delete_update_txstore(
                        "UPDATE", // "DELETE" or "UPDATE"
                        state.clone(),
                        route.to_string(),
                        &mut tx,
                        &crate::SCHEMA_REF_FOREIGN_KEYS,
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
                            data: Value::default(),
                        });
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
                        data: Value::default(),
                    })
                }
                Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error committing transaction: {}", err),
                    total_data: 0,
                    data: Value::default(),
                }),
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PUT: {}", err),
                total_data: 0,
                data: Value::default(),
            })
        }
    }
}
