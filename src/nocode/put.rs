use actix_multipart::Multipart;
use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::{execute_sql_formula, execute_sql_formula_with_transaction, DbParam},
    helpers::{filter_table_schema, multipart_to_json},
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::{check_data_foreign_key, process_foreign_keys_delete_update},
    AppState,
};
use chrono::Local;
use std::sync::Arc;

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
    let limit: u32 = std::env::var("RATE_LIMIT_MUTATE_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    if !RL_WINDOW_MUTATE.check_and_increment(&format!("put:{}:{}", route, ip_key), limit) {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::Null,
        });
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

    let mut set_clause = "SET ".to_string();
    let mut bind_params: Vec<DbParam> = Vec::new();
    let mut id_new = "".to_string();

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

                    // check col.encrypt if true then encrypt value
                    if col.encrypt {
                        // check apakah value udah di encrypt
                        let is_encrypted = is_encrypted_string(value_x.clone().as_str());
                        if !is_encrypted {
                            value_x = encrypt(state.encrypt_key.clone(), value_x.clone());
                        }
                    }

                    // check if value from body is number
                    if col.type_data.contains("int") || col.type_data.contains("float") {
                        if let Ok(n) = value_x.parse::<i64>() {
                            bind_params.push(DbParam::I64(n));
                        } else if let Ok(f) = value_x.parse::<f64>() {
                            bind_params.push(DbParam::F64(f));
                        } else {
                            bind_params.push(DbParam::Str(value_x));
                        }
                        set_clause.push_str(&format!("{} = ?, ", column));
                    } else {
                        bind_params.push(DbParam::Str(value_x));
                        set_clause.push_str(&format!("{} = ?, ", column));
                    }
                }
            }
        }
    }

    // add updated_at to set_clause
    set_clause.push_str(&format!(
        "updated_at = {}, ",
        state.query_converter.datetime_now
    ));
    set_clause.push_str("updated_by_id = ?, ");
    bind_params.push(DbParam::I64(claims.id));

    // remove last ", " from set_clause
    set_clause = set_clause[..set_clause.len() - 2].to_string();

    // create query UPDATE sql from table in variable route and structure table in table_schemas where id = id, set set_clause
    let s_sql = format!("UPDATE {} {} WHERE id = ?", table_schema.table, set_clause);

    // Bind id with type inference (int/str)
    if let Ok(n) = id_raw.clone().parse::<i64>() {
        bind_params.push(DbParam::I64(n));
    } else {
        bind_params.push(DbParam::Str(id_raw.clone()));
    }

    log_output("QUERY", "PUT", route.as_str(), s_sql.clone(), true);
    log_output(
        "PARAM",
        "PUT",
        route.as_str(),
        format!("{:?}", bind_params),
        true,
    );

    // check validation_data
    if table_schema.put.validate_data.contains("SQL:") {
        match execute_sql_formula(
            &state.db,
            table_schema.put.validate_data.clone(),
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

    if table_schema.put.pre_process.contains("SQL:") {
        if let Err(err) = execute_sql_formula_with_transaction(
            &mut transaction,
            table_schema.put.pre_process,
            &body,
            route.as_str(),
        )
        .await
        {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    match transaction.query_with_params(&s_sql, bind_params).await {
        Ok(_) => {
            if table_schema.put.post_process.contains("SQL:") {
                if let Err(err) = execute_sql_formula_with_transaction(
                    &mut transaction,
                    table_schema.put.post_process,
                    &body,
                    route.as_str(),
                )
                .await
                {
                    let _ = transaction.rollback().await;
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
                let (is_fk_ok, err_message) = process_foreign_keys_delete_update(
                    "UPDATE", // "DELETE" or "UPDATE"
                    state.clone(),
                    &mut transaction,
                    reference_foreign_keys,
                    claims.id,
                    id_raw.clone(),
                    id_new, // for UPDATE
                )
                .await;

                if !is_fk_ok {
                    let _ = transaction.rollback().await;
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

            // Commit transaction if all operations succeeded
            match transaction.commit().await {
                Ok(_) => {
                    // Audit
                    write_audit(&AuditEntry {
                        at: Local::now().to_rfc3339(),
                        actor_id: claims.id,
                        action: "PUT",
                        route: &route,
                        id: Some(&id_raw),
                        ip: Some(get_client_ip(&req)).as_deref(),
                    });
                    HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data deleted successfully".to_string(),
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
            let _ = transaction.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PUT: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
