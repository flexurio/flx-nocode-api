use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use sonic_rs::{Value, json};

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    helpers::filter_table_schema,
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::process_foreign_keys_delete_update_txstore,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::storage::ast::{Filter as QF, Val as QV};

// NCO-DELETE
pub async fn delete(
    state: Data<AppState>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
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
                    message: crate::constants::ERR_INVALID_TOKEN.to_string(),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        };

        if !check_access(&claims, &route, "delete") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let id_raw: String = path.into_inner();
    // Rate-limit
    let ip_key = get_client_ip(&req);
    let limit_i64: i64 = std::env::var("RATE_LIMIT_MUTATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if limit_i64 > 0
        && !RL_WINDOW_MUTATE
            .check_and_increment(&format!("delete:{}:{}", route, ip_key), limit_i64 as u32)
    {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::default(),
        });
    }
    // Per-user limit (for non-public routes only)
    if !state.route_publics.contains(&route) {
        let user_key = claims.id.clone();
        if limit_i64 > 0
            && !user_key.is_empty()
            && !RL_WINDOW_MUTATE
                .check_and_increment(&format!("delete:{}:user:{}", route, user_key), limit_i64 as u32)
        {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".into(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let table_schema = filter_table_schema(table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    // check table_schemas.delete.type_delete
    let type_delete = table_schema.del.type_delete.clone();
    // Build AST-compiled SQL and params to execute
    let mut exec_sql = String::new();
    let mut exec_params: Vec<crate::database::state::DbParam> = Vec::new();

    if type_delete == "soft" {
        // Decide types for deleted_by_id and id
        let deleted_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "deleted_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
        log_output("TYPE", "deleted_by_id", route.as_str(), deleted_by_type.clone(), true);

        // Build fields with a raw DB now() expression to avoid string conversion issues (MSSQL)
        let mut fields: Vec<(String, InsertValue)> = vec![
            ("deleted_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())),
        ];
        // Set deleted_by_id typed
        if deleted_by_type.contains("int") {
            if let Ok(n) = claims.id.parse::<i64>() {
                fields.push(("deleted_by_id".into(), InsertValue::Param(crate::database::state::DbParam::I64(n))));
            } else {
                fields.push(("deleted_by_id".into(), InsertValue::Param(crate::database::state::DbParam::Str(claims.id.clone()))));
            }
        } else if deleted_by_type.contains("float")
            || deleted_by_type.contains("double")
            || deleted_by_type.contains("decimal")
            || deleted_by_type.contains("money")
        {
            if let Ok(n) = claims.id.parse::<f64>() {
                fields.push(("deleted_by_id".into(), InsertValue::Param(crate::database::state::DbParam::F64(n))));
            } else {
                fields.push(("deleted_by_id".into(), InsertValue::Param(crate::database::state::DbParam::Str(claims.id.clone()))));
            }
        } else {
            fields.push(("deleted_by_id".into(), InsertValue::Param(crate::database::state::DbParam::Str(claims.id.clone()))));
        }

        // Filter by id typed
        let id_filter_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        match ds.preview_update_with(&table_schema.table, Some(&QF::Eq("id".into(), id_filter_val)), &fields) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-soft)", route.as_str(), sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-soft)", route.as_str(), format!("{:?}", params), true);
                }
                exec_sql = sql;
                exec_params = params;
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST soft delete: {}", e),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    } else if type_delete == "hard" {
        // compile DELETE via AST
        let id_filter_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        match ds.preview_delete(&table_schema.table, Some(&QF::Eq("id".into(), id_filter_val))) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-hard)", route.as_str(), sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-hard)", route.as_str(), format!("{:?}", params), true);
                }
                exec_sql = sql;
                exec_params = params;
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST hard delete: {}", e),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    }
    log_output("QUERY", "DELETE(AST)", route.as_str(), exec_sql.clone(), true);

    // MongoDB path: no transactions; perform direct update/delete
    if state.db_type == "mongodb" {
        // Build filter by id with numeric inference
        let id_filter_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let filter = Some(QF::Eq("id".into(), id_filter_val));
        let result = if type_delete == "soft" {
            // patch deleted_at and deleted_by_id
            let now_ts = Local::now().to_rfc3339();
            let mut patch = sonic_rs::Object::new();
            patch.insert("deleted_at", json!(now_ts));
            // type for deleted_by_id
            let deleted_by_type = table_schema
                .columns
                .iter()
                .find(|c| c.name == "deleted_by_id")
                .map(|c| c.type_data.clone())
                .unwrap_or("int".to_string());
            if deleted_by_type.contains("int") {
                if let Ok(n) = claims.id.parse::<i64>() {
                    patch.insert("deleted_by_id", json!(n));
                } else {
                    patch.insert("deleted_by_id", json!(claims.id.clone()));
                }
            } else if deleted_by_type.contains("float")
                || deleted_by_type.contains("double")
                || deleted_by_type.contains("decimal")
                || deleted_by_type.contains("money")
            {
                if let Ok(n) = claims.id.parse::<f64>() {
                    patch.insert("deleted_by_id", json!(n));
                } else {
                    patch.insert("deleted_by_id", json!(claims.id.clone()));
                }
            } else {
                patch.insert("deleted_by_id", json!(claims.id.clone()));
            }
            state.store.update(&table_schema.table, filter, Value::from(patch)).await.map(|_| ())
        } else {
            state.store.delete(&table_schema.table, filter).await.map(|_| ())
        };
        return match result {
            Ok(()) => {
                // Audit
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "DELETE",
                    route: &route,
                    id: Some(&id_raw),
                    ip: Some(get_client_ip(&req)).as_deref(),
                });
                HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: if type_delete == "soft" { "Data soft-deleted".to_string() } else { "Data deleted".to_string() },
                    total_data: 1,
                    data: Value::default(),
                })
            }
            Err(err) => {
                HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-DELETE (mongo): {}", err),
                    total_data: 0,
                    data: Value::default(),
                })
            }
        };
    }

    // Begin transaction via generic store (SQL backends)
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::default(),
            });
        }
    };

    // Execute main delete query
    match tx.raw_sql(&exec_sql, exec_params).await {
        Ok(_) => {
            let (is_fk_ok, err_message) = process_foreign_keys_delete_update_txstore(
                "DELETE", // "DELETE" or "UPDATE"
                state.clone(),
                route.clone(),
                &mut tx,
                reference_foreign_keys,
                claims.id.clone(),
                id_raw.clone(),
                "".to_string(), // for UPDATE
            )
            .await;

            if is_fk_ok {
                // Commit transaction if all operations succeeded
                match tx.commit().await {
                    Ok(_) => {
                        // Audit
                        write_audit(&AuditEntry {
                            at: Local::now().to_rfc3339(),
                            actor_id: claims.id.clone(),
                            action: "DELETE",
                            route: &route,
                            id: Some(&id_raw),
                            ip: Some(get_client_ip(&req)).as_deref(),
                        });
                        HttpResponse::Ok().json(WebResponse {
                            success: true,
                            message: "Data deleted successfully".to_string(),
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
            } else {
                // Rollback transaction due to foreign key failures
                let _ = tx.rollback().await;
                HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!(
                        "Transaction rolled back due to foreign key failures: {}",
                        err_message
                    ),
                    total_data: 0,
                    data: Value::default(),
                })
            }
        }
        Err(err) => {
            // Rollback transaction due to main delete failure
            let _ = tx.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-DELETE: {}", err),
                total_data: 0,
                data: Value::default(),
            })
        }
    }
}
