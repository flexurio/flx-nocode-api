use actix_web::{
    HttpResponse, Responder, web::{self, Data, Path}
};
use serde_json::Value;

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
// Mutation rate limiting moved to middleware
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

/// Build a composite primary key filter
/// For single PK: returns Eq(pk_col, value)
/// For composite PK: returns And([Eq(pk_col1, val1), Eq(pk_col2, val2), ...])
fn build_pk_filter(pk_columns: &[String], pk_values: &[String]) -> Result<QF, String> {
    if pk_columns.is_empty() {
        return Err("No primary key columns defined".to_string());
    }
    if pk_columns.len() != pk_values.len() {
        return Err(format!(
            "Primary key mismatch: expected {} values for {} columns",
            pk_columns.len(),
            pk_values.len()
        ));
    }

    if pk_columns.len() == 1 {
        // Single PK: use simple Eq
        Ok(QF::Eq(pk_columns[0].clone(), QV::Str(pk_values[0].clone())))
    } else {
        // Composite PK: use And with multiple Eq
        let filters = pk_columns
            .iter()
            .zip(pk_values.iter())
            .map(|(col, val)| QF::Eq(col.clone(), QV::Str(val.clone())))
            .collect();
        Ok(QF::And(filters))
    }
}

/// Parse composite PK values from path parameter using ~ as delimiter
/// Single value: "123" -> ["123"]
/// Composite: "123~456" -> ["123", "456"]
fn parse_pk_values(id_raw: &str) -> Vec<String> {
    id_raw
        .split('~')
        .map(|s| s.to_string())
        .collect()
}

// NCO-DELETE
pub async fn delete(
    state: Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;
    let reference_foreign_keys = &schemas.1;
    let mut claims = Claims::default();
    if state.require_auth && !state.route_publics.contains(&route){
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

        if !check_access(&claims, &route, "delete") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let id_raw: String = path.into_inner();
    // Rate limiting removed; handled globally

    let isqueue = parameters
        .clone()
        .into_inner()
        .as_object()
        .and_then(|map| map.get("isqueue"))
        .map(|v| *v == Value::Bool(true) || *v == Value::String("true".to_string()))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
        let t0 = std::time::Instant::now();
        // auth check before enqueue
        let mut actor_id_opt: Option<String> = None;
        if state.require_auth && !state.route_publics.contains(&route){
            let req_for_auth = req.clone();
            let claims = match get_user_info_from_token(req_for_auth, state.clone()) {
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
            if !check_access(&claims, &route, "delete") {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Unauthorized".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
            actor_id_opt = Some(claims.id.clone());
        }

        let job = crate::nocode::consumer::WriteJob {
            route: route.clone(),
            op: crate::nocode::consumer::WriteOpKind::Delete { id: id_raw },
            body: Value::Null,
            headers: vec![],
            enqueued_at: chrono::Utc::now().to_rfc3339(),
            actor_id: actor_id_opt,
        };
        if state.write_queue_fast_ack {
            tokio::spawn(async move {
                let _ = crate::nocode::consumer::enqueue_job(&job).await;
            });
            log_output(
                "QUEUE",
                "DELETE-HANDLER",
                route.as_str(),
                format!("queued (async) in {} ms", t0.elapsed().as_millis()),
                true,
            );
            return HttpResponse::Accepted().json(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        } else {
            match crate::nocode::consumer::enqueue_job(&job).await {
                Ok(_) => {
                    log_output(
                        "QUEUE",
                        "DELETE-HANDLER",
                        route.as_str(),
                        format!("queued in {} ms", t0.elapsed().as_millis()),
                        true,
                    );
                    return HttpResponse::Accepted().json(WebResponse {
                        success: true,
                        message: "Enqueued".to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
                Err(e) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Queue error: {}", e),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        }
    }

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

    // check table_schemas.delete.type_delete
    let type_delete = table_schema.del.type_delete.clone();
    
    // Parse composite PK values using ~ delimiter
    let pk_values = parse_pk_values(&id_raw);
    
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

        // Filter by composite PK (parse ~ delimiter)
        let filter = match build_pk_filter(&table_schema.primary_key.columns, &pk_values) {
            Ok(f) => f,
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error building PK filter: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        match ds.preview_update_with(&table_schema.table, Some(&filter), &fields) {
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
                    data: Value::Null,
                });
            }
        }
    } else if type_delete == "hard" {
        // compile DELETE via AST using composite PK (parse ~ delimiter)
        let filter = match build_pk_filter(&table_schema.primary_key.columns, &pk_values) {
            Ok(f) => f,
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error building PK filter: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        match ds.preview_delete(&table_schema.table, Some(&filter)) {
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
                    data: Value::Null,
                });
            }
        }
    }
    log_output("QUERY", "DELETE(AST)", route.as_str(), exec_sql.clone(), true);

    // MongoDB path: no transactions; perform direct update/delete
    if state.db_type == "mongodb" {
        // Build filter using composite PK (parse ~ delimiter)
        let filter = match build_pk_filter(&table_schema.primary_key.columns, &pk_values) {
            Ok(f) => Some(f),
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error building PK filter: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };
        let result = if type_delete == "soft" {
            // patch deleted_at and deleted_by_id
            let mut patch = serde_json::Map::new();
            patch.insert("deleted_at".into(), serde_json::json!(Local::now().to_rfc3339()));
            // type for deleted_by_id
            let deleted_by_type = table_schema
                .columns
                .iter()
                .find(|c| c.name == "deleted_by_id")
                .map(|c| c.type_data.clone())
                .unwrap_or("int".to_string());
            if deleted_by_type.contains("int") {
                if let Ok(n) = claims.id.parse::<i64>() {
                    patch.insert("deleted_by_id".into(), serde_json::json!(n));
                } else {
                    patch.insert("deleted_by_id".into(), serde_json::json!(claims.id.clone()));
                }
            } else if deleted_by_type.contains("float")
                || deleted_by_type.contains("double")
                || deleted_by_type.contains("decimal")
                || deleted_by_type.contains("money")
            {
                if let Ok(n) = claims.id.parse::<f64>() {
                    patch.insert("deleted_by_id".into(), serde_json::json!(n));
                } else {
                    patch.insert("deleted_by_id".into(), serde_json::json!(claims.id.clone()));
                }
            } else {
                patch.insert("deleted_by_id".into(), serde_json::json!(claims.id.clone()));
            }
            state.store.update(&table_schema.table, filter, Value::Object(patch)).await.map(|_| ())
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
                    data: Value::Null,
                })
            }
            Err(err) => {
                HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-DELETE (mongo): {}", err),
                    total_data: 0,
                    data: Value::Null,
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
                data: Value::Null,
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
                    data: Value::Null,
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
                data: Value::Null,
            })
        }
    }
}
