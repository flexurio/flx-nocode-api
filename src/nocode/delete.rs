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
    database::state::DbParam,
    helpers::filter_table_schema,
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::process_foreign_keys_delete_update,
    AppState,
};
use chrono::Local;
use std::sync::Arc;

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
    // Rate-limit
    let ip_key = get_client_ip(&req);
    let limit: u32 = std::env::var("RATE_LIMIT_MUTATE_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    if !RL_WINDOW_MUTATE.check_and_increment(&format!("delete:{}:{}", route, ip_key), limit) {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::Null,
        });
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
    let mut s_sql = "".to_string();
    let mut bind_params: Vec<DbParam> = Vec::new();

    if type_delete == "soft" {
        s_sql = format!(
            "UPDATE {} SET deleted_at = {}, deleted_by_id = ? WHERE id = ?",
            table_schema.table, state.query_converter.datetime_now
        );
        bind_params.push(DbParam::Str(claims.id.clone()));
    } else if type_delete == "hard" {
        // create query DELETE sql parameterized by id
        s_sql = format!("DELETE FROM {} WHERE id = ?", table_schema.table);
    }

    // Bind id by type
    if let Ok(n) = id_raw.clone().parse::<i64>() {
        bind_params.push(DbParam::I64(n));
    } else {
        bind_params.push(DbParam::Str(id_raw.clone()));
    }

    log_output("QUERY", "DELETE", route.as_str(), s_sql.clone(), true);

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

    // Execute main delete query
    match transaction
        .query_with_params(&s_sql, bind_params.clone())
        .await
    {
        Ok(_) => {
            let (is_fk_ok, err_message) = process_foreign_keys_delete_update(
                "DELETE", // "DELETE" or "UPDATE"
                state.clone(),
                &mut transaction,
                reference_foreign_keys,
                claims.id.clone(),
                id_raw.clone(),
                "".to_string(), // for UPDATE
            )
            .await;

            if is_fk_ok {
                // Commit transaction if all operations succeeded
                match transaction.commit().await {
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
                let _ = transaction.rollback().await;
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
            let _ = transaction.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-DELETE: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
