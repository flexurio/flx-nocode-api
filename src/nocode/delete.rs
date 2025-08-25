use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token, Claims}, database::state::DbParam, helpers:: filter_table_schema, log::log_output, model::{ReferenceForeignKey, TableSchema, WebResponse}, AppState
};
use std::{sync::Arc};


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
        claims = match get_user_info_from_token(req, state.clone()) {
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
        bind_params.push(DbParam::I64(claims.id));
    } else if type_delete == "hard" {
        // create query DELETE sql parameterized by id
        s_sql = format!("DELETE FROM {} WHERE id = ?", table_schema.table);
    }

    // Bind id by type
    if let Ok(n) = id_raw.clone().parse::<i64>() { bind_params.push(DbParam::I64(n)); }
    else { bind_params.push(DbParam::Str(id_raw.clone())); }

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
    match transaction.query_with_params(&s_sql, bind_params.clone()).await {
        Ok(_) => {
            
            // process reference_foreign_keys delete
            let mut failed_fk_operations = Vec::new();
            
            for fk in reference_foreign_keys.iter() {
                let data_table_delete = fk.on_delete_action.clone();
                let mut bind_params_fk: Vec<DbParam> = Vec::new();
                let s_sql_fk;

                if fk.on_delete_action.type_delete == "soft" {
                    s_sql_fk = format!(
                        "UPDATE {} SET deleted_at = {}, deleted_by_id = ? WHERE {} = ?",
                        data_table_delete.table, state.query_converter.datetime_now, fk.on_delete_action.column
                    );
                    bind_params_fk.push(DbParam::I64(claims.id));
                } else if fk.on_delete_action.type_delete == "hard" {
                    // create query DELETE sql parameterized by id
                    s_sql_fk = format!("DELETE FROM {} WHERE {} = ?", data_table_delete.table, fk.on_delete_action.column);
                } else {
                    continue; // skip if type_delete is not soft or hard
                }

                // Bind id by type
                if let Ok(n) = id_raw.clone().parse::<i64>() { 
                    bind_params_fk.push(DbParam::I64(n)); 
                } else { 
                    bind_params_fk.push(DbParam::Str(id_raw.clone())); 
                }

                log_output("FOREIGN KEY", "DELETE", "QUERY", s_sql_fk.clone(), true);
                log_output(
                    "FOREIGN KEY",
                    "DELETE",
                    "PARAM",
                    format!("{:?}", bind_params_fk),
                    true,
                );

                match transaction.query_with_params(&s_sql_fk, bind_params_fk).await {
                    Ok(_) => {
                        log_output("SUCCESS", "FOREIGN KEY DELETE", route.as_str(), s_sql_fk.clone(), true);
                    },
                    Err(err) => {
                        log_output("ERR QUERY", "DELETE", route.as_str(), err.to_string(), false);
                        failed_fk_operations.push(format!("Failed to delete from {}: {}", data_table_delete.table, err));
                    },
                }
            }

            if failed_fk_operations.is_empty() {
                // Commit transaction if all operations succeeded
                match transaction.commit().await {
                    Ok(_) => {
                        HttpResponse::Ok().json(WebResponse {
                            success: true,
                            message: "Data deleted successfully".to_string(),
                            total_data: 1,
                            data: Value::Null,
                        })
                    },
                    Err(err) => {
                        HttpResponse::InternalServerError().json(WebResponse {
                            success: false,
                            message: format!("Error committing transaction: {}", err),
                            total_data: 0,
                            data: Value::Null,
                        })
                    }
                }
            } else {
                // Rollback transaction due to foreign key failures
                let _ = transaction.rollback().await;
                HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Transaction rolled back due to foreign key failures: {}", failed_fk_operations.join("; ")),
                    total_data: 0,
                    data: Value::Null,
                })
            }
        },
        Err(err) => {
            // Rollback transaction due to main delete failure
            let _ = transaction.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-DELETE: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        },
    }
}

