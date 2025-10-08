use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use sonic_rs::{Value, json};

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip; // retained for audit/logs
// Global rate limiting now handled in main.rs (removed RL_WINDOW_MUTATE)
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    helpers::filter_table_schema,
    log::log_output,
    model::{TableSchema, WebResponse},
    nocode::foreign_key::process_foreign_keys_delete_update_txstore,
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{InsertValue};
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::storage::ast::Val as AstVal;
use crate::database::state::DbParam;
use crate::json_compat::value_from_f64;

/// Internal lightweight structure for soft delete data (AST-based)
/// Avoids heavy JSON manipulation until final serialization
#[derive(Debug, Clone)]
struct DeleteData { fields: Vec<(String, AstVal)> }

impl DeleteData {
    fn new() -> Self { Self { fields: Vec::with_capacity(6) } }
    fn add_field(&mut self, key: String, value: AstVal) { self.fields.push((key, value)); }
    fn to_insert_values(&self) -> Vec<(String, InsertValue)> {
        let mut out = Vec::with_capacity(self.fields.len());
        for (k,v) in &self.fields {
            let param = match v {
                AstVal::I64(n) => DbParam::I64(*n),
                AstVal::F64(f) => DbParam::F64(*f),
                AstVal::Bool(b) => DbParam::Bool(*b),
                AstVal::Str(s) => DbParam::Str(s.clone()),
                AstVal::Null => DbParam::Null,
            };
            out.push((k.clone(), InsertValue::Param(param)));
        }
        out
    }
}

#[inline]
fn push_deleted_by(delete_data: &mut DeleteData, deleted_by_type: &str, claims_id_i64: Option<i64>, claims_id_f64: Option<f64>, claims_id: &str) {
    if deleted_by_type.contains("int") {
        if let Some(n) = claims_id_i64 { delete_data.add_field("deleted_by_id".into(), AstVal::I64(n)); }
        else { delete_data.add_field("deleted_by_id".into(), AstVal::Str(claims_id.to_string())); }
    } else if deleted_by_type.contains("float") || deleted_by_type.contains("double") || deleted_by_type.contains("decimal") || deleted_by_type.contains("money") {
        if let Some(f) = claims_id_f64 { delete_data.add_field("deleted_by_id".into(), AstVal::F64(f)); }
        else if let Some(i) = claims_id_i64 { delete_data.add_field("deleted_by_id".into(), AstVal::I64(i)); }
        else { delete_data.add_field("deleted_by_id".into(), AstVal::Str(claims_id.to_string())); }
    } else {
        delete_data.add_field("deleted_by_id".into(), AstVal::Str(claims_id.to_string()));
    }
}

// NCO-DELETE
pub async fn delete(
    state: Data<AppState>,
    route: Arc<str>,
    table_schemas: Arc<Vec<TableSchema>>,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
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

    if !check_access(&claims, route.as_ref(), "delete") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let id_raw: String = path.into_inner();
    let id_raw_i64 = id_raw.parse::<i64>().ok(); // parsed once
    // Rate limiting: removed (handled by global middleware). Keep IP for potential auditing.
    let _ip_key = get_client_ip(&req);

    let table_schema = filter_table_schema(&table_schemas, route.as_ref());
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
        // Use lightweight AST structure for soft delete data
        let mut delete_data = DeleteData::new();
        
        // Decide types for deleted_by_id
        let deleted_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "deleted_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
        
        log_output("TYPE", "deleted_by_id", route.as_ref(), deleted_by_type.clone(), true);

        // Add deleted_by_id with proper type
        let claims_id_i64 = claims.id.parse::<i64>().ok();
        let claims_id_f64 = if claims_id_i64.is_none() { claims.id.parse::<f64>().ok() } else { None };
        push_deleted_by(&mut delete_data, &deleted_by_type, claims_id_i64, claims_id_f64, &claims.id);
        
        // Convert DeleteData to InsertValue vector for SQL compilation
        let mut fields = delete_data.to_insert_values();
        
        // Add deleted_at as raw DB expression (server-side timestamp)
    fields.push(("deleted_at".into(), InsertValue::Raw(state.query_converter.datetime_now.clone())));

        // Filter by id typed
    let id_filter_val = id_raw_i64.map(QV::I64).unwrap_or_else(|| QV::Str(id_raw.clone()));
    match state.sql_store.preview_update_with(&table_schema.table, Some(&QF::Eq("id".into(), id_filter_val)), &fields) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-soft)", route.as_ref(), sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-soft)", route.as_ref(), format!("{:?}", params), true);
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
    let id_filter_val = id_raw_i64.map(QV::I64).unwrap_or_else(|| QV::Str(id_raw.clone()));
    match state.sql_store.preview_delete(&table_schema.table, Some(&QF::Eq("id".into(), id_filter_val))) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-hard)", route.as_ref(), sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-hard)", route.as_ref(), format!("{:?}", params), true);
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
    log_output("QUERY", "DELETE(AST)", route.as_ref(), exec_sql.clone(), true);

    // MongoDB path: no transactions; perform direct update/delete
    if state.db_type == "mongodb" {
        // Build filter by id with numeric inference (reused by soft/hard)
        let id_filter_val = id_raw_i64.map(QV::I64).unwrap_or_else(|| QV::Str(id_raw.clone()));
        let filter = Some(QF::Eq("id".into(), id_filter_val));
        let result = if type_delete == "soft" {
            // Construct soft-delete patch document
            let mut delete_data = DeleteData::new();
            let deleted_by_type = table_schema
                .columns
                .iter()
                .find(|c| c.name == "deleted_by_id")
                .map(|c| c.type_data.clone())
                .unwrap_or("int".to_string());
            let claims_id_i64 = claims.id.parse::<i64>().ok();
            let claims_id_f64 = if claims_id_i64.is_none() { claims.id.parse::<f64>().ok() } else { None };
            push_deleted_by(&mut delete_data, &deleted_by_type, claims_id_i64, claims_id_f64, &claims.id);
            // timestamp
            let now_ts = Local::now().to_rfc3339();
            delete_data.add_field("deleted_at".into(), AstVal::Str(now_ts));
            // Convert to JSON
            let mut patch_obj = sonic_rs::Object::new();
            for (k, v) in &delete_data.fields {
                let json_val = match v {
                    AstVal::I64(n) => json!(*n),
                    AstVal::F64(f) => value_from_f64(*f),
                    AstVal::Bool(b) => json!(*b),
                    AstVal::Str(s) => json!(s.as_str()),
                    AstVal::Null => Value::default(),
                };
                patch_obj.insert(k.as_str(), json_val);
            }
            state.store.update(&table_schema.table, filter, Value::from(patch_obj)).await.map(|_| ())
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
                route.to_string(),
                &mut tx,
                &crate::SCHEMA_REF_FOREIGN_KEYS,
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
                            actor_id: claims.id.clone(), // needs owned String for audit struct
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
