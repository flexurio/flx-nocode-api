use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::audit::{write_audit, AuditEntry};
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token},
    database::state::DbParam,
    helpers::filter_table_schema,
    log::log_output,
    model::{TableSchema, WebResponse},
    AppState,
};
use chrono::Local;
use std::sync::Arc;

// NCO-TRACE
pub async fn process_sp(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !state.route_publics.contains(&route) {
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

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }
    // Rate-limit
    let ip_key = req
        .clone()
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|| "unknown".into());
    let limit: u32 = std::env::var("RATE_LIMIT_MUTATE_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    if !RL_WINDOW_MUTATE.check_and_increment(&format!("patch:{}:{}", route, ip_key), limit) {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // get parameters value only allowed from table_schema.trace.parameters
    // loop every table_schema.trace.parameters

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

    // declare param_sp as Vec<String>
    let mut param_sp: Vec<String> = Vec::new();
    let mut bind_params: Vec<DbParam> = Vec::new();

    for param in table_schema.patch.parameters.iter() {
        for (key, value) in parameters
            .clone()
            .into_inner()
            .as_object()
            .unwrap_or(&serde_json::Map::new())
            .iter()
        {
            // check if param contain in key
            if key == param {
                let s = value.to_string().trim_matches('"').to_string();
                // Try parse numeric else treat as string
                if let Ok(n) = s.parse::<i64>() {
                    bind_params.push(DbParam::I64(n));
                } else if let Ok(f) = s.parse::<f64>() {
                    bind_params.push(DbParam::F64(f));
                } else {
                    bind_params.push(DbParam::Str(s));
                }
                param_sp.push("?".into());
            }
        }
    }

    let s_sql = format!(
        "CALL {} ({})",
        table_schema.patch.pre_process_sp,
        param_sp.join(", ")
    );

    log_output("QUERY", "PATCH", route.as_str(), s_sql.clone(), true);

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

    match transaction.query_with_params(&s_sql, bind_params).await {
        Ok(_) => {
            transaction.commit().await.ok();
            // Audit
            write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: 0, // unknown actor from claims in this scope
                action: "PATCH",
                route: &route,
                id: None,
                ip: req
                    .clone()
                    .peer_addr()
                    .map(|a| a.ip().to_string())
                    .as_deref(),
            });
            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Processes executed".to_string(),
                total_data: 1,
                data: Value::Null,
            })
        }

        Err(err) => {
            transaction.rollback().await.ok();
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PATCH: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
