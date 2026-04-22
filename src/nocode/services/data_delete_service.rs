use actix_web::{web, HttpRequest};
use serde_json::Value;
use std::sync::Arc;
use chrono::Local;

use crate::AppState;
use crate::model::{WebResponse, TableSchema, ReferenceForeignKey, DbType};
use crate::auth::{check_access, get_user_info_from_token, Claims};
use crate::log::log_output;
use crate::helpers::get_client_ip;
use crate::audit::{write_audit, AuditEntry};
use crate::nocode::pk_utils::parse_pk_values;
use crate::nocode::repositories::data_delete_repo::{perform_delete_sql, perform_delete_mongo};

pub async fn process_delete_request(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    ref_fks: Arc<Vec<ReferenceForeignKey>>,
    id_raw: String,
    req: HttpRequest,
) -> Result<WebResponse, String> {

    // 1. Auth Check
    let mut claims = Claims::default();
    if state.require_auth && !state.route_publics.contains(&route) {
        claims = get_user_info_from_token(&req, state.clone())
            .map_err(|_| "Invalid token".to_string())?;

        if let Err(e) = check_access(&claims, &req) {
            return Err(format!("Unauthorized: {}", e));
        }
    }

    // 2. Queueing
    let isqueue = parameters
        .into_inner()
        .as_object()
        .and_then(|map| map.get("isqueue"))
        .map(|v| *v == Value::Bool(true) || *v == Value::String("true".to_string()))
        .unwrap_or(false);

    if state.write_queue_enabled && isqueue {
        let t0 = std::time::Instant::now();
        // actor_id logic
        let actor_id_opt = if state.require_auth && !state.route_publics.contains(&route) {
             Some(claims.id.clone())
        } else {
             None
        };

        let job = crate::nocode::consumer::WriteJob {
            route: route.clone(),
            op: crate::nocode::consumer::WriteOpKind::Delete { id: id_raw.clone() },
            body: Value::Null,
            headers: vec![],
            enqueued_at: Local::now().to_rfc3339(),
            actor_id: actor_id_opt,
        };

        if state.write_queue_fast_ack {
            crate::nocode::consumer::enqueue_job_background(job, "DELETE-HANDLER");
            log_output("QUEUE", "DELETE-HANDLER", &route, format!("queued (async) in {} ms", t0.elapsed().as_millis()), true);
            return Ok(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            }); 
        } else {
             crate::nocode::consumer::enqueue_job(&job).await.map_err(|e| format!("Queue error: {}", e))?;
             log_output("QUEUE", "DELETE-HANDLER", &route, format!("queued in {} ms", t0.elapsed().as_millis()), true);
             return Ok(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // 3. Schema Check
    if table_schema.table.is_empty() {
        return Err(format!("Entity {} on folder config/{}.json not found", route, route));
    }

    let type_delete = table_schema.del.type_delete.clone();
    let is_soft = type_delete == "soft";
    let pk_values = parse_pk_values(&id_raw);

    // 4. Perform Delete
    if state.db_type == DbType::Mongodb {
        perform_delete_mongo(
            &state,
            &table_schema,
            &route,
            &id_raw,
            &pk_values,
            &claims.id,
            is_soft
        ).await?;
    } else {
        perform_delete_sql(
            &state,
            &table_schema,
            &route,
            &id_raw,
            &pk_values,
            &ref_fks,
            &claims.id,
            is_soft
        ).await?;
    }

    // 5. Audit
    let ip_opt = get_client_ip(&req);
    write_audit(&AuditEntry {
        at: Local::now().to_rfc3339(),
        actor_id: claims.id.clone(),
        action: "DELETE",
        route: &route,
        id: Some(&id_raw),
        ip: Some(ip_opt.as_str()),
    });

    Ok(WebResponse {
        success: true,
        message: if is_soft { "Data soft-deleted".to_string() } else { "Data deleted".to_string() },
        total_data: 1,
        data: Value::Null,
    })
}
