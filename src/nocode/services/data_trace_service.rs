use actix_web::{web, HttpRequest};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::model::{WebResponse, TableSchema};
use crate::auth::{check_access, get_user_info_from_token};
use crate::nocode::repositories::data_trace_repo::perform_trace_execution;

pub async fn process_trace_request(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: HttpRequest,
) -> Result<WebResponse, String> {

    // 1. Auth Check
    if state.require_auth && !state.route_publics.contains(&route) {
        let claims = get_user_info_from_token(&req, state.clone())
            .map_err(|_| "Invalid token".to_string())?;

        if let Err(e) = check_access(&claims, &req) {
            return Err(format!("Unauthorized: {}", e));
        }
    }

    // 2. Schema Check
    if table_schema.table.is_empty() {
        return Err(format!("Entity {} on folder config/{}.json not found", route, route));
    }

    // 3. Perform Trace (Insert-Select)
    perform_trace_execution(
        &state,
        &table_schema,
        &route,
        &parameters.into_inner(),
    ).await.map(|msg| WebResponse {
        success: true,
        message: msg,
        total_data: 1,
        data: Value::Null,
    })
}
