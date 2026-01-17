use actix_web::{web, Responder};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_patch_service;

pub async fn process_sp(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    data_patch_service::process_patch_request(&state, &parameters, &route, &table_schema, &req).await
}

// Overload/Alternate for when route is passed as String directly if needed,
// but usually the handler function bound to actix needs specific Extractors.
// If we use the closure approach in main.rs, the arguments are extracted there or passed through.
// The previous handlers (get_handler etc) used `web::Path<String>` or similar.
// Let's stick to the closure-friendly signature if that's what main uses,
// OR update main to usage like `get_handler::select`.
