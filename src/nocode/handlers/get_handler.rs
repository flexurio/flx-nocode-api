use actix_web::{web, HttpRequest, Responder};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_read_service;

pub async fn select(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: HttpRequest,
) -> impl Responder {
    data_read_service::process_get_request(
        &state,
        &parameters,
        &route,
        &table_schema,
        &req
    )
    .await
}
