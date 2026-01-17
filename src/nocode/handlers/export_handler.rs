use actix_multipart::Multipart;
use actix_web::{web, Responder};
use std::sync::Arc;

use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_export_service;

pub async fn export(
    state: web::Data<AppState>,
    route: String,
    table_schema: Arc<TableSchema>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    data_export_service::process_export_request(&state, &route, &table_schema, multipart, &req).await
}
