use actix_web::{web, HttpRequest, Responder};
use serde_json::Value;
use std::sync::Arc;
use actix_multipart::Multipart;

use crate::AppState;
use crate::model::{TableSchema, ReferenceForeignKey};
use crate::nocode::services::data_update_service;

#[allow(clippy::too_many_arguments)]
pub async fn update(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    ref_fks: Arc<Vec<ReferenceForeignKey>>,
    multipart: Multipart,
    path: web::Path<String>,
    req: HttpRequest,
) -> impl Responder {
    data_update_service::process_update_request(
        &state,
        &parameters,
        &route,
        &table_schema,
        &ref_fks,
        multipart,
        path,
        &req
    )
    .await
}
