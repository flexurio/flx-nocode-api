use actix_multipart::Multipart;
use actix_web::{web, Responder};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_export_service;

pub async fn export_get(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    data_export_service::process_export_request(
        &state,
        &route,
        &table_schema,
        Some(&parameters),
        None,
        &req,
    )
    .await
}

pub async fn export_post(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    data_export_service::process_export_request(
        &state,
        &route,
        &table_schema,
        Some(&parameters),
        Some(multipart),
        &req,
    )
    .await
}

// Legacy export handler kept for backward compatibility
#[allow(dead_code)]
pub async fn export(
    state: web::Data<AppState>,
    route: String,
    table_schema: Arc<TableSchema>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    data_export_service::process_export_request(
        &state,
        &route,
        &table_schema,
        None,
        Some(multipart),
        &req,
    )
    .await
}
