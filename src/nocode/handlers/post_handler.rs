use actix_web::{web, HttpResponse, Responder, HttpRequest};
use actix_multipart::Multipart;
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_create_service;

pub async fn insert(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    multipart: Multipart,
    req: HttpRequest,
) -> impl Responder {

    println!("Received insert request for route: {}", route);

    match data_create_service::process_insert_request(
        &state,
        &parameters,
        &route,
        &table_schema,
        multipart,
        &req
    ).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(err_response) => {
            // Map error response to appropriate HTTP status
            if err_response.message == "Unauthorized" || err_response.message == "Invalid token" {
                HttpResponse::Unauthorized().json(err_response)
            } else if err_response.message.contains("not found") {
                HttpResponse::NotFound().json(err_response)
            } else if err_response.message.contains("Missing required field") || err_response.message.contains("Invalid") {
                 HttpResponse::BadRequest().json(err_response)
            } else {
                 HttpResponse::InternalServerError().json(err_response)
            }
        }
    }
}
