use actix_web::{web, HttpResponse, Responder, HttpRequest};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::helpers::extract_request_payload;
use crate::model::TableSchema;
use crate::nocode::services::data_create_service;

pub async fn insert(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    payload: web::Payload,
    req: HttpRequest,
) -> impl Responder {
    let body = match extract_request_payload(&req, payload).await {
        Ok(b) => b,
        Err(e) => {
            return HttpResponse::BadRequest().json(crate::nocode::services::web_err(format!(
                "Failed to parse request payload: {}",
                e
            )));
        }
    };

    match data_create_service::process_insert_request(
        &state,
        &parameters,
        &route,
        &table_schema,
        body,
        &req,
    )
    .await
    {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(err_response) => {
            // Map error response to appropriate HTTP status
            if err_response.message == "Unauthorized" || err_response.message == "Invalid token" {
                HttpResponse::Unauthorized().json(err_response)
            } else if err_response.message.contains("not found") {
                HttpResponse::NotFound().json(err_response)
            } else if err_response.message.contains("Missing required field")
                || err_response.message.contains("Invalid")
                || err_response.message.contains("must be an array")
            {
                HttpResponse::BadRequest().json(err_response)
            } else {
                HttpResponse::InternalServerError().json(err_response)
            }
        }
    }
}
