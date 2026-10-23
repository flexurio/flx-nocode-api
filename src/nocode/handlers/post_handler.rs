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
        // Status + client-safe text derived centrally (409 for unique violations,
        // 422 for FK, 503 for contention, SQL never echoed to the client).
        Err(err_response) => crate::error::ApiError::respond_message(&err_response.message),
    }
}
