use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::helpers::extract_request_payload;
use crate::model::{ReferenceForeignKey, TableSchema};
use crate::nocode::services::data_update_service;

#[allow(clippy::too_many_arguments)]
pub async fn update(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    ref_fks: Arc<Vec<ReferenceForeignKey>>,
    payload: web::Payload,
    path: web::Path<String>,
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

    data_update_service::process_update_request(
        &state,
        &parameters,
        &route,
        &table_schema,
        &ref_fks,
        body,
        path,
        &req,
    )
    .await
}
