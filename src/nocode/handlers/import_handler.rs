use actix_multipart::Multipart;
use actix_web::{web, HttpResponse, Responder};
use serde_json::Value;
use std::sync::Arc;
use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_import_service::process_import_request;

pub async fn import(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    
    match process_import_request(state, parameters, route, table_schema, multipart, req).await {
        Ok(res) => HttpResponse::Ok().json(res),
        Err(res) => {
            // Map WebResponse back to specific HTTP Status codes if possible
            // The service returns WebResponse in both Ok and Err
            // We can infer status from message or use default BadRequest
            if res.message == "Unauthorized" || res.message == "Invalid token" {
                 return HttpResponse::Unauthorized().json(res);
            } else if res.message.contains("not found") {
                 return HttpResponse::FailedDependency().json(res);
            } else if res.message.contains("too large") {
                 return HttpResponse::PayloadTooLarge().json(res);
            } else if res.message == "No content" {
                 return HttpResponse::NoContent().json(res);
            }
             HttpResponse::BadRequest().json(res)
        }
    }
}
