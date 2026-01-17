use actix_web::{web, HttpResponse, Responder};
use serde_json::Value;
use std::sync::Arc;
use crate::AppState;
use crate::model::TableSchema;
use crate::nocode::services::data_trace_service::process_trace_request;

pub async fn process(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {

    match process_trace_request(state, parameters, route, table_schema, req).await {
        Ok(res) => HttpResponse::Ok().json(res),
        Err(err) => {
            let mut status = if err == "Unauthorized" || err == "Invalid token" {
                HttpResponse::Unauthorized()
            } else if err.contains("not found") {
                HttpResponse::FailedDependency()
            } else {
                HttpResponse::BadRequest()
            };
            
            status.json(crate::model::WebResponse {
                success: false,
                message: err,
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
