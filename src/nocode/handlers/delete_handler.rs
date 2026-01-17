use actix_web::{web, HttpResponse, Responder};
use serde_json::Value;
use std::sync::Arc;
use crate::AppState;
use crate::model::{TableSchema, ReferenceForeignKey};
use crate::nocode::services::data_delete_service::process_delete_request;

pub async fn delete(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    ref_fks: Arc<Vec<ReferenceForeignKey>>,
    path: web::Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let id_raw = path.into_inner();

    match process_delete_request(state, parameters, route, table_schema, ref_fks, id_raw, req).await {
        Ok(res) => if res.message == "Enqueued" {
             HttpResponse::Accepted().json(res)
        } else {
             HttpResponse::Ok().json(res)
        },
        Err(err) => {
            let mut status = if err == "Unauthorized" || err == "Invalid token" {
                HttpResponse::Unauthorized()
            } else if err.contains("not found") {
                HttpResponse::FailedDependency()
            } else if err.contains("mismatch") {
                HttpResponse::BadRequest()
            } else {
                HttpResponse::InternalServerError()
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
