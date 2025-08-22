use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token}, helpers::{
        filter_table_schema
    }, log::log_output, model::{TableSchema, WebResponse}, AppState
};
use std::sync::Arc;




// NCO-TRACE
pub async fn process_sp(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !state.route_publics.contains(&route) {
        let claims = match get_user_info_from_token(req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }
    // get parameters value only allowed from table_schema.trace.parameters
    // loop every table_schema.trace.parameters
    
    let table_schema: TableSchema = filter_table_schema(&table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // declare param_sp as Vec<String>
    let mut param_sp: Vec<String> = Vec::new();

    for param in table_schema.patch.parameters.iter() {
        for (key, value) in parameters.clone().into_inner().as_object().unwrap().iter() {
            // check if param contain in key
            if key == param {
                param_sp.push(value.to_string());
            }
        }
    }

    let s_sql = format!(
        "CALL {} ({})",
        table_schema.patch.pre_process_sp,
        param_sp.join(", ")
    );

    log_output("QUERY", "TRACE", route.as_str(), s_sql.clone(), true);

    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Data inserted".to_string(),
            total_data: 1,
            data: Value::Null,
        }),

        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-TRACE: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }

}
