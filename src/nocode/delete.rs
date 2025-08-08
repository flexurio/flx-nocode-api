use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token, Claims}, helpers::
        filter_table_schema
    , log::log_output, model::{TableSchema, WebResponse}, AppState
};


// NCO-DELETE
pub async fn delete(
    state: Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let mut claims = Claims::default();
    if !state.route_publics.contains(&route) {
        claims = match get_user_info_from_token(req, state.clone()) {
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

        if !check_access(&claims, &route, "delete") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let mut id: String = path.into_inner();

    let table_schema = filter_table_schema(&table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // check if id is number or string
    id = if id.parse::<i64>().is_ok() {
        id
    } else {
        format!("'{}'", id)
    };

    // check table_schemas.delete.type_delete
    let type_delete = table_schema.del.type_delete.clone();
    let mut s_sql = "".to_string();

    if type_delete == "soft" {
        s_sql = format!(
            "UPDATE {} SET deleted_at = {}, deleted_by_id = {} WHERE id = {}",
            table_schema.table, state.query_convertor.datetime_now, claims.id, id
        );
    } else if type_delete == "hard" {
        // create query DELETE sql from table in variable route and structure table in table_schemas where id = id
        s_sql = format!(
            "DELETE FROM {} WHERE id = {}",
            table_schema.table, id
        );
    }

    log_output("QUERY", "DELETE", route.as_str(), s_sql.clone(), true);

    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Data deleted".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-DELETE: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }
}

