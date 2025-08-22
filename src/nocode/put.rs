use actix_multipart::Multipart;
use actix_web::{
    web::{Data, Path},
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::{execute_sql_formula, DbParam},
    helpers::{ filter_table_schema, multipart_to_json },
    log::log_output,
    model::{TableSchema, WebResponse},
    AppState
};
use std::sync::Arc;



// NCO-PUT
pub async fn update(
    state: Data<AppState>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    multipart: Multipart,
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

        if !check_access(&claims, &route, "write") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let body = multipart_to_json(multipart).await.unwrap();
    let id_raw: String = path.into_inner();

    // get body from request and compare with table_schemas.put.columns
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

    if table_schema.put.before.contains("SQL:"){
        execute_sql_formula(&state.db, table_schema.put.before, &body, route.as_str()).await;
    }


    let mut set_clause = "SET ".to_string();
    let mut bind_params: Vec<DbParam> = Vec::new();

    // loop every column in table_schemas.put.columns
    for column in table_schema.put.columns.iter() {
        // loop every key and value in body
        for (key, value) in body.as_object().unwrap().iter() {
            // check if key from body is equal to column
            if key == column {

                // convert value to string
                let mut value_x = format!("{}", value)
                    .replace("\"", "")
                    .replace("null", "");

                // check if value from body is not empty
                if !value_x.is_empty() {

                    // find column properties in table_schemas.columns
                    let col = table_schema
                        .columns
                        .iter()
                        .find(|col| col.name == *column)
                        .unwrap();

                    // check col.encrypt if true then encrypt value
                    if col.encrypt {
                        // check apakah value udah di encrypt
                        let is_encrypted = is_encrypted_string(value_x.clone().as_str());
                        if !is_encrypted {
                            value_x = encrypt(
                                state.encrypt_key.clone(),
                                value_x.clone(),
                            );
                        }
                    }                    

                    // check if value from body is number
                    if col.type_data.contains("int") || col.type_data.contains("float") {
                        if let Ok(n) = value_x.parse::<i64>() { bind_params.push(DbParam::I64(n)); }
                        else if let Ok(f) = value_x.parse::<f64>() { bind_params.push(DbParam::F64(f)); }
                        else { bind_params.push(DbParam::Str(value_x)); }
                        set_clause.push_str(&format!("{} = ?, ", column));
                    } else {
                        bind_params.push(DbParam::Str(value_x));
                        set_clause.push_str(&format!("{} = ?, ", column));
                    }

                    
                }
            }
        }
    }

    // add updated_at to set_clause
    set_clause.push_str(&format!("updated_at = {}, ", state.query_convertor.datetime_now));
    set_clause.push_str("updated_by_id = ?, ");
    bind_params.push(DbParam::I64(claims.id));

    // remove last ", " from set_clause
    set_clause = set_clause[..set_clause.len() - 2].to_string();

    // create query UPDATE sql from table in variable route and structure table in table_schemas where id = id, set set_clause
    let s_sql = format!(
        "UPDATE {} {} WHERE id = ?",
        table_schema.table, set_clause
    );

    // Bind id with type inference (int/str)
    if let Ok(n) = id_raw.parse::<i64>() { bind_params.push(DbParam::I64(n)); }
    else { bind_params.push(DbParam::Str(id_raw)); }

    log_output("QUERY", "PUT", route.as_str(), s_sql.clone(), true);

    match &state.db.query_with_params(&s_sql, bind_params).await {
        Ok(_) => {

            if table_schema.put.after.contains("SQL:"){
                execute_sql_formula(&state.db, table_schema.put.after, &body, route.as_str()).await;
            }

            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Data updated".to_string(),
                total_data: 1,
                data: Value::Null,
            })
        },
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-PUT: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }
}
