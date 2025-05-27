use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token}, helpers::{
        filter_table_schema, split_column_operator
    }, log::log_output, model::{TableSchema, WebResponse}, AppState
};



// NCO-TRACE
pub async fn process(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Vec<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {

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

    // get parameters value only allowed from table_schema.trace.parameters
    // loop every table_schema.trace.parameters
    let mut where_clause: String = "WHERE ".to_string();
    

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

    let mut is_deleted_at = true;

    for param in table_schema.trace.parameters.iter() {
        for (key, value) in parameters.clone().into_inner().as_object().unwrap().iter() {
            if key.contains("deleted_at") {
                is_deleted_at = false;
            }

            // check if parameters contains key from table_schema.trace.parameters
            if param == key {
                if param.contains("|") {
                    where_clause.push_str(" ( ");
                    let param_split: Vec<&str> = param.split("|").collect();
                    // loop every param_split
                    for (idx, param) in param_split.iter().enumerate() {
                        let value_str = value.as_str().unwrap_or("");
                        let (column, operator, value) =
                            split_column_operator(param, &table_schema.table, value_str);

                        if idx == 0 {
                            where_clause.push_str(&format!("{} {} '{}' ", column, operator, value));
                        } else {
                            where_clause
                                .push_str(&format!("OR {} {} '{}' ", column, operator, value));
                        }
                    }

                    where_clause.push_str(" ) AND ");
                } else {
                    let value_str = value.as_str().unwrap_or("");
                    let (column, operator, value) =
                        split_column_operator(param, &table_schema.table, value_str);

                    if value.parse::<i64>().is_ok() || value_str.contains("NULL") {
                        where_clause.push_str(&format!("{} {} {} AND ", column, operator, value));
                    } else {
                        where_clause.push_str(&format!("{} {} '{}' AND ", column, operator, value));
                    }
                }
            }
        }
    }

    // check table insert
    let table_insert_clause = table_schema.trace.insert_into.clone();

    // check column insert
    let mut column_insert_clause = "(".to_string();
    for column in table_schema.trace.column_inserts.iter() {
        column_insert_clause.push_str(&format!("{}, ", column));
    }
    column_insert_clause.push_str("created_at) ");

    // check group by
    let mut group_clause = format!("GROUP BY {}", table_schema.trace.column_groups.join(", "));
    if group_clause.len() < 10 {
        group_clause = "".to_string();
    }

    // jika gak ada deleted_at di where_clause, maka tambahkan deleted_at IS NULL
    if is_deleted_at {
        where_clause.push_str(format!("{}.deleted_at IS NULL AND ", route).as_str());
    }

    // remove last " AND " from where_clause
    if where_clause.len() > 6 {
        where_clause = where_clause[..where_clause.len() - 5].to_string();
    } else {
        where_clause = "".to_string();
    }

    let mut conflict_clause = "".to_string();
    for column in table_schema.trace.column_conflicts.iter() {
        conflict_clause.push_str(&format!("{}=VALUES({}), ", column, column));
    }
    conflict_clause.push_str(format!("updated_at={}, deleted_at=null", state.query_convertor.datetime_now).as_str());

    let table_schema = filter_table_schema(&table_schemas, route.clone()).await;
    let mut select_columns = table_schema.trace.column_selects.join(", ");
    select_columns.push_str(format!(", {} as created_at", state.query_convertor.datetime_now).as_str());
    let joins: Vec<String> = table_schema
        .trace
        .join_tables
        .iter()
        .map(|join| {
            format!(
                "{} JOIN {} ON {}",
                join.type_join.to_uppercase(),
                join.table,
                join.logical
            )
        })
        .collect();

    let join_clause = if joins.is_empty() {
        "".to_string()
    } else {
        format!(" {}", joins.join(" "))
    };
    let s_sql = format!(
        "INSERT INTO {} {}
        SELECT {}
        FROM {} {} {} {}
        ON DUPLICATE KEY UPDATE {}",
        table_insert_clause,
        column_insert_clause,
        select_columns,
        table_schema.table,
        join_clause,
        where_clause,
        group_clause,
        conflict_clause
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
