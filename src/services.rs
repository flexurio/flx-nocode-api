use std::collections::HashSet;
use actix_multipart::Multipart;
use actix_web::{
    web::{self, Data, Path},
    HttpResponse, Responder,
};
use serde_json::{json, Value};

use crate::{
    crypt::is_encrypted_string, 
    log::log_output, model::ParamJoin,
    auth::{check_access, get_user_info_from_token},
    crypt::encrypt,
    helpers::{filter_table_schema, generate_table, split_column_operator,validate_table_design, multipart_to_json, sanitize_sql_input},
    model::{Column, TableSchema, WebResponse},
    AppState,
};


// NCO-GET
pub async fn nocode_get(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Vec<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();

    if !check_access(&claims, &route, "read") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }


    // get parameters value only allowed from table_schemas.get.parameters
    // loop every table_schemas.get.parameters
    let mut where_clause: String = "WHERE ".to_string();
    let mut limit_clause: String = "LIMIT ".to_string();
    let mut i_limit = 100;
    let mut pagination_clause: String = "".to_string();
    let mut i_page = 1;
    let mut order_clause: String = "ORDER BY ".to_string();
    let mut order_column = "id".to_string();
    let mut order_type = "ASC".to_string();
    let mut group_clause: String = "GROUP BY ".to_string();
    let mut having_clause: String = "HAVING ".to_string();
    let mut paramjoins: Vec<ParamJoin> = Vec::new();
    let table_schema: TableSchema = filter_table_schema(&table_schemas, route.clone()).await;


    log_output(
        "CONFIGURATION",
        "FILTERED PARAMETERS",
        "filter_table_schema",
        serde_json::to_string(&table_schema.get.parameters).unwrap_or_else(|_| "Failed to serialize TableSchema".to_string()),
        true
    );


    if table_schema.table.is_empty() {
        let message_error = format!("ER01(nocode_get): Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    let mut is_deleted_at = true;

    log_output(
        "CONFIGURATION",
        "PARAMETERS ON ROUTES",
        "TableSchema",
        table_schema.get.parameters.join(", "),
        true
    );


    for param in table_schema.get.parameters.iter() {
        for (key, value) in parameters.clone().into_inner().as_object().unwrap().iter() {
            if key.contains("deleted_at") {
                is_deleted_at = false;
            }

            // check if parameters contains key from table_schemas.get.parameters
            if param == key {

                // check if in PARAMS_PAGINATION then add to pagination_data
                if param == "page" {
                    i_page = value.as_str().unwrap().parse().unwrap();
                } else if param == "sort" {
                    if value != "" {
                        order_column = value.to_string();
                    }
                } else if param == "ascending" {
                    if value != "true" {
                        order_type = "ASC".to_string();
                    } else {
                        order_type = "DESC".to_string();
                    }
                } else if param == "limit" {
                    i_limit = value.as_str().unwrap().parse().unwrap();
                } else if param == "redis" {
                    // check redis
                    println!("Redis: {}", value);
                } else if param == "search" {
                    let value_str = value.as_str().unwrap_or("");
                    let mut search_clause = "( ".to_string();

                    for column in table_schema.primary_key.columns.iter() {
                        if column.contains(".") {
                            search_clause
                                .push_str(&format!("{} LIKE '%{}%' OR ", column, value_str));
                        } else {
                            search_clause.push_str(&format!(
                                "{}.{} LIKE '%{}%' OR ",
                                table_schema.table, column, value_str
                            ));
                        }
                    }

                    //  get column frim table_schema.index.columns
                    for index in table_schema.indexes.iter() {
                        for column in index.columns.iter() {
                            if column.contains(".") {
                                search_clause
                                    .push_str(&format!("{} LIKE '%{}%' OR ", column, value_str));
                            } else {
                                search_clause.push_str(&format!(
                                    "{}.{} LIKE '%{}%' OR ",
                                    table_schema.table, column, value_str
                                ));
                            }
                        }
                    }

                    search_clause = search_clause[..search_clause.len() - 4].to_string();
                    search_clause.push_str(" )");
                    where_clause.push_str(&format!("{} AND ", search_clause));
                } else if param.contains("|") {
                    where_clause.push_str(" ( ");
                    let param_split: Vec<&str> = param.split("|").collect();

                    println!("param_split: {:?}", param_split);

                    // loop every param_split
                    for (idx, param) in param_split.iter().enumerate() {
                        let value_str = value.as_str().unwrap_or("");
                        let (column, operator, value) =
                            split_column_operator(param, &table_schema.table, value_str);

                        if idx == 0 {
                            where_clause
                                .push_str(&format!("{} {} '{}' ", column, operator, value));
                        } else {
                            where_clause
                                .push_str(&format!("OR {} {} '{}' ", column, operator, value));
                        }
                    }

                    where_clause.push_str(" ) AND ");
                } else if param.contains("paramjoin") {
                    // add to paramjoins
                    paramjoins.push(ParamJoin {
                        name: param.to_string().replace(".eq", ""),
                        value: sanitize_sql_input(value.as_str().unwrap_or("").to_string()),
                    });
                } else {
                    let value_str = value.as_str().unwrap_or("");
                    let (column, operator, value) =
                        split_column_operator(param, &table_schema.table, value_str);

                    if value.parse::<i64>().is_ok() || value_str.contains("NULL") {
                        where_clause
                            .push_str(&format!("{} {} {} AND ", column, operator, value));
                    } else {
                        where_clause
                            .push_str(&format!("{} {} '{}' AND ", column, operator, value));
                    }
                }
            }
        }
    }

    // check group by
    for group in table_schema.get.column_groups.iter() {
        group_clause.push_str(&format!("{}, ", group));
    }
    if group_clause.len() > 10 {
        // jika group_clause lebih dari 10, maka hapus ", "
        group_clause = group_clause[..group_clause.len() - 2].to_string();
    } else {
        group_clause = "".to_string();
    }

    // check having
    for having in table_schema.get.having.iter() {
        having_clause.push_str(&format!("{}, ", having));
    }
    if having_clause.len() > 7 {
        // jika having_clause lebih dari 10, maka hapus ", "
        having_clause = having_clause[..having_clause.len() - 2].to_string();
    } else {
        having_clause = "".to_string();
    }

    // check order by
    order_clause.push_str(&format!("{} {} ", order_column, order_type));
    order_clause = order_clause.replace("\"", "");

    // check limit
    limit_clause.push_str(&format!("{}", i_limit));

    // check page
    let offset = (i_page - 1) * i_limit;
    pagination_clause.push_str(&format!("OFFSET {} ", offset));

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

    let table_schema = filter_table_schema(&table_schemas, route.clone()).await;
    let select_columns = table_schema.get.columns.join(", ");



    let joins: Vec<String> = table_schema
        .get
        .join_tables
        .iter()
        .map(|join| {
            // loop every paramjoins and replace join.logical string parameter with value
            let mut logical = join.logical.clone();
            for paramjoin in paramjoins.iter() {
               let paramjoin_value = paramjoin.value.replace("'", "");
                logical = logical.replace(&paramjoin.name, &paramjoin_value);
            }
            format!(
                "{} JOIN {} ON {}",
                join.type_join.to_uppercase(),
                join.table,
                logical
            )
        })
        .collect();

    let join_clause = if joins.is_empty() {
        "".to_string()
    } else {
        format!(" {}", joins.join(" "))
    };
    let s_sql = format!(
        "SELECT {} FROM {} {} {} {} {} {} {} {} ",
        select_columns,
        table_schema.table,
        join_clause,
        where_clause,
        group_clause,
        having_clause,
        order_clause,
        limit_clause,
        pagination_clause
    );

    log_output("QUERY", "GET", route.as_str(), s_sql.clone(), true);

    // get query without WHERE and ORDER BY. to get total data in table
    let s_sql_total = format!(
        "SELECT COUNT(*) as total_data FROM {} {} {} {}",
        table_schema.table, join_clause, where_clause, group_clause
    );

    // get total data from 
    let total_data:i32 = state.db.get_total_rows(&s_sql_total).await.unwrap_or(0);
    let query_result = state.db.query(&s_sql).await;
    match query_result {
        Ok(res) => {
            let result = WebResponse {
                success: true,
                message: "Data found".to_string(),
                total_data,
                data: Value::Array(res),
            };

            HttpResponse::Ok().json(result)
        },
        Err(e) => {
            let res = WebResponse {
                success: false,
                message: format!("Error NCO-GET: {}", e),
                total_data: 0,
                data: Value::Null,
            };
            HttpResponse::InternalServerError().json(res)
    
        },
    }


}

// NCO-TRACE
pub async fn nocode_trace(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Vec<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();
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
    conflict_clause.push_str("updated_at=NOW(), deleted_at=null");

    let table_schema = filter_table_schema(&table_schemas, route.clone()).await;
    let mut select_columns = table_schema.trace.column_selects.join(", ");
    select_columns.push_str(", NOW() as created_at");
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

// NCO-DELETE
pub async fn nocode_delete(
    state: Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();
    if !check_access(&claims, &route, "delete") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let mut id: String = path.into_inner();

    // create query UPDATE sql from table in variable route and structure table in table_schemas where id = id, set deleted_at = NOW(), deleted_by_id = 1
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
        // create query UPDATE sql from table in variable route and structure table in table_schemas where id = id, set deleted_at = NOW(), deleted_by_id = 1
        s_sql = format!(
            "UPDATE {} SET deleted_at = NOW(), deleted_by_id = {} WHERE id = {}",
            table_schema.table, claims.id, id
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


// NCO-POST
pub async fn nocode_post(
    state: Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {

    let mut function_id_split: Vec<String> = Vec::new();
    let mut id: String = String::new();

    let claims = get_user_info_from_token(req, state.clone()).unwrap();
    if !check_access(&claims, &route, "write") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let body = multipart_to_json(multipart).await.unwrap();

    // Generate SQL query INSERT to table in variable route, from data structure table in table_schemas
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

    let skip_columns: HashSet<&str> = [
        "created_at",
        "created_by_id",
        "updated_at",
        "updated_by_id",
        "deleted_at",
        "deleted_by_id",
    ]
    .iter()
    .cloned()
    .collect();

    // **1️⃣ Filter kolom yang valid untuk INSERT**
    let filtered_columns: Vec<&Column> = table_schema
        .columns
        .iter()
        .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()))
        .collect();

    // **2️⃣ Buat daftar kolom untuk INSERT**
    let mut insert_columns: Vec<&str> = filtered_columns
        .iter()
        .map(|col| col.name.as_str())
        .collect();

    // **3️⃣ Buat daftar nilai untuk INSERT**
    let mut insert_values: Vec<String> = filtered_columns
        .iter()
        .map(|col| {

            println!("col: {:?}", col);
            
            if col.name == "id" && !col.function.is_empty() {
                // split col.function with "/"
                function_id_split = col.function.split("/").map(|s| s.to_string()).collect();
            }


            let mut value = body
                .get(&col.name)
                .map(|v| {
                    format!("{}", v)
                        .replace("\"", "")
                        .replace("null", "")
                })
                .unwrap_or_default();

            // check col.encrypt if true then encrypt value
            if col.encrypt {
                // check apakah value udah di encrypt
                let is_encrypted = is_encrypted_string(value.clone().as_str());
                if !is_encrypted {
                    value = encrypt(
                        state.encrypt_key.clone(),
                        value.clone(),
                    );
                }
            }

            if value.is_empty() {
                value = 0.to_string();
            }

            if value.parse::<i64>().is_ok() {
                value // Jika angka, tidak pakai kutip
            } else {
                format!("'{}'", value) // Jika string, pakai kutip
            }
        })
        .collect();

    if !function_id_split.is_empty() {
        // loop every function_id_split
        for function_id in function_id_split.iter() {
            if function_id == "%Y"{
                // get year from now with format YYYY
                let year = chrono::Utc::now().format("%Y").to_string();
                id.push('/');
                id.push_str(&year);
            } else if function_id == "%m" {
                // get month from now with format MM
                let month = chrono::Utc::now().format("%m").to_string();
                id.push('/');
                id.push_str(&month);
            } else if function_id == "%d" {
                // get day from now with format DD
                let day = chrono::Utc::now().format("%d").to_string();
                id.push('/');
                id.push_str(&day);
            } else if function_id.contains("ID"){
                println!("id: {:?}", id);
                let mut id_find = id.clone();
                id_find.remove(0);

                let s_append = function_id.replace("ID", "");
                let len_id = s_append.len();


                // get max id from table from column id with length len_id from left
                let s_sql_max_id = format!("SELECT COALESCE(MAX(id),0) as max_id FROM {} WHERE id like '%{}%' ", table_schema.table, id_find);
                log_output("QUERY", "GET", route.as_str(), s_sql_max_id.clone(), true);
                let max_id: String = match &state.db.query(&s_sql_max_id).await {
                    Ok(row) => row[0].get("max_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    Err(_) => "0".to_string(),
                };
                println!("max_id: {:?}", max_id);
                // get n char from right
                let max_id_str: String = max_id.rsplit('/').next().unwrap_or("0").to_string();
                println!("max_id_str: {:?}", max_id_str);
                // remove leading zero
                let _ = max_id_str.trim_start_matches('0');
                println!("max_id_str: {:?}", max_id_str);
                let max_id: i64 = max_id_str.parse().unwrap_or(0) + 1;
                let max_id_str = format!("{:0>len$}", max_id, len = len_id);
                id.push('/');
                id.push_str(&max_id_str);
            } else {
                id.push('/');
                id.push_str(function_id);
            }
        }

        // remove first "/" from id
        id.remove(0);

        // remove id from insert_columns
        insert_columns.retain(|&x| x != "id");
        insert_columns.push("id"); 
        // remove first index from insert_values
        insert_values.remove(0);
        insert_values.push(format!("'{}'", id)); 


    }

    // **Tambahkan created_at**
    insert_columns.push("created_at"); 
    insert_values.push("NOW()".to_string());

    // **Tambahkan created_by_id**
    insert_columns.push("created_by_id");
    insert_values.push(format!("{}", claims.id));




    let s_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_schema.table,
        insert_columns.join(", "),
        insert_values.join(", ")
    );

    log_output("QUERY", "POST", route.as_str(), s_sql.clone(), true);

    match &state.db.query(&s_sql).await {
        Ok(_) => {
            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Data inserted".to_string(),
                total_data: 1,
                data: Value::Null,
            })
        },
        Err(err) => {
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        },
    }
}

// NCO-PUT
pub async fn nocode_put(
    state: Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    multipart: Multipart,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();
    if !check_access(&claims, &route, "write") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let body = multipart_to_json(multipart).await.unwrap();
    println!("body: {:?}", body);
    let mut id: String = path.into_inner();

    // check if id is number or string
    id = if id.parse::<i64>().is_ok() {
        id
    } else {
        format!("'{}'", id)
    };

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

    let mut set_clause = "SET ".to_string();

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
                    let column_properties = table_schema
                        .columns
                        .iter()
                        .find(|col| col.name == *column)
                        .unwrap();

                    // check col.encrypt if true then encrypt value
                    if column_properties.encrypt {
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
                    if value_x.parse::<i64>().is_ok() {
                        set_clause.push_str(&format!("{} = {}, ", column, value_x));
                    } else {
                        set_clause.push_str(&format!("{} = '{}', ", column, value_x));
                    }
                }
            }
        }
    }

    // add updated_at to set_clause
    set_clause.push_str("updated_at = NOW(), ");
    set_clause.push_str(&format!("updated_by_id = {}, ", claims.id));

    // remove last ", " from set_clause
    set_clause = set_clause[..set_clause.len() - 2].to_string();

    // create query UPDATE sql from table in variable route and structure table in table_schemas where id = id, set set_clause
    let s_sql = format!(
        "UPDATE {} {} WHERE id = {}",
        table_schema.table, set_clause, id
    );

    log_output("QUERY", "PUT", route.as_str(), s_sql.clone(), true);

    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Data updated".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-PUT: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }
}

// NCO-VALIDATE
pub async fn nocode_validate(
    state: Data<AppState>,
    route: String,
    mut table_schemas: Vec<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();
    if !check_access(&claims, &route, "execute") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // get table schema from table_schemas where table = route
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

    table_schemas = vec![validate_table_design(table_schema.clone())];

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Table validated".to_string(),
        total_data: 1,
        data: json!(table_schemas),
    })
}

// NCO-GENERATE-TABLE
pub async fn nocode_generate_table(
    state: web::Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let claims = get_user_info_from_token(req, state.clone()).unwrap();
    if !check_access(&claims, &route, "execute") {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Unauthorized".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

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

    let (sql_create_table, sql_create_index) = generate_table(state.db_type.clone(), &table_schema);
    let mut err_message = String::new();

    log_output(
        "QUERY",
        "GENERATE TABLE",
        route.clone().as_str(),
        sql_create_table.clone(),
        true
    );
    log_output(
        "QUERY",
        "GENERATE INDEX",
        route.clone().as_str(),
        sql_create_index.clone().join("\n"),
        true
    );

    // execute sql_create_table
    match &state.db.query(&sql_create_table).await
    {
        Ok(_) => {
            println!("Table {} created", table_schema.table);
        }
        Err(err) => {
            let error_message = format!(
                "Failed to create table {} with error : {}",
                table_schema.table, err
            );
            err_message = error_message;
        }
    }

    
    // execute sql_create_index
    // loop every sql_create_index
    for sql_create_index in sql_create_index.iter() {
        log_output(
            "QUERY",
            "GENERATE INDEX",
            route.clone().as_str(),
            sql_create_index.clone(),
            true
        );


        match &state.db.query(sql_create_index).await
        {
            Ok(_) => {
                println!("Index {} created", table_schema.table);
            }
            Err(err) => {
                err_message = format!(
                    "{} \n
                    Failed to create index {} with error : {}",
                    err_message, table_schema.table, err
                );
            }
        }        

    }
    

    if !err_message.is_empty() {
        HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: err_message,
            total_data: 0,
            data: Value::Null,
        })
    } else {
        HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Table created".to_string(),
            total_data: 1,
            data: Value::Null,
        })
    }
}

