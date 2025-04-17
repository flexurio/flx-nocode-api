use std::collections::HashSet;

use crate::{crypt::is_encrypted_string, db::concat_column_values, log::log_output, model::ParamJoin};
use actix_multipart::Multipart;
use actix_web::{
    web::{self, Data, Path},
    HttpResponse, Responder,
};
use anyhow::Result;
use base64::{self, Engine};
use futures::StreamExt;
use rand::Rng;
use serde_json::{json, Value};

use crate::{
    auth::{check_access, create_token, get_user_info_from_token},
    crypt::{decrypt, encrypt},
    helpers::{
        filter_table_schema, generate_table, split_column_operator,
        validate_table_design,
    },
    model::{Column, TableSchema, WebResponse},
    AppState,
};

fn sanitize_sql_input(input: String) -> String {
    input
        .replace("'", "''")       // escape single quotes (SQL standard)
        .replace("--", "")        // remove SQL comment syntax
        .replace(";", "")         // prevent query stacking
        .replace("\"", "")        // remove double quotes
        .replace("\\", "")        // prevent backslash escape (esp. in MySQL)
        .replace("/*", "")        // remove block comment start
        .replace("*/", "")        // remove block comment end
        .replace("#", "")         // MySQL comment
        .replace("`", "")         // MySQL identifier escape
        .replace(" OR ", " ")     // remove logic operators
        .replace(" or ", " ")
        .replace(" AND ", " ")
        .replace(" and ", " ")
        .replace("=", "")         // remove equal signs
        .replace("(", "")         // remove open parenthesis
        .replace(")", "")         // remove close parenthesis
        .replace("%", "")         // remove wildcards in LIKE
        .replace("_", "")         // remove underscore wildcard
        .replace("\u{0000}", "")  // remove null byte
}

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

    let s_sql = format!(
        "UPDATE {} SET deleted_at = NOW(), deleted_by_id = {} WHERE id = {}",
        table_schema.table, claims.id, id
    );

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

// create function convert MultiPart to Json
async fn multipart_to_json(mut multipart: Multipart) -> Result<Value, actix_web::Error> {
    let mut json_data = json!({});

    while let Some(item) = multipart.next().await {
        let mut field = item.map_err(actix_web::Error::from)?;

        let content_disposition = field.content_disposition().cloned();
        let field_name = content_disposition
            .as_ref()
            .and_then(|cd| cd.get_name())
            .map(|name| name.to_string())
            .unwrap_or_else(|| "unnamed".to_string());

        if let Some(filename) = content_disposition
            .as_ref()
            .and_then(|cd| cd.get_filename())
        {
            let mut buffer = Vec::new();
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                buffer.extend_from_slice(&data);
            }

            // check env IMAGE_STORAGE
            // if IMAGE_STORAGE is DB than convert file to base64 else save to disk that defined in IMAGE_STORAGE

            let image_storage = std::env::var("LOC_IMAGE").unwrap_or("DB".to_string());
            if image_storage == "DB" {
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&buffer);
                let mime_type = field
                    .content_type()
                    .map(|t| t.to_string())
                    .unwrap_or("application/octet-stream".to_string());
                let data_uri = format!("data:{};base64,{}", mime_type, base64_data);
                json_data[field_name] = json!(data_uri);
            } else {
                // save to disk
                let file_path = format!("{}/{}", image_storage, filename);
                std::fs::write(&file_path, &buffer)?;
                let base_url =
                    std::env::var("BASE_URL").unwrap_or("http://localhost:8080".to_string());
                let url = format!("{}/{}", base_url, file_path);
                log_output(
                    "QUERY",
                    "POST IMAGE",
                    field_name.clone().as_str(),
                    url.clone(),
                    true
                );
                json_data[field_name] = json!(url);
            }
        } else {
            let mut text_data = String::new();
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                text_data.push_str(&String::from_utf8_lossy(&data));
            }
            json_data[field_name] = match serde_json::from_str(&text_data) {
                Ok(parsed) => parsed,
                Err(_) => json!(text_data),
            };
        }
    }

    Ok(json_data)
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

    let (sql_create_table, sql_create_index) = generate_table(&table_schema);
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
        sql_create_index.clone(),
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
    match &state.db.query(&sql_create_index).await
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

// NCO-GENERATE-TABLE
pub async fn login(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {
    // get username password from req Authorization Basic
    let auth_split: Vec<&str> = req
        .headers()
        .get("Authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .split(" ")
        .collect();
    let auth_decoded = base64::engine::general_purpose::STANDARD
        .decode(auth_split[1])
        .unwrap();
    let auth_str = String::from_utf8(auth_decoded).unwrap();
    let auth_str_split: Vec<&str> = auth_str.split(":").collect();

    // check if username and password is valid from mysql
    let s_sql = format!(
        "SELECT id, name, CAST(password as CHAR(255)) as password FROM flx_users WHERE email = '{}' AND enabled=1 LIMIT 1;",
        auth_str_split[0]
    );
    log_output("QUERY", "POST", "login", s_sql.clone(), true);

    let (password_db, id_user, name) = match &state.db.query(&s_sql).await {
        Ok(row) =>  {
            let password = row[0].get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            println!("id: {:?}", row[0].get("id"));
            let id = row[0].get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
    
            let name = row[0].get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
    
            (password, id, name)
        },
        Err(_) => ("".to_string(), 0_i64, "".to_string()),
    };

    let decrypt_password = decrypt(state.encrypt_key.clone(), password_db);

    if auth_str_split[1] != decrypt_password {
        return HttpResponse::Unauthorized().json(WebResponse {
            success: false,
            message: "Login Failed".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // query to table flx_roles and save to variable roles
    let s_sql = format!(
        "SELECT CONCAT(endpoint,'/', role) as endpoint_role
         FROM flx_roles
         WHERE id_users = {}",
        id_user
    );

    log_output("QUERY", "POST", "flx_roles", s_sql.clone(), true);

    let roles = state.db.query(&s_sql).await.unwrap_or_default();

    let roles_data = concat_column_values(roles,"endpoint_role", ",");

    let token = create_token(id_user, name, state.clone(), roles_data);
    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Login Success".to_string(),
        total_data: 1,
        data: json!(token.await),
    })
}

// NCO-POST
pub async fn register(state: Data<AppState>, multipart: Multipart) -> impl Responder {
    let body = multipart_to_json(multipart).await.unwrap();

    if body["email"] == "" || body["password"] == "" || body["name"] == "" || body["phone"] == "" {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Email and Password is required".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let encrypt_password = encrypt(
        state.encrypt_key.clone(),
        body["password"].as_str().unwrap().to_string(),
    );

    // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
    let s_sql = format!(
        "INSERT INTO flx_users (email, phone,  password, name, created_at, updated_at, enabled) VALUES ('{}', '{}', '{}', '{}', NOW(), NOW(), 1)",
        body["email"], body["phone"], encrypt_password, body["name"]
    ).replace("\"", "");

    log_output("QUERY", "POST", "register", s_sql.clone(), true);

    // execute sql
    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Register Success".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-POST: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    }
}

// NCO-POST
pub async fn generate_users(state: Data<AppState>) -> impl Responder {

    // read sql from file db/mysql/create-flx_users.sql
    let db_file_path = format!("db/{}/create-flx_users.sql", state.db_type);
    let mut s_sql = std::fs::read_to_string(db_file_path)
        .expect("Failed to read SQL file")
        .replace("\"", "");

    log_output("QUERY", "POST", "generate/table/flx_users", s_sql.clone(), true);

    // execute sql
    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Generate Table users".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-POST: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    };

    // // role
    // 1 → Delete
    // 2 → Write (Insert/Update)
    // 4 → Read
    // 8 → Execute (Eksekusi query/prosedur)

    // 1 (Delete) + 2 (Write) + 4 (Read) + 8 (Execute)    = 15 FULL ACCESS
    // 1 (delete) + 2 (write) + 4 (read)        = 7

    // read sql from file db/mysql/create-flx_users.sql
    let db_file_path = format!("db/{}/create-flx_roles.sql", state.db_type);
    s_sql = std::fs::read_to_string(db_file_path)
        .expect("Failed to read SQL file")
        .replace("\"", "");

    log_output("QUERY", "POST", "generate/table/flx_roles", s_sql.clone(), true);

    // execute sql
    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Generate Table users".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-POST: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    };

    // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
    let mut s_sql = "CREATE INDEX idx_flx_users_enabled ON flx_users(enabled);".to_string().replace("\"", "");

    log_output("QUERY", "POST", "generate/table/users", s_sql.clone(), true);

    // execute sql
    match &state.db.query(&s_sql).await {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Generate Table users".to_string(),
            total_data: 1,
            data: Value::Null,
        }),
        Err(err) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Error NCO-POST: {}", err),
            total_data: 0,
            data: Value::Null,
        }),
    };


    // guery to flx_users where name = "Flexurio Admin"
    s_sql = "SELECT id FROM flx_users WHERE email = 'admin';".to_string().replace("\"", "");
    let mut id_user: i64 = match &state.db.query(&s_sql).await {
        Ok(row) => {
            // check if row is empty
            if row.is_empty() {
                0                
            } else {
                row[0].get("id").and_then(|v| v.as_i64()).unwrap_or(0)
            }
        },
        Err(_) => 0,
    };

    log_output("QUERY", "POST", "generate/table/users", s_sql.clone(), true);


    if id_user == 0 {
        id_user = 1;
        // create string number
        let random_pass = rand::rng().random_range(1000..9999).to_string();
        let encrypt_password = encrypt(state.encrypt_key.clone(), random_pass.clone());

        println!("==========================================");
        println!("Your admin Password: {:?}", random_pass);
        println!("==========================================");


        // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
        s_sql = format!(
            "INSERT INTO flx_users 
                (id, email, phone,  password, name, created_at, updated_at, enabled) 
            VALUES ({},'admin', '5758', '{}', 'Admin Flexurio', NOW(), NOW(), 1)",
            id_user, encrypt_password
        ).replace("\"", "");

        log_output("EXEC", "POST", "generate/table/users", s_sql.clone(), true);


        // execute sql
        match &state.db.query(&s_sql).await {
            Ok(_) => HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Generate Table users".to_string(),
                total_data: 1,
                data: Value::Null,
            }),
            Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err),
                total_data: 0,
                data: Value::Null,
            }),
        };



        s_sql = format!(
            "INSERT INTO flx_roles 
                (id_users, endpoint, role, created_at) 
            VALUES ({}, 'flx_users', 15, NOW())", 
            id_user
        ).replace("\"", "");
        log_output("EXEC", "POST", "generate/table/users", s_sql.clone(), true);

        // execute sql
        match &state.db.query(&s_sql).await {
            Ok(_) => HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Generate Table users".to_string(),
                total_data: 1,
                data: Value::Null,
            }),
            Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err),
                total_data: 0,
                data: Value::Null,
            }),
        };

        s_sql = format!(
            "INSERT INTO flx_roles 
                (id_users, endpoint, role, created_at) 
            VALUES ({}, 'flx_roles', 15, NOW())", 
            id_user
        ).replace("\"", "");
        
        // execute sql
        match &state.db.query(&s_sql).await {
            Ok(_) => HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Generate Table users".to_string(),
                total_data: 1,
                data: Value::Null,
            }),
            Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err),
                total_data: 0,
                data: Value::Null,
            }),
        };        
    }

    

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Generate Table users".to_string(),
        total_data: 1,
        data: Value::Null,
    })

}
