use std::collections::HashSet;
use actix_multipart::Multipart;
use actix_web::{
    web::Data,
    HttpResponse, Responder,
};
use serde_json::{Value};

use crate::{
    auth::{check_access, get_user_info_from_token}, crypt::{encrypt, is_encrypted_string}, database::state::{execute_sql_formula, formula_replace}, helpers::{
        filter_table_schema, find_column_match, multipart_to_json
    }, log::log_output, model::{Column, TableSchema, WebResponse}, AppState
};

// NCO-POST
pub async fn insert(
    state: Data<AppState>,
    route: String,
    table_schemas: Vec<TableSchema>,
    multipart: Multipart,
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


    let mut function_id_split: Vec<String> = Vec::new();
    let mut id: String = String::new();

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

    if table_schema.post.before.contains("SQL:"){
        execute_sql_formula(&state.db, table_schema.post.before, &body, route.as_str()).await;
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

            if col.auto_increment {
                // Jika kolom adalah auto_increment, kita tidak perlu memasukkan nilainya
                return "XXXAUTOINC".to_string();
            }

            let mut value = String::new();
            let mut isformula = false;

            let post_columns: Vec<&str> = table_schema.post.columns.iter().map(|s| s.as_str()).collect();
            let (exists, matched_string) = find_column_match(&post_columns, &col.name);

            // check if col.name is in table_schema.post.columns, and get column name from table_schema.post.columns
            if exists && col.name != "id" {
                // get column name from table_schema.post.columns
                let string_formula = matched_string.unwrap().to_string();
                if string_formula.contains("=") {
                    isformula = true;

                    value = formula_replace(string_formula, &body);
                    value = value.replace(&(col.name.clone().to_string() + "="), "");

                }                
            }


            if col.name == "id" && !col.function.is_empty() {
                // split col.function with "/"
                function_id_split = col.function.split("/").map(|s| s.to_string()).collect();
            }

            if !isformula {
                value = body
                    .get(&col.name)
                    .map(|v| {
                        format!("{}", v)
                            .replace("\"", "")
                            .replace("null", "")
                    })
                    .unwrap_or_default();
            }            

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

            if col.type_data.contains("int") || col.type_data.contains("float") {
                value // Jika angka, tidak pakai kutip
            } else if isformula {
                value.to_string() // Jika string, pakai kutip
            } else {
                format!("'{}'", value) // Jika string, pakai kutip
            }
        })
        .collect();


    // remove element "XXXAUTOINC" from insert_values
    insert_values.retain(|v| v != "XXXAUTOINC");

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

                // get n char from right
                let max_id_str: String = max_id.rsplit('/').next().unwrap_or("0").to_string();

                // remove leading zero
                let _ = max_id_str.trim_start_matches('0');

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
    insert_values.push(state.query_convertor.datetime_now.clone());

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

            if table_schema.post.after.contains("SQL:"){
                execute_sql_formula(&state.db, table_schema.post.after, &body, route.as_str()).await;
            }

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

