use base64::Engine;
use colored::Colorize;
use std::collections::HashSet;
use actix_multipart::Multipart;
use serde_json::{json, Value};
use futures::StreamExt;
use regex::Regex;


use crate::{log::log_output, model::TableSchema, ISDEBUG};

pub fn cetak_label(host: String, port: u16) {
    println!("\n\n");
    println!("{}",           r"  __________       _________    _____     _______          _____   _____  ".green());
    println!("{}",           r" (________  |     (________ \  / __  |   |  _____ \       (_____) / ___ \ ".green());
    println!("{}",           r"  _____   | |      _____   \ \/ /  | |   | | ____) )         _   | |   | |".green());
    println!("{}",           r" |  ___)  | |     |  ___)   )  (   | |   | |(____ (         | |  | |   | |".green());
    println!("{}",   format!(r" | |      | |___  | |____  / /\ \  | |___| |     | |  {}   _| |_ | |___| |", "__".red()).green());
    println!("{}", format!(r" |_|      |_____) |______)/_/  \_\  \______|     |_| {} (_____) \_____/", "(__)".red()).green());
    println!("{}",           r"                                 AI-Powered No-Code, Rust-Level Robustness".red());
    println!("\n");
    println!("Server started at http://{}:{}", host.green(), port.to_string().green());
    if *ISDEBUG {
        println!("\n{}\n", "<   Running in DEBUG mode   >".on_red());
    }

}

// create function to get data from table_schemas where table is equal to route
pub async fn filter_table_schema(table_schemas:&[TableSchema], route:String) -> TableSchema {
        let mut result = Vec::new();
        

        for table_schema in table_schemas {

            // check if table_schema.table contains .
            // if it does, split the string by . and get the last element
            // if it does not, use the table_schema.table as is
            let table_schema_table = if table_schema.table.contains('.') {
                let table_schema_table_split: Vec<&str> = table_schema.table.split('.').collect();
                table_schema_table_split[table_schema_table_split.len() - 1].to_string()
            } else {
                table_schema.table.clone()
            };

            if table_schema_table == route {
                
                // tambah parameter mandatory
                let mut table_schema_clone = table_schema.clone();

                let deleted_at_param = format!("{}.deleted_at", table_schema_clone.table);

                let existing_params: HashSet<_> = table_schema_clone.get.parameters.iter().cloned().collect();
                
                if !existing_params.contains(&deleted_at_param) {
                    table_schema_clone.get.parameters.push(deleted_at_param);
                }
            
                let params_mandatory = &[
                    "page", "sort", "ascending", "limit", "search", "redis",
                ];
                    
                for param in params_mandatory {
                    if !existing_params.contains(*param) {
                        table_schema_clone.get.parameters.push(param.to_string());
                    }
                }
                
                result.push(table_schema_clone);
            }
        }
        if result.is_empty() {
                TableSchema::default()
        } else {
            result[0].clone()
        }
}

// create function to split column and operator
pub fn split_column_operator(key: &str, s_table: &str, value: &str) -> (String, String, String) {
        println!("key: {}", key);
        let key_splitted: Vec<&str> = key.split('.').collect();
        let jml_key = key_splitted.len();
    
        let mut column = key_splitted[jml_key - 2].to_string();
        let opertr = key_splitted[jml_key - 1].to_string();
    
        if jml_key >= 3 {
            column = format!("{}.{}", key_splitted[jml_key - 3], column);
        } else {
            column = format!("{}.{}", s_table, column);
        }
    
        let operator = operator_query(&opertr);
    
        let value = if operator == "like" {
            format!("%{}%", value)
        } else {
            value.to_string()
        };
    
        (column, operator, value)
}

pub fn operator_query(symbol: &str) -> String {
        let operator = match symbol {
                "eq" => "=",
                "like" => "like",
                "lt" => "<",
                "lte" => "<=",
                "gt" => ">",
                "gte" => ">=",
                "is" => "is",
                _ => ""
        };

        operator.to_string()
}

// create function convert MultiPart to Json
pub async fn multipart_to_json(mut multipart: Multipart) -> Result<Value, actix_web::Error> {
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
                // save to disk safely under LOC_STATIC/<LOC_IMAGE>
                let safe_name = sanitize_filename::sanitize(filename);
                let static_root = std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
                let file_rel = format!("{}/{}", image_storage.trim_matches('/'), safe_name);
                let file_path = std::path::Path::new(&static_root).join(&file_rel);

                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                std::fs::write(&file_path, &buffer)?;
                let base_url = std::env::var("BASE_URL").unwrap_or("http://localhost:8080".to_string());
                let url = format!("{}/static/{}", base_url.trim_end_matches('/'), file_rel);
                log_output("QUERY","POST IMAGE", field_name.clone().as_str(), url.clone(), true);
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


pub fn extract_expressions(input: &str) -> HashSet<String> {
    let mut results = HashSet::new();

    // Regex untuk ekspresi dalam kurung kurawal { ... }
    let re_braces = Regex::new(r"\{([^{}]+)\}").unwrap();

    for cap in re_braces.captures_iter(input) {
        let expr = cap[1].to_string();
        results.insert(expr.clone());

        // Cek apakah di dalam ekspresi ada ekspresi lain, seperti [{...}]
        let nested_expr = extract_expressions(&expr);
        results.extend(nested_expr);
    }

    results
}

pub fn find_column_match<'a>(columns: &'a [&str], target: &str) -> (bool, Option<&'a str>) {
    for col in columns.iter() {
        if let Some((name, _)) = col.split_once('=') {
            if name == target {
                return (true, Some(*col));
            }
        }
    }
    (false, None)
}
