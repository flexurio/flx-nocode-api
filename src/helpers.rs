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
pub async fn filter_table_schema(table_schemas: &[TableSchema], route: String) -> TableSchema {
    // Use iterator for better performance instead of loop
    if let Some(schema) = table_schemas.iter().find(|schema| {
        let table_name = if schema.table.contains('.') {
            schema.table.split('.').next_back().unwrap_or(&schema.table)
        } else {
            &schema.table
        };
        table_name == route
    }) {
        let mut table_schema_clone = schema.clone();
        
        // Pre-calculate mandatory parameters
        let deleted_at_param = format!("{}.deleted_at", table_schema_clone.table);
        let params_mandatory = ["page", "sort", "ascending", "limit", "search", "redis"];
        
        // Use HashSet for O(1) lookup instead of O(n) contains
        let existing_params: HashSet<String> = table_schema_clone.get.parameters.iter().cloned().collect();
        
        if !existing_params.contains(&deleted_at_param) {
            table_schema_clone.get.parameters.push(deleted_at_param);
        }
        
        for &param in &params_mandatory {
            if !existing_params.contains(param) {
                table_schema_clone.get.parameters.push(param.to_string());
            }
        }
        
        table_schema_clone
    } else {
        TableSchema::default()
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

// create function convert MultiPart to Json with security and memory optimizations
pub async fn multipart_to_json(mut multipart: Multipart) -> Result<Value, actix_web::Error> {
    let mut json_data = json!({});
    const MAX_FILE_SIZE: usize = 50 * 1024 * 1024; // 50MB limit
    const MAX_FIELD_SIZE: usize = 1024 * 1024; // 1MB limit for text fields

    while let Some(item) = multipart.next().await {
        let mut field = item.map_err(actix_web::Error::from)?;

        let content_disposition = field.content_disposition().cloned();
        let field_name = content_disposition
            .as_ref()
            .and_then(|cd| cd.get_name())
            .map(|name| name.to_string())
            .unwrap_or_else(|| "unnamed".to_string());

        // Validate field name to prevent path traversal
        if field_name.contains("..") || field_name.contains('/') || field_name.contains('\\') {
            return Err(actix_web::error::ErrorBadRequest("Invalid field name"));
        }

        if let Some(filename) = content_disposition
            .as_ref()
            .and_then(|cd| cd.get_filename())
        {
            // File upload handling with size limit
            let mut buffer = Vec::new();
            let mut total_size = 0usize;
            
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                total_size += data.len();
                
                if total_size > MAX_FILE_SIZE {
                    return Err(actix_web::error::ErrorPayloadTooLarge("File too large"));
                }
                
                buffer.extend_from_slice(&data);
            }

            let image_storage = std::env::var("LOC_IMAGE").unwrap_or("DB".to_string());
            if image_storage == "DB" {
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&buffer);
                let mime_type = field
                    .content_type()
                    .map(|t| t.to_string())
                    .unwrap_or("application/octet-stream".to_string());
                
                // Validate MIME type for security
                if !is_safe_mime_type(&mime_type) {
                    return Err(actix_web::error::ErrorBadRequest("Unsupported file type"));
                }
                
                let data_uri = format!("data:{};base64,{}", mime_type, base64_data);
                json_data[field_name] = json!(data_uri);
            } else {
                // save to disk safely under LOC_STATIC/<LOC_IMAGE>
                let safe_name = sanitize_filename::sanitize(filename);
                let static_root = std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
                let file_rel = format!("{}/{}", image_storage.trim_matches('/'), safe_name);
                let file_path = std::path::Path::new(&static_root).join(&file_rel);

                // Validate path to prevent directory traversal
                if !file_path.starts_with(&static_root) {
                    return Err(actix_web::error::ErrorBadRequest("Invalid file path"));
                }

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
            // Text field handling with size limit
            let mut text_data = String::new();
            let mut total_size = 0usize;
            
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                total_size += data.len();
                
                if total_size > MAX_FIELD_SIZE {
                    return Err(actix_web::error::ErrorPayloadTooLarge("Field too large"));
                }
                
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

// Helper function to validate MIME types for security
fn is_safe_mime_type(mime_type: &str) -> bool {
    const ALLOWED_TYPES: &[&str] = &[
        "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp",
        "application/pdf", "text/plain", "application/json",
        "application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ];
    
    ALLOWED_TYPES.contains(&mime_type)
}


pub fn extract_expressions(input: &str) -> HashSet<String> {
    let mut results = HashSet::new();

    // Regex untuk ekspresi dalam kurung kurawal { ... }
    let re_braces = match Regex::new(r"\{([^{}]+)\}") {
        Ok(regex) => regex,
        Err(e) => {
            eprintln!("Failed to compile regex: {}", e);
            return results;
        }
    };

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
