use actix_multipart::Multipart;
use base64::Engine;
use tokio::io::AsyncWriteExt;
use colored::Colorize;
use futures::StreamExt;
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashSet;

use crate::{log::log_output, ISDEBUG};

pub fn cetak_label(host: String, port: u16) {
    // print version from cargo.toml

    println!("\n\nFlexurio ");

    println!(
        "Server started at http://{}:{}",
        host.green(),
        port.to_string().green()
    );
    if *ISDEBUG {
        println!("\n{}\n", "<   Running in DEBUG mode   >".on_red());
    }
}

// Whitelist of safe MIME prefixes
fn is_safe_mime_type(mime: &str) -> bool {
    // Common safe types: images, pdf, text, json, csv, excel openxml, zip
    let allowed = [
        "image/", // png, jpeg, gif, webp, svg+xml
        "application/pdf",
        "text/plain",
        "application/json",
        "text/csv",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
    ];
    allowed.iter().any(|p| {
        if p.ends_with('/') {
            mime.starts_with(p)
        } else {
            mime == *p
        }
    })
}

// create function to split column and operator
pub fn split_column_operator(key: &str, s_table: &str, value: &str) -> (String, String, String) {
    let parts: Vec<&str> = key.split('.').collect();
    let n = parts.len();
    // If key doesn't have expected pattern, fall back to s_table.key = value
    if n < 2 {
        let col = format!("{}.{}", s_table, key);
        return (col, "=".to_string(), value.to_string());
    }

    let mut column = parts[n - 2].to_string();
    let opertr = parts[n - 1].to_string();

    if n >= 3 {
        column = format!("{}.{}", parts[n - 3], column);
    } else {
        column = format!("{}.{}", s_table, column);
    }

    let operator = operator_query(&opertr);
    let operator = if operator.is_empty() { "=".to_string() } else { operator };

    let value = if operator.eq_ignore_ascii_case("like") {
        format!("%{}%", value)
    } else {
        value.to_string()
    };

    (column, operator, value)
}

// Extract client IP reliably (supports proxies) with safe fallbacks
pub fn get_client_ip(req: &actix_web::HttpRequest) -> String {
    // Check common proxy headers first (take the first IP when multiple)
    if let Some(ip) = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.split(',').map(|s| s.trim()).find(|s| !s.is_empty()))
    {
        return ip.to_string();
    }
    if let Some(real) = req
        .headers()
        .get("x-real-ip")
        .and_then(|h| h.to_str().ok())
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        return real.to_string();
    }
    
    
    // Fallback to peer_addr
    req.peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
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
        // new operators
        "nin" => "nin",
        "between" => "between",
        _ => "",
    };

    operator.to_string()
}

// create function convert MultiPart to Json with security and memory optimizations
pub async fn multipart_to_json(mut multipart: Multipart) -> Result<Value, actix_web::Error> {
    let mut json_data = json!({});
    // Limits (configurable via env)
    let max_file_mb: usize = std::env::var("UPLOAD_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let max_file_size: usize = max_file_mb * 1024 * 1024;
    let max_field_kb: usize = std::env::var("UPLOAD_TEXT_LIMIT_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);
    let max_field_size: usize = max_field_kb * 1024;
    let max_files: usize = std::env::var("UPLOAD_MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let max_fields: usize = std::env::var("UPLOAD_MAX_FIELDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    // Allowed extensions (optional)
    let allowed_ext: Option<HashSet<String>> = std::env::var("UPLOAD_EXT_ALLOW").ok().map(|v| {
        v.split(',')
            .filter_map(|s| {
                let t = s.trim().to_lowercase();
                if t.is_empty() {
                    None
                } else {
                    Some(t)
                }
            })
            .collect()
    });

    let mut file_count = 0usize;
    let mut field_count = 0usize;

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
            file_count += 1;
            if file_count > max_files {
                return Err(actix_web::error::ErrorPayloadTooLarge("Too many files"));
            }

            // MIME validation (declared by client); verify later with sniffing
            let client_mime = field
                .content_type()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "application/octet-stream".to_string());
            if !is_safe_mime_type(&client_mime) {
                return Err(actix_web::error::ErrorBadRequest("Unsupported file type"));
            }

            // Extension validation (if provided)
            let safe_name = sanitize_filename::sanitize(filename);
            if let Some(ref allow) = allowed_ext {
                let disallowed = std::path::Path::new(&safe_name)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|s| s.to_lowercase())
                    .filter(|ext| !allow.contains(ext))
                    .is_some();
                if disallowed {
                    return Err(actix_web::error::ErrorBadRequest(
                        "File extension not allowed",
                    ));
                }
            }

            let image_storage = std::env::var("LOC_IMAGE").unwrap_or("DB".to_string());
            if image_storage == "DB" {
                // Base64 (still buffered) — keep strict size limit
                let mut buffer = Vec::new();
                let mut total_size = 0usize;
                let mut sniff_buf: Vec<u8> = Vec::with_capacity(8192);
                while let Some(chunk) = field.next().await {
                    let data = chunk?;
                    total_size += data.len();
                    if total_size > max_file_size {
                        return Err(actix_web::error::ErrorPayloadTooLarge("File too large"));
                    }
                    if sniff_buf.len() < 8192 {
                        let take = 8192 - sniff_buf.len();
                        sniff_buf.extend_from_slice(&data[..data.len().min(take)]);
                    }
                    buffer.extend_from_slice(&data);
                }
                if let Some(kind) = infer::get(&sniff_buf) {
                    let detected = kind.mime_type();
                    if !is_safe_mime_type(detected) {
                        return Err(actix_web::error::ErrorBadRequest(
                            "Detected MIME not allowed",
                        ));
                    }
                    // if allowed_ext is set, ensure detected extension is allowed
                    if let Some(ref allow) = allowed_ext {
                        let ext = kind.extension();
                        if !allow.contains(&ext.to_lowercase()) {
                            return Err(actix_web::error::ErrorBadRequest(
                                "Detected extension not allowed",
                            ));
                        }
                    }
                }
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&buffer);
                let data_uri = format!("data:{};base64,{}", client_mime, base64_data);
                json_data[field_name] = json!(data_uri);
            } else {
                // Stream to disk under LOC_STATIC/<LOC_IMAGE>
                let static_root =
                    std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
                let file_rel = format!("{}/{}", image_storage.trim_matches('/'), safe_name);
                let file_path = std::path::Path::new(&static_root).join(&file_rel);

                // Ensure directory
                if let Some(parent) = file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                // Defensive path check
                if !file_path.starts_with(&static_root) {
                    return Err(actix_web::error::ErrorBadRequest("Invalid file path"));
                }

                let mut f = match tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&file_path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        return Err(actix_web::error::ErrorInternalServerError(format!(
                            "Write open failed: {}",
                            e
                        )))
                    }
                };
                let mut total_size = 0usize;
                let mut sniff_buf: Vec<u8> = Vec::with_capacity(8192);
                let mut detected_checked = false;
                while let Some(chunk) = field.next().await {
                    let data = chunk?;
                    total_size += data.len();
                    if total_size > max_file_size {
                        return Err(actix_web::error::ErrorPayloadTooLarge("File too large"));
                    }

                    // collect first bytes for sniffing
                    if !detected_checked {
                        if sniff_buf.len() < 8192 {
                            let take = 8192 - sniff_buf.len();
                            sniff_buf.extend_from_slice(&data[..data.len().min(take)]);
                        }
                        if sniff_buf.len() >= 32 || data.is_empty() {
                            // have enough to detect
                            if let Some(kind) = infer::get(&sniff_buf) {
                                let detected = kind.mime_type();
                                if !is_safe_mime_type(detected) {
                                    let _ = tokio::fs::remove_file(&file_path).await;
                                    return Err(actix_web::error::ErrorBadRequest(
                                        "Detected MIME not allowed",
                                    ));
                                }
                                if let Some(ref allow) = allowed_ext {
                                    let ext = kind.extension();
                                    if !allow.contains(&ext.to_lowercase()) {
                                        let _ = tokio::fs::remove_file(&file_path).await;
                                        return Err(actix_web::error::ErrorBadRequest(
                                            "Detected extension not allowed",
                                        ));
                                    }
                                }
                            }
                            detected_checked = true;
                        }
                    }
                    if let Err(e) = f.write_all(&data).await {
                        return Err(actix_web::error::ErrorInternalServerError(format!(
                            "Write failed: {}",
                            e
                        )));
                    }
                }
                let base_url = std::env::var("BASE_URL")
                    .unwrap_or_else(|_| "http://localhost:8080".to_string());
                let url = format!("{}/static/{}", base_url.trim_end_matches('/'), file_rel);
                log_output(
                    "QUERY",
                    "POST IMAGE",
                    field_name.clone().as_str(),
                    url.clone(),
                    true,
                );
                json_data[field_name] = json!(url);
            }
        } else {
            // Text field handling with size limit and count
            field_count += 1;
            if field_count > max_fields {
                return Err(actix_web::error::ErrorPayloadTooLarge("Too many fields"));
            }

            let mut text_data = String::new();
            let mut total_size = 0usize;
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                total_size += data.len();
                if total_size > max_field_size {
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

// (removed duplicate is_safe_mime_type; using the public version defined earlier)

// Static compiled regex for performance - compiled only once instead of every call
use once_cell::sync::Lazy;
static EXPR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\{([^{}]+)\}").expect("Failed to compile expression regex")
});

pub fn extract_expressions(input: &str) -> HashSet<String> {
    let mut results = HashSet::with_capacity(8); // Pre-allocate for common case

    for cap in EXPR_REGEX.captures_iter(input) {
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
        if col
            .split_once('=')
            .map(|(name, _)| name == target)
            .unwrap_or(false)
        {
            return (true, Some(*col));
        }
    }
    (false, None)
}
