use actix_multipart::Multipart;
use actix_web::{web::Data, HttpResponse, Responder};
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::io::Cursor;
use std::sync::Arc;

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::DbParam,
    helpers::filter_table_schema,
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use chrono::Local;

// CSV/XLSX import into a route table
pub async fn import(
    state: Data<AppState>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    mut multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;

    // AuthZ like POST (write)
    let mut claims = Claims::default();
    if !state.route_publics.contains(&route) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
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

    // Rate-limit
    let ip_key = get_client_ip(&req);
    let limit: u32 = std::env::var("RATE_LIMIT_MUTATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if !RL_WINDOW_MUTATE.check_and_increment(&format!("import:{}:{}", route, ip_key), limit) {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".into(),
            total_data: 0,
            data: Value::Null,
        });
    }

    let table_schema: TableSchema = filter_table_schema(table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Read file and additional fields from multipart
    let max_file_mb: usize = std::env::var("UPLOAD_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let max_file_size = max_file_mb * 1024 * 1024;
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut filename: String = String::new();
    let mut declared_mime: Option<String> = None;
    let mut additional_columns: Map<String, Value> = Map::new();
    
    while let Some(item) = multipart.next().await {
        let mut field = match item {
            Ok(f) => f,
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Multipart error: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };
        let cd = field.content_disposition().cloned();
        let name = cd
            .as_ref()
            .and_then(|c| c.get_name())
            .unwrap_or("");
        
        if name == "file" {
            if let Some(fname) = cd.as_ref().and_then(|c| c.get_filename()) {
                filename = fname.to_string();
            }
            declared_mime = field.content_type().map(|t| t.to_string());
            let mut buf: Vec<u8> = Vec::new();
            let mut total = 0usize;
            while let Some(chunk) = field.next().await {
                let data = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Read chunk error: {}", e),
                            total_data: 0,
                            data: Value::Null,
                        })
                    }
                };
                total += data.len();
                if total > max_file_size {
                    return HttpResponse::PayloadTooLarge().json(WebResponse {
                        success: false,
                        message: "File too large".to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
                buf.extend_from_slice(&data);
            }
            file_bytes = Some(buf);
        } else if !name.is_empty() {
            // Handle additional column fields
            let mut field_value = String::new();
            while let Some(chunk) = field.next().await {
                let data = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Read field '{}' error: {}", name, e),
                            total_data: 0,
                            data: Value::Null,
                        })
                    }
                };
                field_value.push_str(&String::from_utf8_lossy(&data));
            }
            // Store additional column value
            additional_columns.insert(name.to_string(), json!(field_value));
        }
    }

    let file_bytes = match file_bytes { Some(b) => b, None => {
        return HttpResponse::BadRequest().json(WebResponse{
            success:false,
            message:"No file provided (field name 'file')".to_string(),
            total_data:0,
            data: Value::Null,
        });
    }};

    // Detect type (csv or xlsx)
    let lower_name = filename.to_lowercase();
    let is_csv_ext = lower_name.ends_with(".csv");
    let is_xlsx_ext = lower_name.ends_with(".xlsx");
    let sniff = infer::get(&file_bytes);
    let mime_detected = sniff.map(|k| k.mime_type().to_string());
    let is_xlsx_mime = matches!(mime_detected.as_deref(), Some("application/zip"))
        && (is_xlsx_ext
            || matches!(declared_mime.as_deref(), Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")));
    let is_csv_mime = matches!(declared_mime.as_deref(), Some("text/csv")) || is_csv_ext;

    // Parse rows into Vec<Map<String, Value>>
    let rows: Vec<Map<String, Value>> = if is_xlsx_mime || is_xlsx_ext {
        match parse_xlsx(file_bytes) {
            Ok(v) => v,
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Failed to read XLSX: {}", e),
                    total_data: 0,
                    data: Value::Null,
                })
            }
        }
    } else if is_csv_mime || is_csv_ext {
        match parse_csv(file_bytes) {
            Ok(v) => v,
            Err(e) => {
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Failed to read CSV: {}", e),
                    total_data: 0,
                    data: Value::Null,
                })
            }
        }
    } else {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Unsupported file type (expect .csv or .xlsx)".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    };

    if rows.is_empty() {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "No data rows found".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Log additional columns for debugging
    if !additional_columns.is_empty() {
        log_output(
            "INFO",
            "IMPORT",
            &table_schema.table,
            format!("Additional columns from multipart: {:?}", additional_columns),
            true,
        );
    }

    // Build list of insertable columns (skip auto_increment and audit columns)
    let mut base_columns: Vec<&crate::model::Column> = table_schema
        .columns
        .iter()
        .filter(|c| !c.auto_increment)
        .collect();

    // We'll add created_at/created_by_id separately later
    let skip_names = [
        "created_at",
        "updated_at",
        "deleted_at",
        "created_by_id",
        "updated_by_id",
        "deleted_by_id",
    ];
    base_columns.retain(|c| !skip_names.contains(&c.name.as_str()));

    // Prepare columns to include uniformly across rows
    let has_id_col = base_columns.iter().any(|c| c.name == "id");
    let id_fn: Option<String> = table_schema
        .columns
        .iter()
        .find(|c| c.name == "id")
        .map(|c| c.function.clone())
        .filter(|s| !s.is_empty());
    let all_rows_have_id = rows.iter().all(|r| r.get("id").is_some());
    let include_id = has_id_col && (id_fn.is_some() || all_rows_have_id);

    // Final column list (excluding audit; we'll append created_at/created_by_id)
    let final_columns: Vec<&crate::model::Column> = base_columns
        .into_iter()
        .filter(|c| if include_id { true } else { c.name != "id" })
        .collect();

    // Build ID generator context if needed
    let mut id_ctx: Option<(String, usize, i64)> = None; // (prefix, width, next_number)
    if include_id {
        if let Some(func) = id_fn.as_ref() {
            let (prefix, width) = derive_id_prefix_and_width(func);
            // Query once for max existing id with this prefix
            let id_find = prefix.clone();
            let s_sql_max_id = format!(
                "SELECT COALESCE(MAX(id),0) as max_id FROM {} WHERE id like '%{}%' ",
                table_schema.table, id_find
            );
            log_output("QUERY", "GET", table_schema.table.as_str(), s_sql_max_id.clone(), true);
            let max_id: String = match state.db.query(&s_sql_max_id).await {
                Ok(row) if !row.is_empty() => row[0]
                    .get("max_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .to_string(),
                _ => "0".to_string(),
            };
            let last = max_id.rsplit('/').next().unwrap_or("0");
            let next_num: i64 = last.trim_start_matches('0').parse().unwrap_or(0);
            // next number to use will be +1 on first row build
            id_ctx = Some((prefix, width, next_num));
        }
    }

    // Start transaction
    let mut tx = match state.db.begin_transaction().await {
        Ok(t) => t,
        Err(e) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", e),
                total_data: 0,
                data: Value::Null,
            })
        }
    };

    // Chunked bulk insert to avoid excessively long SQL
    let batch_size: usize = std::env::var("IMPORT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let mut inserted: i32 = 0;
    for chunk in rows.chunks(batch_size) {
        // Build VALUES and params
        let mut values_groups: Vec<String> = Vec::with_capacity(chunk.len());
        let mut params: Vec<DbParam> = Vec::with_capacity(chunk.len() * (final_columns.len() + 1));

        for (i_row, row) in chunk.iter().enumerate() {
            // One row worth of placeholders
            let mut placeholders: Vec<String> = Vec::with_capacity(final_columns.len() + 2);

            for col in final_columns.iter() {
                // Compute value - first check additional columns, then row data
                let mut value_str = if let Some(additional_value) = additional_columns.get(&col.name) {
                    // Use value from multipart additional fields
                    json_value_to_string(additional_value)
                } else {
                    // Use value from file data
                    row.get(&col.name)
                        .map(json_value_to_string)
                        .unwrap_or_default()
                };

                // Special handling for id
                if col.name == "id"
                    && value_str.is_empty() {
                        if let Some((ref prefix, width, ref mut next_num)) = id_ctx {
                            *next_num += 1;
                            let num_str = format!("{:0>len$}", *next_num, len = width);
                            value_str = format!("{}/{}", prefix, num_str);
                        } else if !all_rows_have_id {
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: format!("Row {} missing id and no function configured", inserted as usize + i_row + 1),
                                total_data: inserted,
                                data: Value::Null,
                            });
                        }
                    }

                // FK validation when value present
                if !value_str.is_empty() {
                    for fk in table_schema.foreign_keys.iter() {
                        if fk.column == col.name {
                            let ok = check_data_foreign_key(
                                &state,
                                fk.reference_table.clone(),
                                fk.reference_column.clone(),
                                value_str.clone(),
                            )
                            .await;
                            if !ok {
                                let _ = tx.rollback().await;
                                return HttpResponse::BadRequest().json(WebResponse {
                                    success: false,
                                    message: format!(
                                        "Invalid foreign key value '{}' for column '{}' at row {}",
                                        value_str,
                                        col.name,
                                        inserted as usize + i_row + 1
                                    ),
                                    total_data: inserted,
                                    data: Value::Null,
                                });
                            }
                        }
                    }
                }

                // Encrypt if needed
                if col.encrypt && !value_str.is_empty() {
                    let is_encrypted = is_encrypted_string(&value_str);
                    if !is_encrypted {
                        value_str = encrypt(state.encrypt_key.clone(), value_str);
                    }
                }

                // Decide param value (NULL vs default)
                if value_str.is_empty() && col.nullable {
                    placeholders.push("?".into());
                    params.push(DbParam::Null);
                } else if (value_str.is_empty())
                    && (col.type_data.contains("int") || col.type_data.contains("float"))
                {
                    placeholders.push("?".into());
                    if let Ok(n) = "0".parse::<i64>() { params.push(DbParam::I64(n)); }
                    else { params.push(DbParam::Str("0".to_string())); }
                } else {
                    placeholders.push("?".into());
                    if col.type_data.contains("int") || col.type_data.contains("float") {
                        if let Ok(n) = value_str.parse::<i64>() {
                            params.push(DbParam::I64(n));
                        } else if let Ok(f) = value_str.parse::<f64>() {
                            params.push(DbParam::F64(f));
                        } else {
                            params.push(DbParam::Str(value_str));
                        }
                    } else {
                        params.push(DbParam::Str(value_str));
                    }
                }
            }

            // created_at (expr) and created_by_id
            placeholders.push(state.query_converter.datetime_now.clone());
            placeholders.push("?".into());
            params.push(DbParam::Str(claims.id.clone()));

            values_groups.push(format!("({})", placeholders.join(", ")));
        }

        // Build column names + audit
        let mut col_names: Vec<String> = final_columns.iter().map(|c| c.name.clone()).collect();
        col_names.push("created_at".into());
        col_names.push("created_by_id".into());

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_schema.table,
            col_names.join(", "),
            values_groups.join(", ")
        );

        log_output("QUERY", "IMPORT-BULK", &table_schema.table, sql.clone(), true);
        log_output(
            "PARAMS",
            "IMPORT-BULK",
            &table_schema.table,
            format!("{} params", params.len()),
            true,
        );

        if let Err(e) = tx.query_with_params(&sql, params).await {
            let _ = tx.rollback().await;
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Bulk insert error: {} (after {} rows)", e, inserted),
                total_data: inserted,
                data: Value::Null,
            });
        }

        inserted += chunk.len() as i32;
    }

    if let Err(e) = tx.commit().await {
        return HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Commit error: {}", e),
            total_data: inserted,
            data: Value::Null,
        });
    }

    // Audit
    write_audit(&AuditEntry {
        at: Local::now().to_rfc3339(),
        actor_id: claims.id.clone(),
        action: "IMPORT",
        route: &route,
        id: None,
        ip: Some(get_client_ip(&req)).as_deref(),
    });

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: format!("Imported {} rows", inserted),
        total_data: inserted,
        data: Value::Null,
    })
}

// Build prefix and numeric width from ID function for bulk id generation
fn derive_id_prefix_and_width(function: &str) -> (String, usize) {
    let parts: Vec<&str> = function.split('/').collect();
    let mut prefix = String::new();
    let mut width: usize = 0;
    for part in parts.iter() {
        match *part {
            "%Y" => {
                prefix.push('/');
                prefix.push_str(&chrono::Utc::now().format("%Y").to_string());
            }
            "%m" => {
                prefix.push('/');
                prefix.push_str(&chrono::Utc::now().format("%m").to_string());
            }
            "%d" => {
                prefix.push('/');
                prefix.push_str(&chrono::Utc::now().format("%d").to_string());
            }
            p if p.contains("ID") => {
                let s_append = p.replace("ID", "");
                width = s_append.len();
                // number will be appended later
            }
            other => {
                prefix.push('/');
                prefix.push_str(other);
            }
        }
    }
    if !prefix.is_empty() { prefix.remove(0); }
    (prefix, width)
}

fn json_value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "1".to_string() } else { "0".to_string() },
        Value::Null => String::new(),
        other => other.to_string().trim_matches('"').to_string(),
    }
}

// generate_id no longer used in bulk path; keep or remove if needed

fn parse_csv(bytes: Vec<u8>) -> anyhow::Result<Vec<Map<String, Value>>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(Cursor::new(bytes));
    let headers = rdr
        .headers()
        .map(|h| h.iter().map(|s| s.trim().to_string()).collect::<Vec<_>>())?;
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    for result in rdr.records() {
        let rec = result?;
        let mut map = Map::new();
        for (i, val) in rec.iter().enumerate() {
            if let Some(h) = headers.get(i) {
                if !h.is_empty() {
                    map.insert(h.clone(), json!(val.trim())) ;
                }
            }
        }
        if !map.is_empty() { rows.push(map); }
    }
    Ok(rows)
}

fn parse_xlsx(bytes: Vec<u8>) -> anyhow::Result<Vec<Map<String, Value>>> {
    use calamine::{Reader, Xlsx};
    let cursor = Cursor::new(bytes);
    let mut workbook: Xlsx<Cursor<Vec<u8>>> = Xlsx::new(cursor)?;
    let range = workbook
        .worksheet_range_at(0)
        .ok_or_else(|| anyhow::anyhow!("No worksheet"))??;
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut headers: Vec<String> = Vec::new();
    for (r_idx, row) in range.rows().enumerate() {
        if r_idx == 0 {
            headers = row
                .iter()
                .map(|c| c.to_string())
                .map(|s| s.trim().trim_matches('"').to_string())
                .collect();
            continue;
        }
        let mut map = Map::new();
        for (i, cell) in row.iter().enumerate() {
            if let Some(h) = headers.get(i) {
                if !h.is_empty() {
                    // Avoid full enum matching; treat blank text as empty
                    let text = cell.to_string();
                    if !text.trim().is_empty() {
                        map.insert(h.clone(), json!(text.trim()));
                    }
                }
            }
        }
        if !map.is_empty() { rows.push(map); }
    }
    Ok(rows)
}
