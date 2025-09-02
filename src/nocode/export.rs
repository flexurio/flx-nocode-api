use actix_multipart::Multipart;
use actix_web::{http::header, web, HttpResponse, Responder};
use serde_json::Value;
use std::sync::Arc;

use crate::audit::{write_audit, AuditEntry};
use crate::auth::{check_access, get_user_info_from_token, Claims};
use crate::database::state::{DbParam};
use crate::helpers::{filter_table_schema, multipart_to_json, split_column_operator};
use crate::log::log_output;
use crate::model::{ParamJoin, ReferenceForeignKey, TableSchema, WebResponse};
use crate::AppState;
use chrono::Local;

/// Export data for a route using filters provided via multipart fields.
/// Fields supported (same as nocode GET):
/// - filters according to schema.get.parameters
/// - optional: type = csv | xlsx (default: csv)
/// - optional: filename = <base name without extension>
pub async fn export(
    state: web::Data<AppState>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;

    // AuthZ like GET (read)
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
        if !check_access(&claims, &route, "read") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // Parse multipart fields to JSON (re-use secure helper)
    // Accept both actual multipart body and empty body (treat as no filters)
    let body_json: Value = match multipart_to_json(multipart).await {
        Ok(v) => v,
        Err(_) => Value::Object(serde_json::Map::new()),
    };

    let mut export_type = body_json
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("csv")
        .to_lowercase();
    if export_type != "xlsx" && export_type != "csv" {
        export_type = "csv".to_string();
    }
    let filename_base = body_json
        .get("filename")
        .and_then(|v| v.as_str())
        .unwrap_or(&route);

    let table_schema: TableSchema = filter_table_schema(table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!(
            "Entity {} on folder config/{}.json not found",
            route, route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Build SQL using same rules as nocode::get
    let mut where_clause: String = "WHERE ".to_string();
    let i_limit = body_json
        .get("limit")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
        .map(|v| v.clamp(1, 100_000)) // allow bigger limit for export
        .unwrap_or(10_000);
    let mut order_clause: String = "ORDER BY ".to_string();
    let mut order_column = table_schema.get.order_by.clone().join(", ");
    let mut order_type = "ASC".to_string();
    let mut group_clause: String = "GROUP BY ".to_string();
    let mut having_clause: String = "HAVING ".to_string();
    let mut paramjoins: Vec<ParamJoin> = Vec::new();
    let mut bind_params: Vec<DbParam> = Vec::new();
    let mut is_deleted_at = true;

    // Use only allowed parameters from schema
    let allowed: std::collections::HashSet<String> =
        table_schema.get.parameters.iter().cloned().collect();
    if let Some(map) = body_json.as_object() {
        for (key, value) in map {
            if key.contains("deleted_at") {
                is_deleted_at = false;
            }
            if !allowed.contains(key) {
                continue;
            }
            if key == "sort" {
                let mut val = value.as_str().unwrap_or("").to_string();
                if !val.is_empty() {
                    val = val
                        .chars()
                        .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ')
                        .collect();
                    order_column = val;
                }
            } else if key == "ascending" {
                let v = value.as_str().unwrap_or("");
                order_type = if v.eq_ignore_ascii_case("true") {
                    "ASC".into()
                } else {
                    "DESC".into()
                };
            } else if key == "limit" {
                // handled above
            } else if key == "page" {
                // ignore for export; export is not paginated unless user adds explicit OFFSET below
            } else if key == "search" {
                let value_str = value.as_str().unwrap_or("").to_string();
                if !value_str.is_empty() {
                    let mut search_clause = "( ".to_string();
                    for column in table_schema.primary_key.columns.iter() {
                        if column.contains('.') {
                            search_clause.push_str(&format!("{} LIKE ? OR ", column));
                            bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                        } else {
                            search_clause.push_str(&format!(
                                "{}.{} LIKE ? OR ",
                                table_schema.table, column
                            ));
                            bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                        }
                    }
                    for index in table_schema.indexes.iter() {
                        for column in index.columns.iter() {
                            if column.contains('.') {
                                search_clause.push_str(&format!("{} LIKE ? OR ", column));
                                bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                            } else {
                                search_clause.push_str(&format!(
                                    "{}.{} LIKE ? OR ",
                                    table_schema.table, column
                                ));
                                bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                            }
                        }
                    }
                    search_clause = search_clause[..search_clause.len() - 4].to_string();
                    search_clause.push_str(" )");
                    where_clause.push_str(&format!("{} AND ", search_clause));
                }
            } else if key.contains("|") {
                where_clause.push_str(" ( ");
                let param_split: Vec<&str> = key.split('|').collect();
                let value_str = value.as_str().unwrap_or("").to_string();
                for (idx, key_part) in param_split.iter().enumerate() {
                    let (column, operator, val) =
                        split_column_operator(key_part, &table_schema.table, &value_str);
                    if idx == 0 {
                        where_clause.push_str(&format!("{} {} ? ", column, operator));
                    } else {
                        where_clause.push_str(&format!("OR {} {} ? ", column, operator));
                    }
                    // best-effort numeric binding
                    if let Ok(n) = val.parse::<i64>() {
                        bind_params.push(DbParam::I64(n));
                    } else if let Ok(f) = val.parse::<f64>() {
                        bind_params.push(DbParam::F64(f));
                    } else {
                        bind_params.push(DbParam::Str(val));
                    }
                }
                where_clause.push_str(" ) AND ");
            } else if key.contains("paramjoin") {
                paramjoins.push(ParamJoin {
                    name: key.to_string().replace(".eq", ""),
                    value: value.as_str().unwrap_or("").to_string(),
                });
            } else {
                let value_str = value.as_str().unwrap_or("").to_string();
                if value_str.is_empty() { continue; }
                let (column, operator, val) =
                    split_column_operator(key, &table_schema.table, &value_str);
                if value_str.eq_ignore_ascii_case("NULL") {
                    where_clause.push_str(&format!("{} {} NULL AND ", column, operator));
                } else if let Ok(n) = val.parse::<i64>() {
                    where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                    bind_params.push(DbParam::I64(n));
                } else if let Ok(f) = val.parse::<f64>() {
                    where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                    bind_params.push(DbParam::F64(f));
                } else {
                    where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                    bind_params.push(DbParam::Str(val));
                }
            }
        }
    }

    // GROUP BY
    for group in table_schema.get.column_groups.iter() {
        group_clause.push_str(&format!("{}, ", group));
    }
    if group_clause.len() > 10 {
        group_clause = group_clause[..group_clause.len() - 2].to_string();
    } else {
        group_clause = "".to_string();
    }

    // HAVING (schema-provided only)
    for having in table_schema.get.having.iter() {
        having_clause.push_str(&format!("{}, ", having));
    }
    if having_clause.len() > 7 {
        having_clause = having_clause[..having_clause.len() - 2].to_string();
    } else {
        having_clause = "".to_string();
    }

    // ORDER BY (sanitize against allowed columns)
    if order_column.is_empty() {
        order_clause = "".to_string();
    } else {
        let allowed_cols: Vec<&str> = table_schema
            .get
            .columns
            .iter()
            .map(|s| s.as_str())
            .chain(
                table_schema
                    .indexes
                    .iter()
                    .flat_map(|idx| idx.columns.iter().map(|s| s.as_str())),
            )
            .collect();
        let sanitized: String = order_column
            .split(',')
            .map(|c| c.trim())
            .filter(|c| allowed_cols.contains(c))
            .collect::<Vec<&str>>()
            .join(", ");
        if sanitized.is_empty() {
            order_clause = "".to_string();
        } else {
            order_clause.push_str(&format!("{} {} ", sanitized, order_type));
        }
    }

    // Default soft-delete filter when not present
    if is_deleted_at {
        where_clause.push_str(format!("{}.deleted_at IS NULL AND ", route).as_str());
    }
    // Trim trailing AND
    if where_clause.len() > 6 {
        where_clause = where_clause[..where_clause.len() - 5].to_string();
    } else {
        where_clause = "".to_string();
    }

    // LIMIT (export large by default, but still bounded)
    let limit_clause = format!("LIMIT {}", i_limit);

    // JOIN clause
    let joins: Vec<String> = table_schema
        .get
        .join_tables
        .iter()
        .map(|join| {
            let mut logical = join.logical.clone();
            for paramjoin in paramjoins.iter() {
                let safe_val: String = paramjoin
                    .value
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                    .collect();
                logical = logical.replace(&paramjoin.name, &safe_val);
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

    let select_columns = table_schema.get.columns.join(", ");
    let s_sql = format!(
        "SELECT {} FROM {} {} {} {} {} {}",
        select_columns,
        table_schema.table,
        join_clause,
        where_clause,
        group_clause,
        having_clause,
        order_clause,
    );
    let s_sql_final = format!("{} {}", s_sql, limit_clause);

    log_output("QUERY", "EXPORT", route.as_str(), s_sql_final.clone(), true);
    log_output(
        "PARAMS",
        "EXPORT",
        route.as_str(),
        format!("{:?}", bind_params),
        true,
    );

    let rows = match state
        .db
        .query_with_params(&s_sql_final, bind_params.clone())
        .await
    {
        Ok(res) => res,
        Err(e) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error EXPORT query: {}", e),
                total_data: 0,
                data: Value::Null,
            })
        }
    };

    // Convert rows to a stable vector of maps for export
    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    if let Some(first) = rows.first() {
        if let Some(obj) = first.as_object() {
            headers = obj.keys().cloned().collect();
        }
    }
    for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            if headers.is_empty() {
                headers = obj.keys().cloned().collect();
            }
            let mut line: Vec<String> = Vec::with_capacity(headers.len());
            for h in headers.iter() {
                let val = obj.get(h).unwrap_or(&Value::Null);
                line.push(match val {
                    Value::Null => String::new(),
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => if *b { "1".into() } else { "0".into() },
                    other => other.to_string().trim_matches('"').to_string(),
                });
            }
            data_rows.push(line);
        }
    }

    let ts = Local::now().format("%Y%m%d-%H%M%S");
    let (content_type, file_ext, bytes) = if export_type == "xlsx" {
        let buf = write_xlsx(&headers, &data_rows).unwrap_or_else(|e| {
            log_output(
                "WARN",
                "EXPORT",
                route.as_str(),
                format!("Falling back to CSV: {}", e),
                true,
            );
            write_csv(&headers, &data_rows).unwrap_or_default()
        });
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
            "xlsx".to_string(),
            buf,
        )
    } else {
        let buf = write_csv(&headers, &data_rows).unwrap_or_default();
        ("text/csv".to_string(), "csv".to_string(), buf)
    };

    // Audit
    write_audit(&AuditEntry {
        at: Local::now().to_rfc3339(),
        actor_id: claims.id,
        action: "EXPORT",
        route: &route,
        id: None,
        ip: req.peer_addr().map(|a| a.ip().to_string()).as_deref(),
    });

    let filename = format!("{}-{}.{}", filename_base, ts, file_ext);
    HttpResponse::Ok()
        .insert_header((
            header::CONTENT_TYPE,
            header::HeaderValue::from_str(&content_type).unwrap(),
        ))
        .insert_header((
            header::CONTENT_DISPOSITION,
            header::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename))
                .unwrap(),
        ))
        .body(bytes)
}

fn write_csv(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(Vec::new());
    if !headers.is_empty() {
        wtr.write_record(headers)?;
    }
    for r in rows.iter() {
        wtr.write_record(r)?;
    }
    wtr.flush()?;
    Ok(wtr.into_inner()?)
}

fn write_xlsx(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
    use rust_xlsxwriter::{Format, Workbook};
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let mut row_idx: u32 = 0;
    if !headers.is_empty() {
        let bold = Format::new().set_bold();
        for (col, h) in headers.iter().enumerate() {
            worksheet.write_string_with_format(row_idx, col as u16, h, &bold)?;
        }
        row_idx += 1;
    }
    for r in rows.iter() {
        for (c, val) in r.iter().enumerate() {
            worksheet.write_string(row_idx, c as u16, val)?;
        }
        row_idx += 1;
    }
    let buf: Vec<u8> = workbook.save_to_buffer()?;
    Ok(buf)
}
