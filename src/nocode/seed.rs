use actix_web::{
    web, HttpResponse, Responder,
};
use serde_json::{Map, Value};
use std::sync::Arc;

use crate::{
    AppState,
    auth::{check_access, get_user_info_from_token},
    config::SEED_LOCATION,
    log::log_output,
    model::{DbType, TableSchema, WebResponse},
    storage::sql_store::SqlStore,
};

/// Split raw SQL script into individual executable statements.
/// Handles single quotes, double quotes, backticks, line comments (--), and block comments (/* */).
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if !in_single_quote && !in_double_quote && !in_backtick {
            if ch == '-' && chars.peek() == Some(&'-') {
                chars.next();
                in_line_comment = true;
                continue;
            }
            if ch == '/' && chars.peek() == Some(&'*') {
                chars.next();
                in_block_comment = true;
                continue;
            }
        }

        match ch {
            '\'' if !in_double_quote && !in_backtick => {
                in_single_quote = !in_single_quote;
                current.push(ch);
            }
            '"' if !in_single_quote && !in_backtick => {
                in_double_quote = !in_double_quote;
                current.push(ch);
            }
            '`' if !in_single_quote && !in_double_quote => {
                in_backtick = !in_backtick;
                current.push(ch);
            }
            ';' if !in_single_quote && !in_double_quote && !in_backtick => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

/// Helper to parse CSV bytes into a vector of JSON maps.
fn parse_csv_data(content: &str) -> anyhow::Result<Vec<Map<String, Value>>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(content.as_bytes());

    let headers = rdr.headers()?.clone();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    for result in rdr.records() {
        let rec = result?;
        let mut map = Map::new();
        for (i, field) in rec.iter().enumerate() {
            if let Some(h) = headers.get(i) {
                map.insert(h.trim().to_string(), Value::String(field.to_string()));
            }
        }
        rows.push(map);
    }
    Ok(rows)
}

/// NCO-SEED-TABLE
/// Handler to seed a table with initial data from the seed directory.
pub async fn seed_table(
    state: web::Data<AppState>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    // 1. Auth check
    if state.require_auth && !state.route_publics.contains(&route) {
        let claims = match get_user_info_from_token(&req, state.clone()) {
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

        if let Err(e) = check_access(&claims, &req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // 2. Validate schema
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // 3. Locate seed file from SEED_LOCATION (LOC_SEED env or "seed")
    let seed_dir = SEED_LOCATION.as_str();
    let candidates = [
        format!("{}/{}.sql", seed_dir, route),
        format!("{}/{}.sql", seed_dir, table_schema.table),
        format!("{}/{}.json", seed_dir, route),
        format!("{}/{}.json", seed_dir, table_schema.table),
        format!("{}/{}.csv", seed_dir, route),
        format!("{}/{}.csv", seed_dir, table_schema.table),
    ];

    let found_path = candidates.iter().find(|p| std::path::Path::new(p.as_str()).is_file());
    let file_path = match found_path {
        Some(p) => p.clone(),
        None => {
            let msg = format!(
                "Seed file for route '{}' (table '{}') not found in folder '{}'",
                route, table_schema.table, seed_dir
            );
            log_output("ERROR", "SEED TABLE", &route, msg.clone(), true);
            return HttpResponse::NotFound().json(WebResponse {
                success: false,
                message: msg,
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    let content = match std::fs::read_to_string(&file_path) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Failed to read seed file '{}': {}", file_path, e);
            log_output("ERROR", "SEED TABLE", &route, msg.clone(), true);
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: msg,
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    log_output(
        "INFO",
        "SEED TABLE",
        route.as_str(),
        format!("Starting seed process for table '{}' from '{}'", table_schema.table, file_path),
        true,
    );

    let trimmed = content.trim();

    // 4. Process file content based on format
    if file_path.ends_with(".csv") {
        // CSV data seeding
        let rows = match parse_csv_data(trimmed) {
            Ok(r) => r,
            Err(e) => {
                let msg = format!("Failed to parse CSV in '{}': {}", file_path, e);
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: msg,
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };

        return execute_seed_json_rows(&state, &route, &table_schema, rows).await;
    }

    // Check if .json file or .sql file containing JSON payload
    if file_path.ends_with(".json") || (file_path.ends_with(".sql") && (trimmed.starts_with('[') || (trimmed.starts_with('{') && !trimmed.to_lowercase().starts_with("begin")))) {
        if let Ok(json_val) = serde_json::from_str::<Value>(trimmed) {
            let rows_opt: Option<Vec<Map<String, Value>>> = match json_val {
                Value::Array(arr) => {
                    let mut list = Vec::new();
                    for item in arr {
                        if let Value::Object(m) = item {
                            list.push(m);
                        }
                    }
                    Some(list)
                }
                Value::Object(ref obj) => {
                    // Check keys like "data", "rows", "records", "items", "seed"
                    let candidate_keys = ["data", "rows", "records", "items", "seed"];
                    let mut found_rows = None;
                    for key in candidate_keys {
                        if let Some(Value::Array(arr)) = obj.get(key) {
                            let mut list = Vec::new();
                            for item in arr {
                                if let Value::Object(m) = item {
                                    list.push(m.clone());
                                }
                            }
                            found_rows = Some(list);
                            break;
                        }
                    }
                    found_rows
                }
                _ => None,
            };

            if let Some(rows) = rows_opt {
                if !rows.is_empty() {
                    return execute_seed_json_rows(&state, &route, &table_schema, rows).await;
                }
            }
        }
    }

    // Default: SQL statement execution
    let statements = split_sql_statements(trimmed);
    if statements.is_empty() {
        return HttpResponse::Ok().json(WebResponse {
            success: true,
            message: format!("Seed file '{}' is empty; no operations executed", file_path),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Execute SQL statements in a transaction
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            let msg = format!("Error starting database transaction: {}", err);
            log_output("ERROR", "SEED TABLE", &route, msg.clone(), true);
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: msg,
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    let mut executed_count: usize = 0;
    for stmt in &statements {
        if stmt.trim().is_empty() {
            continue;
        }

        log_output(
            "QUERY",
            "SEED TABLE",
            route.as_str(),
            format!("Executing SQL: {}", stmt),
            true,
        );

        if let Err(err) = tx.raw_sql(stmt, vec![]).await {
            let _ = tx.rollback().await;
            let err_msg = format!(
                "Failed executing seed query for table '{}': {}",
                table_schema.table, err
            );
            log_output("ERROR", "SEED TABLE", &route, format!("{} ~ QUERY: {}", err_msg, stmt), true);
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: err_msg,
                total_data: executed_count as i32,
                data: Value::Null,
            });
        }
        executed_count += 1;
    }

    if let Err(err) = tx.commit().await {
        let err_msg = format!("Failed committing seed transaction for table '{}': {}", table_schema.table, err);
        log_output("ERROR", "SEED TABLE", &route, err_msg.clone(), true);
        return HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: err_msg,
            total_data: 0,
            data: Value::Null,
        });
    }

    log_output(
        "INFO",
        "SEED TABLE",
        route.as_str(),
        format!("Successfully seeded table '{}' ({} SQL statements executed)", table_schema.table, executed_count),
        true,
    );

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: format!("Successfully seeded table '{}'", table_schema.table),
        total_data: executed_count as i32,
        data: Value::Null,
    })
}

/// Execute JSON / Map rows insertion into database
async fn execute_seed_json_rows(
    state: &AppState,
    route: &str,
    table_schema: &TableSchema,
    rows: Vec<Map<String, Value>>,
) -> HttpResponse {
    let row_count = rows.len();

    if state.db_type == DbType::Mongodb {
        let mut inserted = 0;
        for row in rows {
            let doc = Value::Object(row);
            if let Err(e) = state.store.insert(&table_schema.table, doc).await {
                let msg = format!("Failed inserting seed document for MongoDB '{}': {}", table_schema.table, e);
                log_output("ERROR", "SEED TABLE", route, msg.clone(), true);
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: msg,
                    total_data: inserted,
                    data: Value::Null,
                });
            }
            inserted += 1;
        }

        log_output(
            "INFO",
            "SEED TABLE",
            route,
            format!("Successfully seeded table '{}' (MongoDB: {} documents)", table_schema.table, inserted),
            true,
        );

        return HttpResponse::Ok().json(WebResponse {
            success: true,
            message: format!("Successfully seeded table '{}'", table_schema.table),
            total_data: inserted,
            data: Value::Null,
        });
    }

    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            let msg = format!("Error starting database transaction: {}", err);
            log_output("ERROR", "SEED TABLE", route, msg.clone(), true);
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: msg,
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let mut inserted = 0;

    for row in rows {
        let doc = Value::Object(row);
        let (sql, params) = match ds.preview_insert(&table_schema.table, &doc) {
            Ok(res) => res,
            Err(e) => {
                let _ = tx.rollback().await;
                let msg = format!("Failed building insert statement for '{}': {}", table_schema.table, e);
                log_output("ERROR", "SEED TABLE", route, msg.clone(), true);
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: msg,
                    total_data: inserted,
                    data: Value::Null,
                });
            }
        };

        log_output("QUERY", "SEED TABLE", route, sql.clone(), true);

        if let Err(e) = tx.raw_sql(&sql, params).await {
            let _ = tx.rollback().await;
            let msg = format!("Failed executing seed insert for table '{}': {}", table_schema.table, e);
            log_output("ERROR", "SEED TABLE", route, msg.clone(), true);
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: msg,
                total_data: inserted,
                data: Value::Null,
            });
        }
        inserted += 1;
    }

    if let Err(err) = tx.commit().await {
        let msg = format!("Failed committing seed transaction for table '{}': {}", table_schema.table, err);
        log_output("ERROR", "SEED TABLE", route, msg.clone(), true);
        return HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: msg,
            total_data: 0,
            data: Value::Null,
        });
    }

    log_output(
        "INFO",
        "SEED TABLE",
        route,
        format!("Successfully seeded table '{}' ({} rows inserted)", table_schema.table, inserted),
        true,
    );

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: format!("Successfully seeded table '{}'", table_schema.table),
        total_data: row_count as i32,
        data: Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_sql_statements_simple() {
        let sql = "INSERT INTO users (name) VALUES ('Alice'); INSERT INTO users (name) VALUES ('Bob');";
        let statements = split_sql_statements(sql);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "INSERT INTO users (name) VALUES ('Alice')");
        assert_eq!(statements[1], "INSERT INTO users (name) VALUES ('Bob')");
    }

    #[test]
    fn test_split_sql_statements_with_semicolon_in_string() {
        let sql = "INSERT INTO users (name, bio) VALUES ('Alice', 'Hello; world!'); INSERT INTO users (name) VALUES ('Bob');";
        let statements = split_sql_statements(sql);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "INSERT INTO users (name, bio) VALUES ('Alice', 'Hello; world!')");
        assert_eq!(statements[1], "INSERT INTO users (name) VALUES ('Bob')");
    }

    #[test]
    fn test_split_sql_statements_with_comments() {
        let sql = r#"
            -- This is a comment; with a semicolon
            INSERT INTO users (name) VALUES ('Alice');
            /* Multi-line comment;
               with semicolons; */
            INSERT INTO users (name) VALUES ('Bob');
        "#;
        let statements = split_sql_statements(sql);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "INSERT INTO users (name) VALUES ('Alice')");
        assert_eq!(statements[1], "INSERT INTO users (name) VALUES ('Bob')");
    }

    #[test]
    fn test_parse_csv_data() {
        let csv_text = "id,name,value\n1,Alpha,100\n2,Beta,200";
        let rows = parse_csv_data(csv_text).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alpha"));
        assert_eq!(rows[1].get("value").and_then(|v| v.as_str()), Some("200"));
    }
}
