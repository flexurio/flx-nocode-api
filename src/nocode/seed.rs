use actix_web::{
    web, HttpResponse, Responder,
};
use serde_json::{Map, Value};
use std::sync::Arc;

use crate::{
    AppState,
    auth::{get_user_info_from_token, Claims},
    config::SEED_LOCATION,
    log::log_output,
    model::{Column, DbType, TableSchema, WebResponse},
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

/// Validate if the claims have Admin role.
/// Allowed roles: "admin" or "administrator" (case-insensitive, e.g. "admin", "Admin", "Administrator", "admin/1"),
/// or bitmask "127" / "*/127".
pub fn is_admin_role(claims: &Claims) -> bool {
    claims.get_roles().iter().any(|r| {
        let r_trimmed = r.trim();
        let role_name = r_trimmed.split('/').next().unwrap_or(r_trimmed).trim();
        role_name.eq_ignore_ascii_case("admin")
            || role_name.eq_ignore_ascii_case("administrator")
            || r_trimmed == "127"
            || r_trimmed == "*/127"
    })
}

fn is_string_type(type_lower: &str) -> bool {
    type_lower.contains("char")
        || type_lower.contains("text")
        || type_lower.contains("blob")
        || type_lower.contains("enum")
}

/// Helper to coerce a raw CSV string into a typed serde_json::Value
/// based on the target column's type_data and nullable definitions.
pub fn convert_field_value(raw: &str, col_opt: Option<&Column>) -> Value {
    let trimmed = raw.trim();
    let is_null_literal = trimmed.eq_ignore_ascii_case("null") || trimmed == "\\N";

    if let Some(col) = col_opt {
        let type_lower = col.type_data.to_ascii_lowercase();

        // Check if value should be treated as NULL
        if is_null_literal || (trimmed.is_empty() && (col.nullable || !is_string_type(&type_lower))) {
            return Value::Null;
        }

        // Integer types: int, tinyint, smallint, mediumint, bigint, serial
        if type_lower.contains("int") || type_lower.contains("serial") {
            if let Ok(n) = trimmed.parse::<i64>() {
                return Value::Number(serde_json::Number::from(n));
            } else if let Ok(u) = trimmed.parse::<u64>() {
                return Value::Number(serde_json::Number::from(u));
            } else if trimmed.is_empty() {
                return Value::Null;
            }
            return Value::String(trimmed.to_string());
        }

        // Float / Decimal / Double / Real / Numeric / Money
        if type_lower.contains("float")
            || type_lower.contains("double")
            || type_lower.contains("decimal")
            || type_lower.contains("numeric")
            || type_lower.contains("real")
            || type_lower.contains("money")
        {
            if let Ok(f) = trimmed.parse::<f64>() {
                if let Some(num) = serde_json::Number::from_f64(f) {
                    return Value::Number(num);
                }
            } else if trimmed.is_empty() {
                return Value::Null;
            }
            return Value::String(trimmed.to_string());
        }

        // Boolean types: boolean, bool
        if type_lower.starts_with("bool") {
            let lower = trimmed.to_ascii_lowercase();
            if lower == "true" || lower == "1" || lower == "t" || lower == "yes" {
                return Value::Bool(true);
            } else if lower == "false" || lower == "0" || lower == "f" || lower == "no" {
                return Value::Bool(false);
            } else if trimmed.is_empty() {
                return Value::Null;
            }
            return Value::String(trimmed.to_string());
        }

        // JSON types: json, jsonb
        if type_lower.contains("json") {
            if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                return val;
            }
            return Value::String(trimmed.to_string());
        }

        // Date / Datetime / Timestamp / Time / Year
        if type_lower.contains("date") || type_lower.contains("time") || type_lower.contains("year") {
            if trimmed.is_empty() {
                return Value::Null;
            }
            if type_lower.contains("year") {
                if let Ok(n) = trimmed.parse::<i64>() {
                    return Value::Number(serde_json::Number::from(n));
                }
            }
            return Value::String(trimmed.to_string());
        }

        // String types (varchar, char, text, longtext, etc.)
        if trimmed.is_empty() && col.nullable {
            return Value::Null;
        }
        Value::String(raw.to_string())
    } else {
        // Fallback when column metadata is unknown
        if is_null_literal || trimmed.is_empty() {
            Value::Null
        } else if let Ok(n) = trimmed.parse::<i64>() {
            Value::Number(serde_json::Number::from(n))
        } else if let Ok(f) = trimmed.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(f) {
                Value::Number(num)
            } else {
                Value::String(trimmed.to_string())
            }
        } else if trimmed.eq_ignore_ascii_case("true") {
            Value::Bool(true)
        } else if trimmed.eq_ignore_ascii_case("false") {
            Value::Bool(false)
        } else if (trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']'))
        {
            if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                val
            } else {
                Value::String(raw.to_string())
            }
        } else {
            Value::String(raw.to_string())
        }
    }
}

/// Helper to parse CSV bytes into a vector of JSON maps,
/// converting each field according to the table schema columns.
pub fn parse_csv_data(content: &str, table_schema: &TableSchema) -> anyhow::Result<Vec<Map<String, Value>>> {
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
                let h_clean = h.trim();
                if h_clean.is_empty() {
                    continue;
                }
                let col = table_schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(h_clean));

                if let Some(c) = col {
                    let field_trimmed = field.trim();
                    // If auto-increment column and field is empty or null, omit it so DB sequence handles it
                    if c.auto_increment
                        && (field_trimmed.is_empty()
                            || field_trimmed.eq_ignore_ascii_case("null")
                            || field_trimmed == "\\N")
                    {
                        continue;
                    }
                    let val = convert_field_value(field, Some(c));
                    map.insert(c.name.clone(), val);
                } else {
                    let val = convert_field_value(field, None);
                    map.insert(h_clean.to_string(), val);
                }
            }
        }
        if !map.is_empty() {
            rows.push(map);
        }
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
    // 1. Auth check: Only Admin role is allowed to execute seed
    if state.require_auth {
        let claims = match get_user_info_from_token(&req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid or missing token".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };

        if !is_admin_role(&claims) {
            return HttpResponse::Forbidden().json(WebResponse {
                success: false,
                message: "Forbidden: Only Admin role is allowed to seed tables".to_string(),
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
        let rows = match parse_csv_data(trimmed, &table_schema) {
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
    fn test_is_admin_role() {
        let admin_claims1 = Claims {
            rl: "admin/1".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims1));

        let admin_claims2 = Claims {
            rl: "Admin".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims2));

        let admin_claims3 = Claims {
            rl: "ADMIN/99".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims3));

        let admin_claims4 = Claims {
            rl: "127".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims4));

        let admin_claims5 = Claims {
            rl: "*/127".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims5));

        let admin_claims6 = Claims {
            rl: "user/2,admin/1".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims6));

        let admin_claims7 = Claims {
            rl: "Administrator".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims7));

        let admin_claims8 = Claims {
            rl: "user/2,administrator/1".to_string(),
            ..Claims::default()
        };
        assert!(is_admin_role(&admin_claims8));

        let non_admin = Claims {
            rl: "user/1,viewer/2".to_string(),
            ..Claims::default()
        };
        assert!(!is_admin_role(&non_admin));

        let empty_claims = Claims {
            rl: "".to_string(),
            ..Claims::default()
        };
        assert!(!is_admin_role(&empty_claims));
    }

    #[test]
    fn test_parse_csv_data_with_column_types() {
        let schema = TableSchema {
            table: "products".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_data: "bigint".to_string(),
                    auto_increment: true,
                    nullable: false,
                    ..Default::default()
                },
                Column {
                    name: "name".to_string(),
                    type_data: "varchar(100)".to_string(),
                    auto_increment: false,
                    nullable: false,
                    ..Default::default()
                },
                Column {
                    name: "price".to_string(),
                    type_data: "decimal(10,2)".to_string(),
                    auto_increment: false,
                    nullable: false,
                    ..Default::default()
                },
                Column {
                    name: "is_active".to_string(),
                    type_data: "boolean".to_string(),
                    auto_increment: false,
                    nullable: false,
                    ..Default::default()
                },
                Column {
                    name: "metadata".to_string(),
                    type_data: "json".to_string(),
                    auto_increment: false,
                    nullable: true,
                    ..Default::default()
                },
                Column {
                    name: "deleted_at".to_string(),
                    type_data: "datetime".to_string(),
                    auto_increment: false,
                    nullable: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let csv_text = "id,name,price,is_active,metadata,deleted_at\n\
                        1,Widget A,19.99,true,\"{\"\"tag\"\":\"\"sample\"\"}\",\n\
                        ,Widget B,25.50,false,NULL,NULL";

        let rows = parse_csv_data(csv_text, &schema).unwrap();
        assert_eq!(rows.len(), 2);

        // Row 1:
        // id is non-empty -> parsed as i64
        assert_eq!(rows[0].get("id"), Some(&serde_json::json!(1)));
        assert_eq!(rows[0].get("name"), Some(&serde_json::json!("Widget A")));
        assert_eq!(rows[0].get("price"), Some(&serde_json::json!(19.99)));
        assert_eq!(rows[0].get("is_active"), Some(&serde_json::json!(true)));
        assert_eq!(rows[0].get("metadata"), Some(&serde_json::json!({"tag": "sample"})));
        assert_eq!(rows[0].get("deleted_at"), Some(&Value::Null));

        // Row 2:
        // id is empty and auto_increment -> omitted from map
        assert_eq!(rows[1].get("id"), None);
        assert_eq!(rows[1].get("name"), Some(&serde_json::json!("Widget B")));
        assert_eq!(rows[1].get("price"), Some(&serde_json::json!(25.5)));
        assert_eq!(rows[1].get("is_active"), Some(&serde_json::json!(false)));
        assert_eq!(rows[1].get("metadata"), Some(&Value::Null));
        assert_eq!(rows[1].get("deleted_at"), Some(&Value::Null));
    }

    #[test]
    fn test_parse_csv_data_without_schema_fallback() {
        let schema = TableSchema::default();
        let csv_text = "id,name,value\n1,Alpha,100\n2,Beta,200";
        let rows = parse_csv_data(csv_text, &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&serde_json::json!(1)));
        assert_eq!(rows[0].get("name"), Some(&serde_json::json!("Alpha")));
        assert_eq!(rows[1].get("value"), Some(&serde_json::json!(200)));
    }
}
