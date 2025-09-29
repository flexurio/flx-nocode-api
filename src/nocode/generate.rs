use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;
use std::fmt::Write;

use crate::{
    AppState, auth::{check_access, get_user_info_from_token}, helpers::filter_table_schema, log::{self, log_output}, model::{TableSchema, WebResponse}
};
use std::sync::Arc;

// NCO-GENERATE-TABLE
pub async fn create_table(
    state: web::Data<AppState>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !state.route_publics.contains(&route) {
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

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
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


    // execute sql_create_table
    match &state.db.query(&sql_create_table).await {
        Ok(_) => {
            println!("Table {} created", table_schema.table);
        }
        Err(err) => {
            let error_message = format!(
                "Failed to create table {} with error : {}",
                table_schema.table, err
            );

            log_output(
                "QUERY",
                "GENERATE TABLE",
                route.clone().as_str(),
                sql_create_table.clone() + " ~ ERROR : " + &error_message,
                true,
            );

            err_message = error_message;
        }
    }

    // execute sql_create_index
    // loop every sql_create_index
    for sql_create_index in sql_create_index.iter() {

        match &state.db.query(sql_create_index).await {
            Ok(_) => {
                println!("Index {} created", table_schema.table);
            }
            Err(err) => {
                err_message = format!(
                    "{} \n
                    Failed to create index {} with error : {}",
                    err_message, table_schema.table, err
                );

                log_output(
                    "QUERY",
                    "GENERATE INDEX",
                    route.clone().as_str(),
                    sql_create_index.clone() + " ~ ERROR : " + &err_message,
                    true,
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

pub fn generate_table(db_type: String, data: &TableSchema) -> (String, Vec<String>) {
    // SQL Server (MSSQL) doesn't support "CREATE TABLE IF NOT EXISTS".
    // Use OBJECT_ID guard for MSSQL, keep original for others.
    let mut create_table_sql = if db_type == "mssql" {
        format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL\nBEGIN\nCREATE TABLE {} (\n",
            data.table, data.table
        )
    } else {
        format!("CREATE TABLE IF NOT EXISTS {} (\n", data.table)
    };

    for col in &data.columns {
        // DB-specific type mapping
        let mut mapped_type = col.type_data.clone();
        if db_type == "mssql" {
            // In SQL Server, TIMESTAMP means ROWVERSION (binary) and only one per table.
            // Map generic 'timestamp' to a datetime type instead.
            if col.type_data.eq_ignore_ascii_case("timestamp") {
                mapped_type = "datetime".to_string();
            }
        }
        let mut auto_increment = "".to_string();
        if data.primary_key.columns.len() == 1 && data.primary_key.columns[0] == col.name {
            if db_type == "mysql" {
                auto_increment = " auto_increment".to_string();
                create_table_sql.push_str(&format!(
                    "  {} {}{},\n",
                    col.name, mapped_type, auto_increment
                ));
            } else if db_type == "postgres" {
                auto_increment = " bigserial".to_string();
                create_table_sql.push_str(&format!("  {} {},\n", col.name, auto_increment));
            } else if db_type == "mssql" {
                // Use IDENTITY for auto-increment in MSSQL only when the PK type supports it
                let lt_clean = mapped_type.to_lowercase().replace(' ', "");
                let mut identity_ok = matches!(lt_clean.as_str(), "int" | "bigint" | "smallint" | "tinyint");
                if !identity_ok && (lt_clean.starts_with("decimal(") || lt_clean.starts_with("numeric(")) {
                    // allow decimal/numeric with scale 0 e.g., decimal(18,0)
                    if lt_clean.ends_with(",0)") {
                        identity_ok = true;
                    }
                }
                if identity_ok {
                    auto_increment = " IDENTITY(1,1)".to_string();
                } else {
                    auto_increment.clear();
                }
                create_table_sql.push_str(&format!(
                    "  {} {}{},\n",
                    col.name, mapped_type, auto_increment
                ));
            } else if db_type == "sqlite" {
                auto_increment = " INTEGER PRIMARY KEY AUTOINCREMENT".to_string();
                create_table_sql.push_str(&format!("  {} {},\n", col.name, auto_increment));
            } else {
                create_table_sql.push_str(&format!(
                    "  {} {}{},\n",
                    col.name, mapped_type, auto_increment
                ));
            }
        } else {
            create_table_sql.push_str(&format!(
                "  {} {}{},\n",
                col.name, mapped_type, auto_increment
            ));
        }
    }

    if db_type == "sqlite" {
        // remove the last comma and newline
        if create_table_sql.ends_with(",\n") {
            create_table_sql.truncate(create_table_sql.len() - 2);
        }
        create_table_sql.push_str(");\n");
    } else {
        // Close statement, handling MSSQL BEGIN..END wrapper
        if db_type == "mssql" {
            let _ = writeln!(
                create_table_sql,
                "  PRIMARY KEY ({})\n);\nEND",
                data.primary_key.columns.join(", ")
            );
        } else {
            let _ = writeln!(
                create_table_sql,
                "  PRIMARY KEY ({})\n);",
                data.primary_key.columns.join(", ")
            );
        }
    }

    // create variable to store multipe query create index Vec<String>
    let mut create_index_sql_vec = Vec::new();

    for idx in &data.indexes {
        if idx.columns.is_empty() {
            println!("Err. Index 01 : Index columns is empty");
            continue;
        }
        if idx.columns.len() == 1 && idx.columns[0] == data.primary_key.columns[0] {
            println!("Err. Index 02 : Index columns is empty");
            continue;
        }
        let unique = if idx.unique { "UNIQUE " } else { "" };
        let index_name = if idx.name.contains(&data.table) {
            idx.name.clone()
        } else {
            format!("{}_{}", data.table, idx.name)
        };
        if db_type == "mssql" {
            // Guard for MSSQL: only create index if it doesn't exist
            create_index_sql_vec.push(format!(
                "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{}' AND object_id = OBJECT_ID(N'{}'))\nBEGIN\nCREATE {}INDEX {} ON {} ({});\nEND",
                index_name,
                data.table,
                unique,
                index_name,
                data.table,
                idx.columns.join(", ")
            ));
        } else {
            create_index_sql_vec.push(format!(
                "CREATE {}INDEX {} ON {} ({});",
                unique,
                index_name,
                data.table,
                idx.columns.join(", ")
            ));
        }
    }

    // print log for create_table_sql
    log_output(
        "QUERY",
        "GENERATE TABLE",
        data.table.as_str(),
        create_table_sql.clone(),
        false,
    );

    log_output(
        "QUERY",
        "GENERATE INDEX",
        data.table.as_str(),
        create_index_sql_vec.join("\n"),
        false,
    );

    (create_table_sql, create_index_sql_vec)
}
