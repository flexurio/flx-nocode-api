use base64::Engine;
use colored::Colorize;
use std::collections::HashSet;
use std::fmt::Write;
use actix_multipart::Multipart;
use serde_json::{json, Value};
use futures::StreamExt;


use crate::{log::log_output, model::{Column, GetOperation, Index, JoinTable, Operation, OperationDelete, Patch, PrimaryKey, Redis, TableSchema, Trace}, ISDEBUG};

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

pub fn generate_table(db_type:String, data: &TableSchema) -> (String, Vec<String>) {
    let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS {} (\n", data.table);

    for col in &data.columns {
        let mut auto_increment = "".to_string();
        if data.primary_key.columns.len() == 1 && data.primary_key.columns[0] == col.name {
            if db_type == "mysql" {
                auto_increment = " auto_increment".to_string();
                create_table_sql.push_str(&format!("  {} {}{},\n", col.name, col.type_data, auto_increment));
            } else if db_type == "postgres" {
                auto_increment = " bigserial".to_string();
                create_table_sql.push_str(&format!("  {} {},\n", col.name, auto_increment));
            } else {
                create_table_sql.push_str(&format!("  {} {}{},\n", col.name, col.type_data, auto_increment));
            }
        } else {
            create_table_sql.push_str(&format!("  {} {}{},\n", col.name, col.type_data, auto_increment));
        }
    }

    let _ = writeln!(
        create_table_sql,
        "  PRIMARY KEY ({})\n);",
        data.primary_key.columns.join(", ")
    );

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
        create_index_sql_vec.push(format!("CREATE {}INDEX {} ON {} ({});",unique,index_name,data.table,idx.columns.join(", ")));
    }

    (create_table_sql, create_index_sql_vec)
}

pub fn validate_table_design(design: TableSchema) -> TableSchema {
    let mut schema_check = TableSchema {
        table: String::new(),
        primary_key: PrimaryKey {
            columns: Vec::new(),
        },
        columns: Vec::new(),
        indexes: Vec::new(),
        redis: Redis {
            keys: Vec::new(),
            ttl: 0,
        },
        get: GetOperation {
            columns: Vec::new(),
            parameters: Vec::new(),
            join_tables: Vec::new(),
            column_groups: Vec::new(),
            having: Vec::new(),
        },
        post: Operation {
            columns: Vec::new(),
        },
        put: Operation {
            columns: Vec::new(),
        },
        del: OperationDelete {
            columns: Vec::new(),
            type_delete: "soft".to_string()
        },
        patch: Patch {
            pre_process_sp: String::new(),
            parameters: Vec::new(),
        },
        trace: Trace {
            insert_into: String::new(),
            column_inserts: Vec::new(),
            column_selects: Vec::new(),
            parameters: Vec::new(),
            join_tables: Vec::new(),
            column_groups: Vec::new(),
            column_conflicts: Vec::new(),
        },
    };

    // Check if table exists
    schema_check.table = if design.table.is_empty() {
        "NOT OK - root.table does not exist".to_string()
    } else {
        "OK".to_string()
    };

    // Check primary key
    if design.primary_key.columns.is_empty() {
        schema_check.primary_key = PrimaryKey {
            columns: vec!["NOT OK - root.primary_key.columns does not exist".to_string()],
        };
    } else {
        schema_check.primary_key = PrimaryKey {
            columns: vec!["OK".to_string()],
        };

        for pk_col in &design.primary_key.columns {
            if !design.columns.iter().any(|col| col.name == *pk_col) {
                schema_check.primary_key.columns = vec![
                    format!("NOT OK - primary key column '{}' does not exist in columns", pk_col)
                ];
            }
        }
    }

    // Check columns
    schema_check.columns = if design.columns.is_empty() {
        vec![Column {
            name: "NOT OK - root.columns.name do not exist".to_string(),
            type_data: "NOT OK - root.columns.type do not exist".to_string(),
            auto_increment: false,
            nullable: false,
            function: "NOT OK - root.columns.function do not exist".to_string(),
            encrypt: false,
        }]
    } else {
        vec![Column {
            name: "OK".to_string(),
            type_data: "OK".to_string(),
            auto_increment: false,
            nullable: false,
            function: "OK".to_string(),
            encrypt: false,
        }]
    };

    // Check indexes
    schema_check.indexes = if design.indexes.is_empty() {
        vec![Index {
            name: "NOT OK - root.indexes do not exist".to_string(),
            columns: vec!["NOT OK - root.indexes.columns do not exist".to_string()],
            unique: false,
        }]
    } else {
        vec![Index {
            name: "OK".to_string(),
            columns: vec!["OK".to_string()],
            unique: true,
        }]
    };

    for index in &design.indexes {
        for index_col in &index.columns {
            if !design.columns.iter().any(|col| col.name == *index_col) {
                schema_check.indexes = vec![Index {
                    name: format!("NOT OK - index column '{}' does not exist in columns", index_col),
                    columns: vec![format!("NOT OK - index column '{}' does not exist in columns", index_col)],
                    unique: false,
                }];
            }

            if design.primary_key.columns.contains(index_col) {
                schema_check.indexes = vec![Index {
                    name: format!("NOT OK - primary key column '{}' should not be indexed", index_col),
                    columns: vec![format!("NOT OK - primary key column '{}' should not be indexed", index_col)],
                    unique: false,
                }];
            }
        }
    }

    // Check GET
    let is_get_columns_exist = !design.get.columns.is_empty();
    schema_check.get.columns = if design.get.columns.is_empty() {
        vec!["NOT OK - root.GET.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    let ok_or_optional = if is_get_columns_exist { "NOT OK" } else { "OPTIONAL" };

    // Check GET parameters
    let required_params = ["search", "page", "sort", "ascending", "limit"];
    let has_required_params = required_params.iter().all(|p| design.get.parameters.contains(&p.to_string()));

    if !has_required_params {
        schema_check.get.parameters = vec![
            format!("{} - root.GET.parameters must contain search,page,sort,ascending,limit", ok_or_optional)
        ];
    } else if design.get.parameters.is_empty() {
        schema_check.get.parameters = vec![
            format!("{} - root.GET.parameters do not exist", ok_or_optional)
        ];
    } else {
        let mut column_problems = Vec::new();
        for param in &design.get.parameters {
            if !required_params.contains(&param.as_str()) && !param.contains("deleted_at") {
                let parts: Vec<&str> = param.split('.').collect();
                let (table, param_name) = if parts.len() >= 2 {
                    (parts[0], parts[parts.len() - 2])
                } else {
                    (design.table.as_str(), parts[0])
                };

                let is_col_ok = if table == design.table {
                    design.columns.iter().any(|col| col.name == param_name)
                } else {
                    design.get.join_tables.iter()
                        .filter(|jt| jt.table == table)
                        .any(|jt| jt.columns.contains(&param_name.to_string()))
                };

                if !is_col_ok {
                    column_problems.push(param.clone());
                } else if !design.primary_key.columns.contains(&param_name.to_string()) {
                    let in_index = design.indexes.iter()
                        .any(|idx| idx.columns.contains(&param_name.to_string()));
                    if !in_index {
                        column_problems.push(param.clone());
                    }
                }
            }
        }

        schema_check.get.parameters = if !column_problems.is_empty() {
            vec![format!(
                "{} - root.GET.parameters must exist in columns, indexes, and primary key. Check: {}",
                ok_or_optional,
                column_problems.join(", ")
            )]
        } else {
            vec!["OK".to_string()]
        };
    }

    // Check join tables
    schema_check.get.join_tables = if design.get.join_tables.is_empty() {
        vec![JoinTable {
            table: "OPTIONAL - root.GET.join_tables.table do not exist".to_string(),
            columns: vec!["OPTIONAL - root.GET.join_tables.columns do not exist".to_string()],
            logical: "OPTIONAL - root.GET.join_tables.logical do not exist".to_string(),
            type_join: "OPTIONAL - root.GET.join_tables.type do not exist".to_string(),
        }]
    } else {
        vec![JoinTable {
            table: "OK".to_string(),
            columns: vec!["OK".to_string()],
            logical: "OK".to_string(),
            type_join: "OK".to_string(),
        }]
    };

    // Check group by
    schema_check.get.column_groups = if design.get.column_groups.is_empty() {
        vec!["OPTIONAL - root.GET.group_by do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check POST
    schema_check.post.columns = if design.post.columns.is_empty() {
        vec!["OPTIONAL - root.POST.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check PUT
    schema_check.put.columns = if design.put.columns.is_empty() {
        vec!["OPTIONAL - root.PUT.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check DELETE
    schema_check.del.columns = if design.del.columns.is_empty() {
        vec!["OPTIONAL - root.DELETE.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Note: The Rust struct doesn't fully match the Go version for PATCH,
    // so I've adapted it based on what's available in the provided Rust structs
    schema_check.patch.parameters = if design.patch.parameters.is_empty() {
        vec!["OPTIONAL - root.PATCH.parameters do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    schema_check
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




