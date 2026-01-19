use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::{
    AppState,
    auth::{check_access, get_user_info_from_token},

    log::log_output,
    model::{TableSchema, WebResponse},
    storage::prelude::{CreateTable as DdlCreateTable, TableConstraint, ColumnDef, ColumnType, ForeignAction, Ddl},
    storage::sql_store::SqlStore,
};
use std::sync::Arc;

// Helper function to add standard audit trail columns
fn add_audit_columns(cols: &mut Vec<ColumnDef>, db_type: &str) {
    // Check if audit columns already exist to avoid duplicates
    let existing_cols: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    
    let audit_columns = [
        ("created_at", "datetime"),
        ("updated_at", "datetime"), 
        ("deleted_at", "datetime"),
        ("created_by_id", "bigint unsigned"),
        ("updated_by_id", "bigint unsigned"),
        ("deleted_by_id", "bigint unsigned"),
    ];

    for (col_name, col_type) in audit_columns.iter() {
        if !existing_cols.contains(&col_name.to_string()) {
            let mut type_data = col_type.to_string();
            
            // Adjust type based on database dialect
            match db_type {
                "postgres" => {
                    if type_data == "datetime" {
                        type_data = "timestamp".to_string();
                    } else if type_data == "bigint unsigned" {
                        type_data = "bigint".to_string();
                    }
                }
                "sqlite" => {
                    if type_data == "datetime" {
                        type_data = "text".to_string(); // SQLite stores datetime as text
                    } else if type_data == "bigint unsigned" {
                        type_data = "integer".to_string();
                    }
                }
                "mssql" => {
                    if type_data == "datetime" {
                        type_data = "datetime2".to_string();
                    } else if type_data == "bigint unsigned" {
                        type_data = "bigint".to_string();
                    }
                }
                // "mysql" keeps the original types
                _ => {}
            }

            cols.push(ColumnDef {
                name: col_name.to_string(),
                col_type: ColumnType::Raw(type_data),
                nullable: true, // Audit columns are typically nullable
                default: None,
                auto_increment: false,
                primary_key_inline: false,
                collate: None,
            });
        }
    }
}

// NCO-GENERATE-TABLE
pub async fn create_table(
    state: web::Data<AppState>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if state.require_auth && !state.route_publics.contains(&route){
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

    // let table_schema = filter_table_schema(&table_schemas, route.clone()).await; -- use passed schema
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // MongoDB: Generate indexes
    if state.db_type == crate::model::DbType::Mongodb {
        log_output(
            "INFO",
            "GENERATE TABLE",
            route.as_str(),
            format!("MongoDB backend: generating indexes for collection '{}'.", table_schema.table),
            true,
        );
        
        let mongo_cmds = generate_mongo_indexes(&table_schema);
        let mut err_message = String::new();
        
        // Reuse generic logic below but bypass SQL generation
        // Or handle explicitly here
        
        let mut tx = match state.store.begin_tx().await {
            Ok(t) => t,
            Err(err) => return HttpResponse::InternalServerError().json(WebResponse { success: false, message: format!("Error starting tx: {}", err), total_data: 0, data: Value::Null }),
        };

        for cmd in mongo_cmds {
            if let Err(e) = tx.raw_sql(&cmd, vec![]).await {
                 let err_str = e.to_string();
                 if err_str.contains("Index with name") && err_str.contains("already exists") {
                      // ignore
                 } else {
                     err_message = format!("{}\nFailed to create index: {}", err_message, e);
                 }
            }
        }
        
        if err_message.is_empty() {
            let _ = tx.commit().await;
            return HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Table (Indexes) created (MongoDB)".to_string(),
                total_data: 1,
                data: Value::Null,
            });
        } else {
             let _ = tx.rollback().await;
             return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: err_message,
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    
    // Create a mutable copy of table_schema with default collate applied if not set
    let mut table_schema_with_collate = table_schema.as_ref().clone();
    if table_schema_with_collate.collate.trim().is_empty() && state.db_type == crate::model::DbType::Mysql {
        table_schema_with_collate.collate = state.default_collate.clone();
    }
    
    let (sql_create_table, sql_create_index) = generate_table(&ds, &table_schema_with_collate);
    
    log_output(
        "INFO",
        "GENERATE TABLE",
        route.as_str(),
        format!("Starting table generation for: {}", table_schema.table),
        true,
    );
    
    let mut err_message = String::new();

    // Run DDL statements inside a TxStore transaction for consistency
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    // execute create table
    log_output(
        "QUERY",
        "GENERATE TABLE",
        route.clone().as_str(),
        format!("Executing CREATE TABLE: {}", sql_create_table),
        true,
    );
    
    if let Err(err) = tx.raw_sql(&sql_create_table, vec![]).await {
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

    // execute each create index
    for sql_idx in sql_create_index.iter() {
        log_output(
            "QUERY",
            "GENERATE INDEX", 
            route.clone().as_str(),
            format!("Executing CREATE INDEX: {}", sql_idx),
            true,
        );
        
        if let Err(err) = tx.raw_sql(sql_idx, vec![]).await {
            let err_str = err.to_string();
            // Skip error if index already exists or column doesn't exist
            if err_str.contains("Duplicate key name") 
                || err_str.contains("already exists")
                || err_str.contains("doesn't exist")
                || err_str.contains("Key column") {
                log_output(
                    "QUERY",
                    "GENERATE INDEX",
                    route.clone().as_str(),
                    format!("Skipping index ({}): {}", if err_str.contains("Duplicate") { "duplicate" } else { "column not found" }, sql_idx),
                    true,
                );
            } else {
                err_message = format!(
                    "{} \nFailed to create index {} with error : {}",
                    err_message, table_schema.table, err
                );
                log_output(
                    "QUERY",
                    "GENERATE INDEX",
                    route.clone().as_str(),
                    sql_idx.clone() + " ~ ERROR : " + &err_message,
                    true,
                );
            }
        }
    }

    if err_message.is_empty() {
        let _ = tx.commit().await;
        log_output(
            "INFO",
            "GENERATE TABLE",
            route.as_str(),
            format!("Successfully created table: {}", table_schema.table),
            true,
        );
    } else {
        let _ = tx.rollback().await;
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



pub async fn execute_generate_table(stable: String, state: &AppState, sql_create_table:String, sql_create_index: Vec<String>) -> (bool, String) {
    // Run DDL statements inside a TxStore transaction for consistency
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return (false, format!("Error starting transaction: {}", err));
        }
    };

    let mut err_message = String::new();

    // execute create table
    log_output(
        "INFO",
        "GENERATE TABLE",
        stable.as_str(),
        format!("Starting table generation for: {}", stable),
        true,
    );
    
    if let Err(err) = tx.raw_sql(&sql_create_table, vec![]).await {
        err_message = format!(
            "Failed to create table {} with error : {}",
            stable, err
        );
    }

    // execute each create index
    for sql_idx in sql_create_index.iter() {
        log_output(
            "QUERY",
            "GENERATE INDEX",
            stable.clone().as_str(),
            format!("Executing CREATE INDEX: {}", sql_idx),
            true,
        );
        
        if let Err(err) = tx.raw_sql(sql_idx, vec![]).await {
            let err_str = err.to_string();
            // Skip error if index already exists or column doesn't exist
            if err_str.contains("Duplicate key name") 
                || err_str.contains("already exists")
                || err_str.contains("doesn't exist")
                || err_str.contains("Key column") {
                log_output(
                    "QUERY",
                    "GENERATE INDEX",
                    stable.clone().as_str(),
                    format!("Skipping index ({}): {}", if err_str.contains("Duplicate") { "duplicate" } else { "column not found" }, sql_idx),
                    true,
                );
            } else {
                err_message = format!(
                    "{} \nFailed to create index {} with error : {}",
                    err_message, stable, err
                );
                log_output(
                    "QUERY",
                    "GENERATE INDEX",
                    stable.clone().as_str(),
                    sql_idx.clone() + " ~ ERROR : " + &err_message,
                    true,
                );
            }
        }
    }

    if err_message.is_empty() {
        let _ = tx.commit().await;
        log_output(
            "INFO",
            "GENERATE TABLE",
            stable.as_str(),
            format!("Successfully created table: {}", stable),
            true,
        );
        (true, "success".to_string())
    } else {
        let _ = tx.rollback().await;
        (false, err_message)
    }
}

pub fn generate_table(ds: &SqlStore, data: &TableSchema) -> (String, Vec<String>) {
    // Map TableSchema -> DDL AST
    let db_type = ds.dialect();
    let pk_cols = data.primary_key.columns.clone();
    let pk_single = pk_cols.len() == 1;

    // Helper: map action string to ForeignAction
    fn map_action(s: &str) -> Option<ForeignAction> {
        match s.to_lowercase().as_str() {
            "cascade" => Some(ForeignAction::Cascade),
            "set null" => Some(ForeignAction::SetNull),
            "restrict" => Some(ForeignAction::Restrict),
            "no action" => Some(ForeignAction::NoAction),
            _ => None,
        }
    }

    // ColumnDefs with per-dialect type mapping for auto-increment PK
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(data.columns.len());
    let mut any_inline_pk = false;
    for c in &data.columns {
        let is_pk_col = pk_cols.contains(&c.name);
        let mut ty = c.type_data.clone();
        // Dialect fixes
        if db_type == "mssql" && ty.eq_ignore_ascii_case("timestamp") {
            ty = "datetime".to_string();
        }

    // Not mutated later; no need for mut
    let primary_key_inline = false;
        let mut auto_increment = false;
        match db_type {
            "postgres" if pk_single && is_pk_col && c.auto_increment => {
                ty = "BIGSERIAL".into();
            }
            "mssql" if pk_single && is_pk_col && c.auto_increment => {
                // best-effort: append IDENTITY(1,1)
                if !ty.to_lowercase().contains("identity") {
                    ty = format!("{} IDENTITY(1,1)", ty);
                }
            }
            "sqlite" if pk_single && is_pk_col && c.auto_increment => {
                // For SQLite autoincrement single-column PK we embed
                // the full "INTEGER PRIMARY KEY AUTOINCREMENT" in the type.
                // Do NOT also mark primary_key_inline = true because the
                // compiler would append an extra "PRIMARY KEY" producing
                // invalid DDL: "INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY".
                ty = "INTEGER PRIMARY KEY AUTOINCREMENT".into();
                any_inline_pk = true; // signal to skip table-level PK constraint
            }
            "mysql" if pk_single && is_pk_col && c.auto_increment => {
                auto_increment = true; // compiler will append AUTO_INCREMENT
            }
            _ => {}
        }

        cols.push(ColumnDef {
            name: c.name.clone(),
            col_type: ColumnType::Raw(ty),
            nullable: c.nullable,
            default: None,
            auto_increment,
            primary_key_inline,
            collate: if c.collate.trim().is_empty() { None } else { Some(c.collate.trim().to_string()) },
        });
    }

    // Add standard audit trail columns
    let initial_col_count = cols.len();
    add_audit_columns(&mut cols, db_type);
    let added_col_count = cols.len() - initial_col_count;
    
    if added_col_count > 0 {
        log_output(
            "INFO",
            "GENERATE TABLE",
            &data.table,
            format!("Added {} audit trail columns to table {}", added_col_count, data.table),
            true,
        );
    }

    // Constraints: PK (unless inline used), Unique/Indexes, Foreign Keys
    let mut constraints: Vec<TableConstraint> = Vec::new();
    if !any_inline_pk && !pk_cols.is_empty() {
        constraints.push(TableConstraint::PrimaryKey { columns: pk_cols.clone() });
    }

    // Indexes
    for ix in &data.indexes {
        if ix.columns.is_empty() { continue; }
        // Skip pure PK duplicate index (single col equal pk first col)
        if pk_single && ix.columns.len() == 1 && ix.columns[0] == pk_cols[0] { continue; }
        let name = if ix.name.contains(&data.table) { Some(ix.name.clone()) } else { Some(format!("{}_{}", data.table, ix.name)) };
        constraints.push(TableConstraint::Index { name, columns: ix.columns.clone(), unique: ix.unique });
    }

    // Foreign keys
    for fk in &data.foreign_keys {
        let on_del = map_action(&fk.on_delete);
        let on_upd = map_action(&fk.on_update);
        constraints.push(TableConstraint::ForeignKey {
            name: Some(format!("fk_{}_{}_{}", data.table, fk.column, fk.reference_table)),
            columns: vec![fk.column.clone()],
            ref_table: fk.reference_table.clone(),
            ref_columns: vec![fk.reference_column.clone()],
            on_delete: on_del,
            on_update: on_upd,
        });
    }

    let ddl = Ddl::CreateTable(DdlCreateTable {
        if_not_exists: true,
        name: data.table.clone(),
        columns: cols,
        constraints,
        collate: if data.collate.trim().is_empty() { None } else { Some(data.collate.trim().to_string()) },
    });

    // Compile using SqlStore
    ds.preview_ddl_separate(&ddl)
}

fn generate_mongo_indexes(data: &TableSchema) -> Vec<String> {
    let mut cmds = Vec::new();
    
    // Convert table schema indexes to MongoDB `createIndexes` commands
    // Command format: { createIndexes: <collection>, indexes: [ { key: { <col>: 1, ... }, name: <name>, unique: <bool> } ] }
    
    // 1. Unique Constraints & Indexes
    for ix in &data.indexes {
        if ix.columns.is_empty() { continue; }
        
        let mut key_doc = serde_json::Map::new();
        for col in &ix.columns {
            key_doc.insert(col.clone(), serde_json::json!(1));
        }
        
        let idx_name = if ix.name.contains(&data.table) { ix.name.clone() } else { format!("{}_{}", data.table, ix.name) };
        
        let index_spec = serde_json::json!({
            "key": key_doc,
            "name": idx_name,
            "unique": ix.unique
        });
        
        let cmd = serde_json::json!({
            "createIndexes": data.table,
            "indexes": [ index_spec ]
        });
        
        cmds.push(cmd.to_string());
    }
    
    // 2. Foreign Keys (Index on foreign key column for performance, though checking is manual)
    for fk in &data.foreign_keys {
        let idx_name = format!("idx_fk_{}_{}", data.table, fk.column);
        
        // check if this column is already indexed by explicit indexes (simple check)
        let already_indexed = data.indexes.iter().any(|ix| ix.columns.len() == 1 && ix.columns[0] == fk.column);
        if already_indexed { continue; }
        
        let index_spec = serde_json::json!({
            "key": { fk.column.clone(): 1 },
            "name": idx_name,
            "unique": false
        });
        
        let cmd = serde_json::json!({
            "createIndexes": data.table,
            "indexes": [ index_spec ]
        });
        cmds.push(cmd.to_string());
    }
    
    cmds
}
