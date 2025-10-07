use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use sonic_rs::Value;

use crate::{
    AppState,
    auth::{check_access, get_user_info_from_token},
    helpers::filter_table_schema,
    log::log_output,
    model::{TableSchema, WebResponse},
    storage::prelude::{CreateTable as DdlCreateTable, TableConstraint, ColumnDef, ColumnType, ForeignAction, Ddl},
    storage::sql_store::SqlStore,
};
use std::sync::Arc;

// NCO-GENERATE-TABLE
pub async fn create_table(
    state: web::Data<AppState>,
    route: Arc<str>,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !state.route_publics.iter().any(|r| r == route.as_ref()) {
        let claims = match get_user_info_from_token(req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: crate::constants::ERR_INVALID_TOKEN.to_string(),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        };

    if !check_access(&claims, route.as_ref(), "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let table_schema = filter_table_schema(&table_schemas, route.as_ref());
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    // MongoDB: No DDL needed. Consider this a success to match behavior.
    if state.db_type == "mongodb" {
        log_output(
            "INFO",
            "GENERATE TABLE",
            route.as_ref(),
            format!("MongoDB backend: skipping DDL for collection '{}'.", table_schema.table),
            true,
        );
        return HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Table created (MongoDB - no DDL)".to_string(),
            total_data: 1,
            data: Value::default(),
        });
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let (sql_create_table, sql_create_index) = generate_table(&ds, &table_schema);
    let mut err_message = String::new();

    // Run DDL statements inside a TxStore transaction for consistency
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::default(),
            });
        }
    };

    // execute create table
    if let Err(err) = tx.raw_sql(&sql_create_table, vec![]).await {
        let error_message = format!(
            "Failed to create table {} with error : {}",
            table_schema.table, err
        );
        log_output(
            "QUERY",
            "GENERATE TABLE",
            route.as_ref(),
            sql_create_table.clone() + " ~ ERROR : " + &error_message,
            true,
        );
        err_message = error_message;
    }

    // execute each create index
    for sql_idx in sql_create_index.iter() {
        if let Err(err) = tx.raw_sql(sql_idx, vec![]).await {
            err_message = format!(
                "{} \nFailed to create index {} with error : {}",
                err_message, table_schema.table, err
            );
            log_output(
                "QUERY",
                "GENERATE INDEX",
                route.as_ref(),
                sql_idx.clone() + " ~ ERROR : " + &err_message,
                true,
            );
        }
    }

    if err_message.is_empty() {
        let _ = tx.commit().await;
    } else {
        let _ = tx.rollback().await;
    }

    if !err_message.is_empty() {
        HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: err_message,
            total_data: 0,
            data: Value::default(),
        })
    } else {
        HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Table created".to_string(),
            total_data: 1,
            data: Value::default(),
        })
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

        let mut primary_key_inline = false;
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
                ty = "INTEGER PRIMARY KEY AUTOINCREMENT".into();
                primary_key_inline = true;
                any_inline_pk = true;
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
        });
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
    });

    // Compile using SqlStore
    ds.preview_ddl_separate(&ddl)
}
