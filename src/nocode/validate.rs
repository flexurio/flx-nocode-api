use actix_web::{web::Data, HttpResponse, Responder};
use serde_json::{json, Value};

use crate::{
    auth::{check_access, get_user_info_from_token},

    model::{
        TableSchema, WebResponse,
    },
    AppState,
};
use std::sync::Arc;

// NCO-VALIDATE
pub async fn check_table_design(
    state: Data<AppState>,
    route: String,
    table_schema_in: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if state.require_auth && !state.route_publics.contains(&route){
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

    // get table schema from table_schemas where table = route
    // let table_schema = filter_table_schema(&table_schemas, route.clone()).await; -- Use passed schema
    let table_schema = table_schema_in.as_ref().clone(); // Clone for validation mutation or just deref? validate_table_design takes value
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Check table schema
    match validate_table_design(&table_schema) {
        Ok(_) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Table validated".to_string(),
            total_data: 1,
            data: json!(table_schema),
        }),
        Err(errors) => HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Schema validation failed".to_string(),
            total_data: 0,
            data: json!({ "errors": errors }),
        }),
    }
}

pub fn validate_table_design(design: &TableSchema) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    // Check if table exists
    if design.table.is_empty() {
        errors.push("Table name cannot be empty".to_string());
    }

    // Check primary key
    if design.primary_key.columns.is_empty() {
        errors.push("Primary key columns cannot be empty".to_string());
    } else {
        for pk_col in &design.primary_key.columns {
            if !design.columns.iter().any(|col| col.name == *pk_col) {
                errors.push(format!("Primary key column '{}' does not exist in columns", pk_col));
            }
        }
    }

    // Check columns
    if design.columns.is_empty() {
        errors.push("Columns cannot be empty".to_string());
    }

    // Check indexes
    for index in &design.indexes {
        for index_col in &index.columns {
            if !design.columns.iter().any(|col| col.name == *index_col) {
                errors.push(format!("Index column '{}' does not exist in columns", index_col));
            }
            if design.primary_key.columns.contains(index_col) {
                errors.push(format!("Primary key column '{}' should not be indexed", index_col));
            }
        }
    }

    // Check GET parameters
    let required_params = ["search", "page", "sort", "ascending", "limit"];
    let has_required_params = required_params
        .iter()
        .all(|p| design.get.parameters.contains(&p.to_string()));

    if !has_required_params && design.get.enable_method {
         errors.push("GET parameters must contain search, page, sort, ascending, limit".to_string());
    }

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
                 errors.push(format!("GET parameter '{}' does not exist in columns or joined tables", param));
             } else if !design.primary_key.columns.contains(&param_name.to_string()) {
                 let in_index = design.indexes.iter().any(|idx| idx.columns.contains(&param_name.to_string()));
                 if !in_index {
                      errors.push(format!("GET parameter '{}' is not indexed (and not PK)", param));
                 }
             }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}
