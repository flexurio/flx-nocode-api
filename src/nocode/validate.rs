use actix_web::{web::Data, HttpResponse, Responder};
use serde_json::{json, Value};

use crate::{
    AppState, auth::{check_access, get_user_info_from_token}, log::log_output, model::{
        TableSchema, WebResponse,
    }
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

pub async fn validate_api_formula(formula: &str, body: &Value, auth_token: Option<&str>) -> Result<(), String> {
    if !formula.starts_with("API:") {
        return Ok(());
    }

    // Check for suffix |operator:response_path:request_variable
    // Operator can be: eq, neq, in (formerly EXISTS)
    let (base_formula, validation_rule) = match formula.find('|') {
        Some(idx) => {
            let rule = &formula[idx + 1..];
            (&formula[..idx], Some(rule))
        },
        None => (formula, None),
    };

    let parts: Vec<&str> = base_formula.splitn(3, ':').collect();
    // API:METHOD:URL
    if parts.len() < 3 {
        return Ok(()); 
    }

    let method = parts[1].to_uppercase();
    let url_formula = parts[2];

    match crate::database::state::build_url_from_formula(url_formula, body) {
        Ok(url) => {
            // log_output untuk debug
            log_output("DEBUG", "API VALIDATION","URL", url.to_string(), true);
            let client = reqwest::Client::new();
            let mut builder = match method.as_str() {
                "GET" => client.get(&url),
                "POST" => client.post(&url).json(body),
                "PUT" => client.put(&url).json(body),
                "DELETE" => client.delete(&url),
                _ => client.get(&url),
            };

            if let Some(token) = auth_token {
                builder = builder.header("Authorization", token);
            }

            match builder.send().await {
                Ok(res) => {
                    if !res.status().is_success() {
                        let status = res.status();
                        let msg = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        return Err(format!("Validation failed (API {}): {}", status, msg));
                    }
                    
                    // Operator Check Logic
                    if let Some(rule_str) = validation_rule {
                        let rule_parts: Vec<&str> = rule_str.splitn(3, ':').collect();
                        if rule_parts.len() == 3 {
                            let operator = rule_parts[0];
                            let resp_path = rule_parts[1];
                            let req_path = rule_parts[2];

                            let resp_json: Value = res.json().await.map_err(|e| format!("Failed to parse response JSON: {}", e))?;
                            
                            // Get value from response path
                            let resp_val_opt = crate::database::state::get_by_path_value(&resp_json, resp_path);

                            // Get value from request body to check
                            let req_key = req_path.strip_prefix("request.").unwrap_or(req_path);
                            let req_val_opt = crate::database::state::get_by_path_value(body, req_key);

                            // Handle unwrapping based on operator requirements
                            // 'eq', 'neq' need single values. 'in' needs array from response.

                            match operator {
                                "eq" => {
                                     let resp_val = resp_val_opt.ok_or_else(|| format!("Validation failed: Response path '{}' not found", resp_path))?;
                                     let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                     if resp_val != req_val {
                                         return Err(format!("Validation failed: Response '{:?}' != Request '{:?}'", resp_val, req_val));
                                     }
                                },
                                "neq" => {
                                     let resp_val = resp_val_opt.ok_or_else(|| format!("Validation failed: Response path '{}' not found", resp_path))?;
                                     let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                     if resp_val == req_val {
                                         return Err(format!("Validation failed: Response '{:?}' == Request '{:?}'", resp_val, req_val));
                                     }
                                },
                                "in" => {
                                    // Response must be array
                                    let target_array = match resp_val_opt {
                                        Some(Value::Array(arr)) => arr,
                                        _ => return Err(format!("Validation failed: Response path '{}' is not an array for 'in' operator", resp_path)),
                                    };
                                    let req_val = req_val_opt.ok_or_else(|| format!("Validation failed: Request variable '{}' not found", req_path))?;
                                    
                                    let found = target_array.iter().any(|item| item == req_val);
                                    if !found {
                                        return Err(format!("Validation failed: Value '{:?}' not found in allowed list", req_val));
                                    }
                                },
                                _ => {
                                    return Err(format!("Unknown validation operator: {}", operator));
                                }
                            }
                        }
                    }

                    Ok(())
                },
                Err(e) => {
                    Err(format!("Error calling validation API: {}", e))
                }
            }
        },
        Err(e) => {
            Err(format!("Error building validation URL: {}", e))
        }
    }
}
