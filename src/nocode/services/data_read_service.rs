use actix_web::{web, HttpRequest, HttpResponse};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::auth::{check_access, get_user_info_from_token};
use crate::database::redis::{build_key_prefix, redis_get_json, redis_set_json};
use crate::log::log_output;
use crate::model::{TableSchema, WebResponse};
use crate::nocode::repositories::data_read_repo;

pub async fn process_get_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    req: &HttpRequest,
) -> HttpResponse {
    let mut cache_tenant = String::from("public");
    
    // Auth Check
    if state.require_auth && !state.route_publics.contains(&route.to_string()) {
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

        if !claims.id.is_empty() {
            cache_tenant = claims.id.clone();
        }

        if let Err(e) = check_access(&claims, req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // Schema Validations
    if table_schema.table.is_empty() {
        let message_error = format!(
            "ER01(nocode_get): Entity {} on folder config/{}.json not found",
            route, route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    if table_schema.get.columns.is_empty() {
        let message_error = format!(
            "ER02(nocode_get): No columns defined for GET operation on entity {}",
            route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Parameter parsing
    let params_obj = parameters.clone().into_inner();
    let params_map_awal = match params_obj.as_object() {
        Some(map) => map,
        None => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: "Invalid parameters format".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    };
    let mut params_map = params_map_awal.clone();
    let mut table_schema_get_params = table_schema.get.parameters.clone();

    // Required Param Validation
    let missing_required =
        super::collect_missing_required_params(&mut table_schema_get_params, &params_map);
    if !missing_required.is_empty() {
        return HttpResponse::BadRequest().json(super::web_err(format!(
            "Required parameters missing: {}",
            missing_required.join(", ")
        )));
    }

    // Cache Logic
    let mut isredis = false;
    let mut use_cache = false;
    let mut cache_key: Option<String> = None;

    if state.is_cachedb
        && let Some(redis_val) = params_map_awal.get("redis") {
            isredis = match redis_val {
                Value::Bool(b) => *b,
                Value::String(s) => {
                    let s_lower = s.to_ascii_lowercase();
                    s_lower == "true" || s_lower == "1" || s_lower == "yes"
                }
                Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
                _ => false,
            };
            params_map.remove("redis");
        }

    // log isredis 
    log_output("DEBUG", "ISREDIS", route, format!("isredis: {}", isredis), true);

    if isredis {
        use_cache = (isredis || table_schema.redis.ttl > 0) && state.is_cachedb;
        if use_cache {
            let prefix = build_key_prefix(&cache_tenant, route);
            let mut keys: Vec<_> = params_map
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            keys.sort();
            let key_suffix = keys.join("&");
            let full_key = if key_suffix.is_empty() { prefix } else { format!("{}:{}", prefix, key_suffix) };
            cache_key = Some(full_key);

            if let Some(ref k) = cache_key {
                match redis_get_json::<WebResponse>(k.as_str()).await {
                    Ok(Some(cached)) => {
                        log_output("REDIS", "CACHE HIT", route, format!("Key: {}, Records: {}", k, cached.total_data), true);
                        return HttpResponse::Ok().json(cached);
                    }
                    Ok(None) => {
                        log_output("REDIS", "CACHE MISS", route, format!("Key: {}", k), true);
                    }
                    Err(e) => {
                        log_output("ERROR", "CACHE READ", route, format!("Redis error: {} - falling back to DB", e), false);
                    }
                }
            }
        }
    }

    // Call Repository
    match data_read_repo::fetch_dynamic_data(state, route, table_schema, &params_map).await {
        Ok((rows, total)) => {            
            let result = WebResponse {
                success: true,
                message: "Data found".to_string(),
                total_data: total as i32,
                data: Value::Array(rows),
            };

            // Cache Write
            if let Some(k) = cache_key
                .as_ref()
                .filter(|_| use_cache && state.is_cachedb && table_schema.redis.ttl > 0)
            {
                let ttl = table_schema.redis.ttl as usize;
                if let Err(e) = redis_set_json(k, &result, Some(ttl)).await {
                     log_output("ERROR", "CACHE WRITE", route, format!("Failed to cache: {}", e), false);
                } else {
                     log_output("REDIS", "CACHE WRITE", route, format!("Key: {}, TTL: {}s, Records: {}", k, ttl, total), true);
                }
            }

            HttpResponse::Ok().json(result)
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: e, // Error message formatted in repo
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
