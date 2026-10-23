use actix_web::{web, HttpRequest, HttpResponse};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::auth::{check_access, get_user_info_from_token};
use actix_web::http::header;
use crate::database::redis::{redis_get, redis_set, route_version};
use crate::database::state::{build_cache_key, CachedPage};
use crate::metrics::METRICS;
use crate::log::{log_output, log_output_lazy};
use crate::model::{TableSchema, WebResponse};
use crate::nocode::repositories::data_read_repo;

pub async fn process_get_request(
    state: &web::Data<AppState>,
    parameters: &web::Query<Value>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    req: &HttpRequest,
) -> HttpResponse {
    // Role-specific projection from rules.json `allowed_fields` (None = no restriction).
    let mut field_filter: Option<Vec<String>> = None;
    
    // Auth Check
    if state.require_auth && !state.route_publics.contains(route) {
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

        // NOTE: the cache key is intentionally NOT scoped to the user. Read results
        // are not user-specific (rules.json only gates route+method), and every
        // writer invalidates `flx:public:<route>*`; a per-user key was never
        // invalidated and served stale rows until the Redis TTL expired.
        let _ = &claims.id;

        if let Err(e) = check_access(&claims, req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
        field_filter = crate::auth::allowed_fields_for(&claims, req);
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

    // Parameter parsing (one clone of the extractor; the map is then owned).
    let mut params_map = match parameters.clone().into_inner() {
        Value::Object(map) => map,
        _ => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: "Invalid parameters format".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    };
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

    // `?redis=` is a control parameter: honoured, then removed before planning.
    let isredis = match params_map.remove("redis") {
        Some(Value::Bool(b)) => b,
        Some(Value::String(s)) => {
            let s_lower = s.to_ascii_lowercase();
            s_lower == "true" || s_lower == "1" || s_lower == "yes"
        }
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        _ => false,
    };
    log_output_lazy("DEBUG", "ISREDIS", route, || format!("isredis: {}", isredis), true);

    // A role-specific projection must never be served from, or written to, the shared cache.
    let use_cache = (isredis || table_schema.redis.ttl > 0) && field_filter.is_none();

    if use_cache {
        let version = route_version(route, state.is_cachedb).await;
        let key = build_cache_key(route, version, &params_map);
        let if_none_match = req
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Single-flight: concurrent requests for the same key share ONE loader
        // (L2 lookup, then DB). No stampede when a hot page expires.
        let st = state.clone();
        let route_s = route.to_string();
        let schema = table_schema.clone();
        let pm = params_map.clone();
        let k2 = key.clone();
        let ttl = if table_schema.redis.ttl > 0 { table_schema.redis.ttl as usize } else { 300 };
        let loaded = state
            .l1_cache
            .try_get_with(key, async move {
                // Tier 2: Redis (bounded by REDIS_CACHE_TIMEOUT_MS; errors = miss).
                if st.is_cachedb {
                    match redis_get(&k2).await {
                        Ok(Some(raw)) => {
                            if let Some(page) = CachedPage::decode(raw) {
                                METRICS.record_cache_hit();
                                log_output_lazy("REDIS", "CACHE HIT", &route_s, || format!("Key: {}, Records: {}", k2, page.total_data), true);
                                return Ok::<CachedPage, String>(page);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => log_output("ERROR", "CACHE READ", &route_s, format!("Redis error: {} - falling back to DB", e), false),
                    }
                }
                METRICS.record_cache_miss();

                let (rows, total, next_cursor) =
                    data_read_repo::fetch_dynamic_data(&st, &route_s, &schema, &pm).await?;
                let result = WebResponse {
                    success: true,
                    message: "Data found".to_string(),
                    total_data: total as i32,
                    data: Value::Array(rows),
                };
                let page = CachedPage::from_response(&result, next_cursor)
                    .ok_or_else(|| "Failed to serialize response".to_string())?;

                // Tier 2 write: never make the client wait for the Redis round-trip.
                if st.is_cachedb {
                    let payload = page.encode();
                    let route2 = route_s.clone();
                    tokio::spawn(async move {
                        if let Err(e) = redis_set(&k2, &payload, Some(ttl)).await {
                            log_output("ERROR", "CACHE WRITE", &route2, format!("Failed to cache: {}", e), false);
                        }
                    });
                }
                Ok(page)
            })
            .await;

        return match loaded {
            Ok(page) => {
                if if_none_match.as_deref() == Some(page.etag.as_str()) {
                    let mut resp = HttpResponse::NotModified();
                    resp.insert_header((header::ETAG, page.etag.clone()));
                    return resp.finish();
                }
                let mut resp = HttpResponse::Ok();
                resp.insert_header((header::CONTENT_TYPE, "application/json"));
                resp.insert_header((header::ETAG, page.etag.clone()));
                if let Some(c) = &page.next_cursor {
                    resp.insert_header(("X-Next-Cursor", c.clone()));
                }
                // `Bytes` clone is a ref-count increment: no copy, no re-serialization.
                resp.body(page.body.clone())
            }
            Err(e) => HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: e.to_string(),
                total_data: 0,
                data: Value::Null,
            }),
        };
    }

    // Uncached path (role-specific projection, or caching disabled for the entity).
    match data_read_repo::fetch_dynamic_data(state, route, table_schema, &params_map).await {
        Ok((rows, total, next_cursor)) => {
            let mut data = Value::Array(rows);
            if let Some(allowed) = field_filter.as_deref() {
                crate::auth::retain_fields_on(allowed, &mut data);
            }
            let result = WebResponse {
                success: true,
                message: "Data found".to_string(),
                total_data: total as i32,
                data,
            };
            let mut resp = HttpResponse::Ok();
            if let Some(c) = next_cursor { resp.insert_header(("X-Next-Cursor", c)); }
            resp.json(result)
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
