use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use futures_util::future::{ready, Ready, LocalBoxFuture};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::rc::Rc;
use ahash::AHashMap;

use crate::helpers::get_client_ip;
use crate::model::WebResponse;
use crate::rate_limit::{RL_WINDOW_GET, RL_WINDOW_MUTATE, build_key, RateOp};
use crate::auth::validate_token;
use crate::database::state::AppState;
use crate::metrics::METRICS;
use actix_web::{web, HttpResponse};
use crate::log::log_output;

// Preload limits: global override or per-class (GET vs MUTATE)
static RL_ALL: Lazy<Option<i64>> = Lazy::new(|| std::env::var("RATE_LIMIT_ALL_PER_SEC").ok().and_then(|v| v.parse().ok()));
static RL_GET: Lazy<i64> = Lazy::new(|| std::env::var("RATE_LIMIT_GET_PER_SEC").ok().and_then(|v| v.parse().ok()).unwrap_or(20));
static RL_MUTATE: Lazy<i64> = Lazy::new(|| std::env::var("RATE_LIMIT_MUTATE_PER_SEC").ok().and_then(|v| v.parse().ok()).unwrap_or(10));
// MCP endpoint: every JSON-RPC message (reads included) is a POST, so it gets its
// own bucket instead of the write limit. Individual tool calls still hit the
// service layer, which applies no extra limit of its own.
#[cfg(feature = "mcp")]
static RL_MCP: Lazy<i64> = Lazy::new(|| std::env::var("RATE_LIMIT_MCP_PER_SEC").ok().and_then(|v| v.parse().ok()).unwrap_or(30));

// Allow per-method override if desired (optional; falls back to GET / MUTATE buckets)
// Use AHashMap for faster lookups (optimized hash function for strings)
static RL_METHOD: Lazy<AHashMap<&'static str, i64>> = Lazy::new(|| {
    let mut map = AHashMap::new();
    for m in ["POST", "PUT", "DELETE", "PATCH", "TRACE", "IMPORT"].iter() {
        if let Some(parsed) = std::env::var(format!("RATE_LIMIT_{}_PER_SEC", m))
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        { map.insert(*m, parsed); }
    }
    map
});

pub struct GlobalRateLimit;

impl<S> Transform<S, ServiceRequest> for GlobalRateLimit
where
    S: Service<ServiceRequest, Response = ServiceResponse<BoxBody>, Error = Error> + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = GlobalRateLimitMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(GlobalRateLimitMiddleware { service: Rc::new(service) }))
    }
}

pub struct GlobalRateLimitMiddleware<S> { service: Rc<S> }

impl<S> Service<ServiceRequest> for GlobalRateLimitMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<BoxBody>, Error = Error> + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, ctx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Standard HTTP methods are always uppercase on the wire (RFC 7230 method
        // token); avoid allocating a new String per request just to normalize case.
        let method = req.method().as_str();
        #[cfg(feature = "mcp")]
        let is_mcp = crate::mcp::MCP_CONFIG.is_mcp_path(req.path());
        #[cfg(not(feature = "mcp"))]
        let is_mcp = false;
        let limit_val = RL_ALL.unwrap_or_else(|| {
            #[cfg(feature = "mcp")]
            if is_mcp {
                return *RL_MCP;
            }
            if method == "GET" { *RL_GET } else { RL_METHOD.get(method).copied().unwrap_or(*RL_MUTATE) }
        });
        if limit_val > 0 {
            let ip = get_client_ip(req.request());
            let path_seg = req.path().trim_matches('/').split('/').next().unwrap_or("");
            let op = match method {
                "GET" => RateOp::Get,
                "POST" => RateOp::Post,
                "PUT" => RateOp::Put,
                "DELETE" => RateOp::Delete,
                "PATCH" => RateOp::Patch,
                "TRACE" => RateOp::Trace,
                _ => RateOp::Import,
            };
            let key = build_key(op, path_seg, &ip);
            let limiter = if method == "GET" { &*RL_WINDOW_GET } else { &*RL_WINDOW_MUTATE };
            if !limiter.check_and_increment(&key, limit_val as u32) {
                METRICS.record_rate_limit_hit();  // Record rate limit metric
                let resp = if is_mcp {
                    // JSON-RPC shaped so MCP clients can surface the error.
                    actix_web::HttpResponse::TooManyRequests()
                        .insert_header(("retry-after", "1"))
                        .json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": null,
                            "error": { "code": -32000, "message": "Too many requests; retry after 1 second" }
                        }))
                        .map_into_boxed_body()
                } else {
                    actix_web::HttpResponse::TooManyRequests().json(WebResponse { success: false, message: "Too many requests".into(), total_data: 0, data: Value::default() }).map_into_boxed_body()
                };
                let (req_head, _pl) = req.into_parts();
                return Box::pin(async move { Ok(ServiceResponse::new(req_head, resp)) });
            }
        }
        METRICS.record_request();  // Record all requests
        let fut = self.service.call(req);
        Box::pin(fut)
    }
}

// ---------------- Authentication Middleware ----------------
// Replicates previous inline wrap_fn auth logic so we can unify body type handling cleanly.
pub struct AuthMiddleware;

impl<S> Transform<S, ServiceRequest> for AuthMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<BoxBody>, Error = Error> + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddlewareImpl<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddlewareImpl { service: Rc::new(service) }))
    }
}

pub struct AuthMiddlewareImpl<S> { service: Rc<S> }

const AUTH_WHITELIST: [&str; 6] = ["/login", "/register", "/healthz", "/livez", "/readyz", "/token/refresh"];

impl<S> Service<ServiceRequest> for AuthMiddlewareImpl<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<BoxBody>, Error = Error> + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, ctx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Fast-path whitelist
        if AUTH_WHITELIST.contains(&req.path()) {
            let fut = self.service.call(req);
            return Box::pin(fut);
        }
        // MCP authorization discovery (RFC 9728) must be reachable without a token.
        #[cfg(feature = "mcp")]
        if crate::mcp::auth::is_public_discovery_path(req.path()) {
            let fut = self.service.call(req);
            return Box::pin(fut);
        }
        // Access AppState to evaluate public routes
        let is_public = req
            .app_data::<web::Data<AppState>>()
            .map(|st| {
                let route = req.path().trim_start_matches('/');
                st.is_public_route(route)
            })
            .unwrap_or(false);
        if is_public {
            let fut = self.service.call(req);
            return Box::pin(fut);
        }
        // Validate token
        if let Some(app_state) = req.app_data::<web::Data<AppState>>() {
            #[allow(clippy::needless_borrow)]
            match validate_token(req.request(), &app_state) {
                Ok(claims) => {
                    use actix_web::HttpMessage;
                    req.extensions_mut().insert(claims);
                    let fut = self.service.call(req);
                    Box::pin(fut)
                }
                Err(err_resp) => {
                    #[allow(unused_mut)]
                    let mut resp = err_resp.map_into_boxed_body();
                    // MCP spec: a 401 from the MCP endpoint carries a
                    // WWW-Authenticate challenge pointing at the resource metadata.
                    #[cfg(feature = "mcp")]
                    if resp.status() == actix_web::http::StatusCode::UNAUTHORIZED
                        && crate::mcp::MCP_CONFIG.is_mcp_path(req.path())
                    {
                        let presented = req.headers().contains_key(actix_web::http::header::AUTHORIZATION);
                        let challenge = crate::mcp::auth::www_authenticate(&crate::mcp::MCP_CONFIG, presented);
                        if let Ok(v) = actix_web::http::header::HeaderValue::from_str(&challenge) {
                            resp.headers_mut().insert(actix_web::http::header::WWW_AUTHENTICATE, v);
                        }
                    }
                    let (parts, _pl) = req.into_parts();
                    Box::pin(async move { Ok(ServiceResponse::new(parts, resp)) })
                }
            }
        } else {
            let resp = HttpResponse::InternalServerError().json(WebResponse { success: false, message: "AppState missing".into(), total_data: 0, data: Value::default() }).map_into_boxed_body();
            let (parts, _pl) = req.into_parts();
            Box::pin(async move { Ok(ServiceResponse::new(parts, resp)) })
        }
    }
}

// ---------------- Request observer (StatusLogger) ----------------
// Per request: assigns/propagates `x-request-id`, measures latency into the
// `flx_http_request_duration_seconds` histogram, emits one `tracing` access
// event, and keeps the old 404/405 debug line.

static REQUEST_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Known first path segments get their own series; everything else is folded
/// into `other` so a path scanner cannot inflate metric cardinality.
fn route_label(path: &str) -> String {
    let seg = path.trim_matches('/').split('/').next().unwrap_or("");
    if seg.is_empty() {
        return "root".to_string();
    }
    if crate::registry::current().schemas.contains_key(seg)
        || matches!(seg, "login" | "register" | "roles" | "healthz" | "livez" | "readyz" | "metrics" | "static" | "email" | "import" | "export" | "validate" | "generate" | "seed" | "mcp")
    {
        seg.to_string()
    } else {
        "other".to_string()
    }
}

fn new_request_id() -> String {
    let n = REQUEST_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    format!("{:x}-{:x}", t, n)
}

pub struct StatusLogger;

impl<S, B> Transform<S, ServiceRequest> for StatusLogger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = StatusLoggerImpl<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(StatusLoggerImpl { service: Rc::new(service) }))
    }
}

pub struct StatusLoggerImpl<S> { service: Rc<S> }

impl<S, B> Service<ServiceRequest> for StatusLoggerImpl<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, ctx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        let start = std::time::Instant::now();
        let method = req.method().clone();
        let path = req.path().to_string();
        let route = route_label(&path);
        // Propagate the caller's id (max 64 visible ASCII chars) or mint one.
        let request_id = req
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_graphic()))
            .map(|s| s.to_string())
            .unwrap_or_else(new_request_id);
        if let Ok(v) = actix_web::http::header::HeaderValue::from_str(&request_id) {
            req.headers_mut().insert(actix_web::http::header::HeaderName::from_static("x-request-id"), v);
        }

        let fut = self.service.call(req);
        Box::pin(async move {
            let mut res = fut.await?;
            let status = res.status();
            let elapsed = start.elapsed();

            if let Ok(v) = actix_web::http::header::HeaderValue::from_str(&request_id) {
                res.headers_mut().insert(actix_web::http::header::HeaderName::from_static("x-request-id"), v);
            }
            METRICS.record_http(&route, method.as_str(), status.as_u16(), elapsed);

            tracing::info!(
                target: "flx::access",
                request_id = %request_id,
                method = %method,
                path = %path,
                route = %route,
                status = status.as_u16(),
                latency_ms = elapsed.as_secs_f64() * 1000.0,
                "request"
            );

            if status == StatusCode::METHOD_NOT_ALLOWED || status == StatusCode::NOT_FOUND {
                let allow_hdr = res
                    .headers()
                    .get("Allow")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                log_output(
                    "HTTP",
                    "STATUS",
                    "DEBUG",
                    if allow_hdr.is_empty() {
                        format!("{} {} -> {}", method, path, status)
                    } else {
                        format!("{} {} -> {} (Allow: {})", method, path, status, allow_hdr)
                    },
                    false,
                );
            }

            Ok(res)
        })
    }
}
