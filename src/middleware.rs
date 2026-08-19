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
        let method = req.method().as_str().to_uppercase();
        let limit_val = RL_ALL.unwrap_or_else(|| {
            if method == "GET" { *RL_GET } else { RL_METHOD.get(method.as_str()).copied().unwrap_or(*RL_MUTATE) }
        });
        if limit_val > 0 {
            let ip = get_client_ip(req.request());
            let path_seg = req.path().trim_matches('/').split('/').next().unwrap_or("");
            let op = match method.as_str() {
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
                let resp = actix_web::HttpResponse::TooManyRequests().json(WebResponse { success: false, message: "Too many requests".into(), total_data: 0, data: Value::default() }).map_into_boxed_body();
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

const AUTH_WHITELIST: [&str; 3] = ["/login", "/register", "/healthz"];

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
        // Access AppState to evaluate public routes
        let is_public = req
            .app_data::<web::Data<AppState>>()
            .map(|st| {
                let route = req.path().trim_start_matches('/');
                st.route_publics.contains(&route.to_string())
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
                    let resp = err_resp.map_into_boxed_body();
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

// ---------------- Status Logger Middleware ----------------
// Log all requests that end up with 404 / 405 for easier debugging
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

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let method = req.method().clone();
        let path = req.path().to_string();

        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await?;
            let status = res.status();

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
