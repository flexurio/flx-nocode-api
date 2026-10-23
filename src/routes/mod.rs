//! Route registration for the Actix-web application.
//!
//! Static (core) endpoints are registered first. Entity endpoints are **not**
//! registered per entity any more: a handful of catch-all resources
//! (`/{route}`, `/{route}/{id}`, `/import/{route}`, …) resolve the entity from
//! the live [`crate::registry`] on every request, which is what makes a hot
//! reload (`POST /admin/reload`, `SIGHUP`) take effect without a restart.
//! Resource order matters: actix matches in registration order, so the static
//! resources above always win over `/{route}`.

use actix_files::Files;
use actix_multipart::Multipart;
use actix_web::http::Method;
use actix_web::web::Path;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use colored::Colorize;
use serde_json::Value;
use std::sync::Arc;

use crate::config::ISDEBUG;
use crate::core::{get_roles, login, logout, refresh_token, register};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::metrics::METRICS;
use crate::model::{TableSchema, WebResponse};
use crate::nocode::generate::create_table;
use crate::nocode::seed::seed_table;
use crate::nocode::validate::check_table_design;
use crate::registry;

// ── Logging helper ────────────────────────────────────────────────────────────

/// Emit a single `METHOD → URL` log line. No-op when `do_log` is false.
#[inline]
fn log_ep(do_log: bool, label: &str, method: &str, host: &str, port: &str, path: &str) {
    if !do_log {
        return;
    }
    log_output(
        label,
        "METHOD",
        method,
        format!("http://{}:{}/{}", host.red(), port.green(), path.purple()),
        false,
    );
}

fn json_err(status: actix_web::http::StatusCode, msg: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).json(WebResponse {
        success: false,
        message: msg.into(),
        total_data: 0,
        data: Value::Null,
    })
}

/// Resolve `route` against the live registry. `flx_users` / `flx_roles` are
/// hidden when authentication is disabled (they are not generated either).
fn resolve(state: &AppState, route: &str) -> Result<Arc<TableSchema>, Box<HttpResponse>> {
    if !state.require_auth && (route == "flx_users" || route == "flx_roles") {
        return Err(Box::new(json_err(actix_web::http::StatusCode::NOT_FOUND, format!("Route '{}' not found", route))));
    }
    registry::schema(route)
        .ok_or_else(|| Box::new(json_err(actix_web::http::StatusCode::NOT_FOUND, format!("Route '{}' not found", route))))
}

fn method_disabled(route: &str, method: &Method) -> HttpResponse {
    json_err(
        actix_web::http::StatusCode::METHOD_NOT_ALLOWED,
        format!("{} is not enabled for route '{}'", method, route),
    )
}

fn boxed(r: impl Responder, req: &HttpRequest) -> HttpResponse {
    r.respond_to(req).map_into_boxed_body()
}

// ── Dynamic entity handlers ───────────────────────────────────────────────────

async fn entity_get(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.get.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(crate::nocode::handlers::get_handler::select(state, parameters, route, schema, req.clone()).await, &req)
}

async fn entity_post(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    payload: web::Payload,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.post.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(
        crate::nocode::handlers::post_handler::insert(state, parameters, route, schema, payload, req.clone()).await,
        &req,
    )
}

async fn entity_trace(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.trace.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(crate::nocode::handlers::trace_handler::process(state, parameters, route, schema, req.clone()).await, &req)
}

async fn entity_patch(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.patch.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(crate::nocode::handlers::patch_handler::process_sp(state, parameters, route, schema, req.clone()).await, &req)
}

async fn entity_delete(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    path: Path<(String, String)>,
) -> HttpResponse {
    let (route, id) = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.del.enable_method {
        return method_disabled(&route, req.method());
    }
    let ref_fks = registry::current().ref_foreign_keys.clone();
    boxed(
        crate::nocode::handlers::delete_handler::delete(
            state, parameters, route, schema, ref_fks, Path::from(id), req.clone(),
        )
        .await,
        &req,
    )
}

/// PUT and PATCH on `/{route}/{id}` share the update handler (PATCH = partial / status update).
async fn entity_put(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    payload: web::Payload,
    path: Path<(String, String)>,
) -> HttpResponse {
    let (route, id) = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.put.enable_method {
        return method_disabled(&route, req.method());
    }
    let ref_fks = registry::current().ref_foreign_keys.clone();
    boxed(
        crate::nocode::handlers::put_handler::update(
            state, parameters, route, schema, ref_fks, payload, Path::from(id), req.clone(),
        )
        .await,
        &req,
    )
}

async fn entity_import(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    multipart: Multipart,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.post.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(
        crate::nocode::handlers::import_handler::import(state, parameters, route, schema, multipart, req.clone()).await,
        &req,
    )
}

async fn entity_export_get(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.get.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(
        crate::nocode::handlers::export_handler::export_get(state, parameters, route, schema, req.clone()).await,
        &req,
    )
}

async fn entity_export_post(
    state: web::Data<AppState>,
    req: HttpRequest,
    parameters: web::Query<Value>,
    multipart: Multipart,
    path: Path<String>,
) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.get.enable_method {
        return method_disabled(&route, req.method());
    }
    boxed(
        crate::nocode::handlers::export_handler::export_post(state, parameters, route, schema, multipart, req.clone())
            .await,
        &req,
    )
}

async fn entity_validate(state: web::Data<AppState>, req: HttpRequest, path: Path<String>) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    boxed(check_table_design(state, route, schema, req.clone()).await, &req)
}

async fn entity_generate_table(state: web::Data<AppState>, req: HttpRequest, path: Path<String>) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.auto_generate || route == "flx_users" || route == "flx_roles" {
        return json_err(
            actix_web::http::StatusCode::NOT_FOUND,
            format!("Table generation is not enabled for route '{}'", route),
        );
    }
    boxed(create_table(state, route, schema, req.clone()).await, &req)
}

async fn entity_seed(state: web::Data<AppState>, req: HttpRequest, path: Path<String>) -> HttpResponse {
    let route = path.into_inner();
    let schema = match resolve(&state, &route) { Ok(s) => s, Err(r) => return *r };
    if !schema.seed || route == "flx_users" || route == "flx_roles" {
        return json_err(
            actix_web::http::StatusCode::NOT_FOUND,
            format!("Seeding is not enabled for route '{}'", route),
        );
    }
    boxed(seed_table(state, route, schema, req.clone()).await, &req)
}

// ── Admin: hot reload ─────────────────────────────────────────────────────────

/// Roles allowed to call `/admin/*`. Matched against the part before `/` of
/// each JWT role entry (`flx_roles/127` → `flx_roles`). Env `ADMIN_ROLES`.
fn admin_roles() -> Vec<String> {
    std::env::var("ADMIN_ROLES")
        .unwrap_or_else(|_| "admin,Super Admin,Administrator,flx_roles".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn is_admin(state: &AppState, req: &HttpRequest) -> bool {
    if !state.require_auth {
        return true;
    }
    use actix_web::HttpMessage;
    let Some(claims) = req.extensions().get::<crate::auth::Claims>().cloned() else { return false };
    if claims.id == "1" {
        return true;
    }
    let allowed = admin_roles();
    claims
        .get_roles()
        .iter()
        .any(|r| allowed.iter().any(|a| r.split('/').next().unwrap_or("") == a))
}

async fn admin_reload(state: web::Data<AppState>, req: HttpRequest) -> HttpResponse {
    if !is_admin(&state, &req) {
        return json_err(actix_web::http::StatusCode::FORBIDDEN, "Admin role required");
    }
    match registry::reload(&state).await {
        Ok(report) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: format!("Configuration reloaded (generation {})", report.generation),
            total_data: report.routes as i32,
            data: serde_json::to_value(report).unwrap_or(Value::Null),
        }),
        Err(e) => {
            log_output("ERROR", "CONFIG-RELOAD", "admin", e.clone(), true);
            json_err(actix_web::http::StatusCode::UNPROCESSABLE_ENTITY, format!("Reload rejected, previous configuration kept: {}", e))
        }
    }
}

async fn admin_registry(state: web::Data<AppState>, req: HttpRequest) -> HttpResponse {
    if !is_admin(&state, &req) {
        return json_err(actix_web::http::StatusCode::FORBIDDEN, "Admin role required");
    }
    let reg = registry::current();
    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Live configuration".to_string(),
        total_data: reg.routes.len() as i32,
        data: serde_json::json!({
            "generation": reg.generation,
            "loaded_at": reg.loaded_at.to_rfc3339(),
            "routes": reg.routes,
            "route_publics": reg.route_publics,
        }),
    })
}

// ── Public registration function ──────────────────────────────────────────────

/// Register all routes — static (core) and dynamic (nocode) — on `cfg`.
///
/// Called once per Actix worker from inside `HttpServer::new(move || { ... })`.
pub fn configure_routes(
    cfg: &mut web::ServiceConfig,
    require_auth: bool,
    do_log: bool,
    host: &'static str,
    port: u16,
    app_state: web::Data<AppState>,
) {
    let port_str = port.to_string();

    // ── Static files ─────────────────────────────────────────────────────────
    {
        let static_loc = std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
        let static_files = Files::new("/static", static_loc);
        if *ISDEBUG {
            cfg.service(static_files.show_files_listing());
        } else {
            cfg.service(static_files);
        }
        log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "static");
    }

    // ── Auth endpoints ────────────────────────────────────────────────────────
    if require_auth {
        cfg.service(web::resource("/login").route(web::post().to(
            move |state: web::Data<AppState>, req: HttpRequest| login(state, req),
        )));
        log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "login");

        cfg.service(web::resource("/register").route(web::post().to(
            move |state: web::Data<AppState>, multipart: Multipart| register(state, multipart),
        )));
        log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "register");

        cfg.service(web::resource("/token/refresh").route(web::post().to(refresh_token)));
        cfg.service(web::resource("/logout").route(web::post().to(logout)));
        log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "token/refresh | logout");
    }

    // ── Roles ─────────────────────────────────────────────────────────────────
    cfg.service(web::resource("/roles").route(web::get().to(move |state: web::Data<AppState>| get_roles(state))));
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "roles");

    // ── Health: liveness (process up) and readiness (database reachable) ─────
    cfg.service(web::resource("/livez").route(web::get().to(|| async {
        HttpResponse::Ok().json(serde_json::json!({ "status": "ok" }))
    })));
    let readiness = {
        let state = app_state.clone();
        move || {
            let state = state.clone();
            async move {
                let db_ok = tokio::time::timeout(std::time::Duration::from_secs(2), state.db.query("SELECT 1"))
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false);
                let body = serde_json::json!({
                    "status": if db_ok { "ok" } else { "degraded" },
                    "db": if db_ok { "up" } else { "down" },
                    "db_type": state.db_type,
                    "config_generation": registry::current().generation,
                });
                if db_ok { HttpResponse::Ok().json(body) } else { HttpResponse::ServiceUnavailable().json(body) }
            }
        }
    };
    cfg.service(web::resource("/healthz").route(web::get().to(readiness.clone())));
    cfg.service(web::resource("/readyz").route(web::get().to(readiness)));
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "healthz | livez | readyz");

    // ── Prometheus metrics ────────────────────────────────────────────────────
    cfg.service(web::resource("/metrics").route(web::get().to({
        let state = app_state.clone();
        move || {
            let state = state.clone();
            async move {
                let out = METRICS.to_prometheus_format(state.db.pool_stats());
                HttpResponse::Ok()
                    .content_type("text/plain; version=0.0.4; charset=utf-8")
                    .body(out)
            }
        }
    })));
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "metrics");

    // ── Email queue: POST /email/send ─────────────────────────────────────────
    cfg.service(web::resource("/email/send").route(web::post().to(crate::nocode::handlers::email_handler::send)));
    log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "email/send");

    // ── Admin: live configuration ─────────────────────────────────────────────
    cfg.service(web::resource("/admin/reload").route(web::post().to(admin_reload)));
    cfg.service(web::resource("/admin/registry").route(web::get().to(admin_registry)));
    log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "admin/reload");

    // ── Dynamic nocode routes (resolved per request from the registry) ────────
    // Specific prefixes first, then the two catch-alls.
    cfg.service(web::resource("/import/{route}").route(web::post().to(entity_import)));
    cfg.service(
        web::resource("/export/{route}")
            .route(web::get().to(entity_export_get))
            .route(web::post().to(entity_export_post)),
    );
    cfg.service(web::resource("/validate/{route}").route(web::get().to(entity_validate)));
    cfg.service(web::resource("/generate/table/{route}").route(web::post().to(entity_generate_table)));
    cfg.service(web::resource("/generate/seed/{route}").route(web::post().to(entity_seed)));
    cfg.service(web::resource("/seed/{route}").route(web::post().to(entity_seed)));
    cfg.service(
        web::resource("/{route}")
            .route(web::get().to(entity_get))
            .route(web::post().to(entity_post))
            .route(web::trace().to(entity_trace))
            .route(web::patch().to(entity_patch)),
    );
    cfg.service(
        web::resource("/{route}/{id}")
            .route(web::delete().to(entity_delete))
            .route(web::put().to(entity_put))
            .route(web::patch().to(entity_put)),
    );

    if do_log {
        let reg = registry::current();
        for route in reg.routes.iter() {
            if !require_auth && (route == "flx_users" || route == "flx_roles") {
                continue;
            }
            let Some(schema) = reg.schemas.get(route) else { continue };
            let mut methods: Vec<&str> = Vec::new();
            if schema.get.enable_method { methods.push("GET"); methods.push("GET /export"); }
            if schema.post.enable_method { methods.push("POST"); methods.push("POST /import"); }
            if schema.put.enable_method { methods.push("PUT /{id}"); methods.push("PATCH /{id}"); }
            if schema.patch.enable_method { methods.push("PATCH"); }
            if schema.del.enable_method { methods.push("DELETE /{id}"); }
            if schema.trace.enable_method { methods.push("TRACE"); }
            if schema.auto_generate { methods.push("POST /generate/table"); }
            if schema.seed { methods.push("POST /seed"); }
            log_ep(true, "ENDPOINT", &methods.join(", "), host, &port_str, route);
        }
        println!("\n");
    }
}
