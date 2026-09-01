//! Route registration for the Actix-web application.
//!
//! All static (core) and dynamic (nocode) endpoints are wired here, keeping
//! `main.rs` free of route-level detail.
//!
//! ## Key pattern — `RouteBundle`
//!
//! Rather than cloning `Arc<str>` and `Arc<TableSchema>` separately for every
//! HTTP-method closure, we pack them into [`RouteBundle`] and derive `Clone`.
//! Each handler closure only needs `let b = bundle.clone()` — one clone instead
//! of the previous 10+ per route.

use actix_files::Files;
use actix_multipart::Multipart;
use actix_web::{HttpResponse, web};
use actix_web::web::Path;
use colored::Colorize;
use serde_json::Value;
use std::sync::Arc;

use crate::config::{CONFIG, SCHEMAS, ISDEBUG};
use crate::core::{get_roles, login, register};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::metrics::METRICS;
use crate::model::{ReferenceForeignKey, TableSchema};
use crate::nocode::generate::create_table;
use crate::nocode::seed::seed_table;
use crate::nocode::validate::check_table_design;

// ── Route bundle ──────────────────────────────────────────────────────────────

/// Cheaply-clonable bundle of per-route shared handles.
/// All fields are `Arc`, so `.clone()` is O(1) ref-count increment.
#[derive(Clone)]
struct RouteBundle {
    route:   Arc<str>,
    schema:  Arc<TableSchema>,
    ref_fks: Arc<Vec<ReferenceForeignKey>>,
}

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
        let static_loc =
            std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
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
            move |state: web::Data<AppState>, req: actix_web::HttpRequest| login(state, req),
        )));
        log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "login");

        cfg.service(web::resource("/register").route(web::post().to(
            move |state: web::Data<AppState>, multipart: Multipart| register(state, multipart),
        )));
        log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "register");
    }

    // ── Roles ─────────────────────────────────────────────────────────────────
    cfg.service(
        web::resource("/roles")
            .route(web::get().to(move |state: web::Data<AppState>| get_roles(state))),
    );
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "roles");

    // ── Health check ──────────────────────────────────────────────────────────
    cfg.service(web::resource("/healthz").route(web::get().to({
        let state = app_state.clone();
        move || {
            let state = state.clone();
            async move {
                let db_ok = state.db.query("SELECT 1").await.is_ok();
                let body = serde_json::json!({
                    "status": "ok",
                    "db":      if db_ok { "up" } else { "down" },
                    "db_type": state.db_type,
                });
                if db_ok {
                    HttpResponse::Ok().json(body)
                } else {
                    HttpResponse::ServiceUnavailable().json(body)
                }
            }
        }
    })));
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "healthz");

    // ── Prometheus metrics ────────────────────────────────────────────────────
    cfg.service(web::resource("/metrics").route(web::get().to(|| async {
        let out = METRICS.to_prometheus_format();
        HttpResponse::Ok()
            .content_type("text/plain; charset=utf-8")
            .body(out)
    })));
    log_ep(do_log, "CORE ENDPOINT", "GET", host, &port_str, "metrics");

    // ── Email queue: POST /email/send ─────────────────────────────────────────
    cfg.service(web::resource("/email/send").route(
        web::post().to(crate::nocode::handlers::email_handler::send),
    ));
    log_ep(do_log, "CORE ENDPOINT", "POST", host, &port_str, "email/send");

    // ── Dynamic nocode routes ─────────────────────────────────────────────────
    for route in CONFIG.routes.iter() {
        // Build the bundle for this route (skips routes with no schema).
        let bundle = {
            let route_arc: Arc<str> = Arc::from(route.as_str());

            // Skip auth-only tables when auth is disabled.
            if !require_auth
                && (route_arc.as_ref() == "flx_users" || route_arc.as_ref() == "flx_roles")
            {
                continue;
            }

            let schema_arc = match SCHEMAS.0.get(route) {
                Some(s) => s.clone(),
                None => continue,
            };

            RouteBundle {
                route:   route_arc,
                schema:  schema_arc,
                ref_fks: SCHEMAS.1.clone(),
            }
        };

        let schema = bundle.schema.as_ref();

        // ── Base resource: GET / POST / TRACE / PATCH ─────────────────────
        let mut base_res = web::resource(bundle.route.as_ref());
        let mut has_base = false;

        if schema.get.enable_method {
            log_ep(do_log, "ENDPOINT", "GET", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            base_res = base_res.route(web::get().to(
                move |state: web::Data<AppState>,
                      req: actix_web::HttpRequest,
                      parameters: web::Query<Value>| {
                    crate::nocode::handlers::get_handler::select(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        req,
                    )
                },
            ));
            has_base = true;
        }

        if schema.post.enable_method {
            log_ep(do_log, "ENDPOINT", "POST", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            base_res = base_res.route(web::post().to(
                move |state: web::Data<AppState>,
                      parameters: web::Query<Value>,
                      payload: web::Payload,
                      req: actix_web::HttpRequest| {
                    crate::nocode::handlers::post_handler::insert(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        payload,
                        req,
                    )
                },
            ));
            has_base = true;
        }

        if schema.trace.enable_method {
            log_ep(do_log, "ENDPOINT", "TRACE", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            base_res = base_res.route(web::trace().to(
                move |state: web::Data<AppState>,
                      parameters: web::Query<Value>,
                      req: actix_web::HttpRequest| {
                    crate::nocode::handlers::trace_handler::process(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        req,
                    )
                },
            ));
            has_base = true;
        }

        if schema.patch.enable_method {
            log_ep(do_log, "ENDPOINT", "PATCH", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            base_res = base_res.route(web::patch().to(
                move |state: web::Data<AppState>,
                      parameters: web::Query<Value>,
                      req: actix_web::HttpRequest| {
                    crate::nocode::handlers::patch_handler::process_sp(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        req,
                    )
                },
            ));
            has_base = true;
        }

        if has_base {
            cfg.service(base_res);
        }

        // ── ID resource: DELETE / PUT ─────────────────────────────────────
        let mut id_res = web::resource(format!("{}/{{id}}", bundle.route.as_ref()));
        let mut has_id = false;

        if schema.del.enable_method {
            log_ep(do_log, "ENDPOINT", "DELETE", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            id_res = id_res.route(web::delete().to(
                move |state: web::Data<AppState>,
                      parameters: web::Query<Value>,
                      path: Path<String>,
                      req: actix_web::HttpRequest| {
                    crate::nocode::handlers::delete_handler::delete(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        b.ref_fks.clone(),
                        path,
                        req,
                    )
                },
            ));
            has_id = true;
        }

        if schema.put.enable_method {
            log_ep(do_log, "ENDPOINT", "PUT", host, &port_str, bundle.route.as_ref());
            let b = bundle.clone();
            id_res = id_res.route(web::put().to(
                move |state: web::Data<AppState>,
                      parameters: web::Query<Value>,
                      payload: web::Payload,
                      path: Path<String>,
                      req: actix_web::HttpRequest| {
                    crate::nocode::handlers::put_handler::update(
                        state,
                        parameters,
                        b.route.to_string(),
                        b.schema.clone(),
                        b.ref_fks.clone(),
                        payload,
                        path,
                        req,
                    )
                },
            ));
            has_id = true;
        }

        if has_id {
            cfg.service(id_res);
        }

        // ── Import  POST /import/<route> ──────────────────────────────────
        if schema.post.enable_method {
            let b = bundle.clone();
            log_ep(
                do_log,
                "ENDPOINT",
                "POST",
                host,
                &port_str,
                &format!("import/{}", b.route.as_ref()),
            );
            cfg.service(
                web::resource(format!("/import/{}", b.route.as_ref())).route(web::post().to(
                    move |state: web::Data<AppState>,
                          parameters: web::Query<Value>,
                          multipart: Multipart,
                          req: actix_web::HttpRequest| {
                        crate::nocode::handlers::import_handler::import(
                            state,
                            parameters,
                            b.route.to_string(),
                            b.schema.clone(),
                            multipart,
                            req,
                        )
                    },
                )),
            );
        }

        // ── Export  GET /export/<route> ───────────────────────────────────
        if schema.get.enable_method {
            let b = bundle.clone();
            log_ep(
                do_log,
                "ENDPOINT",
                "GET",
                host,
                &port_str,
                &format!("export/{}", b.route.as_ref()),
            );
            cfg.service(
                web::resource(format!("/export/{}", b.route.as_ref())).route(web::get().to(
                    move |state: web::Data<AppState>,
                          multipart: Multipart,
                          req: actix_web::HttpRequest| {
                        crate::nocode::handlers::export_handler::export(
                            state,
                            b.route.to_string(),
                            b.schema.clone(),
                            multipart,
                            req,
                        )
                    },
                )),
            );
        }

        // ── Validate  GET /validate/<route> ──────────────────────────────
        {
            let b = bundle.clone();
            if do_log {
                log_output(
                    "ENDPOINT",
                    "METHOD",
                    "GET",
                    format!(
                        "http://{}:{}/{}/{}",
                        host.red(),
                        port_str.green(),
                        "validate".yellow(),
                        b.route.as_ref().purple()
                    ),
                    false,
                );
            }
            cfg.service(
                web::resource(format!("validate/{}", b.route.as_ref())).route(web::get().to(
                    move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                        check_table_design(state, b.route.to_string(), b.schema.clone(), req)
                    },
                )),
            );
        }

        // ── Generate table  POST /generate/table/<route> ──────────────────
        if bundle.schema.auto_generate
            && bundle.route.as_ref() != "flx_users"
            && bundle.route.as_ref() != "flx_roles"
        {
            let b = bundle.clone();
            if do_log {
                log_output(
                    "ENDPOINT",
                    "METHOD",
                    "POST",
                    format!(
                        "http://{}:{}/{}/{}",
                        host.red(),
                        port_str.green(),
                        "generate/table".yellow(),
                        b.route.as_ref().purple()
                    ),
                    false,
                );
            }
            cfg.service(
                web::resource(format!("generate/table/{}", b.route.as_ref())).route(
                    web::post().to(
                        move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                            create_table(state, b.route.to_string(), b.schema.clone(), req)
                        },
                    ),
                ),
            );
        }

        // ── Seed table  POST /seed/<route> & POST /generate/seed/<route> ──
        if bundle.schema.seed
            && bundle.route.as_ref() != "flx_users"
            && bundle.route.as_ref() != "flx_roles"
        {
            let b = bundle.clone();
            if do_log {
                log_output(
                    "ENDPOINT",
                    "METHOD",
                    "POST",
                    format!(
                        "http://{}:{}/{}/{}",
                        host.red(),
                        port_str.green(),
                        "seed".yellow(),
                        b.route.as_ref().purple()
                    ),
                    false,
                );
            }
            let b1 = bundle.clone();
            cfg.service(
                web::resource(format!("seed/{}", b.route.as_ref())).route(
                    web::post().to(
                        move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                            seed_table(state, b1.route.to_string(), b1.schema.clone(), req)
                        },
                    ),
                ),
            );
            let b2 = bundle.clone();
            cfg.service(
                web::resource(format!("generate/seed/{}", b.route.as_ref())).route(
                    web::post().to(
                        move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                            seed_table(state, b2.route.to_string(), b2.schema.clone(), req)
                        },
                    ),
                ),
            );
        }

        if do_log {
            println!("\n");
        }
    }
}
