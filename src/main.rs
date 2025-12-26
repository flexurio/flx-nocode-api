use actix_cors::Cors;
use actix_files::Files;
use actix_multipart::Multipart;
// removed unused dev imports after migrating to dedicated middleware modules
use actix_web::web::Path;
use actix_web::{web, App, HttpResponse, HttpServer};
use actix_web::middleware::Compress;
use actix_web::middleware::Condition;
// validate_token now invoked inside AuthMiddleware; no direct import needed here
use colored::Colorize;
use dotenv::dotenv;
use helpers::cetak_label;
use log::log_output;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use std::env;
use std::time::Duration;

mod auth;
mod crypt;
mod database;
use database::state::{AppState, QueryConverter};
mod nocode;
use nocode::{
    delete::delete, export::export, generate::create_table, get::select, import::import,
    patch::process_sp, post::insert, put::update, trace::process, validate::check_table_design,
};
use nocode::consumer::{start_consumer};
mod core;
use core::{generate_users, login, register};
mod model;
use model::TableSchema;

use crate::auth::ClaimsConverter;
use crate::core::generate_role_admin;
use crate::model::{ReferenceForeignKey, ReferenceForeignKeyAction};
use crate::nocode::generate::{execute_generate_table, generate_table};
use crate::storage::sql_store::SqlStore;
mod audit;
mod helpers;
mod log;
mod rate_limit;
mod storage; // new optional storage abstraction (not used yet)
mod middleware;
mod metrics;
use metrics::METRICS;
use middleware::{GlobalRateLimit, AuthMiddleware};
#[cfg(feature = "mongodb")]
use crate::storage::mongodb_store::MongoStore;

// Consolidate config location to avoid repeated env::var reads
static CONFIG_LOCATION: Lazy<String> = Lazy::new(|| {
    std::env::var("LOC_CONFIG").unwrap_or_else(|_| {
        eprintln!("Warning: LOC_CONFIG not set, using default 'config'");
        "config".to_string()
    })
});

// Load routes.json once and expose via CONFIG
static CONFIG: Lazy<crate::model::Config> = Lazy::new(|| {
    let file_path = format!("{}/routes.json", CONFIG_LOCATION.as_str());

    let mut content = match std::fs::read_to_string(&file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "ERROR 9081231287 : Can't read file {} - {}",
                file_path.on_bright_red(),
                e
            );
            return crate::model::Config {
                routes: vec![],
                route_publics: vec![],
                converter_token: ClaimsConverter::default(),
            };
        }
    };

    if !content.contains("converter_token") {
        content = content.replace(
            "}",
            ", \"converter_token\": {\"id\":\"id\",\"nm\":\"nm\",\"exp\":\"exp\",\"at\":\"at\",\"rl\":\"rl\",\"cs\":\"cs\"} }",
        );
    }

    match serde_json::from_str(&content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!(
                "ERROR main 75 : Sorry, content of /{}/routes.json is not valid JSON, with ERROR Message : {}",
                CONFIG_LOCATION.as_str(), e
            );
            std::process::exit(1);
        }
    }
});

// Whitelist handled inside AuthMiddleware

pub(crate) static ISDEBUG: Lazy<bool> = Lazy::new(|| match env::var("DEBUG") {
    Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "yes"),
    Err(_) => false,
});

// Ensure endpoint logging happens only once even if server factory runs multiple times
static ENDPOINT_LOG_ONCE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// Read REQUIRE_AUTH from .env (default: true for security)
pub(crate) static REQUIRE_AUTH: Lazy<bool> = Lazy::new(|| match env::var("REQUIRE_AUTH") {
    Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "yes"),
    Err(_) => true, // default to true (require auth) if not set
});


// Static Routes for once initialization
static FOREIGNKEY_ACTION: [&str; 4] = ["cascade", "set null", "restrict", "no action"];

static SCHEMAS: Lazy<Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>> = Lazy::new(|| {
    let config_dir = format!("{}/entity", CONFIG_LOCATION.as_str());
    let mut schemas = Vec::with_capacity(CONFIG.routes.len()); // Pre-allocate capacity
    let mut ref_foreign_keys: Vec<ReferenceForeignKey> = Vec::with_capacity(CONFIG.routes.len());

    for route in CONFIG.routes.iter() {
        let file_path = format!("{}/{}.json", config_dir, route);

        let content = match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!(
                    "ERROR 908ihu76 : Can't read file {} - {}",
                    file_path.on_bright_red(),
                    e
                );
                exit(1);
            }
        };
        let schema: TableSchema = match serde_json::from_str(&content) {
            Ok(schema) => schema,
            Err(e) => {
                eprintln!(
                    "Sorry, content of /{}/entity/{}.json is not valid JSON, with ERROR Message : {}",
                    CONFIG_LOCATION.as_str(), route, e
                );
                exit(1);
            }
        };

        // Early validation for TRACE upsert/merge requirements based on backend
        // Determine db type from env (same as later runtime config) - compute lowercase once
        let dbt_raw = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());
        let dbt = dbt_raw.to_ascii_lowercase();  // Use ascii_lowercase which is faster than to_lowercase
        let trace_active = !schema.trace.insert_into.is_empty() || !schema.trace.column_selects.is_empty();
        if trace_active && (dbt == "postgres" || dbt == "mssql") {
            // Resolve conflict keys: allow special entry "index:NAME" to reference an index by name
            let mut resolved_conflict_cols: Vec<String> = vec![];
            if !schema.trace.column_conflicts.is_empty() {
                if let Some(idx_spec) = schema
                    .trace
                    .column_conflicts
                    .iter()
                    .find(|s| s.to_lowercase().starts_with("index:"))
                {
                    let name = idx_spec.split_once(':').map(|(_, n)| n.trim()).unwrap_or("");
                    if let Some(ix) = schema
                        .indexes
                        .iter()
                        .find(|ix| ix.name.eq_ignore_ascii_case(name))
                    {
                        resolved_conflict_cols = ix.columns.clone();
                    } else {
                        eprintln!(
                            "TRACE config error for table '{}': referenced index '{}' not found in indexes",
                            schema.table, name
                        );
                        exit(1);
                    }
                } else {
                    resolved_conflict_cols = schema.trace.column_conflicts.clone();
                }
            }

            if dbt == "postgres" {
                if resolved_conflict_cols.is_empty() {
                    eprintln!(
                        "TRACE config error for table '{}': Postgres requires 'column_conflicts' (or index:<name>) for upsert",
                        schema.table
                    );
                    exit(1);
                }
            } else if dbt == "mssql" && resolved_conflict_cols.is_empty() {
                // Allow fallback to a single unique index if present unambiguously
                let unique_indexes: Vec<_> = schema.indexes.iter().filter(|ix| ix.unique).collect();
                if unique_indexes.len() == 1 {
                    // ok: will use this unique index at runtime
                } else {
                    eprintln!(
                        "TRACE config error for table '{}': MSSQL requires 'column_conflicts' (or index:<name>); no unambiguous unique index found",
                        schema.table
                    );
                    exit(1);
                }
            }
        }

        // check if schema.foreign_keys is not empty
        if !schema.foreign_keys.is_empty() {
            // loop througt all schema.foreign_keys
            for fk in schema.foreign_keys.iter() {
                // check if fk.on_delete not in FOREIGNKEY_ACTION
                if !FOREIGNKEY_ACTION.contains(&fk.on_delete.as_str()) {
                    eprintln!("ERROR FK_Check Delete : Foreign key on_delete action '{}' is not supported", fk.on_delete);
                    exit(1);
                }
                if !FOREIGNKEY_ACTION.contains(&fk.on_update.as_str()) {
                    eprintln!("ERROR FK_Check Update : Foreign key on_update action '{}' is not supported", fk.on_update);
                    exit(1);
                }
                ref_foreign_keys.push(ReferenceForeignKey {
                    table: fk.reference_table.clone(),
                    column: fk.reference_column.clone(),
                    on_delete_action: ReferenceForeignKeyAction {
                        table: schema.table.clone(),
                        column: fk.column.clone(),
                        action: fk.on_delete.clone(),
                        type_delete: schema.del.type_delete.clone(), // soft or hard
                    },
                    on_update_action: ReferenceForeignKeyAction {
                        table: schema.table.clone(),
                        column: fk.column.clone(),
                        action: fk.on_update.clone(),
                        type_delete: "soft".to_string(), // on_update always soft
                    },
                });
            }
        }

        schemas.push(schema);
    }

    log_output(
        "INFO",
        "FOREIGN KEY",
        "ref_foreign_keys",
        format!("{:?}", ref_foreign_keys),
        true,
    );
    // Shrink to fit to reduce memory overhead
    schemas.shrink_to_fit();
    Arc::new((schemas, ref_foreign_keys))
});

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load .env early so DEBUG/LOG_* are visible before any Lazy env reads
    dotenv().ok();
    // Initialize async, non-blocking logger (no-op if DEBUG is off)
    // Early CLI handling: print version and exit
    {
        if matches!(env::args().nth(1).as_deref(), Some("--version") | Some("-V") | Some("version")) {
            println!("flx-nocode-api {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
    }

    // check .env exit or not
    if !std::path::Path::new(".env").exists() {
        if let Err(e) = core::download_env_file().await {
            eprintln!("Failed to download .env file: {}", e);
            return Err(std::io::Error::other(e));
        } else {
            println!(".env file downloaded successfully.");
            println!("Please configure your .env file with the required settings.");
            exit(1);
        }
    }

    let secret_key = env::var("SECRET_KEY").expect("SECRET_KEY must be set");
    let encrypt_key = env::var("ENCRYPT_KEY").expect("ENCRYPT_KEY must be set");

    // Check config folder - use consolidated CONFIG_LOCATION to avoid re-reading env
    let config_location = CONFIG_LOCATION.as_str();
    if !std::path::Path::new(config_location).exists() {
        if let Err(e) = core::create_dir_and_get_config(config_location).await {
            eprintln!("Failed to initialize config directory: {}", e);
        }
    } else {
        let _ = core::create_core_config_if_not_exists(config_location).await;
    }

    // Ensure static directory
    let static_storage = std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
    // check if directory exists
    if !std::path::Path::new(&static_storage).exists() {
        std::fs::create_dir_all(&static_storage)?;
    }

    // Ensure static directory
    let image_storage = std::env::var("LOC_IMAGE").unwrap_or("DB".to_string());
    if image_storage != "DB" {
        let path_image = format!("{}/{}", static_storage, image_storage);
        // check if directory exists
        if !std::path::Path::new(&path_image).exists() {
            std::fs::create_dir_all(&path_image)?;
        }
    }

    // Determine CPU to scale defaults for database pooling and Actix workers
    let cpu = num_cpus::get().max(1);

    let database::connection::DbInitialization { db_type, repo: db_repo, .. } =
        database::connection::initialize_database(cpu).await?;

    // Inline per-dialect datetime SQL function
    let datetime_now: String = match db_type.as_str() {
        "mysql" => "NOW()".to_string(),
        "postgres" => "NOW()".to_string(),
        "sqlite" => "CURRENT_TIMESTAMP".to_string(),
        "mssql" => "GETDATE()".to_string(),
        _ => "CURRENT_TIMESTAMP".to_string(),
    };

    let query_converter = QueryConverter {
        datetime_now,
    };

    let whitelist_ips: Vec<String> = env::var("WHITE_LIST_IP")
        .unwrap_or_else(|_| "".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Build generic DataStore adapter: SQL by default, MongoDB when selected
    let store_adapter: Arc<dyn crate::storage::traits::DataStore> = {
        match db_type.as_str() {
            #[cfg(feature = "mongodb")]
            "mongodb" => {
                let uri = match env::var("MONGODB_URI") {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("Please set MONGODB_URI in .env for DB_TYPE=mongodb");
                        std::process::exit(1);
                    }
                };
                let dbname = match env::var("MONGODB_DB") {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("Please set MONGODB_DB in .env for DB_TYPE=mongodb");
                        std::process::exit(1);
                    }
                };
                let mongo = MongoStore::connect(&uri, &dbname).await.map_err(|e| {
                    eprintln!("Failed to connect to MongoDB: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
                Arc::new(mongo)
            }
            _ => {
                let sql = crate::storage::sql_store::SqlStore::new(db_repo.clone(), db_type.clone());
                Arc::new(sql)
            }
        }
    };

    let mut is_cachedb = env::var("REDIS_HOST").map(|v| !v.is_empty()).unwrap_or(false);
    let mut write_queue_enabled = false;
    let mut write_queue_fast_ack = true; // default true: handlers return immediately
    // check if REDIS_HOST is configured in .env (already used to initialize is_cachedb above)
    // Proactively verify Redis connectivity once at startup for read-cache usage.
    // If unreachable, disable caching to avoid expensive per-request connection attempts
    // when clients pass `?redis=true`.
    if is_cachedb {
        match tokio::time::timeout(Duration::from_millis(1000), crate::database::redis::get_manager()).await {
            Ok(Ok(_)) => {
                // Redis is reachable; keep caching enabled
            }
            _ => {
                is_cachedb = false;
                eprintln!("Redis not reachable at startup. Disabling read-cache. Remove ?redis=true or fix REDIS_HOST to re-enable.");
            }
        }
    }
    // enable write queue if configured (default false)
    if let Ok(val) = env::var("WRITE_QUEUE_ENABLED") { write_queue_enabled = matches!(val.to_lowercase().as_str(), "1"|"true"|"yes"); }
    if let Ok(val) = env::var("WRITE_QUEUE_FAST_ACK") { write_queue_fast_ack = matches!(val.to_lowercase().as_str(), "1"|"true"|"yes"); }

    let app_state = web::Data::new(AppState {
        db: db_repo,
        db_type,
        secret: secret_key,
        encrypt_key,
        query_converter,
        whitelist_ips,
        route_publics: CONFIG.route_publics.clone(),
        converter_token: CONFIG.converter_token.clone(),
        store: store_adapter,
        is_cachedb,
        write_queue_enabled,
        write_queue_fast_ack,
    });

    let id_user_str: String = if *REQUIRE_AUTH {
        generate_users(app_state.clone()).await
    } else {
        "1".to_string()
    };

    // Initialize Routes only once, using Lazy
    let _ = &*CONFIG;
    let _ = &*SCHEMAS;

    // Start Redis consumer if write queue enabled
    if app_state.write_queue_enabled {
        // verify Redis connectivity early
        if let Err(e) = crate::database::redis::get_manager().await {
            eprintln!("WRITE QUEUE enabled but Redis not available: {}", e);
        } else {
            start_consumer(app_state.clone(), Arc::clone(&SCHEMAS)).await;
            log_output("QUEUE", "BOOT", "consumer", "Write consumer started".to_string(), true);
        }
    }

    // loop every config.routes and check if table is exist in database
    let state = web::Data::new(app_state.clone());
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    for route in CONFIG.routes.iter() {
        let schema = match SCHEMAS.0.iter().find(|s| s.table == *route) {
            Some(s) => s.clone(),
            None => {
                eprintln!("No schema found for route '{}'", route);
                exit(1);
            }
        };
        let should_generate = if schema.table == "flx_users" || schema.table == "flx_roles" {
                *REQUIRE_AUTH
            } else {
                schema.auto_generate
            };
        
        if should_generate {
            let (sql_create_table, sql_create_index) = generate_table(&ds, &schema);
            let (is_valid, msg) = execute_generate_table(route.to_string(), &app_state, sql_create_table, sql_create_index).await;
            if !is_valid {
                log_output("ERROR", "TABLE DESIGN CHECK", "FAILED", msg, true);
                exit(1);
            } else {
                log_output("INFO", "TABLE DESIGN CHECK", "SUCCESS", route.to_string(), false);
            }
        }

    }
    if app_state.db_type != "mongodb" {
        // convert id_user_string to i64
        let id_user: i64 = id_user_str.parse().unwrap_or(1);
        let fut = generate_role_admin(&app_state, ds, id_user, CONFIG.routes.clone());
        std::mem::drop(fut); // fire-and-forget as before
    }

    let _ = &*ISDEBUG;

    log_output(
        "BOOT",
        "AUTH",
        "REQUIRE_AUTH",
        if *REQUIRE_AUTH { "enabled" } else { "disabled" }.to_string(),
        false,
    );

    if CONFIG.routes.is_empty() {
        println!("--------------------------------------");
        println!("{}", "ROUTES NOT VALID ! ".on_red());
        println!("--------------------------------------");
        return Ok(());
    }

    // check if any table name in SCHEMAS is double
    let mut table_names: HashSet<String> = HashSet::new();
    for schema in SCHEMAS.0.iter() {
        if !table_names.insert(schema.table.clone()) {
            println!("--------------------------------------");
            println!(
                "{}",
                format!(
                    "ERROR 9081231287 : Table name '{}' is duplicated in config entity.",
                    schema.table
                )
                .on_red()
            );
            println!("--------------------------------------");
            exit(1);
        }
    }

    let host: &str = "0.0.0.0";
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    cetak_label(host.to_string(), port);

    // HTTP server tunables (with sensible defaults)
    // HTTP server defaults tuned for higher concurrency; override via env for fine control
    let keepalive_secs: u64 = env::var("HTTP_KEEPALIVE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let http_backlog: u32 = env::var("HTTP_BACKLOG").ok().and_then(|s| s.parse().ok()).unwrap_or(32768);
    let max_conn_rate: usize = env::var("HTTP_MAX_CONN_RATE").ok().and_then(|s| s.parse().ok()).unwrap_or(16384);
    let workers_default = (cpu * 2).clamp(2, 32);

    log_output(
        "BOOT",
        "HTTP",
        "ACTIX",
        format!(
            "workers={} keepalive={}s backlog={} max_conn_rate={} max_connections={}",
            env::var("ACTIX_WORKERS").ok().unwrap_or_else(|| workers_default.to_string()),
            keepalive_secs,
            http_backlog,
            max_conn_rate,
            env::var("HTTP_MAX_CONNECTIONS").ok().unwrap_or_else(|| "25000".to_string())
        ),
        false,
    );

    HttpServer::new(move || {
        // Build CORS policy from env
        let cors = match env::var("CORS_ALLOW_ORIGINS") {
            Ok(val) if !val.trim().is_empty() => {
                let mut c = Cors::default()
                    .allow_any_method()
                    .allow_any_header()
                    .supports_credentials()
                    .max_age(3600);
                for origin in val.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    c = c.allowed_origin(origin);
                }
                c
            }
            _ => Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600),
        };

        // Only log endpoint URLs once per process; always register routes regardless
        let do_log = !ENDPOINT_LOG_ONCE.swap(true, Ordering::SeqCst);

        App::new()
            .app_data(app_state.clone())
            .app_data(web::PayloadConfig::new(
                env::var("UPLOAD_LIMIT_MB")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .map(|mb| mb * 1024 * 1024)
                    .unwrap_or(10 * 1024 * 1024), 
            ))
            .app_data({
                let kb: usize = env::var("JSON_LIMIT_KB")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .map(|n: usize| n.clamp(1, 1024 * 16))
                    .unwrap_or(4096);
                let bytes = kb * 1024; // convert KB to bytes as required by Actix limit()
                web::JsonConfig::default()
                    .limit(bytes)
                    .error_handler(|err, _req| {
                        actix_web::error::InternalError::from_response(
                            format!("JSON error: {}", err),
                            actix_web::HttpResponse::BadRequest().json("Invalid JSON payload"),
                        )
                        .into()
                    })
            })
            // Global rate limit middleware (per-IP & method class)
            .wrap(GlobalRateLimit)
            // Authentication middleware (uses whitelist & public routes)
            .wrap(AuthMiddleware)
            .wrap(Condition::new(
                env::var("ALLOW_ANY_ORIGINS")
                    .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
                    .unwrap_or(false),
                cors,
            ))
            // Enable response compression to reduce memory/bandwidth
            .wrap(Compress::default())
            .configure(|cfg: &mut web::ServiceConfig| {
                let static_loc =
                    std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
                // end point for static files (disable directory listing in prod)
                let static_files = Files::new("/static", static_loc);
                if *ISDEBUG {
                    cfg.service(static_files.show_files_listing());
                } else {
                    cfg.service(static_files);
                }
                if do_log {
                    log_output(
                        "CORE ENDPOINT",
                        "METHOD",
                        "GET",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            "static".purple()
                        ),
                        false,
                    );
                }

                // end point for login (only if REQUIRE_AUTH is enabled)
                if *REQUIRE_AUTH {
                    cfg.service(web::resource("/login").route(web::post().to(
                        move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                            login(state, req)
                        },
                    )));
                    if do_log {
                        log_output(
                            "CORE ENDPOINT",
                            "METHOD",
                            "POST",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port.clone().to_string().green(),
                                "login".purple()
                            ),
                            false,
                        );
                    }
                }

                // end point for register (only if REQUIRE_AUTH is enabled)
                if *REQUIRE_AUTH {
                    cfg.service(web::resource("/register").route(web::post().to(
                        move |state: web::Data<AppState>, multipart: Multipart| {
                            register(state, multipart)
                        },
                    )));
                    if do_log {
                        log_output(
                            "CORE ENDPOINT",
                            "METHOD",
                            "POST",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port.clone().to_string().green(),
                                "register".purple()
                            ),
                            false,
                        );
                    }
                }

                // health check endpoint (public)
                cfg.service(web::resource("/healthz").route(web::get().to({
                    let state = app_state.clone();
                    move || {
                        let state = state.clone();
                        async move {
                            let probe_sql = "SELECT 1";
                            let db_ok = state.db.query(probe_sql).await.is_ok();
                            let body = serde_json::json!({
                                "status": "ok",
                                "db": if db_ok { "up" } else { "down" },
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
                if do_log {
                    log_output(
                        "CORE ENDPOINT",
                        "METHOD",
                        "GET",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            "healthz".purple()
                        ),
                        false,
                    );
                }

                // metrics endpoint for Prometheus monitoring
                cfg.service(web::resource("/metrics").route(web::get().to(|| {
                    async {
                        let metrics_output = METRICS.to_prometheus_format();
                        HttpResponse::Ok()
                            .content_type("text/plain; charset=utf-8")
                            .body(metrics_output)
                    }
                })));
                if do_log {
                    log_output(
                        "CORE ENDPOINT",
                        "METHOD",
                        "GET",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            "metrics".purple()
                        ),
                        false,
                    );
                }

                // setup endpoint for each route
                // Cache Arc clone of SCHEMAS to avoid cloning per route (Arc clone is cheap but compound overhead)
                let schemas_arc = Arc::clone(&SCHEMAS);
                for route in CONFIG.routes.iter() {
                    let route_arc: Arc<str> = Arc::from(route.as_str());
                    let route_ra = Arc::clone(&route_arc);
                    if !*REQUIRE_AUTH && ( route_ra.as_ref() == "flx_users" || route_ra.as_ref() == "flx_roles" ) {
                        continue;
                    }

                    // Use Arc<str> for efficient shared ownership - cheap to clone, reduces heap allocations
                    let port_str = port.to_string(); // Cache port string conversion
                    
                    // Clone Arc only when needed (Arc clone is just pointer increment, very cheap)
                    let route_get = Arc::clone(&route_arc);
                    let route_trace = Arc::clone(&route_arc);
                    let route_patch = Arc::clone(&route_arc);
                    let route_post = Arc::clone(&route_arc);
                    let route_delete = Arc::clone(&route_arc);
                    let route_import = Arc::clone(&route_arc);
                    let route_export = Arc::clone(&route_arc);
                    let route_put = Arc::clone(&route_arc);
                    let route_validate = Arc::clone(&route_arc);
                    let route_generate_table = Arc::clone(&route_arc);
                    let schemas_get = Arc::clone(&schemas_arc);
                    let schemas_post = Arc::clone(&schemas_arc);
                    let schemas_trace = Arc::clone(&schemas_arc);
                    let schemas_patch = Arc::clone(&schemas_arc);
                    let schemas_delete = Arc::clone(&schemas_arc);
                    let schemas_import = Arc::clone(&schemas_arc);
                    let schemas_export = Arc::clone(&schemas_arc);
                    let schemas_put = Arc::clone(&schemas_arc);
                    let schemas_validate = Arc::clone(&schemas_arc);
                    let schemas_generate = Arc::clone(&schemas_arc);

                    if do_log {
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "GET",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_get.as_ref().purple()
                            ),
                            false,
                        );
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "POST",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_post.as_ref().purple()
                            ),
                            false,
                        );
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "TRACE",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_trace.as_ref().purple()
                            ),
                            false,
                        );
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "PATCH",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_patch.as_ref().purple()
                            ),
                            false,
                        );
                    }

                    cfg.service(
                        web::resource(route_get.as_ref())
                            // register nocode_get
                            .route(web::get().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      req: actix_web::HttpRequest| {
                                    select(
                                        state,
                                        parameters,
                                        route_get.to_string(),
                                        schemas_get.as_ref().0.clone().into(),
                                        req,
                                    )
                                },
                            ))
                            // register create_nocode
                            .route(web::post().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    insert(
                                        state,
                                        parameters,
                                        route_post.to_string(),
                                        schemas_post.as_ref().0.clone().into(),
                                        multipart,
                                        req,
                                    )
                                },
                            ))
                            // register nocode_trace
                            .route(web::trace().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      req: actix_web::HttpRequest| {
                                    process(
                                        state,
                                        parameters,
                                        route_trace.to_string(),
                                        schemas_trace.as_ref().0.clone().into(),
                                        req,
                                    )
                                },
                            ))
                            // register nocode_patch
                            .route(web::patch().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      req: actix_web::HttpRequest| {
                                    process_sp(
                                        state,
                                        parameters,
                                        route_patch.to_string(),
                                        schemas_patch.as_ref().0.clone().into(),
                                        req,
                                    )
                                },
                            )),
                    );


                    if do_log {
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "DELETE",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_delete.as_ref().purple()
                            ),
                            false,
                        );
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "PUT",
                            format!(
                                "http://{}:{}/{}",
                                host.red(),
                                port_str.green(),
                                route_put.as_ref().purple()
                            ),
                            false,
                        );
                    }

                    cfg.service(
                        web::resource(format!("{}/{{id}}", route_delete.as_ref()))
                            // register delete_nocode
                            .route(web::delete().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    delete(state,parameters, route_delete.to_string(), schemas_delete.clone(), path, req)
                                },
                            ))
                            // register create_nocode
                            .route(web::put().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      multipart: Multipart,
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    update(
                                        state,
                                        parameters,
                                        route_put.to_string(),
                                        schemas_put.clone(),
                                        multipart,
                                        path,
                                        req,
                                    )
                                },
                            )),
                            
                    );

                    // register import BEFORE the dynamic {id} route to avoid conflicts
                    if do_log {
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "POST",
                            format!(
                                "http://{}:{}/import/{}",
                                host.red(),
                                port_str.green(),
                                route_import.as_ref().purple()
                            ),
                            false,
                        );
                    }
                    cfg.service(
                        web::resource(format!("/import/{}", route_import.as_ref()))
                            .route(web::post().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    import(
                                        state,
                                        parameters,
                                        route_import.to_string(),
                                        Arc::clone(&schemas_import),
                                        multipart,
                                        req,
                                    )
                                },
                            )),
                    );


                    // register export endpoint
                    if do_log {
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "GET",
                            format!(
                                "http://{}:{}/export/{}",
                                host.red(),
                                port_str.green(),
                                route_export.as_ref().purple()
                            ),
                            false,
                        );
                    }
                    cfg.service(
                        web::resource(format!("/export/{}", route_export.as_ref()))
                            .route(web::get().to(
                                move |state: web::Data<AppState>,
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    export(
                                        state,
                                        route_export.to_string(),
                                        Arc::clone(&schemas_export),
                                        multipart,
                                        req,
                                    )
                                },
                            )),
                    );


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
                                route_validate.as_ref().purple()
                            ),
                            false,
                        );
                    }
                    cfg.service(
                        web::resource(format!("validate/{}", route_validate.as_ref())).route(
                            web::get().to(
                                move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                                    check_table_design(
                                        state,
                                        route_validate.to_string(),
                                        schemas_validate.as_ref().0.clone().into(),
                                        req,
                                    )
                                },
                            ),
                        ),
                    );

                    if route_generate_table.as_ref() != "flx_users" && route_generate_table.as_ref() != "flx_roles" {
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
                                    route_generate_table.as_ref().purple()
                                ),
                                false,
                            );
                        }
                        cfg.service(
                            web::resource(format!("generate/table/{}", route_generate_table.as_ref()))
                                .route(web::post().to(
                                move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                                    create_table(
                                        state,
                                        route_generate_table.to_string(),
                                        schemas_generate.as_ref().0.clone().into(),
                                        req,
                                    )
                                },
                            )),
                        );
                    }
                    if do_log {
                        println!("\n");
                    }
                }
            })
    })
    .workers(
        env::var("ACTIX_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(workers_default),
    )
    .max_connections(
        env::var("HTTP_MAX_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(25000)
    )
    .max_connection_rate(max_conn_rate)
    .keep_alive(Duration::from_secs(keepalive_secs))
    .backlog(http_backlog)
    .client_request_timeout(std::time::Duration::from_secs(30)) // 30s request timeout
    .client_disconnect_timeout(std::time::Duration::from_secs(5)) // 5s disconnect timeout
    .bind((host, port))
    .map_err(|e| {
        eprintln!("Failed to bind to {}:{} - {}", host, port, e);
        e
    })?
    .run()
    .await
}
