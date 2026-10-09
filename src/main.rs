use actix_cors::Cors;
use actix_web::middleware::{Compress, Condition};
use actix_web::{App, HttpServer, web};
use colored::Colorize;
use dotenv::EnvLoader;
use helpers::cetak_label;
use log::log_output;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod auth;
mod crypt;
mod database;
use database::state::{AppState, QueryConverter};
mod error;
mod nocode;
use nocode::consumer::start_consumer;
use nocode::email_queue::start_email_consumer;
mod core;
use core::generate_users;
mod audit;
mod helpers;
mod log;
mod metrics;
mod middleware;
mod model;
mod rate_limit;
mod storage;
#[cfg(feature = "mongodb")]
use crate::storage::mongodb_store::MongoStore;
use middleware::{AuthMiddleware, GlobalRateLimit, StatusLogger};

mod config;
use config::{CONFIG, CONFIG_LOCATION, ENDPOINT_LOG_ONCE, ISDEBUG, SCHEMAS, SEED_LOCATION};

mod cli;
#[cfg(feature = "mcp")]
mod mcp;
mod routes;
mod startup;

/// Response headers browser-based clients (e.g. MCP Inspector) must be able to read.
const CORS_EXPOSE_HEADERS: [&str; 3] = ["www-authenticate", "mcp-session-id", "mcp-protocol-version"];

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // Load .env early so DEBUG / LOG_* are visible before any Lazy env reads.
    // SAFETY: Called at startup in main before spawning worker threads.
    unsafe {
        let _ = EnvLoader::new().load_and_modify();
    }

    // ── CLI: --version ────────────────────────────────────────────────────────
    if matches!(
        env::args().nth(1).as_deref(),
        Some("--version") | Some("-V") | Some("version")
    ) {
        println!("flx-nocode-api {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // ── CLI: reset-password ───────────────────────────────────────────────────
    let args: Vec<String> = env::args().collect();
    let is_reset_cmd = args.iter().any(|arg| arg == "reset-password");

    if is_reset_cmd {
        return cli::reset_password(&args).await;
    }

    // ── CLI: mcp [--stdio] ────────────────────────────────────────────────────
    // stdout must carry only JSON-RPC in stdio mode, so it is isolated before
    // any boot message is printed (they go to stderr instead).
    #[cfg(feature = "mcp")]
    if mcp::stdio::is_help_command(&args) {
        print!("{}", mcp::stdio::HELP);
        return Ok(());
    }
    #[cfg(feature = "mcp")]
    let is_mcp_stdio = mcp::stdio::is_stdio_command(&args);
    #[cfg(feature = "mcp")]
    let mcp_stdout = if is_mcp_stdio {
        mcp::stdio::isolate_stdout()?
    } else {
        None
    };

    // ── Ensure .env exists ────────────────────────────────────────────────────
    if !std::path::Path::new(".env").exists() {
        match core::download_env_file().await {
            Err(e) => {
                eprintln!("Failed to download .env file: {}", e);
                return Err(anyhow::anyhow!("Failed to download .env file: {}", e));
            }
            Ok(_) => {
                println!(".env file downloaded successfully.");
                println!("Please configure your .env file with the required settings.");
                return Err(anyhow::anyhow!("Please configure your .env file"));
            }
        }
    }

    let secret_key = env::var("SECRET_KEY").expect("SECRET_KEY must be set");
    let encrypt_key = env::var("ENCRYPT_KEY").expect("ENCRYPT_KEY must be set");

    // ── Ensure config directory ───────────────────────────────────────────────
    let config_location = CONFIG_LOCATION.as_str();
    if !std::path::Path::new(config_location).exists() {
        if let Err(e) = core::create_dir_and_get_config(config_location).await {
            eprintln!("Failed to initialize config directory: {}", e);
        }
    } else {
        let _ = core::create_core_config_if_not_exists(config_location).await;
    }

    // ── Ensure static & image directories ────────────────────────────────────
    let static_storage = env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
    if !std::path::Path::new(&static_storage).exists() {
        std::fs::create_dir_all(&static_storage)?;
    }
    let image_storage = env::var("LOC_IMAGE").unwrap_or_else(|_| "DB".to_string());
    println!("image_storage: {}", image_storage);
    if image_storage != "DB" {
        let path_image = format!("{}/{}", static_storage, image_storage);
        if !std::path::Path::new(&path_image).exists() {
            std::fs::create_dir_all(&path_image)?;
        }
        println!("path image: {}", path_image);
    }

    // ── Ensure seed directory ────────────────────────────────────────────────
    let seed_storage = SEED_LOCATION.as_str();
    if !std::path::Path::new(seed_storage).exists() {
        let _ = std::fs::create_dir_all(seed_storage);
    }

    // ── Database initialisation ───────────────────────────────────────────────
    let cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let database::connection::DbInitialization {
        db_type,
        repo: db_repo,
        ..
    } = database::connection::initialize_database(cpu).await?;

    let datetime_now: String = match db_type {
        crate::model::DbType::Mysql => "NOW()".to_string(),
        crate::model::DbType::Postgres => "NOW()".to_string(),
        crate::model::DbType::Sqlite => "CURRENT_TIMESTAMP".to_string(),
        crate::model::DbType::Mssql => "GETDATE()".to_string(),
        _ => "CURRENT_TIMESTAMP".to_string(),
    };

    let query_converter = QueryConverter { datetime_now };

    let whitelist_ips: Vec<String> = env::var("WHITE_LIST_IP")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // ── DataStore adapter ─────────────────────────────────────────────────────
    let store_adapter: Arc<dyn crate::storage::traits::DataStore> = {
        match db_type {
            #[cfg(feature = "mongodb")]
            crate::model::DbType::Mongodb => {
                let uri = env::var("MONGODB_URI").map_err(|_| {
                    eprintln!("Please set MONGODB_URI in .env for DB_TYPE=mongodb");
                    anyhow::anyhow!("MONGODB_URI not set")
                })?;
                let dbname = env::var("MONGODB_DB").map_err(|_| {
                    eprintln!("Please set MONGODB_DB in .env for DB_TYPE=mongodb");
                    anyhow::anyhow!("MONGODB_DB not set")
                })?;
                let mongo = MongoStore::connect(&uri, &dbname).await.map_err(|e| {
                    eprintln!("Failed to connect to MongoDB: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
                Arc::new(mongo)
            }
            _ => Arc::new(crate::storage::sql_store::SqlStore::new(
                db_repo.clone(),
                db_type.as_str().to_string(),
            )),
        }
    };

    // ── Redis / write-queue setup ─────────────────────────────────────────────
    let mut is_cachedb = env::var("REDIS_HOST")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if is_cachedb {
        match tokio::time::timeout(
            Duration::from_millis(1000),
            crate::database::redis::get_manager(),
        )
        .await
        {
            Ok(Ok(_)) => {} // reachable — keep enabled
            _ => {
                is_cachedb = false;
                eprintln!(
                    "Redis not reachable at startup. Disabling read-cache. \
                     Remove ?redis=true or fix REDIS_HOST to re-enable."
                );
            }
        }
    }
    let mut write_queue_enabled = false;
    if let Ok(val) = env::var("WRITE_QUEUE_ENABLED") {
        write_queue_enabled = matches!(val.to_lowercase().as_str(), "1" | "true" | "yes");
    }
    // WRITE_QUEUE_FAST_ACK is accepted but ignored: the handler always waits for
    // the LPUSH to succeed before replying 202 (fire-and-forget lost writes).
    if env::var("WRITE_QUEUE_FAST_ACK").is_ok() {
        eprintln!("WRITE_QUEUE_FAST_ACK is deprecated and ignored; writes are acknowledged after enqueue.");
    }
    let require_auth = env::var("REQUIRE_AUTH")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);

    // ── L1 In-Memory Cache (Moka) ──────────────────────────────────────────
    let l1_cache_ttl_secs: u64 = env::var("L1_CACHE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let l1_cache_max_capacity: u64 = env::var("L1_CACHE_MAX_CAPACITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    // Capacity is a BYTE budget (approximate serialized size per entry), not an
    // entry count: 50 000 entries x 2 000 rows each could reach gigabytes.
    let l1_cache_max_bytes: u64 = env::var("L1_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024 * 1024);
    let _ = l1_cache_max_capacity; // kept for env compatibility; bytes win
    let l1_cache = moka::future::Cache::builder()
        .max_capacity(l1_cache_max_bytes)
        .weigher(|_k: &String, v: &crate::model::WebResponse| {
            crate::database::state::approx_json_bytes(&v.data)
                .saturating_add(64)
                .min(u32::MAX as usize) as u32
        })
        .time_to_live(Duration::from_secs(l1_cache_ttl_secs))
        .support_invalidation_closures()
        .build();

    // ── AppState ──────────────────────────────────────────────────────────────
    let app_state = web::Data::new(AppState {
        db: db_repo,
        require_auth,
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
        default_collate: env::var("DEFAULT_COLLATE").unwrap_or_else(|_| "utf8mb4_bin".to_string()),
        rules: {
            let rules_path = format!("{}/rules.json", CONFIG_LOCATION.as_str());
            match std::fs::read_to_string(&rules_path) {
                Ok(c) => serde_json::from_str(&c).unwrap_or_else(|_| serde_json::json!({})),
                Err(_) => serde_json::json!({}),
            }
        },
        l1_cache,
    });

    // ── Generate default users ────────────────────────────────────────────────
    if require_auth {
        let _ = generate_users(app_state.clone(), &SCHEMAS.0).await;
    }
    // Force lazy statics to initialise now (avoids first-request latency).
    let _ = &*CONFIG;
    let _ = &*SCHEMAS;

    // ── Write-queue consumer ──────────────────────────────────────────────────
    if app_state.write_queue_enabled {
        match crate::database::redis::get_manager().await {
            Err(e) => eprintln!("WRITE QUEUE enabled but Redis not available: {}", e),
            Ok(_) => {
                start_consumer(app_state.clone(), Arc::clone(&SCHEMAS.0)).await;
                log_output(
                    "QUEUE",
                    "BOOT",
                    "consumer",
                    "Write consumer started".to_string(),
                    true,
                );
            }
        }
    }

    // ── Email-queue consumer ──────────────────────────────────────────────────
    if std::env::var("EMAIL_CONSUMER_ENABLED")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
    {
        match crate::database::redis::get_manager().await {
            Err(e) => eprintln!("EMAIL CONSUMER enabled but Redis not available: {}", e),
            Ok(_) => {
                start_email_consumer().await;
                log_output(
                    "EMAIL",
                    "BOOT",
                    "email-consumer",
                    "Email consumer started".to_string(),
                    true,
                );
            }
        }
    }

    // ── Table generation (blocks until done; fatal on error) ─────────────────
    startup::run_table_generation(&app_state).await?;

    // // ── Role seeding (background task) ────────────────────────────────────────
    // if app_state.db_type != crate::model::DbType::Mongodb {
    //     startup::run_role_seeding(app_state.clone(), &id_user_str).await;
    // }

    let _ = &*ISDEBUG;

    log_output(
        "BOOT",
        "AUTH",
        "REQUIRE_AUTH",
        if require_auth { "enabled" } else { "disabled" }.to_string(),
        false,
    );

    // ── Guard: empty routes ───────────────────────────────────────────────────
    if CONFIG.routes.is_empty() {
        println!("--------------------------------------");
        println!("{}", "ROUTES NOT VALID ! ".on_red());
        println!("--------------------------------------");
        return Ok(());
    }

    // ── Guard: duplicate table names ──────────────────────────────────────────
    let mut table_names: HashSet<String> = HashSet::new();
    for (_route, schema) in SCHEMAS.0.iter() {
        if !table_names.insert(schema.table.clone()) {
            let msg = format!(
                "ERROR 9081231287 : Table name '{}' is duplicated in config entity.",
                schema.table
            );
            println!("--------------------------------------");
            println!("{}", msg.on_red());
            println!("--------------------------------------");
            return Err(anyhow::anyhow!("{}", msg));
        }
    }

    // ── MCP over stdio (no HTTP server) ───────────────────────────────────────
    #[cfg(feature = "mcp")]
    if is_mcp_stdio {
        return mcp::stdio::run(app_state.clone(), &args, mcp_stdout).await;
    }

    // ── MCP Streamable HTTP service (shared by all workers) ───────────────────
    #[cfg(feature = "mcp")]
    let mcp_service = if mcp::MCP_CONFIG.enabled {
        log_output(
            "BOOT",
            "MCP",
            "streamable-http",
            format!(
                "path={} write_tools={} max_rows={} allowed_hosts={}",
                mcp::MCP_CONFIG.path,
                mcp::MCP_CONFIG.write_tools,
                mcp::MCP_CONFIG.max_rows,
                if mcp::MCP_CONFIG.allowed_hosts.is_empty() {
                    "*".to_string()
                } else {
                    mcp::MCP_CONFIG.allowed_hosts.join(",")
                }
            ),
            false,
        );
        Some(web::Data::new(mcp::http_adapter::build_service(
            app_state.clone(),
            Arc::new(mcp::MCP_CONFIG.clone()),
        )))
    } else {
        None
    };

    // ── HTTP server ───────────────────────────────────────────────────────────
    let host: &'static str = "0.0.0.0";
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    cetak_label(host.to_string(), port);

    let keepalive_secs: u64 = env::var("HTTP_KEEPALIVE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let http_backlog: u32 = env::var("HTTP_BACKLOG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32768);
    let max_conn_rate: usize = env::var("HTTP_MAX_CONN_RATE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16384);
    let workers_default = (cpu * 2).clamp(2, 32);

    log_output(
        "BOOT",
        "HTTP",
        "ACTIX",
        format!(
            "workers={} keepalive={}s backlog={} max_conn_rate={} max_connections={}",
            env::var("ACTIX_WORKERS")
                .ok()
                .unwrap_or_else(|| workers_default.to_string()),
            keepalive_secs,
            http_backlog,
            max_conn_rate,
            env::var("HTTP_MAX_CONNECTIONS")
                .ok()
                .unwrap_or_else(|| "25000".to_string()),
        ),
        false,
    );

    HttpServer::new(move || {
        // CORS policy from env
        let cors = match env::var("CORS_ALLOW_ORIGINS") {
            Ok(val) if !val.trim().is_empty() => {
                let mut c = Cors::default()
                    .allow_any_method()
                    .allow_any_header()
                    .expose_headers(CORS_EXPOSE_HEADERS)
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
                .expose_headers(CORS_EXPOSE_HEADERS)
                .max_age(3600),
        };

        // Log endpoints only on the first worker invocation.
        let do_log = !ENDPOINT_LOG_ONCE.swap(true, Ordering::SeqCst);

        let app = App::new()
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
                web::JsonConfig::default()
                    .limit(kb * 1024)
                    .error_handler(|err, _req| {
                        actix_web::error::InternalError::from_response(
                            format!("JSON error: {}", err),
                            actix_web::HttpResponse::BadRequest().json("Invalid JSON payload"),
                        )
                        .into()
                    })
            })
            // actix applies the LAST `.wrap` first. Rate limiting must run
            // before JWT verification so an auth-fail flood is rejected cheaply.
            .wrap(AuthMiddleware)
            .wrap(GlobalRateLimit)
            .wrap(Condition::new(
                env::var("ALLOW_ANY_ORIGINS")
                    .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
                    .unwrap_or(false),
                cors,
            ))
            .wrap(Condition::new(
                env::var("ENABLE_COMPRESSION")
                    .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
                    .unwrap_or(false),
                Compress::default(),
            ))
            .wrap(StatusLogger);

        // MCP first so MCP_PATH takes precedence over dynamic entity routes.
        #[cfg(feature = "mcp")]
        let app = {
            let svc = mcp_service.clone();
            app.configure(move |cfg| mcp::configure(cfg, svc, do_log, host, port))
        };

        app.configure(|cfg| {
            routes::configure_routes(cfg, require_auth, do_log, host, port, app_state.clone())
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
            .unwrap_or(25000),
    )
    .max_connection_rate(max_conn_rate)
    .keep_alive(Duration::from_secs(keepalive_secs))
    .backlog(http_backlog)
    .client_request_timeout(std::time::Duration::from_secs(30))
    .client_disconnect_timeout(std::time::Duration::from_secs(5))
    .bind((host, port))
    .map_err(|e| {
        eprintln!("Failed to bind to {}:{} - {}", host, port, e);
        e
    })?
    .run()
    .await
    .map_err(Into::into)
}
