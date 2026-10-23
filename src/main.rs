use actix_cors::Cors;
use actix_web::middleware::{Compress, Condition};
use actix_web::{App, HttpServer, web};
use colored::Colorize;
use dotenv::EnvLoader;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use flx_nocode_api::config::{CONFIG, CONFIG_LOCATION, ENDPOINT_LOG_ONCE, ISDEBUG, SCHEMAS, SEED_LOCATION};
use flx_nocode_api::core::generate_users;
use flx_nocode_api::database::{self, state::{AppState, QueryConverter}};
use flx_nocode_api::helpers::cetak_label;
use flx_nocode_api::log::log_output;
#[cfg(feature = "mcp")]
use flx_nocode_api::mcp;
use flx_nocode_api::middleware::{AuthMiddleware, GlobalRateLimit, StatusLogger};
use flx_nocode_api::nocode::consumer::start_consumer;
use flx_nocode_api::nocode::email_queue::start_email_consumer;
use flx_nocode_api::{cli, routes, startup};
#[cfg(feature = "mongodb")]
use flx_nocode_api::storage::mongodb_store::MongoStore;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Response headers browser-based clients (e.g. MCP Inspector) must be able to read.
const CORS_EXPOSE_HEADERS: [&str; 3] = ["www-authenticate", "mcp-session-id", "mcp-protocol-version"];

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    // stdio MCP mode owns stdout for JSON-RPC: logs must go to stderr there.
    let to_stderr = env::args().any(|a| a == "--stdio") || json;
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(false);
    let result = if json {
        if to_stderr { builder.json().with_writer(std::io::stderr).try_init() } else { builder.json().try_init() }
    } else if to_stderr {
        builder.compact().with_writer(std::io::stderr).try_init()
    } else {
        builder.compact().try_init()
    };
    let _ = result; // already initialised (tests) is fine
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // Load .env early so DEBUG / LOG_* are visible before any Lazy env reads.
    // SAFETY: Called at startup in main before spawning worker threads.
    unsafe {
        let _ = EnvLoader::new().load_and_modify();
    }

    // ── Structured logging (tracing) ─────────────────────────────────────────
    // RUST_LOG controls the level (default `info`; `flx::access=warn` silences
    // the per-request access line). LOG_FORMAT=json emits one JSON object per
    // line for log shippers; the default is a human-readable compact format.
    init_tracing();

    // ── CLI: --version ────────────────────────────────────────────────────────
    if matches!(
        env::args().nth(1).as_deref(),
        Some("--version") | Some("-V") | Some("version")
    ) {
        println!("flx-nocode-api {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // ── CLI: init / reset-password ────────────────────────────────────────────
    let args: Vec<String> = env::args().collect();
    if args.get(1).map(|a| a == "init").unwrap_or(false) {
        return cli::init(&args);
    }
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

    // ── Ensure .env exists (never downloaded: run `flx-nocode-api init`) ────
    if !std::path::Path::new(".env").exists() && env::var("SECRET_KEY").is_err() {
        eprintln!("No .env file found and SECRET_KEY is not set.");
        eprintln!("Run `flx-nocode-api init` to create .env and the core config offline.");
        return Err(anyhow::anyhow!("missing .env (run `flx-nocode-api init`)"));
    }

    let secret_key = env::var("SECRET_KEY").expect("SECRET_KEY must be set");
    let encrypt_key = env::var("ENCRYPT_KEY").expect("ENCRYPT_KEY must be set");

    // ── Ensure config directory + core entities (embedded, no network) ───────
    let config_location = CONFIG_LOCATION.as_str();
    match flx_nocode_api::embedded::ensure_core_config(config_location) {
        Ok(created) => {
            for f in created {
                println!("config: created {}", f);
            }
        }
        Err(e) => eprintln!("Failed to initialize config directory {}: {}", config_location, e),
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
        flx_nocode_api::model::DbType::Mysql => "NOW()".to_string(),
        flx_nocode_api::model::DbType::Postgres => "NOW()".to_string(),
        flx_nocode_api::model::DbType::Sqlite => "CURRENT_TIMESTAMP".to_string(),
        flx_nocode_api::model::DbType::Mssql => "GETDATE()".to_string(),
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
    let store_adapter: Arc<dyn flx_nocode_api::storage::traits::DataStore> = {
        match db_type {
            #[cfg(feature = "mongodb")]
            flx_nocode_api::model::DbType::Mongodb => {
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
            _ => Arc::new(flx_nocode_api::storage::sql_store::SqlStore::new(
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
            flx_nocode_api::database::redis::get_manager(),
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

    // Capacity is a BYTE budget (serialized size per entry), not an entry count.
    let l1_cache_max_bytes: u64 = env::var("L1_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024 * 1024);
    let _ = l1_cache_max_capacity; // kept for env compatibility; bytes win
    let l1_cache = moka::future::Cache::builder()
        .max_capacity(l1_cache_max_bytes)
        .weigher(|_k: &String, v: &flx_nocode_api::database::state::CachedPage| v.weight())
        .time_to_live(Duration::from_secs(l1_cache_ttl_secs))
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
        converter_token: CONFIG.converter_token.clone(),
        store: store_adapter,
        is_cachedb,
        write_queue_enabled,
        default_collate: env::var("DEFAULT_COLLATE").unwrap_or_else(|_| "utf8mb4_bin".to_string()),
        l1_cache,
    });

    // ── Generate default users ────────────────────────────────────────────────
    if require_auth {
        let _ = generate_users(app_state.clone(), &SCHEMAS.0).await;
    }
    // Force lazy statics to initialise now (avoids first-request latency).
    let _ = &*CONFIG;
    let _ = &*SCHEMAS;

    // ── Configuration lint (fail-fast; CONFIG_LINT=strict|warn|off) ─────────
    if let Err(e) = startup::run_config_lint() {
        eprintln!("{}", format!("CONFIG-LINT: {}", e).on_red());
        std::process::exit(3);
    }

    // ── Write-queue consumer ──────────────────────────────────────────────────
    if app_state.write_queue_enabled {
        match flx_nocode_api::database::redis::get_manager().await {
            Err(e) => eprintln!("WRITE QUEUE enabled but Redis not available: {}", e),
            Ok(_) => {
                start_consumer(app_state.clone()).await;
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
        match flx_nocode_api::database::redis::get_manager().await {
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

    // ── Atomic sequences table (custom ids / {seq:}) ─────────────────────────
    if app_state.db_type != flx_nocode_api::model::DbType::Mongodb {
        let ds = flx_nocode_api::storage::sql_store::SqlStore::new(app_state.db.clone(), app_state.db_type.as_str().to_string());
        let (sql_t, sql_i) = flx_nocode_api::nocode::generate::generate_table(&ds, &flx_nocode_api::nocode::sequences::table_schema());
        let (ok, msg) = flx_nocode_api::nocode::generate::execute_generate_table("flx_sequences".into(), &app_state, sql_t, sql_i).await;
        if !ok {
            eprintln!("WARNING: could not create flx_sequences ({}); custom ids fall back to MAX+1", msg);
        }
    }

    // ── Transactional outbox (webhooks) ──────────────────────────────────────
    if app_state.db_type != flx_nocode_api::model::DbType::Mongodb
        && env::var("OUTBOX_ENABLED").map(|v| !matches!(v.to_lowercase().as_str(), "0" | "false" | "no" | "off")).unwrap_or(true)
    {
        let ds = flx_nocode_api::storage::sql_store::SqlStore::new(app_state.db.clone(), app_state.db_type.as_str().to_string());
        let (sql_t, sql_i) = flx_nocode_api::nocode::generate::generate_table(&ds, &flx_nocode_api::nocode::outbox::table_schema());
        let (ok, msg) = flx_nocode_api::nocode::generate::execute_generate_table("flx_outbox".into(), &app_state, sql_t, sql_i).await;
        if ok {
            flx_nocode_api::nocode::outbox::start_worker(app_state.clone().into_inner());
        } else {
            eprintln!("OUTBOX disabled: could not create flx_outbox: {}", msg);
        }
    }

    // ── JWT revocation list (cross-replica via Redis when available) ─────────
    flx_nocode_api::auth::spawn_denylist_sync(app_state.is_cachedb);

    // ── Live registry (hot reload via POST /admin/reload or SIGHUP) ──────────
    let _ = &*flx_nocode_api::registry::current();
    flx_nocode_api::registry::spawn_sighup_listener(app_state.clone());

    // // ── Role seeding (background task) ────────────────────────────────────────
    // if app_state.db_type != flx_nocode_api::model::DbType::Mongodb {
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

    let server_result = HttpServer::new(move || {
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
            // Response compression is ON by default (JSON lists shrink 70-85 %).
            // Set ENABLE_COMPRESSION=false to turn it off (e.g. when a reverse
            // proxy already compresses). Binary downloads opt out per response.
            .wrap(Condition::new(
                env::var("ENABLE_COMPRESSION")
                    .map(|v| !matches!(v.to_lowercase().as_str(), "0" | "false" | "no" | "off"))
                    .unwrap_or(true),
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
    // Give in-flight HTTP requests up to 30 s to finish after SIGTERM/SIGINT.
    .shutdown_timeout(30)
    .bind((host, port))
    .map_err(|e| {
        eprintln!("Failed to bind to {}:{} - {}", host, port, e);
        e
    })?
    .run()
    .await;

    // ── Graceful shutdown of background work ─────────────────────────────────
    // HTTP has stopped (actix handled the signal). Stop pulling queue entries,
    // let the ones being processed finish, then flush the audit log.
    flx_nocode_api::shutdown::begin();
    let drained = flx_nocode_api::shutdown::drain(Duration::from_secs(30)).await;
    flx_nocode_api::audit::flush().await;
    log_output(
        "BOOT",
        "SHUTDOWN",
        "workers",
        if drained { "all background work finished".to_string() } else { format!("{} unit(s) still in flight after 30 s; left pending for re-delivery", flx_nocode_api::shutdown::inflight()) },
        true,
    );
    server_result.map_err(Into::into)
}
