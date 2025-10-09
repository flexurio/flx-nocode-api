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
use database::state::{AppState, DbRepository, QueryConverter};
#[cfg(feature = "mysql")]
use database::mysql::MySqlRepo;
#[cfg(feature = "postgres")]
use database::postgres::PostgresRepo;
#[cfg(feature = "sqlite")]
use database::sqlite::SqliteRepo;
#[cfg(feature = "mssql")]
use database::mssql::MssqlRepo;
#[cfg(all(feature = "mssql", feature = "bb8"))]
use database::mssql::MssqlConnectionManager;
mod nocode;
use nocode::{
    delete::delete, export::export, generate::create_table, get::select, import::import,
    patch::process_sp, post::insert, put::update, trace::process, validate::check_table_design,
};
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
use middleware::{GlobalRateLimit, AuthMiddleware};
#[cfg(feature = "mongodb")]
use crate::storage::mongodb_store::MongoStore;

// Load routes.json once and expose via CONFIG
static CONFIG: Lazy<crate::model::Config> = Lazy::new(|| {
    let config_location = std::env::var("LOC_CONFIG").unwrap_or_else(|_| {
        eprintln!("Warning: LOC_CONFIG not set, using default 'config/example'");
        "config/example".to_string()
    });
    let file_path = format!("{}/routes.json", config_location);

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
                config_location, e
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

// create static ISLOGGING from env LOGGING
static ISLOGGING: Lazy<bool> = Lazy::new(|| match env::var("LOGGING") {
    Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "yes"),
    Err(_) => false,
});

// create static LOC_LOGGING from env LOC_LOGGING
static LOC_LOGGING: Lazy<String> = Lazy::new(|| match env::var("LOC_LOGGING") {
    Ok(val) => val,
    Err(_) => "logs".to_string(),
});

// Ensure endpoint logging happens only once even if server factory runs multiple times
static ENDPOINT_LOG_ONCE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));


// Static Routes for once initialization
static FOREIGNKEY_ACTION: [&str; 4] = ["cascade", "set null", "restrict", "no action"];

static SCHEMAS: Lazy<Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>> = Lazy::new(|| {
    let config_location = env::var("LOC_CONFIG").unwrap_or_else(|_| "config/example".to_string());
    let config_dir = format!("{}/entity", config_location);
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
                    config_location, route, e
                );
                exit(1);
            }
        };

        // Early validation for TRACE upsert/merge requirements based on backend
        // Determine db type from env (same as later runtime config)
        let dbt = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string()).to_lowercase();
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
    // Early CLI handling: print version and exit
    {
        let mut args = env::args();
        let _ = args.next(); // skip binary name
        if let Some(first) = args.next() {
            if matches!(first.as_str(), "--version" | "-V" | "version") {
                println!("flx-nocode-api {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
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

    dotenv().ok();
    let secret_key = env::var("SECRET_KEY").expect("SECRET_KEY must be set");
    let encrypt_key = env::var("ENCRYPT_KEY").expect("ENCRYPT_KEY must be set");

    // Check config folder
    let config_location = env::var("LOC_CONFIG").unwrap_or_else(|_| "config".to_string());
    if !std::path::Path::new(&config_location).exists() {
        if let Err(e) = core::create_dir_and_get_config(&config_location).await {
            eprintln!("Failed to initialize config directory: {}", e);
        }
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

    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());
    // Pool configuration via env - optimized defaults for better resource management
    let max_pool: u32 = env::var("MAX_POOL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10); // Reduced from 100 to 10 for better memory usage
    let acquire_secs: u64 = env::var("CONNECT_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    // Optional pool tunings
    let min_pool: Option<u32> = env::var("MIN_POOL").ok().and_then(|s| s.parse().ok());
    let max_lifetime: Option<Duration> = env::var("POOL_MAX_LIFETIME_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);
    let idle_timeout: Option<Duration> = env::var("POOL_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);
    let db_repo: Arc<dyn DbRepository> = match db_type.as_str() {
        "mysql" => {
            #[cfg(not(feature = "mysql"))]
            {
                eprintln!("Feature 'mysql' not enabled at compile time");
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "mysql feature disabled"));
            }
            #[cfg(feature = "mysql")]
            {
            let url = match env::var("MYSQL_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output(
                        "ERROR",
                        ".ENV",
                        "MYSQL_URL",
                        "Please set MYSQL_URL on .env file".to_string(),
                        true,
                    );
                    exit(1);
                }
            };

            // Optimized MySQL connection pool with PoolOptions
            let mut opts = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(max_pool)
                .acquire_timeout(Duration::from_secs(acquire_secs));
            if let Some(min) = min_pool { opts = opts.min_connections(min); }
            if let Some(d) = max_lifetime { opts = opts.max_lifetime(d); }
            if let Some(d) = idle_timeout { opts = opts.idle_timeout(d); }
            let pool = opts
                .connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to MySQL: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;

            Arc::new(MySqlRepo { pool })
            }
        }
        "postgres" => {
            #[cfg(not(feature = "postgres"))]
            {
                eprintln!("Feature 'postgres' not enabled at compile time");
                return Err(std::io::Error::other("postgres feature disabled"));
            }
            #[cfg(feature = "postgres")]
            {
            let url = match env::var("POSTGRES_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output(
                        "ERROR",
                        ".ENV",
                        "POSTGRES_URL",
                        "Please set POSTGRES_URL on .env file".to_string(),
                        true,
                    );
                    exit(1);
                }
            };

            // Optimized PostgreSQL connection pool with PoolOptions
            let mut opts = sqlx::postgres::PgPoolOptions::new()
                .max_connections(max_pool)
                .acquire_timeout(Duration::from_secs(acquire_secs));
            if let Some(min) = min_pool { opts = opts.min_connections(min); }
            if let Some(d) = max_lifetime { opts = opts.max_lifetime(d); }
            if let Some(d) = idle_timeout { opts = opts.idle_timeout(d); }
            let pool = opts
                .connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to PostgreSQL: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;

            Arc::new(PostgresRepo { pool })
            }
        }
        "sqlite" => {
            #[cfg(not(feature = "sqlite"))]
            {
                eprintln!("Feature 'sqlite' not enabled at compile time");
                return Err(std::io::Error::other("sqlite feature disabled"));
            }
            #[cfg(feature = "sqlite")]
            {
            let url = match env::var("SQLITE_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output(
                        "ERROR",
                        ".ENV",
                        "SQLITE_URL",
                        "Please set SQLITE_URL on .env file".to_string(),
                        true,
                    );
                    exit(1);
                }
            };
            let path_db = url.replace("sqlite://", "");
            let db_path = std::path::Path::new(&path_db);

            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            if !db_path.exists() {
                std::fs::File::create(db_path)?;
                println!("File {} berhasil dibuat.", db_path.display());
            }

            // Optimized SQLite connection with PoolOptions
            let mut opts = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(max_pool)
                .acquire_timeout(Duration::from_secs(acquire_secs));
            if let Some(min) = min_pool { opts = opts.min_connections(min); }
            if let Some(d) = max_lifetime { opts = opts.max_lifetime(d); }
            if let Some(d) = idle_timeout { opts = opts.idle_timeout(d); }
            let pool = opts
                .connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to SQLite: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;

            Arc::new(SqliteRepo { pool })
            }
        }
        "mssql" => {
            #[cfg(not(feature = "mssql"))]
            {
                eprintln!("Feature 'mssql' not enabled at compile time");
                return Err(std::io::Error::other("mssql feature disabled"));
            }
            #[cfg(feature = "mssql")]
            {
            let url = match env::var("MSSQL_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output(
                        "ERROR",
                        ".ENV",
                        "MSSQL_URL",
                        "Please set MSSQL_URL on .env file".to_string(),
                        true,
                    );
                    exit(1);
                }
            };
            let tcp_timeout = acquire_secs;
            
            #[cfg(feature = "bb8")]
            {
                // Use BB8 connection pool for high performance
                let manager = MssqlConnectionManager::new(url.clone(), tcp_timeout);
                let pool = bb8::Pool::builder()
                    .max_size(max_pool)
                    .build(manager)
                    .await
                    .map_err(|e| {
                        eprintln!("Failed to create MSSQL pool: {}", e);
                        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                    })?;
                
                log_output(
                    "INFO",
                    "MSSQL",
                    "CONNECTION POOL",
                    format!("✅ BB8 pool created with max_size={}", max_pool),
                    false,
                );
                
                Arc::new(MssqlRepo { pool })
            }
            
            #[cfg(not(feature = "bb8"))]
            {
                // Fallback to single client (not recommended for production)
                let client = connect_mssql(&url, tcp_timeout).await.map_err(|e| {
                    eprintln!("Failed to connect to MSSQL: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
                
                eprintln!("⚠️  WARNING: MSSQL running with single client. Enable 'bb8' feature for better performance!");
                
                Arc::new(MssqlRepo { client: std::sync::Arc::new(tokio::sync::Mutex::new(client)) })
            }
            }
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            // For MongoDB, we don't have an SQL DbRepository; provide a dummy that always succeeds for simple checks.
            struct DummyRepo;
            #[async_trait::async_trait]
            impl DbRepository for DummyRepo {
                async fn query(&self, _sql: &str) -> anyhow::Result<Vec<Value>, anyhow::Error> { Ok(vec![]) }
                async fn begin_transaction(&self) -> anyhow::Result<Box<dyn database::state::DbTransaction>, anyhow::Error> {
                    Err(anyhow::anyhow!("Transactions not supported for DummyRepo"))
                }
            }
            Arc::new(DummyRepo)
        }

        _ => {
            eprintln!("Unsupported DB_TYPE: {}", db_type);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unsupported DB_TYPE: {}", db_type),
            ));
        }
    };

    // Inline per-dialect datetime SQL function
    let datetime_now: String = match db_type.as_str() {
        "mysql" => "NOW()".to_string(),
        "postgres" => "NOW()".to_string(),
        "sqlite" => "CURRENT_TIMESTAMP".to_string(),
        "mssql" => "GETDATE()".to_string(),
        _ => "CURRENT_TIMESTAMP".to_string(),
    };

    let query_converter = QueryConverter {
        datetime_now: datetime_now.clone(),
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

    let mut is_cachedb = false;

    // check if REDIS_HOST is configured in .env
    if env::var("REDIS_HOST") != Ok("".to_string()) {
        is_cachedb = true;
    }

    let app_state = web::Data::new(AppState {
        db: db_repo,
        db_type,
        secret: secret_key,
        encrypt_key,
        query_converter,
        whitelist_ips,
        route_publics: CONFIG.route_publics.clone().to_vec(),
        converter_token: CONFIG.converter_token.clone(),
        store: store_adapter,
        is_cachedb,
    });

    let (is_createdb, id_user_str) = generate_users(app_state.clone()).await;

    // Initialize Routes only once, using Lazy
    let _ = &*CONFIG;
    let _ = &*SCHEMAS;

    // loop every config.routes and check if table is exist in database
    if is_createdb {
        let state = web::Data::new(app_state.clone());
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        for route in CONFIG.routes.iter() {
            println!("Checking table design for route: {}", route);
            let schema = match SCHEMAS.0.iter().find(|s| s.table == *route) {
                Some(s) => s.clone(),
                None => {
                    eprintln!("No schema found for route '{}'", route);
                    exit(1);
                }
            };
            let (sql_create_table, sql_create_index) = generate_table(&ds, &schema);
            let (is_valid, msg) = execute_generate_table(route.to_string(), &app_state, sql_create_table, sql_create_index).await;
            if !is_valid {
                log_output("ERROR", "TABLE DESIGN CHECK", "FAILED", msg, true);
                exit(1);
            } else {
                log_output("INFO", "TABLE DESIGN CHECK", "SUCCESS", route.to_string(), false);
            }

        }
        if app_state.db_type != "mongodb" {
            // convert id_user_string to i64
            let id_user: i64 = id_user_str.parse().unwrap_or(1);
            let _ = generate_role_admin(&app_state, ds, id_user, CONFIG.routes.clone());
        }
    }

    let _ = &*ISDEBUG;

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

                // end point for login
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

                // end point for register
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

                // setup endpoint for each route
                for route in CONFIG.routes.iter() {
                    // Use Arc<str> for efficient shared ownership - cheap to clone, reduces heap allocations
                    let route_arc: Arc<str> = Arc::from(route.as_str());
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
                                        SCHEMAS.0.clone().into(),
                                        req,
                                    )
                                },
                            ))
                            // register create_nocode
                            .route(web::post().to(
                                move |state: web::Data<AppState>,
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    insert(
                                        state,
                                        route_post.to_string(),
                                        SCHEMAS.0.clone().into(),
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
                                        SCHEMAS.0.clone().into(),
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
                                        SCHEMAS.0.clone().into(),
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
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    delete(state, route_delete.to_string(), SCHEMAS.clone(), path, req)
                                },
                            ))
                            // register create_nocode
                            .route(web::put().to(
                                move |state: web::Data<AppState>,
                                      multipart: Multipart,
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    update(
                                        state,
                                        route_put.to_string(),
                                        SCHEMAS.clone(),
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
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    import(
                                        state,
                                        route_import.to_string(),
                                        SCHEMAS.clone(),
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
                                        SCHEMAS.clone(),
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
                                        SCHEMAS.0.clone().into(),
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
                                        SCHEMAS.0.clone().into(),
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
            .unwrap_or(1),
    )
    .max_connections(
        env::var("HTTP_MAX_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(25000)
    )
    .client_request_timeout(std::time::Duration::from_secs(30)) // 30 second timeout
    .client_disconnect_timeout(std::time::Duration::from_secs(5)) // 5 second disconnect timeout
    .bind((host, port))
    .map_err(|e| {
        eprintln!("Failed to bind to {}:{} - {}", host, port, e);
        e
    })?
    .run()
    .await
}
