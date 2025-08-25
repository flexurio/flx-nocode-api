use actix_files::Files;
use actix_multipart::Multipart;
use actix_web::dev::{Service, ServiceRequest};
use actix_web::web::Path;
use actix_web::{web, App, HttpServer};
use actix_cors::Cors;
use auth::validate_token;
use colored::Colorize;
use dotenv::dotenv;
use helpers::cetak_label;
use log::log_output;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::fs;
use std::fs::{File, create_dir_all};
use std::sync::Arc;
use std::process::exit;
use std::collections::HashSet;


use std::env;

mod auth;
mod crypt;
mod database;
use database::{
    state::{AppState, QueryConvertor, DbRepository},
    mysql::MySqlRepo,
    postgres::PostgresRepo,
    sqlite::SqliteRepo,
};
mod nocode;
use nocode::{
    get::select,
    post::insert,
    delete::delete,
    put::update,
    validate::check_table_design,
    generate::create_table,
    trace::process,
    patch::process_sp,
};
mod core;
use core::{generate_users, login, register};
mod model;
use model::TableSchema;

use crate::model::{ReferenceForeignKey, ReferenceForeignKeyAction};
mod helpers;
mod log;


// Load routes.json once and expose via CONFIG
static CONFIG: Lazy<crate::model::Config> = Lazy::new(|| {
    let config_location = std::env::var("LOC_CONFIG").unwrap_or_else(|_| {
        eprintln!("Warning: LOC_CONFIG not set, using default 'config/example'");
        "config/example".to_string()
    });
    let file_path = format!("{}/routes.json", config_location);

    let content = match std::fs::read_to_string(&file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "ERROR 9081231287 : Can't read file {} - {}",
                file_path.on_bright_red(),
                e
            );
            return crate::model::Config { routes: vec![], route_publics: vec![] };
        }
    };

    match serde_json::from_str(&content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!(
                "Sorry, content of /{}/routes.json is not valid JSON, with ERROR Message : {}",
                config_location, e
            );
            std::process::exit(1);
        }
    }
});

// Static whitelist for unauthenticated endpoints
const ROUTE_WHITELIST: [&str; 2] = ["/login", "/register"];

static ISDEBUG: Lazy<bool> = Lazy::new(|| match env::var("DEBUG") {
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

// Static Routes for once initialization
static ROUTE_PUBLICS: Lazy<Vec<String>> = Lazy::new(|| CONFIG.route_publics.clone());

// Static Routes for once initialization
static ROUTES: Lazy<Vec<String>> = Lazy::new(|| CONFIG.routes.clone());

static SCHEMAS: Lazy<Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>> = Lazy::new(|| {
    let config_location = env::var("LOC_CONFIG").unwrap_or_else(|_| "config/example".to_string());
    let config_dir = format!("{}/entity", config_location);
    let mut schemas = Vec::with_capacity(ROUTES.len()); // Pre-allocate capacity
    let mut ref_foreign_keys: Vec<ReferenceForeignKey> = Vec::with_capacity(ROUTES.len());

    for route in ROUTES.iter() {
        let file_path = format!("{}/{}.json", config_dir, route);

        let content = match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("ERROR 908ihu76 : Can't read file {} - {}", file_path.on_bright_red(), e);
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

        // check if schema.foreign_keys is not empty
        if !schema.foreign_keys.is_empty() {
            // loop througt all schema.foreign_keys
            for fk in schema.foreign_keys.iter() {
                ref_foreign_keys.push(ReferenceForeignKey{
                    table: fk.reference_table.clone(),
                    column: fk.reference_column.clone(),
                    on_delete_action: ReferenceForeignKeyAction{
                        table: schema.table.clone(),
                        column: fk.column.clone(),
                        action: fk.on_delete.clone(),
                    },
                    on_update_action: ReferenceForeignKeyAction{
                        table: schema.table.clone(),
                        column: fk.column.clone(),
                        action: fk.on_update.clone(),
                    },
                });
            }
        }

        schemas.push(schema);
    }

    // Shrink to fit to reduce memory overhead
    schemas.shrink_to_fit();

    println!("=========================");
    println!("ref_foreign_keys: {:?}", ref_foreign_keys);

    Arc::new((schemas, ref_foreign_keys))
});

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // check .env exit or not
    if !std::path::Path::new(".env").exists() {
        if let Err(e) = core::download_env_file().await {
            eprintln!("Failed to download .env file: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
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
        let path_image = format!("{}/{}",static_storage,image_storage);
        // check if directory exists
        if !std::path::Path::new(&path_image).exists() {
            std::fs::create_dir_all(&path_image)?;
        }
    }

    // Ensure db directory exists; download and extract config only if missing
    let current_dir = env::current_dir()?;
    let db_dir_path = current_dir.join("db");
    if !db_dir_path.exists() {
        if let Err(e) = core::create_dir_and_get_db().await {
            eprintln!("Failed to initialize db directory: {}", e);
        }
    }

    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());
    let db_repo: Arc<dyn DbRepository> = match db_type.as_str() {
        "mysql" => {
            let url = match env::var("MYSQL_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output("ERROR",".ENV","MYSQL_URL","Please set MYSQL_URL on .env file".to_string(),true);                    
                    exit(1);
                }
            };
            
            // Optimized MySQL connection pool
            let pool = sqlx::MySqlPool::connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to MySQL: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
            
            Arc::new(MySqlRepo { pool })
        }
        "postgres" => {
            let url = match env::var("POSTGRES_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output("ERROR",".ENV","POSTGRES_URL","Please set POSTGRES_URL on .env file".to_string(),true);                    
                    exit(1);
                }
            };
            
            // Optimized PostgreSQL connection pool
            let pool = sqlx::PgPool::connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to PostgreSQL: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
            
            Arc::new(PostgresRepo { pool })
        },
        "sqlite" => {
            let url = match env::var("SQLITE_URL") {
                Ok(url) => url,
                Err(_) => {
                    log_output("ERROR",".ENV","SQLITE_URL","Please set SQLITE_URL on .env file".to_string(),true);                    
                    exit(1);
                }
            };
            let path_db = url.replace("sqlite://", "");
            let db_path = std::path::Path::new(&path_db);

            if let Some(parent) = db_path.parent() {
                create_dir_all(parent)?;
            }

            if !db_path.exists() {
                File::create(db_path)?;
                println!("File {} berhasil dibuat.", db_path.display());
            }

            // Optimized SQLite connection
            let pool = sqlx::SqlitePool::connect(&url)
                .await
                .map_err(|e| {
                    eprintln!("Failed to connect to SQLite: {}", e);
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                })?;
            
            Arc::new(SqliteRepo { pool })
        },
        _ => {
            eprintln!("Unsupported DB_TYPE: {}", db_type);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unsupported DB_TYPE: {}", db_type)
            ));
        }
    };

    // Embed DB-specific datetime SQL to avoid runtime I/O
    let datetime_now: String = match db_type.as_str() {
        "mysql" => include_str!("../db/mysql/datetime.sql").to_string(),
        "postgres" => include_str!("../db/postgres/datetime.sql").to_string(),
        "sqlite" => include_str!("../db/sqlite/datetime.sql").to_string(),
        _ => "CURRENT_TIMESTAMP".to_string(),
    };

    let query_convertor = QueryConvertor{
        datetime_now: datetime_now.clone(),
    };

    let whitelist_ips: Vec<String> = env::var("WHITE_LIST_IP")
        .unwrap_or_else(|_| "".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let app_state = web::Data::new(AppState {
        db: db_repo,
        db_type,
        secret: secret_key,
        encrypt_key,
        query_convertor,
        whitelist_ips,
        route_publics:ROUTE_PUBLICS.to_vec(),
    });

    generate_users(app_state.clone()).await;

    // Initialize Routes only once, using Lazy
    let _ = &*ROUTES;
    let _ = &*SCHEMAS;
    let _ = &*ISDEBUG;

    if ROUTES.is_empty() {
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
            println!("{}",
                format!(
                    "ERROR 9081231287 : Table name '{}' is duplicated in config entity.",
                    schema.table
                ).on_red()
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

        App::new()
            .app_data(app_state.clone())
            .app_data(
                web::PayloadConfig::new(
                    env::var("UPLOAD_LIMIT_MB").ok()
                        .and_then(|s| s.parse::<usize>().ok())
                        .map(|mb| mb * 1024 * 1024)
                        .unwrap_or(10 * 1024 * 1024)
                )
            )
            .app_data(
                web::JsonConfig::default()
                    .limit(4096) // Limit JSON payload to 4KB for security
                    .error_handler(|err, _req| {
                        actix_web::error::InternalError::from_response(
                            format!("JSON error: {}", err),
                            actix_web::HttpResponse::BadRequest().json("Invalid JSON payload")
                        ).into()
                    })
            )
            .wrap_fn({
                // clone AppState handle into middleware closure so it owns it (satisfies 'static)
                let app_state = app_state.clone();
                move |req: ServiceRequest, srv| {
                    // check if route contain in whitelist or not
                    let route = req.path().trim_start_matches('/').to_string();
                    if ROUTE_WHITELIST.contains(&req.path()) || app_state.route_publics.contains(&route) {
                        return srv.call(req);
                    }

                    let app_data = req
                        .app_data::<web::Data<AppState>>()
                        .expect("AppState missing");
                    match validate_token(req.request().clone(), app_data.clone()) {
                        Ok(_) => srv.call(req),
                        Err(e) => Box::pin(async move { Ok(req.into_response(e)) }),
                    }
                }
            })
            .wrap(cors)
            .configure(|cfg: &mut web::ServiceConfig| {
                let static_loc = std::env::var("LOC_STATIC").unwrap_or_else(|_| "static".to_string());
                // end point for static files (disable directory listing in prod)
                let static_files = Files::new("/static", static_loc);
                if *ISDEBUG {
                    cfg.service(static_files.show_files_listing());
                } else {
                    cfg.service(static_files);
                }
                log_output("CORE ENDPOINT","METHOD", "GET",
                    format!(
                        "http://{}:{}/{}",
                        host.red(),
                        port.clone().to_string().green(),
                        "static".purple()
                    ),false
                );

                // end point for login
                cfg.service(web::resource("/login").route(web::post().to(
                    move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                        login(state, req)
                    },
                )));
                log_output("CORE ENDPOINT","METHOD","POST",
                    format!("http://{}:{}/{}",host.red(),port.clone().to_string().green(),"login".purple()
                    ),false
                );

                // end point for register
                cfg.service(web::resource("/register").route(web::post().to(
                    move |state: web::Data<AppState>, multipart: Multipart| {
                        register(state, multipart)
                    },
                )));
                log_output("CORE ENDPOINT","METHOD",
                    "POST",
                    format!(
                        "http://{}:{}/{}",
                        host.red(),
                        port.clone().to_string().green(),
                        "register".purple()
                    ),
                    false
                );

                println!("\n");

                // setup endpoint for each route
                for route in ROUTES.iter() {
                    let route_get = route.clone();
                    let route_trace = route.clone();
                    let route_patch = route.clone();
                    let route_post = route.clone();
                    let route_delete = route.clone();
                    let route_put = route.clone();
                    let route_validate = route.clone();
                    let route_generate_table = route.clone();

                    log_output("ENDPOINT","METHOD","GET",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_get.clone().purple()
                        ),false 
                    );
                    log_output("ENDPOINT","METHOD","POST",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_post.clone().purple()
                        ),false
                    );
                    log_output("ENDPOINT","METHOD","TRACE",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_trace.clone().purple()
                        ),false
                    );
                    log_output("ENDPOINT","METHOD","PATCH",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_patch.clone().purple()
                        ),false
                    );

                    cfg.service(
                        web::resource((*(route_get.clone())).to_string())
                            // register nocode_get
                            .route(web::get().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      req: actix_web::HttpRequest| {
                                    select(
                                        state,
                                        parameters,
                                        route_get.clone(),
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
                                        route_post.clone(),
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
                                        route_trace.clone(),
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
                                        route_patch.clone(),
                                        SCHEMAS.0.clone().into(),
                                        req,
                                    )
                                },
                            )),
                    );

                    log_output("ENDPOINT","METHOD","DELETE",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_delete.clone().purple()
                        ),false
                    );
            log_output("ENDPOINT","METHOD","PUT",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                route_put.clone().purple()
                        ),false
                    );

                    cfg.service(
                        web::resource(format!("{}/{{id}}", &*route_delete))
                            // register delete_nocode
                            .route(web::delete().to(
                                move |state: web::Data<AppState>,
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    delete(
                                        state,
                                        route_delete.clone(),
                                        SCHEMAS.clone(),
                                        path,
                                        req,
                                    )
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
                                        route_put.clone(),
                                        SCHEMAS.0.clone().into(),
                                        multipart,
                                        path,
                                        req,
                                    )
                                },
                            )),
                    );

                    log_output("ENDPOINT","METHOD","GET",
                        format!(
                            "http://{}:{}/{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            "validate".yellow(),
                            route_validate.clone().purple()),
                        false
                    );
                    cfg.service(
                        web::resource(format!("validate/{}", &*route_validate)).route(
                            web::get().to(
                                move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                                    check_table_design(
                                        state,
                                        route_validate.clone(),
                                        SCHEMAS.0.clone().into(),
                                        req,
                                    )
                                },
                            ),
                        ),
                    );

                    if route_generate_table != "flx_users" && route_generate_table != "flx_roles" {
                        log_output("ENDPOINT","METHOD", "POST",
                            format!(
                                "http://{}:{}/{}/{}",
                                host.red(),
                                port.clone().to_string().green(),
                                "generate/table".yellow(),
                                route_generate_table.clone().purple()
                            ),
                            false
                        );
                        cfg.service(
                            web::resource(format!("generate/table/{}", &*route_generate_table)).route(
                                web::post().to(
                                    move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                                        create_table(
                                            state,
                                            route_generate_table.clone(),
                                            SCHEMAS.0.clone().into(),
                                            req,
                                        )
                                    },
                                ),
                            ),
                        );

                    }

                    println!("\n");
                }
            })
    })
    .workers(
        env::var("ACTIX_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1)
    )
    .max_connections(25000) // Limit concurrent connections
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
