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
use serde::{Deserialize, Serialize};
use serde_json::{from_str, Value};
use std::env;
use std::fs;

mod db;
use db::AppState;
use db::DbRepository;
use db::MySqlRepo;
use db::PostgresRepo;
use std::sync::Arc;

mod services;
use services::{
    generate_users, login, nocode_delete, nocode_generate_table, nocode_get, nocode_post,
    nocode_put, nocode_trace, nocode_validate, register,
};

mod auth;
mod crypt;

mod model;
use model::{Config, TableSchema};

mod helpers;
mod log;

static ISDEBUG: Lazy<bool> = Lazy::new(|| match env::var("DEBUG") {
    Ok(val) => val == "True",
    Err(_) => false,
});

// create static ISLOGGING from env LOGGING
static ISLOGGING: Lazy<bool> = Lazy::new(|| match env::var("LOGGING") {
    Ok(val) => val == "True",
    Err(_) => false,
});

// create static LOC_LOGGING from env LOC_LOGGING
static LOC_LOGGING: Lazy<String> = Lazy::new(|| match env::var("LOC_LOGGING") {
    Ok(val) => val,
    Err(_) => "logs".to_string(),
});

// Struktur untuk claims JWT
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
    iat: usize,
}

// Static Routes for once initialization
static ROUTES: Lazy<Vec<String>> = Lazy::new(|| {
    let config_location = env::var("LOC_CONFIG").expect("LOC_CONFIG must be set");
    // let file_path = format!("{}/config/routes.json", env::current_dir().unwrap().display());
    // // Buat path ke file
    let file_path = match env::current_dir() {
        Ok(dir) => format!("{}/{}/routes.json", dir.display(), config_location),
        Err(e) => {
            println!("Not found : {}", e);
            return Vec::new();
        }
    };
    
    // Baca isi file
    let content = match fs::read_to_string(&file_path) {
        Ok(content) => content,
        Err(_) => {
            println!("ERROR : Can't read file {}", file_path.on_bright_red());
            return Vec::new();
        }
    };

    // Parse JSON
    let config: Config = match from_str(&content) {
        Ok(config) => config,
        Err(e) => {
            println!(
                "Sorry, content of /{}/routes.json is not valid JSON, with ERROR Message : {}",
                config_location, e
            );
            return Vec::new();
        }
    };

    config.routes.clone()
});

static SCHEMAS: Lazy<Vec<TableSchema>> = Lazy::new(|| {
    let config_location = env::var("LOC_CONFIG").expect("LOC_CONFIG must be set");
    let config_dir = format!(
        "{}/{}/entity",
        env::current_dir().unwrap().display(),
        config_location
    );
    let mut schemas = Vec::new();

    // loop every route in ROUTES
    for route in ROUTES.iter(){
        let file_path = format!("{}/{}.json", config_dir, route);
        // Baca isi file
        let content = match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(_) => {
                println!("ERROR : Can't read file {}", file_path.on_bright_red());
                return Vec::new();
            }
        };

        // Parse JSON
        let schema: TableSchema = match from_str(&content) {
            Ok(schema) => schema,
            Err(e) => {
                println!(
                    "Sorry, content of /{}/entity/{}.json is not valid JSON, with ERROR Message : {}",
                    config_location, route, e
                );
                return Vec::new();
            }
        };
        schemas.push(schema);

    }

    schemas
});

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let secret_key = env::var("SECRET_KEY").expect("SECRET_KEY must be set");
    let encrypt_key = env::var("ENCRYPT_KEY").expect("ENCRYPT_KEY must be set");
    // let pool = MySqlPoolOptions::new()
    //     .max_connections(5)
    //     .connect(&database_url)
    //     .await
    //     .expect("Error building a connection pool");

    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());

    let db_repo: Arc<dyn DbRepository> = match db_type.as_str() {
        "mysql" => {
            let url: String = env::var("MYSQL_URL").expect("MYSQL_URL must be set");
            let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
            Arc::new(MySqlRepo { pool })
        }
        "postgres" => {
            let url = env::var("POSTGRES_URL").expect("POSTGRES_URL must be set");
            let pool = sqlx::PgPool::connect(&url).await.unwrap();
            Arc::new(PostgresRepo { pool })
        }
        _ => panic!("Unsupported DB_TYPE: {}", db_type),
    };
            
    let app_state = web::Data::new(AppState {
        db: db_repo,
        secret: secret_key,
        encrypt_key,
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


    let host: &str = "0.0.0.0";
    let port: u16 = env::var("PORT")
        .expect("PORT must be set")
        .parse()
        .expect("PORT must be a valid u16");

    cetak_label(host.to_string(), port);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap_fn(|req: ServiceRequest, srv| {
                let whitelist = ["/login", "/register"];
                // check if route contain in whitelist or not
                if whitelist.contains(&req.path()) {
                    return srv.call(req);
                }
                
                let app_data = req
                    .app_data::<web::Data<AppState>>()
                    .expect("AppState missing");
                match validate_token(req.request().clone(), app_data.clone()) {
                    Ok(_) => srv.call(req),
                    Err(e) => Box::pin(async move { Ok(req.into_response(e)) }),
                }
            })
            .wrap(
                Cors::default()
                    .allow_any_origin()  // Mengizinkan semua origin (bisa disesuaikan)
                    .allow_any_method()  // Mengizinkan semua HTTP methods
                    .allow_any_header()  // Mengizinkan semua header
                    .supports_credentials()  // Mengizinkan credentials
                    .max_age(3600)  // Cache preflight request selama 1 jam
            )
            .configure(|cfg: &mut web::ServiceConfig| {
                // end point for static files
                cfg.service(Files::new("/static", "./static").show_files_listing());
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
                    false
                );

                // end point for login
                cfg.service(web::resource("/login").route(web::post().to(
                    move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                        login(state, req)
                    },
                )));
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
                    false
                );

                // end point for register
                cfg.service(web::resource("/register").route(web::post().to(
                    move |state: web::Data<AppState>, multipart: Multipart| {
                        register(state, multipart)
                    },
                )));
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
                    false
                );

                println!("\n");

                // setup endpoint for each route
                for route in ROUTES.iter() {
                    let route_get = route.clone();
                    let route_query = route.to_owned();
                    let route_post = route.to_owned();
                    let route_delete = route.to_owned();
                    let route_put = route.to_owned();
                    let route_validate = route.to_owned();
                    let route_generate_table = route.to_owned();

                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "GET",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_get.clone().purple()
                        ),
                        false 
                    );
                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "POST",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_get.clone().purple()
                        ),
                        false
                    );
                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "TRACE",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_get.clone().purple()
                        ),
                        false
                    );

                    cfg.service(
                        web::resource((*(route_get.clone())).to_string())
                            // register nocode_get
                            .route(web::get().to(
                                move |state: web::Data<AppState>,
                                      parameters: web::Query<Value>,
                                      req: actix_web::HttpRequest| {
                                    nocode_get(
                                        state,
                                        parameters,
                                        route_get.clone(),
                                        SCHEMAS.clone(),
                                        req,
                                    )
                                },
                            ))
                            // register create_nocode
                            .route(web::post().to(
                                move |state: web::Data<AppState>,
                                      multipart: Multipart,
                                      req: actix_web::HttpRequest| {
                                    nocode_post(
                                        state,
                                        route_post.clone(),
                                        SCHEMAS.clone(),
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
                                    nocode_trace(
                                        state,
                                        parameters,
                                        route_query.clone(),
                                        SCHEMAS.clone(),
                                        req,
                                    )
                                },
                            )),
                    );

                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "DELETE",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_delete.clone().purple()
                        ),
                        false
                    );
                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "PUT",
                        format!(
                            "http://{}:{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            route_delete.clone().purple()
                        ),
                        false
                    );

                    cfg.service(
                        web::resource(format!("{}/{{id}}", &*route_delete))
                            // register delete_nocode
                            .route(web::delete().to(
                                move |state: web::Data<AppState>,
                                      path: Path<String>,
                                      req: actix_web::HttpRequest| {
                                    nocode_delete(
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
                                    nocode_put(
                                        state,
                                        route_put.clone(),
                                        SCHEMAS.clone(),
                                        multipart,
                                        path,
                                        req,
                                    )
                                },
                            )),
                    );

                    log_output(
                        "ENDPOINT",
                        "METHOD",
                        "GET",
                        format!(
                            "http://{}:{}/{}/{}",
                            host.red(),
                            port.clone().to_string().green(),
                            "validate".yellow(),
                            route_validate.clone().purple()
                        ),
                        false
                    );
                    cfg.service(
                        web::resource(format!("validate/{}", &*route_validate)).route(
                            web::get().to(
                                move |state: web::Data<AppState>, req: actix_web::HttpRequest| {
                                    nocode_validate(
                                        state,
                                        route_validate.clone(),
                                        SCHEMAS.clone(),
                                        req,
                                    )
                                },
                            ),
                        ),
                    );

                    if route_generate_table != "flx_users" && route_generate_table != "flx_roles" {
                        log_output(
                            "ENDPOINT",
                            "METHOD",
                            "POST",
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
                                        nocode_generate_table(
                                            state,
                                            route_generate_table.clone(),
                                            SCHEMAS.clone(),
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
    .workers(1)
    .bind((host, port))?
    .run()
    .await
}
