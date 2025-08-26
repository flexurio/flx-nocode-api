
use std::{error::Error, fs::File, io, path::Path};

use actix_multipart::Multipart;
use actix_web::{
    web::{self, Data},
    HttpResponse, Responder,
};
use base64::{self, Engine};
use rand::Rng;
use reqwest::Client;
use zip::ZipArchive;
use serde_json::{json, Value};

use crate::{
       auth::create_token,
       crypt::{decrypt, encrypt},
       database::state::DbParam,
       helpers::multipart_to_json,
       log::log_output,
       model::WebResponse,
       AppState
};
use crate::rate_limit::RL_WINDOW_LOGIN;
use chrono::Local;


pub async fn login(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {   
                // Rate limit by IP (fixed window)
                let ip_key = req.peer_addr().map(|a| a.ip().to_string()).unwrap_or_else(|| "unknown".into());
                let limit: u32 = std::env::var("RATE_LIMIT_LOGIN_PER_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(10);
                if !RL_WINDOW_LOGIN.check_and_increment(&format!("login:{}", ip_key), limit) {
                       return HttpResponse::TooManyRequests().json(WebResponse { success: false, message: "Too many login attempts".into(), total_data: 0, data: Value::Null });
                }
          // Parse Authorization Basic header safely
          let Some(hdr) = req.headers().get("Authorization") else {
                 return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Missing Authorization".into(), total_data: 0, data: Value::Null });
          };
              let Ok(hstr) = hdr.to_str() else {
                     return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Invalid Authorization".into(), total_data: 0, data: Value::Null });
              };
              let Some(b64) = hstr.strip_prefix("Basic ") else {
                     return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Expect Basic".into(), total_data: 0, data: Value::Null });
              };
              let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64) else {
                     return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Invalid base64".into(), total_data: 0, data: Value::Null });
              };
              let Ok(pair) = String::from_utf8(bytes) else {
                     return HttpResponse::Unauthorized().json(WebResponse { success: false, message: "Invalid credentials".into(), total_data: 0, data: Value::Null });
              };
              let mut parts = pair.splitn(2, ":");
              let email = parts.next().unwrap_or("");
              let pass_in = parts.next().unwrap_or("");
   
       // read sql from file db/mysql/create-flx_users.sql
       let s_sql_tpl = match std::fs::read_to_string(format!("db/{}/select-flx_users-login.sql", state.db_type)) {
                 Ok(s) => s,
                 Err(_) => return HttpResponse::InternalServerError().json(WebResponse { success: false, message: "Login SQL missing".into(), total_data: 0, data: Value::Null }),
          };
       // Use parameter placeholder and bind email safely (backends handle Postgres placeholder conversion)
         let s_sql = s_sql_tpl
                .replace("\"", "")
                .replace("'{{email}}'", "?")
                .replace("{{email}}", "?");
       
       log_output("QUERY", "POST", "login", s_sql.clone(), true);
   
          let (password_db, id_user, name) = match &state.db.query_with_params(&s_sql, vec![DbParam::Str(email.to_string())]).await {
                     Ok(rows) => {
                            if let Some(row0) = rows.first() {
                                   let password = row0.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string().replace(" ", "");
                                   let id = row0.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                                   let name = row0.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                   (password, id, name)
                            } else {
                                   println!("No user found with email: {}", email);
                                   ("".to_string(), 0_i64, "".to_string())
                            }
                     },
                     Err(_) => ("".to_string(), 0_i64, "".to_string()),
          };
   
       let decrypt_password = decrypt(state.encrypt_key.clone(), password_db);
   
       if pass_in != decrypt_password {
           return HttpResponse::Unauthorized().json(WebResponse {
               success: false,
               message: "Login Failed".to_string(),
               total_data: 0,
               data: Value::Null,
           });
       }
   
         // Cross-DB: avoid CONCAT differences; fetch endpoint & role and join in Rust
         let s_sql = "SELECT endpoint, role FROM flx_roles WHERE id_users = ?".to_string();
         log_output("QUERY", "POST", "flx_roles", s_sql.clone(), true);
         let roles_rows = state.db.query_with_params(&s_sql, vec![DbParam::I64(id_user)]).await.unwrap_or_default();
         let roles_data = roles_rows.into_iter().filter_map(|v| {
                let obj = v.as_object()?;
                let ep = obj.get("endpoint")?.as_str()?.to_string();
                let rl = obj.get("role")?.as_i64().map(|n| n.to_string()).or_else(|| obj.get("role")?.as_str().map(|s| s.to_string()))?;
                Some(format!("{}/{}", ep, rl))
         }).collect::<Vec<_>>().join(",");
   
       let token = create_token(id_user, name, state.clone(), roles_data);
       HttpResponse::Ok().json(WebResponse {
           success: true,
           message: "Login Success".to_string(),
           total_data: 1,
           data: json!(token.await),
       })
   }
   
// NCO-POST
pub async fn register(state: Data<AppState>, multipart: Multipart) -> impl Responder {
       let body = multipart_to_json(multipart).await.unwrap();

       if body["email"] == "" || body["password"] == "" || body["name"] == "" || body["phone"] == "" {
              return HttpResponse::BadRequest().json(WebResponse {
                     success: false,
                     message: "Email and Password is required".to_string(),
                     total_data: 0,
                     data: Value::Null,
              });
       }

       let password_value = &body["password"];
       let password = if password_value.is_string() {
              password_value.as_str().unwrap().to_string()
       } else {
              password_value.to_string()
       };

       let encrypt_password = encrypt(
              state.encrypt_key.clone(),
              password,
       );

       // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
       let s_sql = format!(
              "INSERT INTO flx_users (email, phone, password, name, created_at, updated_at, enabled) VALUES (?, ?, ?, ?, {}, {}, 1)",
              state.query_converter.datetime_now,
              state.query_converter.datetime_now
       );

       log_output("QUERY", "POST", "register", s_sql.clone(), true);

       // execute sql
       match &state.db.query_with_params(&s_sql, vec![
              DbParam::Str(body["email"].to_string().trim_matches('"').to_string()),
              DbParam::Str(body["phone"].to_string().trim_matches('"').to_string()),
              DbParam::Str(encrypt_password),
              DbParam::Str(body["name"].to_string().trim_matches('"').to_string()),
       ]).await {
              Ok(_) => HttpResponse::Ok().json(WebResponse {
                     success: true,
                     message: "Register Success".to_string(),
                     total_data: 1,
                     data: Value::Null,
              }),
              Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                     success: false,
                     message: format!("Error NCO-POST: {}", err),
                     total_data: 0,
                     data: Value::Null,
              }),
       }
}

// NCO-POST
pub async fn generate_users(state: Data<AppState>) -> impl Responder {

       // read sql from file db/mysql/create-flx_users.sql
       let db_file_path = format!("db/{}/create-flx_users.sql", state.db_type);
       let mut s_sql = std::fs::read_to_string(db_file_path)
              .expect("Failed to read SQL file")
              .replace("\"", "");

       log_output("QUERY", "POST", "generate/table/flx_users", s_sql.clone(), true);

       // execute sql
       match &state.db.query(&s_sql).await {
              Ok(_) => HttpResponse::Ok().json(WebResponse {
                     success: true,
                     message: "Generate Table users".to_string(),
                     total_data: 1,
                     data: Value::Null,
              }),
              Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                     success: false,
                     message: format!("Error NCO-POST: {}", err),
                     total_data: 0,
                     data: Value::Null,
              }),
       };

       // read sql from file db/mysql/create-flx_users.sql
       s_sql = std::fs::read_to_string(format!("db/{}/create-flx_roles.sql", state.db_type))
              .expect("Failed to read SQL file")
              .replace("\"", "");

       log_output("QUERY", "POST", "generate/table/flx_roles", s_sql.clone(), true);

       // execute sql
       match &state.db.query(&s_sql).await {
              Ok(_) => HttpResponse::Ok().json(WebResponse {
                     success: true,
                     message: "Generate Table users".to_string(),
                     total_data: 1,
                     data: Value::Null,
              }),
              Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                     success: false,
                     message: format!("Error NCO-POST: {}", err),
                     total_data: 0,
                     data: Value::Null,
              }),
       };


       // guery to flx_users where name = "Flexurio Admin"
       // read sql from file db/mysql/create-flx_users.sql
       s_sql = std::fs::read_to_string(format!("db/{}/select-flx_users-admin.sql", state.db_type))
              .expect("Failed to read SQL file")
              .replace("\"", "");

       let mut id_user: i64 = match &state.db.query(&s_sql).await {
              Ok(row) => {
                     // check if row is empty
                     if row.is_empty() {
                     0                
                     } else {
                     row[0].get("id").and_then(|v| v.as_i64()).unwrap_or(0)
                     }
              },
              Err(_) => 0,
       };

       log_output("QUERY", "POST", "generate/table/users", s_sql.clone(), true);


       if id_user == 0 {
              id_user = 1;
              // create string number
              let random_pass = rand::rng().random_range(1000..9999).to_string();
              let encrypt_password = encrypt(state.encrypt_key.clone(), random_pass.clone());

              println!("==========================================");
              println!("Your admin Password: {:?}", random_pass);
              println!("==========================================");


              // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
              s_sql = std::fs::read_to_string(format!("db/{}/insert-flx_users-admin.sql", state.db_type))
              .expect("Failed to read SQL file")
              .replace("\"", "").
              replace("{{password}}", &encrypt_password);

              log_output("EXEC", "POST", "generate/table/users", s_sql.clone(), true);


              // execute sql
              match &state.db.query(&s_sql).await {
                     Ok(_) => HttpResponse::Ok().json(WebResponse {
                     success: true,
                     message: "Generate Table users".to_string(),
                     total_data: 1,
                     data: Value::Null,
                     }),
                     Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                     success: false,
                     message: format!("Error NCO-POST: {}", err),
                     total_data: 0,
                     data: Value::Null,
                     }),
              };

              // insert into test.users (id, email, phone, role, password, name, photo, email_verified, created_at, updated_at, enabled)
              s_sql = std::fs::read_to_string(format!("db/{}/insert-flx_roles.sql", state.db_type))
              .expect("Failed to read SQL file")
              .replace("\"", "").
              replace("{{id_user}}", &id_user.to_string());

              // split s_sql by ;
              let array_sql: Vec<&str> = s_sql.split(";").collect();

              // loop through array_sql and execute each sql
              for sql in array_sql {
                     if !sql.trim().is_empty() {
                            log_output("EXEC", "POST", "generate/table/users", sql.to_string(), true);
                            match &state.db.query(sql).await {
                                   Ok(_) => (),
                                   Err(err) => {
                                          return HttpResponse::InternalServerError().json(WebResponse {
                                                 success: false,
                                                 message: format!("Error NCO-POST: {}", err),
                                                 total_data: 0,
                                                 data: Value::Null,
                                          });
                                   },
                            };
                     }
              }


       }



       HttpResponse::Ok().json(WebResponse {
              success: true,
              message: "Generate Table users".to_string(),
              total_data: 1,
              data: Value::Null,
       })

}
   

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
}

async fn get_latest_release() -> Result<String, Box<dyn Error + Send + Sync>> {
    let url = "https://api.github.com/repos/flexurio/flx-nocode-api/releases/latest";

    let client = Client::new();
    let resp = client
        .get(url)
        .header("User-Agent", "flexurio-client") // GitHub butuh user-agent
        .send()
        .await?
        .error_for_status()?
        .json::<Release>()
        .await?;

    Ok(resp.tag_name)
}

/// Download file zip dari URL dan simpan ke file lokal
async fn download_file(url: String, output: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let mut resp = client.get(url).send().await?.error_for_status()?;
    let mut out = File::create(output)?;
    while let Some(chunk) = resp.chunk().await? {
        use std::io::Write;
        out.write_all(&chunk)?;
    }
    Ok(())
}

/// Ekstrak file zip ke folder tujuan
fn extract_zip(zip_path: &str, target_dir: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let out_path = Path::new(target_dir).join(file.name());

        if file.is_dir() {
            std::fs::create_dir_all(&out_path)?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut outfile = File::create(&out_path)?;
            io::copy(&mut file, &mut outfile)?;
        }
    }

    Ok(())
}


// Function create directory & get config
pub(crate) async fn create_dir_and_get_config(conf: &str) -> Result<(), std::io::Error> {
       // If db directory already exists, skip
       if Path::new(conf).exists() { return Ok(()); }

       // get latest version from github release
       let latest_version = get_latest_release().await.unwrap_or_else(|_| "v1.0.0".to_string());
       let url = format!("https://github.com/flexurio/flx-nocode-api/releases/download/{}/config.zip", latest_version);
       let zip_path = "config.zip";
       let extract_to = "."; // current working directory

       println!("Downloading...");
       download_file(url, zip_path).await.map_err(std::io::Error::other)?;

       println!("Extracting...");
       extract_zip(zip_path, extract_to).map_err(std::io::Error::other)?;

       // move config to <conf>
       std::fs::rename(format!("{}/config", extract_to), conf)?;

       // Ensure config directory now exists
       if !Path::new(conf).exists() {
              return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "config directory not found after extraction"));
       }

       // Clean up zip file
       let _ = std::fs::remove_file(zip_path);

       Ok(())
}

// Function create directory & get config
pub(crate) async fn create_dir_and_get_db() -> Result<(), std::io::Error> {
       // If db directory already exists, skip
       if Path::new("db").exists() { return Ok(()); }

       // get latest version from github release
       let latest_version = get_latest_release().await.unwrap_or_else(|_| "v1.0.0".to_string());
       let url = format!("https://github.com/flexurio/flx-nocode-api/releases/download/{}/db.zip", latest_version);
       let zip_path = "db.zip";
       let extract_to = "."; // current working directory

       println!("Downloading...");
       download_file(url, zip_path).await.map_err(std::io::Error::other)?;

       println!("Extracting...");
       extract_zip(zip_path, extract_to).map_err(std::io::Error::other)?;

       // Ensure db directory now exists
       if !Path::new("db").exists() {
              return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "db directory not found after extraction"));
       }

       // Clean up zip file
       let _ = std::fs::remove_file(zip_path);

       Ok(())
}


// function download .env from latest release github
pub(crate) async fn download_env_file() -> Result<(), Box<dyn Error + Send + Sync>> {
    let latest_version = get_latest_release().await?;
    let url = format!("https://github.com/flexurio/flx-nocode-api/releases/download/{}/env", latest_version);
    let output_path = ".env";

    println!("Downloading .env file from {}", url);
    download_file(url, output_path).await?;

    Ok(())
}