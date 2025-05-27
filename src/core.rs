
use actix_multipart::Multipart;
use actix_web::{
    web::{self, Data},
    HttpResponse, Responder,
};
use base64::{self, Engine};
use rand::Rng;
use serde_json::{json, Value};

use crate::{
    auth::create_token, crypt::{decrypt, encrypt}, db::concat_column_values, helpers::multipart_to_json, log::log_output, model::WebResponse, AppState
};


pub async fn login(state: web::Data<AppState>, req: actix_web::HttpRequest) -> impl Responder {
       println!("Masuk Login");
   
       // get username password from req Authorization Basic
       let auth_split: Vec<&str> = req
           .headers()
           .get("Authorization")
           .unwrap()
           .to_str()
           .unwrap()
           .split(" ")
           .collect();
   
       let auth_decoded = base64::engine::general_purpose::STANDARD
           .decode(auth_split[1])
           .unwrap();
       let auth_str = String::from_utf8(auth_decoded).unwrap();
       let auth_str_split: Vec<&str> = auth_str.split(":").collect();
   
       // read sql from file db/mysql/create-flx_users.sql
       let s_sql = std::fs::read_to_string(format!("db/{}/select-flx_users-login.sql", state.db_type))
       .expect("Failed to read SQL file")
       .replace("\"", "")
       .replace("{{email}}", auth_str_split[0]);
   
       log_output("QUERY", "POST", "login", s_sql.clone(), true);
   
       let (password_db, id_user, name) = match &state.db.query(&s_sql).await {
           Ok(row) =>  {
               println!("row: {:?}", row);
   
               let password = row[0].get("password")
                   .and_then(|v| v.as_str())
                   .unwrap_or("")
                   .to_string()
                   .replace(" ", "");
   
               let id = row[0].get("id")
                   .and_then(|v| v.as_i64())
                   .unwrap_or(0);
       
               let name = row[0].get("name")
                   .and_then(|v| v.as_str())
                   .unwrap_or("")
                   .to_string();
       
               (password, id, name)
           },
           Err(_) => ("".to_string(), 0_i64, "".to_string()),
       };
   
   
       let decrypt_password = decrypt(state.encrypt_key.clone(), password_db);
   
       if auth_str_split[1] != decrypt_password {
           return HttpResponse::Unauthorized().json(WebResponse {
               success: false,
               message: "Login Failed".to_string(),
               total_data: 0,
               data: Value::Null,
           });
       }
   
       // query to table flx_roles and save to variable roles
       let s_sql = format!(
           "SELECT CONCAT(endpoint,'/', role) as endpoint_role
            FROM flx_roles
            WHERE id_users = {}",
           id_user
       );
   
       log_output("QUERY", "POST", "flx_roles", s_sql.clone(), true);
   
       let roles = state.db.query(&s_sql).await.unwrap_or_default();
   
       let roles_data = concat_column_values(roles,"endpoint_role", ",");
   
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
              "INSERT INTO flx_users (email, phone,  password, name, created_at, updated_at, enabled) VALUES ('{}', '{}', '{}', '{}', {}, {}, 1)",
              body["email"], body["phone"], encrypt_password, body["name"], state.query_convertor.datetime_now, state.query_convertor.datetime_now
       ).replace("\"", "");

       log_output("QUERY", "POST", "register", s_sql.clone(), true);

       // execute sql
       match &state.db.query(&s_sql).await {
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
   