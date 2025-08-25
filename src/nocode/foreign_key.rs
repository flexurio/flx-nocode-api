use actix_web::{web::Data};

use crate::{database::state::{AppState, DbParam, DbTransaction}, log::log_output, model::ReferenceForeignKey};




// create function to post delete or update table with foreign key constraints
pub(crate) async fn process_foreign_keys_delete_update(
       type_process: &str, // "DELETE" or "UPDATE"
       state: Data<AppState>,
       transaction: &mut Box<dyn DbTransaction>,
       reference_foreign_keys: &[ReferenceForeignKey],
       id_user: i64,
       id_data: String,
       id_new: String, // for UPDATE
) -> (bool, String) {
       let mut status_executed = true;
       let mut error_message = String::new();

       for fk in reference_foreign_keys.iter() {
              let data_table = fk.on_delete_action.clone();
              let mut bind_params_fk: Vec<DbParam> = Vec::new();
              let s_sql_fk;

              if type_process == "DELETE" {
                     if fk.on_delete_action.type_delete == "soft" {
                            s_sql_fk = format!(
                                   "UPDATE {} SET deleted_at = {}, deleted_by_id = ? WHERE {} = ?",
                                   data_table.table, state.query_converter.datetime_now, fk.on_delete_action.column
                            );
                            bind_params_fk.push(DbParam::I64(id_user));
                     } else if fk.on_delete_action.type_delete == "hard" {
                            // create query DELETE sql parameterized by id
                            s_sql_fk = format!("DELETE FROM {} WHERE {} = ?", data_table.table, fk.on_delete_action.column);
                     } else {
                            continue; // skip if type_delete is not soft or hard
                     }

                     // Bind id by type
                     if let Ok(n) = id_data.clone().parse::<i64>() { 
                            bind_params_fk.push(DbParam::I64(n)); 
                     } else { 
                            bind_params_fk.push(DbParam::Str(id_data.clone())); 
                     }

              } else if type_process == "UPDATE" {
                     s_sql_fk = format!(
                            "UPDATE {} SET {} = ?, updated_at = {}, updated_by_id = ? WHERE {} = ?",
                            data_table.table, fk.on_update_action.column, state.query_converter.datetime_now, fk.on_update_action.column
                     );
                     // isikan bind_params_fk id_new 
                     if let Ok(n) = id_new.clone().parse::<i64>() { 
                            bind_params_fk.push(DbParam::I64(n)); 
                     } else { 
                            bind_params_fk.push(DbParam::Str(id_new.clone())); 
                     }

                     // isikan bind_params_fk updated_by_id 
                     bind_params_fk.push(DbParam::I64(id_user));

                     // isikan bind_params_fk id lama 
                     if let Ok(n) = id_data.clone().parse::<i64>() { 
                            bind_params_fk.push(DbParam::I64(n)); 
                     } else { 
                            bind_params_fk.push(DbParam::Str(id_data.clone())); 
                     }

              } else {
                     continue; // skip if type_process is not DELETE or UPDATE
                     
              }


              log_output(
                     "FOREIGN KEY", type_process, 
                     "QUERY", 
                     s_sql_fk.clone(), 
                     true);
              log_output(
                     "FOREIGN KEY", type_process,
                     "PARAM",
                     format!("{:?}", bind_params_fk),
                     true,
              );

              match transaction.query_with_params(&s_sql_fk, bind_params_fk).await {
                     Ok(_) => (),
                     Err(err) => {
                            log_output("ERROR", type_process, "FOREIGN KEY", err.to_string(), false);
                            status_executed = false;
                            error_message = format!("Foreign key delete error: {}", err);
                            break;
                     },
              }
       }

       (status_executed, error_message)
}