use actix_web::web::Data;
use serde_json::Value;

use crate::{
    database::state::{AppState, DbParam, DbTransaction},
    log::log_output,
    model::ReferenceForeignKey,
};

// create function to post delete or update table with foreign key constraints
pub(crate) async fn process_foreign_keys_delete_update(
    type_process: &str, // "DELETE" or "UPDATE"
    state: Data<AppState>,
    transaction: &mut Box<dyn DbTransaction>,
    reference_foreign_keys: &[ReferenceForeignKey],
    id_user: String,
    id_data: String,
    id_new: String, // for UPDATE
) -> (bool, String) {
    let mut status_executed = true;
    let mut error_message = String::new();
    let mut s_sql_fk;

    for fk in reference_foreign_keys.iter() {
        let mut bind_params_fk: Vec<DbParam> = Vec::new();

        if type_process == "DELETE" {
            let data_table = fk.on_delete_action.clone();
            if data_table.action == "cascade" {
                if data_table.type_delete == "soft" {
                    s_sql_fk = format!(
                        "UPDATE {} SET deleted_at = {}, deleted_by_id = ? WHERE {} = ?",
                        data_table.table, state.query_converter.datetime_now, data_table.column
                    );
                    bind_params_fk.push(DbParam::Str(id_user.clone()));
                } else if data_table.type_delete == "hard" {
                    // create query DELETE sql parameterized by id
                    s_sql_fk = format!(
                        "DELETE FROM {} WHERE {} = ?",
                        data_table.table, data_table.column
                    );
                } else {
                    continue; // skip if type_delete is not soft or hard
                }

                // Bind id by type
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }
                let _ = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
            } else if data_table.action == "set null" {
                s_sql_fk = format!(
                    "UPDATE {} SET {} = NULL, updated_at = {}, updated_by_id = ? WHERE {} = ?",
                    data_table.table,
                    data_table.column,
                    state.query_converter.datetime_now,
                    data_table.column
                );
                // isikan bind_params_fk updated_by_id
                bind_params_fk.push(DbParam::Str(id_user.clone()));
                // isikan bind_params_fk id lama
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }
                let _ = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
            } else if data_table.action == "restrict" {
                // create query check table
                s_sql_fk = format!(
                    "SELECT COUNT(*) FROM {} WHERE {} = ?",
                    data_table.table, data_table.column
                );
                // isikan bind_params_fk id lama
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }
                let result = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
                match result {
                    Ok(rows) => {
                        if !rows.is_empty() {
                            if let Some(count) = rows[0].get(0) {
                                if count.as_i64().unwrap_or(0) > 0 {
                                    status_executed = false;
                                    error_message = format!(
                                        "Cannot delete data because it is referenced in table {}",
                                        data_table.table
                                    );
                                    break; // exit the loop and return
                                }
                            }
                        }
                    }
                    Err(err) => {
                        status_executed = false;
                        error_message = err.to_string();
                        break; // exit the loop and return
                    }
                }
            } else {
                continue; // skip if action is not cascade or set null
            }
        } else if type_process == "UPDATE" {
            let data_table = fk.on_update_action.clone();
            if data_table.action == "cascade" {
                s_sql_fk = format!(
                    "UPDATE {} SET {} = ?, updated_at = {}, updated_by_id = ? WHERE {} = ?",
                    data_table.table,
                    data_table.column,
                    state.query_converter.datetime_now,
                    data_table.column
                );
                // isikan bind_params_fk id_new
                if let Ok(n) = id_new.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_new.clone()));
                }

                // isikan bind_params_fk updated_by_id
                bind_params_fk.push(DbParam::Str(id_user.clone()));

                // isikan bind_params_fk id lama
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }

                let _ = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
            } else if data_table.action == "set null" {
                let data_table = fk.on_update_action.clone();
                s_sql_fk = format!(
                    "UPDATE {} SET {} = NULL, updated_at = {}, updated_by_id = ? WHERE {} = ?",
                    data_table.table,
                    data_table.column,
                    state.query_converter.datetime_now,
                    data_table.column
                );
                // isikan bind_params_fk updated_by_id
                bind_params_fk.push(DbParam::Str(id_user.clone()));
                // isikan bind_params_fk id lama
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }
                let _ = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
            } else if data_table.action == "restrict" {
                s_sql_fk = format!(
                    "SELECT COUNT(*) FROM {} WHERE {} = ?",
                    data_table.table, data_table.column
                );
                // isikan bind_params_fk id lama
                if let Ok(n) = id_data.clone().parse::<i64>() {
                    bind_params_fk.push(DbParam::I64(n));
                } else {
                    bind_params_fk.push(DbParam::Str(id_data.clone()));
                }
                let result = execute(transaction, &s_sql_fk, bind_params_fk, type_process).await;
                match result {
                    Ok(rows) => {
                        if !rows.is_empty() {
                            if let Some(count) = rows[0].get(0) {
                                if count.as_i64().unwrap_or(0) > 0 {
                                    status_executed = false;
                                    error_message = format!(
                                        "Cannot delete data because it is referenced in table {}",
                                        data_table.table
                                    );
                                    break; // exit the loop and return
                                }
                            }
                        }
                    }
                    Err(err) => {
                        status_executed = false;
                        error_message = err.to_string();
                        break; // exit the loop and return
                    }
                }
            } else {
                continue; // skip if action is not cascade or set null
            }
        } else {
            continue; // skip if type_process is not DELETE or UPDATE
        }
    }

    (status_executed, error_message)
}

async fn execute(
    transaction: &mut Box<dyn DbTransaction>,
    s_sql_fk: &str,
    bind_params_fk: Vec<DbParam>,
    type_process: &str,
) -> Result<Vec<Value>, anyhow::Error> {
    log_output(
        "FOREIGN KEY",
        type_process,
        "QUERY",
        s_sql_fk.to_string(),
        true,
    );
    log_output(
        "FOREIGN KEY",
        type_process,
        "PARAM",
        format!("{:?}", bind_params_fk),
        true,
    );

    match transaction
        .query_with_params(s_sql_fk, bind_params_fk)
        .await
    {
        Ok(rows) => Ok(rows),
        Err(err) => Err(err),
    }
}

// create function to check master if column foreign key
pub(crate) async fn check_data_foreign_key(
    state: &Data<AppState>,
    reference_table: String,
    reference_column: String,
    id_data: String,
) -> bool {
    // query to table
    let s_query = format!(
        "SELECT 1 FROM {} WHERE {} = ?",
        reference_table, reference_column
    );
    let mut bind_params_fk: Vec<DbParam> = Vec::new();

    if let Ok(n) = id_data.clone().parse::<i64>() {
        bind_params_fk.push(DbParam::I64(n));
    } else {
        bind_params_fk.push(DbParam::Str(id_data.clone()));
    }

    match state.db.query_with_params(&s_query, bind_params_fk).await {
        Ok(rows) => !rows.is_empty(),
        Err(err) => {
            log_output(
                "ERROR",
                "CHECK FOREIGN KEY",
                "QUERY",
                err.to_string(),
                false,
            );
            false
        }
    }
}
