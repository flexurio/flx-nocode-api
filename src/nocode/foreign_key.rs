use actix_web::web::Data;
use serde_json::Value;

use crate::{
    database::state::{AppState, DbParam, DbTransaction},
    log::log_output,
    model::ReferenceForeignKey,
    storage::sql_store::SqlStore, // Restore SqlStore import
};
use crate::storage::sql_store::InsertValue as IV;
use crate::storage::ast::{Query as Q, Filter as F, Val as V};


// TxStore variant to support new generic transaction path
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_foreign_keys_delete_update_txstore(
    type_process: &str, // "DELETE" or "UPDATE"
    state: Data<AppState>,
    route: String,
    tx: &mut Box<dyn crate::storage::traits::TxStore>,
    reference_foreign_keys: &[ReferenceForeignKey],
    id_user: String,
    id_data: String,
    id_new: String, // for UPDATE
) -> (bool, String) {
    let mut status_executed = true;
    let mut error_message = String::new();
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());

    for fk in reference_foreign_keys.iter() {
        if route == fk.table {
            if type_process == "DELETE" {
                let data_table = fk.on_delete_action.clone();
                if data_table.action == "cascade" {
                    if data_table.type_delete == "soft" {
                        let now_fn = state.query_converter.datetime_now.clone();
                        let mut fields: Vec<(String, IV)> = vec![("deleted_at".into(), IV::Raw(now_fn))];
                        if let Ok(n) = id_user.clone().parse::<i64>() { fields.push(("deleted_by_id".into(), IV::Param(DbParam::I64(n)))); }
                        else { fields.push(("deleted_by_id".into(), IV::Param(DbParam::Str(id_user.clone())))); }
                        let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                        match ds.preview_update_with(&data_table.table, Some(&filter), &fields) {
                            Ok((sql_u, params_u)) => {
                                let built = crate::database::state::rehydrate_placeholders(&sql_u, &state.db_type);
                                if let Err(err) = tx.raw_sql(&built, params_u).await { status_executed = false; error_message = err.to_string(); break; }
                            }
                            Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                        }
                    } else if data_table.type_delete == "hard" {
                        let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                        match ds.preview_delete(&data_table.table, Some(&filter)) {
                            Ok((sql_d, params_d)) => {
                                let built = crate::database::state::rehydrate_placeholders(&sql_d, &state.db_type);
                                if let Err(err) = tx.raw_sql(&built, params_d).await { status_executed = false; error_message = err.to_string(); break; }
                            }
                            Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                        }
                    } else { continue; }
                } else if data_table.action == "set null" {
                    let now_fn = state.query_converter.datetime_now.clone();
                    let mut fields: Vec<(String, IV)> = vec![(data_table.column.clone(), IV::Raw("NULL".into())), ("updated_at".into(), IV::Raw(now_fn))];
                    if let Ok(n) = id_user.clone().parse::<i64>() { fields.push(("updated_by_id".into(), IV::Param(DbParam::I64(n)))); }
                    else { fields.push(("updated_by_id".into(), IV::Param(DbParam::Str(id_user.clone())))); }
                    let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                    match ds.preview_update_with(&data_table.table, Some(&filter), &fields) {
                        Ok((sql_u, params_u)) => {
                            let built = crate::database::state::rehydrate_placeholders(&sql_u, &state.db_type);
                            if let Err(err) = tx.raw_sql(&built, params_u).await { status_executed = false; error_message = err.to_string(); break; }
                        }
                        Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                    }
                } else if data_table.action == "restrict" {
                    let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                    let q = Q::from(data_table.table.clone()).select(["1"]).r#where(filter).limit(1);
                    let (sql_q, params_q) = ds.preview_sql(&q);
                    let built = crate::database::state::rehydrate_placeholders(&sql_q, &state.db_type);
                    match tx.raw_sql(&built, params_q).await {
                        Ok(rows) => {
                            if !rows.is_empty() { status_executed = false; error_message = format!("Cannot delete data because it is referenced in table {}", data_table.table); break; }
                        }
                        Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                    }
                } else { continue; }
            } else if type_process == "UPDATE" {
                let data_table = fk.on_update_action.clone();
                if data_table.action == "cascade" {
                    let now_fn = state.query_converter.datetime_now.clone();
                    let mut fields: Vec<(String, IV)> = Vec::new();
                    if let Ok(n) = id_new.clone().parse::<i64>() { fields.push((data_table.column.clone(), IV::Param(DbParam::I64(n)))); } else { fields.push((data_table.column.clone(), IV::Param(DbParam::Str(id_new.clone())))); }
                    fields.push(("updated_at".into(), IV::Raw(now_fn)));
                    if let Ok(n) = id_user.clone().parse::<i64>() { fields.push(("updated_by_id".into(), IV::Param(DbParam::I64(n)))); } else { fields.push(("updated_by_id".into(), IV::Param(DbParam::Str(id_user.clone())))); }
                    let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                    match ds.preview_update_with(&data_table.table, Some(&filter), &fields) {
                        Ok((sql_u, params_u)) => {
                            let built = crate::database::state::rehydrate_placeholders(&sql_u, &state.db_type);
                            if let Err(err) = tx.raw_sql(&built, params_u).await { status_executed = false; error_message = err.to_string(); break; }
                        }
                        Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                    }
                } else if data_table.action == "set null" {
                    let now_fn = state.query_converter.datetime_now.clone();
                    let mut fields: Vec<(String, IV)> = vec![(data_table.column.clone(), IV::Raw("NULL".into())), ("updated_at".into(), IV::Raw(now_fn))];
                    if let Ok(n) = id_user.clone().parse::<i64>() { fields.push(("updated_by_id".into(), IV::Param(DbParam::I64(n)))); } else { fields.push(("updated_by_id".into(), IV::Param(DbParam::Str(id_user.clone())))); }
                    let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                    match ds.preview_update_with(&data_table.table, Some(&filter), &fields) {
                        Ok((sql_u, params_u)) => {
                            let built = crate::database::state::rehydrate_placeholders(&sql_u, &state.db_type);
                            if let Err(err) = tx.raw_sql(&built, params_u).await { status_executed = false; error_message = err.to_string(); break; }
                        }
                        Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                    }
                } else if data_table.action == "restrict" {
                    let filter = if let Ok(n) = id_data.clone().parse::<i64>() { F::Eq(data_table.column.clone(), V::I64(n)) } else { F::Eq(data_table.column.clone(), V::Str(id_data.clone())) };
                    let q = Q::from(data_table.table.clone()).select(["1"]).r#where(filter).limit(1);
                    let (sql_q, params_q) = ds.preview_sql(&q);
                    let built = crate::database::state::rehydrate_placeholders(&sql_q, &state.db_type);
                    match tx.raw_sql(&built, params_q).await {
                        Ok(rows) => {
                            if !rows.is_empty() { status_executed = false; error_message = format!("Cannot delete data because it is referenced in table {}", data_table.table); break; }
                        }
                        Err(err) => { status_executed = false; error_message = err.to_string(); break; }
                    }
                } else { continue; }
            } else { continue; }
        }
    }

    (status_executed, error_message)
}

#[allow(dead_code)]
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
    // portable AST query: SELECT 1 FROM ref WHERE col = ? LIMIT 1
    let val = if let Ok(n) = id_data.clone().parse::<i64>() { V::I64(n) } else { V::Str(id_data.clone()) };
    let q = Q::from(reference_table)
        .select(["1"]) // lighter projection
        .r#where(F::Eq(reference_column, val))
        .limit(1);
    match state.store.query(&q).await {
        Ok(rows) => !rows.is_empty(),
        Err(err) => {
            log_output("ERROR", "CHECK FOREIGN KEY", "QUERY", err.to_string(), false);
            false
        }
    }
}
