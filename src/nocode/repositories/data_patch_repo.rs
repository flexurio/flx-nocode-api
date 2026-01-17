use actix_web::web;
use serde_json::Value;

use crate::AppState;
use crate::model::TableSchema;
use crate::database::state::DbParam;
use crate::storage::sql_store::SqlStore;
use crate::log::log_output;

#[allow(clippy::too_many_arguments)]
pub async fn execute_procedure(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    route: &str,
    bind_params: Vec<DbParam>,
    param_count: usize,
) -> Result<(Vec<Value>, usize), String> {
    // MongoDB check
    if state.db_type == crate::model::DbType::Mongodb {
         return Err("PATCH procedure execution is not supported for MongoDB".to_string());
    }

    // Use SqlStore to compile dialect-aware procedure call
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    
    let (s_sql, compiled_params) = match ds.preview_call_procedure(&table_schema.patch.pre_process_sp, param_count, bind_params) {
        Ok((sql, params)) => {
            if *crate::ISDEBUG {
                log_output("QUERY", "PATCH(CALL)", route, sql.clone(), true);
                log_output("PARAMS", "PATCH(CALL)", route, format!("{:?}", params), true);
            }
            (sql, params)
        }
        Err(e) => return Err(format!("Unsupported or invalid procedure call: {}", e)),
    };

    // Transaction
    let mut tx = state.store.begin_tx().await.map_err(|e| format!("Error starting transaction: {}", e))?;

    match tx.raw_sql(&s_sql, compiled_params).await {
         Ok(rows) => {
             if let Err(e) = tx.commit().await {
                 return Err(format!("Error committing transaction: {}", e));
             }
             let count = rows.len();
             Ok((rows, count))
         }
         Err(e) => {
             let _ = tx.rollback().await;
             Err(format!("Error NCO-PATCH: {}", e))
         }
    }
}
