use serde_json::Value;
use actix_web::web;

use crate::AppState;
use crate::storage::ast::Query as QQ;
use crate::storage::sql_store::SqlStore;
use crate::log::log_output;

pub async fn execute_export_query(
    state: &web::Data<AppState>,
    query: &QQ,
    route: &str,
) -> Result<Vec<Value>, String> {
    // Preview for debug log
    if *crate::ISDEBUG && state.db_type != crate::model::DbType::Mongodb {
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        let (s_sql_dbg, params_dbg) = ds.preview_sql(query);
        log_output("QUERY", "EXPORT(AST)", route, s_sql_dbg.clone(), true);
        log_output("PARAMS", "EXPORT(AST)", route, format!("{:?}", params_dbg), true);
    }

    match state.store.query(query).await {
        Ok(res) => Ok(res),
        Err(e) => Err(format!("Error EXPORT(AST) query: {}", e)),
    }
}
