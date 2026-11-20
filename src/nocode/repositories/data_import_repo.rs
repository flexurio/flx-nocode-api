use crate::model::{TableSchema, DbType};
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::storage::ast::{Query as Q, Filter as F};
use crate::AppState;

// Max ID calculation for Mongo or SQL backend
pub async fn calculate_max_id(
    state: &AppState,
    table_schema: &TableSchema,
    prefix: &str,
) -> String {
    if state.db_type == DbType::Mongodb {
        // Use AST aggregation for Mongo: MAX(id) with prefix% (case-insensitive implied by prefix often)
        use crate::storage::ast::Query as QQ;
        let qmax = QQ::from(table_schema.table.clone())
            .agg_max("max_id", "id")
            .r#where(F::ILike("id".into(), format!("{}%", prefix)))
            .limit(1);
        match state.store.query(&qmax).await {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("max_id")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "0".to_string()),
            _ => "0".to_string(),
        }
    } else {
        // SQL path: allow COALESCE(MAX(id), 0) projection
        let id_find = prefix;
        // Construct query to find max ID with specific prefix
        let q = Q::from(table_schema.table.clone())
            .select(["COALESCE(MAX(id), 0) as max_id"])
            .r#where(F::Like("id".into(), format!("%{}%", id_find)));
            
        match state.store.query(&q).await {
            Ok(rows) if !rows.is_empty() => {
                let v = rows[0].get("max_id");
                if let Some(s) = v.and_then(|x| x.as_str()) { s.to_string() }
                else if let Some(n) = v.and_then(|x| x.as_i64()) { n.to_string() }
                else if let Some(f) = v.and_then(|x| x.as_f64()) { f.to_string() }
                else { "0".to_string() }
            }
            _ => "0".to_string(),
        }
    }
}

/// Insert rows one by one and return each row's generated primary key
/// (`RETURNING` / `OUTPUT INSERTED` / MySQL `LAST_INSERT_ID()`). Used instead of
/// [`perform_bulk_insert_sql`] when the CPOB audit trail needs the record id of
/// rows whose PK is assigned by the database.
pub async fn perform_insert_rows_returning_pk(
    state: &AppState,
    tx: &mut Box<dyn crate::storage::traits::TxStore>,
    table_schema: &TableSchema,
    col_names: &[String],
    bulk_rows: Vec<Vec<InsertValue>>,
    pk_cols: &[String],
) -> Result<Vec<Option<serde_json::Map<String, serde_json::Value>>>, String> {
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let returning: Vec<&str> = pk_cols.iter().map(|s| s.as_str()).collect();
    let mut keys = Vec::with_capacity(bulk_rows.len());
    for row in bulk_rows {
        let fields: Vec<(String, InsertValue)> = col_names.iter().cloned().zip(row).collect();
        let (sql, params) = ds
            .preview_insert_with_returning(&table_schema.table, &fields, &returning)
            .map_err(|e| format!("Insert compile error: {}", e))?;
        let mut returned = tx.raw_sql(&sql, params).await.map_err(|e| format!("Insert error: {}", e))?;
        let mut key = returned
            .drain(..)
            .next()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        key.retain(|k, _| pk_cols.iter().any(|p| p == k));
        if key.is_empty()
            && pk_cols.len() == 1
            && state.db_type == crate::model::DbType::Mysql
            && let Ok(id_rows) = tx.raw_sql("SELECT LAST_INSERT_ID() AS id", vec![]).await
            && let Some(id) = id_rows.first().and_then(|r| r.get("id")).cloned()
        {
            key.insert(pk_cols[0].clone(), id);
        }
        keys.push(if key.len() == pk_cols.len() { Some(key) } else { None });
    }
    Ok(keys)
}

pub async fn perform_bulk_insert_sql(
    state: &AppState,
    tx: &mut Box<dyn crate::storage::traits::TxStore>,
    table_schema: &TableSchema,
    col_names: &[String],
    bulk_rows: Vec<Vec<InsertValue>>,
) -> Result<(), String> {
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let (sql, params) = ds.preview_insert_bulk(&table_schema.table, col_names, &bulk_rows)
        .map_err(|e| format!("Bulk compile error: {}", e))?;

    if *crate::ISDEBUG {
        crate::log::log_output("QUERY", "IMPORT-BULK", &table_schema.table, sql.clone(), true);
        crate::log::log_output("PARAMS", "IMPORT-BULK", &table_schema.table, format!("{} params", params.len()), true);
    }

    tx.raw_sql(&sql, params).await.map_err(|e| format!("Bulk insert error: {}", e))?;
    Ok(())
}
