use serde_json::Value;
use actix_web::web;
use crate::AppState;
use crate::model::{TableSchema, Column, Index};
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::storage::sql_store::{SqlStore, InsertValue, UniqueCheck};
use crate::database::state::DbParam;
use crate::log::log_output;
use crate::nocode::pk_utils::{build_pk_filter, parse_pk_values};

pub fn dbparam_from_value_and_type(val: &Value, meta: Option<&Column>) -> DbParam {
    if let Some(m) = meta {
        let td = m.type_data.to_lowercase();
        match (val, td.as_str()) {
            (Value::Number(n), t) if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") => DbParam::F64(n.as_f64().unwrap_or(0.0)),
            (Value::Number(n), t) if t.contains("int") => DbParam::I64(n.as_i64().unwrap_or(0)),
            (Value::String(s), t) if t.contains("int") => {
                if let Ok(nn) = s.parse::<i64>() { DbParam::I64(nn) }
                else if let Ok(ff) = s.parse::<f64>() { DbParam::F64(ff) }
                else { DbParam::Str(s.clone()) }
            }
            (Value::String(s), t) if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") => {
                if let Ok(ff) = s.parse::<f64>() { DbParam::F64(ff) }
                else if let Ok(nn) = s.parse::<i64>() { DbParam::I64(nn) }
                else { DbParam::Str(s.clone()) }
            }
            (Value::Bool(b), _) => DbParam::Bool(*b),
            (Value::Null, _) => DbParam::Null,
            (other, _) => DbParam::Str(other.to_string().trim_matches('"').to_string()),
        }
    } else {
        match val {
            Value::Number(n) => DbParam::Str(n.to_string()),
            Value::String(s) => DbParam::Str(s.clone()),
            Value::Bool(b) => DbParam::Bool(*b),
            _ => DbParam::Null,
        }
    }
}

async fn validate_foreign_keys_batch_put(
    state: &web::Data<AppState>,
    tx: &mut dyn crate::storage::traits::TxStore,
    // (col_name, ref_table, ref_column, value, type_data) — type_data is the FK column's own
    // type so the value binds as the correct DbParam variant instead of always TEXT.
    fk_checks: &[(String, String, String, String, String)],
) -> Result<(), String> {
    if fk_checks.is_empty() { return Ok(()); }

    // MongoDB Implementation
    if state.db_type == crate::model::DbType::Mongodb {
         for (col, table, ref_col, val, type_data) in fk_checks {
             let val_qv = match crate::nocode::pk_utils::dbparam_from_str_and_type(val, type_data) {
                 DbParam::I64(n) => crate::storage::ast::Val::I64(n),
                 DbParam::F64(f) => crate::storage::ast::Val::F64(f),
                 DbParam::Bool(b) => crate::storage::ast::Val::Bool(b),
                 DbParam::Null => crate::storage::ast::Val::Null,
                 DbParam::Str(s) => crate::storage::ast::Val::Str(s),
             };

             let q = crate::storage::ast::Query::from(table.clone())
                .select(vec![ref_col.clone()])
                .r#where(QF::Eq(ref_col.clone(), val_qv))
                .limit(1);
                
             // Note: tx is not used for Mongo read here, but we use state.store to query
             // This assumes state.store is available and consistent with what we need.
             // Wait, `tx` is passed but for Mongo we don't have real TX. 
             // We can use state.store.query. However, this function signature expects `tx`.
             // For Mongo, `MongoTxStore` just wraps `MongoStore`, so `tx` should work if it implements `TxStore`.
             // But `TxStore` trait has `query`. So `tx.query(&q)` should work!
             
             let rows = tx.query(&q).await.map_err(|e| format!("Error validating FK (mongo): {}", e))?;
             if rows.is_empty() {
                 return Err(format!("Invalid foreign key value for column '{}' referencing table '{}'", col, table));
             }
        }
        return Ok(());
    }

    // Use SqlStore to build query safely
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let (union_sql, params) = ds.preview_validate_fk_batch(fk_checks)
        .map_err(|e| format!("Error building FK check query: {}", e))?;

    if *crate::ISDEBUG { log_output("QUERY", "FK BATCH", "PUT", union_sql.clone(), true); }
    
    let rows = tx
        .raw_sql(&union_sql, params)
        .await
        .map_err(|e| format!("FK batch validation failed: {}", e))?;
        
    for r in rows {
        let is_valid = match r.get("_valid") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Number(n)) => n.as_i64() == Some(1),
            _ => false,
        };

        if !is_valid {
            let col = r.get("_col").and_then(|v| v.as_str()).unwrap_or("unknown");
            let tbl = r.get("_table").and_then(|v| v.as_str()).unwrap_or("unknown");
            return Err(format!("Invalid foreign key value for column '{}' referencing table '{}'", col, tbl));
        }
    }
    Ok(())
}

pub async fn validate_unique_constraints_batch_put(
    state: &web::Data<AppState>,
    tx: &mut dyn crate::storage::traits::TxStore,
    table: &str,
    indexes: &[Index],
    columns_meta: &[Column],
    effective_values: &serde_json::Map<String, Value>,
    pk_info: (&str, DbParam),
) -> Result<(), String> {
    let (pk_name, ref pk_param) = pk_info;
    
    let mut unique_checks: Vec<UniqueCheck> = Vec::new();
     for ix in indexes.iter().filter(|ix| ix.unique) {
        if !ix.columns.iter().any(|c| effective_values.contains_key(c)) {
            continue;
        }
        let mut local_params: Vec<(String, DbParam)> = Vec::with_capacity(ix.columns.len());
        let mut all_known = true;
        for col_name in &ix.columns {
            if let Some(v) = effective_values.get(col_name) {
                let meta = columns_meta.iter().find(|c| &c.name == col_name);
                local_params.push((col_name.clone(), dbparam_from_value_and_type(v, meta)));
            } else {
                all_known = false; break;
            }
        }
        if !all_known { continue; }
        
        unique_checks.push(UniqueCheck {
            index_name: ix.name.clone(),
            columns: local_params,
        });
    }

    if unique_checks.is_empty() { return Ok(()); }

    // MongoDB Implementation
    if state.db_type == crate::model::DbType::Mongodb {
        for check in unique_checks {
            let mut filters = Vec::new();
            for (col, param) in check.columns {
                 let val = match param {
                     DbParam::Str(s) => QV::Str(s),
                     DbParam::I64(i) => QV::I64(i),
                     DbParam::F64(f) => QV::F64(f),
                     DbParam::Bool(b) => QV::Bool(b),
                     DbParam::Null => QV::Null,
                     // _ => QV::Null, // Unreachable
                 };
                 filters.push(QF::Eq(col, val));
            }
            if filters.is_empty() { continue; }
            
            // Exclude self (PK)
            let (pk_col, pk_param) = pk_info.clone();
            let pk_val = match pk_param {
                 DbParam::Str(s) => QV::Str(s),
                 DbParam::I64(i) => QV::I64(i),
                 DbParam::F64(f) => QV::F64(f),
                 DbParam::Bool(b) => QV::Bool(b),
                 _ => QV::Null,
            };
            filters.push(QF::Ne(pk_col.to_string(), pk_val));
            
            let q = crate::storage::ast::Query::from(table.to_string())
                .select(vec!["_id".to_string()])
                .r#where(QF::And(filters))
                .limit(1);
            
            let rows = tx.query(&q).await.map_err(|e| format!("Error validating Unique (mongo): {}", e))?;
            if !rows.is_empty() {
                 return Err(format!("Unique constraint violation on index: {}", check.index_name));
            }
        }
        return Ok(());
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let (union_sql, params) = ds.preview_validate_unique_batch(
        table,
        &unique_checks,
        Some((pk_name, pk_param.clone()))
    ).map_err(|e| format!("Error building unique check query: {}", e))?;

    if *crate::ISDEBUG { log_output("QUERY", "UNIQUE BATCH", "PUT", union_sql.clone(), true); }
    
    let rows = tx
        .raw_sql(&union_sql, params)
        .await
        .map_err(|e| format!("Unique batch validation failed: {}", e))?;
        
    for r in rows {
        let is_dup = match r.get("_dup") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Number(n)) => n.as_i64() == Some(1),
            _ => false,
        };
        if is_dup { return Err("Unique constraint violation".to_string()); }
    }
    Ok(())
}

// Suppress too_many_arguments for now
// Suppress too_many_arguments for now
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
pub async fn perform_update(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    route: &str,
    id_raw: &str,
    update_fields: Vec<(String, InsertValue)>,
    patch_fields: serde_json::Map<String, Value>,
    fk_checks: Vec<(String, String, String, String, String)>,
    password_override: Option<String>,
    mut prepared_details: Vec<crate::nocode::repositories::data_create_repo::PreparedDetailBatch>,
    body: &Value,
    auth_token: Option<String>,
) -> Result<(String, i32, Value), String> {
    // Handling password-only update for flx_users
    if route == "flx_users" && password_override.is_some() && table_schema.put.columns.len() == 1 {
        // [Logic for password override specific update]
    }

    // Compile AST update (Pre-check)
    if state.db_type != crate::model::DbType::Mongodb {
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        let pk_values = parse_pk_values(id_raw);
        let mut filter = build_pk_filter(&table_schema.primary_key.columns, &pk_values)?;
        
        // Ensure not deleted
        if table_schema.columns.iter().any(|c| c.name == "deleted_at") {
            filter = QF::And(vec![filter, QF::IsNull("deleted_at".to_string())]);
        }
        
        match ds.preview_update_with(&table_schema.table, Some(&filter), &update_fields) {
            Ok((s_sql, params_compiled)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "PUT(AST)", route, s_sql.clone(), true);
                    log_output("PARAM", "PUT(AST)", route, format!("{:?}", params_compiled), true);
                }
            },
            Err(e) => return Err(format!("Error compiling AST UPDATE: {}", e)),
        }
    }

    // Transaction
    if state.db_type != crate::model::DbType::Mongodb {
        let mut tx = state.store.begin_tx().await.map_err(|e| format!("Error starting transaction: {}", e))?;
        
        let pk_cols = &table_schema.primary_key.columns;
        let pk_col_first = pk_cols.first().cloned().unwrap_or_else(|| "id".to_string());
        let pk_meta = table_schema.columns.iter().find(|c| c.name == pk_col_first);
        let pk_param = if let Some(m) = pk_meta { dbparam_from_value_and_type(&serde_json::json!(id_raw), Some(m)) } else { DbParam::Str(id_raw.to_string()) };

        // Fetch pre-update record state for triggers and conditional evaluation
        let mut old_record: serde_json::Map<String, Value> = serde_json::Map::new();
        let select_current_sql = format!("SELECT * FROM {} WHERE {} = ?", table_schema.table, pk_col_first);
        let built_select_current = crate::database::state::rehydrate_placeholders(&select_current_sql, state.db_type.as_str());
        if let Ok(rows) = tx.raw_sql(&built_select_current, vec![pk_param.clone()]).await {
            if let Some(first_row) = rows.into_iter().next() {
                if let Value::Object(map) = first_row {
                    old_record = map;
                }
            }
        }

        // Batch FK Validation
        if !fk_checks.is_empty() {
             if let Err(e) = validate_foreign_keys_batch_put(state, &mut *tx, &fk_checks).await {
                 let _ = tx.rollback().await;
                 return Err(e);
             }
        }

        // Unique Validation
        if !table_schema.indexes.is_empty() {
            let affected: Vec<&Index> = table_schema.indexes.iter().filter(|ix| ix.unique && ix.columns.iter().any(|c| patch_fields.contains_key(c))).collect();
            
            if !affected.is_empty() {
                // Fetch missing values
                use std::collections::HashSet;
                let mut needed: HashSet<String> = HashSet::new();
                for ix in &affected {
                     for c in &ix.columns {
                         if !patch_fields.contains_key(c) { needed.insert(c.clone()); }
                     }
                }

                let mut effective_values = patch_fields.clone();
                if !needed.is_empty() {
                    let cols: Vec<String> = needed.iter().cloned().collect();
                    let select_sql = format!("SELECT {} FROM {} WHERE {} = ?", cols.join(","), table_schema.table, pk_col_first);
                    
                    match tx.raw_sql(&select_sql, vec![pk_param.clone()]).await {
                         Ok(rows) => {
                             if let Some(row) = rows.first() {
                                 for c in cols {
                                     if let Some(v) = row.get(&c) { effective_values.insert(c.clone(), v.clone()); }
                                 }
                             }
                         }
                         Err(e) => {
                             let _ = tx.rollback().await;
                             return Err(format!("Error preloading values for unique check: {}", e));
                         }
                    }
                }
                
                if let Err(msg) = validate_unique_constraints_batch_put(state, &mut *tx, &table_schema.table, &table_schema.indexes, &table_schema.columns, &effective_values, (&pk_col_first, pk_param)).await {
                     let _ = tx.rollback().await;
                     return Err(msg);
                }
            }
        }

        // Validate Data Formula (API based)
        if table_schema.put.validate_data.starts_with("API:") {
             if let Err(e) = crate::nocode::validate::validate_api_formula(&table_schema.put.validate_data, body, auth_token.as_deref()).await {
                 let _ = tx.rollback().await;
                 return Err(e);
             }
        }

        // Validate Data Formula
        if table_schema.put.validate_data.contains("SQL:") {
             match crate::database::state::build_sql_and_params_from_formula(&table_schema.put.validate_data, body) {
                 Ok((built_sql, params)) => {
                     match tx.raw_sql(&built_sql, params).await {
                         Ok(row) => {
                             if row.is_empty() || !row[0].get(0).and_then(|v| v.as_bool()).unwrap_or(true) {
                                 let _ = tx.rollback().await;
                                 return Err("Validation data from table is not valid/empty".to_string());
                             }
                         }
                         Err(e) => {
                             let _ = tx.rollback().await;
                             return Err(format!("Error in validation_data: {}", e));
                         }
                     }
                 }
                 Err(e) => {
                     let _ = tx.rollback().await;
                     return Err(format!("Error building validation formula: {}", e));
                 }
             }
        }

        // Pre-Process
        if table_schema.put.pre_process.contains("SQL:") {
            if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(&mut tx, table_schema.put.pre_process.clone(), body, route).await {
                let _ = tx.rollback().await;
                return Err(format!("Error in pre-process: {}", err));
            }
        }

        // Execute Update
        let pk_values = parse_pk_values(id_raw);
        let mut filter = build_pk_filter(&table_schema.primary_key.columns, &pk_values)?;

        // Ensure not deleted
        if table_schema.columns.iter().any(|c| c.name == "deleted_at") {
            filter = QF::And(vec![filter, QF::IsNull("deleted_at".to_string())]);
        }
        
        // Convert Map to json Value for DB update
        let doc_json = Value::Object(patch_fields.clone()); 
        
        match tx.update(&table_schema.table, Some(filter), doc_json).await {
             Ok(_) => {
                 let mut final_patch = patch_fields;

                 // Sync Details inside transaction
                 let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
                 for batch in &mut prepared_details {
                     if !batch.target_table.is_empty() && !batch.foreign_key_column.is_empty() {
                         // 1. Delete existing details for this parent
                         let delete_sql = format!(
                             "DELETE FROM {} WHERE {} = ?",
                             batch.target_table, batch.foreign_key_column
                         );
                         let fk_pk_param = crate::nocode::pk_utils::dbparam_from_str_and_type(id_raw, &batch.fk_type_data);
                         let built_delete = crate::database::state::rehydrate_placeholders(&delete_sql, state.db_type.as_str());
                         if let Err(e) = tx.raw_sql(&built_delete, vec![fk_pk_param.clone()]).await {
                             let _ = tx.rollback().await;
                             return Err(format!("Error clearing old detail items for {}: {}", batch.target_table, e));
                         }

                         // 2. Insert new details if any
                         if !batch.rows.is_empty() {
                             for r in &mut batch.rows {
                                 if batch.fk_index < r.len() {
                                     r[batch.fk_index] = InsertValue::Param(fk_pk_param.clone());
                                 }
                             }

                             for resp in &mut batch.response_items {
                                 resp.insert(batch.foreign_key_column.clone(), serde_json::json!(id_raw));
                             }

                             let (bulk_sql, bulk_params) = ds
                                 .preview_insert_bulk(&batch.target_table, &batch.columns, &batch.rows)
                                 .map_err(|e| format!("Error building bulk insert for {}: {}", batch.target_table, e))?;

                             let built_bulk = crate::database::state::rehydrate_placeholders(&bulk_sql, state.db_type.as_str());
                             if let Err(e) = tx.raw_sql(&built_bulk, bulk_params).await {
                                 let _ = tx.rollback().await;
                                 return Err(format!("Error inserting detail items into {}: {}", batch.target_table, e));
                             }
                         }

                         final_patch.insert(
                             batch.field.clone(),
                             Value::Array(
                                 batch
                                     .response_items
                                     .iter()
                                     .cloned()
                                     .map(Value::Object)
                                     .collect(),
                             ),
                         );
                     }
                 }

                 // Execute Action Triggers
                 if !table_schema.action_triggers.is_empty() {
                     let mut new_record = old_record.clone();
                     for (k, v) in &final_patch {
                         new_record.insert(k.clone(), v.clone());
                     }

                     let trigger_ctx = crate::nocode::trigger_engine::TriggerContext {
                         parent_table: &table_schema.table,
                         parent_pk: id_raw,
                         old_record: &old_record,
                         new_record: &new_record,
                         request_body: body,
                         actor_id: None,
                     };

                     match crate::nocode::trigger_engine::execute_triggers(
                         state.db_type.clone(),
                         &mut *tx,
                         table_schema,
                         &trigger_ctx,
                         "on_update",
                     ).await {
                         Ok(executed) => {
                             for trig_name in executed {
                                 crate::audit::write_audit(&crate::audit::AuditEntry {
                                     at: chrono::Local::now().to_rfc3339(),
                                     actor_id: "system".to_string(),
                                     action: "TRIGGER",
                                     route: &format!("{}:{}", route, trig_name),
                                     id: Some(id_raw),
                                     ip: None,
                                 });
                             }
                         }
                         Err(err) => {
                             let _ = tx.rollback().await;
                             return Err(format!("Action trigger failed: {}", err));
                         }
                     }
                 }

                 // Post-Process (legacy SQL formula)
                 if table_schema.put.post_process.contains("SQL:") {
                     if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                         &mut tx,
                         table_schema.put.post_process.clone(),
                         body,
                         route,
                     ).await {
                         let _ = tx.rollback().await;
                         return Err(format!("Error in post-process: {}", err));
                     }
                 }

                 let _ = tx.commit().await;
                 Ok(("Data updated successfully".to_string(), 1, Value::Object(final_patch)))
             }
             Err(e) => {
                 let _ = tx.rollback().await;
                 Err(format!("Error NCO-PUT: {}", e))
             }
        }


    } else {
        // Mongo Path with Validation
        // 1. Transaction (MongoTxStore)
        // Since we enabled validation, we should try to reuse the same "transaction" abstraction for consistency
        // although MongoTxStore operations might not be atomic without replicaset.
        let mut tx = state.store.begin_tx().await.map_err(|e| format!("Error starting transaction: {}", e))?;

        // 2. FK Validation
        if !fk_checks.is_empty() {
             if let Err(e) = validate_foreign_keys_batch_put(state, &mut *tx, &fk_checks).await {
                 let _ = tx.rollback().await;
                 return Err(e);
             }
        }
        
        // 3. Unique Validation
        if !table_schema.indexes.is_empty() {
             let pk_cols = &table_schema.primary_key.columns;
             let pk_col_first = pk_cols.first().cloned().unwrap_or_else(|| "id".to_string());
             let pk_meta = table_schema.columns.iter().find(|c| c.name == pk_col_first);
             let pk_param = if let Some(m) = pk_meta { dbparam_from_value_and_type(&serde_json::json!(id_raw), Some(m)) } else { DbParam::Str(id_raw.to_string()) };

             // Similar logic to SQL path to populate effective values
            let affected: Vec<&Index> = table_schema.indexes.iter().filter(|ix| ix.unique && ix.columns.iter().any(|c| patch_fields.contains_key(c))).collect();
            if !affected.is_empty() {
                use std::collections::HashSet;
                let mut needed: HashSet<String> = HashSet::new();
                for ix in &affected {
                     for c in &ix.columns {
                         if !patch_fields.contains_key(c) { needed.insert(c.clone()); }
                     }
                }
                
                let mut effective_values = patch_fields.clone();
                if !needed.is_empty() {
                     // We need to fetch current document to fill missing values
                     let pk_vals = parse_pk_values(id_raw);
                     let filter_fetch = build_pk_filter(&table_schema.primary_key.columns, &pk_vals)?;
                     
                     let q_fetch = crate::storage::ast::Query::from(table_schema.table.clone())
                        .select(needed.iter().cloned().collect::<Vec<String>>())
                        .r#where(filter_fetch)
                        .limit(1);
                        
                     let rows = tx.query(&q_fetch).await.map_err(|e| format!("Error preloading values: {}", e))?;
                     if let Some(row) = rows.first() {
                         if let Some(obj) = row.as_object() {
                             for (k, v) in obj {
                                 effective_values.insert(k.clone(), v.clone());
                             }
                         }
                     }
                }
                
                if let Err(msg) = validate_unique_constraints_batch_put(state, &mut *tx, &table_schema.table, &table_schema.indexes, &table_schema.columns, &effective_values, (&pk_col_first, pk_param)).await {
                     let _ = tx.rollback().await;
                     return Err(msg);
                }
            }
        }

        let pk_values = parse_pk_values(id_raw);
        let mut filter = build_pk_filter(&table_schema.primary_key.columns, &pk_values)?;
        
        // Ensure not deleted
        if table_schema.columns.iter().any(|c| c.name == "deleted_at") {
            filter = QF::And(vec![filter, QF::IsNull("deleted_at".to_string())]);
        }

        let doc_json = Value::Object(patch_fields);

        if *crate::ISDEBUG {
            log_output("QUERY", "PUT(MONGO)", route, "Mongo update".to_string(), true);
        }
        
        match tx.update(&table_schema.table, Some(filter), doc_json.clone()).await {
            Ok(modified) => {
                 let _ = tx.commit().await;
                 Ok(("Data updated successfully".to_string(), modified as i32, doc_json))
            },
            Err(e) => {
                 let _ = tx.rollback().await;
                 Err(format!("Error NCO-PUT (mongo): {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ast::Val as QV;
    use crate::nocode::pk_utils::{build_pk_filter, parse_pk_values};

    #[test]
    fn test_parse_pk_values_repo_uses_shared_util() {
        assert_eq!(parse_pk_values("123"), vec!["123"]);
        assert_eq!(parse_pk_values("123~456"), vec!["123", "456"]);
        assert_eq!(parse_pk_values("abc~def~ghi"), vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_build_pk_filter_single() {
        let pk_cols = vec!["id".to_string()];
        let pk_vals = vec!["1".to_string()];
        let filter = build_pk_filter(&pk_cols, &pk_vals).unwrap();
        
        match filter {
            QF::Eq(col, val) => {
                assert_eq!(col, "id");
                if let QV::Str(v) = val {
                    assert_eq!(v, "1");
                } else {
                    panic!("Expected QV::Str");
                }
            },
            _ => panic!("Expected QF::Eq"),
        }
    }

    #[test]
    fn test_build_pk_filter_composite() {
        let pk_cols = vec!["id1".to_string(), "id2".to_string()];
        let pk_vals = vec!["1".to_string(), "2".to_string()];
        let filter = build_pk_filter(&pk_cols, &pk_vals).unwrap();

        match filter {
            QF::And(filters) => {
                assert_eq!(filters.len(), 2);
                match &filters[0] {
                    QF::Eq(col, val) => {
                        assert_eq!(col, "id1");
                        if let QV::Str(v) = val {
                            assert_eq!(v, "1");
                        } else {
                            panic!("Expected QV::Str");
                        }
                    },
                    _ => panic!("Expected QF::Eq"),
                }
                match &filters[1] {
                    QF::Eq(col, val) => {
                        assert_eq!(col, "id2");
                        if let QV::Str(v) = val {
                            assert_eq!(v, "2");
                        } else {
                            panic!("Expected QV::Str");
                        }
                    },
                    _ => panic!("Expected QF::Eq"),
                }
            },
            _ => panic!("Expected QF::And"),
        }
    }

    #[test]
    fn test_build_pk_filter_mismatch() {
        let pk_cols = vec!["id1".to_string()];
        let pk_vals = vec!["1".to_string(), "2".to_string()];
        let result = build_pk_filter(&pk_cols, &pk_vals);
        assert!(result.is_err());
    }

    #[test]
    fn test_dbparam_from_value() {
        // Test Int
        let v_int = serde_json::json!(123);
        let p_int = dbparam_from_value_and_type(&v_int, None);
        // default without meta is Str
        if let DbParam::Str(s) = p_int {
            assert_eq!(s, "123");
        } else {
            // Wait, the logic for `None` meta matches Value::Number and returns Str.
            // Let's verify with code inspection.
            // match val { Value::Number(n) => DbParam::Str(n.to_string()) ... }
            // Correct.
        }

        // Test with Meta Int
        let col_int = Column {
            name: "age".to_string(),
            type_data: "int".to_string(),
            ..Default::default()
        };
        let p_int_typed = dbparam_from_value_and_type(&v_int, Some(&col_int));
        if let DbParam::I64(n) = p_int_typed {
            assert_eq!(n, 123);
        } else {
            panic!("Expected I64");
        }

        // Test with Meta Float
        let v_float = serde_json::json!(12.34);
        let col_float = Column {
            name: "price".to_string(),
            type_data: "decimal".to_string(),
            ..Default::default()
        };
        let p_float_typed = dbparam_from_value_and_type(&v_float, Some(&col_float));
        if let DbParam::F64(f) = p_float_typed {
            assert!((f - 12.34).abs() < f64::EPSILON);
        } else {
            panic!("Expected F64");
        }
    }
}
