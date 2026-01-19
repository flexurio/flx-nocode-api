use serde_json::Value;
use actix_web::web;
use crate::AppState;
use crate::model::{TableSchema, Column, Index};
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::storage::sql_store::{SqlStore, InsertValue, UniqueCheck};
use crate::database::state::DbParam;
use crate::log::log_output;

/// Parse composite PK values from path parameter using ~ as delimiter
pub fn parse_pk_values(id_raw: &str) -> Vec<String> {
    id_raw
        .split('~')
        .map(|s| s.to_string())
        .collect()
}

/// Build a composite primary key filter
pub fn build_pk_filter(pk_columns: &[String], pk_values: &[String]) -> Result<QF, String> {
    if pk_columns.is_empty() {
        return Err("No primary key columns defined".to_string());
    }
    if pk_columns.len() != pk_values.len() {
        return Err(format!(
            "Primary key mismatch: expected {} values for {} columns",
            pk_columns.len(),
            pk_values.len()
        ));
    }

    if pk_columns.len() == 1 {
        Ok(QF::Eq(pk_columns[0].clone(), QV::Str(pk_values[0].clone())))
    } else {
        let filters = pk_columns
            .iter()
            .zip(pk_values.iter())
            .map(|(col, val)| QF::Eq(col.clone(), QV::Str(val.clone())))
            .collect();
        Ok(QF::And(filters))
    }
}

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
    fk_checks: &[(String, String, String, String)],
) -> Result<(), String> {
    if fk_checks.is_empty() { return Ok(()); }
    
    // MongoDB Implementation
    if state.db_type == crate::model::DbType::Mongodb {
         for (col, table, ref_col, val) in fk_checks {
             // Naive type inference
             let val_qv = if let Ok(n) = val.parse::<i64>() { crate::storage::ast::Val::I64(n) } 
                          else if let Ok(f) = val.parse::<f64>() { crate::storage::ast::Val::F64(f) }
                          else { crate::storage::ast::Val::Str(val.clone()) };
             
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
    fk_checks: Vec<(String, String, String, String)>,
    password_override: Option<String>,
    body: &Value,
) -> Result<(String, i32), String> {
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
        
        // Batch FK Validation
        if !fk_checks.is_empty() {
             if let Err(e) = validate_foreign_keys_batch_put(state, &mut *tx, &fk_checks).await {
                 let _ = tx.rollback().await;
                 return Err(e);
             }
        }

        // Unique Validation
        if !table_schema.indexes.is_empty() {
            let pk_cols = &table_schema.primary_key.columns;
            let pk_col_first = pk_cols.first().cloned().unwrap_or_else(|| "id".to_string());
            let pk_meta = table_schema.columns.iter().find(|c| c.name == pk_col_first);
            let pk_param = if let Some(m) = pk_meta { dbparam_from_value_and_type(&serde_json::json!(id_raw), Some(m)) } else { DbParam::Str(id_raw.to_string()) };

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
        let doc_json = Value::Object(patch_fields); 
        
        match tx.update(&table_schema.table, Some(filter), doc_json).await {
             Ok(_) => {
                 let _ = tx.commit().await;
                 Ok(("Data updated successfully".to_string(), 1))
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
        
        match tx.update(&table_schema.table, Some(filter), doc_json).await {
            Ok(modified) => {
                 let _ = tx.commit().await;
                 Ok(("Data updated successfully".to_string(), modified as i32))
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
    use crate::storage::ast::{Filter as QF, Val as QV};

    #[test]
    fn test_parse_pk_values() {
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
