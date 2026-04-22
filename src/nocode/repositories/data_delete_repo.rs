use actix_web::web;
use crate::AppState;
use crate::model::{TableSchema, ReferenceForeignKey};
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::log::log_output;
use crate::nocode::foreign_key::process_foreign_keys_delete_update_txstore;
use crate::nocode::pk_utils::{build_pk_filter, dbparam_from_str_and_type, json_value_from_str_and_type};
use chrono::Local;
use serde_json::Value;

#[allow(clippy::too_many_arguments)]
pub async fn perform_delete_sql(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    route: &str,
    id_raw: &str,
    pk_values: &[String],
    ref_fks: &[ReferenceForeignKey],
    actor_id: &str,
    is_soft: bool,
) -> Result<(), String> {
    
    let (exec_sql, exec_params) = if is_soft {
        // Decide types for deleted_by_id
        let deleted_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "deleted_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
        log_output("TYPE", "deleted_by_id", route, deleted_by_type.clone(), true);

        // Build fields with a raw DB now() expression
        let mut fields: Vec<(String, InsertValue)> = vec![
            ("deleted_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())),
        ];
        
        // Typed deleted_by_id
        fields.push((
            "deleted_by_id".into(),
            InsertValue::Param(dbparam_from_str_and_type(actor_id, &deleted_by_type)),
        ));

        let filter = build_pk_filter(&table_schema.primary_key.columns, pk_values)
            .map_err(|e| format!("Error building PK filter: {}", e))?;
            
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        match ds.preview_update_with(&table_schema.table, Some(&filter), &fields) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-soft)", route, sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-soft)", route, format!("{:?}", params), true);
                }
                (sql, params)
            }
            Err(e) => return Err(format!("Error compiling AST soft delete: {}", e)),
        }
    } else {
        // Hard delete
        let filter = build_pk_filter(&table_schema.primary_key.columns, pk_values)
            .map_err(|e| format!("Error building PK filter: {}", e))?;
            
        let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        match ds.preview_delete(&table_schema.table, Some(&filter)) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "DELETE(AST-hard)", route, sql.clone(), true);
                    log_output("PARAMS", "DELETE(AST-hard)", route, format!("{:?}", params), true);
                }
                (sql, params)
            }
            Err(e) => return Err(format!("Error compiling AST hard delete: {}", e)),
        }
    };
    log_output("QUERY", "DELETE(AST)", route, exec_sql.clone(), true);

    // Build a body for SQL formula interpolation in pre/post process
    let body: Value = {
        let mut m = serde_json::Map::new();
        for (col, val) in table_schema.primary_key.columns.iter().zip(pk_values.iter()) {
            m.insert(col.clone(), Value::String(val.clone()));
        }
        if !m.contains_key("id") {
            m.insert("id".to_string(), Value::String(id_raw.to_string()));
        }
        Value::Object(m)
    };

    // Transaction
    let mut tx = state.store.begin_tx().await.map_err(|e| format!("Error starting transaction: {}", e))?;

    // PRE-PROCESS
    if table_schema.del.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(&mut tx, table_schema.del.pre_process.clone(), &body, route).await {
            let _ = tx.rollback().await;
            return Err(format!("Error in pre-process: {}", err));
        }
    }

    match tx.raw_sql(&exec_sql, exec_params).await {
        Ok(_) => {
            let (is_fk_ok, err_message) = process_foreign_keys_delete_update_txstore(
                "DELETE", 
                state.clone(),
                route.to_string(),
                &mut tx,
                ref_fks,
                actor_id.to_string(),
                id_raw.to_string(),
                "".to_string(), // for UPDATE
            ).await;

            if is_fk_ok {
                // POST-PROCESS
                if table_schema.del.post_process.contains("SQL:") {
                    if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(&mut tx, table_schema.del.post_process.clone(), &body, route).await {
                        let _ = tx.rollback().await;
                        return Err(format!("Error in post-process: {}", err));
                    }
                }
                tx.commit().await.map_err(|e| format!("Error committing transaction: {}", e))?;
                Ok(())
            } else {
                let _ = tx.rollback().await;
                Err(format!("Transaction rolled back due to foreign key failures: {}", err_message))
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            Err(format!("Error NCO-DELETE: {}", err))
        }
    }
}

pub async fn perform_delete_mongo(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    _route: &str, // route used for logging/logic if needed
    _id_raw: &str,
    pk_values: &[String],
    actor_id: &str,
    is_soft: bool,
) -> Result<(), String> {
    
    let filter = build_pk_filter(&table_schema.primary_key.columns, pk_values)
        .map_err(|e| format!("Error building PK filter: {}", e))
        .map(Some)?;

    if is_soft {
        let mut patch = serde_json::Map::new();
        patch.insert("deleted_at".into(), serde_json::json!(Local::now().to_rfc3339()));
        
        let deleted_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "deleted_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
            
        patch.insert(
            "deleted_by_id".into(),
            json_value_from_str_and_type(actor_id, &deleted_by_type),
        );
        
        state.store.update(&table_schema.table, filter, Value::Object(patch)).await
            .map(|_| ())
            .map_err(|e| format!("Error NCO-DELETE (mongo): {}", e))
    } else {
        state.store.delete(&table_schema.table, filter).await
            .map(|_| ())
            .map_err(|e| format!("Error NCO-DELETE (mongo): {}", e))
    }
}

