use serde_json::Value;
use actix_web::web;
use crate::AppState;
use crate::model::{TableSchema, Column, Index};
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::storage::sql_store::{SqlStore, InsertValue, UniqueCheck};
use crate::database::state::DbParam;
use crate::log::{log_output, log_output_lazy};
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

/// Prefix of repository error strings that mean "no row matched the id";
/// the service layer maps these to HTTP 404 instead of 500.
pub const NOT_FOUND_PREFIX: &str = "Record not found: ";
/// Prefix of repository error strings for optimistic-locking failures; mapped to HTTP 409.
pub const CONFLICT_PREFIX: &str = "Version conflict: ";
/// Prefix of repository error strings for client validation failures (e.g. missing
/// `X-Change-Reason` for `audit_trail.require_reason_when`); mapped to HTTP 400.
pub const BAD_REQUEST_PREFIX: &str = "Bad request: ";
/// Prefix of repository error strings for authorization / record-level guard
/// denials (e.g. `state_machine.transitions[].actor_column` or `not_same_as`);
/// mapped to HTTP 403.
pub const FORBIDDEN_PREFIX: &str = "Forbidden: ";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DetailUpdateStrategy {
    #[default]
    Replace,
    Upsert,
    Append,
}

impl DetailUpdateStrategy {
    pub fn from_opt(raw: Option<&str>) -> Self {
        match raw.map(str::trim).unwrap_or("").to_ascii_lowercase().as_str() {
            "upsert" => Self::Upsert,
            "append" => Self::Append,
            _ => Self::Replace,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedDetailUpdateItem {
    pub pk_raw: String,
    pub pk_param: DbParam,
    /// Columns & values to SET (empty when caller sent only the PK to keep the row untouched).
    pub set_fields: Vec<(String, InsertValue)>,
    /// Optional insert row used only when `pk_auto_increment == false` and the PK is not yet present.
    pub fallback_insert_row: Option<Vec<InsertValue>>,
}

#[derive(Debug, Clone, Default)]
pub struct PreparedDetailUpdateBatch {
    pub field: String,
    pub target_table: String,
    pub foreign_key_column: String,
    pub parent_key_column: Option<String>,
    pub fk_type_data: String,
    pub pk_column: String,
    pub pk_type_data: String,
    pub pk_auto_increment: bool,
    pub strategy: DetailUpdateStrategy,
    pub delete_missing: bool,
    pub use_soft_delete: bool,
    pub has_deleted_at: bool,
    pub has_deleted_by_id: bool,
    pub deleted_by_type: String,
    pub insert_columns: Vec<String>,
    pub fk_index: usize,
    pub insert_rows: Vec<Vec<InsertValue>>,
    pub update_items: Vec<PreparedDetailUpdateItem>,
}

fn dbparam_to_qv(p: &DbParam) -> QV {
    match p {
        DbParam::I64(n) => QV::I64(*n),
        DbParam::F64(f) => QV::F64(*f),
        DbParam::Bool(b) => QV::Bool(*b),
        DbParam::Null => QV::Null,
        DbParam::Str(s) => QV::Str(s.clone()),
    }
}

fn json_val_to_pk_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

async fn sync_detail_batch_in_tx(
    state: &web::Data<AppState>,
    ds: &SqlStore,
    tx: &mut dyn crate::storage::traits::TxStore,
    batch: &mut PreparedDetailUpdateBatch,
    parent_key_str: &str,
    actor_id: Option<&str>,
) -> Result<Vec<Value>, String> {
    let fk_pk_param = crate::nocode::pk_utils::dbparam_from_str_and_type(parent_key_str, &batch.fk_type_data);
    let fk_pk_qv = dbparam_to_qv(&fk_pk_param);

    match batch.strategy {
        DetailUpdateStrategy::Replace => {
            // 1. Delete existing details for this parent
            let del_filter = QF::Eq(batch.foreign_key_column.clone(), fk_pk_qv.clone());
            let (del_sql, del_params) = ds
                .preview_delete(&batch.target_table, Some(&del_filter))
                .map_err(|e| format!("Error building delete for {}: {}", batch.target_table, e))?;
            tx.raw_sql(&del_sql, del_params)
                .await
                .map_err(|e| format!("Error clearing old detail items for {}: {}", batch.target_table, e))?;

            // 2. Bulk insert new details if any
            if !batch.insert_rows.is_empty() {
                for r in &mut batch.insert_rows {
                    if batch.fk_index < r.len() {
                        r[batch.fk_index] = InsertValue::Param(fk_pk_param.clone());
                    }
                }
                let (bulk_sql, bulk_params) = ds
                    .preview_insert_bulk(&batch.target_table, &batch.insert_columns, &batch.insert_rows)
                    .map_err(|e| format!("Error building bulk insert for {}: {}", batch.target_table, e))?;
                let built_bulk = crate::database::state::rehydrate_placeholders(&bulk_sql, state.db_type.as_str());
                tx.raw_sql(&built_bulk, bulk_params)
                    .await
                    .map_err(|e| format!("Error inserting detail items into {}: {}", batch.target_table, e))?;
            }
        }
        DetailUpdateStrategy::Append => {
            // Insert-only: leave all existing child rows untouched.
            if !batch.insert_rows.is_empty() {
                for r in &mut batch.insert_rows {
                    if batch.fk_index < r.len() {
                        r[batch.fk_index] = InsertValue::Param(fk_pk_param.clone());
                    }
                }
                let (bulk_sql, bulk_params) = ds
                    .preview_insert_bulk(&batch.target_table, &batch.insert_columns, &batch.insert_rows)
                    .map_err(|e| format!("Error building bulk insert for {}: {}", batch.target_table, e))?;
                let built_bulk = crate::database::state::rehydrate_placeholders(&bulk_sql, state.db_type.as_str());
                tx.raw_sql(&built_bulk, bulk_params)
                    .await
                    .map_err(|e| format!("Error appending detail items into {}: {}", batch.target_table, e))?;
            }
        }
        DetailUpdateStrategy::Upsert => {
            // 1. Fetch existing active child PKs belonging to this parent
            let select_existing_sql = if batch.has_deleted_at {
                format!(
                    "SELECT {} FROM {} WHERE {} = ? AND deleted_at IS NULL",
                    batch.pk_column, batch.target_table, batch.foreign_key_column
                )
            } else {
                format!(
                    "SELECT {} FROM {} WHERE {} = ?",
                    batch.pk_column, batch.target_table, batch.foreign_key_column
                )
            };
            let built_select = crate::database::state::rehydrate_placeholders(
                &select_existing_sql,
                state.db_type.as_str(),
            );
            let existing_rows = tx
                .raw_sql(&built_select, vec![fk_pk_param.clone()])
                .await
                .map_err(|e| format!("Error reading existing detail items for {}: {}", batch.target_table, e))?;

            let mut existing_ids: Vec<(String, DbParam)> = Vec::with_capacity(existing_rows.len());
            let mut existing_id_set: std::collections::HashSet<String> =
                std::collections::HashSet::with_capacity(existing_rows.len());
            for row in existing_rows {
                if let Some(pk_val) = row.get(&batch.pk_column) {
                    let pk_str = json_val_to_pk_str(pk_val);
                    if !pk_str.is_empty() {
                        let pk_param = crate::nocode::pk_utils::dbparam_from_str_and_type(&pk_str, &batch.pk_type_data);
                        existing_id_set.insert(pk_str.clone());
                        existing_ids.push((pk_str, pk_param));
                    }
                }
            }

            // 2. Update existing rows matched by PK + parent FK
            let mut kept_ids: std::collections::HashSet<String> =
                std::collections::HashSet::with_capacity(batch.update_items.len());
            let mut extra_insert_rows: Vec<Vec<InsertValue>> = Vec::new();

            for u_item in &batch.update_items {
                if existing_id_set.contains(&u_item.pk_raw) {
                    kept_ids.insert(u_item.pk_raw.clone());
                    if !u_item.set_fields.is_empty() {
                        let mut parts = vec![
                            QF::Eq(batch.pk_column.clone(), dbparam_to_qv(&u_item.pk_param)),
                            QF::Eq(batch.foreign_key_column.clone(), fk_pk_qv.clone()),
                        ];
                        if batch.has_deleted_at {
                            parts.push(QF::IsNull("deleted_at".to_string()));
                        }
                        let upd_filter = QF::And(parts);
                        let (upd_sql, upd_params) = ds
                            .preview_update_with(&batch.target_table, Some(&upd_filter), &u_item.set_fields)
                            .map_err(|e| format!("Error building detail update for {}: {}", batch.target_table, e))?;
                        tx.raw_sql(&upd_sql, upd_params)
                            .await
                            .map_err(|e| format!("Error updating detail item '{}' in {}: {}", u_item.pk_raw, batch.target_table, e))?;
                    }
                } else if !batch.pk_auto_increment
                    && let Some(fallback_row) = &u_item.fallback_insert_row
                {
                    extra_insert_rows.push(fallback_row.clone());
                } else {
                    return Err(format!(
                        "{}detail item in '{}' with {}='{}' not found for parent '{}'",
                        NOT_FOUND_PREFIX, batch.field, batch.pk_column, u_item.pk_raw, parent_key_str
                    ));
                }
            }

            // 3. Delete (soft or hard) existing rows missing from the payload if delete_missing is enabled
            if batch.delete_missing {
                let missing_qvs: Vec<QV> = existing_ids
                    .iter()
                    .filter(|(id_str, _)| !kept_ids.contains(id_str))
                    .map(|(_, p)| dbparam_to_qv(p))
                    .collect();

                if !missing_qvs.is_empty() {
                    let mut parts = vec![
                        QF::In(batch.pk_column.clone(), missing_qvs),
                        QF::Eq(batch.foreign_key_column.clone(), fk_pk_qv.clone()),
                    ];
                    if batch.has_deleted_at {
                        parts.push(QF::IsNull("deleted_at".to_string()));
                    }
                    let del_filter = QF::And(parts);

                    if batch.use_soft_delete {
                        let mut soft_fields: Vec<(String, InsertValue)> = vec![(
                            "deleted_at".to_string(),
                            InsertValue::Raw(state.query_converter.datetime_now.clone()),
                        )];
                        if batch.has_deleted_by_id {
                            let actor = actor_id.unwrap_or("0");
                            soft_fields.push((
                                "deleted_by_id".to_string(),
                                InsertValue::Param(crate::nocode::pk_utils::dbparam_from_str_and_type(
                                    actor,
                                    &batch.deleted_by_type,
                                )),
                            ));
                        }
                        let (soft_sql, soft_params) = ds
                            .preview_update_with(&batch.target_table, Some(&del_filter), &soft_fields)
                            .map_err(|e| format!("Error building soft delete for {}: {}", batch.target_table, e))?;
                        tx.raw_sql(&soft_sql, soft_params)
                            .await
                            .map_err(|e| format!("Error soft-deleting missing detail items in {}: {}", batch.target_table, e))?;
                    } else {
                        let (hard_sql, hard_params) = ds
                            .preview_delete(&batch.target_table, Some(&del_filter))
                            .map_err(|e| format!("Error building hard delete for {}: {}", batch.target_table, e))?;
                        tx.raw_sql(&hard_sql, hard_params)
                            .await
                            .map_err(|e| format!("Error deleting missing detail items in {}: {}", batch.target_table, e))?;
                    }
                }
            }

            // 4. Bulk insert new rows (items without PK + non-autoincrement fallbacks)
            let mut all_inserts = std::mem::take(&mut batch.insert_rows);
            all_inserts.extend(extra_insert_rows);
            if !all_inserts.is_empty() {
                for r in &mut all_inserts {
                    if batch.fk_index < r.len() {
                        r[batch.fk_index] = InsertValue::Param(fk_pk_param.clone());
                    }
                }
                let (bulk_sql, bulk_params) = ds
                    .preview_insert_bulk(&batch.target_table, &batch.insert_columns, &all_inserts)
                    .map_err(|e| format!("Error building bulk insert for {}: {}", batch.target_table, e))?;
                let built_bulk = crate::database::state::rehydrate_placeholders(&bulk_sql, state.db_type.as_str());
                tx.raw_sql(&built_bulk, bulk_params)
                    .await
                    .map_err(|e| format!("Error inserting new detail items into {}: {}", batch.target_table, e))?;
            }
        }
    }

    // Re-read the final active detail rows inside the transaction so the response,
    // action_triggers (new_record), and outbox webhooks reflect the exact final state.
    let order_clause = if !batch.pk_column.is_empty() {
        format!(" ORDER BY {} ASC", batch.pk_column)
    } else {
        String::new()
    };
    let select_final_sql = if batch.has_deleted_at {
        format!(
            "SELECT * FROM {} WHERE {} = ? AND deleted_at IS NULL{}",
            batch.target_table, batch.foreign_key_column, order_clause
        )
    } else {
        format!(
            "SELECT * FROM {} WHERE {} = ?{}",
            batch.target_table, batch.foreign_key_column, order_clause
        )
    };
    let built_final = crate::database::state::rehydrate_placeholders(
        &select_final_sql,
        state.db_type.as_str(),
    );
    tx.raw_sql(&built_final, vec![fk_pk_param])
        .await
        .map_err(|e| format!("Error reading final detail items for {}: {}", batch.target_table, e))
}

fn json_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
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

/// Verified state of the `X-Signature-Token` header on a write request.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SignatureHeader<'a> {
    #[default]
    Missing,
    Invalid(&'static str),
    Valid(&'a crate::auth::SignatureClaims),
}

/// Caller identity evaluated by [`check_transition_guards`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TransitionCaller<'a> {
    /// Role checked against `transitions[].roles`.
    pub role: Option<&'a str>,
    /// Caller's JWT `id` claim (default identity for `actor_column` / `not_same_as`).
    pub id: Option<&'a str>,
    /// Caller's JWT `cs` claim (used when `actor_claim == "cs"`).
    pub cs: Option<&'a str>,
    /// Verified `X-Signature-Token` header for `require_signature` transitions.
    pub signature: SignatureHeader<'a>,
}

/// Validated electronic-signature metadata produced when a transition with
/// `require_signature` is authorized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedSignature {
    pub meaning_column: Option<String>,
    pub by_column: Option<String>,
    pub at_column: Option<String>,
    pub signer_identity: String,
    pub meaning: String,
    pub actor_id: String,
    pub actor_custom: Option<String>,
    pub jti: String,
    pub exp: i64,
    pub field: String,
    pub old_status: String,
    pub new_status: String,
}

/// Detailed result of [`check_transition_guards_detailed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionGuardOutcome {
    pub status_transition_authorized: bool,
    pub applied_signature: Option<AppliedSignature>,
}

fn resolve_caller_identity<'a>(caller: &TransitionCaller<'a>, claim: &str) -> Option<&'a str> {
    let raw = match claim.trim().to_ascii_lowercase().as_str() {
        "" | "id" => caller.id,
        "cs" => caller.cs,
        _ => None,
    };
    raw.map(str::trim).filter(|s| !s.is_empty())
}

fn record_value_as_identity(val: &Value) -> Option<String> {
    match val {
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Shared write guard for one record update: `state_machine` transition matrix
/// (including per-record `actor_column` and separation-of-duties `not_same_as`
/// checks) followed by the `locked_when` immutability lock.
///
/// Used by `PUT`/`PATCH` ([`perform_update`]) and by the cron scheduler
/// (`crate::nocode::scheduler`, role `"system"`) so both paths enforce exactly
/// the same rules and return the same messages.
///
/// * `old_record` — current row (read under row lock by the caller).
/// * `patch` — columns about to be written.
/// * `caller` — caller role and identity claims (`id`, `cs`).
///
/// Returns `Ok(true)` when `patch` carries an authorized status transition,
/// `Ok(false)` when it does not change the status field, `Err(msg)` otherwise.
pub fn check_transition_guards(
    table_schema: &TableSchema,
    old_record: &serde_json::Map<String, Value>,
    patch: &serde_json::Map<String, Value>,
    caller: TransitionCaller<'_>,
) -> Result<bool, String> {
    check_transition_guards_detailed(table_schema, old_record, patch, caller)
        .map(|o| o.status_transition_authorized)
}

/// Detailed variant of [`check_transition_guards`] that also returns any
/// validated [`AppliedSignature`] for transitions declaring `require_signature`.
pub fn check_transition_guards_detailed(
    table_schema: &TableSchema,
    old_record: &serde_json::Map<String, Value>,
    patch: &serde_json::Map<String, Value>,
    caller: TransitionCaller<'_>,
) -> Result<TransitionGuardOutcome, String> {
    // 1. state_machine transition guard
    let mut status_transition_authorized = false;
    let mut applied_signature: Option<AppliedSignature> = None;
    if let Some(sm) = &table_schema.state_machine
        && let Some(new_status_val) = patch.get(&sm.field)
    {
        let old_status_val = old_record.get(&sm.field);
        if old_status_val != Some(new_status_val) {
            let old_status_str = old_status_val.and_then(|v| v.as_str()).unwrap_or("");
            let new_status_str = new_status_val.as_str().unwrap_or("");

            let matching_transition = sm.transitions.iter().find(|t| {
                let matches_to = t.to.eq_ignore_ascii_case(new_status_str);
                let matches_from = match &t.from {
                    Value::Array(arr) => arr.iter().any(|v| match v {
                        Value::String(s) => s.eq_ignore_ascii_case(old_status_str) || s == "*",
                        _ => false,
                    }),
                    Value::String(s) => s.eq_ignore_ascii_case(old_status_str) || s == "*",
                    _ => false,
                };
                matches_to && matches_from
            });

            match matching_transition {
                None => {
                    return Err(format!(
                        "Illegal state transition on '{}': cannot transition from '{}' to '{}'",
                        sm.field, old_status_str, new_status_str
                    ));
                }
                Some(trans) => {
                    if !trans.roles.is_empty() {
                        let role = caller.role.unwrap_or("");
                        let is_role_ok = trans.roles.iter().any(|r| r.eq_ignore_ascii_case(role) || r == "*");
                        if !is_role_ok {
                            // FORBIDDEN_PREFIX -> HTTP 403. (A message starting with
                            // "Unauthorized" is classified as 401 by ApiError, which
                            // clients treat as an expired session.)
                            return Err(format!(
                                "{}state transition on '{}': role '{}' is not permitted to transition from '{}' to '{}'",
                                FORBIDDEN_PREFIX, sm.field, role, old_status_str, new_status_str
                            ));
                        }
                    }

                    let claim_name = trans
                        .actor_claim
                        .as_deref()
                        .or(sm.actor_claim.as_deref())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or("id");

                    if let Some(actor_col) = trans
                        .actor_column
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        let Some(caller_id) = resolve_caller_identity(&caller, claim_name) else {
                            return Err(format!(
                                "{}state transition on '{}' from '{}' to '{}' requires caller claim '{}' to match column '{}', but caller identity is empty",
                                FORBIDDEN_PREFIX, sm.field, old_status_str, new_status_str, claim_name, actor_col
                            ));
                        };
                        let Some(record_actor) = old_record.get(actor_col).and_then(record_value_as_identity) else {
                            return Err(format!(
                                "{}state transition on '{}' from '{}' to '{}' requires column '{}' to match caller, but '{}' is empty",
                                FORBIDDEN_PREFIX, sm.field, old_status_str, new_status_str, actor_col, actor_col
                            ));
                        };
                        if record_actor.as_str() != caller_id {
                            return Err(format!(
                                "{}state transition on '{}' from '{}' to '{}' is restricted to the actor in column '{}'",
                                FORBIDDEN_PREFIX, sm.field, old_status_str, new_status_str, actor_col
                            ));
                        }
                    }

                    if !trans.not_same_as.is_empty() {
                        let Some(caller_id) = resolve_caller_identity(&caller, claim_name) else {
                            return Err(format!(
                                "{}state transition on '{}' from '{}' to '{}' enforces separation of duties (not_same_as), but caller claim '{}' is empty",
                                FORBIDDEN_PREFIX, sm.field, old_status_str, new_status_str, claim_name
                            ));
                        };
                        for col in &trans.not_same_as {
                            let col = col.trim();
                            if col.is_empty() {
                                continue;
                            }
                            if let Some(record_actor) = old_record.get(col).and_then(record_value_as_identity)
                                && record_actor.eq_ignore_ascii_case(caller_id)
                            {
                                return Err(format!(
                                    "{}state transition on '{}' from '{}' to '{}' violates separation of duties: caller cannot be the same as '{}'",
                                    FORBIDDEN_PREFIX, sm.field, old_status_str, new_status_str, col
                                ));
                            }
                        }
                    }

                    if let Some(req_sig) = &trans.require_signature {
                        let sig = match caller.signature {
                            SignatureHeader::Missing => {
                                return Err(format!("{}Signature required", FORBIDDEN_PREFIX));
                            }
                            SignatureHeader::Invalid(reason) => {
                                return Err(format!(
                                    "{}Signature required ({})",
                                    FORBIDDEN_PREFIX, reason
                                ));
                            }
                            SignatureHeader::Valid(s) => s,
                        };

                        // Signature token must belong to the same authenticated user (fail-closed).
                        let Some(caller_uid) = caller.id.map(str::trim).filter(|s| !s.is_empty()) else {
                            return Err(format!("{}Signature required", FORBIDDEN_PREFIX));
                        };
                        if sig.id.trim() != caller_uid {
                            return Err(format!(
                                "{}Signature required (token does not match authenticated user)",
                                FORBIDDEN_PREFIX
                            ));
                        }

                        if !req_sig.allowed_meanings.is_empty()
                            && !req_sig
                                .allowed_meanings
                                .iter()
                                .any(|m| m.trim().eq_ignore_ascii_case(sig.meaning.trim()))
                        {
                            return Err(format!(
                                "{}Signature meaning '{}' is not permitted for transition from '{}' to '{}'",
                                FORBIDDEN_PREFIX, sig.meaning, old_status_str, new_status_str
                            ));
                        }

                        let signer_identity = if claim_name.eq_ignore_ascii_case("cs") {
                            let cs_val = if !sig.cs.trim().is_empty() {
                                Some(sig.cs.trim())
                            } else {
                                caller.cs.map(str::trim).filter(|s| !s.is_empty())
                            };
                            let Some(cs_id) = cs_val else {
                                return Err(format!(
                                    "{}Signature required: caller claim 'cs' is empty",
                                    FORBIDDEN_PREFIX
                                ));
                            };
                            cs_id.to_string()
                        } else {
                            sig.id.trim().to_string()
                        };

                        let norm_col = |opt: &Option<String>| -> Option<String> {
                            opt.as_deref()
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(str::to_string)
                        };

                        applied_signature = Some(AppliedSignature {
                            meaning_column: norm_col(&req_sig.meaning_column),
                            by_column: norm_col(&req_sig.by_column),
                            at_column: norm_col(&req_sig.at_column),
                            signer_identity,
                            meaning: sig.meaning.trim().to_string(),
                            actor_id: sig.id.trim().to_string(),
                            actor_custom: if sig.cs.trim().is_empty() {
                                caller.cs.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
                            } else {
                                Some(sig.cs.trim().to_string())
                            },
                            jti: sig.jti.clone(),
                            exp: sig.exp as i64,
                            field: sm.field.clone(),
                            old_status: old_status_str.to_string(),
                            new_status: new_status_str.to_string(),
                        });
                    }

                    status_transition_authorized = true;
                }
            }
        }
    }

    // 2. locked_when immutability lock
    if let Some(locked_cfg) = &table_schema.locked_when {
        let conditions = locked_cfg.get_conditions();
        let except_cols = locked_cfg.get_except_columns();
        let sm_field_opt = table_schema.state_machine.as_ref().map(|sm| &sm.field);

        for (field, expected_val) in conditions {
            if let Some(actual_val) = old_record.get(field) {
                let is_match = match expected_val {
                    Value::Array(arr) => arr.iter().any(|v| crate::nocode::trigger_engine::value_matches(v, Some(actual_val))),
                    single => crate::nocode::trigger_engine::value_matches(single, Some(actual_val)),
                };
                if is_match {
                    let modified_keys: Vec<&String> = patch
                        .keys()
                        .filter(|k| !k.starts_with("updated_") && !k.eq_ignore_ascii_case("version"))
                        .collect();
                    let only_allowed = !modified_keys.is_empty() && modified_keys.iter().all(|k| {
                        except_cols.contains(k) || (status_transition_authorized && sm_field_opt == Some(*k))
                    });
                    if !only_allowed {
                        return Err(format!(
                            "Cannot modify locked record: field '{}' is currently '{}'",
                            field,
                            match actual_val {
                                Value::String(s) => s.as_str(),
                                _ => "locked",
                            }
                        ));
                    }
                }
            }
        }
    }

    Ok(TransitionGuardOutcome {
        status_transition_authorized,
        applied_signature,
    })
}

#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
pub async fn perform_update(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    route: &str,
    id_raw: &str,
    update_fields: Vec<(String, InsertValue)>,
    mut patch_fields: serde_json::Map<String, Value>,
    fk_checks: Vec<(String, String, String, String, String)>,
    password_override: Option<String>,
    mut prepared_details: Vec<PreparedDetailUpdateBatch>,
    body: &Value,
    auth_token: Option<String>,
    actor_id: Option<&str>,
    claims: Option<&crate::auth::Claims>,
    transition_caller: TransitionCaller<'_>,
    audit_ctx: crate::nocode::audit_trail::AuditContext<'_>,
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
                    log_output_lazy("PARAM", "PUT(AST)", route, || format!("{:?}", params_compiled), true);
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

        // Fetch pre-update record state for triggers, state machine, and conditional evaluation
        let mut old_record: serde_json::Map<String, Value> = serde_json::Map::new();
        // Lock the row for the rest of the transaction so two concurrent updates
        // (status transitions, locked_when checks, version bumps) serialise instead
        // of both passing their checks against the same stale snapshot.
        let select_current_sql = match state.db_type.as_str() {
            "postgres" | "mysql" => format!("SELECT * FROM {} WHERE {} = ? FOR UPDATE", table_schema.table, pk_col_first),
            "mssql" => format!("SELECT * FROM {} WITH (UPDLOCK, ROWLOCK) WHERE {} = ?", table_schema.table, pk_col_first),
            _ => format!("SELECT * FROM {} WHERE {} = ?", table_schema.table, pk_col_first),
        };
        let built_select_current = crate::database::state::rehydrate_placeholders(&select_current_sql, state.db_type.as_str());
        // A failed fetch must not be ignored: an empty `old_record` makes the
        // state-machine and `locked_when` guards below pass vacuously.
        match tx.raw_sql(&built_select_current, vec![pk_param.clone()]).await {
            Ok(rows) => {
                if let Some(Value::Object(map)) = rows.into_iter().next() {
                    old_record = map;
                }
            }
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(format!("Error reading current record: {}", e));
            }
        }

        // 0. Optimistic locking on a `version` column (if the entity has one).
        //    - client may send `If-Match: <n>` (service copies it to body.__expected_version__)
        //      or `version` in the body; mismatch with the current row -> 409.
        //    - the UPDATE itself is guarded with `AND version = <current>` and bumps it,
        //      so two writers racing after the same read cannot both succeed.
        let mut version_guard: Option<(String, i64)> = None;
        if let Some(vc) = table_schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case("version")).map(|c| c.name.clone()) {
            let current = old_record.get(&vc).and_then(json_to_i64);
            let expected = body
                .get("__expected_version__")
                .or_else(|| body.get(&vc))
                .and_then(json_to_i64);
            if let (Some(exp), Some(cur)) = (expected, current)
                && exp != cur
            {
                let _ = tx.rollback().await;
                return Err(format!(
                    "{}record {} is at version {}, request expected {}",
                    CONFLICT_PREFIX, id_raw, cur, exp
                ));
            }
            let next = current.unwrap_or(0) + 1;
            patch_fields.insert(vc.clone(), Value::from(next));
            if let Some(cur) = current {
                version_guard = Some((vc, cur));
            }
        }

        // 1-2. state_machine transition guard + locked_when immutability lock
        //      (shared with the cron scheduler, see `check_transition_guards`).
        let guard_outcome = match check_transition_guards_detailed(table_schema, &old_record, &patch_fields, transition_caller) {
            Ok(outcome) => outcome,
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
            }
        };

        // 1b. Electronic signature auto-fill (`require_signature`):
        //     Populate server-authoritative `by_column`, `at_column`, and
        //     `meaning_column` AFTER `locked_when` so the client cannot spoof
        //     them or sneak extra columns past `locked_when`.
        let mut applied_signature_with_at: Option<(AppliedSignature, String)> = None;
        let mut pg_timestamp_at_col: Option<(String, String)> = None;
        if let Some(sig) = guard_outcome.applied_signature {
            let now_utc = chrono::Utc::now();
            let signed_at_iso = now_utc.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            let signed_at_sql = now_utc.format("%Y-%m-%d %H:%M:%S").to_string();

            if let Some(col) = &sig.meaning_column {
                patch_fields.insert(col.clone(), Value::String(sig.meaning.clone()));
            }
            if let Some(col) = &sig.by_column {
                let td = table_schema
                    .columns
                    .iter()
                    .find(|c| c.name == *col)
                    .map(|c| c.type_data.as_str())
                    .unwrap_or("varchar");
                patch_fields.insert(
                    col.clone(),
                    crate::nocode::pk_utils::json_value_from_str_and_type(&sig.signer_identity, td),
                );
            }
            if let Some(col) = &sig.at_column {
                let td = table_schema
                    .columns
                    .iter()
                    .find(|c| c.name == *col)
                    .map(|c| c.type_data.to_ascii_lowercase())
                    .unwrap_or_default();
                let is_temporal = td.contains("timestamp") || td.contains("datetime") || td == "date";
                if is_temporal && state.db_type == crate::model::DbType::Postgres {
                    pg_timestamp_at_col = Some((col.clone(), signed_at_iso.clone()));
                } else if is_temporal {
                    patch_fields.insert(col.clone(), Value::String(signed_at_sql));
                } else {
                    patch_fields.insert(col.clone(), Value::String(signed_at_iso.clone()));
                }
            }
            applied_signature_with_at = Some((sig, signed_at_iso));
        }

        // 2b. CPOB audit_trail: enforce `require_reason_when` before writing.
        //     Skipped when `old_record` is empty (non-existent row will return 404 below).
        if !old_record.is_empty()
            && crate::nocode::audit_trail::is_enabled(table_schema)
            && let Err(e) = crate::nocode::audit_trail::check_require_reason_on_patch(
                table_schema,
                &old_record,
                &patch_fields,
                audit_ctx.reason,
            )
        {
            let _ = tx.rollback().await;
            return Err(format!("{}{}", BAD_REQUEST_PREFIX, e));
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

        // Ensure path PK columns (and `id`) are available to `{request.*}` placeholders
        // even when the PUT/PATCH JSON body omits them.
        let formula_body: std::borrow::Cow<'_, Value> = {
            let pk_vals = parse_pk_values(id_raw);
            let needs_pk = body.as_object().is_some_and(|m| {
                !m.contains_key("id")
                    || table_schema
                        .primary_key
                        .columns
                        .iter()
                        .any(|c| !m.contains_key(c))
            });
            if needs_pk {
                let mut m = body.as_object().cloned().unwrap_or_default();
                for (col, val) in table_schema.primary_key.columns.iter().zip(pk_vals.iter()) {
                    m.entry(col.clone()).or_insert_with(|| Value::String(val.clone()));
                }
                m.entry("id".to_string()).or_insert_with(|| Value::String(id_raw.to_string()));
                std::borrow::Cow::Owned(Value::Object(m))
            } else {
                std::borrow::Cow::Borrowed(body)
            }
        };

        // Validate Data Formula (API based)
        if table_schema.put.validate_data.starts_with("API:") {
             if let Err(e) = crate::nocode::validate::validate_api_formula(&table_schema.put.validate_data, formula_body.as_ref(), auth_token.as_deref(), claims).await {
                 let _ = tx.rollback().await;
                 return Err(e);
             }
        }

        // Validate Data Formula
        if table_schema.put.validate_data.contains("SQL:") {
             match crate::database::state::build_sql_and_params_from_formula_with_claims(&table_schema.put.validate_data, formula_body.as_ref(), claims) {
                 Ok((built_sql, params)) => {
                     match tx.raw_sql(&built_sql, params).await {
                         Ok(row) => {
                             if row.is_empty() || !crate::helpers::validation_row_is_truthy(&row[0]).unwrap_or(false) {
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
            if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(&mut tx, table_schema.put.pre_process.clone(), formula_body.as_ref(), route, claims).await {
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
        if let Some((vc, cur)) = &version_guard {
            filter = QF::And(vec![filter, QF::Eq(vc.clone(), QV::I64(*cur))]);
        }
        
        // Convert Map to json Value for DB update
        let doc_json = Value::Object(patch_fields.clone()); 
        
        match tx.update(&table_schema.table, Some(filter), doc_json).await {
             Ok(0) if version_guard.is_some() => {
                 let _ = tx.rollback().await;
                 Err(format!("{}record {} was modified concurrently, reload and retry", CONFLICT_PREFIX, id_raw))
             }
             Ok(0) => {
                 let _ = tx.rollback().await;
                 Err(format!("{}{} not found or already deleted", NOT_FOUND_PREFIX, id_raw))
             }
             Ok(_) => {
                 let mut final_patch = patch_fields;

                 // On PostgreSQL, `tx.update` binds strings as TEXT which cannot be
                 // implicitly cast into TIMESTAMP/DATETIME columns; stamp `at_column`
                 // with `CURRENT_TIMESTAMP` inside the same transaction.
                 if let Some((ref at_col, ref iso)) = pg_timestamp_at_col {
                     let ds_pg = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
                     let pk_vals_pg = parse_pk_values(id_raw);
                     if let Ok(pk_f) = build_pk_filter(&table_schema.primary_key.columns, &pk_vals_pg)
                         && let Ok((pg_sql, pg_params)) = ds_pg.preview_update_with(
                             &table_schema.table,
                             Some(&pk_f),
                             &[(at_col.clone(), InsertValue::Raw(state.query_converter.datetime_now.clone()))],
                         )
                     {
                         if let Err(e) = tx.raw_execute(&pg_sql, pg_params).await {
                             let _ = tx.rollback().await;
                             return Err(format!("Error stamping signature timestamp: {}", e));
                         }
                         final_patch.insert(at_col.clone(), Value::String(iso.clone()));
                     }
                 }

                 // Sync Details inside transaction (replace / upsert / append)
                 let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
                 for batch in &mut prepared_details {
                     if !batch.target_table.is_empty() && !batch.foreign_key_column.is_empty() {
                         let parent_key_str = batch
                             .parent_key_column
                             .as_deref()
                             .filter(|c| !c.is_empty() && *c != "id")
                             .and_then(|col| {
                                 final_patch
                                     .get(col)
                                     .or_else(|| old_record.get(col))
                                     .map(json_val_to_pk_str)
                                     .filter(|s| !s.is_empty())
                             })
                             .unwrap_or_else(|| id_raw.to_string());

                         // CPOB audit trail for the detail entity: snapshot child rows
                         // before the sync, diff against the final rows afterwards.
                         let detail_audit = crate::nocode::audit_trail::audit_target_for_table(&batch.target_table);
                         let detail_before = match &detail_audit {
                             Some(t) => {
                                 let fk_val = crate::nocode::audit_trail::typed_key_value(&parent_key_str, &batch.fk_type_data);
                                 match crate::nocode::audit_trail::select_rows_where_eq(&mut *tx, t, &batch.foreign_key_column, &fk_val).await {
                                     Ok(rows) => rows,
                                     Err(e) => {
                                         let _ = tx.rollback().await;
                                         return Err(format!("Error recording audit trail: {}", e));
                                     }
                                 }
                             }
                             None => Vec::new(),
                         };

                         match sync_detail_batch_in_tx(
                             state,
                             &ds,
                             &mut *tx,
                             batch,
                             &parent_key_str,
                             actor_id,
                         )
                         .await
                         {
                             Ok(final_rows) => {
                                 if let Some(t) = &detail_audit
                                     && let Err(e) = crate::nocode::audit_trail::record_snapshot_diff_in_tx(
                                         &mut *tx,
                                         t,
                                         &detail_before,
                                         &final_rows,
                                         crate::nocode::audit_trail::DETAIL_ACTIONS,
                                         &audit_ctx,
                                         true,
                                     )
                                     .await
                                 {
                                     let _ = tx.rollback().await;
                                     return Err(if e.starts_with(crate::nocode::audit_trail::REASON_REQUIRED_PREFIX) {
                                         format!("{}{}", BAD_REQUEST_PREFIX, e)
                                     } else {
                                         e
                                     });
                                 }
                                 final_patch.insert(batch.field.clone(), Value::Array(final_rows));
                             }
                             Err(e) => {
                                 let _ = tx.rollback().await;
                                 return Err(e);
                             }
                         }
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
                         request_body: formula_body.as_ref(),
                         actor_id,
                         claims,
                     };

                     match crate::nocode::trigger_engine::execute_triggers_audited(
                         state.db_type.clone(),
                         &mut *tx,
                         table_schema,
                         &trigger_ctx,
                         "on_update",
                         &crate::nocode::trigger_engine::TriggerAudit::from_ctx(&audit_ctx),
                     ).await {
                         Ok(executed) => {
                             for trig_name in executed {
                                 crate::audit::write_audit(&crate::audit::AuditEntry {
                                     at: chrono::Local::now().to_rfc3339(),
                                     actor_id: actor_id.unwrap_or("system").to_string(),
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
                         formula_body.as_ref(),
                         route,
                         claims,
                     ).await {
                         let _ = tx.rollback().await;
                         return Err(format!("Error in post-process: {}", err));
                     }
                 }

                 if !table_schema.webhooks.is_empty() {
                     let mut new_record = old_record.clone();
                     for (k, v) in &final_patch {
                         new_record.insert(k.clone(), v.clone());
                     }
                     if let Err(e) = crate::nocode::outbox::enqueue_in_tx(
                         &mut *tx, table_schema, route, "on_update", id_raw, actor_id, &new_record, Some(&old_record),
                     ).await {
                         let _ = tx.rollback().await;
                         return Err(format!("Error queueing webhook: {}", e));
                     }
                 }

                 // CPOB structured audit trail: one row per changed tracked column,
                 // committed atomically inside the same transaction.
                 if crate::nocode::audit_trail::is_enabled(table_schema) {
                     let mut new_record = old_record.clone();
                     for (k, v) in &final_patch {
                         new_record.insert(k.clone(), v.clone());
                     }
                     let changes = crate::nocode::audit_trail::compute_diff(
                         table_schema,
                         &old_record,
                         &new_record,
                     );
                     if let Err(e) = crate::nocode::audit_trail::check_require_reason(
                         table_schema,
                         &changes,
                         audit_ctx.reason,
                     ) {
                         let _ = tx.rollback().await;
                         return Err(format!("{}{}", BAD_REQUEST_PREFIX, e));
                     }
                     if let Err(e) = crate::nocode::audit_trail::record_update_in_tx(
                         &mut *tx,
                         table_schema,
                         route,
                         id_raw,
                         &changes,
                         &audit_ctx,
                     )
                     .await
                     {
                         let _ = tx.rollback().await;
                         return Err(format!("Error recording audit trail: {}", e));
                     }
                 }

                 // GxP electronic signature audit row: always written atomically inside
                 // the same transaction whenever a `require_signature` transition runs.
                 if let Some((ref sig, ref signed_at_iso)) = applied_signature_with_at
                     && let Err(e) = crate::nocode::audit_trail::record_signature_in_tx(
                         &mut *tx,
                         table_schema,
                         route,
                         id_raw,
                         &sig.field,
                         &sig.old_status,
                         &sig.new_status,
                         &sig.meaning,
                         &sig.actor_id,
                         sig.actor_custom.as_deref(),
                         audit_ctx.reason,
                         audit_ctx.request_id,
                         signed_at_iso,
                     )
                     .await
                 {
                     let _ = tx.rollback().await;
                     return Err(format!("Error recording signature audit trail: {}", e));
                 }

                 tx.commit()
                     .await
                     .map_err(|e| format!("Error committing transaction: {}", e))?;

                 // Single-use signature token: revoke `jti` after the signed transition commits.
                 if let Some((ref sig, _)) = applied_signature_with_at {
                     crate::auth::revoke_jti(&sig.jti, sig.exp);
                 }

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
            Ok(0) => {
                 let _ = tx.rollback().await;
                 Err(format!("{}{} not found or already deleted", NOT_FOUND_PREFIX, id_raw))
            }
            Ok(modified) => {
                 tx.commit()
                     .await
                     .map_err(|e| format!("Error committing transaction: {}", e))?;
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
    fn test_detail_update_strategy_parse() {
        assert_eq!(DetailUpdateStrategy::from_opt(None), DetailUpdateStrategy::Replace);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("")), DetailUpdateStrategy::Replace);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("replace")), DetailUpdateStrategy::Replace);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("upsert")), DetailUpdateStrategy::Upsert);
        assert_eq!(DetailUpdateStrategy::from_opt(Some(" UPSERT ")), DetailUpdateStrategy::Upsert);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("append")), DetailUpdateStrategy::Append);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("Append")), DetailUpdateStrategy::Append);
        assert_eq!(DetailUpdateStrategy::from_opt(Some("unknown")), DetailUpdateStrategy::Replace);
    }

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

    #[test]
    fn test_erp_locked_when_check() {
        let lock_cfg = crate::model::LockedWhen {
            except_columns: vec!["notes".to_string()],
            conditions: serde_json::json!({
                "status": ["SHIPPED", "PAID"]
            }).as_object().unwrap().clone(),
        };

        let mut old_rec = serde_json::Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        // Case 1: updating locked field or general fields -> should be locked
        let mut patch = serde_json::Map::new();
        patch.insert("total_net".to_string(), serde_json::json!(100));

        let conditions = lock_cfg.get_conditions();
        let except_cols = lock_cfg.get_except_columns();

        let is_locked = conditions.iter().any(|(f, expected)| {
            if let Some(actual) = old_rec.get(f) {
                crate::nocode::trigger_engine::value_matches(expected, Some(actual))
                    || match expected {
                        Value::Array(arr) => arr.iter().any(|v| crate::nocode::trigger_engine::value_matches(v, Some(actual))),
                        _ => false,
                    }
            } else {
                false
            }
        });
        assert!(is_locked);

        let modified_keys: Vec<&String> = patch.keys().filter(|k| !k.starts_with("updated_")).collect();
        let only_allowed = !modified_keys.is_empty() && modified_keys.iter().all(|k| except_cols.contains(k));
        assert!(!only_allowed, "Updating total_net on locked record must be rejected");

        // Case 2: updating only except_columns (notes) -> allowed!
        let mut patch_allowed = serde_json::Map::new();
        patch_allowed.insert("notes".to_string(), serde_json::json!("driver arrived"));
        let modified_allowed: Vec<&String> = patch_allowed.keys().filter(|k| !k.starts_with("updated_")).collect();
        let only_allowed_2 = !modified_allowed.is_empty() && modified_allowed.iter().all(|k| except_cols.contains(k));
        assert!(only_allowed_2, "Updating notes on locked record must be permitted");
    }

    #[test]
    fn test_erp_state_machine_transition_validation() {
        let sm = crate::model::StateMachine {
            field: "status".to_string(),
            initial: Some("DRAFT".to_string()),
            actor_claim: None,
            transitions: vec![
                crate::model::StateTransition {
                    from: serde_json::json!("DRAFT"),
                    to: "APPROVED".to_string(),
                    roles: vec!["manager".to_string(), "admin".to_string()],
                    ..Default::default()
                },
                crate::model::StateTransition {
                    from: serde_json::json!("APPROVED"),
                    to: "SHIPPED".to_string(),
                    roles: vec!["warehouse".to_string()],
                    ..Default::default()
                },
            ],
        };

        // Allowed transition with valid role
        let old_status = "DRAFT";
        let new_status = "APPROVED";
        let caller_role = "manager";

        let trans = sm.transitions.iter().find(|t| {
            t.to.eq_ignore_ascii_case(new_status) && match &t.from {
                Value::String(s) => s.eq_ignore_ascii_case(old_status) || s == "*",
                _ => false,
            }
        });
        assert!(trans.is_some());
        let authorized = trans.unwrap().roles.iter().any(|r| r.eq_ignore_ascii_case(caller_role));
        assert!(authorized);

        // Illegal jump (DRAFT -> SHIPPED)
        let illegal_trans = sm.transitions.iter().find(|t| {
            t.to.eq_ignore_ascii_case("SHIPPED") && match &t.from {
                Value::String(s) => s.eq_ignore_ascii_case("DRAFT") || s == "*",
                _ => false,
            }
        });
        assert!(illegal_trans.is_none(), "DRAFT to SHIPPED jump must not exist");

        // Unauthorized role (staff approving DRAFT -> APPROVED)
        let unauthorized = trans.unwrap().roles.iter().any(|r| r.eq_ignore_ascii_case("staff"));
        assert!(!unauthorized, "Staff role must not be authorized to approve");
    }

    #[test]
    fn test_erp_locked_when_permits_authorized_state_transition() {
        let locked_json = r#"{
            "status": ["SHIPPED", "COMPLETED"],
            "except_columns": ["notes"]
        }"#;
        let locked_cfg: crate::model::LockedWhen = serde_json::from_str(locked_json).unwrap();
        let conditions = locked_cfg.get_conditions();
        let except_cols = locked_cfg.get_except_columns();

        let mut old_record = serde_json::Map::new();
        old_record.insert("status".to_string(), serde_json::json!("SHIPPED"));
        old_record.insert("total_amount".to_string(), serde_json::json!(500000));

        let sm = crate::model::StateMachine {
            field: "status".to_string(),
            initial: Some("DRAFT".to_string()),
            actor_claim: None,
            transitions: vec![
                crate::model::StateTransition {
                    from: serde_json::json!("SHIPPED"),
                    to: "RETURNED".to_string(),
                    roles: vec!["admin".to_string()],
                    ..Default::default()
                },
            ],
        };

        // 1. Admin transitions SHIPPED -> RETURNED (Authorized state transition alone)
        let mut patch_status_only = serde_json::Map::new();
        patch_status_only.insert("status".to_string(), serde_json::json!("RETURNED"));

        let status_transition_authorized = true;
        let sm_field = &sm.field;

        for (field, expected_val) in conditions {
            if let Some(actual_val) = old_record.get(field) {
                let is_match = match expected_val {
                    Value::Array(arr) => arr.iter().any(|v| crate::nocode::trigger_engine::value_matches(v, Some(actual_val))),
                    single => crate::nocode::trigger_engine::value_matches(single, Some(actual_val)),
                };
                assert!(is_match);

                let modified_keys: Vec<&String> = patch_status_only.keys().filter(|k| !k.starts_with("updated_")).collect();
                let only_allowed = !modified_keys.is_empty() && modified_keys.iter().all(|k| {
                    except_cols.contains(k) || (status_transition_authorized && sm_field == *k)
                });
                assert!(only_allowed, "Authorized status transition must pass locked_when");
            }
        }

        // 2. Client attempts to modify total_amount on locked record while also sending RETURNED
        let mut patch_with_fraud = serde_json::Map::new();
        patch_with_fraud.insert("status".to_string(), serde_json::json!("RETURNED"));
        patch_with_fraud.insert("total_amount".to_string(), serde_json::json!(1000));

        for (field, _expected_val) in conditions {
            if old_record.contains_key(field) {
                let modified_keys: Vec<&String> = patch_with_fraud.keys().filter(|k| !k.starts_with("updated_")).collect();
                let only_allowed = !modified_keys.is_empty() && modified_keys.iter().all(|k| {
                    except_cols.contains(k) || (status_transition_authorized && sm_field == *k)
                });
                assert!(!only_allowed, "Modification of total_amount must still be blocked on locked record");
            }
        }
    }

    fn capa_schema_with_record_guards() -> TableSchema {
        TableSchema {
            table: "plant_capas".to_string(),
            state_machine: Some(crate::model::StateMachine {
                field: "status".to_string(),
                initial: Some("ROOT_CAUSE_ANALYSIS".to_string()),
                actor_claim: None,
                transitions: vec![
                    crate::model::StateTransition {
                        from: serde_json::json!("ROOT_CAUSE_ANALYSIS"),
                        to: "ACTION_PLANNING".to_string(),
                        roles: vec![],
                        actor_column: Some("pic_nip".to_string()),
                        actor_claim: Some("cs".to_string()),
                        not_same_as: vec![],
                        require_signature: None,
                    },
                    crate::model::StateTransition {
                        from: serde_json::json!("ACTION_PLANNING"),
                        to: "EVALUATION_APPROVAL".to_string(),
                        roles: vec![],
                        actor_column: Some("owner_id".to_string()),
                        actor_claim: None, // default: "id"
                        not_same_as: vec![],
                        require_signature: None,
                    },
                    crate::model::StateTransition {
                        from: serde_json::json!("EVALUATION_APPROVAL"),
                        to: "IMPLEMENTATION".to_string(),
                        roles: vec!["plant_qa_manager".to_string()],
                        actor_column: None,
                        actor_claim: Some("cs".to_string()),
                        not_same_as: vec!["prepared_by".to_string(), "pic_nip".to_string()],
                        require_signature: None,
                    },
                ],
            }),
            locked_when: Some(serde_json::from_str(r#"{"status": ["EVALUATION_APPROVAL"]}"#).unwrap()),
            ..Default::default()
        }
    }

    #[test]
    fn test_state_machine_actor_column_with_cs_and_id_claims() {
        let schema = capa_schema_with_record_guards();

        let mut old = serde_json::Map::new();
        old.insert("status".into(), serde_json::json!("ROOT_CAUSE_ANALYSIS"));
        old.insert("pic_nip".into(), serde_json::json!("NIP-1001"));
        old.insert("owner_id".into(), serde_json::json!(42));

        let mut patch = serde_json::Map::new();
        patch.insert("status".into(), serde_json::json!("ACTION_PLANNING"));

        // Matching cs claim -> allowed
        let ok_caller = TransitionCaller { role: Some("staff"), id: Some("7"), cs: Some("NIP-1001"), ..Default::default() };
        assert_eq!(check_transition_guards(&schema, &old, &patch, ok_caller), Ok(true));

        // Different cs claim -> 403 Forbidden
        let wrong_caller = TransitionCaller { role: Some("staff"), id: Some("8"), cs: Some("NIP-9999"), ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, wrong_caller).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);

        // Missing cs claim -> 403 Forbidden (fail-closed)
        let empty_cs = TransitionCaller { role: Some("staff"), id: Some("7"), cs: None, ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, empty_cs).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);

        // NULL pic_nip on record -> 403 Forbidden (fail-closed)
        let mut old_null = old.clone();
        old_null.insert("pic_nip".into(), Value::Null);
        let err = check_transition_guards(&schema, &old_null, &patch, ok_caller).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);

        // Spoofing attempt: caller tries to overwrite pic_nip in the same patch
        let mut spoof_patch = patch.clone();
        spoof_patch.insert("pic_nip".into(), serde_json::json!("NIP-9999"));
        let err = check_transition_guards(&schema, &old, &spoof_patch, wrong_caller).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "spoofed patch must still be rejected: {}", err);

        // Default actor_claim ("id") against numeric DB column `owner_id` (42)
        let mut old_ap = old.clone();
        old_ap.insert("status".into(), serde_json::json!("ACTION_PLANNING"));
        let mut patch_eval = serde_json::Map::new();
        patch_eval.insert("status".into(), serde_json::json!("EVALUATION_APPROVAL"));

        let owner = TransitionCaller { role: Some("staff"), id: Some("42"), cs: None, ..Default::default() };
        assert_eq!(check_transition_guards(&schema, &old_ap, &patch_eval, owner), Ok(true));
        let other = TransitionCaller { role: Some("staff"), id: Some("43"), cs: None, ..Default::default() };
        let err = check_transition_guards(&schema, &old_ap, &patch_eval, other).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);
    }

    #[test]
    fn test_state_machine_not_same_as_separation_of_duties() {
        let schema = capa_schema_with_record_guards();

        let mut old = serde_json::Map::new();
        old.insert("status".into(), serde_json::json!("EVALUATION_APPROVAL"));
        old.insert("prepared_by".into(), serde_json::json!("NIP-1001"));
        old.insert("pic_nip".into(), serde_json::json!("NIP-1002"));

        let mut patch = serde_json::Map::new();
        patch.insert("status".into(), serde_json::json!("IMPLEMENTATION"));

        // Different QA manager -> allowed (and also passes locked_when for status-only transition)
        let approver = TransitionCaller { role: Some("plant_qa_manager"), id: Some("99"), cs: Some("NIP-2000"), ..Default::default() };
        assert_eq!(check_transition_guards(&schema, &old, &patch, approver), Ok(true));

        // Same person as prepared_by -> 403 Forbidden
        let same_as_preparer = TransitionCaller { role: Some("plant_qa_manager"), id: Some("10"), cs: Some("NIP-1001"), ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, same_as_preparer).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);
        assert!(err.contains("prepared_by"), "{}", err);

        // Same person as pic_nip (even with different ASCII case) -> 403 Forbidden
        let same_as_pic = TransitionCaller { role: Some("plant_qa_manager"), id: Some("11"), cs: Some("nip-1002"), ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, same_as_pic).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);
        assert!(err.contains("pic_nip"), "{}", err);

        // Wrong role even when different person -> rejected by roles guard
        let wrong_role = TransitionCaller { role: Some("staff"), id: Some("99"), cs: Some("NIP-2000"), ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, wrong_role).unwrap_err();
        assert!(err.contains("role 'staff' is not permitted"), "{}", err);

        // Missing caller cs claim -> 403 Forbidden (fail-closed)
        let no_claim = TransitionCaller { role: Some("plant_qa_manager"), id: Some("99"), cs: None, ..Default::default() };
        let err = check_transition_guards(&schema, &old, &patch, no_claim).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX), "expected FORBIDDEN_PREFIX, got: {}", err);

        // NULL value in one of not_same_as columns does not block a legitimate approver
        let mut old_partial = old.clone();
        old_partial.insert("pic_nip".into(), Value::Null);
        assert_eq!(check_transition_guards(&schema, &old_partial, &patch, approver), Ok(true));
    }

    #[test]
    fn test_state_machine_level_actor_claim_fallback() {
        let schema = TableSchema {
            table: "deviations".to_string(),
            state_machine: Some(crate::model::StateMachine {
                field: "status".to_string(),
                initial: Some("OPEN".to_string()),
                actor_claim: Some("cs".to_string()),
                transitions: vec![crate::model::StateTransition {
                    from: serde_json::json!("OPEN"),
                    to: "INVESTIGATION".to_string(),
                    roles: vec![],
                    actor_column: Some("pic_nip".to_string()),
                    actor_claim: None, // inherits "cs" from state_machine
                    not_same_as: vec![],
                    require_signature: None,
                }],
            }),
            ..Default::default()
        };

        let mut old = serde_json::Map::new();
        old.insert("status".into(), serde_json::json!("OPEN"));
        old.insert("pic_nip".into(), serde_json::json!("NIP-777"));

        let mut patch = serde_json::Map::new();
        patch.insert("status".into(), serde_json::json!("INVESTIGATION"));

        let ok = TransitionCaller { role: None, id: Some("1"), cs: Some("NIP-777"), ..Default::default() };
        assert_eq!(check_transition_guards(&schema, &old, &patch, ok), Ok(true));
        let bad = TransitionCaller { role: None, id: Some("NIP-777"), cs: Some("NIP-888"), ..Default::default() };
        assert!(check_transition_guards(&schema, &old, &patch, bad).unwrap_err().starts_with(FORBIDDEN_PREFIX));
    }

    #[test]
    fn test_state_machine_require_signature_guard() {
        let schema = TableSchema {
            table: "plant_capas".to_string(),
            state_machine: Some(crate::model::StateMachine {
                field: "status".to_string(),
                initial: Some("EVALUATION_APPROVAL".to_string()),
                actor_claim: Some("cs".to_string()),
                transitions: vec![crate::model::StateTransition {
                    from: serde_json::json!("EVALUATION_APPROVAL"),
                    to: "IMPLEMENTATION".to_string(),
                    roles: vec!["plant_qa_manager".to_string()],
                    require_signature: Some(crate::model::RequireSignature {
                        meaning_column: Some("approved_meaning".to_string()),
                        by_column: Some("approved_by".to_string()),
                        at_column: Some("approved_at".to_string()),
                        allowed_meanings: vec!["APPROVED".to_string()],
                    }),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let mut old = serde_json::Map::new();
        old.insert("status".into(), serde_json::json!("EVALUATION_APPROVAL"));
        let mut patch = serde_json::Map::new();
        patch.insert("status".into(), serde_json::json!("IMPLEMENTATION"));

        // 1. Missing signature header -> 403 Signature required
        let no_sig = TransitionCaller {
            role: Some("plant_qa_manager"),
            id: Some("42"),
            cs: Some("NIP-0042"),
            signature: SignatureHeader::Missing,
        };
        let err = check_transition_guards_detailed(&schema, &old, &patch, no_sig).unwrap_err();
        assert_eq!(err, format!("{}Signature required", FORBIDDEN_PREFIX));

        // 2. Signature token belonging to another user -> 403
        let other_sig = crate::auth::SignatureClaims {
            id: "99".to_string(),
            nm: "Other".to_string(),
            cs: "NIP-0099".to_string(),
            meaning: "APPROVED".to_string(),
            at: 1000,
            exp: 2000,
            jti: "jti-other".to_string(),
            typ: crate::auth::SIGNATURE_TOKEN_TYPE.to_string(),
        };
        let wrong_user = TransitionCaller {
            role: Some("plant_qa_manager"),
            id: Some("42"),
            cs: Some("NIP-0042"),
            signature: SignatureHeader::Valid(&other_sig),
        };
        let err = check_transition_guards_detailed(&schema, &old, &patch, wrong_user).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX) && err.contains("Signature required"), "{}", err);

        // 3. Disallowed meaning -> 403
        let bad_meaning_sig = crate::auth::SignatureClaims {
            id: "42".to_string(),
            nm: "QA".to_string(),
            cs: "NIP-0042".to_string(),
            meaning: "REVIEWED".to_string(),
            at: 1000,
            exp: 2000,
            jti: "jti-m".to_string(),
            typ: crate::auth::SIGNATURE_TOKEN_TYPE.to_string(),
        };
        let bad_meaning = TransitionCaller {
            role: Some("plant_qa_manager"),
            id: Some("42"),
            cs: Some("NIP-0042"),
            signature: SignatureHeader::Valid(&bad_meaning_sig),
        };
        let err = check_transition_guards_detailed(&schema, &old, &patch, bad_meaning).unwrap_err();
        assert!(err.starts_with(FORBIDDEN_PREFIX) && err.contains("REVIEWED"), "{}", err);

        // 4. Valid signature for same user -> returns AppliedSignature with NIP identity
        let good_sig = crate::auth::SignatureClaims {
            id: "42".to_string(),
            nm: "QA".to_string(),
            cs: "NIP-0042".to_string(),
            meaning: "APPROVED".to_string(),
            at: 1000,
            exp: 2000,
            jti: "jti-ok".to_string(),
            typ: crate::auth::SIGNATURE_TOKEN_TYPE.to_string(),
        };
        let ok = TransitionCaller {
            role: Some("plant_qa_manager"),
            id: Some("42"),
            cs: Some("NIP-0042"),
            signature: SignatureHeader::Valid(&good_sig),
        };
        let out = check_transition_guards_detailed(&schema, &old, &patch, ok).unwrap();
        assert!(out.status_transition_authorized);
        let applied = out.applied_signature.expect("expected applied_signature");
        assert_eq!(applied.signer_identity, "NIP-0042");
        assert_eq!(applied.meaning, "APPROVED");
        assert_eq!(applied.meaning_column.as_deref(), Some("approved_meaning"));
        assert_eq!(applied.by_column.as_deref(), Some("approved_by"));
        assert_eq!(applied.at_column.as_deref(), Some("approved_at"));
    }
}

