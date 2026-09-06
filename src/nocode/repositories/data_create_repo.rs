use actix_web::web;
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::database::state::DbParam;
use crate::model::{Column, DbType, Index, TableSchema};
use crate::storage::sql_store::{InsertValue, SqlStore, UniqueCheck};
// use crate::helpers::extract_expressions;
use crate::log::log_output;
use crate::storage::ast::{Filter as F, Query as Q};
// use crate::crypt::{encrypt, is_encrypted_string};

/// Represents a pre-prepared detail batch ready for instant bulk insertion inside the transaction.
#[derive(Debug, Clone, Default)]
pub struct PreparedDetailBatch {
    pub field: String,
    pub target_table: String,
    pub foreign_key_column: String,
    pub fk_type_data: String,
    pub columns: Vec<String>,
    pub fk_index: usize,
    pub rows: Vec<Vec<InsertValue>>,
    pub response_items: Vec<serde_json::Map<String, Value>>,
}

/// Batch validate foreign keys in a single query for better performance (optimization)
/// This replaces N sequential DB queries with 1 UNION ALL query
pub(crate) async fn validate_foreign_keys_batch(
    state: &web::Data<AppState>,
    // (col_name, ref_table, ref_column, value, type_data) — type_data is the FK column's own
    // type so the value can be bound with the correct DbParam variant (avoids comparing an
    // int PK against a TEXT-bound value, which silently fails FK validation on some backends).
    fk_checks: &[(String, String, String, String, String)],
) -> Result<(), String> {
    if fk_checks.is_empty() {
        return Ok(());
    }

    // MongoDB Implementation
    if state.db_type == DbType::Mongodb {
        for (col, table, ref_col, val, type_data) in fk_checks {
            // For each FK, perform a query to check existence
            // This is N queries, but unavoidable without relational joins or stored procedures in Mongo

            let val_qv = match crate::nocode::pk_utils::dbparam_from_str_and_type(val, type_data) {
                DbParam::I64(n) => crate::storage::ast::Val::I64(n),
                DbParam::F64(f) => crate::storage::ast::Val::F64(f),
                DbParam::Bool(b) => crate::storage::ast::Val::Bool(b),
                DbParam::Null => crate::storage::ast::Val::Null,
                DbParam::Str(s) => crate::storage::ast::Val::Str(s),
            };

            let q = Q::from(table.clone())
                .select(vec![ref_col.clone()])
                .r#where(F::Eq(ref_col.clone(), val_qv))
                .limit(1);

            let rows = state
                .store
                .query(&q)
                .await
                .map_err(|e| format!("Error validating FK (mongo): {}", e))?;
            if rows.is_empty() {
                return Err(format!(
                    "Invalid foreign key value for column '{}' referencing table '{}'",
                    col, table
                ));
            }
        }
        return Ok(());
    }

    log_output(
        "OPTIMIZATION",
        "FK BATCH CHECK",
        "POST",
        format!("Validating {} foreign keys in batch", fk_checks.len()),
        true,
    );

    // Use SqlStore to build the query safely
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let (union_sql, params) = ds
        .preview_validate_fk_batch(fk_checks)
        .map_err(|e| format!("Error building FK check query: {}", e))?;

    log_output("QUERY", "FK BATCH", "POST", union_sql.clone(), true);
    log_output("PARAMS", "FK BATCH", "POST", format!("{:?}", params), true);

    let results = state
        .db
        .query_with_params(&union_sql, params)
        .await
        .map_err(|e| format!("FK batch validation failed: {}", e))?;

    for row in results {
        let is_valid = match row.get("_valid") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Number(n)) => n.as_i64() == Some(1),
            _ => false,
        };

        if !is_valid {
            let col = row
                .get("_col")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let tbl = row
                .get("_table")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            log_output(
                "ERROR",
                "FK VALIDATION",
                "POST",
                format!(
                    "Invalid foreign key: column={}, reference_table={}",
                    col, tbl
                ),
                false,
            );
            return Err(format!(
                "Invalid foreign key value for column '{}' referencing table '{}'",
                col, tbl
            ));
        }
    }
    Ok(())
}

/// Batch unique constraint validation to avoid N+1 queries
async fn validate_unique_constraints_batch(
    state: &web::Data<AppState>,
    table: &str,
    indexes: &[Index],
    columns_meta: &[Column],
    body: &Value,
) -> Result<(), String> {
    let mut unique_checks: Vec<UniqueCheck> = Vec::new();
    let mut idx_cols_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for ix in indexes.iter().filter(|ix| ix.unique) {
        let mut ix_params: Vec<(String, DbParam)> = Vec::with_capacity(ix.columns.len());
        let mut all_present = true;
        for col_name in &ix.columns {
            let meta = columns_meta.iter().find(|c| &c.name == col_name);
            let raw_val_opt = body.get(col_name).cloned();
            let val = match raw_val_opt {
                Some(v) => v,
                None => {
                    all_present = false;
                    Value::Null
                }
            };
            if !all_present {
                break;
            }
            let is_empty = match &val {
                Value::Null => true,
                Value::String(s) => s.trim().is_empty(),
                _ => false,
            };
            if is_empty {
                all_present = false;
                break;
            }

            let param = if let Some(m) = meta {
                let td = m.type_data.to_lowercase();
                match (&val, td.as_str()) {
                    (Value::Number(n), t)
                        if t.contains("float")
                            || t.contains("double")
                            || t.contains("decimal")
                            || t.contains("money") =>
                    {
                        DbParam::F64(n.as_f64().unwrap_or(0.0))
                    }
                    (Value::Number(n), t) if t.contains("int") => {
                        DbParam::I64(n.as_i64().unwrap_or(0))
                    }
                    (Value::String(s), t) if t.contains("int") => {
                        if let Ok(nn) = s.parse::<i64>() {
                            DbParam::I64(nn)
                        } else if let Ok(ff) = s.parse::<f64>() {
                            DbParam::F64(ff)
                        } else {
                            DbParam::Str(s.clone())
                        }
                    }
                    (Value::String(s), t)
                        if t.contains("float")
                            || t.contains("double")
                            || t.contains("decimal")
                            || t.contains("money") =>
                    {
                        if let Ok(ff) = s.parse::<f64>() {
                            DbParam::F64(ff)
                        } else if let Ok(nn) = s.parse::<i64>() {
                            DbParam::I64(nn)
                        } else {
                            DbParam::Str(s.clone())
                        }
                    }
                    (Value::Bool(b), _) => DbParam::Bool(*b),
                    (Value::Null, _) => {
                        all_present = false;
                        break;
                    }
                    (other, _) => DbParam::Str(other.to_string().trim_matches('"').to_string()),
                }
            } else {
                match val {
                    Value::Number(n) => DbParam::Str(n.to_string()),
                    Value::String(s) => DbParam::Str(s),
                    Value::Bool(b) => DbParam::Bool(b),
                    _ => DbParam::Null,
                }
            };

            if !all_present {
                break;
            } // Check again if break was triggered inside inner blocks
            ix_params.push((col_name.clone(), param));
        }

        if all_present {
            unique_checks.push(UniqueCheck {
                index_name: ix.name.clone(),
                columns: ix_params,
            });
            idx_cols_map.insert(ix.name.clone(), ix.columns.clone());
        }
    }

    if unique_checks.is_empty() {
        return Ok(());
    }

    // MongoDB Implementation
    if state.db_type == DbType::Mongodb {
        for check in unique_checks {
            let mut filters = Vec::new();
            for (col, param) in check.columns {
                let val = match param {
                    DbParam::Str(s) => crate::storage::ast::Val::Str(s),
                    DbParam::I64(i) => crate::storage::ast::Val::I64(i),
                    DbParam::F64(f) => crate::storage::ast::Val::F64(f),
                    DbParam::Bool(b) => crate::storage::ast::Val::Bool(b),
                    DbParam::Null => crate::storage::ast::Val::Null,
                    // _ => crate::storage::ast::Val::Null, // binary etc not supported in simple unique ck yet
                };
                filters.push(F::Eq(col, val));
            }
            if filters.is_empty() {
                continue;
            }

            let q = Q::from(table.to_string())
                .select(vec!["_id".to_string()])
                .r#where(F::And(filters))
                .limit(1);

            let rows = state
                .store
                .query(&q)
                .await
                .map_err(|e| format!("Error validating Unique (mongo): {}", e))?;
            if !rows.is_empty() {
                return Err(format!(
                    "Unique constraint violation on index: {}",
                    check.index_name
                ));
            }
        }
        return Ok(());
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let (union_sql, params) = ds
        .preview_validate_unique_batch(table, &unique_checks, None)
        .map_err(|e| format!("Error building unique check query: {}", e))?;

    log_output("QUERY", "UNIQUE BATCH", "POST", union_sql.clone(), true);
    log_output(
        "PARAMS",
        "UNIQUE BATCH",
        "POST",
        format!("{:?}", params),
        true,
    );

    let rows = state
        .db
        .query_with_params(&union_sql, params)
        .await
        .map_err(|e| format!("Unique batch validation failed: {}", e))?;

    for r in rows {
        let is_dup = match r.get("_dup") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Number(n)) => n.as_i64() == Some(1),
            _ => false,
        };

        if is_dup {
            let idx = r.get("_idx").and_then(|v| v.as_str()).unwrap_or("");
            let cols = idx_cols_map.get(idx).cloned().unwrap_or_default();
            return Err(format!(
                "Unique constraint violation on columns: {}",
                cols.join(", ")
            ));
        }
    }
    Ok(())
}

/// Fetch the next running-number for a custom-ID `ID` token from an external endpoint.
///
/// The endpoint URL supports `{request.field}` interpolation against the request `body`.
/// The already-built ID prefix (e.g. `SO/2026/01`) is appended as a `prefix` query param so
/// the endpoint can compute the sequence per-prefix. The response is expected to be JSON; the
/// number is read from `resp_path` (dotted path, e.g. `data`) and coerced to an integer.
///
/// There is no fallback: any failure (network, non-success status, missing/invalid field)
/// returns an `Err` so the insert is aborted.
async fn fetch_next_number_from_endpoint(
    function_endpoint: &str,
    function_endpoint_path: &str,
    id_prefix: &str,
    body: &Value,
    auth_token: Option<&str>,
) -> Result<i64, String> {
    // Interpolate {request.field} placeholders in the configured URL.
    let mut url = crate::database::state::build_url_from_formula(function_endpoint, body)
        .map_err(|e| format!("ID endpoint URL build failed: {}", e))?;

    // Append the built prefix so the endpoint can scope the sequence.
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push_str(&format!("{}prefix={}", sep, urlencoding::encode(id_prefix)));

    // url = http://localhost:8080/api/next-sequence?prefix=SO%2F2026%2F

    // prefix = SO/2026
    // data = SO/2026/0002
    // data = 0002

    log_output("DEBUG", "ID ENDPOINT", "GET", url.clone(), true);

    let client = reqwest::Client::new();
    let mut builder = client.get(&url);
    if let Some(token) = auth_token {
        builder = builder.header("Authorization", token);
    }

    let res = builder
        .send()
        .await
        .map_err(|e| format!("ID endpoint request failed: {}", e))?;

    if !res.status().is_success() {
        let status = res.status();
        let msg = res
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(format!("ID endpoint returned {}: {}", status, msg));
    }

    let json: Value = res
        .json()
        .await
        .map_err(|e| format!("ID endpoint response is not valid JSON: {}", e))?;

    let val = crate::database::state::get_by_path_value(&json, function_endpoint_path).ok_or_else(
        || {
            format!(
                "ID endpoint response missing field '{}'",
                function_endpoint_path
            )
        },
    )?;

    match val {
        Value::Number(n) => n.as_i64().ok_or_else(|| {
            format!(
                "ID endpoint field '{}' is not an integer: {}",
                function_endpoint_path, n
            )
        }),
        // jika type string maka replace prefix terlebih dahulu
        // 0009/CC/2026/06 prefix CC/2026/06 = 0009
        //
        Value::String(s) => s
            .trim()
            .replace(id_prefix, "")
            .replace("/", "")
            .parse::<i64>()
            .map_err(|_| {
                format!(
                    "ID endpoint field '{}' is not a number: {}",
                    function_endpoint_path, s
                )
            }),
        other => Err(format!(
            "ID endpoint field '{}' has unsupported type: {}",
            function_endpoint_path, other
        )),
    }
}

/// Compute the next running-number for a custom-ID `ID` token by reading the highest existing id
/// that shares the given prefix and incrementing its numeric suffix (`MAX(id)+1`).
///
/// `len_id` is the width of the numeric suffix (e.g. `000ID` -> 3). For SQL backends the lookup
/// runs inside the open transaction (`tx_opt`) so concurrent inserts stay consistent.
async fn query_next_number_from_max(
    state: &web::Data<AppState>,
    tx_opt: &mut Option<Box<dyn crate::storage::traits::TxStore>>,
    table: &str,
    id_prefix: &str,
    len_id: usize,
) -> Result<i64, String> {
    // Find latest id by prefix.
    let like_pat = if id_prefix.is_empty() {
        "%".to_string()
    } else {
        format!("{}%", id_prefix)
    };

    let q_max = Q::from(table.to_string())
        .select(["id"])
        .r#where(F::ILike("id".into(), like_pat))
        .order_by("id", false)
        .limit(1);

    let max_id: String = if state.db_type == DbType::Mongodb {
        match state.store.query(&q_max).await {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("id")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "0".to_string()),
            _ => "0".to_string(),
        }
    } else {
        let tx = tx_opt
            .as_mut()
            .ok_or_else(|| "Transaction not available for ID generation".to_string())?;
        match tx.query(&q_max).await {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("id")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "0".to_string()),
            _ => "0".to_string(),
        }
    };

    // Extract numeric suffix (width = len_id) and increment
    let mut current_num: i64 = 0;
    if max_id.len() >= len_id {
        let suffix = &max_id[max_id.len() - len_id..];
        current_num = suffix.parse::<i64>().unwrap_or(0);
    }
    Ok(current_num + 1)
}

// Suppress too_many_arguments as refactoring this signature affects many calls
// Suppress too_many_arguments as refactoring this signature affects many calls
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
pub async fn perform_insert(
    state: &web::Data<AppState>,
    table_schema: &Arc<TableSchema>,
    body: &Value,
    _insert_columns: &[&str],
    _filtered_columns: &[&Column],
    mut insert_fields: Vec<(String, InsertValue)>,
    mut doc_map: serde_json::Map<String, Value>,
    fk_checks: Vec<(String, String, String, String, String)>,
    function_id_split: Vec<String>,
    mut prepared_details: Vec<PreparedDetailBatch>,
    route: &str,
    auth_token: Option<String>,
) -> Result<(String, i64, Value), String> {
    // Batch validate all foreign keys in one query
    validate_foreign_keys_batch(state, &fk_checks).await?;

    // Batch validate unique constraints when all indexed columns are present
    // For MongoDB, we must call this explicitly as well
    validate_unique_constraints_batch(
        state,
        &table_schema.table,
        &table_schema.indexes,
        &table_schema.columns,
        body,
    )
    .await?;

    // Begin transaction early for SQL backends so ID generation runs in the same TX
    let mut tx_opt: Option<Box<dyn crate::storage::traits::TxStore>> = None;
    if state.db_type != DbType::Mongodb {
        match state.store.begin_tx().await {
            Ok(t) => tx_opt = Some(t),
            Err(err) => return Err(format!("Error starting transaction: {}", err)),
        }
    }

    // Handle Custom ID Generation if needed
    if !function_id_split.is_empty() {
        // Resolve optional sequence endpoint configured on the id column. When set, the
        // numeric `ID` token is fetched from this endpoint instead of MAX(id)+1.
        let id_col = table_schema.columns.iter().find(|c| c.name == "id");
        let seq_endpoint = id_col
            .map(|c| c.function_endpoint.clone())
            .filter(|s| !s.trim().is_empty());
        let function_endpoint_path = id_col
            .map(|c| c.function_endpoint_path.clone())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "data".to_string());

        // Build parts without leading slash; join with '/'
        let mut parts: Vec<String> = Vec::new();
        // lokasi urutan ID ada dimana
        let mut loc_id: usize = 0;
        let mut len_id: usize = 0;

        // 000ID/CC/2026/06
        // part = ['CC', '2026', '06']

        for (i_urut, token) in function_id_split.iter().enumerate() {
            if token == "%Y" {
                parts.push(chrono::Utc::now().format("%Y").to_string());
            } else if token == "%m" {
                parts.push(chrono::Utc::now().format("%m").to_string());
            } else if token == "%d" {
                parts.push(chrono::Utc::now().format("%d").to_string());
            } else if token == "%mROMAWI" {
                let m = chrono::Utc::now().format("%m").to_string();
                let roman = match m.as_str() {
                    "01" => "I",
                    "02" => "II",
                    "03" => "III",
                    "04" => "IV",
                    "05" => "V",
                    "06" => "VI",
                    "07" => "VII",
                    "08" => "VIII",
                    "09" => "IX",
                    "10" => "X",
                    "11" => "XI",
                    "12" => "XII",
                    _ => "",
                };
                parts.push(roman.to_string());
            } else if token.contains("ID") {
                loc_id = i_urut;
                // Numeric suffix with zero-padding based on token, e.g. 000ID -> width 3
                let s_append = token.replace("ID", "");
                len_id = s_append.len();
            } else if token.starts_with('{') && token.ends_with('}') {
                // Placeholder, likely {request.field}
                let key = token.trim_start_matches('{').trim_end_matches('}');
                if let Some(stripped) = key.strip_prefix("request.") {
                    let val = body
                        .get(stripped)
                        .map(|v| v.to_string().replace('"', "").replace("null", ""))
                        .unwrap_or_default();
                    if !val.is_empty() {
                        parts.push(val);
                    }
                }
            } else if !token.is_empty() {
                // literal segment
                parts.push(token.clone());
            }
        }

        let id_prefix = parts.join("/");
        // id_prefix = CC/2026/06
        let next_num: i64 = if let Some(function_endpoint) = &seq_endpoint {
            // Get the running number from the configured endpoint (no fallback).
            // 0009/CC/2026/06
            fetch_next_number_from_endpoint(
                function_endpoint,
                &function_endpoint_path,
                &id_prefix,
                body,
                auth_token.as_deref(),
            )
            .await?
        } else {
            // No endpoint configured: derive the number from MAX(id)+1.
            query_next_number_from_max(state, &mut tx_opt, &table_schema.table, &id_prefix, len_id)
                .await?
        };

        let next_str = format!("{:0width$}", next_num, width = len_id);
        // sisipkan next_str ke parts index loc_id
        if loc_id != usize::MAX {
            parts.insert(loc_id, next_str);
        }

        let id = parts.join("/");
        // after building from tokens, bind id into params and AST fields
        doc_map.insert("id".to_string(), serde_json::json!(id.clone()));
        insert_fields.push(("id".to_string(), InsertValue::Param(DbParam::Str(id))));
    }

    // For MongoDB, directly insert JSON doc without SQL; for SQL, compile INSERT
    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());

    if state.db_type == DbType::Mongodb {
        let doc_json = Value::Object(doc_map.clone());
        match state.store.insert(&table_schema.table, doc_json).await {
            Ok(returned_val) => {
                let mut final_doc = doc_map;
                if returned_val != Value::Null && !final_doc.contains_key("id") {
                    final_doc.insert("id".to_string(), returned_val.clone());
                }
                let header_id = final_doc.get("id").cloned().unwrap_or(Value::Null);

                // Insert details in MongoDB
                for batch in &mut prepared_details {
                    for resp in &mut batch.response_items {
                        resp.insert(batch.foreign_key_column.clone(), header_id.clone());
                        let detail_json = Value::Object(resp.clone());
                        let _ = state
                            .store
                            .insert(&batch.target_table, detail_json)
                            .await
                            .map_err(|e| format!("Error inserting detail (mongo): {}", e))?;
                    }
                    final_doc.insert(
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

                Ok((
                    "Data inserted successfully".to_string(),
                    1,
                    Value::Object(final_doc),
                ))
            }
            Err(e) => Err(format!("Error NCO-POST (mongo): {}", e)),
        }
    } else {
        // Try to include RETURNING id when supported
        let returning_cols: Vec<&str> = vec!["id"]; // core id fetch
        let attempt =
            ds.preview_insert_with_returning(&table_schema.table, &insert_fields, &returning_cols);

        let mut tx = match tx_opt {
            Some(t) => t,
            None => return Err("Transaction not available for insert".to_string()),
        };

        // Execute VALIDATE_DATA (API based) if exists
        if table_schema.post.validate_data.starts_with("API:") {
            if let Err(e) = crate::nocode::validate::validate_api_formula(
                &table_schema.post.validate_data,
                body,
                auth_token.as_deref(),
            )
            .await
            {
                let _ = tx.rollback().await;
                return Err(e);
            }
        }

        // Execute VALIDATE_DATA (SQL based) if exists
        if table_schema.post.validate_data.contains("SQL:") {
            match crate::database::state::build_sql_and_params_from_formula(
                &table_schema.post.validate_data,
                body,
            ) {
                Ok((built_sql, params)) => match tx.raw_sql(&built_sql, params).await {
                    Ok(row) => {
                        if !row.is_empty() {
                            let is_valid = row[0].get(0).and_then(|v| v.as_bool()).unwrap_or(true);
                            if !is_valid {
                                let _ = tx.rollback().await;
                                return Err("Validation data from table is not valid. Please contact your administrator".to_string());
                            }
                        } else {
                            let _ = tx.rollback().await;
                            return Err("Validation data from table is empty. Please contact your administrator".to_string());
                        }
                    }
                    Err(e) => {
                        let _ = tx.rollback().await;
                        return Err(format!("Error validating data: {}", e));
                    }
                },
                Err(e) => {
                    let _ = tx.rollback().await;
                    return Err(format!("Error building validation SQL: {}", e));
                }
            }
        }

        // PRE-PROCESS (SQL based)
        if table_schema.post.pre_process.contains("SQL:") {
            if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                &mut tx,
                table_schema.post.pre_process.clone(),
                body,
                route,
            )
            .await
            {
                let _ = tx.rollback().await;
                return Err(format!("Error in pre-process: {}", err));
            }
        }

        // EXECUTE INSERT
        match attempt.or_else(|_| ds.preview_insert_with(&table_schema.table, &insert_fields)) {
            Ok((sql, params)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "POST(AST)", route, sql.clone(), true);
                    log_output("PARAMS", "POST(AST)", route, format!("{:?}", params), true);
                }

                match tx.raw_sql(&sql, params).await {
                    Ok(rows_returned) => {
                        let mut inserted_id = if !rows_returned.is_empty() {
                            // Try to get "id" column
                            rows_returned[0]
                                .get("id")
                                .cloned()
                                .or_else(|| rows_returned[0].get(0).cloned()) // Or first column
                                .unwrap_or(Value::Null)
                        } else {
                            // Fallback to what we generated/passed
                            doc_map.get("id").cloned().unwrap_or(Value::Null)
                        };

                        // For MySQL auto_increment if id is not yet retrieved
                        if inserted_id == Value::Null && state.db_type == DbType::Mysql {
                            if let Ok(id_rows) = tx.raw_sql("SELECT LAST_INSERT_ID() as id", vec![]).await {
                                if !id_rows.is_empty() {
                                    inserted_id = id_rows[0].get("id").cloned().unwrap_or(Value::Null);
                                }
                            }
                        }

                        if inserted_id != Value::Null && !doc_map.contains_key("id") {
                            doc_map.insert("id".to_string(), inserted_id.clone());
                        }

                        // Bulk insert prepared details inside active transaction
                        for batch in &mut prepared_details {
                            if !batch.rows.is_empty() {
                                let fk_param = if let Some(n) = inserted_id.as_i64() {
                                    DbParam::I64(n)
                                } else if let Some(s) = inserted_id.as_str() {
                                    if batch.fk_type_data.to_lowercase().contains("int") {
                                        if let Ok(n) = s.parse::<i64>() {
                                            DbParam::I64(n)
                                        } else {
                                            DbParam::Str(s.to_string())
                                        }
                                    } else {
                                        DbParam::Str(s.to_string())
                                    }
                                } else {
                                    DbParam::Str(inserted_id.to_string())
                                };

                                for r in &mut batch.rows {
                                    if batch.fk_index < r.len() {
                                        r[batch.fk_index] = InsertValue::Param(fk_param.clone());
                                    }
                                }

                                for resp in &mut batch.response_items {
                                    resp.insert(batch.foreign_key_column.clone(), inserted_id.clone());
                                }

                                let (bulk_sql, bulk_params) = ds
                                    .preview_insert_bulk(&batch.target_table, &batch.columns, &batch.rows)
                                    .map_err(|e| format!("Error building bulk insert for {}: {}", batch.target_table, e))?;

                                if *crate::ISDEBUG {
                                    log_output("QUERY", "POST DETAIL BULK", route, bulk_sql.clone(), true);
                                    log_output("PARAMS", "POST DETAIL BULK", route, format!("{:?}", bulk_params), true);
                                }

                                let built_bulk = crate::database::state::rehydrate_placeholders(&bulk_sql, state.db_type.as_str());
                                if let Err(e) = tx.raw_sql(&built_bulk, bulk_params).await {
                                    let _ = tx.rollback().await;
                                    return Err(format!("Error inserting detail items into {}: {}", batch.target_table, e));
                                }
                            }

                            // Embed detail items into response doc_map
                            doc_map.insert(
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

                        // Construct merged body containing doc_map (which includes generated id and fields)
                        let merged_body = {
                            let mut map = match body {
                                Value::Object(m) => m.clone(),
                                _ => serde_json::Map::new(),
                            };
                            for (k, v) in &doc_map {
                                map.insert(k.clone(), v.clone());
                            }
                            // Map id_new to id if id_new is not explicitly provided in the request
                            if !map.contains_key("id_new") {
                                if let Some(id_val) = map.get("id") {
                                    map.insert("id_new".to_string(), id_val.clone());
                                }
                            }
                            Value::Object(map)
                        };

                        // POST-PROCESS (SQL based)
                        if table_schema.post.post_process.contains("SQL:") {
                            if let Err(err) =
                                crate::database::state::execute_sql_formula_with_txstore(
                                    &mut tx,
                                    table_schema.post.post_process.clone(),
                                    &merged_body,
                                    route,
                                )
                                .await
                            {
                                let _ = tx.rollback().await;
                                return Err(format!("Error in post-process: {}", err));
                            }
                        }

                        // Execute Action Triggers for on_create
                        if !table_schema.action_triggers.is_empty() {
                            let id_str = doc_map
                                .get("id")
                                .map(|v| match v {
                                    Value::String(s) => s.clone(),
                                    Value::Number(n) => n.to_string(),
                                    _ => v.to_string(),
                                })
                                .unwrap_or_default();

                            let empty_old = serde_json::Map::new();
                            let trigger_ctx = crate::nocode::trigger_engine::TriggerContext {
                                parent_table: &table_schema.table,
                                parent_pk: &id_str,
                                old_record: &empty_old,
                                new_record: &doc_map,
                                request_body: body,
                                actor_id: None,
                            };

                            match crate::nocode::trigger_engine::execute_triggers(
                                state.db_type.clone(),
                                &mut *tx,
                                table_schema,
                                &trigger_ctx,
                                "on_create",
                            )
                            .await
                            {
                                Ok(executed) => {
                                    for trig_name in executed {
                                        crate::audit::write_audit(&crate::audit::AuditEntry {
                                            at: chrono::Local::now().to_rfc3339(),
                                            actor_id: "system".to_string(),
                                            action: "TRIGGER_CREATE",
                                            route: &format!("{}:{}", route, trig_name),
                                            id: Some(&id_str),
                                            ip: None,
                                        });
                                    }
                                }
                                Err(err) => {
                                    let _ = tx.rollback().await;
                                    return Err(format!("Action trigger on_create failed: {}", err));
                                }
                            }
                        }

                        if let Err(e) = tx.commit().await {
                            return Err(format!("Error committing transaction: {}", e));
                        }

                        Ok((
                            "Data inserted successfully".to_string(),
                            1,
                            Value::Object(doc_map),
                        ))
                    }
                    Err(e) => {
                        let _ = tx.rollback().await;
                        Err(format!("Error executing insert: {}", e))
                    }
                }
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(format!("Error compiling AST INSERT: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::state::DbParam;
    use crate::storage::sql_store::{InsertValue, SqlStore};

    #[test]
    fn test_repo_structure_compiles() {
        assert_eq!(1, 1);
    }

    #[actix_web::test]
    async fn test_prepared_detail_batch_fk_injection_and_bulk_sql() {
        // 1. Simulate pre-allocated PreparedDetailBatch before transaction
        let mut batch = PreparedDetailBatch {
            field: "items".to_string(),
            target_table: "transaction_purchase_order_item".to_string(),
            foreign_key_column: "purchase_order_id".to_string(),
            fk_type_data: "int".to_string(),
            columns: vec![
                "purchase_order_id".to_string(),
                "material_id".to_string(),
                "qty_ordered".to_string(),
                "unit_price".to_string(),
            ],
            fk_index: 0,
            rows: vec![
                vec![
                    InsertValue::Param(DbParam::Null), // placeholder for header FK
                    InsertValue::Param(DbParam::I64(10)),
                    InsertValue::Param(DbParam::F64(500.0)),
                    InsertValue::Param(DbParam::F64(15000.0)),
                ],
                vec![
                    InsertValue::Param(DbParam::Null), // placeholder for header FK
                    InsertValue::Param(DbParam::I64(20)),
                    InsertValue::Param(DbParam::F64(250.0)),
                    InsertValue::Param(DbParam::F64(8000.0)),
                ],
            ],
            response_items: vec![
                serde_json::json!({ "material_id": 10, "qty_ordered": 500.0, "unit_price": 15000.0 })
                    .as_object()
                    .unwrap()
                    .clone(),
                serde_json::json!({ "material_id": 20, "qty_ordered": 250.0, "unit_price": 8000.0 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ],
        };

        // 2. Simulate Header Insert resulting in header_id = 99
        let header_id = 99i64;
        let fk_param = DbParam::I64(header_id);

        for r in &mut batch.rows {
            r[batch.fk_index] = InsertValue::Param(fk_param.clone());
        }
        for resp in &mut batch.response_items {
            resp.insert(
                batch.foreign_key_column.clone(),
                serde_json::json!(header_id),
            );
        }

        // 3. Verify that FK slot was stamped with 99
        for r in &batch.rows {
            match &r[0] {
                InsertValue::Param(DbParam::I64(n)) => assert_eq!(*n, 99),
                _ => panic!("Expected DbParam::I64(99)"),
            }
        }
        for resp in &batch.response_items {
            assert_eq!(resp.get("purchase_order_id"), Some(&serde_json::json!(99)));
        }

        // 4. Test preview_insert_bulk compilation with SQLite pool
        let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let db_repo = std::sync::Arc::new(crate::database::sqlite::SqliteRepo { pool });

        let ds_pg = SqlStore::new(db_repo.clone(), "postgres".to_string());
        let (sql_pg, params_pg) = ds_pg
            .preview_insert_bulk(&batch.target_table, &batch.columns, &batch.rows)
            .expect("Failed to build Postgres bulk insert");

        assert!(sql_pg.contains("INSERT INTO transaction_purchase_order_item"));
        assert!(sql_pg.contains("purchase_order_id,material_id,qty_ordered,unit_price"));
        assert_eq!(params_pg.len(), 8); // 4 columns * 2 rows = 8 params

        let ds_mysql = SqlStore::new(db_repo, "mysql".to_string());
        let (sql_mysql, params_mysql) = ds_mysql
            .preview_insert_bulk(&batch.target_table, &batch.columns, &batch.rows)
            .expect("Failed to build MySQL bulk insert");

        assert!(sql_mysql.contains("INSERT INTO transaction_purchase_order_item"));
        assert_eq!(params_mysql.len(), 8);
    }

    #[test]
    fn test_master_detail_entity_schema_file() {
        let schema_json = include_str!("../../../config/entity/transaction_purchase_order.json");
        let schema: TableSchema =
            serde_json::from_str(schema_json).expect("Failed to deserialize transaction_purchase_order");

        assert_eq!(schema.table, "transaction_purchase_order");
        assert_eq!(schema.details.len(), 1);
        let detail = &schema.details[0];
        assert_eq!(detail.field, "items");
        assert_eq!(detail.target_table, "transaction_purchase_order_item");
        assert_eq!(detail.foreign_key_column, "purchase_order_id");
        assert_eq!(detail.update_strategy.as_deref(), Some("replace"));
        assert_eq!(detail.cascade_delete, Some(true));
    }
}
