use actix_multipart::Multipart;
use actix_web::{
    HttpResponse, Responder, web::{self, Data, Path}
};
use serde_json::Value;

use crate::{audit::{AuditEntry, write_audit}};
use crate::helpers::get_client_ip; // retained for other logic if needed; mutation rate limiting now global
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    database::state::{DbParam},
    helpers::{filter_table_schema, multipart_to_json},
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse, Index, Column},
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{SqlStore, InsertValue};
use crate::storage::ast::{Filter as QF, Val as QV};

/// Batch validate foreign keys in a single query (PUT path)
async fn validate_foreign_keys_batch_put(
    tx: &mut dyn crate::storage::traits::TxStore,
    fk_checks: &[(String, String, String, String)], // (col_name, ref_table, ref_column, value)
) -> Result<(), String> {
    if fk_checks.is_empty() { return Ok(()); }
    let mut queries = Vec::with_capacity(fk_checks.len());
    let mut params: Vec<crate::database::state::DbParam> = Vec::with_capacity(fk_checks.len());
    for (col_name, ref_table, ref_column, value) in fk_checks {
        queries.push(format!(
            "SELECT '{}' as _col, '{}' as _table, EXISTS(SELECT 1 FROM {} WHERE {} = ?) as _valid",
            col_name, ref_table, ref_table, ref_column
        ));
        params.push(crate::database::state::DbParam::Str(value.clone()));
    }
    let union_sql = queries.join(" UNION ALL ");
    if *crate::ISDEBUG { log_output("QUERY", "FK BATCH", "PUT", union_sql.clone(), true); }
    let rows = tx
        .raw_sql(&union_sql, params)
        .await
        .map_err(|e| format!("FK batch validation failed: {}", e))?;
    for r in rows {
        let ok = r.get("_valid").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let col = r.get("_col").and_then(|v| v.as_str()).unwrap_or("unknown");
            let tbl = r.get("_table").and_then(|v| v.as_str()).unwrap_or("unknown");
            return Err(format!("Invalid foreign key value for column '{}' referencing table '{}'", col, tbl));
        }
    }
    Ok(())
}

fn dbparam_from_value_and_type(val: &serde_json::Value, meta: Option<&Column>) -> crate::database::state::DbParam {
    if let Some(m) = meta {
        let td = m.type_data.to_lowercase();
        match (val, td.as_str()) {
            (serde_json::Value::Number(n), t) if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") => crate::database::state::DbParam::F64(n.as_f64().unwrap_or(0.0)),
            (serde_json::Value::Number(n), t) if t.contains("int") => crate::database::state::DbParam::I64(n.as_i64().unwrap_or(0)),
            (serde_json::Value::String(s), t) if t.contains("int") => {
                if let Ok(nn) = s.parse::<i64>() { crate::database::state::DbParam::I64(nn) }
                else if let Ok(ff) = s.parse::<f64>() { crate::database::state::DbParam::F64(ff) }
                else { crate::database::state::DbParam::Str(s.clone()) }
            }
            (serde_json::Value::String(s), t) if t.contains("float") || t.contains("double") || t.contains("decimal") || t.contains("money") => {
                if let Ok(ff) = s.parse::<f64>() { crate::database::state::DbParam::F64(ff) }
                else if let Ok(nn) = s.parse::<i64>() { crate::database::state::DbParam::I64(nn) }
                else { crate::database::state::DbParam::Str(s.clone()) }
            }
            (serde_json::Value::Bool(b), _) => crate::database::state::DbParam::Bool(*b),
            (serde_json::Value::Null, _) => crate::database::state::DbParam::Null,
            (other, _) => crate::database::state::DbParam::Str(other.to_string().trim_matches('"').to_string()),
        }
    } else {
        match val {
            serde_json::Value::Number(n) => crate::database::state::DbParam::Str(n.to_string()),
            serde_json::Value::String(s) => crate::database::state::DbParam::Str(s.clone()),
            serde_json::Value::Bool(b) => crate::database::state::DbParam::Bool(*b),
            _ => crate::database::state::DbParam::Null,
        }
    }
}

/// Batch unique validation for PUT: checks unique indexes impacted by this update.
/// It will use values from the request body (already transformed) and fetch missing
/// parts of composite indexes from DB in a single row select. Excludes current row by PK.
async fn validate_unique_constraints_batch_put(
    tx: &mut dyn crate::storage::traits::TxStore,
    table: &str,
    indexes: &[Index],
    columns_meta: &[Column],
    effective_values: &serde_json::Map<String, serde_json::Value>,
    pk_name: &str,
    pk_param: crate::database::state::DbParam,
) -> Result<(), String> {
    // Determine which unique indexes are affected (any column in index is present in effective_values)
    let mut queries: Vec<String> = Vec::new();
    let mut params: Vec<crate::database::state::DbParam> = Vec::new();
    for ix in indexes.iter().filter(|ix| ix.unique) {
        if !ix.columns.iter().any(|c| effective_values.contains_key(c)) {
            continue;
        }
        // Build predicates for all index columns; require that all column values are known (either from effective_values or will be fetched by caller beforehand)
        let mut local_params: Vec<crate::database::state::DbParam> = Vec::with_capacity(ix.columns.len() + 1);
        let mut conds: Vec<String> = Vec::with_capacity(ix.columns.len() + 1);
        let mut all_known = true;
        for col_name in &ix.columns {
            if let Some(v) = effective_values.get(col_name) {
                let meta = columns_meta.iter().find(|c| &c.name == col_name);
                local_params.push(dbparam_from_value_and_type(v, meta));
                conds.push(format!("{} = ?", col_name));
            } else {
                all_known = false; break;
            }
        }
        if !all_known { continue; }
        // Exclude current row
        conds.push(format!("{} <> ?", pk_name));
        local_params.push(pk_param.clone());
        queries.push(format!(
            "SELECT '{}' as _idx, EXISTS(SELECT 1 FROM {} WHERE {}) as _dup",
            ix.name, table, conds.join(" AND ")
        ));
        params.extend(local_params);
    }
    if queries.is_empty() { return Ok(()); }
    let union_sql = queries.join(" UNION ALL ");
    if *crate::ISDEBUG { log_output("QUERY", "UNIQUE BATCH", "PUT", union_sql.clone(), true); }
    let rows = tx
        .raw_sql(&union_sql, params)
        .await
        .map_err(|e| format!("Unique batch validation failed: {}", e))?;
    for r in rows {
        let dup = r.get("_dup").and_then(|v| v.as_bool()).unwrap_or(false);
        if dup { return Err("Unique constraint violation".to_string()); }
    }
    Ok(())
}

// NCO-PUT
pub async fn update(
    state: Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    multipart: Multipart,
    path: Path<String>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;
    let reference_foreign_keys = &schemas.1;
    
    let mut claims = Claims::default();
    if !state.route_publics.contains(&route) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };

        if !check_access(&claims, &route, "write") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let mut body = match multipart_to_json(multipart).await {
        Ok(json) => json,
        Err(e) => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Failed to parse multipart data: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    };
    // Rate limiting removed; enforced by middleware
    let id_raw: String = path.into_inner();
    
    let mut isqueue = false;
    if let Some(map) = parameters.clone().into_inner().as_object() {
        if let Some(v) = map.get("isqueue") {
            isqueue = *v == Value::Bool(true) || *v == Value::String("true".to_string());
        }
    }

    if state.write_queue_enabled && isqueue {
        let t0 = std::time::Instant::now();


        // auth check before enqueue
        let mut actor_id_opt: Option<String> = None;
        if !state.route_publics.contains(&route) {
            let req_for_auth = req.clone();
            let claims = match get_user_info_from_token(req_for_auth, state.clone()) {
                Ok(c) => c,
                Err(_) => {
                    return HttpResponse::Unauthorized().json(WebResponse {
                        success: false,
                        message: "Invalid token".to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            };
            if !check_access(&claims, &route, "write") {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Unauthorized".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
            actor_id_opt = Some(claims.id.clone());
            // include as hidden value for consumer
            if let Some(map) = body.as_object_mut() { map.insert("__actor_id__".into(), serde_json::json!(claims.id)); }
        }

        let job = crate::nocode::consumer::WriteJob {
            route: route.clone(),
            op: crate::nocode::consumer::WriteOpKind::Put { id: id_raw },
            body,
            headers: vec![],
            enqueued_at: chrono::Utc::now().to_rfc3339(),
            actor_id: actor_id_opt,
        };
        if state.write_queue_fast_ack {
            tokio::spawn(async move {
                let _ = crate::nocode::consumer::enqueue_job(&job).await;
            });
            log_output(
                "QUEUE",
                "PUT-HANDLER",
                route.as_str(),
                format!("queued (async) in {} ms", t0.elapsed().as_millis()),
                true,
            );
            return HttpResponse::Accepted().json(WebResponse {
                success: true,
                message: "Enqueued".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        } else {
            match crate::nocode::consumer::enqueue_job(&job).await {
                Ok(_) => {
                    log_output(
                        "QUEUE",
                        "PUT-HANDLER",
                        route.as_str(),
                        format!("queued in {} ms", t0.elapsed().as_millis()),
                        true,
                    );
                    return HttpResponse::Accepted().json(WebResponse {
                        success: true,
                        message: "Enqueued".to_string(),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
                Err(e) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Queue error: {}", e),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        }
    }

    // get body from request and compare with table_schemas.put.columns
    let table_schema = filter_table_schema(table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    // Collect update fields using expression-aware builder
    let mut update_fields: Vec<(String, InsertValue)> = Vec::new();
    let mut id_new = "".to_string();
    let mut password_override: Option<String> = None;
    let mut patch_fields = serde_json::Map::new(); // kept for special-case flx_users password-only path
    let mut fk_checks: Vec<(String, String, String, String)> = Vec::new();

    // loop every column in table_schemas.put.columns
    for column in table_schema.put.columns.iter() {
        // loop every key and value in body
        for (key, value) in body.as_object().unwrap_or(&serde_json::Map::new()).iter() {
            // check if key from body is equal to column
            if key == column {
                // convert value to string
                let mut value_x = format!("{}", value).replace("\"", "").replace("null", "");

                // check if value from body is not empty
                if !value_x.is_empty() {
                    // check jika ada kolom id maka id nya diganti. Sehingga perlu dipakai buat update foreign key
                    if key == "id" {
                        // convert value to string
                        id_new = value_x.clone();
                    }

                    // collect FK checks for batch (only when value present)
                    for fk in table_schema.foreign_keys.iter() {
                        if fk.column == *column && !value_x.is_empty() {
                            fk_checks.push((column.clone(), fk.reference_table.clone(), fk.reference_column.clone(), value_x.clone()));
                        }
                    }

                    // find column properties in table_schemas.columns (handle not found)
                    let col = match table_schema.columns.iter().find(|col| col.name == *column) {
                        Some(c) => c,
                        None => {
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: format!(
                                    "Unknown column '{}' for route '{}'",
                                    column, route
                                ),
                                total_data: 0,
                                data: Value::Null,
                            });
                        }
                    };

                    // check col.encrypt if true then encrypt value (and capture password override for flx_users)
                    if col.encrypt {
                        let is_encrypted = is_encrypted_string(value_x.clone().as_str());
                        if !is_encrypted {
                            value_x = encrypt(state.encrypt_key.clone(), value_x.clone());
                        }
                        if route == "flx_users" && column == "password" {
                            password_override = Some(value_x.clone());
                        }
                    }

                    // check if value from body is number
                    if col.type_data.contains("int") || col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
                        if let Ok(n) = value_x.parse::<i64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::I64(n))));
                            patch_fields.insert(column.clone(), serde_json::json!(n));
                        } else if let Ok(f) = value_x.parse::<f64>() {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::F64(f))));
                            patch_fields.insert(column.clone(), serde_json::json!(f));
                        } else {
                            update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                            patch_fields.insert(column.clone(), serde_json::json!(value_x));
                        }
                    } else {
                        update_fields.push((column.clone(), InsertValue::Param(DbParam::Str(value_x.clone()))));
                        patch_fields.insert(column.clone(), serde_json::json!(value_x));
                    }
                }
            }
        }
    }

    // add updated_at/by into update_fields (server-side now expression)
    update_fields.push(("updated_at".to_string(), InsertValue::Raw(state.query_converter.datetime_now.clone())));

    // get type data updated_by_id from table_schema
    let created_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "updated_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or("int".to_string());

    log_output("TYPE", "updated_by_id", route.as_str(), created_by_type.clone(), true);

    if created_by_type.contains("int") {
        if let Ok(n) = claims.id.parse::<i64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::I64(n))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else if created_by_type.contains("float") || 
        created_by_type.contains("double") || 
        created_by_type.contains("decimal") || 
        created_by_type.contains("money") {
        if let Ok(n) = claims.id.parse::<f64>() {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::F64(n))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(n));
        } else {
            update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
            patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
        }
    } else {
        update_fields.push(("updated_by_id".to_string(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        patch_fields.insert("updated_by_id".to_string(), serde_json::json!(claims.id.clone()));
    }
    
    // legacy set_clause kept only for logging; actual SQL compiled via AST

    // Compile AST update (SQL only). For MongoDB we'll use DataStore.update with patch_fields
    let (s_sql, params_compiled) = if state.db_type == "mongodb" {
        (String::new(), vec![])
    } else {
        let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
        let filter = Some(QF::Eq("id".into(), QV::Str(id_raw.clone())));
        match ds.preview_update_with(&table_schema.table, filter.as_ref(), &update_fields) {
            Ok(pair) => pair,
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST UPDATE: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    };

    // Preview AST-style update for debug (filter id, patch keys from body + timestamps)
    if *crate::ISDEBUG && state.db_type != "mongodb" {
        log_output("QUERY", "PUT(AST)", route.as_str(), s_sql.clone(), true);
        log_output("PARAM", "PUT(AST)", route.as_str(), format!("{:?}", params_compiled), true);
    }

    // validation_data moved to run inside the transaction below
    // Begin transaction for SQL backends only
    let mut tx_opt: Option<Box<dyn crate::storage::traits::TxStore>> = None;
    if state.db_type != "mongodb" {
        match state.store.begin_tx().await {
            Ok(t) => tx_opt = Some(t),
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error starting transaction: {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // Execute batched FK validation inside transaction
    if state.db_type != "mongodb" && !fk_checks.is_empty() {
        let res = validate_foreign_keys_batch_put(tx_opt.as_mut().unwrap().as_mut(), &fk_checks).await;
        if let Err(msg) = res {
            let _ = tx_opt.take().unwrap().rollback().await;
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: msg,
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // Build effective values map for unique checks: start with updated values (patch_fields)
    // and fetch any missing columns for affected unique indexes.
    if state.db_type != "mongodb" && !table_schema.indexes.is_empty() {
        // Determine PK name and param for exclusion
        let pk_name = table_schema
            .primary_key
            .columns
            .get(0)
            .cloned()
            .unwrap_or_else(|| "id".to_string());
        let pk_meta = table_schema.columns.iter().find(|c| c.name == pk_name);
        let pk_param = if let Some(m) = pk_meta { dbparam_from_value_and_type(&serde_json::json!(id_raw), Some(m)) } else { crate::database::state::DbParam::Str(id_raw.clone()) };

        // Figure out which unique indexes are impacted
        let affected: Vec<&Index> = table_schema
            .indexes
            .iter()
            .filter(|ix| ix.unique && ix.columns.iter().any(|c| patch_fields.contains_key(c)))
            .collect();
        if !affected.is_empty() {
            // Collect missing columns to fetch in one SELECT
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
                let select_sql = format!("SELECT {} FROM {} WHERE {} = ?", cols.join(","), table_schema.table, pk_name);
                if *crate::ISDEBUG { log_output("QUERY", "UNIQUE PRELOAD", "PUT", select_sql.clone(), true); }
                match tx_opt.as_mut().unwrap().raw_sql(&select_sql, vec![pk_param.clone()]).await {
                    Ok(rows) => {
                        if let Some(row) = rows.first() {
                            for c in cols {
                                if let Some(v) = row.get(&c) { effective_values.insert(c.clone(), v.clone()); }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx_opt.take().unwrap().rollback().await;
                        return HttpResponse::InternalServerError().json(WebResponse {
                            success: false,
                            message: format!("Error preloading values for unique check: {}", e),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }
            }

            // Run batched unique validation
            if let Err(msg) = validate_unique_constraints_batch_put(
                tx_opt.as_mut().unwrap().as_mut(),
                &table_schema.table,
                &table_schema.indexes,
                &table_schema.columns,
                &effective_values,
                &pk_name,
                pk_param.clone(),
            )
            .await
            {
                let _ = tx_opt.take().unwrap().rollback().await;
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: msg,
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // Run validate_data inside transaction (expects boolean in first column of first row)
    if state.db_type != "mongodb" && table_schema.put.validate_data.contains("SQL:") {
        match crate::database::state::build_sql_and_params_from_formula(
            &table_schema.put.validate_data,
            &body,
        ) {
            Ok((built_sql, params)) => {
                match tx_opt.as_mut().unwrap().raw_sql(&built_sql, params).await {
                    Ok(row) => {
                        if !row.is_empty() {
                            let is_valid = row[0].get(0).and_then(|v| v.as_bool()).unwrap_or(true);
                            if !is_valid {
                                let _ = tx_opt.take().unwrap().rollback().await;
                                return HttpResponse::BadRequest().json(WebResponse {
                                    success: false,
                                    message: "Validation data from table is not valid. Please contact your administrator".to_string(),
                                    total_data: 0,
                                    data: Value::Null,
                                });
                            }
                        } else {
                            let _ = tx_opt.take().unwrap().rollback().await;
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: "Validation data from table is empty. Please contact your administrator".to_string(),
                                total_data: 0,
                                data: Value::Null,
                            });
                        }
                    }
                    Err(err) => {
                        let _ = tx_opt.take().unwrap().rollback().await;
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Error in validation_data: {}", err),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }
            }
            Err(e) => {
                let _ = tx_opt.take().unwrap().rollback().await;
                return HttpResponse::BadRequest().json(WebResponse {
                    success: false,
                    message: format!("Error building validation formula: {}", e),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    if state.db_type != "mongodb" && table_schema.put.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
            tx_opt.as_mut().unwrap(),
            table_schema.put.pre_process,
            &body,
            route.as_str(),
        )
        .await
        {
            let _ = tx_opt.take().unwrap().rollback().await;
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // If this is a password-only update for flx_users, prefer DataStore for clarity and consistency
    if route == "flx_users" && password_override.is_some() && table_schema.put.columns.len() == 1 {
        let now = chrono::Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut doc = serde_json::json!({
            "password": password_override.unwrap(),
            "updated_at": now,
        });
        // updated_by_id from claims
        // detect numeric type for updated_by_id
        let created_by_type = table_schema
            .columns
            .iter()
            .find(|c| c.name == "updated_by_id")
            .map(|c| c.type_data.clone())
            .unwrap_or("int".to_string());
        if created_by_type.contains("int") {
            if let Ok(n) = claims.id.parse::<i64>() {
                doc["updated_by_id"] = serde_json::json!(n);
            } else {
                doc["updated_by_id"] = serde_json::json!(claims.id.clone());
            }
        } else if created_by_type.contains("float")
            || created_by_type.contains("double")
            || created_by_type.contains("decimal")
            || created_by_type.contains("money")
        {
            if let Ok(n) = claims.id.parse::<f64>() {
                doc["updated_by_id"] = serde_json::json!(n);
            } else {
                doc["updated_by_id"] = serde_json::json!(claims.id.clone());
            }
        } else {
            doc["updated_by_id"] = serde_json::json!(claims.id.clone());
        }

        // Build WHERE id = ? by calling update with Filter
        use crate::storage::ast::{Filter as QF, Val as QV};
        let filter = Some(QF::Eq("id".into(), QV::Str(id_raw.clone())));
        if state.db_type == "mongodb" {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_str(), "Mongo password update flx_users".to_string(), true);
            }
            match state.store.update("flx_users", filter, doc).await {
                Ok(_) => {
                    // Audit
                    write_audit(&AuditEntry {
                        at: Local::now().to_rfc3339(),
                        actor_id: claims.id.clone(),
                        action: "PUT",
                        route: &route,
                        id: Some(&id_raw),
                        ip: Some(get_client_ip(&req)).as_deref(),
                    });
                    return HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    });
                }
                Err(err) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT (mongo): {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        } else {
            if *crate::ISDEBUG {
                log_output("QUERY", "PUT", route.as_str(), "AST password update flx_users".to_string(), true);
            }
            let mut tx = tx_opt.take().unwrap();
            match tx.update("flx_users", filter, doc).await {
                Ok(_) => {
                    let _ = tx.commit().await;
                    return HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    });
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-PUT: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }
        }
    }

    // MongoDB main update path (no transaction)
    if state.db_type == "mongodb" {
        // Ensure updated_at exists in patch_fields for Mongo (ISO timestamp)
        let now_iso = Local::now().to_rfc3339();
        patch_fields.insert("updated_at".to_string(), serde_json::json!(now_iso));
        // Build filter by id (attempt numeric, fallback to string)
        let filt_val = if let Ok(n) = id_raw.parse::<i64>() { QV::I64(n) } else { QV::Str(id_raw.clone()) };
        let filter = Some(QF::Eq("id".into(), filt_val));
        match state.store.update(&table_schema.table, filter, Value::Object(patch_fields.clone())).await {
            Ok(_) => {
                // Audit
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "PUT",
                    route: &route,
                    id: Some(&id_raw),
                    ip: Some(get_client_ip(&req)).as_deref(),
                });
                return HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Data updated successfully".to_string(),
                    total_data: 1,
                    data: Value::Null,
                });
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-PUT (mongo): {}", err),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        }
    }

    // SQL path below
    let mut tx = tx_opt.take().unwrap();
    match tx.raw_sql(&s_sql, params_compiled).await {
        Ok(_) => {
            if state.db_type != "mongodb" && table_schema.put.post_process.contains("SQL:") {
                if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                    &mut tx,
                    table_schema.put.post_process,
                    &body,
                    route.as_str(),
                )
                .await
                {
                    let _ = tx.rollback().await;
                    // Rollback transaction if post-process SQL fails
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::Null,
                    });
                }
            }

            // jika id_new TIDAK SAMA dg "" maka ada perubahan nilai id
            if !id_new.is_empty()
                && state.db_type != "mongodb" {
                    let (is_fk_ok, err_message) = crate::nocode::foreign_key::process_foreign_keys_delete_update_txstore(
                        "UPDATE", // "DELETE" or "UPDATE"
                        state.clone(),
                        route.clone(),
                        &mut tx,
                        reference_foreign_keys,
                        claims.id.clone(),
                        id_raw.clone(),
                        id_new, // for UPDATE                        
                    )
                    .await;

                    if !is_fk_ok {
                        let _ = tx.rollback().await;
                        return HttpResponse::InternalServerError().json(WebResponse {
                            success: false,
                            message: format!(
                                "Transaction rolled back due to foreign key failures: {}",
                                err_message
                            ),
                            total_data: 0,
                            data: Value::Null,
                        });
                    }
                }

            // Commit transaction if all operations succeeded
            match tx.commit().await {
                Ok(_) => {
                    // Audit
                    write_audit(&AuditEntry {
                        at: Local::now().to_rfc3339(),
                        actor_id: claims.id.clone(),
                        action: "PUT",
                        route: &route,
                        id: Some(&id_raw),
                        ip: Some(get_client_ip(&req)).as_deref(),
                    });
                    HttpResponse::Ok().json(WebResponse {
                        success: true,
                        message: "Data updated successfully".to_string(),
                        total_data: 1,
                        data: Value::Null,
                    })
                }
                Err(err) => HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error committing transaction: {}", err),
                    total_data: 0,
                    data: Value::Null,
                }),
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-PUT: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}
