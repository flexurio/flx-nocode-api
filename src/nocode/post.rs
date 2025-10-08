use crate::crypt::{encrypt_ref, is_encrypted_string};
use actix_multipart::Multipart;
use actix_web::{web::Data, HttpResponse, Responder};
use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait, JsonValueMutTrait};
use regex::Regex;
use std::collections::HashSet;
// use std::result; // unused

use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip; // still used for audit/logging
// Global rate limiting handled in main.rs (removed local RL_WINDOW_MUTATE usage)
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    database::state::{DbParam},
    helpers::{extract_expressions, filter_table_schema, find_column_match, multipart_to_json},
    log::log_output,
    model::{Column, TableSchema, WebResponse},
    AppState,
};
use chrono::Local;
use std::sync::Arc;
use crate::storage::sql_store::{SqlStore, InsertValue};
// (compat helpers unused currently)
use crate::storage::ast::{Query as Q, Filter as F, Val as V};
use crate::storage::ast::Val as AstVal;
use crate::json_compat::value_from_f64;

/// Internal lightweight structure for insert data (AST-based)
/// Avoids heavy JSON manipulation until final serialization
#[derive(Debug, Clone)]
struct InsertData {
    fields: Vec<(String, AstVal)>,
}

impl InsertData {
    fn new() -> Self {
        // Typical insert touches a small number of columns; preallocate to avoid growth realloc
        Self { fields: Vec::with_capacity(12) }
    }

    fn add_field(&mut self, key: String, value: AstVal) {
        self.fields.push((key, value));
    }

    /// Convert to InsertValue vector for SQL operations
    fn to_insert_values(&self) -> Vec<(String, InsertValue)> {
        let mut out = Vec::with_capacity(self.fields.len());
        for (k, v) in &self.fields {
            let param = match v {
                AstVal::I64(n) => DbParam::I64(*n),
                AstVal::F64(f) => DbParam::F64(*f),
                AstVal::Bool(b) => DbParam::Bool(*b),
                AstVal::Str(s) => DbParam::Str(s.clone()),
                AstVal::Null => DbParam::Null,
            };
            out.push((k.clone(), InsertValue::Param(param)));
        }
        out
    }

    /// Convert to sonic_rs::Value for MongoDB or logging
    fn to_json_value(&self) -> sonic_rs::Value {
        let mut obj = sonic_rs::Object::new();
        for (k, v) in &self.fields {
            let json_val = match v {
                AstVal::I64(n) => sonic_rs::json!(*n),
                AstVal::F64(f) => value_from_f64(*f),
                AstVal::Bool(b) => sonic_rs::json!(*b),
                AstVal::Str(s) => sonic_rs::json!(s.as_str()),
                AstVal::Null => Value::default(),
            };
            obj.insert(k.as_str(), json_val);
        }
        Value::from(obj)
    }
}

/// Batch validate foreign keys in a single query for better performance (optimization)
/// This replaces N sequential DB queries with 1 UNION ALL query
async fn validate_foreign_keys_batch(
    state: &Data<AppState>,
    fk_checks: &[(String, String, String, String)], // (col_name, ref_table, ref_column, value)
) -> Result<(), String> {
    if fk_checks.is_empty() {
        return Ok(());
    }
    
    log_output(
        "OPTIMIZATION",
        "FK BATCH CHECK",
        "POST",
        format!("Validating {} foreign keys in batch", fk_checks.len()),
        true,
    );
    
    // Build UNION ALL query for batch validation - much faster than N queries
    let mut queries = Vec::with_capacity(fk_checks.len());
    let mut params = Vec::with_capacity(fk_checks.len());
    
    for (col_name, ref_table, ref_column, value) in fk_checks {
        queries.push(format!(
            "SELECT '{}' as _col, '{}' as _table, EXISTS(SELECT 1 FROM {} WHERE {} = ?) as _valid",
            col_name, ref_table, ref_table, ref_column
        ));
        params.push(DbParam::Str(value.clone()));
    }
    
    let union_sql = queries.join(" UNION ALL ");
    
    log_output("QUERY", "FK BATCH", "POST", union_sql.clone(), true);
    log_output("PARAMS", "FK BATCH", "POST", format!("{:?}", params), true);
    
    let rows = state.db.query_with_params(&union_sql, params).await
        .map_err(|e| format!("FK batch validation failed: {}", e))?;
    for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            let k_valid = "_valid".to_string();
            if let Some(valid) = obj.get(&k_valid).and_then(|v| v.as_bool()) {
                if !valid {
                    let k_col = "_col".to_string();
                    let k_table = "_table".to_string();
                    let col = obj.get(&k_col).and_then(|v| v.as_str()).unwrap_or("unknown");
                    let tbl = obj.get(&k_table).and_then(|v| v.as_str()).unwrap_or("unknown");
                    log_output(
                        "ERROR",
                        "FK VALIDATION",
                        "POST",
                        format!("Invalid foreign key: column={}, reference_table={}", col, tbl),
                        false,
                    );
                    return Err(format!("Invalid foreign key value for column '{}' referencing table '{}'", col, tbl));
                }
            }
        }
    }
    
    Ok(())
}

// NCO-POST
pub async fn insert(
    state: Data<AppState>,
    route: Arc<str>,
    table_schemas: Arc<Vec<TableSchema>>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let mut claims = Claims::default();
    if !state.route_publics.iter().any(|r| r == route.as_ref()) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: crate::constants::ERR_INVALID_TOKEN.to_string(),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        };


    if !check_access(&claims, route.as_ref(), "write") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let mut function_id_split: Vec<String> = Vec::new();

    let mut body = match multipart_to_json(multipart).await {
        Ok(json) => json,
        Err(e) => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Failed to parse multipart data: {}", e),
                total_data: 0,
                data: Value::default(),
            });
        }
    };

    // Rate limiting removed here (handled globally). Keep IP for audit if needed.
    let _ip_key = get_client_ip(&req);

    // Generate SQL query INSERT to table in variable route, from data structure table in table_schemas
    let table_schema = filter_table_schema(&table_schemas, route.as_ref());
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    // Validate required fields (non-nullable columns listed in post.columns)
    for post_col in &table_schema.post.columns {
        if let Some(col_def) = table_schema.columns.iter().find(|c| c.name == *post_col) {
            if !col_def.nullable && !col_def.auto_increment {
                let present = body
                    .get(post_col)
                    .map(|v| v.to_string().replace('"', ""))
                    .map(|s| !s.trim().is_empty())
                    .unwrap_or(false);
                if !present {
                    return HttpResponse::BadRequest().json(WebResponse {
                        success: false,
                        message: format!("Missing required field: {}", post_col),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            }
        }
    }

    let skip_columns: HashSet<&str> = [
        "created_at",
        "created_by_id",
        "updated_at",
        "updated_by_id",
        "deleted_at",
        "deleted_by_id",
    ]
    .iter()
    .cloned()
    .collect();

    // **1️⃣ Filter kolom yang valid untuk INSERT**
    // Pre-allocate with estimated capacity (optimization)
    let mut filtered_columns: Vec<&Column> = Vec::with_capacity(table_schema.post.columns.len());
    filtered_columns.extend(
        table_schema
            .columns
            .iter()
            .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()) && table_schema.post.columns.contains(&col.name))
    );

    // **2️⃣ Buat daftar kolom untuk INSERT, kolom hanya ambil dari nama kolom yang di sebutkan di table_schema.post.columns**
    let mut insert_columns: Vec<&str> = Vec::with_capacity(filtered_columns.len() + 4);
    insert_columns.extend(
        filtered_columns
            .iter()
            .filter(|col| table_schema.post.columns.contains(&col.name))
            .map(|col| col.name.as_str())
    );

    // check if table_schema.columns.id auto_increment false then insert_columns & filtered_columns must contain "id"
    if let Some(col) = table_schema
        .columns
        .iter().find(|c| c.name == "id") {
        if !col.auto_increment {
            insert_columns.push("id");
            filtered_columns.push(col);
        }
    }

    log_output("COLUMNS", "insert_columns", route.as_ref(), format!("{:?}", insert_columns), true);
    log_output("COLUMNS", "filtered_columns", route.as_ref(), format!("{:?}", filtered_columns), true);

    // Helper: build formula with placeholders and collect params
    fn build_formula_value(raw: &str, body: &Value) -> (String, Vec<DbParam>) {
        // raw like: "col=CONCAT({request.x}, {table[1].f})". Caller will strip "col=".
        let mut sql = raw.to_string();
        let mut params: Vec<DbParam> = Vec::new();
        let exprs = extract_expressions(&sql);
        for expr in exprs.into_iter() {
            let needle = format!("{{{}}}", expr);
            if expr.contains('[') {
                // table lookup, convert to SQL subselect
                let sub = crate::database::state::convert_to_sql(&expr);
                sql = sql.replace(&needle, &sub);
            } else if let Some(stripped) = expr.strip_prefix("request.") {
                // bind request value
                let val = body
                    .get(stripped)
                    .map(|v| v.to_string().replace('"', "").replace("null", ""))
                    .unwrap_or_default();
                // Infer numeric
                if let Ok(n) = val.parse::<i64>() {
                    params.push(DbParam::I64(n));
                } else if let Ok(f) = val.parse::<f64>() {
                    params.push(DbParam::F64(f));
                } else {
                    params.push(DbParam::Str(val));
                }
                sql = sql.replace(&needle, "?");
            } else {
                // unknown placeholder, drop
                sql = sql.replace(&needle, "");
            }
        }
        (sql, params)
    }

    // Use lightweight AST-based structure for collecting insert data
    let mut insert_data = InsertData::new();
    
    // Track formula-based columns separately (will be added as InsertValue::Raw later)
    let mut formula_fields: Vec<(String, InsertValue)> = Vec::with_capacity(4);
    
    // Collect FK checks for batch validation (optimization)
    let mut fk_checks: Vec<(String, String, String, String)> = Vec::with_capacity(filtered_columns.len());
    
    for col in filtered_columns.iter() {
        if col.auto_increment {
            continue;
        }

        let mut isformula = false;
        let post_columns: Vec<&str> = table_schema
            .post
            .columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        let (exists, matched_string) = find_column_match(&post_columns, &col.name);

        if exists && col.name != "id" {
            let string_formula = matched_string.unwrap_or("").to_string();
            if string_formula.contains('=') {
                isformula = true;
                // Remove leading "col=" part
                let rhs = string_formula.replace(&format!("{}=", col.name), "");
                let (frag, params) = build_formula_value(&rhs, &body);
                if params.is_empty() {
                    formula_fields.push((col.name.clone(), InsertValue::Raw(frag)));
                } else {
                    // Raw fragment with its own params (will be rebound per dialect later)
                    formula_fields.push((col.name.clone(), InsertValue::RawWithParams { sql: frag, params: params.clone() }));
                }
            }
        }

        if col.name == "id" && !col.function.is_empty() {
            function_id_split = col.function.split("/").map(|s| s.to_string()).collect();
        }

        if !isformula && (col.name != "id" || col.function.is_empty()) {
            // Ambil dari body dan bind sebagai param
            let mut value = body
                .get(&col.name)
                .map(|v| v.to_string().replace('"', "").replace("null", ""))
                .unwrap_or_default();

            // Collect FK checks for batch validation instead of sequential queries (optimization)
            for fk in table_schema.foreign_keys.iter() {
                if fk.column == col.name && !value.is_empty() {
                    fk_checks.push((
                        col.name.clone(),
                        fk.reference_table.clone(),
                        fk.reference_column.clone(),
                        value.clone(),
                    ));
                }
            }

            if col.encrypt && !is_encrypted_string(value.as_str()) {
                    // Avoid cloning key each time
                    value = encrypt_ref(&state.encrypt_key, &value);
                }

            // Store in lightweight AST structure
            if col.type_data.contains("int") || col.type_data.contains("float") {
                if let Ok(n) = value.parse::<i64>() {
                    insert_data.add_field(col.name.clone(), AstVal::I64(n));
                } else if let Ok(f) = value.parse::<f64>() {
                    insert_data.add_field(col.name.clone(), AstVal::F64(f));
                } else {
                    insert_data.add_field(col.name.clone(), AstVal::Str(value));
                }
            } else {
                insert_data.add_field(col.name.clone(), AstVal::Str(value));
            }
        }
    }

    // Batch validate all foreign keys in one query (optimization - replaces N queries with 1)
    if !fk_checks.is_empty() {
        if let Err(err_msg) = validate_foreign_keys_batch(&state, &fk_checks).await {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: err_msg,
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    // Begin transaction early for SQL backends so ID generation runs in the same TX
    let mut tx_opt: Option<Box<dyn crate::storage::traits::TxStore>> = None;
    if state.db_type != "mongodb" {
        match state.store.begin_tx().await {
            Ok(t) => tx_opt = Some(t),
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error starting transaction: {}", err),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    }

    if !function_id_split.is_empty() {
        // Build parts without leading slash; join with '/'
        let mut parts: Vec<String> = Vec::with_capacity(function_id_split.len());
        
        // Cache datetime to avoid multiple Utc::now() calls
        let now = chrono::Utc::now();
        let year = now.format("%Y").to_string();
        let month = now.format("%m").to_string();
        let day = now.format("%d").to_string();
        
        for token in function_id_split.iter() {
            if token == "%Y" {
                parts.push(year.clone());
            } else if token == "%m" {
                parts.push(month.clone());
            } else if token == "%d" {
                parts.push(day.clone());
            } else if token.contains("ID") {
                // Numeric suffix with zero-padding based on token, e.g. 000ID -> width 3
                let s_append = token.replace("ID", "");
                let len_id = s_append.len();
                let id_prefix = parts.join("/");

                // Find latest id by prefix. For Mongo, use simple sort by id DESC; for SQL, you can still sort.
                let like_pat = if id_prefix.is_empty() { "%".to_string() } else { format!("{}%", id_prefix) };
                // Build: SELECT id FROM table WHERE id ILIKE prefix% ORDER BY id DESC LIMIT 1
                let q_max = Q::from(table_schema.table.clone())
                    .select(["id"]).r#where(F::ILike("id".into(), like_pat))
                    .order_by("id", false).limit(1);
                let max_id: String = if state.db_type == "mongodb" {
                    match state.store.query(&q_max).await {
                        Ok(rows) if !rows.is_empty() => rows[0].get("id").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_else(|| "0".to_string()),
                        _ => "0".to_string(),
                    }
                } else {
                    // Debug SQL preview for SQL backends
                    if *crate::ISDEBUG {
                        let ds_dbg = SqlStore::new(state.db.clone(), state.db_type.clone());
                        let (sql_dbg, params_dbg) = ds_dbg.preview_sql(&q_max);
                        log_output("QUERY", "MAX-ID", route.as_ref(), sql_dbg, true);
                        log_output("PARAMS", "MAX-ID", route.as_ref(), format!("{:?}", params_dbg), true);
                    }
                    match tx_opt.as_mut().unwrap().query(&q_max).await {
                        Ok(rows) if !rows.is_empty() => rows[0].get("id").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_else(|| "0".to_string()),
                        _ => "0".to_string(),
                    }
                };
                // Extract numeric suffix (width = len_id) and increment
                let mut current_num: i64 = 0;
                if max_id.len() >= len_id {
                    let suffix = &max_id[max_id.len() - len_id..];
                    current_num = suffix.parse::<i64>().unwrap_or(0);
                }
                let next_num = current_num + 1;
                let next_str = format!("{:0width$}", next_num, width = len_id);
                parts.push(next_str);
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
        let id = parts.join("/");
        // Store id in AST structure
        insert_data.add_field("id".to_string(), AstVal::Str(id));
    }

    // Convert InsertData to InsertValue vector for SQL compilation
    let mut insert_fields = insert_data.to_insert_values();
    
    // Add formula-based fields
    insert_fields.extend(formula_fields);
    
    // **Tambahkan created_at** (server-side raw expression)
    insert_columns.push("created_at");
    insert_fields.push(("created_at".into(), InsertValue::Raw(state.query_converter.datetime_now.clone())));

    // **Tambahkan created_by_id**
    insert_columns.push("created_by_id");
    
    // get type data created_by_id from table_schema
    let created_by_type = table_schema
        .columns
        .iter()
        .find(|c| c.name == "created_by_id")
        .map(|c| c.type_data.clone())
        .unwrap_or("int".to_string());

    log_output("TYPE", "created_by_id", route.as_ref(), created_by_type.clone(), true);

    // Cache parsed variants of claims.id to avoid multi-parse
    let claims_id_i64 = claims.id.parse::<i64>().ok();
    let claims_id_f64 = if claims_id_i64.is_none() { claims.id.parse::<f64>().ok() } else { None };
    if created_by_type.contains("int") {
        if let Some(n) = claims_id_i64 {
            insert_fields.push(("created_by_id".into(), InsertValue::Param(DbParam::I64(n))));
        } else {
            insert_fields.push(("created_by_id".into(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        }
    } else if created_by_type.contains("float") || created_by_type.contains("double") || created_by_type.contains("decimal") || created_by_type.contains("money") {
        if let Some(n) = claims_id_f64 {
            insert_fields.push(("created_by_id".into(), InsertValue::Param(DbParam::F64(n))));
        } else {
            insert_fields.push(("created_by_id".into(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
        }
    } else {
        insert_fields.push(("created_by_id".into(), InsertValue::Param(DbParam::Str(claims.id.clone()))));
    }

    // For MongoDB, directly insert JSON doc without SQL; for SQL, compile INSERT
    let (exec_sql, exec_params) = if state.db_type == "mongodb" {
        (String::new(), vec![])
    } else {
        // Try to include RETURNING id when supported
        let returning_cols: Vec<&str> = vec!["id"]; // core id fetch
        let attempt = state.sql_store.preview_insert_with_returning(&table_schema.table, &insert_fields, &returning_cols);
        match attempt.or_else(|_| state.sql_store.preview_insert_with(&table_schema.table, &insert_fields)) {
            Ok((sql_dbg, params_dbg)) => {
                if *crate::ISDEBUG {
                    log_output("QUERY", "POST(AST)", route.as_ref(), sql_dbg.clone(), true);
                    log_output("PARAMS", "POST(AST)", route.as_ref(), format!("{:?}", params_dbg), true);
                }
                (sql_dbg, params_dbg)
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error compiling AST INSERT: {}", e),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    };

    // (moved) validation_data will be executed inside the transaction below (SQL only)
    // Run validate_data inside transaction for SQL backends
    if state.db_type != "mongodb" && table_schema.post.validate_data.contains("SQL:") {
        match crate::database::state::build_sql_and_params_from_formula(
            &table_schema.post.validate_data,
            &body,
        ) {
            Ok((built_sql, params)) => {
                match tx_opt.as_mut().unwrap().raw_sql(&built_sql, params).await {
                    Ok(row) => {
                        if !row.is_empty() {
                            let is_valid = row[0]
                                .as_object()
                                .and_then(|o| o.iter().find_map(|(_, v)| v.as_bool()))
                                .unwrap_or(true);
                            if !is_valid {
                                let _ = tx_opt.take().unwrap().rollback().await;
                                return HttpResponse::BadRequest().json(WebResponse {
                                    success: false,
                                    message: "Validation data from table is not valid. Please contact your administrator".to_string(),
                                    total_data: 0,
                                    data: Value::default(),
                                });
                            }
                        } else {
                            let _ = tx_opt.take().unwrap().rollback().await;
                            return HttpResponse::BadRequest().json(WebResponse {
                                success: false,
                                message: "Validation data from table is empty. Please contact your administrator".to_string(),
                                total_data: 0,
                                data: Value::default(),
                            });
                        }
                    }
                    Err(err) => {
                        let _ = tx_opt.take().unwrap().rollback().await;
                        return HttpResponse::BadRequest().json(WebResponse {
                            success: false,
                            message: format!("Error in validation_data: {}", err),
                            total_data: 0,
                            data: Value::default(),
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
                    data: Value::default(),
                });
            }
        }
    }

    if state.db_type != "mongodb" && table_schema.post.pre_process.contains("SQL:") {
        if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
            tx_opt.as_mut().unwrap(),
            &table_schema.post.pre_process,
            &body,
            route.as_ref(),
        )
        .await
        {
            let _ = tx_opt.take().unwrap().rollback().await;
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error in pre-process: {}", err),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    if state.db_type == "mongodb" {
        // Convert InsertData to JSON for MongoDB with timestamps
        let mut doc_obj = sonic_rs::Object::new();
        
        // Add all fields from InsertData
        for (k, v) in &insert_data.fields {
            let json_val = match v {
                AstVal::I64(n) => sonic_rs::json!(*n),
                AstVal::F64(f) => value_from_f64(*f),
                AstVal::Bool(b) => sonic_rs::json!(*b),
                AstVal::Str(s) => sonic_rs::json!(s.as_str()),
                AstVal::Null => Value::default(),
            };
            doc_obj.insert(k.as_str(), json_val);
        }
        
        // Add timestamps
        let now_iso = Local::now().to_rfc3339();
        doc_obj.insert("created_at", sonic_rs::json!(now_iso));
        
        // Add created_by_id with proper type
        if created_by_type.contains("int") {
            if let Some(n) = claims_id_i64 { doc_obj.insert("created_by_id", sonic_rs::json!(n)); }
            else if let Some(nf) = claims_id_f64 { doc_obj.insert("created_by_id", value_from_f64(nf)); }
            else { doc_obj.insert("created_by_id", sonic_rs::json!(claims.id)); }
        } else if created_by_type.contains("float") || created_by_type.contains("double") || created_by_type.contains("decimal") || created_by_type.contains("money") {
            if let Some(nf) = claims_id_f64 { doc_obj.insert("created_by_id", value_from_f64(nf)); }
            else if let Some(ni) = claims_id_i64 { doc_obj.insert("created_by_id", sonic_rs::json!(ni)); }
            else { doc_obj.insert("created_by_id", sonic_rs::json!(claims.id)); }
        } else {
            doc_obj.insert("created_by_id", sonic_rs::json!(claims.id));
        }
        
        let doc = Value::from(doc_obj);
        match state.store.insert(&table_schema.table, doc).await {
            Ok(result) => {
                // log_output result
                log_output("INFO", "MONGO INSERT RESULT", route.as_ref(), format!("{:?}", result), true);
                // Try to capture returned id when supported
                let returned_id = result.get("inserted_id").cloned().unwrap_or(Value::default()).get("$oid").cloned().unwrap_or(Value::default());
                // Audit trail
                write_audit(&AuditEntry {
                    at: Local::now().to_rfc3339(),
                    actor_id: claims.id.clone(),
                    action: "POST",
                    route: &route,
                    id: None,
                    ip: Some(get_client_ip(&req)).as_deref(),
                });

                body = body.as_object_mut().map(|map| {
                    map.insert("id_new", returned_id.clone());
                    Value::from(map.clone())
                }).unwrap();

                if table_schema.post.post_process.contains("SQL:") {
                    // Execute a simplified MongoDB equivalent of a SQL SELECT post_process.
                    // We support basic patterns like: SQL:SELECT <cols> FROM <table> WHERE <col> = {request.x}
                    match crate::database::state::build_sql_and_params_from_formula(
                        &table_schema.post.post_process,
                        &body,
                    ) {
                        Ok((built_sql, params)) => {
                            // Try to parse a simple SELECT ... FROM ... [WHERE ...] statement
                            // Note: we intentionally keep this conservative; unsupported forms are no-ops.
                            let re = Regex::new(
                                r"(?is)^\s*select\s+(?P<cols>.+?)\s+from\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s*(?:where\s+(?P<where>.+?))?\s*;?\s*$",
                            )
                            .unwrap();
                            if let Some(cap) = re.captures(&built_sql) {
                                let table = cap.name("table").unwrap().as_str().to_string();
                                let cols_raw = cap.name("cols").unwrap().as_str().trim().to_string();
                                let where_raw = cap.name("where").map(|m| m.as_str().trim().to_string());

                                let mut q = Q::from(table.clone());
                                if cols_raw != "*" {
                                    let cols: Vec<&str> = cols_raw
                                        .split(',')
                                        .map(|s| s.trim())
                                        .filter(|s| !s.is_empty())
                                        .collect();
                                    if !cols.is_empty() {
                                        q = q.select(cols);
                                    }
                                }

                                if let Some(w) = where_raw {
                                    // Support a single predicate in the form: <col> = ? | <col> like ? | <col> ilike ?
                                    // If multiple predicates exist, take the first recognizable one.
                                    let w_eq = Regex::new(r"(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\?\s*$").unwrap();
                                    let w_like = Regex::new(r"(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s+like\s*\?\s*$").unwrap();
                                    let w_ilike = Regex::new(r"(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s+ilike\s*\?\s*$").unwrap();

                                    // In case of compound conditions (AND ...), try the first recognizable segment
                                    let re_and = Regex::new(r"(?i)\s+AND\s+").unwrap();
                                    let first_part = re_and
                                        .split(&w)
                                        .next()
                                        .unwrap_or(w.as_str())
                                        .trim()
                                        .to_string();

                                    let mut applied_filter = false;
                                    if let Some(capw) = w_eq.captures(first_part.trim()) {
                                        let col = capw.get(1).unwrap().as_str().to_string();
                                        if let Some(p) = params.first() {
                                            let val = match p.clone() {
                                                crate::database::state::DbParam::I64(n) => V::I64(n),
                                                crate::database::state::DbParam::F64(n) => V::F64(n),
                                                crate::database::state::DbParam::Bool(b) => V::Bool(b),
                                                crate::database::state::DbParam::Str(s) => V::Str(s),
                                                crate::database::state::DbParam::Null => V::Null,
                                            };
                                            q = q.r#where(F::Eq(col, val));
                                            applied_filter = true;
                                        }
                                    } else if let Some(capw) = w_ilike.captures(first_part.trim()) {
                                        let col = capw.get(1).unwrap().as_str().to_string();
                                        if let Some(p) = params.first() {
                                            if let crate::database::state::DbParam::Str(s) = p.clone() {
                                                q = q.r#where(F::ILike(col, s));
                                                applied_filter = true;
                                            }
                                        }
                                    } else if let Some(capw) = w_like.captures(first_part.trim()) {
                                        let col = capw.get(1).unwrap().as_str().to_string();
                                        if let Some(p) = params.first() {
                                            if let crate::database::state::DbParam::Str(s) = p.clone() {
                                                q = q.r#where(F::Like(col, s));
                                                applied_filter = true;
                                            }
                                        }
                                    }

                                    if !applied_filter {
                                        // Not a supported WHERE form; log and continue
                                        log_output(
                                            "WARN",
                                            "POST(MONGO) post_process",
                                            route.as_ref(),
                                            format!("Unsupported WHERE clause for Mongo: {}", w),
                                            false,
                                        );
                                    }
                                }

                                // Execute via DataStore (Mongo adapter). Ignore result; log errors only.
                                match state.store.query(&q).await {
                                    Ok(_rows) => {
                                        if *crate::ISDEBUG {
                                            log_output(
                                                "INFO",
                                                "POST(MONGO) post_process",
                                                route.as_ref(),
                                                "Executed SELECT-equivalent on Mongo".to_string(),
                                                true,
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        log_output(
                                            "ERROR",
                                            "POST(MONGO) post_process",
                                            route.as_ref(),
                                            format!("Error executing Mongo post_process: {}", e),
                                            false,
                                        );
                                    }
                                }
                            } else {
                                // Non-SELECT forms are not supported on Mongo; just log.
                                log_output(
                                    "WARN",
                                    "POST(MONGO) post_process",
                                    route.as_ref(),
                                    "Only simple SELECT post_process is supported on Mongo".to_string(),
                                    false,
                                );
                            }
                        }
                        Err(e) => {
                            log_output(
                                "ERROR",
                                "POST(MONGO) post_process",
                                route.as_ref(),
                                format!("Error building SQL formula for Mongo: {}", e),
                                false,
                            );
                        }
                    }
                }

                let mut resp_obj = sonic_rs::Object::new();
                resp_obj.insert("id", returned_id);
                return HttpResponse::Ok().json(WebResponse {
                    success: true,
                    message: "Data inserted".to_string(),
                    total_data: 1,
                    data: Value::from(resp_obj),
                });
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(WebResponse {
                    success: false,
                    message: format!("Error NCO-POST (mongo): {}", err),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        }
    }

    let mut tx = tx_opt.take().unwrap();
    match tx.raw_sql(&exec_sql, exec_params).await {
        Ok(result) => {
            // Try to capture returned id when supported
            let mut returned_id: Option<Value> = None;
            if let Some(first) = result.first() {
                if let Some(idv) = first.get("id") { returned_id = Some(idv.clone()); }
            }
            // MySQL fallback: fetch LAST_INSERT_ID() when INSERT has no RETURNING and id wasn't provided
            if returned_id.is_none() && state.db_type == "mysql" {
                if let Ok(rows) = tx.raw_sql("SELECT LAST_INSERT_ID() AS id", vec![]).await {
                    if let Some(first) = rows.first() {
                        if let Some(idv) = first.get("id") { returned_id = Some(idv.clone()); }
                    }
                }
            }
            // If numeric zero treat as None to allow fallback
            if let Some(val) = &returned_id {
                let is_zero = val.as_i64() == Some(0)
                    || val.as_u64() == Some(0)
                    || val.as_f64() == Some(0.0);
                if is_zero { returned_id = None; }
            }

            if returned_id.is_none() {
                // Try to get id from InsertData
                let tmp_val = insert_data.to_json_value();
                if let Some(v) = tmp_val.get("id") { returned_id = Some(v.clone()); }
            }

            body = body.as_object_mut().map(|map| {
                map.insert("id_new", returned_id.clone().unwrap_or_default());
                Value::from(map.clone())
            }).unwrap();
            

            if state.db_type != "mongodb" && table_schema.post.post_process.contains("SQL:") {
                if let Err(err) = crate::database::state::execute_sql_formula_with_txstore(
                    &mut tx,
                    &table_schema.post.post_process,
                    &body,
                    route.as_ref(),
                )
                .await
                {
                    let _ = tx.rollback().await;
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error executing post-process SQL: {}", err),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            }
            let _ = tx.commit().await;
            // Audit trail
            write_audit(&AuditEntry {
                at: Local::now().to_rfc3339(),
                actor_id: claims.id.clone(),
                action: "POST",
                route: &route,
                id: None,
                ip: Some(get_client_ip(&req)).as_deref(),
            });
            let mut resp_obj = sonic_rs::Object::new();
            resp_obj.insert("id", returned_id.unwrap_or_default());
            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Data inserted".to_string(),
                total_data: 1,
                data: Value::from(resp_obj),
            })
        }
        Err(err) => {
            let _ = tx.rollback().await;
            let mut err_message = err.to_string().to_lowercase();
            if err_message.contains("created_by_id") {
                err_message += format!(" \n id from token : {}", &claims.id).as_str();
            }
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-POST: {}", err_message),
                total_data: 0,
                data: Value::default(),
            })
        }
    }
}
