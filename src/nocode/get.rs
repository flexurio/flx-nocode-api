use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use sonic_rs::{Value, JsonValueTrait, json, Object};
use std::collections::HashMap;

use crate::{
    AppState, auth::{check_access, get_user_info_from_token}, database::redis::redis_del_key, helpers::{filter_table_schema, get_client_ip, split_column_operator}, log::log_output, model::{ParamJoin, TableSchema, WebResponse}, rate_limit::RL_WINDOW_GET
};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV, Expr as QE, Join as QJ, JoinKind as QJK};
use crate::storage::sql_store::SqlStore;
use std::sync::Arc;
use std::collections::HashSet;
use crate::database::redis::{redis_get_json, redis_set_json, build_key_prefix};

// NCO-GET
pub async fn select(
    state: web::Data<AppState>,
    parameters: web::Query<HashMap<String, String>>,
    route: Arc<str>,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    // Default cache tenant scope (use &str to avoid allocation)
    let mut cache_tenant = String::from("public");
    // Per-IP GET rate limit per second
    let ip_key = get_client_ip(&req);
    
    // Cache rate limit config (read once)
    use once_cell::sync::Lazy;
    static RATE_LIMIT_GET: Lazy<i64> = Lazy::new(|| {
        std::env::var("RATE_LIMIT_GET_PER_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20)
    });
    let get_limit_i64 = *RATE_LIMIT_GET;
    if get_limit_i64 > 0
        && !RL_WINDOW_GET
            .check_and_increment(&format!("get:{}:{}", route, ip_key), get_limit_i64 as u32)
    {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".to_string(),
            total_data: 0,
            data: Value::default(),
        });
    }
    if !state.route_publics.iter().any(|r| r == route.as_ref()) {
        
        let claims = match get_user_info_from_token(req, state.clone()) {
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

        // Capture tenant for cache key scoping
        if !claims.id.is_empty() {
            cache_tenant = claims.id.clone();
        }

    if !check_access(&claims, route.as_ref(), "read") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }

        // Per-user GET rate limit per second
        if get_limit_i64 > 0
            && !claims.id.is_empty()
            && !RL_WINDOW_GET
                .check_and_increment(&format!("get:{}:user:{}", route, claims.id), get_limit_i64 as u32)
        {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    let table_schema: TableSchema = filter_table_schema(&table_schemas, route.as_ref()).await;
    // legacy SQL variables removed; using AST end-to-end

    log_output(
        "CONFIGURATION",
        "FILTERED PARAMETERS",
        "filter_table_schema",
        // sonic_rs Value implements Display -> to_string
    json!(table_schema.get.parameters.clone()).to_string(),
        true,
    );

    if table_schema.table.is_empty() {
        let message_error = format!(
            "ER01(nocode_get): Entity {} on folder config/{}.json not found",
            route, route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    


    let mut is_deleted_at = true;

    log_output(
        "CONFIGURATION",
        "PARAMETERS ON ROUTES",
        "TableSchema",
        table_schema.get.parameters.join(", "),
        true,
    );

    // Build sonic_rs Object from query HashMap for unified downstream logic
    let mut params_map_awal: Object = Object::with_capacity(parameters.len());
    for (k, v) in parameters.iter() {
        params_map_awal.insert(k.as_str(), Value::from(v.as_str()));
    }
    let mut params_map = params_map_awal.clone();
    let mut isredis = false;

    // check if in parameters contain redis 
    let redis_key = String::from("redis");
    if let Some(v) = params_map_awal.get(&redis_key) {
        if v.as_bool() == Some(true) || v.as_str() == Some("true") { isredis = true; }
        let _ = params_map.remove(&redis_key);
    }
    
    // Build cache key if caching enabled
    let prefix = build_key_prefix(&cache_tenant, route.as_ref());
    // include stable request parameters in the key (sorted by name)
    let mut keys: Vec<_> = params_map
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    keys.sort();
    let key_suffix = keys.join("&");
    let full_key = if key_suffix.is_empty() { prefix } else { format!("{}:{}", prefix, key_suffix) };
    let cache_key = Some(full_key);

    // OPTIMIZATION: Automatic cache read-through (cache-aside pattern)
    // Try cache first if enabled (either by TTL config or explicit ?redis=true)
    if isredis {
        // read-through cache only if cache_key constructed
        if let Some(ref k) = cache_key {
            if let Ok(Some(cached)) = redis_get_json::<WebResponse>(k).await {
                log_output(
                    "REDIS",
                    "CACHE HIT",
                    route.as_ref(),
                    format!("Key: {}, Records: {}", k, cached.total_data),
                    true,
                );
                return HttpResponse::Ok().json(cached);
            } else {
                log_output(
                    "REDIS",
                    "CACHE MISS",
                    route.as_ref(),
                    format!("Key: {}, will query DB", k),
                    true,
                );
            }
        }
    } else {
        // remove cache k on redis 
        if let Some(ref k) = cache_key {
            let _ = redis_del_key(k).await;
        }
    }

    // AST path (now supports MSSQL, JOINs, GROUP BY, HAVING, and paramjoin)
    {
            // Build AST query
            // Tuneable limits via env to control memory usage per request
            let limit_default_env: i32 = std::env::var("LIMIT_DEFAULT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(200);
            let limit_max_env: i32 = std::env::var("LIMIT_MAX")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2000);
            let mut i_limit_ast = limit_default_env;
            let mut i_page_ast = 1i32;
            let mut order_col_ast = table_schema.get.order_by.clone().join(", ");
            let mut order_type_ast = "ASC".to_string();
            // Pre-allocate with estimated capacity to reduce reallocations (optimization)
            let mut filters: Vec<QF> = Vec::with_capacity(table_schema.get.parameters.len());
            // collect paramjoin values if provided
            let mut paramjoins_ast: Vec<ParamJoin> = Vec::with_capacity(4);
            for p in &table_schema.get.parameters {
                if p.contains("paramjoin") {
                    if let Some(v) = params_map.get(p).and_then(|vv| vv.as_str()) {
                        paramjoins_ast.push(ParamJoin { name: p.replace(".eq", ""), value: v.to_string() });
                    }
                }
            }

            // helper to parse value into QV
            fn to_val(s: &str) -> QV {
                if s.eq_ignore_ascii_case("true") { return QV::Bool(true); }
                if s.eq_ignore_ascii_case("false") { return QV::Bool(false); }
                if let Ok(i) = s.parse::<i64>() { return QV::I64(i); }
                if let Ok(f) = s.parse::<f64>() { return QV::F64(f); }
                QV::Str(s.to_string())
            }

            // process allowed parameters
            for param in &table_schema.get.parameters {
                if let Some(value) = params_map.get(param) {
                    let value_str = value.as_str().unwrap_or("").to_string();
                    if param.contains("deleted_at") { is_deleted_at = false; }
                    match param.as_str() {
                        "page" => {
                            i_page_ast = value_str.parse::<i32>().ok().filter(|v| *v > 0).unwrap_or(1);
                        }
                        "sort" => {
                            if !value_str.is_empty() {
                                let sanitized: String = value_str
                                    .chars()
                                    .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ' || *c == '_' || *c == '-')
                                    .collect();
                                order_col_ast = sanitized;
                            }
                        }
                        "ascending" => {
                            order_type_ast = if value_str.eq_ignore_ascii_case("true") { "ASC".into() } else { "DESC".into() };
                        }
                        "limit" => {
                            i_limit_ast = value_str
                                .parse::<i32>()
                                .ok()
                                .map(|v| v.clamp(1, limit_max_env))
                                .unwrap_or(limit_default_env);
                        }
                        p if p.contains("paramjoin") => {
                            // handled via join logical substitution below
                        }
                        "search" => {
                            let v = value_str;
                            if !v.is_empty() {
                                // Pre-calculate capacity for OR filters
                                let pk_count = table_schema.primary_key.columns.len();
                                let idx_count: usize = table_schema.indexes.iter()
                                    .map(|idx| idx.columns.len())
                                    .sum();
                                let mut ors: Vec<QF> = Vec::with_capacity(pk_count + idx_count);
                                
                                // primary key columns
                                for column in table_schema.primary_key.columns.iter() {
                                    let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                    ors.push(QF::ILike(col, format!("%{}%", v)));
                                }
                                // indexed columns
                                for index in table_schema.indexes.iter() {
                                    for column in index.columns.iter() {
                                        let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                        ors.push(QF::ILike(col, format!("%{}%", v)));
                                    }
                                }
                                if !ors.is_empty() { filters.push(QF::Or(ors)); }
                            }
                        }
                        p if p.contains('|') => {
                            // OR across multiple columns
                            let parts_count = p.matches('|').count() + 1;
                            let mut ors: Vec<QF> = Vec::with_capacity(parts_count); // Pre-allocate
                            for part in p.split('|') {
                                let (column, operator, val) = split_column_operator(part, &table_schema.table, &value_str);
                                let f = match operator.as_str() {
                                    "=" => QF::Eq(column, to_val(&val)),
                                    "<" => QF::Lt(column, to_val(&val)),
                                    "<=" => QF::Lte(column, to_val(&val)),
                                    ">" => QF::Gt(column, to_val(&val)),
                                    ">=" => QF::Gte(column, to_val(&val)),
                                    "like" => QF::ILike(column, val),
                                    "nin" => {
                                        // nin: support JSON array or comma-separated
                                        let val_trim = value_str.trim();
                                        if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                            if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                                let vs = arr.into_iter().map(|x| {
                                                    if let Some(i) = x.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = x.as_f64() { QV::F64(f) }
                                                    else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                                    else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                                    else if x.is_null() { QV::Null }
                                                    else { QV::Str(x.to_string()) }
                                                }).collect::<Vec<QV>>();
                                                QF::NotIn(column, vs)
                                            } else { QF::NotIn(column, vec![to_val(&val)]) }
                                        } else if value_str.contains(',') {
                                            let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                            QF::NotIn(column, vs)
                                        } else { QF::NotIn(column, vec![to_val(&val)]) }
                                    }
                                    "between" => {
                                        // between: support JSON [a,b] or 'a,b'
                                        let val_trim = value_str.trim();
                                        if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                            if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                                if arr.len() == 2 {
                                                    let to_qv = |v: &Value| {
                                                        if let Some(i) = v.as_i64() { QV::I64(i) }
                                                        else if let Some(f) = v.as_f64() { QV::F64(f) }
                                                        else if let Some(b) = v.as_bool() { QV::Bool(b) }
                                                        else if let Some(s) = v.as_str() { QV::Str(s.to_string()) }
                                                        else if v.is_null() { QV::Null }
                                                        else { QV::Str(v.to_string()) }
                                                    };
                                                    QF::Between(column, to_qv(&arr[0]), to_qv(&arr[1]))
                                                } else { QF::Eq(column, to_val(&val)) }
                                            } else { QF::Eq(column, to_val(&val)) }
                                        } else if value_str.contains(',') {
                                            let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                            let a = parts.next().unwrap_or_default();
                                            let b = parts.next().unwrap_or_default();
                                            QF::Between(column, to_val(&a), to_val(&b))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    }
                                    "is" => {
                                        if val.eq_ignore_ascii_case("NULL") { QF::IsNull(column) }
                                        else if val.eq_ignore_ascii_case("NOT NULL") { QF::IsNotNull(column) }
                                        else { QF::Eq(column, to_val(&val)) }
                                    }
                                    _ => QF::Eq(column, to_val(&val)),
                                };
                                ors.push(f);
                            }
                            if !ors.is_empty() { filters.push(QF::Or(ors)); }
                        }
                        _ => {
                            let (column, operator, val) = split_column_operator(param, &table_schema.table, &value_str);
                            if operator == "is" {
                                if value_str.eq_ignore_ascii_case("NULL") {
                                    filters.push(QF::IsNull(column));
                                } else if value_str.eq_ignore_ascii_case("NOT NULL") {
                                    filters.push(QF::IsNotNull(column));
                                } else {
                                    filters.push(QF::Eq(column, to_val(&val)));
                                }
                            } else {
                                // Support IN via comma-separated list or JSON array string when operator is equality
                                let f = match operator.as_str() {
                                    "=" => {
                                        let val_trim = value_str.trim();
                                        if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                            if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                                let vs = arr.into_iter().map(|x| {
                                                    if let Some(i) = x.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = x.as_f64() { QV::F64(f) }
                                                    else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                                    else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                                    else if x.is_null() { QV::Null }
                                                    else { QV::Str(x.to_string()) }
                                                }).collect::<Vec<QV>>();
                                                QF::In(column, vs)
                                            } else { QF::Eq(column, to_val(&val)) }
                                        } else if value_str.contains(',') {
                                            let vs = value_str
                                                .split(',')
                                                .map(|s| to_val(s.trim()))
                                                .collect::<Vec<QV>>();
                                            QF::In(column, vs)
                                        } else {
                                            QF::Eq(column, to_val(&val))
                                        }
                                    }
                                    "<" => QF::Lt(column, to_val(&val)),
                                    "<=" => QF::Lte(column, to_val(&val)),
                                    ">" => QF::Gt(column, to_val(&val)),
                                    ">=" => QF::Gte(column, to_val(&val)),
                                    "like" => QF::ILike(column, val),
                                    "nin" => {
                                        let val_trim = value_str.trim();
                                        if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                            if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                                let vs = arr.into_iter().map(|x| {
                                                    if let Some(i) = x.as_i64() { QV::I64(i) }
                                                    else if let Some(f) = x.as_f64() { QV::F64(f) }
                                                    else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                                    else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                                    else if x.is_null() { QV::Null }
                                                    else { QV::Str(x.to_string()) }
                                                }).collect::<Vec<QV>>();
                                                QF::NotIn(column, vs)
                                            } else { QF::NotIn(column, vec![to_val(&val)]) }
                                        } else if value_str.contains(',') {
                                            let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                            QF::NotIn(column, vs)
                                        } else { QF::NotIn(column, vec![to_val(&val)]) }
                                    }
                                    "between" => {
                                        let val_trim = value_str.trim();
                                        if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                            if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                                if arr.len() == 2 {
                                                    let to_qv = |v: &Value| {
                                                        if let Some(i) = v.as_i64() { QV::I64(i) }
                                                        else if let Some(f) = v.as_f64() { QV::F64(f) }
                                                        else if let Some(b) = v.as_bool() { QV::Bool(b) }
                                                        else if let Some(s) = v.as_str() { QV::Str(s.to_string()) }
                                                        else if v.is_null() { QV::Null }
                                                        else { QV::Str(v.to_string()) }
                                                    };
                                                    QF::Between(column, to_qv(&arr[0]), to_qv(&arr[1]))
                                                } else { QF::Eq(column, to_val(&val)) }
                                            } else { QF::Eq(column, to_val(&val)) }
                                        } else if value_str.contains(',') {
                                            let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                            let a = parts.next().unwrap_or_default();
                                            let b = parts.next().unwrap_or_default();
                                            QF::Between(column, to_val(&a), to_val(&b))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    }
                                    _ => QF::Eq(column, to_val(&val)),
                                };
                                filters.push(f);
                            }
                        }
                    }
                }
            }

            // default deleted_at IS NULL if not requested otherwise
            if is_deleted_at {
                filters.push(QF::IsNull(format!("{}.deleted_at", table_schema.table)));
            }

            // projection
            let select_columns = table_schema.get.columns.clone();
            let mut q = QQ::from(table_schema.table.clone()).select(select_columns);

            // where
            if !filters.is_empty() {
                q = q.r#where(QF::And(filters));
            }

            // order by: support formats
            // - "col" (uses global ascending)
            // - "-col" (desc)
            // - "col asc" or "col desc" (per-column)
            // - allow fully-qualified (table.col); validate by unqualified name against allowlist
            // Build allowlist of unqualified column names including aliases from SELECT list
            let mut allowed_unqualified: HashSet<String> = HashSet::new();
            // From projection (get.columns): capture unqualified names and aliases
            for c in table_schema.get.columns.iter() {
                let s = c.trim();
                // handle "expr AS alias" (case-insensitive)
                if let Some((left, right)) = s.to_lowercase().split_once(" as ") {
                    // right is alias in lowercase; recover actual alias by splitting original string
                    if let Some((_l, alias_actual)) = s.split_once(" as ") {
                        allowed_unqualified.insert(alias_actual.trim().to_string());
                    } else if let Some((_l, alias_actual)) = s.split_once(" AS ") {
                        allowed_unqualified.insert(alias_actual.trim().to_string());
                    } else {
                        // fallback: use lower-case parsed alias
                        allowed_unqualified.insert(right.trim().to_string());
                    }
                    // also add unqualified of left expr
                    let left_orig = &s[..left.len()];
                    let base = left_orig.rsplit('.').next().unwrap_or(left_orig).trim();
                    if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
                } else {
                    // no alias; add unqualified token
                    let base = s.rsplit('.').next().unwrap_or(s).trim();
                    if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
                }
            }
            // Indexed columns
            for idx in table_schema.indexes.iter() {
                for c in idx.columns.iter() {
                    let base = c.rsplit('.').next().unwrap_or(c).trim();
                    if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
                }
            }
            // Columns from join tables definitions
            for j in table_schema.get.join_tables.iter() {
                for c in j.columns.iter() {
                    let base = c.rsplit('.').next().unwrap_or(c).trim();
                    if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
                }
            }

            let global_asc = order_type_ast.eq_ignore_ascii_case("ASC");
            let mut any_order = false;
            for token in order_col_ast.split(',') {
                let raw = token.trim();
                if raw.is_empty() { continue; }
                // detect per-column direction
                let mut col_str = raw;
                let mut asc_opt: Option<bool> = None;
                if let Some(stripped) = raw.strip_prefix('-') {
                    col_str = stripped.trim();
                    asc_opt = Some(false);
                } else if let Some((name, dir)) = raw.rsplit_once(' ') {
                    let d = dir.trim().to_ascii_lowercase();
                    if d == "asc" || d == "desc" {
                        col_str = name.trim();
                        asc_opt = Some(d == "asc");
                    }
                }
                let unqualified = col_str.rsplit('.').next().unwrap_or(col_str).trim();
                if !allowed_unqualified.contains(unqualified) { continue; }
                let asc = asc_opt.unwrap_or(global_asc);
                q = q.order_by(col_str.to_string(), asc);
                any_order = true;
            }

            // If still no ORDER BY (client omitted or all tokens invalid), fall back to schema.get.order_by
            if !any_order {
                for col in table_schema.get.order_by.iter() {
                    let col_trim = col.trim();
                    if col_trim.is_empty() { continue; }
                    // allow alias or unqualified name from projection
                    let unqualified = col_trim.rsplit('.').next().unwrap_or(col_trim);
                    if allowed_unqualified.contains(unqualified) || allowed_unqualified.contains(col_trim) {
                        q = q.order_by(col_trim.to_string(), global_asc);
                    }
                }
            }

            // JOINs (with safe paramjoin replacements)
            if !table_schema.get.join_tables.is_empty() {
                for j in &table_schema.get.join_tables {
                    let mut logical = j.logical.clone();
                    for pj in &paramjoins_ast {
                        let safe_val: String = pj
                            .value
                            .chars()
                            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                            .collect();
                        logical = logical.replace(&pj.name, &safe_val);
                    }
                    // Try to parse a simple column equality: `<lhs> = <rhs>`
                    let parse_col_eq = |s: &str| -> Option<(String, String)> {
                        // split on '=' once
                        let (l, r) = s.split_once('=')?;
                        let lhs = l.trim();
                        let rhs = r.trim();
                        if lhs.is_empty() || rhs.is_empty() { return None; }
                        // Require both sides look like column refs (contain a dot)
                        if !lhs.contains('.') || !rhs.contains('.') { return None; }
                        Some((lhs.to_string(), rhs.to_string()))
                    };

                    if let Some((lhs, rhs)) = parse_col_eq(&logical) {
                        // Structured join expression so Mongo adapter can map ColEq(local, foreign)
                        if j.type_join.eq_ignore_ascii_case("left") {
                            q = q.join_left_expr(j.table.clone(), QE::ColEq(lhs, rhs));
                        } else {
                            q = q.join_inner_expr(j.table.clone(), QE::ColEq(lhs, rhs));
                        }
                    } else {
                        // Fall back to raw join string; place it in legacy `on` so executors can parse on_raw
                        let kind = if j.type_join.eq_ignore_ascii_case("left") { QJK::Left } else { QJK::Inner };
                        q.joins.push(QJ { kind, table: j.table.clone(), on: logical, on_expr: None });
                    }
                }
            }

            // GROUP BY
            if !table_schema.get.column_groups.is_empty() {
                q = q.group_by(table_schema.get.column_groups.clone());
            }

            // HAVING (raw-safe from config)
            if !table_schema.get.having.is_empty() {
                let hv = table_schema
                    .get
                    .having
                    .iter()
                    .cloned()
                    .map(QE::Raw)
                    .collect::<Vec<_>>();
                q = q.having_expr(hv);
            }

            // pagination
            let offset_ast = (i_page_ast - 1) * i_limit_ast;
            q = q.limit(i_limit_ast as u32).offset(offset_ast.max(0) as u32);

            // Execute via generic DataStore; keep SQL preview for debug using a temporary SqlStore
            if *crate::ISDEBUG {
                let ds_prev = SqlStore::new(state.db.clone(), state.db_type.clone());
                let (sql_dbg, params_dbg) = ds_prev.preview_sql(&q);
                log_output("QUERY", "GET(AST)", route.as_ref(), sql_dbg, true);
                log_output("PARAMS", "GET(AST)", route.as_ref(), format!("{:?}", params_dbg), true);
            }
            let rows = match state.store.query(&q).await {
                Ok(rs) => rs,
                Err(e) => {
                    return HttpResponse::InternalServerError().json(WebResponse {
                        success: false,
                        message: format!("Error NCO-GET(AST): {}", e),
                        total_data: 0,
                        data: Value::default(),
                    });
                }
            };
            let total_data = 9999;
            let result = WebResponse {
                success: true,
                message: "Data found".to_string(),
                total_data,
                data: json!(rows),
            };
            
            // OPTIMIZATION: Automatic cache write-through when caching enabled
            if isredis {
                if let Some(ref k) = cache_key {
                    if table_schema.redis.ttl > 0 {
                        let ttl = table_schema.redis.ttl as usize;
                        match redis_set_json(k, &result, Some(ttl)).await {
                            Ok(_) => {
                                log_output(
                                    "REDIS",
                                    "CACHE WRITE",
                                    route.as_ref(),
                                    format!("Key: {}, TTL: {}s, Records: {}", k, ttl, total_data),
                                    true,
                                );
                            }
                            Err(e) => {
                                log_output(
                                    "ERROR",
                                    "CACHE WRITE",
                                    route.as_ref(),
                                    format!("Failed to cache: {}", e),
                                    false,
                                );
                            }
                        }
                    }
                }
            }
            
            HttpResponse::Ok().json(result)
    }

    // no legacy fallback; AST path returned above
}
