use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::{
    auth::{check_access, get_user_info_from_token},
    helpers::{split_column_operator},
    // rate limiting handled by middleware
    log::log_output,
    model::{TableSchema, WebResponse},
    AppState,
};
use crate::storage::sql_store::SqlStore;
use crate::storage::ast::{Query as Q, Filter as F, Val as V, Expr as E};
use std::sync::Arc;

// NCO-TRACE
pub async fn process(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schema: Arc<TableSchema>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    // Rate limiting removed (now global)

    if state.require_auth && !state.route_publics.contains(&route){
        let claims = match get_user_info_from_token(req, state.clone()) {
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

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    // claims.id retained for potential auditing in future (actor_id removed)
    }
    // Per-user rate limiting removed (handled globally)

    // Resolve schema first
    // let table_schema: TableSchema = filter_table_schema(&table_schemas, route.clone()).await; -- Use passed schema
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }
    // Build AST for SELECT part and bind parameters safely
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let mut q = Q::from(table_schema.table.clone());

    let mut is_deleted_at = true;
    let params_obj = parameters.clone().into_inner();
    let param_count = table_schema.trace.parameters.len();
    let mut filters: Vec<F> = Vec::with_capacity(param_count + 1); // Pre-allocate
    for param in table_schema.trace.parameters.iter() {
        for (key, value) in params_obj.as_object().unwrap_or(&serde_json::Map::new()).iter() {
            if key.contains("deleted_at") { is_deleted_at = false; }
            if param == key {
                let val_str = value.as_str().unwrap_or("");
                // split_column_operator masih mengembalikan potongan string, tapi kita perlu binding.
                // Gunakan operator dan kolom, lalu tentukan Filter AST yang sepadan.
                let (column, operator, v_raw) = split_column_operator(param, &table_schema.table, val_str);
                // Handle OR pipe: a|b|c kita jadikan Or([...])
                if param.contains('|') {
                    let ps: Vec<&str> = param.split('|').collect();
                    let mut or_terms: Vec<F> = Vec::with_capacity(ps.len()); // Pre-allocate
                    for p in ps {
                        let (c2, op2, v2) = split_column_operator(p, &table_schema.table, val_str);
                        let filt = match op2.as_str() {
                            "=" => build_eq_filter(c2, v2),
                            ">" => F::Gt(c2, V::Str(v2)),
                            ">=" => F::Gte(c2, V::Str(v2)),
                            "<" => F::Lt(c2, V::Str(v2)),
                            "<=" => F::Lte(c2, V::Str(v2)),
                            "<>" | "!=" => F::Ne(c2, V::Str(v2)),
                            "LIKE" => F::Like(c2, v2),
                            _ => build_eq_filter(c2, v2),
                        };
                        or_terms.push(filt);
                    }
                    if !or_terms.is_empty() { filters.push(F::Or(or_terms)); }
                } else {
                    let filt = match operator.as_str() {
                        "=" => build_eq_filter(column, v_raw),
                        ">" => F::Gt(column, V::Str(v_raw)),
                        ">=" => F::Gte(column, V::Str(v_raw)),
                        "<" => F::Lt(column, V::Str(v_raw)),
                        "<=" => F::Lte(column, V::Str(v_raw)),
                        "<>" | "!=" => F::Ne(column, V::Str(v_raw)),
                        "LIKE" => F::Like(column, v_raw),
                        _ => build_eq_filter(column, v_raw),
                    };
                    filters.push(filt);
                }
            }
        }
    }

    // Tambah filter deleted_at IS NULL jika tidak disediakan
    if is_deleted_at {
        filters.push(F::IsNull(format!("{}.deleted_at", route)));
    }
    if !filters.is_empty() {
        q = q.r#where(F::And(filters));
    }

    // Projection columns + created_at (raw now expr)
    let mut select_columns = table_schema.trace.column_selects.clone();
    select_columns.push(format!("{} as created_at", state.query_converter.datetime_now));
    q = q.select(select_columns);

    // Joins
    for jt in table_schema.trace.join_tables.iter() {
        match jt.type_join.to_ascii_lowercase().as_str() {
            "left" => { q = q.join_left_expr(jt.table.clone(), E::Raw(jt.logical.clone())); }
            _ => { q = q.join_inner_expr(jt.table.clone(), E::Raw(jt.logical.clone())); }
        }
    }
    // Group by
    if !table_schema.trace.column_groups.is_empty() {
        q = q.group_by(table_schema.trace.column_groups.clone());
    }

    // Prepare insert columns list (trace.column_inserts + created_at)
    let mut insert_cols: Vec<String> = table_schema.trace.column_inserts.to_vec();
    insert_cols.push("created_at".to_string());

    // Conflict keys and extra update assignments, dialect-aware via SqlStore
    let mut conflict_keys: Vec<String> = Vec::new();
    let dbt = state.db_type.to_lowercase();
    // Resolve conflict keys allowing special token index:NAME
    if !table_schema.trace.column_conflicts.is_empty() {
        if let Some(idx_tok) = table_schema
            .trace
            .column_conflicts
            .iter()
            .find(|s| s.to_lowercase().starts_with("index:"))
        {
            let name = idx_tok.split_once(':').map(|(_, n)| n.trim()).unwrap_or("");
            if let Some(ix) = table_schema
                .indexes
                .iter()
                .find(|ix| ix.name.eq_ignore_ascii_case(name))
            {
                conflict_keys = ix.columns.clone();
            }
        }
        if conflict_keys.is_empty() {
            conflict_keys = table_schema.trace.column_conflicts.clone();
        }
    } else if dbt == "mssql" {
        // allow unambiguous single unique index as fallback
        let uniques: Vec<_> = table_schema.indexes.iter().filter(|ix| ix.unique).collect();
        if uniques.len() == 1 {
            conflict_keys = uniques[0].columns.clone();
        }
    }
    // Guard: some backends require keys for upsert/merge semantics
    if (dbt == "postgres" || dbt == "mssql") && conflict_keys.is_empty() {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: format!(
                "TRACE requires 'column_conflicts' (or index:<name>) for backend '{}'",
                dbt
            ),
            total_data: 0,
            data: Value::Null,
        });
    }
    // conflict_clause previously included updated_at/deleted_at; keep those as extra assignments
    let extra_assignments = vec![
        format!("updated_at={}", state.query_converter.datetime_now),
        "deleted_at=null".to_string(),
    ];

    // Compile SELECT via AST to SQL and params
    let (select_sql, select_params) = ds.preview_sql(&q);
    if *crate::ISDEBUG {
        log_output("QUERY", "TRACE(AST-SELECT)", route.as_str(), select_sql.clone(), true);
        log_output("PARAMS", "TRACE(AST-SELECT)", route.as_str(), format!("{:?}", select_params), true);
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    let (s_sql, compiled_params) = match ds.preview_insert_select_upsert(
        &table_schema.trace.insert_into,
        &insert_cols,
        &select_sql,
        &conflict_keys,
        &extra_assignments,
        select_params,
    ) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Unsupported TRACE operation for backend: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    if *crate::ISDEBUG {
        log_output("QUERY", "TRACE(AST)", route.as_str(), s_sql.clone(), true);
        log_output("PARAMS", "TRACE(AST)", route.as_str(), format!("{:?}", compiled_params), true);
    }

    // MongoDB: current TRACE path relies on SQL upsert; return explicit unsupported for Mongo
    if state.db_type == "mongodb" {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "TRACE insert-select upsert is not supported for MongoDB yet".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }

    // Begin transaction via generic store
    let mut tx = match state.store.begin_tx().await {
        Ok(t) => t,
        Err(err) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error starting transaction: {}", err),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    match tx.raw_sql(&s_sql, compiled_params).await {
        Ok(_) => {
            // Commit transaction
            tx.commit().await.ok();
            HttpResponse::Ok().json(WebResponse {
                success: true,
                message: "Data inserted".to_string(),
                total_data: 1,
                data: Value::Null,
            })
        }

        Err(err) => {
            tx.rollback().await.ok();
            HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error NCO-TRACE: {}", err),
                total_data: 0,
                data: Value::Null,
            })
        }
    }
}

// Helper: build equality filter with NULL handling and numeric inference
fn build_eq_filter(column: String, raw: String) -> F {
    let s = raw.clone();
    if s.eq_ignore_ascii_case("NULL") { return F::IsNull(column); }
    if let Ok(n) = s.parse::<i64>() { return F::Eq(column, V::I64(n)); }
    if let Ok(f) = s.parse::<f64>() { return F::Eq(column, V::F64(f)); }
    F::Eq(column, V::Str(raw))
}
