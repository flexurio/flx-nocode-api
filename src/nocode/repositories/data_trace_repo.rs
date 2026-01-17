use actix_web::web;
use crate::AppState;
use crate::model::{TableSchema, DbType};
use crate::storage::sql_store::SqlStore;
use crate::storage::ast::{Query as Q, Filter as F, Val as V, Expr as E};
use crate::helpers::split_column_operator;
use crate::log::log_output;
use serde_json::Value;

// Helper: build equality filter with NULL handling and numeric inference
fn build_eq_filter(column: String, raw: String) -> F {
    let s = raw.clone();
    if s.eq_ignore_ascii_case("NULL") { return F::IsNull(column); }
    if let Ok(n) = s.parse::<i64>() { return F::Eq(column, V::I64(n)); }
    if let Ok(f) = s.parse::<f64>() { return F::Eq(column, V::F64(f)); }
    F::Eq(column, V::Str(raw))
}

pub async fn perform_trace_execution(
    state: &web::Data<AppState>,
    table_schema: &TableSchema,
    route: &str,
    parameters: &Value,
) -> Result<String, String> {
    if state.db_type == DbType::Mongodb {
        return Err("TRACE insert-select upsert is not supported for MongoDB yet".to_string());
    }

    let ds = SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
    let mut q = Q::from(table_schema.table.clone());

    let mut is_deleted_at = true;
    let params_obj = parameters.as_object().unwrap_or(&serde_json::Map::new()).clone();
    let param_count = table_schema.trace.parameters.len();
    let mut filters: Vec<F> = Vec::with_capacity(param_count + 1);

    for param in table_schema.trace.parameters.iter() {
        for (key, value) in params_obj.iter() {
            if key.contains("deleted_at") { is_deleted_at = false; }
            if param == key {
                let val_str = value.as_str().unwrap_or("");
                let (column, operator, v_raw) = split_column_operator(param, &table_schema.table, val_str);
                
                if param.contains('|') {
                    let ps: Vec<&str> = param.split('|').collect();
                    let mut or_terms: Vec<F> = Vec::with_capacity(ps.len());
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

    if is_deleted_at {
        filters.push(F::IsNull(format!("{}.deleted_at", route)));
    }
    if !filters.is_empty() {
        q = q.r#where(F::And(filters));
    }

    // Projection columns + created_at
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

    // Prepare insert columns list
    let mut insert_cols: Vec<String> = table_schema.trace.column_inserts.to_vec();
    insert_cols.push("created_at".to_string());

    // Conflict keys
    let mut conflict_keys: Vec<String> = Vec::new();
    let dbt = state.db_type.as_str().to_lowercase();
    
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
        let uniques: Vec<_> = table_schema.indexes.iter().filter(|ix| ix.unique).collect();
        if uniques.len() == 1 {
            conflict_keys = uniques[0].columns.clone();
        }
    }

    if (dbt == "postgres" || dbt == "mssql") && conflict_keys.is_empty() {
        return Err(format!("TRACE requires 'column_conflicts' (or index:<name>) for backend '{}'", dbt));
    }

    let extra_assignments = vec![
        format!("updated_at={}", state.query_converter.datetime_now),
        "deleted_at=null".to_string(),
    ];

    // Compile SELECT
    let (select_sql, select_params) = ds.preview_sql(&q);
    if *crate::ISDEBUG {
        log_output("QUERY", "TRACE(AST-SELECT)", route, select_sql.clone(), true);
        log_output("PARAMS", "TRACE(AST-SELECT)", route, format!("{:?}", select_params), true);
    }

    // Compile Insert-Select-Upsert
    let (s_sql, compiled_params) = ds.preview_insert_select_upsert(
        &table_schema.trace.insert_into,
        &insert_cols,
        &select_sql,
        &conflict_keys,
        &extra_assignments,
        select_params,
    ).map_err(|e| format!("Unsupported TRACE operation for backend: {}", e))?;

    if *crate::ISDEBUG {
        log_output("QUERY", "TRACE(AST)", route, s_sql.clone(), true);
        log_output("PARAMS", "TRACE(AST)", route, format!("{:?}", compiled_params), true);
    }

    // Transaction
    let mut tx = state.store.begin_tx().await.map_err(|e| format!("Error starting transaction: {}", e))?;

    match tx.raw_sql(&s_sql, compiled_params).await {
        Ok(_) => {
            tx.commit().await.ok();
            Ok("Data inserted".to_string())
        }
        Err(err) => {
            tx.rollback().await.ok();
            Err(format!("Error NCO-TRACE: {}", err))
        }
    }
}
