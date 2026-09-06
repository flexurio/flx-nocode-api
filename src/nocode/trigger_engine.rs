//! Declarative Action Trigger Engine for `flx-nocode-api`.
//!
//! Executes automated cascading multi-table workflows (such as ERP Sales Order fulfillment,
//! stock deductions, AR invoice creation, and GL auto-posting) within the SAME atomic database
//! transaction scope (`TxStore`).
//!
//! If any action or validation fails (e.g. insufficient inventory), the error bubbles up
//! and causes the entire transaction to rollback (`tx.rollback()`).

use chrono::{Duration, Local};
use regex::Regex;
use serde_json::{Map, Value};

use crate::database::state::DbParam;
use crate::log::log_output;
use crate::model::{ActionTrigger, DbType, TableSchema, TriggerAction, TriggerCondition};
use crate::storage::traits::TxStore;

/// Context data available during trigger execution.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TriggerContext<'a> {
    pub parent_table: &'a str,
    pub parent_pk: &'a str,
    pub old_record: &'a Map<String, Value>,
    pub new_record: &'a Map<String, Value>,
    pub request_body: &'a Value,
    pub actor_id: Option<&'a str>,
}

/// Check whether a trigger condition is met based on old and new record states.
pub fn evaluate_condition(
    cond_opt: &Option<TriggerCondition>,
    event_name: &str,
    old_record: &Map<String, Value>,
    new_record: &Map<String, Value>,
) -> bool {
    let Some(cond) = cond_opt else {
        // No condition declared: runs for any event matching the event_name
        return true;
    };

    // If target field is specified
    if !cond.field.is_empty() {
        let old_val = old_record.get(&cond.field);
        let new_val = new_record.get(&cond.field);

        // For update / status_change events, if the field didn't change at all, do NOT trigger.
        if event_name.contains("update") || event_name.contains("status") {
            if old_val == new_val && !old_record.is_empty() {
                return false;
            }
        }

        // Check `from` constraint if specified
        if let Some(from_rule) = &cond.from {
            let matches_from = match from_rule {
                Value::Array(arr) => arr.iter().any(|v| value_matches(v, old_val)),
                single => value_matches(single, old_val),
            };
            if !matches_from {
                return false;
            }
        }

        // Check `to` constraint if specified
        if let Some(to_rule) = &cond.to {
            let matches_to = match to_rule {
                Value::Array(arr) => arr.iter().any(|v| value_matches(v, new_val)),
                single => value_matches(single, new_val),
            };
            if !matches_to {
                return false;
            }
        }
    }

    true
}

/// Flexible value comparison (handles string vs number, case-insensitive strings).
fn value_matches(expected: &Value, actual_opt: Option<&Value>) -> bool {
    let Some(actual) = actual_opt else {
        return expected.is_null();
    };

    match (expected, actual) {
        (Value::String(s1), Value::String(s2)) => {
            s1.trim().eq_ignore_ascii_case(s2.trim())
        }
        (Value::Number(n1), Value::Number(n2)) => n1 == n2,
        (Value::String(s), Value::Number(n)) => s.trim() == n.to_string(),
        (Value::Number(n), Value::String(s)) => s.trim() == n.to_string(),
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        (Value::Null, Value::Null) => true,
        _ => expected == actual,
    }
}

/// Resolve template strings with placeholders:
/// - `{parent.<col>}`: field from `new_record` (fallback `old_record`)
/// - `{item.<col>}`: field from child row during `iterate_detail`
/// - `{request.<col>}`: field from request body
/// - `{now:YYYY-MM-DD}`, `{now+30d:YYYY-MM-DD}`, `{now()}`
/// - `{<var>|<default>}`: fallback syntax
pub fn interpolate_string(
    template: &str,
    ctx: &TriggerContext,
    item_opt: Option<&Map<String, Value>>,
) -> String {
    let re = Regex::new(r"\{([^}]+)\}").expect("Invalid regex");
    re.replace_all(template, |caps: &regex::Captures| {
        let raw_token = &caps[1];
        let (expr, fallback) = match raw_token.split_once('|') {
            Some((e, f)) => (e.trim(), Some(f.trim())),
            None => (raw_token.trim(), None),
        };

        // Date interpolation: e.g. "now:YYYY-MM-DD" or "now+30d:YYYY-MM-DD"
        if expr == "now()" || expr.starts_with("now") || expr.starts_with("date:") {
            return format_date_expression(expr);
        }

        // Resolving parent / item / request references
        if let Some(prop) = expr.strip_prefix("parent.") {
            if let Some(v) = ctx.new_record.get(prop).or_else(|| ctx.old_record.get(prop)) {
                return json_val_to_str(v);
            }
        } else if let Some(prop) = expr.strip_prefix("item.") {
            if let Some(item) = item_opt {
                if let Some(v) = item.get(prop) {
                    return json_val_to_str(v);
                }
            }
        } else if let Some(prop) = expr.strip_prefix("request.") {
            if let Some(v) = ctx.request_body.get(prop) {
                return json_val_to_str(v);
            }
        } else {
            // Direct lookups: check item first, then parent, then request
            if let Some(item) = item_opt && let Some(v) = item.get(expr) {
                return json_val_to_str(v);
            }
            if let Some(v) = ctx.new_record.get(expr).or_else(|| ctx.old_record.get(expr)) {
                return json_val_to_str(v);
            }
            if let Some(v) = ctx.request_body.get(expr) {
                return json_val_to_str(v);
            }
        }

        fallback.unwrap_or("").to_string()
    })
    .to_string()
}

fn json_val_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "".to_string(),
        _ => v.to_string(),
    }
}

/// Parse date formulas such as `now:YYYY-MM-DD`, `now+30d:YYYY-MM-DD`, `now()`
fn format_date_expression(expr: &str) -> String {
    let now = Local::now();
    let mut target_date = now;

    if expr.contains('+') {
        if let Some(plus_part) = expr.split('+').nth(1) {
            let num_str: String = plus_part.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(days) = num_str.parse::<i64>() {
                target_date = now + Duration::days(days);
            }
        }
    }

    if expr.contains("YYYY-MM-DD") || expr == "now()" {
        target_date.format("%Y-%m-%d").to_string()
    } else if expr.contains("YYYY/MM") {
        target_date.format("%Y/%m").to_string()
    } else if expr.contains("YYYYMMDD") {
        target_date.format("%Y%m%d").to_string()
    } else {
        target_date.format("%Y-%m-%d").to_string()
    }
}

/// Evaluate arithmetic set expression, e.g. "qty - {item.qty}"
fn compute_set_value(
    current_val: f64,
    set_expr: &str,
    ctx: &TriggerContext,
    item_opt: Option<&Map<String, Value>>,
) -> Result<f64, String> {
    let interpolated = interpolate_string(set_expr, ctx, item_opt);
    let trimmed = interpolated.trim();

    // Check pattern: "<col_name> - <val>"
    if let Some((_col, right)) = trimmed.split_once('-') {
        let deduct_val: f64 = right
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("Cannot parse deduction value in expression '{}'", trimmed))?;
        return Ok(current_val - deduct_val);
    }

    // Check pattern: "<col_name> + <val>"
    if let Some((_col, right)) = trimmed.split_once('+') {
        let add_val: f64 = right
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("Cannot parse addition value in expression '{}'", trimmed))?;
        return Ok(current_val + add_val);
    }

    // Fallback: evaluate as direct number
    if let Ok(num) = trimmed.parse::<f64>() {
        return Ok(num);
    }

    Err(format!("Unsupported set expression: '{}'", set_expr))
}

/// Execute all matching triggers within the current transaction scope.
pub async fn execute_triggers<'a>(
    db_type: DbType,
    tx: &mut (dyn TxStore + 'a),
    table_schema: &TableSchema,
    ctx: &TriggerContext<'a>,
    event_name: &str,
) -> Result<Vec<String>, String> {
    if table_schema.action_triggers.is_empty() {
        return Ok(vec![]);
    }

    let mut executed_triggers = Vec::new();

    for trigger in &table_schema.action_triggers {
        // Match event name (e.g. "on_update", "on_status_change")
        let trigger_event = trigger.event.to_lowercase();
        let matches_event = trigger_event == event_name
            || (event_name == "on_update" && trigger_event == "on_status_change")
            || trigger_event == "any";

        if !matches_event {
            continue;
        }

        // Evaluate conditions
        if !evaluate_condition(&trigger.condition, event_name, ctx.old_record, ctx.new_record) {
            continue;
        }

        if *crate::ISDEBUG {
            log_output(
                "TRIGGER",
                "START",
                &trigger.name,
                format!("Firing trigger for table '{}' PK '{}'", ctx.parent_table, ctx.parent_pk),
                true,
            );
        }

        // Execute sequential actions
        for action in &trigger.actions {
            execute_action(db_type.clone(), tx, action, ctx, None).await.map_err(|err| {
                format!(
                    "Trigger '{}' action failed: {}",
                    trigger.name, err
                )
            })?;
        }

        executed_triggers.push(trigger.name.clone());

        if *crate::ISDEBUG {
            log_output(
                "TRIGGER",
                "SUCCESS",
                &trigger.name,
                format!("Trigger '{}' completed successfully", trigger.name),
                true,
            );
        }
    }

    Ok(executed_triggers)
}

/// Execute a single trigger action (and recurse if `iterate_detail`).
fn execute_action<'a>(
    db_type: DbType,
    tx: &'a mut (dyn TxStore + 'a),
    action: &'a TriggerAction,
    ctx: &'a TriggerContext<'a>,
    item_opt: Option<&'a Map<String, Value>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        let action_type = action.action_type.to_lowercase();

        match action_type.as_str() {
            // ── 1. Iterate Detail (Line Items / BOM) ─────────────────────────────
            "iterate_detail" | "loop_detail" | "for_each_detail" => {
                let detail_table = action
                    .detail_table
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&action.target_table);

                if detail_table.is_empty() {
                    return Err("iterate_detail requires 'detail_table' or 'target_table'".to_string());
                }

                let fk_column = action
                    .foreign_key
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("sales_order_id");

                // Query child items belonging to this parent header
                let select_sql = format!(
                    "SELECT * FROM {} WHERE {} = ?",
                    detail_table, fk_column
                );
                let built_sql = crate::database::state::rehydrate_placeholders(
                    &select_sql,
                    db_type.as_str(),
                );

                let fk_param = DbParam::Str(ctx.parent_pk.to_string());
                let items = tx
                    .raw_sql(&built_sql, vec![fk_param])
                    .await
                    .map_err(|e| format!("Querying detail items from '{}' failed: {}", detail_table, e))?;

                let sub_actions = action.actions.as_deref().unwrap_or(&[]);
                if sub_actions.is_empty() {
                    return Ok(());
                }

                for item in &items {
                    if let Some(item_map) = item.as_object() {
                        for sub_act in sub_actions {
                            execute_action(db_type.clone(), tx, sub_act, ctx, Some(item_map)).await?;
                        }
                    }
                }
                Ok(())
            }

            // ── 2. Update (e.g. Deduct Inventory Lot) ───────────────────────────
            "update" | "decrement" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'update' requires 'target_table'".to_string());
                }

                let filter_map = action.filter.as_ref().ok_or_else(|| {
                    format!("Action 'update' on '{}' requires 'filter'", target_table)
                })?;
                let set_map = action.set.as_ref().ok_or_else(|| {
                    format!("Action 'update' on '{}' requires 'set'", target_table)
                })?;

                // 1. Build filter WHERE clause
                let mut where_clauses = Vec::new();
                let mut filter_params = Vec::new();
                for (col, tmpl_val) in filter_map {
                    let resolved_str = match tmpl_val {
                        Value::String(s) => interpolate_string(s, ctx, item_opt),
                        other => json_val_to_str(other),
                    };
                    where_clauses.push(format!("{} = ?", col));
                    filter_params.push(DbParam::Str(resolved_str));
                }
                let where_sql = where_clauses.join(" AND ");

                // 2. Fetch current record to perform atomic validation & calculation
                let select_sql = format!("SELECT * FROM {} WHERE {}", target_table, where_sql);
                let built_select = crate::database::state::rehydrate_placeholders(
                    &select_sql,
                    db_type.as_str(),
                );

                let rows = tx
                    .raw_sql(&built_select, filter_params.clone())
                    .await
                    .map_err(|e| format!("Error querying record in '{}': {}", target_table, e))?;

                if rows.is_empty() {
                    return Err(format!(
                        "Record not found in '{}' matching filter {:?}",
                        target_table, filter_map
                    ));
                }

                let existing_row = rows[0].as_object().ok_or_else(|| {
                    format!("Invalid row format returned from '{}'", target_table)
                })?;

                // 3. Compute new values and enforce validations
                let mut update_assignments = Vec::new();
                let mut update_params = Vec::new();

                for (col, set_expr_val) in set_map {
                    let current_col_val = match existing_row.get(col) {
                        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                        Some(Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
                        _ => 0.0,
                    };

                    let expr_str = match set_expr_val {
                        Value::String(s) => s.as_str(),
                        _ => return Err(format!("Set expression for '{}' must be a string", col)),
                    };

                    let new_calculated = compute_set_value(current_col_val, expr_str, ctx, item_opt)?;

                    // Enforce minimum validation constraint (e.g. preventing negative inventory)
                    if let Some(validate) = &action.validate {
                        if let Some(min_map) = &validate.min {
                            if let Some(min_val_json) = min_map.get(col) {
                                let min_threshold = min_val_json.as_f64().unwrap_or(0.0);
                                if new_calculated < min_threshold {
                                    let default_msg = format!(
                                        "Validation failed on '{}': resulting {} ({}) is below minimum allowed ({})",
                                        target_table, col, new_calculated, min_threshold
                                    );
                                    let err_msg = validate
                                        .error_message
                                        .as_ref()
                                        .map(|m| interpolate_string(m, ctx, item_opt))
                                        .unwrap_or(default_msg);
                                    return Err(err_msg);
                                }
                            }
                        }
                    }

                    update_assignments.push(format!("{} = ?", col));
                    if new_calculated.fract() == 0.0 {
                        update_params.push(DbParam::I64(new_calculated as i64));
                    } else {
                        update_params.push(DbParam::F64(new_calculated));
                    }
                }

                // 4. Execute the update query
                let mut all_params = update_params;
                all_params.extend(filter_params);

                let update_sql = format!(
                    "UPDATE {} SET {} WHERE {}",
                    target_table,
                    update_assignments.join(", "),
                    where_sql
                );
                let built_update = crate::database::state::rehydrate_placeholders(
                    &update_sql,
                    db_type.as_str(),
                );

                tx.raw_sql(&built_update, all_params)
                    .await
                    .map_err(|e| format!("Failed to update '{}': {}", target_table, e))?;

                Ok(())
            }

            // ── 3. Insert Record (e.g. Create AR Invoice Draft) ─────────────────
            "insert" | "create_record" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'insert' requires 'target_table'".to_string());
                }

                let fields_map = action.fields.as_ref().ok_or_else(|| {
                    format!("Action 'insert' on '{}' requires 'fields'", target_table)
                })?;

                let mut cols = Vec::new();
                let mut placeholders = Vec::new();
                let mut params = Vec::new();

                for (col, val) in fields_map {
                    cols.push(col.clone());
                    placeholders.push("?");

                    let resolved_str = match val {
                        Value::String(s) => interpolate_string(s, ctx, item_opt),
                        other => json_val_to_str(other),
                    };

                    // Type coercion for numeric values
                    if let Ok(i) = resolved_str.parse::<i64>() {
                        params.push(DbParam::I64(i));
                    } else if let Ok(f) = resolved_str.parse::<f64>() {
                        params.push(DbParam::F64(f));
                    } else {
                        params.push(DbParam::Str(resolved_str));
                    }
                }

                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    target_table,
                    cols.join(", "),
                    placeholders.join(", ")
                );
                let built_insert = crate::database::state::rehydrate_placeholders(
                    &insert_sql,
                    db_type.as_str(),
                );

                tx.raw_sql(&built_insert, params)
                    .await
                    .map_err(|e| format!("Failed to insert into '{}': {}", target_table, e))?;

                Ok(())
            }

            // ── 4. Insert Batch (e.g. GL Journal Lines) ─────────────────────────
            "insert_batch" | "create_records" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'insert_batch' requires 'target_table'".to_string());
                }

                let rows = action.rows.as_deref().unwrap_or(&[]);
                for row_map in rows {
                    let mut cols = Vec::new();
                    let mut placeholders = Vec::new();
                    let mut params = Vec::new();

                    for (col, val) in row_map {
                        cols.push(col.clone());
                        placeholders.push("?");

                        let resolved_str = match val {
                            Value::String(s) => interpolate_string(s, ctx, item_opt),
                            other => json_val_to_str(other),
                        };

                        if let Ok(i) = resolved_str.parse::<i64>() {
                            params.push(DbParam::I64(i));
                        } else if let Ok(f) = resolved_str.parse::<f64>() {
                            params.push(DbParam::F64(f));
                        } else {
                            params.push(DbParam::Str(resolved_str));
                        }
                    }

                    let insert_sql = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        target_table,
                        cols.join(", "),
                        placeholders.join(", ")
                    );
                    let built_insert = crate::database::state::rehydrate_placeholders(
                        &insert_sql,
                        db_type.as_str(),
                    );

                    tx.raw_sql(&built_insert, params)
                        .await
                        .map_err(|e| format!("Failed batch insert into '{}': {}", target_table, e))?;
                }

                Ok(())
            }

            // ── 5. Raw/Parameterized SQL ─────────────────────────────────────────
            "sql" => {
                let stmt = action
                    .statement
                    .as_deref()
                    .ok_or_else(|| "Action 'sql' requires 'statement'".to_string())?;

                let mut params = Vec::new();
                if let Some(param_templates) = &action.params {
                    for tmpl in param_templates {
                        let resolved = interpolate_string(tmpl, ctx, item_opt);
                        if let Ok(i) = resolved.parse::<i64>() {
                            params.push(DbParam::I64(i));
                        } else if let Ok(f) = resolved.parse::<f64>() {
                            params.push(DbParam::F64(f));
                        } else {
                            params.push(DbParam::Str(resolved));
                        }
                    }
                }

                let built_sql = crate::database::state::rehydrate_placeholders(stmt, db_type.as_str());
                tx.raw_sql(&built_sql, params)
                    .await
                    .map_err(|e| format!("Custom SQL action failed: {}", e))?;

                Ok(())
            }

            unsupported => Err(format!("Unsupported action type: '{}'", unsupported)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_condition_matches_field_change() {
        let cond = TriggerCondition {
            field: "status".to_string(),
            from: Some(serde_json::json!(["APPROVED", "PENDING"])),
            to: Some(serde_json::json!("SHIPPED")),
            expression: None,
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));

        let mut new_rec = Map::new();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        assert!(evaluate_condition(&Some(cond.clone()), "on_update", &old_rec, &new_rec));

        // When old status is not in allowed `from` list
        old_rec.insert("status".to_string(), serde_json::json!("CANCELLED"));
        assert!(!evaluate_condition(&Some(cond.clone()), "on_update", &old_rec, &new_rec));

        // When status didn't change (idempotency check)
        old_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));
        assert!(!evaluate_condition(&Some(cond), "on_update", &old_rec, &new_rec));
    }

    #[test]
    fn test_interpolate_string_parent_and_item() {
        let mut old_rec = Map::new();
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        let mut new_rec = old_rec.clone();
        new_rec.insert("total_net".to_string(), serde_json::json!(250000));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: Some("1"),
        };

        let mut item = Map::new();
        item.insert("product_id".to_string(), serde_json::json!(12));
        item.insert("qty".to_string(), serde_json::json!(5));

        let tmpl = "UPDATE lot SET qty = qty - {item.qty} WHERE product_id = {item.product_id} AND so = {parent.customer_id}";
        let res = interpolate_string(tmpl, &ctx, Some(&item));
        assert_eq!(res, "UPDATE lot SET qty = qty - 5 WHERE product_id = 12 AND so = 3");
    }

    #[test]
    fn test_compute_set_value_deduction() {
        let old_rec = Map::new();
        let new_rec = Map::new();
        let req_body = serde_json::json!({});
        let ctx = TriggerContext {
            parent_table: "test",
            parent_pk: "1",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut item = Map::new();
        item.insert("qty".to_string(), serde_json::json!(5));

        let res = compute_set_value(100.0, "qty - {item.qty}", &ctx, Some(&item)).unwrap();
        assert_eq!(res, 95.0);
    }

    // ── Mock TxStore for end-to-end ERP trigger testing ─────────────────────
    struct MockTxStore {
        pub executed_sqls: Vec<(String, Vec<DbParam>)>,
        pub lot_stock_12: f64,
        pub lot_stock_14: f64,
    }

    #[async_trait::async_trait]
    impl TxStore for MockTxStore {
        async fn query(&mut self, _q: &crate::storage::ast::Query) -> anyhow::Result<Vec<Value>> {
            Ok(vec![])
        }
        async fn insert(&mut self, _collection: &str, doc: Value) -> anyhow::Result<Value> {
            Ok(doc)
        }
        async fn update(&mut self, _collection: &str, _filter: Option<crate::storage::ast::Filter>, _patch: Value) -> anyhow::Result<u64> {
            Ok(1)
        }
        async fn delete(&mut self, _collection: &str, _filter: Option<crate::storage::ast::Filter>) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn raw_sql(&mut self, sql: &str, params: Vec<DbParam>) -> anyhow::Result<Vec<Value>> {
            self.executed_sqls.push((sql.to_string(), params.clone()));

            // Detail items for Sales Order
            if sql.contains("SELECT * FROM transaction_sales_order_item") {
                return Ok(vec![
                    serde_json::json!({
                        "id": 10,
                        "sales_order_id": 105,
                        "product_id": 12,
                        "product_name": "Paracetamol 500mg",
                        "qty": 5
                    }),
                    serde_json::json!({
                        "id": 11,
                        "sales_order_id": 105,
                        "product_id": 14,
                        "product_name": "Amoxicillin 500mg",
                        "qty": 10
                    }),
                ]);
            }

            // Current inventory lot stock query
            if sql.contains("SELECT * FROM transaction_product_lot") {
                for p in &params {
                    if let DbParam::Str(s) = p {
                        if s == "12" {
                            return Ok(vec![serde_json::json!({
                                "id": 1,
                                "product_id": 12,
                                "qty": self.lot_stock_12,
                            })]);
                        }
                        if s == "14" {
                            return Ok(vec![serde_json::json!({
                                "id": 2,
                                "product_id": 14,
                                "qty": self.lot_stock_14,
                            })]);
                        }
                    }
                }
            }

            Ok(vec![])
        }
        async fn commit(self: Box<Self>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sales_order_fulfillment_end_to_end() {
        let trigger_json = r#"{
            "name": "sales_order_fulfillment",
            "event": "on_update",
            "condition": {
                "field": "status",
                "from": ["APPROVED", "PENDING"],
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "name": "deduct_inventory_stock",
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "foreign_key": "sales_order_id",
                    "actions": [
                        {
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 } }
                        }
                    ]
                },
                {
                    "name": "generate_ar_invoice",
                    "type": "insert",
                    "target_table": "transaction_account_receivable",
                    "fields": {
                        "customer_id": "{parent.customer_id}",
                        "total_receivable": "{parent.total_net}",
                        "status": "UNPAID"
                    }
                },
                {
                    "name": "post_gl_entries",
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "rows": [
                        {
                            "account_code": "1103",
                            "account_name": "Accounts Receivable (AR)",
                            "debit": "{parent.total_net}",
                            "credit": 0
                        },
                        {
                            "account_code": "4101",
                            "account_name": "Sales Revenue",
                            "debit": 0,
                            "credit": "{parent.total_net}"
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(105));
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        old_rec.insert("total_net".to_string(), serde_json::json!(250000));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: Some("1"),
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 50.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Trigger execution should succeed: {:?}", res.err());
        let executed = res.unwrap();
        assert_eq!(executed, vec!["sales_order_fulfillment"]);

        // Verify SQL queries executed:
        // 1. SELECT items from detail
        assert!(mock_tx.executed_sqls.iter().any(|(s, _)| s.contains("SELECT * FROM transaction_sales_order_item")));

        // 2. Stock deductions: lot 12 deducted from 100 -> 95, lot 14 deducted from 50 -> 40
        let updates: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("UPDATE transaction_product_lot"))
            .collect();
        assert_eq!(updates.len(), 2, "Should execute 2 lot stock updates (1 per item)");

        // Product 12 lot update (95)
        let has_update_12 = updates.iter().any(|(_, params)| {
            params.iter().any(|p| match p { DbParam::I64(n) => *n == 95, DbParam::F64(f) => *f == 95.0, _ => false })
        });
        assert!(has_update_12, "Lot 12 should be updated to 95");

        // Product 14 lot update (40)
        let has_update_14 = updates.iter().any(|(_, params)| {
            params.iter().any(|p| match p { DbParam::I64(n) => *n == 40, DbParam::F64(f) => *f == 40.0, _ => false })
        });
        assert!(has_update_14, "Lot 14 should be updated to 40");

        // 3. AR invoice creation
        let ar_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_account_receivable"))
            .collect();
        assert_eq!(ar_inserts.len(), 1, "Should create 1 AR invoice draft");
        let ar_has_total = ar_inserts[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 250000, _ => false });
        assert!(ar_has_total, "AR invoice should have total 250000");

        // 4. GL Journal entries
        let gl_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_general_ledger_line"))
            .collect();
        assert_eq!(gl_inserts.len(), 2, "Should post 2 GL journal lines (debit & credit)");
    }

    #[tokio::test]
    async fn test_sales_order_fulfillment_fails_on_insufficient_stock() {
        let trigger_json = r#"{
            "name": "sales_order_fulfillment",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "actions": [
                        {
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 } }
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        // Insufficient stock: lot 12 has only 2 items in stock, but order item requires 5
        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 2.0,
            lot_stock_14: 50.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_err(), "Trigger execution must fail when stock is insufficient");
        let err_msg = res.unwrap_err();
        assert!(err_msg.contains("Validation failed on 'transaction_product_lot'"), "Error should mention validation failure: {}", err_msg);
    }
}
