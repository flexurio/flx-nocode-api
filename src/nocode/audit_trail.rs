//! Structured CPOB/GxP Audit Trail (`flx_audit_trail`).
//!
//! Records immutable, column-level change history inside the SAME database
//! transaction as the business mutation so an audit row exists if and only if
//! the underlying write committed.
//!
//! Table layout:
//! `flx_audit_trail (id, entity, table_name, record_id, action, column_name,
//!  old_value, new_value, actor_id, actor_custom, reason, request_id, at)`
//!
//! Read-only query surface:
//! `GET /audit_trail?record_id.eq=…&entity.eq=…` (standard filter operators,
//! pagination, sorting, cursor) restricted to `ADMIN_ROLES` or roles listed in
//! an entity's `audit_trail.readers`. Audit rows can never be modified or
//! deleted through the API.

use std::collections::HashMap;
use std::sync::Arc;

use actix_web::HttpRequest;
use serde_json::{Map, Value, json};

use crate::database::state::AppState;
use crate::helpers::SUPPORTED_OPERATORS;
use crate::model::{Column, Index, PrimaryKey, TableSchema};
use crate::nocode::repositories::data_read_repo;
use crate::nocode::trigger_engine::value_matches;
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::storage::traits::TxStore;

pub const TABLE: &str = "flx_audit_trail";

/// Prefix of the `require_reason_when` violation message (mapped to HTTP 400).
pub const REASON_REQUIRED_PREFIX: &str = "Change reason required";

/// HTTP header carrying the mandatory/optional change reason for CPOB audits.
pub const CHANGE_REASON_HEADER: &str = "x-change-reason";

/// Columns ignored when `audit_trail.columns` is empty (auto-maintained metadata).
const AUTO_IGNORED_COLUMNS: &[&str] = &["updated_at", "updated_by_id", "version"];

/// Schema of the core `flx_audit_trail` table.
///
/// String timestamps (`varchar(40)`, ISO-8601 UTC with milliseconds) keep DDL
/// and range queries (`at.gte=…&at.lte=…`) identical across SQLite, MySQL,
/// PostgreSQL and MSSQL — matching `flx_outbox` and `flx_sequences`.
pub fn table_schema() -> TableSchema {
    let col = |name: &str, type_data: &str, nullable: bool| Column {
        name: name.into(),
        type_data: type_data.into(),
        nullable,
        ..Default::default()
    };

    let mut t = TableSchema {
        table: TABLE.into(),
        ..Default::default()
    };
    t.primary_key = PrimaryKey {
        columns: vec!["id".into()],
    };
    t.columns = vec![
        Column {
            name: "id".into(),
            type_data: "bigint".into(),
            auto_increment: true,
            nullable: false,
            ..Default::default()
        },
        col("entity", "varchar(120)", false),
        col("table_name", "varchar(120)", false),
        col("record_id", "varchar(120)", false),
        col("action", "varchar(20)", false),
        col("column_name", "varchar(120)", false),
        col("old_value", "text", true),
        col("new_value", "text", true),
        col("actor_id", "varchar(120)", false),
        col("actor_custom", "text", true),
        col("reason", "text", true),
        col("request_id", "varchar(64)", true),
        col("at", "varchar(40)", false),
    ];
    t.indexes = vec![
        Index {
            name: "idx_audit_trail_entity_record".into(),
            columns: vec!["entity".into(), "record_id".into()],
            unique: false,
        },
        Index {
            name: "idx_audit_trail_entity_at".into(),
            columns: vec!["entity".into(), "at".into()],
            unique: false,
        },
        Index {
            name: "idx_audit_trail_actor_at".into(),
            columns: vec!["actor_id".into(), "at".into()],
            unique: false,
        },
        Index {
            name: "idx_audit_trail_request".into(),
            columns: vec!["request_id".into()],
            unique: false,
        },
    ];

    let all_cols = [
        "id",
        "entity",
        "table_name",
        "record_id",
        "action",
        "column_name",
        "old_value",
        "new_value",
        "actor_id",
        "actor_custom",
        "reason",
        "request_id",
        "at",
    ];
    t.get.enable_method = true;
    t.get.columns = all_cols.iter().map(|c| format!("{}.{}", TABLE, c)).collect();
    t.get.order_by = vec![format!("{}.id", TABLE)];

    let mut params = Vec::with_capacity(all_cols.len() * SUPPORTED_OPERATORS.len());
    for c in &all_cols {
        for op in SUPPORTED_OPERATORS {
            params.push(format!("{}.{}", c, op));
        }
    }
    t.get.parameters = params;
    t.auto_generate = true;
    t
}

/// Metadata captured from the HTTP request / caller for an audited mutation.
#[derive(Debug, Clone, Default)]
pub struct AuditContext<'a> {
    pub action: &'a str,
    pub actor_id: &'a str,
    pub actor_custom: Option<&'a str>,
    pub reason: Option<&'a str>,
    pub request_id: Option<&'a str>,
}

/// One changed column between `old_record` and `new_record`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChange {
    pub column_name: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    /// Raw JSON representation of the new value (used for `require_reason_when`).
    pub new_raw: Value,
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Extract the trimmed `X-Change-Reason` header value, or `None` when absent/blank.
pub fn extract_change_reason(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get(CHANGE_REASON_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Extract the trimmed `x-request-id` header value (injected by `StatusLogger`).
pub fn extract_request_id(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Audit-relevant request headers (`X-Change-Reason`, `x-request-id`) to carry
/// on a queued `WriteJob` so the consumer replays them on its synthetic request.
pub fn forwarded_headers(req: &HttpRequest) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(reason) = extract_change_reason(req) {
        out.push((CHANGE_REASON_HEADER.to_string(), reason));
    }
    if let Some(req_id) = extract_request_id(req) {
        out.push(("x-request-id".to_string(), req_id));
    }
    out
}

/// True when `schema` has structured audit trail enabled.
pub fn is_enabled(schema: &TableSchema) -> bool {
    schema.audit_trail.as_ref().map(|a| a.enabled).unwrap_or(false)
}

/// Serialize a JSON column value into the text representation stored in
/// `old_value` / `new_value` (`None` → SQL `NULL`).
pub fn value_to_audit_text(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

/// Equality check for column diffing.
///
/// - `None` and `Some(Value::Null)` are equivalent.
/// - Strings are compared **case-sensitively** (changing `"abc"` to `"ABC"` is a
///   real change that must be audited).
/// - Numbers compare numerically (`1` == `1.0`, `"42"` == `42`).
/// - Booleans match `true`/`1` and `false`/`0` (SQLite/MySQL store booleans as 0/1).
pub fn values_equal(old: Option<&Value>, new: Option<&Value>) -> bool {
    match (old, new) {
        (None, None)
        | (None, Some(Value::Null))
        | (Some(Value::Null), None)
        | (Some(Value::Null), Some(Value::Null)) => true,
        (Some(a), Some(b)) => {
            if a == b {
                return true;
            }
            match (a, b) {
                (Value::String(s1), Value::String(s2)) => s1 == s2,
                (Value::Number(n1), Value::Number(n2)) => {
                    match (n1.as_i64(), n2.as_i64()) {
                        (Some(i1), Some(i2)) => i1 == i2,
                        _ => match (n1.as_f64(), n2.as_f64()) {
                            (Some(f1), Some(f2)) => (f1 - f2).abs() < 1e-12,
                            _ => n1.to_string() == n2.to_string(),
                        },
                    }
                }
                (Value::String(s), Value::Number(n)) | (Value::Number(n), Value::String(s)) => {
                    if let (Ok(i1), Some(i2)) = (s.trim().parse::<i64>(), n.as_i64()) {
                        i1 == i2
                    } else if let (Ok(f1), Some(f2)) = (s.trim().parse::<f64>(), n.as_f64()) {
                        (f1 - f2).abs() < 1e-12
                    } else {
                        false
                    }
                }
                (Value::Bool(b), Value::Number(n)) | (Value::Number(n), Value::Bool(b)) => {
                    n.as_i64().map(|i| (i != 0) == *b).unwrap_or(false)
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// Columns audited when `audit_trail.columns` is empty: every key of `record`
/// that is a real schema column (detail arrays / embedded objects excluded),
/// minus `ignored`, sorted for deterministic output.
fn auto_columns(schema: &TableSchema, record: &Map<String, Value>, ignored: &[&str]) -> Vec<String> {
    let mut keys: Vec<String> = record
        .iter()
        .filter(|(k, v)| {
            !ignored.iter().any(|ign| ign.eq_ignore_ascii_case(k))
                && !v.is_array()
                && !v.is_object()
                && (schema.columns.is_empty() || schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(k)))
        })
        .map(|(k, _)| k.clone())
        .collect();
    keys.sort();
    keys
}

/// Metadata columns never audited on create/delete when `columns` is empty.
const AUTO_IGNORED_CREATE_DELETE: &[&str] =
    &["created_at", "created_by_id", "updated_at", "updated_by_id", "deleted_at", "deleted_by_id"];

/// Compute the list of changed tracked columns between `old_record` and `new_record`.
pub fn compute_diff(
    schema: &TableSchema,
    old_record: &Map<String, Value>,
    new_record: &Map<String, Value>,
) -> Vec<ColumnChange> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Vec::new();
    };

    let candidate_cols: Vec<String> = if !cfg.columns.is_empty() {
        cfg.columns.clone()
    } else {
        auto_columns(schema, new_record, AUTO_IGNORED_COLUMNS)
    };

    let mut changes = Vec::new();
    for col in candidate_cols {
        // Only consider columns present in new_record (i.e. part of the record / patch).
        let Some(new_val) = new_record.get(&col) else {
            continue;
        };
        let old_val = old_record.get(&col);
        if values_equal(old_val, Some(new_val)) {
            continue;
        }
        changes.push(ColumnChange {
            column_name: col,
            old_value: value_to_audit_text(old_val),
            new_value: value_to_audit_text(Some(new_val)),
            new_raw: new_val.clone(),
        });
    }
    changes
}

fn rule_matches_value(rule: &Value, new_val: &Value) -> bool {
    match rule {
        Value::Bool(b) => *b,
        Value::String(s) if s.trim() == "*" => true,
        Value::Array(arr) => {
            if arr.is_empty() {
                true
            } else {
                arr.iter().any(|item| match item {
                    Value::String(s) if s.trim() == "*" => true,
                    other => value_matches(other, Some(new_val)),
                })
            }
        }
        other => value_matches(other, Some(new_val)),
    }
}

/// Enforce `audit_trail.require_reason_when` on the computed diff.
///
/// Returns `Err(message)` (mapped to HTTP `400 Bad Request`) when any changed
/// column matches a `require_reason_when` rule and `reason` is empty/absent.
pub fn check_require_reason(
    schema: &TableSchema,
    changes: &[ColumnChange],
    reason: Option<&str>,
) -> Result<(), String> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Ok(());
    };
    if cfg.require_reason_when.is_empty() {
        return Ok(());
    }
    let has_reason = reason.map(|s| !s.trim().is_empty()).unwrap_or(false);
    if has_reason {
        return Ok(());
    }

    for change in changes {
        let matched_rule = cfg
            .require_reason_when
            .get(&change.column_name)
            .or_else(|| {
                cfg.require_reason_when
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(&change.column_name))
                    .map(|(_, v)| v)
            });

        if let Some(rule) = matched_rule
            && rule_matches_value(rule, &change.new_raw)
        {
            return Err(format!(
                "{} via 'X-Change-Reason' header when changing '{}' to '{}'",
                REASON_REQUIRED_PREFIX,
                change.column_name,
                change.new_value.as_deref().unwrap_or("null")
            ));
        }
    }

    Ok(())
}

/// Also check `require_reason_when` for columns changed in `patch_fields` even
/// if `audit_trail.columns` didn't explicitly list that column.
pub fn check_require_reason_on_patch(
    schema: &TableSchema,
    old_record: &Map<String, Value>,
    patch_fields: &Map<String, Value>,
    reason: Option<&str>,
) -> Result<(), String> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Ok(());
    };
    if cfg.require_reason_when.is_empty() {
        return Ok(());
    }
    if reason.map(|s| !s.trim().is_empty()).unwrap_or(false) {
        return Ok(());
    }

    for (col, rule) in &cfg.require_reason_when {
        let Some(new_val) = patch_fields.get(col) else {
            continue;
        };
        let old_val = old_record.get(col);
        if values_equal(old_val, Some(new_val)) {
            continue;
        }
        if rule_matches_value(rule, new_val) {
            let new_str = value_to_audit_text(Some(new_val)).unwrap_or_else(|| "null".to_string());
            return Err(format!(
                "{} via 'X-Change-Reason' header when changing '{}' to '{}'",
                REASON_REQUIRED_PREFIX, col, new_str
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_row_in_tx(
    tx: &mut (dyn TxStore + '_),
    entity: &str,
    table_name: &str,
    record_id: &str,
    column_name: &str,
    old_value: Option<&str>,
    new_value: Option<&str>,
    ctx: &AuditContext<'_>,
    at: &str,
) -> anyhow::Result<()> {
    let doc = json!({
        "entity": entity,
        "table_name": table_name,
        "record_id": record_id,
        "action": ctx.action,
        "column_name": column_name,
        "old_value": old_value,
        "new_value": new_value,
        "actor_id": ctx.actor_id,
        "actor_custom": ctx.actor_custom,
        "reason": ctx.reason,
        "request_id": ctx.request_id,
        "at": at,
    });
    tx.insert(TABLE, doc).await?;
    Ok(())
}

/// Insert one `flx_audit_trail` row per changed column inside `tx`.
pub async fn record_update_in_tx(
    tx: &mut (dyn TxStore + '_),
    schema: &TableSchema,
    route: &str,
    record_id: &str,
    changes: &[ColumnChange],
    ctx: &AuditContext<'_>,
) -> anyhow::Result<usize> {
    if !is_enabled(schema) || changes.is_empty() {
        return Ok(0);
    }
    let at = now_iso();
    for ch in changes {
        insert_row_in_tx(
            tx,
            route,
            &schema.table,
            record_id,
            &ch.column_name,
            ch.old_value.as_deref(),
            ch.new_value.as_deref(),
            ctx,
            &at,
        )
        .await?;
    }
    Ok(changes.len())
}

/// Insert `flx_audit_trail` rows for a newly created record (`POST`) inside `tx`.
pub async fn record_create_in_tx(
    tx: &mut (dyn TxStore + '_),
    schema: &TableSchema,
    route: &str,
    record_id: &str,
    new_record: &Map<String, Value>,
    ctx: &AuditContext<'_>,
) -> anyhow::Result<usize> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Ok(0);
    };
    let cols: Vec<String> = if !cfg.columns.is_empty() {
        cfg.columns.clone()
    } else {
        auto_columns(schema, new_record, AUTO_IGNORED_CREATE_DELETE)
    };

    let at = now_iso();
    let mut written = 0usize;
    for col in cols {
        let Some(new_val) = new_record.get(&col) else {
            continue;
        };
        let Some(new_str) = value_to_audit_text(Some(new_val)) else {
            continue;
        };
        insert_row_in_tx(
            tx,
            route,
            &schema.table,
            record_id,
            &col,
            None,
            Some(&new_str),
            ctx,
            &at,
        )
        .await?;
        written += 1;
    }
    Ok(written)
}

/// Insert `flx_audit_trail` rows for a deleted record (`DELETE`) inside `tx`.
pub async fn record_delete_in_tx(
    tx: &mut (dyn TxStore + '_),
    schema: &TableSchema,
    route: &str,
    record_id: &str,
    old_record: &Map<String, Value>,
    ctx: &AuditContext<'_>,
) -> anyhow::Result<usize> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Ok(0);
    };
    let cols: Vec<String> = if !cfg.columns.is_empty() {
        cfg.columns.clone()
    } else {
        auto_columns(schema, old_record, AUTO_IGNORED_CREATE_DELETE)
    };

    let at = now_iso();
    let mut written = 0usize;
    for col in cols {
        let Some(old_val) = old_record.get(&col) else {
            continue;
        };
        let Some(old_str) = value_to_audit_text(Some(old_val)) else {
            continue;
        };
        insert_row_in_tx(
            tx,
            route,
            &schema.table,
            record_id,
            &col,
            Some(&old_str),
            None,
            ctx,
            &at,
        )
        .await?;
        written += 1;
    }
    if written == 0 {
        insert_row_in_tx(
            tx,
            route,
            &schema.table,
            record_id,
            "*",
            None,
            None,
            ctx,
            &at,
        )
        .await?;
        written = 1;
    }
    Ok(written)
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot-based auditing for indirect writes (details[], action_triggers,
// FK cascades, bulk import). These paths write with raw SQL against a *table*
// name, so we resolve the audited entity from the registry and diff
// before/after row snapshots taken inside the same transaction.
// ─────────────────────────────────────────────────────────────────────────────

/// An audited entity resolved from a physical table name.
#[derive(Debug, Clone)]
pub struct AuditTarget {
    /// Registry route name (stored in `flx_audit_trail.entity`).
    pub entity: String,
    pub schema: Arc<TableSchema>,
    pub pk_cols: Vec<String>,
}

/// Action labels used when writing snapshot-diff rows.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotActions<'a> {
    pub create: &'a str,
    pub update: &'a str,
    pub delete: &'a str,
}

pub const DETAIL_ACTIONS: SnapshotActions<'static> =
    SnapshotActions { create: "DETAIL_CREATE", update: "DETAIL_UPDATE", delete: "DETAIL_DELETE" };
pub const TRIGGER_ACTIONS: SnapshotActions<'static> =
    SnapshotActions { create: "TRIGGER_CREATE", update: "TRIGGER_UPDATE", delete: "TRIGGER_DELETE" };
pub const CASCADE_ACTIONS: SnapshotActions<'static> =
    SnapshotActions { create: "CASCADE_CREATE", update: "CASCADE_UPDATE", delete: "CASCADE_DELETE" };

/// Resolve the audited entity for a physical table (or route) name. Returns
/// `None` when no registered entity maps to it or its audit trail is disabled.
pub fn audit_target_for_table(table: &str) -> Option<AuditTarget> {
    let table = table.trim();
    if table.is_empty() || table.eq_ignore_ascii_case(TABLE) {
        return None;
    }
    let reg = crate::registry::current();
    let (route, schema) = reg
        .schemas
        .get(table)
        .map(|s| (table.to_string(), s.clone()))
        .or_else(|| {
            reg.schemas
                .iter()
                .filter(|(_, s)| s.table.eq_ignore_ascii_case(table))
                .min_by(|a, b| a.0.cmp(b.0))
                .map(|(r, s)| (r.clone(), s.clone()))
        })?;
    if !is_enabled(&schema) {
        return None;
    }
    let pk_cols = if schema.primary_key.columns.is_empty() {
        vec!["id".to_string()]
    } else {
        schema.primary_key.columns.clone()
    };
    Some(AuditTarget { entity: route, schema, pk_cols })
}

fn json_to_qv(v: &Value) -> QV {
    match v {
        Value::Null => QV::Null,
        Value::Bool(b) => QV::Bool(*b),
        Value::Number(n) => n
            .as_i64()
            .map(QV::I64)
            .or_else(|| n.as_f64().map(QV::F64))
            .unwrap_or_else(|| QV::Str(n.to_string())),
        Value::String(s) => QV::Str(s.clone()),
        other => QV::Str(other.to_string()),
    }
}

/// JSON value for a key given as string, typed by the column's `type_data`
/// (integer-like columns become numbers so PostgreSQL/MSSQL bind correctly).
pub fn typed_key_value(raw: &str, type_data: &str) -> Value {
    let t = type_data.to_ascii_lowercase();
    if (t.contains("int") || t.contains("serial"))
        && let Ok(n) = raw.trim().parse::<i64>()
    {
        return Value::from(n);
    }
    Value::String(raw.to_string())
}

fn pk_part(v: &Value) -> String {
    match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Primary-key string of a row (`~`-joined for composite keys, matching the
/// `/{route}/{a~b}` path format). `None` when any PK part is missing.
pub fn row_pk(row: &Map<String, Value>, pk_cols: &[String]) -> Option<String> {
    let mut parts = Vec::with_capacity(pk_cols.len());
    for c in pk_cols {
        let p = row.get(c).map(pk_part).unwrap_or_default();
        if p.is_empty() {
            return None;
        }
        parts.push(p);
    }
    Some(parts.join("~"))
}

fn is_soft_deleted(row: &Map<String, Value>) -> bool {
    matches!(row.get("deleted_at"), Some(v) if !v.is_null())
}

/// `SELECT *` rows of `target` matching `filter` inside `tx`.
pub async fn select_rows(
    tx: &mut (dyn TxStore + '_),
    target: &AuditTarget,
    filter: QF,
) -> anyhow::Result<Vec<Value>> {
    let q = crate::storage::ast::Query::from(target.schema.table.clone()).r#where(filter);
    tx.query(&q).await
}

/// `SELECT *` rows of `target` with column `col` equal to `val` inside `tx`.
pub async fn select_rows_where_eq(
    tx: &mut (dyn TxStore + '_),
    target: &AuditTarget,
    col: &str,
    val: &Value,
) -> anyhow::Result<Vec<Value>> {
    select_rows(tx, target, QF::Eq(col.to_string(), json_to_qv(val))).await
}

/// Re-read rows of `target` by the primary keys found in `rows` (chunked).
pub async fn reselect_by_pks(
    tx: &mut (dyn TxStore + '_),
    target: &AuditTarget,
    rows: &[Value],
) -> anyhow::Result<Vec<Value>> {
    let mut out = Vec::new();
    let keyed: Vec<&Map<String, Value>> = rows
        .iter()
        .filter_map(|r| r.as_object())
        .filter(|m| row_pk(m, &target.pk_cols).is_some())
        .collect();
    for chunk in keyed.chunks(200) {
        let filter = if target.pk_cols.len() == 1 {
            let c = &target.pk_cols[0];
            QF::In(c.clone(), chunk.iter().map(|m| json_to_qv(&m[c])).collect())
        } else {
            QF::Or(
                chunk
                    .iter()
                    .map(|m| {
                        QF::And(
                            target
                                .pk_cols
                                .iter()
                                .map(|c| QF::Eq(c.clone(), json_to_qv(&m[c])))
                                .collect(),
                        )
                    })
                    .collect(),
            )
        };
        out.extend(select_rows(tx, target, filter).await?);
    }
    Ok(out)
}

/// Diff two row snapshots of `target` by primary key and write audit rows:
/// - only in `after`               → `actions.create`
/// - only in `before` / soft-deleted → `actions.delete`
/// - in both with changed columns  → `actions.update`
///
/// `ctx.action` is ignored (taken from `actions`). When `enforce_reason` is
/// true, `require_reason_when` of the target is checked on updates
/// (`Err` carries the client-facing message for a 400 response).
pub async fn record_snapshot_diff_in_tx(
    tx: &mut (dyn TxStore + '_),
    target: &AuditTarget,
    before: &[Value],
    after: &[Value],
    actions: SnapshotActions<'_>,
    ctx: &AuditContext<'_>,
    enforce_reason: bool,
) -> Result<usize, String> {
    let index = |rows: &[Value]| -> Vec<(String, Map<String, Value>)> {
        rows.iter()
            .filter_map(|r| r.as_object())
            .filter_map(|m| row_pk(m, &target.pk_cols).map(|k| (k, m.clone())))
            .collect()
    };
    let before_rows = index(before);
    let after_rows = index(after);
    let before_map: HashMap<&str, &Map<String, Value>> =
        before_rows.iter().map(|(k, m)| (k.as_str(), m)).collect();
    let after_map: HashMap<&str, &Map<String, Value>> =
        after_rows.iter().map(|(k, m)| (k.as_str(), m)).collect();

    let schema = &*target.schema;
    let mut written = 0usize;
    let err = |e: anyhow::Error| format!("Error recording audit trail: {}", e);

    for (pk, new_row) in &after_rows {
        match before_map.get(pk.as_str()) {
            Some(old_row) if !is_soft_deleted(old_row) && is_soft_deleted(new_row) => {
                let c = AuditContext { action: actions.delete, ..ctx.clone() };
                written += record_delete_in_tx(tx, schema, &target.entity, pk, old_row, &c)
                    .await
                    .map_err(err)?;
            }
            Some(old_row) => {
                let changes = compute_diff(schema, old_row, new_row);
                if enforce_reason {
                    check_require_reason(schema, &changes, ctx.reason)?;
                }
                let c = AuditContext { action: actions.update, ..ctx.clone() };
                written += record_update_in_tx(tx, schema, &target.entity, pk, &changes, &c)
                    .await
                    .map_err(err)?;
            }
            None if !is_soft_deleted(new_row) => {
                let c = AuditContext { action: actions.create, ..ctx.clone() };
                written += record_create_in_tx(tx, schema, &target.entity, pk, new_row, &c)
                    .await
                    .map_err(err)?;
            }
            None => {}
        }
    }
    for (pk, old_row) in &before_rows {
        if !after_map.contains_key(pk.as_str()) && !is_soft_deleted(old_row) {
            let c = AuditContext { action: actions.delete, ..ctx.clone() };
            written += record_delete_in_tx(tx, schema, &target.entity, pk, old_row, &c)
                .await
                .map_err(err)?;
        }
    }
    Ok(written)
}

/// Synchronous `require_reason_when` check for a PUT/PATCH that is about to be
/// queued (`?isqueue=true`): reads the current row and returns the 400 message
/// when the patch needs a reason that was not sent. Missing rows pass (the
/// consumer reports 404); lookup errors pass (the consumer re-checks).
pub async fn precheck_reason_for_queue(
    state: &AppState,
    schema: &TableSchema,
    id_raw: &str,
    body: &Value,
    reason: Option<&str>,
) -> Result<(), String> {
    let Some(cfg) = schema.audit_trail.as_ref().filter(|c| c.enabled) else {
        return Ok(());
    };
    if cfg.require_reason_when.is_empty() || reason.map(|r| !r.trim().is_empty()).unwrap_or(false) {
        return Ok(());
    }
    let Some(patch) = body.as_object() else { return Ok(()) };
    let pk_cols = if schema.primary_key.columns.is_empty() {
        vec!["id".to_string()]
    } else {
        schema.primary_key.columns.clone()
    };
    let parts = crate::nocode::pk_utils::parse_pk_values(id_raw);
    if parts.len() != pk_cols.len() {
        return Ok(());
    }
    let filters: Vec<QF> = pk_cols
        .iter()
        .zip(parts.iter())
        .map(|(c, v)| {
            let type_data = schema
                .columns
                .iter()
                .find(|col| col.name == *c)
                .map(|col| col.type_data.as_str())
                .unwrap_or("");
            QF::Eq(c.clone(), json_to_qv(&typed_key_value(v, type_data)))
        })
        .collect();
    let q = crate::storage::ast::Query::from(schema.table.clone())
        .r#where(QF::And(filters))
        .limit(1);
    let Ok(rows) = state.store.query(&q).await else { return Ok(()) };
    let Some(old) = rows.first().and_then(|r| r.as_object()) else { return Ok(()) };
    check_require_reason_on_patch(schema, old, patch, None)
}

/// Before-images of child rows that an FK `on_delete` cascade (`cascade` /
/// `set null`) of `route`'s record `id_raw` is about to modify, for every
/// audited child entity. Pass the result to [`record_fk_cascade_in_tx`].
pub async fn snapshot_fk_cascade(
    tx: &mut (dyn TxStore + '_),
    route: &str,
    ref_fks: &[crate::model::ReferenceForeignKey],
    id_raw: &str,
) -> Result<Vec<(AuditTarget, Vec<Value>)>, String> {
    let mut out = Vec::new();
    for fk in ref_fks.iter().filter(|fk| fk.table == route) {
        let act = &fk.on_delete_action;
        let touches = act.action == "cascade" && matches!(act.type_delete.as_str(), "soft" | "hard")
            || act.action == "set null";
        if !touches || act.column.is_empty() {
            continue;
        }
        let Some(t) = audit_target_for_table(&act.table) else { continue };
        // Same literal typing as foreign_key.rs (integer when it parses).
        let key = typed_key_value(id_raw, "int");
        let rows = select_rows_where_eq(tx, &t, &act.column, &key)
            .await
            .map_err(|e| format!("Error recording audit trail: {}", e))?;
        if !rows.is_empty() {
            out.push((t, rows));
        }
    }
    Ok(out)
}

/// Write `CASCADE_*` rows for the snapshots taken by [`snapshot_fk_cascade`]
/// after the cascade ran (rows are re-read by primary key).
pub async fn record_fk_cascade_in_tx(
    tx: &mut (dyn TxStore + '_),
    snapshots: &[(AuditTarget, Vec<Value>)],
    ctx: &AuditContext<'_>,
) -> Result<(), String> {
    for (t, before) in snapshots {
        let after = reselect_by_pks(tx, t, before)
            .await
            .map_err(|e| format!("Error recording audit trail: {}", e))?;
        record_snapshot_diff_in_tx(tx, t, before, &after, CASCADE_ACTIONS, ctx, false).await?;
    }
    Ok(())
}

/// Return sorted route names whose `audit_trail.readers` includes at least one
/// of the caller's JWT roles (comparing the base role name before `/`).
pub fn readable_entities_for_roles(
    schemas: &HashMap<String, Arc<TableSchema>>,
    user_roles: &[String],
) -> Vec<String> {
    let caller_roles: Vec<&str> = user_roles
        .iter()
        .map(|r| r.split('/').next().unwrap_or("").trim())
        .filter(|r| !r.is_empty())
        .collect();
    if caller_roles.is_empty() {
        return Vec::new();
    }

    let mut allowed: Vec<String> = schemas
        .iter()
        .filter_map(|(route, schema)| {
            let cfg = schema.audit_trail.as_ref()?;
            let ok = cfg.readers.iter().any(|reader| {
                let r = reader.trim();
                !r.is_empty()
                    && (r == "*"
                        || caller_roles
                            .iter()
                            .any(|cr| cr.eq_ignore_ascii_case(r)))
            });
            if ok { Some(route.clone()) } else { None }
        })
        .collect();
    allowed.sort();
    allowed
}

/// Coerce non-`id` filter literals (`I64`/`F64`/`Bool`) to `QV::Str` so queries
/// like `GET /audit_trail?record_id.eq=42` or `actor_id.eq=1` bind as strings
/// against `varchar` columns on PostgreSQL, MySQL, SQLite, and MSSQL.
fn is_id_column(col: &str) -> bool {
    let bare = col.rsplit('.').next().unwrap_or(col).trim();
    bare.eq_ignore_ascii_case("id")
}

fn qv_to_str(v: QV) -> QV {
    match v {
        QV::I64(n) => QV::Str(n.to_string()),
        QV::F64(f) => QV::Str(f.to_string()),
        QV::Bool(b) => QV::Str(b.to_string()),
        other => other,
    }
}

pub fn coerce_text_filters(f: QF) -> QF {
    match f {
        QF::And(fs) => QF::And(fs.into_iter().map(coerce_text_filters).collect()),
        QF::Or(fs) => QF::Or(fs.into_iter().map(coerce_text_filters).collect()),
        QF::Eq(c, v) if !is_id_column(&c) => QF::Eq(c, qv_to_str(v)),
        QF::Ne(c, v) if !is_id_column(&c) => QF::Ne(c, qv_to_str(v)),
        QF::Lt(c, v) if !is_id_column(&c) => QF::Lt(c, qv_to_str(v)),
        QF::Lte(c, v) if !is_id_column(&c) => QF::Lte(c, qv_to_str(v)),
        QF::Gt(c, v) if !is_id_column(&c) => QF::Gt(c, qv_to_str(v)),
        QF::Gte(c, v) if !is_id_column(&c) => QF::Gte(c, qv_to_str(v)),
        QF::In(c, vs) if !is_id_column(&c) => QF::In(c, vs.into_iter().map(qv_to_str).collect()),
        QF::NotIn(c, vs) if !is_id_column(&c) => {
            QF::NotIn(c, vs.into_iter().map(qv_to_str).collect())
        }
        QF::Between(c, lo, hi) if !is_id_column(&c) => {
            QF::Between(c, qv_to_str(lo), qv_to_str(hi))
        }
        other => other,
    }
}

/// Query `flx_audit_trail` with standard GET filters, sorting, pagination,
/// optional `count=true`, cursor (`after=`), and optional entity-scope
/// restriction (for non-admin `audit_trail.readers`).
pub async fn query_audit_trail(
    state: &AppState,
    params_map: &Map<String, Value>,
    allowed_entities: Option<&[String]>,
) -> Result<(Vec<Value>, usize, Option<String>), String> {
    let schema = table_schema();
    let mut plan = data_read_repo::build_read_query(&schema, params_map, state.db_type.as_str())?;

    if let Some(f) = plan.query.filter.take() {
        plan.query.filter = Some(coerce_text_filters(f));
    }
    if let Some(ref mut cq) = plan.count_query
        && let Some(f) = cq.filter.take()
    {
        cq.filter = Some(coerce_text_filters(f));
    }

    if let Some(entities) = allowed_entities {
        let scope_filter = QF::In(
            format!("{}.entity", TABLE),
            entities.iter().cloned().map(QV::Str).collect(),
        );
        plan.query.filter = Some(match plan.query.filter.take() {
            Some(QF::And(mut fs)) => {
                fs.push(scope_filter.clone());
                QF::And(fs)
            }
            Some(other) => QF::And(vec![other, scope_filter.clone()]),
            None => scope_filter.clone(),
        });
        if let Some(ref mut cq) = plan.count_query {
            cq.filter = Some(match cq.filter.take() {
                Some(QF::And(mut fs)) => {
                    fs.push(scope_filter.clone());
                    QF::And(fs)
                }
                Some(other) => QF::And(vec![other, scope_filter]),
                None => scope_filter,
            });
        }
    }

    let rows = state
        .store
        .query(&plan.query)
        .await
        .map_err(|e| format!("Error querying {}: {}", TABLE, e))?;

    let next_cursor = if rows.len() >= plan.limit as usize {
        rows.last()
            .and_then(|r| data_read_repo::next_cursor_from_row(r, &plan.sort_keys))
    } else {
        None
    };

    let total = match plan.count_query {
        Some(cq) => match state.store.query(&cq).await {
            Ok(count_rows) => count_rows
                .first()
                .and_then(|v| v.as_object())
                .and_then(|m| {
                    m.get("cnt")
                        .or_else(|| m.values().next())
                        .and_then(|val| {
                            val.as_i64()
                                .or_else(|| val.as_str().and_then(|s| s.parse::<i64>().ok()))
                        })
                })
                .map(|n| n.max(0) as usize)
                .unwrap_or(rows.len()),
            Err(_) => rows.len(),
        },
        None => rows.len(),
    };

    Ok((rows, total, next_cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AuditTrailConfig;
    use std::collections::BTreeMap;

    fn schema_with_audit(
        columns: &[&str],
        require_reason_when: BTreeMap<String, Value>,
        readers: &[&str],
    ) -> TableSchema {
        let mut s = TableSchema {
            table: "capa".into(),
            ..Default::default()
        };
        s.audit_trail = Some(AuditTrailConfig {
            enabled: true,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            require_reason_when,
            readers: readers.iter().map(|r| r.to_string()).collect(),
        });
        s
    }

    #[test]
    fn table_schema_has_all_13_required_columns() {
        let s = table_schema();
        assert_eq!(s.table, "flx_audit_trail");
        let names: Vec<&str> = s.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "id",
                "entity",
                "table_name",
                "record_id",
                "action",
                "column_name",
                "old_value",
                "new_value",
                "actor_id",
                "actor_custom",
                "reason",
                "request_id",
                "at",
            ]
        );
        assert!(s.get.parameters.contains(&"record_id.eq".to_string()));
        assert!(s.get.parameters.contains(&"entity.eq".to_string()));
        assert!(s.get.parameters.contains(&"at.gte".to_string()));
    }

    #[test]
    fn compute_diff_tracks_only_changed_configured_columns() {
        let s = schema_with_audit(&["status", "problem", "evaluation"], BTreeMap::new(), &[]);
        let old: Map<String, Value> = serde_json::from_value(json!({
            "id": 1,
            "status": "OPEN",
            "problem": "Old problem",
            "evaluation": null,
            "notes": "ignored"
        }))
        .unwrap();
        let new: Map<String, Value> = serde_json::from_value(json!({
            "id": 1,
            "status": "IMPLEMENTATION",
            "problem": "Old problem",
            "evaluation": "Root cause identified",
            "notes": "changed but untracked"
        }))
        .unwrap();

        let changes = compute_diff(&s, &old, &new);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].column_name, "status");
        assert_eq!(changes[0].old_value.as_deref(), Some("OPEN"));
        assert_eq!(changes[0].new_value.as_deref(), Some("IMPLEMENTATION"));
        assert_eq!(changes[1].column_name, "evaluation");
        assert_eq!(changes[1].old_value, None);
        assert_eq!(
            changes[1].new_value.as_deref(),
            Some("Root cause identified")
        );
    }

    #[test]
    fn compute_diff_is_case_sensitive_for_strings_and_ignores_numeric_format() {
        assert!(!values_equal(Some(&json!("open")), Some(&json!("OPEN"))));
        assert!(values_equal(Some(&json!(42)), Some(&json!("42"))));
        assert!(values_equal(Some(&json!(1)), Some(&json!(true))));
        assert!(values_equal(None, Some(&Value::Null)));
    }

    #[test]
    fn require_reason_when_enforces_header_only_on_matching_change() {
        let mut rules = BTreeMap::new();
        rules.insert("status".into(), json!(["CLOSED", "IMPLEMENTATION"]));
        let s = schema_with_audit(&["status", "problem"], rules, &[]);

        let old: Map<String, Value> =
            serde_json::from_value(json!({ "status": "OPEN", "problem": "p1" })).unwrap();

        // Transition to IMPLEMENTATION without reason -> Err
        let new_impl: Map<String, Value> =
            serde_json::from_value(json!({ "status": "IMPLEMENTATION", "problem": "p1" })).unwrap();
        let diff_impl = compute_diff(&s, &old, &new_impl);
        assert!(check_require_reason(&s, &diff_impl, None).is_err());
        assert!(check_require_reason(&s, &diff_impl, Some("   ")).is_err());
        assert!(check_require_reason(&s, &diff_impl, Some("QA approved")).is_ok());

        // Transition to IN_REVIEW (not in require_reason_when) without reason -> Ok
        let new_rev: Map<String, Value> =
            serde_json::from_value(json!({ "status": "IN_REVIEW", "problem": "p1" })).unwrap();
        let diff_rev = compute_diff(&s, &old, &new_rev);
        assert!(check_require_reason(&s, &diff_rev, None).is_ok());

        // Status already CLOSED, only problem changes -> Ok without reason
        let old_closed: Map<String, Value> =
            serde_json::from_value(json!({ "status": "CLOSED", "problem": "p1" })).unwrap();
        let new_closed: Map<String, Value> =
            serde_json::from_value(json!({ "status": "CLOSED", "problem": "p2" })).unwrap();
        let diff_closed = compute_diff(&s, &old_closed, &new_closed);
        assert_eq!(diff_closed.len(), 1);
        assert_eq!(diff_closed[0].column_name, "problem");
        assert!(check_require_reason(&s, &diff_closed, None).is_ok());
    }

    #[test]
    fn readable_entities_matches_role_prefix_case_insensitively() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "capa".into(),
            Arc::new(schema_with_audit(&["status"], BTreeMap::new(), &["qa_reviewer", "auditor"])),
        );
        schemas.insert(
            "deviations".into(),
            Arc::new(schema_with_audit(&["status"], BTreeMap::new(), &["auditor"])),
        );

        assert_eq!(
            readable_entities_for_roles(&schemas, &["QA_Reviewer/127".into()]),
            vec!["capa"]
        );
        assert_eq!(
            readable_entities_for_roles(&schemas, &["auditor".into()]),
            vec!["capa", "deviations"]
        );
        assert!(readable_entities_for_roles(&schemas, &["operator/7".into()]).is_empty());
    }

    #[test]
    fn auto_columns_skip_detail_arrays_and_non_schema_keys() {
        let mut s = schema_with_audit(&[], BTreeMap::new(), &[]);
        s.columns = ["id", "status", "updated_at"]
            .iter()
            .map(|n| Column { name: n.to_string(), ..Default::default() })
            .collect();
        let old: Map<String, Value> = serde_json::from_value(json!({ "id": 1, "status": "OPEN" })).unwrap();
        let new: Map<String, Value> = serde_json::from_value(json!({
            "id": 1, "status": "CLOSED", "updated_at": "now",
            "steps": [{ "id": 9 }], "__actor_id__": "7"
        }))
        .unwrap();
        let changes = compute_diff(&s, &old, &new);
        assert_eq!(changes.len(), 1, "{:?}", changes);
        assert_eq!(changes[0].column_name, "status");
    }

    #[test]
    fn row_pk_and_typed_key_value() {
        let row: Map<String, Value> = serde_json::from_value(json!({ "a": 1, "b": "X" })).unwrap();
        assert_eq!(row_pk(&row, &["a".into()]).as_deref(), Some("1"));
        assert_eq!(row_pk(&row, &["a".into(), "b".into()]).as_deref(), Some("1~X"));
        assert_eq!(row_pk(&row, &["missing".into()]), None);
        assert_eq!(typed_key_value("42", "int"), json!(42));
        assert_eq!(typed_key_value("SO/001", "int"), json!("SO/001"));
        assert_eq!(typed_key_value("42", "varchar(20)"), json!("42"));
    }

    #[test]
    fn reason_error_uses_shared_prefix() {
        let mut rules = BTreeMap::new();
        rules.insert("status".into(), json!("*"));
        let s = schema_with_audit(&["status"], rules, &[]);
        let old: Map<String, Value> = serde_json::from_value(json!({ "status": "A" })).unwrap();
        let new: Map<String, Value> = serde_json::from_value(json!({ "status": "B" })).unwrap();
        let err = check_require_reason(&s, &compute_diff(&s, &old, &new), None).unwrap_err();
        assert!(err.starts_with(REASON_REQUIRED_PREFIX), "{}", err);
        let err2 = check_require_reason_on_patch(&s, &old, &new, None).unwrap_err();
        assert!(err2.starts_with(REASON_REQUIRED_PREFIX), "{}", err2);
    }

    #[test]
    fn coerce_text_filters_converts_numeric_record_id_but_keeps_pk_id_numeric() {
        let f = QF::And(vec![
            QF::Eq("flx_audit_trail.id".into(), QV::I64(5)),
            QF::Eq("flx_audit_trail.record_id".into(), QV::I64(42)),
            QF::In("flx_audit_trail.actor_id".into(), vec![QV::I64(1), QV::I64(2)]),
        ]);
        let coerced = coerce_text_filters(f);
        match coerced {
            QF::And(fs) => {
                assert!(matches!(&fs[0], QF::Eq(c, QV::I64(5)) if c == "flx_audit_trail.id"));
                assert!(matches!(&fs[1], QF::Eq(c, QV::Str(s)) if c == "flx_audit_trail.record_id" && s == "42"));
                assert!(matches!(
                    &fs[2],
                    QF::In(c, vs)
                        if c == "flx_audit_trail.actor_id"
                            && vs.len() == 2
                            && matches!(&vs[0], QV::Str(a) if a == "1")
                            && matches!(&vs[1], QV::Str(b) if b == "2")
                ));
            }
            _ => panic!("expected And"),
        }
    }
}
