//! Transactional outbox for webhooks.
//!
//! A write that matches an entity's `webhooks[]` inserts one row into
//! `flx_outbox` **inside the same database transaction** as the business
//! change, so the event exists if and only if the change was committed. A
//! background worker then delivers pending rows over HTTP with exponential
//! backoff. The request that caused the change never waits for a third party.
//!
//! Claiming is done with a conditional `UPDATE … WHERE status = 'pending'`
//! (rows affected == 1), which is correct on every supported dialect and with
//! several API replicas polling the same table.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::database::state::AppState;
use crate::log::log_output;
use crate::metrics::METRICS;
use crate::model::{Column, PrimaryKey, TableSchema, Webhook};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
use crate::storage::traits::TxStore;

pub const TABLE: &str = "flx_outbox";

/// Schema of the outbox table (string timestamps keep it dialect-neutral; ISO-8601
/// UTC strings compare correctly as text).
pub fn table_schema() -> TableSchema {
    let col = |name: &str, type_data: &str, nullable: bool| Column {
        name: name.into(),
        type_data: type_data.into(),
        nullable,
        ..Default::default()
    };
    let mut t = TableSchema { table: TABLE.into(), ..Default::default() };
    t.primary_key = PrimaryKey { columns: vec!["id".into()] };
    t.columns = vec![
        Column { name: "id".into(), type_data: "bigint".into(), auto_increment: true, nullable: false, ..Default::default() },
        col("kind", "varchar(30)", false),
        col("entity", "varchar(120)", false),
        col("event", "varchar(30)", false),
        col("target", "varchar(2048)", false),
        col("headers", "text", true),
        col("payload", "text", false),
        col("status", "varchar(20)", false),
        col("attempts", "int", false),
        col("next_attempt_at", "varchar(40)", false),
        col("last_error", "text", true),
        col("sent_at", "varchar(40)", true),
    ];
    t.indexes = vec![crate::model::Index {
        name: "idx_outbox_status_next".into(),
        columns: vec!["status".into(), "next_attempt_at".into()],
        unique: false,
    }];
    t.auto_generate = true;
    t
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Webhooks of `schema` that fire for `event` (`on_create` / `on_update` / `on_delete`).
pub fn matching<'a>(schema: &'a TableSchema, event: &str) -> Vec<&'a Webhook> {
    schema
        .webhooks
        .iter()
        .filter(|w| !w.url.trim().is_empty() && (w.event == "*" || w.event.eq_ignore_ascii_case(event)))
        .collect()
}

/// Insert one outbox row per matching webhook, inside `tx`.
#[allow(clippy::too_many_arguments)]
pub async fn enqueue_in_tx(
    tx: &mut (dyn TxStore + '_),
    schema: &TableSchema,
    route: &str,
    event: &str,
    id: &str,
    actor_id: Option<&str>,
    record: &serde_json::Map<String, Value>,
    old_record: Option<&serde_json::Map<String, Value>>,
) -> anyhow::Result<usize> {
    let hooks = matching(schema, event);
    if hooks.is_empty() {
        return Ok(0);
    }
    let payload = json!({
        "event": event,
        "entity": route,
        "table": schema.table,
        "id": id,
        "actor_id": actor_id,
        "at": now_iso(),
        "record": Value::Object(record.clone()),
        "old_record": old_record.map(|m| Value::Object(m.clone())),
    })
    .to_string();
    let mut n = 0;
    for hook in hooks {
        let doc = json!({
            "kind": "webhook",
            "entity": route,
            "event": event,
            "target": hook.url,
            "headers": serde_json::to_string(&hook.headers).unwrap_or_else(|_| "{}".into()),
            "payload": payload,
            "status": "pending",
            "attempts": 0,
            "next_attempt_at": now_iso(),
        });
        tx.insert(TABLE, doc).await?;
        n += 1;
    }
    Ok(n)
}

fn backoff_secs(attempt: i64) -> i64 {
    (2i64.pow(attempt.clamp(0, 12) as u32) * 5).min(3600)
}

async fn deliver(client: &reqwest::Client, target: &str, headers: &str, payload: &str) -> Result<(), String> {
    let mut req = client.post(target).header("Content-Type", "application/json").header("User-Agent", "flexurio-webhook");
    if let Ok(Value::Object(h)) = serde_json::from_str::<Value>(headers) {
        for (k, v) in h {
            if let Some(vs) = v.as_str() {
                req = req.header(k, vs);
            }
        }
    }
    let res = req.body(payload.to_string()).send().await.map_err(|e| e.to_string())?;
    if res.status().is_success() {
        Ok(())
    } else {
        Err(format!("HTTP {}", res.status()))
    }
}

/// Deliver one claimed row and record the result.
async fn process_row(state: &AppState, client: &reqwest::Client, row: &Value, max_attempts: i64) {
    let id = row.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
    let attempts = row.get("attempts").and_then(|v| v.as_i64()).unwrap_or(0);
    let target = row.get("target").and_then(|v| v.as_str()).unwrap_or("");
    let headers = row.get("headers").and_then(|v| v.as_str()).unwrap_or("{}");
    let payload = row.get("payload").and_then(|v| v.as_str()).unwrap_or("{}");

    // Claim: only one worker/replica wins the conditional update.
    let claimed = match state.store.begin_tx().await {
        Ok(mut tx) => {
            let n = tx
                .update(
                    TABLE,
                    Some(QF::And(vec![QF::Eq("id".into(), QV::I64(id)), QF::Eq("status".into(), QV::Str("pending".into()))])),
                    json!({ "status": "sending", "attempts": attempts + 1 }),
                )
                .await
                .unwrap_or(0);
            let _ = tx.commit().await;
            n == 1
        }
        Err(_) => false,
    };
    if !claimed {
        return;
    }

    let result = deliver(client, target, headers, payload).await;
    let patch = match &result {
        Ok(()) => {
            METRICS.record_outbox_sent();
            json!({ "status": "sent", "sent_at": now_iso(), "last_error": Value::Null })
        }
        Err(e) if attempts + 1 >= max_attempts => {
            METRICS.record_outbox_failed();
            log_output("ERROR", "OUTBOX", "FAILED", format!("id={} target={} giving up after {} attempts: {}", id, target, attempts + 1, e), true);
            json!({ "status": "failed", "last_error": e })
        }
        Err(e) => {
            let next = chrono::Utc::now() + chrono::Duration::seconds(backoff_secs(attempts + 1));
            json!({ "status": "pending", "last_error": e, "next_attempt_at": next.to_rfc3339_opts(chrono::SecondsFormat::Millis, true) })
        }
    };
    let _ = state.store.update(TABLE, Some(QF::Eq("id".into(), QV::I64(id))), patch).await;
}

/// Poll `flx_outbox` and deliver due rows. One task per process; replicas
/// coordinate through the conditional claim.
pub fn start_worker(state: Arc<AppState>) {
    let poll_ms: u64 = std::env::var("OUTBOX_POLL_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(1000).max(100);
    let batch: u32 = std::env::var("OUTBOX_BATCH").ok().and_then(|s| s.parse().ok()).unwrap_or(50).max(1);
    let max_attempts: i64 = std::env::var("OUTBOX_MAX_ATTEMPTS").ok().and_then(|s| s.parse().ok()).unwrap_or(8).max(1);
    let timeout_secs: u64 = std::env::var("OUTBOX_HTTP_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10).max(1);
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(timeout_secs)).build().unwrap_or_default();
    log_output("OUTBOX", "START", "worker", format!("poll={}ms batch={} max_attempts={}", poll_ms, batch, max_attempts), true);

    tokio::spawn(async move {
        let mut tick: u64 = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
            if crate::shutdown::is_shutting_down() {
                break;
            }
            let _inflight = crate::shutdown::InflightGuard::new();
            tick += 1;
            let due = QQ::from(TABLE)
                .select(["id", "attempts", "target", "headers", "payload"])
                .r#where(QF::And(vec![
                    QF::Eq("status".into(), QV::Str("pending".into())),
                    QF::Lte("next_attempt_at".into(), QV::Str(now_iso())),
                ]))
                .order_by("id", true)
                .limit(batch);
            match state.store.query(&due).await {
                Ok(rows) => {
                    for row in &rows {
                        process_row(&state, &client, row, max_attempts).await;
                    }
                }
                Err(e) => {
                    if tick % 60 == 1 {
                        log_output("WARN", "OUTBOX", "poll", format!("{}", e), true);
                    }
                }
            }
            // Pending gauge every ~10 ticks.
            if tick.is_multiple_of(10) {
                let q = QQ::from(TABLE).r#where(QF::Eq("status".into(), QV::Str("pending".into()))).agg_count_all("cnt");
                if let Ok(rows) = state.store.query(&q).await {
                    let n = rows.first().and_then(|r| r.get("cnt")).and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).unwrap_or(0);
                    METRICS.outbox_pending.store(n.max(0) as u64, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_secs(1), 10);
        assert_eq!(backoff_secs(2), 20);
        assert_eq!(backoff_secs(12), 3600);
        assert_eq!(backoff_secs(50), 3600);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn matching_filters_by_event() {
        let mut t = TableSchema::default();
        t.webhooks = vec![
            Webhook { event: "on_create".into(), url: "https://a".into(), headers: Default::default() },
            Webhook { event: "*".into(), url: "https://b".into(), headers: Default::default() },
            Webhook { event: "on_delete".into(), url: "".into(), headers: Default::default() },
        ];
        assert_eq!(matching(&t, "on_create").len(), 2);
        assert_eq!(matching(&t, "on_update").len(), 1);
        assert_eq!(matching(&t, "on_delete").len(), 1, "blank url is skipped, wildcard stays");
    }

    #[test]
    fn outbox_schema_is_consistent() {
        let t = table_schema();
        assert_eq!(t.table, TABLE);
        assert!(t.columns.iter().any(|c| c.name == "next_attempt_at"));
        assert!(t.auto_generate);
    }
}
