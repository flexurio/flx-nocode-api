use std::sync::Arc;

use actix_web::web::Data;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rand::RngExt;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;

use std::collections::HashMap;

use crate::log::log_output;
use crate::metrics::METRICS;
use crate::model::TableSchema;
use crate::storage::ast::{Filter as QF, Val as QV};
use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteOpKind {
    Post,
    Put { id: String },
    Delete { id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteJob {
    pub route: String,
    pub op: WriteOpKind,
    pub body: Value,
    pub headers: Vec<(String, String)>,
    pub enqueued_at: String,
    pub actor_id: Option<String>,
}

impl WriteJob {
    /// Redis Stream carrying write jobs (`XADD` / `XREADGROUP`).
    pub fn stream_key() -> String { "flx:wq:stream".into() }
    pub fn group() -> String { "flx-writers".into() }
    pub fn dlq_key() -> String { "flx:wq:dlq".into() }
    /// Legacy list keys (pre-Streams); drained into the stream once at boot.
    pub fn legacy_queue_key() -> String { "flx:wq:default".into() }
    pub fn legacy_processing_pattern() -> &'static str { "flx:wq:processing:*" }

    pub fn queue() -> crate::nocode::stream_queue::StreamQueue {
        crate::nocode::stream_queue::StreamQueue::new(Self::stream_key(), Self::group(), Self::dlq_key())
    }
}

fn op_name(op: &WriteOpKind) -> &'static str {
    match op {
        WriteOpKind::Post => "POST",
        WriteOpKind::Put { .. } => "PUT",
        WriteOpKind::Delete { .. } => "DELETE",
    }
}

use once_cell::sync::Lazy;

static WRITE_QUEUE_MAX_LEN: Lazy<i64> = Lazy::new(|| {
    std::env::var("WRITE_QUEUE_MAX_LEN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
});

static WRITE_QUEUE_ENQUEUE_RETRY: Lazy<usize> = Lazy::new(|| {
    std::env::var("WRITE_QUEUE_ENQUEUE_RETRY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
});

static WRITE_EXEC_RETRY_MAX: Lazy<usize> = Lazy::new(|| {
    std::env::var("WRITE_EXEC_RETRY_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
});

/// Idle time after which another consumer may claim a pending entry
/// (`WRITE_CLAIM_IDLE_MS`, default 60 000). Must exceed the longest expected job.
static WRITE_CLAIM_IDLE_MS: Lazy<u64> = Lazy::new(|| {
    std::env::var("WRITE_CLAIM_IDLE_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(60_000)
});

/// Publish a job to the stream (`XADD`) and return the queue length.
/// The HTTP handler replies 202 only after this succeeds.
pub async fn enqueue_job(job: &WriteJob) -> Result<i64> {
    let max_len: i64 = *WRITE_QUEUE_MAX_LEN;
    let retry_count: usize = *WRITE_QUEUE_ENQUEUE_RETRY;
    let q = WriteJob::queue();

    let payload = serde_json::to_string(job)?;
    // Shared auto-reconnecting connection: no TCP connect + AUTH per request.
    let mut conn = crate::database::redis::get_connection().await?;
    for attempt in 0..=retry_count {
        if max_len > 0 {
            let cur_len: i64 = redis::cmd("XLEN").arg(WriteJob::stream_key()).query_async(&mut conn).await?;
            if cur_len >= max_len {
                return Err(anyhow!(
                    "Queue backpressure: len={} reached max={}",
                    cur_len,
                    max_len
                ));
            }
        }

        let res: redis::RedisResult<String> = redis::cmd("XADD")
            .arg(WriteJob::stream_key())
            .arg("*")
            .arg("payload")
            .arg(&payload)
            .query_async(&mut conn)
            .await;
        match res {
            Ok(_) => {
                let len: i64 = redis::cmd("XLEN").arg(WriteJob::stream_key()).query_async(&mut conn).await.unwrap_or(0);
                METRICS.write_queue_len.store(len.max(0) as u64, std::sync::atomic::Ordering::Relaxed);
                return Ok(len);
            }
            Err(e) => {
                if attempt >= retry_count {
                    return Err(anyhow!(e));
                }
                let jitter_ms: u64 = rand::rng().random_range(0..=60);
                let sleep_ms = ((attempt as u64) + 1) * 120 + jitter_ms;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
        }
    }
    let _ = q;
    Err(anyhow!("enqueue failed unexpectedly"))
}

#[derive(Debug, Serialize)]
struct FailedJobRecord {
    failed_at: String,
    worker: String,
    error: String,
    route: String,
    op: String,
    job: WriteJob,
}

async fn push_dlq(conn: &mut MultiplexedConnection, job: WriteJob, worker: &str, error: &str) -> Result<i64> {
    let record = FailedJobRecord {
        failed_at: Utc::now().to_rfc3339(),
        worker: worker.to_string(),
        error: error.to_string(),
        route: job.route.clone(),
        op: op_name(&job.op).to_string(),
        job,
    };
    WriteJob::queue().dead_letter(conn, &serde_json::to_string(&record)?).await
}

/// Dead-letter a payload that could not even be parsed as a `WriteJob`.
async fn push_dlq_raw(conn: &mut MultiplexedConnection, raw: &str, worker: &str, error: &str) -> Result<i64> {
    let record = serde_json::json!({
        "failed_at": Utc::now().to_rfc3339(),
        "worker": worker,
        "error": error,
        "raw": raw,
    });
    WriteJob::queue().dead_letter(conn, &record.to_string()).await
}

async fn execute_with_retry(
    state: Data<AppState>,
    job: WriteJob,
    retry_max: usize,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=retry_max {
        match execute_job(state.clone(), job.clone()).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                if attempt < retry_max {
                    let jitter_ms: u64 = rand::rng().random_range(0..=80);
                    let sleep_ms = ((attempt as u64) + 1) * 200 + jitter_ms;
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("execution failed")))
}

/// Process one delivered entry end-to-end: parse → execute (with retries) →
/// dead-letter on failure → `XACK`. Nothing is acknowledged before it is
/// either applied to the database or safely stored in the DLQ.
async fn handle_entry(
    state: &Data<AppState>,
    conn: &mut MultiplexedConnection,
    q: &crate::nocode::stream_queue::StreamQueue,
    worker: &str,
    entry: crate::nocode::stream_queue::Entry,
) {
    let retry_max: usize = *WRITE_EXEC_RETRY_MAX;
    let job = match serde_json::from_str::<WriteJob>(&entry.payload) {
        Ok(j) => j,
        Err(e) => {
            match push_dlq_raw(conn, &entry.payload, worker, &format!("invalid job payload: {}", e)).await {
                Ok(n) => log_output("QUEUE", "DLQ-PUSH", worker, format!("malformed payload, dlq_len={}", n), false),
                Err(dlq_err) => {
                    // Keep it pending: it will be re-delivered via XAUTOCLAIM.
                    log_output("QUEUE", "DLQ-ERR", worker, format!("{} (entry kept pending)", dlq_err), false);
                    return;
                }
            }
            if let Err(e) = q.ack(conn, &entry.id).await {
                log_output("QUEUE", "ACK-ERR", worker, format!("{}", e), false);
            }
            return;
        }
    };
    let job_for_dlq = job.clone();
    if let Err(e) = execute_with_retry(state.clone(), job, retry_max).await {
        log_output("QUEUE", "EXEC-ERR", worker, format!("{}", e), false);
        METRICS.record_queue_failure();
        match push_dlq(conn, job_for_dlq, worker, &e.to_string()).await {
            Ok(n) => log_output("QUEUE", "DLQ-PUSH", worker, format!("dlq_len={}", n), false),
            Err(dlq_err) => {
                log_output("QUEUE", "DLQ-ERR", worker, format!("{} (entry kept pending)", dlq_err), false);
                return;
            }
        }
    } else {
        METRICS.record_queue_processed();
    }
    if let Err(e) = q.ack(conn, &entry.id).await {
        log_output("QUEUE", "ACK-ERR", worker, format!("{}", e), false);
    }
}

async fn open_connection(worker: &str) -> MultiplexedConnection {
    loop {
        match crate::database::redis::get_manager().await {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(c) => return c,
                Err(e) => log_output("QUEUE", "DEQUEUE-ERR", worker, format!("{}", e), false),
            },
            Err(e) => log_output("QUEUE", "DEQUEUE-ERR", worker, format!("{}", e), false),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Start N concurrent workers that consume the write stream and execute writes.
pub async fn start_consumer(state: Data<AppState>) {
    let concurrency: usize = std::env::var("WRITE_CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    let batch_size: usize = std::env::var("WRITE_BATCH_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| format!("pid{}", std::process::id()));
    log_output("QUEUE", "START", "consumer", format!("Workers={}, BatchSize={}, stream={}, group={}", concurrency, batch_size, WriteJob::stream_key(), WriteJob::group()), true);

    let q = WriteJob::queue();

    // Group + one-off migration of jobs left in the pre-Streams lists.
    {
        let mut conn = open_connection("consumer-boot").await;
        if let Err(e) = q.ensure_group(&mut conn).await {
            log_output("QUEUE", "BOOT-ERR", "consumer", format!("{}", e), false);
        }
        match q.migrate_legacy_lists(&mut conn, &WriteJob::legacy_queue_key(), Some(WriteJob::legacy_processing_pattern())).await {
            Ok(0) => {}
            Ok(n) => log_output("QUEUE", "MIGRATE", "consumer", format!("moved {} job(s) from legacy lists into the stream", n), true),
            Err(e) => log_output("QUEUE", "MIGRATE-ERR", "consumer", format!("{}", e), false),
        }
    }

    let mut set = JoinSet::new();
    for idx in 0..concurrency {
        let state_cl = state.clone();
        let q = q.clone();
        let worker_name = format!("{}-w{}", host, idx);
        set.spawn(async move {
            log_output("QUEUE", "WORKER-START", worker_name.as_str(), "ready".to_string(), true);
            let mut consecutive_errors = 0u32;
            let max_consecutive_errors = 10;
            let mut backoff_ms = 250u64;
            let mut conn = open_connection(&worker_name).await;
            let mut last_claim = std::time::Instant::now();

            loop {
                // Periodically adopt entries a dead consumer left pending.
                if last_claim.elapsed() >= std::time::Duration::from_secs(30) {
                    last_claim = std::time::Instant::now();
                    match q.claim_stale(&mut conn, &worker_name, *WRITE_CLAIM_IDLE_MS, batch_size).await {
                        Ok(stale) if !stale.is_empty() => {
                            log_output("QUEUE", "RECLAIM", worker_name.as_str(), format!("re-delivering {} stale entr(y/ies)", stale.len()), true);
                            for entry in stale {
                                handle_entry(&state_cl, &mut conn, &q, &worker_name, entry).await;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => log_output("QUEUE", "RECLAIM-ERR", worker_name.as_str(), format!("{}", e), false),
                    }
                }

                match q.read_batch(&mut conn, &worker_name, batch_size, 5_000).await {
                    Ok(items) if !items.is_empty() => {
                        consecutive_errors = 0;
                        backoff_ms = 250;
                        for entry in items {
                            handle_entry(&state_cl, &mut conn, &q, &worker_name, entry).await;
                        }
                    }
                    Ok(_) => {
                        consecutive_errors = 0; // idle tick
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors == 1 || consecutive_errors.is_multiple_of(10) {
                            log_output("QUEUE", "DEQUEUE-ERR", worker_name.as_str(), format!("{} (count: {})", e, consecutive_errors), false);
                        }
                        conn = open_connection(&worker_name).await;
                        let _ = q.ensure_group(&mut conn).await;
                        if consecutive_errors >= max_consecutive_errors {
                            log_output("QUEUE", "CIRCUIT-BREAKER", worker_name.as_str(), format!("Too many errors ({}), entering long sleep (30s)", consecutive_errors), false);
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            consecutive_errors = 0;
                            backoff_ms = 250;
                        } else {
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(5000);
                        }
                    }
                }
            }
        });
    }

    // Lag sampler: XLEN / XPENDING -> Prometheus gauges every 5 s.
    {
        let q = q.clone();
        tokio::spawn(async move {
            let mut conn = open_connection("queue-metrics").await;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match (q.len(&mut conn).await, q.pending(&mut conn).await) {
                    (Ok(len), Ok(pending)) => {
                        METRICS.write_queue_len.store(len.max(0) as u64, std::sync::atomic::Ordering::Relaxed);
                        METRICS.write_queue_pending.store(pending.max(0) as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                    _ => conn = open_connection("queue-metrics").await,
                }
            }
        });
    }

    // detach in background
    tokio::spawn(async move {
        while let Some(res) = set.join_next().await {
            match res {
                Ok(_) => log_output("QUEUE", "WORKER-EXIT", "consumer-supervisor", "worker exited unexpectedly".to_string(), false),
                Err(e) => log_output("QUEUE", "WORKER-PANIC", "consumer-supervisor", format!("worker join error: {}", e), false),
            }
        }
    });
}

async fn execute_job(state: Data<AppState>, job: WriteJob) -> Result<()> {
    // Always resolve against the live registry so a hot reload applies to queued jobs too.
    let schemas_map = crate::registry::snapshot().schemas.clone();


    // Build a fake HttpRequest headers for auth reuse if needed in future. For now handlers do their own auth when route not public.
    // Execute according to op using existing modules logic but via internal helpers.
    match job.op {
        WriteOpKind::Post => {
            // Use existing post::insert logic, but we need to call internal execution path.
            // For simplicity, we reconstruct a minimal Multipart-equivalent JSON and call a new helper.
            exec_post(state, job.route, schemas_map.clone(), job.body, job.actor_id.clone()).await
        }
        WriteOpKind::Put { id } => exec_put(state, job.route, schemas_map.clone(), job.body, id).await,
        WriteOpKind::Delete { id } => exec_delete(state, job.route, schemas_map.clone(), id).await,
    }
}

// Internal execution helpers built from existing code paths without HTTP types
// Internal execution helpers built from existing code paths without HTTP types
use crate::crypt::{encrypt, is_encrypted_string};
use crate::helpers::find_column_match;
use crate::nocode::pk_utils::json_value_from_str_and_type;
// use crate::database::state::DbParam;
use std::collections::HashSet;

/// Extract a column value from JSON body as a trimmed string, returning empty
/// for `null`/missing values without leaking the literal string `"null"`.
fn body_value_as_str(body: &Value, col: &str) -> String {
    match body.get(col) {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.trim().to_string(),
        Some(v) => v.to_string().trim().to_string(),
    }
}

async fn exec_post(state: Data<AppState>, route: String, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>, body: Value, actor_id: Option<String>) -> Result<()> {
    let schema = schemas_map.get(&route).ok_or_else(|| anyhow!("Schema not found for {}", route))?;

    // Logic ported from data_create_service.rs
    let skip_columns: HashSet<&str> = [
        "created_at", "created_by_id", "updated_at", "updated_by_id", "deleted_at", "deleted_by_id",
    ].iter().cloned().collect();

    let mut filtered_columns: Vec<&crate::model::Column> = Vec::with_capacity(schema.post.columns.len());
    filtered_columns.extend(
        schema
            .columns
            .iter()
            .filter(|col| !col.auto_increment && !skip_columns.contains(col.name.as_str()) && schema.post.columns.contains(&col.name))
    );
     // explicit id check
    if let Some(col) = schema.columns.iter().find(|c| c.name == "id" && !c.auto_increment) {
        filtered_columns.push(col);
    }
    
    let mut doc_map = serde_json::Map::new();

    for col in filtered_columns.iter() {
        if col.auto_increment { continue; }

        let mut isformula = false;
        let post_columns: Vec<&str> = schema.post.columns.iter().map(|s| s.as_str()).collect();
        let (exists, matched_string) = find_column_match(&post_columns, &col.name);

        if exists && col.name != "id" {
             let string_formula = matched_string.unwrap_or("").to_string();
             if string_formula.contains('=') {
                 isformula = true;
                 // Formulas not fully supported in queue yet without major refactor of InsertValue; 
                 // For now, skip formula calculation in queue or use simpler logic. 
                 // We will skip formulas for now in consumer to avoid complex dependencies, 
                 // assuming queue is mostly for raw data.
                 // If formula is critical, we need to port `build_formula_value_service` too.
             }
        }
        
        if !isformula && (col.name != "id" || col.function.is_empty()) {
            let raw_str = body_value_as_str(&body, &col.name);
            if raw_str.is_empty() {
                continue;
            }
            let value = if col.encrypt && !is_encrypted_string(&raw_str) {
                encrypt(state.encrypt_key.clone(), raw_str)
            } else {
                raw_str
            };
            doc_map.insert(col.name.clone(), json_value_from_str_and_type(&value, &col.type_data));
        }
    }

    // server-side created_at
    doc_map.insert("created_at".into(), Value::String(Utc::now().to_rfc3339()));
    
    // ensure created_by_id if column exists
    if !doc_map.contains_key("created_by_id")
        && let Some(col) = schema.columns.iter().find(|c| c.name == "created_by_id")
    {
        let actor_opt = actor_id.or_else(|| {
            body.get("__actor_id__")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
        if let Some(actor) = actor_opt {
            doc_map.insert(
                "created_by_id".into(),
                json_value_from_str_and_type(&actor, &col.type_data),
            );
        }
    }

    match state.store.insert(&schema.table, Value::Object(doc_map)).await {
        Ok(_) => {
            crate::database::state::invalidate_route(&state, &route).await;
            Ok(())
        }
        Err(e) => {
            log_output("QUEUE", "INSERT-ERR", route.as_str(), format!("{}", e), false);
            Err(anyhow!(e))
        }
    }
}

async fn exec_put(state: Data<AppState>, route: String, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>, body: Value, id: String) -> Result<()> {
    let schema = schemas_map.get(&route).ok_or_else(|| anyhow!("Schema not found for {}", route))?;

    let mut doc_map = serde_json::Map::new();
    let put_columns = &schema.put.columns;

    // Filter columns that are in schema.put.columns
    let skip_columns: HashSet<&str> = [
        "created_at", "created_by_id", "updated_at", "updated_by_id", "deleted_at", "deleted_by_id",
    ].iter().cloned().collect();

    let mut filtered_columns: Vec<&crate::model::Column> = Vec::with_capacity(put_columns.len());
    filtered_columns.extend(
        schema
            .columns
            .iter()
            .filter(|col| !skip_columns.contains(col.name.as_str()) && put_columns.contains(&col.name))
    );

    for col in filtered_columns.iter() {
        let mut isformula = false;
        let put_cols_slice: Vec<&str> = put_columns.iter().map(|s| s.as_str()).collect();
        let (exists, matched_string) = find_column_match(&put_cols_slice, &col.name);
        
        if exists {
             let string_formula = matched_string.unwrap_or("").to_string();
             if string_formula.contains('=') {
                 isformula = true;
             }
        }

        if !isformula {
            let raw_str = body_value_as_str(&body, &col.name);
            if raw_str.is_empty() {
                continue;
            }
            let value = if col.encrypt && !is_encrypted_string(&raw_str) {
                encrypt(state.encrypt_key.clone(), raw_str)
            } else {
                raw_str
            };
            doc_map.insert(col.name.clone(), json_value_from_str_and_type(&value, &col.type_data));
        }
    }

    doc_map.insert("updated_at".into(), Value::String(Utc::now().to_rfc3339()));

    // updated_by_id if exists
    if let Some(col) = schema.columns.iter().find(|c| c.name == "updated_by_id")
        && let Some(actor) = body
            .get("__actor_id__")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    {
        doc_map.insert(
            "updated_by_id".into(),
            json_value_from_str_and_type(&actor, &col.type_data),
        );
    }

    let id_for_filter = id.clone();
    let filt_val = if let Ok(n) = id_for_filter.parse::<i64>() { QV::I64(n) } else { QV::Str(id_for_filter) };
    let filter = Some(QF::Eq("id".into(), filt_val));

    match state.store.update(&schema.table, filter, Value::Object(doc_map)).await {
        Ok(_) => {
            crate::database::state::invalidate_route(&state, &route).await;
            Ok(())
        }
        Err(e) => {
            log_output("QUEUE", "UPDATE-ERR", route.as_str(), format!("{}", e), false);
            Err(anyhow!(e))
        }
    }
}

async fn exec_delete(state: Data<AppState>, route: String, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>, id: String) -> Result<()> {
    let schema = schemas_map.get(&route).ok_or_else(|| anyhow!("Schema not found for {}", route))?;

    let id_for_filter = id.clone();
    let filt_val = if let Ok(n) = id_for_filter.parse::<i64>() { QV::I64(n) } else { QV::Str(id_for_filter) };
    let filter = Some(QF::Eq("id".into(), filt_val));

    if schema.del.type_delete == "soft" {
        let mut patch = serde_json::Map::new();
        patch.insert("deleted_at".into(), Value::String(Utc::now().to_rfc3339()));
        // deleted_by_id from actor if provided
        // Note: could set deleted_by_id if actor was carried; skipped for now.

        match state.store.update(&schema.table, filter, Value::Object(patch)).await {
            Ok(_) => {
                crate::database::state::invalidate_route(&state, &route).await;
                Ok(())
            }
            Err(e) => {
                log_output("QUEUE", "DELETE-ERR", route.as_str(), format!("{}", e), false);
                Err(anyhow!(e))
            }
        }
    } else {
        match state.store.delete(&schema.table, filter).await {
            Ok(_) => {
                crate::database::state::invalidate_route(&state, &route).await;
                Ok(())
            }
            Err(e) => {
                log_output("QUEUE", "DELETE-ERR", route.as_str(), format!("{}", e), false);
                Err(anyhow!(e))
            }
        }
    }
}
