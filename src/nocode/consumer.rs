
use actix_web::web::Data;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rand::RngExt;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;


use crate::log::log_output;
use crate::metrics::METRICS;
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
    /// Validated JWT claims of the caller, so the consumer can run the exact
    /// same service path (rules.json, allowed_fields, audit) as the HTTP request.
    #[serde(default)]
    pub claims: Option<crate::auth::Claims>,
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
    let _inflight = crate::shutdown::InflightGuard::new();
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

    // Each worker owns a thread with a current-thread runtime: the service layer
    // takes an actix `HttpRequest` (not `Send`), so the job future must stay on
    // one thread — exactly how actix itself runs its workers.
    for idx in 0..concurrency {
        let state_cl = state.clone();
        let q = q.clone();
        let worker_name = format!("{}-w{}", host, idx);
        let thread_name = worker_name.clone();
        let spawned = std::thread::Builder::new().name(format!("flx-queue-{}", idx)).spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    log_output("QUEUE", "WORKER-PANIC", thread_name.as_str(), format!("runtime: {}", e), false);
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
            log_output("QUEUE", "WORKER-START", worker_name.as_str(), "ready".to_string(), true);
            let mut consecutive_errors = 0u32;
            let max_consecutive_errors = 10;
            let mut backoff_ms = 250u64;
            let mut conn = open_connection(&worker_name).await;
            let mut last_claim = std::time::Instant::now();

            loop {
                if crate::shutdown::is_shutting_down() {
                    log_output("QUEUE", "WORKER-STOP", worker_name.as_str(), "shutdown requested".to_string(), true);
                    break;
                }
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
            log_output("QUEUE", "WORKER-EXIT", thread_name.as_str(), "worker thread finished".to_string(), false);
        });
        if let Err(e) = spawned {
            log_output("QUEUE", "WORKER-PANIC", "consumer", format!("cannot spawn worker thread: {}", e), false);
        }
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

}

/// Build the synthetic request the service layer expects: same method/path as
/// the REST call and the caller's validated claims in the extensions (the
/// services' fast path), so rules.json, allowed_fields, formulas, details,
/// triggers, state machine and audit all apply exactly as for a direct call.
fn synthetic_request(method: actix_web::http::Method, path: &str, job: &WriteJob) -> actix_web::HttpRequest {
    use actix_web::HttpMessage;
    let req = actix_web::test::TestRequest::default()
        .method(method)
        .uri(path)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-request-id", format!("queue-{}", job.enqueued_at)))
        .to_http_request();
    match (&job.claims, &job.actor_id) {
        (Some(c), _) => {
            req.extensions_mut().insert(c.clone());
        }
        (None, Some(actor)) => {
            // Legacy job (enqueued before claims were recorded): it was already
            // authorised when accepted, so run it as that actor with full rights.
            req.extensions_mut().insert(crate::auth::Claims { id: actor.clone(), ..crate::auth::Claims::default() });
        }
        (None, None) => {}
    }
    req
}

async fn response_error(resp: actix_web::HttpResponse) -> String {
    let status = resp.status();
    let body = actix_web::body::to_bytes(resp.into_body()).await.map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default();
    format!("{} {}", status, body)
}

async fn execute_job(state: Data<AppState>, job: WriteJob) -> Result<()> {
    // Always resolve against the live registry so a hot reload applies to queued jobs too.
    let registry = crate::registry::snapshot();
    let schema = registry
        .schemas
        .get(&job.route)
        .cloned()
        .ok_or_else(|| anyhow!("Schema not found for {}", job.route))?;
    let empty_query = actix_web::web::Query(Value::Object(serde_json::Map::new()));
    let route = job.route.clone();

    match job.op.clone() {
        WriteOpKind::Post => {
            let req = synthetic_request(actix_web::http::Method::POST, &format!("/{}", route), &job);
            crate::nocode::services::data_create_service::process_insert_request(
                &state, &empty_query, &route, &schema, job.body.clone(), &req,
            )
            .await
            .map(|_| ())
            .map_err(|e| anyhow!("{}", e.message))
        }
        WriteOpKind::Put { id } => {
            let req = synthetic_request(actix_web::http::Method::PUT, &format!("/{}/{}", route, id), &job);
            let resp = crate::nocode::services::data_update_service::process_update_request(
                &state,
                &empty_query,
                &route,
                &schema,
                &registry.ref_foreign_keys,
                job.body.clone(),
                actix_web::web::Path::from(id),
                &req,
            )
            .await;
            if resp.status().is_success() { Ok(()) } else { Err(anyhow!(response_error(resp).await)) }
        }
        WriteOpKind::Delete { id } => {
            let req = synthetic_request(actix_web::http::Method::DELETE, &format!("/{}/{}", route, id), &job);
            crate::nocode::services::data_delete_service::process_delete_request(
                state.clone(),
                empty_query,
                route,
                schema,
                registry.ref_foreign_keys.clone(),
                id,
                req,
            )
            .await
            .map(|_| ())
            .map_err(|e| anyhow!(e))
        }
    }
}
