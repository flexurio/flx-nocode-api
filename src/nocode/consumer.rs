use std::sync::Arc;

use actix_web::web::Data;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rand::RngExt;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;

use std::collections::HashMap;

use crate::log::log_output;
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
    pub fn queue_key() -> String { "flx:wq:default".into() }
    pub fn dlq_key() -> String { "flx:wq:dlq".into() }
}

fn op_name(op: &WriteOpKind) -> &'static str {
    match op {
        WriteOpKind::Post => "POST",
        WriteOpKind::Put { .. } => "PUT",
        WriteOpKind::Delete { .. } => "DELETE",
    }
}

/// Push a job to the Redis list (LPUSH) and return queue length.
pub async fn enqueue_job(job: &WriteJob) -> Result<i64> {
    let max_len: i64 = std::env::var("WRITE_QUEUE_MAX_LEN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let retry_count: usize = std::env::var("WRITE_QUEUE_ENQUEUE_RETRY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let payload = serde_json::to_string(job)?;
    let client = crate::database::redis::get_manager().await?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    for attempt in 0..=retry_count {
        if max_len > 0 {
            let cur_len: i64 = conn.llen(WriteJob::queue_key()).await?;
            if cur_len >= max_len {
                return Err(anyhow!(
                    "Queue backpressure: len={} reached max={}",
                    cur_len,
                    max_len
                ));
            }
        }

        match conn.lpush(WriteJob::queue_key(), payload.clone()).await {
            Ok(len) => return Ok(len),
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

async fn push_dlq(job: WriteJob, worker: &str, error: &str) -> Result<i64> {
    let record = FailedJobRecord {
        failed_at: Utc::now().to_rfc3339(),
        worker: worker.to_string(),
        error: error.to_string(),
        route: job.route.clone(),
        op: op_name(&job.op).to_string(),
        job,
    };
    let payload = serde_json::to_string(&record)?;
    let client = crate::database::redis::get_manager().await?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let len: i64 = conn.lpush(WriteJob::dlq_key(), payload).await?;
    Ok(len)
}

async fn execute_with_retry(
    state: Data<AppState>,
    schemas_map: Arc<HashMap<String, Arc<TableSchema>>>,
    job: WriteJob,
    retry_max: usize,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=retry_max {
        match execute_job(state.clone(), schemas_map.clone(), job.clone()).await {
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

/// Fire-and-forget enqueue with explicit success/error observability.
pub fn enqueue_job_background(job: WriteJob, source: &str) {
    let source = source.to_string();
    tokio::spawn(async move {
        let op = op_name(&job.op);
        let route = job.route.clone();
        match enqueue_job(&job).await {
            Ok(queue_len) => {
                log_output(
                    "QUEUE",
                    "ENQUEUE-OK",
                    source.as_str(),
                    format!("{} {} queued (len={})", op, route, queue_len),
                    true,
                );
            }
            Err(e) => {
                log_output(
                    "QUEUE",
                    "ENQUEUE-ERR",
                    source.as_str(),
                    format!("{} {} failed: {}", op, route, e),
                    false,
                );
            }
        }
    });
}

/// Batch variant for fast-ack flows to preserve visibility and reduce log spam.
pub fn enqueue_jobs_background(jobs: Vec<WriteJob>, source: &str) {
    let source = source.to_string();
    tokio::spawn(async move {
        let total = jobs.len();
        let mut ok_count = 0usize;
        let mut err_count = 0usize;
        for job in jobs {
            if enqueue_job(&job).await.is_ok() {
                ok_count += 1;
            } else {
                err_count += 1;
            }
        }
        if err_count == 0 {
            log_output(
                "QUEUE",
                "ENQUEUE-BATCH-OK",
                source.as_str(),
                format!("queued {} jobs", ok_count),
                true,
            );
        } else {
            log_output(
                "QUEUE",
                "ENQUEUE-BATCH-ERR",
                source.as_str(),
                format!("queued={}, failed={}, total={}", ok_count, err_count, total),
                false,
            );
        }
    });
}


/// BRPOP with an existing Redis connection. Returns None on timeout.
async fn dequeue_with_conn(conn: &mut MultiplexedConnection) -> Result<Option<WriteJob>> {
    // BRPOP returns (key, value)
    let res: Option<(String, String)> = redis::cmd("BRPOP")
        .arg(WriteJob::queue_key())
        .arg(10) // seconds (longer to reduce wakeups)
        .query_async(conn)
        .await?;
    if let Some((_k, v)) = res {
        let job: WriteJob = serde_json::from_str(&v)?;
        Ok(Some(job))
    } else {
        Ok(None)
    }
}

/// Start N concurrent workers to pull from queue and execute writes.
pub async fn start_consumer(state: Data<AppState>, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>) {
    let concurrency: usize = std::env::var("WRITE_CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    log_output("QUEUE", "START", "consumer", format!("Workers={}", concurrency), true);

    let mut set = JoinSet::new();
    for idx in 0..concurrency {
        let state_cl = state.clone();
        let schemas_map_cl = schemas_map.clone();
        set.spawn(async move {
            log_output(
                "QUEUE",
                "WORKER-START",
                format!("worker-{}", idx).as_str(),
                "ready".to_string(),
                true,
            );
            
            // Error tracking for circuit breaker
            let mut consecutive_errors = 0u32;
            let max_consecutive_errors = 10; // Circuit breaker threshold
            let mut backoff_ms = 250u64; // Initial backoff
            
            // Establish a dedicated Redis connection for this worker and reuse it
            let client = match crate::database::redis::get_manager().await {
                Ok(c) => c,
                Err(e) => {
                    log_output(
                        "QUEUE",
                        "DEQUEUE-ERR",
                        format!("worker-{}", idx).as_str(),
                        format!("{}", e),
                        false,
                    );
                    // If we cannot even get a client, enter a slow retry loop
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        if let Ok(c2) = crate::database::redis::get_manager().await {
                            break c2;
                        }
                    }
                }
            };
            let mut conn = loop {
                match client.get_multiplexed_async_connection().await {
                    Ok(c) => break c,
                    Err(e) => {
                        log_output(
                            "QUEUE",
                            "DEQUEUE-ERR",
                            format!("worker-{}", idx).as_str(),
                            format!("{}", e),
                            false,
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            };
            
            loop {
                match dequeue_with_conn(&mut conn).await {
                    Ok(Some(job)) => {
                        // Reset error counter on success
                        consecutive_errors = 0;
                        backoff_ms = 250;
                        
                        let worker_name = format!("worker-{}", idx);
                        let retry_max: usize = std::env::var("WRITE_EXEC_RETRY_MAX")
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(2);
                        let job_for_dlq = job.clone();
                        if let Err(e) = execute_with_retry(state_cl.clone(), schemas_map_cl.clone(), job, retry_max).await {
                            log_output("QUEUE", "EXEC-ERR", worker_name.as_str(), format!("{}", e), false);
                            match push_dlq(job_for_dlq, worker_name.as_str(), &e.to_string()).await {
                                Ok(dlq_len) => log_output("QUEUE", "DLQ-PUSH", worker_name.as_str(), format!("len={}", dlq_len), false),
                                Err(dlq_err) => log_output("QUEUE", "DLQ-ERR", worker_name.as_str(), format!("{}", dlq_err), false),
                            }
                        }
                    }
                    Ok(None) => {
                        // idle tick - queue empty
                        consecutive_errors = 0; // Reset on successful poll
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        
                        // Log only first error and every 10th error to avoid spam
                        if consecutive_errors == 1 || consecutive_errors.is_multiple_of(10) {
                            log_output(
                                "QUEUE", 
                                "DEQUEUE-ERR", 
                                format!("worker-{}", idx).as_str(), 
                                format!("{} (count: {})", e, consecutive_errors), 
                                false
                            );
                        }

                        // Attempt to re-establish the connection on error
                        match client.get_multiplexed_async_connection().await {
                            Ok(c) => {
                                conn = c;
                            }
                            Err(e2) => {
                                log_output(
                                    "QUEUE",
                                    "DEQUEUE-ERR",
                                    format!("worker-{}", idx).as_str(),
                                    format!("reconnect failed: {}", e2),
                                    false,
                                );
                            }
                        }
                        
                        // Circuit breaker: if too many errors, sleep longer
                        if consecutive_errors >= max_consecutive_errors {
                            log_output(
                                "QUEUE",
                                "CIRCUIT-BREAKER",
                                format!("worker-{}", idx).as_str(),
                                format!("Too many errors ({}), entering long sleep (30s)", consecutive_errors),
                                false,
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            consecutive_errors = 0; // Reset after long sleep
                            backoff_ms = 250; // Reset backoff
                        } else {
                            // Exponential backoff with max 5 seconds
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(5000);
                        }
                    }
                }
            }
        });
    }

    // detach in background
    tokio::spawn(async move {
        while let Some(res) = set.join_next().await {
            match res {
                Ok(_) => {
                    log_output("QUEUE", "WORKER-EXIT", "consumer-supervisor", "worker exited unexpectedly".to_string(), false);
                }
                Err(e) => {
                    log_output("QUEUE", "WORKER-PANIC", "consumer-supervisor", format!("worker join error: {}", e), false);
                }
            }
        }
    });
}

async fn execute_job(state: Data<AppState>, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>, job: WriteJob) -> Result<()> {
    // schemas_map lookup is O(1)


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
            // Invalidate cached GET results for this route (public scope)
            let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
            let _ = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await;
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
            let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
            let _ = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await;
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
                let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
                if let Ok(n) = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await {
                    log_output("REDIS", "INVALIDATE", route.as_str(), format!("prefix={}, deleted={}", cache_prefix, n), true);
                }
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
                let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
                if let Ok(n) = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await {
                    log_output("REDIS", "INVALIDATE", route.as_str(), format!("prefix={}, deleted={}", cache_prefix, n), true);
                }
                Ok(())
            }
            Err(e) => {
                log_output("QUEUE", "DELETE-ERR", route.as_str(), format!("{}", e), false);
                Err(anyhow!(e))
            }
        }
    }
}
