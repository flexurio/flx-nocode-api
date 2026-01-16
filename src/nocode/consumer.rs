use std::sync::Arc;

use actix_web::web::Data;
use anyhow::{anyhow, Result};
use chrono::Utc;
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
}

/// Push a job to the Redis list (LPUSH) and return queue length.
pub async fn enqueue_job(job: &WriteJob) -> Result<i64> {
    let payload = serde_json::to_string(job)?;
    let client = crate::database::redis::get_manager().await?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let len: i64 = conn.lpush(WriteJob::queue_key(), payload).await?;
    Ok(len)
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
                        
                        if let Err(e) = execute_job(state_cl.clone(), schemas_map_cl.clone(), job).await {
                            log_output("QUEUE", "EXEC-ERR", format!("worker-{}", idx).as_str(), format!("{}", e), false);
                        }
                    }
                    Ok(None) => {
                        // idle tick - queue empty
                        consecutive_errors = 0; // Reset on successful poll
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        
                        // Log only first error and every 10th error to avoid spam
                        if consecutive_errors == 1 || consecutive_errors % 10 == 0 {
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
        while let Some(_res) = set.join_next().await {
            // workers are endless; this shouldn't happen, but if it does, spawn a replacement
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
async fn exec_post(state: Data<AppState>, route: String, schemas_map: Arc<HashMap<String, Arc<TableSchema>>>, body: Value, actor_id: Option<String>) -> Result<()> {
    // Reuse SQL generation by adapting internals would require refactor; as a pragmatic approach, call store.insert directly based on schema.post.columns
    let schema = schemas_map.get(&route).ok_or_else(|| anyhow!("Schema not found for {}", route))?;

    // Build doc from allowed columns
    let mut doc = serde_json::Map::new();
    for col in schema.post.columns.iter() {
        if let Some(v) = body.get(col) { doc.insert(col.clone(), v.clone()); }
    }
    // server-side created_at
    doc.insert("created_at".into(), Value::String(Utc::now().to_rfc3339()));
    // ensure created_by_id if column exists but not provided
    if schema.columns.iter().any(|c| c.name == "created_by_id") && !doc.contains_key("created_by_id") {
        let actor_opt = actor_id.or_else(|| body.get("__actor_id__").and_then(|v| v.as_str()).map(|s| s.to_string()));
        let col_opt = schema.columns.iter().find(|c| c.name == "created_by_id");
        if let (Some(actor), Some(col)) = (actor_opt, col_opt) {
            if col.type_data.contains("int") {
                if let Ok(n) = actor.parse::<i64>() { doc.insert("created_by_id".into(), serde_json::json!(n)); }
                else { doc.insert("created_by_id".into(), Value::String(actor)); }
            } else if col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
                if let Ok(f) = actor.parse::<f64>() { doc.insert("created_by_id".into(), serde_json::json!(f)); }
                else { doc.insert("created_by_id".into(), Value::String(actor)); }
            } else {
                doc.insert("created_by_id".into(), Value::String(actor));
            }
        }
    }

    match state.store.insert(&schema.table, Value::Object(doc)).await {
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

    // Build patch from allowed columns
    let mut patch = serde_json::Map::new();
    for col in schema.put.columns.iter() {
        if let Some(v) = body.get(col) { patch.insert(col.clone(), v.clone()); }
    }
    patch.insert("updated_at".into(), Value::String(Utc::now().to_rfc3339()));
    // updated_by_id if exists in schema and actor_id provided
    let col_opt = schema.columns.iter().find(|c| c.name == "updated_by_id");
    let actor_opt = body.get("__actor_id__").and_then(|v| v.as_str()).map(|s| s.to_string());
    if let (Some(col), Some(actor)) = (col_opt, actor_opt) {
        if col.type_data.contains("int") {
            if let Ok(n) = actor.parse::<i64>() { patch.insert("updated_by_id".into(), Value::Number(n.into())); }
            else { patch.insert("updated_by_id".into(), Value::String(actor)); }
        } else if col.type_data.contains("float") || col.type_data.contains("double") || col.type_data.contains("decimal") || col.type_data.contains("money") {
            if let Ok(f) = actor.parse::<f64>() { patch.insert("updated_by_id".into(), serde_json::json!(f)); }
            else { patch.insert("updated_by_id".into(), Value::String(actor)); }
        } else {
            patch.insert("updated_by_id".into(), Value::String(actor));
        }
    }

    let id_for_filter = id.clone();
    let filt_val = if let Ok(n) = id_for_filter.parse::<i64>() { QV::I64(n) } else { QV::Str(id_for_filter) };
    let filter = Some(QF::Eq("id".into(), filt_val));

    match state.store.update(&schema.table, filter, Value::Object(patch)).await {
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
