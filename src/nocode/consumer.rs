use std::sync::Arc;

use actix_web::web::Data;
use anyhow::{anyhow, Result};
use chrono::Utc;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;

use crate::helpers::filter_table_schema;
use crate::log::log_output;
use crate::model::{ReferenceForeignKey, TableSchema};
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
    let mut conn = crate::database::redis::get_manager().await?.clone();
    let len: i64 = conn.lpush(WriteJob::queue_key(), payload).await?;
    log_output(
        "QUEUE",
        "ENQUEUE",
        job.route.as_str(),
        format!("op={:?}, new_len={}", job.op, len),
        true,
    );
    Ok(len)
}

/// Blocking pop with timeout (BRPOP with 1s) to allow graceful shutdown checks.
pub async fn dequeue_job() -> Result<Option<WriteJob>> {
    let mut conn = crate::database::redis::get_manager().await?.clone();
    // BRPOP returns (key, value)
    let res: Option<(String, String)> = redis::cmd("BRPOP")
        .arg(WriteJob::queue_key())
        .arg(1) // seconds
        .query_async(&mut conn)
        .await?;
    if let Some((_k, v)) = res {
        let job: WriteJob = serde_json::from_str(&v)?;
        log_output(
            "QUEUE",
            "DEQUEUE",
            job.route.as_str(),
            format!("op={:?}", job.op),
            true,
        );
        Ok(Some(job))
    } else {
        Ok(None)
    }
}

/// Start N concurrent workers to pull from queue and execute writes.
pub async fn start_consumer(state: Data<AppState>, schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>) {
    let concurrency: usize = std::env::var("WRITE_CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    log_output("QUEUE", "START", "consumer", format!("Workers={}", concurrency), true);

    let mut set = JoinSet::new();
    for idx in 0..concurrency {
        let state_cl = state.clone();
        let schemas_cl = schemas.clone();
        set.spawn(async move {
            log_output(
                "QUEUE",
                "WORKER-START",
                format!("worker-{}", idx).as_str(),
                "ready".to_string(),
                true,
            );
            loop {
                match dequeue_job().await {
                    Ok(Some(job)) => {
                        log_output(
                            "QUEUE",
                            "EXEC-START",
                            job.route.as_str(),
                            format!("worker={}, op={:?}", idx, job.op),
                            true,
                        );
                        if let Err(e) = execute_job(state_cl.clone(), schemas_cl.clone(), job).await {
                            log_output("QUEUE", "EXEC-ERR", format!("worker-{}", idx).as_str(), format!("{}", e), false);
                        } else {
                            log_output(
                                "QUEUE",
                                "EXEC-OK",
                                format!("worker-{}", idx).as_str(),
                                "done".to_string(),
                                true,
                            );
                        }
                    }
                    Ok(None) => {
                        // idle tick
                    }
                    Err(e) => {
                        log_output("QUEUE", "DEQUEUE-ERR", format!("worker-{}", idx).as_str(), format!("{}", e), false);
                        // small backoff
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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

async fn execute_job(state: Data<AppState>, schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>, job: WriteJob) -> Result<()> {
    let table_schemas = &schemas.0;

    // Build a fake HttpRequest headers for auth reuse if needed in future. For now handlers do their own auth when route not public.
    // Execute according to op using existing modules logic but via internal helpers.
    match job.op {
        WriteOpKind::Post => {
            // Use existing post::insert logic, but we need to call internal execution path.
            // For simplicity, we reconstruct a minimal Multipart-equivalent JSON and call a new helper.
            exec_post(state, job.route, table_schemas.clone().into(), job.body, job.actor_id.clone()).await
        }
        WriteOpKind::Put { id } => exec_put(state, job.route, schemas.clone(), job.body, id).await,
        WriteOpKind::Delete { id } => exec_delete(state, job.route, schemas.clone(), id).await,
    }
}

// Internal execution helpers built from existing code paths without HTTP types
async fn exec_post(state: Data<AppState>, route: String, table_schemas: Arc<Vec<TableSchema>>, body: Value, actor_id: Option<String>) -> Result<()> {
    // Reuse SQL generation by adapting internals would require refactor; as a pragmatic approach, call store.insert directly based on schema.post.columns
    let schema = filter_table_schema(&table_schemas, route.clone()).await;
    if schema.table.is_empty() { return Err(anyhow!("Schema not found for {}", route)); }

    // Build doc from allowed columns
    let mut doc = serde_json::Map::new();
    for col in schema.post.columns.iter() {
        if let Some(v) = body.get(col) { doc.insert(col.clone(), v.clone()); }
    }
    // server-side created_at
    doc.insert("created_at".into(), Value::String(Utc::now().to_rfc3339()));
    // ensure created_by_id if column exists but not provided
    if schema.columns.iter().any(|c| c.name == "created_by_id") && !doc.contains_key("created_by_id") {
        if let Some(actor) = actor_id.or_else(|| body.get("__actor_id__").and_then(|v| v.as_str()).map(|s| s.to_string())) {
            // detect type from schema
            if let Some(col) = schema.columns.iter().find(|c| c.name == "created_by_id") {
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
    }

    log_output("QUEUE", "INSERT", route.as_str(), format!("table={}, body_keys={}", schema.table, schema.post.columns.len()), true);
    match state.store.insert(&schema.table, Value::Object(doc)).await {
        Ok(_) => {
            log_output("QUEUE", "INSERT-OK", route.as_str(), schema.table.clone(), true);
            // Invalidate cached GET results for this route (public scope)
            let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
            if let Ok(n) = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await {
                log_output("REDIS", "INVALIDATE", route.as_str(), format!("prefix={}, deleted={}", cache_prefix, n), true);
            }
            Ok(())
        }
        Err(e) => {
            log_output("QUEUE", "INSERT-ERR", route.as_str(), format!("{}", e), false);
            Err(anyhow!(e))
        }
    }
}

async fn exec_put(state: Data<AppState>, route: String, schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>, body: Value, id: String) -> Result<()> {
    let table_schemas = &schemas.0;
    let schema = filter_table_schema(table_schemas, route.clone()).await;
    if schema.table.is_empty() { return Err(anyhow!("Schema not found for {}", route)); }

    // Build patch from allowed columns
    let mut patch = serde_json::Map::new();
    for col in schema.put.columns.iter() {
        if let Some(v) = body.get(col) { patch.insert(col.clone(), v.clone()); }
    }
    patch.insert("updated_at".into(), Value::String(Utc::now().to_rfc3339()));
    // updated_by_id if exists in schema and actor_id provided
    if let Some(col) = schema.columns.iter().find(|c| c.name == "updated_by_id") {
        if let Some(actor) = body.get("__actor_id__").and_then(|v| v.as_str()).map(|s| s.to_string()) {
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
    }

    let id_for_filter = id.clone();
    let filt_val = if let Ok(n) = id_for_filter.parse::<i64>() { QV::I64(n) } else { QV::Str(id_for_filter) };
    let filter = Some(QF::Eq("id".into(), filt_val));

    log_output("QUEUE", "UPDATE", route.as_str(), format!("table={}, id={}", schema.table, id), true);
    match state.store.update(&schema.table, filter, Value::Object(patch)).await {
        Ok(_) => {
            log_output("QUEUE", "UPDATE-OK", route.as_str(), schema.table.clone(), true);
            let cache_prefix = crate::database::redis::build_key_prefix("public", &route);
            if let Ok(n) = crate::database::redis::redis_delete_by_prefix(&cache_prefix).await {
                log_output("REDIS", "INVALIDATE", route.as_str(), format!("prefix={}, deleted={}", cache_prefix, n), true);
            }
            Ok(())
        }
        Err(e) => {
            log_output("QUEUE", "UPDATE-ERR", route.as_str(), format!("{}", e), false);
            Err(anyhow!(e))
        }
    }
}

async fn exec_delete(state: Data<AppState>, route: String, schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>, id: String) -> Result<()> {
    let table_schemas = &schemas.0;
    let schema = filter_table_schema(table_schemas, route.clone()).await;
    if schema.table.is_empty() { return Err(anyhow!("Schema not found for {}", route)); }

    let id_for_filter = id.clone();
    let filt_val = if let Ok(n) = id_for_filter.parse::<i64>() { QV::I64(n) } else { QV::Str(id_for_filter) };
    let filter = Some(QF::Eq("id".into(), filt_val));

    if schema.del.type_delete == "soft" {
        let mut patch = serde_json::Map::new();
        patch.insert("deleted_at".into(), Value::String(Utc::now().to_rfc3339()));
        // deleted_by_id from actor if provided
        // Note: could set deleted_by_id if actor was carried; skipped for now.
    log_output("QUEUE", "DELETE-SOFT", route.as_str(), format!("table={}, id={}", schema.table, id.clone()), true);
        match state.store.update(&schema.table, filter, Value::Object(patch)).await {
            Ok(_) => {
                log_output("QUEUE", "DELETE-OK", route.as_str(), schema.table.clone(), true);
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
    log_output("QUEUE", "DELETE-HARD", route.as_str(), format!("table={}, id={}", schema.table, id.clone()), true);
        match state.store.delete(&schema.table, filter).await {
            Ok(_) => {
                log_output("QUEUE", "DELETE-OK", route.as_str(), schema.table.clone(), true);
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
