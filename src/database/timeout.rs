//! `TimeoutRepo`: a transparent wrapper around any [`DbRepository`] that
//!
//! * bounds every statement with a client-side timeout (`DB_QUERY_TIMEOUT_SECS`,
//!   default = statement timeout + 5 s) so a stuck connection can never hold a
//!   request hostage, and
//! * records the duration of every statement/transaction in the
//!   `flx_db_query_duration_seconds` histogram.
//!
//! The server-side statement timeout (`DB_STATEMENT_TIMEOUT_MS`) is applied per
//! connection in `connection.rs`; this wrapper is the safety net for cases the
//! server cannot enforce (network partitions, drivers without a timeout).

use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use serde_json::Value;

use super::state::{DbParam, DbRepository, DbTransaction, PoolStats};
use crate::metrics::METRICS;

/// Server-side statement timeout in milliseconds (`DB_STATEMENT_TIMEOUT_MS`, default 30 000).
pub static STATEMENT_TIMEOUT_MS: Lazy<u64> = Lazy::new(|| {
    std::env::var("DB_STATEMENT_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000)
});

/// Client-side per-statement timeout (`DB_QUERY_TIMEOUT_SECS`); defaults to the
/// statement timeout plus 5 s so the server normally fires first with a clean error.
pub static QUERY_TIMEOUT: Lazy<Duration> = Lazy::new(|| {
    let secs: u64 = std::env::var("DB_QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(*STATEMENT_TIMEOUT_MS / 1000 + 5);
    Duration::from_secs(secs.max(1))
});

pub struct TimeoutRepo {
    inner: Arc<dyn DbRepository>,
}

impl TimeoutRepo {
    pub fn wrap(inner: Arc<dyn DbRepository>) -> Arc<dyn DbRepository> {
        Arc::new(Self { inner })
    }
}

async fn timed<T, F>(kind: &'static str, fut: F) -> Result<T, anyhow::Error>
where
    F: std::future::Future<Output = Result<T, anyhow::Error>>,
{
    let start = Instant::now();
    let res = tokio::time::timeout(*QUERY_TIMEOUT, fut).await;
    let elapsed = start.elapsed();
    METRICS.db_duration.observe(kind, elapsed);
    match res {
        Ok(r) => r,
        Err(_) => {
            METRICS.record_db_timeout();
            Err(anyhow::anyhow!(
                "database {} timed out after {:?}",
                kind,
                *QUERY_TIMEOUT
            ))
        }
    }
}

#[async_trait::async_trait]
impl DbRepository for TimeoutRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        timed("query", self.inner.query(sql)).await
    }

    async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        timed("query", self.inner.query_with_params(sql, params)).await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        let tx = timed("begin", self.inner.begin_transaction()).await?;
        Ok(Box::new(TimeoutTx { inner: tx }))
    }

    fn pool_stats(&self) -> Option<PoolStats> {
        self.inner.pool_stats()
    }
}

struct TimeoutTx {
    inner: Box<dyn DbTransaction>,
}

#[async_trait::async_trait]
impl DbTransaction for TimeoutTx {
    async fn query_with_params(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        timed("tx_query", self.inner.query_with_params(sql, params)).await
    }

    async fn execute(&mut self, sql: &str, params: Vec<DbParam>) -> Result<u64, anyhow::Error> {
        timed("tx_execute", self.inner.execute(sql, params)).await
    }

    async fn commit(self: Box<Self>) -> Result<(), anyhow::Error> {
        timed("commit", self.inner.commit()).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), anyhow::Error> {
        timed("rollback", self.inner.rollback()).await
    }
}
