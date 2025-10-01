use async_trait::async_trait;
use serde_json::Value;

use crate::storage::ast::{Filter, Query};

#[derive(Clone, Debug, Default)]
pub struct BackendCapabilities {
    pub transactions: bool,
    pub joins: bool,
    pub like: bool,
    pub sql_formula: bool,
}

#[async_trait]
pub trait TxStore: Send + Sync {
    // Query (transaction-scoped)
    async fn query(&mut self, q: &Query) -> anyhow::Result<Vec<Value>>;
    async fn insert(&mut self, collection: &str, doc: Value) -> anyhow::Result<Value>;
    async fn update(
        &mut self,
        collection: &str,
        filter: Option<Filter>,
        patch: Value,
    ) -> anyhow::Result<u64>;
    async fn delete(&mut self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64>;

    // Optional: raw SQL escape hatch (used by legacy SQL hooks)
    async fn raw_sql(
        &mut self,
        _sql: &str,
        _params: Vec<crate::database::state::DbParam>,
    ) -> anyhow::Result<Vec<Value>> {
        Err(anyhow::anyhow!("raw_sql unsupported by this backend"))
    }

    async fn commit(self: Box<Self>) -> anyhow::Result<()>;
    async fn rollback(self: Box<Self>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait DataStore: Send + Sync {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }

    // Query
    async fn query(&self, q: &Query) -> anyhow::Result<Vec<Value>>;

    // CRUD
    async fn insert(&self, collection: &str, doc: Value) -> anyhow::Result<Value>;
    async fn update(
        &self,
        collection: &str,
        filter: Option<Filter>,
        patch: Value,
    ) -> anyhow::Result<u64>;
    async fn delete(&self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64>;

    // Optional: transactions
    async fn begin_tx(&self) -> anyhow::Result<Box<dyn TxStore>> {
        Err(anyhow::anyhow!("transactions unsupported"))
    }

    // Legacy escape hatch for gradual migration
    async fn raw_sql(
        &self,
        _sql: &str,
        _params: Vec<crate::database::state::DbParam>,
    ) -> anyhow::Result<Vec<Value>> {
        Err(anyhow::anyhow!("raw_sql unsupported by this backend"))
    }
}
