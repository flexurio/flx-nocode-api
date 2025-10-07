use async_trait::async_trait;
use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait};

use crate::storage::ast::{Filter, Query, LogicalPlan};

#[allow(dead_code)]
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
    #[allow(dead_code)]
    async fn query(&mut self, q: &Query) -> anyhow::Result<Vec<Value>>;
    #[allow(dead_code)]
    async fn insert(&mut self, collection: &str, doc: Value) -> anyhow::Result<Value>;
    async fn update(
        &mut self,
        collection: &str,
        filter: Option<Filter>,
        patch: Value,
    ) -> anyhow::Result<u64>;
    #[allow(dead_code)]
    async fn delete(&mut self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64>;

    // Optional: execute a storage-agnostic logical plan
    #[allow(dead_code)]
    async fn execute_plan(&mut self, _plan: &LogicalPlan) -> anyhow::Result<Vec<Value>> {
        Err(anyhow::anyhow!("execute_plan unsupported by this backend (TxStore)"))
    }

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
    #[allow(dead_code)]
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }

    // Query
    async fn query(&self, q: &Query) -> anyhow::Result<Vec<Value>>;

    // CRUD
    async fn insert(&self, collection: &str, doc: Value) -> anyhow::Result<Value>;
    #[allow(dead_code)]
    async fn update(
        &self,
        collection: &str,
        filter: Option<Filter>,
        patch: Value,
    ) -> anyhow::Result<u64>;
    #[allow(dead_code)]
    async fn delete(&self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64>;

    // Optional: execute a storage-agnostic logical plan
    #[allow(dead_code)]
    async fn execute_plan(&self, _plan: &LogicalPlan) -> anyhow::Result<Vec<Value>> {
        Err(anyhow::anyhow!("execute_plan unsupported by this backend"))
    }

    // Optional: transactions
    async fn begin_tx(&self) -> anyhow::Result<Box<dyn TxStore>> {
        Err(anyhow::anyhow!("transactions unsupported"))
    }

    // Legacy escape hatch for gradual migration
    #[allow(dead_code)]
    async fn raw_sql(
        &self,
        _sql: &str,
        _params: Vec<crate::database::state::DbParam>,
    ) -> anyhow::Result<Vec<Value>> {
        Err(anyhow::anyhow!("raw_sql unsupported by this backend"))
    }
}
