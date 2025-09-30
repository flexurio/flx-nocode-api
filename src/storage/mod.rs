#![allow(unused_imports)]
//! Storage abstraction layer: backend-agnostic Query AST and Store traits.
//! This module is additive and unused by existing code paths (no breaking changes).

pub mod ast;
pub mod traits;
pub mod sql_store;
pub mod ddl;

// Convenience re-exports for consumers
pub mod prelude {
    pub use super::ast::{Filter, Query, Sort, Val};
    pub use super::traits::{BackendCapabilities, DataStore, TxStore};
    pub use super::ddl::{Ddl, CreateTable, ColumnDef, ColumnType, TableConstraint, DefaultExpr, ForeignAction};
}
