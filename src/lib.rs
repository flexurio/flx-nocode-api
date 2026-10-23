//! Flexurio no-code API engine as a library.
//!
//! The binary (`src/main.rs`) only wires the process together; everything else
//! lives here so benchmarks (`benches/`) and integration tests (`tests/`) can
//! exercise the query compiler, planner and services directly.

pub mod audit;
pub mod auth;
pub mod cli;
pub mod config;
pub mod core;
pub mod crypt;
pub mod database;
pub mod error;
pub mod helpers;
pub mod log;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod metrics;
pub mod middleware;
pub mod model;
pub mod nocode;
pub mod rate_limit;
pub mod routes;
pub mod startup;
pub mod storage;

// Crate-root re-exports used throughout the modules as `crate::AppState` etc.
pub use config::ISDEBUG;
pub use database::state::{AppState, QueryConverter};
