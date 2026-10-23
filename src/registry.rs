//! Live, hot-swappable view of the no-code configuration.
//!
//! Request handlers, the consumer and MCP never touch the boot-time statics
//! directly; they read the [`Registry`] through [`current`], which is an
//! `ArcSwap` load (a single atomic read, no lock). [`reload`] builds a new
//! registry from disk, validates it (configuration lint, DDL for new entities)
//! and swaps it in atomically. A failed reload leaves the running registry
//! untouched, so a bad edit can never take the API down.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::Value;

use crate::auth::Rule;
use crate::config::{load_config_from, LoadedConfig, CONFIG_LOCATION};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::model::{ReferenceForeignKey, TableSchema};

/// Immutable snapshot of one configuration generation.
pub struct Registry {
    pub generation: u64,
    pub loaded_at: chrono::DateTime<chrono::Utc>,
    /// `routes.json` order.
    pub routes: Vec<String>,
    pub route_publics: HashSet<String>,
    pub schemas: Arc<HashMap<String, Arc<TableSchema>>>,
    pub ref_foreign_keys: Arc<Vec<ReferenceForeignKey>>,
    /// `rules.json` as JSON (served by `/roles` and the MCP resource).
    pub rules_raw: Value,
    /// `rules.json` parsed into access rules.
    pub rules: Arc<Vec<Rule>>,
    /// Per-entity file fingerprints (change detection on reload).
    pub fingerprints: HashMap<String, u64>,
}

impl Registry {
    fn from_loaded(loaded: &LoadedConfig, generation: u64) -> Self {
        Registry {
            generation,
            loaded_at: chrono::Utc::now(),
            routes: loaded.config.routes.clone(),
            route_publics: loaded.config.route_publics.clone(),
            schemas: Arc::new(loaded.schemas.clone()),
            ref_foreign_keys: Arc::new(loaded.ref_foreign_keys.clone()),
            rules_raw: loaded.rules.clone(),
            rules: Arc::new(crate::auth::parse_rules_value(&loaded.rules)),
            fingerprints: loaded.fingerprints.clone(),
        }
    }

    pub fn schema(&self, route: &str) -> Option<Arc<TableSchema>> {
        self.schemas.get(route).cloned()
    }

    pub fn is_public(&self, route: &str) -> bool {
        self.route_publics.contains(route)
    }
}

static REGISTRY: Lazy<ArcSwap<Registry>> = Lazy::new(|| {
    ArcSwap::from_pointee(Registry::from_loaded(crate::config::boot_config(), 1))
});

/// The registry in service right now. Cheap (one atomic load); hold the
/// guard only for the duration of a lookup.
pub fn current() -> arc_swap::Guard<Arc<Registry>> {
    REGISTRY.load()
}

/// Owned handle to the current registry (for background tasks).
pub fn snapshot() -> Arc<Registry> {
    REGISTRY.load_full()
}

/// Schema of `route` from the live registry.
pub fn schema(route: &str) -> Option<Arc<TableSchema>> {
    REGISTRY.load().schema(route)
}

/// Is `route` listed in `route_publics` of the live registry?
pub fn is_public(route: &str) -> bool {
    REGISTRY.load().is_public(route)
}

/// Result of a reload, returned to the caller (`POST /admin/reload`) and logged.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ReloadReport {
    pub generation: u64,
    pub routes: usize,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
    pub tables_generated: Vec<String>,
    pub lint_warnings: Vec<String>,
    pub elapsed_ms: u128,
}

/// Re-read the configuration directory and, if it is valid, swap it in.
///
/// Steps: load (no panics) → configuration lint (strict unless
/// `CONFIG_LINT=warn|off`) → `CREATE TABLE IF NOT EXISTS` + indexes for new
/// or changed entities with `auto_generate` → atomic swap → bump the cache
/// version of every route so no stale page survives the schema change.
///
/// Existing columns are never altered (that stays an explicit migration).
pub async fn reload(state: &AppState) -> Result<ReloadReport, String> {
    let started = std::time::Instant::now();
    let dir = CONFIG_LOCATION.as_str();
    let loaded = load_config_from(dir, state.db_type.as_str())?;

    // ── Lint ─────────────────────────────────────────────────────────────
    let mode = crate::startup::LintMode::from_env();
    let mut lint_warnings = Vec::new();
    if mode != crate::startup::LintMode::Off {
        let mut report = crate::nocode::validate::lint_schemas(&loaded.schemas, &loaded.config.routes);
        let mut raw_routes: Vec<&String> = loaded.raw.keys().collect();
        raw_routes.sort();
        for route in raw_routes {
            report.merge(crate::nocode::validate::lint_raw_entity(route, &loaded.raw[route]));
        }
        lint_warnings = report.warnings.clone();
        if !report.errors.is_empty() && mode == crate::startup::LintMode::Strict {
            return Err(format!(
                "configuration lint failed ({} error(s)): {}",
                report.errors.len(),
                report.errors.join(" | ")
            ));
        }
    }

    // ── Duplicate table guard (same rule as boot) ────────────────────────
    let mut seen: HashSet<&str> = HashSet::new();
    for (route, s) in loaded.schemas.iter() {
        if !s.table.is_empty() && !seen.insert(s.table.as_str()) {
            return Err(format!("table '{}' (route '{}') is declared more than once", s.table, route));
        }
    }

    // ── Diff against the running registry ───────────────────────────────
    let old = snapshot();
    let old_set: HashSet<&String> = old.routes.iter().collect();
    let new_set: HashSet<&String> = loaded.config.routes.iter().collect();
    let added: Vec<String> = loaded.config.routes.iter().filter(|r| !old_set.contains(r)).cloned().collect();
    let removed: Vec<String> = old.routes.iter().filter(|r| !new_set.contains(r)).cloned().collect();
    let changed: Vec<String> = loaded
        .config
        .routes
        .iter()
        .filter(|r| old_set.contains(r) && old.fingerprints.get(*r) != loaded.fingerprints.get(*r))
        .cloned()
        .collect();

    // ── DDL for new / changed entities (idempotent CREATE IF NOT EXISTS) ──
    let mut tables_generated = Vec::new();
    if state.db_type != crate::model::DbType::Mongodb {
        let ds = crate::storage::sql_store::SqlStore::new(state.db.clone(), state.db_type.as_str().to_string());
        for route in added.iter().chain(changed.iter()) {
            let Some(schema) = loaded.schemas.get(route) else { continue };
            let should_generate = if schema.table == "flx_users" || schema.table == "flx_roles" {
                state.require_auth
            } else {
                schema.auto_generate
            };
            if !should_generate {
                continue;
            }
            let mut with_collate = schema.as_ref().clone();
            if with_collate.collate.trim().is_empty() && state.db_type == crate::model::DbType::Mysql {
                with_collate.collate = state.default_collate.clone();
            }
            let (sql_table, sql_indexes) = crate::nocode::generate::generate_table(&ds, &with_collate);
            let (ok, msg) =
                crate::nocode::generate::execute_generate_table(route.clone(), state, sql_table, sql_indexes).await;
            if !ok {
                return Err(format!("table generation failed for '{}': {}", route, msg));
            }
            tables_generated.push(route.clone());
        }
    }

    // ── Swap ─────────────────────────────────────────────────────────────
    let generation = old.generation + 1;
    let fresh = Arc::new(Registry::from_loaded(&loaded, generation));
    REGISTRY.store(fresh.clone());

    // Every cached page may now be shaped differently: drop them all (O(1) per route).
    for route in old.routes.iter().chain(fresh.routes.iter()) {
        crate::database::redis::bump_route_version(route, state.is_cachedb).await;
    }

    let report = ReloadReport {
        generation,
        routes: fresh.routes.len(),
        added,
        removed,
        changed,
        tables_generated,
        lint_warnings,
        elapsed_ms: started.elapsed().as_millis(),
    };
    log_output(
        "INFO",
        "CONFIG-RELOAD",
        "registry",
        format!(
            "generation={} routes={} added={:?} removed={:?} changed={:?} tables={:?} warnings={} in {} ms",
            report.generation,
            report.routes,
            report.added,
            report.removed,
            report.changed,
            report.tables_generated,
            report.lint_warnings.len(),
            report.elapsed_ms
        ),
        true,
    );
    Ok(report)
}

/// Reload on `SIGHUP` (unix). Spawned once from `main`.
#[cfg(unix)]
pub fn spawn_sighup_listener(state: actix_web::web::Data<AppState>) {
    tokio::spawn(async move {
        let mut sig = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                log_output("WARN", "CONFIG-RELOAD", "SIGHUP", format!("cannot listen: {}", e), false);
                return;
            }
        };
        while sig.recv().await.is_some() {
            match reload(&state).await {
                Ok(_) => {}
                Err(e) => log_output("ERROR", "CONFIG-RELOAD", "SIGHUP", e, true),
            }
        }
    });
}

#[cfg(not(unix))]
pub fn spawn_sighup_listener(_state: actix_web::web::Data<AppState>) {}
