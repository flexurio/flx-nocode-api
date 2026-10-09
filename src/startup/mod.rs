//! Startup jobs extracted from `main()`.
//!
//! These functions are called once during server boot and are kept here to
//! keep `main.rs` focused on wiring rather than business logic.

use actix_web::web;
use anyhow::anyhow;

use crate::config::{CONFIG, SCHEMAS};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::model::DbType;
use crate::nocode::generate::{execute_generate_table, generate_table};
use crate::nocode::validate::{lint_raw_entity, lint_schemas};
use crate::storage::sql_store::SqlStore;

// ── Configuration lint ────────────────────────────────────────────────────────

/// `CONFIG_LINT` env: `strict` (default: errors abort startup), `warn`
/// (errors are logged but startup continues) or `off` (lint is skipped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LintMode {
    Strict,
    Warn,
    Off,
}

impl LintMode {
    pub fn from_env() -> Self {
        match std::env::var("CONFIG_LINT")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "0" | "false" | "none" => LintMode::Off,
            "warn" | "warning" | "lenient" => LintMode::Warn,
            _ => LintMode::Strict,
        }
    }
}

/// Run the fail-fast configuration linter over every loaded entity schema.
///
/// Warnings are logged as `WARN CONFIG-LINT`, errors as `ERROR CONFIG-LINT`.
/// In strict mode any error makes this return `Err`, so `main` can exit with
/// a non-zero status before the HTTP server binds.
pub fn run_config_lint() -> anyhow::Result<()> {
    let mode = LintMode::from_env();
    if mode == LintMode::Off {
        log_output("BOOT", "CONFIG-LINT", "mode", "off (CONFIG_LINT=off)".to_string(), false);
        return Ok(());
    }

    let mut report = lint_schemas(&SCHEMAS.0, &CONFIG.routes);
    let raw_entities = crate::config::take_raw_entities();
    let mut raw_routes: Vec<&String> = raw_entities.keys().collect();
    raw_routes.sort();
    for route in raw_routes {
        report.merge(lint_raw_entity(route, &raw_entities[route]));
    }

    for w in &report.warnings {
        let (route, msg) = split_route(w);
        log_output("WARN", "CONFIG-LINT", route, msg.to_string(), true);
    }
    for e in &report.errors {
        let (route, msg) = split_route(e);
        log_output("ERROR", "CONFIG-LINT", route, msg.to_string(), true);
    }

    let summary = format!(
        "{} route(s) checked: {} error(s), {} warning(s)",
        CONFIG.routes.len(),
        report.errors.len(),
        report.warnings.len()
    );

    if report.is_clean() {
        log_output("BOOT", "CONFIG-LINT", "ok", summary, false);
        return Ok(());
    }

    match mode {
        LintMode::Strict => {
            log_output(
                "ERROR",
                "CONFIG-LINT",
                "FAILED",
                format!("{} — fix the entity JSON or start with CONFIG_LINT=warn", summary),
                true,
            );
            Err(anyhow!("configuration lint failed: {}", summary))
        }
        LintMode::Warn => {
            log_output(
                "WARN",
                "CONFIG-LINT",
                "continuing",
                format!("{} (CONFIG_LINT=warn)", summary),
                true,
            );
            Ok(())
        }
        LintMode::Off => Ok(()),
    }
}

/// Lint messages are formatted `[route] message`; split for log columns.
fn split_route(line: &str) -> (&str, &str) {
    if let Some(rest) = line.strip_prefix('[') {
        if let Some((route, msg)) = rest.split_once("] ") {
            return (route, msg);
        }
    }
    ("-", line)
}

// ── Table generation ──────────────────────────────────────────────────────────

/// Iterate every configured route, generate the corresponding database table
/// when `auto_generate` is true, and validate the resulting schema.
///
/// Returns the first fatal error encountered, or `Ok(())` on success.
pub async fn run_table_generation(app_state: &web::Data<AppState>) -> anyhow::Result<()> {
    let ds = SqlStore::new(app_state.db.clone(), app_state.db_type.as_str().to_string());

    for route in CONFIG.routes.iter() {
        let schema_arc = match SCHEMAS.0.get(route) {
            Some(s) => s.clone(),
            None => {
                eprintln!("No schema found for route '{}'", route);
                return Err(anyhow!("No schema found for route '{}'", route));
            }
        };
        let schema = schema_arc.as_ref();

        // Auth-required tables follow `require_auth`; others follow their own flag.
        let should_generate = if schema.table == "flx_users" || schema.table == "flx_roles" {
            app_state.require_auth
        } else {
            schema.auto_generate
        };

        if !should_generate {
            continue;
        }

        // Apply default collation when the schema doesn't specify one (MySQL only).
        let mut schema_with_collate = schema.clone();
        if schema_with_collate.collate.trim().is_empty() && app_state.db_type == DbType::Mysql {
            schema_with_collate.collate = app_state.default_collate.clone();
        }

        let (sql_create_table, sql_create_index) = generate_table(&ds, &schema_with_collate);
        let (is_valid, msg) = execute_generate_table(
            route.to_string(),
            app_state,
            sql_create_table,
            sql_create_index,
        )
        .await;

        if !is_valid {
            log_output("ERROR", "TABLE DESIGN CHECK", "FAILED", msg.clone(), true);
            return Err(anyhow!(
                "Table design check failed for route '{}': {}",
                route,
                msg
            ));
        }

        log_output(
            "INFO",
            "TABLE DESIGN CHECK",
            "SUCCESS",
            route.to_string(),
            false,
        );
    }

    Ok(())
}

// ── Role seeding ──────────────────────────────────────────────────────────────

// /// Spawn a background task that seeds admin roles.
// ///
// /// Non-blocking — completes asynchronously after `main()` continues.
// pub async fn run_role_seeding(app_state: web::Data<AppState>, id_user_str: &str) {
//     let id_user: i64 = id_user_str.parse().unwrap_or(1);
//     let ds = SqlStore::new(app_state.db.clone(), app_state.db_type.as_str().to_string());
//     let routes_cl = CONFIG.routes.clone();

//     tokio::spawn(async move {
//         match generate_role_admin(&app_state, ds, id_user, routes_cl).await {
//             Ok(_) => log_output(
//                 "BOOT",
//                 "ROLE-SEED",
//                 "generate_role_admin",
//                 "completed".to_string(),
//                 true,
//             ),
//             Err(e) => log_output(
//                 "ERROR",
//                 "ROLE-SEED",
//                 "generate_role_admin",
//                 format!("{}", e),
//                 false,
//             ),
//         }
//     });
// }
