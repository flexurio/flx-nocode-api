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
use crate::storage::sql_store::SqlStore;

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
