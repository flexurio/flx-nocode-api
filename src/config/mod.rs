//! Application configuration — all `static Lazy` globals live here.
//!
//! Keeping statics in one place avoids scattering them across `main.rs`
//! and makes their initialization order easy to reason about.

use crate::auth::ClaimsConverter;
use crate::model::{ReferenceForeignKey, ReferenceForeignKeyAction, TableSchema};
use colored::Colorize;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

// ── Consolidated config location ─────────────────────────────────────────────

/// Config directory path, read once from `LOC_CONFIG` env-var.
pub static CONFIG_LOCATION: Lazy<String> = Lazy::new(|| {
    env::var("LOC_CONFIG").unwrap_or_else(|_| {
        eprintln!("Warning: LOC_CONFIG not set, using default 'config'");
        "config".to_string()
    })
});

// ── routes.json ──────────────────────────────────────────────────────────────

/// Parsed `routes.json`, loaded exactly once at startup.
pub static CONFIG: Lazy<crate::model::Config> = Lazy::new(|| {
    let file_path = format!("{}/routes.json", CONFIG_LOCATION.as_str());

    let mut content = match fs::read_to_string(&file_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "ERROR 9081231287 : Can't read file {} - {}",
                file_path.on_bright_red(),
                e
            );
            return crate::model::Config {
                routes: vec![],
                route_publics: vec![],
                converter_token: ClaimsConverter::default(),
            };
        }
    };

    // Back-fill missing `converter_token` key for older config files.
    if !content.contains("converter_token") {
        content = content.replace(
            "}",
            r#", "converter_token": {"id":"id","nm":"nm","exp":"exp","at":"at","rl":"rl","cs":"cs"} }"#,
        );
    }

    match serde_json::from_str(&content) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!(
                "ERROR main 75 : Sorry, content of /{}/routes.json is not valid JSON, \
                 with ERROR Message : {}",
                CONFIG_LOCATION.as_str(),
                e
            );
            panic!("Invalid routes.json");
        }
    }
});

// ── Foreign-key action allow-list ────────────────────────────────────────────

pub static FOREIGNKEY_ACTION: [&str; 4] = ["cascade", "set null", "restrict", "no action"];

// ── Entity schemas ────────────────────────────────────────────────────────────

/// All entity schemas and FK references, loaded exactly once at startup.
///
/// Returns a tuple:
/// * `Arc<HashMap<route, Arc<TableSchema>>>` — schemas keyed by route name
/// * `Arc<Vec<ReferenceForeignKey>>`         — flattened FK reference list
#[allow(clippy::type_complexity)]
pub static SCHEMAS: Lazy<(
    Arc<HashMap<String, Arc<TableSchema>>>,
    Arc<Vec<ReferenceForeignKey>>,
)> = Lazy::new(|| {
    let config_dir = format!("{}/entity", CONFIG_LOCATION.as_str());
    let mut schemas_map: HashMap<String, Arc<TableSchema>> =
        HashMap::with_capacity(CONFIG.routes.len());
    let mut ref_foreign_keys: Vec<ReferenceForeignKey> = Vec::with_capacity(CONFIG.routes.len());

    // DB type used for early TRACE upsert validation
    let dbt_raw = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());
    let dbt = dbt_raw.to_ascii_lowercase();

    for route in CONFIG.routes.iter() {
        let file_path = format!("{}/{}.json", config_dir, route);

        let content = match fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "ERROR 908ihu76 : Can't read file {} - {}",
                    file_path.on_bright_red(),
                    e
                );
                panic!("Cannot read entity file");
            }
        };

        let schema: TableSchema = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "Sorry, content of /{}/entity/{}.json is not valid JSON, \
                     with ERROR Message : {}",
                    CONFIG_LOCATION.as_str(),
                    route,
                    e
                );
                panic!("Invalid entity JSON");
            }
        };

        // ── Early TRACE upsert validation (Postgres / MSSQL) ─────────────
        let trace_active =
            !schema.trace.insert_into.is_empty() || !schema.trace.column_selects.is_empty();
        if trace_active && (dbt == "postgres" || dbt == "mssql") {
            let resolved_conflict_cols = resolve_conflict_cols(&schema);

            if dbt == "postgres" && resolved_conflict_cols.is_empty() {
                eprintln!(
                    "TRACE config error for table '{}': Postgres requires \
                     'column_conflicts' (or index:<name>) for upsert",
                    schema.table
                );
                panic!("Trace config error: Postgres requires column_conflicts");
            } else if dbt == "mssql" && resolved_conflict_cols.is_empty() {
                let unique_indexes: Vec<_> =
                    schema.indexes.iter().filter(|ix| ix.unique).collect();
                if unique_indexes.len() != 1 {
                    eprintln!(
                        "TRACE config error for table '{}': MSSQL requires \
                         'column_conflicts' (or index:<name>); no unambiguous unique index found",
                        schema.table
                    );
                    panic!("Trace config error: MSSQL ambiguous unique index");
                }
                // else: single unique index — runtime will use it
            }
        }

        // ── Validate and collect foreign-key references ───────────────────
        for fk in schema.foreign_keys.iter() {
            if !FOREIGNKEY_ACTION.contains(&fk.on_delete.as_str()) {
                eprintln!(
                    "ERROR FK_Check Delete : Foreign key on_delete action '{}' is not supported",
                    fk.on_delete
                );
                panic!("Unsupported FK on_delete action");
            }
            if !FOREIGNKEY_ACTION.contains(&fk.on_update.as_str()) {
                eprintln!(
                    "ERROR FK_Check Update : Foreign key on_update action '{}' is not supported",
                    fk.on_update
                );
                panic!("Unsupported FK on_update action");
            }
            ref_foreign_keys.push(ReferenceForeignKey {
                table:  fk.reference_table.clone(),
                column: fk.reference_column.clone(),
                on_delete_action: ReferenceForeignKeyAction {
                    table:       schema.table.clone(),
                    column:      fk.column.clone(),
                    action:      fk.on_delete.clone(),
                    type_delete: schema.del.type_delete.clone(),
                },
                on_update_action: ReferenceForeignKeyAction {
                    table:       schema.table.clone(),
                    column:      fk.column.clone(),
                    action:      fk.on_update.clone(),
                    type_delete: "soft".to_string(),
                },
            });
        }

        schemas_map.insert(route.clone(), Arc::new(schema));
    }

    crate::log::log_output(
        "INFO",
        "FOREIGN KEY",
        "ref_foreign_keys",
        format!("{:?}", ref_foreign_keys),
        true,
    );

    schemas_map.shrink_to_fit();
    (Arc::new(schemas_map), Arc::new(ref_foreign_keys))
});

// ── Debug flag ────────────────────────────────────────────────────────────────

pub(crate) static ISDEBUG: Lazy<bool> = Lazy::new(|| match env::var("DEBUG") {
    Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "yes"),
    Err(_) => false,
});

// ── One-shot endpoint log guard ───────────────────────────────────────────────

/// Set to `true` after the first worker has logged all endpoint URLs.
/// Prevents duplicate log lines when Actix spawns multiple workers.
pub static ENDPOINT_LOG_ONCE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Resolve TRACE `column_conflicts`, expanding `index:<name>` references.
fn resolve_conflict_cols(schema: &TableSchema) -> Vec<String> {
    if schema.trace.column_conflicts.is_empty() {
        return vec![];
    }

    if let Some(idx_spec) = schema
        .trace
        .column_conflicts
        .iter()
        .find(|s| s.to_lowercase().starts_with("index:"))
    {
        let name = idx_spec
            .split_once(':')
            .map(|(_, n)| n.trim())
            .unwrap_or("");
        if let Some(ix) = schema
            .indexes
            .iter()
            .find(|ix| ix.name.eq_ignore_ascii_case(name))
        {
            return ix.columns.clone();
        } else {
            eprintln!(
                "TRACE config error for table '{}': referenced index '{}' not found in indexes",
                schema.table, name
            );
            panic!("Trace config error: index not found");
        }
    }

    schema.trace.column_conflicts.clone()
}
