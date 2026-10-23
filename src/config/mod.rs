//! Application configuration.
//!
//! * [`load_config_from`] reads `routes.json` + every entity file from a config
//!   directory and returns a [`LoadedConfig`] **without panicking**. It is used
//!   both at boot and by the hot-reload path (`crate::registry::reload`).
//! * The `static Lazy` globals (`CONFIG`, `SCHEMAS`, …) hold the configuration
//!   loaded at boot. Request-time code must go through `crate::registry`, which
//!   starts from these values and is swapped atomically on reload.

use crate::model::{ReferenceForeignKey, ReferenceForeignKeyAction, TableSchema};
use colored::Colorize;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

// ── Consolidated config location ─────────────────────────────────────────────

/// Config directory path, read once from `LOC_CONFIG` env-var.
pub static CONFIG_LOCATION: Lazy<String> = Lazy::new(|| {
    env::var("LOC_CONFIG").unwrap_or_else(|_| {
        eprintln!("Warning: LOC_CONFIG not set, using default 'config'");
        "config".to_string()
    })
});

// ── Seed directory location ───────────────────────────────────────────────────

/// Seed directory path, read once from `LOC_SEED` env-var (defaults to "seed" if empty or unset).
pub static SEED_LOCATION: Lazy<String> = Lazy::new(|| match env::var("LOC_SEED") {
    Ok(val) if !val.trim().is_empty() => val.trim().to_string(),
    _ => "seed".to_string(),
});

// ── Foreign-key action allow-list ────────────────────────────────────────────

pub static FOREIGNKEY_ACTION: [&str; 4] = ["cascade", "set null", "restrict", "no action"];

/// Everything `load_config_from` produces for one config directory.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: crate::model::Config,
    pub schemas: HashMap<String, Arc<TableSchema>>,
    pub ref_foreign_keys: Vec<ReferenceForeignKey>,
    /// Raw entity JSON per route (for the unknown-key linter).
    pub raw: HashMap<String, serde_json::Value>,
    /// Stable fingerprint of each entity file, used by the reloader to detect
    /// which entities actually changed.
    pub fingerprints: HashMap<String, u64>,
    /// `rules.json` as loaded (empty object when missing/invalid).
    pub rules: serde_json::Value,
}

impl Default for LoadedConfig {
    fn default() -> Self {
        LoadedConfig {
            config: crate::model::Config {
                routes: vec![],
                route_publics: std::collections::HashSet::new(),
                converter_token: crate::auth::ClaimsConverter::default(),
            },
            schemas: HashMap::new(),
            ref_foreign_keys: vec![],
            raw: HashMap::new(),
            fingerprints: HashMap::new(),
            rules: serde_json::json!({}),
        }
    }
}

fn fingerprint(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

/// Parse `routes.json` content (with the legacy `converter_token` back-fill).
pub fn parse_routes_json(mut content: String) -> Result<crate::model::Config, String> {
    if !content.contains("converter_token") {
        content = content.replace(
            "}",
            r#", "converter_token": {"id":"id","nm":"nm","exp":"exp","at":"at","rl":"rl","cs":"cs"} }"#,
        );
    }
    serde_json::from_str(&content).map_err(|e| format!("routes.json is not valid JSON: {}", e))
}

/// Read `routes.json`, every `entity/<route>.json` and `rules.json` from `dir`.
///
/// Performs the same structural validation the boot path always did (TRACE
/// upsert prerequisites per dialect, FK action allow-list) but reports problems
/// as `Err(message)` instead of panicking, so a bad edit can be rejected by the
/// reloader while the running configuration stays in service.
pub fn load_config_from(dir: &str, db_type: &str) -> Result<LoadedConfig, String> {
    let routes_path = format!("{}/routes.json", dir);
    let content = fs::read_to_string(&routes_path)
        .map_err(|e| format!("ERROR 9081231287 : Can't read file {} - {}", routes_path, e))?;
    let config = parse_routes_json(content)?;

    let dbt = db_type.to_ascii_lowercase();
    let config_dir = format!("{}/entity", dir);
    let mut schemas: HashMap<String, Arc<TableSchema>> = HashMap::with_capacity(config.routes.len());
    let mut ref_foreign_keys: Vec<ReferenceForeignKey> = Vec::with_capacity(config.routes.len());
    let mut raw_map: HashMap<String, serde_json::Value> = HashMap::with_capacity(config.routes.len());
    let mut fingerprints: HashMap<String, u64> = HashMap::with_capacity(config.routes.len());

    for route in config.routes.iter() {
        let file_path = format!("{}/{}.json", config_dir, route);
        let content = fs::read_to_string(&file_path)
            .map_err(|e| format!("ERROR 908ihu76 : Can't read file {} - {}", file_path, e))?;
        let raw: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| format!("entity/{}.json is not valid JSON: {}", route, e))?;
        let schema: TableSchema = serde_json::from_value(raw.clone())
            .map_err(|e| format!("entity/{}.json is not a valid entity schema: {}", route, e))?;

        // ── Early TRACE upsert validation (Postgres / MSSQL) ─────────────
        let trace_active =
            !schema.trace.insert_into.is_empty() || !schema.trace.column_selects.is_empty();
        if trace_active && (dbt == "postgres" || dbt == "mssql") {
            let resolved_conflict_cols = resolve_conflict_cols(&schema)?;
            if dbt == "postgres" && resolved_conflict_cols.is_empty() {
                return Err(format!(
                    "TRACE config error for table '{}': Postgres requires 'column_conflicts' (or index:<name>) for upsert",
                    schema.table
                ));
            } else if dbt == "mssql" && resolved_conflict_cols.is_empty() {
                let unique_indexes: Vec<_> = schema.indexes.iter().filter(|ix| ix.unique).collect();
                if unique_indexes.len() != 1 {
                    return Err(format!(
                        "TRACE config error for table '{}': MSSQL requires 'column_conflicts' (or index:<name>); no unambiguous unique index found",
                        schema.table
                    ));
                }
            }
        }

        // ── Validate and collect foreign-key references ───────────────────
        for fk in schema.foreign_keys.iter() {
            if !FOREIGNKEY_ACTION.contains(&fk.on_delete.as_str()) {
                return Err(format!(
                    "ERROR FK_Check Delete : Foreign key on_delete action '{}' is not supported (entity {})",
                    fk.on_delete, route
                ));
            }
            if !FOREIGNKEY_ACTION.contains(&fk.on_update.as_str()) {
                return Err(format!(
                    "ERROR FK_Check Update : Foreign key on_update action '{}' is not supported (entity {})",
                    fk.on_update, route
                ));
            }
            ref_foreign_keys.push(ReferenceForeignKey {
                table: fk.reference_table.clone(),
                column: fk.reference_column.clone(),
                on_delete_action: ReferenceForeignKeyAction {
                    table: schema.table.clone(),
                    column: fk.column.clone(),
                    action: fk.on_delete.clone(),
                    type_delete: schema.del.type_delete.clone(),
                },
                on_update_action: ReferenceForeignKeyAction {
                    table: schema.table.clone(),
                    column: fk.column.clone(),
                    action: fk.on_update.clone(),
                    type_delete: "soft".to_string(),
                },
            });
        }

        fingerprints.insert(route.clone(), fingerprint(&content));
        raw_map.insert(route.clone(), raw);
        schemas.insert(route.clone(), Arc::new(schema));
    }

    let rules_path = format!("{}/rules.json", dir);
    let rules = match fs::read_to_string(&rules_path) {
        Ok(c) => serde_json::from_str(&c).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };

    schemas.shrink_to_fit();
    Ok(LoadedConfig { config, schemas, ref_foreign_keys, raw: raw_map, fingerprints, rules })
}

/// Configuration loaded at boot. A failure here is fatal (the process cannot
/// serve anything), so it panics with the same messages as before.
static BOOT_CONFIG: Lazy<LoadedConfig> = Lazy::new(|| {
    let dbt = env::var("DB_TYPE").unwrap_or_else(|_| "mysql".to_string());
    match load_config_from(CONFIG_LOCATION.as_str(), &dbt) {
        Ok(loaded) => {
            if let Ok(mut raw_map) = RAW_ENTITIES.lock() {
                *raw_map = loaded.raw.clone();
            }
            crate::log::log_output(
                "INFO",
                "FOREIGN KEY",
                "ref_foreign_keys",
                format!("{:?}", loaded.ref_foreign_keys),
                true,
            );
            loaded
        }
        Err(e) => {
            // routes.json unreadable used to yield an empty config (server
            // then prints "ROUTES NOT VALID"); keep that behaviour.
            if e.starts_with("ERROR 9081231287") {
                eprintln!("{}", e.on_bright_red());
                return LoadedConfig::default();
            }
            eprintln!("{}", e.on_bright_red());
            panic!("Invalid configuration: {}", e);
        }
    }
});

// ── routes.json ──────────────────────────────────────────────────────────────

/// Parsed `routes.json`, loaded exactly once at startup.
pub static CONFIG: Lazy<crate::model::Config> = Lazy::new(|| BOOT_CONFIG.config.clone());

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
    (
        Arc::new(BOOT_CONFIG.schemas.clone()),
        Arc::new(BOOT_CONFIG.ref_foreign_keys.clone()),
    )
});

/// Boot-time snapshot used to seed the live registry.
pub fn boot_config() -> &'static LoadedConfig {
    &BOOT_CONFIG
}

// ── Raw entity JSON (for the startup linter only) ────────────────────────────

/// Raw `serde_json::Value` of every entity file, keyed by route. Filled while
/// the boot config loads; `take_raw_entities()` drains it after the startup
/// lint so the duplicate copy does not stay resident.
pub static RAW_ENTITIES: Lazy<Mutex<HashMap<String, serde_json::Value>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Drain the raw entity JSON captured during boot.
pub fn take_raw_entities() -> HashMap<String, serde_json::Value> {
    let _ = &*SCHEMAS; // make sure the capture has happened
    RAW_ENTITIES
        .lock()
        .map(|mut m| std::mem::take(&mut *m))
        .unwrap_or_default()
}

// ── Debug flag ────────────────────────────────────────────────────────────────

pub static ISDEBUG: Lazy<bool> = Lazy::new(|| match env::var("DEBUG") {
    Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "yes"),
    Err(_) => false,
});

// ── One-shot endpoint log guard ───────────────────────────────────────────────

/// Set to `true` after the first worker has logged all endpoint URLs.
/// Prevents duplicate log lines when Actix spawns multiple workers.
pub static ENDPOINT_LOG_ONCE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Resolve TRACE `column_conflicts`, expanding `index:<name>` references.
fn resolve_conflict_cols(schema: &TableSchema) -> Result<Vec<String>, String> {
    if schema.trace.column_conflicts.is_empty() {
        return Ok(vec![]);
    }

    if let Some(idx_spec) = schema
        .trace
        .column_conflicts
        .iter()
        .find(|s| s.to_lowercase().starts_with("index:"))
    {
        let name = idx_spec.split_once(':').map(|(_, n)| n.trim()).unwrap_or("");
        return match schema.indexes.iter().find(|ix| ix.name.eq_ignore_ascii_case(name)) {
            Some(ix) => Ok(ix.columns.clone()),
            None => Err(format!(
                "TRACE config error for table '{}': referenced index '{}' not found in indexes",
                schema.table, name
            )),
        };
    }

    Ok(schema.trace.column_conflicts.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config_reports_errors_instead_of_panicking() {
        let dir = std::env::temp_dir().join(format!("flx-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("entity")).unwrap();
        // routes.json present, entity missing -> Err, not panic
        std::fs::write(dir.join("routes.json"), r#"{"routes":["ghost"]}"#).unwrap();
        let err = load_config_from(dir.to_str().unwrap(), "sqlite").unwrap_err();
        assert!(err.contains("ghost"));
        // valid minimal entity -> Ok with fingerprint
        std::fs::write(dir.join("entity/ghost.json"), r#"{"table":"ghost","columns":[{"name":"id","type_data":"int"}]}"#).unwrap();
        let ok = load_config_from(dir.to_str().unwrap(), "sqlite").unwrap();
        assert_eq!(ok.schemas.len(), 1);
        assert!(ok.fingerprints.contains_key("ghost"));
        assert_eq!(ok.config.converter_token, crate::auth::ClaimsConverter::default(), "converter_token back-filled");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
