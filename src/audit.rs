use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;

#[derive(Serialize)]
pub struct AuditEntry<'a> {
    pub at: String,
    pub actor_id: String,
    pub action: &'a str,
    pub route: &'a str,
    pub id: Option<&'a str>,
    pub ip: Option<&'a str>,
}

pub fn write_audit(entry: &AuditEntry<'_>) {
    let loc = std::env::var("LOC_AUDIT").unwrap_or_else(|_| "audit".to_string());
    let path = std::path::Path::new(&loc);
    if let Some(parent) = path.parent()
        && !parent.exists() {
            let _ = std::fs::create_dir_all(parent);
        }
    let file_path = if path.is_dir() {
        path.join("events.log")
    } else {
        path.to_path_buf()
    };
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        && let Ok(json) = serde_json::to_string(entry) {
            let _ = writeln!(f, "{}", json);
        }
}
