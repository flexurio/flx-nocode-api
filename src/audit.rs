use once_cell::sync::Lazy;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[derive(Serialize)]
pub struct AuditEntry<'a> {
    pub at: String,
    pub actor_id: String,
    pub action: &'a str,
    pub route: &'a str,
    pub id: Option<&'a str>,
    pub ip: Option<&'a str>,
}

enum AuditMsg {
    Line(String),
    /// Flush buffered lines to disk and reply when done (used at shutdown).
    Flush(oneshot::Sender<()>),
}

static AUDIT_CHANNEL: Lazy<Sender<AuditMsg>> = Lazy::new(|| {
    let (tx, mut rx) = channel::<AuditMsg>(65536);

    // Spawn background dedicated writer task
    tokio::spawn(async move {
        let loc = std::env::var("LOC_AUDIT").unwrap_or_else(|_| "audit".to_string());
        let path = std::path::Path::new(&loc);
        if let Some(parent) = path.parent().filter(|p| !p.exists()) {
            let _ = std::fs::create_dir_all(parent);
        }
        let file_path = if path.is_dir() {
            path.join("events.log")
        } else {
            path.to_path_buf()
        };

        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open audit log file: {}", e);
                // Keep draining so senders never block; acknowledge flushes.
                while let Some(msg) = rx.recv().await {
                    if let AuditMsg::Flush(done) = msg {
                        let _ = done.send(());
                    }
                }
                return;
            }
        };

        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        let mut count = 0;

        while let Some(msg) = rx.recv().await {
            match msg {
                AuditMsg::Line(line) => {
                    let _ = writeln!(writer, "{}", line);
                    count += 1;
                    // Flush after every 100 messages or if channel is currently empty
                    if count >= 100 || rx.is_empty() {
                        let _ = writer.flush();
                        count = 0;
                    }
                }
                AuditMsg::Flush(done) => {
                    let _ = writer.flush();
                    count = 0;
                    let _ = done.send(());
                }
            }
        }
        let _ = writer.flush();
    });

    tx
});

pub fn write_audit(entry: &AuditEntry<'_>) {
    if let Ok(json) = serde_json::to_string(entry) {
        let _ = AUDIT_CHANNEL.try_send(AuditMsg::Line(json));
    }
}

/// Flush every buffered audit line to disk. Resolves once the writer confirms
/// (or immediately if the writer task is gone).
pub async fn flush() {
    let (tx, rx) = oneshot::channel();
    if AUDIT_CHANNEL.send(AuditMsg::Flush(tx)).await.is_ok() {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    }
}
