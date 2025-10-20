// Non-blocking logger: queue log messages to a background thread to avoid
// synchronous stdout I/O on the request path.

use crate::ISDEBUG;
use colored::Colorize;
use once_cell::sync::Lazy;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;

// Internal message representation for the logger thread
struct LogMsg {
    tipe: String,
    title: String,
    subtitle: String,
    body: String,
    print_datetime: bool,
    is_query: bool,
}

// Bounded channel to prevent unbounded memory growth and avoid blocking
// the caller; when full, we will drop messages.
static LOGGER_TX: Lazy<Option<SyncSender<LogMsg>>> = Lazy::new(|| {
    // If debug is disabled, we don't even spin up the logger queue
    if !*ISDEBUG {
        return None;
    }

    // Buffer size can be tuned through env, default 2048
    let cap: usize = std::env::var("LOG_QUEUE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2048);
    let (tx, rx) = sync_channel::<LogMsg>(cap);

    // Spawn a single background writer thread with buffered stdout
    thread::Builder::new()
        .name("flx-logger".to_string())
        .spawn(move || {
            let stdout = std::io::stdout();
            let mut out = BufWriter::new(stdout.lock());

            // Drain until sender is dropped; this thread ends when process exits
            while let Ok(msg) = rx.recv() {
                // Level filtering and sampling to reduce overhead
                let level = classify_level(&msg.tipe);
                if level > *LOG_MIN_LEVEL {
                    continue; // drop lower-priority messages
                }
                if (level >= LEVEL_DEBUG) && *LOG_SAMPLE_DEBUG_N > 1 {
                    let n = LOG_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % (*LOG_SAMPLE_DEBUG_N as u64) != 0 {
                        continue; // sample debug logs
                    }
                }

                // Minimal formatting in background to keep request path cheaper
                let mut subtitle = msg.subtitle;
                if !msg.tipe.contains("QUERY") && subtitle.len() < 6 {
                    let diff = 6 - subtitle.len();
                    subtitle.push_str(&" ".repeat(diff));
                }
                let s_datetime = if msg.print_datetime {
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
                } else {
                    String::new()
                };

                // Header line (with optional color)
                let header = if *LOG_ENABLE_COLOR {
                    format!(
                        "# {}, {} {} at {}: ",
                        msg.tipe.yellow(),
                        msg.title.cyan(),
                        subtitle.blue(),
                        s_datetime
                    )
                } else {
                    format!(
                        "# {}, {} {} at {}: ",
                        msg.tipe,
                        msg.title,
                        subtitle,
                        s_datetime
                    )
                };
                let _ = write!(out, "{}", header);
                if msg.is_query {
                    let _ = writeln!(out, "\n");
                }
                // Body on its own line; avoid extra allocations
                let mut body = msg.body;
                // Truncate large bodies to avoid excessive I/O
                let max_bytes = *LOG_MAX_BODY_BYTES as usize;
                if max_bytes > 0 && body.len() > max_bytes {
                    let omitted = body.len() - max_bytes;
                    body.truncate(max_bytes);
                    body.push_str(&format!(" …(+{} bytes)", omitted));
                }
                if *LOG_ENABLE_COLOR {
                    let body_colored = body.green();
                    let _ = writeln!(out, "{}", body_colored);
                } else {
                    let _ = writeln!(out, "{}", body);
                }
                if msg.is_query {
                    let _ = writeln!(out, "\n");
                }

                // Periodically flush; BufWriter will batch writes, flushing here is fine
                let _ = out.flush();
            }
        })
        .ok()?;

    Some(tx)
});

/// Initialize the non-blocking logger explicitly (optional). Safe to call multiple times.
pub fn init_logger() {
    let _ = &*LOGGER_TX; // force Lazy init
}

/// Public logging function kept API-compatible; internally queues to background logger.
/// If DEBUG is off, this is a fast no-op aside from argument evaluation by the caller.
pub fn log_output(tipe: &str, title: &str, ssubtitle: &str, body: String, print_datetime: bool) {
    // Fast path: drop early when debug disabled (still pays for body formatting by caller)
    if !*ISDEBUG {
        return;
    }

    if let Some(tx) = &*LOGGER_TX {
        let msg = LogMsg {
            tipe: tipe.to_string(),
            title: title.to_string(),
            subtitle: ssubtitle.to_string(),
            body,
            print_datetime,
            is_query: tipe.contains("QUERY"),
        };
        // Non-blocking send; drop message if the queue is full
        let _ = tx.try_send(msg);
    }
}

// ---------------------- Log controls ----------------------
// Levels: lower number = higher priority
const LEVEL_ERROR: u8 = 0;
const LEVEL_WARN: u8 = 1;
const LEVEL_INFO: u8 = 2;
const LEVEL_DEBUG: u8 = 3;
const LEVEL_TRACE: u8 = 4;

// Minimum level to emit (inclusive). Defaults to INFO for safety
static LOG_MIN_LEVEL: Lazy<u8> = Lazy::new(|| match std::env::var("LOG_MIN_LEVEL") {
    Ok(s) => match s.to_lowercase().as_str() {
        "error" => LEVEL_ERROR,
        "warn" | "warning" => LEVEL_WARN,
        "info" => LEVEL_INFO,
        "debug" => LEVEL_DEBUG,
        "trace" => LEVEL_TRACE,
        _ => LEVEL_INFO,
    },
    Err(_) => LEVEL_INFO,
});

// Maximum bytes of body to print (0 = unlimited). Default 8192
static LOG_MAX_BODY_BYTES: Lazy<u32> = Lazy::new(|| {
    std::env::var("LOG_MAX_BODY_BYTES").ok().and_then(|s| s.parse().ok()).unwrap_or(8192)
});

// Sample every N debug/trace logs (1 = no sampling). Default 1
static LOG_SAMPLE_DEBUG_N: Lazy<u32> = Lazy::new(|| {
    std::env::var("LOG_SAMPLE_DEBUG_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1)
});

// Sequence counter for sampling
static LOG_SEQ: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

fn classify_level(tipe: &str) -> u8 {
    let t = tipe.to_ascii_uppercase();
    match t.as_str() {
        "ERROR" | "ERROR QUERY" | "ERROR-QUERY" => LEVEL_ERROR,
        "WARN" | "WARNING" => LEVEL_WARN,
        "INFO" => LEVEL_INFO,
        // Treat SQL/params and most operational logs as DEBUG by default
        "QUERY" | "PARAM" | "PARAMS" | "INSERT" | "UPDATE" | "DELETE" | "QUEUE" | "ENDPOINT" | "CORE ENDPOINT" => LEVEL_DEBUG,
        _ => LEVEL_DEBUG,
    }
}

// Enable ANSI colors for logs (default true). Set LOG_COLOR=0 to disable.
static LOG_ENABLE_COLOR: Lazy<bool> = Lazy::new(|| {
    match std::env::var("LOG_COLOR") {
        Ok(v) => !matches!(v.as_str(), "0" | "false" | "no"),
        Err(_) => true,
    }
});
