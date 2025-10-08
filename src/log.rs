// Logging module with feature-flagged async + optional structured format
use crate::{ISDEBUG, ISLOGGING, LOC_LOGGING};
use colored::Colorize;
// Lazy used only in async-log backend; keep import behind feature flag below
use chrono::Local;

#[derive(Copy, Clone)]
#[allow(dead_code)]
enum LogFormat { Plain, #[cfg(feature = "structured-log")] Json }

#[cfg(feature = "structured-log")]
static LOG_FORMAT: LogFormat = LogFormat::Json;
#[cfg(not(feature = "structured-log"))]
static LOG_FORMAT: LogFormat = LogFormat::Plain;

#[inline]
fn now_ts() -> String { Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string() }

// ---------------- Sync backend (no thread) ----------------
#[cfg(not(feature = "async-log"))]
mod backend {
    use super::*;
    use std::fs::OpenOptions; use std::io::Write;
    pub fn init() {}
    pub fn shutdown() {}
    pub fn write_line(line: &str) {
        if !*ISLOGGING { return; }
        let file_path = format!("{}/.log.txt", *LOC_LOGGING);
        if let Some(parent) = std::path::Path::new(&file_path).parent() { let _ = std::fs::create_dir_all(parent); }
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&file_path) {
            let _ = f.write_all(line.as_bytes()); let _ = f.write_all(b"\n");
        }
    }
}

// ---------------- Async backend (channel + thread) ----------------
#[cfg(feature = "async-log")]
mod backend {
    use super::*;
    use std::sync::mpsc::{channel, Sender, Receiver};
    use std::fs::OpenOptions; use std::io::Write; use once_cell::sync::Lazy; 
    enum Msg { Line(String), Shutdown }
    static SENDER: Lazy<Sender<Msg>> = Lazy::new(|| { let (tx, rx) = channel(); std::thread::spawn(move || writer_loop(rx)); tx });
    fn writer_loop(rx: Receiver<Msg>) {
        let file_path = format!("{}/.log.txt", *LOC_LOGGING);
        if let Some(parent) = std::path::Path::new(&file_path).parent() { let _ = std::fs::create_dir_all(parent); }
        let mut file = OpenOptions::new().create(true).append(true).open(&file_path).ok();
        while let Ok(msg) = rx.recv() {
            match msg {
                Msg::Line(line) => {
                    if file.is_none() { file = OpenOptions::new().create(true).append(true).open(&file_path).ok(); }
                    if let Some(f) = file.as_mut() { let _ = f.write_all(line.as_bytes()); let _ = f.write_all(b"\n"); }
                }
                Msg::Shutdown => break,
            }
        }
    }
    pub fn init() { let _ = &*SENDER; }
    pub fn shutdown() { let _ = SENDER.send(Msg::Shutdown); }
    pub fn write_line(line: &str) { if !*ISLOGGING { return; } let _ = SENDER.send(Msg::Line(line.to_owned())); }
}

pub fn init_logger() { backend::init(); }
pub fn shutdown_logger() { backend::shutdown(); }

fn format_line(level: &str, target: &str, subtitle: &str, body: &str, with_dt: bool) -> String {
    match LOG_FORMAT {
        LogFormat::Plain => {
            if with_dt { format!("{} | {:<8} | {:<14} | {} | {}", now_ts(), level, target, subtitle, body) }
            else { format!("{:<8} | {:<14} | {} | {}", level, target, subtitle, body) }
        }
        #[cfg(feature = "structured-log")]
        LogFormat::Json => {
            // Encode body as JSON string via sonic_rs
            let msg_json = match sonic_rs::to_string(&sonic_rs::Value::from(body)) { Ok(s) => s, Err(_) => "\"\"".to_string() };
            if with_dt { format!(r#"{{"ts":"{}","lvl":"{}","target":"{}","sub":"{}","msg":{}}}"#, now_ts(), level, target, subtitle, msg_json) }
            else { format!(r#"{{"lvl":"{}","target":"{}","sub":"{}","msg":{}}}"#, level, target, subtitle, msg_json) }
        }
    }
}

pub fn log_output(level: &str, title: &str, subtitle: &str, body: String, print_datetime: bool) {
    if *ISDEBUG {
        let head = if print_datetime { format!("{} {}", now_ts(), level.yellow()) } else { level.yellow().to_string() };
        println!("{} {} {} {}", head, title.cyan(), subtitle.blue(), body.green());
    }
    if *ISLOGGING {
        let line = format_line(level, title, subtitle, &body, print_datetime);
        backend::write_line(&line);
    }
}

// pub fn log_simple(level: &str, msg: &str) { log_output(level, "APP", "-", msg.to_string(), true); }
