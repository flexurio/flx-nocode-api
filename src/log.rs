// create function to log message and save it to file

use crate::{ISDEBUG, ISLOGGING, LOC_LOGGING};
use colored::Colorize;
use regex::Regex;
use once_cell::sync::Lazy;
use std::sync::mpsc::{Sender, channel};
use std::thread;

// Async logging channel: one background thread writes to file to reduce caller I/O latency
static LOG_SENDER: Lazy<Sender<String>> = Lazy::new(|| {
    let (tx, rx) = channel::<String>();
    let file_path = format!("{}/.log.txt", *LOC_LOGGING);
    // Ensure directory exists
    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        let _ = std::fs::create_dir_all(parent); // ignore errors; fallback will try again on write
    }
    thread::spawn(move || {
        // Open once; if fails, attempt reopen each message
        let mut file_opt: Option<std::fs::File> = OpenOptions::new().create(true).append(true).open(&file_path).ok();
        while let Ok(line) = rx.recv() {
            // Lazily open if previous open failed
            if file_opt.is_none() {
                file_opt = OpenOptions::new().create(true).append(true).open(&file_path).ok();
            }
            if let Some(file) = file_opt.as_mut() {
                let clean = ANSI_ESCAPE.replace_all(&line, "");
                if let Err(e) = writeln!(file, "{}", clean) {
                    eprintln!("Async log write error: {}", e);
                    file_opt = None; // force reopen next iteration
                }
            }
        }
    });
    tx
});
use std::fs::OpenOptions;
use std::io::Write;

static ANSI_ESCAPE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\x1B\[[0-9;]*[mK]").unwrap());

pub fn log_to_file(message: &str) {
    // Non-blocking best effort; on send failure fallback to direct stderr
    if let Err(e) = LOG_SENDER.send(message.to_string()) {
        eprintln!("Log channel send failed: {} -> {}", e, message);
    }
}

pub fn log_output(tipe: &str, title: &str, ssubtitle: &str, body: String, print_datetime: bool) {
    let mut subtitle = ssubtitle.to_string();
    let mut s_message_1 = String::new();
    if *ISDEBUG {
        // make subtitle length to 6
        if !tipe.contains("QUERY") && subtitle.len() < 6 {
            let diff = 6 - subtitle.len();
            for _ in 0..diff {
                subtitle.push(' ');
            }
        }
        let s_datetime = if print_datetime {
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
        } else { String::new() };
        s_message_1 = if print_datetime {
            format!(
                "# {}, {} {} at {}: ",
                tipe.yellow(),
                title.cyan(),
                subtitle.blue(),
                s_datetime
            )
        } else {
            format!(
                "# {}, {} {}: ",
                tipe.yellow(),
                title.cyan(),
                subtitle.blue()
            )
        };
        print!("{}", s_message_1);
        if tipe.contains("QUERY") {
            println!("\n");
        }
        println!("{}", body.green());
        if tipe.contains("QUERY") {
            println!("\n");
        }
    }

    if *ISLOGGING {
        log_to_file(s_message_1.as_str());
        log_to_file(&body);
    }
}
