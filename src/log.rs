// create function to log message and save it to file

use crate::ISDEBUG;
use colored::{Color, Colorize};
use regex::Regex;

const MAX_LOG_BODY_LEN: usize = 2000;

fn is_verbose_enabled() -> bool {
    match std::env::var("LOG_VERBOSE") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => false,
    }
}

fn redact_sensitive(body: &str) -> String {
    // Redact common secrets in key/value text and JSON.
    let mut out = body.to_string();
    let patterns: [(&str, &str); 3] = [
        (
            r#"(?i)(password|pass|secret|token|authorization|api[_-]?key|encrypt_key|secret_key)\s*[:=]\s*\"[^\"]*\""#,
            "$1=***",
        ),
        (
            r#"(?i)(password|pass|secret|token|authorization|api[_-]?key|encrypt_key|secret_key)\s*[:=]\s*[^,\s\}]+"#,
            "$1=***",
        ),
        (r#"(?i)Bearer\s+[A-Za-z0-9\-._~+/]+=*"#, "Bearer ***"),
    ];
    for (pat, repl) in patterns {
        if let Ok(re) = Regex::new(pat) {
            out = re.replace_all(&out, repl).to_string();
        }
    }
    out
}

fn truncate_log_body(body: String) -> String {
    if body.len() <= MAX_LOG_BODY_LEN {
        return body;
    }
    let mut truncated = body[..MAX_LOG_BODY_LEN].to_string();
    let remainder = body.len().saturating_sub(MAX_LOG_BODY_LEN);
    truncated.push_str(&format!(" ... (truncated {} chars)", remainder));
    truncated
}

pub fn log_output(tipe: &str, title: &str, ssubtitle: &str, body: String, print_datetime: bool) {
    let mut subtitle = ssubtitle.to_string();
    if *ISDEBUG {
        let verbose = is_verbose_enabled();
        if !verbose {
            let t = tipe.to_ascii_uppercase();
            if t == "PARAM" || t == "PARAMS" || t == "BODY" {
                return;
            }
        }
        // make subtitle length to 6
        if !tipe.contains("QUERY") && subtitle.len() < 6 {
            let diff = 6 - subtitle.len();
            for _ in 0..diff {
                subtitle.push(' ');
            }
        }
        let mut s_datetime = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if !print_datetime {
            s_datetime = "".to_string();
        }

        let color_tipe = match tipe {
            "INFO" => Color::Green,
            "ERROR" => Color::Red,
            "WARN" => Color::Yellow,
            "DEBUG" => Color::Blue,
            "QUERY" => Color::Magenta,
            _ => Color::White,
        };

        let s_message_1 = format!(
            "# {}, {} {} at {}: ",
            tipe.color(color_tipe),
            title.cyan(),
            subtitle.color(color_tipe),
            s_datetime
        );
        print!("{}", s_message_1);
        if tipe.contains("QUERY") {
            println!("\n");
        }
        let sanitized = truncate_log_body(redact_sensitive(&body));
        println!("{}", sanitized.color(color_tipe));
        if tipe.contains("QUERY") {
            println!("\n");
        }
    }

}
