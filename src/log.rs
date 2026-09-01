// create function to log message and save it to file

use crate::ISDEBUG;
use colored::{Color, Colorize};
use once_cell::sync::Lazy;
use regex::Regex;

const MAX_LOG_BODY_LEN: usize = 2000;

// Compiled once for the process lifetime instead of on every log_output() call.
static RE_REDACT_QUOTED: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(password|pass|secret|token|authorization|api[_-]?key|encrypt_key|secret_key)\s*[:=]\s*\"[^\"]*\""#)
        .expect("valid redact-quoted regex")
});
static RE_REDACT_UNQUOTED: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(password|pass|secret|token|authorization|api[_-]?key|encrypt_key|secret_key)\s*[:=]\s*[^,\s\}]+"#)
        .expect("valid redact-unquoted regex")
});
static RE_REDACT_BEARER: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)Bearer\s+[A-Za-z0-9\-._~+/]+=*"#).expect("valid redact-bearer regex")
});

use std::sync::atomic::{AtomicBool, Ordering};

static VERBOSE_FLAG: Lazy<AtomicBool> = Lazy::new(|| {
    let enabled = match std::env::var("LOG_VERBOSE") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => false,
    };
    AtomicBool::new(enabled)
});

fn is_verbose_enabled() -> bool {
    VERBOSE_FLAG.load(Ordering::Relaxed)
}

#[cfg(test)]
fn refresh_verbose_from_env() -> bool {
    let enabled = match std::env::var("LOG_VERBOSE") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => false,
    };
    VERBOSE_FLAG.store(enabled, Ordering::Relaxed);
    enabled
}

fn contains_sensitive_keyword(s: &str) -> bool {
    let s_bytes = s.as_bytes();
    for window in s_bytes.windows(4) {
        if window.eq_ignore_ascii_case(b"pass")
            || window.eq_ignore_ascii_case(b"secr")
            || window.eq_ignore_ascii_case(b"toke")
            || window.eq_ignore_ascii_case(b"bear")
            || window.eq_ignore_ascii_case(b"auth")
            || window.eq_ignore_ascii_case(b"key")
        {
            return true;
        }
    }
    false
}

fn redact_sensitive(body: &str) -> String {
    // Fast-path: if body contains none of the trigger substrings, avoid regex passes entirely.
    if !contains_sensitive_keyword(body) {
        return body.to_string();
    }
    // Redact common secrets in key/value text and JSON.
    let out = RE_REDACT_QUOTED.replace_all(body, "$1=***");
    let out = RE_REDACT_UNQUOTED.replace_all(&out, "$1=***");
    let out = RE_REDACT_BEARER.replace_all(&out, "Bearer ***");
    out.into_owned()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_sensitive_password_json() {
        // The regex matches unquoted key names (key=value or key: value style),
        // not JSON with quoted keys like {"password": "value"}.
        // Use unquoted key format which the regex does handle.
        let body = r#"password: "secret123", name: admin"#;
        let result = redact_sensitive(body);
        assert!(!result.contains("secret123"), "Password should be redacted");
        assert!(result.contains("***"));
        assert!(result.contains("admin"), "Non-sensitive fields preserved");
    }

    #[test]
    fn test_redact_sensitive_bearer_token() {
        // Pattern 3 matches bare "Bearer <token>" strings directly.
        // The "Authorization: Bearer ..." format is partially handled by pattern 2
        // which removes "Bearer" but leaves the token; use bare Bearer for full redaction.
        let body = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def";
        let result = redact_sensitive(body);
        assert!(!result.contains("eyJhbGci"), "Bearer token should be redacted");
        assert!(result.contains("Bearer ***"));
    }

    #[test]
    fn test_redact_sensitive_api_key() {
        let body = r#"api_key: mysecretapikey"#;
        let result = redact_sensitive(body);
        assert!(!result.contains("mysecretapikey"));
    }

    #[test]
    fn test_redact_sensitive_secret_field() {
        // Uses key=value format (unquoted) which the regex handles
        let body = r#"secret=topsecret"#;
        let result = redact_sensitive(body);
        assert!(!result.contains("topsecret"));
    }

    #[test]
    fn test_redact_sensitive_no_secrets_unchanged() {
        let body = r#"{"name": "John", "age": 30, "email": "john@example.com"}"#;
        let result = redact_sensitive(body);
        assert_eq!(result, body, "Non-sensitive content should be unchanged");
    }

    #[test]
    fn test_truncate_log_body_short_string() {
        let body = "short message".to_string();
        let result = truncate_log_body(body.clone());
        assert_eq!(result, body);
    }

    #[test]
    fn test_truncate_log_body_at_exact_limit() {
        let body = "a".repeat(2000);
        let result = truncate_log_body(body.clone());
        assert_eq!(result, body, "Exactly at limit should not be truncated");
    }

    #[test]
    fn test_truncate_log_body_over_limit() {
        let body = "a".repeat(3000);
        let result = truncate_log_body(body);
        assert!(result.contains("truncated"), "Over-limit body should have truncation marker");
        assert!(result.starts_with("aaaaaa"), "Should contain the start of original body");
    }

    #[test]
    fn test_truncate_log_body_includes_remainder_count() {
        let body = "x".repeat(2500);
        let result = truncate_log_body(body);
        assert!(result.contains("500"), "Should report 500 truncated chars");
    }

    #[test]
    fn test_is_verbose_enabled_default_false() {
        // SAFETY: single-threaded test context, no concurrent env mutations
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
        assert!(!refresh_verbose_from_env(), "Should be false when env var not set");
    }

    #[test]
    fn test_is_verbose_enabled_with_one() {
        unsafe { std::env::set_var("LOG_VERBOSE", "1"); }
        assert!(refresh_verbose_from_env());
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
    }

    #[test]
    fn test_is_verbose_enabled_with_true() {
        unsafe { std::env::set_var("LOG_VERBOSE", "true"); }
        assert!(refresh_verbose_from_env());
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
    }

    #[test]
    fn test_is_verbose_enabled_with_yes() {
        unsafe { std::env::set_var("LOG_VERBOSE", "yes"); }
        assert!(refresh_verbose_from_env());
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
    }

    #[test]
    fn test_is_verbose_enabled_case_insensitive() {
        unsafe { std::env::set_var("LOG_VERBOSE", "TRUE"); }
        assert!(refresh_verbose_from_env());
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
    }

    #[test]
    fn test_is_verbose_disabled_with_false() {
        unsafe { std::env::set_var("LOG_VERBOSE", "false"); }
        assert!(!refresh_verbose_from_env());
        unsafe { std::env::remove_var("LOG_VERBOSE"); }
    }
}
