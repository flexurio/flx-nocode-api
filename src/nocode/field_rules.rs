//! Declarative per-column validation (`enum`, `pattern`, `min`, `max`, `min_length`,
//! `max_length`, `email`, `url`, `message`) declared directly on a schema column.
//!
//! ```json
//! { "name": "status",  "type_data": "varchar(20)", "enum": ["DRAFT", "APPROVED"] }
//! { "name": "qty",     "type_data": "int",         "min": 0, "max": 1000 }
//! { "name": "code",    "type_data": "varchar(10)", "pattern": "^[A-Z]{3}-[0-9]+$", "min_length": 5 }
//! { "name": "email",   "type_data": "varchar(100)","email": true, "message": "Email tidak valid" }
//! { "name": "website", "type_data": "text",        "url": true }
//! ```
//!
//! Rules are checked for every column present in a POST/PUT body before any DB write.
//! `null` values are never validated here (nullability is enforced separately).

use dashmap::DashMap;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;

use crate::model::{Column, FieldRules};

/// Compiled-regex cache keyed by the pattern source.
static REGEX_CACHE: Lazy<DashMap<String, Regex>> = Lazy::new(DashMap::new);

static EMAIL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[^@\s]+@[^@\s]+\.[^@\s]+$").expect("valid email regex"));
static URL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z][A-Za-z0-9+.-]*://[^\s/?#]+[^\s]*$").expect("valid url regex"));

/// Fetch (or compile and cache) a user-supplied regex.
fn cached_regex(pattern: &str) -> Result<Regex, String> {
    if let Some(re) = REGEX_CACHE.get(pattern) {
        return Ok(re.clone());
    }
    let re = Regex::new(pattern).map_err(|e| format!("invalid pattern '{}': {}", pattern, e))?;
    REGEX_CACHE.insert(pattern.to_string(), re.clone());
    Ok(re)
}

/// Loose string form used for enum/pattern/length checks: strings as-is (trimmed),
/// numbers/bools via `to_string()`. Arrays/objects use compact JSON.
fn loose_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Numeric view: JSON numbers or numeric strings.
fn as_number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Loose equality between a body value and an enum entry: `1 == "1"`, `"A" == "A"`.
fn loose_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    if let (Some(x), Some(y)) = (as_number(a), as_number(b)) {
        return x == y;
    }
    loose_string(a) == loose_string(b)
}

/// Pretty number for messages (no trailing `.0` for integers).
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        n.to_string()
    }
}

/// Validate `value` against the rules declared on `col`.
///
/// Returns `Err(reason)` where `reason` is the column's custom `message` when set, otherwise a
/// generated description. `Value::Null` always passes (nullability is enforced elsewhere).
pub fn validate_field(col: &Column, value: &Value) -> Result<(), String> {
    let rules = &col.rules;
    if rules.is_empty() || value.is_null() {
        return Ok(());
    }
    check_rules(rules, value).map_err(|reason| rules.message.clone().unwrap_or(reason))
}

/// Same as [`validate_field`] but formats the error as a user-facing message:
/// `Invalid field '<col>': <reason>`. (The POST handler maps messages containing
/// "Invalid" to HTTP 400.)
pub fn validate_field_message(col: &Column, value: &Value) -> Result<(), String> {
    validate_field(col, value).map_err(|reason| format!("Invalid field '{}': {}", col.name, reason))
}

/// Validate every body key that names a schema column. First failure wins.
pub fn validate_body(columns: &[Column], body: &serde_json::Map<String, Value>) -> Result<(), String> {
    for col in columns {
        if col.rules.is_empty() {
            continue;
        }
        if let Some(v) = body.get(&col.name) {
            validate_field_message(col, v)?;
        }
    }
    Ok(())
}

fn check_rules(rules: &FieldRules, value: &Value) -> Result<(), String> {
    let s = loose_string(value);

    if let Some(allowed) = &rules.enum_values
        && !allowed.iter().any(|a| loose_eq(value, a))
    {
        let list: Vec<String> = allowed.iter().map(loose_string).collect();
        return Err(format!("must be one of [{}]", list.join(", ")));
    }

    if let Some(min) = rules.min {
        match as_number(value) {
            Some(n) if n < min => return Err(format!("must be >= {}", fmt_num(min))),
            Some(_) => {}
            None => return Err("must be a number".to_string()),
        }
    }
    if let Some(max) = rules.max {
        match as_number(value) {
            Some(n) if n > max => return Err(format!("must be <= {}", fmt_num(max))),
            Some(_) => {}
            None => return Err("must be a number".to_string()),
        }
    }

    let len = s.chars().count();
    if let Some(min_len) = rules.min_length
        && len < min_len
    {
        return Err(format!("must be at least {} characters", min_len));
    }
    if let Some(max_len) = rules.max_length
        && len > max_len
    {
        return Err(format!("must be at most {} characters", max_len));
    }

    if let Some(p) = &rules.pattern {
        let re = cached_regex(p)?;
        if !re.is_match(&s) {
            return Err(format!("does not match pattern {}", p));
        }
    }

    if rules.email == Some(true) && !EMAIL_RE.is_match(&s) {
        return Err("must be a valid email address".to_string());
    }
    if rules.url == Some(true) && !URL_RE.is_match(&s) {
        return Err("must be a valid URL".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(rules_json: &str) -> Column {
        let mut j: Value = serde_json::from_str(rules_json).unwrap();
        j["name"] = json!("f");
        serde_json::from_value(j).unwrap()
    }

    #[test]
    fn no_rules_always_ok() {
        let c = col(r#"{"type_data":"varchar(10)"}"#);
        assert!(validate_field(&c, &json!("anything")).is_ok());
        assert!(validate_field(&c, &json!(123)).is_ok());
    }

    #[test]
    fn null_skips_validation() {
        let c = col(r#"{"enum":["A"],"min":5,"email":true}"#);
        assert!(validate_field(&c, &Value::Null).is_ok());
    }

    #[test]
    fn enum_loose_equality() {
        let c = col(r#"{"enum":["DRAFT","APPROVED",1]}"#);
        assert!(validate_field(&c, &json!("DRAFT")).is_ok());
        assert!(validate_field(&c, &json!(1)).is_ok());
        assert!(validate_field(&c, &json!("1")).is_ok(), "string vs number compares loosely");
        assert!(validate_field(&c, &json!(1.0)).is_ok());
        let e = validate_field(&c, &json!("VOID")).unwrap_err();
        assert_eq!(e, "must be one of [DRAFT, APPROVED, 1]");
        // `values` alias
        let c2 = col(r#"{"values":["x"]}"#);
        assert!(validate_field(&c2, &json!("y")).is_err());
    }

    #[test]
    fn pattern_matching_and_cache() {
        let c = col(r#"{"pattern":"^[A-Z]{3}-[0-9]+$"}"#);
        assert!(validate_field(&c, &json!("ABC-12")).is_ok());
        assert!(validate_field(&c, &json!("abc-12")).is_err());
        assert!(REGEX_CACHE.contains_key("^[A-Z]{3}-[0-9]+$"));
        // numbers use their string form
        let c2 = col(r#"{"pattern":"^[0-9]{3}$"}"#);
        assert!(validate_field(&c2, &json!(123)).is_ok());
        assert!(validate_field(&c2, &json!(12)).is_err());
    }

    #[test]
    fn invalid_pattern_reports_error() {
        let c = col(r#"{"pattern":"(unclosed"}"#);
        let e = validate_field(&c, &json!("x")).unwrap_err();
        assert!(e.starts_with("invalid pattern"), "{}", e);
    }

    #[test]
    fn min_max_numbers_and_numeric_strings() {
        let c = col(r#"{"type_data":"int","min":0,"max":10}"#);
        assert!(validate_field(&c, &json!(0)).is_ok());
        assert!(validate_field(&c, &json!(10)).is_ok());
        assert!(validate_field(&c, &json!("7.5")).is_ok());
        assert_eq!(validate_field(&c, &json!(-1)).unwrap_err(), "must be >= 0");
        assert_eq!(validate_field(&c, &json!("11")).unwrap_err(), "must be <= 10");
        assert_eq!(validate_field(&c, &json!("abc")).unwrap_err(), "must be a number");
        let c2 = col(r#"{"max":2.5}"#);
        assert_eq!(validate_field(&c2, &json!(3)).unwrap_err(), "must be <= 2.5");
    }

    #[test]
    fn length_rules_count_chars() {
        let c = col(r#"{"min_length":2,"max_length":4}"#);
        assert!(validate_field(&c, &json!("ab")).is_ok());
        assert!(validate_field(&c, &json!("héllo")).is_err(), "5 chars > 4");
        assert!(validate_field(&c, &json!("héll")).is_ok(), "4 chars (not bytes)");
        assert_eq!(validate_field(&c, &json!("a")).unwrap_err(), "must be at least 2 characters");
        assert_eq!(validate_field(&c, &json!("abcde")).unwrap_err(), "must be at most 4 characters");
    }

    #[test]
    fn email_and_url_rules() {
        let e = col(r#"{"email":true}"#);
        assert!(validate_field(&e, &json!("a.b@example.co.id")).is_ok());
        assert!(validate_field(&e, &json!("not-an-email")).is_err());
        assert!(validate_field(&e, &json!("a@b")).is_err());
        let u = col(r#"{"url":true}"#);
        assert!(validate_field(&u, &json!("https://example.com/path?q=1")).is_ok());
        assert!(validate_field(&u, &json!("example.com")).is_err());
        // `false` disables the rule
        let off = col(r#"{"email":false}"#);
        assert!(validate_field(&off, &json!("nope")).is_ok());
    }

    #[test]
    fn custom_message_replaces_reason() {
        let c = col(r#"{"enum":["A"],"message":"Status tidak dikenal"}"#);
        assert_eq!(validate_field(&c, &json!("B")).unwrap_err(), "Status tidak dikenal");
        assert_eq!(
            validate_field_message(&c, &json!("B")).unwrap_err(),
            "Invalid field 'f': Status tidak dikenal"
        );
    }

    #[test]
    fn validate_body_checks_only_present_columns() {
        let a = col(r#"{"name":"a","min":1}"#);
        let mut b = col(r#"{"enum":["X"]}"#);
        b.name = "b".into();
        let cols = vec![a, b];
        let body = json!({"a": 5}).as_object().unwrap().clone();
        assert!(validate_body(&cols, &body).is_ok(), "absent 'b' is not validated");
        let body2 = json!({"a": 5, "b": "Y"}).as_object().unwrap().clone();
        assert_eq!(validate_body(&cols, &body2).unwrap_err(), "Invalid field 'b': must be one of [X]");
    }
}
