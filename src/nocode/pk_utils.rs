use crate::storage::ast::{Filter as QF, Val as QV};
use crate::database::state::DbParam;
use serde_json::Value as JsonValue;

/// Parse primary key values from path parameter using `~` as delimiter.
pub fn parse_pk_values(id_raw: &str) -> Vec<String> {
    id_raw.split('~').map(|s| s.to_string()).collect()
}

/// Validate that a composite-PK path segment is well-formed: no empty parts,
/// and every part is non-empty after trimming. Returns an error message suitable
/// for a 400 response when the input is malformed.
///
/// Note: because `~` is the delimiter, callers must encode `~` in literal PK
/// values (it is reserved). This function does not attempt to decode escapes.
pub fn validate_pk_path(id_raw: &str, expected_parts: usize) -> Result<Vec<String>, String> {
    if id_raw.is_empty() {
        return Err("Primary key path is empty".to_string());
    }
    let parts = parse_pk_values(id_raw);
    if parts.len() != expected_parts {
        return Err(format!(
            "Expected {} primary key parts (delimited by '~'), got {}",
            expected_parts,
            parts.len()
        ));
    }
    for (i, p) in parts.iter().enumerate() {
        if p.trim().is_empty() {
            return Err(format!("Primary key part #{} is empty", i + 1));
        }
    }
    Ok(parts)
}

/// Build a primary key filter, including composite PK support.
pub fn build_pk_filter(pk_columns: &[String], pk_values: &[String]) -> Result<QF, String> {
    if pk_columns.is_empty() {
        return Err("No primary key columns defined".to_string());
    }
    if pk_columns.len() != pk_values.len() {
        return Err(format!(
            "Primary key mismatch: expected {} values for {} columns",
            pk_columns.len(),
            pk_values.len()
        ));
    }

    if pk_columns.len() == 1 {
        Ok(QF::Eq(pk_columns[0].clone(), QV::Str(pk_values[0].clone())))
    } else {
        let filters = pk_columns
            .iter()
            .zip(pk_values.iter())
            .map(|(col, val)| QF::Eq(col.clone(), QV::Str(val.clone())))
            .collect();
        Ok(QF::And(filters))
    }
}

/// Boolean column kinds. `bool`/`boolean` bind as a native boolean; `bit`/`tinyint(1)`
/// bind as integer `0`/`1` (the portable encoding for MySQL/MSSQL/SQLite).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolKind {
    Native,
    Int01,
}

/// Classify a column type as boolean-like, if it is one.
pub fn bool_kind(type_data: &str) -> Option<BoolKind> {
    let td: String = type_data
        .to_ascii_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if td.starts_with("bool") {
        return Some(BoolKind::Native);
    }
    if td == "bit" || td.starts_with("bit(") || td == "tinyint(1)" || td.starts_with("tinyint(1)") {
        return Some(BoolKind::Int01);
    }
    None
}

/// True when the column type is boolean-like (`bool`, `boolean`, `bit`, `tinyint(1)`).
pub fn is_bool_type(type_data: &str) -> bool {
    bool_kind(type_data).is_some()
}

/// Parse a loose boolean string: true/false, 1/0, yes/no, y/n, t/f, on/off (case-insensitive).
pub fn parse_bool_str(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "t" | "on" => Some(true),
        "false" | "0" | "no" | "n" | "f" | "off" => Some(false),
        _ => None,
    }
}

/// Parse a loose boolean from a JSON value (bool, 0/1 number, or boolean-ish string).
pub fn parse_bool_value(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(b) => Some(*b),
        JsonValue::Number(n) => match n.as_f64() {
            Some(0.0) => Some(false),
            Some(1.0) => Some(true),
            _ => None,
        },
        JsonValue::String(s) => parse_bool_str(s),
        _ => None,
    }
}

fn bool_dbparam(b: bool, kind: BoolKind) -> DbParam {
    match kind {
        BoolKind::Native => DbParam::Bool(b),
        BoolKind::Int01 => DbParam::I64(b as i64),
    }
}

fn bool_json(b: bool, kind: BoolKind) -> JsonValue {
    match kind {
        BoolKind::Native => JsonValue::Bool(b),
        BoolKind::Int01 => serde_json::json!(b as i64),
    }
}

/// Coerce string input into typed DB parameter based on column type metadata.
pub fn dbparam_from_str_and_type(raw: &str, type_data: &str) -> DbParam {
    if let Some(kind) = bool_kind(type_data) {
        if let Some(b) = parse_bool_str(raw) {
            return bool_dbparam(b, kind);
        }
        return DbParam::Str(raw.to_string());
    }
    let td = type_data.to_ascii_lowercase();
    if td.contains("int") {
        if let Ok(n) = raw.parse::<i64>() {
            return DbParam::I64(n);
        }
        return DbParam::Str(raw.to_string());
    }
    if td.contains("float") || td.contains("double") || td.contains("decimal") || td.contains("money") || td.contains("numeric") || td.contains("real") {
        if let Ok(n) = raw.parse::<f64>() {
            return DbParam::F64(n);
        }
        return DbParam::Str(raw.to_string());
    }
    DbParam::Str(raw.to_string())
}

/// Coerce a JSON body value into a typed DB parameter based on column type metadata.
/// Booleans (`true`/`false`, `"yes"`, `1`, ...) on boolean columns become `DbParam::Bool`
/// (bool/boolean) or `DbParam::I64(0|1)` (bit/tinyint(1)); JSON null becomes `DbParam::Null`.
pub fn dbparam_from_value_and_type(value: &JsonValue, type_data: &str) -> DbParam {
    if value.is_null() {
        return DbParam::Null;
    }
    if let Some(kind) = bool_kind(type_data) {
        if let Some(b) = parse_bool_value(value) {
            return bool_dbparam(b, kind);
        }
        return DbParam::Str(loose_str(value));
    }
    match value {
        JsonValue::Bool(b) => DbParam::Str(b.to_string()),
        JsonValue::Number(n) => {
            let td = type_data.to_ascii_lowercase();
            if td.contains("int") {
                if let Some(i) = n.as_i64() {
                    return DbParam::I64(i);
                }
                return DbParam::Str(n.to_string());
            }
            if let Some(i) = n.as_i64()
                && !(td.contains("float") || td.contains("double") || td.contains("decimal") || td.contains("money") || td.contains("numeric") || td.contains("real"))
            {
                return DbParam::I64(i);
            }
            n.as_f64().map(DbParam::F64).unwrap_or_else(|| DbParam::Str(n.to_string()))
        }
        JsonValue::String(s) => dbparam_from_str_and_type(s.trim(), type_data),
        other => DbParam::Str(other.to_string()),
    }
}

/// JSON response value for a body value, based on column type metadata (mirror of
/// [`dbparam_from_value_and_type`]).
pub fn json_value_from_value_and_type(value: &JsonValue, type_data: &str) -> JsonValue {
    if value.is_null() {
        return JsonValue::Null;
    }
    if let Some(kind) = bool_kind(type_data) {
        return match parse_bool_value(value) {
            Some(b) => bool_json(b, kind),
            None => JsonValue::String(loose_str(value)),
        };
    }
    match value {
        JsonValue::String(s) => json_value_from_str_and_type(s.trim(), type_data),
        other => other.clone(),
    }
}

/// Loose string form of a JSON scalar (strings untouched, no surrounding quotes).
fn loose_str(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => s.trim().to_string(),
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

/// Coerce string input into JSON value based on column type metadata.
pub fn json_value_from_str_and_type(raw: &str, type_data: &str) -> JsonValue {
    if let Some(kind) = bool_kind(type_data) {
        if let Some(b) = parse_bool_str(raw) {
            return bool_json(b, kind);
        }
        return serde_json::json!(raw);
    }
    let td = type_data.to_ascii_lowercase();
    if td.contains("int") {
        if let Ok(n) = raw.parse::<i64>() {
            return serde_json::json!(n);
        }
        return serde_json::json!(raw);
    }
    if td.contains("float") || td.contains("double") || td.contains("decimal") || td.contains("money") || td.contains("numeric") || td.contains("real") {
        if let Ok(n) = raw.parse::<f64>() {
            return serde_json::json!(n);
        }
        return serde_json::json!(raw);
    }
    serde_json::json!(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pk_values() {
        assert_eq!(parse_pk_values("123"), vec!["123"]);
        assert_eq!(parse_pk_values("123~456"), vec!["123", "456"]);
        assert_eq!(parse_pk_values("abc~def~ghi"), vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_build_pk_filter_single() {
        let pk_cols = vec!["id".to_string()];
        let pk_vals = vec!["1".to_string()];
        let filter = build_pk_filter(&pk_cols, &pk_vals).unwrap();
        match filter {
            QF::Eq(col, val) => {
                assert_eq!(col, "id");
                assert!(matches!(val, QV::Str(v) if v == "1"));
            }
            _ => panic!("Expected QF::Eq"),
        }
    }

    #[test]
    fn test_build_pk_filter_composite() {
        let pk_cols = vec!["id1".to_string(), "id2".to_string()];
        let pk_vals = vec!["1".to_string(), "2".to_string()];
        let filter = build_pk_filter(&pk_cols, &pk_vals).unwrap();
        match filter {
            QF::And(filters) => assert_eq!(filters.len(), 2),
            _ => panic!("Expected QF::And"),
        }
    }

    #[test]
    fn test_build_pk_filter_mismatch() {
        let pk_cols = vec!["id1".to_string()];
        let pk_vals = vec!["1".to_string(), "2".to_string()];
        let result = build_pk_filter(&pk_cols, &pk_vals);
        assert!(result.is_err());
    }

    #[test]
    fn test_dbparam_from_str_and_type() {
        assert!(matches!(dbparam_from_str_and_type("12", "int"), DbParam::I64(12)));
        assert!(matches!(dbparam_from_str_and_type("12.5", "decimal"), DbParam::F64(v) if (v - 12.5).abs() < f64::EPSILON));
        assert!(matches!(dbparam_from_str_and_type("abc", "varchar"), DbParam::Str(v) if v == "abc"));
    }

    #[test]
    fn test_bool_kind_classification() {
        assert_eq!(bool_kind("bool"), Some(BoolKind::Native));
        assert_eq!(bool_kind("BOOLEAN"), Some(BoolKind::Native));
        assert_eq!(bool_kind("bit"), Some(BoolKind::Int01));
        assert_eq!(bool_kind("bit(1)"), Some(BoolKind::Int01));
        assert_eq!(bool_kind("tinyint(1)"), Some(BoolKind::Int01));
        assert_eq!(bool_kind("TINYINT (1)"), Some(BoolKind::Int01));
        assert_eq!(bool_kind("tinyint"), None, "plain tinyint is a number");
        assert_eq!(bool_kind("tinyint(4)"), None);
        assert_eq!(bool_kind("bigint"), None);
        assert_eq!(bool_kind("varchar(10)"), None);
        assert!(is_bool_type("boolean") && !is_bool_type("int"));
    }

    #[test]
    fn test_parse_bool_str() {
        for t in ["true", "TRUE", "1", "yes", "Y", "on", " t "] {
            assert_eq!(parse_bool_str(t), Some(true), "{}", t);
        }
        for f in ["false", "0", "no", "N", "off", "f"] {
            assert_eq!(parse_bool_str(f), Some(false), "{}", f);
        }
        assert_eq!(parse_bool_str("maybe"), None);
        assert_eq!(parse_bool_str(""), None);
    }

    #[test]
    fn test_dbparam_bool_typing_from_value() {
        use serde_json::json;
        // JSON bool on a native boolean column -> DbParam::Bool
        assert!(matches!(dbparam_from_value_and_type(&json!(true), "boolean"), DbParam::Bool(true)));
        assert!(matches!(dbparam_from_value_and_type(&json!(false), "bool"), DbParam::Bool(false)));
        // bit / tinyint(1) -> I64 0/1
        assert!(matches!(dbparam_from_value_and_type(&json!(true), "tinyint(1)"), DbParam::I64(1)));
        assert!(matches!(dbparam_from_value_and_type(&json!(false), "bit"), DbParam::I64(0)));
        // strings accepted on boolean columns
        assert!(matches!(dbparam_from_value_and_type(&json!("yes"), "boolean"), DbParam::Bool(true)));
        assert!(matches!(dbparam_from_value_and_type(&json!("0"), "boolean"), DbParam::Bool(false)));
        assert!(matches!(dbparam_from_value_and_type(&json!("no"), "tinyint(1)"), DbParam::I64(0)));
        // numbers 0/1 accepted
        assert!(matches!(dbparam_from_value_and_type(&json!(1), "bool"), DbParam::Bool(true)));
        // unparsable stays a string (caller decides to reject)
        assert!(matches!(dbparam_from_value_and_type(&json!("maybe"), "bool"), DbParam::Str(v) if v == "maybe"));
        // null -> Null
        assert!(matches!(dbparam_from_value_and_type(&serde_json::Value::Null, "bool"), DbParam::Null));
        // bool on a non-boolean column stays a string "true"
        assert!(matches!(dbparam_from_value_and_type(&json!(true), "varchar(10)"), DbParam::Str(v) if v == "true"));
    }

    #[test]
    fn test_dbparam_from_value_numbers() {
        use serde_json::json;
        assert!(matches!(dbparam_from_value_and_type(&json!(12), "int"), DbParam::I64(12)));
        assert!(matches!(dbparam_from_value_and_type(&json!(12), "decimal(10,2)"), DbParam::F64(v) if v == 12.0));
        assert!(matches!(dbparam_from_value_and_type(&json!(12.5), "decimal(10,2)"), DbParam::F64(v) if v == 12.5));
        assert!(matches!(dbparam_from_value_and_type(&json!("12.5"), "numeric"), DbParam::F64(v) if v == 12.5));
        assert!(matches!(dbparam_from_value_and_type(&json!(7), "varchar(5)"), DbParam::I64(7)));
        assert!(matches!(dbparam_from_value_and_type(&json!("abc"), "varchar(5)"), DbParam::Str(v) if v == "abc"));
    }

    #[test]
    fn test_str_and_json_bool_typing() {
        assert!(matches!(dbparam_from_str_and_type("true", "boolean"), DbParam::Bool(true)));
        assert!(matches!(dbparam_from_str_and_type("1", "tinyint(1)"), DbParam::I64(1)));
        assert!(matches!(dbparam_from_str_and_type("5", "tinyint"), DbParam::I64(5)));
        assert_eq!(json_value_from_str_and_type("true", "bool"), serde_json::json!(true));
        assert_eq!(json_value_from_str_and_type("yes", "tinyint(1)"), serde_json::json!(1));
    }

    #[test]
    fn test_json_value_from_str_and_type() {
        assert_eq!(json_value_from_str_and_type("12", "int"), serde_json::json!(12));
        assert_eq!(json_value_from_str_and_type("12.5", "money"), serde_json::json!(12.5));
        assert_eq!(json_value_from_str_and_type("abc", "varchar"), serde_json::json!("abc"));
    }
}
