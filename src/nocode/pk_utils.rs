use crate::storage::ast::{Filter as QF, Val as QV};
use crate::database::state::DbParam;
use serde_json::Value as JsonValue;

/// Parse primary key values from path parameter using `~` as delimiter.
pub fn parse_pk_values(id_raw: &str) -> Vec<String> {
    id_raw.split('~').map(|s| s.to_string()).collect()
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

/// Coerce string input into typed DB parameter based on column type metadata.
pub fn dbparam_from_str_and_type(raw: &str, type_data: &str) -> DbParam {
    let td = type_data.to_ascii_lowercase();
    if td.contains("int") {
        if let Ok(n) = raw.parse::<i64>() {
            return DbParam::I64(n);
        }
        return DbParam::Str(raw.to_string());
    }
    if td.contains("float") || td.contains("double") || td.contains("decimal") || td.contains("money") {
        if let Ok(n) = raw.parse::<f64>() {
            return DbParam::F64(n);
        }
        return DbParam::Str(raw.to_string());
    }
    DbParam::Str(raw.to_string())
}

/// Coerce string input into JSON value based on column type metadata.
pub fn json_value_from_str_and_type(raw: &str, type_data: &str) -> JsonValue {
    let td = type_data.to_ascii_lowercase();
    if td.contains("int") {
        if let Ok(n) = raw.parse::<i64>() {
            return serde_json::json!(n);
        }
        return serde_json::json!(raw);
    }
    if td.contains("float") || td.contains("double") || td.contains("decimal") || td.contains("money") {
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
    fn test_json_value_from_str_and_type() {
        assert_eq!(json_value_from_str_and_type("12", "int"), serde_json::json!(12));
        assert_eq!(json_value_from_str_and_type("12.5", "money"), serde_json::json!(12.5));
        assert_eq!(json_value_from_str_and_type("abc", "varchar"), serde_json::json!("abc"));
    }
}
