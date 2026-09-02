use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::auth::ClaimsConverter;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DbType {
    Mysql,
    Postgres,
    Sqlite,
    Mssql,
    Mongodb,
}

impl DbType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DbType::Mysql => "mysql",
            DbType::Postgres => "postgres",
            DbType::Sqlite => "sqlite",
            DbType::Mssql => "mssql",
            DbType::Mongodb => "mongodb",
        }
    }
    
    #[allow(dead_code)]
    pub fn is_sql(&self) -> bool {
        !matches!(self, DbType::Mongodb)
    }
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub routes: Vec<String>,
    #[serde(default)]
    pub route_publics: HashSet<String>,
    #[serde(default)]
    pub converter_token: ClaimsConverter,
}

pub struct ParamJoin {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub reference_table: String,
    #[serde(default)]
    pub reference_column: String,
    #[serde(default)]
    pub on_delete: String, // "cascade", "restrict", "set null", "no action"
    #[serde(default)]
    pub on_update: String, // "cascade", "restrict", "set null", "no action"
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// create struct pivot ForeingKey to triger action in reference table
pub struct ReferenceForeignKey {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub on_delete_action: ReferenceForeignKeyAction,
    #[serde(default)]
    pub on_update_action: ReferenceForeignKeyAction,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ReferenceForeignKeyAction {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub action: String,      // "cascade", "restrict", "set null", "no action"
    #[serde(default)]
    pub type_delete: String, // soft or hard
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DetailSchema {
    /// Field name in the request JSON payload (e.g. "items", "details", "lines")
    #[serde(default)]
    pub field: String,
    /// Target detail table / entity name (e.g. "transaction_purchase_order_item")
    #[serde(default)]
    pub target_table: String,
    /// Foreign key column in the detail table referencing the parent header (e.g. "po_id")
    #[serde(default)]
    pub foreign_key_column: String,
    /// Parent primary key column to reference (default: "id")
    #[serde(default)]
    pub parent_key_column: Option<String>,
    /// Optional column whitelist/mapping for the detail table
    #[serde(default)]
    pub columns: Vec<String>,
    /// Update strategy on PUT: "replace" (default), "upsert", "append"
    #[serde(default)]
    pub update_strategy: Option<String>,
    /// Cascade delete detail records when parent header is deleted (default: true)
    #[serde(default)]
    pub cascade_delete: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub primary_key: PrimaryKey,
    #[serde(default)]
    pub columns: Vec<Column>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(default)]
    pub details: Vec<DetailSchema>,
    #[serde(default)]
    pub indexes: Vec<Index>,
    #[serde(default)]
    pub redis: Redis,
    #[serde(default)]
    pub get: OperationGet,
    #[serde(default)]
    pub post: OperationPost,
    #[serde(default)]
    pub put: OperationPut,
    #[serde(default)]
    pub del: OperationDelete,
    #[serde(default)]
    pub patch: Patch,
    #[serde(default)]
    pub trace: Trace,
    #[serde(default)]
    pub auto_generate: bool,
    #[serde(default)]
    pub seed: bool,
    #[serde(default)]
    pub collate: String,
}

// default value for TableSchema
impl Default for TableSchema {
    fn default() -> Self {
        TableSchema {
            table: "".to_string(),
            primary_key: PrimaryKey { columns: vec![] },
            columns: vec![],
            foreign_keys: vec![],
            details: vec![],
            indexes: vec![],
            redis: Redis {
                keys: vec![],
                ttl: 0,
            },
            get: OperationGet {
                enable_method: false,
                columns: vec![],
                parameters: vec![],
                join_tables: vec![],
                column_groups: vec![],
                having: vec![],
                order_by: vec![],
                where_clause: vec![],
            },
            post: OperationPost {
                enable_method: false,
                validate_data: "".to_string(),
                pre_process: "".to_string(),
                columns: vec![],
                post_process: "".to_string(),
            },
            put: OperationPut {
                enable_method: false,
                validate_data: "".to_string(),
                pre_process: "".to_string(),
                columns: vec![],
                post_process: "".to_string(),
            },
            del: OperationDelete {
                enable_method: false,
                pre_process: "".to_string(),
                columns: vec![],
                type_delete: "soft".to_string(),
                post_process: "".to_string(),
            },
            patch: Patch {
                enable_method: false,
                pre_process_sp: "".to_string(),
                parameters: vec![],
                return_mode: "".to_string(),
            },
            trace: Trace {
                enable_method: false,
                insert_into: "".to_string(),
                column_inserts: vec![],
                column_selects: vec![],
                parameters: vec![],
                join_tables: vec![],
                column_groups: vec![],
                column_conflicts: vec![],
            },
            auto_generate: false,
            seed: false,
            collate: "".to_string(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct PrimaryKey {
    #[serde(default)]
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Column {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub type_data: String,
    #[serde(default)]
    pub function: String,
    /// Optional endpoint to generate the numeric running-number for the `ID` token in `function`.
    /// When non-empty, the `000ID`-style token fetches the number from this endpoint instead of
    /// querying MAX(id)+1. Supports `{request.field}` interpolation in the URL.
    /// Empty string (the default, also used when the key is absent in JSON) disables it.
    #[serde(default)]
    pub function_endpoint: String,
    /// Dotted JSON path inside the endpoint response that holds the number.
    /// Empty (the default) is treated as "data". Ignored when `function_endpoint` is empty.
    #[serde(default)]
    pub function_endpoint_path: String,
    #[serde(default)]
    pub encrypt: bool,
    #[serde(default)]
    pub collate: String,
    #[serde(default)]
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Index {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Redis {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub ttl: u32,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct OperationGet {
    
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub join_tables: Vec<JoinTable>,
    #[serde(default)]
    pub column_groups: Vec<String>,
    #[serde(default)]
    pub having: Vec<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub where_clause: Vec<String>,
    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinTable {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub logical: String,
    #[serde(default)]
    pub type_join: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    #[serde(default)]
    pub columns: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct OperationPost {
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub validate_data: String,
    #[serde(default)]
    pub pre_process: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub post_process: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct OperationPut {
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub validate_data: String,
    #[serde(default)]
    pub pre_process: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub post_process: String,
}


#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct OperationDelete {
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub pre_process: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub type_delete: String,
    #[serde(default)]
    pub post_process: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Patch {
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub pre_process_sp: String,
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub return_mode: String, // "" | "rows" | "affected"
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Trace {
    #[serde(default)]
    pub enable_method: bool,
    #[serde(default)]
    pub insert_into: String,
    #[serde(default)]
    pub column_inserts: Vec<String>,
    #[serde(default)]
    pub column_selects: Vec<String>,
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub join_tables: Vec<JoinTable>,
    #[serde(default)]
    pub column_groups: Vec<String>,
    #[serde(default)]
    pub column_conflicts: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebResponse {
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub total_data: i32,
    #[serde(default)]
    pub data: serde_json::Value,
}

// create struct for logging
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Log {
    #[serde(default)]
    pub level: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub timestamp: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- DbType ---

    #[test]
    fn test_db_type_as_str() {
        assert_eq!(DbType::Mysql.as_str(), "mysql");
        assert_eq!(DbType::Postgres.as_str(), "postgres");
        assert_eq!(DbType::Sqlite.as_str(), "sqlite");
        assert_eq!(DbType::Mssql.as_str(), "mssql");
        assert_eq!(DbType::Mongodb.as_str(), "mongodb");
    }

    #[test]
    fn test_db_type_is_sql_returns_false_for_mongodb() {
        assert!(DbType::Mysql.is_sql());
        assert!(DbType::Postgres.is_sql());
        assert!(DbType::Sqlite.is_sql());
        assert!(DbType::Mssql.is_sql());
        assert!(!DbType::Mongodb.is_sql(), "MongoDB should not be SQL");
    }

    #[test]
    fn test_db_type_display() {
        assert_eq!(format!("{}", DbType::Mysql), "mysql");
        assert_eq!(format!("{}", DbType::Postgres), "postgres");
        assert_eq!(format!("{}", DbType::Mongodb), "mongodb");
    }

    #[test]
    fn test_db_type_serde_roundtrip() {
        for (variant, expected) in [
            (DbType::Mysql, "\"mysql\""),
            (DbType::Postgres, "\"postgres\""),
            (DbType::Sqlite, "\"sqlite\""),
            (DbType::Mssql, "\"mssql\""),
            (DbType::Mongodb, "\"mongodb\""),
        ] {
            let serialized = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialized, expected);
            let deserialized: DbType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, variant);
        }
    }

    #[test]
    fn test_db_type_equality() {
        assert_eq!(DbType::Mysql, DbType::Mysql);
        assert_ne!(DbType::Mysql, DbType::Postgres);
        assert_ne!(DbType::Sqlite, DbType::Mongodb);
    }

    // --- TableSchema default ---

    #[test]
    fn test_table_schema_default_values() {
        let schema = TableSchema::default();
        assert!(schema.table.is_empty(), "Default table name should be empty");
        assert!(schema.columns.is_empty(), "Default columns should be empty");
        assert!(schema.primary_key.columns.is_empty(), "Default PK should be empty");
        assert!(schema.foreign_keys.is_empty());
        assert!(schema.details.is_empty());
        assert!(schema.indexes.is_empty());
        assert!(!schema.auto_generate);
        assert!(!schema.seed);
        assert!(!schema.get.enable_method);
        assert!(!schema.post.enable_method);
        assert!(!schema.put.enable_method);
        assert!(!schema.del.enable_method);
    }

    #[test]
    fn test_detail_schema_serde() {
        let json = r#"{
            "field": "items",
            "target_table": "transaction_purchase_order_item",
            "foreign_key_column": "po_id",
            "parent_key_column": "id",
            "columns": ["material_id", "qty_ordered", "unit_price"],
            "update_strategy": "replace",
            "cascade_delete": true
        }"#;
        let detail: DetailSchema = serde_json::from_str(json).unwrap();
        assert_eq!(detail.field, "items");
        assert_eq!(detail.target_table, "transaction_purchase_order_item");
        assert_eq!(detail.foreign_key_column, "po_id");
        assert_eq!(detail.parent_key_column.as_deref(), Some("id"));
        assert_eq!(detail.columns, vec!["material_id", "qty_ordered", "unit_price"]);
        assert_eq!(detail.update_strategy.as_deref(), Some("replace"));
        assert_eq!(detail.cascade_delete, Some(true));
    }

    // --- Column default ---

    #[test]
    fn test_column_default_values() {
        let col = Column::default();
        assert!(col.name.is_empty());
        assert!(!col.auto_increment);
        assert!(!col.nullable);
        assert!(col.type_data.is_empty());
        assert!(!col.encrypt);
    }

    // --- WebResponse ---

    #[test]
    fn test_web_response_serialization() {
        let resp = WebResponse {
            success: true,
            message: "OK".to_string(),
            total_data: 1,
            data: serde_json::json!({"id": 1}),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["message"], "OK");
        assert_eq!(json["total_data"], 1);
    }
}
