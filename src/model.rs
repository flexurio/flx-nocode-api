use serde::{Deserialize, Serialize};

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
    pub routes: Vec<String>,
    pub route_publics: Vec<String>,
    pub converter_token: ClaimsConverter,
}

pub struct ParamJoin {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub column: String,
    pub reference_table: String,
    pub reference_column: String,
    pub on_delete: String, // "cascade", "restrict", "set null", "no action"
    pub on_update: String, // "cascade", "restrict", "set null", "no action"
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// create struct pivot ForeingKey to triger action in reference table
pub struct ReferenceForeignKey {
    pub table: String,
    pub column: String,
    pub on_delete_action: ReferenceForeignKeyAction,
    pub on_update_action: ReferenceForeignKeyAction,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReferenceForeignKeyAction {
    pub table: String,
    pub column: String,
    pub action: String,      // "cascade", "restrict", "set null", "no action"
    pub type_delete: String, // soft or hard
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table: String,
    pub primary_key: PrimaryKey,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<Index>,
    pub redis: Redis,
    pub get: OperationGet,
    pub post: OperationPost,
    pub put: OperationPut,
    pub del: OperationDelete,
    pub patch: Patch,
    pub trace: Trace,
    #[serde(default)]
    pub auto_generate: bool,
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
            collate: "".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryKey {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Redis {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub ttl: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    #[serde(default)]
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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


#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct WebResponse {
    pub success: bool,
    pub message: String,
    pub total_data: i32,
    pub data: serde_json::Value,
}

// create struct for logging
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
        assert!(schema.indexes.is_empty());
        assert!(!schema.auto_generate);
        assert!(!schema.get.enable_method);
        assert!(!schema.post.enable_method);
        assert!(!schema.put.enable_method);
        assert!(!schema.del.enable_method);
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
