use serde::{Deserialize, Serialize};

use crate::auth::ClaimsConverter;

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
    pub get: GetOperation,
    pub post: OperationPostPut,
    pub put: OperationPostPut,
    pub del: OperationDelete,
    pub patch: Patch,
    pub trace: Trace,
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
            get: GetOperation {
                columns: vec![],
                parameters: vec![],
                join_tables: vec![],
                column_groups: vec![],
                having: vec![],
                order_by: vec![],
            },
            post: OperationPostPut {
                validate_data: "".to_string(),
                pre_process: "".to_string(),
                columns: vec![],
                post_process: "".to_string(),
            },
            put: OperationPostPut {
                validate_data: "".to_string(),
                pre_process: "".to_string(),
                columns: vec![],
                post_process: "".to_string(),
            },
            del: OperationDelete {
                pre_process: "".to_string(),
                columns: vec![],
                type_delete: "soft".to_string(),
                post_process: "".to_string(),
            },
            patch: Patch {
                pre_process_sp: "".to_string(),
                parameters: vec![],
                return_mode: "".to_string(),
            },
            trace: Trace {
                insert_into: "".to_string(),
                column_inserts: vec![],
                column_selects: vec![],
                parameters: vec![],
                join_tables: vec![],
                column_groups: vec![],
                column_conflicts: vec![],
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
pub struct GetOperation {
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
pub struct OperationPostPut {
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
    pub pre_process: String,
    pub columns: Vec<String>,
    pub type_delete: String,
    pub post_process: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Patch {
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
