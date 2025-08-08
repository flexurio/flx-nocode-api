
use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub routes: Vec<String>,
    pub route_publics: Vec<String>,
}

pub struct ParamJoin {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table: String,
    pub primary_key: PrimaryKey,
    pub columns: Vec<Column>,
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
            primary_key: PrimaryKey {
                columns: vec![],
            },
            columns: vec![],
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
                before: "".to_string(),
                columns: vec![], 
                after: "".to_string(),
            },
            put: OperationPostPut { 
                before: "".to_string(),
                columns: vec![], 
                after: "".to_string(),
            },
            del: OperationDelete { 
                columns: vec![],
                type_delete: "soft".to_string()
            },
            patch: Patch {
                pre_process_sp: "".to_string(),
                parameters: vec![],
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
    pub columns: Vec<String>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub auto_increment: bool,
    pub nullable: bool,
    pub type_data: String,
    pub function: String,
    pub encrypt: bool
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Redis {
    pub keys: Vec<String>,
    pub ttl: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetOperation {
    pub columns: Vec<String>,
    pub parameters: Vec<String>,
    pub join_tables: Vec<JoinTable>,
    pub column_groups: Vec<String>,
    pub having: Vec<String>,
    pub order_by: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinTable {
    pub table: String,
    pub columns: Vec<String>,
    pub logical: String,
    pub type_join: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub columns: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperationPostPut {
    pub before: String,
    pub columns: Vec<String>,
    pub after: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperationDelete {
    pub columns: Vec<String>,
    pub type_delete: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Patch {
    pub pre_process_sp: String,
    pub parameters: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trace {
    pub insert_into: String,
    pub column_inserts: Vec<String>,
    pub column_selects: Vec<String>,
    pub parameters: Vec<String>,
    pub join_tables: Vec<JoinTable>,
    pub column_groups: Vec<String>,
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
    pub level: String,
    pub message: String,
    pub timestamp: String,
}

