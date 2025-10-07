use serde::{Deserialize, Serialize};
use actix_web::{body::BoxBody, http::header, HttpResponse, Responder};

use crate::auth::ClaimsConverter;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub routes: Vec<String>,
    pub route_publics: Vec<String>,
    pub converter_token: ClaimsConverter,
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

// default value for TableSchema - optimized with String::new() instead of "".to_string()
impl Default for TableSchema {
    fn default() -> Self {
        TableSchema {
            table: String::new(),
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
                validate_data: String::new(),
                pre_process: String::new(),
                columns: vec![],
                post_process: String::new(),
            },
            put: OperationPostPut {
                validate_data: String::new(),
                pre_process: String::new(),
                columns: vec![],
                post_process: String::new(),
            },
            del: OperationDelete {
                pre_process: String::new(),
                columns: vec![],
                type_delete: "soft".to_string(), // Keep this as it's a meaningful default
                post_process: String::new(),
            },
            patch: Patch {
                pre_process_sp: String::new(),
                parameters: vec![],
                return_mode: String::new(),
            },
            trace: Trace {
                insert_into: String::new(),
                parameters: vec![],
                column_conflicts: vec![],
                column_inserts: vec![],
                column_selects: vec![],
                join_tables: vec![],
                column_groups: vec![],
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
    pub name: String,
    pub auto_increment: bool,
    pub nullable: bool,
    pub type_data: String,
    pub function: String,
    pub encrypt: bool,
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
    pub validate_data: String,
    pub pre_process: String,
    pub columns: Vec<String>,
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
    pub pre_process_sp: String,
    pub parameters: Vec<String>,
    #[serde(default)]
    pub return_mode: String, // "" | "rows" | "affected"
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
    pub data: sonic_rs::Value,
}

// Custom responder to avoid serde_json overhead and leverage sonic_rs directly
impl Responder for WebResponse {
    type Body = BoxBody;

    fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        // Build a temporary sonic_rs::Value object manually to ensure stable order
        let mut obj = sonic_rs::Object::with_capacity(4);
        obj.insert("success", sonic_rs::Value::from(self.success));
        obj.insert("message", sonic_rs::Value::from(self.message.as_str()));
        obj.insert("total_data", sonic_rs::Value::from(self.total_data));
        obj.insert("data", self.data);
        let root = sonic_rs::Value::from(obj);
        // Serialize using sonic_rs (already in dependency graph)
        match sonic_rs::to_vec(&root) {
            Ok(buf) => HttpResponse::Ok()
                .insert_header((header::CONTENT_TYPE, "application/json"))
                .body(buf),
            Err(e) => {
                // Fallback minimal JSON error payload
                let fallback = format!("{{\"success\":false,\"message\":\"serialize error: {}\"}}", e).into_bytes();
                HttpResponse::InternalServerError()
                    .insert_header((header::CONTENT_TYPE, "application/json"))
                    .body(fallback)
            }
        }
    }
}

// create struct for logging
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Log {
    pub level: String,
    pub message: String,
    pub timestamp: String,
}
