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

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct ActionTrigger {
    #[serde(default)]
    pub name: String,
    /// Event type: "on_update", "on_status_change", "on_create", "on_delete"
    #[serde(default = "default_trigger_event")]
    pub event: String,
    /// Condition to evaluate before executing actions
    #[serde(default)]
    pub condition: Option<TriggerCondition>,
    /// List of actions to execute sequentially within the same database transaction
    #[serde(default)]
    pub actions: Vec<TriggerAction>,
}

fn default_trigger_event() -> String {
    "on_update".to_string()
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct TriggerCondition {
    /// Field to check for changes (e.g. "status")
    #[serde(default)]
    pub field: String,
    /// Expected previous value or list of allowed previous values (e.g. ["APPROVED", "PENDING"])
    #[serde(default)]
    pub from: Option<serde_json::Value>,
    /// Target value that triggers the action (e.g. "SHIPPED")
    #[serde(default)]
    pub to: Option<serde_json::Value>,
    /// Optional boolean expression or formula (e.g. "total_net > 0")
    #[serde(default)]
    pub expression: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct TriggerAction {
    #[serde(default)]
    pub name: Option<String>,
    /// Action type: "iterate_detail", "update", "insert", "insert_batch", "sql"
    #[serde(default, rename = "type")]
    pub action_type: String,
    /// Target table
    #[serde(default)]
    pub target_table: String,
    /// Detail table to iterate over (used when type is "iterate_detail")
    #[serde(default)]
    pub detail_table: Option<String>,
    /// Foreign key column in detail table pointing to parent (e.g. "sales_order_id")
    #[serde(default)]
    pub foreign_key: Option<String>,
    /// Filter condition for update (e.g. { "product_id": "{item.product_id}" })
    #[serde(default)]
    pub filter: Option<serde_json::Map<String, serde_json::Value>>,
    /// Set map for updates (e.g. { "qty": "qty - {item.qty}" })
    #[serde(default)]
    pub set: Option<serde_json::Map<String, serde_json::Value>>,
    /// Single row fields for insert (e.g. { "customer_id": "{parent.customer_id}", ... })
    #[serde(default, alias = "values")]
    pub fields: Option<serde_json::Map<String, serde_json::Value>>,
    /// Multiple rows for insert_batch (e.g. GL journal lines)
    #[serde(default)]
    pub rows: Option<Vec<serde_json::Map<String, serde_json::Value>>>,
    /// Sub-actions for iterate_detail
    #[serde(default)]
    pub actions: Option<Vec<TriggerAction>>,
    /// Raw or parameterized SQL statement (for type "sql")
    #[serde(default)]
    pub statement: Option<String>,
    /// SQL parameters (for type "sql")
    #[serde(default)]
    pub params: Option<Vec<String>>,
    /// Optional validations (e.g. min quantity checks)
    #[serde(default)]
    pub validate: Option<TriggerValidation>,
    /// Execute update with row-level lock (FOR UPDATE) or atomic guard
    #[serde(default)]
    pub atomic: Option<bool>,
    /// Alias for lookup result (e.g. "product", "customer", "category")
    #[serde(default, rename = "as")]
    pub alias: Option<String>,
    /// Accumulate expressions across detail iterations (e.g. { "total_cogs": "item.qty * product.cost_price" })
    #[serde(default)]
    pub accumulate: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional condition to guard this specific action
    #[serde(default)]
    pub condition: Option<TriggerCondition>,
    /// Whether lookup is optional (if false/omitted, failure returns error and aborts tx)
    #[serde(default)]
    pub optional: Option<bool>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct TriggerValidation {
    /// Minimum allowed values per column after update (e.g. { "qty": 0 })
    #[serde(default)]
    pub min: Option<serde_json::Map<String, serde_json::Value>>,
    /// Custom error message if validation fails
    #[serde(default)]
    pub error_message: Option<String>,
    /// Assert balanced double-entry (e.g. GL journal debit == credit)
    #[serde(default)]
    pub assert_balanced: Option<AssertBalanced>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct AssertBalanced {
    #[serde(default)]
    pub debit_field: String,
    #[serde(default)]
    pub credit_field: String,
    #[serde(default)]
    pub tolerance: Option<f64>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateMachine {
    #[serde(default)]
    pub field: String,
    #[serde(default)]
    pub initial: Option<String>,
    #[serde(default)]
    pub transitions: Vec<StateTransition>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateTransition {
    pub from: serde_json::Value,
    pub to: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct LockedWhen {
    #[serde(default)]
    pub except_columns: Vec<String>,
    #[serde(flatten)]
    pub conditions: serde_json::Map<String, serde_json::Value>,
}

impl LockedWhen {
    pub fn get_conditions(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.conditions
    }

    pub fn get_except_columns(&self) -> &[String] {
        &self.except_columns
    }
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
    #[serde(default, alias = "triggers")]
    pub action_triggers: Vec<ActionTrigger>,
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
    pub locked_when: Option<LockedWhen>,
    #[serde(default)]
    pub state_machine: Option<StateMachine>,
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
            action_triggers: vec![],
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
            locked_when: None,
            state_machine: None,
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

    // --- ActionTrigger Serde ---

    #[test]
    fn test_action_trigger_serde_roundtrip() {
        let json = r#"{
            "name": "sales_order_fulfillment",
            "event": "on_update",
            "condition": {
                "field": "status",
                "from": ["APPROVED", "PENDING"],
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "foreign_key": "sales_order_id",
                    "actions": [
                        {
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 }, "error_message": "Insufficient stock" }
                        }
                    ]
                },
                {
                    "type": "insert",
                    "target_table": "transaction_account_receivable",
                    "fields": {
                        "customer_id": "{parent.customer_id}",
                        "total_receivable": "{parent.total_net}",
                        "status": "UNPAID"
                    }
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(json).unwrap();
        assert_eq!(trigger.name, "sales_order_fulfillment");
        assert_eq!(trigger.event, "on_update");
        let cond = trigger.condition.unwrap();
        assert_eq!(cond.field, "status");
        assert_eq!(cond.to, Some(serde_json::json!("SHIPPED")));
        assert_eq!(trigger.actions.len(), 2);
        assert_eq!(trigger.actions[0].action_type, "iterate_detail");
        assert_eq!(trigger.actions[0].detail_table.as_deref(), Some("transaction_sales_order_item"));
        assert_eq!(trigger.actions[1].action_type, "insert");
        assert_eq!(trigger.actions[1].target_table, "transaction_account_receivable");
    }

    #[test]
    fn test_table_schema_backward_compatibility_without_triggers() {
        let json = r#"{
            "table": "legacy_table",
            "columns": []
        }"#;
        let schema: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.table, "legacy_table");
        assert!(schema.action_triggers.is_empty(), "Schemas without action_triggers should default to empty vec");
    }

    #[test]
    fn test_table_schema_with_triggers_alias() {
        let json = r#"{
            "table": "sample_table",
            "triggers": [
                {
                    "name": "auto_log",
                    "event": "on_update",
                    "actions": []
                }
            ]
        }"#;
        let schema: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.action_triggers.len(), 1);
        assert_eq!(schema.action_triggers[0].name, "auto_log");
    }

    #[test]
    fn test_erp_state_machine_and_locked_when_serde() {
        let json = r#"{
            "table": "transaction_sales_order",
            "locked_when": {
                "status": ["SHIPPED", "PAID", "VOID"]
            },
            "state_machine": {
                "field": "status",
                "initial": "DRAFT",
                "transitions": [
                    { "from": "DRAFT", "to": "APPROVED", "roles": ["manager"] },
                    { "from": "APPROVED", "to": "SHIPPED", "roles": ["warehouse"] },
                    { "from": ["DRAFT", "APPROVED"], "to": "CANCELLED", "roles": ["admin"] }
                ]
            }
        }"#;

        let schema: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.table, "transaction_sales_order");

        let lock_cfg = schema.locked_when.unwrap();
        let conds = lock_cfg.get_conditions();
        assert!(conds.contains_key("status"));

        let sm = schema.state_machine.unwrap();
        assert_eq!(sm.field, "status");
        assert_eq!(sm.transitions.len(), 3);
        assert_eq!(sm.transitions[0].to, "APPROVED");
        assert_eq!(sm.transitions[0].roles, vec!["manager"]);
    }

    #[test]
    fn test_trigger_action_assert_balanced_serde() {
        let json = r#"{
            "name": "gl_posting",
            "type": "insert_batch",
            "target_table": "transaction_general_ledger_line",
            "atomic": true,
            "validate": {
                "assert_balanced": {
                    "debit_field": "debit",
                    "credit_field": "credit",
                    "tolerance": 0.01
                }
            }
        }"#;

        let action: TriggerAction = serde_json::from_str(json).unwrap();
        assert_eq!(action.action_type, "insert_batch");
        assert_eq!(action.atomic, Some(true));
        let validate = action.validate.unwrap();
        let balanced = validate.assert_balanced.unwrap();
        assert_eq!(balanced.debit_field, "debit");
        assert_eq!(balanced.credit_field, "credit");
        assert_eq!(balanced.tolerance, Some(0.01));
    }

    #[test]
    fn test_trigger_action_lookup_and_accumulate_serde() {
        let json = r#"{
            "name": "lookup_product_cost",
            "type": "lookup",
            "target_table": "master_product",
            "as": "product",
            "filter": { "id": "{item.product_id}" },
            "optional": false,
            "accumulate": {
                "total_cogs": "item.qty * product.cost_price"
            }
        }"#;

        let action: TriggerAction = serde_json::from_str(json).unwrap();
        assert_eq!(action.action_type, "lookup");
        assert_eq!(action.target_table, "master_product");
        assert_eq!(action.alias, Some("product".to_string()));
        assert_eq!(action.optional, Some(false));
        let acc = action.accumulate.unwrap();
        assert_eq!(acc.get("total_cogs").unwrap(), "item.qty * product.cost_price");
    }
}

