//! MCP tools: discovery, schema introspection and CRUD over configured entities.
//!
//! | Tool                  | REST equivalent                      | Annotations             |
//! |-----------------------|--------------------------------------|-------------------------|
//! | `list_entities`       | —  (routes.json + entity metadata)   | read-only, idempotent   |
//! | `describe_entity`     | —  (entity JSON schema)              | read-only, idempotent   |
//! | `query_records`       | `GET /<entity>?col.op=value`         | read-only, idempotent   |
//! | `create_record`       | `POST /<entity>`                     | additive                |
//! | `update_record`       | `PUT /<entity>/{id}`                 | destructive, idempotent |
//! | `patch_record`        | `PATCH /<entity>/{id}`               | destructive             |
//! | `delete_record`       | `DELETE /<entity>/{id}`              | destructive, idempotent |
//! | `run_procedure`       | `PATCH /<entity>` (stored procedure) | destructive             |
//! | `validate_entity`     | `GET /validate/<entity>`             | read-only, idempotent   |
//! | `health`              | `GET /healthz`                       | read-only, idempotent   |
//!
//! Every tool that touches data goes through the existing service layer via
//! [`super::ctx::ServiceRequest`], so `rules.json`, validation, triggers,
//! caching, the write queue and the audit trail all apply unchanged.
//!
//! Input errors the model can fix (unknown entity, disabled operation, bad
//! arguments, service validation failure) are returned as tool results with
//! `isError: true` — per the MCP spec — so the model can self-correct. Only an
//! unknown tool name is a JSON-RPC protocol error.

use std::sync::Arc;

use actix_web::http::Method;
use actix_web::{Responder, web};
use rmcp::ErrorData;
use rmcp::handler::server::common::{schema_for_empty_input, schema_for_input, schema_for_output};
use rmcp::model::{CallToolResult, ContentBlock, JsonObject, Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::McpConfig;
use super::ctx::{CallerIdentity, ServiceOutcome, ServiceRequest, outcome_from_response};
use crate::config::{CONFIG, SCHEMAS};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::metrics::METRICS;
use crate::model::TableSchema;

// ── Tool names ────────────────────────────────────────────────────────────────

pub const LIST_ENTITIES: &str = "list_entities";
pub const DESCRIBE_ENTITY: &str = "describe_entity";
pub const QUERY_RECORDS: &str = "query_records";
pub const CREATE_RECORD: &str = "create_record";
pub const UPDATE_RECORD: &str = "update_record";
pub const PATCH_RECORD: &str = "patch_record";
pub const DELETE_RECORD: &str = "delete_record";
pub const RUN_PROCEDURE: &str = "run_procedure";
pub const VALIDATE_ENTITY: &str = "validate_entity";
pub const HEALTH: &str = "health";

/// GET parameters with engine-level meaning (pagination / ordering / search).
const CONTROL_PARAMS: [&str; 5] = ["page", "limit", "sort", "ascending", "search"];

/// Core tables that are never exposed when authentication is disabled
/// (mirrors `routes::configure_routes`).
const CORE_AUTH_TABLES: [&str; 2] = ["flx_users", "flx_roles"];

// ── Operations ────────────────────────────────────────────────────────────────

/// A data operation on an entity, with its REST method/path equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Read,
    Create,
    Update,
    Patch,
    Delete,
    RunProcedure,
    Validate,
}

impl Op {
    pub const ALL: [Op; 7] = [
        Op::Read,
        Op::Create,
        Op::Update,
        Op::Patch,
        Op::Delete,
        Op::RunProcedure,
        Op::Validate,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Create => "create",
            Op::Update => "update",
            Op::Patch => "patch",
            Op::Delete => "delete",
            Op::RunProcedure => "run_procedure",
            Op::Validate => "validate",
        }
    }

    pub fn method(&self) -> Method {
        match self {
            Op::Read | Op::Validate => Method::GET,
            Op::Create => Method::POST,
            Op::Update => Method::PUT,
            Op::Patch | Op::RunProcedure => Method::PATCH,
            Op::Delete => Method::DELETE,
        }
    }

    /// REST path this operation is authorized against in `rules.json`.
    pub fn path(&self, entity: &str, id: Option<&str>) -> String {
        match self {
            Op::Read | Op::Create | Op::RunProcedure => format!("/{}", entity),
            Op::Update | Op::Patch | Op::Delete => {
                format!("/{}/{}", entity, id.unwrap_or("{id}"))
            }
            Op::Validate => format!("/validate/{}", entity),
        }
    }

    /// Whether the entity schema enables this operation (same switches as REST).
    pub fn enabled(&self, s: &TableSchema) -> bool {
        match self {
            Op::Read => s.get.enable_method,
            Op::Create => s.post.enable_method,
            // PATCH /<entity>/{id} is registered together with PUT.
            Op::Update | Op::Patch => s.put.enable_method,
            Op::Delete => s.del.enable_method,
            Op::RunProcedure => s.patch.enable_method,
            Op::Validate => true,
        }
    }

    #[allow(dead_code)]
    pub fn is_write(&self) -> bool {
        !matches!(self, Op::Read | Op::Validate)
    }
}

// ── Entity registry ───────────────────────────────────────────────────────────

/// Entities exposed over MCP, in `routes.json` order.
pub fn exposed_entities(require_auth: bool) -> Vec<(String, Arc<TableSchema>)> {
    CONFIG
        .routes
        .iter()
        .filter(|r| require_auth || !CORE_AUTH_TABLES.contains(&r.as_str()))
        .filter_map(|r| SCHEMAS.0.get(r).map(|s| (r.clone(), s.clone())))
        .collect()
}

fn entity_names_for(require_auth: bool, op: Op) -> Vec<String> {
    exposed_entities(require_auth)
        .into_iter()
        .filter(|(_, s)| op.enabled(s))
        .map(|(r, _)| r)
        .collect()
}

pub fn find_entity(require_auth: bool, entity: &str) -> Option<Arc<TableSchema>> {
    exposed_entities(require_auth)
        .into_iter()
        .find(|(r, _)| r == entity)
        .map(|(_, s)| s)
}

fn resolve_entity(require_auth: bool, entity: &str) -> ToolResult<Arc<TableSchema>> {
    find_entity(require_auth, entity).ok_or_else(|| {
        let names: Vec<String> = exposed_entities(require_auth)
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        fail(format!(
            "Unknown entity '{}'. Available entities: {}. Call `{}` for details.",
            entity,
            names.join(", "),
            LIST_ENTITIES
        ))
    })
}

fn require_op(entity: &str, schema: &TableSchema, op: Op) -> ToolResult<()> {
    if op.enabled(schema) {
        Ok(())
    } else {
        Err(fail(format!(
            "Operation '{}' is disabled for entity '{}' (enable_method=false in its entity config).",
            op.as_str(),
            entity
        )))
    }
}

// ── Parameter helpers ─────────────────────────────────────────────────────────

/// One entry of `get.parameters`, decoded.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct FilterParam {
    /// Exact key to use in `query_records.filters` (e.g. `email.eq`).
    pub name: String,
    /// `filter` (single column), `or_filter` (`a.like|b.like`), `control`
    /// (`page`, `limit`, `sort`, `ascending`, `search`) or `join` (`paramjoin*`).
    pub kind: String,
    /// Column(s) addressed by the filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Operator suffix (`eq`, `like`, `lt`, `lte`, `gt`, `gte`, `is`, `nin`, `between`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// True when the parameter is mandatory (declared with a leading `*`).
    pub required: bool,
}

pub fn parse_filter_param(raw: &str) -> FilterParam {
    let required = raw.starts_with('*');
    let name = raw.trim_start_matches('*').to_string();

    if CONTROL_PARAMS.contains(&name.as_str()) {
        return FilterParam {
            name,
            kind: "control".into(),
            column: None,
            operator: None,
            required,
        };
    }
    if name.contains("paramjoin") {
        return FilterParam {
            name,
            kind: "join".into(),
            column: None,
            operator: None,
            required,
        };
    }
    let split = |part: &str| -> (String, Option<String>) {
        match part.rsplit_once('.') {
            Some((col, op)) => (col.to_string(), Some(op.to_string())),
            None => (part.to_string(), Some("eq".to_string())),
        }
    };
    if name.contains('|') {
        let parts: Vec<(String, Option<String>)> = name.split('|').map(split).collect();
        let cols = parts
            .iter()
            .map(|(c, _)| c.as_str())
            .collect::<Vec<_>>()
            .join("|");
        let op = parts.first().and_then(|(_, o)| o.clone());
        return FilterParam {
            name: name.clone(),
            kind: "or_filter".into(),
            column: Some(cols),
            operator: op,
            required,
        };
    }
    let (col, op) = split(&name);
    FilterParam {
        name,
        kind: "filter".into(),
        column: Some(col),
        operator: op,
        required,
    }
}

fn filter_params(schema: &TableSchema) -> Vec<FilterParam> {
    schema
        .get
        .parameters
        .iter()
        .map(|p| parse_filter_param(p))
        .collect()
}

/// Mask values whose key looks sensitive, recursively.
pub fn redact(value: &mut Value, fields: &[String]) {
    if fields.is_empty() {
        return;
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let lower = k.to_ascii_lowercase();
                let key = lower.rsplit('.').next().unwrap_or(&lower);
                let sensitive = fields.iter().any(|f| {
                    key == f
                        || key.ends_with(&format!("_{}", f))
                        || key.starts_with(&format!("{}_", f))
                });
                if sensitive && !v.is_null() {
                    *v = Value::String("***REDACTED***".into());
                } else {
                    redact(v, fields);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| redact(v, fields)),
        _ => {}
    }
}

// ── Tool arguments ────────────────────────────────────────────────────────────

/// Primary-key value of a record (string or integer).
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum RecordId {
    Int(i64),
    Text(String),
}

impl RecordId {
    fn as_path_segment(&self) -> ToolResult<String> {
        let s = match self {
            RecordId::Int(n) => n.to_string(),
            RecordId::Text(s) => s.trim().to_string(),
        };
        if s.is_empty() || s.contains('/') {
            return Err(fail("`id` must be a non-empty value without '/'."));
        }
        Ok(s)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntityArgs {
    /// Entity (route) name exactly as returned by `list_entities`.
    pub entity: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryRecordsArgs {
    /// Entity (route) name exactly as returned by `list_entities`.
    pub entity: String,
    /// Filters keyed by the entity's declared filter parameters
    /// (`describe_entity` → `operations.read.parameters`), e.g.
    /// `{"email.eq": "a@b.c", "join_date.between": "2024-01-01,2024-12-31"}`.
    /// Operators: eq, like, lt, lte, gt, gte, is, nin (JSON array or CSV), between ("a,b").
    #[serde(default)]
    pub filters: Option<Map<String, Value>>,
    /// 1-based page number (only honoured when `page` is a declared parameter).
    #[serde(default)]
    pub page: Option<u32>,
    /// Page size (only honoured when `limit` is declared; capped server-side).
    #[serde(default)]
    pub limit: Option<u32>,
    /// Sort column(s), comma separated (only honoured when `sort` is declared).
    #[serde(default)]
    pub sort: Option<String>,
    /// Ascending order when true (only honoured when `ascending` is declared).
    #[serde(default)]
    pub ascending: Option<bool>,
    /// Free-text search over primary-key and indexed columns (when `search` is declared).
    #[serde(default)]
    pub search: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateRecordArgs {
    /// Entity (route) name that allows `create`.
    pub entity: String,
    /// Column values for the new record. Master-detail entities accept their
    /// detail arrays here too (see `describe_entity` → `details[].field`).
    pub data: Map<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateRecordArgs {
    /// Entity (route) name that allows `update`.
    pub entity: String,
    /// Primary-key value of the record to change.
    pub id: RecordId,
    /// Column values to write.
    pub data: Map<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordArgs {
    /// Entity (route) name that allows `delete`.
    pub entity: String,
    /// Primary-key value of the record to delete.
    pub id: RecordId,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunProcedureArgs {
    /// Entity (route) name that allows `run_procedure`.
    pub entity: String,
    /// Named parameters declared in `describe_entity` → `operations.run_procedure.parameters`.
    #[serde(default)]
    pub parameters: Option<Map<String, Value>>,
}

// ── Tool outputs (structuredContent) ──────────────────────────────────────────

#[derive(Debug, Serialize, JsonSchema)]
pub struct EntitySummary {
    /// Entity (route) name to pass as `entity` to other tools.
    pub name: String,
    /// Physical table / collection name.
    pub table: String,
    /// Operations enabled in the entity config.
    pub operations: Vec<String>,
    /// Operations the current caller is authorized for by `rules.json`
    /// (omitted when authorization is not enforced for this entity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_operations: Option<Vec<String>>,
    /// Primary-key column(s).
    pub primary_key: Vec<String>,
    /// Filter keys accepted by `query_records`.
    pub filter_parameters: Vec<String>,
    /// Payload fields holding detail (child) arrays for master-detail writes.
    pub detail_fields: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListEntitiesOutput {
    pub total: usize,
    pub entities: Vec<EntitySummary>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnInfo {
    pub name: String,
    pub type_data: String,
    pub nullable: bool,
    pub auto_increment: bool,
    /// Value is stored encrypted at rest.
    pub encrypted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Value is generated by the engine (`function`, e.g. running numbers).
    pub generated: bool,
    /// True for primary-key columns.
    pub primary_key: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ForeignKeyInfo {
    pub column: String,
    pub reference_table: String,
    pub reference_column: String,
    pub on_delete: String,
    pub on_update: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DetailInfo {
    /// Payload field holding the detail array.
    pub field: String,
    pub target_entity: String,
    pub foreign_key_column: String,
    pub update_strategy: String,
    pub cascade_delete: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ReadOpInfo {
    pub enabled: bool,
    pub columns: Vec<String>,
    pub parameters: Vec<FilterParam>,
    pub joins: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteOpInfo {
    pub enabled: bool,
    /// Columns the operation writes (empty = engine default).
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DeleteOpInfo {
    pub enabled: bool,
    /// `soft` (sets deleted_at) or `hard` (row removed).
    pub type_delete: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ProcedureOpInfo {
    pub enabled: bool,
    pub parameters: Vec<String>,
    pub return_mode: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct OperationsInfo {
    pub read: ReadOpInfo,
    pub create: WriteOpInfo,
    /// Used by both `update_record` (PUT) and `patch_record` (PATCH).
    pub update: WriteOpInfo,
    pub delete: DeleteOpInfo,
    pub run_procedure: ProcedureOpInfo,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DescribeEntityOutput {
    pub name: String,
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub details: Vec<DetailInfo>,
    pub operations: OperationsInfo,
    /// Document-lock rule (`locked_when`), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked_when: Option<Value>,
    /// Status workflow (`state_machine`), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_machine: Option<Value>,
    /// Ready-to-use `query_records` arguments for this entity.
    pub example_query: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct QueryRecordsOutput {
    pub entity: String,
    /// Total matching rows reported by the engine.
    pub total: i64,
    /// Rows included in this result.
    pub returned: usize,
    /// True when rows were cut to `MCP_MAX_ROWS`; narrow filters or paginate.
    pub truncated: bool,
    pub rows: Vec<Value>,
    /// Arguments dropped because the entity does not declare them.
    pub ignored_parameters: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteOutput {
    pub entity: String,
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// HTTP status the equivalent REST call would return.
    pub status: u16,
    /// True when the write was accepted by the Redis write queue (async).
    pub queued: bool,
    pub message: String,
    pub total_data: i64,
    pub data: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidateOutput {
    pub entity: String,
    pub status: u16,
    pub valid: bool,
    pub message: String,
    pub data: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct HealthOutput {
    /// `ok` when the database answers, `degraded` otherwise.
    pub status: String,
    pub db: String,
    pub db_type: String,
    pub server_version: String,
    pub entities: usize,
    pub write_tools_enabled: bool,
}

// ── Result helpers ────────────────────────────────────────────────────────────

pub fn tool_error(msg: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(msg.into())])
}

/// A tool failure the model can act on; rendered as an `isError: true` result.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolFailure(pub String);

type ToolResult<T> = Result<T, ToolFailure>;

fn fail(msg: impl Into<String>) -> ToolFailure {
    ToolFailure(msg.into())
}

fn structured<T: Serialize>(out: &T) -> CallToolResult {
    match serde_json::to_value(out) {
        Ok(v) => CallToolResult::structured(v),
        Err(e) => tool_error(format!("Failed to serialize tool output: {}", e)),
    }
}

fn service_error(outcome: &ServiceOutcome) -> ToolFailure {
    let hint = match outcome.status {
        401 => {
            " Hint: the bearer token is missing, invalid or not authorized by rules.json for this entity/method."
        }
        403 => " Hint: access denied by rules.json.",
        400 | 422 => {
            " Hint: call `describe_entity` to check required columns, parameters and types."
        }
        404 | 424 => " Hint: the entity config or record was not found.",
        429 => " Hint: rate limited; retry later.",
        _ => "",
    };
    fail(format!(
        "Request failed (HTTP {}): {}.{}",
        outcome.status,
        outcome.message(),
        hint
    ))
}

fn parse_args<T: DeserializeOwned>(args: Option<JsonObject>) -> ToolResult<T> {
    serde_json::from_value(Value::Object(args.unwrap_or_default()))
        .map_err(|e| fail(format!("Invalid arguments: {}", e)))
}

// ── Tool definitions ──────────────────────────────────────────────────────────

/// schemars annotates Rust integer widths with `format` values (`uint`, `int64`,
/// `uint16`, …) that are not JSON Schema formats; strict validators warn on
/// them. Range constraints (`minimum`) already carry the useful information.
fn strip_non_standard_formats(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let drop = matches!(
                map.get("format").and_then(Value::as_str),
                Some(
                    "int"
                        | "uint"
                        | "int8"
                        | "uint8"
                        | "int16"
                        | "uint16"
                        | "int32"
                        | "uint32"
                        | "int64"
                        | "uint64"
                        | "float"
                        | "double"
                )
            );
            if drop {
                map.remove("format");
            }
            map.values_mut().for_each(strip_non_standard_formats);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_non_standard_formats),
        _ => {}
    }
}

fn clean_schema(schema: Arc<JsonObject>) -> Arc<JsonObject> {
    let mut v = Value::Object(schema.as_ref().clone());
    strip_non_standard_formats(&mut v);
    match v {
        Value::Object(map) => Arc::new(map),
        _ => schema,
    }
}

fn input_schema<T: JsonSchema + 'static>() -> Arc<JsonObject> {
    clean_schema(schema_for_input::<T>().expect("tool argument types are JSON objects"))
}

fn output_schema<T: JsonSchema + 'static>() -> Arc<JsonObject> {
    clean_schema(schema_for_output::<T>())
}

/// Constrain the `entity` property to the given names (helps the model pick valid values).
fn with_entity_enum(schema: Arc<JsonObject>, names: &[String]) -> Arc<JsonObject> {
    let mut obj = schema.as_ref().clone();
    if let Some(Value::Object(props)) = obj.get_mut("properties")
        && let Some(Value::Object(entity)) = props.get_mut("entity")
    {
        entity.insert("enum".into(), json!(names));
    }
    Arc::new(obj)
}

fn annotations(
    title: &str,
    read_only: bool,
    destructive: bool,
    idempotent: bool,
) -> ToolAnnotations {
    ToolAnnotations::with_title(title)
        .read_only(read_only)
        .destructive(destructive)
        .idempotent(idempotent)
        .open_world(false)
}

/// Build the tool list for this server instance. Tools whose operation is not
/// enabled on any entity are omitted; write tools are omitted when
/// `MCP_WRITE_TOOLS_ENABLED=false`.
pub fn tool_definitions(require_auth: bool, cfg: &McpConfig) -> Vec<Tool> {
    let all: Vec<String> = exposed_entities(require_auth)
        .into_iter()
        .map(|(r, _)| r)
        .collect();
    let mut tools = vec![
        Tool::new(
            LIST_ENTITIES,
            "List every entity (table/route) exposed by this Flexurio API, with the operations \
             it enables, the operations the caller is authorized for, its primary key and the \
             filter keys accepted by `query_records`. Start here.",
            schema_for_empty_input(),
        )
        .with_title("List entities")
        .with_raw_output_schema(output_schema::<ListEntitiesOutput>())
        .with_annotations(annotations("List entities", true, false, true)),
        Tool::new(
            DESCRIBE_ENTITY,
            "Describe one entity: columns (type, nullability, defaults, encryption), primary \
             key, foreign keys, indexes, master-detail fields, per-operation columns and \
             parameters, document locks and state machine. Call before writing data.",
            with_entity_enum(input_schema::<EntityArgs>(), &all),
        )
        .with_title("Describe entity")
        .with_raw_output_schema(output_schema::<DescribeEntityOutput>())
        .with_annotations(annotations("Describe entity", true, false, true)),
    ];

    let readable = entity_names_for(require_auth, Op::Read);
    if !readable.is_empty() {
        tools.push(
            Tool::new(
                QUERY_RECORDS,
                format!(
                    "Read records from an entity (REST: GET /<entity>). Filters use the entity's \
                     declared parameter keys `<column>.<op>`; undeclared keys are ignored and \
                     reported. Results are capped at {} rows; use page/limit to paginate.",
                    cfg.max_rows
                ),
                with_entity_enum(input_schema::<QueryRecordsArgs>(), &readable),
            )
            .with_title("Query records")
            .with_raw_output_schema(output_schema::<QueryRecordsOutput>())
            .with_annotations(annotations("Query records", true, false, true)),
        );
    }

    if cfg.write_tools {
        let creatable = entity_names_for(require_auth, Op::Create);
        if !creatable.is_empty() {
            tools.push(
                Tool::new(
                    CREATE_RECORD,
                    "Create a record (REST: POST /<entity>). Runs validation, ID generation, \
                     master-detail inserts and action triggers in one transaction.",
                    with_entity_enum(input_schema::<CreateRecordArgs>(), &creatable),
                )
                .with_title("Create record")
                .with_raw_output_schema(output_schema::<WriteOutput>())
                .with_annotations(annotations(
                    "Create record",
                    false,
                    false,
                    false,
                )),
            );
        }
        let updatable = entity_names_for(require_auth, Op::Update);
        if !updatable.is_empty() {
            tools.push(
                Tool::new(
                    UPDATE_RECORD,
                    "Update a record by primary key (REST: PUT /<entity>/{id}). Synchronises \
                     detail rows per `update_strategy`, enforces locks and state transitions.",
                    with_entity_enum(input_schema::<UpdateRecordArgs>(), &updatable),
                )
                .with_title("Update record")
                .with_raw_output_schema(output_schema::<WriteOutput>())
                .with_annotations(annotations("Update record", false, true, true)),
            );
            tools.push(
                Tool::new(
                    PATCH_RECORD,
                    "Partially update a record by primary key (REST: PATCH /<entity>/{id}); only \
                     the provided fields change. Authorized as PATCH in rules.json.",
                    with_entity_enum(input_schema::<UpdateRecordArgs>(), &updatable),
                )
                .with_title("Patch record")
                .with_raw_output_schema(output_schema::<WriteOutput>())
                .with_annotations(annotations("Patch record", false, true, false)),
            );
        }
        let deletable = entity_names_for(require_auth, Op::Delete);
        if !deletable.is_empty() {
            tools.push(
                Tool::new(
                    DELETE_RECORD,
                    "Delete a record by primary key (REST: DELETE /<entity>/{id}). Soft or hard \
                     per the entity's `type_delete`; foreign-key actions and detail cascades apply.",
                    with_entity_enum(input_schema::<DeleteRecordArgs>(), &deletable),
                )
                .with_title("Delete record")
                .with_raw_output_schema(output_schema::<WriteOutput>())
                .with_annotations(annotations("Delete record", false, true, true)),
            );
        }
        let procedures = entity_names_for(require_auth, Op::RunProcedure);
        if !procedures.is_empty() {
            tools.push(
                Tool::new(
                    RUN_PROCEDURE,
                    "Run the entity's configured stored procedure / parameterized operation \
                     (REST: PATCH /<entity>). May modify data.",
                    with_entity_enum(input_schema::<RunProcedureArgs>(), &procedures),
                )
                .with_title("Run procedure")
                .with_raw_output_schema(output_schema::<WriteOutput>())
                .with_annotations(annotations("Run procedure", false, true, false)),
            );
        }
    }

    tools.push(
        Tool::new(
            VALIDATE_ENTITY,
            "Check that an entity's JSON config matches the physical database table \
             (REST: GET /validate/<entity>).",
            with_entity_enum(input_schema::<EntityArgs>(), &all),
        )
        .with_title("Validate entity")
        .with_raw_output_schema(output_schema::<ValidateOutput>())
        .with_annotations(annotations("Validate entity", true, false, true)),
    );
    tools.push(
        Tool::new(
            HEALTH,
            "Report server and database health (REST: GET /healthz).",
            schema_for_empty_input(),
        )
        .with_title("Health check")
        .with_raw_output_schema(output_schema::<HealthOutput>())
        .with_annotations(annotations("Health check", true, false, true)),
    );
    tools
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

/// Execute a tool call. `Err` only for unknown tools (JSON-RPC `-32602`).
pub async fn call_tool(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    name: &str,
    args: Option<JsonObject>,
) -> Result<CallToolResult, ErrorData> {
    let known = tool_definitions(state.require_auth, cfg)
        .iter()
        .any(|t| t.name == name);
    if !known {
        return Err(ErrorData::invalid_params(
            format!("Unknown tool: {}", name),
            None,
        ));
    }

    let entity_label = args
        .as_ref()
        .and_then(|a| a.get("entity"))
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string();

    let result = match name {
        LIST_ENTITIES => Ok(list_entities(state, identity)),
        DESCRIBE_ENTITY => describe_entity(state, args),
        QUERY_RECORDS => query_records(state, cfg, identity, args).await,
        CREATE_RECORD => create_record(state, cfg, identity, args).await,
        UPDATE_RECORD => update_record(state, cfg, identity, args, Op::Update).await,
        PATCH_RECORD => update_record(state, cfg, identity, args, Op::Patch).await,
        DELETE_RECORD => delete_record(state, cfg, identity, args).await,
        RUN_PROCEDURE => run_procedure(state, cfg, identity, args).await,
        VALIDATE_ENTITY => validate_entity(state, identity, args).await,
        HEALTH => Ok(health(state, cfg).await),
        _ => {
            return Err(ErrorData::invalid_params(
                format!("Unknown tool: {}", name),
                None,
            ));
        }
    };
    let result = result.unwrap_or_else(|e| tool_error(e.0));

    let is_error = result.is_error == Some(true);
    METRICS.record_mcp_tool_call(is_error);
    log_output(
        if is_error { "WARN" } else { "INFO" },
        "MCP TOOL",
        name,
        format!(
            "entity={} caller={} result={}",
            entity_label,
            identity.subject(),
            if is_error { "error" } else { "ok" }
        ),
        true,
    );
    Ok(result)
}

// ── Tool implementations ──────────────────────────────────────────────────────

/// Evaluate `rules.json` for `op` exactly like the REST services do.
fn caller_may(state: &AppState, identity: &CallerIdentity, entity: &str, op: Op) -> Option<bool> {
    if !state.require_auth || state.route_publics.contains(entity) {
        return None;
    }
    let claims = identity.claims.as_ref()?;
    let req = ServiceRequest::new(op.method(), op.path(entity, None)).build(identity);
    Some(crate::auth::check_access(claims, &req).is_ok())
}

pub fn summarize_entity(
    state: &AppState,
    identity: Option<&CallerIdentity>,
    name: &str,
    s: &TableSchema,
) -> EntitySummary {
    let enabled: Vec<Op> = Op::ALL.iter().copied().filter(|op| op.enabled(s)).collect();
    let allowed = identity.and_then(|id| {
        let checks: Vec<(Op, Option<bool>)> = enabled
            .iter()
            .map(|op| (*op, caller_may(state, id, name, *op)))
            .collect();
        if checks.iter().all(|(_, c)| c.is_none()) {
            None
        } else {
            Some(
                checks
                    .into_iter()
                    .filter(|(_, c)| c.unwrap_or(true))
                    .map(|(op, _)| op.as_str().to_string())
                    .collect(),
            )
        }
    });
    EntitySummary {
        name: name.to_string(),
        table: s.table.clone(),
        operations: enabled.iter().map(|op| op.as_str().to_string()).collect(),
        allowed_operations: allowed,
        primary_key: s.primary_key.columns.clone(),
        filter_parameters: s
            .get
            .parameters
            .iter()
            .map(|p| p.trim_start_matches('*').to_string())
            .collect(),
        detail_fields: s.details.iter().map(|d| d.field.clone()).collect(),
    }
}

fn list_entities(state: &web::Data<AppState>, identity: &CallerIdentity) -> CallToolResult {
    let entities: Vec<EntitySummary> = exposed_entities(state.require_auth)
        .iter()
        .map(|(r, s)| summarize_entity(state, Some(identity), r, s))
        .collect();
    structured(&ListEntitiesOutput {
        total: entities.len(),
        entities,
    })
}

pub fn describe(name: &str, s: &TableSchema) -> DescribeEntityOutput {
    let params = filter_params(s);
    let mut example_filters = Map::new();
    for p in params.iter().filter(|p| p.kind == "filter").take(2) {
        example_filters.insert(p.name.clone(), json!("<value>"));
    }
    let mut example = json!({ "entity": name });
    if !example_filters.is_empty() {
        example["filters"] = Value::Object(example_filters);
    }
    if params.iter().any(|p| p.name == "limit") {
        example["limit"] = json!(20);
    }

    DescribeEntityOutput {
        name: name.to_string(),
        table: s.table.clone(),
        primary_key: s.primary_key.columns.clone(),
        columns: s
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                type_data: c.type_data.clone(),
                nullable: c.nullable,
                auto_increment: c.auto_increment,
                encrypted: c.encrypt,
                default: c.default.clone(),
                generated: !c.function.trim().is_empty(),
                primary_key: s.primary_key.columns.contains(&c.name),
            })
            .collect(),
        foreign_keys: s
            .foreign_keys
            .iter()
            .map(|fk| ForeignKeyInfo {
                column: fk.column.clone(),
                reference_table: fk.reference_table.clone(),
                reference_column: fk.reference_column.clone(),
                on_delete: fk.on_delete.clone(),
                on_update: fk.on_update.clone(),
            })
            .collect(),
        indexes: s
            .indexes
            .iter()
            .map(|ix| IndexInfo {
                name: ix.name.clone(),
                columns: ix.columns.clone(),
                unique: ix.unique,
            })
            .collect(),
        details: s
            .details
            .iter()
            .map(|d| DetailInfo {
                field: d.field.clone(),
                target_entity: d.target_table.clone(),
                foreign_key_column: d.foreign_key_column.clone(),
                update_strategy: d
                    .update_strategy
                    .clone()
                    .unwrap_or_else(|| "replace".into()),
                cascade_delete: d.cascade_delete.unwrap_or(true),
            })
            .collect(),
        operations: OperationsInfo {
            read: ReadOpInfo {
                enabled: s.get.enable_method,
                columns: s.get.columns.clone(),
                parameters: params,
                joins: s
                    .get
                    .join_tables
                    .iter()
                    .map(|j| j.table.clone())
                    .filter(|t| !t.is_empty())
                    .collect(),
            },
            create: WriteOpInfo {
                enabled: s.post.enable_method,
                columns: s.post.columns.clone(),
            },
            update: WriteOpInfo {
                enabled: s.put.enable_method,
                columns: s.put.columns.clone(),
            },
            delete: DeleteOpInfo {
                enabled: s.del.enable_method,
                type_delete: if s.del.type_delete.is_empty() {
                    "soft".into()
                } else {
                    s.del.type_delete.clone()
                },
            },
            run_procedure: ProcedureOpInfo {
                enabled: s.patch.enable_method,
                parameters: s.patch.parameters.clone(),
                return_mode: s.patch.return_mode.clone(),
            },
        },
        locked_when: s
            .locked_when
            .as_ref()
            .and_then(|l| serde_json::to_value(l).ok()),
        state_machine: s
            .state_machine
            .as_ref()
            .and_then(|m| serde_json::to_value(m).ok()),
        example_query: example,
    }
}

fn describe_entity(
    state: &web::Data<AppState>,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: EntityArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    Ok(structured(&describe(&a.entity, &schema)))
}

async fn query_records(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: QueryRecordsArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    require_op(&a.entity, &schema, Op::Read)?;

    let declared: Vec<String> = schema
        .get
        .parameters
        .iter()
        .map(|p| p.trim_start_matches('*').to_string())
        .collect();

    let mut params = Map::new();
    let mut ignored = Vec::new();
    for (k, v) in a.filters.unwrap_or_default() {
        if declared.contains(&k) {
            params.insert(k, v);
        } else {
            ignored.push(k);
        }
    }
    let max = u32::try_from(cfg.max_rows).unwrap_or(u32::MAX);
    let controls: [(&str, Option<Value>); 5] = [
        ("page", a.page.map(|v| json!(v))),
        ("limit", a.limit.map(|v| json!(v.min(max)))),
        ("sort", a.sort.map(Value::String)),
        ("ascending", a.ascending.map(Value::Bool)),
        ("search", a.search.map(Value::String)),
    ];
    for (k, v) in controls {
        if let Some(v) = v {
            if declared.iter().any(|d| d == k) {
                params.insert(k.to_string(), v);
            } else {
                ignored.push(k.to_string());
            }
        }
    }

    let sr =
        ServiceRequest::new(Op::Read.method(), Op::Read.path(&a.entity, None)).with_query(params);
    let req = sr.build(identity);
    let resp = crate::nocode::services::data_read_service::process_get_request(
        state,
        &sr.query_value(),
        &a.entity,
        &schema,
        &req,
    )
    .await;
    let outcome = outcome_from_response(resp).await;
    if !outcome.is_success() {
        return Err(service_error(&outcome));
    }

    let total = outcome
        .body
        .get("total_data")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut rows = match outcome.body.get("data") {
        Some(Value::Array(rows)) => rows.clone(),
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other.clone()],
    };
    let truncated = rows.len() > cfg.max_rows;
    rows.truncate(cfg.max_rows);
    for row in rows.iter_mut() {
        redact(row, &cfg.redact_fields);
    }

    Ok(structured(&QueryRecordsOutput {
        entity: a.entity,
        total,
        returned: rows.len(),
        truncated,
        rows,
        ignored_parameters: ignored,
    }))
}

fn write_output(
    cfg: &McpConfig,
    entity: &str,
    op: Op,
    id: Option<String>,
    outcome: &ServiceOutcome,
) -> CallToolResult {
    let mut data = outcome.body.get("data").cloned().unwrap_or(Value::Null);
    redact(&mut data, &cfg.redact_fields);
    let message = outcome.message();
    structured(&WriteOutput {
        entity: entity.to_string(),
        operation: op.as_str().to_string(),
        id,
        status: outcome.status,
        queued: outcome.status == 202 || message.eq_ignore_ascii_case("enqueued"),
        message,
        total_data: outcome
            .body
            .get("total_data")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        data,
    })
}

/// HTTP status `post_handler::insert` would return for a create failure.
fn create_error_status(msg: &str) -> u16 {
    if msg == "Invalid token" || msg.starts_with("Unauthorized") {
        401
    } else if msg.contains("not found") {
        404
    } else if msg.contains("Missing required field")
        || msg.contains("Invalid")
        || msg.contains("must be an array")
    {
        400
    } else {
        500
    }
}

/// HTTP status `delete_handler::delete` would return for a delete failure.
fn delete_error_status(msg: &str) -> u16 {
    if msg == "Invalid token" || msg.starts_with("Unauthorized") {
        401
    } else if msg.contains("not found") {
        424
    } else if msg.contains("mismatch") {
        400
    } else {
        500
    }
}

fn enqueued_status(message: &str) -> u16 {
    if message == "Enqueued" { 202 } else { 200 }
}

async fn create_record(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: CreateRecordArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    require_op(&a.entity, &schema, Op::Create)?;
    if a.data.is_empty() {
        return Err(fail("`data` must contain at least one column value."));
    }

    let sr = ServiceRequest::new(Op::Create.method(), Op::Create.path(&a.entity, None));
    let req = sr.build(identity);
    let res = crate::nocode::services::data_create_service::process_insert_request(
        state,
        &sr.query_value(),
        &a.entity,
        &schema,
        Value::Object(a.data),
        &req,
    )
    .await;
    let outcome = match res {
        Ok(r) => ServiceOutcome::from_web_response(enqueued_status(&r.message), &r),
        Err(r) => ServiceOutcome::from_web_response(create_error_status(&r.message), &r),
    };
    if !outcome.is_success() {
        return Err(service_error(&outcome));
    }
    Ok(write_output(cfg, &a.entity, Op::Create, None, &outcome))
}

async fn update_record(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
    op: Op,
) -> ToolResult<CallToolResult> {
    let a: UpdateRecordArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    require_op(&a.entity, &schema, op)?;
    let id = a.id.as_path_segment()?;
    if a.data.is_empty() {
        return Err(fail("`data` must contain at least one column value."));
    }

    let sr = ServiceRequest::new(op.method(), op.path(&a.entity, Some(&id)));
    let req = sr.build(identity);
    let resp = crate::nocode::services::data_update_service::process_update_request(
        state,
        &sr.query_value(),
        &a.entity,
        &schema,
        &SCHEMAS.1,
        Value::Object(a.data),
        web::Path::from(id.clone()),
        &req,
    )
    .await;
    let outcome = outcome_from_response(resp).await;
    if !outcome.is_success() {
        return Err(service_error(&outcome));
    }
    Ok(write_output(cfg, &a.entity, op, Some(id), &outcome))
}

async fn delete_record(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: DeleteRecordArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    require_op(&a.entity, &schema, Op::Delete)?;
    let id = a.id.as_path_segment()?;

    let sr = ServiceRequest::new(Op::Delete.method(), Op::Delete.path(&a.entity, Some(&id)));
    let req = sr.build(identity);
    let res = crate::nocode::services::data_delete_service::process_delete_request(
        state.clone(),
        sr.query_value(),
        a.entity.clone(),
        schema.clone(),
        SCHEMAS.1.clone(),
        id.clone(),
        req,
    )
    .await;
    let outcome = match res {
        Ok(r) => ServiceOutcome::from_web_response(enqueued_status(&r.message), &r),
        Err(msg) => ServiceOutcome {
            status: delete_error_status(&msg),
            body: json!({ "success": false, "message": msg }),
        },
    };
    if !outcome.is_success() {
        return Err(service_error(&outcome));
    }
    Ok(write_output(cfg, &a.entity, Op::Delete, Some(id), &outcome))
}

async fn run_procedure(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: RunProcedureArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;
    require_op(&a.entity, &schema, Op::RunProcedure)?;

    let sr = ServiceRequest::new(
        Op::RunProcedure.method(),
        Op::RunProcedure.path(&a.entity, None),
    )
    .with_query(a.parameters.unwrap_or_default());
    let req = sr.build(identity);
    let resp = crate::nocode::services::data_patch_service::process_patch_request(
        state,
        &sr.query_value(),
        &a.entity,
        &schema,
        &req,
    )
    .await;
    let outcome = outcome_from_response(resp).await;
    if !outcome.is_success() {
        return Err(service_error(&outcome));
    }
    Ok(write_output(
        cfg,
        &a.entity,
        Op::RunProcedure,
        None,
        &outcome,
    ))
}

async fn validate_entity(
    state: &web::Data<AppState>,
    identity: &CallerIdentity,
    args: Option<JsonObject>,
) -> ToolResult<CallToolResult> {
    let a: EntityArgs = parse_args(args)?;
    let schema = resolve_entity(state.require_auth, &a.entity)?;

    let sr = ServiceRequest::new(Op::Validate.method(), Op::Validate.path(&a.entity, None));
    let req = sr.build(identity);
    let resp = crate::nocode::validate::check_table_design(
        state.clone(),
        a.entity.clone(),
        schema,
        req.clone(),
    )
    .await
    .respond_to(&req);
    let outcome = outcome_from_response(resp).await;
    if outcome.status == 401 || outcome.status == 403 {
        return Err(service_error(&outcome));
    }
    let data = outcome
        .body
        .get("data")
        .cloned()
        .unwrap_or_else(|| outcome.body.clone());
    Ok(structured(&ValidateOutput {
        entity: a.entity,
        status: outcome.status,
        valid: outcome.is_success(),
        message: outcome.message(),
        data,
    }))
}

async fn health(state: &web::Data<AppState>, cfg: &McpConfig) -> CallToolResult {
    let db_ok = state.db.query("SELECT 1").await.is_ok();
    structured(&HealthOutput {
        status: if db_ok { "ok" } else { "degraded" }.into(),
        db: if db_ok { "up" } else { "down" }.into(),
        db_type: state.db_type.as_str().into(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        entities: exposed_entities(state.require_auth).len(),
        write_tools_enabled: cfg.write_tools,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_param_simple() {
        let p = parse_filter_param("*email.eq");
        assert_eq!(p.name, "email.eq");
        assert_eq!(p.kind, "filter");
        assert_eq!(p.column.as_deref(), Some("email"));
        assert_eq!(p.operator.as_deref(), Some("eq"));
        assert!(p.required);
    }

    #[test]
    fn test_parse_filter_param_joined_table_column() {
        let p = parse_filter_param("master_department.name.like");
        assert_eq!(p.column.as_deref(), Some("master_department.name"));
        assert_eq!(p.operator.as_deref(), Some("like"));
        assert!(!p.required);
    }

    #[test]
    fn test_parse_filter_param_control_join_and_or() {
        assert_eq!(parse_filter_param("limit").kind, "control");
        assert_eq!(parse_filter_param("paramjoin_dept").kind, "join");
        let p = parse_filter_param("name.like|email.like");
        assert_eq!(p.kind, "or_filter");
        assert_eq!(p.column.as_deref(), Some("name|email"));
        assert_eq!(p.operator.as_deref(), Some("like"));
    }

    #[test]
    fn test_parse_filter_param_without_operator_defaults_to_eq() {
        let p = parse_filter_param("status");
        assert_eq!(p.column.as_deref(), Some("status"));
        assert_eq!(p.operator.as_deref(), Some("eq"));
    }

    #[test]
    fn test_redact_masks_sensitive_keys_recursively() {
        let fields = vec!["password".to_string(), "token".to_string()];
        let mut v = json!({
            "id": 1,
            "password": "hash",
            "flx_users.password": "hash2",
            "reset_token": "t",
            "nested": [{"api": 1, "password": "x"}],
            "tokenizer": "keep",
            "empty_password": null
        });
        redact(&mut v, &fields);
        assert_eq!(v["id"], json!(1));
        assert_eq!(v["password"], json!("***REDACTED***"));
        assert_eq!(v["flx_users.password"], json!("***REDACTED***"));
        assert_eq!(v["reset_token"], json!("***REDACTED***"));
        assert_eq!(v["nested"][0]["password"], json!("***REDACTED***"));
        assert_eq!(v["tokenizer"], json!("keep"));
        assert_eq!(v["empty_password"], Value::Null);
    }

    #[test]
    fn test_redact_disabled_with_empty_list() {
        let mut v = json!({"password": "x"});
        redact(&mut v, &[]);
        assert_eq!(v["password"], json!("x"));
    }

    #[test]
    fn test_op_rest_mapping() {
        assert_eq!(Op::Read.method(), Method::GET);
        assert_eq!(Op::Read.path("e", None), "/e");
        assert_eq!(Op::Update.method(), Method::PUT);
        assert_eq!(Op::Update.path("e", Some("5")), "/e/5");
        assert_eq!(Op::Patch.method(), Method::PATCH);
        assert_eq!(Op::Delete.path("e", Some("5")), "/e/5");
        assert_eq!(Op::RunProcedure.path("e", None), "/e");
        assert_eq!(Op::Validate.path("e", None), "/validate/e");
        assert!(Op::Create.is_write());
        assert!(!Op::Read.is_write());
    }

    #[test]
    fn test_op_enabled_follows_schema_switches() {
        let mut s = TableSchema::default();
        assert!(!Op::Read.enabled(&s));
        assert!(Op::Validate.enabled(&s));
        s.get.enable_method = true;
        s.put.enable_method = true;
        assert!(Op::Read.enabled(&s));
        assert!(Op::Update.enabled(&s));
        assert!(Op::Patch.enabled(&s));
        assert!(!Op::Delete.enabled(&s));
    }

    #[test]
    fn test_record_id_rejects_empty_and_slash() {
        assert_eq!(RecordId::Int(5).as_path_segment().unwrap(), "5");
        assert_eq!(
            RecordId::Text(" ab ".into()).as_path_segment().unwrap(),
            "ab"
        );
        assert!(RecordId::Text("".into()).as_path_segment().is_err());
        assert!(RecordId::Text("a/b".into()).as_path_segment().is_err());
    }

    #[test]
    fn test_record_id_deserializes_number_and_string() {
        let a: DeleteRecordArgs = serde_json::from_value(json!({"entity": "e", "id": 3})).unwrap();
        assert!(matches!(a.id, RecordId::Int(3)));
        let b: DeleteRecordArgs =
            serde_json::from_value(json!({"entity": "e", "id": "PO-1"})).unwrap();
        assert!(matches!(b.id, RecordId::Text(ref s) if s == "PO-1"));
    }

    #[test]
    fn test_args_reject_unknown_fields() {
        let r: Result<EntityArgs, _> = serde_json::from_value(json!({"entity": "e", "x": 1}));
        assert!(r.is_err());
    }

    #[test]
    fn test_input_schema_has_entity_enum() {
        let s = with_entity_enum(
            input_schema::<EntityArgs>(),
            &["a".to_string(), "b".to_string()],
        );
        assert_eq!(s["type"], json!("object"));
        assert_eq!(s["properties"]["entity"]["enum"], json!(["a", "b"]));
        assert_eq!(s["required"], json!(["entity"]));
        assert_eq!(s["additionalProperties"], json!(false));
    }

    #[test]
    fn test_schemas_have_no_non_standard_integer_formats() {
        let tools = tool_definitions(true, &McpConfig::from_env());
        for t in tools {
            let input = serde_json::to_string(t.input_schema.as_ref()).unwrap();
            let output = serde_json::to_string(t.output_schema.as_ref().unwrap().as_ref()).unwrap();
            for s in [input, output] {
                assert!(
                    !s.contains("\"format\":\"uint"),
                    "{} has uint format",
                    t.name
                );
                assert!(!s.contains("\"format\":\"int"), "{} has int format", t.name);
            }
        }
    }

    #[test]
    fn test_strip_keeps_standard_formats() {
        let mut v = json!({"a": {"type": "string", "format": "date-time"}, "b": {"type": "integer", "format": "uint16", "minimum": 0}});
        strip_non_standard_formats(&mut v);
        assert_eq!(v["a"]["format"], json!("date-time"));
        assert!(v["b"].get("format").is_none());
        assert_eq!(v["b"]["minimum"], json!(0));
    }

    #[test]
    fn test_output_schemas_are_objects() {
        for s in [
            schema_for_output::<ListEntitiesOutput>(),
            schema_for_output::<DescribeEntityOutput>(),
            schema_for_output::<QueryRecordsOutput>(),
            schema_for_output::<WriteOutput>(),
            schema_for_output::<ValidateOutput>(),
            schema_for_output::<HealthOutput>(),
        ] {
            assert_eq!(s["type"], json!("object"));
        }
    }

    #[test]
    fn test_describe_builds_operations_and_example() {
        let mut s = TableSchema {
            table: "t".into(),
            ..Default::default()
        };
        s.primary_key.columns = vec!["id".into()];
        s.columns = vec![crate::model::Column {
            name: "id".into(),
            type_data: "int".into(),
            auto_increment: true,
            ..Default::default()
        }];
        s.get.enable_method = true;
        s.get.parameters = vec!["*nik.eq".into(), "limit".into()];
        s.del.enable_method = true;
        let d = describe("t", &s);
        assert!(d.columns[0].primary_key);
        assert!(d.operations.read.enabled);
        assert_eq!(d.operations.read.parameters.len(), 2);
        assert_eq!(d.operations.delete.type_delete, "soft");
        assert_eq!(d.example_query["filters"]["nik.eq"], json!("<value>"));
        assert_eq!(d.example_query["limit"], json!(20));
    }

    #[test]
    fn test_error_status_mapping_matches_rest_handlers() {
        assert_eq!(create_error_status("Invalid token"), 401);
        assert_eq!(create_error_status("Unauthorized: rule"), 401);
        assert_eq!(create_error_status("Missing required field: x"), 400);
        assert_eq!(create_error_status("boom"), 500);
        assert_eq!(delete_error_status("record not found"), 424);
        assert_eq!(delete_error_status("id mismatch"), 400);
        assert_eq!(enqueued_status("Enqueued"), 202);
        assert_eq!(enqueued_status("Data inserted"), 200);
    }
}
