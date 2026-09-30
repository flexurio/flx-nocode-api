//! MCP resources: read-only context the client can attach to a conversation.
//!
//! | URI                           | MIME               | Content                                   |
//! |-------------------------------|--------------------|-------------------------------------------|
//! | `flexurio://guide`            | `text/markdown`    | How to use this server (filters, flows)   |
//! | `flexurio://entities`         | `application/json` | Summary of every exposed entity           |
//! | `flexurio://entity/{name}`    | `application/json` | Full description of one entity (template) |
//! | `flexurio://rules`            | `application/json` | `rules.json` — listed for admins only     |

use actix_web::web;
use rmcp::ErrorData;
use rmcp::model::{ReadResourceResult, Resource, ResourceContents, ResourceTemplate};
use serde_json::json;

use super::McpConfig;
use super::ctx::CallerIdentity;
use super::tools::{describe, exposed_entities, find_entity, summarize_entity};
use crate::database::state::AppState;

pub const URI_GUIDE: &str = "flexurio://guide";
pub const URI_ENTITIES: &str = "flexurio://entities";
pub const URI_RULES: &str = "flexurio://rules";
pub const URI_ENTITY_PREFIX: &str = "flexurio://entity/";
pub const URI_ENTITY_TEMPLATE: &str = "flexurio://entity/{name}";

const MIME_JSON: &str = "application/json";
const MIME_MARKDOWN: &str = "text/markdown";

/// Usage guide shown to the model; kept in sync with `tools.rs`.
pub const GUIDE: &str = r#"# Flexurio No-Code API — MCP guide

Flexurio turns JSON entity configs into a REST API. Every entity (route) below is
backed by one database table/collection and can be used through these tools.

## Recommended flow
1. `list_entities` — discover entities, enabled operations and what *you* may do.
2. `describe_entity` — columns, keys, filters, master-detail fields, locks.
3. `query_records` — read data with declared filters, paginate with page/limit.
4. Write with `create_record` / `update_record` / `patch_record` / `delete_record`
   only after confirming intent with the user. Re-read to verify.

## Filters (`query_records.filters`)
Keys must be declared in the entity's read parameters, e.g. `email.eq`.
Format: `<column>.<operator>` or `<table>.<column>.<operator>` for joins.

| operator | meaning                | example value           |
|----------|------------------------|-------------------------|
| eq       | equals                 | `"A-001"`               |
| like     | contains (case-insens.)| `"john"`                |
| lt / lte | less than (or equal)   | `"2024-12-31"`          |
| gt / gte | greater than (or equal)| `100`                   |
| is       | IS (e.g. null)         | `"null"`                |
| nin      | not in                 | `[1, 2]` or `"1,2"`     |
| between  | inclusive range        | `"2024-01-01,2024-12-31"` |

`a.like|b.like` keys search several columns with OR. `page`, `limit`, `sort`,
`ascending` and `search` work only when the entity declares them.
Undeclared keys are ignored and reported in `ignored_parameters`.

## Writes
* `create_record.data` holds column values; master-detail entities also accept
  their detail arrays (see `details[].field`).
* `update_record` (PUT) and `patch_record` (PATCH) change a record by primary key.
* Deletes are soft (`deleted_at`) or hard per the entity's `type_delete`.
* Validation, generated IDs, action triggers, document locks (`locked_when`) and
  state machines are enforced by the server. A `queued: true` result means the
  write was accepted by the async write queue.

## Errors
Tool errors come back as results with `isError: true` and a hint. HTTP 401
means the token is missing/invalid or `rules.json` denies the operation.
Sensitive fields (passwords, tokens, secrets) are masked as `***REDACTED***`.
"#;

fn is_rules_visible(state: &AppState, cfg: &McpConfig, identity: &CallerIdentity) -> bool {
    state.require_auth && cfg.is_admin(identity.claims.as_ref())
}

/// Concrete resources for `resources/list`.
pub fn list(state: &AppState, cfg: &McpConfig, identity: &CallerIdentity) -> Vec<Resource> {
    let mut out = vec![
        Resource::new(URI_GUIDE, "guide")
            .with_title("Flexurio MCP usage guide")
            .with_description("How to discover entities, filter queries and write data safely.")
            .with_mime_type(MIME_MARKDOWN)
            .with_size(GUIDE.len() as u64),
        Resource::new(URI_ENTITIES, "entities")
            .with_title("Entity catalogue")
            .with_description(
                "Every exposed entity with enabled operations, keys and filter parameters.",
            )
            .with_mime_type(MIME_JSON),
    ];
    for (name, schema) in exposed_entities(state.require_auth) {
        out.push(
            Resource::new(format!("{}{}", URI_ENTITY_PREFIX, name), name.clone())
                .with_title(format!("Entity: {}", name))
                .with_description(format!(
                    "Schema of table `{}`: columns, keys, filters, operations.",
                    schema.table
                ))
                .with_mime_type(MIME_JSON),
        );
    }
    if is_rules_visible(state, cfg, identity) {
        out.push(
            Resource::new(URI_RULES, "rules")
                .with_title("Authorization rules (rules.json)")
                .with_description(
                    "Role-based access rules evaluated for every request. Admin only.",
                )
                .with_mime_type(MIME_JSON),
        );
    }
    out
}

/// Parameterised resources for `resources/templates/list`.
pub fn templates() -> Vec<ResourceTemplate> {
    vec![
        ResourceTemplate::new(URI_ENTITY_TEMPLATE, "entity")
            .with_title("Entity description")
            .with_description(
                "Full description of one entity; `name` is an entity from `flexurio://entities`.",
            )
            .with_mime_type(MIME_JSON),
    ]
}

fn json_contents(uri: &str, value: &serde_json::Value) -> ReadResourceResult {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type(MIME_JSON),
    ])
}

/// Handle `resources/read`.
pub fn read(
    state: &web::Data<AppState>,
    cfg: &McpConfig,
    identity: &CallerIdentity,
    uri: &str,
) -> Result<ReadResourceResult, ErrorData> {
    match uri {
        URI_GUIDE => Ok(ReadResourceResult::new(vec![
            ResourceContents::text(GUIDE, uri).with_mime_type(MIME_MARKDOWN),
        ])),
        URI_ENTITIES => {
            let entities: Vec<_> = exposed_entities(state.require_auth)
                .iter()
                .map(|(n, s)| summarize_entity(state, Some(identity), n, s))
                .collect();
            Ok(json_contents(
                uri,
                &json!({ "total": entities.len(), "entities": entities }),
            ))
        }
        URI_RULES => {
            if !is_rules_visible(state, cfg, identity) {
                return Err(ErrorData::resource_not_found(
                    format!("Resource not found: {}", uri),
                    None,
                ));
            }
            Ok(json_contents(uri, &state.rules))
        }
        _ => {
            let name = uri.strip_prefix(URI_ENTITY_PREFIX).map(|n| {
                urlencoding::decode(n)
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| n.to_string())
            });
            match name.and_then(|n| find_entity(state.require_auth, &n).map(|s| (n, s))) {
                Some((n, schema)) => {
                    let value = serde_json::to_value(describe(&n, &schema)).unwrap_or_default();
                    Ok(json_contents(uri, &value))
                }
                None => Err(ErrorData::resource_not_found(
                    format!("Resource not found: {}", uri),
                    Some(json!({ "uri": uri })),
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guide_mentions_every_tool() {
        for t in [
            "list_entities",
            "describe_entity",
            "query_records",
            "create_record",
            "update_record",
            "patch_record",
            "delete_record",
        ] {
            assert!(GUIDE.contains(t), "guide should mention {}", t);
        }
    }

    #[test]
    fn test_template_uri_matches_prefix() {
        assert!(URI_ENTITY_TEMPLATE.starts_with(URI_ENTITY_PREFIX));
        assert_eq!(templates().len(), 1);
    }
}
