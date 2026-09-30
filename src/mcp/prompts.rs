//! MCP prompts: reusable, user-invoked workflows (slash commands in most clients).
//!
//! | Prompt                | Arguments               | Purpose                                   |
//! |-----------------------|-------------------------|-------------------------------------------|
//! | `explore_entity`      | `entity`                | Describe an entity and sample its data    |
//! | `safe_write`          | `entity`, `goal`        | Plan → confirm → write → verify           |
//! | `data_quality_report` | `entity`, `focus`?      | Config/table validation + data checks     |

use rmcp::ErrorData;
use rmcp::model::{GetPromptResult, JsonObject, Prompt, PromptArgument, PromptMessage, Role};
use serde_json::Value;

use super::tools::find_entity;

pub const EXPLORE_ENTITY: &str = "explore_entity";
pub const SAFE_WRITE: &str = "safe_write";
pub const DATA_QUALITY_REPORT: &str = "data_quality_report";

fn entity_arg() -> PromptArgument {
    PromptArgument::new("entity")
        .with_title("Entity")
        .with_description("Entity (route) name, e.g. from `list_entities`.")
        .with_required(true)
}

pub fn list() -> Vec<Prompt> {
    vec![
        Prompt::new(
            EXPLORE_ENTITY,
            Some("Explain an entity's structure and show representative records."),
            Some(vec![entity_arg()]),
        )
        .with_title("Explore entity"),
        Prompt::new(
            SAFE_WRITE,
            Some("Safely create/update/delete data: inspect, propose, confirm, execute, verify."),
            Some(vec![
                entity_arg(),
                PromptArgument::new("goal")
                    .with_title("Goal")
                    .with_description("What should change, in plain language.")
                    .with_required(true),
            ]),
        )
        .with_title("Safe write"),
        Prompt::new(
            DATA_QUALITY_REPORT,
            Some("Validate an entity against its table and report data-quality issues."),
            Some(vec![
                entity_arg(),
                PromptArgument::new("focus")
                    .with_title("Focus")
                    .with_description(
                        "Optional area to focus on (e.g. duplicates, missing values).",
                    )
                    .with_required(false),
            ]),
        )
        .with_title("Data quality report"),
    ]
}

fn arg(args: &Option<JsonObject>, name: &str) -> Option<String> {
    args.as_ref()
        .and_then(|a| a.get(name))
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn required(args: &Option<JsonObject>, name: &str) -> Result<String, ErrorData> {
    arg(args, name).ok_or_else(|| {
        ErrorData::invalid_params(format!("Missing required argument: {}", name), None)
    })
}

/// Render a prompt. Unknown prompt or bad arguments → JSON-RPC `-32602`.
pub fn get(
    require_auth: bool,
    name: &str,
    args: Option<JsonObject>,
) -> Result<GetPromptResult, ErrorData> {
    let entity = required(&args, "entity")?;
    if find_entity(require_auth, &entity).is_none() {
        return Err(ErrorData::invalid_params(
            format!(
                "Unknown entity '{}'. Use `list_entities` to see valid names.",
                entity
            ),
            None,
        ));
    }

    let (description, text) = match name {
        EXPLORE_ENTITY => (
            format!("Explore entity `{}`", entity),
            format!(
                "Help me understand the `{e}` entity of our Flexurio API.\n\n\
                 1. Call `describe_entity` with entity `{e}` and summarise its purpose, primary key, \
                 important columns, relations (foreign keys, master-detail) and any locks or state machine.\n\
                 2. Call `query_records` for `{e}` with a small page (limit 5 when supported) and show \
                 the rows as a table.\n\
                 3. List the filters I can use, with one example for each.\n\
                 Do not modify any data.",
                e = entity
            ),
        ),
        SAFE_WRITE => {
            let goal = required(&args, "goal")?;
            (
                format!("Safely change data in `{}`", entity),
                format!(
                    "Goal for entity `{e}`: {goal}\n\n\
                     Follow this procedure strictly:\n\
                     1. Call `describe_entity` for `{e}` to learn required columns, generated fields, \
                     detail arrays, locks (`locked_when`) and allowed state transitions.\n\
                     2. Use `query_records` to find the affected record(s) and show their current values.\n\
                     3. Propose the exact tool call(s) (tool, entity, id, data) and wait for my explicit \
                     confirmation before executing anything.\n\
                     4. After I confirm, execute the calls one at a time; stop at the first error and explain it.\n\
                     5. Re-read the records with `query_records` and report what changed.",
                    e = entity,
                    goal = goal
                ),
            )
        }
        DATA_QUALITY_REPORT => {
            let focus = arg(&args, "focus")
                .map(|f| format!(" Pay special attention to: {}.", f))
                .unwrap_or_default();
            (
                format!("Data quality report for `{}`", entity),
                format!(
                    "Produce a data-quality report for entity `{e}`.{focus}\n\n\
                     1. Call `validate_entity` for `{e}` and report any mismatch between config and table.\n\
                     2. Call `describe_entity` to identify required, unique and foreign-key columns.\n\
                     3. Sample data with `query_records` (paginate if needed) and check for missing \
                     required values, duplicate unique keys, and dangling references.\n\
                     4. Summarise findings as a prioritised list with counts and example ids. \
                     Do not modify any data.",
                    e = entity,
                    focus = focus
                ),
            )
        }
        other => {
            return Err(ErrorData::invalid_params(
                format!("Unknown prompt: {}", other),
                None,
            ));
        }
    };

    Ok(
        GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)])
            .with_description(description),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_declares_three_prompts_with_required_entity() {
        let prompts = list();
        assert_eq!(prompts.len(), 3);
        for p in prompts {
            let args = p.arguments.expect("arguments");
            let entity = args
                .iter()
                .find(|a| a.name == "entity")
                .expect("entity arg");
            assert_eq!(entity.required, Some(true));
        }
    }

    #[test]
    fn test_missing_entity_is_invalid_params() {
        let err = get(true, EXPLORE_ENTITY, None).unwrap_err();
        assert!(err.message.contains("entity"));
    }

    #[test]
    fn test_arg_trims_and_ignores_empty() {
        let mut m = JsonObject::new();
        m.insert("goal".into(), Value::String("  ".into()));
        m.insert("entity".into(), Value::String(" x ".into()));
        let a = Some(m);
        assert_eq!(arg(&a, "goal"), None);
        assert_eq!(arg(&a, "entity").as_deref(), Some("x"));
    }
}
