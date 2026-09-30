//! `ServerHandler` implementation: wires tools, resources, prompts and
//! completions to the Flexurio engine.
//!
//! ## Threading
//!
//! rmcp requires handler futures to be `Send`, but the service layer works on
//! Actix [`actix_web::HttpRequest`] values, which are `!Send`. Tool calls are
//! therefore executed on a [`LocalPoolHandle`] — a small pool of single-threaded
//! Tokio runtimes — where `!Send` futures are allowed. Only `Send` data (state,
//! identity, arguments, result) crosses the boundary. Cancellation from the
//! client (`notifications/cancelled`) aborts the pooled task.

use std::borrow::Cow;
use std::sync::Arc;

use actix_web::web;
use once_cell::sync::Lazy;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CompleteRequestParams, CompleteResult, CompletionInfo,
    GetPromptRequestParams, GetPromptResponse, Implementation, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, Reference,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use tokio_util::task::LocalPoolHandle;

use super::ctx::CallerIdentity;
use super::tools::exposed_entities;
use super::{McpConfig, SERVER_NAME, SERVER_TITLE, prompts, resources, tools};
use crate::database::state::AppState;

/// Pool of single-threaded runtimes executing tool calls (`MCP_WORKERS`).
static TOOL_POOL: Lazy<LocalPoolHandle> = Lazy::new(|| {
    let default = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(2, 16);
    let n = std::env::var("MCP_WORKERS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default);
    LocalPoolHandle::new(n)
});

const INSTRUCTIONS: &str = "Flexurio No-Code API: every entity is a database table exposed \
through generic tools. Start with `list_entities`, then `describe_entity` before querying or \
writing. `query_records` filters use the entity's declared keys `<column>.<op>` (eq, like, lt, \
lte, gt, gte, is, nin, between). Confirm with the user before any create/update/patch/delete/\
run_procedure call. Authorization follows the server's rules.json for the caller's token. \
Read the `flexurio://guide` resource for details.";

/// The MCP server. Cheap to clone (all fields are `Arc`s); one instance is
/// created per MCP session / stateless request by the transport.
#[derive(Clone)]
pub struct FlexurioMcpServer {
    state: web::Data<AppState>,
    cfg: Arc<McpConfig>,
    /// Identity used when the transport carries none (stdio).
    fallback_identity: Option<CallerIdentity>,
}

impl FlexurioMcpServer {
    pub fn new(state: web::Data<AppState>, cfg: Arc<McpConfig>) -> Self {
        FlexurioMcpServer {
            state,
            cfg,
            fallback_identity: None,
        }
    }

    /// Use a fixed identity (stdio transport).
    pub fn with_identity(mut self, identity: CallerIdentity) -> Self {
        self.fallback_identity = Some(identity);
        self
    }

    /// Identity of the caller: the one attached by the HTTP adapter to the
    /// request parts, otherwise the fixed stdio identity.
    fn identity(&self, ctx: &RequestContext<RoleServer>) -> CallerIdentity {
        ctx.extensions
            .get::<http1::request::Parts>()
            .and_then(|p| p.extensions.get::<CallerIdentity>().cloned())
            .or_else(|| self.fallback_identity.clone())
            .unwrap_or_default()
    }

    fn entity_names(&self) -> Vec<String> {
        exposed_entities(self.state.require_auth)
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    }
}

fn completion(values: Vec<String>) -> CompleteResult {
    let total = values.len();
    let mut values = values;
    values.truncate(CompletionInfo::MAX_VALUES);
    let info = CompletionInfo::with_pagination(
        values,
        Some(total as u32),
        total > CompletionInfo::MAX_VALUES,
    )
    .unwrap_or_else(|_| CompletionInfo::new(Vec::new()).expect("empty completion is valid"));
    CompleteResult::new(info)
}

impl ServerHandler for FlexurioMcpServer {
    fn get_info(&self) -> ServerConfig {
        let capabilities = ServerCapabilities::builder()
            .enable_completions()
            .enable_prompts()
            .enable_resources()
            .enable_tools()
            .build();
        ServerConfig::new(capabilities)
            .with_server_info(
                Implementation::new(SERVER_NAME, env!("CARGO_PKG_VERSION"))
                    .with_title(SERVER_TITLE),
            )
            .with_instructions(INSTRUCTIONS)
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(ProtocolVersion::KNOWN_VERSIONS)
    }

    // ── Tools ────────────────────────────────────────────────────────────────

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools::tool_definitions(
            self.state.require_auth,
            &self.cfg,
        )))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::tool_definitions(self.state.require_auth, &self.cfg)
            .into_iter()
            .find(|t| t.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let identity = self.identity(&context);
        let state = self.state.clone();
        let cfg = self.cfg.clone();
        let name = request.name.to_string();
        let args = request.arguments;

        let handle = TOOL_POOL.spawn_pinned(move || async move {
            tools::call_tool(&state, &cfg, &identity, &name, args).await
        });
        let abort = handle.abort_handle();

        tokio::select! {
            joined = handle => match joined {
                Ok(result) => result.map(CallToolResponse::Complete),
                Err(e) => Err(ErrorData::internal_error(format!("Tool execution failed: {}", e), None)),
            },
            _ = context.ct.cancelled() => {
                abort.abort();
                Err(ErrorData::internal_error("Request cancelled by client", None))
            }
        }
    }

    // ── Resources ────────────────────────────────────────────────────────────

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let identity = self.identity(&context);
        Ok(ListResourcesResult::with_all_items(resources::list(
            &self.state,
            &self.cfg,
            &identity,
        )))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(ListResourceTemplatesResult::with_all_items(
            resources::templates(),
        ))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let identity = self.identity(&context);
        resources::read(&self.state, &self.cfg, &identity, &request.uri)
            .map(ReadResourceResponse::Complete)
    }

    // ── Prompts ──────────────────────────────────────────────────────────────

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        Ok(ListPromptsResult::with_all_items(prompts::list()))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        prompts::get(self.state.require_auth, &request.name, request.arguments)
            .map(GetPromptResponse::Complete)
    }

    // ── Completions ──────────────────────────────────────────────────────────

    async fn complete(
        &self,
        request: CompleteRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let wants_entity = match &request.r#ref {
            Reference::Prompt(p) => {
                request.argument.name == "entity"
                    && prompts::list().iter().any(|x| x.name == p.name)
            }
            Reference::Resource(r) => {
                request.argument.name == "name" && r.uri == resources::URI_ENTITY_TEMPLATE
            }
            _ => false,
        };
        if !wants_entity {
            return Ok(completion(Vec::new()));
        }
        let prefix = request.argument.value.to_ascii_lowercase();
        let values: Vec<String> = self
            .entity_names()
            .into_iter()
            .filter(|n| n.to_ascii_lowercase().starts_with(&prefix))
            .collect();
        Ok(completion(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_caps_values_and_reports_more() {
        let values: Vec<String> = (0..150).map(|i| format!("e{}", i)).collect();
        let r = completion(values);
        assert_eq!(r.completion.values.len(), CompletionInfo::MAX_VALUES);
        assert_eq!(r.completion.total, Some(150));
        assert_eq!(r.completion.has_more, Some(true));
    }

    #[test]
    fn test_completion_small_list() {
        let r = completion(vec!["a".into(), "b".into()]);
        assert_eq!(r.completion.values, vec!["a", "b"]);
        assert_eq!(r.completion.has_more, Some(false));
    }
}
