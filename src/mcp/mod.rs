//! Model Context Protocol (MCP) server for the Flexurio no-code engine.
//!
//! Exposes every configured entity (route) to MCP clients — Claude Desktop,
//! Claude Code, Cursor, the MCP Inspector, … — through the official Rust SDK
//! (`rmcp`), using the two standard transports:
//!
//! * **Streamable HTTP** on [`McpConfig::path`] (default `/mcp`), mounted on the
//!   same Actix server as the REST API and protected by the same JWT
//!   middleware (see [`auth`] for the RFC 9728 protected-resource metadata).
//! * **stdio** via `flx-nocode-api mcp --stdio` (see [`stdio`]) for local
//!   clients that spawn the server as a child process.
//!
//! ## Design
//!
//! Tools never talk to the database directly. Each tool call builds an
//! in-process request that mirrors the equivalent REST call (method, path,
//! query, caller identity — see [`ctx`]) and hands it to the existing service
//! layer (`nocode::services::*`). Authentication, `rules.json` authorization,
//! validation, caching, the Redis write queue, action triggers and the audit
//! trail therefore behave exactly as they do for REST clients.

pub mod auth;
pub mod ctx;
pub mod http_adapter;
pub mod prompts;
pub mod resources;
pub mod server;
pub mod stdio;
pub mod tools;

use once_cell::sync::Lazy;
use std::env;

pub use server::FlexurioMcpServer;

/// Name advertised in `serverInfo.name`.
pub const SERVER_NAME: &str = "flexurio-nocode";
/// Human-readable title advertised in `serverInfo.title`.
pub const SERVER_TITLE: &str = "Flexurio No-Code API";

/// Runtime configuration for the MCP server, read once from the environment.
#[derive(Debug, Clone)]
pub struct McpConfig {
    /// `MCP_ENABLED` — mount the Streamable HTTP endpoint (default `true`).
    pub enabled: bool,
    /// `MCP_PATH` — HTTP path of the MCP endpoint (default `/mcp`).
    pub path: String,
    /// `MCP_ALLOWED_HOSTS` — `Host` header allow-list (DNS-rebinding protection).
    /// Empty means "disabled" (only when explicitly set to `*`).
    pub allowed_hosts: Vec<String>,
    /// `MCP_ALLOWED_ORIGINS` — browser `Origin` allow-list (empty = not enforced).
    pub allowed_origins: Vec<String>,
    /// `MCP_MAX_ROWS` — hard cap on rows returned by one `query_records` call.
    pub max_rows: usize,
    /// `MCP_WRITE_TOOLS_ENABLED` — expose create/update/patch/delete/run tools.
    pub write_tools: bool,
    /// `MCP_RESOURCE_URL` — canonical URL of this protected resource
    /// (defaults to `BASE_URL` + `path`).
    pub resource_url: String,
    /// `MCP_OAUTH_AUTHORIZATION_SERVERS` — issuer URLs advertised in the
    /// protected-resource metadata (comma separated, optional).
    pub authorization_servers: Vec<String>,
    /// `MCP_OAUTH_SCOPES` — scopes advertised in the metadata (optional).
    pub scopes_supported: Vec<String>,
    /// `MCP_REDACT_FIELDS` — field names masked in tool output before it reaches
    /// the model (default `password,secret,token,api_key,apikey`; `none` disables).
    pub redact_fields: Vec<String>,
    /// `MCP_ADMIN_ROLES` — roles allowed to read the `flexurio://rules` resource
    /// (default `admin,Super Admin,Administrator`; compared case-insensitively).
    pub admin_roles: Vec<String>,
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .map(|v| {
            matches!(
                v.trim().to_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn env_list_from(v: &str) -> Vec<String> {
    v.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn env_list(key: &str) -> Vec<String> {
    env_list_from(&env::var(key).unwrap_or_default())
}

/// Extract `host` and `host:port` from a URL such as `https://api.example.com:8443`.
fn authorities_of(url_str: &str) -> Vec<String> {
    match url::Url::parse(url_str) {
        Ok(u) => {
            let mut out = Vec::new();
            if let Some(h) = u.host_str() {
                out.push(h.to_string());
                if let Some(p) = u.port() {
                    out.push(format!("{}:{}", h, p));
                }
            }
            out
        }
        Err(_) => Vec::new(),
    }
}

impl McpConfig {
    pub fn from_env() -> Self {
        let mut path = env::var("MCP_PATH").unwrap_or_else(|_| "/mcp".to_string());
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        while path.len() > 1 && path.ends_with('/') {
            path.pop();
        }

        let base_url = env::var("BASE_URL").unwrap_or_default();
        let base_url = base_url.trim_end_matches('/').to_string();

        let allowed_hosts = match env::var("MCP_ALLOWED_HOSTS") {
            Ok(v) if v.trim() == "*" => Vec::new(),
            Ok(v) if !v.trim().is_empty() => env_list_from(&v),
            _ => {
                let mut hosts: Vec<String> =
                    vec!["localhost".into(), "127.0.0.1".into(), "::1".into()];
                for a in authorities_of(&base_url) {
                    if !hosts.contains(&a) {
                        hosts.push(a);
                    }
                }
                hosts
            }
        };

        let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
        let resource_url = env::var("MCP_RESOURCE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| {
                if base_url.is_empty() {
                    format!("http://localhost:{}{}", port, path)
                } else {
                    format!("{}{}", base_url, path)
                }
            });

        McpConfig {
            enabled: env_bool("MCP_ENABLED", true),
            path,
            allowed_hosts,
            allowed_origins: env_list("MCP_ALLOWED_ORIGINS"),
            max_rows: env::var("MCP_MAX_ROWS")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(200),
            write_tools: env_bool("MCP_WRITE_TOOLS_ENABLED", true),
            resource_url,
            authorization_servers: env_list("MCP_OAUTH_AUTHORIZATION_SERVERS"),
            scopes_supported: env_list("MCP_OAUTH_SCOPES"),
            redact_fields: match env::var("MCP_REDACT_FIELDS") {
                Ok(v) if v.trim().eq_ignore_ascii_case("none") => Vec::new(),
                Ok(v) if !v.trim().is_empty() => env_list_from(&v.to_lowercase()),
                _ => ["password", "secret", "token", "api_key", "apikey"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            admin_roles: match env::var("MCP_ADMIN_ROLES") {
                Ok(v) if !v.trim().is_empty() => env_list_from(&v),
                _ => vec!["admin".into(), "Super Admin".into(), "Administrator".into()],
            },
        }
    }

    /// True when the caller holds one of [`Self::admin_roles`].
    pub fn is_admin(&self, claims: Option<&crate::auth::Claims>) -> bool {
        claims.is_some_and(|c| {
            c.get_roles().iter().any(|r| {
                self.admin_roles
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(r.trim()))
            })
        })
    }

    /// True when `path` is the MCP endpoint (used by the auth middleware to
    /// attach the `WWW-Authenticate` challenge required by the MCP spec).
    pub fn is_mcp_path(&self, path: &str) -> bool {
        let p = path.trim_end_matches('/');
        p == self.path || (p.is_empty() && self.path == "/")
    }
}

/// Process-wide MCP configuration.
pub static MCP_CONFIG: Lazy<McpConfig> = Lazy::new(McpConfig::from_env);

/// Register the MCP endpoint and the RFC 9728 discovery documents.
///
/// Must be called **before** `routes::configure_routes` so `MCP_PATH` wins over
/// an entity accidentally named like it.
pub fn configure(
    cfg: &mut actix_web::web::ServiceConfig,
    service: Option<actix_web::web::Data<http_adapter::McpHttpService>>,
    do_log: bool,
    host: &str,
    port: u16,
) {
    use actix_web::web;
    use colored::Colorize;

    let Some(service) = service else { return };
    let mcp = &*MCP_CONFIG;

    cfg.service(
        web::resource(mcp.path.as_str())
            .app_data(service)
            .route(web::post().to(http_adapter::handle))
            .route(web::get().to(http_adapter::handle))
            .route(web::delete().to(http_adapter::handle)),
    );
    for p in auth::metadata_paths(mcp) {
        cfg.service(web::resource(p).route(web::get().to(auth::protected_resource_metadata)));
    }

    if do_log {
        let url = |p: &str| {
            format!(
                "http://{}:{}{}",
                host.red(),
                port.to_string().green(),
                p.purple()
            )
        };
        crate::log::log_output("MCP ENDPOINT", "METHOD", "POST", url(&mcp.path), false);
        crate::log::log_output(
            "MCP ENDPOINT",
            "METHOD",
            "GET",
            url(auth::WELL_KNOWN_PREFIX),
            false,
        );
        if crate::config::CONFIG
            .routes
            .iter()
            .any(|r| format!("/{}", r) == mcp.path)
        {
            crate::log::log_output(
                "WARN",
                "MCP",
                "path",
                format!(
                    "entity route '{}' is shadowed by MCP_PATH; set MCP_PATH to another path",
                    mcp.path
                ),
                false,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorities_of_url_with_port() {
        let a = authorities_of("https://api.example.com:8443/x");
        assert_eq!(a, vec!["api.example.com", "api.example.com:8443"]);
    }

    #[test]
    fn test_authorities_of_url_without_port() {
        let a = authorities_of("https://api.example.com");
        assert_eq!(a, vec!["api.example.com"]);
    }

    #[test]
    fn test_authorities_of_invalid_url() {
        assert!(authorities_of("not a url").is_empty());
    }

    #[test]
    fn test_env_list_from_trims_and_drops_empty() {
        assert_eq!(env_list_from(" a, ,b ,"), vec!["a", "b"]);
    }

    #[test]
    fn test_is_mcp_path_ignores_trailing_slash() {
        let mut cfg = McpConfig::from_env();
        cfg.path = "/mcp".into();
        assert!(cfg.is_mcp_path("/mcp"));
        assert!(cfg.is_mcp_path("/mcp/"));
        assert!(!cfg.is_mcp_path("/mcpx"));
        assert!(!cfg.is_mcp_path("/flx_users"));
    }
}
