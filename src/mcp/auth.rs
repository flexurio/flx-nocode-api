//! MCP authorization discovery (spec: "Authorization", RFC 9728).
//!
//! The MCP endpoint is an OAuth 2.0 *protected resource*: it accepts
//! `Authorization: Bearer <token>` and, when a request is unauthenticated,
//! answers `401` with
//!
//! ```text
//! WWW-Authenticate: Bearer resource_metadata="https://host/.well-known/oauth-protected-resource/mcp"
//! ```
//!
//! so clients can discover the authorization server(s) from the Protected
//! Resource Metadata document served here. Tokens are the same JWTs the REST
//! API accepts: Flexurio-issued (`POST /login`) or externally issued in
//! converter-token mode (`CONVERTER_JWT_*`), which is how a real OAuth
//! authorization server is plugged in (`MCP_OAUTH_AUTHORIZATION_SERVERS`).

use actix_web::{HttpResponse, http::header};
use serde::Serialize;

use super::{McpConfig, SERVER_TITLE};

/// Well-known path prefix defined by RFC 9728 §3.
pub const WELL_KNOWN_PREFIX: &str = "/.well-known/oauth-protected-resource";

/// RFC 9728 Protected Resource Metadata.
#[derive(Debug, Serialize, PartialEq)]
pub struct ProtectedResourceMetadata {
    pub resource: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub authorization_servers: Vec<String>,
    pub bearer_methods_supported: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub scopes_supported: Vec<String>,
    pub resource_name: String,
}

impl ProtectedResourceMetadata {
    pub fn from_config(cfg: &McpConfig) -> Self {
        ProtectedResourceMetadata {
            resource: cfg.resource_url.clone(),
            authorization_servers: cfg.authorization_servers.clone(),
            bearer_methods_supported: vec!["header".into()],
            scopes_supported: cfg.scopes_supported.clone(),
            resource_name: SERVER_TITLE.to_string(),
        }
    }
}

/// Absolute URL of the metadata document for `cfg.resource_url`
/// (RFC 9728 §3.1: the well-known segment goes between host and path).
pub fn metadata_url(cfg: &McpConfig) -> String {
    match url::Url::parse(&cfg.resource_url) {
        Ok(u) => {
            let origin = u.origin().ascii_serialization();
            let path = u.path().trim_end_matches('/');
            format!("{}{}{}", origin, WELL_KNOWN_PREFIX, path)
        }
        Err(_) => format!("{}{}", WELL_KNOWN_PREFIX, cfg.path),
    }
}

/// Paths on which the metadata document is served.
pub fn metadata_paths(cfg: &McpConfig) -> Vec<String> {
    let mut paths = vec![WELL_KNOWN_PREFIX.to_string()];
    if cfg.path != "/" {
        paths.push(format!("{}{}", WELL_KNOWN_PREFIX, cfg.path));
    }
    paths
}

/// True for requests that must bypass authentication (discovery documents).
pub fn is_public_discovery_path(path: &str) -> bool {
    path == WELL_KNOWN_PREFIX || path.starts_with(&format!("{}/", WELL_KNOWN_PREFIX))
}

/// `WWW-Authenticate` challenge for a 401 on the MCP endpoint.
pub fn www_authenticate(cfg: &McpConfig, token_presented: bool) -> String {
    let mut v = format!("Bearer resource_metadata=\"{}\"", metadata_url(cfg));
    if !cfg.scopes_supported.is_empty() {
        v.push_str(&format!(", scope=\"{}\"", cfg.scopes_supported.join(" ")));
    }
    if token_presented {
        v.push_str(", error=\"invalid_token\"");
    }
    v
}

/// Handler for `GET /.well-known/oauth-protected-resource[/mcp]`.
pub async fn protected_resource_metadata() -> HttpResponse {
    let cfg = &*super::MCP_CONFIG;
    HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "public, max-age=3600"))
        .json(ProtectedResourceMetadata::from_config(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(resource: &str) -> McpConfig {
        let mut c = McpConfig::from_env();
        c.path = "/mcp".into();
        c.resource_url = resource.into();
        c.scopes_supported = vec![];
        c.authorization_servers = vec![];
        c
    }

    #[test]
    fn test_metadata_url_inserts_well_known_before_path() {
        let c = cfg("https://api.example.com/mcp");
        assert_eq!(
            metadata_url(&c),
            "https://api.example.com/.well-known/oauth-protected-resource/mcp"
        );
    }

    #[test]
    fn test_metadata_url_keeps_port() {
        let c = cfg("http://localhost:8080/mcp");
        assert_eq!(
            metadata_url(&c),
            "http://localhost:8080/.well-known/oauth-protected-resource/mcp"
        );
    }

    #[test]
    fn test_www_authenticate_variants() {
        let mut c = cfg("https://a.b/mcp");
        assert_eq!(
            www_authenticate(&c, false),
            "Bearer resource_metadata=\"https://a.b/.well-known/oauth-protected-resource/mcp\""
        );
        c.scopes_supported = vec!["read".into(), "write".into()];
        let v = www_authenticate(&c, true);
        assert!(v.contains("scope=\"read write\""));
        assert!(v.ends_with("error=\"invalid_token\""));
    }

    #[test]
    fn test_metadata_serialization_omits_empty_lists() {
        let c = cfg("https://a.b/mcp");
        let v = serde_json::to_value(ProtectedResourceMetadata::from_config(&c)).unwrap();
        assert_eq!(v["resource"], "https://a.b/mcp");
        assert_eq!(v["bearer_methods_supported"], serde_json::json!(["header"]));
        assert!(v.get("authorization_servers").is_none());
        assert!(v.get("scopes_supported").is_none());
    }

    #[test]
    fn test_discovery_paths() {
        let c = cfg("https://a.b/mcp");
        assert_eq!(
            metadata_paths(&c),
            vec![
                WELL_KNOWN_PREFIX.to_string(),
                format!("{}/mcp", WELL_KNOWN_PREFIX)
            ]
        );
        assert!(is_public_discovery_path(
            "/.well-known/oauth-protected-resource"
        ));
        assert!(is_public_discovery_path(
            "/.well-known/oauth-protected-resource/mcp"
        ));
        assert!(!is_public_discovery_path(
            "/.well-known/oauth-protected-resourcex"
        ));
        assert!(!is_public_discovery_path("/mcp"));
    }
}
