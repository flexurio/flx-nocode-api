//! stdio transport: `flx-nocode-api mcp --stdio [--token <jwt> | --email <user>]`.
//!
//! Local MCP clients (Claude Desktop, Claude Code, Cursor, …) spawn the binary
//! and exchange newline-delimited JSON-RPC over stdin/stdout. stdout must carry
//! protocol messages only, so on Unix [`isolate_stdout`] duplicates the real
//! stdout for the transport and points fd 1 at stderr — every boot message and
//! `println!` from the engine then lands on stderr, which clients show as logs.
//!
//! The caller identity is fixed for the process lifetime and resolved from, in
//! order: `--token` / `MCP_STDIO_TOKEN` (a JWT, validated exactly like REST),
//! `--email` / `MCP_STDIO_EMAIL` (an enabled `flx_users` account; a token is
//! minted for it), or anonymous when `REQUIRE_AUTH=false`.

use std::sync::Arc;

use actix_web::test::TestRequest;
use actix_web::web;
use anyhow::{Context, anyhow, bail};
use rmcp::ServiceExt;
use serde_json::Value;

use super::ctx::CallerIdentity;
use super::{FlexurioMcpServer, MCP_CONFIG};
use crate::auth::{Claims, create_token, validate_token};
use crate::database::state::AppState;
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};

/// True when the process was started as `… mcp --stdio` (or just `… mcp`).
pub fn is_stdio_command(args: &[String]) -> bool {
    args.get(1).map(String::as_str) == Some("mcp")
        && !args.iter().any(|a| a == "--help" || a == "-h")
}

/// True for `… mcp --help`.
pub fn is_help_command(args: &[String]) -> bool {
    args.get(1).map(String::as_str) == Some("mcp")
        && args.iter().any(|a| a == "--help" || a == "-h")
}

pub const HELP: &str = "\
Usage: flx-nocode-api mcp [--stdio] [--token <jwt> | --email <user-email>]

Run the Flexurio MCP server over stdio (JSON-RPC on stdin/stdout, logs on stderr).

Identity (first match wins):
  --token <jwt>     / MCP_STDIO_TOKEN   JWT from POST /login (or external IdP)
  --email <email>   / MCP_STDIO_EMAIL   enabled flx_users account
  (none)                                allowed only when REQUIRE_AUTH=false

Example client config (Claude Desktop / Claude Code):
  { \"command\": \"flx-nocode-api\", \"args\": [\"mcp\", \"--stdio\"],
    \"env\": { \"MCP_STDIO_EMAIL\": \"admin\" }, \"cwd\": \"/path/with/.env\" }
";

fn arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1).cloned())
        .filter(|v| !v.starts_with("--"))
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Point fd 1 at stderr and return a handle to the original stdout.
#[cfg(unix)]
pub fn isolate_stdout() -> std::io::Result<Option<std::fs::File>> {
    use std::io::Write;
    use std::os::fd::FromRawFd;

    std::io::stdout().flush()?;
    // SAFETY: plain POSIX fd duplication on the process's standard streams,
    // done once at start-up before any other thread writes to stdout. The
    // duplicated fd is owned exclusively by the returned `File`.
    unsafe {
        let saved = libc::dup(libc::STDOUT_FILENO);
        if saved < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(saved);
            return Err(err);
        }
        Ok(Some(std::fs::File::from_raw_fd(saved)))
    }
}

/// Non-Unix: stdout cannot be re-pointed portably; keep `DEBUG=false` so boot
/// logs stay quiet.
#[cfg(not(unix))]
pub fn isolate_stdout() -> std::io::Result<Option<std::fs::File>> {
    eprintln!("[mcp] stdout isolation is unavailable on this platform; set DEBUG=false.");
    Ok(None)
}

fn value_to_id(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(o) => o.get("$oid").and_then(Value::as_str).map(str::to_string),
        _ => None,
    }
}

/// Validate a JWT exactly as `AuthMiddleware` does (incl. converter-token mode).
fn claims_from_token(state: &web::Data<AppState>, token: &str) -> anyhow::Result<Claims> {
    let bearer = if token.starts_with("Bearer ") {
        token.to_string()
    } else {
        format!("Bearer {}", token)
    };
    // No peer address: IP-whitelist shortcuts must not replace a real identity.
    let req = TestRequest::default()
        .uri("/mcp")
        .insert_header(("authorization", bearer))
        .to_http_request();
    validate_token(&req, state).map_err(|_| anyhow!("the MCP stdio token is invalid or expired"))
}

async fn claims_from_email(
    state: &web::Data<AppState>,
    email: &str,
) -> anyhow::Result<(Claims, String)> {
    let q_user = QQ::from("flx_users")
        .select(["id", "name"])
        .r#where(QF::And(vec![
            QF::Eq("email".into(), QV::Str(email.to_string())),
            QF::Eq("enabled".into(), QV::Bool(true)),
        ]))
        .limit(1);
    let rows = state
        .store
        .query(&q_user)
        .await
        .context("looking up flx_users")?;
    let row = rows
        .first()
        .ok_or_else(|| anyhow!("no enabled user with email '{}'", email))?;
    let id = row
        .get("id")
        .and_then(value_to_id)
        .ok_or_else(|| anyhow!("user '{}' has no id", email))?;
    let name = row
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(email)
        .to_string();

    let id_filter = id
        .parse::<i64>()
        .map(QV::I64)
        .unwrap_or_else(|_| QV::Str(id.clone()));
    let q_roles = QQ::from("flx_roles")
        .select(["role"])
        .r#where(QF::Eq("id_users".into(), id_filter));
    let roles = state
        .store
        .query(&q_roles)
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|r| match r.get("role") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(",");

    let token = create_token(id, name, state.clone(), roles).await;
    if token.is_empty() {
        bail!("failed to mint a token for '{}'", email);
    }
    let claims = claims_from_token(state, &token)?;
    Ok((claims, token))
}

/// Resolve the fixed identity of this stdio session.
pub async fn resolve_identity(
    state: &web::Data<AppState>,
    args: &[String],
) -> anyhow::Result<CallerIdentity> {
    if let Some(token) = arg_value(args, "--token").or_else(|| env_nonempty("MCP_STDIO_TOKEN")) {
        let claims = claims_from_token(state, &token)?;
        return Ok(CallerIdentity::from_claims(claims, Some(token)));
    }
    if let Some(email) = arg_value(args, "--email").or_else(|| env_nonempty("MCP_STDIO_EMAIL")) {
        let (claims, token) = claims_from_email(state, &email).await?;
        return Ok(CallerIdentity::from_claims(claims, Some(token)));
    }
    if !state.require_auth {
        return Ok(CallerIdentity::from_claims(Claims::default(), None));
    }
    bail!(
        "REQUIRE_AUTH is enabled: start with --token <jwt> / MCP_STDIO_TOKEN \
         or --email <user> / MCP_STDIO_EMAIL"
    )
}

/// Serve MCP over stdio until the client closes the stream.
pub async fn run(
    state: web::Data<AppState>,
    args: &[String],
    stdout: Option<std::fs::File>,
) -> anyhow::Result<()> {
    let identity = resolve_identity(&state, args).await?;
    eprintln!(
        "[mcp] Flexurio MCP server {} on stdio as {} ({} entities)",
        env!("CARGO_PKG_VERSION"),
        identity.subject(),
        super::tools::exposed_entities(state.require_auth).len()
    );

    let server =
        FlexurioMcpServer::new(state, Arc::new(MCP_CONFIG.clone())).with_identity(identity);
    let running = match stdout {
        Some(file) => server
            .serve((tokio::io::stdin(), tokio::fs::File::from_std(file)))
            .await
            .map_err(|e| anyhow!("MCP stdio initialization failed: {}", e))?,
        None => server
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|e| anyhow!("MCP stdio initialization failed: {}", e))?,
    };
    running
        .waiting()
        .await
        .map_err(|e| anyhow!("MCP stdio server stopped: {}", e))?;
    eprintln!("[mcp] client disconnected, shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_command_detection() {
        assert!(is_stdio_command(&a(&["bin", "mcp"])));
        assert!(is_stdio_command(&a(&["bin", "mcp", "--stdio"])));
        assert!(!is_stdio_command(&a(&["bin", "mcp", "--help"])));
        assert!(is_help_command(&a(&["bin", "mcp", "-h"])));
        assert!(!is_stdio_command(&a(&["bin", "reset-password"])));
        assert!(!is_stdio_command(&a(&["bin"])));
    }

    #[test]
    fn test_arg_value_ignores_following_flag() {
        let args = a(&["bin", "mcp", "--email", "--stdio"]);
        assert_eq!(arg_value(&args, "--email"), None);
        let args = a(&["bin", "mcp", "--email", "admin"]);
        assert_eq!(arg_value(&args, "--email").as_deref(), Some("admin"));
    }

    #[test]
    fn test_value_to_id_variants() {
        assert_eq!(value_to_id(&serde_json::json!(5)).as_deref(), Some("5"));
        assert_eq!(value_to_id(&serde_json::json!("u1")).as_deref(), Some("u1"));
        assert_eq!(
            value_to_id(&serde_json::json!({"$oid": "abc"})).as_deref(),
            Some("abc")
        );
        assert_eq!(value_to_id(&serde_json::json!("")), None);
        assert_eq!(value_to_id(&Value::Null), None);
    }
}
