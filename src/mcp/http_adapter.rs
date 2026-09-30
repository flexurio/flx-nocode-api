//! Streamable HTTP transport mounted on Actix-web.
//!
//! rmcp's [`StreamableHttpService`] speaks `http` 1.x types while Actix-web 4
//! uses `http` 0.2, so this adapter converts the request (method, URI, headers,
//! body) in, calls [`StreamableHttpService::handle`], and converts the response
//! back — buffering plain JSON replies and streaming `text/event-stream` ones.
//!
//! The server runs **stateless** (no `Mcp-Session-Id`), which is safe with
//! multiple Actix workers and matches the direction of MCP `2026-07-28`
//! (sessions removed). rmcp still negotiates older protocol versions.
//!
//! Authentication happens *before* this handler, in `AuthMiddleware`; the
//! validated caller is captured as a [`CallerIdentity`] and attached to the
//! request extensions, where [`super::server::FlexurioMcpServer`] reads it.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::StreamExt;
use http_body_util::{BodyExt, BodyStream, Full};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};

use super::ctx::CallerIdentity;
use super::{FlexurioMcpServer, McpConfig};
use crate::database::state::AppState;
use crate::log::log_output;
use crate::metrics::METRICS;

/// The rmcp HTTP service type shared by all Actix workers.
pub type McpHttpService = StreamableHttpService<FlexurioMcpServer, NeverSessionManager>;

/// Hop-by-hop / framing headers Actix computes itself.
const SKIP_RESPONSE_HEADERS: [&str; 3] = ["content-length", "transfer-encoding", "connection"];

/// Build the shared Streamable HTTP service.
pub fn build_service(state: web::Data<AppState>, cfg: Arc<McpConfig>) -> McpHttpService {
    let server = FlexurioMcpServer::new(state, cfg.clone());

    let max_body = std::env::var("UPLOAD_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(10 * 1024 * 1024);

    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(Some(Duration::from_secs(15)))
        .with_max_request_body_bytes(max_body);
    config = if cfg.allowed_hosts.is_empty() {
        config.disable_allowed_hosts()
    } else {
        config.with_allowed_hosts(cfg.allowed_hosts.clone())
    };
    if !cfg.allowed_origins.is_empty() {
        config = config.with_allowed_origins(cfg.allowed_origins.clone());
    }

    StreamableHttpService::new(
        move || Ok(server.clone()),
        Arc::new(NeverSessionManager::default()),
        config,
    )
}

/// Convert an Actix request + body into an `http` 1.x request.
pub fn to_http1_request(
    req: &HttpRequest,
    body: web::Bytes,
) -> Result<http1::Request<Full<web::Bytes>>, String> {
    let method = http1::Method::from_bytes(req.method().as_str().as_bytes())
        .map_err(|e| format!("invalid method: {}", e))?;
    let uri: http1::Uri = req
        .uri()
        .to_string()
        .parse()
        .map_err(|e| format!("invalid uri: {}", e))?;

    let mut builder = http1::Request::builder().method(method).uri(uri);
    // Actix keeps the Host header out of `uri()` for server requests; forward it.
    for (name, value) in req.headers().iter() {
        if let (Ok(n), Ok(v)) = (
            http1::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            http1::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            builder = builder.header(n, v);
        }
    }
    let mut out = builder
        .body(Full::new(body))
        .map_err(|e| format!("invalid request: {}", e))?;
    out.extensions_mut().insert(CallerIdentity::from_actix(req));
    Ok(out)
}

/// Convert an rmcp response into an Actix response.
pub async fn to_actix_response<B>(resp: http1::Response<B>) -> HttpResponse
where
    B: http_body::Body<Data = web::Bytes, Error = Infallible> + Unpin + 'static,
{
    let (parts, body) = resp.into_parts();
    let status =
        StatusCode::from_u16(parts.status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = HttpResponse::build(status);
    let mut is_sse = false;
    for (name, value) in parts.headers.iter() {
        let n = name.as_str();
        if SKIP_RESPONSE_HEADERS.contains(&n) {
            continue;
        }
        if n == "content-type" && value.as_bytes().starts_with(b"text/event-stream") {
            is_sse = true;
        }
        builder.append_header((n, value.as_bytes()));
    }

    if is_sse {
        // Keep the global Compress middleware from buffering the event stream.
        builder.insert_header(("content-encoding", "identity"));
        builder.insert_header(("x-accel-buffering", "no"));
        let stream = BodyStream::new(body).filter_map(|frame| async move {
            match frame {
                Ok(f) => f.into_data().ok().map(Ok::<web::Bytes, Infallible>),
                Err(never) => match never {},
            }
        });
        builder.streaming(stream)
    } else {
        match body.collect().await {
            Ok(collected) => builder.body(collected.to_bytes()),
            Err(never) => match never {},
        }
    }
}

/// Actix handler for `POST|GET|DELETE {MCP_PATH}`.
pub async fn handle(
    req: HttpRequest,
    body: web::Bytes,
    svc: web::Data<McpHttpService>,
) -> HttpResponse {
    METRICS.record_mcp_request();
    let http_req = match to_http1_request(&req, body) {
        Ok(r) => r,
        Err(e) => {
            log_output("ERROR", "MCP HTTP", "adapter", e.clone(), true);
            return HttpResponse::BadRequest().json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32600, "message": format!("Invalid request: {}", e) }
            }));
        }
    };
    let resp = svc.handle(http_req).await;
    to_actix_response(resp).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;
    use http_body_util::Full;

    #[test]
    fn test_to_http1_request_copies_method_uri_headers_and_identity() {
        let req = TestRequest::post()
            .uri("/mcp?x=1")
            .insert_header(("authorization", "Bearer t"))
            .insert_header(("mcp-protocol-version", "2025-11-25"))
            .insert_header(("host", "localhost:8080"))
            .to_http_request();
        let out = to_http1_request(&req, web::Bytes::from_static(b"{}")).unwrap();
        assert_eq!(out.method(), http1::Method::POST);
        assert_eq!(out.uri().path(), "/mcp");
        assert_eq!(out.uri().query(), Some("x=1"));
        assert_eq!(out.headers()["authorization"], "Bearer t");
        assert_eq!(out.headers()["mcp-protocol-version"], "2025-11-25");
        let id = out.extensions().get::<CallerIdentity>().unwrap();
        assert_eq!(id.authorization.as_deref(), Some("Bearer t"));
    }

    #[actix_web::test]
    async fn test_to_actix_response_buffers_json_and_drops_framing_headers() {
        let resp = http1::Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .header("content-length", "2")
            .body(Full::new(web::Bytes::from_static(b"{}")))
            .unwrap();
        let out = to_actix_response(resp).await;
        assert_eq!(out.status(), StatusCode::OK);
        assert_eq!(
            out.headers().get("content-type").unwrap(),
            "application/json"
        );
        let bytes = actix_web::body::to_bytes(out.into_body()).await.unwrap();
        assert_eq!(&bytes[..], b"{}");
    }

    #[actix_web::test]
    async fn test_to_actix_response_streams_sse_without_compression() {
        let resp = http1::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(Full::new(web::Bytes::from_static(b"data: {}\n\n")))
            .unwrap();
        let out = to_actix_response(resp).await;
        assert_eq!(out.headers().get("content-encoding").unwrap(), "identity");
        let bytes = actix_web::body::to_bytes(out.into_body()).await.unwrap();
        assert_eq!(&bytes[..], b"data: {}\n\n");
    }
}
