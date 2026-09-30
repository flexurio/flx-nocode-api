//! Caller identity and the in-process request bridge used by MCP tools.
//!
//! The service layer (`nocode::services::*`) takes an Actix [`HttpRequest`] so it
//! can read the caller's token (`get_user_info_from_token`), evaluate
//! `rules.json` against the request path + method (`check_access`), and record
//! the client IP for rate limiting and audit.
//!
//! An MCP tool call is *not* an HTTP request to that path, so [`ServiceRequest`]
//! builds a faithful synthetic one: the same method and path as the equivalent
//! REST endpoint, the caller's original `Authorization` header, peer address and
//! proxy headers, and — most importantly — the `Claims` that `AuthMiddleware`
//! already validated for the `/mcp` request. Services find those claims in the
//! request extensions (their fast path), so the token is verified exactly once
//! and authorization is identical to REST.

use actix_web::body::MessageBody;
use actix_web::http::Method;
use actix_web::http::header::AUTHORIZATION;
use actix_web::test::TestRequest;
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use serde_json::{Map, Value};
use std::net::SocketAddr;

use crate::auth::Claims;

/// Who is calling an MCP tool. Carried from the transport into every tool call.
#[derive(Clone, Debug, Default)]
pub struct CallerIdentity {
    /// Claims validated by `AuthMiddleware` (HTTP) or resolved at start-up (stdio).
    pub claims: Option<Claims>,
    /// Raw `Authorization` header value, forwarded for services that re-read it.
    pub authorization: Option<String>,
    /// TCP peer address of the MCP client (HTTP transport only).
    pub peer_addr: Option<SocketAddr>,
    /// `X-Forwarded-For` as received (only honoured when `TRUST_PROXY_HEADERS`).
    pub forwarded_for: Option<String>,
    /// `X-Real-IP` as received (only honoured when `TRUST_PROXY_HEADERS`).
    pub real_ip: Option<String>,
}

impl CallerIdentity {
    /// Capture the identity of an incoming Actix request (after `AuthMiddleware`).
    pub fn from_actix(req: &HttpRequest) -> Self {
        let header = |name: &str| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };
        CallerIdentity {
            claims: req.extensions().get::<Claims>().cloned(),
            authorization: header("authorization"),
            peer_addr: req.peer_addr(),
            forwarded_for: header("x-forwarded-for"),
            real_ip: header("x-real-ip"),
        }
    }

    /// Identity for the stdio transport: fixed claims, no network peer.
    pub fn from_claims(claims: Claims, bearer: Option<String>) -> Self {
        CallerIdentity {
            claims: Some(claims),
            authorization: bearer.map(|t| {
                if t.starts_with("Bearer ") {
                    t
                } else {
                    format!("Bearer {}", t)
                }
            }),
            peer_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            forwarded_for: None,
            real_ip: None,
        }
    }

    /// Short label for logs (`name#id`), never the token.
    pub fn subject(&self) -> String {
        match &self.claims {
            Some(c) if !c.id.is_empty() => format!("{}#{}", c.nm, c.id),
            Some(c) => c.nm.clone(),
            None => "anonymous".to_string(),
        }
    }
}

/// Convert a JSON scalar to the string form the REST query parser would produce.
pub fn value_to_query_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        // Arrays/objects are sent as compact JSON, which `nin`/`between` accept.
        other => Some(other.to_string()),
    }
}

/// Normalise tool arguments into the `{"key": "string"}` object that
/// `web::Query<Value>` yields for REST requests (all values are strings there).
pub fn to_query_object(params: &Map<String, Value>) -> Map<String, Value> {
    params
        .iter()
        .filter_map(|(k, v)| value_to_query_string(v).map(|s| (k.clone(), Value::String(s))))
        .collect()
}

/// Encode a query object as `a=1&b=x%20y` (keys sorted for determinism).
pub fn encode_query(params: &Map<String, Value>) -> String {
    let mut pairs: Vec<(String, String)> = params
        .iter()
        .filter_map(|(k, v)| value_to_query_string(v).map(|s| (k.clone(), s)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// A request that mirrors one REST call, executed in-process.
pub struct ServiceRequest {
    pub method: Method,
    pub path: String,
    pub query: Map<String, Value>,
}

impl ServiceRequest {
    pub fn new(method: Method, path: impl Into<String>) -> Self {
        let mut path = path.into();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        ServiceRequest {
            method,
            path,
            query: Map::new(),
        }
    }

    pub fn with_query(mut self, query: Map<String, Value>) -> Self {
        self.query = to_query_object(&query);
        self
    }

    /// The `web::Query<Value>` payload handed to services.
    pub fn query_value(&self) -> actix_web::web::Query<Value> {
        actix_web::web::Query(Value::Object(self.query.clone()))
    }

    /// Build the synthetic [`HttpRequest`] carrying the caller's identity.
    pub fn build(&self, identity: &CallerIdentity) -> HttpRequest {
        let qs = encode_query(&self.query);
        let uri = if qs.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, qs)
        };

        let mut tr = TestRequest::default()
            .method(self.method.clone())
            .uri(&uri)
            .insert_header(("content-type", "application/json"));
        if let Some(a) = &identity.authorization {
            tr = tr.insert_header((AUTHORIZATION, a.as_str()));
        }
        if let Some(addr) = identity.peer_addr {
            tr = tr.peer_addr(addr);
        }
        if let Some(v) = &identity.forwarded_for {
            tr = tr.insert_header(("x-forwarded-for", v.as_str()));
        }
        if let Some(v) = &identity.real_ip {
            tr = tr.insert_header(("x-real-ip", v.as_str()));
        }

        let req = tr.to_http_request();
        if let Some(c) = &identity.claims {
            req.extensions_mut().insert(c.clone());
        }
        req
    }
}

/// Outcome of a service call, decoded from its `HttpResponse`.
#[derive(Debug, Clone)]
pub struct ServiceOutcome {
    pub status: u16,
    pub body: Value,
}

impl ServiceOutcome {
    /// A call succeeded when the status is 2xx and the body does not say
    /// `"success": false` (the `WebResponse` convention).
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
            && self.body.get("success").and_then(Value::as_bool) != Some(false)
    }

    /// Human-readable message (`WebResponse.message` or the raw body).
    pub fn message(&self) -> String {
        match &self.body {
            Value::Object(o) => o
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| self.body.to_string()),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    pub fn from_web_response(status: u16, resp: &crate::model::WebResponse) -> Self {
        ServiceOutcome {
            status,
            body: serde_json::to_value(resp).unwrap_or(Value::Null),
        }
    }
}

/// Drain an Actix response produced by the service layer into a [`ServiceOutcome`].
pub async fn outcome_from_response<B>(resp: HttpResponse<B>) -> ServiceOutcome
where
    B: MessageBody + 'static,
{
    let status = resp.status().as_u16();
    let bytes = match actix_web::body::to_bytes(resp.into_body()).await {
        Ok(b) => b,
        Err(_) => {
            return ServiceOutcome {
                status: 500,
                body: Value::String("Failed to read service response body".into()),
            };
        }
    };
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    ServiceOutcome { status, body }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn test_to_query_object_stringifies_scalars_and_drops_null() {
        let q = to_query_object(&obj(json!({"a": 1, "b": true, "c": "x", "d": null})));
        assert_eq!(q.get("a"), Some(&json!("1")));
        assert_eq!(q.get("b"), Some(&json!("true")));
        assert_eq!(q.get("c"), Some(&json!("x")));
        assert!(!q.contains_key("d"));
    }

    #[test]
    fn test_to_query_object_serialises_arrays_as_json() {
        let q = to_query_object(&obj(json!({"id.nin": [1, 2]})));
        assert_eq!(q.get("id.nin"), Some(&json!("[1,2]")));
    }

    #[test]
    fn test_encode_query_is_sorted_and_escaped() {
        let qs = encode_query(&obj(json!({"z": "a b", "a": "1"})));
        assert_eq!(qs, "a=1&z=a%20b");
    }

    #[test]
    fn test_build_carries_method_path_query_and_auth() {
        let identity = CallerIdentity {
            authorization: Some("Bearer abc".into()),
            ..Default::default()
        };
        let sr = ServiceRequest::new(Method::GET, "master_employee")
            .with_query(obj(json!({"nik.eq": "001"})));
        let req = sr.build(&identity);
        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.path(), "/master_employee");
        assert_eq!(req.query_string(), "nik.eq=001");
        assert_eq!(
            req.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer abc"
        );
    }

    #[test]
    fn test_build_injects_validated_claims() {
        let claims = Claims {
            id: "7".into(),
            nm: "tester".into(),
            exp: 0,
            at: 0,
            rl: "admin".into(),
            cs: String::new(),
        };
        let identity = CallerIdentity::from_claims(claims, None);
        let req = ServiceRequest::new(Method::DELETE, "/x/1").build(&identity);
        let got = req.extensions().get::<Claims>().cloned().unwrap();
        assert_eq!(got.id, "7");
        assert_eq!(identity.subject(), "tester#7");
    }

    #[test]
    fn test_from_claims_prefixes_bearer() {
        let id = CallerIdentity::from_claims(Claims::default(), Some("tok".into()));
        assert_eq!(id.authorization.as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn test_outcome_success_rules() {
        let ok = ServiceOutcome {
            status: 200,
            body: json!({"success": true}),
        };
        let soft_fail = ServiceOutcome {
            status: 200,
            body: json!({"success": false, "message": "x"}),
        };
        let hard_fail = ServiceOutcome {
            status: 401,
            body: json!("Invalid token"),
        };
        assert!(ok.is_success());
        assert!(!soft_fail.is_success());
        assert_eq!(soft_fail.message(), "x");
        assert!(!hard_fail.is_success());
        assert_eq!(hard_fail.message(), "Invalid token");
    }

    #[actix_web::test]
    async fn test_outcome_from_response_parses_json() {
        let resp = HttpResponse::Ok().json(json!({"success": true, "total_data": 1}));
        let out = outcome_from_response(resp).await;
        assert_eq!(out.status, 200);
        assert_eq!(out.body["total_data"], json!(1));
    }
}
