use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;

use crate::auth::{check_access, get_user_info_from_token};
use crate::database::state::AppState;
use crate::model::WebResponse;
use crate::nocode::email_queue::{enqueue_email, EmailAttachment, EmailJob};

/// JSON body accepted by `POST /email/send`.
///
/// `attachments[].content_base64` must hold the file bytes encoded as base64.
/// Example:
/// ```json
/// {
///   "to": ["user@example.com"],
///   "cc": ["boss@example.com"],
///   "subject": "Invoice #123",
///   "content_email": "<h1>Hello</h1>",
///   "is_html": true,
///   "attachments": [
///     { "filename": "invoice.pdf", "content_type": "application/pdf", "content_base64": "JVBERi0..." }
///   ]
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct SendEmailRequest {
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    pub subject: String,
    pub content_email: String,
    #[serde(default)]
    pub is_html: bool,
    #[serde(default)]
    pub attachments: Vec<EmailAttachment>,
}

/// Push an email job onto the Redis queue. The SMTP consumer delivers it.
///
/// Requires a valid bearer token (same scheme as the nocode write endpoints)
/// when `require_auth` is enabled.
pub async fn send(
    state: web::Data<AppState>,
    http_req: HttpRequest,
    payload: web::Json<SendEmailRequest>,
) -> impl Responder {
    // ── Auth check ─────────────────────────────────────────────────────────
    if state.require_auth {
        let claims = match get_user_info_from_token(&http_req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                    data: serde_json::Value::Null,
                });
            }
        };
        if let Err(e) = check_access(&claims, &http_req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: serde_json::Value::Null,
            });
        }
    }

    let req = payload.into_inner();

    if req.to.is_empty() {
        return HttpResponse::BadRequest().json(WebResponse {
            success: false,
            message: "Field 'to' must contain at least one recipient".to_string(),
            total_data: 0,
            data: serde_json::Value::Null,
        });
    }

    let job = EmailJob {
        to: req.to,
        cc: req.cc,
        subject: req.subject,
        content_email: req.content_email,
        is_html: req.is_html,
        attachments: req.attachments,
        enqueued_at: chrono::Utc::now().to_rfc3339(),
    };

    match enqueue_email(&job).await {
        Ok(queue_len) => HttpResponse::Ok().json(WebResponse {
            success: true,
            message: "Email queued".to_string(),
            total_data: 1,
            data: serde_json::json!({ "queue_len": queue_len }),
        }),
        Err(e) => HttpResponse::InternalServerError().json(WebResponse {
            success: false,
            message: format!("Failed to enqueue email: {}", e),
            total_data: 0,
            data: serde_json::Value::Null,
        }),
    }
}
