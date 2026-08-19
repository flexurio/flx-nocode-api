use anyhow::{anyhow, Result};
use chrono::Utc;
use lettre::message::header::ContentType;
use lettre::message::{Attachment, Mailbox, Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use rand::RngExt;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::log::log_output;

/// A single file attached to an outgoing email.
///
/// `content_base64` holds the raw file bytes encoded as base64 so the payload
/// can be safely serialized to JSON and stored in Redis. The consumer decodes
/// it back to bytes before sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailAttachment {
    /// File name shown to the recipient, e.g. "invoice.pdf".
    pub filename: String,
    /// MIME type, e.g. "application/pdf". Defaults to octet-stream when empty.
    #[serde(default)]
    pub content_type: String,
    /// File bytes encoded as base64.
    pub content_base64: String,
}

impl EmailAttachment {
    /// Build an attachment from raw bytes, encoding them to base64.
    #[allow(dead_code)] // public producer API, called from feature code
    pub fn from_bytes(filename: impl Into<String>, content_type: impl Into<String>, bytes: &[u8]) -> Self {
        use base64::Engine;
        Self {
            filename: filename.into(),
            content_type: content_type.into(),
            content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }
}

/// An email-send job placed on the Redis queue. A separate worker pops these
/// and performs the actual SMTP/provider delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailJob {
    /// Primary recipients.
    pub to: Vec<String>,
    /// Carbon-copy recipients.
    #[serde(default)]
    pub cc: Vec<String>,
    pub subject: String,
    /// Email body. May be plain text or HTML depending on `is_html`.
    pub content_email: String,
    #[serde(default)]
    pub is_html: bool,
    #[serde(default)]
    pub attachments: Vec<EmailAttachment>,
    pub enqueued_at: String,
}

#[allow(dead_code)] // builder helpers are public producer API, called from feature code
impl EmailJob {
    /// Redis list key the email workers consume from.
    pub fn queue_key() -> String {
        std::env::var("EMAIL_QUEUE_KEY").unwrap_or_else(|_| "flx:email:default".into())
    }

    /// Dead-letter queue key for permanently failed email jobs.
    pub fn dlq_key() -> String {
        std::env::var("EMAIL_QUEUE_DLQ_KEY").unwrap_or_else(|_| "flx:email:dlq".into())
    }

    /// Convenience constructor for a simple email with no attachments.
    pub fn new(
        to: Vec<String>,
        cc: Vec<String>,
        subject: impl Into<String>,
        content_email: impl Into<String>,
    ) -> Self {
        Self {
            to,
            cc,
            subject: subject.into(),
            content_email: content_email.into(),
            is_html: false,
            attachments: Vec::new(),
            enqueued_at: Utc::now().to_rfc3339(),
        }
    }

    /// Mark the body as HTML.
    pub fn html(mut self) -> Self {
        self.is_html = true;
        self
    }

    /// Attach a file (raw bytes are base64-encoded internally).
    pub fn with_attachment(
        mut self,
        filename: impl Into<String>,
        content_type: impl Into<String>,
        bytes: &[u8],
    ) -> Self {
        self.attachments
            .push(EmailAttachment::from_bytes(filename, content_type, bytes));
        self
    }
}

/// Push an email job to the Redis list (LPUSH) and return the new queue length.
///
/// Callable from anywhere in the project — it lazily resolves the shared Redis
/// client via [`crate::database::redis::get_manager`]. Applies optional
/// backpressure (`EMAIL_QUEUE_MAX_LEN`) and retries transient push failures
/// (`EMAIL_QUEUE_ENQUEUE_RETRY`).
#[allow(dead_code)] // public producer API, called from feature code
pub async fn enqueue_email(job: &EmailJob) -> Result<i64> {
    if job.to.is_empty() {
        return Err(anyhow!("email job has no 'to' recipients"));
    }

    let max_len: i64 = std::env::var("EMAIL_QUEUE_MAX_LEN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let retry_count: usize = std::env::var("EMAIL_QUEUE_ENQUEUE_RETRY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let payload = serde_json::to_string(job)?;
    let client = crate::database::redis::get_manager().await?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    for attempt in 0..=retry_count {
        if max_len > 0 {
            let cur_len: i64 = conn.llen(EmailJob::queue_key()).await?;
            if cur_len >= max_len {
                return Err(anyhow!(
                    "Email queue backpressure: len={} reached max={}",
                    cur_len,
                    max_len
                ));
            }
        }

        match conn.lpush(EmailJob::queue_key(), payload.clone()).await {
            Ok(len) => return Ok(len),
            Err(e) => {
                if attempt >= retry_count {
                    return Err(anyhow!(e));
                }
                let jitter_ms: u64 = rand::rng().random_range(0..=60);
                let sleep_ms = ((attempt as u64) + 1) * 120 + jitter_ms;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
        }
    }

    Err(anyhow!("email enqueue failed unexpectedly"))
}

/// Fire-and-forget enqueue with explicit success/error logging. Use this from
/// request handlers when you don't want to await the Redis round-trip.
#[allow(dead_code)] // public producer API, called from feature code
pub fn enqueue_email_background(job: EmailJob, source: &str) {
    let source = source.to_string();
    tokio::spawn(async move {
        let to = job.to.join(",");
        match enqueue_email(&job).await {
            Ok(queue_len) => {
                log_output(
                    "EMAIL",
                    "ENQUEUE-OK",
                    source.as_str(),
                    format!("to=[{}] subject='{}' queued (len={})", to, job.subject, queue_len),
                    true,
                );
            }
            Err(e) => {
                log_output(
                    "EMAIL",
                    "ENQUEUE-ERR",
                    source.as_str(),
                    format!("to=[{}] subject='{}' failed: {}", to, job.subject, e),
                    false,
                );
            }
        }
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Consumer side: pop EmailJob from Redis and deliver via SMTP (lettre).
// ─────────────────────────────────────────────────────────────────────────────

type SmtpTransport = AsyncSmtpTransport<Tokio1Executor>;

#[derive(Debug, Serialize)]
struct FailedEmailRecord {
    failed_at: String,
    worker: String,
    error: String,
    subject: String,
    to: Vec<String>,
    job: EmailJob,
}

/// Build the shared async SMTP transport from environment configuration.
///
/// Env: `SMTP_HOST` (required), `SMTP_PORT` (default 587), `SMTP_USER`,
/// `SMTP_PASS`, `SMTP_TLS` = "starttls" (default) | "tls" | "none".
fn build_smtp_transport() -> Result<SmtpTransport> {
    let host = std::env::var("SMTP_HOST")
        .map_err(|_| anyhow!("SMTP_HOST not set; cannot start email consumer"))?;
    let port: u16 = std::env::var("SMTP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(587);
    let tls_mode = std::env::var("SMTP_TLS").unwrap_or_else(|_| "starttls".into());

    let mut builder = match tls_mode.to_lowercase().as_str() {
        // Implicit TLS (usually port 465).
        "tls" | "ssl" | "implicit" => SmtpTransport::relay(&host)
            .map_err(|e| anyhow!("SMTP relay (tls) build failed: {}", e))?,
        // Plaintext / no encryption (dev only).
        "none" | "plain" | "off" => SmtpTransport::builder_dangerous(&host),
        // STARTTLS (usually port 587) — default.
        _ => SmtpTransport::starttls_relay(&host)
            .map_err(|e| anyhow!("SMTP starttls relay build failed: {}", e))?,
    };

    builder = builder.port(port);

    if let (Ok(user), Ok(pass)) = (std::env::var("SMTP_USER"), std::env::var("SMTP_PASS"))
        && !user.is_empty()
    {
        builder = builder.credentials(Credentials::new(user, pass));
    }

    Ok(builder.build())
}

/// Convert an [`EmailJob`] into a fully-formed lettre [`Message`], decoding any
/// base64 attachments back to bytes and assembling a MIME multipart when needed.
fn build_message(job: &EmailJob) -> Result<Message> {
    use base64::Engine;

    let from_raw = std::env::var("EMAIL_FROM")
        .map_err(|_| anyhow!("EMAIL_FROM not set; required as sender address"))?;
    let from: Mailbox = from_raw
        .parse()
        .map_err(|e| anyhow!("Invalid EMAIL_FROM '{}': {}", from_raw, e))?;

    let mut builder = Message::builder().from(from).subject(&job.subject);

    for addr in &job.to {
        let mbox: Mailbox = addr
            .parse()
            .map_err(|e| anyhow!("Invalid 'to' address '{}': {}", addr, e))?;
        builder = builder.to(mbox);
    }
    for addr in &job.cc {
        let mbox: Mailbox = addr
            .parse()
            .map_err(|e| anyhow!("Invalid 'cc' address '{}': {}", addr, e))?;
        builder = builder.cc(mbox);
    }

    // Body part — HTML or plain text.
    let body_part = if job.is_html {
        SinglePart::html(job.content_email.clone())
    } else {
        SinglePart::plain(job.content_email.clone())
    };

    let message = if job.attachments.is_empty() {
        builder
            .singlepart(body_part)
            .map_err(|e| anyhow!("Build email body failed: {}", e))?
    } else {
        let mut multipart = MultiPart::mixed().singlepart(body_part);
        for att in &job.attachments {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(att.content_base64.as_bytes())
                .map_err(|e| anyhow!("Decode attachment '{}' failed: {}", att.filename, e))?;
            let ct_raw = if att.content_type.is_empty() {
                "application/octet-stream"
            } else {
                att.content_type.as_str()
            };
            let content_type = ContentType::parse(ct_raw)
                .map_err(|e| anyhow!("Invalid content-type '{}': {}", ct_raw, e))?;
            multipart = multipart
                .singlepart(Attachment::new(att.filename.clone()).body(bytes, content_type));
        }
        builder
            .multipart(multipart)
            .map_err(|e| anyhow!("Build multipart email failed: {}", e))?
    };

    Ok(message)
}

/// Send one job, retrying transient SMTP failures with backoff.
async fn send_with_retry(transport: &SmtpTransport, job: &EmailJob, retry_max: usize) -> Result<()> {
    let message = build_message(job)?;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=retry_max {
        match transport.send(message.clone()).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = Some(anyhow!("SMTP send failed: {}", e));
                if attempt < retry_max {
                    let jitter_ms: u64 = rand::rng().random_range(0..=80);
                    let sleep_ms = ((attempt as u64) + 1) * 300 + jitter_ms;
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("email send failed")))
}

/// Push a permanently-failed email job to the dead-letter queue.
async fn push_email_dlq(job: EmailJob, worker: &str, error: &str) -> Result<i64> {
    let record = FailedEmailRecord {
        failed_at: Utc::now().to_rfc3339(),
        worker: worker.to_string(),
        error: error.to_string(),
        subject: job.subject.clone(),
        to: job.to.clone(),
        job,
    };
    let payload = serde_json::to_string(&record)?;
    let client = crate::database::redis::get_manager().await?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let len: i64 = conn.lpush(EmailJob::dlq_key(), payload).await?;
    Ok(len)
}

/// BRPOP one email job off the queue. Returns None on timeout.
async fn dequeue_email(conn: &mut MultiplexedConnection) -> Result<Option<EmailJob>> {
    let res: Option<(String, String)> = redis::cmd("BRPOP")
        .arg(EmailJob::queue_key())
        .arg(10)
        .query_async(conn)
        .await?;
    if let Some((_k, v)) = res {
        let job: EmailJob = serde_json::from_str(&v)?;
        Ok(Some(job))
    } else {
        Ok(None)
    }
}

/// Start N concurrent workers that pull email jobs from Redis and deliver them
/// via SMTP. Mirrors [`crate::nocode::consumer::start_consumer`] — workers are
/// detached and self-healing (reconnect + circuit breaker on repeated errors).
///
/// Concurrency via `EMAIL_CONCURRENCY` (default 2); per-job send retries via
/// `EMAIL_EXEC_RETRY_MAX` (default 2).
pub async fn start_email_consumer() {
    // Validate SMTP config up front so misconfiguration is loud, not silent.
    let transport = match build_smtp_transport() {
        Ok(t) => Arc::new(t),
        Err(e) => {
            log_output("EMAIL", "BOOT-ERR", "email-consumer", format!("{}", e), false);
            return;
        }
    };

    let concurrency: usize = std::env::var("EMAIL_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    log_output("EMAIL", "START", "email-consumer", format!("Workers={}", concurrency), true);

    let mut set = JoinSet::new();
    for idx in 0..concurrency {
        let transport_cl = transport.clone();
        set.spawn(async move {
            let worker_name = format!("email-worker-{}", idx);
            log_output("EMAIL", "WORKER-START", worker_name.as_str(), "ready".to_string(), true);

            let mut consecutive_errors = 0u32;
            let max_consecutive_errors = 10u32;
            let mut backoff_ms = 250u64;

            // Dedicated Redis connection per worker, reconnecting as needed.
            let client = loop {
                match crate::database::redis::get_manager().await {
                    Ok(c) => break c,
                    Err(e) => {
                        log_output("EMAIL", "DEQUEUE-ERR", worker_name.as_str(), format!("{}", e), false);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            };
            let mut conn = loop {
                match client.get_multiplexed_async_connection().await {
                    Ok(c) => break c,
                    Err(e) => {
                        log_output("EMAIL", "DEQUEUE-ERR", worker_name.as_str(), format!("{}", e), false);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            };

            loop {
                match dequeue_email(&mut conn).await {
                    Ok(Some(job)) => {
                        consecutive_errors = 0;
                        backoff_ms = 250;

                        let retry_max: usize = std::env::var("EMAIL_EXEC_RETRY_MAX")
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(2);
                        let to = job.to.join(",");
                        let subject = job.subject.clone();
                        let job_for_dlq = job.clone();
                        match send_with_retry(&transport_cl, &job, retry_max).await {
                            Ok(_) => log_output(
                                "EMAIL",
                                "SEND-OK",
                                worker_name.as_str(),
                                format!("to=[{}] subject='{}'", to, subject),
                                true,
                            ),
                            Err(e) => {
                                log_output(
                                    "EMAIL",
                                    "SEND-ERR",
                                    worker_name.as_str(),
                                    format!("to=[{}] subject='{}': {}", to, subject, e),
                                    false,
                                );
                                match push_email_dlq(job_for_dlq, worker_name.as_str(), &e.to_string()).await {
                                    Ok(dlq_len) => log_output("EMAIL", "DLQ-PUSH", worker_name.as_str(), format!("len={}", dlq_len), false),
                                    Err(dlq_err) => log_output("EMAIL", "DLQ-ERR", worker_name.as_str(), format!("{}", dlq_err), false),
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors == 1 || consecutive_errors.is_multiple_of(10) {
                            log_output(
                                "EMAIL",
                                "DEQUEUE-ERR",
                                worker_name.as_str(),
                                format!("{} (count: {})", e, consecutive_errors),
                                false,
                            );
                        }
                        if let Ok(c) = client.get_multiplexed_async_connection().await {
                            conn = c;
                        }
                        if consecutive_errors >= max_consecutive_errors {
                            log_output(
                                "EMAIL",
                                "CIRCUIT-BREAKER",
                                worker_name.as_str(),
                                format!("Too many errors ({}), sleeping 30s", consecutive_errors),
                                false,
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            consecutive_errors = 0;
                            backoff_ms = 250;
                        } else {
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(5000);
                        }
                    }
                }
            }
        });
    }

    tokio::spawn(async move {
        while let Some(res) = set.join_next().await {
            match res {
                Ok(_) => log_output("EMAIL", "WORKER-EXIT", "email-supervisor", "worker exited unexpectedly".to_string(), false),
                Err(e) => log_output("EMAIL", "WORKER-PANIC", "email-supervisor", format!("worker join error: {}", e), false),
            }
        }
    });
}
