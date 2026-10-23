use thiserror::Error;
use actix_web::{HttpResponse, ResponseError};

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Internal server error: {0}")]
    Internal(String),
    #[error("Not Found: {0}")]
    NotFound(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),

    // Add support for anyhow to wrap miscellaneous errors
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::Validation(msg) => HttpResponse::BadRequest().json(serde_json::json!({"error": msg, "success": false})),
            AppError::BadRequest(msg) => HttpResponse::BadRequest().json(serde_json::json!({"error": msg, "success": false})),
            AppError::Auth(msg) => HttpResponse::Unauthorized().json(serde_json::json!({"error": msg, "success": false})),
            AppError::NotFound(msg) => HttpResponse::NotFound().json(serde_json::json!({"error": msg, "success": false})),
            AppError::Config(msg) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Configuration Error: {}", msg), "success": false})),
            // Don't leak internal DB/IO errors to client, log them if needed in the caller or here via tracing
            AppError::Database(e) => {
                eprintln!("Database Error: {}", e);
                HttpResponse::InternalServerError().json(serde_json::json!({"error": "Internal Database Error", "success": false}))
            }
            AppError::Io(e) => {
                eprintln!("IO Error: {}", e);
                HttpResponse::InternalServerError().json(serde_json::json!({"error": "Internal IO Error", "success": false}))
            }
            AppError::Internal(msg) => {
                eprintln!("Internal Error: {}", msg);
                HttpResponse::InternalServerError().json(serde_json::json!({"error": "Internal Server Error", "success": false}))
            },
            AppError::Anyhow(e) => {
                eprintln!("Unexpected Error: {}", e);
                HttpResponse::InternalServerError().json(serde_json::json!({"error": "Internal Server Error", "success": false}))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_display() {
        let e = AppError::Validation("field required".to_string());
        assert!(e.to_string().contains("Validation error"));
        assert!(e.to_string().contains("field required"));
    }

    #[test]
    fn test_auth_error_display() {
        let e = AppError::Auth("invalid token".to_string());
        assert!(e.to_string().contains("Authentication error"));
        assert!(e.to_string().contains("invalid token"));
    }

    #[test]
    fn test_config_error_display() {
        let e = AppError::Config("missing key".to_string());
        assert!(e.to_string().contains("Configuration error"));
        assert!(e.to_string().contains("missing key"));
    }

    #[test]
    fn test_not_found_error_display() {
        let e = AppError::NotFound("resource missing".to_string());
        assert!(e.to_string().contains("Not Found"));
        assert!(e.to_string().contains("resource missing"));
    }

    #[test]
    fn test_bad_request_error_display() {
        let e = AppError::BadRequest("malformed input".to_string());
        assert!(e.to_string().contains("Bad Request"));
        assert!(e.to_string().contains("malformed input"));
    }

    #[test]
    fn test_internal_error_display() {
        let e = AppError::Internal("unexpected crash".to_string());
        assert!(e.to_string().contains("Internal server error"));
        assert!(e.to_string().contains("unexpected crash"));
    }

    #[test]
    fn test_io_error_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: AppError = io_err.into();
        assert!(e.to_string().contains("IO error"));
    }

    #[test]
    fn test_from_anyhow_error() {
        let anyhow_err = anyhow::anyhow!("something failed");
        let e: AppError = anyhow_err.into();
        assert!(matches!(e, AppError::Anyhow(_)));
        assert!(e.to_string().contains("something failed"));
    }

    #[test]
    fn test_error_debug_format() {
        let e = AppError::Validation("test".to_string());
        let debug_str = format!("{:?}", e);
        assert!(debug_str.contains("Validation"));
    }
}

// ── Typed API error mapping ───────────────────────────────────────────────────
//
// Services still report failures as messages (a full migration to typed errors
// is a larger refactor); this is the single place that turns such a message
// into an HTTP status and a client-safe text. Database driver details (SQL
// text, constraint names) go to the server log with the request id, never to
// the client.

use actix_web::http::StatusCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbErrorKind {
    UniqueViolation,
    ForeignKeyViolation,
    /// Statement/lock timeout, deadlock, "database is locked", cancelled statement.
    Contention,
    Other,
    NotDatabase,
}

/// Recognise constraint and contention errors across sqlx (PostgreSQL, MySQL,
/// SQLite) and tiberius (MSSQL) message formats.
pub fn classify_db_error(msg: &str) -> DbErrorKind {
    let m = msg.to_ascii_lowercase();
    let is_db = m.contains("error executing")
        || m.contains("error returned from database")
        || m.contains("database")
        || m.contains("constraint")
        || m.contains("sqlstate")
        || m.contains("msg ")
        || m.contains("timed out");
    if !is_db {
        return DbErrorKind::NotDatabase;
    }
    if m.contains("unique constraint")        // PostgreSQL / SQLite
        || m.contains("duplicate key")        // PostgreSQL 23505 / MSSQL 2627
        || m.contains("duplicate entry")      // MySQL 1062
        || m.contains("23505")
        || m.contains("cannot insert duplicate key") // MSSQL 2601/2627
        || m.contains("violation of unique key")
        || m.contains("violation of primary key")
    {
        return DbErrorKind::UniqueViolation;
    }
    if m.contains("foreign key constraint")   // PostgreSQL / MySQL 1452 / SQLite
        || m.contains("23503")
        || m.contains("conflicted with the foreign key") // MSSQL 547
        || m.contains("reference constraint")
    {
        return DbErrorKind::ForeignKeyViolation;
    }
    if m.contains("timed out")
        || m.contains("statement timeout")
        || m.contains("canceling statement")
        || m.contains("lock wait timeout")
        || m.contains("lock request time out")
        || m.contains("deadlock")
        || m.contains("database is locked")
        || m.contains("max_statement_time")
        || m.contains("max_execution_time")
    {
        return DbErrorKind::Contention;
    }
    DbErrorKind::Other
}

/// HTTP status + client-safe message derived from a service error message.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// Server-side detail (logged, never sent).
    pub detail: Option<String>,
}

impl ApiError {
    pub fn from_message(msg: &str) -> ApiError {
        let lower = msg.to_ascii_lowercase();
        let plain = |status: StatusCode| ApiError { status, message: msg.to_string(), detail: None };

        if msg == "Invalid token" || lower.starts_with("unauthorized") || lower.contains("invalid or expired token") {
            return plain(StatusCode::UNAUTHORIZED);
        }
        if lower.starts_with("forbidden") {
            return plain(StatusCode::FORBIDDEN);
        }
        if lower.contains("too many") {
            return plain(StatusCode::TOO_MANY_REQUESTS);
        }
        match classify_db_error(msg) {
            DbErrorKind::UniqueViolation => {
                return ApiError {
                    status: StatusCode::CONFLICT,
                    message: "Duplicate value: a record with the same unique key already exists".into(),
                    detail: Some(msg.to_string()),
                };
            }
            DbErrorKind::ForeignKeyViolation => {
                return ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    message: "Referenced record does not exist or is still referenced by other records".into(),
                    detail: Some(msg.to_string()),
                };
            }
            DbErrorKind::Contention => {
                return ApiError {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "The database is busy; please retry".into(),
                    detail: Some(msg.to_string()),
                };
            }
            DbErrorKind::Other => {
                return ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "Database error; see server log (x-request-id)".into(),
                    detail: Some(msg.to_string()),
                };
            }
            DbErrorKind::NotDatabase => {}
        }
        if lower.contains("unique constraint violation") || lower.contains("version mismatch") || lower.contains("conflict") {
            return plain(StatusCode::CONFLICT);
        }
        if lower.contains("invalid foreign key value") {
            return plain(StatusCode::UNPROCESSABLE_ENTITY);
        }
        if lower.contains("not found") {
            return plain(StatusCode::NOT_FOUND);
        }
        if lower.contains("missing required")
            || lower.contains("required parameters")
            || lower.contains("invalid")
            || lower.contains("must be")
            || lower.contains("mismatch")
            || lower.contains("not allowed")
            || lower.contains("locked")
            || lower.contains("transition")
        {
            return plain(StatusCode::BAD_REQUEST);
        }
        if lower.contains("queue error") || lower.contains("temporarily unavailable") || lower.contains("backpressure") {
            return plain(StatusCode::SERVICE_UNAVAILABLE);
        }
        if lower.contains("query :") || lower.contains("select ") || lower.contains("insert into") || lower.contains("update ") {
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "Internal error; see server log (x-request-id)".into(),
                detail: Some(msg.to_string()),
            };
        }
        plain(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Build the JSON response; logs the server-side detail when there is one.
    pub fn respond(self) -> HttpResponse {
        if let Some(detail) = &self.detail {
            tracing::warn!(status = self.status.as_u16(), detail = %detail, "request failed");
        }
        let mut b = HttpResponse::build(self.status);
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            b.insert_header(("Retry-After", "1"));
        }
        b.json(crate::model::WebResponse { success: false, message: self.message, total_data: 0, data: serde_json::Value::Null })
    }

    /// Convenience: message → response.
    pub fn respond_message(msg: &str) -> HttpResponse {
        ApiError::from_message(msg).respond()
    }
}

#[cfg(test)]
mod mapping_tests {
    use super::*;

    #[test]
    fn classifies_driver_messages() {
        assert_eq!(classify_db_error("Error executing query: error returned from database: duplicate key value violates unique constraint \"idx_code\""), DbErrorKind::UniqueViolation);
        assert_eq!(classify_db_error("Error executing query: error returned from database: Duplicate entry 'A' for key 't.idx'"), DbErrorKind::UniqueViolation);
        assert_eq!(classify_db_error("Error executing query: error returned from database: (code: 2067) UNIQUE constraint failed: t.code"), DbErrorKind::UniqueViolation);
        assert_eq!(classify_db_error("Error executing statement: Msg 2627: Violation of UNIQUE KEY constraint. Cannot insert duplicate key"), DbErrorKind::UniqueViolation);
        assert_eq!(classify_db_error("error returned from database: insert or update on table violates foreign key constraint"), DbErrorKind::ForeignKeyViolation);
        assert_eq!(classify_db_error("database query timed out after 35s"), DbErrorKind::Contention);
        assert_eq!(classify_db_error("error returned from database: database is locked"), DbErrorKind::Contention);
        assert_eq!(classify_db_error("Missing required field: name"), DbErrorKind::NotDatabase);
    }

    #[test]
    fn maps_messages_to_status_and_hides_sql() {
        let e = ApiError::from_message("Error executing query: error returned from database: duplicate key value violates unique constraint");
        assert_eq!(e.status, StatusCode::CONFLICT);
        assert!(!e.message.contains("database"));
        let e = ApiError::from_message("Error NCO-GET(AST) route x: boom. Query : SELECT * FROM t");
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!e.message.contains("SELECT"));
        assert_eq!(ApiError::from_message("Missing required field: code").status, StatusCode::BAD_REQUEST);
        assert_eq!(ApiError::from_message("Invalid token").status, StatusCode::UNAUTHORIZED);
        assert_eq!(ApiError::from_message("Route 'x' not found").status, StatusCode::NOT_FOUND);
        assert_eq!(ApiError::from_message("Unique constraint violation on columns: code").status, StatusCode::CONFLICT);
        assert_eq!(ApiError::from_message("Queue error: backpressure").status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
