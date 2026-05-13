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
