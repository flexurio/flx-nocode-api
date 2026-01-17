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
