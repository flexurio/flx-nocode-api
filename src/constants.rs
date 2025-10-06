// src/constants.rs
// Centralized constants to avoid repeated string allocations

/// Error messages
pub const ERR_INVALID_TOKEN: &str = "Invalid token";
pub const ERR_UNAUTHORIZED: &str = "Unauthorized";
// pub const ERR_RATE_LIMITED: &str = "Rate limit exceeded";
// pub const ERR_MISSING_PARAM: &str = "Missing required parameter";
// pub const ERR_INVALID_MULTIPART: &str = "Invalid multipart";
// pub const ERR_INVALID_JSON: &str = "Invalid JSON";
// pub const ERR_INVALID_FK: &str = "Invalid foreign key value";
// pub const ERR_DB_ERROR: &str = "Database error";

/// Success messages
pub const MSG_LOGIN_SUCCESS: &str = "Login Success";
// pub const MSG_REGISTER_SUCCESS: &str = "Register Success";
// pub const MSG_CREATED: &str = "Created";
// pub const MSG_UPDATED: &str = "Updated";
// pub const MSG_DELETED: &str = "Deleted";
// pub const MSG_SUCCESS: &str = "Success";

/// Database type constants
pub const DB_MYSQL: &str = "mysql";
pub const DB_POSTGRES: &str = "postgres";
pub const DB_SQLITE: &str = "sqlite";
pub const DB_MSSQL: &str = "mssql";
// pub const DB_MONGODB: &str = "mongodb";

/// SQL datetime functions per database
pub const DATETIME_MYSQL: &str = "NOW()";
pub const DATETIME_POSTGRES: &str = "NOW()";
pub const DATETIME_SQLITE: &str = "CURRENT_TIMESTAMP";
pub const DATETIME_MSSQL: &str = "GETDATE()";
pub const DATETIME_DEFAULT: &str = "CURRENT_TIMESTAMP";

// Default values
// pub const DEFAULT_CONFIG_LOCATION: &str = "config";
// pub const DEFAULT_STATIC_LOCATION: &str = "static";
// pub const DEFAULT_IMAGE_LOCATION: &str = "DB";
// pub const DEFAULT_LOG_LOCATION: &str = "logs";


// HTTP status codes (can use actix_web::http::StatusCode, but these are for messages)
// pub const STATUS_SUCCESS: bool = true;
// pub const STATUS_FAILURE: bool = false;
