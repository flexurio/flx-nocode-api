// Request path: a panic here would abort an in-flight request. Flagged as
// warnings now; promoted to `deny` once the remaining sites are cleaned up.
#![warn(clippy::unwrap_used, clippy::expect_used)]
pub mod get_handler;
pub mod put_handler;
pub mod post_handler;
pub mod delete_handler;
pub mod trace_handler;
pub mod import_handler;
pub mod patch_handler;
pub mod export_handler;
pub mod email_handler;
