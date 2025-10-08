// Compatibility layer untuk abstraksi pembuatan Value dengan sonic_rs
// Menyediakan wrappers dan helpers yang dipakai di beberapa modul

pub use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait};

// Wrapper functions untuk Value creation (missing From impls)
#[inline]
// Retained minimal helpers actually referenced in codebase
pub fn value_from_string(s: String) -> Value { Value::from(s.as_str()) }

pub fn value_from_f64(f: f64) -> Value { Value::new_f64(f).unwrap_or_else(|| Value::from(0)) }

// Provide from_string_ref for ergonomic conversions
#[allow(dead_code)]
pub fn value_from_string_ref(s: &str) -> Value { Value::from(s) }

// --- Sonic direct Actix responder wrappers ---
use actix_web::{body::BoxBody, http::{header, StatusCode}, HttpResponse, Responder};

// Simple wrapper so we can return sonic_rs::Value without serde
pub struct SonicValue(pub Value);

impl Responder for SonicValue {
	type Body = BoxBody;
	fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
		match sonic_rs::to_vec(&self.0) {
			Ok(buf) => HttpResponse::Ok()
				.insert_header((header::CONTENT_TYPE, "application/json"))
				.body(buf),
			Err(_) => HttpResponse::InternalServerError()
				.insert_header((header::CONTENT_TYPE, "application/json"))
				.body(b"{\"success\":false,\"message\":\"encode error\"}".as_ref()),
		}
	}
}

// Helper to return with custom status
pub fn sonic_status(val: Value, status: StatusCode) -> HttpResponse {
	match sonic_rs::to_vec(&val) {
		Ok(buf) => HttpResponse::build(status)
			.insert_header((header::CONTENT_TYPE, "application/json"))
			.body(buf),
		Err(_) => HttpResponse::InternalServerError()
			.insert_header((header::CONTENT_TYPE, "application/json"))
			.body(b"{\"success\":false,\"message\":\"encode error\"}".as_ref()),
	}
}


