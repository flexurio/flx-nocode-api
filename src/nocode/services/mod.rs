pub mod data_read_service;
pub mod data_update_service;
pub mod data_create_service;
pub mod data_delete_service;
pub mod data_trace_service;
pub mod data_import_service;
pub mod data_patch_service;
pub mod data_export_service;

use crate::model::WebResponse;
use serde_json::Value;

/// Build a failure `WebResponse` payload.
pub fn web_err(msg: impl Into<String>) -> WebResponse {
    WebResponse {
        success: false,
        message: msg.into(),
        total_data: 0,
        data: Value::Null,
    }
}

/// Returns the list of missing required query parameters (those prefixed with `*`)
/// from `params_map`. A parameter is considered missing when absent, JSON `null`,
/// or — for `Value::String` — empty/whitespace-only.
///
/// `parameters` is mutated in-place to strip the leading `*` from required names
/// so callers downstream can treat them as plain names.
pub fn collect_missing_required_params(
    parameters: &mut [String],
    params_map: &serde_json::Map<String, Value>,
) -> Vec<String> {
    let mut missing: Vec<String> = Vec::new();
    for param in parameters.iter_mut() {
        if let Some(name) = param.strip_prefix('*') {
            let present = match params_map.get(name) {
                None | Some(Value::Null) => false,
                Some(Value::String(s)) => !s.trim().is_empty(),
                Some(_) => true,
            };
            if !present {
                missing.push(name.to_string());
            } else {
                *param = name.to_string();
            }
        }
    }
    missing
}
