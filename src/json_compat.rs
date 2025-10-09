// Compatibility layer untuk migrasi dari serde_json ke sonic_rs
// Menyediakan wrappers dan helpers untuk ease migration

pub use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait};

// Wrapper functions untuk Value creation (missing From impls)
#[inline]
// Retained minimal helpers actually referenced in codebase
pub fn value_from_string(s: String) -> Value { Value::from(s.as_str()) }

#[inline]
pub fn value_from_f64(f: f64) -> Value { 
    if f.is_finite() {
        Value::new_f64(f).unwrap_or_else(|| Value::from(0))
    } else {
        Value::from(0)
    }
}

// Provide from_string_ref for ergonomic conversions
#[allow(dead_code)]
pub fn value_from_string_ref(s: &str) -> Value { Value::from(s) }


