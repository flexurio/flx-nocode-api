// Compatibility layer untuk migrasi dari serde_json ke sonic_rs
// Menyediakan wrappers dan helpers untuk ease migration

pub use sonic_rs::serde::JsonNumberTrait; // for as_i64(), as_f64(), etc.
pub use sonic_rs::{from_slice, from_str, json, to_string, to_value, Array, Number, Object, Value, JsonValueTrait, JsonContainerTrait};

// Wrapper functions untuk Value creation (missing From impls)
#[inline]
pub fn value_from_str(s: impl AsRef<str>) -> Value {
    Value::from(s.as_ref())
}

#[inline]
pub fn value_from_string(s: String) -> Value {
    Value::from(s.as_str())
}

// sonic_rs tidak implement From<f64> langsung; gunakan new_f64
#[inline]
pub fn value_from_f64(f: f64) -> Value { Value::new_f64(f).unwrap_or_else(|| Value::from(0)) }

#[inline]
pub fn value_from_i64(i: i64) -> Value {
    Value::from(i)
}

#[inline]
pub fn value_from_bool(b: bool) -> Value { Value::from(b) }

#[inline]
pub fn value_null() -> Value {
    Value::default()
}

// Helper untuk convert HashMap ke Object
pub fn hashmap_to_object(map: std::collections::HashMap<String, Value>) -> Object {
    let mut obj = Object::with_capacity(map.len());
    for (k, v) in map {
        obj.insert(&k, v);
    }
    obj
}

// Helper untuk convert HashMap ke Value
pub fn hashmap_to_value(map: std::collections::HashMap<String, Value>) -> Value { Value::from(hashmap_to_object(map)) }

// Helper untuk convert Object ke HashMap  
pub fn object_to_hashmap(obj: Object) -> std::collections::HashMap<String, Value> {
    let mut map = std::collections::HashMap::new();
    for (k, v) in &obj { // iterate by reference; Object is not IntoIterator by value
        map.insert(k.to_string(), v.clone());
    }
    map
}

// Helper untuk match Value type safely
pub fn value_to_qv(v: &Value) -> crate::storage::ast::Val {
    use crate::storage::ast::Val as QV;
    
    if v.is_null() {
        QV::Null
    } else if let Some(b) = v.as_bool() {
        QV::Bool(b)
    } else if let Some(n) = v.as_number() {
        if n.is_i64() { QV::I64(n.as_i64().unwrap()) }
        else if n.is_f64() { QV::F64(n.as_f64().unwrap()) }
        else { QV::Str(v.to_string()) }
    } else if let Some(s) = v.as_str() {
        QV::Str(s.to_string())
    } else {
        QV::Str(v.to_string())
    }
}

// Helper umum untuk membuat Value dari &String atau String tanpa copy berlebihan
#[inline]
pub fn value_from_string_ref(s: &String) -> Value { Value::from(s.as_str()) }


