//! JSON <-> GValue conversion

use crate::types::*;
use crate::error::*;
use serde_json::{Value as JsonValue, Number, Map};

/// Convert JSON value to GValue
pub fn from_json(json: &JsonValue) -> GValue {
    match json {
        JsonValue::Null => GValue::Null,
        JsonValue::Bool(b) => GValue::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                GValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                GValue::Float(f)
            } else {
                GValue::Float(0.0)
            }
        }
        JsonValue::String(s) => GValue::Str(s.clone()),
        JsonValue::Array(arr) => {
            GValue::List(arr.iter().map(from_json).collect())
        }
        JsonValue::Object(obj) => {
            let entries: Vec<MapEntry> = obj
                .iter()
                .map(|(k, v)| MapEntry::new(k.clone(), from_json(v)))
                .collect();
            GValue::Map(entries)
        }
    }
}

/// Convert GValue to JSON value
pub fn to_json(gv: &GValue) -> JsonValue {
    match gv {
        GValue::Null => JsonValue::Null,
        GValue::Bool(b) => JsonValue::Bool(*b),
        GValue::Int(n) => JsonValue::Number(Number::from(*n)),
        GValue::Float(f) => {
            Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        GValue::Str(s) => JsonValue::String(s.clone()),
        GValue::Bytes(data) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
            JsonValue::String(BASE64.encode(data))
        }
        GValue::Time(t) => JsonValue::String(t.to_rfc3339()),
        GValue::Id(ref_id) => {
            if ref_id.prefix.is_empty() {
                JsonValue::String(format!("^{}", ref_id.value))
            } else {
                JsonValue::String(format!("^{}:{}", ref_id.prefix, ref_id.value))
            }
        }
        GValue::List(items) => {
            JsonValue::Array(items.iter().map(to_json).collect())
        }
        GValue::Map(entries) => {
            let mut map = Map::new();
            for entry in entries {
                map.insert(entry.key.clone(), to_json(&entry.value));
            }
            JsonValue::Object(map)
        }
        GValue::Struct(s) => {
            let mut map = Map::new();
            for field in &s.fields {
                map.insert(field.key.clone(), to_json(&field.value));
            }
            // Include type name as special field
            map.insert("_type".to_string(), JsonValue::String(s.type_name.clone()));
            JsonValue::Object(map)
        }
        GValue::Sum(s) => {
            let mut map = Map::new();
            map.insert("_tag".to_string(), JsonValue::String(s.tag.clone()));
            if let Some(ref value) = s.value {
                map.insert("_value".to_string(), to_json(value));
            }
            JsonValue::Object(map)
        }
    }
}

/// Parse JSON string to GValue
pub fn parse_json(json_str: &str) -> Result<GValue> {
    let json: JsonValue = serde_json::from_str(json_str)?;
    Ok(from_json(&json))
}

/// Stringify GValue to JSON string
pub fn stringify_json(gv: &GValue) -> String {
    serde_json::to_string(&to_json(gv)).unwrap_or_default()
}

/// Stringify GValue to pretty JSON string
pub fn stringify_json_pretty(gv: &GValue) -> String {
    serde_json::to_string_pretty(&to_json(gv)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_json_null() {
        let gv = from_json(&json!(null));
        assert!(gv.is_null());
    }

    #[test]
    fn test_from_json_bool() {
        let gv = from_json(&json!(true));
        assert_eq!(gv.as_bool(), Some(true));
    }

    #[test]
    fn test_from_json_int() {
        let gv = from_json(&json!(42));
        assert_eq!(gv.as_int(), Some(42));
    }

    #[test]
    fn test_from_json_float() {
        let gv = from_json(&json!(3.14));
        assert_eq!(gv.as_float(), Some(3.14));
    }

    #[test]
    fn test_from_json_string() {
        let gv = from_json(&json!("hello"));
        assert_eq!(gv.as_str(), Some("hello"));
    }

    #[test]
    fn test_from_json_array() {
        let gv = from_json(&json!([1, 2, 3]));
        assert!(gv.is_list());
        let items = gv.as_list().unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_from_json_object() {
        let gv = from_json(&json!({"a": 1, "b": 2}));
        assert!(gv.is_map());
        assert_eq!(gv.get("a").and_then(|v| v.as_int()), Some(1));
    }

    #[test]
    fn test_roundtrip() {
        let original = json!({
            "name": "Alice",
            "age": 30,
            "active": true,
            "scores": [95, 87, 92]
        });

        let gv = from_json(&original);
        let restored = to_json(&gv);

        assert_eq!(original, restored);
    }
}
