//! Tests for GLYPH codec

use crate::*;
use serde_json::json;

#[test]
fn test_canon_null() {
    let gv = GValue::null();
    assert_eq!(canonicalize_loose(&gv), "_");
}

#[test]
fn test_canon_null_pretty() {
    let gv = GValue::null();
    let opts = LooseCanonOpts::pretty();
    assert_eq!(canonicalize_loose_with_opts(&gv, &opts), "∅");
}

#[test]
fn test_canon_bool() {
    assert_eq!(canonicalize_loose(&GValue::bool(true)), "t");
    assert_eq!(canonicalize_loose(&GValue::bool(false)), "f");
}

#[test]
fn test_canon_int() {
    assert_eq!(canonicalize_loose(&GValue::int(42)), "42");
    assert_eq!(canonicalize_loose(&GValue::int(-123)), "-123");
    assert_eq!(canonicalize_loose(&GValue::int(0)), "0");
}

#[test]
fn test_canon_float() {
    assert_eq!(canonicalize_loose(&GValue::float(3.14)), "3.14");
    assert_eq!(canonicalize_loose(&GValue::float(0.0)), "0");
    assert_eq!(canonicalize_loose(&GValue::float(-0.0)), "0");
}

#[test]
fn test_canon_string_bare() {
    let gv = GValue::str("hello");
    assert_eq!(canonicalize_loose(&gv), "hello");
}

#[test]
fn test_canon_string_quoted() {
    let gv = GValue::str("hello world");
    assert_eq!(canonicalize_loose(&gv), "\"hello world\"");
}

#[test]
fn test_canon_string_escapes() {
    let gv = GValue::str("line1\nline2");
    assert_eq!(canonicalize_loose(&gv), "\"line1\\nline2\"");
}

#[test]
fn test_canon_list() {
    let gv = GValue::list(vec![GValue::int(1), GValue::int(2), GValue::int(3)]);
    assert_eq!(canonicalize_loose(&gv), "[1 2 3]");
}

#[test]
fn test_canon_map_sorted() {
    let gv = GValue::map(vec![
        field("b", GValue::int(2)),
        field("a", GValue::int(1)),
    ]);
    assert_eq!(canonicalize_loose(&gv), "{a=1 b=2}");
}

#[test]
fn test_canon_ref() {
    let gv = GValue::id("user", "123");
    assert_eq!(canonicalize_loose(&gv), "^user:123");

    let gv2 = GValue::simple_id("abc");
    assert_eq!(canonicalize_loose(&gv2), "^abc");
}

#[test]
fn test_json_roundtrip() {
    let data = json!({
        "name": "Alice",
        "age": 30,
        "active": true
    });

    let gv = from_json(&data);
    let restored = to_json(&gv);

    assert_eq!(data, restored);
}

#[test]
fn test_sparse_keys_no_tabular() {
    // Disjoint keys should NOT become tabular
    let data = json!([{"a": 1}, {"b": 2}, {"c": 3}]);
    let gv = from_json(&data);
    let result = canonicalize_loose(&gv);

    assert!(!result.contains("@tab"), "Disjoint keys should not use tabular");
    assert_eq!(result, "[{a=1} {b=2} {c=3}]");
}

#[test]
fn test_homogeneous_array_tabular() {
    // Same keys should become tabular
    let data = json!([
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6}
    ]);
    let gv = from_json(&data);
    let result = canonicalize_loose(&gv);

    assert!(result.contains("@tab"), "Homogeneous array should use tabular");
}

#[test]
fn test_empty_objects_no_tabular() {
    let data = json!([{}, {}, {}]);
    let gv = from_json(&data);
    let result = canonicalize_loose(&gv);

    assert!(!result.contains("@tab"), "Empty objects should not use tabular");
    assert_eq!(result, "[{} {} {}]");
}

#[test]
fn test_equality() {
    let a = from_json(&json!({"x": 1, "y": 2}));
    let b = from_json(&json!({"y": 2, "x": 1})); // Different order

    assert!(equal_loose(&a, &b), "Same data, different order should be equal");
}

#[test]
fn test_fingerprint_deterministic() {
    let data = json!({"a": 1, "b": [2, 3]});
    let gv = from_json(&data);

    let fp1 = fingerprint_loose(&gv);
    let fp2 = fingerprint_loose(&gv);

    assert_eq!(fp1, fp2);
}

#[test]
fn test_hash_loose() {
    let gv = from_json(&json!({"test": "value"}));
    let h = hash_loose(&gv);

    assert_eq!(h.len(), 16); // 8 bytes = 16 hex chars
}

#[test]
fn test_unicode() {
    let gv = GValue::str("你好世界");
    let result = canonicalize_loose(&gv);
    assert_eq!(result, "你好世界");
}

#[test]
fn test_complex_nested() {
    let data = json!({
        "tool_call": {
            "name": "search",
            "args": {
                "query": "weather",
                "limit": 10
            }
        }
    });

    let gv = from_json(&data);
    let result = canonicalize_loose(&gv);

    assert!(result.contains("search"));
    assert!(result.contains("query=weather"));
    assert!(result.contains("limit=10"));
}

#[test]
fn test_tabular_threshold() {
    // 2 items - below threshold
    let data2 = json!([{"a": 1}, {"a": 2}]);
    let result2 = canonicalize_loose(&from_json(&data2));
    assert!(!result2.contains("@tab"));

    // 3 items - at threshold
    let data3 = json!([{"a": 1}, {"a": 2}, {"a": 3}]);
    let result3 = canonicalize_loose(&from_json(&data3));
    assert!(result3.contains("@tab"));
}
