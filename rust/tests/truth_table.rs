//! Truth table tests driven by testdata/robustness/truth_cases.json.
//!
//! Each case in the "cowrie" section maps to a specific test action:
//! roundtrip, decode_raw, trailing_garbage, truncated, roundtrip_depth.

use std::collections::BTreeMap;
use cowrie_rs::gen2::{decode, encode, Value};

fn truth_manifest() -> serde_json::Value {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/robustness/truth_cases.json");
    let data = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
    serde_json::from_str(&data).expect("parse truth_cases.json")
}

fn cowrie_cases() -> Vec<serde_json::Value> {
    let manifest = truth_manifest();
    manifest["cowrie"]["cases"]
        .as_array()
        .expect("cowrie.cases array")
        .clone()
}

/// Build a cowrie Value from a truth-case input descriptor.
fn build_value(input: &serde_json::Value) -> Value {
    let typ = input["type"].as_str().unwrap_or("");
    match typ {
        "null" => Value::Null,
        "bool" => Value::Bool(input["value"].as_bool().unwrap()),
        "int64" => {
            // serde_json may parse large i64 as i64 or f64
            if let Some(i) = input["value"].as_i64() {
                Value::Int(i)
            } else {
                let f = input["value"].as_f64().unwrap();
                Value::Int(f as i64)
            }
        }
        "float64" => {
            let raw = input["value"].as_str().unwrap_or("");
            match raw {
                "NaN" => Value::Float(f64::NAN),
                "+Inf" => Value::Float(f64::INFINITY),
                "-Inf" => Value::Float(f64::NEG_INFINITY),
                "-0.0" => Value::Float(-0.0_f64),
                _ => {
                    let f = raw.parse::<f64>().unwrap_or_else(|_| {
                        input["value"].as_f64().unwrap()
                    });
                    Value::Float(f)
                }
            }
        }
        "string" => Value::String(input["value"].as_str().unwrap().to_string()),
        "bytes" => {
            let b64 = input["value_base64"].as_str().unwrap_or("");
            if b64.is_empty() {
                Value::Bytes(vec![])
            } else {
                Value::Bytes(base64_decode(b64))
            }
        }
        "array" => {
            let arr = input["value"].as_array().unwrap();
            Value::Array(arr.iter().map(json_to_value).collect())
        }
        "object" => {
            let entries = input["entries"].as_array().unwrap();
            let mut map = BTreeMap::new();
            for e in entries {
                let arr = e.as_array().unwrap();
                let key = arr[0].as_str().unwrap().to_string();
                let val = json_to_value(&arr[1]);
                map.insert(key, val);
            }
            Value::Object(map)
        }
        "nested_arrays" => {
            let depth = input["depth"].as_u64().unwrap() as usize;
            build_nested_arrays(depth)
        }
        _ => panic!("unknown input type: {}", typ),
    }
}

fn build_nested_arrays(depth: usize) -> Value {
    let mut v = Value::Null;
    for _ in 0..depth {
        v = Value::Array(vec![v]);
    }
    v
}

fn json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(map) => {
            let btree: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect();
            Value::Object(btree)
        }
    }
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .expect("base64 decode")
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => {
            if x.is_nan() && y.is_nan() {
                true
            } else {
                x.to_bits() == y.to_bits()
            }
        }
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| values_equal(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((ka, va), (kb, vb))| ka == kb && values_equal(va, vb))
        }
        _ => false,
    }
}

// ============================================================
// Individual test cases driven by the manifest
// ============================================================

#[test]
fn truth_null_roundtrip() {
    run_case_by_id("null_roundtrip");
}

#[test]
fn truth_bool_true_roundtrip() {
    run_case_by_id("bool_true_roundtrip");
}

#[test]
fn truth_bool_false_roundtrip() {
    run_case_by_id("bool_false_roundtrip");
}

#[test]
fn truth_int64_max() {
    run_case_by_id("int64_max");
}

#[test]
fn truth_int64_min() {
    run_case_by_id("int64_min");
}

#[test]
fn truth_empty_string_key() {
    run_case_by_id("empty_string_key");
}

#[test]
fn truth_negative_zero_float() {
    run_case_by_id("negative_zero_float");
}

#[test]
fn truth_nan_binary_roundtrip() {
    run_case_by_id("nan_binary_roundtrip");
}

#[test]
fn truth_positive_inf_binary_roundtrip() {
    run_case_by_id("positive_inf_binary_roundtrip");
}

#[test]
fn truth_negative_inf_binary_roundtrip() {
    run_case_by_id("negative_inf_binary_roundtrip");
}

#[test]
fn truth_unknown_tag_rejected() {
    run_case_by_id("unknown_tag_rejected");
}

#[test]
fn truth_empty_array_roundtrip() {
    run_case_by_id("empty_array_roundtrip");
}

#[test]
fn truth_empty_object_roundtrip() {
    run_case_by_id("empty_object_roundtrip");
}

#[test]
fn truth_nested_depth_100() {
    run_case_by_id("nested_depth_100");
}

#[test]
fn truth_duplicate_map_keys() {
    run_case_by_id("duplicate_map_keys");
}

#[test]
fn truth_empty_bytes_roundtrip() {
    run_case_by_id("empty_bytes_roundtrip");
}

#[test]
fn truth_unicode_string_roundtrip() {
    run_case_by_id("unicode_string_roundtrip");
}

#[test]
fn truth_trailing_garbage_rejected() {
    run_case_by_id("trailing_garbage_rejected");
}

#[test]
fn truth_truncated_input_rejected() {
    run_case_by_id("truncated_input_rejected");
}

#[test]
fn truth_empty_input_rejected() {
    run_case_by_id("empty_input_rejected");
}

// ============================================================
// Case runner
// ============================================================

fn run_case_by_id(id: &str) {
    let cases = cowrie_cases();
    let case = cases
        .iter()
        .find(|c| c["id"].as_str() == Some(id))
        .unwrap_or_else(|| panic!("case '{}' not found in truth_cases.json", id));

    let action = case["action"].as_str().unwrap_or("");
    let expect = &case["expect"];

    match action {
        "roundtrip" => run_roundtrip(id, &case["input"], expect),
        "encode_decode" => run_encode_decode(id, &case["input"], expect),
        "decode_raw" => run_decode_raw(id, &case["input"], expect),
        "trailing_garbage" => run_trailing_garbage(id, expect),
        "truncated" => run_truncated(id, expect),
        "roundtrip_depth" => run_roundtrip_depth(id, &case["input"], expect),
        _ => panic!("unknown action '{}' for case '{}'", action, id),
    }
}

fn run_roundtrip(id: &str, input: &serde_json::Value, expect: &serde_json::Value) {
    let val = build_value(input);
    let encoded = encode(&val).unwrap_or_else(|e| panic!("{}: encode failed: {}", id, e));
    let decoded = decode(&encoded).unwrap_or_else(|e| panic!("{}: decode failed: {}", id, e));

    if expect.get("is_nan") == Some(&serde_json::Value::Bool(true)) {
        match &decoded {
            Value::Float(f) => assert!(f.is_nan(), "{}: expected NaN, got {}", id, f),
            other => panic!("{}: expected Float(NaN), got {:?}", id, other),
        }
    } else if expect.get("is_positive_inf") == Some(&serde_json::Value::Bool(true)) {
        match &decoded {
            Value::Float(f) => {
                assert!(f.is_infinite() && f.is_sign_positive(), "{}: expected +Inf, got {}", id, f);
            }
            other => panic!("{}: expected Float(+Inf), got {:?}", id, other),
        }
    } else if expect.get("is_negative_inf") == Some(&serde_json::Value::Bool(true)) {
        match &decoded {
            Value::Float(f) => {
                assert!(f.is_infinite() && f.is_sign_negative(), "{}: expected -Inf, got {}", id, f);
            }
            other => panic!("{}: expected Float(-Inf), got {:?}", id, other),
        }
    } else if expect.get("negative_zero") == Some(&serde_json::Value::Bool(true)) {
        match &decoded {
            Value::Float(f) => {
                assert!(*f == 0.0 && f.is_sign_negative(), "{}: expected -0.0, got {}", id, f);
            }
            other => panic!("{}: expected Float(-0.0), got {:?}", id, other),
        }
    } else {
        assert!(
            values_equal(&val, &decoded),
            "{}: roundtrip mismatch.\n  input:   {:?}\n  decoded: {:?}",
            id, val, decoded
        );
    }
}

fn run_encode_decode(id: &str, input: &serde_json::Value, expect: &serde_json::Value) {
    let val = build_value(input);
    let encoded = encode(&val).unwrap_or_else(|e| panic!("{}: encode failed: {}", id, e));
    let decoded = decode(&encoded).unwrap_or_else(|e| panic!("{}: decode failed: {}", id, e));

    let ok = expect["ok"].as_bool().unwrap_or(false);
    assert!(ok, "{}: expected ok=true", id);

    // For duplicate_map_keys: verify last-writer-wins by checking against expected value
    if let Some(expected_obj) = expect["value"].as_object() {
        match &decoded {
            Value::Object(map) => {
                for (ek, ev) in expected_obj {
                    let actual_val = map.get(ek);
                    assert!(actual_val.is_some(), "{}: key '{}' not found", id, ek);
                    let expected_val = json_to_value(ev);
                    assert!(
                        values_equal(actual_val.unwrap(), &expected_val),
                        "{}: key '{}' mismatch: {:?} vs {:?}",
                        id, ek, actual_val, expected_val
                    );
                }
            }
            other => panic!("{}: expected Object, got {:?}", id, other),
        }
    }
}

fn run_decode_raw(id: &str, input: &serde_json::Value, expect: &serde_json::Value) {
    let ok = expect["ok"].as_bool().unwrap_or(true);
    let hex_str = input["value"].as_str().unwrap_or("");

    if hex_str.is_empty() {
        // Empty input case
        let result = decode(&[]);
        if ok {
            result.unwrap_or_else(|e| panic!("{}: expected ok but got: {}", id, e));
        } else {
            assert!(result.is_err(), "{}: expected error on empty input", id);
        }
        return;
    }

    let raw = hex_decode(hex_str);
    let result = decode(&raw);
    if ok {
        result.unwrap_or_else(|e| panic!("{}: expected ok but got: {}", id, e));
    } else {
        assert!(result.is_err(), "{}: expected error on raw bytes {:?}", id, hex_str);
    }
}

fn run_trailing_garbage(id: &str, expect: &serde_json::Value) {
    let ok = expect["ok"].as_bool().unwrap_or(true);
    assert!(!ok, "{}: trailing_garbage must expect error", id);

    let encoded = encode(&Value::Null).expect("encode null");
    let mut with_garbage = encoded.clone();
    with_garbage.push(0xFF);

    let result = decode(&with_garbage);
    assert!(
        result.is_err(),
        "{}: expected error for trailing garbage, got {:?}",
        id,
        result.ok()
    );
}

fn run_truncated(id: &str, expect: &serde_json::Value) {
    let ok = expect["ok"].as_bool().unwrap_or(true);
    assert!(!ok, "{}: truncated must expect error", id);

    // Encode a non-trivial value so there are bytes to truncate
    let val = Value::Array(vec![
        Value::Int(42),
        Value::String("hello".to_string()),
    ]);
    let encoded = encode(&val).expect("encode array");
    assert!(encoded.len() > 1, "encoded must have >1 byte to truncate");

    let half = encoded.len() / 2;
    let truncated = &encoded[..half];
    let result = decode(truncated);
    assert!(
        result.is_err(),
        "{}: expected error for truncated input, got {:?}",
        id,
        result.ok()
    );
}

fn run_roundtrip_depth(id: &str, input: &serde_json::Value, expect: &serde_json::Value) {
    let ok = expect["ok"].as_bool().unwrap_or(false);
    let val = build_value(input);
    let encoded = encode(&val);

    if ok {
        let enc = encoded.unwrap_or_else(|e| panic!("{}: encode failed: {}", id, e));
        let decoded = decode(&enc).unwrap_or_else(|e| panic!("{}: decode failed: {}", id, e));
        assert!(
            values_equal(&val, &decoded),
            "{}: depth roundtrip mismatch",
            id
        );
    } else {
        // Expect encode or decode to fail
        match encoded {
            Err(_) => {} // encode rejected, ok
            Ok(enc) => {
                assert!(
                    decode(&enc).is_err(),
                    "{}: expected depth limit error",
                    id
                );
            }
        }
    }
}
