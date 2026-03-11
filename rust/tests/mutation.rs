//! Suite 4: Mutation tests — corrupt valid cowrie binary fixtures and verify
//! the decoder never panics. Three strategies: truncation, single-byte flip,
//! length inflation.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use cowrie_rs::gen2::{decode, encode, Value};

// ============================================================
// Helpers
// ============================================================

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/fixtures")
}

fn load_fixture(name: &str) -> Option<Vec<u8>> {
    let path = fixture_dir().join(name);
    fs::read(&path).ok()
}

/// Decode at every truncation point [0..len-1]. None may panic.
fn assert_no_panic_on_truncation(data: &[u8]) {
    for i in 0..data.len() {
        let truncated = &data[..i];
        let _ = decode(truncated); // must not panic
    }
}

/// XOR each byte with 0xFF and decode. None may panic.
fn assert_no_panic_on_bitflip(data: &[u8]) {
    for i in 0..data.len() {
        let mut corrupted = data.to_vec();
        corrupted[i] ^= 0xFF;
        let _ = decode(&corrupted); // must not panic
    }
}

/// Inflate varint-like bytes to large values. Must not OOM.
fn assert_no_oom_on_length_inflation(data: &[u8]) {
    let header_len = 6.min(data.len());
    for i in header_len..data.len() {
        let mut inflated = data.to_vec();
        inflated[i] = 0xFE;
        let _ = decode(&inflated);

        let mut big = data[..i].to_vec();
        big.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x7F]); // ~2GB varint
        if i + 1 < data.len() {
            big.extend_from_slice(&data[i + 1..]);
        }
        let _ = decode(&big);
    }
}

/// Run all three mutation strategies on data.
fn mutate_all(data: &[u8]) {
    assert_no_panic_on_truncation(data);
    assert_no_panic_on_bitflip(data);
    assert_no_oom_on_length_inflation(data);
}

// ============================================================
// Core fixtures from testdata/fixtures/core/
// ============================================================

#[test]
fn mutation_fixture_null() {
    if let Some(data) = load_fixture("core/null.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_true() {
    if let Some(data) = load_fixture("core/true.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_int() {
    if let Some(data) = load_fixture("core/int.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_float() {
    if let Some(data) = load_fixture("core/float.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_string() {
    if let Some(data) = load_fixture("core/string.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_array() {
    if let Some(data) = load_fixture("core/array.cowrie") {
        mutate_all(&data);
    }
}

#[test]
fn mutation_fixture_object() {
    if let Some(data) = load_fixture("core/object.cowrie") {
        mutate_all(&data);
    }
}

// ============================================================
// Synthetic values: encode known values, then mutate
// ============================================================

#[test]
fn mutation_synthetic_null() {
    let data = encode(&Value::Null).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_string() {
    let data = encode(&Value::String("hello world".to_string())).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_array() {
    let val = Value::Array(vec![
        Value::Int(1),
        Value::String("two".to_string()),
        Value::Bool(true),
        Value::Null,
    ]);
    let data = encode(&val).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_object() {
    let mut map = BTreeMap::new();
    map.insert("name".to_string(), Value::String("test".to_string()));
    map.insert("count".to_string(), Value::Int(42));
    map.insert("active".to_string(), Value::Bool(false));
    let data = encode(&Value::Object(map)).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_float() {
    let data = encode(&Value::Float(3.14159)).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_bytes() {
    let data = encode(&Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])).unwrap();
    mutate_all(&data);
}

#[test]
fn mutation_synthetic_nested() {
    // 10 levels of nesting
    let mut v = Value::Int(42);
    for _ in 0..10 {
        v = Value::Array(vec![v]);
    }
    let data = encode(&v).unwrap();
    mutate_all(&data);
}

// ============================================================
// Edge cases
// ============================================================

#[test]
fn mutation_empty_input() {
    let result = decode(&[]);
    assert!(result.is_err(), "empty input must return error");
}

#[test]
fn mutation_single_byte_inputs() {
    for b in 0u8..=255 {
        let _ = decode(&[b]);
    }
}

#[test]
fn mutation_two_byte_inputs() {
    for b0 in 0u8..=255 {
        for b1 in 0u8..=255 {
            let _ = decode(&[b0, b1]);
        }
    }
}
