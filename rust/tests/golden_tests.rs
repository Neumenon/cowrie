//! Golden test vectors - verify Rust decodes Go-generated binaries correctly.

use cowrie_rs::gen2::{decode, encode, Value};
use std::fs;
use std::path::PathBuf;

fn testdata_path(subpath: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push("testdata");
    path.push(subpath);
    path
}

#[test]
fn test_decode_gen2_tensor() {
    let path = testdata_path("gen2/tensor.cowrie");
    let data = fs::read(&path).expect("failed to read tensor.cowrie");

    let value = decode(&data).expect("failed to decode tensor.cowrie");

    // Verify structure
    if let Value::Object(obj) = &value {
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("scores"));
        assert!(obj.contains_key("embedding"));

        // Check id
        if let Some(Value::String(id)) = obj.get("id") {
            assert_eq!(id, "model-output");
        } else {
            panic!("expected id to be string");
        }

        // Check scores array
        if let Some(Value::Array(scores)) = obj.get("scores") {
            assert_eq!(scores.len(), 3);
        } else {
            panic!("expected scores to be array");
        }

        // Check embedding - JSON bridge encodes as object with _type field
        if let Some(Value::Object(emb)) = obj.get("embedding") {
            // JSON bridge format: {"_type": "tensor", "dtype": ..., "dims": ..., "data": ...}
            assert!(emb.contains_key("_type"));
            assert!(emb.contains_key("dtype"));
            assert!(emb.contains_key("dims"));
            assert!(emb.contains_key("data"));
        } else {
            panic!("expected embedding to be object (JSON bridge format)");
        }
    } else {
        panic!("expected root to be object");
    }
}

#[test]
fn test_decode_gen2_image_meta() {
    let path = testdata_path("gen2/image_meta.cowrie");
    let data = fs::read(&path).expect("failed to read image_meta.cowrie");

    let value = decode(&data).expect("failed to decode image_meta.cowrie");

    // Verify structure
    if let Value::Object(obj) = &value {
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("width"));
        assert!(obj.contains_key("height"));
        assert!(obj.contains_key("format"));
        assert!(obj.contains_key("tags"));

        // Check dimensions
        if let Some(Value::Int(w)) = obj.get("width") {
            assert_eq!(*w, 1920);
        }
        if let Some(Value::Int(h)) = obj.get("height") {
            assert_eq!(*h, 1080);
        }
    } else {
        panic!("expected root to be object");
    }
}

#[test]
fn test_roundtrip_gen2() {
    // Create a value
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("name".to_string(), Value::String("test".to_string()));
    obj.insert("count".to_string(), Value::Int(42));
    obj.insert("active".to_string(), Value::Bool(true));
    let original = Value::Object(obj);

    // Encode
    let encoded = encode(&original).expect("encode failed");

    // Decode
    let decoded = decode(&encoded).expect("decode failed");

    // Compare
    if let (Value::Object(orig), Value::Object(dec)) = (&original, &decoded) {
        assert_eq!(orig.get("name"), dec.get("name"));
        assert_eq!(orig.get("count"), dec.get("count"));
        assert_eq!(orig.get("active"), dec.get("active"));
    } else {
        panic!("type mismatch");
    }
}
