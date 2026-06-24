//! Graph JSON encode-after-roundtrip parity.
//!
//! Mirrors go/graph_json_roundtrip_test.go (review gap #2). The JSON bridge
//! reconstructs graph values; this asserts the reconstructed value also ENCODES
//! (the Go bug was encode-after-JSON, which a pure to_json/from_json round-trip
//! would not catch). Confirms the Rust bridge has no analogous bug.

use cowrie_rs::gen2::types::{CowrieError, EdgeBatchData, EdgeData, NodeBatchData, NodeData};
use cowrie_rs::gen2::{decode, encode, from_json, to_json, Value};
use std::collections::BTreeMap;

fn props(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// to_json -> from_json -> encode: the full bridge + encode path. Graph tags (0x35-0x38) are
/// RESERVED in canonical v1 (SPEC-v1 §2.3), so the produced bytes are intentionally NOT decodable:
/// we assert the JSON bridge faithfully reconstructs the value and that it still ENCODES (the Go
/// bug was encode-after-JSON failing), then confirm a conformant decoder rejects the reserved tag.
fn json_then_encode(v: &Value, reserved_tag: u8) -> Value {
    let js = to_json(v).expect("to_json");
    let back = from_json(&js).expect("from_json");
    let bytes = encode(&back).expect("encode after JSON round-trip");
    assert!(
        matches!(decode(&bytes), Err(CowrieError::InvalidTag(t)) if t == reserved_tag),
        "reserved graph tag 0x{:02x} must reject on decode",
        reserved_tag
    );
    back
}

#[test]
fn node_encodes_after_json_roundtrip() {
    let n = Value::Node(NodeData {
        id: "n1".to_string(),
        labels: vec!["Person".to_string()],
        props: props(&[
            ("age", Value::Int(42)),
            ("score", Value::Float(3.5)),
            ("name", Value::String("Alice".to_string())),
        ]),
    });
    assert_eq!(json_then_encode(&n, 0x35), n);
}

#[test]
fn edge_encodes_after_json_roundtrip() {
    let e = Value::Edge(EdgeData {
        from: "a".to_string(),
        to: "b".to_string(),
        edge_type: "KNOWS".to_string(),
        props: props(&[("weight", Value::Int(7)), ("w2", Value::Float(1.5))]),
    });
    assert_eq!(json_then_encode(&e, 0x36), e);
}

#[test]
fn node_batch_encodes_after_json_roundtrip() {
    let nb = Value::NodeBatch(NodeBatchData {
        nodes: vec![NodeData {
            id: "n1".to_string(),
            labels: vec!["P".to_string()],
            props: props(&[("age", Value::Int(1))]),
        }],
    });
    assert_eq!(json_then_encode(&nb, 0x37), nb);
}

#[test]
fn edge_batch_encodes_after_json_roundtrip() {
    let eb = Value::EdgeBatch(EdgeBatchData {
        edges: vec![EdgeData {
            from: "a".to_string(),
            to: "b".to_string(),
            edge_type: "R".to_string(),
            props: props(&[("w", Value::Int(2))]),
        }],
    });
    assert_eq!(json_then_encode(&eb, 0x38), eb);
}
