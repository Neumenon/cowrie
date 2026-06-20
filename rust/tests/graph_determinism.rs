//! Regression test: Rust graph encoding must produce byte-identical output to
//! the Go canonical hex for NODE, EDGE, NODE_BATCH, and EDGE_BATCH.
//!
//! Go ground truth captured from the reference implementation with
//! Deterministic=true. BTreeMap<String,Value> iteration is sorted by key bytes,
//! so no explicit sort step is needed — but this test pins the output to catch
//! any future regression (e.g. props type changed to HashMap, or field-order
//! drift in the wire format).

use cowrie_rs::gen2::{
    encode,
    types::{EdgeBatchData, EdgeData, NodeBatchData, NodeData, Value},
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Go canonical hex (ground truth)
// ---------------------------------------------------------------------------

const GO_NODE: &str = "534a02000203616765046e616d6535026e310106506572736f6e02005e010505416c696365";

const GO_EDGE: &str = "534a0200020573696e6365067765696768743601610162054b4e4f5753020003c81f0145";

const GO_NODE_BATCH: &str =
    "534a02000203616765046e616d653701026e310106506572736f6e02005e010505416c696365";

const GO_EDGE_BATCH: &str =
    "534a0200020573696e636506776569676874380101610162054b4e4f5753020003c81f0145";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Build the canonical NODE value:
///   id="n1", labels=["Person"], props={"age": Int(30), "name": String("Alice")}
///   (BTreeMap sorts keys: "age" < "name")
fn make_node() -> Value {
    let mut props = BTreeMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    Value::Node(NodeData::new("n1", vec!["Person".to_string()], props))
}

/// Build the canonical EDGE value:
///   from="a", to="b", type="KNOWS", props={"since": Int(2020), "weight": Int(5)}
///   (BTreeMap sorts keys: "since" < "weight")
fn make_edge() -> Value {
    let mut props = BTreeMap::new();
    props.insert("weight".to_string(), Value::Int(5));
    props.insert("since".to_string(), Value::Int(2020));
    Value::Edge(EdgeData::new("a", "b", "KNOWS", props))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn graph_determinism_node_matches_go() {
    let encoded = encode(&make_node()).expect("encode NODE");
    let got = hex(&encoded);
    assert_eq!(
        got, GO_NODE,
        "NODE encoding does not match Go ground truth.\n  got: {}\n want: {}",
        got, GO_NODE
    );
}

#[test]
fn graph_determinism_edge_matches_go() {
    let encoded = encode(&make_edge()).expect("encode EDGE");
    let got = hex(&encoded);
    assert_eq!(
        got, GO_EDGE,
        "EDGE encoding does not match Go ground truth.\n  got: {}\n want: {}",
        got, GO_EDGE
    );
}

#[test]
fn graph_determinism_node_batch_matches_go() {
    let node = match make_node() {
        Value::Node(n) => n,
        _ => unreachable!(),
    };
    let batch = Value::NodeBatch(NodeBatchData { nodes: vec![node] });
    let encoded = encode(&batch).expect("encode NODE_BATCH");
    let got = hex(&encoded);
    assert_eq!(
        got, GO_NODE_BATCH,
        "NODE_BATCH encoding does not match Go ground truth.\n  got: {}\n want: {}",
        got, GO_NODE_BATCH
    );
}

#[test]
fn graph_determinism_edge_batch_matches_go() {
    let edge = match make_edge() {
        Value::Edge(e) => e,
        _ => unreachable!(),
    };
    let batch = Value::EdgeBatch(EdgeBatchData { edges: vec![edge] });
    let encoded = encode(&batch).expect("encode EDGE_BATCH");
    let got = hex(&encoded);
    assert_eq!(
        got, GO_EDGE_BATCH,
        "EDGE_BATCH encoding does not match Go ground truth.\n  got: {}\n want: {}",
        got, GO_EDGE_BATCH
    );
}
