//! Schema fingerprinting for Cowrie values.
//!
//! Provides FNV-1a based fingerprints for type routing and schema equality checks.

use super::types::Value;

/// FNV-1a 64-bit offset basis
const FNV_OFFSET_BASIS: u64 = 14695981039346656037;

/// FNV-1a 64-bit prime
const FNV_PRIME: u64 = 1099511628211;

/// Compute a 64-bit FNV-1a fingerprint of a value's schema.
///
/// The fingerprint captures type structure (field names, types, tensor metadata)
/// but not actual values. Two values with identical structure produce the same fingerprint.
///
/// This is useful for:
/// - Type routing in stream protocols
/// - Schema-based dispatch
/// - Detecting schema drift
/// - Fast schema equality checks
pub fn schema_fingerprint64(value: &Value) -> u64 {
    hash_schema(value, FNV_OFFSET_BASIS)
}

/// Returns the low 32 bits of the 64-bit schema fingerprint.
/// Suitable for use as a type ID in stream frames.
pub fn schema_fingerprint32(value: &Value) -> u32 {
    schema_fingerprint64(value) as u32
}

/// Check if two values have the same schema.
pub fn schema_equals(a: &Value, b: &Value) -> bool {
    schema_fingerprint64(a) == schema_fingerprint64(b)
}

/// Map Value variant to Go's Type enum for cross-language fingerprint compatibility.
/// Go's Type enum (iota): Null=0, Bool=1, Int64=2, Uint64=3, Float64=4, Decimal128=5,
/// String=6, Bytes=7, Datetime64=8, UUID128=9, BigInt=10, Array=11, Object=12,
/// Tensor=13, TensorRef=14, Image=15, Audio=16, Adjlist=17, RichText=18, Delta=19, UnknownExt=20
/// Node=21, Edge=22, NodeBatch=23, EdgeBatch=24, GraphShard=25
fn value_type_ord(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Uint(_) => 3,
        Value::Float(_) => 4,
        Value::Decimal(_) => 5,
        Value::String(_) => 6,
        Value::Bytes(_) => 7,
        Value::DateTime(_) => 8,
        Value::Uuid(_) => 9,
        Value::BigInt(_) => 10,
        Value::Array(_) => 11,
        Value::Object(_) => 12,
        Value::Tensor(_) => 13,
        Value::TensorRef(_) => 14,
        Value::Image(_) => 15,
        Value::Audio(_) => 16,
        Value::Adjlist(_) => 17,
        Value::RichText(_) => 18,
        Value::Delta(_) => 19,
        Value::Ext(_) => 20, // UnknownExt in Go
        Value::Node(_) => 21,
        Value::Edge(_) => 22,
        Value::NodeBatch(_) => 23,
        Value::EdgeBatch(_) => 24,
        Value::GraphShard(_) => 25,
        Value::Bitmask { .. } => 26,
    }
}

fn hash_schema(value: &Value, mut h: u64) -> u64 {
    // Hash the type ordinal (Go's Type enum value for cross-language compatibility)
    h = fnv_hash_byte(h, value_type_ord(value));

    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Uint(_) |
        Value::Float(_) | Value::String(_) | Value::Bytes(_) |
        Value::Decimal(_) | Value::DateTime(_) | Value::Uuid(_) | Value::BigInt(_) => {
            // Scalar types: type tag is sufficient
        }

        Value::Array(arr) => {
            // Hash array length and element schemas
            h = fnv_hash_u64(h, arr.len() as u64);
            for item in arr {
                h = hash_schema(item, h);
            }
        }

        Value::Object(obj) => {
            // Hash object length and key+schema pairs (already sorted by BTreeMap)
            h = fnv_hash_u64(h, obj.len() as u64);
            for (key, val) in obj {
                h = fnv_hash_string(h, key);
                h = hash_schema(val, h);
            }
        }

        Value::Tensor(t) => {
            // Include dtype and rank (dims are data, not schema)
            h = fnv_hash_byte(h, t.dtype as u8);
            h = fnv_hash_u64(h, t.shape.len() as u64);
        }

        Value::TensorRef(r) => {
            h = fnv_hash_byte(h, r.store_id);
        }

        Value::Image(img) => {
            h = fnv_hash_byte(h, img.format as u8);
        }

        Value::Audio(aud) => {
            h = fnv_hash_byte(h, aud.encoding as u8);
            h = fnv_hash_byte(h, aud.channels);
        }

        Value::Adjlist(adj) => {
            h = fnv_hash_byte(h, adj.id_width);
        }

        Value::RichText(_) => {
            // RichText schema is just the type tag
        }

        Value::Delta(_) => {
            // Delta schema is just the type tag
        }

        Value::Ext(ext) => {
            h = fnv_hash_u64(h, ext.type_id);
        }

        // Graph types
        Value::Node(node) => {
            h = fnv_hash_u64(h, node.labels.len() as u64);
            h = fnv_hash_u64(h, node.props.len() as u64);
            for (key, val) in &node.props {
                h = fnv_hash_string(h, key);
                h = hash_schema(val, h);
            }
        }
        Value::Edge(edge) => {
            h = fnv_hash_u64(h, edge.props.len() as u64);
            for (key, val) in &edge.props {
                h = fnv_hash_string(h, key);
                h = hash_schema(val, h);
            }
        }
        Value::NodeBatch(batch) => {
            h = fnv_hash_u64(h, batch.nodes.len() as u64);
            for node in &batch.nodes {
                h = hash_schema(&Value::Node(node.clone()), h);
            }
        }
        Value::EdgeBatch(batch) => {
            h = fnv_hash_u64(h, batch.edges.len() as u64);
            for edge in &batch.edges {
                h = hash_schema(&Value::Edge(edge.clone()), h);
            }
        }
        Value::GraphShard(shard) => {
            h = fnv_hash_u64(h, shard.nodes.len() as u64);
            h = fnv_hash_u64(h, shard.edges.len() as u64);
            h = fnv_hash_u64(h, shard.metadata.len() as u64);
        }
        Value::Bitmask { count, .. } => {
            h = fnv_hash_u64(h, *count);
        }
    }

    h
}

#[inline]
fn fnv_hash_byte(mut h: u64, b: u8) -> u64 {
    h ^= b as u64;
    h = h.wrapping_mul(FNV_PRIME);
    h
}

#[inline]
fn fnv_hash_u64(mut h: u64, v: u64) -> u64 {
    for i in 0..8 {
        h ^= (v >> (i * 8)) & 0xFF;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

#[inline]
fn fnv_hash_string(mut h: u64, s: &str) -> u64 {
    // Hash length first
    h = fnv_hash_u64(h, s.len() as u64);
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Return a human-readable description of the schema.
pub fn schema_descriptor(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Int(_) => "int64".to_string(),
        Value::Uint(_) => "uint64".to_string(),
        Value::Float(_) => "float64".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Bytes(_) => "bytes".to_string(),
        Value::Decimal(_) => "decimal128".to_string(),
        Value::DateTime(_) => "datetime64".to_string(),
        Value::Uuid(_) => "uuid128".to_string(),
        Value::BigInt(_) => "bigint".to_string(),

        Value::Array(arr) => {
            if arr.is_empty() {
                "[]".to_string()
            } else {
                format!("[{},...]", schema_descriptor(&arr[0]))
            }
        }

        Value::Object(obj) => {
            if obj.is_empty() {
                "{}".to_string()
            } else {
                let first_key = obj.keys().next().unwrap();
                format!("{{{},...}}", first_key)
            }
        }

        Value::Tensor(t) => format!("tensor<{:?}>", t.dtype),
        Value::TensorRef(_) => "tensor_ref".to_string(),
        Value::Image(_) => "image".to_string(),
        Value::Audio(_) => "audio".to_string(),
        Value::Adjlist(_) => "adjlist".to_string(),
        Value::RichText(_) => "richtext".to_string(),
        Value::Delta(_) => "delta".to_string(),
        Value::Ext(_) => "ext".to_string(),
        Value::Node(_) => "node".to_string(),
        Value::Edge(_) => "edge".to_string(),
        Value::NodeBatch(_) => "node_batch".to_string(),
        Value::EdgeBatch(_) => "edge_batch".to_string(),
        Value::GraphShard(_) => "graph_shard".to_string(),
        Value::Bitmask { .. } => "bitmask".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_different_types() {
        let null = Value::Null;
        let bool_val = Value::Bool(true);
        let int_val = Value::Int(42);

        let fp_null = schema_fingerprint64(&null);
        let fp_bool = schema_fingerprint64(&bool_val);
        let fp_int = schema_fingerprint64(&int_val);

        assert_ne!(fp_null, fp_bool);
        assert_ne!(fp_bool, fp_int);
        assert_ne!(fp_null, fp_int);
    }

    #[test]
    fn test_fingerprint_same_schema() {
        let obj1 = Value::object(vec![
            ("name", Value::String("Alice".into())),
            ("age", Value::Int(30)),
        ]);

        let obj2 = Value::object(vec![
            ("name", Value::String("Bob".into())),
            ("age", Value::Int(25)),
        ]);

        assert_eq!(
            schema_fingerprint64(&obj1),
            schema_fingerprint64(&obj2)
        );
    }

    #[test]
    fn test_fingerprint_different_fields() {
        let obj1 = Value::object(vec![("name", Value::String("test".into()))]);
        let obj2 = Value::object(vec![("id", Value::String("test".into()))]);

        assert_ne!(
            schema_fingerprint64(&obj1),
            schema_fingerprint64(&obj2)
        );
    }

    #[test]
    fn test_schema_equals() {
        let obj1 = Value::object(vec![
            ("x", Value::Int(1)),
            ("y", Value::Int(2)),
        ]);

        let obj2 = Value::object(vec![
            ("x", Value::Int(100)),
            ("y", Value::Int(200)),
        ]);

        assert!(schema_equals(&obj1, &obj2));
    }

    #[test]
    fn test_fingerprint32() {
        let obj = Value::object(vec![("test", Value::Int(1))]);
        let fp64 = schema_fingerprint64(&obj);
        let fp32 = schema_fingerprint32(&obj);

        assert_eq!(fp32, fp64 as u32);
    }
}
