//! Schema fingerprinting for Cowrie values (SPEC-v1 §4).
//!
//! FNV-1a-64 over a per-variant contribution grammar that captures *type structure only*, using the
//! spec-pinned fingerprint codes (§4.1) — a namespace DISTINCT from the wire tags and from any host
//! enum/`iota` (the B4 root bug). Lengths are MINIMAL LEB128 uvarints; Tensor includes dtype + rank
//! + dims, Bitmask includes its bit count, Extension includes its extType. Byte-for-byte identical
//! to the Python oracle `tools/cowrie_ref/fingerprint.py`.

use super::types::Value;

/// FNV-1a 64-bit offset basis
const FNV_OFFSET_BASIS: u64 = 14695981039346656037;

/// FNV-1a 64-bit prime
const FNV_PRIME: u64 = 1099511628211;

/// §4.1 fingerprint codes (FPC) — a namespace distinct from the §2.3 wire tags.
/// Mirrors `model.FPC` in the Python oracle exactly.
mod fpc {
    pub const NULL: u8 = 0x00;
    pub const BOOL: u8 = 0x01;
    pub const INT: u8 = 0x02;
    pub const UINT: u8 = 0x03;
    pub const BIGINT: u8 = 0x04;
    pub const FLOAT: u8 = 0x05;
    pub const DECIMAL: u8 = 0x06;
    pub const STRING: u8 = 0x07;
    pub const BYTES: u8 = 0x08;
    pub const UUID: u8 = 0x09;
    pub const DATETIME: u8 = 0x0A;
    pub const ARRAY: u8 = 0x0B;
    pub const OBJECT: u8 = 0x0C;
    pub const TENSOR: u8 = 0x0D;
    pub const BITMASK: u8 = 0x0E;
    pub const EXTENSION: u8 = 0x0F;
}

/// Append a MINIMAL LEB128 uvarint to `out` (matches ref `varint.encode_uvarint`).
fn push_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Append the §4.2 fingerprint contribution of `value` (type structure only — never values).
/// Byte-identical to `fingerprint._fp` in the Python oracle.
fn fp_contribution(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(fpc::NULL),
        Value::Bool(_) => out.push(fpc::BOOL),
        // §1.1 number domain: i64 is always the "int" domain; a canonical Uint is always
        // > INT64_MAX so it is the "uint" domain; BigInt is the "bigint" domain.
        Value::Int(_) => out.push(fpc::INT),
        Value::Uint(_) => out.push(fpc::UINT),
        Value::BigInt(_) => out.push(fpc::BIGINT),
        Value::Float(_) => out.push(fpc::FLOAT),
        Value::Decimal(_) => out.push(fpc::DECIMAL),
        Value::String(_) => out.push(fpc::STRING),
        Value::Bytes(_) => out.push(fpc::BYTES),
        Value::Uuid(_) => out.push(fpc::UUID),
        Value::DateTime(_) => out.push(fpc::DATETIME),
        Value::Bitmask { count, .. } => {
            // §4.2: collection sizes are structural.
            out.push(fpc::BITMASK);
            push_uvarint(out, *count);
        }
        Value::Tensor(t) => {
            // §4.2: dtype + rank + shape are type structure.
            out.push(fpc::TENSOR);
            out.push(t.dtype as u8);
            out.push(t.shape.len() as u8);
            for d in &t.shape {
                push_uvarint(out, *d);
            }
        }
        Value::Ext(ext) => {
            // §4.2: extType is structure, payload excluded.
            out.push(fpc::EXTENSION);
            push_uvarint(out, ext.type_id);
        }
        Value::Array(arr) => {
            out.push(fpc::ARRAY);
            push_uvarint(out, arr.len() as u64);
            for item in arr {
                fp_contribution(out, item);
            }
        }
        Value::Object(obj) => {
            out.push(fpc::OBJECT);
            push_uvarint(out, obj.len() as u64);
            // BTreeMap iterates keys in ascending byte order — the §2.4 canonical order the
            // oracle's `sorted_keys` produces (keys are UTF-8 and BTreeMap<String> sorts by bytes).
            for (key, val) in obj {
                let raw = key.as_bytes();
                push_uvarint(out, raw.len() as u64);
                out.extend_from_slice(raw);
                fp_contribution(out, val);
            }
        }
        // Reserved/non-core variants (TensorRef/Image/Audio/Node/Edge/batches) are never part of a
        // canonical decoded value (the decoder rejects their tags with ERR_RESERVED_TAG), so they
        // never reach this path via `recode`. They are not in the §4 grammar; hash them by a stable
        // distinct sentinel so the function remains total without colliding with a core code.
        Value::TensorRef(_) => out.push(0xF1),
        Value::Image(_) => out.push(0xF2),
        Value::Audio(_) => out.push(0xF3),
        Value::Node(_) => out.push(0xF4),
        Value::Edge(_) => out.push(0xF5),
        Value::NodeBatch(_) => out.push(0xF6),
        Value::EdgeBatch(_) => out.push(0xF7),
    }
}

/// Compute the §4.3 64-bit type-structure fingerprint of a value: FNV-1a-64 over its §4.2
/// fingerprint-grammar byte string. Byte-for-byte identical to the Python oracle.
pub fn schema_fingerprint64(value: &Value) -> u64 {
    let mut buf = Vec::new();
    fp_contribution(&mut buf, value);
    let mut h = FNV_OFFSET_BASIS;
    for b in &buf {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Returns the low 32 bits of the 64-bit schema fingerprint.
/// Suitable for use as a type ID in stream frames.
pub fn schema_fingerprint32(value: &Value) -> u32 {
    schema_fingerprint64(value) as u32
}

/// Check if two values have structurally identical schemas.
///
/// Recursively compares types, object field names, array element schemas,
/// tensor dtype/rank, image format, audio encoding/channels, tensor_ref store ID,
/// and unknown-ext type_id — mirroring exactly what `schema_fingerprint64` hashes.
/// Actual values (ints, strings, tensor data, etc.) are not compared.
///
/// Use `schema_fingerprint_equal` for a faster probabilistic check.
pub fn schema_equals(a: &Value, b: &Value) -> bool {
    // Must be the same variant first
    match (a, b) {
        // Scalar types: variant equality is sufficient
        (Value::Null, Value::Null)
        | (Value::Bool(_), Value::Bool(_))
        | (Value::Int(_), Value::Int(_))
        | (Value::Uint(_), Value::Uint(_))
        | (Value::Float(_), Value::Float(_))
        | (Value::String(_), Value::String(_))
        | (Value::Bytes(_), Value::Bytes(_))
        | (Value::Decimal(_), Value::Decimal(_))
        | (Value::DateTime(_), Value::DateTime(_))
        | (Value::Uuid(_), Value::Uuid(_))
        | (Value::BigInt(_), Value::BigInt(_)) => true,

        (Value::Array(av), Value::Array(bv)) => {
            av.len() == bv.len()
                && av
                    .iter()
                    .zip(bv.iter())
                    .all(|(ai, bi)| schema_equals(ai, bi))
        }

        (Value::Object(ao), Value::Object(bo)) => {
            // BTreeMap iterates in sorted key order — same canonical order as hash_schema
            ao.len() == bo.len()
                && ao
                    .iter()
                    .zip(bo.iter())
                    .all(|((ak, av), (bk, bv))| ak == bk && schema_equals(av, bv))
        }

        (Value::Tensor(at), Value::Tensor(bt)) => {
            at.dtype == bt.dtype && at.shape.len() == bt.shape.len()
        }

        (Value::TensorRef(ar), Value::TensorRef(br)) => ar.store_id == br.store_id,

        (Value::Image(ai), Value::Image(bi)) => ai.format == bi.format,

        (Value::Audio(aa), Value::Audio(ba)) => {
            aa.encoding == ba.encoding && aa.channels == ba.channels
        }

        (Value::Ext(ae), Value::Ext(be)) => ae.type_id == be.type_id,

        // Graph types and bitmask: type tag equality (ensured by same variant) is sufficient.
        (Value::Node(_), Value::Node(_))
        | (Value::Edge(_), Value::Edge(_))
        | (Value::NodeBatch(_), Value::NodeBatch(_))
        | (Value::EdgeBatch(_), Value::EdgeBatch(_))
        | (Value::Bitmask { .. }, Value::Bitmask { .. }) => true,

        // Different variants
        _ => false,
    }
}

/// Check if two values have the same schema using a probabilistic 64-bit fingerprint.
///
/// This is the fast path: O(1) for flat values, and shares cost with any fingerprint
/// already computed. Carries a 1-in-2^64 collision risk — prefer `schema_equals`
/// for correctness-critical comparisons.
pub fn schema_fingerprint_equal(a: &Value, b: &Value) -> bool {
    schema_fingerprint64(a) == schema_fingerprint64(b)
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
        Value::Ext(_) => "ext".to_string(),
        Value::Node(_) => "node".to_string(),
        Value::Edge(_) => "edge".to_string(),
        Value::NodeBatch(_) => "node_batch".to_string(),
        Value::EdgeBatch(_) => "edge_batch".to_string(),
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

        assert_eq!(schema_fingerprint64(&obj1), schema_fingerprint64(&obj2));
    }

    #[test]
    fn test_fingerprint_different_fields() {
        let obj1 = Value::object(vec![("name", Value::String("test".into()))]);
        let obj2 = Value::object(vec![("id", Value::String("test".into()))]);

        assert_ne!(schema_fingerprint64(&obj1), schema_fingerprint64(&obj2));
    }

    #[test]
    fn test_schema_equals() {
        let obj1 = Value::object(vec![("x", Value::Int(1)), ("y", Value::Int(2))]);

        let obj2 = Value::object(vec![("x", Value::Int(100)), ("y", Value::Int(200))]);

        assert!(schema_equals(&obj1, &obj2));
    }

    // ============================================================
    // FIX 4 — schema_equals is structural, not fingerprint-based
    // ============================================================

    #[test]
    fn test_schema_equals_structural_same_fields_different_values() {
        // Same field names + types, different values → must be equal
        let a = Value::object(vec![
            ("name", Value::String("Alice".into())),
            ("age", Value::Int(30)),
        ]);
        let b = Value::object(vec![
            ("name", Value::String("Bob".into())),
            ("age", Value::Int(99)),
        ]);
        assert!(
            schema_equals(&a, &b),
            "same fields+types but different values should be schema-equal"
        );
    }

    #[test]
    fn test_schema_equals_structural_renamed_field() {
        // Field renamed → schemas must differ
        let a = Value::object(vec![("name", Value::String("Alice".into()))]);
        let b = Value::object(vec![("alias", Value::String("Alice".into()))]);
        assert!(
            !schema_equals(&a, &b),
            "renamed field should make schemas unequal"
        );
    }

    #[test]
    fn test_schema_equals_structural_type_change() {
        // Field type changed → schemas must differ
        let a = Value::object(vec![("count", Value::Int(1))]);
        let b = Value::object(vec![("count", Value::Float(1.0))]);
        assert!(
            !schema_equals(&a, &b),
            "type change on field should make schemas unequal"
        );
    }

    #[test]
    fn test_schema_fingerprint_equal_agrees_with_schema_equals() {
        // On matching schemas, both functions agree
        let a = Value::object(vec![("x", Value::Int(1)), ("y", Value::Int(2))]);
        let b = Value::object(vec![("x", Value::Int(99)), ("y", Value::Int(0))]);
        assert!(schema_equals(&a, &b));
        assert!(schema_fingerprint_equal(&a, &b));
    }

    #[test]
    fn test_schema_equals_structural_nested() {
        // Nested objects: structural equality must recurse
        let a = Value::object(vec![(
            "inner",
            Value::object(vec![("val", Value::Bool(true))]),
        )]);
        let b = Value::object(vec![(
            "inner",
            Value::object(vec![("val", Value::Bool(false))]),
        )]);
        let c = Value::object(vec![(
            "inner",
            Value::object(vec![("other", Value::Bool(true))]),
        )]);
        assert!(
            schema_equals(&a, &b),
            "same nested field names should be equal"
        );
        assert!(
            !schema_equals(&a, &c),
            "different nested field names should be unequal"
        );
    }

    #[test]
    fn test_fingerprint32() {
        let obj = Value::object(vec![("test", Value::Int(1))]);
        let fp64 = schema_fingerprint64(&obj);
        let fp32 = schema_fingerprint32(&obj);

        assert_eq!(fp32, fp64 as u32);
    }

    #[test]
    fn test_fingerprint_fpc_oracle_parity() {
        // Ground truth from the Python oracle `tools/cowrie_ref/fingerprint.py` (§4 FPC grammar),
        // NOT the legacy ordinal/iota scheme. Null matches the golden fingerprint64 for "null".
        use crate::gen2::types::ExtData;
        assert_eq!(schema_fingerprint64(&Value::Null), 12638153115695167455);
        // Extension: [FPC.extension=0x0F] + uvarint(ext_type); payload excluded.
        let ext = Value::Ext(ExtData {
            type_id: 99,
            payload: vec![1, 2, 3],
        });
        assert_eq!(schema_fingerprint64(&ext), 595430659518474151);
        // Bitmask: [FPC.bitmask=0x0E] + uvarint(count) — count=3.
        let bm = Value::Bitmask {
            count: 3,
            bits: vec![0b0000_0101],
        };
        assert_eq!(schema_fingerprint64(&bm), 596424618030187670);
        // Object: [FPC.object] + uvarint(len) + per sorted key uvarint(len)+raw+fp(val).
        let obj = Value::object(vec![("age", Value::Int(1)), ("name", Value::String("x".into()))]);
        assert_eq!(schema_fingerprint64(&obj), 3154875172356742563);
    }
}
