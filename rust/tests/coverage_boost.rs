//! Comprehensive tests to boost coverage across gen1, gen2 encode/decode, json, schema, types.

use std::collections::BTreeMap;

use cowrie_rs::gen2::{
    self, decode, encode, encode_with_options, decode_with_options,
    Value, DType, TensorData, EncodeOptions, CowrieError,
    encode_framed, decode_framed, Compression,
    schema_fingerprint32, schema_fingerprint64, schema_equals,
    from_json, to_json, to_json_pretty,
    write_frame, read_frame, MasterWriterOptions, MasterFrame,
};
use cowrie_rs::gen2::decode::DecodeOptions;
use cowrie_rs::gen2::types::*;

// ============================================================
// Gen2 encode/decode roundtrip — all value types
// ============================================================

#[test]
fn roundtrip_null() {
    let v = Value::Null;
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Null);
}

#[test]
fn roundtrip_bool_true() {
    let v = Value::Bool(true);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Bool(true));
}

#[test]
fn roundtrip_bool_false() {
    let v = Value::Bool(false);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Bool(false));
}

#[test]
fn roundtrip_int_zero() {
    let v = Value::Int(0);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Int(0));
}

#[test]
fn roundtrip_fixint_small() {
    // FIXINT range: 0..=127
    for i in [0i64, 1, 42, 100, 127] {
        let v = Value::Int(i);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Int(i), "FIXINT failed for {}", i);
    }
}

#[test]
fn roundtrip_fixneg() {
    // FIXNEG range: -1..=-16
    for i in [-1i64, -2, -8, -15, -16] {
        let v = Value::Int(i);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Int(i), "FIXNEG failed for {}", i);
    }
}

#[test]
fn roundtrip_int_large() {
    // Outside FIXINT/FIXNEG range → full INT64
    for i in [128i64, 256, 1000, -17, -100, -999999, i64::MAX, i64::MIN] {
        let v = Value::Int(i);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Int(i), "INT64 failed for {}", i);
    }
}

#[test]
fn roundtrip_uint() {
    for u in [0u64, 1, 255, 65535, u64::MAX] {
        let v = Value::Uint(u);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Uint(u), "UINT64 failed for {}", u);
    }
}

#[test]
fn roundtrip_float() {
    for f in [0.0f64, 1.5, -3.14, f64::MAX, f64::MIN, f64::EPSILON] {
        let v = Value::Float(f);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Float(f), "FLOAT64 failed for {}", f);
    }
}

#[test]
fn roundtrip_string() {
    for s in ["", "hello", "unicode: \u{1F600}", &"x".repeat(300)] {
        let v = Value::String(s.to_string());
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::String(s.to_string()));
    }
}

#[test]
fn roundtrip_bytes() {
    for b in [vec![], vec![0u8], vec![1, 2, 3, 255], vec![0u8; 1000]] {
        let v = Value::Bytes(b.clone());
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::Bytes(b));
    }
}

#[test]
fn roundtrip_datetime() {
    for dt in [0i64, 1_000_000_000, -1_000_000_000, i64::MAX, i64::MIN] {
        let v = Value::DateTime(dt);
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, Value::DateTime(dt));
    }
}

#[test]
fn roundtrip_uuid() {
    let uuid = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let v = Value::Uuid(uuid);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Uuid(uuid));
}

#[test]
fn roundtrip_bigint() {
    let data = vec![0x01, 0x02, 0x03, 0xFF];
    let v = Value::BigInt(data.clone());
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::BigInt(data));
}

#[test]
fn roundtrip_decimal() {
    let mut data = vec![0u8; 17];
    data[0] = 2; // scale
    data[1] = 0x39; data[2] = 0x30; // coef
    let v = Value::Decimal(data.clone());
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Decimal(data));
}

#[test]
fn roundtrip_fixarray() {
    // FIXARRAY: 0..=15 elements
    let arr: Vec<Value> = (0..5).map(|i| Value::Int(i)).collect();
    let v = Value::Array(arr.clone());
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Array(arr));
}

#[test]
fn roundtrip_large_array() {
    // > 15 elements → ARRAY tag
    let arr: Vec<Value> = (0..20).map(|i| Value::Int(i)).collect();
    let v = Value::Array(arr.clone());
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Array(arr));
}

#[test]
fn roundtrip_empty_array() {
    let v = Value::Array(vec![]);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Array(vec![]));
}

#[test]
fn roundtrip_fixmap() {
    // FIXMAP: 0..=15 entries
    let v = Value::object(vec![
        ("a", Value::Int(1)),
        ("b", Value::String("hi".into())),
    ]);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_large_map() {
    // > 15 entries → OBJECT tag
    let pairs: Vec<(String, Value)> = (0..20)
        .map(|i| (format!("key_{:02}", i), Value::Int(i)))
        .collect();
    let mut map = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k, v);
    }
    let v = Value::Object(map.clone());
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, Value::Object(map));
}

#[test]
fn roundtrip_nested() {
    let v = Value::object(vec![
        ("outer", Value::object(vec![
            ("inner", Value::Array(vec![
                Value::Int(1), Value::Null, Value::Bool(false),
            ])),
        ])),
    ]);
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Tensor roundtrip
// ============================================================

#[test]
fn roundtrip_tensor_float32() {
    let data: Vec<u8> = 1.0f32.to_le_bytes().iter()
        .chain(2.0f32.to_le_bytes().iter())
        .chain(3.0f32.to_le_bytes().iter())
        .copied()
        .collect();
    let v = Value::Tensor(TensorData {
        dtype: DType::Float32,
        shape: vec![3],
        data,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_tensor_float64() {
    let data: Vec<u8> = 1.0f64.to_le_bytes().iter()
        .chain(2.0f64.to_le_bytes().iter())
        .copied()
        .collect();
    let v = Value::Tensor(TensorData {
        dtype: DType::Float64,
        shape: vec![2],
        data,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_tensor_int32() {
    let data: Vec<u8> = 42i32.to_le_bytes().iter()
        .chain((-1i32).to_le_bytes().iter())
        .copied()
        .collect();
    let v = Value::Tensor(TensorData {
        dtype: DType::Int32,
        shape: vec![2],
        data,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_tensor_all_dtypes() {
    let dtypes = [
        DType::Float32, DType::Float16, DType::BFloat16,
        DType::Int8, DType::Int16, DType::Int32, DType::Int64,
        DType::Uint8, DType::Uint16, DType::Uint32, DType::Uint64,
        DType::Float64, DType::Bool,
        DType::QINT4, DType::QINT2, DType::QINT3,
        DType::Ternary, DType::Binary,
    ];
    for dtype in dtypes {
        let v = Value::Tensor(TensorData {
            dtype,
            shape: vec![2],
            data: vec![0u8; 8],
        });
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, v, "dtype roundtrip failed for {:?}", dtype);
    }
}

#[test]
fn roundtrip_tensor_ref() {
    let v = Value::TensorRef(TensorRef {
        store_id: 3,
        key: vec![0xDE, 0xAD, 0xBE, 0xEF],
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Image / Audio roundtrip
// ============================================================

#[test]
fn roundtrip_image() {
    let formats = [
        ImageFormat::Jpeg, ImageFormat::Png, ImageFormat::Webp,
        ImageFormat::Avif, ImageFormat::Bmp,
    ];
    for fmt in formats {
        let v = Value::Image(ImageData {
            format: fmt,
            width: 640,
            height: 480,
            data: vec![0xFF; 100],
        });
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, v, "image format {:?}", fmt);
    }
}

#[test]
fn roundtrip_audio() {
    let encodings = [
        AudioEncoding::PcmInt16, AudioEncoding::PcmFloat32,
        AudioEncoding::Opus, AudioEncoding::Aac,
    ];
    for ae in encodings {
        let v = Value::Audio(AudioData {
            encoding: ae,
            sample_rate: 44100,
            channels: 2,
            data: vec![0x80; 200],
        });
        let enc = encode(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, v, "audio encoding {:?}", ae);
    }
}

// ============================================================
// Adjlist roundtrip
// ============================================================

#[test]
fn roundtrip_adjlist() {
    let v = Value::Adjlist(AdjlistData {
        id_width: 1,
        node_count: 3,
        edge_count: 2,
        row_offsets: vec![0, 1, 1, 2],
        col_indices: vec![0u8; 8], // 2 edges * 4 bytes (id_width=1)
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_adjlist_int64() {
    let v = Value::Adjlist(AdjlistData {
        id_width: 2,
        node_count: 2,
        edge_count: 1,
        row_offsets: vec![0, 1, 1],
        col_indices: vec![0u8; 8], // 1 edge * 8 bytes (id_width=2)
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// RichText roundtrip
// ============================================================

#[test]
fn roundtrip_richtext_plain() {
    let v = Value::RichText(RichTextData {
        text: "hello world".to_string(),
        tokens: None,
        spans: None,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_richtext_with_tokens() {
    let v = Value::RichText(RichTextData {
        text: "hello".to_string(),
        tokens: Some(vec![100, 200, 300]),
        spans: None,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_richtext_with_spans() {
    let v = Value::RichText(RichTextData {
        text: "hello world".to_string(),
        tokens: None,
        spans: Some(vec![
            RichTextSpan { start: 0, end: 5, kind_id: 1 },
            RichTextSpan { start: 6, end: 11, kind_id: 2 },
        ]),
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_richtext_with_both() {
    let v = Value::RichText(RichTextData {
        text: "test".to_string(),
        tokens: Some(vec![1, 2]),
        spans: Some(vec![RichTextSpan { start: 0, end: 4, kind_id: 1 }]),
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Delta roundtrip
// ============================================================

#[test]
fn roundtrip_delta() {
    let v = Value::Delta(DeltaData {
        base_id: 42,
        ops: vec![
            DeltaOp {
                op_code: DeltaOpCode::SetField,
                field_id: 1,
                value: Some(Box::new(Value::String("new_val".into()))),
            },
            DeltaOp {
                op_code: DeltaOpCode::DeleteField,
                field_id: 2,
                value: None,
            },
            DeltaOp {
                op_code: DeltaOpCode::AppendArray,
                field_id: 3,
                value: Some(Box::new(Value::Int(99))),
            },
        ],
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Ext roundtrip
// ============================================================

#[test]
fn roundtrip_ext() {
    let v = Value::Ext(ExtData {
        type_id: 42,
        payload: vec![1, 2, 3, 4, 5],
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Bitmask roundtrip
// ============================================================

#[test]
fn roundtrip_bitmask() {
    let v = Value::Bitmask {
        count: 16,
        bits: vec![0xFF, 0x00],
    };
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Graph types roundtrip
// ============================================================

#[test]
fn roundtrip_node() {
    let mut props = BTreeMap::new();
    props.insert("weight".to_string(), Value::Float(1.5));
    props.insert("name".to_string(), Value::String("Alice".into()));
    let v = Value::Node(NodeData {
        id: "node1".to_string(),
        labels: vec!["Person".to_string(), "Active".to_string()],
        props,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_edge() {
    let mut props = BTreeMap::new();
    props.insert("since".to_string(), Value::Int(2020));
    let v = Value::Edge(EdgeData {
        from: "a".to_string(),
        to: "b".to_string(),
        edge_type: "KNOWS".to_string(),
        props,
    });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_node_batch() {
    let nodes = vec![
        NodeData::new("n1", vec!["L1".into()], BTreeMap::new()),
        NodeData::new("n2", vec!["L2".into()], {
            let mut m = BTreeMap::new();
            m.insert("x".into(), Value::Int(10));
            m
        }),
    ];
    let v = Value::NodeBatch(NodeBatchData { nodes });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_edge_batch() {
    let edges = vec![
        EdgeData::new("a", "b", "REL", BTreeMap::new()),
        EdgeData::new("c", "d", "REL2", {
            let mut m = BTreeMap::new();
            m.insert("w".into(), Value::Float(0.5));
            m
        }),
    ];
    let v = Value::EdgeBatch(EdgeBatchData { edges });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn roundtrip_graph_shard() {
    let nodes = vec![
        NodeData::new("n1", vec!["L".into()], BTreeMap::new()),
    ];
    let edges = vec![
        EdgeData::new("n1", "n1", "SELF", BTreeMap::new()),
    ];
    let mut metadata = BTreeMap::new();
    metadata.insert("version".to_string(), Value::Int(1));
    let v = Value::GraphShard(GraphShardData { nodes, edges, metadata });
    let enc = encode(&v).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, v);
}

// ============================================================
// Encode options: omit_null
// ============================================================

#[test]
fn encode_omit_null_in_nested() {
    let inner = Value::object(vec![
        ("keep", Value::Int(1)),
        ("drop", Value::Null),
    ]);
    let v = Value::object(vec![
        ("child", inner),
        ("gone", Value::Null),
    ]);
    let opts = EncodeOptions { omit_null: true, ..Default::default() };
    let enc = encode_with_options(&v, &opts).unwrap();
    let dec = decode(&enc).unwrap();
    // "gone" and "drop" should be absent
    let obj = dec.as_object().unwrap();
    assert!(!obj.contains_key("gone"));
    let child = obj.get("child").unwrap().as_object().unwrap();
    assert!(!child.contains_key("drop"));
    assert_eq!(child.get("keep"), Some(&Value::Int(1)));
}

#[test]
fn encode_omit_null_in_node_props() {
    let mut props = BTreeMap::new();
    props.insert("keep".to_string(), Value::Int(1));
    props.insert("drop".to_string(), Value::Null);
    let v = Value::Node(NodeData {
        id: "n".to_string(),
        labels: vec![],
        props,
    });
    let opts = EncodeOptions { omit_null: true, ..Default::default() };
    let enc = encode_with_options(&v, &opts).unwrap();
    let dec = decode(&enc).unwrap();
    let node = dec.as_node().unwrap();
    assert!(!node.props.contains_key("drop"));
    assert_eq!(node.props.get("keep"), Some(&Value::Int(1)));
}

// ============================================================
// Decode error cases
// ============================================================

#[test]
fn decode_empty_data() {
    let result = decode(&[]);
    assert!(matches!(result, Err(CowrieError::Truncated)));
}

#[test]
fn decode_bad_magic() {
    let result = decode(&[0x00, 0x00, 0x02, 0x00, 0x00]);
    assert!(matches!(result, Err(CowrieError::InvalidMagic)));
}

#[test]
fn decode_bad_version() {
    let result = decode(b"SJ\xFF\x00\x00");
    assert!(matches!(result, Err(CowrieError::InvalidVersion(0xFF))));
}

#[test]
fn decode_truncated_after_header() {
    // Valid header but no data
    let result = decode(b"SJ\x02\x00");
    assert!(matches!(result, Err(CowrieError::Truncated)));
}

#[test]
fn decode_trailing_data() {
    // Encode a value, then append garbage
    let v = Value::Null;
    let mut enc = encode(&v).unwrap();
    enc.push(0xFF);
    let result = decode(&enc);
    assert!(matches!(result, Err(CowrieError::TrailingData { .. })));
}

#[test]
fn decode_with_custom_options() {
    let v = Value::Array((0..100).map(|i| Value::Int(i)).collect());
    let enc = encode(&v).unwrap();

    // Restrict array to 10 elements
    let opts = DecodeOptions {
        max_array_len: 10,
        ..Default::default()
    };
    let result = decode_with_options(&enc, &opts);
    assert!(matches!(result, Err(CowrieError::TooLarge)));
}

#[test]
fn decode_max_depth_exceeded() {
    // Build deeply nested arrays
    let mut v = Value::Int(1);
    for _ in 0..100 {
        v = Value::Array(vec![v]);
    }
    let enc = encode(&v).unwrap();

    let opts = DecodeOptions {
        max_depth: 10,
        ..Default::default()
    };
    let result = decode_with_options(&enc, &opts);
    assert!(matches!(result, Err(CowrieError::TooDeep)));
}

// ============================================================
// JSON bridge: from_json / to_json / to_json_pretty
// ============================================================

#[test]
fn json_null() {
    let v = from_json("null").unwrap();
    assert_eq!(v, Value::Null);
    let back = to_json(&v).unwrap();
    assert_eq!(back, "null");
}

#[test]
fn json_bool() {
    let v = from_json("true").unwrap();
    assert_eq!(v, Value::Bool(true));
    let v2 = from_json("false").unwrap();
    assert_eq!(v2, Value::Bool(false));
}

#[test]
fn json_int() {
    let v = from_json("42").unwrap();
    assert_eq!(v, Value::Int(42));
}

#[test]
fn json_negative_int() {
    let v = from_json("-7").unwrap();
    assert_eq!(v, Value::Int(-7));
}

#[test]
fn json_float() {
    let v = from_json("3.14").unwrap();
    assert!(matches!(v, Value::Float(f) if (f - 3.14).abs() < 1e-10));
}

#[test]
fn json_string() {
    let v = from_json(r#""hello""#).unwrap();
    assert_eq!(v, Value::String("hello".into()));
}

#[test]
fn json_array() {
    let v = from_json("[1,2,3]").unwrap();
    assert_eq!(v, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
}

#[test]
fn json_object() {
    let v = from_json(r#"{"a":1,"b":"c"}"#).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("a"), Some(&Value::Int(1)));
    assert_eq!(obj.get("b"), Some(&Value::String("c".into())));
}

#[test]
fn json_tensor_roundtrip() {
    let tensor = Value::Tensor(TensorData {
        dtype: DType::Float32,
        shape: vec![2, 3],
        data: vec![0u8; 24],
    });
    let json_str = to_json(&tensor).unwrap();
    let back = from_json(&json_str).unwrap();
    assert_eq!(back, tensor);
}

#[test]
fn json_tensor_all_dtypes() {
    let dtypes_str = [
        ("float32", DType::Float32), ("float64", DType::Float64),
        ("float16", DType::Float16), ("bfloat16", DType::BFloat16),
        ("int8", DType::Int8), ("int16", DType::Int16),
        ("int32", DType::Int32), ("int64", DType::Int64),
        ("uint8", DType::Uint8), ("uint16", DType::Uint16),
        ("uint32", DType::Uint32), ("uint64", DType::Uint64),
        ("bool", DType::Bool),
        ("qint4", DType::QINT4), ("qint2", DType::QINT2),
        ("qint3", DType::QINT3),
        ("ternary", DType::Ternary), ("binary", DType::Binary),
    ];
    for (name, dtype) in dtypes_str {
        let tensor = Value::Tensor(TensorData {
            dtype,
            shape: vec![1],
            data: vec![0u8; 4],
        });
        let json_str = to_json(&tensor).unwrap();
        assert!(json_str.contains(name), "JSON should contain dtype name {}", name);
        let back = from_json(&json_str).unwrap();
        assert_eq!(back, tensor, "roundtrip failed for {}", name);
    }
}

#[test]
fn json_bytes_roundtrip() {
    let v = Value::Bytes(vec![1, 2, 3, 4, 5]);
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"bytes\""));
    let back = from_json(&json_str).unwrap();
    assert_eq!(back, v);
}

#[test]
fn json_datetime_roundtrip() {
    let v = Value::DateTime(1234567890);
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"datetime\""));
    let back = from_json(&json_str).unwrap();
    assert_eq!(back, v);
}

#[test]
fn json_uuid_roundtrip() {
    let uuid = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let v = Value::Uuid(uuid);
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"uuid\""));
    let back = from_json(&json_str).unwrap();
    assert_eq!(back, v);
}

#[test]
fn json_decimal() {
    let v = Value::Decimal(vec![0u8; 17]);
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"decimal128\""));
}

#[test]
fn json_bigint() {
    let v = Value::BigInt(vec![1, 2, 3]);
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"bigint\""));
}

#[test]
fn json_tensor_ref() {
    let v = Value::TensorRef(TensorRef { store_id: 1, key: vec![0xAB] });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"tensor_ref\""));
}

#[test]
fn json_image() {
    let v = Value::Image(ImageData {
        format: ImageFormat::Png,
        width: 100,
        height: 200,
        data: vec![0xFF; 10],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"image\""));
    assert!(json_str.contains("\"format\":\"png\""));
}

#[test]
fn json_image_all_formats() {
    let formats = [
        (ImageFormat::Jpeg, "jpeg"),
        (ImageFormat::Png, "png"),
        (ImageFormat::Webp, "webp"),
        (ImageFormat::Avif, "avif"),
        (ImageFormat::Bmp, "bmp"),
    ];
    for (fmt, name) in formats {
        let v = Value::Image(ImageData {
            format: fmt, width: 1, height: 1, data: vec![],
        });
        let json_str = to_json(&v).unwrap();
        assert!(json_str.contains(name));
    }
}

#[test]
fn json_audio() {
    let v = Value::Audio(AudioData {
        encoding: AudioEncoding::Opus,
        sample_rate: 48000,
        channels: 1,
        data: vec![0; 50],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"audio\""));
    assert!(json_str.contains("\"encoding\":\"opus\""));
}

#[test]
fn json_audio_all_encodings() {
    let encodings = [
        (AudioEncoding::PcmInt16, "pcm_int16"),
        (AudioEncoding::PcmFloat32, "pcm_float32"),
        (AudioEncoding::Opus, "opus"),
        (AudioEncoding::Aac, "aac"),
    ];
    for (enc, name) in encodings {
        let v = Value::Audio(AudioData {
            encoding: enc, sample_rate: 44100, channels: 2, data: vec![],
        });
        let json_str = to_json(&v).unwrap();
        assert!(json_str.contains(name));
    }
}

#[test]
fn json_adjlist() {
    let v = Value::Adjlist(AdjlistData {
        id_width: 1, node_count: 2, edge_count: 1,
        row_offsets: vec![0, 1, 1],
        col_indices: vec![0; 4],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"adjlist\""));
}

#[test]
fn json_richtext() {
    let v = Value::RichText(RichTextData {
        text: "hello".into(),
        tokens: Some(vec![1, 2]),
        spans: Some(vec![RichTextSpan { start: 0, end: 5, kind_id: 1 }]),
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"richtext\""));
}

#[test]
fn json_delta() {
    let v = Value::Delta(DeltaData {
        base_id: 1,
        ops: vec![
            DeltaOp { op_code: DeltaOpCode::SetField, field_id: 0, value: Some(Box::new(Value::Int(42))) },
            DeltaOp { op_code: DeltaOpCode::DeleteField, field_id: 1, value: None },
        ],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"delta\""));
}

#[test]
fn json_ext() {
    let v = Value::Ext(ExtData { type_id: 99, payload: vec![1, 2, 3] });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"ext\""));
}

#[test]
fn json_bitmask() {
    let v = Value::Bitmask { count: 8, bits: vec![0xFF] };
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"bitmask\""));
}

#[test]
fn json_float_non_finite() {
    // Non-finite floats should become null in JSON
    let v = Value::Float(f64::INFINITY);
    let json_str = to_json(&v).unwrap();
    assert_eq!(json_str, "null");

    let v = Value::Float(f64::NAN);
    let json_str = to_json(&v).unwrap();
    assert_eq!(json_str, "null");
}

#[test]
fn json_pretty() {
    let v = Value::object(vec![("a", Value::Int(1))]);
    let pretty = to_json_pretty(&v).unwrap();
    assert!(pretty.contains('\n'));
}

#[test]
fn json_invalid_input() {
    let result = from_json("not json");
    assert!(result.is_err());
}

#[test]
fn json_unknown_type_passthrough() {
    // An object with _type that isn't a recognized special type
    let json = r#"{"_type":"unknown","data":123}"#;
    let v = from_json(json).unwrap();
    // Should be parsed as a regular object
    assert!(v.as_object().is_some());
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("_type"), Some(&Value::String("unknown".into())));
}

#[test]
fn json_bytes_missing_data() {
    let json = r#"{"_type":"bytes"}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_tensor_missing_dtype() {
    let json = r#"{"_type":"tensor","dims":[1],"data":"AA=="}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_tensor_missing_dims() {
    let json = r#"{"_type":"tensor","dtype":"float32","data":"AA=="}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_tensor_missing_data() {
    let json = r#"{"_type":"tensor","dtype":"float32","dims":[1]}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_tensor_unknown_dtype() {
    let json = r#"{"_type":"tensor","dtype":"complex128","dims":[1],"data":"AA=="}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_datetime_missing_nanos() {
    let json = r#"{"_type":"datetime"}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_uuid_missing_hex() {
    let json = r#"{"_type":"uuid"}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

#[test]
fn json_uuid_invalid_hex_length() {
    let json = r#"{"_type":"uuid","hex":"1234"}"#;
    let result = from_json(json);
    assert!(result.is_err());
}

// Graph types → JSON
#[test]
fn json_node() {
    let mut props = BTreeMap::new();
    props.insert("score".to_string(), Value::Float(0.9));
    let v = Value::Node(NodeData {
        id: "n1".into(),
        labels: vec!["Person".into()],
        props,
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"node\""));
    assert!(json_str.contains("\"id\":\"n1\""));
}

#[test]
fn json_edge() {
    let v = Value::Edge(EdgeData {
        from: "a".into(),
        to: "b".into(),
        edge_type: "KNOWS".into(),
        props: BTreeMap::new(),
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"edge\""));
}

#[test]
fn json_node_batch() {
    let v = Value::NodeBatch(NodeBatchData {
        nodes: vec![NodeData::new("n1", vec![], BTreeMap::new())],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"node_batch\""));
}

#[test]
fn json_edge_batch() {
    let v = Value::EdgeBatch(EdgeBatchData {
        edges: vec![EdgeData::new("a", "b", "R", BTreeMap::new())],
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"edge_batch\""));
}

#[test]
fn json_graph_shard() {
    let v = Value::GraphShard(GraphShardData {
        nodes: vec![NodeData::new("n1", vec![], BTreeMap::new())],
        edges: vec![EdgeData::new("n1", "n1", "SELF", BTreeMap::new())],
        metadata: {
            let mut m = BTreeMap::new();
            m.insert("v".into(), Value::Int(1));
            m
        },
    });
    let json_str = to_json(&v).unwrap();
    assert!(json_str.contains("\"_type\":\"graph_shard\""));
}

// ============================================================
// Schema fingerprint tests
// ============================================================

#[test]
fn schema_scalar_types() {
    let types: Vec<Value> = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int(1),
        Value::Uint(1),
        Value::Float(1.0),
        Value::String("x".into()),
        Value::Bytes(vec![1]),
        Value::Decimal(vec![0; 17]),
        Value::DateTime(0),
        Value::Uuid([0; 16]),
        Value::BigInt(vec![1]),
    ];
    // All should produce different fingerprints
    let fps: Vec<u64> = types.iter().map(|v| schema_fingerprint64(v)).collect();
    for i in 0..fps.len() {
        for j in (i+1)..fps.len() {
            assert_ne!(fps[i], fps[j], "types {} and {} have same fingerprint", i, j);
        }
    }
}

#[test]
fn schema_tensor_includes_dtype() {
    let t1 = Value::Tensor(TensorData { dtype: DType::Float32, shape: vec![3], data: vec![0; 12] });
    let t2 = Value::Tensor(TensorData { dtype: DType::Float64, shape: vec![3], data: vec![0; 24] });
    assert_ne!(schema_fingerprint64(&t1), schema_fingerprint64(&t2));
}

#[test]
fn schema_tensor_includes_rank() {
    let t1 = Value::Tensor(TensorData { dtype: DType::Float32, shape: vec![6], data: vec![0; 24] });
    let t2 = Value::Tensor(TensorData { dtype: DType::Float32, shape: vec![2, 3], data: vec![0; 24] });
    assert_ne!(schema_fingerprint64(&t1), schema_fingerprint64(&t2));
}

#[test]
fn schema_image_includes_format() {
    let i1 = Value::Image(ImageData { format: ImageFormat::Jpeg, width: 1, height: 1, data: vec![] });
    let i2 = Value::Image(ImageData { format: ImageFormat::Png, width: 1, height: 1, data: vec![] });
    assert_ne!(schema_fingerprint64(&i1), schema_fingerprint64(&i2));
}

#[test]
fn schema_audio_includes_encoding_channels() {
    let a1 = Value::Audio(AudioData { encoding: AudioEncoding::Opus, sample_rate: 48000, channels: 1, data: vec![] });
    let a2 = Value::Audio(AudioData { encoding: AudioEncoding::Opus, sample_rate: 48000, channels: 2, data: vec![] });
    assert_ne!(schema_fingerprint64(&a1), schema_fingerprint64(&a2));
}

#[test]
fn schema_adjlist() {
    let v = Value::Adjlist(AdjlistData { id_width: 1, node_count: 0, edge_count: 0, row_offsets: vec![0], col_indices: vec![] });
    let fp = schema_fingerprint64(&v);
    assert_ne!(fp, 0);
}

#[test]
fn schema_richtext() {
    let v = Value::RichText(RichTextData { text: "hi".into(), tokens: None, spans: None });
    let fp = schema_fingerprint64(&v);
    assert_ne!(fp, 0);
}

#[test]
fn schema_delta() {
    let v = Value::Delta(DeltaData { base_id: 0, ops: vec![] });
    let fp = schema_fingerprint64(&v);
    assert_ne!(fp, 0);
}

#[test]
fn schema_ext_includes_type_id() {
    let e1 = Value::Ext(ExtData { type_id: 1, payload: vec![] });
    let e2 = Value::Ext(ExtData { type_id: 2, payload: vec![] });
    assert_ne!(schema_fingerprint64(&e1), schema_fingerprint64(&e2));
}

#[test]
fn schema_bitmask() {
    let b1 = Value::Bitmask { count: 8, bits: vec![0xFF] };
    let b2 = Value::Bitmask { count: 16, bits: vec![0xFF, 0xFF] };
    assert_ne!(schema_fingerprint64(&b1), schema_fingerprint64(&b2));
}

#[test]
fn schema_graph_types() {
    let node = Value::Node(NodeData::new("n", vec!["L".into()], BTreeMap::new()));
    let edge = Value::Edge(EdgeData::new("a", "b", "R", BTreeMap::new()));
    let nb = Value::NodeBatch(NodeBatchData { nodes: vec![NodeData::new("n", vec![], BTreeMap::new())] });
    let eb = Value::EdgeBatch(EdgeBatchData { edges: vec![EdgeData::new("a", "b", "R", BTreeMap::new())] });
    let gs = Value::GraphShard(GraphShardData { nodes: vec![], edges: vec![], metadata: BTreeMap::new() });

    let fps = [
        schema_fingerprint64(&node),
        schema_fingerprint64(&edge),
        schema_fingerprint64(&nb),
        schema_fingerprint64(&eb),
        schema_fingerprint64(&gs),
    ];
    for i in 0..fps.len() {
        for j in (i+1)..fps.len() {
            assert_ne!(fps[i], fps[j], "graph types {} and {} collide", i, j);
        }
    }
}

#[test]
fn schema_descriptor_all() {
    use cowrie_rs::gen2::schema::schema_descriptor;

    assert_eq!(schema_descriptor(&Value::Null), "null");
    assert_eq!(schema_descriptor(&Value::Bool(true)), "bool");
    assert_eq!(schema_descriptor(&Value::Int(0)), "int64");
    assert_eq!(schema_descriptor(&Value::Uint(0)), "uint64");
    assert_eq!(schema_descriptor(&Value::Float(0.0)), "float64");
    assert_eq!(schema_descriptor(&Value::String("x".into())), "string");
    assert_eq!(schema_descriptor(&Value::Bytes(vec![])), "bytes");
    assert_eq!(schema_descriptor(&Value::Decimal(vec![0;17])), "decimal128");
    assert_eq!(schema_descriptor(&Value::DateTime(0)), "datetime64");
    assert_eq!(schema_descriptor(&Value::Uuid([0;16])), "uuid128");
    assert_eq!(schema_descriptor(&Value::BigInt(vec![])), "bigint");
    assert_eq!(schema_descriptor(&Value::Array(vec![])), "[]");
    assert_eq!(schema_descriptor(&Value::Array(vec![Value::Int(1)])), "[int64,...]");
    assert_eq!(schema_descriptor(&Value::Object(BTreeMap::new())), "{}");
    let v = Value::object(vec![("abc", Value::Int(1))]);
    assert_eq!(schema_descriptor(&v), "{abc,...}");
    assert!(schema_descriptor(&Value::Tensor(TensorData { dtype: DType::Float32, shape: vec![1], data: vec![0;4] })).starts_with("tensor<"));
    assert_eq!(schema_descriptor(&Value::TensorRef(TensorRef { store_id: 0, key: vec![] })), "tensor_ref");
    assert_eq!(schema_descriptor(&Value::Image(ImageData { format: ImageFormat::Jpeg, width: 1, height: 1, data: vec![] })), "image");
    assert_eq!(schema_descriptor(&Value::Audio(AudioData { encoding: AudioEncoding::Opus, sample_rate: 44100, channels: 1, data: vec![] })), "audio");
    assert_eq!(schema_descriptor(&Value::Adjlist(AdjlistData { id_width: 1, node_count: 0, edge_count: 0, row_offsets: vec![0], col_indices: vec![] })), "adjlist");
    assert_eq!(schema_descriptor(&Value::RichText(RichTextData { text: "".into(), tokens: None, spans: None })), "richtext");
    assert_eq!(schema_descriptor(&Value::Delta(DeltaData { base_id: 0, ops: vec![] })), "delta");
    assert_eq!(schema_descriptor(&Value::Ext(ExtData { type_id: 0, payload: vec![] })), "ext");
    assert_eq!(schema_descriptor(&Value::Node(NodeData::new("n", vec![], BTreeMap::new()))), "node");
    assert_eq!(schema_descriptor(&Value::Edge(EdgeData::new("a", "b", "R", BTreeMap::new()))), "edge");
    assert_eq!(schema_descriptor(&Value::NodeBatch(NodeBatchData { nodes: vec![] })), "node_batch");
    assert_eq!(schema_descriptor(&Value::EdgeBatch(EdgeBatchData { edges: vec![] })), "edge_batch");
    assert_eq!(schema_descriptor(&Value::GraphShard(GraphShardData { nodes: vec![], edges: vec![], metadata: BTreeMap::new() })), "graph_shard");
    assert_eq!(schema_descriptor(&Value::Bitmask { count: 1, bits: vec![0] }), "bitmask");
}

// ============================================================
// Compression tests
// ============================================================

#[test]
fn compress_gzip_roundtrip() {
    let v = Value::object(vec![
        ("data", Value::String("x".repeat(500))),
        ("num", Value::Int(42)),
    ]);
    let enc = encode_framed(&v, Compression::Gzip).unwrap();
    let dec = decode_framed(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn compress_none_roundtrip() {
    let v = Value::Int(42);
    let enc = encode_framed(&v, Compression::None).unwrap();
    let dec = decode_framed(&enc).unwrap();
    assert_eq!(dec, v);
}

#[test]
fn compress_zstd_not_enabled() {
    let v = Value::Int(1);
    let result = encode_framed(&v, Compression::Zstd);
    // Without feature, should fail
    assert!(result.is_err());
}

#[test]
fn decode_framed_bad_magic() {
    let result = decode_framed(&[0x00, 0x00, 0x00, 0x00]);
    assert!(result.is_err());
}

#[test]
fn decode_framed_truncated() {
    let result = decode_framed(&[0x53, 0x4A]); // "SJ" only
    assert!(result.is_err());
}

// ============================================================
// MasterStream tests
// ============================================================

#[test]
fn master_stream_no_crc() {
    let v = Value::object(vec![("x", Value::Int(1))]);
    let mut buf = Vec::new();
    let opts = MasterWriterOptions {
        enable_crc: false,
        deterministic: false,
        compress: false,
    };
    write_frame(&mut buf, &v, None, &opts).unwrap();
    let (frame, _) = read_frame(&buf).unwrap();
    assert_eq!(frame.payload.get("x").unwrap().as_i64(), Some(1));
}

#[test]
fn master_stream_is_checks() {
    use cowrie_rs::gen2::master_stream::{is_master_stream, is_cowrie_document};

    assert!(is_master_stream(b"SJST\x02\x00"));
    assert!(!is_master_stream(b"SJ\x02\x00"));
    assert!(!is_master_stream(b"XX"));
    assert!(!is_master_stream(b""));

    assert!(is_cowrie_document(b"SJ\x02\x00"));
    assert!(!is_cowrie_document(b"SJST"));
    assert!(!is_cowrie_document(b""));
    assert!(!is_cowrie_document(b"XX\x02\x00"));
}

#[test]
fn master_stream_truncated() {
    let result = read_frame(&[0u8; 10]);
    assert!(matches!(result, Err(_)));
}

// ============================================================
// Value helper methods / From impls
// ============================================================

#[test]
fn value_is_null() {
    assert!(Value::Null.is_null());
    assert!(!Value::Bool(false).is_null());
}

#[test]
fn value_as_bool() {
    assert_eq!(Value::Bool(true).as_bool(), Some(true));
    assert_eq!(Value::Bool(false).as_bool(), Some(false));
    assert_eq!(Value::Null.as_bool(), None);
}

#[test]
fn value_as_i64() {
    assert_eq!(Value::Int(42).as_i64(), Some(42));
    assert_eq!(Value::Null.as_i64(), None);
}

#[test]
fn value_as_u64() {
    assert_eq!(Value::Uint(99).as_u64(), Some(99));
    assert_eq!(Value::Int(10).as_u64(), Some(10));
    assert_eq!(Value::Int(-1).as_u64(), None);
    assert_eq!(Value::Null.as_u64(), None);
}

#[test]
fn value_as_f64() {
    assert_eq!(Value::Float(1.5).as_f64(), Some(1.5));
    assert_eq!(Value::Null.as_f64(), None);
}

#[test]
fn value_as_str() {
    assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
    assert_eq!(Value::Null.as_str(), None);
}

#[test]
fn value_as_bytes() {
    assert_eq!(Value::Bytes(vec![1,2]).as_bytes(), Some(&[1u8, 2][..]));
    assert_eq!(Value::Null.as_bytes(), None);
}

#[test]
fn value_as_array() {
    let arr = vec![Value::Int(1)];
    assert_eq!(Value::Array(arr.clone()).as_array(), Some(&arr[..]));
    assert_eq!(Value::Null.as_array(), None);
}

#[test]
fn value_as_object() {
    let v = Value::object(vec![("a", Value::Int(1))]);
    assert!(v.as_object().is_some());
    assert!(Value::Null.as_object().is_none());
}

#[test]
fn value_get() {
    let v = Value::object(vec![("a", Value::Int(1))]);
    assert_eq!(v.get("a"), Some(&Value::Int(1)));
    assert_eq!(v.get("b"), None);
    assert_eq!(Value::Null.get("a"), None);
}

#[test]
fn value_as_tensor() {
    let t = TensorData { dtype: DType::Float32, shape: vec![1], data: vec![0;4] };
    assert!(Value::Tensor(t.clone()).as_tensor().is_some());
    assert!(Value::Null.as_tensor().is_none());
}

#[test]
fn value_as_node() {
    let n = NodeData::new("n", vec![], BTreeMap::new());
    assert!(Value::Node(n).as_node().is_some());
    assert!(Value::Null.as_node().is_none());
}

#[test]
fn value_as_edge() {
    let e = EdgeData::new("a", "b", "R", BTreeMap::new());
    assert!(Value::Edge(e).as_edge().is_some());
    assert!(Value::Null.as_edge().is_none());
}

#[test]
fn value_as_node_batch() {
    let v = Value::NodeBatch(NodeBatchData { nodes: vec![] });
    assert!(v.as_node_batch().is_some());
    assert!(Value::Null.as_node_batch().is_none());
}

#[test]
fn value_as_edge_batch() {
    let v = Value::EdgeBatch(EdgeBatchData { edges: vec![] });
    assert!(v.as_edge_batch().is_some());
    assert!(Value::Null.as_edge_batch().is_none());
}

#[test]
fn value_as_graph_shard() {
    let v = Value::GraphShard(GraphShardData { nodes: vec![], edges: vec![], metadata: BTreeMap::new() });
    assert!(v.as_graph_shard().is_some());
    assert!(Value::Null.as_graph_shard().is_none());
}

#[test]
fn value_from_bool() {
    let v: Value = true.into();
    assert_eq!(v, Value::Bool(true));
}

#[test]
fn value_from_i64() {
    let v: Value = 42i64.into();
    assert_eq!(v, Value::Int(42));
}

#[test]
fn value_from_i32() {
    let v: Value = 42i32.into();
    assert_eq!(v, Value::Int(42));
}

#[test]
fn value_from_u64() {
    let v: Value = 42u64.into();
    assert_eq!(v, Value::Uint(42));
}

#[test]
fn value_from_f64() {
    let v: Value = 1.5f64.into();
    assert_eq!(v, Value::Float(1.5));
}

#[test]
fn value_from_str() {
    let v: Value = "hello".into();
    assert_eq!(v, Value::String("hello".to_string()));
}

#[test]
fn value_from_string() {
    let v: Value = String::from("hello").into();
    assert_eq!(v, Value::String("hello".to_string()));
}

#[test]
fn value_from_vec_u8() {
    let v: Value = vec![1u8, 2, 3].into();
    assert_eq!(v, Value::Bytes(vec![1, 2, 3]));
}

// ============================================================
// TensorData view/copy methods
// ============================================================

#[test]
fn tensor_view_float32_empty() {
    let t = TensorData::new(DType::Float32, vec![0], vec![]);
    assert_eq!(t.view_float32(), Some(&[][..]));
}

#[test]
fn tensor_view_float32_bad_len() {
    let t = TensorData::new(DType::Float32, vec![1], vec![0, 0, 0]); // 3 bytes, not divisible by 4
    assert!(t.view_float32().is_none());
}

#[test]
fn tensor_view_float64_empty() {
    let t = TensorData::new(DType::Float64, vec![0], vec![]);
    assert_eq!(t.view_float64(), Some(&[][..]));
}

#[test]
fn tensor_view_float64_bad_len() {
    let t = TensorData::new(DType::Float64, vec![1], vec![0; 5]); // 5 bytes, not divisible by 8
    assert!(t.view_float64().is_none());
}

#[test]
fn tensor_view_float64_wrong_dtype() {
    let t = TensorData::new(DType::Float32, vec![2], vec![0; 16]);
    assert!(t.view_float64().is_none());
}

#[test]
fn tensor_copy_float64() {
    let data: Vec<u8> = 1.0f64.to_le_bytes().iter()
        .chain(2.0f64.to_le_bytes().iter())
        .copied()
        .collect();
    let t = TensorData::new(DType::Float64, vec![2], data);
    let copied = t.copy_float64().unwrap();
    assert_eq!(copied, vec![1.0, 2.0]);
}

#[test]
fn tensor_copy_float64_wrong_dtype() {
    let t = TensorData::new(DType::Float32, vec![1], vec![0; 4]);
    assert!(t.copy_float64().is_none());
}

#[test]
fn tensor_copy_float32_wrong_dtype() {
    let t = TensorData::new(DType::Float64, vec![1], vec![0; 8]);
    assert!(t.copy_float32().is_none());
}

#[test]
fn tensor_float32_slice() {
    let data: Vec<u8> = 1.0f32.to_le_bytes().iter()
        .chain(2.0f32.to_le_bytes().iter())
        .copied()
        .collect();
    let t = TensorData::new(DType::Float32, vec![2], data);
    let s = t.float32_slice().unwrap();
    assert!((s[0] - 1.0).abs() < 1e-6);
    assert!((s[1] - 2.0).abs() < 1e-6);
}

#[test]
fn tensor_float64_slice() {
    let data: Vec<u8> = 3.14f64.to_le_bytes().to_vec();
    let t = TensorData::new(DType::Float64, vec![1], data);
    let s = t.float64_slice().unwrap();
    assert!((s[0] - 3.14).abs() < 1e-10);
}

#[test]
fn tensor_view_int32_empty() {
    let t = TensorData::new(DType::Int32, vec![0], vec![]);
    assert_eq!(t.view_int32(), Some(&[][..]));
}

#[test]
fn tensor_view_int32_bad_len() {
    let t = TensorData::new(DType::Int32, vec![1], vec![0; 3]);
    assert!(t.view_int32().is_none());
}

#[test]
fn tensor_view_int64_empty() {
    let t = TensorData::new(DType::Int64, vec![0], vec![]);
    assert_eq!(t.view_int64(), Some(&[][..]));
}

#[test]
fn tensor_view_int64_bad_len() {
    let t = TensorData::new(DType::Int64, vec![1], vec![0; 5]);
    assert!(t.view_int64().is_none());
}

#[test]
fn tensor_view_int64_wrong_dtype() {
    let t = TensorData::new(DType::Float32, vec![2], vec![0; 16]);
    assert!(t.view_int64().is_none());
}

// ============================================================
// DType TryFrom<u8>
// ============================================================

#[test]
fn dtype_try_from_all_valid() {
    let pairs = [
        (0x01, DType::Float32), (0x02, DType::Float16), (0x03, DType::BFloat16),
        (0x04, DType::Int8), (0x05, DType::Int16), (0x06, DType::Int32),
        (0x07, DType::Int64), (0x08, DType::Uint8), (0x09, DType::Uint16),
        (0x0A, DType::Uint32), (0x0B, DType::Uint64), (0x0C, DType::Float64),
        (0x0D, DType::Bool),
        (0x10, DType::QINT4), (0x11, DType::QINT2), (0x12, DType::QINT3),
        (0x13, DType::Ternary), (0x14, DType::Binary),
    ];
    for (byte, expected) in pairs {
        assert_eq!(DType::try_from(byte).unwrap(), expected);
    }
}

#[test]
fn dtype_try_from_invalid() {
    assert!(DType::try_from(0x00).is_err());
    assert!(DType::try_from(0x0E).is_err());
    assert!(DType::try_from(0x0F).is_err());
    assert!(DType::try_from(0x15).is_err());
    assert!(DType::try_from(0xFF).is_err());
}

// ============================================================
// CowrieError Display
// ============================================================

#[test]
fn cowrie_error_display() {
    let errors: Vec<CowrieError> = vec![
        CowrieError::InvalidMagic,
        CowrieError::InvalidVersion(0xFF),
        CowrieError::InvalidTag(0xAB),
        CowrieError::InvalidData("test".into()),
        CowrieError::Truncated,
        CowrieError::InvalidUtf8,
        CowrieError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test")),
        CowrieError::TooDeep,
        CowrieError::TooLarge,
        CowrieError::TrailingData { pos: 10, remaining: 5 },
        CowrieError::InvalidDictIndex { index: 5, dict_len: 3 },
        CowrieError::RankExceeded { rank: 64, max: 32 },
    ];
    for err in errors {
        let s = format!("{}", err);
        assert!(!s.is_empty(), "Display should produce non-empty string");
    }
}

#[test]
fn cowrie_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test");
    let cowrie_err: CowrieError = io_err.into();
    let s = format!("{}", cowrie_err);
    assert!(s.contains("test"));
}

// ============================================================
// Gen1 tests
// ============================================================

#[test]
fn gen1_roundtrip_all_scalars() {
    use cowrie_rs::gen1::{encode, decode, Value};

    let values = vec![
        Value::Null,
        Value::Bool(true),
        Value::Bool(false),
        Value::Int64(0),
        Value::Int64(42),
        Value::Int64(-100),
        Value::Int64(i64::MAX),
        Value::Int64(i64::MIN),
        Value::Float64(0.0),
        Value::Float64(3.14),
        Value::Float64(-2.5),
        Value::String("".to_string()),
        Value::String("hello world".to_string()),
        Value::String("\u{1F600}".to_string()),
        Value::Bytes(vec![]),
        Value::Bytes(vec![1, 2, 3, 255]),
    ];

    for val in values {
        let enc = encode(&val).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(val, dec, "Gen1 roundtrip failed for {:?}", val);
    }
}

#[test]
fn gen1_roundtrip_arrays() {
    use cowrie_rs::gen1::{encode, decode, Value};

    let vals = vec![
        Value::Array(vec![Value::Int64(1), Value::String("two".into()), Value::Bool(false)]),
        Value::Array(vec![]),
        Value::Array(vec![Value::Array(vec![Value::Null])]),
    ];

    for val in vals {
        let enc = encode(&val).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(val, dec);
    }
}

#[test]
fn gen1_roundtrip_nested_object() {
    use cowrie_rs::gen1::{encode, decode, Value};

    let val = Value::Object(vec![
        ("outer".into(), Value::Object(vec![
            ("inner".into(), Value::Int64(42)),
        ])),
    ]);
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_proto_tensors() {
    use cowrie_rs::gen1::{encode, decode, Value};

    let vals = vec![
        Value::Int64Array(vec![]),
        Value::Int64Array(vec![1, -2, 3, i64::MAX, i64::MIN]),
        Value::Float64Array(vec![]),
        Value::Float64Array(vec![1.0, -2.5, 3.14, f64::MAX]),
        Value::StringArray(vec![]),
        Value::StringArray(vec!["a".into(), "bb".into(), "ccc".into()]),
    ];

    for val in vals {
        let enc = encode(&val).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(val, dec, "Gen1 proto-tensor roundtrip failed for {:?}", val);
    }
}

#[test]
fn gen1_roundtrip_node() {
    use cowrie_rs::gen1::{encode, decode, Value};
    use std::collections::HashMap;

    let mut props = HashMap::new();
    props.insert("weight".to_string(), Value::Float64(1.5));
    let val = Value::Node { id: 42, label: "Person".into(), properties: props };
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_edge() {
    use cowrie_rs::gen1::{encode, decode, Value};
    use std::collections::HashMap;

    let mut props = HashMap::new();
    props.insert("since".to_string(), Value::Int64(2020));
    let val = Value::Edge { src: 1, dst: 2, label: "KNOWS".into(), properties: props };
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_adjlist() {
    use cowrie_rs::gen1::{encode, decode, Value};

    let val = Value::AdjList {
        id_width: 1,
        node_count: 3,
        edge_count: 2,
        row_offsets: vec![0, 1, 1, 2],
        col_indices: vec![0u8; 8],
    };
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_node_batch() {
    use cowrie_rs::gen1::{encode, decode, Value};
    use std::collections::HashMap;

    let node1 = Value::Node { id: 1, label: "A".into(), properties: HashMap::new() };
    let node2 = Value::Node { id: 2, label: "B".into(), properties: HashMap::new() };
    let val = Value::NodeBatch(vec![node1, node2]);
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_edge_batch() {
    use cowrie_rs::gen1::{encode, decode, Value};
    use std::collections::HashMap;

    let e1 = Value::Edge { src: 1, dst: 2, label: "R".into(), properties: HashMap::new() };
    let val = Value::EdgeBatch(vec![e1]);
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_roundtrip_graph_shard() {
    use cowrie_rs::gen1::{encode, decode, Value};
    use std::collections::HashMap;

    let n = Value::Node { id: 1, label: "N".into(), properties: HashMap::new() };
    let e = Value::Edge { src: 1, dst: 1, label: "SELF".into(), properties: HashMap::new() };
    let mut meta = HashMap::new();
    meta.insert("version".to_string(), Value::Int64(1));
    let val = Value::GraphShard { nodes: vec![n], edges: vec![e], meta };
    let enc = encode(&val).unwrap();
    let dec = decode(&enc).unwrap();
    assert_eq!(val, dec);
}

#[test]
fn gen1_error_invalid_tag() {
    use cowrie_rs::gen1::decode;
    // Tag 0xFF is not valid in Gen1
    let result = decode(&[0xFF]);
    assert!(result.is_err());
}

#[test]
fn gen1_error_unexpected_eof() {
    use cowrie_rs::gen1::decode;
    let result = decode(&[]);
    assert!(result.is_err());
}

#[test]
fn gen1_error_display() {
    use cowrie_rs::gen1::Gen1Error;

    let errors = vec![
        Gen1Error::InvalidTag(0xFF),
        Gen1Error::UnexpectedEof,
        Gen1Error::InvalidUtf8,
        Gen1Error::IoError("test".into()),
        Gen1Error::MaxDepthExceeded,
        Gen1Error::MaxArrayLen,
        Gen1Error::MaxObjectLen,
        Gen1Error::MaxStringLen,
        Gen1Error::MaxBytesLen,
    ];
    for err in errors {
        let s = format!("{}", err);
        assert!(!s.is_empty());
    }
}

#[test]
fn gen1_error_from_io() {
    use cowrie_rs::gen1::Gen1Error;
    let io_err = std::io::Error::new(std::io::ErrorKind::Other, "boom");
    let gen1_err: Gen1Error = io_err.into();
    let s = format!("{}", gen1_err);
    assert!(s.contains("boom"));
}

// ============================================================
// Invariant #2: encode(decode(bytes)) == bytes (canonical roundtrip)
// ============================================================

#[test]
fn test_encode_decode_encode_canonical() {
    let test_values = vec![
        // scalar int
        Value::Int(42),
        // string
        Value::String("hello".into()),
        // array of ints
        Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        // map with string keys
        Value::Object({
            let mut m = BTreeMap::new();
            m.insert("a".into(), Value::Int(1));
            m.insert("b".into(), Value::Int(2));
            m
        }),
        // nested map-of-arrays
        Value::Object({
            let mut m = BTreeMap::new();
            m.insert("x".into(), Value::Array(vec![Value::Int(10), Value::Int(20)]));
            m.insert("y".into(), Value::Array(vec![
                Value::String("nested".into()),
                Value::String("map".into()),
            ]));
            m
        }),
    ];

    for val in &test_values {
        let bytes1 = encode(val).expect("encode should succeed");
        let decoded = decode(&bytes1).expect("decode should succeed");
        let bytes2 = encode(&decoded).expect("re-encode should succeed");
        assert_eq!(
            bytes1, bytes2,
            "Canonical roundtrip failed: encode(decode(bytes)) != bytes for {:?}",
            val
        );
    }
}

/// Invariant #4: Trailing garbage after a valid root value must be rejected.
#[test]
fn test_decode_rejects_trailing_garbage() {
    let v = Value::object(vec![("a", Value::Int(42))]);
    let mut data = encode(&v).expect("encode should succeed");
    data.push(0xFF);
    let result = decode(&data);
    assert!(result.is_err(), "expected error for trailing garbage, got Ok");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("trailing"),
        "expected 'trailing' in error message, got: {}",
        err_msg
    );
}

// ============================================================
// NaN/Inf binary roundtrip
// ============================================================

#[test]
fn test_binary_nan_inf_roundtrip() {
    // NaN must roundtrip
    let nan_val = Value::Float(f64::NAN);
    let enc = encode(&nan_val).unwrap();
    let dec = decode(&enc).unwrap();
    match dec {
        Value::Float(f) => assert!(f.is_nan(), "expected NaN, got {}", f),
        other => panic!("expected Float, got {:?}", other),
    }

    // +Infinity must roundtrip
    let inf_val = Value::Float(f64::INFINITY);
    let enc = encode(&inf_val).unwrap();
    let dec = decode(&enc).unwrap();
    match dec {
        Value::Float(f) => assert!(
            f.is_infinite() && f.is_sign_positive(),
            "expected +Infinity, got {}",
            f
        ),
        other => panic!("expected Float, got {:?}", other),
    }

    // -Infinity must roundtrip
    let neg_inf_val = Value::Float(f64::NEG_INFINITY);
    let enc = encode(&neg_inf_val).unwrap();
    let dec = decode(&enc).unwrap();
    match dec {
        Value::Float(f) => assert!(
            f.is_infinite() && f.is_sign_negative(),
            "expected -Infinity, got {}",
            f
        ),
        other => panic!("expected Float, got {:?}", other),
    }
}
