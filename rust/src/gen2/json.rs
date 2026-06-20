//! JSON bridge for Cowrie Gen2.
//!
//! Provides conversion between JSON strings and Cowrie Values.

use super::types::{CowrieError, DType, TensorData, Value};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::collections::BTreeMap;

/// Parse a JSON string into an Cowrie Value.
pub fn from_json(json: &str) -> Result<Value, CowrieError> {
    let parsed: serde_json::Value =
        serde_json::from_str(json).map_err(|e| CowrieError::InvalidData(e.to_string()))?;
    json_to_value(&parsed)
}

/// Convert an Cowrie Value to a JSON string.
pub fn to_json(value: &Value) -> Result<String, CowrieError> {
    let json = value_to_json(value)?;
    serde_json::to_string(&json).map_err(|e| CowrieError::InvalidData(e.to_string()))
}

/// Convert an Cowrie Value to a pretty-printed JSON string.
pub fn to_json_pretty(value: &Value) -> Result<String, CowrieError> {
    let json = value_to_json(value)?;
    serde_json::to_string_pretty(&json).map_err(|e| CowrieError::InvalidData(e.to_string()))
}

fn json_to_value(json: &serde_json::Value) -> Result<Value, CowrieError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::Uint(u))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(CowrieError::InvalidData("invalid number".into()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        serde_json::Value::Array(arr) => {
            let items: Result<Vec<Value>, CowrieError> = arr.iter().map(json_to_value).collect();
            Ok(Value::Array(items?))
        }
        serde_json::Value::Object(obj) => {
            // Check for special _type field for extension types
            if let Some(serde_json::Value::String(type_name)) = obj.get("_type") {
                match type_name.as_str() {
                    "tensor" => return parse_tensor_from_json(obj),
                    "tensor_ref" => return parse_tensor_ref_from_json(obj),
                    "image" => return parse_image_from_json(obj),
                    "audio" => return parse_audio_from_json(obj),
                    "unknown_ext" => return parse_unknown_ext_from_json(obj),
                    "node" => return parse_node_from_json(obj),
                    "edge" => return parse_edge_from_json(obj),
                    "node_batch" => return parse_node_batch_from_json(obj),
                    "edge_batch" => return parse_edge_batch_from_json(obj),
                    "bitmask" => return parse_bitmask_from_json(obj),
                    "bytes" => return parse_bytes_from_json(obj),
                    "datetime" => return parse_datetime_from_json(obj),
                    "uuid" => return parse_uuid_from_json(obj),
                    _ => {} // Fall through to regular object
                }
            }

            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_value(v)?);
            }
            Ok(Value::Object(map))
        }
    }
}

fn parse_tensor_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let dtype_str = obj
        .get("dtype")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("tensor missing dtype".into()))?;

    let dtype = match dtype_str {
        "float32" => DType::Float32,
        "float64" => DType::Float64,
        "int8" => DType::Int8,
        "int16" => DType::Int16,
        "int32" => DType::Int32,
        "int64" => DType::Int64,
        "uint8" => DType::Uint8,
        "uint16" => DType::Uint16,
        "uint32" => DType::Uint32,
        "uint64" => DType::Uint64,
        "bfloat16" => DType::BFloat16,
        "float16" => DType::Float16,
        "bool" => DType::Bool,
        "qint4" => DType::QINT4,
        "qint2" => DType::QINT2,
        "qint3" => DType::QINT3,
        "ternary" => DType::Ternary,
        "binary" => DType::Binary,
        _ => {
            return Err(CowrieError::InvalidData(format!(
                "unknown dtype: {}",
                dtype_str
            )))
        }
    };

    let dims: Vec<u64> = obj
        .get("dims")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CowrieError::InvalidData("tensor missing dims".into()))?
        .iter()
        .filter_map(|v| v.as_u64())
        .collect();

    let data_b64 = obj
        .get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("tensor missing data".into()))?;

    let data = BASE64
        .decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Tensor(TensorData {
        dtype,
        shape: dims,
        data,
    }))
}

fn parse_bytes_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let data_b64 = obj
        .get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("bytes missing data".into()))?;

    let data = BASE64
        .decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Bytes(data))
}

fn parse_datetime_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let nanos = obj
        .get("nanos")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| CowrieError::InvalidData("datetime missing nanos".into()))?;

    Ok(Value::DateTime(nanos))
}

fn parse_uuid_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let hex = obj
        .get("hex")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("uuid missing hex".into()))?;

    let clean: String = hex
        .chars()
        .filter(|c: &char| c.is_ascii_hexdigit())
        .collect();
    if clean.len() != 32 {
        return Err(CowrieError::InvalidData("invalid uuid hex".into()));
    }

    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16)
            .map_err(|_| CowrieError::InvalidData("invalid uuid hex".into()))?;
    }

    Ok(Value::Uuid(bytes))
}

fn parse_tensor_ref_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let store_id = obj
        .get("store")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CowrieError::InvalidData("tensor_ref missing store".into()))?
        as u8;

    let key_b64 = obj
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("tensor_ref missing key".into()))?;

    let key = BASE64
        .decode(key_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::TensorRef(super::types::TensorRef { store_id, key }))
}

fn parse_image_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let format_str = obj
        .get("format")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("image missing format".into()))?;

    let format = match format_str {
        "jpeg" => super::types::ImageFormat::Jpeg,
        "png" => super::types::ImageFormat::Png,
        "webp" => super::types::ImageFormat::Webp,
        "avif" => super::types::ImageFormat::Avif,
        "bmp" => super::types::ImageFormat::Bmp,
        _ => {
            return Err(CowrieError::InvalidData(format!(
                "unknown image format: {}",
                format_str
            )))
        }
    };

    let width =
        obj.get("width")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| CowrieError::InvalidData("image missing width".into()))? as u16;

    let height =
        obj.get("height")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| CowrieError::InvalidData("image missing height".into()))? as u16;

    let data_b64 = obj
        .get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("image missing data".into()))?;

    let data = BASE64
        .decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Image(super::types::ImageData {
        format,
        width,
        height,
        data,
    }))
}

fn parse_audio_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let encoding_str = obj
        .get("encoding")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("audio missing encoding".into()))?;

    let encoding = match encoding_str {
        "pcm_int16" => super::types::AudioEncoding::PcmInt16,
        "pcm_float32" => super::types::AudioEncoding::PcmFloat32,
        "opus" => super::types::AudioEncoding::Opus,
        "aac" => super::types::AudioEncoding::Aac,
        _ => {
            return Err(CowrieError::InvalidData(format!(
                "unknown audio encoding: {}",
                encoding_str
            )))
        }
    };

    let sample_rate =
        obj.get("rate")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| CowrieError::InvalidData("audio missing rate".into()))? as u32;

    let channels = obj
        .get("channels")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CowrieError::InvalidData("audio missing channels".into()))?
        as u8;

    let data_b64 = obj
        .get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("audio missing data".into()))?;

    let data = BASE64
        .decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Audio(super::types::AudioData {
        encoding,
        sample_rate,
        channels,
        data,
    }))
}

fn parse_unknown_ext_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let type_id = obj
        .get("ext_type")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CowrieError::InvalidData("unknown_ext missing ext_type".into()))?;

    let payload_b64 = obj
        .get("payload")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("unknown_ext missing payload".into()))?;

    let payload = BASE64
        .decode(payload_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Ext(super::types::ExtData { type_id, payload }))
}

fn parse_node_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("node missing id".into()))?
        .to_string();

    let labels: Vec<String> = obj
        .get("labels")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CowrieError::InvalidData("node missing labels".into()))?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let props_obj = obj
        .get("props")
        .and_then(|v| v.as_object())
        .ok_or_else(|| CowrieError::InvalidData("node missing props".into()))?;

    let mut props = BTreeMap::new();
    for (k, v) in props_obj {
        props.insert(k.clone(), json_to_value(v)?);
    }

    Ok(Value::Node(super::types::NodeData { id, labels, props }))
}

fn parse_edge_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let from = obj
        .get("fromId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("edge missing fromId".into()))?
        .to_string();

    let to = obj
        .get("toId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("edge missing toId".into()))?
        .to_string();

    let edge_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("edge missing type".into()))?
        .to_string();

    let props_obj = obj
        .get("props")
        .and_then(|v| v.as_object())
        .ok_or_else(|| CowrieError::InvalidData("edge missing props".into()))?;

    let mut props = BTreeMap::new();
    for (k, v) in props_obj {
        props.insert(k.clone(), json_to_value(v)?);
    }

    Ok(Value::Edge(super::types::EdgeData {
        from,
        to,
        edge_type,
        props,
    }))
}

fn parse_node_batch_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let arr = obj
        .get("nodes")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CowrieError::InvalidData("node_batch missing nodes".into()))?;

    let nodes: Result<Vec<super::types::NodeData>, CowrieError> = arr
        .iter()
        .map(|item| {
            match parse_node_from_json(item.as_object().ok_or_else(|| {
                CowrieError::InvalidData("node_batch: node is not an object".into())
            })?)? {
                Value::Node(n) => Ok(n),
                _ => Err(CowrieError::InvalidData("node_batch: expected node".into())),
            }
        })
        .collect();

    Ok(Value::NodeBatch(super::types::NodeBatchData {
        nodes: nodes?,
    }))
}

fn parse_edge_batch_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let arr = obj
        .get("edges")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CowrieError::InvalidData("edge_batch missing edges".into()))?;

    let edges: Result<Vec<super::types::EdgeData>, CowrieError> = arr
        .iter()
        .map(|item| {
            match parse_edge_from_json(item.as_object().ok_or_else(|| {
                CowrieError::InvalidData("edge_batch: edge is not an object".into())
            })?)? {
                Value::Edge(e) => Ok(e),
                _ => Err(CowrieError::InvalidData("edge_batch: expected edge".into())),
            }
        })
        .collect();

    Ok(Value::EdgeBatch(super::types::EdgeBatchData {
        edges: edges?,
    }))
}

fn parse_bitmask_from_json(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, CowrieError> {
    let count = obj
        .get("count")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CowrieError::InvalidData("bitmask missing count".into()))?;

    let bits_b64 = obj
        .get("bits")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("bitmask missing bits".into()))?;

    let bits = BASE64
        .decode(bits_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Bitmask { count, bits })
}

fn value_to_json(value: &Value) -> Result<serde_json::Value, CowrieError> {
    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::json!(*i)),
        Value::Uint(u) => Ok(serde_json::json!(*u)),
        Value::Float(f) => {
            if f.is_finite() {
                Ok(serde_json::json!(*f))
            } else {
                Ok(serde_json::Value::Null)
            }
        }
        Value::Decimal(d) => {
            // Encode as base64 for lossless representation
            Ok(serde_json::json!({
                "_type": "decimal128",
                "data": BASE64.encode(d)
            }))
        }
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Bytes(b) => Ok(serde_json::json!({
            "_type": "bytes",
            "data": BASE64.encode(b)
        })),
        Value::DateTime(nanos) => Ok(serde_json::json!({
            "_type": "datetime",
            "nanos": *nanos
        })),
        Value::Uuid(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            let formatted = format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            );
            Ok(serde_json::json!({
                "_type": "uuid",
                "hex": formatted
            }))
        }
        Value::BigInt(b) => Ok(serde_json::json!({
            "_type": "bigint",
            "data": BASE64.encode(b)
        })),
        Value::Array(arr) => {
            let items: Result<Vec<serde_json::Value>, CowrieError> =
                arr.iter().map(value_to_json).collect();
            Ok(serde_json::Value::Array(items?))
        }
        Value::Object(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(obj))
        }
        Value::Tensor(t) => {
            let dtype_str = match t.dtype {
                DType::Float32 => "float32",
                DType::Float64 => "float64",
                DType::Int8 => "int8",
                DType::Int16 => "int16",
                DType::Int32 => "int32",
                DType::Int64 => "int64",
                DType::Uint8 => "uint8",
                DType::Uint16 => "uint16",
                DType::Uint32 => "uint32",
                DType::Uint64 => "uint64",
                DType::BFloat16 => "bfloat16",
                DType::Float16 => "float16",
                DType::Bool => "bool",
                DType::QINT4 => "qint4",
                DType::QINT2 => "qint2",
                DType::QINT3 => "qint3",
                DType::Ternary => "ternary",
                DType::Binary => "binary",
            };
            Ok(serde_json::json!({
                "_type": "tensor",
                "dtype": dtype_str,
                "dims": t.shape,
                "data": BASE64.encode(&t.data)
            }))
        }
        Value::TensorRef(r) => Ok(serde_json::json!({
            "_type": "tensor_ref",
            "store": r.store_id as u64,
            "key": BASE64.encode(&r.key)
        })),
        Value::Image(img) => {
            let format_str = match img.format {
                super::types::ImageFormat::Jpeg => "jpeg",
                super::types::ImageFormat::Png => "png",
                super::types::ImageFormat::Webp => "webp",
                super::types::ImageFormat::Avif => "avif",
                super::types::ImageFormat::Bmp => "bmp",
            };
            Ok(serde_json::json!({
                "_type": "image",
                "format": format_str,
                "width": img.width,
                "height": img.height,
                "data": BASE64.encode(&img.data)
            }))
        }
        Value::Audio(aud) => {
            let encoding_str = match aud.encoding {
                super::types::AudioEncoding::PcmInt16 => "pcm_int16",
                super::types::AudioEncoding::PcmFloat32 => "pcm_float32",
                super::types::AudioEncoding::Opus => "opus",
                super::types::AudioEncoding::Aac => "aac",
            };
            Ok(serde_json::json!({
                "_type": "audio",
                "encoding": encoding_str,
                "rate": aud.sample_rate,
                "channels": aud.channels,
                "data": BASE64.encode(&aud.data)
            }))
        }
        Value::Ext(ext) => Ok(serde_json::json!({
            "_type": "unknown_ext",
            "ext_type": ext.type_id,
            "payload": BASE64.encode(&ext.payload)
        })),
        // Graph types
        Value::Node(node) => {
            let props: Result<serde_json::Map<String, serde_json::Value>, CowrieError> = node
                .props
                .iter()
                .map(|(k, v)| Ok((k.clone(), value_to_json(v)?)))
                .collect();
            Ok(serde_json::json!({
                "_type": "node",
                "id": node.id,
                "labels": node.labels,
                "props": serde_json::Value::Object(props?)
            }))
        }
        Value::Edge(edge) => {
            let props: Result<serde_json::Map<String, serde_json::Value>, CowrieError> = edge
                .props
                .iter()
                .map(|(k, v)| Ok((k.clone(), value_to_json(v)?)))
                .collect();
            Ok(serde_json::json!({
                "_type": "edge",
                "fromId": edge.from,
                "toId": edge.to,
                "type": edge.edge_type,
                "props": serde_json::Value::Object(props?)
            }))
        }
        Value::NodeBatch(batch) => {
            let nodes: Result<Vec<serde_json::Value>, CowrieError> = batch
                .nodes
                .iter()
                .map(|n| value_to_json(&Value::Node(n.clone())))
                .collect();
            Ok(serde_json::json!({
                "_type": "node_batch",
                "nodes": nodes?
            }))
        }
        Value::EdgeBatch(batch) => {
            let edges: Result<Vec<serde_json::Value>, CowrieError> = batch
                .edges
                .iter()
                .map(|e| value_to_json(&Value::Edge(e.clone())))
                .collect();
            Ok(serde_json::json!({
                "_type": "edge_batch",
                "edges": edges?
            }))
        }
        Value::Bitmask { count, bits } => Ok(serde_json::json!({
            "_type": "bitmask",
            "count": count,
            "bits": BASE64.encode(bits)
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{
        AudioData, AudioEncoding, EdgeBatchData, EdgeData, ExtData, ImageData, ImageFormat,
        NodeBatchData, NodeData, TensorRef,
    };
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_json_roundtrip_object() {
        let json = r#"{"name":"Alice","age":30,"active":true}"#;
        let value = from_json(json).unwrap();

        if let Value::Object(obj) = &value {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(obj.get("age"), Some(&Value::Int(30)));
            assert_eq!(obj.get("active"), Some(&Value::Bool(true)));
        } else {
            panic!("expected object");
        }

        let back = to_json(&value).unwrap();
        let reparsed = from_json(&back).unwrap();

        if let Value::Object(obj) = &reparsed {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
        }
    }

    #[test]
    fn test_json_tensor() {
        let json = r#"{"_type":"tensor","dtype":"float32","dims":[2,2],"data":"AAAAPwAAAD8AAAA/AAAAPw=="}"#;
        let value = from_json(json).unwrap();

        if let Value::Tensor(t) = &value {
            assert_eq!(t.dtype, DType::Float32);
            assert_eq!(t.shape, vec![2, 2]);
            assert_eq!(t.data.len(), 16);
        } else {
            panic!("expected tensor");
        }
    }

    // Round-trip helpers: serialize then deserialize and assert equal.
    fn roundtrip(v: &Value) -> Value {
        let json = to_json(v).expect("to_json");
        from_json(&json).expect("from_json")
    }

    #[test]
    fn test_roundtrip_tensor() {
        let v = Value::Tensor(super::super::types::TensorData {
            dtype: DType::Float32,
            shape: vec![2, 3],
            data: vec![0u8; 24],
        });
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_tensor_field_names() {
        // Verify to_json emits the canonical field names.
        let v = Value::Tensor(super::super::types::TensorData {
            dtype: DType::Int8,
            shape: vec![4],
            data: vec![1, 2, 3, 4],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "tensor");
        assert_eq!(obj["dtype"], "int8");
        assert!(obj.get("dims").is_some());
        assert!(obj.get("data").is_some());
    }

    #[test]
    fn test_roundtrip_tensor_ref() {
        let v = Value::TensorRef(TensorRef {
            store_id: 7,
            key: vec![0xDE, 0xAD, 0xBE, 0xEF],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Canonical field names
        assert_eq!(obj["_type"], "tensor_ref");
        assert_eq!(obj["store"], 7);
        assert!(obj.get("key").is_some());
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_image() {
        let v = Value::Image(ImageData {
            format: ImageFormat::Png,
            width: 16,
            height: 16,
            data: vec![0u8; 10],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "image");
        assert_eq!(obj["format"], "png");
        assert_eq!(obj["width"], 16);
        assert_eq!(obj["height"], 16);
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_audio() {
        let v = Value::Audio(AudioData {
            encoding: AudioEncoding::PcmInt16,
            sample_rate: 44100,
            channels: 2,
            data: vec![0u8; 8],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Canonical: "rate" not "sample_rate"
        assert_eq!(obj["_type"], "audio");
        assert_eq!(obj["rate"], 44100);
        assert!(
            obj.get("sample_rate").is_none(),
            "must not emit sample_rate"
        );
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_unknown_ext() {
        let v = Value::Ext(ExtData {
            type_id: 999,
            payload: vec![0x01, 0x02, 0x03],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Canonical: "_type":"unknown_ext", "ext_type", "payload"
        assert_eq!(obj["_type"], "unknown_ext");
        assert_eq!(obj["ext_type"], 999);
        assert!(obj.get("payload").is_some());
        assert!(obj.get("type_id").is_none(), "must not emit type_id");
        assert!(obj.get("data").is_none(), "must not emit data");
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_node() {
        let mut props = BTreeMap::new();
        props.insert("age".to_string(), Value::Int(30));
        let v = Value::Node(NodeData {
            id: "n1".to_string(),
            labels: vec!["Person".to_string()],
            props,
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "node");
        assert_eq!(obj["id"], "n1");
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_edge() {
        let v = Value::Edge(EdgeData {
            from: "n1".to_string(),
            to: "n2".to_string(),
            edge_type: "KNOWS".to_string(),
            props: BTreeMap::new(),
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Canonical: "fromId" and "toId"
        assert_eq!(obj["_type"], "edge");
        assert_eq!(obj["fromId"], "n1");
        assert_eq!(obj["toId"], "n2");
        assert!(obj.get("from").is_none(), "must not emit from");
        assert!(obj.get("to").is_none(), "must not emit to");
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_node_batch() {
        let mut props = BTreeMap::new();
        props.insert("x".to_string(), Value::Int(1));
        let v = Value::NodeBatch(NodeBatchData {
            nodes: vec![NodeData {
                id: "n1".to_string(),
                labels: vec!["A".to_string()],
                props,
            }],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "node_batch");
        assert!(obj["nodes"].as_array().is_some());
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_edge_batch() {
        let v = Value::EdgeBatch(EdgeBatchData {
            edges: vec![EdgeData {
                from: "a".to_string(),
                to: "b".to_string(),
                edge_type: "REL".to_string(),
                props: BTreeMap::new(),
            }],
        });
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "edge_batch");
        // Inner edges must use fromId/toId
        assert_eq!(obj["edges"][0]["fromId"], "a");
        assert_eq!(obj["edges"][0]["toId"], "b");
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_roundtrip_bitmask() {
        let v = Value::Bitmask {
            count: 10,
            bits: vec![0b10101010, 0b01010101],
        };
        let json = to_json(&v).unwrap();
        let obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(obj["_type"], "bitmask");
        assert_eq!(obj["count"], 10);
        // bits must be a base64 string, not an array
        assert!(obj["bits"].as_str().is_some(), "bits must be base64 string");
        assert_eq!(roundtrip(&v), v);
    }
}
