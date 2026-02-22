//! JSON bridge for Cowrie Gen2.
//!
//! Provides conversion between JSON strings and Cowrie Values.

use super::types::{Value, CowrieError, DType, TensorData};
use std::collections::BTreeMap;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

/// Parse a JSON string into an Cowrie Value.
pub fn from_json(json: &str) -> Result<Value, CowrieError> {
    let parsed: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;
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

fn parse_tensor_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Value, CowrieError> {
    let dtype_str = obj.get("dtype")
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
        _ => return Err(CowrieError::InvalidData(format!("unknown dtype: {}", dtype_str))),
    };

    let dims: Vec<u64> = obj.get("dims")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CowrieError::InvalidData("tensor missing dims".into()))?
        .iter()
        .filter_map(|v| v.as_u64())
        .collect();

    let data_b64 = obj.get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("tensor missing data".into()))?;

    let data = BASE64.decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Tensor(TensorData {
        dtype,
        shape: dims,
        data,
    }))
}

fn parse_bytes_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Value, CowrieError> {
    let data_b64 = obj.get("data")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("bytes missing data".into()))?;

    let data = BASE64.decode(data_b64)
        .map_err(|e| CowrieError::InvalidData(e.to_string()))?;

    Ok(Value::Bytes(data))
}

fn parse_datetime_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Value, CowrieError> {
    let nanos = obj.get("nanos")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| CowrieError::InvalidData("datetime missing nanos".into()))?;

    Ok(Value::DateTime(nanos))
}

fn parse_uuid_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Value, CowrieError> {
    let hex = obj.get("hex")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CowrieError::InvalidData("uuid missing hex".into()))?;

    let clean: String = hex.chars().filter(|c: &char| c.is_ascii_hexdigit()).collect();
    if clean.len() != 32 {
        return Err(CowrieError::InvalidData("invalid uuid hex".into()));
    }

    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&clean[i*2..i*2+2], 16)
            .map_err(|_| CowrieError::InvalidData("invalid uuid hex".into()))?;
    }

    Ok(Value::Uuid(bytes))
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
        Value::Bytes(b) => {
            Ok(serde_json::json!({
                "_type": "bytes",
                "data": BASE64.encode(b)
            }))
        }
        Value::DateTime(nanos) => {
            Ok(serde_json::json!({
                "_type": "datetime",
                "nanos": *nanos
            }))
        }
        Value::Uuid(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            let formatted = format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]
            );
            Ok(serde_json::json!({
                "_type": "uuid",
                "hex": formatted
            }))
        }
        Value::BigInt(b) => {
            Ok(serde_json::json!({
                "_type": "bigint",
                "data": BASE64.encode(b)
            }))
        }
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
        Value::TensorRef(r) => {
            Ok(serde_json::json!({
                "_type": "tensor_ref",
                "store_id": r.store_id,
                "key": BASE64.encode(&r.key)
            }))
        }
        Value::Image(img) => {
            Ok(serde_json::json!({
                "_type": "image",
                "format": img.format,
                "width": img.width,
                "height": img.height,
                "data": BASE64.encode(&img.data)
            }))
        }
        Value::Audio(aud) => {
            Ok(serde_json::json!({
                "_type": "audio",
                "encoding": aud.encoding,
                "sample_rate": aud.sample_rate,
                "channels": aud.channels,
                "data": BASE64.encode(&aud.data)
            }))
        }
        Value::Adjlist(adj) => {
            Ok(serde_json::json!({
                "_type": "adjlist",
                "id_width": adj.id_width,
                "node_count": adj.node_count,
                "edge_count": adj.edge_count,
                "row_offsets": adj.row_offsets,
                "col_indices": BASE64.encode(&adj.col_indices)
            }))
        }
        Value::RichText(rt) => {
            Ok(serde_json::json!({
                "_type": "richtext",
                "text": rt.text,
                "tokens": rt.tokens,
                "spans": rt.spans.as_ref().map(|spans| spans.iter().map(|s| {
                    serde_json::json!({"start": s.start, "end": s.end, "kind_id": s.kind_id})
                }).collect::<Vec<_>>())
            }))
        }
        Value::Delta(delta) => {
            let ops: Vec<serde_json::Value> = delta.ops.iter().map(|op| {
                serde_json::json!({
                    "op_code": op.op_code as u8,
                    "field_id": op.field_id,
                    "value": op.value.as_ref().map(|v| value_to_json(v).ok()).flatten()
                })
            }).collect();
            Ok(serde_json::json!({
                "_type": "delta",
                "base_id": delta.base_id,
                "ops": ops
            }))
        }
        Value::Ext(ext) => {
            Ok(serde_json::json!({
                "_type": "ext",
                "type_id": ext.type_id,
                "data": BASE64.encode(&ext.payload)
            }))
        }
        // Graph types
        Value::Node(node) => {
            let props: Result<serde_json::Map<String, serde_json::Value>, CowrieError> =
                node.props.iter().map(|(k, v)| {
                    Ok((k.clone(), value_to_json(v)?))
                }).collect();
            Ok(serde_json::json!({
                "_type": "node",
                "id": node.id,
                "labels": node.labels,
                "props": serde_json::Value::Object(props?)
            }))
        }
        Value::Edge(edge) => {
            let props: Result<serde_json::Map<String, serde_json::Value>, CowrieError> =
                edge.props.iter().map(|(k, v)| {
                    Ok((k.clone(), value_to_json(v)?))
                }).collect();
            Ok(serde_json::json!({
                "_type": "edge",
                "from": edge.from,
                "to": edge.to,
                "type": edge.edge_type,
                "props": serde_json::Value::Object(props?)
            }))
        }
        Value::NodeBatch(batch) => {
            let nodes: Result<Vec<serde_json::Value>, CowrieError> =
                batch.nodes.iter().map(|n| value_to_json(&Value::Node(n.clone()))).collect();
            Ok(serde_json::json!({
                "_type": "node_batch",
                "nodes": nodes?
            }))
        }
        Value::EdgeBatch(batch) => {
            let edges: Result<Vec<serde_json::Value>, CowrieError> =
                batch.edges.iter().map(|e| value_to_json(&Value::Edge(e.clone()))).collect();
            Ok(serde_json::json!({
                "_type": "edge_batch",
                "edges": edges?
            }))
        }
        Value::GraphShard(shard) => {
            let nodes: Result<Vec<serde_json::Value>, CowrieError> =
                shard.nodes.iter().map(|n| value_to_json(&Value::Node(n.clone()))).collect();
            let edges: Result<Vec<serde_json::Value>, CowrieError> =
                shard.edges.iter().map(|e| value_to_json(&Value::Edge(e.clone()))).collect();
            let metadata: Result<serde_json::Map<String, serde_json::Value>, CowrieError> =
                shard.metadata.iter().map(|(k, v)| {
                    Ok((k.clone(), value_to_json(v)?))
                }).collect();
            Ok(serde_json::json!({
                "_type": "graph_shard",
                "nodes": nodes?,
                "edges": edges?,
                "metadata": serde_json::Value::Object(metadata?)
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
