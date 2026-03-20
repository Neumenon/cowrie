//! JS ↔ Cowrie Value conversion.
//!
//! Maps JavaScript values to/from Cowrie's 22+ type Value enum.
//!
//! ## Type mapping (JS → Cowrie):
//! - `null`/`undefined` → Null
//! - `boolean` → Bool
//! - `number` (integer, fits i64) → Int
//! - `number` (float or too large) → Float
//! - `bigint` (>= 0, fits u64) → Uint
//! - `bigint` (negative or > u64) → BigInt (two's complement bytes)
//! - `string` → String
//! - `Uint8Array` → Bytes
//! - `Array` → Array (recursive)
//! - `Object` → Object (recursive), unless tagged with `_cowrie_type`
//!
//! ## Tagged objects (`_cowrie_type` field):
//! - `"tensor"` → Tensor { dtype, shape, data }
//! - `"bytes"` → Bytes
//! - `"datetime"` → DateTime (i64 nanos)
//! - `"uuid"` → Uuid ([u8; 16])
//! - `"image"` → Image { format, width, height, data }
//! - `"audio"` → Audio { encoding, sample_rate, channels, data }
//! - `"bitmask"` → Bitmask { count, bits }
//! - `"ext"` → Ext { type_id, payload }

use wasm_bindgen::prelude::*;
use cowrie_rs::gen2::{Value, DType, TensorData, ImageFormat, AudioEncoding};
use cowrie_rs::gen2::types::{ImageData, AudioData};
use std::collections::BTreeMap;

/// Convert a JsValue to a Cowrie Value.
pub fn js_to_value(js: &JsValue) -> Result<Value, String> {
    if js.is_null() || js.is_undefined() {
        return Ok(Value::Null);
    }

    if let Some(b) = js.as_bool() {
        return Ok(Value::Bool(b));
    }

    if let Some(n) = js.as_f64() {
        // Check if it's an integer that fits in i64
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            return Ok(Value::Int(n as i64));
        }
        return Ok(Value::Float(n));
    }

    // BigInt handling
    if js.is_bigint() {
        // Try to convert to i64/u64 first
        if let Some(n) = js_bigint_to_i64(js) {
            return Ok(Value::Int(n));
        }
        if let Some(n) = js_bigint_to_u64(js) {
            return Ok(Value::Uint(n));
        }
        // Fall back to BigInt bytes (two's complement)
        let s = js.as_string()
            .or_else(|| {
                js_sys::JsString::from(js.clone()).as_string()
            })
            .unwrap_or_default();
        return Ok(Value::BigInt(s.into_bytes()));
    }

    if let Some(s) = js.as_string() {
        return Ok(Value::String(s));
    }

    // Uint8Array → Bytes
    if js.is_instance_of::<js_sys::Uint8Array>() {
        let arr = js_sys::Uint8Array::from(js.clone());
        return Ok(Value::Bytes(arr.to_vec()));
    }

    // Array
    if js_sys::Array::is_array(js) {
        let arr = js_sys::Array::from(js);
        let mut items = Vec::with_capacity(arr.length() as usize);
        for i in 0..arr.length() {
            items.push(js_to_value(&arr.get(i))?);
        }
        return Ok(Value::Array(items));
    }

    // Object (check for tagged types first)
    if js.is_object() {
        return js_object_to_value(js);
    }

    Err(format!("unsupported JS type"))
}

/// Convert a JS object to a Cowrie Value, handling tagged types.
fn js_object_to_value(js: &JsValue) -> Result<Value, String> {
    // Check for _cowrie_type tag
    if let Ok(type_val) = js_sys::Reflect::get(js, &"_cowrie_type".into()) {
        if let Some(type_str) = type_val.as_string() {
            return match type_str.as_str() {
                "tensor" => js_to_tensor(js),
                "bytes" => js_to_bytes(js),
                "datetime" => js_to_datetime(js),
                "uuid" => js_to_uuid(js),
                "image" => js_to_image(js),
                "audio" => js_to_audio(js),
                "bitmask" => js_to_bitmask(js),
                "ext" => js_to_ext(js),
                "decimal" => js_to_decimal(js),
                _ => Err(format!("unknown _cowrie_type: {}", type_str)),
            };
        }
    }

    // Regular object
    let keys = js_sys::Object::keys(&js_sys::Object::from(js.clone()));
    let mut map = BTreeMap::new();
    for i in 0..keys.length() {
        let key = keys.get(i).as_string()
            .ok_or_else(|| "non-string object key".to_string())?;
        let val = js_sys::Reflect::get(js, &key.clone().into())
            .map_err(|_| format!("failed to read key: {}", key))?;
        map.insert(key, js_to_value(&val)?);
    }
    Ok(Value::Object(map))
}

fn js_to_tensor(js: &JsValue) -> Result<Value, String> {
    let dtype_str = get_string(js, "dtype")?;
    let dtype = parse_dtype(&dtype_str)?;

    let shape_js = js_sys::Reflect::get(js, &"shape".into())
        .map_err(|_| "tensor missing shape")?;
    let shape_arr = js_sys::Array::from(&shape_js);
    let mut shape = Vec::with_capacity(shape_arr.length() as usize);
    for i in 0..shape_arr.length() {
        let dim = shape_arr.get(i).as_f64()
            .ok_or_else(|| "invalid shape dimension")?;
        shape.push(dim as u64);
    }

    let data_js = js_sys::Reflect::get(js, &"data".into())
        .map_err(|_| "tensor missing data")?;
    let data = js_sys::Uint8Array::from(data_js).to_vec();

    Ok(Value::Tensor(TensorData { dtype, shape, data }))
}

fn js_to_bytes(js: &JsValue) -> Result<Value, String> {
    let data_js = js_sys::Reflect::get(js, &"data".into())
        .map_err(|_| "bytes missing data")?;
    let data = js_sys::Uint8Array::from(data_js).to_vec();
    Ok(Value::Bytes(data))
}

fn js_to_datetime(js: &JsValue) -> Result<Value, String> {
    let nanos_js = js_sys::Reflect::get(js, &"nanos".into())
        .map_err(|_| "datetime missing nanos")?;
    // Support both number and bigint
    if let Some(n) = nanos_js.as_f64() {
        return Ok(Value::DateTime(n as i64));
    }
    if nanos_js.is_bigint() {
        if let Some(n) = js_bigint_to_i64(&nanos_js) {
            return Ok(Value::DateTime(n));
        }
    }
    Err("datetime nanos must be a number or bigint".into())
}

fn js_to_uuid(js: &JsValue) -> Result<Value, String> {
    let hex = get_string(js, "hex")?;
    let clean: String = hex.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if clean.len() != 32 {
        return Err(format!("invalid UUID hex length: {}", clean.len()));
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16)
            .map_err(|_| "invalid UUID hex")?;
    }
    Ok(Value::Uuid(bytes))
}

fn js_to_image(js: &JsValue) -> Result<Value, String> {
    let fmt_str = get_string(js, "format")?;
    let format = match fmt_str.as_str() {
        "jpeg" | "jpg" => ImageFormat::Jpeg,
        "png" => ImageFormat::Png,
        "webp" => ImageFormat::Webp,
        "avif" => ImageFormat::Avif,
        "bmp" => ImageFormat::Bmp,
        _ => return Err(format!("unknown image format: {}", fmt_str)),
    };
    let width = get_u16(js, "width")?;
    let height = get_u16(js, "height")?;
    let data_js = js_sys::Reflect::get(js, &"data".into())
        .map_err(|_| "image missing data")?;
    let data = js_sys::Uint8Array::from(data_js).to_vec();
    Ok(Value::Image(ImageData { format, width, height, data }))
}

fn js_to_audio(js: &JsValue) -> Result<Value, String> {
    let enc_str = get_string(js, "encoding")?;
    let encoding = match enc_str.as_str() {
        "pcm_int16" => AudioEncoding::PcmInt16,
        "pcm_float32" => AudioEncoding::PcmFloat32,
        "opus" => AudioEncoding::Opus,
        "aac" => AudioEncoding::Aac,
        _ => return Err(format!("unknown audio encoding: {}", enc_str)),
    };
    let sample_rate = get_f64(js, "sampleRate")
        .or_else(|_| get_f64(js, "sample_rate"))? as u32;
    let channels = get_f64(js, "channels")? as u8;
    let data_js = js_sys::Reflect::get(js, &"data".into())
        .map_err(|_| "audio missing data")?;
    let data = js_sys::Uint8Array::from(data_js).to_vec();
    Ok(Value::Audio(AudioData { encoding, sample_rate, channels, data }))
}

fn js_to_bitmask(js: &JsValue) -> Result<Value, String> {
    let count = get_f64(js, "count")? as u64;
    let bits_js = js_sys::Reflect::get(js, &"bits".into())
        .map_err(|_| "bitmask missing bits")?;
    let bits = js_sys::Uint8Array::from(bits_js).to_vec();
    Ok(Value::Bitmask { count, bits })
}

fn js_to_ext(js: &JsValue) -> Result<Value, String> {
    use cowrie_rs::gen2::types::ExtData;
    let type_id = get_f64(js, "typeId")
        .or_else(|_| get_f64(js, "type_id"))? as u64;
    let payload_js = js_sys::Reflect::get(js, &"payload".into())
        .or_else(|_| js_sys::Reflect::get(js, &"data".into()))
        .map_err(|_| "ext missing payload/data")?;
    let payload = js_sys::Uint8Array::from(payload_js).to_vec();
    Ok(Value::Ext(ExtData { type_id, payload }))
}

fn js_to_decimal(js: &JsValue) -> Result<Value, String> {
    let data_js = js_sys::Reflect::get(js, &"data".into())
        .map_err(|_| "decimal missing data")?;
    let data = js_sys::Uint8Array::from(data_js).to_vec();
    if data.len() != 17 {
        return Err(format!("decimal data must be 17 bytes, got {}", data.len()));
    }
    Ok(Value::Decimal(data))
}

// ============================================================
// Value → JS conversion
// ============================================================

/// Convert a Cowrie Value to a JsValue.
pub fn value_to_js(value: &Value) -> Result<JsValue, String> {
    match value {
        Value::Null => Ok(JsValue::NULL),
        Value::Bool(b) => Ok(JsValue::from(*b)),
        Value::Int(i) => {
            // Use number for safe integer range, bigint otherwise
            if *i >= -(1i64 << 53) && *i <= (1i64 << 53) {
                Ok(JsValue::from(*i as f64))
            } else {
                Ok(js_sys::BigInt::from(*i).into())
            }
        }
        Value::Uint(u) => {
            if *u <= (1u64 << 53) {
                Ok(JsValue::from(*u as f64))
            } else {
                Ok(js_sys::BigInt::from(*u).into())
            }
        }
        Value::Float(f) => Ok(JsValue::from(*f)),
        Value::String(s) => Ok(JsValue::from_str(s)),
        Value::Bytes(b) => {
            Ok(js_sys::Uint8Array::from(b.as_slice()).into())
        }
        Value::Array(arr) => {
            let js_arr = js_sys::Array::new_with_length(arr.len() as u32);
            for (i, item) in arr.iter().enumerate() {
                js_arr.set(i as u32, value_to_js(item)?);
            }
            Ok(js_arr.into())
        }
        Value::Object(map) => {
            let obj = js_sys::Object::new();
            for (key, val) in map {
                js_sys::Reflect::set(&obj, &key.into(), &value_to_js(val)?)
                    .map_err(|_| format!("failed to set key: {}", key))?;
            }
            Ok(obj.into())
        }
        Value::Tensor(t) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "tensor")?;
            set_str(&obj, "dtype", dtype_to_str(t.dtype))?;
            let shape = js_sys::Array::new_with_length(t.shape.len() as u32);
            for (i, dim) in t.shape.iter().enumerate() {
                shape.set(i as u32, JsValue::from(*dim as f64));
            }
            js_sys::Reflect::set(&obj, &"shape".into(), &shape.into())
                .map_err(|_| "failed to set shape")?;
            let data = js_sys::Uint8Array::from(t.data.as_slice());
            js_sys::Reflect::set(&obj, &"data".into(), &data.into())
                .map_err(|_| "failed to set data")?;
            Ok(obj.into())
        }
        Value::TensorRef(r) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "tensor_ref")?;
            set_f64(&obj, "storeId", r.store_id as f64)?;
            let key = js_sys::Uint8Array::from(r.key.as_slice());
            js_sys::Reflect::set(&obj, &"key".into(), &key.into())
                .map_err(|_| "failed to set key")?;
            Ok(obj.into())
        }
        Value::Image(img) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "image")?;
            set_str(&obj, "format", image_format_to_str(img.format))?;
            set_f64(&obj, "width", img.width as f64)?;
            set_f64(&obj, "height", img.height as f64)?;
            let data = js_sys::Uint8Array::from(img.data.as_slice());
            js_sys::Reflect::set(&obj, &"data".into(), &data.into())
                .map_err(|_| "failed to set data")?;
            Ok(obj.into())
        }
        Value::Audio(aud) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "audio")?;
            set_str(&obj, "encoding", audio_encoding_to_str(aud.encoding))?;
            set_f64(&obj, "sampleRate", aud.sample_rate as f64)?;
            set_f64(&obj, "channels", aud.channels as f64)?;
            let data = js_sys::Uint8Array::from(aud.data.as_slice());
            js_sys::Reflect::set(&obj, &"data".into(), &data.into())
                .map_err(|_| "failed to set data")?;
            Ok(obj.into())
        }
        Value::DateTime(nanos) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "datetime")?;
            // Use bigint for nanos to avoid precision loss
            js_sys::Reflect::set(&obj, &"nanos".into(), &js_sys::BigInt::from(*nanos).into())
                .map_err(|_| "failed to set nanos")?;
            Ok(obj.into())
        }
        Value::Uuid(bytes) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "uuid")?;
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            let formatted = format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]
            );
            set_str(&obj, "hex", &formatted)?;
            Ok(obj.into())
        }
        Value::BigInt(data) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "bigint")?;
            let d = js_sys::Uint8Array::from(data.as_slice());
            js_sys::Reflect::set(&obj, &"data".into(), &d.into())
                .map_err(|_| "failed to set data")?;
            Ok(obj.into())
        }
        Value::Decimal(data) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "decimal")?;
            let d = js_sys::Uint8Array::from(data.as_slice());
            js_sys::Reflect::set(&obj, &"data".into(), &d.into())
                .map_err(|_| "failed to set data")?;
            Ok(obj.into())
        }
        Value::Adjlist(adj) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "adjlist")?;
            set_f64(&obj, "idWidth", adj.id_width as f64)?;
            set_f64(&obj, "nodeCount", adj.node_count as f64)?;
            set_f64(&obj, "edgeCount", adj.edge_count as f64)?;
            let offsets = js_sys::Array::new_with_length(adj.row_offsets.len() as u32);
            for (i, o) in adj.row_offsets.iter().enumerate() {
                offsets.set(i as u32, JsValue::from(*o as f64));
            }
            js_sys::Reflect::set(&obj, &"rowOffsets".into(), &offsets.into())
                .map_err(|_| "failed to set rowOffsets")?;
            let cols = js_sys::Uint8Array::from(adj.col_indices.as_slice());
            js_sys::Reflect::set(&obj, &"colIndices".into(), &cols.into())
                .map_err(|_| "failed to set colIndices")?;
            Ok(obj.into())
        }
        Value::RichText(rt) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "richtext")?;
            set_str(&obj, "text", &rt.text)?;
            if let Some(ref tokens) = rt.tokens {
                let arr = js_sys::Int32Array::from(tokens.as_slice());
                js_sys::Reflect::set(&obj, &"tokens".into(), &arr.into())
                    .map_err(|_| "failed to set tokens")?;
            }
            if let Some(ref spans) = rt.spans {
                let arr = js_sys::Array::new_with_length(spans.len() as u32);
                for (i, span) in spans.iter().enumerate() {
                    let s = js_sys::Object::new();
                    set_f64(&s, "start", span.start as f64)?;
                    set_f64(&s, "end", span.end as f64)?;
                    set_f64(&s, "kindId", span.kind_id as f64)?;
                    arr.set(i as u32, s.into());
                }
                js_sys::Reflect::set(&obj, &"spans".into(), &arr.into())
                    .map_err(|_| "failed to set spans")?;
            }
            Ok(obj.into())
        }
        Value::Delta(delta) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "delta")?;
            set_f64(&obj, "baseId", delta.base_id as f64)?;
            let ops = js_sys::Array::new_with_length(delta.ops.len() as u32);
            for (i, op) in delta.ops.iter().enumerate() {
                let o = js_sys::Object::new();
                set_f64(&o, "opCode", op.op_code as u8 as f64)?;
                set_f64(&o, "fieldId", op.field_id as f64)?;
                if let Some(ref val) = op.value {
                    js_sys::Reflect::set(&o, &"value".into(), &value_to_js(val)?)
                        .map_err(|_| "failed to set op value")?;
                }
                ops.set(i as u32, o.into());
            }
            js_sys::Reflect::set(&obj, &"ops".into(), &ops.into())
                .map_err(|_| "failed to set ops")?;
            Ok(obj.into())
        }
        Value::Ext(ext) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "ext")?;
            set_f64(&obj, "typeId", ext.type_id as f64)?;
            let payload = js_sys::Uint8Array::from(ext.payload.as_slice());
            js_sys::Reflect::set(&obj, &"payload".into(), &payload.into())
                .map_err(|_| "failed to set payload")?;
            Ok(obj.into())
        }
        Value::Bitmask { count, bits } => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "bitmask")?;
            set_f64(&obj, "count", *count as f64)?;
            let b = js_sys::Uint8Array::from(bits.as_slice());
            js_sys::Reflect::set(&obj, &"bits".into(), &b.into())
                .map_err(|_| "failed to set bits")?;
            Ok(obj.into())
        }
        // Graph types
        Value::Node(node) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "node")?;
            set_str(&obj, "id", &node.id)?;
            let labels = js_sys::Array::new_with_length(node.labels.len() as u32);
            for (i, l) in node.labels.iter().enumerate() {
                labels.set(i as u32, JsValue::from_str(l));
            }
            js_sys::Reflect::set(&obj, &"labels".into(), &labels.into())
                .map_err(|_| "failed to set labels")?;
            let props = props_to_js(&node.props)?;
            js_sys::Reflect::set(&obj, &"props".into(), &props)
                .map_err(|_| "failed to set props")?;
            Ok(obj.into())
        }
        Value::Edge(edge) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "edge")?;
            set_str(&obj, "from", &edge.from)?;
            set_str(&obj, "to", &edge.to)?;
            set_str(&obj, "edgeType", &edge.edge_type)?;
            let props = props_to_js(&edge.props)?;
            js_sys::Reflect::set(&obj, &"props".into(), &props)
                .map_err(|_| "failed to set props")?;
            Ok(obj.into())
        }
        Value::NodeBatch(batch) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "node_batch")?;
            let nodes = js_sys::Array::new_with_length(batch.nodes.len() as u32);
            for (i, node) in batch.nodes.iter().enumerate() {
                nodes.set(i as u32, value_to_js(&Value::Node(node.clone()))?);
            }
            js_sys::Reflect::set(&obj, &"nodes".into(), &nodes.into())
                .map_err(|_| "failed to set nodes")?;
            Ok(obj.into())
        }
        Value::EdgeBatch(batch) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "edge_batch")?;
            let edges = js_sys::Array::new_with_length(batch.edges.len() as u32);
            for (i, edge) in batch.edges.iter().enumerate() {
                edges.set(i as u32, value_to_js(&Value::Edge(edge.clone()))?);
            }
            js_sys::Reflect::set(&obj, &"edges".into(), &edges.into())
                .map_err(|_| "failed to set edges")?;
            Ok(obj.into())
        }
        Value::GraphShard(shard) => {
            let obj = js_sys::Object::new();
            set_str(&obj, "_cowrie_type", "graph_shard")?;
            let nodes = js_sys::Array::new_with_length(shard.nodes.len() as u32);
            for (i, node) in shard.nodes.iter().enumerate() {
                nodes.set(i as u32, value_to_js(&Value::Node(node.clone()))?);
            }
            js_sys::Reflect::set(&obj, &"nodes".into(), &nodes.into())
                .map_err(|_| "failed to set nodes")?;
            let edges = js_sys::Array::new_with_length(shard.edges.len() as u32);
            for (i, edge) in shard.edges.iter().enumerate() {
                edges.set(i as u32, value_to_js(&Value::Edge(edge.clone()))?);
            }
            js_sys::Reflect::set(&obj, &"edges".into(), &edges.into())
                .map_err(|_| "failed to set edges")?;
            let meta = props_to_js(&shard.metadata)?;
            js_sys::Reflect::set(&obj, &"metadata".into(), &meta)
                .map_err(|_| "failed to set metadata")?;
            Ok(obj.into())
        }
    }
}

// ============================================================
// Helpers
// ============================================================

fn props_to_js(props: &BTreeMap<String, Value>) -> Result<JsValue, String> {
    let obj = js_sys::Object::new();
    for (key, val) in props {
        js_sys::Reflect::set(&obj, &key.into(), &value_to_js(val)?)
            .map_err(|_| format!("failed to set prop: {}", key))?;
    }
    Ok(obj.into())
}

fn set_str(obj: &js_sys::Object, key: &str, val: &str) -> Result<(), String> {
    js_sys::Reflect::set(obj, &key.into(), &JsValue::from_str(val))
        .map_err(|_| format!("failed to set {}", key))?;
    Ok(())
}

fn set_f64(obj: &js_sys::Object, key: &str, val: f64) -> Result<(), String> {
    js_sys::Reflect::set(obj, &key.into(), &JsValue::from(val))
        .map_err(|_| format!("failed to set {}", key))?;
    Ok(())
}

fn get_string(js: &JsValue, key: &str) -> Result<String, String> {
    js_sys::Reflect::get(js, &key.into())
        .ok()
        .and_then(|v| v.as_string())
        .ok_or_else(|| format!("missing or invalid string field: {}", key))
}

fn get_f64(js: &JsValue, key: &str) -> Result<f64, String> {
    js_sys::Reflect::get(js, &key.into())
        .ok()
        .and_then(|v| v.as_f64())
        .ok_or_else(|| format!("missing or invalid number field: {}", key))
}

fn get_u16(js: &JsValue, key: &str) -> Result<u16, String> {
    let n = get_f64(js, key)?;
    if n < 0.0 || n > u16::MAX as f64 {
        return Err(format!("{} out of u16 range: {}", key, n));
    }
    Ok(n as u16)
}

fn parse_dtype(s: &str) -> Result<DType, String> {
    match s {
        "float32" | "f32" => Ok(DType::Float32),
        "float64" | "f64" => Ok(DType::Float64),
        "float16" | "f16" => Ok(DType::Float16),
        "bfloat16" | "bf16" => Ok(DType::BFloat16),
        "int8" | "i8" => Ok(DType::Int8),
        "int16" | "i16" => Ok(DType::Int16),
        "int32" | "i32" => Ok(DType::Int32),
        "int64" | "i64" => Ok(DType::Int64),
        "uint8" | "u8" => Ok(DType::Uint8),
        "uint16" | "u16" => Ok(DType::Uint16),
        "uint32" | "u32" => Ok(DType::Uint32),
        "uint64" | "u64" => Ok(DType::Uint64),
        "bool" => Ok(DType::Bool),
        "qint4" => Ok(DType::QINT4),
        "qint2" => Ok(DType::QINT2),
        "qint3" => Ok(DType::QINT3),
        "ternary" => Ok(DType::Ternary),
        "binary" => Ok(DType::Binary),
        _ => Err(format!("unknown dtype: {}", s)),
    }
}

fn dtype_to_str(dtype: DType) -> &'static str {
    match dtype {
        DType::Float32 => "float32",
        DType::Float64 => "float64",
        DType::Float16 => "float16",
        DType::BFloat16 => "bfloat16",
        DType::Int8 => "int8",
        DType::Int16 => "int16",
        DType::Int32 => "int32",
        DType::Int64 => "int64",
        DType::Uint8 => "uint8",
        DType::Uint16 => "uint16",
        DType::Uint32 => "uint32",
        DType::Uint64 => "uint64",
        DType::Bool => "bool",
        DType::QINT4 => "qint4",
        DType::QINT2 => "qint2",
        DType::QINT3 => "qint3",
        DType::Ternary => "ternary",
        DType::Binary => "binary",
    }
}

fn image_format_to_str(fmt: ImageFormat) -> &'static str {
    match fmt {
        ImageFormat::Jpeg => "jpeg",
        ImageFormat::Png => "png",
        ImageFormat::Webp => "webp",
        ImageFormat::Avif => "avif",
        ImageFormat::Bmp => "bmp",
    }
}

fn audio_encoding_to_str(enc: AudioEncoding) -> &'static str {
    match enc {
        AudioEncoding::PcmInt16 => "pcm_int16",
        AudioEncoding::PcmFloat32 => "pcm_float32",
        AudioEncoding::Opus => "opus",
        AudioEncoding::Aac => "aac",
    }
}

/// Try to convert a JS BigInt to i64.
fn js_bigint_to_i64(js: &JsValue) -> Option<i64> {
    // wasm-bindgen doesn't have direct BigInt → i64, so use string conversion
    let s = js_sys::JsString::from(js.clone()).as_string()?;
    s.parse::<i64>().ok()
}

/// Try to convert a JS BigInt to u64.
fn js_bigint_to_u64(js: &JsValue) -> Option<u64> {
    let s = js_sys::JsString::from(js.clone()).as_string()?;
    s.parse::<u64>().ok()
}
