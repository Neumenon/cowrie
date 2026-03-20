//! Cowrie WASM bindings.
//!
//! Provides `encode` and `decode` functions callable from JavaScript/TypeScript.
//! Values are marshalled between JS objects and Cowrie's internal `Value` type
//! using a structured conversion that preserves all 22+ Cowrie types.

use wasm_bindgen::prelude::*;
use cowrie_rs::gen2;

mod convert;

/// Encode a JavaScript value to Cowrie binary format.
///
/// Accepts any JSON-compatible JS value (objects, arrays, strings, numbers, booleans, null).
/// For Cowrie-specific types (tensors, images, audio, etc.), use tagged objects:
///
/// ```js
/// // Tensor
/// { _cowrie_type: "tensor", dtype: "float32", shape: [384], data: new Uint8Array(...) }
///
/// // Bytes
/// { _cowrie_type: "bytes", data: new Uint8Array([1, 2, 3]) }
///
/// // DateTime (nanoseconds since epoch)
/// { _cowrie_type: "datetime", nanos: 1234567890000000000n }
///
/// // UUID
/// { _cowrie_type: "uuid", hex: "550e8400-e29b-41d4-a716-446655440000" }
/// ```
///
/// Returns a `Uint8Array` containing the encoded Cowrie bytes.
#[wasm_bindgen]
pub fn encode(value: JsValue) -> Result<js_sys::Uint8Array, JsError> {
    let cowrie_val = convert::js_to_value(&value)
        .map_err(|e| JsError::new(&e))?;
    let bytes = gen2::encode(&cowrie_val)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    Ok(js_sys::Uint8Array::from(bytes.as_slice()))
}

/// Encode a JavaScript value with options.
///
/// Options:
/// - `omitNull` (boolean): Skip null fields in objects (default: false)
/// - `deterministic` (boolean): Sort object keys (default: false)
#[wasm_bindgen(js_name = "encodeWithOptions")]
pub fn encode_with_options(value: JsValue, options: JsValue) -> Result<js_sys::Uint8Array, JsError> {
    let cowrie_val = convert::js_to_value(&value)
        .map_err(|e| JsError::new(&e))?;

    let mut opts = gen2::EncodeOptions::default();
    if let Ok(obj) = js_sys::Reflect::get(&options, &"omitNull".into()) {
        if let Some(b) = obj.as_bool() {
            opts.omit_null = b;
        }
    }
    if let Ok(obj) = js_sys::Reflect::get(&options, &"deterministic".into()) {
        if let Some(b) = obj.as_bool() {
            opts.deterministic = b;
        }
    }

    let bytes = gen2::encode_with_options(&cowrie_val, &opts)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    Ok(js_sys::Uint8Array::from(bytes.as_slice()))
}

/// Decode Cowrie binary data to a JavaScript value.
///
/// Accepts a `Uint8Array` of Cowrie-encoded bytes.
/// Returns a JavaScript value. Cowrie-specific types are returned as tagged objects
/// with a `_cowrie_type` field.
#[wasm_bindgen]
pub fn decode(data: &[u8]) -> Result<JsValue, JsError> {
    let value = gen2::decode(data)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    convert::value_to_js(&value)
        .map_err(|e| JsError::new(&e))
}

/// Decode Cowrie binary data with configurable limits.
///
/// Options:
/// - `maxDepth` (number): Maximum nesting depth (default: 1000)
/// - `maxArrayLen` (number): Maximum array length (default: 1000000)
/// - `maxStringLen` (number): Maximum string length (default: 10000000)
#[wasm_bindgen(js_name = "decodeWithOptions")]
pub fn decode_with_options(data: &[u8], options: JsValue) -> Result<JsValue, JsError> {
    let mut opts = gen2::DecodeOptions::default();

    if let Ok(v) = js_sys::Reflect::get(&options, &"maxDepth".into()) {
        if let Some(n) = v.as_f64() {
            opts.max_depth = n as usize;
        }
    }
    if let Ok(v) = js_sys::Reflect::get(&options, &"maxArrayLen".into()) {
        if let Some(n) = v.as_f64() {
            opts.max_array_len = n as usize;
        }
    }
    if let Ok(v) = js_sys::Reflect::get(&options, &"maxStringLen".into()) {
        if let Some(n) = v.as_f64() {
            opts.max_string_len = n as usize;
        }
    }

    let value = gen2::decode_with_options(data, &opts)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    convert::value_to_js(&value)
        .map_err(|e| JsError::new(&e))
}

/// Encode with gzip compression framing.
#[wasm_bindgen(js_name = "encodeFramed")]
pub fn encode_framed(value: JsValue, compress: bool) -> Result<js_sys::Uint8Array, JsError> {
    let cowrie_val = convert::js_to_value(&value)
        .map_err(|e| JsError::new(&e))?;
    let compression = if compress {
        gen2::Compression::Gzip
    } else {
        gen2::Compression::None
    };
    let bytes = gen2::encode_framed(&cowrie_val, compression)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    Ok(js_sys::Uint8Array::from(bytes.as_slice()))
}

/// Decode a framed (possibly compressed) Cowrie value.
#[wasm_bindgen(js_name = "decodeFramed")]
pub fn decode_framed(data: &[u8]) -> Result<JsValue, JsError> {
    let value = gen2::decode_framed(data)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    convert::value_to_js(&value)
        .map_err(|e| JsError::new(&e))
}

/// Compute a 32-bit schema fingerprint for a value.
/// Useful for type routing in stream protocols.
#[wasm_bindgen(js_name = "schemaFingerprint32")]
pub fn schema_fingerprint32(value: JsValue) -> Result<u32, JsError> {
    let cowrie_val = convert::js_to_value(&value)
        .map_err(|e| JsError::new(&e))?;
    Ok(gen2::schema_fingerprint32(&cowrie_val))
}

/// Check if two values have the same schema structure.
#[wasm_bindgen(js_name = "schemaEquals")]
pub fn schema_equals(a: JsValue, b: JsValue) -> Result<bool, JsError> {
    let va = convert::js_to_value(&a).map_err(|e| JsError::new(&e))?;
    let vb = convert::js_to_value(&b).map_err(|e| JsError::new(&e))?;
    Ok(gen2::schema_equals(&va, &vb))
}

/// Convert a JSON string to Cowrie binary.
#[wasm_bindgen(js_name = "fromJSON")]
pub fn from_json(json: &str) -> Result<js_sys::Uint8Array, JsError> {
    let value = gen2::from_json(json)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    let bytes = gen2::encode(&value)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    Ok(js_sys::Uint8Array::from(bytes.as_slice()))
}

/// Convert Cowrie binary to a JSON string.
#[wasm_bindgen(js_name = "toJSON")]
pub fn to_json(data: &[u8]) -> Result<String, JsError> {
    let value = gen2::decode(data)
        .map_err(|e| JsError::new(&format!("{}", e)))?;
    gen2::to_json(&value)
        .map_err(|e| JsError::new(&format!("{}", e)))
}
