#![no_main]
use std::collections::BTreeMap;
use libfuzzer_sys::fuzz_target;
use cowrie_rs::gen2::{encode, decode, Value};

/// Build a Value from fuzzer bytes (structured fuzzing)
fn value_from_bytes(data: &[u8]) -> Option<Value> {
    if data.is_empty() {
        return None;
    }
    let tag = data[0];
    let rest = &data[1..];
    match tag % 8 {
        0 => Some(Value::Null),
        1 => Some(Value::Bool(tag & 0x10 != 0)),
        2 => {
            if rest.len() >= 8 {
                let n = i64::from_le_bytes(rest[..8].try_into().ok()?);
                Some(Value::Int(n))
            } else {
                Some(Value::Int(tag as i64))
            }
        }
        3 => {
            if rest.len() >= 8 {
                let f = f64::from_le_bytes(rest[..8].try_into().ok()?);
                Some(Value::Float(f))
            } else {
                Some(Value::Float(tag as f64))
            }
        }
        4 => {
            let len = (tag as usize >> 4) & 0x0F;
            let s = String::from_utf8_lossy(&rest[..rest.len().min(len)]).into_owned();
            Some(Value::String(s))
        }
        5 => {
            let len = (tag as usize >> 4) & 0x0F;
            Some(Value::Bytes(rest[..rest.len().min(len)].to_vec()))
        }
        6 => {
            // Build small array from remaining bytes
            let mut items = Vec::new();
            let count = (tag as usize >> 4) & 0x03;
            let mut offset = 0;
            for _ in 0..count {
                if offset < rest.len() {
                    if let Some(v) = value_from_bytes(&rest[offset..]) {
                        let consumed = 1 + ((rest.len() - offset) / (count + 1)).max(1);
                        offset += consumed;
                        items.push(v);
                    }
                }
            }
            Some(Value::Array(items))
        }
        7 => {
            // Build small object
            let mut map = BTreeMap::new();
            if rest.len() >= 2 {
                let key = format!("k{}", rest[0]);
                if let Some(val) = value_from_bytes(&rest[1..]) {
                    map.insert(key, val);
                }
            }
            Some(Value::Object(map))
        }
        _ => Some(Value::Null),
    }
}

fuzz_target!(|data: &[u8]| {
    if let Some(val) = value_from_bytes(data) {
        if let Ok(encoded) = encode(&val) {
            match decode(&encoded) {
                Ok(decoded) => {
                    // Re-encode and compare bytes for canonical check
                    if let Ok(re_encoded) = encode(&decoded) {
                        assert_eq!(encoded, re_encoded, "Non-canonical encoding detected");
                    }
                }
                Err(_) => {
                    // Encoding succeeded but decoding failed - this is a bug
                    panic!("encode succeeded but decode failed for value: {:?}", val);
                }
            }
        }
    }
});
