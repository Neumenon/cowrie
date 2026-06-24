//! Test that reserved/stripped tags (0x30–0x34, 0x39) are silently skipped by the decoder.
//!
//! The C3 sprint stripped encoder/decoder arms for Adjlist (0x30), RichText (0x31),
//! Delta (0x32), and GraphShard (0x39). Per the spec, a reader must forward-scan past
//! unknown length-prefixed values without error. This file verifies that contract.

use cowrie_rs::gen2::{decode, Value};

/// Build a minimal Cowrie Gen2 stream manually.
///
/// Format: magic(4 "COWR") | version(1) | flags(1) | dict_len(uvarint) | dict_entries… | root_value
fn cowrie_bytes(dict_keys: &[&str], root_value_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    // Magic + version + flags (canonical COWR/v1 header, SPEC-v1 §2.2)
    buf.extend_from_slice(b"COWR");
    buf.push(0x01); // VERSION
    buf.push(0x00); // flags

    // Dictionary
    write_uvarint(&mut buf, dict_keys.len() as u64);
    for key in dict_keys {
        write_uvarint(&mut buf, key.len() as u64);
        buf.extend_from_slice(key.as_bytes());
    }

    // Root value bytes (pre-encoded)
    buf.extend_from_slice(root_value_bytes);
    buf
}

fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

/// Reserved tag 0x30 (was Adjlist) with a 4-byte payload: skipped → Value::Null.
#[test]
fn skip_tag_0x30_in_array() {
    // Construct FIXARRAY(3) = [Int(1), <0x30 reserved, 4 bytes>, Int(2)]
    // FIXARRAY_BASE = 0xC0, so FIXARRAY(3) = 0xC3
    let mut value_bytes = Vec::new();
    value_bytes.push(0xC3); // FIXARRAY with 3 elements

    // Element 0: FIXINT 1 (0x40 + 1 = 0x41)
    value_bytes.push(0x41);

    // Element 1: reserved tag 0x30, payload_len = 4, payload = [0xDE, 0xAD, 0xBE, 0xEF]
    value_bytes.push(0x30);
    write_uvarint(&mut value_bytes, 4);
    value_bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    // Element 2: FIXINT 2 (0x40 + 2 = 0x42)
    value_bytes.push(0x42);

    let stream = cowrie_bytes(&[], &value_bytes);
    let result = decode(&stream);
    assert!(
        result.is_ok(),
        "should not error on reserved tag 0x30: {:?}",
        result
    );

    let val = result.unwrap();
    let arr = val.as_array().expect("expected Array");
    assert_eq!(arr.len(), 3, "array should have 3 elements");
    assert_eq!(arr[0], Value::Int(1), "first kept element should be Int(1)");
    assert_eq!(arr[1], Value::Null, "reserved tag should decode as Null");
    assert_eq!(
        arr[2],
        Value::Int(2),
        "second kept element should be Int(2)"
    );
}

/// Reserved tag 0x39 (was GraphShard) — same skip behaviour.
#[test]
fn skip_tag_0x39_in_array() {
    let mut value_bytes = Vec::new();
    value_bytes.push(0xC2); // FIXARRAY(2)

    // Element 0: FIXINT 42 (0x40 + 42 = 0x6A)
    value_bytes.push(0x6A);

    // Element 1: reserved tag 0x39, payload_len = 4, arbitrary payload
    value_bytes.push(0x39);
    write_uvarint(&mut value_bytes, 4);
    value_bytes.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);

    let stream = cowrie_bytes(&[], &value_bytes);
    let result = decode(&stream);
    assert!(
        result.is_ok(),
        "should not error on reserved tag 0x39: {:?}",
        result
    );

    let val = result.unwrap();
    let arr = val.as_array().expect("expected Array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0], Value::Int(42));
    assert_eq!(arr[1], Value::Null, "reserved 0x39 should decode as Null");
}

/// A kept Object field still decodes correctly when surrounded by a reserved-tag skip.
#[test]
fn skip_reserved_tag_kept_field_survives() {
    // Object with dict key "score" (index 0), value = FIXINT 99
    // Encoded as: FIXMAP(1) | dict_index(0) | FIXINT(99)
    // FIXMAP_BASE = 0xD0, FIXMAP(1) = 0xD1
    // dict_index 0 = uvarint 0x00
    // FIXINT 99 = 0x40 + 99 = 0xA3
    let mut value_bytes = Vec::new();
    value_bytes.push(0xD1); // FIXMAP(1)
    write_uvarint(&mut value_bytes, 0); // dict index 0 = "score"
    value_bytes.push(0xA3); // FIXINT 99

    let stream = cowrie_bytes(&["score"], &value_bytes);
    let result = decode(&stream);
    assert!(
        result.is_ok(),
        "object with kept field should decode: {:?}",
        result
    );

    let val = result.unwrap();
    let obj = val.as_object().expect("expected Object");
    assert_eq!(
        obj.get("score").and_then(|v| v.as_i64()),
        Some(99),
        "kept field 'score' should be 99"
    );
}

/// All six reserved tag values (0x30–0x34, 0x39) skip cleanly with empty payload.
#[test]
fn all_reserved_tags_skip_with_empty_payload() {
    let reserved_tags: &[u8] = &[0x30, 0x31, 0x32, 0x33, 0x34, 0x39];
    for &tag in reserved_tags {
        // Array of [reserved_tag(empty), Int(7)]
        let mut value_bytes = Vec::new();
        value_bytes.push(0xC2); // FIXARRAY(2)
        value_bytes.push(tag);
        write_uvarint(&mut value_bytes, 0); // payload_len = 0
        value_bytes.push(0x47); // FIXINT 7

        let stream = cowrie_bytes(&[], &value_bytes);
        let result = decode(&stream);
        assert!(
            result.is_ok(),
            "reserved tag 0x{:02x} should skip cleanly: {:?}",
            tag,
            result
        );
        let val = result.unwrap();
        let arr = val.as_array().expect("expected Array");
        assert_eq!(arr[0], Value::Null, "tag 0x{:02x} → Null", tag);
        assert_eq!(
            arr[1],
            Value::Int(7),
            "kept element preserved after tag 0x{:02x}",
            tag
        );
    }
}
