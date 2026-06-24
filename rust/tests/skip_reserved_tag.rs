//! Test that ALL reserved/non-core tags (incl. 0x30–0x34, 0x39) are REJECTED with
//! ERR_RESERVED_TAG by the decoder (SPEC-v1 §2.3).
//!
//! Earlier builds silently skipped these length-prefixed reserved values (decoding them as
//! `Value::Null`) — a verified freeze-blocker: every conformant implementation rejects them.
//! Per the spec, any tag outside the canonical v1 core MUST reject (in both lenient and strict
//! mode), never be forward-scanned. This file verifies that contract.

use cowrie_rs::gen2::decode;
use cowrie_rs::gen2::types::CowrieError;

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

/// Reserved tag 0x30 (was Adjlist) inside an array: MUST reject with ERR_RESERVED_TAG.
#[test]
fn reject_tag_0x30_in_array() {
    // FIXARRAY(3) = [Int(1), <0x30 reserved, 4 bytes>, Int(2)]; the reserved element must abort.
    let mut value_bytes = Vec::new();
    value_bytes.push(0xC3); // FIXARRAY with 3 elements
    value_bytes.push(0x41); // FIXINT 1
    value_bytes.push(0x30); // reserved tag 0x30
    write_uvarint(&mut value_bytes, 4);
    value_bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    value_bytes.push(0x42); // FIXINT 2

    let stream = cowrie_bytes(&[], &value_bytes);
    assert!(
        matches!(decode(&stream), Err(CowrieError::InvalidTag(0x30))),
        "reserved tag 0x30 must reject with ERR_RESERVED_TAG"
    );
}

/// Reserved tag 0x39 (was GraphShard) — also rejects.
#[test]
fn reject_tag_0x39_in_array() {
    let mut value_bytes = Vec::new();
    value_bytes.push(0xC2); // FIXARRAY(2)
    value_bytes.push(0x6A); // FIXINT 42
    value_bytes.push(0x39); // reserved tag 0x39
    write_uvarint(&mut value_bytes, 4);
    value_bytes.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);

    let stream = cowrie_bytes(&[], &value_bytes);
    assert!(
        matches!(decode(&stream), Err(CowrieError::InvalidTag(0x39))),
        "reserved tag 0x39 must reject with ERR_RESERVED_TAG"
    );
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

/// All six reserved tag values (0x30–0x34, 0x39) reject with ERR_RESERVED_TAG (SPEC-v1 §2.3).
#[test]
fn all_reserved_tags_reject() {
    let reserved_tags: &[u8] = &[0x30, 0x31, 0x32, 0x33, 0x34, 0x39];
    for &tag in reserved_tags {
        // Array of [reserved_tag(empty), Int(7)] — the reserved element must abort the decode.
        let mut value_bytes = Vec::new();
        value_bytes.push(0xC2); // FIXARRAY(2)
        value_bytes.push(tag);
        write_uvarint(&mut value_bytes, 0); // (would-be) payload_len = 0
        value_bytes.push(0x47); // FIXINT 7

        let stream = cowrie_bytes(&[], &value_bytes);
        assert!(
            matches!(decode(&stream), Err(CowrieError::InvalidTag(t)) if t == tag),
            "reserved tag 0x{:02x} must reject with ERR_RESERVED_TAG",
            tag
        );
    }
}
