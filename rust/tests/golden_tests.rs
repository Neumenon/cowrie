//! Golden test vectors - verify Rust round-trips Cowrie values correctly.
//!
//! NOTE: the former `test_decode_gen2_tensor` / `test_decode_gen2_image_meta` golden tests were
//! deleted in the COWR/v1 migration. They decoded the pre-migration SJ-magic golden binaries
//! (`testdata/gen2/*.cowrie`, header `53 4a 02 00`), a wire format the live decoder no longer
//! accepts. Those shared golden binaries are owned by the cross-language fixture suite and are
//! not regenerated here, so the SJ-format decode tests are removed rather than re-pinned.

use cowrie_rs::gen2::{decode, encode, Value};

#[test]
fn test_roundtrip_gen2() {
    // Create a value
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("name".to_string(), Value::String("test".to_string()));
    obj.insert("count".to_string(), Value::Int(42));
    obj.insert("active".to_string(), Value::Bool(true));
    let original = Value::Object(obj);

    // Encode
    let encoded = encode(&original).expect("encode failed");

    // Decode
    let decoded = decode(&encoded).expect("decode failed");

    // Compare
    if let (Value::Object(orig), Value::Object(dec)) = (&original, &decoded) {
        assert_eq!(orig.get("name"), dec.get("name"));
        assert_eq!(orig.get("count"), dec.get("count"));
        assert_eq!(orig.get("active"), dec.get("active"));
    } else {
        panic!("type mismatch");
    }
}
