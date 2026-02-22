use std::fs;
use std::path::Path;

use cowrie_codec::gen2::{decode, json::to_json, CowrieError};

#[test]
fn fixtures_core_decode() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let manifest_path = repo_root.join("testdata/fixtures/manifest.json");
    let manifest_bytes = fs::read_to_string(manifest_path).expect("read manifest");
    let manifest: serde_json::Value = serde_json::from_str(&manifest_bytes).expect("parse manifest");

    let cases = manifest.get("cases").and_then(|v| v.as_array()).cloned().unwrap_or_default();

    for case in cases {
        let gen = case.get("gen").and_then(|v| v.as_i64()).unwrap_or_default();
        let kind = case.get("kind").and_then(|v| v.as_str()).unwrap_or_default();
        if gen != 2 || kind != "decode" {
            continue;
        }

        let id = case.get("id").and_then(|v| v.as_str()).unwrap_or("<unknown>");
        let input = case.get("input").and_then(|v| v.as_str()).unwrap_or("");
        let input_path = repo_root.join("testdata/fixtures").join(input);
        let data = fs::read(&input_path).expect("read input");

        let expect = case.get("expect").and_then(|v| v.as_object()).expect("expect object");
        let ok = expect.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);

        if ok {
            let value = decode(&data).expect("decode failed");
            if let Some(json_path) = expect.get("json").and_then(|v| v.as_str()) {
                let expected_path = repo_root.join("testdata/fixtures").join(json_path);
                let expected_bytes = fs::read_to_string(expected_path).expect("read expected json");
                let expected_json: serde_json::Value = serde_json::from_str(&expected_bytes).expect("parse expected json");

                let actual_json_str = to_json(&value).expect("to_json");
                let actual_json: serde_json::Value = serde_json::from_str(&actual_json_str).expect("parse actual json");

                assert_eq!(actual_json, expected_json, "{} mismatch", id);
            }
        } else {
            match decode(&data) {
                Ok(_) => panic!("{}: expected error but decode succeeded", id),
                Err(err) => {
                    let code = map_error_code(&err);
                    let expected = expect.get("error").and_then(|v| v.as_str()).unwrap_or("");
                    assert_eq!(code, expected, "{}: expected {} got {}", id, expected, code);
                }
            }
        }
    }
}

fn map_error_code(err: &CowrieError) -> &'static str {
    match err {
        CowrieError::InvalidMagic => "ERR_INVALID_MAGIC",
        CowrieError::InvalidVersion(_) => "ERR_INVALID_VERSION",
        CowrieError::Truncated => "ERR_TRUNCATED",
        CowrieError::InvalidTag(_) => "ERR_INVALID_TAG",
        CowrieError::InvalidUtf8 => "ERR_INVALID_UTF8",
        CowrieError::TooDeep => "ERR_TOO_DEEP",
        CowrieError::TooLarge => "ERR_TOO_LARGE",
        _ => "",
    }
}
