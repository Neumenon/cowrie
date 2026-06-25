//! Decode cowrie Gen2 binary (framed; handles compression) from stdin, emit canonical JSON.
//! Mirrors the Go reference CLI's `decode` (gen2 default = DecodeFramed). Used by the
//! cross-language fixture parity gate (validate_fixtures.py, RUST_CLI).
use cowrie_rs::gen2::{decode_framed, to_json};
use std::io::Read;

fn main() {
    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data).expect("failed to read stdin");
    match decode_framed(&data) {
        Ok(value) => match to_json(&value) {
            Ok(json) => println!("{}", json),
            Err(e) => {
                eprintln!("json error: {}", e);
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("decode error: {}", e);
            std::process::exit(1);
        }
    }
}
