//! Canonical round-trip: decode framed Gen2 from stdin, re-encode RAW canonical bytes to stdout.
//! Used by the cross-language IDENTITY gate: encode(decode(wire)) must be byte-identical across
//! Go/Rust/Python/TS. JSON-projection-independent (unlike decode_cli), so it tests identity, not display.
use cowrie_rs::gen2::{decode_framed, encode};
use std::io::{Read, Write};

fn main() {
    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data).expect("failed to read stdin");
    match decode_framed(&data) {
        Ok(v) => match encode(&v) {
            Ok(out) => std::io::stdout().write_all(&out).expect("failed to write stdout"),
            Err(e) => {
                eprintln!("encode error: {}", e);
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("decode error: {}", e);
            std::process::exit(1);
        }
    }
}
