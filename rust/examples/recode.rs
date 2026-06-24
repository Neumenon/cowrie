//! Canonical round-trip: decode framed Gen2 from stdin, re-encode RAW canonical bytes to stdout.
//! Used by the cross-language IDENTITY gate: encode(decode(wire)) must be byte-identical across
//! Go/Rust/Python/TS. JSON-projection-independent (unlike decode_cli), so it tests identity, not display.
//!
//! With `--strict` (or `STRICT=1` in the environment) the decode runs in STRICT mode
//! (SPEC-v1 §5.3): well-formed-but-non-canonical input is REJECTED (decode error -> non-zero
//! exit, empty stdout) instead of being silently accepted. Canonical input round-trips
//! identically in both modes.
//!
//! With `--addr` the tool instead prints the CONTENT ADDRESS (SPEC-v1 §3) of the canonical
//! bytes: the multihash SHA-256 (`0x12 0x20` + 32 digest bytes) of EXACTLY the canonical bytes
//! that plain `recode` would print, rendered as a single line of lowercase hex (68 chars).
//! `--addr` and `--strict` may combine.
use cowrie_rs::gen2::{address_of_bytes, decode_framed, decode_framed_strict, encode, to_hex};
use std::io::{Read, Write};

fn main() {
    let strict = std::env::args().skip(1).any(|a| a == "--strict")
        || std::env::var("STRICT").map(|v| v == "1").unwrap_or(false);
    let addr = std::env::args().skip(1).any(|a| a == "--addr");

    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data).expect("failed to read stdin");

    let decoded = if strict {
        decode_framed_strict(&data)
    } else {
        decode_framed(&data)
    };

    match decoded {
        Ok(v) => match encode(&v) {
            Ok(out) => {
                if addr {
                    // Content address of the canonical bytes we would otherwise emit.
                    let address = address_of_bytes(&out);
                    println!("{}", to_hex(&address));
                } else {
                    std::io::stdout().write_all(&out).expect("failed to write stdout");
                }
            }
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
