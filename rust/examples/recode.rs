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
//! With `--tensor-spans` the tool instead acts as a Phase 2 zero-copy TENSOR LOCATOR: it decodes
//! one canonical Cowrie message from stdin and prints ONE LINE PER TENSOR in document order (the
//! order tensors appear during decode), each line `<data_offset> <data_len> <dtype> <shape>`
//! (space-separated), where `<shape>` is the comma-joined shape dims (e.g. `2,3`) or `-` for a
//! rank-0 scalar. `data_offset` is the ABSOLUTE byte offset (from byte 0 of the input) of the
//! tensor's contiguous little-endian data run; `data_len` its byte length. No tensors => no
//! output. Offsets are captured during decode (not guessed), mirroring `cowrie_ref.tensor_spans`.
use cowrie_rs::gen2::{
    address_of_bytes, decode_file, decode_framed, decode_framed_strict, encode, encode_file,
    merkle_root, schema_fingerprint64, tensor_spans, to_hex,
};
use std::io::{Read, Write};

fn main() {
    let strict = std::env::args().skip(1).any(|a| a == "--strict")
        || std::env::var("STRICT").map(|v| v == "1").unwrap_or(false);
    let addr = std::env::args().skip(1).any(|a| a == "--addr");
    let file_id = std::env::args().skip(1).any(|a| a == "--file-id");
    let file_recode = std::env::args().skip(1).any(|a| a == "--file-recode");
    let tensor_spans_mode = std::env::args().skip(1).any(|a| a == "--tensor-spans");
    let fingerprint_mode = std::env::args().skip(1).any(|a| a == "--fingerprint");
    let dataset_root_mode = std::env::args().skip(1).any(|a| a == "--dataset-root");

    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data).expect("failed to read stdin");

    // --- Dataset/stream layer Merkle root (SPEC-v1 §7 dataset_root) ---
    // Read from stdin: uvarint(n) then n repetitions of (uvarint(len) || blob_bytes).
    // Compute the §7 merkle_root over that ORDERED list of blobs (the shard file-roots) and
    // print the 34-byte multihash as 68 lowercase hex chars on one line. Reuses gen2::merkle_root.
    if dataset_root_mode {
        let mut pos = 0usize;
        let read_uvarint = |data: &[u8], p: &mut usize| -> u64 {
            let mut shift = 0u32;
            let mut val = 0u64;
            loop {
                let b = data[*p];
                *p += 1;
                val |= ((b & 0x7f) as u64) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            val
        };
        let n = read_uvarint(&data, &mut pos);
        let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let len = read_uvarint(&data, &mut pos) as usize;
            blobs.push(data[pos..pos + len].to_vec());
            pos += len;
        }
        println!("{}", to_hex(&merkle_root(&blobs)));
        return;
    }

    // --- Phase 2 zero-copy tensor locator: one line per tensor, document order ---
    if tensor_spans_mode {
        match tensor_spans(&data) {
            Ok(spans) => {
                let mut out = String::new();
                for s in &spans {
                    let shape = if s.shape.is_empty() {
                        "-".to_string()
                    } else {
                        s.shape
                            .iter()
                            .map(|d| d.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    out.push_str(&format!(
                        "{} {} {} {}\n",
                        s.data_offset, s.data_len, s.dtype, shape
                    ));
                }
                std::io::stdout()
                    .write_all(out.as_bytes())
                    .expect("failed to write stdout");
            }
            Err(e) => {
                eprintln!("tensor-spans decode error: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }

    // --- Cowrie FILE container modes (SPEC-v1 §7) ---
    if file_id || file_recode {
        match decode_file(&data, true) {
            Ok(frames) => {
                if file_id {
                    // Print the verified Merkle root as one line of lowercase hex (68 chars).
                    println!("{}", to_hex(&merkle_root(&frames)));
                } else {
                    // Re-encode the file; canonical input reproduces byte-for-byte.
                    let out = encode_file(&frames);
                    std::io::stdout().write_all(&out).expect("failed to write stdout");
                }
            }
            Err(e) => {
                eprintln!("{}: file decode error: {}", e.err_code(), e);
                std::process::exit(1);
            }
        }
        return;
    }

    let decoded = if strict {
        decode_framed_strict(&data)
    } else {
        decode_framed(&data)
    };

    match decoded {
        Ok(v) => {
            // --- §4 schema fingerprint mode: print fingerprint64 as 16 lowercase hex chars ---
            if fingerprint_mode {
                println!("{:016x}", schema_fingerprint64(&v));
                return;
            }
            match encode(&v) {
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
                eprintln!("{}: encode error: {}", e.err_code(), e);
                std::process::exit(1);
            }
            }
        }
        Err(e) => {
            eprintln!("{}: {}", e.err_code(), e);
            std::process::exit(1);
        }
    }
}
