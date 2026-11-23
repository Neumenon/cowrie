//! `cowrie` CLI: human-readable inspection, strict verification, and R0/R1/R2 lens projection
//! for Cowrie shards and 16 KB-aligned hybrid StatePages.

use std::env;
use std::fs;
use std::path::Path;
use std::process;

use cowrie_core::canonical::{decode_frame_strict, encode_frame, parse_json_to_value};
use cowrie_core::lens::{project_r0, project_r1, project_r2};
use cowrie_core::sha256::{content_address, to_hex};
use cowrie_core::shard::{
    build_shard_16kb, parse_statepage_meta, verify_shard_and_localize, FLAG_STATEPAGE, PAGE_ALIGN,
};
use cowrie_core::trust::{
    authority_label, AUTH_MODEL_SYNTH, AUTH_SYS_POLICY, AUTH_UNTRUSTED_EXT, AUTH_VERIFIED_TOOL,
    EPI_OBSERVED,
};

fn usage() {
    eprintln!(
        "cowrie (Cowrie v1.0 CLI)\n\
         Usage:\n\
         \x20 cowrie pack <in.json> <out.cowrie> [--auth policy|verified|untrusted|synth]\n\
         \x20 cowrie inspect <file.cowrie>\n\
         \x20 cowrie verify <file.cowrie>\n\
         \x20 cowrie lens <file.cowrie> [--tier r0|r1|r2]\n\
         \x20 cowrie state-inspect <workspace.state.cowrie> [--verify-hash]"
    );
}

fn parse_auth(s: &str) -> u8 {
    match s {
        "policy" => AUTH_SYS_POLICY,
        "verified" => AUTH_VERIFIED_TOOL,
        "untrusted" => AUTH_UNTRUSTED_EXT,
        "synth" => AUTH_MODEL_SYNTH,
        other => {
            eprintln!("Unknown --auth '{}'; expected policy|verified|untrusted|synth", other);
            process::exit(2);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        usage();
        process::exit(1);
    }

    match args[1].as_str() {
        "pack" => {
            if args.len() < 4 {
                usage();
                process::exit(1);
            }
            let mut auth = AUTH_VERIFIED_TOOL;
            let mut i = 4;
            while i < args.len() {
                if args[i] == "--auth" && i + 1 < args.len() {
                    auth = parse_auth(&args[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            let raw_json = fs::read_to_string(&args[2]).unwrap_or_else(|e| {
                eprintln!("Failed to read {}: {}", args[2], e);
                process::exit(1);
            });
            let val = parse_json_to_value(&raw_json).unwrap_or_else(|e| {
                eprintln!("JSON parse error: {}", e);
                process::exit(1);
            });
            let frame = encode_frame(&val, auth, EPI_OBSERVED).unwrap_or_else(|e| {
                eprintln!("Canonical encode error: {}", e);
                process::exit(1);
            });
            let cid = content_address(&frame);
            let shard = build_shard_16kb(&[&frame], &[], 0);
            fs::write(&args[3], &shard).unwrap_or_else(|e| {
                eprintln!("Failed to write {}: {}", args[3], e);
                process::exit(1);
            });
            println!(
                "PACKED {} -> {} | frame={}B | shard={}B (16KB aligned) | auth={} | cid={}",
                args[2],
                args[3],
                frame.len(),
                shard.len(),
                authority_label(auth),
                to_hex(&cid)
            );
        }

        "inspect" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let buf = fs::read(&args[2]).unwrap_or_else(|e| {
                eprintln!("Read error: {}", e);
                process::exit(1);
            });
            let rep = verify_shard_and_localize(&buf).unwrap_or_else(|e| {
                eprintln!("Shard parse error: {}", e);
                process::exit(1);
            });
            println!("Shard:        {}", args[2]);
            println!("Total bytes:  {} B", buf.len());
            println!("Version:      v{} | Align: {} B (2^{}) | Flags: 0x{:04x}", rep.header.version, 1usize << rep.header.align_log2, rep.header.align_log2, rep.header.flags);
            println!("Plane A:      off={} len={} B | Frames: {}", rep.header.plane_a_off, rep.header.plane_a_len, rep.header.frame_count);
            println!("Plane C:      off={} (mod 16KB = {}) len={} B | Tensors: {}", rep.header.plane_c_off, rep.header.plane_c_off as usize % PAGE_ALIGN, rep.header.plane_c_len, rep.header.tensor_count);
            println!("Plane D:      footer_off={} | Merkle OK: {}", rep.header.footer_off, rep.ok);
            println!("Merkle Root:  {}", to_hex(&rep.computed_root));
            if (rep.header.flags & FLAG_STATEPAGE) != 0 {
                if let Ok(sp) = parse_statepage_meta(&buf) {
                    println!("StatePage:    ctx_tokens={} state_len={} B model_sha={}...", sp.ctx_tokens, sp.state_len, &sp.model_sha[..16]);
                }
            }
            for (idx, fe) in rep.frames.iter().enumerate() {
                let fb = &buf[fe.offset as usize..(fe.offset as usize + fe.len as usize)];
                if let Ok(dec) = decode_frame_strict(fb) {
                    println!("  Frame[{}]:   {}", idx, project_r0(fb, &dec, rep.tensors.len()));
                } else {
                    println!("  Frame[{}]:   <INVALID CANONICAL FRAME>", idx);
                }
            }
        }

        "verify" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let buf = fs::read(&args[2]).unwrap_or_else(|e| {
                eprintln!("Read error: {}", e);
                process::exit(1);
            });
            let rep = verify_shard_and_localize(&buf).unwrap_or_else(|e| {
                eprintln!("VERIFY FAIL: {}", e);
                process::exit(1);
            });
            for (idx, fe) in rep.frames.iter().enumerate() {
                let fb = &buf[fe.offset as usize..(fe.offset as usize + fe.len as usize)];
                if let Err(e) = decode_frame_strict(fb) {
                    eprintln!("VERIFY FAIL: frame[{}] invalid canonical encoding: {}", idx, e);
                    process::exit(1);
                }
            }
            if !rep.ok {
                eprintln!("VERIFY FAIL: ERR_ROOT_MISMATCH | bad_frames={:?}", rep.bad_frames);
                process::exit(1);
            }
            println!(
                "VERIFY PASS | frames={} tensors={} | root={}",
                rep.frames.len(),
                rep.tensors.len(),
                to_hex(&rep.computed_root)
            );
        }

        "lens" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let mut tier = "r2";
            let mut i = 3;
            while i < args.len() {
                if args[i] == "--tier" && i + 1 < args.len() {
                    tier = args[i + 1].as_str();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            let buf = fs::read(&args[2]).unwrap_or_else(|e| {
                eprintln!("Read error: {}", e);
                process::exit(1);
            });
            let rep = verify_shard_and_localize(&buf).unwrap_or_else(|e| {
                eprintln!("Shard error: {}", e);
                process::exit(1);
            });
            if !rep.ok {
                eprintln!("Refusing lens projection on corrupt shard (bad_frames={:?})", rep.bad_frames);
                process::exit(1);
            }
            for fe in &rep.frames {
                let fb = &buf[fe.offset as usize..(fe.offset as usize + fe.len as usize)];
                let dec = decode_frame_strict(fb).unwrap_or_else(|e| {
                    eprintln!("Decode error: {}", e);
                    process::exit(1);
                });
                match tier {
                    "r0" => println!("{}", project_r0(fb, &dec, rep.tensors.len())),
                    "r1" => print!("{}", project_r1(fb, &dec)),
                    _ => print!("{}", project_r2(fb, &dec)),
                }
            }
        }

        "state-inspect" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let verify_hash = args.iter().any(|a| a == "--verify-hash");
            let mp = cowrie_core::shard::MappedStatePage::open_verified(
                Path::new(&args[2]),
                None,
                verify_hash,
            )
            .unwrap_or_else(|e| {
                eprintln!("STATEPAGE FAIL: {}", e);
                process::exit(1);
            });
            let kv_lbl = match mp.meta.kv_quant {
                0 => "FP16",
                1 => "Q4_0",
                2 => "Q8_0",
                _ => "CUSTOM",
            };
            let hm_lbl = if mp.meta.hash_mode == 1 { "parallel-4MiB-merkle-sha256" } else { "flat-sha256" };
            println!("StatePage:    {} (16 KB aligned: {})", args[2], mp.meta.state_offset as usize % PAGE_ALIGN == 0);
            println!("Context Toks: {} | KV Quant: {} | Hash Mode: {} (SHA-NI: {})", mp.meta.ctx_tokens, kv_lbl, hm_lbl, cowrie_core::sha256::has_shani());
            println!("State Bytes:  {} B ({:.2} MB) @ off={}", mp.meta.state_len, mp.meta.state_len as f64 / 1e6, mp.meta.state_offset);
            println!("Model SHA:    {}", mp.meta.model_sha);
            println!("Doc CID:      {}", mp.meta.doc_cid);
            println!("State Digest: {} (verified={})", mp.meta.state_sha, verify_hash);
        }

        _ => {
            usage();
            process::exit(1);
        }
    }
}
