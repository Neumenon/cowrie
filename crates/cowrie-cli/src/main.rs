//! `cowrie` CLI (v1.2): human inspection, zero-alloc verification, R0/R1/R2 lenses,
//! strided model fingerprinting, `Ed25519` declassification signing, and `mcp-proxy`
//! zero-trust JSON-RPC wire firewall for unmodified MCP servers.

use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{self, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cowrie_core::canonical::{
    decode_frame_strict, encode_frame, parse_json_to_value, value_to_compact_json,
    verify_frame_zero_alloc, Value,
};
use cowrie_core::ed25519::{ed25519_public_key, DeclassificationToken};
use cowrie_core::lens::{project_r0, project_r1, project_r2};
use cowrie_core::sha256::{content_address, to_hex};
use cowrie_core::shard::{
    build_shard_16kb, fingerprint_model_file, parse_statepage_meta, verify_shard_and_localize,
    FLAG_STATEPAGE, PAGE_ALIGN,
};
use cowrie_core::trust::{
    authority_label, ToolAction, TrustLedger, AUTH_MODEL_SYNTH, AUTH_SYS_POLICY,
    AUTH_UNTRUSTED_EXT, AUTH_VERIFIED_TOOL, EPI_OBSERVED,
};
use cowrie_core::wal::WalStore;

fn usage() {
    eprintln!(
        "cowrie (Cowrie v1.2 CLI & MCP Wire Firewall)\n\
         Usage:\n\
         \x20 cowrie pack <in.json> <out.cowrie> [--auth policy|verified|untrusted|synth]\n\
         \x20 cowrie inspect <file.cowrie>\n\
         \x20 cowrie verify <file.cowrie>\n\
         \x20 cowrie lens <file.cowrie> [--tier r0|r1|r2]\n\
         \x20 cowrie state-inspect <workspace.state.cowrie> [--verify-hash]\n\
         \x20 cowrie model-fp <weights.gguf|safetensors>\n\
         \x20 cowrie ed25519-pubkey <seed-64-hex>\n\
         \x20 cowrie declassify-sign <seed-64-hex> <cid-68-hex> <shell_exec|cred_read|fs_write|net_exfil> <expiry_unix>\n\
         \x20 cowrie mcp-proxy [--owner-pubkey <64-hex>] [--audit-wal <path.cwwal>] -- <mcp_cmd> [args...]"
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

fn parse_hex_exact<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for i in 0..N {
        out[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn classify_mcp_tool(tool_name: &str) -> ToolAction {
    let lower = tool_name.to_ascii_lowercase();
    if lower.contains("shell")
        || lower.contains("bash")
        || lower.contains("exec")
        || lower.contains("command")
    {
        ToolAction::ShellExec
    } else if lower.contains("cred") || lower.contains("secret") || lower.contains("token") {
        ToolAction::CredRead
    } else if lower.contains("write")
        || lower.contains("delete")
        || lower.contains("remove")
        || lower.contains("edit")
    {
        ToolAction::FsWrite
    } else if lower.contains("post")
        || lower.contains("send")
        || lower.contains("upload")
        || lower.contains("exfil")
        || lower.contains("webhook")
    {
        ToolAction::NetExfil
    } else {
        ToolAction::ReadOnlySafe
    }
}

fn is_untrusted_source_tool(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("web")
        || lower.contains("fetch")
        || lower.contains("http")
        || lower.contains("url")
        || lower.contains("browser")
        || lower.contains("scrape")
        || lower.contains("search")
        || lower.contains("email")
        || lower.contains("mail")
        || lower.contains("slack")
        || lower.contains("issue")
        || lower.contains("external")
}

fn parse_action_name(s: &str) -> Option<ToolAction> {
    match s {
        "shell_exec" => Some(ToolAction::ShellExec),
        "cred_read" => Some(ToolAction::CredRead),
        "fs_write" => Some(ToolAction::FsWrite),
        "net_exfil" => Some(ToolAction::NetExfil),
        "read_only" => Some(ToolAction::ReadOnlySafe),
        _ => None,
    }
}

struct ProxyState {
    ledger: TrustLedger,
    tainted_context_cids: Vec<[u8; 34]>,
    pending_calls: std::collections::HashMap<String, String>, // rpc_id -> tool_name
    wal: Option<WalStore>,
    owner_pubkey: Option<[u8; 32]>,
}

fn run_mcp_proxy(
    owner_pubkey: Option<[u8; 32]>,
    wal_path: Option<&str>,
    cmd_args: &[String],
) -> ! {
    if cmd_args.is_empty() {
        eprintln!("mcp-proxy requires `-- <mcp_cmd> [args...]`");
        process::exit(2);
    }
    let wal = wal_path.map(|p| {
        WalStore::open(Path::new(p), true).unwrap_or_else(|e| {
            eprintln!("Failed to open audit WAL {}: {}", p, e);
            process::exit(1);
        })
    });

    let mut child = Command::new(&cmd_args[0])
        .args(&cmd_args[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| {
            eprintln!("Failed to spawn MCP server '{}': {}", cmd_args[0], e);
            process::exit(1);
        });

    let mut child_in = child.stdin.take().unwrap();
    let child_out = child.stdout.take().unwrap();

    let state = Arc::new(Mutex::new(ProxyState {
        ledger: TrustLedger::new(),
        tainted_context_cids: Vec::new(),
        pending_calls: std::collections::HashMap::new(),
        wal,
        owner_pubkey,
    }));

    // Reader thread: MCP Server stdout -> wrap external tool outputs in 0x04 COWR frames -> Agent stdout
    let state_resp = Arc::clone(&state);
    let reader_handle = std::thread::spawn(move || {
        let reader = BufReader::new(child_out);
        let mut host_out = std::io::stdout();
        for line in reader.lines() {
            let Ok(l) = line else { break };
            if l.trim().is_empty() {
                continue;
            }
            let mut out_line = l.clone();
            if let Ok(mut val) = parse_json_to_value(&l) {
                let id_key = val.get("id").map(value_to_compact_json);
                let is_result = val.get("result").is_some();
                if let (Some(idk), true) = (id_key, is_result) {
                    let mut st = state_resp.lock().unwrap();
                    if let Some(tool_name) = st.pending_calls.remove(&idk) {
                        let resp_auth = if is_untrusted_source_tool(&tool_name) {
                            AUTH_UNTRUSTED_EXT
                        } else {
                            AUTH_VERIFIED_TOOL
                        };
                        let tainted = cowrie_core::trust::is_tainted(resp_auth);
                        if let Ok(frame) = encode_frame(&val, resp_auth, EPI_OBSERVED) {
                            let cid = content_address(&frame);
                            st.ledger.record(cid, resp_auth, &[]);
                            if tainted {
                                st.tainted_context_cids.push(cid);
                            }
                            if let Some(w) = st.wal.as_mut() {
                                let _ = w.append_frame(&frame, true);
                            }
                            if let Value::Map(ref mut entries) = val {
                                entries.push((
                                    "_cowrie_cid".to_string(),
                                    Value::Str(to_hex(&cid)),
                                ));
                                entries.push((
                                    "_cowrie_tainted".to_string(),
                                    Value::Bool(tainted),
                                ));
                                out_line = value_to_compact_json(&val);
                            }
                        }
                    }
                }
            }
            let _ = writeln!(host_out, "{}", out_line);
            let _ = host_out.flush();
        }
    });

    // Main loop: Agent stdin -> enforce causal 0x80 + Ed25519 gate on privileged tools/call -> MCP Server stdin
    let stdin = std::io::stdin();
    let mut host_out = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(l) = line else { break };
        if l.trim().is_empty() {
            continue;
        }
        let mut forward = true;
        if let Ok(val) = parse_json_to_value(&l) {
            let method = val.get("method").and_then(|v| v.as_str()).unwrap_or("");
            if method == "tools/call" {
                let id_json = val
                    .get("id")
                    .map(value_to_compact_json)
                    .unwrap_or_else(|| "null".to_string());
                let params = val.get("params");
                let tool_name = params
                    .and_then(|p| p.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let action = classify_mcp_tool(tool_name);

                let mut st = state.lock().unwrap();
                if action.is_privileged() && !st.tainted_context_cids.is_empty() {
                    // Parse optional `_cowrie_declassify`: [{"cid":"...","expiry":...,"sig":"..."}]
                    let mut tokens = Vec::new();
                    if let Some(Value::Array(arr)) = params.and_then(|p| p.get("_cowrie_declassify")) {
                        for item in arr {
                            let cid_s = item.get("cid").and_then(|v| v.as_str()).unwrap_or("");
                            let sig_s = item.get("sig").and_then(|v| v.as_str()).unwrap_or("");
                            let exp_u = match item.get("expiry") {
                                Some(Value::Int(n)) if *n >= 0 => Some(*n as u64),
                                Some(Value::Uint(u)) => Some(*u),
                                _ => None,
                            };
                            if let (Some(cid_b), Some(sig_b), Some(expiry_unix_secs)) = (
                                parse_hex_exact::<34>(cid_s),
                                parse_hex_exact::<64>(sig_s),
                                exp_u,
                            ) {
                                tokens.push(DeclassificationToken {
                                    target_cid: cid_b,
                                    action,
                                    expiry_unix_secs,
                                    signature: sig_b,
                                });
                            }
                        }
                    }

                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let tainted_snapshot = st.tainted_context_cids.clone();
                    let pk_opt = st.owner_pubkey;
                    if st
                        .ledger
                        .check_tool_call_signed(
                            action,
                            &tainted_snapshot,
                            &tokens,
                            pk_opt.as_ref(),
                            now_secs,
                        )
                        .is_err()
                    {
                        forward = false;
                        let first_cid = to_hex(&tainted_snapshot[0]);
                        let err_resp = format!(
                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":-32025,\"message\":\"ERR_TAINTED_EXECUTION_BLOCKED\",\"data\":{{\"tool\":\"{}\",\"tainted_cid\":\"{}\"}}}}}}",
                            id_json, tool_name, first_cid
                        );
                        let _ = writeln!(host_out, "{}", err_resp);
                        let _ = host_out.flush();
                    }
                }

                if forward {
                    st.pending_calls.insert(id_json, tool_name.to_string());
                }
            }
        }
        if forward {
            if writeln!(child_in, "{}", l).is_err() || child_in.flush().is_err() {
                break;
            }
        }
    }

    drop(child_in);
    let _ = reader_handle.join();
    let status = child.wait().map(|s| s.code().unwrap_or(0)).unwrap_or(0);
    process::exit(status);
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
            println!("Plane D:      footer_off={} | Merkle OK: {} (bad_frames={:?}, bad_tensors={:?})", rep.header.footer_off, rep.ok, rep.bad_frames, rep.bad_tensors);
            println!("Merkle Root:  {}", to_hex(&rep.computed_root));
            if (rep.header.flags & FLAG_STATEPAGE) != 0 {
                if let Ok(sp) = parse_statepage_meta(&buf) {
                    println!(
                        "StatePage:    ctx_tokens={} state_len={} B runtime='{}' hmac={} model_sha={}...",
                        sp.ctx_tokens,
                        sp.state_len,
                        sp.runtime_id,
                        sp.hmac_present,
                        &sp.model_sha[..16]
                    );
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
                if let Err(e) = verify_frame_zero_alloc(fb) {
                    eprintln!("VERIFY FAIL: frame[{}] invalid canonical encoding: {}", idx, e);
                    process::exit(1);
                }
            }
            if !rep.ok {
                eprintln!(
                    "VERIFY FAIL: ERR_ROOT_MISMATCH | bad_frames={:?} bad_tensors={:?}",
                    rep.bad_frames, rep.bad_tensors
                );
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
                eprintln!(
                    "Refusing lens projection on corrupt shard (bad_frames={:?}, bad_tensors={:?})",
                    rep.bad_frames, rep.bad_tensors
                );
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
                3 => "F32",
                _ => "CUSTOM",
            };
            let ssm_lbl = match mp.meta.ssm_dtype {
                0 => "FP16",
                1 => "BF16",
                2 => "FP32",
                _ => "CUSTOM",
            };
            let hm_lbl = if mp.meta.hash_mode == 1 { "parallel-4MiB-merkle-sha256" } else { "flat-sha256" };
            println!("StatePage:    {} (16 KB aligned: {})", args[2], mp.meta.state_offset as usize % PAGE_ALIGN == 0);
            println!("Context Toks: {} | KV Quant: {} | SSM Dtype: {} | Runtime: '{}'", mp.meta.ctx_tokens, kv_lbl, ssm_lbl, mp.meta.runtime_id);
            println!("Integrity:    {} (HW SHA-256: {}, HMAC: {})", hm_lbl, cowrie_core::sha256::has_hw_sha256(), mp.meta.hmac_present);
            println!("State Bytes:  {} B ({:.2} MB) @ off={}", mp.meta.state_len, mp.meta.state_len as f64 / 1e6, mp.meta.state_offset);
            println!("Model SHA:    {}", mp.meta.model_sha);
            println!("Doc CID:      {}", mp.meta.doc_cid);
            println!("State Digest: {} (verified={})", mp.meta.state_sha, verify_hash);
        }

        "model-fp" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let fp = fingerprint_model_file(Path::new(&args[2])).unwrap_or_else(|e| {
                eprintln!("MODEL-FP FAIL: {}", e);
                process::exit(1);
            });
            println!("{}", fp);
        }

        "ed25519-pubkey" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let Some(seed) = parse_hex_exact::<32>(&args[2]) else {
                eprintln!("Expected 64-hex secret seed");
                process::exit(2);
            };
            println!("{}", to_hex(&ed25519_public_key(&seed)));
        }

        "declassify-sign" => {
            if args.len() < 6 {
                usage();
                process::exit(1);
            }
            let Some(seed) = parse_hex_exact::<32>(&args[2]) else {
                eprintln!("Expected 64-hex secret seed");
                process::exit(2);
            };
            let Some(cid) = parse_hex_exact::<34>(&args[3]) else {
                eprintln!("Expected 68-hex 34-byte multihash CID");
                process::exit(2);
            };
            let Some(act) = parse_action_name(&args[4]) else {
                eprintln!("Unknown action '{}'", args[4]);
                process::exit(2);
            };
            let expiry: u64 = args[5].parse().unwrap_or_else(|_| {
                eprintln!("Invalid expiry unix timestamp");
                process::exit(2);
            });
            let tok = DeclassificationToken::sign(&seed, cid, act, expiry);
            println!("{}", to_hex(&tok.signature));
        }

        "mcp-proxy" => {
            let mut owner_pk: Option<[u8; 32]> = None;
            let mut wal_path: Option<String> = None;
            let mut sep = None;
            let mut i = 2;
            while i < args.len() {
                if args[i] == "--" {
                    sep = Some(i + 1);
                    break;
                } else if args[i] == "--owner-pubkey" && i + 1 < args.len() {
                    owner_pk = parse_hex_exact::<32>(&args[i + 1]);
                    if owner_pk.is_none() {
                        eprintln!("Invalid --owner-pubkey (expected 64-hex Ed25519 public key)");
                        process::exit(2);
                    }
                    i += 2;
                } else if args[i] == "--audit-wal" && i + 1 < args.len() {
                    wal_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            let Some(cmd_start) = sep else {
                usage();
                process::exit(2);
            };
            run_mcp_proxy(owner_pk, wal_path.as_deref(), &args[cmd_start..]);
        }

        "bench-live-ivf" => {
            if args.len() < 3 {
                process::exit(2);
            }
            let dir = Path::new(&args[2]);
            fs::create_dir_all(dir).unwrap();
            let base_p = dir.join("base_50k.cwiv");
            let wal_p = dir.join("delta.cwwal");
            let comp_p = dir.join("compacted_50k.cwiv");
            let _ = fs::remove_file(&base_p);
            let _ = fs::remove_file(&wal_p);
            let _ = fs::remove_file(&comp_p);

            let n_clusters = 64usize;
            let dim = 128usize;
            let n_base = 50_000usize;
            let n_delta = 500usize;

            let mut centroids = vec![0i8; n_clusters * dim];
            for c in 0..n_clusters {
                centroids[c * dim + (c % dim)] = 100;
            }
            let mut clusters = vec![Vec::new(); n_clusters];
            let mut metas = vec![Vec::new(); n_clusters];
            for i in 0..n_base {
                let c = i % n_clusters;
                let mut v = vec![((i * 13 + 7) % 19) as i8 - 9; dim];
                v[c % dim] = 95;
                clusters[c].extend_from_slice(&v);
                metas[c].push(cowrie_core::ivf::VecMeta {
                    orig_id: i as u32,
                    scale: 1.0,
                    conf: 0.90,
                    auth: if i % 25 == 0 { AUTH_UNTRUSTED_EXT } else { AUTH_VERIFIED_TOOL },
                    pad: [0; 3],
                });
            }
            let refs: Vec<(&[i8], &[cowrie_core::ivf::VecMeta])> = clusters
                .iter()
                .zip(metas.iter())
                .map(|(e, m)| (e.as_slice(), m.as_slice()))
                .collect();
            cowrie_core::ivf::write_colocated_ivf_shard(&base_p, dim as u32, &centroids, &refs).unwrap();

            let mut store = cowrie_core::wal::LiveIvfStore::open(&base_p, &wal_p).unwrap();
            let t_ins0 = std::time::Instant::now();
            for j in 0..n_delta {
                let mut v = vec![3i8; dim];
                v[7] = 115;
                let m = cowrie_core::ivf::VecMeta {
                    orig_id: (n_base + j) as u32,
                    scale: 1.0,
                    conf: 0.96,
                    auth: AUTH_VERIFIED_TOOL,
                    pad: [0; 3],
                };
                store.append_vector(&v, m, j + 1 == n_delta).unwrap();
            }
            let insert_500_ms = t_ins0.elapsed().as_secs_f64() * 1000.0;

            let mut q = vec![2i8; dim];
            q[7] = 120;
            let mut search_us = Vec::with_capacity(31);
            let mut bread_last = 0usize;
            for _ in 0..31 {
                let t0 = std::time::Instant::now();
                let (hits, bread) = store.search_trusted(&q, 8, 20, 0.5).unwrap();
                search_us.push(t0.elapsed().as_secs_f64() * 1e6);
                bread_last = bread;
                assert!(hits[0].orig_id >= n_base as u32);
            }
            search_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let t_cmp0 = std::time::Instant::now();
            let compacted_bytes = store.compact_to_cwiv(&comp_p).unwrap();
            let compact_ms = t_cmp0.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(compacted_bytes % 16384, 0);

            // Also microbench Ed25519 sign + verify_strict in release Rust
            let seed = [0x42u8; 32];
            let pk = ed25519_public_key(&seed);
            let cid = [0x12u8; 34];
            let mut sign_us = Vec::with_capacity(201);
            let mut ver_us = Vec::with_capacity(201);
            for _ in 0..201 {
                let ts = std::time::Instant::now();
                let tok = DeclassificationToken::sign(&seed, cid, ToolAction::ShellExec, 2_000_000_000);
                sign_us.push(ts.elapsed().as_secs_f64() * 1e6);
                let tv = std::time::Instant::now();
                tok.verify(&pk, ToolAction::ShellExec, 1_900_000_000).unwrap();
                ver_us.push(tv.elapsed().as_secs_f64() * 1e6);
            }
            sign_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
            ver_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let _ = fs::remove_file(&base_p);
            let _ = fs::remove_file(&wal_p);
            let _ = fs::remove_file(&comp_p);

            println!(
                "{{\"base_vecs\":{},\"delta_vecs\":{},\"dim\":{},\"delta_insert_500_fsync_ms\":{:.3},\"hybrid_search_median_us\":{:.2},\"hybrid_bytes_read\":{},\"compact_50500_to_cwiv_ms\":{:.2},\"ed25519_sign_median_us\":{:.2},\"ed25519_verify_median_us\":{:.2}}}",
                n_base,
                n_delta,
                dim,
                insert_500_ms,
                search_us[15],
                bread_last,
                compact_ms,
                sign_us[100],
                ver_us[100]
            );
        }

        _ => {
            usage();
            process::exit(1);
        }
    }
}
