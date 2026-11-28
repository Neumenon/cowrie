//! `cowrie` CLI (v1.2): human inspection, zero-alloc verification, R0/R1/R2 lenses,
//! strided model fingerprinting, `Ed25519` keygen & declassification signing, and
//! fail-closed `mcp-proxy` zero-trust JSON-RPC wire firewall for unmodified MCP servers.

use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::path::Path;
use std::process::{self, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cowrie_core::canonical::{
    decode_frame_strict, encode_frame, parse_json_to_value, parse_plain_json_to_value,
    value_to_compact_json, value_to_plain_json, verify_frame_zero_alloc, Value,
};
use cowrie_core::ed25519::{ed25519_public_key, DeclassificationToken};
use cowrie_core::lens::{
    alpha_handle, extract_field_json, project_r0, project_r1, project_r2, project_r2_compact,
    project_r3, project_virtual_atlas, query_frame_slice,
};
use cowrie_core::sha256::{content_address, to_hex};
#[cfg(unix)]
use cowrie_core::shard::fingerprint_model_file;
use cowrie_core::shard::{
    build_shard_16kb, parse_statepage_meta, resolved_frame_cid, verify_shard_and_localize,
    FLAG_STATEPAGE, PAGE_ALIGN,
};
use cowrie_core::trust::{
    authority_label, ToolAction, TrustLedger, AUTH_MODEL_SYNTH, AUTH_SYS_POLICY,
    AUTH_UNTRUSTED_EXT, AUTH_VERIFIED_TOOL, EPI_OBSERVED,
};
#[cfg(unix)]
use cowrie_core::wal::WalStore;

fn usage() {
    eprintln!(
        "cowrie {} (Cowrie v1.2 CLI & Zero-Trust MCP Wire Firewall)\n\
         Usage:\n\
         \x20 cowrie mcp-proxy [--mode enforce|observe] [--owner-pubkey <64-hex>] [--audit-wal <path.cwwal>] [--policy <policy.json>] [--trust-server-annotations] [--virtualize | --virtualize-bytes <N>] [--quiet] -- <mcp_cmd> [args...]\n\
         \x20 cowrie mcp-config [--client claude-code|claude-desktop|cursor] [--bin cowrie|npx] [--mode enforce|observe] [--virtualize | --virtualize-bytes <N>] [--trust-server-annotations] [--policy <path>] [--audit-wal <path>] -- <server-name> <mcp_cmd> [args...]\n\
         \x20 cowrie pack <in.json> <out.cowrie> [--auth policy|verified|untrusted|synth] [--raw]\n\
         \x20 cowrie pack-batch <out.cowrie> [--auth ...] (--jsonl <in.jsonl> | <in1.json> [<in2.json> ...])\n\
         \x20 cowrie inspect <file.cowrie>\n\
         \x20 cowrie verify <file.cowrie>\n\
         \x20 cowrie lens <file.cowrie> [--tier r0|r1|r2|r2-compact|r3]\n\
         \x20 cowrie get <file.cowrie> <path> [--frame <idx>]\n\
         \x20 cowrie state-inspect <workspace.state.cowrie> [--verify-hash]\n\
         \x20 cowrie model-fp <weights.gguf|safetensors>\n\
         \x20 cowrie keygen [--out-seed-file <path>]\n\
         \x20 cowrie ed25519-pubkey (<seed-64-hex> | --seed-file <path|->)\n\
         \x20 cowrie declassify-sign (<seed-64-hex> | --seed-file <path|->) <cid-68-hex> <shell_exec|cred_read|fs_write|net_exfil> <expiry_unix>\n\
         \x20 cowrie --version | --help",
        env!("CARGO_PKG_VERSION")
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
    let trimmed = s.trim();
    if trimmed.len() != N * 2 || !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; N];
    for i in 0..N {
        out[i] = u8::from_str_radix(&trimmed[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn read_seed_from_file_or_stdin(spec: &str) -> [u8; 32] {
    let raw = if spec == "-" {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s).unwrap_or_else(|e| {
            eprintln!("Failed to read seed from stdin: {}", e);
            process::exit(2);
        });
        s
    } else {
        fs::read_to_string(spec).unwrap_or_else(|e| {
            eprintln!("Failed to read seed file '{}': {}", spec, e);
            process::exit(2);
        })
    };
    parse_hex_exact::<32>(&raw).unwrap_or_else(|| {
        eprintln!("Invalid seed material (expected 64-hex Ed25519 seed)");
        process::exit(2);
    })
}

#[cfg(unix)]
fn generate_random_seed_32() -> [u8; 32] {
    let mut seed = [0u8; 32];
    let mut f = fs::File::open("/dev/urandom").unwrap_or_else(|e| {
        eprintln!("Failed to open /dev/urandom: {}", e);
        process::exit(1);
    });
    f.read_exact(&mut seed).unwrap_or_else(|e| {
        eprintln!("Failed to read entropy from /dev/urandom: {}", e);
        process::exit(1);
    });
    seed
}

#[cfg(not(unix))]
fn generate_random_seed_32() -> [u8; 32] {
    eprintln!("keygen requires /dev/urandom on Unix");
    process::exit(1);
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

/// Normalize JSON-RPC `id` so `1`, `1.0`, and `"1"` do not collide or desync across JSON implementations.
fn normalize_rpc_id(v: &Value) -> String {
    match v {
        Value::Int(n) => format!("n:{}", n),
        Value::Uint(u) => format!("n:{}", u),
        Value::FloatBits(bits) => {
            let f = f64::from_bits(*bits);
            if f.is_finite() && f.fract() == 0.0 && f >= (i64::MIN as f64) && f <= (i64::MAX as f64) {
                format!("n:{}", f as i64)
            } else {
                format!("f:{}", bits)
            }
        }
        Value::Str(s) => format!("s:{}", s),
        Value::Null => "null".to_string(),
        other => format!("o:{}", value_to_compact_json(other)),
    }
}

fn is_explicit_local_readonly_tool(lower: &str) -> bool {
    matches!(
        lower,
        "read_file"
            | "get_file"
            | "list_dir"
            | "list_directory"
            | "list_files"
            | "ls"
            | "stat"
            | "grep"
            | "rg"
            | "search_files"
            | "find_files"
            | "inspect"
            | "status"
            | "get_status"
            | "ping"
            | "echo"
            | "add"
            | "get_time"
            | "health"
            | "help"
            | "describe"
            | "version"
            | "list_tables"
            | "query_readonly"
    )
}

#[derive(Default)]
struct HostToolPolicy {
    allow_readonly: HashSet<String>,
    untrusted_sources: HashSet<String>,
    privileged_overrides: HashMap<String, ToolAction>,
    trust_server_annotations: bool,
}

impl HostToolPolicy {
    fn load(path: &str, trust_server_annotations: bool) -> Self {
        let raw = fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("Failed to read --policy '{}': {}", path, e);
            process::exit(2);
        });
        let val = parse_plain_json_to_value(&raw).unwrap_or_else(|e| {
            eprintln!("Invalid JSON in --policy '{}': {}", path, e);
            process::exit(2);
        });
        let mut pol = HostToolPolicy {
            trust_server_annotations,
            ..Default::default()
        };
        if let Some(Value::Array(arr)) = val.get("allow_readonly") {
            for item in arr {
                if let Some(s) = item.as_str() {
                    pol.allow_readonly.insert(s.to_string());
                }
            }
        }
        if let Some(Value::Array(arr)) = val.get("untrusted_sources") {
            for item in arr {
                if let Some(s) = item.as_str() {
                    pol.untrusted_sources.insert(s.to_string());
                }
            }
        }
        if let Some(Value::Map(entries)) = val.get("privileged") {
            for (k, v) in entries {
                if let Some(act_s) = v.as_str() {
                    if let Some(act) = parse_action_name(act_s) {
                        pol.privileged_overrides.insert(k.clone(), act);
                    } else {
                        eprintln!("Invalid action '{}' for tool '{}' in --policy", act_s, k);
                        process::exit(2);
                    }
                }
            }
        }
        pol
    }
}

/// Default-DENY MCP tool classifier:
/// 1. Host `--policy` `privileged` overrides win unconditionally.
/// 2. Explicit network / exfil / shell / credential / filesystem-write verbs always classify privileged.
/// 3. Host `--policy` `allow_readonly`, built-in local read-only allowlist, OR (ONLY if
///    `--trust-server-annotations` was explicitly enabled by the operator) MCP `tools/list` `annotations`
///    classify `ReadOnlySafe`. Server-supplied `readOnlyHint` NEVER bypasses default-deny by default!
/// 4. Any unknown tool defaults to `ToolAction::ShellExec` (fail-closed!).
fn classify_mcp_tool(
    tool_name: &str,
    readonly_hints: &HashMap<String, bool>,
    policy: &HostToolPolicy,
) -> ToolAction {
    if let Some(&act) = policy.privileged_overrides.get(tool_name) {
        return act;
    }
    let lower = tool_name.to_ascii_lowercase();
    if lower.contains("shell")
        || lower.contains("bash")
        || lower.contains("exec")
        || lower.contains("command")
        || lower.contains("eval")
        || lower.contains("spawn")
        || lower.contains("terminal")
    {
        ToolAction::ShellExec
    } else if lower.contains("cred")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("passwd")
        || lower.contains("apikey")
        || lower.contains("api_key")
    {
        ToolAction::CredRead
    } else if lower.contains("write")
        || lower.contains("delete")
        || lower.contains("remove")
        || lower.contains("edit")
        || lower.contains("patch")
        || lower.contains("chmod")
        || lower.contains("create_file")
        || lower.contains("mkdir")
    {
        ToolAction::FsWrite
    } else if lower.contains("post")
        || lower.contains("send")
        || lower.contains("upload")
        || lower.contains("exfil")
        || lower.contains("webhook")
        || lower.contains("http")
        || lower.contains("fetch")
        || lower.contains("url")
        || lower.contains("curl")
        || lower.contains("web")
        || lower.contains("browser")
        || lower.contains("scrape")
        || lower.contains("email")
        || lower.contains("mail")
        || lower.contains("slack")
    {
        ToolAction::NetExfil
    } else if policy.allow_readonly.contains(tool_name)
        || is_explicit_local_readonly_tool(&lower)
        || (policy.trust_server_annotations && readonly_hints.get(tool_name).copied().unwrap_or(false))
    {
        ToolAction::ReadOnlySafe
    } else {
        // Default-DENY: unknown tools require explicit host `--policy`, clean lineage, or Ed25519 token.
        ToolAction::ShellExec
    }
}

fn is_untrusted_source_tool(
    name: &str,
    open_world_hints: &HashMap<String, bool>,
    policy: &HostToolPolicy,
) -> bool {
    if policy.untrusted_sources.contains(name) || open_world_hints.get(name).copied().unwrap_or(false) {
        return true;
    }
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
        || lower.contains("curl")
        || lower.contains("github")
        || lower.contains("jira")
        || lower.contains("rss")
}

#[derive(Clone, Debug)]
enum PendingKind {
    ToolCall(String),
    ToolsList,
    ResourceRead,
    PromptGet,
}

const MAX_VIRTUAL_ENTRIES: usize = 512;
const DEFAULT_VIRTUALIZE_BYTES: usize = 16_384;

struct VirtualEntry {
    cid: [u8; 34],
    frame: Vec<u8>,
    authority: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyMode {
    Enforce,
    Observe,
}

struct ProxyState {
    ledger: TrustLedger,
    tainted_context_cids: Vec<[u8; 34]>,
    pending_calls: HashMap<String, VecDeque<PendingKind>>,
    tool_readonly_hints: HashMap<String, bool>,
    tool_open_world_hints: HashMap<String, bool>,
    policy: HostToolPolicy,
    mode: ProxyMode,
    quiet: bool,
    wal_failed: bool,
    #[cfg(unix)]
    wal: Option<WalStore>,
    owner_pubkey: Option<[u8; 32]>,
    virtualize_bytes: usize,
    virtual_store: HashMap<String, VirtualEntry>,
    virtual_order: VecDeque<String>,
    total_raw_bytes: u64,
    total_atlas_bytes: u64,
}

impl ProxyState {
    fn store_virtual(&mut self, handle: String, entry: VirtualEntry) {
        if !self.virtual_store.contains_key(&handle) {
            if self.virtual_order.len() >= MAX_VIRTUAL_ENTRIES {
                if let Some(old) = self.virtual_order.pop_front() {
                    self.virtual_store.remove(&old);
                }
            }
            self.virtual_order.push_back(handle.clone());
        }
        self.virtual_store.insert(handle, entry);
    }
}

fn unwrap_mcp_result_payload(val: &Value) -> Option<(Value, usize)> {
    let res = val.get("result")?;
    // Standard MCP `tools/call`: result.content = [{type: "text", text: "..."}]
    if let Some(Value::Array(blocks)) = res.get("content") {
        let mut texts = Vec::new();
        let mut all_text = !blocks.is_empty();
        for b in blocks {
            if b.get("type").and_then(|v| v.as_str()) == Some("text") {
                if let Some(t) = b.get("text").and_then(|v| v.as_str()) {
                    texts.push(t);
                    continue;
                }
            }
            all_text = false;
            break;
        }
        if all_text {
            let joined = texts.join("\n");
            let raw_bytes = joined.len();
            let trimmed = joined.trim();
            if (trimmed.starts_with('{') && trimmed.ends_with('}'))
                || (trimmed.starts_with('[') && trimmed.ends_with(']'))
            {
                if let Ok(parsed) = parse_plain_json_to_value(trimmed) {
                    return Some((parsed, raw_bytes));
                }
            }
            return Some((Value::Map(vec![("text".to_string(), Value::Str(joined))]), raw_bytes));
        }
    }
    // MCP `resources/read`: result.contents = [{text: "..."}]
    if let Some(Value::Array(blocks)) = res.get("contents") {
        let mut texts = Vec::new();
        for b in blocks {
            if let Some(t) = b.get("text").and_then(|v| v.as_str()) {
                texts.push(t);
            }
        }
        if !texts.is_empty() {
            let joined = texts.join("\n");
            let raw_bytes = joined.len();
            let trimmed = joined.trim();
            if (trimmed.starts_with('{') && trimmed.ends_with('}'))
                || (trimmed.starts_with('[') && trimmed.ends_with(']'))
            {
                if let Ok(parsed) = parse_plain_json_to_value(trimmed) {
                    return Some((parsed, raw_bytes));
                }
            }
            return Some((Value::Map(vec![("text".to_string(), Value::Str(joined))]), raw_bytes));
        }
    }
    let raw_j = value_to_plain_json(res);
    Some((res.clone(), raw_j.len()))
}

fn replace_result_with_virtual_atlas(
    val: &mut Value,
    atlas: String,
    handle: String,
    cid_hex: String,
    tainted: bool,
    raw_bytes: usize,
) {
    let atlas_bytes = atlas.len();
    let cowrie_meta = Value::Map(vec![
        ("atlas_bytes".to_string(), Value::Int(atlas_bytes as i64)),
        ("cid".to_string(), Value::Str(cid_hex)),
        ("handle".to_string(), Value::Str(handle)),
        ("raw_bytes".to_string(), Value::Int(raw_bytes as i64)),
        ("tainted".to_string(), Value::Bool(tainted)),
        ("virtualized".to_string(), Value::Bool(true)),
    ]);
    let content_arr = Value::Array(vec![Value::Map(vec![
        ("text".to_string(), Value::Str(atlas)),
        ("type".to_string(), Value::Str("text".to_string())),
    ])]);
    if let Value::Map(ref mut root_entries) = val {
        for (rk, rv) in root_entries.iter_mut() {
            if rk == "result" {
                if let Value::Map(ref mut res_entries) = rv {
                    res_entries.retain(|(k, _)| k != "content" && k != "contents" && k != "_meta");
                    res_entries.push(("content".to_string(), content_arr));
                    res_entries.push((
                        "_meta".to_string(),
                        Value::Map(vec![("cowrie".to_string(), cowrie_meta)]),
                    ));
                    return;
                }
            }
        }
    }
}

fn inject_cowrie_get_tool(val: &mut Value) {
    let schema = parse_plain_json_to_value(
        r#"{"name":"cowrie_get","description":"Retrieve an exact RFC 6901 JSON Pointer, line range, or bounded grep slice from a virtualized Cowrie handle (preserving causal taint).","annotations":{"readOnlyHint":true,"destructiveHint":false,"openWorldHint":false},"inputSchema":{"type":"object","required":["handle"],"properties":{"handle":{"type":"string","description":"10-letter Cowrie handle from VIRTUALIZED_TOOL_OUTPUT (e.g. @abcdefghij)"},"pointer":{"type":"string","description":"Optional RFC 6901 JSON Pointer (e.g. /results/3, /output, /text)"},"grep":{"type":"string","description":"Optional substring or bounded wildcard pattern to search inside the target"},"lines":{"type":"array","items":{"type":"integer"},"description":"Optional 1-based inclusive [start, end] line range"},"offset":{"type":"integer","description":"Optional char offset for pagination"},"max_chars":{"type":"integer","description":"Optional max output characters (default 2400, max 8192)"}}}}"#
    );
    let Ok(tool_def) = schema else { return };
    if let Value::Map(ref mut root_entries) = val {
        for (rk, rv) in root_entries.iter_mut() {
            if rk == "result" {
                if let Value::Map(ref mut res_entries) = rv {
                    for (tk, tv) in res_entries.iter_mut() {
                        if tk == "tools" {
                            if let Value::Array(ref mut tools) = tv {
                                let exists = tools.iter().any(|t| {
                                    t.get("name").and_then(|v| v.as_str()) == Some("cowrie_get")
                                });
                                if !exists {
                                    tools.push(tool_def);
                                }
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

fn handle_local_cowrie_get(id_json: &str, params: Option<&Value>, st: &mut ProxyState) -> String {
    let args = params.and_then(|p| p.get("arguments"));
    let Some(handle) = args.and_then(|a| a.get("handle")).and_then(|v| v.as_str()) else {
        return format_audit_err_jsonrpc(id_json, "ERR_INVALID_CONTRACT: cowrie_get requires `handle`");
    };
    let Some(entry) = st.virtual_store.get(handle) else {
        return format_audit_err_jsonrpc(
            id_json,
            &format!("ERR_INVALID_CONTRACT: unknown virtualized handle '{}'", handle),
        );
    };
    let cid = entry.cid;
    let auth = entry.authority;
    let frame = entry.frame.clone();
    let tainted = cowrie_core::trust::is_tainted(auth);
    if tainted && !st.tainted_context_cids.contains(&cid) {
        st.tainted_context_cids.push(cid);
    }

    let pointer = args.and_then(|a| a.get("pointer")).and_then(|v| v.as_str());
    let grep = args.and_then(|a| a.get("grep")).and_then(|v| v.as_str());
    let lines_range = args.and_then(|a| a.get("lines")).and_then(|v| match v {
        Value::Array(arr) if !arr.is_empty() => {
            let s = match arr.first() {
                Some(Value::Int(n)) if *n >= 1 => *n as usize,
                Some(Value::Uint(u)) if *u >= 1 => *u as usize,
                _ => return None,
            };
            let e = match arr.get(1) {
                Some(Value::Int(n)) if *n >= 1 => *n as usize,
                Some(Value::Uint(u)) if *u >= 1 => *u as usize,
                _ => s.saturating_add(30),
            };
            Some((s, e))
        }
        _ => None,
    });
    let offset = args
        .and_then(|a| a.get("offset"))
        .and_then(|v| match v {
            Value::Int(n) if *n >= 0 => Some(*n as usize),
            Value::Uint(u) => Some(*u as usize),
            _ => None,
        })
        .unwrap_or(0);
    let max_chars = args
        .and_then(|a| a.get("max_chars"))
        .and_then(|v| match v {
            Value::Int(n) if *n > 0 => Some(*n as usize),
            Value::Uint(u) if *u > 0 => Some(*u as usize),
            _ => None,
        })
        .unwrap_or(2400);

    let slice_text = match query_frame_slice(&frame, pointer, grep, lines_range, offset, max_chars) {
        Ok(s) => s,
        Err(e) => {
            return format_audit_err_jsonrpc(
                id_json,
                &format!("ERR_INVALID_CONTRACT: cowrie_get failed ({})", e),
            );
        }
    };

    let delivered = if tainted {
        format!(
            "<<<COWRIE:SLICE {} AUTH=0x{:02x}({}|TAINTED)>>>\n{}\n<<<END:COWRIE>>>",
            handle,
            auth,
            authority_label(auth),
            slice_text
        )
    } else {
        slice_text
    };

    let safe_text = value_to_plain_json(&Value::Str(delivered));
    let safe_handle = value_to_plain_json(&Value::Str(handle.to_string()));
    format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":{}}}],\"_meta\":{{\"cowrie\":{{\"cid\":\"{}\",\"handle\":{},\"tainted\":{}}}}}}}}}",
        id_json,
        safe_text,
        to_hex(&cid),
        safe_handle,
        if tainted { "true" } else { "false" }
    )
}

fn write_host_line(host_out: &Arc<Mutex<std::io::Stdout>>, line: &str) {
    if let Ok(mut out) = host_out.lock() {
        let _ = writeln!(out, "{}", line);
        let _ = out.flush();
    }
}

fn strip_cowrie_control_params(val: &Value) -> Value {
    let Value::Map(entries) = val else {
        return val.clone();
    };
    let mut new_entries = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        if k == "params" {
            if let Value::Map(p_entries) = v {
                let filtered: Vec<(String, Value)> = p_entries
                    .iter()
                    .filter(|(pk, _)| pk != "_cowrie_declassify" && pk != "_cowrie_parents")
                    .cloned()
                    .collect();
                new_entries.push((k.clone(), Value::Map(filtered)));
                continue;
            }
        }
        new_entries.push((k.clone(), v.clone()));
    }
    Value::Map(new_entries)
}

fn attach_result_meta_cowrie(val: &mut Value, cid_hex: String, tainted: bool) {
    let cowrie_meta = Value::Map(vec![
        ("cid".to_string(), Value::Str(cid_hex)),
        ("tainted".to_string(), Value::Bool(tainted)),
    ]);
    if let Value::Map(ref mut root_entries) = val {
        for (rk, rv) in root_entries.iter_mut() {
            if rk == "result" {
                if let Value::Map(ref mut res_entries) = rv {
                    for (mk, mv) in res_entries.iter_mut() {
                        if mk == "_meta" {
                            if let Value::Map(ref mut meta_entries) = mv {
                                meta_entries.retain(|(k, _)| k != "cowrie");
                                meta_entries.push(("cowrie".to_string(), cowrie_meta));
                                return;
                            }
                        }
                    }
                    res_entries.push((
                        "_meta".to_string(),
                        Value::Map(vec![("cowrie".to_string(), cowrie_meta)]),
                    ));
                    return;
                }
            }
        }
    }
}

fn format_blocked_jsonrpc(id_json: &str, tool_name: &str, tainted_cid_hex: &str) -> String {
    let safe_tool = value_to_plain_json(&Value::Str(tool_name.to_string()));
    format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":-32025,\"message\":\"ERR_TAINTED_EXECUTION_BLOCKED\",\"data\":{{\"tool\":{},\"tainted_cid\":\"{}\"}}}}}}",
        id_json, safe_tool, tainted_cid_hex
    )
}

fn format_audit_err_jsonrpc(id_json: &str, msg: &str) -> String {
    let safe_msg = value_to_plain_json(&Value::Str(msg.to_string()));
    format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":-32026,\"message\":{}}}}}",
        id_json, safe_msg
    )
}

fn format_parse_err_jsonrpc(msg: &str) -> String {
    let safe_msg = value_to_plain_json(&Value::Str(msg.to_string()));
    format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{{\"code\":-32700,\"message\":{}}}}}",
        safe_msg
    )
}

fn evaluate_single_mcp_request(
    val: &Value,
    st: &mut ProxyState,
) -> Result<(Value, Option<(String, PendingKind)>), String> {
    let Value::Map(_) = val else {
        return Err(format_parse_err_jsonrpc("ERR_INVALID_JSON: JSON-RPC request must be an object"));
    };
    let method = val.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let id_val = val.get("id");
    let id_json = id_val
        .map(value_to_plain_json)
        .unwrap_or_else(|| "null".to_string());
    let id_norm = id_val.map(normalize_rpc_id);

    match method {
        "tools/call" => {
            let params = val.get("params");
            let tool_name = params
                .and_then(|p| p.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // B2-b: Intercept synthetic local `cowrie_get` without round-tripping to the child MCP server!
            if st.virtualize_bytes > 0 && tool_name == "cowrie_get" {
                return Err(handle_local_cowrie_get(&id_json, params, st));
            }

            let action = classify_mcp_tool(tool_name, &st.tool_readonly_hints, &st.policy);

            if action.is_privileged() && st.wal_failed {
                return Err(format_audit_err_jsonrpc(
                    &id_json,
                    "ERR_AUDIT_WAL_FAILED: refusing privileged tool call after audit log failure",
                ));
            }

            // Determine causal lineage to check:
            // - If orchestrator explicitly supplies `params._cowrie_parents`, verify strictly against those parent CIDs!
            // - Otherwise fall back to conservative connection-wide taint set (`tainted_context_cids`).
            let explicit_parents_val = params.and_then(|p| p.get("_cowrie_parents"));
            let check_cids: Option<Vec<[u8; 34]>> = if let Some(pv) = explicit_parents_val {
                match pv {
                    Value::Array(arr) if !arr.is_empty() => {
                        let mut parsed = Vec::with_capacity(arr.len());
                        for item in arr {
                            let Some(s) = item.as_str() else {
                                return Err(format_blocked_jsonrpc(&id_json, tool_name, "invalid_parent_cid"));
                            };
                            let Some(cid_b) = parse_hex_exact::<34>(s) else {
                                return Err(format_blocked_jsonrpc(&id_json, tool_name, "invalid_parent_cid"));
                            };
                            parsed.push(cid_b);
                        }
                        Some(parsed)
                    }
                    _ => return Err(format_blocked_jsonrpc(&id_json, tool_name, "empty_parent_lineage")),
                }
            } else if !st.tainted_context_cids.is_empty() {
                Some(st.tainted_context_cids.clone())
            } else {
                None
            };

            if action.is_privileged() {
                if let Some(lineage) = check_cids {
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

                    // Fail-closed clock: if system time precedes UNIX_EPOCH, treat now as u64::MAX so tokens expire!
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(u64::MAX);
                    let pk_opt = st.owner_pubkey;
                    if st
                        .ledger
                        .check_tool_call_signed(action, &lineage, &tokens, pk_opt.as_ref(), now_secs)
                        .is_err()
                    {
                        let first_cid = to_hex(&lineage[0]);
                        match st.mode {
                            ProxyMode::Enforce => {
                                if !st.quiet {
                                    eprintln!(
                                        "[cowrie][enforce] BLOCKED tainted call '{}' (code=-32025, cid={}...)",
                                        tool_name,
                                        &first_cid[..16]
                                    );
                                }
                                return Err(format_blocked_jsonrpc(&id_json, tool_name, &first_cid));
                            }
                            ProxyMode::Observe => {
                                if !st.quiet {
                                    eprintln!(
                                        "[cowrie][observe] WOULD BLOCK tainted call '{}' (cid={}...) — pass --mode enforce to hard-block",
                                        tool_name,
                                        &first_cid[..16]
                                    );
                                }
                            }
                        }
                    }
                }
            }

            let sanitized = strip_cowrie_control_params(val);
            let pending = id_norm.map(|k| (k, PendingKind::ToolCall(tool_name.to_string())));
            Ok((sanitized, pending))
        }
        "tools/list" => {
            let pending = id_norm.map(|k| (k, PendingKind::ToolsList));
            Ok((val.clone(), pending))
        }
        "resources/read" => {
            let pending = id_norm.map(|k| (k, PendingKind::ResourceRead));
            Ok((val.clone(), pending))
        }
        "prompts/get" => {
            let pending = id_norm.map(|k| (k, PendingKind::PromptGet));
            Ok((val.clone(), pending))
        }
        _ => Ok((val.clone(), None)),
    }
}

fn process_mcp_response_obj(val: &mut Value, raw_line: &str, st: &mut ProxyState) -> Option<String> {
    let id_norm = val.get("id").map(normalize_rpc_id)?;
    let has_result = val.get("result").is_some();
    let has_error = val.get("error").is_some();
    if !has_result && !has_error {
        return None;
    }

    let pending = {
        let q = st.pending_calls.get_mut(&id_norm)?;
        let item = q.pop_front();
        if q.is_empty() {
            st.pending_calls.remove(&id_norm);
        }
        item?
    };

    match pending {
        PendingKind::ToolsList => {
            if let Some(Value::Array(tools)) = val.get("result").and_then(|r| r.get("tools")) {
                for t in tools {
                    if let Some(name) = t.get("name").and_then(|v| v.as_str()) {
                        let ann = t.get("annotations");
                        let ro = ann
                            .and_then(|a| a.get("readOnlyHint"))
                            .and_then(|v| match v {
                                Value::Bool(b) => Some(*b),
                                _ => None,
                            })
                            .unwrap_or(false);
                        let dest = ann
                            .and_then(|a| a.get("destructiveHint"))
                            .and_then(|v| match v {
                                Value::Bool(b) => Some(*b),
                                _ => None,
                            })
                            .unwrap_or(false);
                        let open_world = ann
                            .and_then(|a| a.get("openWorldHint"))
                            .and_then(|v| match v {
                                Value::Bool(b) => Some(*b),
                                _ => None,
                            })
                            .unwrap_or(false);
                        st.tool_readonly_hints
                            .insert(name.to_string(), ro && !dest && !open_world);
                        st.tool_open_world_hints.insert(name.to_string(), open_world);
                    }
                }
            }
            if st.virtualize_bytes > 0 {
                inject_cowrie_get_tool(val);
                return Some(value_to_plain_json(val));
            }
            None
        }
        PendingKind::ResourceRead | PendingKind::PromptGet => {
            // External resources and dynamic prompts are untrusted content -> always taint!
            let resp_auth = AUTH_UNTRUSTED_EXT;
            let (cid, frame_opt) = match encode_frame(val, resp_auth, EPI_OBSERVED) {
                Ok(frame) => (content_address(&frame), Some(frame)),
                Err(_) => (content_address(raw_line.as_bytes()), None),
            };
            st.ledger.record(cid, resp_auth, &[]);
            st.tainted_context_cids.push(cid);
            #[cfg(unix)]
            if let (Some(w), Some(frame)) = (st.wal.as_mut(), frame_opt.as_ref()) {
                if let Err(e) = w.append_frame(frame, true) {
                    st.wal_failed = true;
                    eprintln!("cowrie mcp-proxy audit WAL append failed: {}", e);
                    let id_json = val.get("id").map(value_to_plain_json).unwrap_or_else(|| "null".to_string());
                    return Some(format_audit_err_jsonrpc(&id_json, &format!("ERR_AUDIT_WAL_FAILED ({})", e)));
                }
            }
            let _ = frame_opt;
            if has_result {
                if st.virtualize_bytes > 0 {
                    if let Some((payload_val, raw_bytes)) = unwrap_mcp_result_payload(val) {
                        if raw_bytes >= st.virtualize_bytes {
                            if let Ok(p_frame) = encode_frame(&payload_val, resp_auth, EPI_OBSERVED) {
                                if let Ok(p_dec) = decode_frame_strict(&p_frame) {
                                    let p_cid = content_address(&p_frame);
                                    let handle = alpha_handle(&p_cid);
                                    let atlas = project_virtual_atlas(&p_frame, &p_dec, 1800);
                                    let atlas_len = atlas.len();
                                    st.total_raw_bytes = st.total_raw_bytes.saturating_add(raw_bytes as u64);
                                    st.total_atlas_bytes = st.total_atlas_bytes.saturating_add(atlas_len as u64);
                                    if !st.quiet {
                                        let pct = if raw_bytes > 0 {
                                            (1.0 - (atlas_len as f64 / raw_bytes as f64)) * 100.0
                                        } else {
                                            0.0
                                        };
                                        let est_tok = raw_bytes.saturating_sub(atlas_len) / 4;
                                        eprintln!(
                                            "[cowrie] virtualized resource/prompt {:.1} KB -> {:.1} KB (-{:.1}%, ~{} tok saved) handle={} tainted=true",
                                            raw_bytes as f64 / 1024.0,
                                            atlas_len as f64 / 1024.0,
                                            pct,
                                            est_tok,
                                            handle
                                        );
                                    }
                                    st.store_virtual(
                                        handle.clone(),
                                        VirtualEntry { cid, frame: p_frame, authority: resp_auth },
                                    );
                                    replace_result_with_virtual_atlas(val, atlas, handle, to_hex(&cid), true, raw_bytes);
                                    return Some(value_to_plain_json(val));
                                }
                            }
                        }
                    }
                }
                attach_result_meta_cowrie(val, to_hex(&cid), true);
                return Some(value_to_plain_json(val));
            }
            None
        }
        PendingKind::ToolCall(tool_name) => {
            let untrusted = is_untrusted_source_tool(&tool_name, &st.tool_open_world_hints, &st.policy);
            let resp_auth = if untrusted {
                AUTH_UNTRUSTED_EXT
            } else {
                AUTH_VERIFIED_TOOL
            };
            let tainted = cowrie_core::trust::is_tainted(resp_auth);
            let (cid, frame_opt) = match encode_frame(val, resp_auth, EPI_OBSERVED) {
                Ok(frame) => (content_address(&frame), Some(frame)),
                Err(_) => (content_address(raw_line.as_bytes()), None),
            };
            st.ledger.record(cid, resp_auth, &[]);
            if tainted {
                st.tainted_context_cids.push(cid);
            }
            #[cfg(unix)]
            if let (Some(w), Some(frame)) = (st.wal.as_mut(), frame_opt.as_ref()) {
                if let Err(e) = w.append_frame(frame, true) {
                    st.wal_failed = true;
                    eprintln!("cowrie mcp-proxy audit WAL append failed: {}", e);
                    let id_json = val.get("id").map(value_to_plain_json).unwrap_or_else(|| "null".to_string());
                    return Some(format_audit_err_jsonrpc(&id_json, &format!("ERR_AUDIT_WAL_FAILED ({})", e)));
                }
            }
            let _ = frame_opt;
            if has_result {
                if st.virtualize_bytes > 0 {
                    if let Some((payload_val, raw_bytes)) = unwrap_mcp_result_payload(val) {
                        if raw_bytes >= st.virtualize_bytes {
                            if let Ok(p_frame) = encode_frame(&payload_val, resp_auth, EPI_OBSERVED) {
                                if let Ok(p_dec) = decode_frame_strict(&p_frame) {
                                    let p_cid = content_address(&p_frame);
                                    let handle = alpha_handle(&p_cid);
                                    let atlas = project_virtual_atlas(&p_frame, &p_dec, 1800);
                                    let atlas_len = atlas.len();
                                    st.total_raw_bytes = st.total_raw_bytes.saturating_add(raw_bytes as u64);
                                    st.total_atlas_bytes = st.total_atlas_bytes.saturating_add(atlas_len as u64);
                                    if !st.quiet {
                                        let pct = if raw_bytes > 0 {
                                            (1.0 - (atlas_len as f64 / raw_bytes as f64)) * 100.0
                                        } else {
                                            0.0
                                        };
                                        let est_tok = raw_bytes.saturating_sub(atlas_len) / 4;
                                        eprintln!(
                                            "[cowrie] virtualized '{}' {:.1} KB -> {:.1} KB (-{:.1}%, ~{} tok saved) handle={} tainted={}",
                                            tool_name,
                                            raw_bytes as f64 / 1024.0,
                                            atlas_len as f64 / 1024.0,
                                            pct,
                                            est_tok,
                                            handle,
                                            tainted
                                        );
                                    }
                                    st.store_virtual(
                                        handle.clone(),
                                        VirtualEntry { cid, frame: p_frame, authority: resp_auth },
                                    );
                                    replace_result_with_virtual_atlas(val, atlas, handle, to_hex(&cid), tainted, raw_bytes);
                                    return Some(value_to_plain_json(val));
                                }
                            }
                        }
                    }
                }
                attach_result_meta_cowrie(val, to_hex(&cid), tainted);
                return Some(value_to_plain_json(val));
            }
            None
        }
    }
}

fn run_mcp_proxy(
    owner_pubkey: Option<[u8; 32]>,
    wal_path: Option<&str>,
    policy: HostToolPolicy,
    mode: ProxyMode,
    quiet: bool,
    virtualize_bytes: usize,
    cmd_args: &[String],
) -> ! {
    if cmd_args.is_empty() {
        eprintln!("mcp-proxy requires `-- <mcp_cmd> [args...]`");
        process::exit(2);
    }

    let resolved_pk = owner_pubkey.or_else(|| {
        env::var("COWRIE_OWNER_PUBKEY")
            .ok()
            .and_then(|s| parse_hex_exact::<32>(&s))
    });

    #[cfg(unix)]
    let wal = wal_path.map(|p| {
        WalStore::open(Path::new(p), true).unwrap_or_else(|e| {
            eprintln!("Failed to open audit WAL {}: {}", p, e);
            process::exit(1);
        })
    });
    #[cfg(not(unix))]
    if wal_path.is_some() {
        eprintln!("--audit-wal is only supported on Unix platforms");
        process::exit(2);
    }

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
    let host_out = Arc::new(Mutex::new(std::io::stdout()));

    let state = Arc::new(Mutex::new(ProxyState {
        ledger: TrustLedger::new(),
        tainted_context_cids: Vec::new(),
        pending_calls: HashMap::new(),
        tool_readonly_hints: HashMap::new(),
        tool_open_world_hints: HashMap::new(),
        policy,
        mode,
        quiet,
        wal_failed: false,
        #[cfg(unix)]
        wal,
        owner_pubkey: resolved_pk,
        virtualize_bytes,
        virtual_store: HashMap::new(),
        virtual_order: VecDeque::new(),
        total_raw_bytes: 0,
        total_atlas_bytes: 0,
    }));

    // Reader thread: MCP Server stdout -> canonicalize & taint external responses -> locked Agent stdout
    let state_resp = Arc::clone(&state);
    let host_out_resp = Arc::clone(&host_out);
    let reader_handle = std::thread::spawn(move || {
        let reader = BufReader::new(child_out);
        for line in reader.lines() {
            let Ok(l) = line else { break };
            if l.trim().is_empty() {
                continue;
            }
            let mut out_line = l.clone();
            match parse_plain_json_to_value(&l) {
                Ok(Value::Array(mut items)) => {
                    let mut st = state_resp.lock().unwrap();
                    let mut changed = false;
                    for item in items.iter_mut() {
                        if process_mcp_response_obj(item, &l, &mut st).is_some() {
                            changed = true;
                        }
                    }
                    if changed {
                        out_line = value_to_plain_json(&Value::Array(items));
                    }
                }
                Ok(mut val) => {
                    let mut st = state_resp.lock().unwrap();
                    if let Some(updated) = process_mcp_response_obj(&mut val, &l, &mut st) {
                        out_line = updated;
                    }
                }
                Err(_) => {
                    // Fail CLOSED: unparseable response from MCP server while requests are pending taints context!
                    let mut st = state_resp.lock().unwrap();
                    if !st.pending_calls.is_empty() {
                        let cid = content_address(l.as_bytes());
                        st.ledger.record(cid, AUTH_UNTRUSTED_EXT, &[]);
                        st.tainted_context_cids.push(cid);
                    }
                }
            }
            write_host_line(&host_out_resp, &out_line);
        }
    });

    // Main loop: Agent stdin -> fail-closed JSON & batch validation + causal 0x80/Ed25519 gate -> MCP Server stdin
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(l) = line else { break };
        if l.trim().is_empty() {
            continue;
        }
        let parsed = match parse_plain_json_to_value(&l) {
            Ok(v) => v,
            Err(e) => {
                // Fail CLOSED on malformed JSON, duplicate keys, or syntax smuggling!
                write_host_line(
                    &host_out,
                    &format_parse_err_jsonrpc(&format!("ERR_INVALID_JSON ({})", e)),
                );
                continue;
            }
        };

        let forward_line: Option<String> = match parsed {
            Value::Map(_) => {
                let mut st = state.lock().unwrap();
                match evaluate_single_mcp_request(&parsed, &mut st) {
                    Ok((sanitized, pending)) => {
                        if let Some((k, kind)) = pending {
                            st.pending_calls.entry(k).or_default().push_back(kind);
                        }
                        Some(value_to_plain_json(&sanitized))
                    }
                    Err(err_resp) => {
                        drop(st);
                        write_host_line(&host_out, &err_resp);
                        None
                    }
                }
            }
            Value::Array(items) => {
                if items.is_empty() {
                    write_host_line(&host_out, &format_parse_err_jsonrpc("ERR_INVALID_JSON: empty batch"));
                    None
                } else {
                    let mut st = state.lock().unwrap();
                    let mut sanitized_items = Vec::with_capacity(items.len());
                    let mut pending_batch = Vec::new();
                    let mut blocked_err: Option<String> = None;
                    for item in &items {
                        match evaluate_single_mcp_request(item, &mut st) {
                            Ok((san, pend)) => {
                                sanitized_items.push(san);
                                if let Some(p) = pend {
                                    pending_batch.push(p);
                                }
                            }
                            Err(e) => {
                                blocked_err = Some(e);
                                break;
                            }
                        }
                    }
                    if let Some(err_resp) = blocked_err {
                        drop(st);
                        write_host_line(&host_out, &err_resp);
                        None
                    } else {
                        for (k, kind) in pending_batch {
                            st.pending_calls.entry(k).or_default().push_back(kind);
                        }
                        Some(value_to_plain_json(&Value::Array(sanitized_items)))
                    }
                }
            }
            _ => {
                write_host_line(
                    &host_out,
                    &format_parse_err_jsonrpc("ERR_INVALID_JSON: JSON-RPC payload must be object or array"),
                );
                None
            }
        };

        if let Some(out_req) = forward_line {
            if writeln!(child_in, "{}", out_req).is_err() || child_in.flush().is_err() {
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
        "--version" | "-V" | "version" => {
            println!("cowrie {}", env!("CARGO_PKG_VERSION"));
        }

        "--help" | "-h" | "help" => {
            usage();
        }

        "mcp-config" => {
            let mut client = "cursor";
            let mut bin_mode = "cowrie";
            let mut mode_arg: Option<&str> = None;
            let mut virtualize = true;
            let mut virtualize_bytes: Option<usize> = None;
            let mut trust_ann = false;
            let mut policy_path: Option<String> = None;
            let mut wal_path: Option<String> = None;
            let mut sep = None;
            let mut i = 2;
            while i < args.len() {
                if args[i] == "--" {
                    sep = Some(i + 1);
                    break;
                } else if args[i] == "--client" && i + 1 < args.len() {
                    client = args[i + 1].as_str();
                    i += 2;
                } else if args[i] == "--bin" && i + 1 < args.len() {
                    bin_mode = args[i + 1].as_str();
                    i += 2;
                } else if args[i] == "--mode" && i + 1 < args.len() {
                    let m = args[i + 1].as_str();
                    if !matches!(m, "enforce" | "observe") {
                        eprintln!("Invalid --mode '{}' (expected enforce|observe)", m);
                        process::exit(2);
                    }
                    mode_arg = Some(m);
                    i += 2;
                } else if args[i] == "--virtualize" {
                    virtualize = true;
                    i += 1;
                } else if args[i] == "--no-virtualize" {
                    virtualize = false;
                    i += 1;
                } else if args[i] == "--virtualize-bytes" && i + 1 < args.len() {
                    virtualize_bytes = Some(args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Invalid --virtualize-bytes '{}'", args[i + 1]);
                        process::exit(2);
                    }));
                    virtualize = true;
                    i += 2;
                } else if args[i] == "--trust-server-annotations" {
                    trust_ann = true;
                    i += 1;
                } else if args[i] == "--policy" && i + 1 < args.len() {
                    policy_path = Some(args[i + 1].clone());
                    i += 2;
                } else if args[i] == "--audit-wal" && i + 1 < args.len() {
                    wal_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Unknown mcp-config option '{}'", args[i]);
                    process::exit(2);
                }
            }
            let Some(cmd_start) = sep else {
                usage();
                process::exit(2);
            };
            if cmd_start + 1 >= args.len() {
                eprintln!("mcp-config requires `-- <server-name> <mcp_cmd> [args...]`");
                process::exit(2);
            }
            let server_name = &args[cmd_start];
            let target_cmd = &args[cmd_start + 1..];

            let (command, mut proxy_args): (&str, Vec<String>) = match bin_mode {
                "cowrie" => ("cowrie", vec!["mcp-proxy".to_string()]),
                "npx" => (
                    "npx",
                    vec![
                        "-y".to_string(),
                        "cowrie-ai".to_string(),
                        "mcp-proxy".to_string(),
                    ],
                ),
                other => {
                    eprintln!("Unknown --bin '{}' (expected cowrie|npx)", other);
                    process::exit(2);
                }
            };
            if let Some(m) = mode_arg {
                proxy_args.push("--mode".to_string());
                proxy_args.push(m.to_string());
            }
            if let Some(vb) = virtualize_bytes {
                proxy_args.push("--virtualize-bytes".to_string());
                proxy_args.push(vb.to_string());
            } else if virtualize {
                proxy_args.push("--virtualize".to_string());
            }
            if trust_ann {
                proxy_args.push("--trust-server-annotations".to_string());
            }
            if let Some(ref p) = policy_path {
                proxy_args.push("--policy".to_string());
                proxy_args.push(p.clone());
            }
            if let Some(ref w) = wal_path {
                proxy_args.push("--audit-wal".to_string());
                proxy_args.push(w.clone());
            }
            proxy_args.push("--".to_string());
            proxy_args.extend(target_cmd.iter().cloned());

            match client {
                "claude-code" => {
                    let quote_sh = |s: &str| -> String {
                        if s.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/' | b':' | b'@'))
                        {
                            s.to_string()
                        } else {
                            format!("'{}'", s.replace('\'', "'\\''"))
                        }
                    };
                    let mut parts = vec![
                        "claude".to_string(),
                        "mcp".to_string(),
                        "add".to_string(),
                        quote_sh(server_name),
                        "--".to_string(),
                        quote_sh(command),
                    ];
                    for a in &proxy_args {
                        parts.push(quote_sh(a));
                    }
                    println!("{}", parts.join(" "));
                }
                "cursor" | "claude-desktop" => {
                    let args_json: Vec<String> = proxy_args
                        .iter()
                        .map(|a| value_to_plain_json(&Value::Str(a.clone())))
                        .collect();
                    let name_json = value_to_plain_json(&Value::Str(server_name.clone()));
                    let cmd_json = value_to_plain_json(&Value::Str(command.to_string()));
                    println!(
                        "{{\n  \"mcpServers\": {{\n    {}: {{\n      \"command\": {},\n      \"args\": [{}]\n    }}\n  }}\n}}",
                        name_json,
                        cmd_json,
                        args_json.join(", ")
                    );
                }
                other => {
                    eprintln!("Unknown --client '{}' (expected claude-code|claude-desktop|cursor)", other);
                    process::exit(2);
                }
            }
        }

        "pack" => {
            if args.len() < 4 {
                usage();
                process::exit(1);
            }
            let mut auth = AUTH_VERIFIED_TOOL;
            let mut raw_mode = false;
            let mut i = 4;
            while i < args.len() {
                if args[i] == "--auth" && i + 1 < args.len() {
                    auth = parse_auth(&args[i + 1]);
                    i += 2;
                } else if args[i] == "--raw" {
                    raw_mode = true;
                    i += 1;
                } else {
                    eprintln!("Unknown pack option '{}'", args[i]);
                    process::exit(2);
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
            if raw_mode {
                fs::write(&args[3], &frame).unwrap_or_else(|e| {
                    eprintln!("Failed to write {}: {}", args[3], e);
                    process::exit(1);
                });
                println!(
                    "PACKED {} -> {} | raw COWR frame={}B | auth={} | cid={}",
                    args[2],
                    args[3],
                    frame.len(),
                    authority_label(auth),
                    to_hex(&cid)
                );
            } else {
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
        }

        "pack-batch" => {
            if args.len() < 4 {
                usage();
                process::exit(1);
            }
            let out_path = &args[2];
            let mut auth = AUTH_VERIFIED_TOOL;
            let mut jsonl_path: Option<String> = None;
            let mut in_files: Vec<String> = Vec::new();
            let mut i = 3;
            while i < args.len() {
                if args[i] == "--auth" && i + 1 < args.len() {
                    auth = parse_auth(&args[i + 1]);
                    i += 2;
                } else if args[i] == "--jsonl" && i + 1 < args.len() {
                    jsonl_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    in_files.push(args[i].clone());
                    i += 1;
                }
            }
            let mut frames: Vec<Vec<u8>> = Vec::new();
            if let Some(jp) = jsonl_path {
                let text = fs::read_to_string(&jp).unwrap_or_else(|e| {
                    eprintln!("Failed to read {}: {}", jp, e);
                    process::exit(1);
                });
                for (ln, line) in text.lines().enumerate() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let val = parse_json_to_value(line).unwrap_or_else(|e| {
                        eprintln!("JSONL parse error at line {}: {}", ln + 1, e);
                        process::exit(1);
                    });
                    let fr = encode_frame(&val, auth, EPI_OBSERVED).unwrap_or_else(|e| {
                        eprintln!("Canonical encode error at line {}: {}", ln + 1, e);
                        process::exit(1);
                    });
                    frames.push(fr);
                }
            }
            for fp in &in_files {
                let text = fs::read_to_string(fp).unwrap_or_else(|e| {
                    eprintln!("Failed to read {}: {}", fp, e);
                    process::exit(1);
                });
                let val = parse_json_to_value(&text).unwrap_or_else(|e| {
                    eprintln!("JSON parse error in {}: {}", fp, e);
                    process::exit(1);
                });
                let fr = encode_frame(&val, auth, EPI_OBSERVED).unwrap_or_else(|e| {
                    eprintln!("Canonical encode error in {}: {}", fp, e);
                    process::exit(1);
                });
                frames.push(fr);
            }
            if frames.is_empty() {
                eprintln!("pack-batch requires at least one JSON input");
                process::exit(2);
            }
            let frefs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();
            let shard = build_shard_16kb(&frefs, &[], 0);
            let rep = verify_shard_and_localize(&shard).unwrap();
            fs::write(out_path, &shard).unwrap_or_else(|e| {
                eprintln!("Failed to write {}: {}", out_path, e);
                process::exit(1);
            });
            println!(
                "PACKED BATCH -> {} | frames={} | shard={}B (16KB aligned) | auth={} | root={}",
                out_path,
                frames.len(),
                shard.len(),
                authority_label(auth),
                to_hex(&rep.computed_root)
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
            if buf.starts_with(b"COWR") {
                let dec = decode_frame_strict(&buf).unwrap_or_else(|e| {
                    eprintln!("Frame parse error: {}", e);
                    process::exit(1);
                });
                let cid = content_address(&buf);
                println!("Raw Frame:    {}", args[2]);
                println!("Total bytes:  {} B", buf.len());
                println!("Authority:    0x{:02x} ({}) | Epistemic: 0x{:02x}", dec.authority, authority_label(dec.authority), dec.epistemic);
                println!("Frame CID:    {}", to_hex(&cid));
                println!("R0 Summary:   {}", project_r0(&buf, &dec, 0));
                return;
            }
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
                    let rcid_s = resolved_frame_cid(&buf, idx)
                        .map(|c| format!(" | resolved_cid={}", &to_hex(&c)[4..20]))
                        .unwrap_or_default();
                    println!("  Frame[{}]:   {}{}", idx, project_r0(fb, &dec, rep.tensors.len()), rcid_s);
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
            if buf.starts_with(b"COWR") {
                let (auth, epi) = verify_frame_zero_alloc(&buf).unwrap_or_else(|e| {
                    eprintln!("VERIFY FAIL: {}", e);
                    process::exit(1);
                });
                let cid = content_address(&buf);
                println!(
                    "VERIFY PASS | raw COWR frame={}B | auth=0x{:02x}({}) epi=0x{:02x} | cid={}",
                    buf.len(),
                    auth,
                    authority_label(auth),
                    epi,
                    to_hex(&cid)
                );
                return;
            }
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
                    eprintln!("Unknown lens option '{}'", args[i]);
                    process::exit(2);
                }
            }
            if !matches!(tier, "r0" | "r1" | "r2" | "r2-compact" | "r3") {
                eprintln!("Unknown --tier '{}'; expected r0, r1, r2, r2-compact, or r3", tier);
                process::exit(2);
            }
            let buf = fs::read(&args[2]).unwrap_or_else(|e| {
                eprintln!("Read error: {}", e);
                process::exit(1);
            });
            if buf.starts_with(b"COWR") {
                let dec = decode_frame_strict(&buf).unwrap_or_else(|e| {
                    eprintln!("Decode error: {}", e);
                    process::exit(1);
                });
                match tier {
                    "r0" => println!("{}", project_r0(&buf, &dec, 0)),
                    "r1" => print!("{}", project_r1(&buf, &dec)),
                    "r2" => print!("{}", project_r2(&buf, &dec)),
                    "r2-compact" => print!("{}", project_r2_compact(&buf, &dec)),
                    "r3" => println!("{}", project_r3(&buf, &dec)),
                    _ => unreachable!(),
                }
                return;
            }
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
                    "r2" => print!("{}", project_r2(fb, &dec)),
                    "r2-compact" => print!("{}", project_r2_compact(fb, &dec)),
                    "r3" => println!("{}", project_r3(fb, &dec)),
                    _ => unreachable!(),
                }
            }
        }

        "get" => {
            if args.len() < 4 {
                usage();
                process::exit(1);
            }
            let path_sel = &args[3];
            let mut frame_idx = 0usize;
            let mut i = 4;
            while i < args.len() {
                if args[i] == "--frame" && i + 1 < args.len() {
                    frame_idx = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Invalid --frame index '{}'", args[i + 1]);
                        process::exit(2);
                    });
                    i += 2;
                } else {
                    eprintln!("Unknown get option '{}'", args[i]);
                    process::exit(2);
                }
            }
            let buf = fs::read(&args[2]).unwrap_or_else(|e| {
                eprintln!("Read error: {}", e);
                process::exit(1);
            });
            let fb: &[u8] = if buf.starts_with(b"COWR") {
                if frame_idx != 0 {
                    eprintln!("ERR_INVALID_CONTRACT: raw COWR file has only frame 0");
                    process::exit(1);
                }
                &buf
            } else {
                let rep = verify_shard_and_localize(&buf).unwrap_or_else(|e| {
                    eprintln!("Shard error: {}", e);
                    process::exit(1);
                });
                if !rep.ok {
                    eprintln!("Refusing extraction on corrupt shard");
                    process::exit(1);
                }
                let Some(fe) = rep.frames.get(frame_idx) else {
                    eprintln!("ERR_INVALID_CONTRACT: frame index {} out of range ({})", frame_idx, rep.frames.len());
                    process::exit(1);
                };
                &buf[fe.offset as usize..(fe.offset as usize + fe.len as usize)]
            };
            let extracted = extract_field_json(fb, path_sel).unwrap_or_else(|e| {
                eprintln!("Extract error: {}", e);
                process::exit(1);
            });
            println!("{}", extracted);
        }

        #[cfg(unix)]
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

        #[cfg(unix)]
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

        "keygen" => {
            let mut out_seed_file: Option<String> = None;
            let mut i = 2;
            while i < args.len() {
                if args[i] == "--out-seed-file" && i + 1 < args.len() {
                    out_seed_file = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            let seed = generate_random_seed_32();
            let pk = ed25519_public_key(&seed);
            let seed_hex = to_hex(&seed);
            let pk_hex = to_hex(&pk);
            if let Some(ref spath) = out_seed_file {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    let mut f = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(spath)
                        .unwrap_or_else(|e| {
                            eprintln!("Failed to write 0600 seed file '{}': {}", spath, e);
                            process::exit(1);
                        });
                    writeln!(f, "{}", seed_hex).unwrap();
                }
                #[cfg(not(unix))]
                {
                    fs::write(spath, format!("{}\n", seed_hex)).unwrap();
                }
                println!("pubkey={}", pk_hex);
            } else {
                println!("seed={}\npubkey={}", seed_hex, pk_hex);
            }
        }

        "ed25519-pubkey" => {
            if args.len() < 3 {
                usage();
                process::exit(1);
            }
            let seed = if args[2] == "--seed-file" {
                if args.len() < 4 {
                    usage();
                    process::exit(2);
                }
                read_seed_from_file_or_stdin(&args[3])
            } else {
                parse_hex_exact::<32>(&args[2]).unwrap_or_else(|| {
                    eprintln!("Expected 64-hex secret seed or --seed-file <path|->");
                    process::exit(2);
                })
            };
            println!("{}", to_hex(&ed25519_public_key(&seed)));
        }

        "declassify-sign" => {
            let (seed, cid_arg, act_arg, exp_arg) = if args.len() >= 7 && args[2] == "--seed-file" {
                (
                    read_seed_from_file_or_stdin(&args[3]),
                    args[4].as_str(),
                    args[5].as_str(),
                    args[6].as_str(),
                )
            } else if args.len() >= 6 {
                let s = parse_hex_exact::<32>(&args[2]).unwrap_or_else(|| {
                    eprintln!("Expected 64-hex secret seed or --seed-file <path|->");
                    process::exit(2);
                });
                (s, args[3].as_str(), args[4].as_str(), args[5].as_str())
            } else {
                usage();
                process::exit(1);
            };
            let Some(cid) = parse_hex_exact::<34>(cid_arg) else {
                eprintln!("Expected 68-hex 34-byte multihash CID");
                process::exit(2);
            };
            let Some(act) = parse_action_name(act_arg) else {
                eprintln!("Unknown action '{}'", act_arg);
                process::exit(2);
            };
            let expiry: u64 = exp_arg.parse().unwrap_or_else(|_| {
                eprintln!("Invalid expiry unix timestamp");
                process::exit(2);
            });
            let tok = DeclassificationToken::sign(&seed, cid, act, expiry);
            println!("{}", to_hex(&tok.signature));
        }

        "mcp-proxy" => {
            let mut owner_pk: Option<[u8; 32]> = None;
            let mut wal_path: Option<String> = None;
            let mut policy_path: Option<String> = None;
            let mut trust_server_annotations = false;
            let mut mode = ProxyMode::Enforce;
            let mut quiet = false;
            let mut virtualize_bytes: usize = 0;
            let mut sep = None;
            let mut i = 2;
            while i < args.len() {
                if args[i] == "--" {
                    sep = Some(i + 1);
                    break;
                } else if args[i] == "--mode" && i + 1 < args.len() {
                    mode = match args[i + 1].as_str() {
                        "enforce" => ProxyMode::Enforce,
                        "observe" => ProxyMode::Observe,
                        other => {
                            eprintln!("Invalid --mode '{}' (expected enforce|observe)", other);
                            process::exit(2);
                        }
                    };
                    i += 2;
                } else if args[i] == "--quiet" || args[i] == "-q" {
                    quiet = true;
                    i += 1;
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
                } else if args[i] == "--policy" && i + 1 < args.len() {
                    policy_path = Some(args[i + 1].clone());
                    i += 2;
                } else if args[i] == "--trust-server-annotations" {
                    trust_server_annotations = true;
                    i += 1;
                } else if args[i] == "--virtualize" {
                    virtualize_bytes = DEFAULT_VIRTUALIZE_BYTES;
                    i += 1;
                } else if args[i] == "--virtualize-bytes" && i + 1 < args.len() {
                    virtualize_bytes = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Invalid --virtualize-bytes '{}' (expected non-negative byte threshold)", args[i + 1]);
                        process::exit(2);
                    });
                    i += 2;
                } else {
                    eprintln!("Unknown mcp-proxy option '{}'", args[i]);
                    process::exit(2);
                }
            }
            let Some(cmd_start) = sep else {
                usage();
                process::exit(2);
            };
            let policy = match policy_path {
                Some(ref p) => HostToolPolicy::load(p, trust_server_annotations),
                None => HostToolPolicy {
                    trust_server_annotations,
                    ..Default::default()
                },
            };
            run_mcp_proxy(
                owner_pk,
                wal_path.as_deref(),
                policy,
                mode,
                quiet,
                virtualize_bytes,
                &args[cmd_start..],
            );
        }

        #[cfg(unix)]
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
