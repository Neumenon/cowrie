//! Plane B: Deterministic Progressive Lenses (`R0` Alpha32, `R1` Atlas, Adaptive `R2`).
//! Zero model inference required: pure functions of canonical Plane A frames.

use std::collections::BTreeSet;
use crate::canonical::{dtype_to_str, value_to_compact_json, DecodedFrame, Value};
use crate::sha256::{content_address, to_hex};
use crate::trust::{authority_label, is_tainted};

const ALPHA26: &[u8; 26] = b"abcdefghijklmnopqrstuvwxyz";
pub const ALPHA_HANDLE_LETTERS: usize = 10; // 26^10 = 1.41e14 (~47.0 bits, collision-hardened)

/// Encode 10 deterministic lowercase ASCII letters from a 34-byte CID (`@abcdefghij`, ~47 bits).
/// Avoids single-digit `\p{N}` BPE fragmentation found in Qwen3.5 / Llama-3 tokenizers while
/// raising the birthday bound from ~17K records (6 letters) to ~12M records (10 letters).
pub fn alpha_handle(cid: &[u8; 34]) -> String {
    let mut s = String::with_capacity(1 + ALPHA_HANDLE_LETTERS);
    s.push('@');
    let mut acc: u64 = u64::from_be_bytes(cid[2..10].try_into().unwrap());
    for _ in 0..ALPHA_HANDLE_LETTERS {
        s.push(ALPHA26[(acc % 26) as usize] as char);
        acc /= 26;
    }
    s
}

/// Coarse human/LLM size bucket (avoids digit-by-digit tokenization tax of raw byte counts).
pub fn size_bucket(bytes: usize) -> &'static str {
    if bytes < 1024 {
        "xs"
    } else if bytes < 8192 {
        "sm"
    } else if bytes < 65536 {
        "md"
    } else {
        "lg"
    }
}

/// Neutralize boundary-fence spoofing sequences (`<<<` and `>>>`) inside untrusted payload text
/// so embedded content can never forge `<<<END:COWRIE ...>>>` or `<<<COWRIE:R2 ...>>>` headers,
/// even under arbitrary `>= 3` runs (`<<<<END:COWRIE>>>>`, `<<<<<<<`, etc.).
///
/// Implemented as a formally verified 3-state Mealy transducer (`formal/CowrieKernel.bend`,
/// `L07_fence_no_triple` & `L08_fence_safe_identity`): tracks the emitted trailing `<` / `>`
/// run in `{0, 1, 2}` and injects `\` before any 3rd consecutive bracket, resetting the run to `1`.
#[inline]
pub fn sanitize_r2_fences(s: &str) -> String {
    if !s.contains("<<<") && !s.contains(">>>") {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 8);
    let mut run_lt: u8 = 0;
    let mut run_gt: u8 = 0;
    for ch in s.chars() {
        match ch {
            '<' => {
                run_gt = 0;
                if run_lt == 2 {
                    out.push('\\');
                    out.push('<');
                    run_lt = 1;
                } else {
                    out.push('<');
                    run_lt += 1;
                }
            }
            '>' => {
                run_lt = 0;
                if run_gt == 2 {
                    out.push('\\');
                    out.push('>');
                    run_gt = 1;
                } else {
                    out.push('>');
                    run_gt += 1;
                }
            }
            _ => {
                run_lt = 0;
                run_gt = 0;
                out.push(ch);
            }
        }
    }
    out
}

#[inline]
pub fn sanitize_r2_inline_key(s: &str) -> String {
    let flat: String = s
        .chars()
        .map(|c| match c {
            '\n' | '\r' | '{' | '}' | ',' | '|' => '_',
            other => other,
        })
        .collect();
    sanitize_r2_fences(&flat)
}

fn format_typed_tensor_ref(dtype: u8, dims: &[u64], space: &str) -> String {
    let dt = dtype_to_str(dtype).unwrap_or("f32");
    let ds: Vec<String> = dims.iter().map(|d| d.to_string()).collect();
    sanitize_r2_fences(&format!(
        "<T:{}[{}]@{}>",
        dt,
        ds.join(","),
        sanitize_r2_inline_key(space)
    ))
}

/// Render `R0`: compact Alpha32 handle for O(1) archive triage (~9–12 tokens per document).
pub fn project_r0(frame_bytes: &[u8], frame: &DecodedFrame, tensor_count: usize) -> String {
    let cid = content_address(frame_bytes);
    let handle = alpha_handle(&cid);
    let trust = if is_tainted(frame.authority) { "UNTRUSTED" } else { "VERIFIED" };
    let title = frame
        .value
        .get("title")
        .or_else(|| frame.value.get("doc_id"))
        .or_else(|| frame.value.get("schema"))
        .and_then(|v| v.as_str())
        .unwrap_or("record");
    let mut short_title = sanitize_r2_fences(&title.replace(['\n', '\r'], " "));
    if short_title.len() > 72 {
        let mut cut = 72;
        while !short_title.is_char_boundary(cut) {
            cut -= 1;
        }
        short_title.truncate(cut);
        short_title.push_str("...");
    }
    if tensor_count > 0 {
        format!("{} [{}|{}|T={}] {}", handle, trust, size_bucket(frame_bytes.len()), tensor_count, short_title)
    } else {
        format!("{} [{}|{}] {}", handle, trust, size_bucket(frame_bytes.len()), short_title)
    }
}

/// Render `R1`: deterministic structural atlas showing schema, keys, table shapes, and tensor handles.
pub fn project_r1(frame_bytes: &[u8], frame: &DecodedFrame) -> String {
    let cid = content_address(frame_bytes);
    let mut out = String::new();
    out.push_str(&format!(
        "R1 {} cid={} auth={} bytes={}\n",
        alpha_handle(&cid),
        &to_hex(&cid)[4..16],
        authority_label(frame.authority),
        frame_bytes.len()
    ));
    if let Value::Map(entries) = &frame.value {
        for (k, v) in entries {
            let desc = match v {
                Value::Null => "null".to_string(),
                Value::Bool(b) => format!("bool({})", b),
                Value::Int(n) => format!("int({})", n),
                Value::Uint(u) => format!("uint({})", u),
                Value::FloatBits(b) => format!("f64({})", f64::from_bits(*b)),
                Value::Str(s) => {
                    let preview: String = s.chars().take(36).collect();
                    let clean = sanitize_r2_fences(&preview.replace(['\n', '\r'], " "));
                    if s.chars().count() > 36 {
                        format!("str[{}B] \"{}...\"", s.len(), clean)
                    } else {
                        format!("str[{}B] \"{}\"", s.len(), clean)
                    }
                }
                Value::Bytes(b) => format!("bytes[{}B]", b.len()),
                Value::TensorRef { dtype, dims, space, .. } => format_typed_tensor_ref(*dtype, dims, space),
                Value::Array(a) => {
                    if let Some(cols) = uniform_table_columns(a) {
                        let safe_cols: Vec<String> = cols.iter().map(|c| sanitize_r2_inline_key(c)).collect();
                        format!("Table[{}]({})", a.len(), safe_cols.join(","))
                    } else if is_numeric_vector(a) {
                        format!("<T:f32[{}]>", a.len())
                    } else {
                        format!("Array[{}]", a.len())
                    }
                }
                Value::Map(m) => {
                    let ks: Vec<String> = m.iter().map(|(kk, _)| sanitize_r2_inline_key(kk)).take(6).collect();
                    format!("Map[{}]({})", m.len(), ks.join(","))
                }
            };
            out.push_str(&format!("  {}: {}\n", sanitize_r2_inline_key(k), desc));
        }
    } else {
        out.push_str(&format!("  root: {}\n", sanitize_r2_fences(&value_to_compact_json(&frame.value))));
    }
    out
}

fn is_numeric_vector(items: &[Value]) -> bool {
    items.len() >= 16
        && items
            .iter()
            .all(|v| matches!(v, Value::FloatBits(_) | Value::Int(_) | Value::Uint(_)))
}

/// Adaptive R2 crossover rule (proven in E2): hoist header table ONLY when
/// `len >= 3` AND schema key density `>= 0.70`, else fall back to compact JSON.
fn uniform_table_columns(items: &[Value]) -> Option<Vec<String>> {
    if items.len() < 3 {
        return None;
    }
    let mut cols: BTreeSet<String> = BTreeSet::new();
    let mut present_cells = 0usize;
    for item in items {
        let Value::Map(m) = item else { return None; };
        for (k, v) in m {
            if matches!(v, Value::Array(_) | Value::Map(_) | Value::TensorRef { .. }) {
                return None;
            }
            cols.insert(k.clone());
            present_cells += 1;
        }
    }
    if cols.is_empty() {
        return None;
    }
    let density = (present_cells as f64) / ((items.len() * cols.len()) as f64);
    if density < 0.70 {
        return None;
    }
    Some(cols.into_iter().collect())
}

fn cell_escape(v: &Value) -> String {
    match v {
        Value::Null => "".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Uint(u) => u.to_string(),
        Value::FloatBits(b) => format!("{}", f64::from_bits(*b)),
        Value::Str(s) => sanitize_r2_fences(&s.replace('\\', "\\\\").replace('|', "\\|").replace('\n', "\\n")),
        Value::Bytes(b) => format!("hex:{}", to_hex(b)),
        Value::TensorRef { dtype, dims, space, .. } => format_typed_tensor_ref(*dtype, dims, space),
        _ => sanitize_r2_fences(&value_to_compact_json(v)),
    }
}

/// Render `R2`: Adaptive Attention Lens wrapped in a CID-bound non-colliding trust fence
/// with inner `<<<` / `>>>` sequences escaped against fence-breakout injection.
pub fn project_r2(frame_bytes: &[u8], frame: &DecodedFrame) -> String {
    let cid = content_address(frame_bytes);
    let cid_short = &to_hex(&cid)[4..16];
    let handle = alpha_handle(&cid);
    let auth_str = authority_label(frame.authority);

    let mut out = String::new();
    out.push_str(&format!(
        "<<<COWRIE:R2 {} AUTH=0x{:02x}({}) CID={}>>>\n",
        handle, frame.authority, auth_str, cid_short
    ));

    if let Value::Map(entries) = &frame.value {
        for (k, v) in entries {
            let safe_k = sanitize_r2_inline_key(k);
            match v {
                Value::Array(items) => {
                    if let Some(cols) = uniform_table_columns(items) {
                        let safe_cols: Vec<String> = cols.iter().map(|c| sanitize_r2_inline_key(c)).collect();
                        out.push_str(&format!("{}[{}]{{{}}}:\n", safe_k, items.len(), safe_cols.join(",")));
                        for row in items {
                            if let Value::Map(rm) = row {
                                let cells: Vec<String> = cols
                                    .iter()
                                    .map(|c| {
                                        rm.iter()
                                            .find(|(rk, _)| rk == c)
                                            .map(|(_, rv)| cell_escape(rv))
                                            .unwrap_or_default()
                                    })
                                    .collect();
                                out.push_str("  ");
                                out.push_str(&cells.join("|"));
                                out.push('\n');
                            }
                        }
                    } else if is_numeric_vector(items) {
                        out.push_str(&format!("{}: <T:f32[{}]@inline_elided>\n", safe_k, items.len()));
                    } else {
                        out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_fences(&value_to_compact_json(v))));
                    }
                }
                Value::TensorRef { dtype, dims, space, .. } => {
                    out.push_str(&format!("{}: {}\n", safe_k, format_typed_tensor_ref(*dtype, dims, space)));
                }
                Value::Map(_) => {
                    out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_fences(&value_to_compact_json(v))));
                }
                Value::Str(s) => out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_fences(s))),
                _ => out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_fences(&value_to_compact_json(v)))),
            }
        }
    } else {
        out.push_str(&sanitize_r2_fences(&value_to_compact_json(&frame.value)));
        out.push('\n');
    }

    out.push_str(&format!("<<<END:COWRIE {}>>>\n", cid_short));
    out
}
