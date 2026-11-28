//! Plane B: Deterministic Progressive Lenses (`R0` Alpha32, `R1` Atlas, Adaptive `R2`).
//! Zero model inference required: pure functions of canonical Plane A frames.

use std::collections::{BTreeSet, HashMap};
use crate::canonical::{decode_frame_strict, dtype_to_str, value_to_compact_json, DecodedFrame, Value};
use crate::sha256::{content_address, to_hex};
use crate::trust::{authority_label, is_tainted, AUTH_SYS_POLICY, AUTH_VERIFIED_TOOL};
use crate::CowrieError;

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

#[inline]
fn is_hidden_or_control_spoof(ch: char) -> bool {
    let cp = ch as u32;
    // Strip zero-width, Bidi override, Unicode line-break, Unicode Tag block, and non-whitespace C0/C1 controls
    matches!(
        cp,
        0x0000..=0x0008
            | 0x000B..=0x000C
            | 0x000E..=0x001F
            | 0x007F..=0x009F
            | 0x00AD
            | 0x034F
            | 0x061C
            | 0x180E
            | 0x200B..=0x200F
            | 0x2028..=0x202E
            | 0x2060..=0x206F
            | 0xFE00..=0xFE0F
            | 0xFEFF
            | 0xE0000..=0xE007F
            | 0xE0100..=0xE01EF
    )
}

/// Neutralize boundary-fence spoofing sequences (`<<<`, `>>>`), chat-template special tokens
/// (`<|`, `|>`), and invisible zero-width/Bidi/Tag/ANSI-control characters inside untrusted payload text
/// so embedded content can never forge `<<<END:COWRIE ...>>>`, `<<<COWRIE:R2 ...>>>`, or `<|im_end|>`,
/// even under arbitrary `>= 3` runs (`<<<<END:COWRIE>>>>`) or zero-width splitting (`<\u{200B}<\u{200B}<`).
///
/// Implemented as a formally verified 3-state Mealy transducer (`formal/CowrieKernel.bend`,
/// `L07_fence_no_triple` & `L08_fence_safe_identity`): tracks the emitted trailing `<` / `>`
/// run in `{0, 1, 2}` and injects `\` before any 3rd consecutive bracket, resetting the run to `1`.
#[inline]
pub fn sanitize_r2_fences(s: &str) -> String {
    let needs_work = s.contains("<<<")
        || s.contains(">>>")
        || s.contains("<|")
        || s.contains("|>")
        || s.chars().any(is_hidden_or_control_spoof);
    if !needs_work {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 8);
    let mut run_lt: u8 = 0;
    let mut run_gt: u8 = 0;
    let mut prev_emitted: char = '\0';
    for ch in s.chars() {
        if is_hidden_or_control_spoof(ch) {
            continue;
        }
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
                prev_emitted = '<';
            }
            '>' => {
                run_lt = 0;
                if prev_emitted == '|' {
                    out.push('\\');
                    out.push('>');
                    run_gt = 1;
                } else if run_gt == 2 {
                    out.push('\\');
                    out.push('>');
                    run_gt = 1;
                } else {
                    out.push('>');
                    run_gt += 1;
                }
                prev_emitted = '>';
            }
            '|' => {
                if prev_emitted == '<' {
                    out.push('\\');
                }
                run_lt = 0;
                run_gt = 0;
                out.push('|');
                prev_emitted = '|';
            }
            _ => {
                run_lt = 0;
                run_gt = 0;
                out.push(ch);
                prev_emitted = ch;
            }
        }
    }
    out
}

#[inline]
fn sanitize_r2_str_value(s: &str) -> String {
    if !s.contains('\n') && !s.contains('\r') {
        return sanitize_r2_fences(s);
    }
    let escaped = s.replace('\r', "\\r").replace('\n', "\\n");
    sanitize_r2_fences(&escaped)
}

#[inline]
pub fn sanitize_r2_inline_key(s: &str) -> String {
    let flat: String = s
        .chars()
        .filter(|&c| !is_hidden_or_control_spoof(c))
        .map(|c| match c {
            '\n' | '\r' | '\t' | '{' | '}' | ',' | '|' => '_',
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
/// Preserves exact `authority_label` (never collapses `MODEL_SYNTH` into `VERIFIED`).
pub fn project_r0(frame_bytes: &[u8], frame: &DecodedFrame, tensor_count: usize) -> String {
    let cid = content_address(frame_bytes);
    let handle = alpha_handle(&cid);
    let trust = authority_label(frame.authority);
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
                    } else if is_float_vector(a) {
                        format!("<T:f64[{}]>", a.len())
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

/// Only elide dense floating-point embedding vectors (`FloatBits`), never exact integer ID arrays.
fn is_float_vector(items: &[Value]) -> bool {
    items.len() >= 16 && items.iter().all(|v| matches!(v, Value::FloatBits(_)))
}

/// Adaptive R2 crossover rule (proven in E2): hoist header table ONLY when
/// `len >= 3` AND schema key density `>= 0.70`, else fall back to compact JSON.
fn uniform_table_columns<'a>(items: &'a [Value]) -> Option<Vec<&'a str>> {
    if items.len() < 3 {
        return None;
    }
    let mut cols: BTreeSet<&'a str> = BTreeSet::new();
    let mut present_cells = 0u128;
    for item in items {
        let Value::Map(m) = item else { return None; };
        for (k, v) in m {
            if matches!(v, Value::Array(_) | Value::Map(_) | Value::TensorRef { .. }) {
                return None;
            }
            cols.insert(k.as_str());
            present_cells += 1;
        }
    }
    if cols.is_empty() {
        return None;
    }
    let total_cells = (items.len() as u128) * (cols.len() as u128);
    let density = (present_cells as f64) / (total_cells as f64);
    if density < 0.70 {
        return None;
    }
    Some(cols.into_iter().collect())
}

fn cell_escape(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Uint(u) => u.to_string(),
        Value::FloatBits(b) => {
            let f = f64::from_bits(*b);
            if f.is_nan() {
                "NaN".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() { "Infinity".to_string() } else { "-Infinity".to_string() }
            } else {
                format!("{}", f)
            }
        }
        Value::Str(s) => {
            let has_spoof = s.chars().any(is_hidden_or_control_spoof);
            let cleaned: String = if has_spoof {
                s.chars().filter(|&c| !is_hidden_or_control_spoof(c)).collect()
            } else {
                String::new()
            };
            let eff: &str = if has_spoof {
                cleaned.as_str()
            } else {
                s.as_str()
            };
            if eff.is_empty() {
                "\"\"".to_string()
            } else if eff == "null" || eff == "<missing>" || eff.starts_with('"') {
                sanitize_r2_fences(&value_to_compact_json(&Value::Str(eff.to_string())))
            } else {
                sanitize_r2_fences(
                    &eff.replace('\\', "\\\\")
                        .replace('|', "\\|")
                        .replace('\r', "\\r")
                        .replace('\n', "\\n"),
                )
            }
        }
        Value::Bytes(b) => format!("hex:{}", to_hex(b)),
        Value::TensorRef { dtype, dims, space, .. } => format_typed_tensor_ref(*dtype, dims, space),
        _ => sanitize_r2_fences(&value_to_compact_json(v)),
    }
}

fn render_r2_body(frame: &DecodedFrame, out: &mut String) {
    if let Value::Map(entries) = &frame.value {
        for (k, v) in entries {
            let safe_k = sanitize_r2_inline_key(k);
            match v {
                Value::Array(items) => {
                    if let Some(cols) = uniform_table_columns(items) {
                        let safe_cols: Vec<String> = cols.iter().map(|c| sanitize_r2_inline_key(c)).collect();
                        out.push_str(&format!("{}[{}]{{{}}}:\n", safe_k, items.len(), safe_cols.join(",")));
                        let mut row_map: HashMap<&str, &Value> = HashMap::with_capacity(cols.len());
                        for row in items {
                            if let Value::Map(rm) = row {
                                row_map.clear();
                                for (rk, rv) in rm {
                                    row_map.insert(rk.as_str(), rv);
                                }
                                let cells: Vec<String> = cols
                                    .iter()
                                    .map(|c| {
                                        row_map
                                            .get(c)
                                            .map(|rv| cell_escape(rv))
                                            .unwrap_or_else(|| "<missing>".to_string())
                                    })
                                    .collect();
                                out.push_str("  ");
                                // [FORMAL-F9] Re-sanitize joined row so adjacent cells ending in `<`
                                // or starting with `>` cannot synthesize `<|` / `|>` across `|`.
                                out.push_str(&sanitize_r2_fences(&cells.join("|")));
                                out.push('\n');
                            }
                        }
                    } else if is_float_vector(items) {
                        out.push_str(&format!("{}: <T:f64[{}]@inline_elided>\n", safe_k, items.len()));
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
                Value::Str(s) => out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_str_value(s))),
                _ => out.push_str(&format!("{}: {}\n", safe_k, sanitize_r2_fences(&value_to_compact_json(v)))),
            }
        }
    } else {
        out.push_str(&sanitize_r2_fences(&value_to_compact_json(&frame.value)));
        out.push('\n');
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
    render_r2_body(frame, &mut out);
    out.push_str(&format!("<<<END:COWRIE {}>>>\n", cid_short));
    out
}

/// Render `R2-Compact` (B2-a trust-tiered lens):
/// - Clean `SYS_POLICY (0x01)` and `VERIFIED_TOOL (0x02)` frames emit a single-line compact header
///   `[@handle|AUTHORITY]\n<body>` (~7 tokens instead of ~58–63 tokens), eliminating small-record fence bloat
///   while preserving Cowrie's unquoted string values and Mealy sanitization.
/// - Any `UNTRUSTED_EXT`, `MODEL_SYNTH`, `BIT_TAINTED`, or mixed/unknown authority fails closed to the full
///   formally verified CID-bound Mealy fence (`project_r2`).
pub fn project_r2_compact(frame_bytes: &[u8], frame: &DecodedFrame) -> String {
    let clean_trusted = !is_tainted(frame.authority)
        && (frame.authority == AUTH_SYS_POLICY || frame.authority == AUTH_VERIFIED_TOOL);
    if !clean_trusted {
        return project_r2(frame_bytes, frame);
    }
    let cid = content_address(frame_bytes);
    let handle = alpha_handle(&cid);
    let auth_str = authority_label(frame.authority);
    let mut out = format!("[{}|{}]\n", handle, auth_str);
    render_r2_body(frame, &mut out);
    out
}

/// Render `R3`: 100% lossless canonical typed JSON extraction (round-trips through `parse_json_to_value`,
/// preserving `Bytes`, `TensorRef`, `NaN`/`±Infinity`, whole floats, and reserved-looking `$` map keys).
pub fn project_r3(_frame_bytes: &[u8], frame: &DecodedFrame) -> String {
    value_to_compact_json(&frame.value)
}

#[derive(Debug, PartialEq, Eq)]
enum PathSeg {
    Key(String),
    Index(usize),
}

fn parse_field_path(path: &str) -> Result<Vec<PathSeg>, CowrieError> {
    if path.is_empty() {
        return Ok(Vec::new());
    }
    if path.starts_with('/') {
        // RFC 6901 JSON Pointer syntax (`/rows/2/score`, `~1` -> `/`, `~0` -> `~`)
        let mut out = Vec::new();
        for raw in path[1..].split('/') {
            let mut decoded = String::with_capacity(raw.len());
            let mut chars = raw.chars();
            while let Some(c) = chars.next() {
                if c == '~' {
                    match chars.next() {
                        Some('0') => decoded.push('~'),
                        Some('1') => decoded.push('/'),
                        _ => return Err(CowrieError::InvalidContract),
                    }
                } else {
                    decoded.push(c);
                }
            }
            out.push(PathSeg::Key(decoded));
        }
        return Ok(out);
    }

    // Dot-bracket path syntax (`rows[2].score`, `metrics.lat[0]`, `[0]`)
    let bytes = path.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < bytes.len() {
        if bytes[i] == b'.' {
            if out.is_empty() || i + 1 >= bytes.len() || bytes[i + 1] == b'.' || bytes[i + 1] == b'[' {
                return Err(CowrieError::InvalidContract);
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'[' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i] != b']' {
                i += 1;
            }
            if i >= bytes.len() || i == start {
                return Err(CowrieError::InvalidContract);
            }
            let idx_str = std::str::from_utf8(&bytes[start..i]).map_err(|_| CowrieError::InvalidContract)?;
            if !idx_str.bytes().all(|b| b.is_ascii_digit()) {
                return Err(CowrieError::InvalidContract);
            }
            let idx = idx_str.parse::<usize>().map_err(|_| CowrieError::InvalidContract)?;
            out.push(PathSeg::Index(idx));
            i += 1; // consume ']'
            if i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                return Err(CowrieError::InvalidContract);
            }
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                if bytes[i] == b']' {
                    return Err(CowrieError::InvalidContract);
                }
                i += 1;
            }
            let key = std::str::from_utf8(&bytes[start..i]).map_err(|_| CowrieError::InvalidContract)?;
            if key.is_empty() {
                return Err(CowrieError::InvalidContract);
            }
            out.push(PathSeg::Key(key.to_string()));
        }
    }
    Ok(out)
}

/// Resolve an exact typed `Value` at `path` (RFC 6901 JSON Pointer `/a/0/b` or dot-bracket `a[0].b`).
pub fn extract_field<'a>(root: &'a Value, path: &str) -> Result<&'a Value, CowrieError> {
    let segs = parse_field_path(path)?;
    let mut cur = root;
    for seg in &segs {
        match (cur, seg) {
            (Value::Map(entries), PathSeg::Key(k)) => {
                cur = entries
                    .iter()
                    .find(|(ek, _)| ek == k)
                    .map(|(_, v)| v)
                    .ok_or(CowrieError::InvalidContract)?;
            }
            (Value::Array(items), PathSeg::Index(idx)) => {
                cur = items.get(*idx).ok_or(CowrieError::InvalidContract)?;
            }
            (Value::Array(items), PathSeg::Key(k)) => {
                // Allow RFC 6901 JSON Pointer numeric index segments on arrays (e.g. `/rows/2/id`)
                if k.is_empty() || !k.bytes().all(|b| b.is_ascii_digit()) || (k.len() > 1 && k.starts_with('0')) {
                    return Err(CowrieError::InvalidContract);
                }
                let idx = k.parse::<usize>().map_err(|_| CowrieError::InvalidContract)?;
                cur = items.get(idx).ok_or(CowrieError::InvalidContract)?;
            }
            _ => return Err(CowrieError::InvalidContract),
        }
    }
    Ok(cur)
}

/// Strictly verify a canonical `COWR` frame and extract the exact value at `path` as lossless typed JSON.
pub fn extract_field_json(frame_bytes: &[u8], path: &str) -> Result<String, CowrieError> {
    let dec = decode_frame_strict(frame_bytes)?;
    let target = extract_field(&dec.value, path)?;
    Ok(value_to_compact_json(target))
}

// ---------- B2-b: Pre-Append Virtualization Atlas & Bounded Slice Query (`cowrie_get`) ----------

fn truncate_utf8_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

fn slice_utf8_chars(s: &str, start_char: usize, max_chars: usize) -> (&str, usize) {
    let total = s.chars().count();
    if start_char >= total {
        return ("", total);
    }
    let start_b = s.char_indices().nth(start_char).map(|(i, _)| i).unwrap_or(s.len());
    let rest = &s[start_b..];
    (truncate_utf8_chars(rest, max_chars), total)
}

fn find_ci_ascii(hay: &str, needle: &str, from_byte: usize) -> Option<(usize, usize)> {
    if needle.is_empty() {
        return Some((from_byte, from_byte));
    }
    let hb = hay.as_bytes();
    let nb = needle.as_bytes();
    if nb.len() > hb.len() || from_byte > hb.len() - nb.len() {
        return None;
    }
    for i in from_byte..=(hb.len() - nb.len()) {
        if !hay.is_char_boundary(i) {
            continue;
        }
        let mut ok = true;
        for j in 0..nb.len() {
            if !hb[i + j].eq_ignore_ascii_case(&nb[j]) {
                ok = false;
                break;
            }
        }
        if ok && hay.is_char_boundary(i + nb.len()) {
            return Some((i, i + nb.len()));
        }
    }
    None
}

fn split_bounded_wildcards(alt: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut cur = alt;
    while let Some(pos) = cur.find(".*") {
        let head = &cur[..pos];
        if !head.is_empty() {
            parts.push(head);
        }
        let mut next = pos + 2;
        if cur.as_bytes().get(next) == Some(&b'?') {
            next += 1;
        }
        cur = &cur[next..];
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    parts
}

/// Zero-dependency pattern matcher supporting `|` alternation and bounded `.*` wildcards (gap <= 80 bytes).
fn find_pattern_matches(hay: &str, pattern: &str, max_hits: usize) -> Vec<(usize, usize)> {
    let alts: Vec<&str> = pattern
        .split('|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if alts.is_empty() {
        return Vec::new();
    }
    let mut hits: Vec<(usize, usize)> = Vec::new();
    let mut scan_from = 0usize;
    while scan_from < hay.len() && hits.len() < max_hits {
        let mut best: Option<(usize, usize)> = None;
        for alt in &alts {
            let segs = split_bounded_wildcards(alt);
            if segs.is_empty() {
                continue;
            }
            let mut try_from = scan_from;
            while let Some((s0, mut cur_end)) = find_ci_ascii(hay, segs[0], try_from) {
                let mut matched_all = true;
                for seg in &segs[1..] {
                    match find_ci_ascii(hay, seg, cur_end) {
                        Some((ns, ne)) if ns.saturating_sub(cur_end) <= 80 => {
                            cur_end = ne;
                        }
                        _ => {
                            matched_all = false;
                            break;
                        }
                    }
                }
                if matched_all {
                    match best {
                        None => best = Some((s0, cur_end)),
                        Some((bs, _)) if s0 < bs => best = Some((s0, cur_end)),
                        _ => {}
                    }
                    break;
                }
                // Advance past first char of s0 to try next occurrence
                if let Some(ch) = hay[s0..].chars().next() {
                    try_from = s0 + ch.len_utf8();
                } else {
                    break;
                }
            }
        }
        match best {
            Some((s, e)) => {
                hits.push((s, e));
                scan_from = if e > s { e } else { s + 1 };
                while scan_from < hay.len() && !hay.is_char_boundary(scan_from) {
                    scan_from += 1;
                }
            }
            None => break,
        }
    }
    hits
}

fn unescape_grep_literal(q: &str) -> String {
    let mut out = String::with_capacity(q.len());
    let mut chars = q.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                if matches!(next, '\\' | '"' | '\'' | '[' | ']' | '(' | ')' | '.' | '$' | '^') {
                    out.push(next);
                    chars.next();
                    continue;
                }
            }
        }
        out.push(c);
    }
    out
}

/// Render a compact `B2-b` pre-append virtualization atlas (`R0` + scalar metadata + head/tail text outline +
/// array previews + concrete `cowrie_get` fetch recipes), hard-capped to `max_chars` (default `1800`) and
/// wrapped in the formally verified Mealy fence whenever the frame is untrusted or tainted.
pub fn project_virtual_atlas(frame_bytes: &[u8], frame: &DecodedFrame, max_chars: usize) -> String {
    let cid = content_address(frame_bytes);
    let cid_short = &to_hex(&cid)[4..16];
    let handle = alpha_handle(&cid);
    let auth_str = authority_label(frame.authority);
    let clean_trusted = !is_tainted(frame.authority)
        && (frame.authority == AUTH_SYS_POLICY || frame.authority == AUTH_VERIFIED_TOOL);
    let untrusted = !clean_trusted;
    let cap = max_chars.clamp(400, 4096);

    let mut lines: Vec<String> = Vec::new();
    let mut recipes: Vec<String> = Vec::new();
    lines.push(format!(
        "VIRTUALIZED_TOOL_OUTPUT handle={} cid={} auth={} bytes={}",
        handle,
        cid_short,
        auth_str,
        frame_bytes.len()
    ));

    if let Value::Map(entries) = &frame.value {
        // Inline small scalar metadata first
        for (k, v) in entries {
            let safe_k = sanitize_r2_inline_key(k);
            match v {
                Value::Null | Value::Bool(_) | Value::Int(_) | Value::Uint(_) | Value::FloatBits(_) => {
                    lines.push(format!("  /{}: {}", safe_k, value_to_compact_json(v)));
                }
                Value::Str(s) if s.chars().count() < 160 => {
                    lines.push(format!("  /{}: {}", safe_k, sanitize_r2_fences(&value_to_compact_json(v))));
                }
                _ => {}
            }
        }
        // Outline large strings, arrays, and nested maps
        for (k, v) in entries {
            let safe_k = sanitize_r2_inline_key(k);
            match v {
                Value::Str(s) if s.chars().count() >= 160 => {
                    let ls: Vec<&str> = s.lines().collect();
                    let n_lines = ls.len().max(1);
                    let head_joined = ls.iter().take(3).copied().collect::<Vec<_>>().join(" | ");
                    let head = sanitize_r2_fences(truncate_utf8_chars(&head_joined, 220));
                    lines.push(format!("  /{}: str[{}B, {} lines]", safe_k, s.len(), n_lines));
                    lines.push(format!("    head: {}", head));
                    if n_lines > 4 {
                        let tail_joined = ls[n_lines - 2..].join(" | ");
                        let tail = sanitize_r2_fences(truncate_utf8_chars(&tail_joined, 180));
                        lines.push(format!("    tail: {}", tail));
                    }
                    recipes.push(format!(
                        "grep: {{\"tool\":\"cowrie_get\",\"args\":{{\"handle\":\"{}\",\"pointer\":\"/{}\",\"grep\":\"<keyword>\"}}}}",
                        handle, safe_k
                    ));
                    recipes.push(format!(
                        "lines: {{\"tool\":\"cowrie_get\",\"args\":{{\"handle\":\"{}\",\"pointer\":\"/{}\",\"lines\":[1,40]}}}}",
                        handle, safe_k
                    ));
                }
                Value::Array(items) => {
                    let item_keys: Vec<String> = match items.first() {
                        Some(Value::Map(m)) => m.iter().take(8).map(|(kk, _)| sanitize_r2_inline_key(kk)).collect(),
                        _ => Vec::new(),
                    };
                    if !item_keys.is_empty() {
                        lines.push(format!("  /{}: Array[{}] item_keys=[{}]", safe_k, items.len(), item_keys.join(",")));
                        if let Some(Value::Map(m0)) = items.first() {
                            let mut preview_entries: Vec<(String, Value)> = Vec::new();
                            for (pk, pv) in m0.iter() {
                                if preview_entries.len() >= 4 {
                                    break;
                                }
                                match pv {
                                    Value::Array(_) | Value::Map(_) => {}
                                    Value::Str(ps) => {
                                        preview_entries.push((pk.clone(), Value::Str(truncate_utf8_chars(ps, 32).to_string())));
                                    }
                                    other => preview_entries.push((pk.clone(), other.clone())),
                                }
                            }
                            let p_json = sanitize_r2_fences(&value_to_compact_json(&Value::Map(preview_entries)));
                            lines.push(format!("    [0]: {}", p_json));
                        }
                    } else {
                        lines.push(format!("  /{}: Array[{}]", safe_k, items.len()));
                    }
                    recipes.push(format!(
                        "item: {{\"tool\":\"cowrie_get\",\"args\":{{\"handle\":\"{}\",\"pointer\":\"/{}/<idx>\"}}}}",
                        handle, safe_k
                    ));
                }
                Value::Map(m) => {
                    let ks: Vec<String> = m.iter().take(8).map(|(kk, _)| sanitize_r2_inline_key(kk)).collect();
                    lines.push(format!("  /{}: Map[{}] keys=[{}]", safe_k, m.len(), ks.join(",")));
                }
                _ => {}
            }
        }
    } else {
        let j = sanitize_r2_fences(&value_to_compact_json(&frame.value));
        lines.push(format!("  /: {}", truncate_utf8_chars(&j, 300)));
    }

    if let Some(first_recipe) = recipes.first() {
        lines.push(format!("  fetch_examples: {}", first_recipe));
    }

    let joined = lines.join("\n");
    let body = truncate_utf8_chars(&joined, cap);
    if untrusted {
        format!(
            "<<<COWRIE:ATLAS {} AUTH=0x{:02x}({}) CID={}>>>\n{}\n<<<END:COWRIE {}>>>\n",
            handle, frame.authority, auth_str, cid_short, body, cid_short
        )
    } else {
        format!("{}\n", body)
    }
}

/// Bounded `cowrie_get` query over a canonical `COWR` frame supporting RFC 6901 `pointer`,
/// `lines=(start, end)` (1-based inclusive), bounded-wildcard `grep`, and char `offset`/`max_chars`.
/// Implements all four live-validated V2 slicing rules (dict scalar priority, `lines`-before-`grep`,
/// giant-line bounded `.*` multi-match windowing, and backslash/quote-normalized literal fallback).
pub fn query_frame_slice(
    frame_bytes: &[u8],
    pointer: Option<&str>,
    grep: Option<&str>,
    lines_range: Option<(usize, usize)>,
    offset: usize,
    max_chars: usize,
) -> Result<String, CowrieError> {
    let dec = decode_frame_strict(frame_bytes)?;
    let cap = max_chars.clamp(128, 8192);
    let ptr = pointer.unwrap_or("").trim();

    let target: &Value = if ptr.is_empty() || ptr == "/" {
        &dec.value
    } else if let Ok(v) = extract_field(&dec.value, ptr) {
        v
    } else if !ptr.starts_with('/') {
        let with_slash = format!("/{}", ptr);
        extract_field(&dec.value, &with_slash)?
    } else {
        return Err(CowrieError::InvalidContract);
    };

    let raw_j = value_to_compact_json(target);

    // Rule 1: When pointer resolves to a JSON dict (without explicit `lines`), surface scalar fields first
    // so giant nested arrays/blobs (e.g. `messages`) cannot shadow top-level metadata during `grep`.
    if let Value::Map(entries) = target {
        if lines_range.is_none() {
            let mut scalars: Vec<(String, Value)> = Vec::new();
            let mut nested_keys: Vec<String> = Vec::new();
            for (k, v) in entries {
                if matches!(v, Value::Array(_) | Value::Map(_)) {
                    nested_keys.push(k.clone());
                } else {
                    scalars.push((k.clone(), v.clone()));
                }
            }
            let slim_j = value_to_compact_json(&Value::Map(scalars));
            if grep.is_none() {
                if offset == 0 && raw_j.chars().count() <= cap {
                    let san = sanitize_r2_fences(&raw_j);
                    if san.chars().count() <= cap {
                        return Ok(san);
                    }
                }
                let (window, total_slim) = slice_utf8_chars(&slim_j, offset, cap);
                let mut out = window.to_string();
                if !nested_keys.is_empty() && out.chars().count() + 80 < cap {
                    let rem = cap.saturating_sub(out.chars().count());
                    let note = format!(
                        "\n[nested keys omitted for cap: {:?}; fetch with sub-pointer]",
                        nested_keys
                    );
                    out.push_str(truncate_utf8_chars(&note, rem));
                } else if offset.saturating_add(cap) < total_slim && out.chars().count() + 48 < cap {
                    let rem = cap.saturating_sub(out.chars().count());
                    let note = format!(
                        "\n...[truncated {}..{} of {} chars]",
                        offset,
                        offset.saturating_add(window.chars().count()),
                        total_slim
                    );
                    out.push_str(truncate_utf8_chars(&note, rem));
                }
                // [FORMAL-F17] Cap AFTER sanitize_r2_fences (prefix-closed fence safety proved in L39).
                let san = sanitize_r2_fences(&out);
                return Ok(truncate_utf8_chars(&san, cap).to_string());
            } else if let Some(q) = grep {
                let norm_slim = slim_j.replace("\\\"", "\"").replace("\\'", "'");
                let q_lit = unescape_grep_literal(q);
                if !find_pattern_matches(&slim_j, q, 1).is_empty()
                    || !find_pattern_matches(&norm_slim, &q_lit, 1).is_empty()
                {
                    let mut out = truncate_utf8_chars(&slim_j, cap).to_string();
                    if !nested_keys.is_empty() && out.chars().count() + 60 < cap {
                        let rem = cap.saturating_sub(out.chars().count());
                        let note = format!("\n[nested keys omitted: {:?}]", nested_keys);
                        out.push_str(truncate_utf8_chars(&note, rem));
                    }
                    let san = sanitize_r2_fences(&out);
                    return Ok(truncate_utf8_chars(&san, cap).to_string());
                }
            }
        }
    }

    let text_view: &str = match target {
        Value::Str(s) => s.as_str(),
        _ => raw_j.as_str(),
    };

    // Rule 2: If `lines` is given (even alongside `grep`), slice line range FIRST.
    if let Some((start_1, end_1)) = lines_range {
        let ls: Vec<&str> = text_view.lines().collect();
        if ls.is_empty() {
            return Ok(String::new());
        }
        let s = start_1.max(1).min(ls.len());
        let e = end_1.max(s).min(ls.len());
        let mut sliced: Vec<(usize, &str)> = (s..=e).map(|i| (i, ls[i - 1])).collect();
        if let Some(q) = grep {
            if sliced.len() > 12 {
                let filt: Vec<(usize, &str)> = sliced
                    .iter()
                    .copied()
                    .filter(|(_, ln)| !find_pattern_matches(ln, q, 1).is_empty())
                    .collect();
                if !filt.is_empty() {
                    sliced = filt;
                }
            }
        }
        let mut out = String::new();
        let mut used = 0usize;
        for (i, line) in sliced {
            let formatted = format!("L{}: {}", i, line);
            let sep_cost = if out.is_empty() { 0 } else { 1 };
            let f_chars = formatted.chars().count();
            if used.saturating_add(sep_cost).saturating_add(f_chars) > cap {
                let rem = cap.saturating_sub(used.saturating_add(sep_cost));
                if rem > 16 {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(truncate_utf8_chars(&formatted, rem.saturating_sub(11)));
                    out.push_str("...[capped]");
                }
                break;
            }
            if !out.is_empty() {
                out.push('\n');
                used += 1;
            }
            out.push_str(&formatted);
            used += f_chars;
        }
        let san = sanitize_r2_fences(&out);
        return Ok(truncate_utf8_chars(&san, cap).to_string());
    }

    // Rule 3 & 4: Bounded-wildcard multi-match `grep` with giant-line windowing + quote/backslash fallback.
    if let Some(q) = grep {
        let ls: Vec<&str> = text_view.lines().collect();
        let run_grep = |pat: &str, normalize_quotes: bool| -> Vec<String> {
            let mut found: Vec<String> = Vec::new();
            let mut budget = cap;
            for (idx, line) in ls.iter().enumerate() {
                let hay_owned: String;
                let hay: &str = if normalize_quotes {
                    hay_owned = line.replace("\\\"", "\"").replace("\\'", "'");
                    hay_owned.as_str()
                } else {
                    line
                };
                if hay.len() > 400 {
                    let mut last_end = 0usize;
                    for (ms, me) in find_pattern_matches(hay, pat, 6) {
                        if ms < last_end {
                            continue;
                        }
                        let mut s0 = ms.saturating_sub(160);
                        while s0 > 0 && !hay.is_char_boundary(s0) {
                            s0 -= 1;
                        }
                        let mut e0 = (me + 220).min(hay.len());
                        while e0 < hay.len() && !hay.is_char_boundary(e0) {
                            e0 += 1;
                        }
                        let raw_chunk = format!("L{}@{}..{}: {}", idx + 1, s0, e0, &hay[s0..e0]);
                        let chunk = truncate_utf8_chars(&raw_chunk, budget).to_string();
                        let clen = chunk.chars().count();
                        found.push(chunk);
                        last_end = e0;
                        budget = budget.saturating_sub(clen + 5);
                        if budget <= 120 || found.len() >= 6 {
                            break;
                        }
                    }
                } else if !find_pattern_matches(hay, pat, 1).is_empty() {
                    let lo = idx.saturating_sub(1);
                    let hi = (idx + 2).min(ls.len());
                    let raw_chunk = (lo..hi)
                        .map(|j| format!("L{}: {}", j + 1, truncate_utf8_chars(ls[j], 350)))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let chunk = truncate_utf8_chars(&raw_chunk, budget).to_string();
                    let clen = chunk.chars().count();
                    found.push(chunk);
                    budget = budget.saturating_sub(clen + 5);
                }
                if budget <= 120 || found.len() >= 6 {
                    break;
                }
            }
            found
        };

        let mut matches = run_grep(q, false);
        if matches.is_empty() {
            let q_lit = unescape_grep_literal(q);
            matches = run_grep(&q_lit, true);
        }
        let out = if matches.is_empty() {
            format!(
                "NO_MATCH for grep={:?} in {} (len={} chars, {} lines)",
                q,
                if ptr.is_empty() { "/" } else { ptr },
                text_view.chars().count(),
                ls.len()
            )
        } else {
            matches.join("\n---\n")
        };
        let san = sanitize_r2_fences(&out);
        return Ok(truncate_utf8_chars(&san, cap).to_string());
    }

    // Plain char-bounded pagination slice
    let (window, total_chars) = slice_utf8_chars(text_view, offset, cap);
    let mut out = window.to_string();
    if offset.saturating_add(cap) < total_chars {
        let note = format!(
            "\n...[truncated {}..{} of {} chars]",
            offset,
            offset.saturating_add(window.chars().count()),
            total_chars
        );
        let note_len = note.chars().count();
        if cap > note_len {
            let trimmed = truncate_utf8_chars(&out, cap - note_len).to_string();
            out = trimmed;
            out.push_str(&note);
        }
    }
    let san = sanitize_r2_fences(&out);
    Ok(truncate_utf8_chars(&san, cap).to_string())
}
