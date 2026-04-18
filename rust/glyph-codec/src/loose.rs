//! GLYPH loose mode canonicalization
//!
//! Provides deterministic canonical string representation for GValues
//! in schema-optional mode. Used for hashing, comparison, and deduplication.

use crate::types::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use sha2::{Sha256, Digest};
use std::collections::HashSet;

/// Null style for canonicalization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullStyle {
    /// Use underscore: _
    #[default]
    Underscore,
    /// Use unicode symbol: ∅
    Symbol,
}

/// Options for loose canonicalization
#[derive(Debug, Clone)]
pub struct LooseCanonOpts {
    /// Enable auto-tabular mode for homogeneous arrays
    pub auto_tabular: bool,
    /// Minimum rows for tabular mode
    pub min_rows: usize,
    /// Maximum columns for tabular mode
    pub max_cols: usize,
    /// Allow missing keys in tabular (fill with null)
    pub allow_missing: bool,
    /// Null value style
    pub null_style: NullStyle,
}

impl Default for LooseCanonOpts {
    fn default() -> Self {
        Self {
            auto_tabular: true,
            min_rows: 3,
            max_cols: 64,
            allow_missing: true,
            null_style: NullStyle::Underscore,
        }
    }
}

impl LooseCanonOpts {
    /// Options optimized for LLM output (same as default)
    pub fn llm() -> Self {
        Self::default()
    }

    /// Options with pretty unicode null symbol
    pub fn pretty() -> Self {
        Self {
            null_style: NullStyle::Symbol,
            ..Self::default()
        }
    }

    /// Options with tabular disabled
    pub fn no_tabular() -> Self {
        Self {
            auto_tabular: false,
            ..Self::default()
        }
    }
}

/// Canonicalize a GValue to GLYPH string with default options
pub fn canonicalize_loose(v: &GValue) -> String {
    canonicalize_loose_with_opts(v, &LooseCanonOpts::default())
}

/// Canonicalize without tabular mode
pub fn canonicalize_loose_no_tabular(v: &GValue) -> String {
    canonicalize_loose_with_opts(v, &LooseCanonOpts::no_tabular())
}

/// Canonicalize with custom options
pub fn canonicalize_loose_with_opts(v: &GValue, opts: &LooseCanonOpts) -> String {
    let mut buf = String::new();
    write_canon_loose(&mut buf, v, opts);
    buf
}

/// Get fingerprint (canonical form) of a GValue
pub fn fingerprint_loose(v: &GValue) -> String {
    canonicalize_loose(v)
}

/// Get SHA-256 hash of canonical form (first 16 hex chars)
pub fn hash_loose(v: &GValue) -> String {
    let canonical = canonicalize_loose(v);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    hex::encode(&result[..8])
}

/// Check if two GValues are semantically equal
pub fn equal_loose(a: &GValue, b: &GValue) -> bool {
    canonicalize_loose(a) == canonicalize_loose(b)
}

// ============================================================
// Internal canonicalization
// ============================================================

fn write_canon_loose(buf: &mut String, v: &GValue, opts: &LooseCanonOpts) {
    match v {
        GValue::Null => buf.push_str(canon_null(opts.null_style)),
        GValue::Bool(b) => buf.push(if *b { 't' } else { 'f' }),
        GValue::Int(n) => buf.push_str(&canon_int(*n)),
        GValue::Float(f) => buf.push_str(&canon_float(*f)),
        GValue::Str(s) => buf.push_str(&canon_string(s)),
        GValue::Bytes(data) => write_canon_bytes(buf, data),
        GValue::Time(t) => buf.push_str(&t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        GValue::Id(ref_id) => write_canon_ref(buf, ref_id),
        GValue::List(items) => write_canon_list(buf, items, opts),
        GValue::Map(entries) => write_canon_map(buf, entries, opts),
        GValue::Struct(s) => write_canon_struct(buf, s, opts),
        GValue::Sum(s) => write_canon_sum(buf, s, opts),
    }
}

fn canon_null(style: NullStyle) -> &'static str {
    match style {
        NullStyle::Underscore => "_",
        NullStyle::Symbol => "∅",
    }
}

fn canon_int(n: i64) -> String {
    n.to_string()
}

fn canon_float(f: f64) -> String {
    if f.is_nan() {
        return "\"NaN\"".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_negative() {
            "\"-Inf\"".to_string()
        } else {
            "\"Inf\"".to_string()
        };
    }

    // Handle negative zero
    let f = if f == 0.0 { 0.0 } else { f };

    // Check if it's a whole number
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{}", f as i64);
    }

    // Use shortest representation
    let abs = f.abs();
    let exp = abs.log10().floor() as i32;

    if exp < -4 || exp >= 15 {
        // Use exponential notation
        let mantissa = f / 10f64.powi(exp);
        if exp >= 0 {
            format!("{}e+{:02}", format_mantissa(mantissa), exp)
        } else {
            format!("{}e{:03}", format_mantissa(mantissa), exp)
        }
    } else {
        // Use decimal notation
        format_decimal(f)
    }
}

fn format_mantissa(m: f64) -> String {
    let s = format!("{:.15}", m);
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn format_decimal(f: f64) -> String {
    let s = format!("{:.15}", f);
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// Check if a string is safe to emit without quotes
fn is_bare_safe(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Must not start with a digit, quote, or special char
    let first = s.chars().next().unwrap();
    if first.is_ascii_digit() || first == '"' || first == '\'' || first == '-' {
        return false;
    }

    // Reserved words
    let reserved = ["t", "f", "true", "false", "null", "none", "nil", "_"];
    if reserved.contains(&s) {
        return false;
    }

    // Must contain only safe characters
    s.chars().all(|c| {
        c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/' || c == '@' || c == ':'
            || (c as u32 > 127) // Allow unicode
    })
}

fn canon_string(s: &str) -> String {
    if is_bare_safe(s) {
        s.to_string()
    } else {
        quote_string(s)
    }
}

fn quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn write_canon_bytes(buf: &mut String, data: &[u8]) {
    buf.push_str("b64\"");
    buf.push_str(&BASE64.encode(data));
    buf.push('"');
}

fn write_canon_ref(buf: &mut String, ref_id: &RefId) {
    buf.push('^');
    if !ref_id.prefix.is_empty() {
        buf.push_str(&ref_id.prefix);
        buf.push(':');
    }
    // Ref IDs allow more characters as bare (including starting with digits)
    if is_ref_bare_safe(&ref_id.value) {
        buf.push_str(&ref_id.value);
    } else {
        buf.push_str(&quote_string(&ref_id.value));
    }
}

/// Check if a ref ID value is safe to emit without quotes
/// (more permissive than regular strings - allows starting with digits)
fn is_ref_bare_safe(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Must contain only safe characters (no spaces, quotes, etc.)
    s.chars().all(|c| {
        c.is_alphanumeric() || c == '_' || c == '-' || c == '.'
            || (c as u32 > 127) // Allow unicode
    })
}

fn write_canon_list(buf: &mut String, items: &[GValue], opts: &LooseCanonOpts) {
    // Try tabular if enabled
    if opts.auto_tabular {
        if let Some(tabular) = try_emit_tabular(items, opts) {
            buf.push_str(&tabular);
            return;
        }
    }

    buf.push('[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            buf.push(' ');
        }
        write_canon_loose(buf, item, opts);
    }
    buf.push(']');
}

fn write_canon_map(buf: &mut String, entries: &[MapEntry], opts: &LooseCanonOpts) {
    buf.push('{');

    // Sort entries by canonical key
    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by(|a, b| canon_string(&a.key).cmp(&canon_string(&b.key)));

    for (i, entry) in sorted.iter().enumerate() {
        if i > 0 {
            buf.push(' ');
        }
        buf.push_str(&canon_string(&entry.key));
        buf.push('=');
        write_canon_loose(buf, &entry.value, opts);
    }
    buf.push('}');
}

fn write_canon_struct(buf: &mut String, s: &StructValue, opts: &LooseCanonOpts) {
    buf.push_str(&s.type_name);
    buf.push('{');

    // Sort fields by canonical key
    let mut sorted: Vec<_> = s.fields.iter().collect();
    sorted.sort_by(|a, b| canon_string(&a.key).cmp(&canon_string(&b.key)));

    for (i, field) in sorted.iter().enumerate() {
        if i > 0 {
            buf.push(' ');
        }
        buf.push_str(&canon_string(&field.key));
        buf.push('=');
        write_canon_loose(buf, &field.value, opts);
    }
    buf.push('}');
}

fn write_canon_sum(buf: &mut String, s: &SumValue, opts: &LooseCanonOpts) {
    buf.push_str(&s.tag);
    buf.push('(');
    if let Some(ref value) = s.value {
        write_canon_loose(buf, value, opts);
    }
    buf.push(')');
}

// ============================================================
// Auto-tabular detection and emission
// ============================================================

fn try_emit_tabular(items: &[GValue], opts: &LooseCanonOpts) -> Option<String> {
    if items.len() < opts.min_rows {
        return None;
    }

    // Collect keys from all items
    let mut all_keys: HashSet<String> = HashSet::new();
    let mut row_keys: Vec<HashSet<String>> = Vec::new();

    for item in items {
        let keys = get_object_keys(item)?;
        let key_set: HashSet<String> = keys.into_iter().collect();
        all_keys.extend(key_set.clone());
        row_keys.push(key_set);
    }

    // Don't use tabular for empty objects or too many columns
    if all_keys.is_empty() || all_keys.len() > opts.max_cols {
        return None;
    }

    // Check homogeneity
    if !opts.allow_missing {
        // Strict mode: all items must have identical keys
        let first_keys = &row_keys[0];
        for keys in &row_keys[1..] {
            if keys != first_keys {
                return None;
            }
        }
    } else {
        // Allow missing, but check that at least 50% keys are common
        let mut common_keys: HashSet<String> = row_keys[0].clone();
        for keys in &row_keys[1..] {
            common_keys = common_keys.intersection(keys).cloned().collect();
        }

        // If less than half the keys are common, don't use tabular
        if common_keys.len() * 2 < all_keys.len() {
            return None;
        }
    }

    // Sort columns
    let mut cols: Vec<String> = all_keys.into_iter().collect();
    cols.sort_by(|a, b| canon_string(a).cmp(&canon_string(b)));

    // Build tabular output
    let mut buf = String::new();
    buf.push_str(&format!(
        "@tab _ rows={} cols={} [{}]\n",
        items.len(),
        cols.len(),
        cols.iter().map(|c| canon_string(c)).collect::<Vec<_>>().join(" ")
    ));

    for item in items {
        buf.push('|');
        let values = get_object_values(item);
        for col in &cols {
            let cell = values.get(col).map(|v| {
                let mut cell_buf = String::new();
                write_canon_loose(&mut cell_buf, v, opts);
                cell_buf.replace('\\', "\\\\").replace('|', "\\|").replace('\n', "\\n")
            }).unwrap_or_else(|| canon_null(opts.null_style).to_string());
            buf.push_str(&cell);
            buf.push('|');
        }
        buf.push('\n');
    }
    buf.push_str("@end");

    Some(buf)
}

fn get_object_keys(v: &GValue) -> Option<Vec<String>> {
    match v {
        GValue::Map(entries) => Some(entries.iter().map(|e| e.key.clone()).collect()),
        GValue::Struct(s) => Some(s.fields.iter().map(|f| f.key.clone()).collect()),
        _ => None,
    }
}

fn get_object_values(v: &GValue) -> std::collections::HashMap<String, &GValue> {
    match v {
        GValue::Map(entries) => entries.iter().map(|e| (e.key.clone(), &e.value)).collect(),
        GValue::Struct(s) => s.fields.iter().map(|f| (f.key.clone(), &f.value)).collect(),
        _ => std::collections::HashMap::new(),
    }
}

// Add hex encoding for hash
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
