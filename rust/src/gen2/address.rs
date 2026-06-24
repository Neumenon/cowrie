//! Content addressing (SPEC-v1 §3).
//!
//! A content address is a **multihash** SHA-256 of the canonical, *uncompressed* wire bytes:
//! the 2 prefix bytes `0x12 0x20` (sha2-256 multihash code, 32-byte digest length) followed by
//! the 32 raw SHA-256 digest bytes — 34 bytes total, conventionally rendered as 68 lowercase
//! hex chars. The bytes that are hashed are EXACTLY what the canonical encoder produces (the same
//! bytes `recode` prints), never compressed and never the decoded in-memory value.
//!
//! Equal canonical bytes ⇒ equal address, across every conforming implementation.

use super::encode::encode;
use super::types::{CowrieError, Value};
use sha2::{Digest, Sha256};

/// sha2-256 multihash prefix: code `0x12`, digest length `0x20` (32).
const MULTIHASH_PREFIX: [u8; 2] = [0x12, 0x20];

/// Content address (§3) of already-canonical, uncompressed wire bytes: a multihash SHA-256 —
/// `0x12 0x20` (sha2-256, 32-byte length) followed by the 32 digest bytes (34 bytes total).
pub fn address_of_bytes(canonical: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(canonical);
    let mut out = Vec::with_capacity(2 + 32);
    out.extend_from_slice(&MULTIHASH_PREFIX);
    out.extend_from_slice(&digest);
    out
}

/// Content address (§3) of a value: encode it canonically, then [`address_of_bytes`]. This is the
/// v1 per-value identity — equal canonical bytes ⇒ equal address.
pub fn content_address(value: &Value) -> Result<Vec<u8>, CowrieError> {
    let canonical = encode(value)?;
    Ok(address_of_bytes(&canonical))
}

/// Render a content address (or any byte slice) as lowercase hex.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Content address of a value as lowercase hex (68 chars). Convenience over
/// [`content_address`] + [`to_hex`].
pub fn content_address_hex(value: &Value) -> Result<String, CowrieError> {
    Ok(to_hex(&content_address(value)?))
}
