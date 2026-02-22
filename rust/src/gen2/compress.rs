//! Compression support for SJSON Gen2.
//!
//! Provides framed encoding/decoding with optional gzip or zstd compression.

use super::types::{Value, SjsonError};
use super::encode::encode;
use super::decode::decode;
use crate::{MAGIC, VERSION};
use std::io::{Read, Write};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;

/// Compression type for framed encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None = 0,
    Gzip = 1,
    Zstd = 2,
}

/// Flag bits in the header.
const FLAG_COMPRESSED: u8 = 0x01;
const COMP_TYPE_MASK: u8 = 0x06;
const COMP_TYPE_SHIFT: u8 = 1;

/// Encode a value with compression framing.
///
/// The framed format adds a 4-byte header before the payload:
/// - Bytes 0-1: Magic "SJ"
/// - Byte 2: Version (0x02)
/// - Byte 3: Flags (compression info)
/// - If compressed: uvarint of uncompressed length, then compressed data
/// - Otherwise: raw SJSON data (after first 4 bytes)
pub fn encode_framed(value: &Value, compression: Compression) -> Result<Vec<u8>, SjsonError> {
    // First encode normally
    let raw = encode(value)?;

    if compression == Compression::None {
        return Ok(raw);
    }

    // Build flags
    let comp_bits = match compression {
        Compression::None => 0,
        Compression::Gzip => 1,
        Compression::Zstd => 2,
    };
    let flags = FLAG_COMPRESSED | ((comp_bits << COMP_TYPE_SHIFT) & COMP_TYPE_MASK);

    // Compress the data after the header
    let payload = &raw[4..];
    let compressed = match compression {
        Compression::Gzip => compress_gzip(payload)?,
        Compression::Zstd => compress_zstd(payload)?,
        Compression::None => unreachable!(),
    };

    // Build output: header + uncompressed length + compressed data
    let mut out = Vec::with_capacity(4 + 10 + compressed.len());
    out.push(raw[0]); // 'S'
    out.push(raw[1]); // 'J'
    out.push(raw[2]); // version
    out.push(flags);

    // Write uncompressed length as uvarint
    let mut len = payload.len() as u64;
    while len >= 0x80 {
        out.push((len as u8) | 0x80);
        len >>= 7;
    }
    out.push(len as u8);

    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decode a framed SJSON value, automatically decompressing if needed.
pub fn decode_framed(data: &[u8]) -> Result<Value, SjsonError> {
    decode_framed_with_limit(data, 100 * 1024 * 1024) // 100MB default limit
}

/// Decode a framed SJSON value with a maximum decompressed size limit.
pub fn decode_framed_with_limit(data: &[u8], max_size: usize) -> Result<Value, SjsonError> {
    if data.len() < 4 {
        return Err(SjsonError::Truncated);
    }

    // Check magic + version
    if &data[0..2] != MAGIC {
        return Err(SjsonError::InvalidMagic);
    }
    if data[2] != VERSION {
        return Err(SjsonError::InvalidVersion(data[2]));
    }

    let flags = data[3];

    if (flags & FLAG_COMPRESSED) == 0 {
        // Not compressed, decode directly
        return decode(data);
    }

    // Get compression type
    let comp_type = (flags & COMP_TYPE_MASK) >> COMP_TYPE_SHIFT;

    // Read uncompressed length
    let mut pos = 4;
    let mut uncompressed_len: u64 = 0;
    let mut shift = 0;
    loop {
        if pos >= data.len() {
            return Err(SjsonError::Truncated);
        }
        let b = data[pos];
        pos += 1;
        uncompressed_len |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(SjsonError::InvalidData("varint overflow".into()));
        }
    }

    if uncompressed_len as usize > max_size {
        return Err(SjsonError::TooLarge);
    }

    // Decompress
    let compressed = &data[pos..];
    let decompressed = match comp_type {
        1 => decompress_gzip(compressed, max_size)?,
        2 => decompress_zstd(compressed, max_size)?,
        _ => return Err(SjsonError::InvalidData("unknown compression".into())),
    };

    if decompressed.len() != uncompressed_len as usize {
        return Err(SjsonError::InvalidData("decompressed length mismatch".into()));
    }

    // Reconstruct full SJSON data with header
    let mut full = Vec::with_capacity(4 + decompressed.len());
    full.push(data[0]); // 'S'
    full.push(data[1]); // 'J'
    full.push(data[2]); // version
    full.push(0);       // flags = 0 (no compression)
    full.extend_from_slice(&decompressed);

    decode(&full)
}

fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, SjsonError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
    encoder.write_all(data).map_err(|e| SjsonError::InvalidData(e.to_string()))?;
    encoder.finish().map_err(|e| SjsonError::InvalidData(e.to_string()))
}

fn decompress_gzip(data: &[u8], max_size: usize) -> Result<Vec<u8>, SjsonError> {
    let mut decoder = GzDecoder::new(data);
    if max_size == 0 {
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).map_err(|e| SjsonError::InvalidData(e.to_string()))?;
        return Ok(out);
    }

    let mut out = Vec::new();
    let mut limited = decoder.take((max_size as u64) + 1);
    limited.read_to_end(&mut out).map_err(|e| SjsonError::InvalidData(e.to_string()))?;
    if out.len() > max_size {
        return Err(SjsonError::TooLarge);
    }
    Ok(out)
}

#[cfg(feature = "zstd")]
fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, SjsonError> {
    zstd::encode_all(data, 3).map_err(|e| SjsonError::InvalidData(e.to_string()))
}

#[cfg(not(feature = "zstd"))]
fn compress_zstd(_data: &[u8]) -> Result<Vec<u8>, SjsonError> {
    Err(SjsonError::InvalidData("zstd feature not enabled".into()))
}

#[cfg(feature = "zstd")]
fn decompress_zstd(data: &[u8], max_size: usize) -> Result<Vec<u8>, SjsonError> {
    let mut decoder = zstd::stream::read::Decoder::new(data)
        .map_err(|e| SjsonError::InvalidData(e.to_string()))?;
    if max_size == 0 {
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).map_err(|e| SjsonError::InvalidData(e.to_string()))?;
        return Ok(out);
    }

    let mut out = Vec::new();
    let mut limited = decoder.take((max_size as u64) + 1);
    limited.read_to_end(&mut out).map_err(|e| SjsonError::InvalidData(e.to_string()))?;
    if out.len() > max_size {
        return Err(SjsonError::TooLarge);
    }
    Ok(out)
}

#[cfg(not(feature = "zstd"))]
fn decompress_zstd(_data: &[u8], _max_size: usize) -> Result<Vec<u8>, SjsonError> {
    Err(SjsonError::InvalidData("zstd feature not enabled".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_roundtrip_no_compression() {
        let mut obj = BTreeMap::new();
        obj.insert("name".to_string(), Value::String("test".to_string()));
        obj.insert("value".to_string(), Value::Int(42));
        let value = Value::Object(obj);

        let encoded = encode_framed(&value, Compression::None).unwrap();
        let decoded = decode_framed(&encoded).unwrap();

        if let Value::Object(dec_obj) = decoded {
            assert_eq!(dec_obj.get("name"), Some(&Value::String("test".to_string())));
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn test_roundtrip_gzip() {
        let mut obj = BTreeMap::new();
        obj.insert("data".to_string(), Value::String("x".repeat(1000)));
        let value = Value::Object(obj);

        let encoded = encode_framed(&value, Compression::Gzip).unwrap();
        let decoded = decode_framed(&encoded).unwrap();

        if let Value::Object(dec_obj) = decoded {
            if let Some(Value::String(s)) = dec_obj.get("data") {
                assert_eq!(s.len(), 1000);
            } else {
                panic!("expected string");
            }
        } else {
            panic!("expected object");
        }
    }
}
