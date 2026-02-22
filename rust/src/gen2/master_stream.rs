//! Master Stream - unified stream framing for Cowrie.
//!
//! Provides a higher-level stream format with:
//! - Type routing via schema fingerprints
//! - Optional compression (gzip/zstd)
//! - CRC32 checksums
//! - Metadata envelope
//! - Legacy stream compatibility

use std::io::Write;
use super::types::{Value, CowrieError};
use super::encode::encode;
use super::decode::decode;
use super::schema::schema_fingerprint32;
use crate::MAGIC;

/// Master Stream magic bytes: "SJST"
pub const MASTER_MAGIC: &[u8; 4] = b"SJST";

/// Master Stream version
pub const MASTER_VERSION: u8 = 0x02;

/// Master stream frame flags
pub mod flags {
    pub const COMPRESSED: u8 = 0x01;
    pub const CRC: u8 = 0x02;
    pub const DETERMINISTIC: u8 = 0x04;
    pub const META: u8 = 0x08;
    pub const COMP_GZIP: u8 = 0x10;
    pub const COMP_ZSTD: u8 = 0x20;
}

/// Master stream errors
#[derive(Debug)]
pub enum MasterStreamError {
    InvalidMagic,
    InvalidVersion(u8),
    Truncated,
    CrcMismatch,
    Sjson(CowrieError),
    Io(std::io::Error),
}

impl From<CowrieError> for MasterStreamError {
    fn from(e: CowrieError) -> Self {
        MasterStreamError::Sjson(e)
    }
}

impl From<std::io::Error> for MasterStreamError {
    fn from(e: std::io::Error) -> Self {
        MasterStreamError::Io(e)
    }
}

/// Master frame header
#[derive(Debug, Clone)]
pub struct MasterFrameHeader {
    pub version: u8,
    pub flags: u8,
    pub header_len: u16,
    pub type_id: u32,
    pub payload_len: u32,
    pub raw_len: u32,
    pub meta_len: u32,
}

/// Writer options
#[derive(Debug, Clone)]
pub struct MasterWriterOptions {
    pub deterministic: bool,
    pub enable_crc: bool,
    pub compress: bool,
}

impl Default for MasterWriterOptions {
    fn default() -> Self {
        MasterWriterOptions {
            deterministic: true,
            enable_crc: true,
            compress: false, // No compression by default (requires external crate)
        }
    }
}

/// A decoded master frame
#[derive(Debug)]
pub struct MasterFrame {
    pub header: MasterFrameHeader,
    pub meta: Option<Value>,
    pub payload: Value,
    pub type_id: u32,
}

/// Write a master stream frame
pub fn write_frame<W: Write>(
    writer: &mut W,
    value: &Value,
    meta: Option<&Value>,
    opts: &MasterWriterOptions,
) -> std::result::Result<(), MasterStreamError> {
    // Encode payload
    let payload = encode(value)?;

    // Encode metadata if present
    let meta_bytes = match meta {
        Some(m) => encode(m)?,
        None => Vec::new(),
    };

    // Compute type ID from schema
    let type_id = schema_fingerprint32(value);

    // Build flags
    let mut frame_flags: u8 = 0;
    if opts.deterministic {
        frame_flags |= flags::DETERMINISTIC;
    }
    if opts.enable_crc {
        frame_flags |= flags::CRC;
    }
    if !meta_bytes.is_empty() {
        frame_flags |= flags::META;
    }

    // Header length (fixed at 24 bytes for v2)
    let header_len: u16 = 24;

    // Build frame
    let mut frame = Vec::new();

    // Magic
    frame.extend_from_slice(MASTER_MAGIC);

    // Version
    frame.push(MASTER_VERSION);

    // Flags
    frame.push(frame_flags);

    // Header length
    frame.extend_from_slice(&header_len.to_le_bytes());

    // Type ID
    frame.extend_from_slice(&type_id.to_le_bytes());

    // Payload length
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());

    // Raw length (0 = not compressed)
    frame.extend_from_slice(&0u32.to_le_bytes());

    // Meta length
    frame.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());

    // Metadata
    frame.extend_from_slice(&meta_bytes);

    // Payload
    frame.extend_from_slice(&payload);

    // CRC32 if enabled
    if opts.enable_crc {
        let crc = crc32(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());
    }

    writer.write_all(&frame)?;
    Ok(())
}

/// Read a master stream frame
pub fn read_frame(data: &[u8]) -> std::result::Result<(MasterFrame, usize), MasterStreamError> {
    if data.len() < 24 {
        return Err(MasterStreamError::Truncated);
    }

    // Check magic
    if &data[0..4] != MASTER_MAGIC {
        return Err(MasterStreamError::InvalidMagic);
    }

    let version = data[4];
    if version != MASTER_VERSION {
        return Err(MasterStreamError::InvalidVersion(version));
    }

    let frame_flags = data[5];
    let header_len = u16::from_le_bytes([data[6], data[7]]) as usize;
    let type_id = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    let payload_len = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;
    let _raw_len = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let meta_len = u32::from_le_bytes([data[20], data[21], data[22], data[23]]) as usize;

    let mut pos = header_len.max(24);

    // Read metadata
    let meta = if frame_flags & flags::META != 0 && meta_len > 0 {
        if pos + meta_len > data.len() {
            return Err(MasterStreamError::Truncated);
        }
        let meta_data = &data[pos..pos + meta_len];
        pos += meta_len;
        Some(decode(meta_data)?)
    } else {
        pos += meta_len;
        None
    };

    // Read payload
    if pos + payload_len > data.len() {
        return Err(MasterStreamError::Truncated);
    }
    let payload_data = &data[pos..pos + payload_len];
    pos += payload_len;

    // Verify CRC
    if frame_flags & flags::CRC != 0 {
        if pos + 4 > data.len() {
            return Err(MasterStreamError::Truncated);
        }
        let expected_crc = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        let actual_crc = crc32(&data[0..pos]);
        if actual_crc != expected_crc {
            return Err(MasterStreamError::CrcMismatch);
        }
        pos += 4;
    }

    // Decode payload
    let payload = decode(payload_data)?;

    let header = MasterFrameHeader {
        version,
        flags: frame_flags,
        header_len: header_len as u16,
        type_id,
        payload_len: payload_len as u32,
        raw_len: 0,
        meta_len: meta_len as u32,
    };

    Ok((MasterFrame { header, meta, payload, type_id }, pos))
}

/// Check if data starts with master stream magic
pub fn is_master_stream(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == MASTER_MAGIC
}

/// Check if data starts with Cowrie document magic
pub fn is_cowrie_document(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    if &data[0..2] != MAGIC {
        return false;
    }
    // Exclude master stream format
    if data.len() >= 4 && &data[2..4] == b"ST" {
        return false;
    }
    true
}

/// Simple CRC32-IEEE implementation
pub fn crc32(data: &[u8]) -> u32 {
    const POLYNOMIAL: u32 = 0xEDB88320;

    // Build table
    let table: [u32; 256] = {
        let mut t = [0u32; 256];
        for i in 0..256 {
            let mut c = i as u32;
            for _ in 0..8 {
                if c & 1 != 0 {
                    c = POLYNOMIAL ^ (c >> 1);
                } else {
                    c >>= 1;
                }
            }
            t[i] = c;
        }
        t
    };

    let mut crc = 0xFFFFFFFF;
    for &byte in data {
        crc = table[((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let value = Value::object(vec![
            ("id", Value::Int(12345)),
            ("name", Value::String("test".into())),
        ]);

        let mut buf = Vec::new();
        let opts = MasterWriterOptions::default();
        write_frame(&mut buf, &value, None, &opts).unwrap();

        let (frame, len) = read_frame(&buf).unwrap();
        assert_eq!(len, buf.len());
        assert_eq!(frame.payload.get("id").unwrap().as_i64(), Some(12345));
        assert_eq!(frame.payload.get("name").unwrap().as_str(), Some("test"));
    }

    #[test]
    fn test_with_meta() {
        let value = Value::object(vec![("content", Value::String("data".into()))]);
        let meta = Value::object(vec![
            ("source", Value::String("test".into())),
        ]);

        let mut buf = Vec::new();
        let opts = MasterWriterOptions::default();
        write_frame(&mut buf, &value, Some(&meta), &opts).unwrap();

        let (frame, _) = read_frame(&buf).unwrap();
        assert!(frame.meta.is_some());
        assert_eq!(
            frame.meta.unwrap().get("source").unwrap().as_str(),
            Some("test")
        );
    }

    #[test]
    fn test_crc_verification() {
        let value = Value::Int(42);

        let mut buf = Vec::new();
        let opts = MasterWriterOptions { enable_crc: true, ..Default::default() };
        write_frame(&mut buf, &value, None, &opts).unwrap();

        // Corrupt the data
        if buf.len() > 10 {
            buf[10] ^= 0xFF;
        }

        let result = read_frame(&buf);
        assert!(matches!(result, Err(MasterStreamError::CrcMismatch)));
    }

    #[test]
    fn test_is_master_stream() {
        assert!(is_master_stream(b"SJST\x02\x00"));
        assert!(!is_master_stream(b"SJ\x02\x00"));
        assert!(!is_master_stream(b""));
    }

    #[test]
    fn test_is_cowrie_document() {
        assert!(is_cowrie_document(b"SJ\x02\x00"));
        assert!(!is_cowrie_document(b"SJST"));
        assert!(!is_cowrie_document(b""));
    }

    #[test]
    fn test_type_id_consistency() {
        let v1 = Value::object(vec![("a", Value::Int(1))]);
        let v2 = Value::object(vec![("a", Value::Int(999))]);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        let opts = MasterWriterOptions::default();

        write_frame(&mut buf1, &v1, None, &opts).unwrap();
        write_frame(&mut buf2, &v2, None, &opts).unwrap();

        let (frame1, _) = read_frame(&buf1).unwrap();
        let (frame2, _) = read_frame(&buf2).unwrap();

        // Same schema = same type ID
        assert_eq!(frame1.type_id, frame2.type_id);
    }
}
