//! Gen2: Full Cowrie v2 codec with ML extensions.
//!
//! Gen2 provides the complete Cowrie v2 implementation with:
//! - Dictionary-coded object keys
//! - 18+ types including ML extensions (Tensor, Image, Audio, etc.)
//! - Schema fingerprinting
//! - MasterStream protocol for framing
//! - Compression (gzip, optional zstd)
//! - JSON bridge

pub mod address;
pub mod compress;
pub mod decode;
pub mod encode;
pub mod file;
pub mod json;
pub mod master_stream;
pub mod schema;
pub(crate) mod tags;
pub mod types;

// Re-export main types
pub use address::{address_of_bytes, content_address, content_address_hex, to_hex};
pub use compress::{decode_framed, decode_framed_strict, encode_framed, Compression};
pub use decode::{decode, decode_strict, decode_with_options, DecodeOptions};
pub use encode::{encode, encode_with_options, EncodeOptions};
pub use file::{decode_file, encode_file, file_identity, merkle_root};
pub use json::{from_json, to_json, to_json_pretty};
pub use master_stream::{read_frame, write_frame, MasterFrame, MasterWriterOptions};
pub use schema::{
    schema_equals, schema_fingerprint32, schema_fingerprint64, schema_fingerprint_equal,
};
pub use types::{AudioEncoding, CowrieError, DType, ImageFormat, TensorData, Value};
