//! Gen2: Full Cowrie v2 codec with ML extensions.
//!
//! Gen2 provides the complete Cowrie v2 implementation with:
//! - Dictionary-coded object keys
//! - 18+ types including ML extensions (Tensor, Image, Audio, etc.)
//! - Schema fingerprinting
//! - MasterStream protocol for framing
//! - Compression (gzip, optional zstd)
//! - JSON bridge

pub mod types;
pub mod encode;
pub mod decode;
pub mod schema;
pub mod master_stream;
pub mod compress;
pub mod json;

// Re-export main types
pub use types::{Value, DType, TensorData, ImageFormat, AudioEncoding, CowrieError};
pub use encode::{encode, encode_with_options, EncodeOptions};
pub use decode::decode;
pub use schema::{schema_fingerprint32, schema_fingerprint64, schema_equals};
pub use master_stream::{write_frame, read_frame, MasterWriterOptions, MasterFrame};
pub use compress::{encode_framed, decode_framed, Compression};
pub use json::{from_json, to_json, to_json_pretty};
