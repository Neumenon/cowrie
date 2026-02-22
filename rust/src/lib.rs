//! Cowrie - Structured JSON binary codec.
//!
//! This crate provides two codecs for binary JSON serialization:
//!
//! - **Gen1**: Lightweight codec with proto-tensor support (11 core types + graph types)
//! - **Gen2**: Full Cowrie v2 with ML extensions (18+ types, dictionary coding, compression)
//!
//! # Quick Start
//!
//! ## Gen1 (Lightweight)
//! ```
//! use cowrie_codec::gen1::{encode, decode, Value};
//!
//! let val = Value::Object(vec![
//!     ("name".to_string(), Value::String("Alice".to_string())),
//!     ("embedding".to_string(), Value::Float64Array(vec![0.1, 0.2, 0.3])),
//! ]);
//!
//! let encoded = encode(&val).unwrap();
//! let decoded = decode(&encoded).unwrap();
//! ```
//!
//! ## Gen2 (Full)
//! ```
//! use cowrie_codec::gen2::{encode, decode, Value, DType, TensorData};
//!
//! let val = Value::object(vec![
//!     ("name", Value::String("Alice".into())),
//!     ("embedding", Value::Tensor(TensorData {
//!         dtype: DType::Float32,
//!         shape: vec![384],
//!         data: vec![0u8; 384 * 4], // 384 float32s
//!     })),
//! ]);
//!
//! let encoded = encode(&val).unwrap();
//! let decoded = decode(&encoded).unwrap();
//! ```
//!
//! # Feature Comparison
//!
//! | Feature | Gen1 | Gen2 |
//! |---------|------|------|
//! | Core types | 11 | 13+ |
//! | ML types | proto-tensors | Tensor, Image, Audio, etc. |
//! | Dictionary coding | No | Yes |
//! | Compression | No | gzip/zstd |
//! | Schema fingerprint | No | Yes |
//! | Graph types | 6 | Adjlist |
//!
//! # Choosing a Codec
//!
//! Use **Gen1** when:
//! - You need a simple, lightweight codec
//! - You're working with GNN/graph data
//! - You want minimal dependencies
//!
//! Use **Gen2** when:
//! - You need full ML/multimodal support
//! - You want dictionary-coded keys for repeated schemas
//! - You need compression
//! - You need schema fingerprinting

pub mod gen1;
pub mod gen2;

/// Cowrie magic bytes: "SJ" (used by Gen2)
pub const MAGIC: &[u8; 2] = b"SJ";

/// Cowrie version 2 (Gen2)
pub const VERSION: u8 = 0x02;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen1_basic() {
        use gen1::{encode, decode, Value};

        let val = Value::Object(vec![
            ("name".to_string(), Value::String("test".to_string())),
            ("count".to_string(), Value::Int64(42)),
        ]);

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        assert_eq!(val, decoded);
    }

    #[test]
    fn test_gen2_basic() {
        use gen2::{encode, decode, Value};

        let val = Value::object(vec![
            ("name", Value::String("test".into())),
            ("count", Value::Int(42)),
        ]);

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        assert_eq!(val, decoded);
    }
}
