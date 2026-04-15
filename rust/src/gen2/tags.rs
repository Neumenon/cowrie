//! Wire format tag constants for gen2. Must match the Go reference.
//!
//! Used by both encode and decode. Keeping them here prevents drift
//! between the two sides (a prior duplicate had FLOAT32 missing from
//! encode.rs).

pub const NULL: u8 = 0x00;
pub const FALSE: u8 = 0x01;
pub const TRUE: u8 = 0x02;
pub const INT64: u8 = 0x03;
pub const FLOAT64: u8 = 0x04;
pub const STRING: u8 = 0x05;
pub const ARRAY: u8 = 0x06;
pub const OBJECT: u8 = 0x07;
pub const BYTES: u8 = 0x08;
pub const UINT64: u8 = 0x09;
pub const DECIMAL128: u8 = 0x0A;
pub const DATETIME64: u8 = 0x0B;
pub const UUID128: u8 = 0x0C;
pub const BIGINT: u8 = 0x0D;
pub const EXT: u8 = 0x0E;
pub const FLOAT32: u8 = 0x0F; // compact float32 → decoded as f64
pub const TENSOR: u8 = 0x20;
pub const TENSOR_REF: u8 = 0x21;
pub const IMAGE: u8 = 0x22;
pub const AUDIO: u8 = 0x23;
pub const BITMASK: u8 = 0x24;
pub const ADJLIST: u8 = 0x30;
pub const RICHTEXT: u8 = 0x31;
pub const DELTA: u8 = 0x32;
// Graph types (v2.1)
pub const NODE: u8 = 0x35;
pub const EDGE: u8 = 0x36;
pub const NODE_BATCH: u8 = 0x37;
pub const EDGE_BATCH: u8 = 0x38;
pub const GRAPH_SHARD: u8 = 0x39;
// v3 inline types
pub const FIXINT_BASE: u8 = 0x40;
pub const FIXINT_MAX: u8 = 0xBF;
pub const FIXARRAY_BASE: u8 = 0xC0;
pub const FIXARRAY_MAX: u8 = 0xCF;
pub const FIXMAP_BASE: u8 = 0xD0;
pub const FIXMAP_MAX: u8 = 0xDF;
pub const FIXNEG_BASE: u8 = 0xE0;
pub const FIXNEG_MAX: u8 = 0xEF;
