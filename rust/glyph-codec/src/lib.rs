//! GLYPH Codec - Token-efficient serialization for AI agents
//!
//! GLYPH is a serialization format designed for LLM tool calls that provides
//! 30-50% token savings over JSON while remaining human-readable.
//!
//! # Example
//!
//! ```rust
//! use glyph_codec::{from_json, canonicalize_loose, GValue};
//! use serde_json::json;
//!
//! let data = json!({"action": "search", "query": "weather"});
//! let gvalue = from_json(&data);
//! let glyph = canonicalize_loose(&gvalue);
//! assert_eq!(glyph, "{action=search query=weather}");
//! ```

mod types;
mod loose;
mod json_bridge;
mod error;
pub mod decimal128;
pub mod schema_evolution;
pub mod stream_validator;

pub use types::*;
pub use loose::*;
pub use json_bridge::*;
pub use error::*;
pub use decimal128::*;
pub use schema_evolution::*;
pub use stream_validator::{
    ArgSchema, ToolSchema, ToolRegistry, ErrorCode, ValidationError, ValidatorState, TimelineEvent,
    StreamingValidator, ValidationResult, default_tool_registry, FieldValue as StreamFieldValue,
};

#[cfg(test)]
mod tests;
