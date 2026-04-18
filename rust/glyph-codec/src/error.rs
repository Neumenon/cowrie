//! Error types for GLYPH codec

use thiserror::Error;

/// Errors that can occur during GLYPH operations
#[derive(Error, Debug)]
pub enum GlyphError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("JSON conversion error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid float: {0}")]
    InvalidFloat(String),

    #[error("Missing required field: {0}")]
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, GlyphError>;
