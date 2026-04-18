//! Core GLYPH types

use chrono::{DateTime, Utc};

/// GLYPH value type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum GValue {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (i64)
    Int(i64),
    /// Floating point value (f64)
    Float(f64),
    /// String value
    Str(String),
    /// Binary data (bytes)
    Bytes(Vec<u8>),
    /// Timestamp (UTC)
    Time(DateTime<Utc>),
    /// Reference ID with optional prefix
    Id(RefId),
    /// Ordered list of values
    List(Vec<GValue>),
    /// Key-value map (ordered by key)
    Map(Vec<MapEntry>),
    /// Typed struct with name and fields
    Struct(StructValue),
    /// Sum type (tagged union)
    Sum(SumValue),
}

/// Reference ID with optional prefix
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefId {
    pub prefix: String,
    pub value: String,
}

impl RefId {
    pub fn new(prefix: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            value: value.into(),
        }
    }

    pub fn simple(value: impl Into<String>) -> Self {
        Self {
            prefix: String::new(),
            value: value.into(),
        }
    }
}

/// Map entry (key-value pair)
#[derive(Debug, Clone, PartialEq)]
pub struct MapEntry {
    pub key: String,
    pub value: GValue,
}

impl MapEntry {
    pub fn new(key: impl Into<String>, value: GValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

/// Typed struct value
#[derive(Debug, Clone, PartialEq)]
pub struct StructValue {
    pub type_name: String,
    pub fields: Vec<MapEntry>,
}

impl StructValue {
    pub fn new(type_name: impl Into<String>, fields: Vec<MapEntry>) -> Self {
        Self {
            type_name: type_name.into(),
            fields,
        }
    }
}

/// Sum type (tagged union)
#[derive(Debug, Clone, PartialEq)]
pub struct SumValue {
    pub tag: String,
    pub value: Option<Box<GValue>>,
}

impl SumValue {
    pub fn new(tag: impl Into<String>, value: Option<GValue>) -> Self {
        Self {
            tag: tag.into(),
            value: value.map(Box::new),
        }
    }
}

// ============================================================
// Builder functions
// ============================================================

impl GValue {
    /// Create a null value
    pub fn null() -> Self {
        GValue::Null
    }

    /// Create a boolean value
    pub fn bool(v: bool) -> Self {
        GValue::Bool(v)
    }

    /// Create an integer value
    pub fn int(v: i64) -> Self {
        GValue::Int(v)
    }

    /// Create a float value
    pub fn float(v: f64) -> Self {
        GValue::Float(v)
    }

    /// Create a string value
    pub fn str(v: impl Into<String>) -> Self {
        GValue::Str(v.into())
    }

    /// Create a bytes value
    pub fn bytes(v: Vec<u8>) -> Self {
        GValue::Bytes(v)
    }

    /// Create a timestamp value
    pub fn time(v: DateTime<Utc>) -> Self {
        GValue::Time(v)
    }

    /// Create a reference ID
    pub fn id(prefix: impl Into<String>, value: impl Into<String>) -> Self {
        GValue::Id(RefId::new(prefix, value))
    }

    /// Create a simple reference ID (no prefix)
    pub fn simple_id(value: impl Into<String>) -> Self {
        GValue::Id(RefId::simple(value))
    }

    /// Create a list value
    pub fn list(items: Vec<GValue>) -> Self {
        GValue::List(items)
    }

    /// Create a map value
    pub fn map(entries: Vec<MapEntry>) -> Self {
        GValue::Map(entries)
    }

    /// Create a struct value
    pub fn struct_val(type_name: impl Into<String>, fields: Vec<MapEntry>) -> Self {
        GValue::Struct(StructValue::new(type_name, fields))
    }

    /// Create a sum type value
    pub fn sum(tag: impl Into<String>, value: Option<GValue>) -> Self {
        GValue::Sum(SumValue::new(tag, value))
    }

    // ============================================================
    // Type checking
    // ============================================================

    pub fn is_null(&self) -> bool {
        matches!(self, GValue::Null)
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, GValue::Bool(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, GValue::Int(_))
    }

    pub fn is_float(&self) -> bool {
        matches!(self, GValue::Float(_))
    }

    pub fn is_str(&self) -> bool {
        matches!(self, GValue::Str(_))
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, GValue::Bytes(_))
    }

    pub fn is_time(&self) -> bool {
        matches!(self, GValue::Time(_))
    }

    pub fn is_id(&self) -> bool {
        matches!(self, GValue::Id(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, GValue::List(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self, GValue::Map(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, GValue::Struct(_))
    }

    pub fn is_sum(&self) -> bool {
        matches!(self, GValue::Sum(_))
    }

    // ============================================================
    // Value extraction
    // ============================================================

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            GValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            GValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            GValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            GValue::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            GValue::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<&DateTime<Utc>> {
        match self {
            GValue::Time(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_id(&self) -> Option<&RefId> {
        match self {
            GValue::Id(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[GValue]> {
        match self {
            GValue::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[MapEntry]> {
        match self {
            GValue::Map(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> Option<&StructValue> {
        match self {
            GValue::Struct(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_sum(&self) -> Option<&SumValue> {
        match self {
            GValue::Sum(v) => Some(v),
            _ => None,
        }
    }

    /// Get a value from a map or struct by key
    pub fn get(&self, key: &str) -> Option<&GValue> {
        match self {
            GValue::Map(entries) => entries.iter().find(|e| e.key == key).map(|e| &e.value),
            GValue::Struct(s) => s.fields.iter().find(|e| e.key == key).map(|e| &e.value),
            _ => None,
        }
    }

    /// Get a value from a list by index
    pub fn index(&self, idx: usize) -> Option<&GValue> {
        match self {
            GValue::List(items) => items.get(idx),
            _ => None,
        }
    }
}

/// Helper to create a map entry
pub fn field(key: impl Into<String>, value: GValue) -> MapEntry {
    MapEntry::new(key, value)
}
