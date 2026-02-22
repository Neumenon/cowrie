//! Schema Evolution - Safe API versioning for GLYPH
//!
//! Enables schemas to evolve safely without breaking clients. Supports:
//! - Adding new optional fields
//! - Renaming fields (with compatibility mapping)
//! - Deprecating fields
//! - Changing defaults
//! - Strict vs tolerant parsing modes

use std::collections::HashMap;
use regex::Regex;

// ============================================================
// Evolution Mode
// ============================================================

/// Schema evolution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvolutionMode {
    /// Fail on unknown fields
    Strict,
    /// Ignore unknown fields (default)
    Tolerant,
    /// Auto-migrate between versions
    Migrate,
}

impl Default for EvolutionMode {
    fn default() -> Self {
        EvolutionMode::Tolerant
    }
}

// ============================================================
// Field Types
// ============================================================

/// Field types for schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Str,
    Int,
    Float,
    Bool,
    List,
    Decimal,
}

// ============================================================
// Evolving Field
// ============================================================

/// A field in a versioned schema.
#[derive(Debug, Clone)]
pub struct EvolvingField {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub default: Option<FieldValue>,
    pub added_in: String,
    pub deprecated_in: Option<String>,
    pub renamed_from: Option<String>,
    pub validation: Option<Regex>,
}

impl EvolvingField {
    /// Create a new field.
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            required: false,
            default: None,
            added_in: "1.0".to_string(),
            deprecated_in: None,
            renamed_from: None,
            validation: None,
        }
    }

    /// Mark as required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Set default value.
    pub fn with_default(mut self, value: FieldValue) -> Self {
        self.default = Some(value);
        self
    }

    /// Set the version when field was added.
    pub fn added_in(mut self, version: impl Into<String>) -> Self {
        self.added_in = version.into();
        self
    }

    /// Set the version when field was deprecated.
    pub fn deprecated_in(mut self, version: impl Into<String>) -> Self {
        self.deprecated_in = Some(version.into());
        self
    }

    /// Set the previous field name (for renames).
    pub fn renamed_from(mut self, name: impl Into<String>) -> Self {
        self.renamed_from = Some(name.into());
        self
    }

    /// Set validation pattern.
    pub fn with_validation(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.validation = Some(Regex::new(pattern)?);
        Ok(self)
    }

    /// Check if field is available in a given version.
    pub fn is_available_in(&self, version: &str) -> bool {
        if compare_versions(version, &self.added_in) < 0 {
            return false;
        }

        if let Some(ref dep) = self.deprecated_in {
            if compare_versions(version, dep) >= 0 {
                return false;
            }
        }

        true
    }

    /// Check if field is deprecated in a given version.
    pub fn is_deprecated_in(&self, version: &str) -> bool {
        match &self.deprecated_in {
            Some(dep) => compare_versions(version, dep) >= 0,
            None => false,
        }
    }

    /// Validate a value against this field.
    pub fn validate(&self, value: &FieldValue) -> Result<(), String> {
        if matches!(value, FieldValue::Null) {
            if self.required {
                return Err(format!("field {} is required", self.name));
            }
            return Ok(());
        }

        // Type checking
        match (self.field_type, value) {
            (FieldType::Str, FieldValue::Str(s)) => {
                if let Some(ref re) = self.validation {
                    if !re.is_match(s) {
                        return Err(format!("field {} does not match pattern", self.name));
                    }
                }
            }
            (FieldType::Str, _) => return Err(format!("field {} must be string", self.name)),
            (FieldType::Int, FieldValue::Int(_)) => {}
            (FieldType::Int, _) => return Err(format!("field {} must be int", self.name)),
            (FieldType::Float, FieldValue::Float(_)) => {}
            (FieldType::Float, FieldValue::Int(_)) => {} // Allow int as float
            (FieldType::Float, _) => return Err(format!("field {} must be float", self.name)),
            (FieldType::Bool, FieldValue::Bool(_)) => {}
            (FieldType::Bool, _) => return Err(format!("field {} must be bool", self.name)),
            (FieldType::List, FieldValue::List(_)) => {}
            (FieldType::List, _) => return Err(format!("field {} must be list", self.name)),
            (FieldType::Decimal, FieldValue::Str(_)) => {} // Decimal stored as string
            (FieldType::Decimal, _) => return Err(format!("field {} must be decimal", self.name)),
        }

        Ok(())
    }
}

// ============================================================
// Field Value
// ============================================================

/// Dynamic field value.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<FieldValue>),
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self { FieldValue::Bool(v) }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self { FieldValue::Int(v) }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self { FieldValue::Float(v) }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self { FieldValue::Str(v.to_string()) }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self { FieldValue::Str(v) }
}

// ============================================================
// Version Schema
// ============================================================

/// Schema for a specific version.
#[derive(Debug, Clone)]
pub struct VersionSchema {
    pub name: String,
    pub version: String,
    pub fields: HashMap<String, EvolvingField>,
    pub description: String,
}

impl VersionSchema {
    /// Create a new version schema.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            fields: HashMap::new(),
            description: String::new(),
        }
    }

    /// Add a field.
    pub fn add_field(&mut self, field: EvolvingField) {
        self.fields.insert(field.name.clone(), field);
    }

    /// Get a field by name.
    pub fn get_field(&self, name: &str) -> Option<&EvolvingField> {
        self.fields.get(name)
    }

    /// Validate data against this schema.
    pub fn validate(&self, data: &HashMap<String, FieldValue>) -> Result<(), String> {
        // Check required fields
        for (name, field) in &self.fields {
            if field.required && !data.contains_key(name) {
                return Err(format!("missing required field: {}", name));
            }
        }

        // Validate field values
        for (name, value) in data {
            if let Some(field) = self.fields.get(name) {
                field.validate(value)?;
            }
        }

        Ok(())
    }
}

// ============================================================
// Versioned Schema
// ============================================================

/// Multi-version schema manager.
#[derive(Debug, Clone)]
pub struct VersionedSchema {
    pub name: String,
    pub versions: HashMap<String, VersionSchema>,
    pub latest_version: String,
    pub mode: EvolutionMode,
}

impl VersionedSchema {
    /// Create a new versioned schema.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            versions: HashMap::new(),
            latest_version: "1.0".to_string(),
            mode: EvolutionMode::Tolerant,
        }
    }

    /// Set evolution mode.
    pub fn with_mode(mut self, mode: EvolutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Add a version with fields.
    pub fn add_version(&mut self, version: impl Into<String>, fields: Vec<EvolvingField>) {
        let version = version.into();
        let mut schema = VersionSchema::new(&self.name, &version);

        for field in fields {
            schema.add_field(field);
        }

        self.versions.insert(version.clone(), schema);
        self.latest_version = self.get_latest_version();
    }

    /// Get schema for a specific version.
    pub fn get_version(&self, version: &str) -> Option<&VersionSchema> {
        self.versions.get(version)
    }

    /// Parse data from a specific version.
    pub fn parse(
        &self,
        data: HashMap<String, FieldValue>,
        from_version: &str,
    ) -> Result<HashMap<String, FieldValue>, String> {
        let schema = self.get_version(from_version)
            .ok_or_else(|| format!("unknown version: {}", from_version))?;

        // Validate in strict mode
        if self.mode == EvolutionMode::Strict {
            schema.validate(&data)?;
        }

        let mut result = data.clone();

        // Migrate to latest if needed
        if from_version != self.latest_version {
            result = self.migrate(data, from_version, &self.latest_version)?;
        }

        // Filter unknown fields in tolerant mode
        if self.mode == EvolutionMode::Tolerant {
            if let Some(target_schema) = self.get_version(&self.latest_version) {
                result.retain(|k, _| target_schema.fields.contains_key(k));
            }
        }

        Ok(result)
    }

    /// Emit version header for data.
    pub fn emit(&self, data: &HashMap<String, FieldValue>, version: Option<&str>) -> Result<String, String> {
        let target_version = version.unwrap_or(&self.latest_version);

        let schema = self.get_version(target_version)
            .ok_or_else(|| format!("unknown version: {}", target_version))?;

        schema.validate(data)?;

        Ok(format!("@version {}", target_version))
    }

    /// Migrate data from one version to another.
    fn migrate(
        &self,
        data: HashMap<String, FieldValue>,
        from_version: &str,
        to_version: &str,
    ) -> Result<HashMap<String, FieldValue>, String> {
        let path = self.get_migration_path(from_version, to_version)
            .ok_or_else(|| format!("cannot migrate from {} to {}", from_version, to_version))?;

        let mut current_data = data;
        let mut current_version = from_version.to_string();

        for next_version in path {
            current_data = self.migrate_step(current_data, &current_version, &next_version)?;
            current_version = next_version;
        }

        Ok(current_data)
    }

    /// Migrate one step.
    fn migrate_step(
        &self,
        data: HashMap<String, FieldValue>,
        _from_version: &str,
        to_version: &str,
    ) -> Result<HashMap<String, FieldValue>, String> {
        let to_schema = self.get_version(to_version)
            .ok_or_else(|| "invalid version".to_string())?;

        let mut result = data;

        // Handle field renames
        for (name, field) in &to_schema.fields {
            if let Some(ref old_name) = field.renamed_from {
                if result.contains_key(old_name) && !result.contains_key(name) {
                    if let Some(value) = result.remove(old_name) {
                        result.insert(name.clone(), value);
                    }
                }
            }
        }

        // Handle new fields with defaults
        for (name, field) in &to_schema.fields {
            if !result.contains_key(name) {
                if let Some(ref default) = field.default {
                    result.insert(name.clone(), default.clone());
                } else if !field.required {
                    result.insert(name.clone(), FieldValue::Null);
                }
            }
        }

        // Remove unknown fields in tolerant mode
        if self.mode == EvolutionMode::Tolerant {
            result.retain(|k, _| to_schema.fields.contains_key(k));
        }

        Ok(result)
    }

    /// Get migration path between versions.
    fn get_migration_path(&self, from_version: &str, to_version: &str) -> Option<Vec<String>> {
        let mut versions: Vec<String> = self.versions.keys().cloned().collect();
        versions.sort_by(|a, b| compare_versions(a, b).cmp(&0));

        let from_idx = versions.iter().position(|v| v == from_version)?;
        let to_idx = versions.iter().position(|v| v == to_version)?;

        if from_idx < to_idx {
            Some(versions[from_idx + 1..=to_idx].to_vec())
        } else if from_idx > to_idx {
            None // Downgrade not supported
        } else {
            Some(vec![])
        }
    }

    /// Get the latest version string.
    fn get_latest_version(&self) -> String {
        let mut versions: Vec<&String> = self.versions.keys().collect();
        versions.sort_by(|a, b| compare_versions(a, b).cmp(&0));
        versions.last().map(|s| s.to_string()).unwrap_or_else(|| "1.0".to_string())
    }

    /// Get changelog of schema evolution.
    pub fn get_changelog(&self) -> Vec<ChangelogEntry> {
        let mut versions: Vec<&String> = self.versions.keys().collect();
        versions.sort_by(|a, b| compare_versions(a, b).cmp(&0));

        versions.iter().map(|version| {
            let schema = &self.versions[*version];

            let added_fields: Vec<String> = schema.fields.iter()
                .filter(|(_, f)| f.added_in == **version)
                .map(|(name, _)| name.clone())
                .collect();

            let deprecated_fields: Vec<String> = schema.fields.iter()
                .filter(|(_, f)| f.deprecated_in.as_ref() == Some(*version))
                .map(|(name, _)| name.clone())
                .collect();

            let renamed_fields: Vec<(String, String)> = schema.fields.iter()
                .filter(|(_, f)| f.renamed_from.is_some())
                .map(|(name, f)| (f.renamed_from.clone().unwrap(), name.clone()))
                .collect();

            ChangelogEntry {
                version: (*version).clone(),
                description: schema.description.clone(),
                added_fields,
                deprecated_fields,
                renamed_fields,
            }
        }).collect()
    }
}

// ============================================================
// Changelog Entry
// ============================================================

/// Changelog entry for a version.
#[derive(Debug, Clone)]
pub struct ChangelogEntry {
    pub version: String,
    pub description: String,
    pub added_fields: Vec<String>,
    pub deprecated_fields: Vec<String>,
    pub renamed_fields: Vec<(String, String)>,
}

// ============================================================
// Helper Functions
// ============================================================

/// Compare two version strings.
/// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2.
fn compare_versions(v1: &str, v2: &str) -> i32 {
    let parts1: Vec<i32> = v1.split('.').map(|s| s.parse().unwrap_or(0)).collect();
    let parts2: Vec<i32> = v2.split('.').map(|s| s.parse().unwrap_or(0)).collect();

    let max_len = parts1.len().max(parts2.len());
    for i in 0..max_len {
        let p1 = parts1.get(i).copied().unwrap_or(0);
        let p2 = parts2.get(i).copied().unwrap_or(0);

        if p1 < p2 {
            return -1;
        }
        if p1 > p2 {
            return 1;
        }
    }

    0
}

/// Parse a version header (e.g., "@version 2.0").
pub fn parse_version_header(text: &str) -> Option<String> {
    let text = text.trim();
    if !text.starts_with("@version ") {
        return None;
    }
    let version = text[9..].trim();
    if version.is_empty() {
        return None;
    }
    Some(version.to_string())
}

/// Format a version header.
pub fn format_version_header(version: &str) -> String {
    format!("@version {}", version)
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        assert_eq!(compare_versions("1.0", "1.0"), 0);
        assert_eq!(compare_versions("1.0", "2.0"), -1);
        assert_eq!(compare_versions("2.0", "1.0"), 1);
        assert_eq!(compare_versions("1.0", "1.1"), -1);
    }

    #[test]
    fn test_field_availability() {
        let field = EvolvingField::new("venue", FieldType::Str)
            .added_in("2.0");

        assert!(!field.is_available_in("1.0"));
        assert!(field.is_available_in("2.0"));
        assert!(field.is_available_in("2.1"));
    }

    #[test]
    fn test_field_deprecation() {
        let field = EvolvingField::new("referee", FieldType::Str)
            .added_in("1.0")
            .deprecated_in("3.0");

        assert!(field.is_available_in("2.0"));
        assert!(!field.is_available_in("3.0"));
        assert!(field.is_deprecated_in("3.0"));
    }

    #[test]
    fn test_versioned_schema_basic() {
        let mut schema = VersionedSchema::new("Match");

        schema.add_version("1.0", vec![
            EvolvingField::new("home", FieldType::Str).required(),
            EvolvingField::new("away", FieldType::Str).required(),
        ]);

        let v = schema.get_version("1.0").unwrap();
        assert!(v.get_field("home").is_some());
        assert!(v.get_field("away").is_some());
    }

    #[test]
    fn test_versioned_schema_migration() {
        let mut schema = VersionedSchema::new("Match");

        schema.add_version("1.0", vec![
            EvolvingField::new("home", FieldType::Str).required(),
            EvolvingField::new("away", FieldType::Str).required(),
        ]);

        schema.add_version("2.0", vec![
            EvolvingField::new("home", FieldType::Str).required(),
            EvolvingField::new("away", FieldType::Str).required(),
            EvolvingField::new("venue", FieldType::Str).added_in("2.0"),
        ]);

        let mut data = HashMap::new();
        data.insert("home".to_string(), FieldValue::Str("Arsenal".to_string()));
        data.insert("away".to_string(), FieldValue::Str("Liverpool".to_string()));

        let result = schema.parse(data, "1.0").unwrap();
        assert!(result.contains_key("venue"));
    }

    #[test]
    fn test_parse_version_header() {
        assert_eq!(parse_version_header("@version 2.0"), Some("2.0".to_string()));
        assert_eq!(parse_version_header("version 2.0"), None);
        assert_eq!(parse_version_header("@version"), None);
    }

    #[test]
    fn test_format_version_header() {
        assert_eq!(format_version_header("2.0"), "@version 2.0");
    }
}
