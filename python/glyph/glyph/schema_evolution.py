"""
Schema Evolution - Safe API versioning for GLYPH

Enables schemas to evolve safely without breaking clients. Supports:
  - Adding new optional fields
  - Renaming fields (with compatibility mapping)
  - Deprecating fields
  - Changing defaults
  - Strict vs tolerant parsing modes

Problem It Solves:
  v1: Match{home away}
  v2: Match{home away venue}  ← Parser fails on unknown field
  
Solution:
  Schema tracks versions and applies migrations automatically
  v1 clients can read v2 data (missing fields get defaults)
  v2 clients can read v1 data (new fields optional)

Example:
  schema = VersionedSchema("Match")
  
  schema.add_version("1.0", {
      "home": {"type": "str", "required": True},
      "away": {"type": "str", "required": True},
  })
  
  schema.add_version("2.0", {
      "home": {"type": "str", "required": True},
      "away": {"type": "str", "required": True},
      "venue": {"type": "str", "required": False, "added_in": "2.0"},
  })
  
  # Auto-migration: v1 → v2 fills in venue=None
  # Forward compat: v1 client ignores unknown venue field
"""

from __future__ import annotations
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import re


class EvolutionMode(Enum):
    """Schema evolution mode."""
    STRICT = "strict"      # Fail on unknown fields
    TOLERANT = "tolerant"  # Ignore unknown fields
    MIGRATE = "migrate"    # Auto-migrate between versions


@dataclass
class FieldSchema:
    """Schema for a single field."""
    name: str
    type: str  # "str", "int", "float", "bool", "list", "decimal"
    required: bool = False
    default: Optional[Any] = None
    added_in: str = "1.0"      # Version when field was added
    deprecated_in: Optional[str] = None  # Version when field was deprecated
    renamed_from: Optional[str] = None   # Previous name if renamed
    validation: Optional[str] = None     # Validation pattern (regex)
    
    def is_available_in(self, version: str) -> bool:
        """Check if field is available in a given version."""
        if self._version_compare(version, self.added_in) < 0:
            return False  # Field not added yet
        
        if self.deprecated_in and self._version_compare(version, self.deprecated_in) >= 0:
            return False  # Field is deprecated
        
        return True
    
    def is_deprecated_in(self, version: str) -> bool:
        """Check if field is deprecated in a given version."""
        if not self.deprecated_in:
            return False
        return self._version_compare(version, self.deprecated_in) >= 0
    
    @staticmethod
    def _version_compare(v1: str, v2: str) -> int:
        """Compare two version strings. Returns: -1 if v1<v2, 0 if v1==v2, 1 if v1>v2."""
        parts1 = [int(x) for x in v1.split('.')]
        parts2 = [int(x) for x in v2.split('.')]
        
        # Pad shorter version
        while len(parts1) < len(parts2):
            parts1.append(0)
        while len(parts2) < len(parts1):
            parts2.append(0)
        
        if parts1 < parts2:
            return -1
        elif parts1 > parts2:
            return 1
        else:
            return 0
    
    def validate_value(self, value: Any) -> Optional[str]:
        """Validate a value against this field schema. Returns error message if invalid."""
        if value is None:
            if self.required:
                return f"Field {self.name} is required"
            return None
        
        # Type checking
        if self.type == "str" and not isinstance(value, str):
            return f"Field {self.name} must be string"
        elif self.type == "int" and not isinstance(value, int):
            return f"Field {self.name} must be int"
        elif self.type == "float" and not isinstance(value, (int, float)):
            return f"Field {self.name} must be float"
        elif self.type == "bool" and not isinstance(value, bool):
            return f"Field {self.name} must be bool"
        elif self.type == "list" and not isinstance(value, list):
            return f"Field {self.name} must be list"
        
        # Pattern validation
        if self.validation and isinstance(value, str):
            if not re.match(self.validation, value):
                return f"Field {self.name} does not match pattern"
        
        return None


@dataclass
class StructSchema:
    """Schema for a struct version."""
    name: str
    version: str  # e.g., "1.0", "2.0"
    fields: Dict[str, FieldSchema] = dataclass_field(default_factory=dict)
    description: str = ""
    
    def add_field(self, field: FieldSchema):
        """Add a field to this version."""
        self.fields[field.name] = field
    
    def get_field(self, name: str) -> Optional[FieldSchema]:
        """Get a field by name."""
        return self.fields.get(name)
    
    def validate(self, data: Dict[str, Any]) -> Optional[str]:
        """Validate data against this schema version."""
        # Check required fields
        for field_name, field_schema in self.fields.items():
            if field_schema.required and field_name not in data:
                return f"Missing required field: {field_name}"
        
        # Check field values
        for field_name, value in data.items():
            field_schema = self.fields.get(field_name)
            if field_schema:
                error = field_schema.validate_value(value)
                if error:
                    return error
        
        return None


@dataclass
class VersionedSchema:
    """Schema with versioning support."""
    name: str
    versions: Dict[str, StructSchema] = dataclass_field(default_factory=dict)
    latest_version: str = "1.0"
    mode: EvolutionMode = EvolutionMode.TOLERANT
    
    def add_version(self, version: str, fields: Dict[str, Dict[str, Any]]):
        """
        Add a version to the schema.
        
        Args:
            version: Version string (e.g., "1.0", "2.0")
            fields: Dict of field_name -> field_config
        """
        struct_schema = StructSchema(name=self.name, version=version)
        
        for field_name, field_config in fields.items():
            field_schema = FieldSchema(
                name=field_name,
                type=field_config.get("type", "str"),
                required=field_config.get("required", False),
                default=field_config.get("default"),
                added_in=field_config.get("added_in", version),
                deprecated_in=field_config.get("deprecated_in"),
                renamed_from=field_config.get("renamed_from"),
                validation=field_config.get("validation"),
            )
            struct_schema.add_field(field_schema)
        
        self.versions[version] = struct_schema
        self.latest_version = self._get_latest_version()
    
    def get_version(self, version: str) -> Optional[StructSchema]:
        """Get schema for a specific version."""
        return self.versions.get(version)
    
    def parse(self, data: Dict[str, Any], from_version: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """
        Parse data from a specific version.

        Args:
            data: The data to parse
            from_version: Version the data is in

        Returns:
            (error_message, parsed_data)
        """
        schema = self.get_version(from_version)
        if not schema:
            return f"Unknown version: {from_version}", {}

        # Validate
        error = schema.validate(data)
        if error and self.mode == EvolutionMode.STRICT:
            return error, {}

        result = data.copy()

        # Migrate to latest if needed
        if from_version != self.latest_version:
            error, result = self._migrate(data, from_version, self.latest_version)
            if error:
                return error, {}

        # Filter unknown fields in tolerant mode
        if self.mode == EvolutionMode.TOLERANT:
            target_schema = self.get_version(self.latest_version)
            if target_schema:
                known_fields = set(target_schema.fields.keys())
                result = {k: v for k, v in result.items() if k in known_fields}

        return None, result
    
    def emit(self, data: Dict[str, Any], version: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Emit data for a specific version.
        
        Args:
            data: The data to emit
            version: Target version (default: latest)
            
        Returns:
            (error_message, version_header)
        """
        target_version = version or self.latest_version
        
        schema = self.get_version(target_version)
        if not schema:
            return f"Unknown version: {target_version}", ""
        
        # Validate
        error = schema.validate(data)
        if error:
            return error, ""
        
        # Format version header
        header = f"@version {target_version}"
        return None, header
    
    def _migrate(self, data: Dict[str, Any], from_version: str, to_version: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Migrate data from one version to another."""
        current_version = from_version
        current_data = data.copy()
        
        # Get migration path
        path = self._get_migration_path(from_version, to_version)
        if not path:
            return f"Cannot migrate from {from_version} to {to_version}", {}
        
        # Apply each migration step
        for next_version in path:
            error, current_data = self._migrate_step(current_data, current_version, next_version)
            if error:
                return error, {}
            current_version = next_version
        
        return None, current_data
    
    def _migrate_step(self, data: Dict[str, Any], from_version: str, to_version: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Migrate data from one version to the next."""
        from_schema = self.get_version(from_version)
        to_schema = self.get_version(to_version)
        
        if not from_schema or not to_schema:
            return "Invalid version", {}
        
        result = data.copy()
        
        # 1. Handle field renames
        for field_name, field_schema in to_schema.fields.items():
            if field_schema.renamed_from:
                old_name = field_schema.renamed_from
                if old_name in result and field_name not in result:
                    result[field_name] = result.pop(old_name)
        
        # 2. Handle new fields (add defaults)
        for field_name, field_schema in to_schema.fields.items():
            if field_name not in result:
                if field_schema.default is not None:
                    result[field_name] = field_schema.default
                elif not field_schema.required:
                    result[field_name] = None
        
        # 3. Handle deprecated fields (keep for now, mark for warning)
        deprecated_fields = []
        for field_name, field_schema in to_schema.fields.items():
            if field_schema.is_deprecated_in(to_version):
                deprecated_fields.append(field_name)
        
        # 4. Remove unknown fields (tolerant mode)
        if self.mode == EvolutionMode.TOLERANT:
            known_fields = set(to_schema.fields.keys())
            result = {k: v for k, v in result.items() if k in known_fields}
        
        return None, result
    
    def _get_migration_path(self, from_version: str, to_version: str) -> Optional[List[str]]:
        """Get the migration path between versions."""
        from_parts = [int(x) for x in from_version.split('.')]
        to_parts = [int(x) for x in to_version.split('.')]
        
        # Simple linear migration (assumes versions are sequential)
        # Real implementation would handle more complex paths
        versions = sorted(self.versions.keys(), key=lambda v: [int(x) for x in v.split('.')])
        
        try:
            from_idx = versions.index(from_version)
            to_idx = versions.index(to_version)
            
            if from_idx < to_idx:
                return versions[from_idx + 1:to_idx + 1]
            elif from_idx > to_idx:
                # Downgrade not supported
                return None
            else:
                return []
        except ValueError:
            return None
    
    def _get_latest_version(self) -> str:
        """Get the latest version string."""
        if not self.versions:
            return "1.0"
        
        versions = sorted(self.versions.keys(), key=lambda v: [int(x) for x in v.split('.')])
        return versions[-1]
    
    def get_changelog(self) -> List[Dict[str, Any]]:
        """Get changelog of schema evolution."""
        changelog = []
        versions = sorted(self.versions.keys(), key=lambda v: [int(x) for x in v.split('.')])
        
        for version in versions:
            schema = self.versions[version]
            
            changes = {
                "version": version,
                "description": schema.description,
                "added_fields": [],
                "deprecated_fields": [],
                "renamed_fields": [],
            }
            
            for field in schema.fields.values():
                if field.added_in == version:
                    changes["added_fields"].append(field.name)
                if field.deprecated_in == version:
                    changes["deprecated_fields"].append(field.name)
                if field.renamed_from:
                    changes["renamed_fields"].append((field.renamed_from, field.name))
            
            changelog.append(changes)
        
        return changelog


def versioned_schema(name: str) -> VersionedSchema:
    """Create a versioned schema."""
    return VersionedSchema(name=name)


# Example usage and integration with GLYPH:
# 
# schema = versioned_schema("Match")
# 
# schema.add_version("1.0", {
#     "home": {"type": "str", "required": True},
#     "away": {"type": "str", "required": True},
# })
# 
# schema.add_version("2.0", {
#     "home": {"type": "str", "required": True},
#     "away": {"type": "str", "required": True},
#     "venue": {"type": "str", "required": False, "added_in": "2.0"},
# })
# 
# # Parsing v1 data with v2 schema
# error, data = schema.parse(
#     {"home": "Arsenal", "away": "Liverpool"},
#     from_version="1.0"
# )
# # Auto-migrates: {"home": "Arsenal", "away": "Liverpool", "venue": None}
# 
# # Emitting with version header
# error, header = schema.emit(data, version="2.0")
# # Returns: "@version 2.0"
# 
# # Get changelog
# for change in schema.get_changelog():
#     print(f"v{change['version']}: added {change['added_fields']}")
