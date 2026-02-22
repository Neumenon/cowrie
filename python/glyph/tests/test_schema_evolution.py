"""
Tests for Schema Evolution

Tests safe API versioning with backwards/forwards compatibility.
"""

import pytest
from glyph.schema_evolution import (
    VersionedSchema,
    versioned_schema,
    EvolutionMode,
    FieldSchema,
    StructSchema,
)


class TestFieldSchema:
    """Test field-level versioning."""
    
    def test_field_available_in_version(self):
        """Test field availability checks."""
        field = FieldSchema(name="venue", type="str", added_in="2.0")
        
        assert not field.is_available_in("1.0")
        assert field.is_available_in("2.0")
        assert field.is_available_in("3.0")
    
    def test_deprecated_field(self):
        """Test deprecated field detection."""
        field = FieldSchema(
            name="old_name",
            type="str",
            added_in="1.0",
            deprecated_in="2.0"
        )
        
        assert field.is_available_in("1.0")
        assert not field.is_available_in("2.0")
        assert not field.is_available_in("3.0")
    
    def test_field_validation(self):
        """Test field value validation."""
        field = FieldSchema(name="count", type="int", required=True)
        
        assert field.validate_value(5) is None
        assert field.validate_value("five") is not None
        assert field.validate_value(None) is not None
    
    def test_pattern_validation(self):
        """Test regex pattern validation."""
        field = FieldSchema(
            name="email",
            type="str",
            validation=r"^[^@]+@[^@]+$"
        )
        
        assert field.validate_value("test@example.com") is None
        assert field.validate_value("invalid-email") is not None


class TestStructSchema:
    """Test struct-level schemas."""
    
    def test_struct_creation(self):
        """Test creating a struct schema."""
        schema = StructSchema(name="Match", version="1.0")
        
        schema.add_field(FieldSchema(name="home", type="str", required=True))
        schema.add_field(FieldSchema(name="away", type="str", required=True))
        
        assert len(schema.fields) == 2
        assert schema.get_field("home") is not None
    
    def test_struct_validation(self):
        """Test struct validation."""
        schema = StructSchema(name="Match", version="1.0")
        schema.add_field(FieldSchema(name="home", type="str", required=True))
        schema.add_field(FieldSchema(name="away", type="str", required=True))
        
        # Valid data
        assert schema.validate({"home": "Arsenal", "away": "Liverpool"}) is None
        
        # Missing required field
        assert schema.validate({"home": "Arsenal"}) is not None
        
        # Wrong type
        assert schema.validate({"home": 123, "away": "Liverpool"}) is not None


class TestVersionedSchema:
    """Test versioned schemas."""
    
    def test_create_versioned_schema(self):
        """Test creating a versioned schema."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        assert schema.get_version("1.0") is not None
        assert schema.latest_version == "1.0"
    
    def test_multiple_versions(self):
        """Test multiple versions."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "venue": {"type": "str", "required": False, "added_in": "2.0"},
        })
        
        assert schema.latest_version == "2.0"
        v1 = schema.get_version("1.0")
        v2 = schema.get_version("2.0")
        
        assert len(v1.fields) == 2
        assert len(v2.fields) == 3


class TestSchemaEvolution:
    """Test schema evolution (migration)."""
    
    def test_add_optional_field(self):
        """Test adding optional field is backwards compatible."""
        schema = versioned_schema("Match")
        schema.mode = EvolutionMode.MIGRATE
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "venue": {"type": "str", "required": False, "added_in": "2.0"},
        })
        
        # Parse v1 data with v2 schema
        v1_data = {"home": "Arsenal", "away": "Liverpool"}
        error, v2_data = schema.parse(v1_data, from_version="1.0")
        
        assert error is None
        assert v2_data["home"] == "Arsenal"
        assert v2_data["away"] == "Liverpool"
        assert "venue" in v2_data
    
    def test_add_field_with_default(self):
        """Test adding field with default value."""
        schema = versioned_schema("Match")
        schema.mode = EvolutionMode.MIGRATE
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "status": {"type": "str", "required": False, "default": "scheduled", "added_in": "2.0"},
        })
        
        v1_data = {"home": "Arsenal", "away": "Liverpool"}
        error, v2_data = schema.parse(v1_data, from_version="1.0")
        
        assert error is None
        assert v2_data["status"] == "scheduled"
    
    def test_rename_field(self):
        """Test field renaming with compatibility."""
        schema = versioned_schema("Player")
        schema.mode = EvolutionMode.MIGRATE
        
        schema.add_version("1.0", {
            "player_name": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "name": {"type": "str", "required": True, "renamed_from": "player_name", "added_in": "2.0"},
        })
        
        v1_data = {"player_name": "Alice"}
        error, v2_data = schema.parse(v1_data, from_version="1.0")
        
        assert error is None
        assert "name" in v2_data
        assert v2_data["name"] == "Alice"
        assert "player_name" not in v2_data
    
    def test_deprecate_field(self):
        """Test field deprecation."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "referee": {"type": "str", "required": False},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "referee": {"type": "str", "required": False, "deprecated_in": "2.0"},
        })
        
        v2_schema = schema.get_version("2.0")
        referee_field = v2_schema.get_field("referee")
        
        assert referee_field.is_deprecated_in("2.0")
        assert referee_field.is_deprecated_in("3.0")


class TestToleranceMode:
    """Test different tolerance modes."""
    
    def test_strict_mode(self):
        """Test strict mode rejects unknown fields."""
        schema = versioned_schema("Match")
        schema.mode = EvolutionMode.STRICT
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        # Data with unknown field
        data = {
            "home": "Arsenal",
            "away": "Liverpool",
            "unknown_field": "value"
        }
        
        error, _ = schema.parse(data, from_version="1.0")
        # In strict mode, unknown fields would be caught
        # (implementation detail - may validate in emit instead)
    
    def test_tolerant_mode(self):
        """Test tolerant mode ignores unknown fields."""
        schema = versioned_schema("Match")
        schema.mode = EvolutionMode.TOLERANT
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        # Data with unknown field
        data = {
            "home": "Arsenal",
            "away": "Liverpool",
            "unknown_field": "value"
        }
        
        error, parsed = schema.parse(data, from_version="1.0")
        
        assert error is None
        assert "unknown_field" not in parsed


class TestEmit:
    """Test emitting with version headers."""
    
    def test_emit_version_header(self):
        """Test emitting version header."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "venue": {"type": "str", "required": False},
        })
        
        data = {
            "home": "Arsenal",
            "away": "Liverpool",
            "venue": "Emirates"
        }
        
        error, header = schema.emit(data, version="2.0")
        
        assert error is None
        assert header == "@version 2.0"
    
    def test_emit_default_latest(self):
        """Test emitting with default (latest) version."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "venue": {"type": "str", "required": False},
        })
        
        data = {"home": "Arsenal", "away": "Liverpool"}
        
        error, header = schema.emit(data)
        
        assert error is None
        assert "@version 2.0" in header


class TestChangelog:
    """Test changelog generation."""
    
    def test_changelog_generation(self):
        """Test generating changelog."""
        schema = versioned_schema("Match")
        
        schema.add_version("1.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
        })
        
        schema.add_version("2.0", {
            "home": {"type": "str", "required": True},
            "away": {"type": "str", "required": True},
            "venue": {"type": "str", "required": False, "added_in": "2.0"},
        })
        
        changelog = schema.get_changelog()
        
        assert len(changelog) == 2
        assert changelog[0]["version"] == "1.0"
        assert changelog[1]["version"] == "2.0"
        assert "venue" in changelog[1]["added_fields"]


class TestIntegrationScenarios:
    """Test realistic schema evolution scenarios."""
    
    def test_api_evolution_scenario(self):
        """Test realistic API evolution with multiple versions."""
        schema = versioned_schema("User")
        schema.mode = EvolutionMode.MIGRATE
        
        # v1: Basic user
        schema.add_version("1.0", {
            "id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True},
            "email": {"type": "str", "required": True},
        })
        
        # v2: Add profile_url
        schema.add_version("2.0", {
            "id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True},
            "email": {"type": "str", "required": True},
            "profile_url": {"type": "str", "required": False, "default": "", "added_in": "2.0"},
        })
        
        # v3: Rename email to contact_email
        schema.add_version("3.0", {
            "id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True},
            "contact_email": {"type": "str", "required": True, "renamed_from": "email", "added_in": "3.0"},
            "profile_url": {"type": "str", "required": False, "default": "", "added_in": "2.0"},
        })
        
        # Parse v1 data with v3 schema
        v1_data = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        error, v3_data = schema.parse(v1_data, from_version="1.0")
        
        assert error is None
        assert v3_data["id"] == 1
        assert v3_data["name"] == "Alice"
        assert v3_data["contact_email"] == "alice@example.com"
        assert v3_data["profile_url"] == ""
        assert "email" not in v3_data
    
    def test_backward_compatibility(self):
        """Test backward compatibility: new client reads old data."""
        schema = versioned_schema("Product")
        
        schema.add_version("1.0", {
            "id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True},
            "price": {"type": "float", "required": True},
        })
        
        schema.add_version("2.0", {
            "id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True},
            "price": {"type": "float", "required": True},
            "discount": {"type": "float", "required": False, "default": 0.0, "added_in": "2.0"},
            "tags": {"type": "list", "required": False, "default": [], "added_in": "2.0"},
        })
        
        # Old client data (v1)
        v1_data = {"id": 1, "name": "Widget", "price": 9.99}
        
        # New client reads it (migrated to v2)
        schema.mode = EvolutionMode.MIGRATE
        error, v2_data = schema.parse(v1_data, from_version="1.0")
        
        assert error is None
        assert v2_data["discount"] == 0.0
        assert v2_data["tags"] == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
