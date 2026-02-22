package glyph

import "testing"

// ============================================================
// @open Struct Tests
// ============================================================

func TestOpenStruct_AcceptsUnknownFields(t *testing.T) {
	schema := NewSchemaBuilder().
		AddOpenStruct("Config", "v1",
			Field("name", PrimitiveType("str")),
			Field("port", PrimitiveType("int")),
		).
		Build()

	// Value with known + unknown fields
	value := Struct("Config",
		MapEntry{Key: "name", Value: Str("myapp")},
		MapEntry{Key: "port", Value: Int(8080)},
		MapEntry{Key: "debug", Value: Bool(true)}, // unknown
		MapEntry{Key: "timeout", Value: Int(30)},  // unknown
	)

	result := ValidateWithSchema(value, schema)

	// Should pass validation
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}

	// Should have warnings for unknown fields
	if len(result.Warnings) != 2 {
		t.Errorf("Expected 2 warnings for unknown fields, got %d: %v", len(result.Warnings), result.Warnings)
	}
}

func TestClosedStruct_RejectsUnknownFields(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Config", "v1",
			Field("name", PrimitiveType("str")),
			Field("port", PrimitiveType("int")),
		).
		Build()

	// Value with unknown field
	value := Struct("Config",
		MapEntry{Key: "name", Value: Str("myapp")},
		MapEntry{Key: "port", Value: Int(8080)},
		MapEntry{Key: "debug", Value: Bool(true)}, // unknown - should fail
	)

	result := ValidateWithSchema(value, schema)

	// Should fail validation
	if result.Valid {
		t.Error("Expected validation to fail for unknown field in closed struct")
	}

	// Should have error for unknown field
	found := false
	for _, err := range result.Errors {
		if err.Code == "unknown_field" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected unknown_field error")
	}
}

func TestStrictValidation_RejectsEvenOpenStruct(t *testing.T) {
	schema := NewSchemaBuilder().
		AddOpenStruct("Config", "v1",
			Field("name", PrimitiveType("str")),
		).
		Build()

	value := Struct("Config",
		MapEntry{Key: "name", Value: Str("myapp")},
		MapEntry{Key: "extra", Value: Bool(true)}, // unknown
	)

	// Normal validation should pass
	normalResult := ValidateWithSchema(value, schema)
	if !normalResult.Valid {
		t.Errorf("Normal validation should pass for @open struct")
	}

	// Strict validation should fail
	strictResult := ValidateStrict(value, schema)
	if strictResult.Valid {
		t.Error("Strict validation should fail even for @open struct")
	}
}

// ============================================================
// map<str,T> Validation Tests
// ============================================================

func TestMapStrInt_AcceptsValidValues(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Config", "v1",
			Field("name", PrimitiveType("str")),
			Field("settings", MapType(PrimitiveType("str"), PrimitiveType("int"))),
		).
		Build()

	// Map with all int values - should pass
	value := Struct("Config",
		MapEntry{Key: "name", Value: Str("myapp")},
		MapEntry{Key: "settings", Value: Map(
			MapEntry{Key: "timeout", Value: Int(30)},
			MapEntry{Key: "retries", Value: Int(3)},
			MapEntry{Key: "port", Value: Int(8080)},
		)},
	)

	result := ValidateWithSchema(value, schema)
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}
}

func TestMapStrInt_RejectsInvalidValues(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Config", "v1",
			Field("name", PrimitiveType("str")),
			Field("settings", MapType(PrimitiveType("str"), PrimitiveType("int"))),
		).
		Build()

	// Map with string value where int expected - should fail
	value := Struct("Config",
		MapEntry{Key: "name", Value: Str("myapp")},
		MapEntry{Key: "settings", Value: Map(
			MapEntry{Key: "timeout", Value: Int(30)},
			MapEntry{Key: "name", Value: Str("invalid")}, // wrong type!
			MapEntry{Key: "port", Value: Int(8080)},
		)},
	)

	result := ValidateWithSchema(value, schema)
	if result.Valid {
		t.Error("Expected validation to fail for string in map<str,int>")
	}

	// Should have type_mismatch error for the invalid value
	found := false
	for _, err := range result.Errors {
		if err.Code == "type_mismatch" && err.Path == "settings.name" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected type_mismatch error at settings.name, got: %v", result.Errors)
	}
}

func TestMapStrStr_AcceptsValidValues(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Metadata", "v1",
			Field("labels", MapType(PrimitiveType("str"), PrimitiveType("str"))),
		).
		Build()

	value := Struct("Metadata",
		MapEntry{Key: "labels", Value: Map(
			MapEntry{Key: "app", Value: Str("frontend")},
			MapEntry{Key: "env", Value: Str("production")},
		)},
	)

	result := ValidateWithSchema(value, schema)
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}
}

func TestMapStrRef_ValidatesNestedTypes(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Address", "v1",
			Field("host", PrimitiveType("str")),
			Field("port", PrimitiveType("int")),
		).
		AddStruct("Registry", "v1",
			Field("services", MapType(PrimitiveType("str"), RefType("Address"))),
		).
		Build()

	// Valid: map values are valid Address structs
	validValue := Struct("Registry",
		MapEntry{Key: "services", Value: Map(
			MapEntry{Key: "api", Value: Struct("Address",
				MapEntry{Key: "host", Value: Str("localhost")},
				MapEntry{Key: "port", Value: Int(8080)},
			)},
			MapEntry{Key: "db", Value: Struct("Address",
				MapEntry{Key: "host", Value: Str("db.local")},
				MapEntry{Key: "port", Value: Int(5432)},
			)},
		)},
	)

	result := ValidateWithSchema(validValue, schema)
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}

	// Invalid: map value is missing required field
	invalidValue := Struct("Registry",
		MapEntry{Key: "services", Value: Map(
			MapEntry{Key: "api", Value: Struct("Address",
				MapEntry{Key: "host", Value: Str("localhost")},
				// port is missing!
			)},
		)},
	)

	result = ValidateWithSchema(invalidValue, schema)
	if result.Valid {
		t.Error("Expected validation to fail for missing required field in nested struct")
	}

	// Should have required_field error
	found := false
	for _, err := range result.Errors {
		if err.Code == "required_field" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected required_field error, got: %v", result.Errors)
	}
}
