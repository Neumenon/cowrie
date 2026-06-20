package cowrie

import "testing"

// TestSchemaEquals_StructuralNotProbabilistic verifies that SchemaEquals performs
// a real structural comparison rather than a probabilistic fingerprint check.
func TestSchemaEquals_StructuralNotProbabilistic(t *testing.T) {
	// Two objects with identical field names+types but different values must match.
	a := Object(
		Member{Key: "name", Value: String("Alice")},
		Member{Key: "score", Value: Float64(9.5)},
	)
	b := Object(
		Member{Key: "name", Value: String("Bob")},
		Member{Key: "score", Value: Float64(1.0)},
	)
	if !SchemaEquals(a, b) {
		t.Error("SchemaEquals: expected true for same field names+types with different values")
	}

	// Renamed field must not match.
	c := Object(
		Member{Key: "label", Value: String("x")}, // "label" != "name"
		Member{Key: "score", Value: Float64(0)},
	)
	if SchemaEquals(a, c) {
		t.Error("SchemaEquals: expected false when a field is renamed")
	}

	// Retyped field must not match.
	d := Object(
		Member{Key: "name", Value: String("x")},
		Member{Key: "score", Value: Int64(5)}, // int64 != float64
	)
	if SchemaEquals(a, d) {
		t.Error("SchemaEquals: expected false when a field type changes")
	}
}

// TestSchemaFingerprintEqual_Preserved verifies that SchemaFingerprintEqual still
// exists and agrees with SchemaEquals on the matching case.
func TestSchemaFingerprintEqual_Preserved(t *testing.T) {
	a := Object(
		Member{Key: "x", Value: Int64(1)},
	)
	b := Object(
		Member{Key: "x", Value: Int64(99)},
	)
	if !SchemaFingerprintEqual(a, b) {
		t.Error("SchemaFingerprintEqual: expected true for same schema")
	}
	if !SchemaEquals(a, b) {
		t.Error("SchemaEquals: expected true for same schema")
	}
	// Both functions must agree on the matching case.
	if SchemaEquals(a, b) != SchemaFingerprintEqual(a, b) {
		t.Error("SchemaEquals and SchemaFingerprintEqual disagree on identical schemas")
	}
}
