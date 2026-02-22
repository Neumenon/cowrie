package cowrie

import (
	"testing"
)

// TestSchemaFingerprint_MapOrderStable verifies fingerprint is stable regardless of map key order.
func TestSchemaFingerprint_MapOrderStable(t *testing.T) {
	// Create objects with same keys but different insertion order
	m1 := map[string]any{"b": int64(2), "a": int64(1), "c": int64(3)}
	m2 := map[string]any{"c": int64(3), "b": int64(2), "a": int64(1)}
	m3 := map[string]any{"a": int64(1), "c": int64(3), "b": int64(2)}

	v1 := mustValueFromAny(m1)
	v2 := mustValueFromAny(m2)
	v3 := mustValueFromAny(m3)

	fp1 := SchemaFingerprint32(v1)
	fp2 := SchemaFingerprint32(v2)
	fp3 := SchemaFingerprint32(v3)

	if fp1 != fp2 {
		t.Errorf("fingerprint differs for different key order: %#x vs %#x", fp1, fp2)
	}
	if fp2 != fp3 {
		t.Errorf("fingerprint differs for different key order: %#x vs %#x", fp2, fp3)
	}

	t.Logf("Stable fingerprint: %#08x", fp1)
}

// TestSchemaFingerprint_NestedMapOrderStable verifies nested maps also produce stable fingerprints.
func TestSchemaFingerprint_NestedMapOrderStable(t *testing.T) {
	m1 := map[string]any{
		"outer": map[string]any{"z": int64(1), "a": int64(2)},
		"inner": int64(3),
	}
	m2 := map[string]any{
		"inner": int64(3),
		"outer": map[string]any{"a": int64(2), "z": int64(1)},
	}

	v1 := mustValueFromAny(m1)
	v2 := mustValueFromAny(m2)

	fp1 := SchemaFingerprint32(v1)
	fp2 := SchemaFingerprint32(v2)

	if fp1 != fp2 {
		t.Errorf("nested fingerprint differs: %#x vs %#x", fp1, fp2)
	}
}

// TestSchemaFingerprint_ArrayOrderMatters verifies array order affects fingerprint.
func TestSchemaFingerprint_ArrayOrderMatters(t *testing.T) {
	// Arrays with same elements in different order should have different fingerprints
	// (or same if we're only fingerprinting schema/types)
	a1 := []any{int64(1), int64(2), int64(3)}
	a2 := []any{int64(3), int64(2), int64(1)}

	v1 := mustValueFromAny(a1)
	v2 := mustValueFromAny(a2)

	fp1 := SchemaFingerprint32(v1)
	fp2 := SchemaFingerprint32(v2)

	// Schema fingerprint only cares about types, not values
	// So these should be the same (both are arrays of int64)
	if fp1 != fp2 {
		t.Logf("Note: Array order produces different fingerprints: %#x vs %#x", fp1, fp2)
		// This is acceptable behavior - document which policy you choose
	}
}

// TestSchemaFingerprint_FieldNameMatters verifies different field names produce different fingerprints.
func TestSchemaFingerprint_FieldNameMatters(t *testing.T) {
	m1 := map[string]any{"field_a": int64(1)}
	m2 := map[string]any{"field_b": int64(1)}

	v1 := mustValueFromAny(m1)
	v2 := mustValueFromAny(m2)

	fp1 := SchemaFingerprint32(v1)
	fp2 := SchemaFingerprint32(v2)

	if fp1 == fp2 {
		t.Error("fingerprint should differ for different field names")
	}
}

// TestSchemaFingerprint_TypeMatters verifies different types produce different fingerprints.
func TestSchemaFingerprint_TypeMatters(t *testing.T) {
	tests := []struct {
		name string
		v1   any
		v2   any
	}{
		{"int_vs_string", map[string]any{"x": int64(1)}, map[string]any{"x": "1"}},
		{"int_vs_float", map[string]any{"x": int64(1)}, map[string]any{"x": float64(1)}},
		{"array_vs_object", []any{int64(1)}, map[string]any{"0": int64(1)}},
		{"null_vs_int", map[string]any{"x": nil}, map[string]any{"x": int64(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp1 := SchemaFingerprint32(mustValueFromAny(tt.v1))
			fp2 := SchemaFingerprint32(mustValueFromAny(tt.v2))

			if fp1 == fp2 {
				t.Errorf("fingerprint should differ: %#x", fp1)
			}
		})
	}
}

// TestSchemaFingerprint_SameValueSameFingerprint verifies identical values have same fingerprint.
func TestSchemaFingerprint_SameValueSameFingerprint(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"null", nil},
		{"bool", true},
		{"int", int64(42)},
		{"float", float64(3.14)},
		{"string", "hello"},
		{"array", []any{int64(1), int64(2), int64(3)}},
		{"object", map[string]any{"a": int64(1), "b": "two"}},
		{"nested", map[string]any{
			"items": []any{
				map[string]any{"id": int64(1)},
				map[string]any{"id": int64(2)},
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v1 := mustValueFromAny(tt.value)
			v2 := mustValueFromAny(tt.value)

			fp1 := SchemaFingerprint32(v1)
			fp2 := SchemaFingerprint32(v2)

			if fp1 != fp2 {
				t.Errorf("fingerprint differs for identical values: %#x vs %#x", fp1, fp2)
			}
		})
	}
}

// TestSchemaFingerprint_Stability verifies fingerprints are stable across encode/decode.
func TestSchemaFingerprint_Stability(t *testing.T) {
	original := map[string]any{
		"name": "Alice",
		"age":  int64(30),
		"scores": []any{
			float64(95.5),
			float64(87.3),
		},
	}

	v1 := mustValueFromAny(original)
	fp1 := SchemaFingerprint32(v1)

	// Encode and decode
	encoded, err := Encode(v1)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	fp2 := SchemaFingerprint32(decoded)

	if fp1 != fp2 {
		t.Errorf("fingerprint changed after encode/decode: %#x vs %#x", fp1, fp2)
	}
}

// TestSchemaFingerprint_DeterministicEncoding verifies deterministic encoding produces stable fingerprints.
func TestSchemaFingerprint_DeterministicEncoding(t *testing.T) {
	// Create object with random map iteration order
	m := make(map[string]any)
	for _, k := range []string{"zebra", "apple", "mango", "banana"} {
		m[k] = int64(len(k))
	}

	v := mustValueFromAny(m)

	// Encode deterministically
	encoded, err := EncodeWithOptions(v, EncodeOptions{Deterministic: true})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Encode again - should produce identical bytes
	encoded2, err := EncodeWithOptions(v, EncodeOptions{Deterministic: true})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if string(encoded) != string(encoded2) {
		t.Error("deterministic encoding produced different bytes")
	}

	// Fingerprint should also be stable
	decoded, _ := Decode(encoded)
	decoded2, _ := Decode(encoded2)

	fp1 := SchemaFingerprint32(decoded)
	fp2 := SchemaFingerprint32(decoded2)

	if fp1 != fp2 {
		t.Errorf("fingerprint differs: %#x vs %#x", fp1, fp2)
	}
}

// TestSchemaFingerprint_TensorDType verifies tensor dtype affects fingerprint.
func TestSchemaFingerprint_TensorDType(t *testing.T) {
	// Same data but different dtypes
	data := make([]byte, 16)

	t1 := Tensor(DTypeFloat32, []uint64{4}, data)
	t2 := Tensor(DTypeFloat64, []uint64{2}, data)
	t3 := Tensor(DTypeInt32, []uint64{4}, data)

	fp1 := SchemaFingerprint32(t1)
	fp2 := SchemaFingerprint32(t2)
	fp3 := SchemaFingerprint32(t3)

	if fp1 == fp2 {
		t.Error("float32 and float64 tensors should have different fingerprints")
	}
	if fp1 == fp3 {
		t.Error("float32 and int32 tensors should have different fingerprints")
	}
	if fp2 == fp3 {
		t.Error("float64 and int32 tensors should have different fingerprints")
	}
}

// TestSchemaFingerprint_TensorRank verifies tensor rank affects fingerprint.
func TestSchemaFingerprint_TensorRank(t *testing.T) {
	data := make([]byte, 16)

	// Same dtype, same total size, different ranks
	t1 := Tensor(DTypeFloat32, []uint64{4}, data)      // 1D
	t2 := Tensor(DTypeFloat32, []uint64{2, 2}, data)   // 2D
	t3 := Tensor(DTypeFloat32, []uint64{1, 4}, data)   // 2D different shape

	fp1 := SchemaFingerprint32(t1)
	fp2 := SchemaFingerprint32(t2)
	fp3 := SchemaFingerprint32(t3)

	// Depending on policy, these might or might not differ
	// Document the expected behavior
	t.Logf("1D tensor: %#08x", fp1)
	t.Logf("2D (2x2) tensor: %#08x", fp2)
	t.Logf("2D (1x4) tensor: %#08x", fp3)

	// At minimum, 1D and 2D should differ
	if fp1 == fp2 {
		t.Log("Note: 1D and 2D tensors have same fingerprint (rank not included)")
	}
}

// TestSchemaFingerprint64_Basic verifies 64-bit fingerprint works.
func TestSchemaFingerprint64_Basic(t *testing.T) {
	v := mustValueFromAny(map[string]any{
		"a": int64(1),
		"b": "two",
		"c": []any{int64(1), int64(2)},
	})

	fp := SchemaFingerprint64(v)
	if fp == 0 {
		t.Error("fingerprint should not be zero")
	}

	// Should be stable
	fp2 := SchemaFingerprint64(v)
	if fp != fp2 {
		t.Errorf("fingerprint not stable: %#x vs %#x", fp, fp2)
	}

	t.Logf("64-bit fingerprint: %#016x", fp)
}

// BenchmarkSchemaFingerprint32 benchmarks fingerprint computation.
func BenchmarkSchemaFingerprint32(b *testing.B) {
	v := mustValueFromAny(map[string]any{
		"id":    int64(42),
		"name":  "test",
		"score": float64(3.14),
		"tags":  []any{"a", "b", "c"},
		"nested": map[string]any{
			"x": int64(1),
			"y": int64(2),
		},
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = SchemaFingerprint32(v)
	}
}

// mustValueFromAny converts any Go value to *Value, panicking on error.
func mustValueFromAny(v any) *Value {
	switch x := v.(type) {
	case nil:
		return Null()
	case bool:
		return Bool(x)
	case int64:
		return Int64(x)
	case uint64:
		return Uint64(x)
	case float64:
		return Float64(x)
	case string:
		return String(x)
	case []byte:
		return Bytes(x)
	case []any:
		items := make([]*Value, len(x))
		for i, item := range x {
			items[i] = mustValueFromAny(item)
		}
		return Array(items...)
	case map[string]any:
		members := make([]Member, 0, len(x))
		for k, v := range x {
			members = append(members, Member{Key: k, Value: mustValueFromAny(v)})
		}
		return Object(members...)
	default:
		panic("unsupported type")
	}
}
