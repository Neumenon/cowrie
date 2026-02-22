package gen2

import (
	"bytes"
	"testing"
)

func TestDeterministicEncoding(t *testing.T) {
	// Create an object with keys in different orders
	obj1 := Object(
		Member{Key: "zebra", Value: Int64(1)},
		Member{Key: "apple", Value: Int64(2)},
		Member{Key: "mango", Value: Int64(3)},
	)

	obj2 := Object(
		Member{Key: "mango", Value: Int64(3)},
		Member{Key: "zebra", Value: Int64(1)},
		Member{Key: "apple", Value: Int64(2)},
	)

	// Non-deterministic encoding should differ
	data1, err := Encode(obj1)
	if err != nil {
		t.Fatalf("Encode obj1 failed: %v", err)
	}

	data2, err := Encode(obj2)
	if err != nil {
		t.Fatalf("Encode obj2 failed: %v", err)
	}

	if bytes.Equal(data1, data2) {
		t.Error("Non-deterministic encoding unexpectedly produced identical output")
	}

	// Deterministic encoding should produce identical output
	opts := EncodeOptions{Deterministic: true}

	det1, err := EncodeWithOptions(obj1, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions obj1 failed: %v", err)
	}

	det2, err := EncodeWithOptions(obj2, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions obj2 failed: %v", err)
	}

	if !bytes.Equal(det1, det2) {
		t.Error("Deterministic encoding produced different output for equivalent objects")
	}
}

func TestDeterministicNestedObjects(t *testing.T) {
	// Nested objects should also be sorted
	nested1 := Object(
		Member{Key: "outer", Value: Object(
			Member{Key: "z", Value: String("last")},
			Member{Key: "a", Value: String("first")},
		)},
		Member{Key: "list", Value: Array(
			Object(
				Member{Key: "y", Value: Int64(2)},
				Member{Key: "x", Value: Int64(1)},
			),
		)},
	)

	nested2 := Object(
		Member{Key: "list", Value: Array(
			Object(
				Member{Key: "x", Value: Int64(1)},
				Member{Key: "y", Value: Int64(2)},
			),
		)},
		Member{Key: "outer", Value: Object(
			Member{Key: "a", Value: String("first")},
			Member{Key: "z", Value: String("last")},
		)},
	)

	opts := EncodeOptions{Deterministic: true}

	det1, err := EncodeWithOptions(nested1, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions nested1 failed: %v", err)
	}

	det2, err := EncodeWithOptions(nested2, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions nested2 failed: %v", err)
	}

	if !bytes.Equal(det1, det2) {
		t.Error("Deterministic encoding produced different output for equivalent nested objects")
	}
}

func TestDeterministicStability(t *testing.T) {
	// Same object encoded multiple times should always produce same output
	obj := Object(
		Member{Key: "id", Value: Int64(12345)},
		Member{Key: "name", Value: String("test")},
		Member{Key: "values", Value: Array(Float64(1.1), Float64(2.2), Float64(3.3))},
	)

	opts := EncodeOptions{Deterministic: true}

	var results [][]byte
	for i := 0; i < 10; i++ {
		data, err := EncodeWithOptions(obj, opts)
		if err != nil {
			t.Fatalf("Encode iteration %d failed: %v", i, err)
		}
		results = append(results, data)
	}

	for i := 1; i < len(results); i++ {
		if !bytes.Equal(results[0], results[i]) {
			t.Errorf("Deterministic encoding not stable: iteration %d differs from iteration 0", i)
		}
	}
}

func TestDeterministicRoundTrip(t *testing.T) {
	obj := Object(
		Member{Key: "c", Value: Int64(3)},
		Member{Key: "a", Value: Int64(1)},
		Member{Key: "b", Value: Int64(2)},
	)

	opts := EncodeOptions{Deterministic: true}

	// Encode deterministically
	data, err := EncodeWithOptions(obj, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Re-encode deterministically
	reencoded, err := EncodeWithOptions(decoded, opts)
	if err != nil {
		t.Fatalf("Re-encode failed: %v", err)
	}

	// Should be identical
	if !bytes.Equal(data, reencoded) {
		t.Error("Deterministic re-encoding produced different output")
	}
}

func TestCanonicalizeNil(t *testing.T) {
	result := canonicalize(nil)
	if result != nil {
		t.Error("canonicalize(nil) should return nil")
	}
}

func TestCanonicalizeScalars(t *testing.T) {
	// Scalars should pass through unchanged
	scalars := []*Value{
		Null(),
		Bool(true),
		Int64(42),
		Float64(3.14),
		String("test"),
	}

	for _, v := range scalars {
		result := canonicalize(v)
		if result != v {
			t.Errorf("canonicalize should return same pointer for scalar %v", v.Type())
		}
	}
}

func BenchmarkDeterministicEncode(b *testing.B) {
	obj := Object(
		Member{Key: "zebra", Value: Int64(26)},
		Member{Key: "yankee", Value: Int64(25)},
		Member{Key: "xray", Value: Int64(24)},
		Member{Key: "whiskey", Value: Int64(23)},
		Member{Key: "victor", Value: Int64(22)},
		Member{Key: "uniform", Value: Int64(21)},
		Member{Key: "tango", Value: Int64(20)},
		Member{Key: "sierra", Value: Int64(19)},
		Member{Key: "romeo", Value: Int64(18)},
		Member{Key: "quebec", Value: Int64(17)},
	)

	opts := EncodeOptions{Deterministic: true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeWithOptions(obj, opts)
	}
}

func BenchmarkNonDeterministicEncode(b *testing.B) {
	obj := Object(
		Member{Key: "zebra", Value: Int64(26)},
		Member{Key: "yankee", Value: Int64(25)},
		Member{Key: "xray", Value: Int64(24)},
		Member{Key: "whiskey", Value: Int64(23)},
		Member{Key: "victor", Value: Int64(22)},
		Member{Key: "uniform", Value: Int64(21)},
		Member{Key: "tango", Value: Int64(20)},
		Member{Key: "sierra", Value: Int64(19)},
		Member{Key: "romeo", Value: Int64(18)},
		Member{Key: "quebec", Value: Int64(17)},
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Encode(obj)
	}
}

// --- OmitNull Tests ---

func TestOmitNullBasic(t *testing.T) {
	obj := Object(
		Member{Key: "name", Value: String("test")},
		Member{Key: "value", Value: Null()},
		Member{Key: "count", Value: Int64(42)},
	)

	opts := EncodeOptions{OmitNull: true}
	data, err := EncodeWithOptions(obj, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	// Decode and verify null is omitted
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	members := decoded.Members()
	if len(members) != 2 {
		t.Errorf("Expected 2 members after omitNull, got %d", len(members))
	}

	// Check that "value" (null) is not present
	for _, m := range members {
		if m.Key == "value" {
			t.Error("Null field 'value' should have been omitted")
		}
	}
}

func TestOmitNullNested(t *testing.T) {
	obj := Object(
		Member{Key: "outer", Value: Object(
			Member{Key: "keep", Value: String("yes")},
			Member{Key: "remove", Value: Null()},
		)},
		Member{Key: "nullOuter", Value: Null()},
	)

	opts := EncodeOptions{OmitNull: true}
	data, err := EncodeWithOptions(obj, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Should have only "outer" at top level
	members := decoded.Members()
	if len(members) != 1 {
		t.Errorf("Expected 1 top-level member, got %d", len(members))
	}

	// Check nested object
	outer := decoded.Get("outer")
	if outer == nil {
		t.Fatal("Missing 'outer' field")
	}

	outerMembers := outer.Members()
	if len(outerMembers) != 1 {
		t.Errorf("Expected 1 nested member, got %d", len(outerMembers))
	}

	if outer.Get("remove") != nil {
		t.Error("Nested null field 'remove' should have been omitted")
	}
}

func TestOmitNullArrayPreserved(t *testing.T) {
	// Nulls in arrays should NOT be removed (only from objects)
	obj := Object(
		Member{Key: "arr", Value: Array(Int64(1), Null(), Int64(3))},
	)

	opts := EncodeOptions{OmitNull: true}
	data, err := EncodeWithOptions(obj, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr := decoded.Get("arr")
	if arr == nil {
		t.Fatal("Missing 'arr' field")
	}

	elements := arr.Array()
	if len(elements) != 3 {
		t.Errorf("Array should preserve nulls, got %d elements", len(elements))
	}

	if elements[1].Type() != TypeNull {
		t.Error("Array null element should be preserved")
	}
}

func TestOmitNullWithDeterministic(t *testing.T) {
	// Both options should work together
	obj1 := Object(
		Member{Key: "z", Value: Null()},
		Member{Key: "a", Value: Int64(1)},
		Member{Key: "m", Value: Null()},
		Member{Key: "b", Value: Int64(2)},
	)

	obj2 := Object(
		Member{Key: "m", Value: Null()},
		Member{Key: "b", Value: Int64(2)},
		Member{Key: "a", Value: Int64(1)},
		Member{Key: "z", Value: Null()},
	)

	opts := EncodeOptions{OmitNull: true, Deterministic: true}

	data1, err := EncodeWithOptions(obj1, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions obj1 failed: %v", err)
	}

	data2, err := EncodeWithOptions(obj2, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions obj2 failed: %v", err)
	}

	// Should be identical (nulls removed, then sorted)
	if !bytes.Equal(data1, data2) {
		t.Error("OmitNull+Deterministic should produce identical output")
	}

	// Verify nulls removed
	decoded, _ := Decode(data1)
	if len(decoded.Members()) != 2 {
		t.Errorf("Expected 2 members, got %d", len(decoded.Members()))
	}
}

func TestOmitNullsNil(t *testing.T) {
	result := omitNulls(nil)
	if result != nil {
		t.Error("omitNulls(nil) should return nil")
	}
}

func TestOmitNullsScalars(t *testing.T) {
	// Non-null scalars should pass through unchanged
	scalars := []*Value{
		Bool(true),
		Int64(42),
		Float64(3.14),
		String("test"),
	}

	for _, v := range scalars {
		result := omitNulls(v)
		if result != v {
			t.Errorf("omitNulls should return same pointer for scalar %v", v.Type())
		}
	}
}

func TestOmitNullAllNulls(t *testing.T) {
	// Object with all null values should become empty object
	obj := Object(
		Member{Key: "a", Value: Null()},
		Member{Key: "b", Value: Null()},
		Member{Key: "c", Value: Null()},
	)

	opts := EncodeOptions{OmitNull: true}
	data, err := EncodeWithOptions(obj, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.Members()) != 0 {
		t.Errorf("All-null object should become empty, got %d members", len(decoded.Members()))
	}
}

func BenchmarkOmitNullEncode(b *testing.B) {
	obj := Object(
		Member{Key: "name", Value: String("test")},
		Member{Key: "null1", Value: Null()},
		Member{Key: "count", Value: Int64(42)},
		Member{Key: "null2", Value: Null()},
		Member{Key: "score", Value: Float64(3.14)},
		Member{Key: "null3", Value: Null()},
	)

	opts := EncodeOptions{OmitNull: true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeWithOptions(obj, opts)
	}
}
