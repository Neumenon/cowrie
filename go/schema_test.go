package cowrie

import (
	"testing"
)

func TestSchemaFingerprint64Basic(t *testing.T) {
	// Different types should have different fingerprints
	null := Null()
	boolVal := Bool(true)
	intVal := Int64(42)
	floatVal := Float64(3.14)
	strVal := String("test")

	fingerprints := map[string]uint64{
		"null":   SchemaFingerprint64(null),
		"bool":   SchemaFingerprint64(boolVal),
		"int":    SchemaFingerprint64(intVal),
		"float":  SchemaFingerprint64(floatVal),
		"string": SchemaFingerprint64(strVal),
	}

	// All should be different
	seen := make(map[uint64]string)
	for name, fp := range fingerprints {
		if existing, ok := seen[fp]; ok {
			t.Errorf("Fingerprint collision between %s and %s: %x", name, existing, fp)
		}
		seen[fp] = name
	}
}

func TestSchemaFingerprint64SameValue(t *testing.T) {
	// Same structure, different values should have same fingerprint
	obj1 := Object(
		Member{Key: "name", Value: String("Alice")},
		Member{Key: "age", Value: Int64(30)},
	)

	obj2 := Object(
		Member{Key: "name", Value: String("Bob")},
		Member{Key: "age", Value: Int64(25)},
	)

	fp1 := SchemaFingerprint64(obj1)
	fp2 := SchemaFingerprint64(obj2)

	if fp1 != fp2 {
		t.Errorf("Same schema should have same fingerprint: %x vs %x", fp1, fp2)
	}
}

func TestSchemaFingerprint64DifferentFields(t *testing.T) {
	obj1 := Object(
		Member{Key: "name", Value: String("test")},
	)

	obj2 := Object(
		Member{Key: "id", Value: String("test")},
	)

	fp1 := SchemaFingerprint64(obj1)
	fp2 := SchemaFingerprint64(obj2)

	if fp1 == fp2 {
		t.Error("Different field names should have different fingerprints")
	}
}

func TestSchemaFingerprint64KeyOrder(t *testing.T) {
	// Key order should not affect fingerprint (uses canonical ordering)
	obj1 := Object(
		Member{Key: "a", Value: Int64(1)},
		Member{Key: "b", Value: Int64(2)},
	)

	obj2 := Object(
		Member{Key: "b", Value: Int64(2)},
		Member{Key: "a", Value: Int64(1)},
	)

	fp1 := SchemaFingerprint64(obj1)
	fp2 := SchemaFingerprint64(obj2)

	if fp1 != fp2 {
		t.Errorf("Key order should not affect fingerprint: %x vs %x", fp1, fp2)
	}
}

func TestSchemaFingerprint64NestedObjects(t *testing.T) {
	nested1 := Object(
		Member{Key: "outer", Value: Object(
			Member{Key: "inner", Value: Int64(1)},
		)},
	)

	nested2 := Object(
		Member{Key: "outer", Value: Object(
			Member{Key: "inner", Value: Int64(999)},
		)},
	)

	// Same structure, different values
	fp1 := SchemaFingerprint64(nested1)
	fp2 := SchemaFingerprint64(nested2)

	if fp1 != fp2 {
		t.Errorf("Same nested schema should have same fingerprint: %x vs %x", fp1, fp2)
	}

	// Different inner field name
	nested3 := Object(
		Member{Key: "outer", Value: Object(
			Member{Key: "different", Value: Int64(1)},
		)},
	)

	fp3 := SchemaFingerprint64(nested3)
	if fp1 == fp3 {
		t.Error("Different nested field names should have different fingerprints")
	}
}

func TestSchemaFingerprint64Arrays(t *testing.T) {
	arr1 := Array(Int64(1), Int64(2), Int64(3))
	arr2 := Array(Int64(10), Int64(20), Int64(30))

	fp1 := SchemaFingerprint64(arr1)
	fp2 := SchemaFingerprint64(arr2)

	if fp1 != fp2 {
		t.Errorf("Same array schema should have same fingerprint: %x vs %x", fp1, fp2)
	}

	// Different element types
	arr3 := Array(String("a"), String("b"), String("c"))
	fp3 := SchemaFingerprint64(arr3)

	if fp1 == fp3 {
		t.Error("Different array element types should have different fingerprints")
	}
}

func TestSchemaFingerprint64Tensors(t *testing.T) {
	tensor1 := Tensor(DTypeFloat32, []uint64{4, 4}, make([]byte, 64))
	tensor2 := Tensor(DTypeFloat32, []uint64{8, 2}, make([]byte, 64))

	fp1 := SchemaFingerprint64(tensor1)
	fp2 := SchemaFingerprint64(tensor2)

	// Same dtype and rank should have same fingerprint (dims are data, not schema)
	if fp1 != fp2 {
		t.Errorf("Same tensor schema should have same fingerprint: %x vs %x", fp1, fp2)
	}

	// Different dtype
	tensor3 := Tensor(DTypeFloat64, []uint64{4, 4}, make([]byte, 128))
	fp3 := SchemaFingerprint64(tensor3)

	if fp1 == fp3 {
		t.Error("Different tensor dtypes should have different fingerprints")
	}

	// Different rank
	tensor4 := Tensor(DTypeFloat32, []uint64{16}, make([]byte, 64))
	fp4 := SchemaFingerprint64(tensor4)

	if fp1 == fp4 {
		t.Error("Different tensor ranks should have different fingerprints")
	}
}

func TestSchemaFingerprint32(t *testing.T) {
	obj := Object(Member{Key: "test", Value: Int64(1)})

	fp64 := SchemaFingerprint64(obj)
	fp32 := SchemaFingerprint32(obj)

	if fp32 != uint32(fp64) {
		t.Errorf("SchemaFingerprint32 should be low 32 bits: %x vs %x", fp32, uint32(fp64))
	}
}

func TestSchemaEquals(t *testing.T) {
	obj1 := Object(
		Member{Key: "name", Value: String("Alice")},
		Member{Key: "age", Value: Int64(30)},
	)

	obj2 := Object(
		Member{Key: "age", Value: Int64(25)},
		Member{Key: "name", Value: String("Bob")},
	)

	if !SchemaEquals(obj1, obj2) {
		t.Error("SchemaEquals should return true for same schema")
	}

	obj3 := Object(
		Member{Key: "id", Value: Int64(1)},
	)

	if SchemaEquals(obj1, obj3) {
		t.Error("SchemaEquals should return false for different schema")
	}
}

func TestSchemaDescriptor(t *testing.T) {
	tests := []struct {
		value    *Value
		expected string
	}{
		{Null(), "null"},
		{Bool(true), "bool"},
		{Int64(42), "int64"},
		{Float64(3.14), "float64"},
		{String("test"), "string"},
		{Bytes([]byte{1, 2, 3}), "bytes"},
		{Object(), "{}"},
		{Array(), "[]"},
		{Tensor(DTypeFloat32, []uint64{4}, make([]byte, 16)), "tensor<float32>"},
	}

	for _, tt := range tests {
		result := SchemaDescriptor(tt.value)
		if result != tt.expected {
			t.Errorf("SchemaDescriptor(%v) = %q, want %q", tt.value.Type(), result, tt.expected)
		}
	}
}

func TestSchemaDescriptorObject(t *testing.T) {
	obj := Object(
		Member{Key: "zebra", Value: Int64(1)},
		Member{Key: "apple", Value: Int64(2)},
	)

	desc := SchemaDescriptor(obj)
	// Should show first key alphabetically
	if desc != "{apple,...}" {
		t.Errorf("Object descriptor = %q, want {apple,...}", desc)
	}
}

func TestSchemaDescriptorArray(t *testing.T) {
	arr := Array(Int64(1), Int64(2), Int64(3))
	desc := SchemaDescriptor(arr)

	if desc != "[int64,...]" {
		t.Errorf("Array descriptor = %q, want [int64,...]", desc)
	}
}

func TestDTypeString(t *testing.T) {
	tests := []struct {
		dtype    DType
		expected string
	}{
		{DTypeFloat32, "float32"},
		{DTypeFloat64, "float64"},
		{DTypeInt32, "int32"},
		{DTypeInt64, "int64"},
		{DTypeUint8, "uint8"},
		{DTypeQINT4, "qint4"},
		{DTypeBinary, "binary"},
	}

	for _, tt := range tests {
		if tt.dtype.String() != tt.expected {
			t.Errorf("DType(%d).String() = %q, want %q", tt.dtype, tt.dtype.String(), tt.expected)
		}
	}
}

func TestSchemaFingerprintNil(t *testing.T) {
	fp := SchemaFingerprint64(nil)
	nullFP := SchemaFingerprint64(Null())

	if fp != nullFP {
		t.Errorf("nil and Null() should have same fingerprint: %x vs %x", fp, nullFP)
	}
}

func TestSchemaFingerprintStability(t *testing.T) {
	obj := Object(
		Member{Key: "id", Value: Int64(1)},
		Member{Key: "name", Value: String("test")},
	)

	// Fingerprint should be deterministic
	fp1 := SchemaFingerprint64(obj)
	fp2 := SchemaFingerprint64(obj)
	fp3 := SchemaFingerprint64(obj)

	if fp1 != fp2 || fp2 != fp3 {
		t.Error("SchemaFingerprint64 should be stable")
	}
}

func BenchmarkSchemaFingerprint64(b *testing.B) {
	obj := Object(
		Member{Key: "id", Value: Int64(1)},
		Member{Key: "name", Value: String("test")},
		Member{Key: "values", Value: Array(Float64(1.1), Float64(2.2))},
		Member{Key: "nested", Value: Object(
			Member{Key: "inner", Value: Bool(true)},
		)},
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SchemaFingerprint64(obj)
	}
}
