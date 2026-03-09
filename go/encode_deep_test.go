package cowrie

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestEncodeToWriter_AllValueTypes(t *testing.T) {
	values := []*Value{
		Null(),
		Bool(true),
		Bool(false),
		Int64(0),
		Int64(42),
		Int64(127),
		Int64(-1),
		Int64(-16),
		Int64(1000),
		Int64(-1000),
		Uint64(42),
		Float64(3.14),
		String("hello"),
		Bytes([]byte{1, 2, 3}),
		Datetime64(12345),
		UUID128([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
		BigInt([]byte{0x01, 0x00}),
		NewDecimal128(2, [16]byte{42}),
	}

	for i, v := range values {
		var buf bytes.Buffer
		if err := EncodeToWriter(&buf, v); err != nil {
			t.Errorf("case %d: EncodeToWriter: %v", i, err)
			continue
		}
		decoded, err := Decode(buf.Bytes())
		if err != nil {
			t.Errorf("case %d: Decode: %v", i, err)
		}
		_ = decoded
	}
}

func TestEncodeToWriter_Containers(t *testing.T) {
	v := Object(
		Member{Key: "str", Value: String("hello")},
		Member{Key: "int", Value: Int64(42)},
		Member{Key: "uint", Value: Uint64(99)},
		Member{Key: "float", Value: Float64(3.14)},
		Member{Key: "bool", Value: Bool(true)},
		Member{Key: "bytes", Value: Bytes([]byte{1, 2})},
		Member{Key: "null", Value: Null()},
		Member{Key: "dt", Value: Datetime64(12345)},
		Member{Key: "uuid", Value: UUID128([16]byte{1})},
		Member{Key: "bigint", Value: BigInt([]byte{0x01})},
		Member{Key: "dec", Value: NewDecimal128(2, [16]byte{42})},
		Member{Key: "bitmask", Value: Bitmask(8, []byte{0xFF})},
	)

	var buf bytes.Buffer
	if err := EncodeToWriter(&buf, v); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if decoded == nil {
		t.Error("nil result")
	}
}

func TestEncodeToWriter_TensorScatter(t *testing.T) {
	td := Tensor(DTypeFloat32, []uint64{3}, encodeFloat32LE([]float32{1.0, 2.0, 3.0}))

	var buf bytes.Buffer
	if err := EncodeToWriter(&buf, td); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type() != TypeTensor {
		t.Error("expected tensor")
	}
}

func TestEncodeToWriter_NestedArray(t *testing.T) {
	v := Array(
		Int64(1),
		String("two"),
		Bool(true),
		Array(Int64(10), Int64(20)),
		Object(Member{Key: "x", Value: Int64(1)}),
	)

	var buf bytes.Buffer
	if err := EncodeToWriter(&buf, v); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type() != TypeArray {
		t.Error("expected array")
	}
}

func TestEncodeAny_AllBranches(t *testing.T) {
	nv := Node("n1", []string{"Person"}, map[string]any{
		"name":    "Alice",
		"age":     42,
		"age64":   int64(42),
		"count":   uint(100),
		"count64": uint64(200),
		"score":   3.14,
		"active":  true,
		"nothing": nil,
		"data":    []byte{1, 2, 3},
		"tags":    []any{"a", "b"},
		"meta":    map[string]any{"x": 1},
	})
	data, err := Encode(nv)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	_ = decoded
}

func TestEncodeAny_NestedMaps(t *testing.T) {
	nv := Node("n2", []string{"Test"}, map[string]any{
		"deep": map[string]any{
			"level1": map[string]any{
				"level2": "leaf",
			},
		},
		"arr_of_maps": []any{
			map[string]any{"k": "v"},
		},
	})
	data, err := Encode(nv)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Decode(data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEncodeAny_FixArrayFixMap(t *testing.T) {
	nv := Node("n3", []string{"Test"}, map[string]any{
		"small_arr": []any{1, 2, 3},
		"small_map": map[string]any{"a": 1, "b": 2},
	})
	data, err := Encode(nv)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	largeArr := make([]any, 20)
	for i := range largeArr {
		largeArr[i] = i
	}
	nv2 := Node("n4", []string{"Test"}, map[string]any{
		"big_arr": largeArr,
	})
	data2, err := Encode(nv2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Decode(data2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestColumnReader_Float32Tensor(t *testing.T) {
	f32data := make([]byte, 3*4)
	for i := 0; i < 3; i++ {
		bits := math.Float32bits(float32(i) + 1.0)
		binary.LittleEndian.PutUint32(f32data[i*4:], bits)
	}

	row1 := Object(Member{Key: "embedding", Value: Bytes(f32data)})
	row2 := Object(Member{Key: "embedding", Value: Bytes(f32data)})
	arr := Array(row1, row2)

	hints := []ColumnHint{
		NewTensorHint("embedding", HintFloat32, []int{3}, 0),
	}

	data, err := EncodeWithHints(arr, hints)
	if err != nil {
		t.Fatal(err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Fatal(err)
	}

	tensors, err := cr.ReadFloat32Tensor("embedding")
	if err != nil {
		t.Fatal(err)
	}
	if len(tensors) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(tensors))
	}
	if len(tensors[0]) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(tensors[0]))
	}
}

func TestColumnReader_Float64Tensor(t *testing.T) {
	f64data := make([]byte, 2*8)
	for i := 0; i < 2; i++ {
		bits := math.Float64bits(float64(i) + 1.0)
		binary.LittleEndian.PutUint64(f64data[i*8:], bits)
	}

	row1 := Object(Member{Key: "vec", Value: Bytes(f64data)})
	row2 := Object(Member{Key: "vec", Value: Bytes(f64data)})
	arr := Array(row1, row2)

	hints := []ColumnHint{
		NewTensorHint("vec", HintFloat64, []int{2}, 0),
	}

	data, err := EncodeWithHints(arr, hints)
	if err != nil {
		t.Fatal(err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Fatal(err)
	}

	tensors, err := cr.ReadFloat64Tensor("vec")
	if err != nil {
		t.Fatal(err)
	}
	if len(tensors) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(tensors))
	}
}

func TestColumnReader_Errors(t *testing.T) {
	f32data := make([]byte, 4)
	row := Object(Member{Key: "x", Value: Bytes(f32data)})
	arr := Array(row)

	hints := []ColumnHint{
		NewTensorHint("x", HintFloat64, []int{1}, 0),
	}
	data, err := EncodeWithHints(arr, hints)
	if err != nil {
		t.Fatal(err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cr.ReadFloat32Tensor("x")
	if err != ErrIncompatibleType {
		t.Errorf("expected ErrIncompatibleType, got %v", err)
	}

	_, err = cr.ReadFloat32Tensor("nonexistent")
	if err != ErrFieldNotFound {
		t.Errorf("expected ErrFieldNotFound, got %v", err)
	}

	hints2 := []ColumnHint{
		NewTensorHint("y", HintFloat32, []int{1}, 0),
	}
	data2, err := EncodeWithHints(arr, hints2)
	if err != nil {
		t.Fatal(err)
	}
	cr2, err := NewColumnReader(data2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cr2.ReadFloat64Tensor("y")
	if err != ErrIncompatibleType {
		t.Errorf("expected ErrIncompatibleType, got %v", err)
	}
}

func TestFallbackViaJSON_Deep(t *testing.T) {
	type Custom struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	opts := AnyOptions{Enriched: true}
	v := fallbackViaJSON(Custom{Name: "Alice", Age: 30}, opts)
	if v == nil {
		t.Fatal("nil result")
	}
}

func TestFromAnyWithHint_Deep(t *testing.T) {
	v1 := fromAnyWithHint("2024-01-01T00:00:00Z", TypeDatetime64)
	if v1.Type() != TypeDatetime64 {
		t.Errorf("expected datetime, got %v", v1.Type())
	}

	v2 := fromAnyWithHint("AQID", TypeBytes)
	if v2.Type() != TypeBytes {
		t.Errorf("expected bytes, got %v", v2.Type())
	}

	v3 := fromAnyWithHint("550e8400-e29b-41d4-a716-446655440000", TypeUUID128)
	if v3.Type() != TypeUUID128 {
		t.Errorf("expected uuid, got %v", v3.Type())
	}

	v4 := fromAnyWithHint(42, TypeDatetime64)
	if v4 == nil {
		t.Error("nil")
	}

	v5 := fromAnyWithHint("not-a-date", TypeDatetime64)
	if v5 == nil {
		t.Error("nil")
	}
}

func TestFromJSONEnriched_Deep(t *testing.T) {
	input := `{"created_at": "2024-01-01T00:00:00Z", "id": "550e8400-e29b-41d4-a716-446655440000", "name": "test"}`
	v, err := FromJSONEnriched([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil")
	}
}

func TestFromAnyEnriched_FieldHints(t *testing.T) {
	input := map[string]any{
		"created_at": "2024-01-01T00:00:00Z",
		"updated":    "2024-06-01T12:00:00Z",
		"data":       "AQID",
		"uuid_field": "550e8400-e29b-41d4-a716-446655440000",
		"name":       "Alice",
		"count":      float64(42),
	}
	v := FromAnyEnriched(input)
	if v == nil {
		t.Fatal("nil")
	}
}

func TestFromAny_UnknownType(t *testing.T) {
	type Custom struct {
		X int `json:"x"`
	}
	v := FromAny(Custom{X: 42})
	if v == nil {
		t.Fatal("nil")
	}
}

func TestFromAny_JsonNumber_Deep(t *testing.T) {
	v1 := FromAny(json.Number("42"))
	if v1.Type() != TypeInt64 {
		t.Errorf("expected int64, got %v", v1.Type())
	}

	v2 := FromAny(json.Number("3.14"))
	if v2.Type() != TypeFloat64 {
		t.Errorf("expected float64, got %v", v2.Type())
	}

	v3 := FromAny(json.Number("1e10"))
	if v3.Type() != TypeFloat64 {
		t.Errorf("expected float64, got %v", v3.Type())
	}

	v4 := FromAny(json.Number("99999999999999999999999999"))
	if v4 == nil {
		t.Error("nil")
	}
}

func TestFromAny_TimeAndBytes(t *testing.T) {
	now := time.Now()
	v := FromAny(now)
	if v.Type() != TypeDatetime64 {
		t.Errorf("expected datetime, got %v", v.Type())
	}

	v2 := FromAny([]byte{1, 2, 3})
	if v2.Type() != TypeBytes {
		t.Errorf("expected bytes, got %v", v2.Type())
	}
}

func TestFromAny_IntTypes(t *testing.T) {
	tests := []struct {
		name string
		v    any
	}{
		{"int", int(42)},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"uint", uint(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := FromAny(tt.v)
			if v == nil {
				t.Fatal("nil")
			}
		})
	}
}

func TestEncodeFloat64MatrixTensor_Variants(t *testing.T) {
	if v := encodeFloat64MatrixTensor(nil, false); v != nil {
		t.Error("expected nil for empty")
	}
	if v := encodeFloat64MatrixTensor([][]float64{{}}, false); v != nil {
		t.Error("expected nil for empty cols")
	}
	if v := encodeFloat64MatrixTensor([][]float64{{1, 2}, {3}}, false); v != nil {
		t.Error("expected nil for jagged")
	}
	v := encodeFloat64MatrixTensor([][]float64{{1.0, 2.0}, {3.0, 4.0}}, true)
	if v == nil {
		t.Error("nil for valid matrix")
	}
	v2 := encodeFloat64MatrixTensor([][]float64{{1.0, 2.0}, {3.0, 4.0}}, false)
	if v2 == nil {
		t.Error("nil for valid matrix")
	}
}
