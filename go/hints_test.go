package cowrie

import (
	"testing"
)

func TestHintTypeString(t *testing.T) {
	tests := []struct {
		typ  HintType
		want string
	}{
		{HintInt64, "int64"},
		{HintFloat64, "float64"},
		{HintFloat32, "float32"},
		{HintString, "string"},
		{HintBytes, "bytes"},
		{HintUint64, "uint64"},
		{HintDatetime, "datetime"},
		{HintUUID, "uuid"},
		{HintType(0xFF), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("HintType(%#x).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestHintFlagsHas(t *testing.T) {
	flags := HintFlagRequired | HintFlagColumnar

	if !flags.Has(HintFlagRequired) {
		t.Error("Expected HintFlagRequired to be set")
	}
	if !flags.Has(HintFlagColumnar) {
		t.Error("Expected HintFlagColumnar to be set")
	}
	if flags.Has(HintFlagFixedSize) {
		t.Error("Expected HintFlagFixedSize to NOT be set")
	}
	if flags.Has(HintFlagSorted) {
		t.Error("Expected HintFlagSorted to NOT be set")
	}
}

func TestHintByteSize(t *testing.T) {
	tests := []struct {
		typ  HintType
		want int
	}{
		{HintInt64, 8},
		{HintUint64, 8},
		{HintFloat64, 8},
		{HintDatetime, 8},
		{HintFloat32, 4},
		{HintUUID, 16},
		{HintString, -1}, // Variable size
		{HintBytes, -1},  // Variable size
	}
	for _, tt := range tests {
		if got := tt.typ.ByteSize(); got != tt.want {
			t.Errorf("HintType(%#x).ByteSize() = %d, want %d", tt.typ, got, tt.want)
		}
	}
}

func TestColumnHintTotalBytes(t *testing.T) {
	tests := []struct {
		name string
		hint ColumnHint
		want int
	}{
		{
			name: "scalar int64",
			hint: ColumnHint{Field: "id", Type: HintInt64},
			want: 8,
		},
		{
			name: "float32 embedding",
			hint: ColumnHint{Field: "embedding", Type: HintFloat32, Shape: []int{128}},
			want: 4 * 128,
		},
		{
			name: "float64 matrix",
			hint: ColumnHint{Field: "matrix", Type: HintFloat64, Shape: []int{10, 10}},
			want: 8 * 10 * 10,
		},
		{
			name: "variable size string",
			hint: ColumnHint{Field: "name", Type: HintString},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.hint.TotalBytes(); got != tt.want {
				t.Errorf("TotalBytes() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNewHint(t *testing.T) {
	h := NewHint("id", HintInt64, HintFlagRequired)
	if h.Field != "id" {
		t.Errorf("Field = %q, want %q", h.Field, "id")
	}
	if h.Type != HintInt64 {
		t.Errorf("Type = %#x, want %#x", h.Type, HintInt64)
	}
	if h.Flags != HintFlagRequired {
		t.Errorf("Flags = %#x, want %#x", h.Flags, HintFlagRequired)
	}
	if len(h.Shape) != 0 {
		t.Errorf("Shape = %v, want empty", h.Shape)
	}
}

func TestNewTensorHint(t *testing.T) {
	h := NewTensorHint("embedding", HintFloat32, []int{128}, HintFlagColumnar)
	if h.Field != "embedding" {
		t.Errorf("Field = %q, want %q", h.Field, "embedding")
	}
	if h.Type != HintFloat32 {
		t.Errorf("Type = %#x, want %#x", h.Type, HintFloat32)
	}
	if !h.Flags.Has(HintFlagColumnar) {
		t.Error("Expected HintFlagColumnar to be set")
	}
	if !h.Flags.Has(HintFlagFixedSize) {
		t.Error("Expected HintFlagFixedSize to be set (auto-added by NewTensorHint)")
	}
	if len(h.Shape) != 1 || h.Shape[0] != 128 {
		t.Errorf("Shape = %v, want [128]", h.Shape)
	}
}

func TestEncodeDecodeWithHints(t *testing.T) {
	// Create a test value
	v := Object(
		Member{Key: "id", Value: Int64(12345)},
		Member{Key: "name", Value: String("test")},
		Member{Key: "active", Value: Bool(true)},
	)

	// Create hints
	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired|HintFlagSorted),
		NewHint("name", HintString, HintFlagRequired),
	}

	// Encode with hints
	data, err := EncodeWithHints(v, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	// Verify flags byte has HAS_COLUMN_HINTS set
	if len(data) < 4 {
		t.Fatal("Data too short")
	}
	if data[3]&FlagHasColumnHints == 0 {
		t.Error("Expected FlagHasColumnHints to be set in flags byte")
	}

	// Decode with hints
	result, err := DecodeWithHints(data)
	if err != nil {
		t.Fatalf("DecodeWithHints failed: %v", err)
	}

	// Verify value
	if result.Value.Get("id").Int64() != 12345 {
		t.Errorf("id = %d, want 12345", result.Value.Get("id").Int64())
	}
	if result.Value.Get("name").String() != "test" {
		t.Errorf("name = %q, want %q", result.Value.Get("name").String(), "test")
	}

	// Verify hints
	if len(result.Hints) != 2 {
		t.Fatalf("len(Hints) = %d, want 2", len(result.Hints))
	}
	if result.Hints[0].Field != "id" {
		t.Errorf("Hints[0].Field = %q, want %q", result.Hints[0].Field, "id")
	}
	if result.Hints[0].Type != HintInt64 {
		t.Errorf("Hints[0].Type = %#x, want %#x", result.Hints[0].Type, HintInt64)
	}
	if !result.Hints[0].Flags.Has(HintFlagSorted) {
		t.Error("Expected Hints[0] to have HintFlagSorted")
	}
}

func TestEncodeDecodeWithTensorHints(t *testing.T) {
	// Create a value with embedded tensor data
	v := Object(
		Member{Key: "id", Value: Int64(1)},
		Member{Key: "embedding", Value: Bytes(make([]byte, 128*4))}, // 128 float32s
	)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired),
		NewTensorHint("embedding", HintFloat32, []int{128}, HintFlagColumnar),
	}

	data, err := EncodeWithHints(v, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	result, err := DecodeWithHints(data)
	if err != nil {
		t.Fatalf("DecodeWithHints failed: %v", err)
	}

	// Verify tensor hint shape
	if len(result.Hints) != 2 {
		t.Fatalf("len(Hints) = %d, want 2", len(result.Hints))
	}

	embeddingHint := result.Hints[1]
	if embeddingHint.Field != "embedding" {
		t.Errorf("Field = %q, want %q", embeddingHint.Field, "embedding")
	}
	if len(embeddingHint.Shape) != 1 || embeddingHint.Shape[0] != 128 {
		t.Errorf("Shape = %v, want [128]", embeddingHint.Shape)
	}
	if !embeddingHint.Flags.Has(HintFlagFixedSize) {
		t.Error("Expected HintFlagFixedSize")
	}
}

func TestDecodeIgnoresHints(t *testing.T) {
	// Create a value with hints
	v := Object(Member{Key: "x", Value: Int64(42)})
	hints := []ColumnHint{NewHint("x", HintInt64, HintFlagRequired)}

	data, err := EncodeWithHints(v, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	// Decode WITHOUT hints (should skip them)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Get("x").Int64() != 42 {
		t.Errorf("x = %d, want 42", decoded.Get("x").Int64())
	}
}

func TestEncodeWithEmptyHints(t *testing.T) {
	v := Object(Member{Key: "a", Value: Int64(1)})

	// Empty hints - should not set flag
	data, err := EncodeWithHints(v, nil)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	if data[3]&FlagHasColumnHints != 0 {
		t.Error("Expected FlagHasColumnHints to NOT be set for empty hints")
	}

	// Decode should work normally
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Get("a").Int64() != 1 {
		t.Errorf("a = %d, want 1", decoded.Get("a").Int64())
	}
}

func TestHintRoundTripMultiDimensional(t *testing.T) {
	v := Object(Member{Key: "tensor", Value: Bytes(make([]byte, 10*20*30*4))})

	hints := []ColumnHint{
		NewTensorHint("tensor", HintFloat32, []int{10, 20, 30}, 0),
	}

	data, err := EncodeWithHints(v, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	result, err := DecodeWithHints(data)
	if err != nil {
		t.Fatalf("DecodeWithHints failed: %v", err)
	}

	if len(result.Hints) != 1 {
		t.Fatalf("len(Hints) = %d, want 1", len(result.Hints))
	}

	h := result.Hints[0]
	if len(h.Shape) != 3 {
		t.Fatalf("len(Shape) = %d, want 3", len(h.Shape))
	}
	if h.Shape[0] != 10 || h.Shape[1] != 20 || h.Shape[2] != 30 {
		t.Errorf("Shape = %v, want [10 20 30]", h.Shape)
	}

	// Verify TotalBytes
	expected := 10 * 20 * 30 * 4
	if h.TotalBytes() != expected {
		t.Errorf("TotalBytes() = %d, want %d", h.TotalBytes(), expected)
	}
}

func BenchmarkEncodeWithHints(b *testing.B) {
	v := Object(
		Member{Key: "id", Value: Int64(12345)},
		Member{Key: "name", Value: String("benchmark")},
		Member{Key: "embedding", Value: Bytes(make([]byte, 128*4))},
	)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired),
		NewHint("name", HintString, HintFlagRequired),
		NewTensorHint("embedding", HintFloat32, []int{128}, HintFlagColumnar),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeWithHints(v, hints)
	}
}

func BenchmarkDecodeWithHints(b *testing.B) {
	v := Object(
		Member{Key: "id", Value: Int64(12345)},
		Member{Key: "name", Value: String("benchmark")},
		Member{Key: "embedding", Value: Bytes(make([]byte, 128*4))},
	)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired),
		NewHint("name", HintString, HintFlagRequired),
		NewTensorHint("embedding", HintFloat32, []int{128}, HintFlagColumnar),
	}

	data, _ := EncodeWithHints(v, hints)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeWithHints(data)
	}
}

// ========== Column Reader Tests ==========

func TestColumnReaderBasic(t *testing.T) {
	// Create array of objects
	v := Array(
		Object(
			Member{Key: "id", Value: Int64(1)},
			Member{Key: "name", Value: String("Alice")},
		),
		Object(
			Member{Key: "id", Value: Int64(2)},
			Member{Key: "name", Value: String("Bob")},
		),
		Object(
			Member{Key: "id", Value: Int64(3)},
			Member{Key: "name", Value: String("Charlie")},
		),
	)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired|HintFlagColumnar),
		NewHint("name", HintString, HintFlagRequired),
	}

	data, err := EncodeWithHints(v, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Fatalf("NewColumnReader failed: %v", err)
	}

	// Check hints
	if len(cr.Hints()) != 2 {
		t.Errorf("Expected 2 hints, got %d", len(cr.Hints()))
	}

	// Read id column
	ids, valid, err := cr.ReadInt64Column("id")
	if err != nil {
		t.Fatalf("ReadInt64Column failed: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("Expected 3 ids, got %d", len(ids))
	}
	for i, expected := range []int64{1, 2, 3} {
		if !valid[i] {
			t.Errorf("ids[%d] should be valid", i)
		}
		if ids[i] != expected {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], expected)
		}
	}

	// Read name column
	names, validNames, err := cr.ReadStringColumn("name")
	if err != nil {
		t.Fatalf("ReadStringColumn failed: %v", err)
	}
	for i, expected := range []string{"Alice", "Bob", "Charlie"} {
		if !validNames[i] {
			t.Errorf("names[%d] should be valid", i)
		}
		if names[i] != expected {
			t.Errorf("names[%d] = %q, want %q", i, names[i], expected)
		}
	}
}

func TestColumnReaderWithNulls(t *testing.T) {
	v := Array(
		Object(Member{Key: "id", Value: Int64(1)}, Member{Key: "value", Value: Float64(1.5)}),
		Object(Member{Key: "id", Value: Int64(2)}), // Missing value field
		Object(Member{Key: "id", Value: Int64(3)}, Member{Key: "value", Value: Null()}),
	)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired),
		NewHint("value", HintFloat64, 0),
	}

	data, _ := EncodeWithHints(v, hints)
	cr, _ := NewColumnReader(data)

	values, valid, _ := cr.ReadFloat64Column("value")
	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if !valid[0] || values[0] != 1.5 {
		t.Errorf("values[0] should be 1.5, got %v (valid=%v)", values[0], valid[0])
	}
	if valid[1] {
		t.Error("values[1] should be invalid (missing)")
	}
	if valid[2] {
		t.Error("values[2] should be invalid (null)")
	}
}

func TestColumnReaderStats(t *testing.T) {
	v := Array(
		Object(Member{Key: "x", Value: Int64(1)}),
		Object(Member{Key: "x", Value: Int64(2)}),
		Object(Member{Key: "y", Value: Int64(3)}), // Missing x
		Object(Member{Key: "x", Value: Null()}),
	)

	hints := []ColumnHint{NewHint("x", HintInt64, 0)}
	data, _ := EncodeWithHints(v, hints)
	cr, _ := NewColumnReader(data)

	stats, err := cr.Stats("x")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.Count != 4 {
		t.Errorf("Count = %d, want 4", stats.Count)
	}
	if stats.ValidCount != 2 {
		t.Errorf("ValidCount = %d, want 2", stats.ValidCount)
	}
	if stats.NullCount != 2 {
		t.Errorf("NullCount = %d, want 2", stats.NullCount)
	}
}

func TestColumnReaderLen(t *testing.T) {
	v := Array(
		Object(Member{Key: "a", Value: Int64(1)}),
		Object(Member{Key: "a", Value: Int64(2)}),
	)

	hints := []ColumnHint{NewHint("a", HintInt64, 0)}
	data, _ := EncodeWithHints(v, hints)
	cr, _ := NewColumnReader(data)

	length, err := cr.Len()
	if err != nil {
		t.Fatalf("Len failed: %v", err)
	}
	if length != 2 {
		t.Errorf("Len = %d, want 2", length)
	}
}

func TestColumnReaderNonArrayRoot(t *testing.T) {
	v := Object(Member{Key: "x", Value: Int64(1)})
	hints := []ColumnHint{NewHint("x", HintInt64, 0)}
	data, _ := EncodeWithHints(v, hints)

	cr, _ := NewColumnReader(data)

	// ReadColumn should fail for non-array root
	_, err := cr.ReadColumn("x")
	if err != ErrArrayRequired {
		t.Errorf("Expected ErrArrayRequired, got %v", err)
	}

	// But Root should work
	root, err := cr.Root()
	if err != nil {
		t.Fatalf("Root failed: %v", err)
	}
	if root.Get("x").Int64() != 1 {
		t.Error("Root value incorrect")
	}
}

func TestColumnReaderGetHint(t *testing.T) {
	v := Array(Object(Member{Key: "a", Value: Int64(1)}))
	hints := []ColumnHint{
		NewHint("a", HintInt64, HintFlagRequired),
		NewTensorHint("emb", HintFloat32, []int{64}, HintFlagColumnar),
	}
	data, _ := EncodeWithHints(v, hints)
	cr, _ := NewColumnReader(data)

	// Found hint
	h := cr.GetHint("a")
	if h == nil {
		t.Fatal("Expected to find hint for 'a'")
	}
	if h.Type != HintInt64 {
		t.Errorf("Type = %v, want HintInt64", h.Type)
	}

	// Found tensor hint
	h2 := cr.GetHint("emb")
	if h2 == nil {
		t.Fatal("Expected to find hint for 'emb'")
	}
	if len(h2.Shape) != 1 || h2.Shape[0] != 64 {
		t.Errorf("Shape = %v, want [64]", h2.Shape)
	}

	// Not found
	if cr.GetHint("nonexistent") != nil {
		t.Error("Expected nil for nonexistent field")
	}
}

func TestColumnReaderFields(t *testing.T) {
	v := Array(Object(Member{Key: "a", Value: Int64(1)}))
	hints := []ColumnHint{
		NewHint("id", HintInt64, 0),
		NewHint("name", HintString, 0),
		NewTensorHint("embedding", HintFloat32, []int{128}, 0),
	}
	data, _ := EncodeWithHints(v, hints)
	cr, _ := NewColumnReader(data)

	fields := cr.Fields()
	if len(fields) != 3 {
		t.Fatalf("len(fields) = %d, want 3", len(fields))
	}
	expected := []string{"id", "name", "embedding"}
	for i, f := range expected {
		if fields[i] != f {
			t.Errorf("fields[%d] = %q, want %q", i, fields[i], f)
		}
	}
}

func BenchmarkColumnReaderInt64(b *testing.B) {
	// Create array of 1000 objects
	items := make([]*Value, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = Object(
			Member{Key: "id", Value: Int64(int64(i))},
			Member{Key: "value", Value: Float64(float64(i) * 1.5)},
		)
	}
	v := Array(items...)

	hints := []ColumnHint{
		NewHint("id", HintInt64, HintFlagRequired|HintFlagColumnar),
		NewHint("value", HintFloat64, HintFlagColumnar),
	}

	data, _ := EncodeWithHints(v, hints)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cr, _ := NewColumnReader(data)
		cr.ReadInt64Column("id")
	}
}
