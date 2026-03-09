package cowrie

import (
	"bytes"
	"strings"
	"testing"
)

func TestTagError(t *testing.T) {
	err := &TagError{Tag: 0xAB}
	msg := err.Error()
	if !strings.Contains(msg, "ab") {
		t.Errorf("TagError: expected hex 'ab' in message, got %q", msg)
	}
}

func TestDecodeFrom(t *testing.T) {
	val := Object(
		Member{Key: "name", Value: String("test")},
	)
	data, err := Encode(val)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrom(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}
	if decoded.Get("name").String() != "test" {
		t.Error("expected name=test")
	}
}

func TestDecodeFromLimited_Coverage(t *testing.T) {
	val := Object(Member{Key: "x", Value: Int64(42)})
	data, err := Encode(val)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Within limit
	decoded, err := DecodeFromLimited(bytes.NewReader(data), int64(len(data)+100))
	if err != nil {
		t.Fatalf("DecodeFromLimited failed: %v", err)
	}
	if decoded.Get("x").Int64() != 42 {
		t.Error("expected x=42")
	}

	// Exceed limit
	_, err = DecodeFromLimited(bytes.NewReader(data), 1)
	if err != ErrInputTooLarge {
		t.Errorf("expected ErrInputTooLarge, got %v", err)
	}
}

func TestDecodeFromWithOptions_Coverage(t *testing.T) {
	val := Object(Member{Key: "y", Value: Float64(3.14)})
	data, err := Encode(val)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	opts := DefaultDecodeOptions()
	decoded, err := DecodeFromWithOptions(bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("DecodeFromWithOptions failed: %v", err)
	}
	if decoded.Get("y").Float64() != 3.14 {
		t.Error("expected y=3.14")
	}
}

func TestValueToAny_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		val  *Value
	}{
		{"nil", nil},
		{"null", Null()},
		{"bool", Bool(true)},
		{"int64", Int64(42)},
		{"uint64", Uint64(99)},
		{"float64", Float64(3.14)},
		{"string", String("hello")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"datetime64", Datetime64(12345)},
		{"decimal128", NewDecimal128(2, [16]byte{})},
		{"uuid128", UUID128([16]byte{1, 2, 3})},
		{"bigint", BigInt([]byte{0x01})},
		{"array", Array(Int64(1), String("two"))},
		{"object", Object(Member{Key: "k", Value: Int64(1)})},
		{"tensor", Tensor(DTypeFloat32, []uint64{2}, make([]byte, 8))},
		{"tensor_ref", TensorRef(0, []byte{0xAA})},
		{"image", Image(0, 10, 10, []byte{0xFF})},
		{"audio", Audio(0, 44100, 2, []byte{0x00})},
		{"adjlist", Adjlist(0, 3, 2, []uint64{0, 1, 2}, []byte{1, 2})},
		{"richtext", RichText("hello", nil, nil)},
		{"delta", Delta(0, []DeltaOp{{OpCode: DeltaOpSetField}})},
		{"bitmask", Bitmask(8, []byte{0xFF})},
		{"unknown_ext", UnknownExtension(999, []byte{0x01})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToAny(tt.val)
			_ = result // Just ensure no panic
		})
	}
}

func TestValueToAny_NestedArray(t *testing.T) {
	val := Array(
		Int64(1),
		String("hello"),
		Array(Int64(2), Int64(3)),
	)
	result := valueToAny(val)
	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d", len(arr))
	}
}

func TestValueToAny_NestedObject(t *testing.T) {
	val := Object(
		Member{Key: "inner", Value: Object(
			Member{Key: "deep", Value: Int64(42)},
		)},
	)
	result := valueToAny(val)
	obj, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	inner, ok := obj["inner"].(map[string]any)
	if !ok {
		t.Fatalf("expected inner map, got %T", obj["inner"])
	}
	if inner["deep"] != int64(42) {
		t.Errorf("expected deep=42, got %v", inner["deep"])
	}
}

func TestValueToAny_GraphTypes(t *testing.T) {
	// Node
	nodeVal := Node("n1", []string{"Person"}, map[string]any{"name": "Alice"})
	result := valueToAny(nodeVal)
	if result == nil {
		t.Error("expected non-nil for Node")
	}

	// Edge
	edgeVal := Edge("n1", "n2", "KNOWS", map[string]any{"since": "2020"})
	result = valueToAny(edgeVal)
	if result == nil {
		t.Error("expected non-nil for Edge")
	}

	// NodeBatch
	nbVal := NodeBatch([]NodeData{
		{ID: "n1", Labels: []string{"A"}},
		{ID: "n2", Labels: []string{"B"}},
	})
	result = valueToAny(nbVal)
	if result == nil {
		t.Error("expected non-nil for NodeBatch")
	}

	// EdgeBatch
	ebVal := EdgeBatch([]EdgeData{
		{From: "n1", To: "n2", Type: "REL"},
	})
	result = valueToAny(ebVal)
	if result == nil {
		t.Error("expected non-nil for EdgeBatch")
	}

	// GraphShard
	gsVal := GraphShard(
		[]NodeData{{ID: "n1"}},
		[]EdgeData{{From: "n1", To: "n2"}},
		map[string]any{"version": "1"},
	)
	result = valueToAny(gsVal)
	if result == nil {
		t.Error("expected non-nil for GraphShard")
	}
}

func TestEncodeDecode_CompressedGzip(t *testing.T) {
	val := Object(
		Member{Key: "data", Value: String(strings.Repeat("hello world ", 100))},
	)
	data, err := EncodeFramed(val, CompressionGzip)
	if err != nil {
		t.Fatalf("EncodeFramed gzip failed: %v", err)
	}

	decoded, err := DecodeFramed(data)
	if err != nil {
		t.Fatalf("DecodeFramed gzip failed: %v", err)
	}
	if decoded.Get("data").String() != strings.Repeat("hello world ", 100) {
		t.Error("gzip roundtrip data mismatch")
	}
}

func TestEncodeDecode_CompressedZstd(t *testing.T) {
	val := Object(
		Member{Key: "data", Value: String(strings.Repeat("zstd test data ", 100))},
	)
	data, err := EncodeFramed(val, CompressionZstd)
	if err != nil {
		t.Fatalf("EncodeFramed zstd failed: %v", err)
	}

	decoded, err := DecodeFramed(data)
	if err != nil {
		t.Fatalf("DecodeFramed zstd failed: %v", err)
	}
	if decoded.Get("data").String() != strings.Repeat("zstd test data ", 100) {
		t.Error("zstd roundtrip data mismatch")
	}
}

func TestDecodeFramed_Uncompressed(t *testing.T) {
	val := Object(Member{Key: "x", Value: Int64(42)})
	data, err := Encode(val)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// DecodeFramed should handle uncompressed data too
	decoded, err := DecodeFramed(data)
	if err != nil {
		t.Fatalf("DecodeFramed failed: %v", err)
	}
	if decoded.Get("x").Int64() != 42 {
		t.Error("expected x=42")
	}
}

func TestEncodeFramed_BelowThreshold(t *testing.T) {
	// Small data should not be compressed
	val := Object(Member{Key: "x", Value: Int64(1)})
	data, err := EncodeFramed(val, CompressionGzip)
	if err != nil {
		t.Fatalf("EncodeFramed failed: %v", err)
	}
	// Should be same as regular Encode
	raw, _ := Encode(val)
	if len(data) != len(raw) {
		t.Errorf("small data should not be compressed: framed=%d, raw=%d", len(data), len(raw))
	}
}

func TestColumnReader_DatetimeAndBytes(t *testing.T) {
	// Build an array of objects with datetime and bytes fields
	rows := Array(
		Object(
			Member{Key: "ts", Value: Datetime64(1000000)},
			Member{Key: "data", Value: Bytes([]byte{1, 2, 3})},
		),
		Object(
			Member{Key: "ts", Value: Datetime64(2000000)},
			Member{Key: "data", Value: Bytes([]byte{4, 5, 6})},
		),
	)

	hints := []ColumnHint{
		{Field: "ts", Type: HintDatetime},
		{Field: "data", Type: HintBytes},
	}

	data, err := EncodeWithHints(rows, hints)
	if err != nil {
		t.Fatalf("EncodeWithHints failed: %v", err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Fatalf("NewColumnReader failed: %v", err)
	}

	// ReadDatetimeColumn
	ts, valid, err := cr.ReadDatetimeColumn("ts")
	if err != nil {
		t.Fatalf("ReadDatetimeColumn failed: %v", err)
	}
	if len(ts) != 2 || !valid[0] || !valid[1] {
		t.Errorf("unexpected datetime results: %v, %v", ts, valid)
	}
	if ts[0] != 1000000 || ts[1] != 2000000 {
		t.Errorf("datetime values: got %v", ts)
	}

	// ReadBytesColumn
	bdata, bvalid, err := cr.ReadBytesColumn("data")
	if err != nil {
		t.Fatalf("ReadBytesColumn failed: %v", err)
	}
	if len(bdata) != 2 || !bvalid[0] || !bvalid[1] {
		t.Errorf("unexpected bytes results: %v, %v", bdata, bvalid)
	}
}
