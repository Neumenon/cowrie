package cowrie

import (
	"bytes"
	"testing"
)

func TestTypeString_Coverage(t *testing.T) {
	tests := []struct {
		typ  Type
		want string
	}{
		{TypeNull, "null"},
		{TypeBool, "bool"},
		{TypeInt64, "int64"},
		{TypeUint64, "uint64"},
		{TypeFloat64, "float64"},
		{TypeDecimal128, "decimal128"},
		{TypeString, "string"},
		{TypeBytes, "bytes"},
		{TypeDatetime64, "datetime64"},
		{TypeUUID128, "uuid128"},
		{TypeBigInt, "bigint"},
		{TypeArray, "array"},
		{TypeObject, "object"},
		{TypeTensor, "tensor"},
		{TypeTensorRef, "tensor_ref"},
		{TypeImage, "image"},
		{TypeAudio, "audio"},
		{TypeAdjlist, "adjlist"},
		{TypeRichText, "rich_text"},
		{TypeDelta, "delta"},
		{TypeNode, "node"},
		{TypeEdge, "edge"},
		{TypeNodeBatch, "node_batch"},
		{TypeEdgeBatch, "edge_batch"},
		{TypeGraphShard, "graph_shard"},
		{TypeBitmask, "bitmask"},
		{TypeUnknownExt, "unknown_ext"},
		{Type(255), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestDTypeString_Coverage(t *testing.T) {
	tests := []struct {
		d    DType
		want string
	}{
		{DTypeFloat32, "float32"},
		{DTypeFloat16, "float16"},
		{DTypeBFloat16, "bfloat16"},
		{DTypeFloat64, "float64"},
		{DTypeInt8, "int8"},
		{DTypeInt16, "int16"},
		{DTypeInt32, "int32"},
		{DTypeInt64, "int64"},
		{DTypeUint8, "uint8"},
		{DTypeUint16, "uint16"},
		{DTypeUint32, "uint32"},
		{DTypeUint64, "uint64"},
		{DType(255), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.d.String(); got != tt.want {
			t.Errorf("DType(%d).String() = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestSchemaFingerprint64_Coverage(t *testing.T) {
	// Scalar types
	h1 := SchemaFingerprint64(Null())
	h2 := SchemaFingerprint64(Int64(42))
	if h1 == h2 {
		t.Error("null and int should have different fingerprints")
	}

	// nil
	h3 := SchemaFingerprint64(nil)
	if h3 == 0 {
		t.Error("nil fingerprint should be non-zero")
	}

	// Object
	obj := Object(
		Member{Key: "a", Value: Int64(1)},
		Member{Key: "b", Value: String("hello")},
	)
	h4 := SchemaFingerprint64(obj)
	if h4 == 0 {
		t.Error("object fingerprint should be non-zero")
	}

	// Array
	arr := Array(Int64(1), Int64(2))
	h5 := SchemaFingerprint64(arr)
	if h5 == 0 {
		t.Error("array fingerprint should be non-zero")
	}

	// Tensor
	tensor := Tensor(DTypeFloat32, []uint64{2}, encodeFloat32LE([]float32{1.0, 2.0}))
	h6 := SchemaFingerprint64(tensor)
	if h6 == 0 {
		t.Error("tensor fingerprint should be non-zero")
	}

	// Image
	img := Image(ImageFormatPNG, 10, 10, []byte{0xFF})
	h7 := SchemaFingerprint64(img)
	if h7 == 0 {
		t.Error("image fingerprint")
	}

	// Audio
	aud := Audio(AudioEncodingPCMInt16, 44100, 1, []byte{0x00, 0x01})
	h8 := SchemaFingerprint64(aud)
	if h8 == 0 {
		t.Error("audio fingerprint")
	}

	// Adjlist
	adj := Adjlist(IDWidthInt32, 2, 1, []uint64{0, 0, 1}, []byte{1, 0, 0, 0})
	h9 := SchemaFingerprint64(adj)
	if h9 == 0 {
		t.Error("adjlist fingerprint")
	}

	// UnknownExt
	ue := UnknownExtension(42, []byte{0x01})
	h10 := SchemaFingerprint64(ue)
	if h10 == 0 {
		t.Error("unknown_ext fingerprint")
	}
}

func TestSchemaFingerprint32_Coverage(t *testing.T) {
	fp := SchemaFingerprint32(Int64(42))
	if fp == 0 {
		t.Error("fingerprint32 should be non-zero")
	}
}

func TestSchemaEquals_Coverage(t *testing.T) {
	a := Object(Member{Key: "x", Value: Int64(1)})
	b := Object(Member{Key: "x", Value: Int64(99)})
	if !SchemaEquals(a, b) {
		t.Error("same schema should be equal")
	}

	c := Object(Member{Key: "y", Value: String("hello")})
	if SchemaEquals(a, c) {
		t.Error("different schemas should not be equal")
	}
}

func TestSchemaDescriptor_Coverage(t *testing.T) {
	tests := []struct {
		name string
		v    *Value
		want string
	}{
		{"nil", nil, "null"},
		{"null", Null(), "null"},
		{"bool", Bool(true), "bool"},
		{"int64", Int64(42), "int64"},
		{"uint64", Uint64(42), "uint64"},
		{"float64", Float64(3.14), "float64"},
		{"string", String("hello"), "string"},
		{"bytes", Bytes([]byte{1}), "bytes"},
		{"empty_array", Array(), "[]"},
		{"array", Array(Int64(1)), "[int64,...]"},
		{"empty_object", Object(), "{}"},
		{"object", Object(Member{Key: "x", Value: Int64(1)}), "{x,...}"},
		{"tensor", Tensor(DTypeFloat32, []uint64{2}, encodeFloat32LE([]float32{1.0, 2.0})), "tensor<float32>"},
		{"image", Image(ImageFormatPNG, 1, 1, []byte{0}), "image"},
		{"audio", Audio(AudioEncodingPCMInt16, 44100, 1, []byte{0, 1}), "audio"},
		{"unknown_ext", UnknownExtension(1, []byte{0}), "ext"},
	}
	for _, tt := range tests {
		got := SchemaDescriptor(tt.v)
		if got != tt.want {
			t.Errorf("SchemaDescriptor(%s) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestEncodeToWriter_Coverage(t *testing.T) {
	v := Object(
		Member{Key: "name", Value: String("Alice")},
		Member{Key: "age", Value: Int64(30)},
	)

	var buf bytes.Buffer
	err := EncodeToWriter(&buf, v)
	if err != nil {
		t.Fatalf("EncodeToWriter: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("empty output")
	}

	// Verify round-trip
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Get("name").String() != "Alice" {
		t.Error("name mismatch")
	}
}

func TestEncodeToWriter_WithTensor(t *testing.T) {
	v := Object(
		Member{Key: "data", Value: Tensor(DTypeFloat32, []uint64{3}, encodeFloat32LE([]float32{1.0, 2.0, 3.0}))},
	)

	var buf bytes.Buffer
	err := EncodeToWriter(&buf, v)
	if err != nil {
		t.Fatalf("EncodeToWriter with tensor: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("empty output")
	}
}
