package cowrie

import (
	"bytes"
	"testing"
)

func TestTensorRoundTrip(t *testing.T) {
	// Create a 2x3 float32 tensor
	dims := []uint64{2, 3}
	data := make([]byte, 24) // 6 float32s = 24 bytes
	for i := range data {
		data[i] = byte(i)
	}

	v := Tensor(DTypeFloat32, dims, data)

	// Verify type
	if v.Type() != TypeTensor {
		t.Fatalf("expected TypeTensor, got %v", v.Type())
	}

	// Encode
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify
	if decoded.Type() != TypeTensor {
		t.Fatalf("decoded type: expected TypeTensor, got %v", decoded.Type())
	}
	tensor := decoded.Tensor()
	if tensor.DType != DTypeFloat32 {
		t.Errorf("dtype: expected %v, got %v", DTypeFloat32, tensor.DType)
	}
	if len(tensor.Dims) != 2 || tensor.Dims[0] != 2 || tensor.Dims[1] != 3 {
		t.Errorf("dims: expected [2, 3], got %v", tensor.Dims)
	}
	if !bytes.Equal(tensor.Data, data) {
		t.Errorf("data mismatch")
	}
}

func TestTensorRefRoundTrip(t *testing.T) {
	key := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	v := TensorRef(42, key)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeTensorRef {
		t.Fatalf("decoded type: expected TypeTensorRef, got %v", decoded.Type())
	}
	ref := decoded.TensorRef()
	if ref.StoreID != 42 {
		t.Errorf("storeID: expected 42, got %v", ref.StoreID)
	}
	if !bytes.Equal(ref.Key, key) {
		t.Errorf("key mismatch")
	}
}

func TestImageRoundTrip(t *testing.T) {
	data := []byte("fake png data")
	v := Image(ImageFormatPNG, 1920, 1080, data)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeImage {
		t.Fatalf("decoded type: expected TypeImage, got %v", decoded.Type())
	}
	img := decoded.Image()
	if img.Format != ImageFormatPNG {
		t.Errorf("format: expected PNG, got %v", img.Format)
	}
	if img.Width != 1920 || img.Height != 1080 {
		t.Errorf("dimensions: expected 1920x1080, got %vx%v", img.Width, img.Height)
	}
	if !bytes.Equal(img.Data, data) {
		t.Errorf("data mismatch")
	}
}

func TestAudioRoundTrip(t *testing.T) {
	data := []byte("fake opus data")
	v := Audio(AudioEncodingOPUS, 48000, 2, data)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeAudio {
		t.Fatalf("decoded type: expected TypeAudio, got %v", decoded.Type())
	}
	audio := decoded.Audio()
	if audio.Encoding != AudioEncodingOPUS {
		t.Errorf("encoding: expected OPUS, got %v", audio.Encoding)
	}
	if audio.SampleRate != 48000 {
		t.Errorf("sampleRate: expected 48000, got %v", audio.SampleRate)
	}
	if audio.Channels != 2 {
		t.Errorf("channels: expected 2, got %v", audio.Channels)
	}
	if !bytes.Equal(audio.Data, data) {
		t.Errorf("data mismatch")
	}
}

func TestAdjlistRoundTrip(t *testing.T) {
	// 3 nodes, 4 edges: 0->1, 0->2, 1->2, 2->0
	rowOffsets := []uint64{0, 2, 3, 4}
	// int32 col indices: [1, 2, 2, 0]
	colIndices := []byte{
		1, 0, 0, 0, // node 0 -> node 1
		2, 0, 0, 0, // node 0 -> node 2
		2, 0, 0, 0, // node 1 -> node 2
		0, 0, 0, 0, // node 2 -> node 0
	}
	v := Adjlist(IDWidthInt32, 3, 4, rowOffsets, colIndices)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeAdjlist {
		t.Fatalf("decoded type: expected TypeAdjlist, got %v", decoded.Type())
	}
	adj := decoded.Adjlist()
	if adj.IDWidth != IDWidthInt32 {
		t.Errorf("idWidth: expected Int32, got %v", adj.IDWidth)
	}
	if adj.NodeCount != 3 {
		t.Errorf("nodeCount: expected 3, got %v", adj.NodeCount)
	}
	if adj.EdgeCount != 4 {
		t.Errorf("edgeCount: expected 4, got %v", adj.EdgeCount)
	}
	for i, expected := range rowOffsets {
		if adj.RowOffsets[i] != expected {
			t.Errorf("rowOffsets[%d]: expected %v, got %v", i, expected, adj.RowOffsets[i])
		}
	}
	if !bytes.Equal(adj.ColIndices, colIndices) {
		t.Errorf("colIndices mismatch")
	}
}

func TestRichTextRoundTrip(t *testing.T) {
	text := "Hello, world!"
	tokens := []int32{101, 7592, 1010, 2088, 999, 102}
	spans := []RichTextSpan{
		{Start: 0, End: 5, KindID: 1},  // "Hello"
		{Start: 7, End: 12, KindID: 2}, // "world"
	}
	v := RichText(text, tokens, spans)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeRichText {
		t.Fatalf("decoded type: expected TypeRichText, got %v", decoded.Type())
	}
	rt := decoded.RichText()
	if rt.Text != text {
		t.Errorf("text: expected %q, got %q", text, rt.Text)
	}
	if len(rt.Tokens) != len(tokens) {
		t.Errorf("tokens length: expected %d, got %d", len(tokens), len(rt.Tokens))
	} else {
		for i, tok := range tokens {
			if rt.Tokens[i] != tok {
				t.Errorf("tokens[%d]: expected %d, got %d", i, tok, rt.Tokens[i])
			}
		}
	}
	if len(rt.Spans) != len(spans) {
		t.Errorf("spans length: expected %d, got %d", len(spans), len(rt.Spans))
	} else {
		for i, span := range spans {
			if rt.Spans[i] != span {
				t.Errorf("spans[%d]: expected %+v, got %+v", i, span, rt.Spans[i])
			}
		}
	}
}

func TestRichTextNoTokensNoSpans(t *testing.T) {
	v := RichText("Just plain text", nil, nil)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	rt := decoded.RichText()
	if rt.Text != "Just plain text" {
		t.Errorf("text mismatch")
	}
	if rt.Tokens != nil {
		t.Errorf("expected nil tokens")
	}
	if rt.Spans != nil {
		t.Errorf("expected nil spans")
	}
}

func TestDeltaRoundTrip(t *testing.T) {
	// Create a delta with various operations
	ops := []DeltaOp{
		{OpCode: DeltaOpSetField, FieldID: 0, Value: String("new_value")},
		{OpCode: DeltaOpDeleteField, FieldID: 1, Value: nil},
		{OpCode: DeltaOpAppendArray, FieldID: 2, Value: Int64(42)},
	}
	v := Delta(123, ops)

	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type() != TypeDelta {
		t.Fatalf("decoded type: expected TypeDelta, got %v", decoded.Type())
	}
	delta := decoded.Delta()
	if delta.BaseID != 123 {
		t.Errorf("baseID: expected 123, got %v", delta.BaseID)
	}
	if len(delta.Ops) != 3 {
		t.Fatalf("ops length: expected 3, got %d", len(delta.Ops))
	}

	// Verify SetField op
	if delta.Ops[0].OpCode != DeltaOpSetField {
		t.Errorf("op[0] opCode: expected SetField, got %v", delta.Ops[0].OpCode)
	}
	if delta.Ops[0].Value.String() != "new_value" {
		t.Errorf("op[0] value: expected 'new_value', got %q", delta.Ops[0].Value.String())
	}

	// Verify DeleteField op
	if delta.Ops[1].OpCode != DeltaOpDeleteField {
		t.Errorf("op[1] opCode: expected DeleteField, got %v", delta.Ops[1].OpCode)
	}

	// Verify AppendArray op
	if delta.Ops[2].OpCode != DeltaOpAppendArray {
		t.Errorf("op[2] opCode: expected AppendArray, got %v", delta.Ops[2].OpCode)
	}
	if delta.Ops[2].Value.Int64() != 42 {
		t.Errorf("op[2] value: expected 42, got %v", delta.Ops[2].Value.Int64())
	}
}

func TestV21TypeStrings(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{TypeTensor, "tensor"},
		{TypeTensorRef, "tensor_ref"},
		{TypeImage, "image"},
		{TypeAudio, "audio"},
		{TypeAdjlist, "adjlist"},
		{TypeRichText, "rich_text"},
		{TypeDelta, "delta"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.expected)
		}
	}
}

func TestV21InArray(t *testing.T) {
	// Test that v2.1 types work correctly when nested in arrays
	arr := Array(
		Tensor(DTypeInt8, []uint64{4}, []byte{1, 2, 3, 4}),
		Image(ImageFormatJPEG, 100, 100, []byte("jpeg")),
		String("mixed"),
		Int64(42),
	)

	encoded, err := Encode(arr)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Len() != 4 {
		t.Fatalf("array length: expected 4, got %d", decoded.Len())
	}
	if decoded.Index(0).Type() != TypeTensor {
		t.Errorf("arr[0] type: expected TypeTensor, got %v", decoded.Index(0).Type())
	}
	if decoded.Index(1).Type() != TypeImage {
		t.Errorf("arr[1] type: expected TypeImage, got %v", decoded.Index(1).Type())
	}
	if decoded.Index(2).String() != "mixed" {
		t.Errorf("arr[2] value: expected 'mixed', got %q", decoded.Index(2).String())
	}
	if decoded.Index(3).Int64() != 42 {
		t.Errorf("arr[3] value: expected 42, got %v", decoded.Index(3).Int64())
	}
}

func TestV21InObject(t *testing.T) {
	// Test that v2.1 types work correctly when nested in objects
	obj := Object(
		Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{768}, make([]byte, 768*4))},
		Member{Key: "audio_clip", Value: Audio(AudioEncodingPCMInt16, 16000, 1, []byte("audio"))},
		Member{Key: "name", Value: String("test")},
	)

	encoded, err := Encode(obj)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Get("embedding").Type() != TypeTensor {
		t.Errorf("embedding type: expected TypeTensor, got %v", decoded.Get("embedding").Type())
	}
	if decoded.Get("audio_clip").Type() != TypeAudio {
		t.Errorf("audio_clip type: expected TypeAudio, got %v", decoded.Get("audio_clip").Type())
	}
	if decoded.Get("name").String() != "test" {
		t.Errorf("name: expected 'test', got %q", decoded.Get("name").String())
	}
}
