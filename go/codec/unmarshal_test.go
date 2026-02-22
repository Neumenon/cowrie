package codec_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Neumenon/cowrie/codec"
)

// Test structs for edge cases
type SimpleStruct struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type NestedStruct struct {
	ID    string       `json:"id"`
	Inner SimpleStruct `json:"inner"`
}

type PointerStruct struct {
	Name  *string       `json:"name"`
	Inner *SimpleStruct `json:"inner"`
}

type SliceStruct struct {
	IDs    []string       `json:"ids"`
	Scores []float32      `json:"scores"`
	Items  []SimpleStruct `json:"items"`
}

type MapStruct struct {
	Meta map[string]string `json:"meta"`
	Data map[string]int    `json:"data"`
}

type EmbeddedStruct struct {
	SimpleStruct
	Extra string `json:"extra"`
}

type OmitEmptyStruct struct {
	Name  string `json:"name,omitempty"`
	Count int    `json:"count,omitempty"`
}

func TestDirectUnmarshalSimpleStruct(t *testing.T) {
	data := map[string]any{
		"name":  "test",
		"count": int64(42),
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result SimpleStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("Name mismatch: got %q, want %q", result.Name, "test")
	}
	if result.Count != 42 {
		t.Errorf("Count mismatch: got %d, want %d", result.Count, 42)
	}
}

func TestDirectUnmarshalNestedStruct(t *testing.T) {
	data := map[string]any{
		"id": "outer",
		"inner": map[string]any{
			"name":  "inner_name",
			"count": int64(100),
		},
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result NestedStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.ID != "outer" {
		t.Errorf("ID mismatch: got %q, want %q", result.ID, "outer")
	}
	if result.Inner.Name != "inner_name" {
		t.Errorf("Inner.Name mismatch: got %q, want %q", result.Inner.Name, "inner_name")
	}
	if result.Inner.Count != 100 {
		t.Errorf("Inner.Count mismatch: got %d, want %d", result.Inner.Count, 100)
	}
}

func TestDirectUnmarshalPointerStruct(t *testing.T) {
	data := map[string]any{
		"name": "ptr_test",
		"inner": map[string]any{
			"name":  "inner_ptr",
			"count": int64(50),
		},
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result PointerStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Name == nil || *result.Name != "ptr_test" {
		t.Errorf("Name mismatch: got %v", result.Name)
	}
	if result.Inner == nil {
		t.Errorf("Inner is nil")
	} else {
		if result.Inner.Name != "inner_ptr" {
			t.Errorf("Inner.Name mismatch: got %q", result.Inner.Name)
		}
	}
}

func TestDirectUnmarshalSliceStruct(t *testing.T) {
	data := map[string]any{
		"ids":    []any{"a", "b", "c"},
		"scores": []float32{0.9, 0.8, 0.7},
		"items": []any{
			map[string]any{"name": "item1", "count": int64(1)},
			map[string]any{"name": "item2", "count": int64(2)},
		},
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result SliceStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(result.IDs) != 3 {
		t.Errorf("IDs length mismatch: got %d, want 3", len(result.IDs))
	}
	if len(result.Scores) != 3 {
		t.Errorf("Scores length mismatch: got %d, want 3", len(result.Scores))
	}
	if len(result.Items) != 2 {
		t.Errorf("Items length mismatch: got %d, want 2", len(result.Items))
	}
	if result.Items[0].Name != "item1" {
		t.Errorf("Items[0].Name mismatch: got %q", result.Items[0].Name)
	}
}

func TestDirectUnmarshalMapStruct(t *testing.T) {
	data := map[string]any{
		"meta": map[string]any{
			"key1": "value1",
			"key2": "value2",
		},
		"data": map[string]any{
			"count": int64(10),
			"size":  int64(20),
		},
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result MapStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Meta["key1"] != "value1" {
		t.Errorf("Meta[key1] mismatch: got %q", result.Meta["key1"])
	}
	if result.Data["count"] != 10 {
		t.Errorf("Data[count] mismatch: got %d", result.Data["count"])
	}
}

func TestDirectUnmarshalNilFields(t *testing.T) {
	data := map[string]any{
		"name": nil,
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result PointerStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Name != nil {
		t.Errorf("Name should be nil, got %v", result.Name)
	}
	if result.Inner != nil {
		t.Errorf("Inner should be nil")
	}
}

func TestDirectUnmarshalEmptySlices(t *testing.T) {
	data := map[string]any{
		"ids":    []any{},
		"scores": []float32{},
		"items":  []any{},
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result SliceStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(result.IDs) != 0 {
		t.Errorf("IDs should be empty, got %d", len(result.IDs))
	}
	if len(result.Scores) != 0 {
		t.Errorf("Scores should be empty, got %d", len(result.Scores))
	}
}

func TestDirectUnmarshalUnknownFields(t *testing.T) {
	// Unknown fields should be ignored
	data := map[string]any{
		"name":    "test",
		"count":   int64(42),
		"unknown": "should be ignored",
		"extra":   int64(999),
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result SimpleStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("Name mismatch: got %q", result.Name)
	}
	if result.Count != 42 {
		t.Errorf("Count mismatch: got %d", result.Count)
	}
}

func TestDirectUnmarshalTypeCoercion(t *testing.T) {
	// Test int/float coercion
	data := map[string]any{
		"name":  "coerce",
		"count": 42.0, // float64 instead of int64
	}

	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var result SimpleStruct
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result.Count != 42 {
		t.Errorf("Count mismatch: got %d, want 42", result.Count)
	}
}

func TestDirectUnmarshalFloat32Tensor(t *testing.T) {
	// Test that float32 slices are correctly decoded from tensors
	scores := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	data := map[string]any{
		"scores": scores,
	}

	var buf bytes.Buffer
	cowrieCodec := codec.CowrieCodec{}
	if err := cowrieCodec.Encode(&buf, data); err != nil {
		t.Fatalf("encode error: %v", err)
	}

	type ScoreStruct struct {
		Scores []float32 `json:"scores"`
	}

	var result ScoreStruct
	if err := cowrieCodec.Decode(bytes.NewReader(buf.Bytes()), &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(result.Scores) != len(scores) {
		t.Fatalf("Scores length mismatch: got %d, want %d", len(result.Scores), len(scores))
	}
	for i, s := range result.Scores {
		if s != scores[i] {
			t.Errorf("Score[%d] mismatch: got %v, want %v", i, s, scores[i])
		}
	}
}

// NOTE: TestCoreMessage* tests removed — they depend on Agent-GO/core which is not part of the standalone codec.

// TestRawMessageField tests json.RawMessage direct handling.
func TestRawMessageField(t *testing.T) {
	type StructWithRawJSON struct {
		ID   int             `json:"id"`
		Data json.RawMessage `json:"data"`
	}

	original := StructWithRawJSON{
		ID:   42,
		Data: json.RawMessage(`{"foo":"bar","nums":[1,2,3]}`),
	}

	// Encode with Cowrie
	var buf bytes.Buffer
	cowrieCodec := codec.CowrieCodec{}
	if err := cowrieCodec.Encode(&buf, original); err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode back
	var decoded StructWithRawJSON
	if err := cowrieCodec.Decode(bytes.NewReader(buf.Bytes()), &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %d, want %d", decoded.ID, original.ID)
	}

	// Verify Data is valid JSON
	var dataMap map[string]any
	if err := json.Unmarshal(decoded.Data, &dataMap); err != nil {
		t.Fatalf("Data is not valid JSON: %v", err)
	}

	if dataMap["foo"] != "bar" {
		t.Errorf("Data.foo mismatch: got %v, want %q", dataMap["foo"], "bar")
	}
}

// TestRawMessageInvalidJSON verifies that invalid JSON in RawMessage is preserved as string
// instead of being lost (converted to null).
func TestRawMessageInvalidJSON(t *testing.T) {
	type StructWithRawJSON struct {
		ID   int             `json:"id"`
		Data json.RawMessage `json:"data"`
	}

	// Invalid JSON that can't be parsed
	invalidJSON := `not valid json {`
	original := StructWithRawJSON{
		ID:   42,
		Data: json.RawMessage(invalidJSON),
	}

	// Encode with Cowrie - should preserve as string, not convert to null
	encoded, err := codec.FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode into generic map to check the value
	var decoded map[string]any
	if err := codec.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Data should be preserved as string, not null
	data, ok := decoded["data"].(string)
	if !ok {
		t.Fatalf("Expected data to be string, got %T: %v", decoded["data"], decoded["data"])
	}
	if data != invalidJSON {
		t.Errorf("Invalid JSON not preserved: got %q, want %q", data, invalidJSON)
	}
}
