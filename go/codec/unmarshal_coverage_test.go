package codec_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Neumenon/cowrie/go/codec"
)

// Coverage for unmarshalPrimitive edge cases

func TestUnmarshal_UintTarget(t *testing.T) {
	type UintStruct struct {
		Count uint   `json:"count"`
		Small uint8  `json:"small"`
		Big   uint64 `json:"big"`
	}

	data := map[string]any{
		"count": int64(42),
		"small": int64(7),
		"big":   int64(999),
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result UintStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Count != 42 {
		t.Errorf("Count: got %d, want 42", result.Count)
	}
	if result.Small != 7 {
		t.Errorf("Small: got %d, want 7", result.Small)
	}
}

func TestUnmarshal_FloatFromInt(t *testing.T) {
	type FloatStruct struct {
		Score  float64 `json:"score"`
		Score2 float32 `json:"score2"`
	}

	data := map[string]any{
		"score":  int64(42),
		"score2": int64(7),
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result FloatStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Score != 42.0 {
		t.Errorf("Score: got %v, want 42.0", result.Score)
	}
}

func TestUnmarshal_IntFromFloat(t *testing.T) {
	type IntStruct struct {
		Count int `json:"count"`
	}

	data := map[string]any{
		"count": 42.0,
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result IntStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Count != 42 {
		t.Errorf("Count: got %d, want 42", result.Count)
	}
}

func TestUnmarshal_UintFromFloat(t *testing.T) {
	type UintStruct struct {
		Count uint `json:"count"`
	}

	data := map[string]any{
		"count": 42.0,
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result UintStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Count != 42 {
		t.Errorf("Count: got %d, want 42", result.Count)
	}
}

func TestUnmarshal_StringFromNonString(t *testing.T) {
	type StringStruct struct {
		Val string `json:"val"`
	}

	data := map[string]any{
		"val": int64(42),
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result StringStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Val == "" {
		t.Error("expected non-empty string conversion")
	}
}

func TestUnmarshal_TimeFromString(t *testing.T) {
	type TimeStruct struct {
		Created time.Time `json:"created"`
	}

	now := time.Now().UTC().Truncate(time.Second)
	data := map[string]any{
		"created": now.Format(time.RFC3339),
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result TimeStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !result.Created.Equal(now) {
		t.Errorf("Created: got %v, want %v", result.Created, now)
	}
}

func TestUnmarshal_TimeFromInt(t *testing.T) {
	type TimeStruct struct {
		Created time.Time `json:"created"`
	}

	ts := int64(1700000000) // Unix timestamp
	data := map[string]any{
		"created": ts,
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result TimeStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	expected := time.Unix(ts, 0).UTC()
	if !result.Created.Equal(expected) {
		t.Errorf("Created: got %v, want %v", result.Created, expected)
	}
}

func TestUnmarshal_BytesFromString(t *testing.T) {
	type ByteStruct struct {
		Data []byte `json:"data"`
	}

	data := map[string]any{
		"data": "hello",
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result ByteStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(result.Data) != "hello" {
		t.Errorf("Data: got %q, want 'hello'", string(result.Data))
	}
}

func TestUnmarshal_BytesFromNull(t *testing.T) {
	type ByteStruct struct {
		Data []byte `json:"data"`
	}

	data := map[string]any{
		"data": nil,
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result ByteStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Data != nil {
		t.Errorf("Data: got %v, want nil", result.Data)
	}
}

func TestUnmarshal_RawMessage(t *testing.T) {
	type RawStruct struct {
		Extra json.RawMessage `json:"extra"`
	}

	data := map[string]any{
		"extra": map[string]any{"nested": int64(1)},
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result RawStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Extra) == 0 {
		t.Error("expected non-empty RawMessage")
	}
}

func TestUnmarshal_MapStringInt(t *testing.T) {
	type MapStruct struct {
		Counts map[string]int `json:"counts"`
	}

	data := map[string]any{
		"counts": map[string]any{"a": int64(1), "b": int64(2)},
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result MapStruct
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Counts["a"] != 1 || result.Counts["b"] != 2 {
		t.Errorf("Counts: got %v", result.Counts)
	}
}

func TestUnmarshal_Interface(t *testing.T) {
	data := map[string]any{
		"val": int64(42),
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result map[string]any
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["val"] == nil {
		t.Error("expected non-nil value")
	}
}

func TestUnmarshal_NullInterface(t *testing.T) {
	data := map[string]any{
		"val": nil,
	}

	encoded, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var result map[string]any
	if err := codec.DecodeBytes(encoded, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["val"] != nil {
		t.Errorf("expected nil, got %v", result["val"])
	}
}
