package gen1

import (
	"bytes"
	"io"
	"testing"
)

func TestEncodeAppend_Coverage(t *testing.T) {
	prefix := []byte{0xAA, 0xBB}
	data, err := EncodeAppend(prefix, map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("EncodeAppend error: %v", err)
	}
	if data[0] != 0xAA || data[1] != 0xBB {
		t.Error("prefix not preserved")
	}
	decoded, err := Decode(data[2:])
	if err != nil {
		t.Fatalf("Decode after EncodeAppend: %v", err)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	if m["x"] != int64(1) {
		t.Error("expected x=1")
	}
}

func TestEncodeAppendWithOptions_Coverage(t *testing.T) {
	opts := DefaultEncodeOptions()
	data, err := EncodeAppendWithOptions(nil, "hello", opts)
	if err != nil {
		t.Fatalf("EncodeAppendWithOptions error: %v", err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != "hello" {
		t.Errorf("expected 'hello', got %v", decoded)
	}
}

func TestNewStreamDecoderWithOptions_Coverage(t *testing.T) {
	// Encode two values
	d1, _ := Encode(42)
	d2, _ := Encode("hello")
	buf := append(d1, d2...)

	opts := DefaultDecodeOptions()
	dec := NewStreamDecoderWithOptions(bytes.NewReader(buf), opts)

	v1, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode 1: %v", err)
	}
	if v1 != int64(42) {
		t.Errorf("expected 42, got %v", v1)
	}

	v2, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode 2: %v", err)
	}
	if v2 != "hello" {
		t.Errorf("expected 'hello', got %v", v2)
	}

	_, err = dec.Decode()
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestEncodeJSON_Coverage(t *testing.T) {
	data, err := EncodeJSON([]byte(`{"a": 1, "b": "hello"}`))
	if err != nil {
		t.Fatalf("EncodeJSON error: %v", err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	if m["b"] != "hello" {
		t.Error("expected b=hello")
	}
}

func TestDecodeJSON_Coverage(t *testing.T) {
	encoded, _ := Encode(map[string]any{"x": 42})
	jsonData, err := DecodeJSON(encoded)
	if err != nil {
		t.Fatalf("DecodeJSON error: %v", err)
	}
	if len(jsonData) == 0 {
		t.Error("empty JSON")
	}
}

func TestEncodeTo_Coverage(t *testing.T) {
	var buf bytes.Buffer
	err := EncodeTo(&buf, map[string]any{"y": "test"})
	if err != nil {
		t.Fatalf("EncodeTo error: %v", err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	if m["y"] != "test" {
		t.Error("expected y=test")
	}
}

func TestSetDefaultEncodeOptions_Coverage(t *testing.T) {
	original := DefaultEncodeOptions()
	opts := EncodeOptions{HighPrecision: false}
	SetDefaultEncodeOptions(opts)
	defer SetDefaultEncodeOptions(original) // restore

	current := globalEncodeOptions
	if current.HighPrecision {
		t.Error("expected HighPrecision=false after setting defaults")
	}
}

func TestEncodeWithOptions_Coverage(t *testing.T) {
	v := map[string]any{"b": 2, "a": 1}
	opts := EncodeOptions{HighPrecision: true}
	data, err := EncodeWithOptions(v, opts)
	if err != nil {
		t.Fatalf("EncodeWithOptions error: %v", err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	if m["a"] != int64(1) {
		t.Error("expected a=1")
	}
}
