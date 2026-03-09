package glyph

import (
	"strings"
	"testing"
)

func TestDefaultAutoPoolOpts(t *testing.T) {
	opts := DefaultAutoPoolOpts()
	if opts.MinLength != 20 {
		t.Errorf("MinLength: got %d, want 20", opts.MinLength)
	}
	if opts.MinOccurs != 2 {
		t.Errorf("MinOccurs: got %d, want 2", opts.MinOccurs)
	}
}

func TestAutoPoolEncode_Basic(t *testing.T) {
	longStr := strings.Repeat("abcdefghij", 3) // 30 chars
	v := List(
		Map(MapEntry{Key: "msg", Value: Str(longStr)}),
		Map(MapEntry{Key: "msg", Value: Str(longStr)}),
		Map(MapEntry{Key: "msg", Value: Str(longStr)}),
	)

	opts := DefaultAutoPoolOpts()
	result, err := AutoPoolEncode(v, opts)
	if err != nil {
		t.Fatalf("AutoPoolEncode error: %v", err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
	if result.Output == "" {
		t.Error("empty output")
	}
}

func TestAutoPoolEncode_Nil(t *testing.T) {
	opts := DefaultAutoPoolOpts()
	_, err := AutoPoolEncode(nil, opts)
	if err == nil {
		t.Error("expected error for nil value")
	}
}

func TestAutoPoolEncode_NoPool(t *testing.T) {
	// Short strings that shouldn't be pooled
	v := Map(
		MapEntry{Key: "a", Value: Str("hi")},
		MapEntry{Key: "b", Value: Str("lo")},
	)

	opts := DefaultAutoPoolOpts()
	result, err := AutoPoolEncode(v, opts)
	if err != nil {
		t.Fatalf("AutoPoolEncode error: %v", err)
	}
	if result.Stats.PoolEntries != 0 {
		t.Errorf("expected 0 pool entries, got %d", result.Stats.PoolEntries)
	}
}

func TestAutoPoolEncodeJSON_Coverage(t *testing.T) {
	longStr := strings.Repeat("abcdefghij", 3)
	jsonData := []byte(`[{"msg":"` + longStr + `"},{"msg":"` + longStr + `"}]`)

	opts := DefaultAutoPoolOpts()
	result, err := AutoPoolEncodeJSON(jsonData, opts)
	if err != nil {
		t.Fatalf("AutoPoolEncodeJSON error: %v", err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
}
