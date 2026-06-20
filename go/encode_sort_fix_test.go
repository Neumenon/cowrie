package cowrie

import (
	"bytes"
	"testing"
)

// TestEncodeAny_MapSortedDeterministic verifies that EncodeAny produces
// byte-identical output across multiple calls for a map[string]any whose
// keys are inserted in non-sorted order. Prior to the fix, Go's map
// iteration order is random, so repeated encodings could differ.
func TestEncodeAny_MapSortedDeterministic(t *testing.T) {
	// Keys deliberately in non-sorted order to expose any random-iteration path.
	m := map[string]any{
		"zebra": "last",
		"alpha": int64(1),
		"mango": float64(3.14),
		"berry": true,
	}

	first, err := EncodeAny(m)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}

	const runs = 20
	for i := 1; i < runs; i++ {
		got, err := EncodeAny(m)
		if err != nil {
			t.Fatalf("EncodeAny failed on run %d: %v", i, err)
		}
		if !bytes.Equal(first, got) {
			t.Fatalf("run %d produced different bytes:\nfirst: %x\n got:  %x", i, first, got)
		}
	}
}

// TestEncodeAny_MapRoundTrip verifies that a map[string]any encoded with
// EncodeAny can be decoded back to a value that contains all original keys
// and values.
func TestEncodeAny_MapRoundTrip(t *testing.T) {
	m := map[string]any{
		"zebra": "last",
		"alpha": int64(1),
		"mango": float64(3.14),
	}

	data, err := EncodeAny(m)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}

	got, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}

	gotMap, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("decoded value is %T, want map[string]any", got)
	}

	if len(gotMap) != len(m) {
		t.Fatalf("decoded map has %d keys, want %d", len(gotMap), len(m))
	}

	// Verify string value round-trips.
	if v, ok := gotMap["zebra"]; !ok {
		t.Error("missing key \"zebra\"")
	} else if s, ok := v.(string); !ok || s != "last" {
		t.Errorf("\"zebra\" = %v (%T), want \"last\" (string)", v, v)
	}

	// int64(1) may decode as int64 or via fixint path — compare numerically.
	if v, ok := gotMap["alpha"]; !ok {
		t.Error("missing key \"alpha\"")
	} else {
		var n int64
		switch x := v.(type) {
		case int64:
			n = x
		case uint64:
			n = int64(x)
		default:
			t.Errorf("\"alpha\" decoded as unexpected type %T: %v", v, v)
		}
		if n != 1 {
			t.Errorf("\"alpha\" = %d, want 1", n)
		}
	}

	if v, ok := gotMap["mango"]; !ok {
		t.Error("missing key \"mango\"")
	} else if f, ok := v.(float64); !ok || f != 3.14 {
		t.Errorf("\"mango\" = %v (%T), want 3.14 (float64)", v, v)
	}
}
