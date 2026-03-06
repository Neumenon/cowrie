package codec_test

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/Neumenon/cowrie/go"
	"github.com/Neumenon/cowrie/go/codec"
)

// TestCodecEquivalence verifies JSON and Cowrie produce equivalent results.
func TestCodecEquivalence(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
	}{
		{
			name: "simple_strings",
			data: map[string]any{
				"ids":  []any{"doc1", "doc2", "doc3"},
				"docs": []any{"hello world", "foo bar", "test"},
			},
		},
		{
			name: "with_integers",
			data: map[string]any{
				"count": int64(42),
				"ids":   []any{"a", "b"},
			},
		},
		{
			name: "with_floats",
			data: map[string]any{
				"score":  0.95,
				"scores": []any{0.1, 0.2, 0.3},
			},
		},
		{
			name: "nested_objects",
			data: map[string]any{
				"meta": map[string]any{
					"author": "test",
					"count":  int64(10),
				},
			},
		},
		{
			name: "query_response_like",
			data: map[string]any{
				"ids":    []any{"id1", "id2", "id3"},
				"docs":   []any{"doc one", "doc two", "doc three"},
				"scores": []any{0.95, 0.87, 0.72},
				"stats":  "3 results",
				"meta":   nil,
				"next":   "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode with JSON
			var jsonBuf bytes.Buffer
			jsonCodec := codec.JSONCodec{}
			if err := jsonCodec.Encode(&jsonBuf, tt.data); err != nil {
				t.Fatalf("JSON encode error: %v", err)
			}

			// Encode with Cowrie
			var cowrieBuf bytes.Buffer
			cowrieCodec := codec.CowrieCodec{}
			if err := cowrieCodec.Encode(&cowrieBuf, tt.data); err != nil {
				t.Fatalf("Cowrie encode error: %v", err)
			}

			// Decode both
			var jsonResult, cowrieResult map[string]any
			if err := jsonCodec.Decode(&jsonBuf, &jsonResult); err != nil {
				t.Fatalf("JSON decode error: %v", err)
			}
			if err := cowrieCodec.Decode(bytes.NewReader(cowrieBuf.Bytes()), &cowrieResult); err != nil {
				t.Fatalf("Cowrie decode error: %v", err)
			}

			// Compare results (normalize for comparison)
			if !compareAny(jsonResult, cowrieResult) {
				t.Errorf("Results differ:\nJSON:  %v\nCowrie: %v", jsonResult, cowrieResult)
			}
		})
	}
}

// TestFloat32TensorOptimization verifies float32 slices use tensor encoding.
func TestFloat32TensorOptimization(t *testing.T) {
	// Create a query response with float32 scores
	scores := []float32{0.95, 0.87, 0.72, 0.65, 0.50}
	data := map[string]any{
		"ids":    []any{"a", "b", "c", "d", "e"},
		"scores": scores,
	}

	// Encode with Cowrie
	cowrieBytes, err := codec.EncodeBytes(data)
	if err != nil {
		t.Fatalf("Cowrie encode error: %v", err)
	}

	// Decode back
	var result map[string]any
	if err := codec.DecodeBytes(cowrieBytes, &result); err != nil {
		t.Fatalf("Cowrie decode error: %v", err)
	}

	// Verify scores are preserved
	// With direct tensor decoding, we now get []float32 directly (better!)
	switch rs := result["scores"].(type) {
	case []float32:
		// Direct tensor decode - preferred path
		if len(rs) != len(scores) {
			t.Fatalf("scores length mismatch: got %d, want %d", len(rs), len(scores))
		}
		for i, f := range rs {
			if math.Abs(float64(f)-float64(scores[i])) > 0.0001 {
				t.Errorf("score[%d] mismatch: got %v, want %v", i, f, scores[i])
			}
		}
	case []any:
		// Fallback path (legacy)
		if len(rs) != len(scores) {
			t.Fatalf("scores length mismatch: got %d, want %d", len(rs), len(scores))
		}
		for i, s := range rs {
			var f float64
			switch v := s.(type) {
			case float32:
				f = float64(v)
			case float64:
				f = v
			default:
				t.Fatalf("score[%d] unexpected type: %T", i, s)
			}
			if math.Abs(f-float64(scores[i])) > 0.0001 {
				t.Errorf("score[%d] mismatch: got %v, want %v", i, f, scores[i])
			}
		}
	default:
		t.Fatalf("scores unexpected type: %T", result["scores"])
	}
}

// TestFloat64TensorOptimization verifies float64 tensor round-trip preserves precision.
func TestFloat64TensorOptimization(t *testing.T) {
	// Create float64 values with high precision
	scores := []float64{0.123456789012345, 0.987654321098765, 3.141592653589793}

	// Encode as tensor
	tensor := codec.EncodeFloat64Tensor(scores)
	if tensor == nil {
		t.Fatal("EncodeFloat64Tensor returned nil")
	}

	// Decode using DecodeFloat64Tensor
	result := codec.DecodeFloat64Tensor(tensor)
	if result == nil {
		t.Fatal("DecodeFloat64Tensor returned nil")
	}

	if len(result) != len(scores) {
		t.Fatalf("length mismatch: got %d, want %d", len(result), len(scores))
	}

	for i, f := range result {
		if f != scores[i] {
			t.Errorf("score[%d] mismatch: got %v, want %v", i, f, scores[i])
		}
	}
}

// TestDecodeTensorAuto verifies dtype-aware tensor decoding.
func TestDecodeTensorAuto(t *testing.T) {
	// Test Float32
	f32 := codec.EncodeFloat32Tensor([]float32{1.0, 2.0, 3.0})
	result := codec.DecodeTensorAuto(f32)
	if f32Slice, ok := result.([]float32); !ok {
		t.Errorf("Float32 tensor should decode to []float32, got %T", result)
	} else if len(f32Slice) != 3 {
		t.Errorf("Float32 slice length mismatch: got %d, want 3", len(f32Slice))
	}

	// Test Float64
	f64 := codec.EncodeFloat64Tensor([]float64{1.0, 2.0, 3.0})
	result = codec.DecodeTensorAuto(f64)
	if f64Slice, ok := result.([]float64); !ok {
		t.Errorf("Float64 tensor should decode to []float64, got %T", result)
	} else if len(f64Slice) != 3 {
		t.Errorf("Float64 slice length mismatch: got %d, want 3", len(f64Slice))
	}
}

// TestRegisterPointerType verifies Register works with pointer types.
// This tests the fix for Register[T] where T is a pointer type like *MyStruct.
func TestRegisterPointerType(t *testing.T) {
	type PtrTestStruct struct {
		Name  string
		Value int64
	}

	// Register with pointer type as the type parameter
	// This used to fail because reflect.TypeOf((*PtrTestStruct)(nil)) returned nil
	codec.Register[*PtrTestStruct](func(v *cowrie.Value, ptr **PtrTestStruct) error {
		*ptr = &PtrTestStruct{
			Name:  "custom_decoded",
			Value: 42,
		}
		return nil
	})
	defer codec.Unregister[*PtrTestStruct]()

	// Verify registration
	if !codec.IsRegistered[*PtrTestStruct]() {
		t.Error("Expected *PtrTestStruct to be registered")
	}

	// Test unregister
	codec.Unregister[*PtrTestStruct]()
	if codec.IsRegistered[*PtrTestStruct]() {
		t.Error("Expected *PtrTestStruct to be unregistered")
	}
}

// TestContentTypeNegotiation verifies codec selection from headers.
func TestContentTypeNegotiation(t *testing.T) {
	tests := []struct {
		contentType string
		wantType    string
	}{
		{"application/json", "application/json"},
		{"application/cowrie", "application/cowrie"},
		{"application/json; charset=utf-8", "application/json"},
		{"", "application/json"},           // default
		{"text/plain", "application/json"}, // unknown -> default
	}

	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			c := codec.FromContentType(tt.contentType)
			if got := c.ContentType(); got != tt.wantType {
				t.Errorf("FromContentType(%q).ContentType() = %q, want %q", tt.contentType, got, tt.wantType)
			}
		})
	}
}

// TestCowrieSizeAdvantage verifies Cowrie is smaller for float32 arrays.
func TestCowrieSizeAdvantage(t *testing.T) {
	// Create realistic query response with scores
	scores := make([]float32, 100) // 100 scores
	for i := range scores {
		scores[i] = float32(i) * 0.01
	}
	data := map[string]any{
		"ids":    make([]any, 100),
		"scores": scores,
	}
	for i := range data["ids"].([]any) {
		data["ids"].([]any)[i] = "doc_" + string(rune('0'+i%10))
	}

	// Encode with JSON
	var jsonBuf bytes.Buffer
	jsonCodec := codec.JSONCodec{}
	if err := jsonCodec.Encode(&jsonBuf, data); err != nil {
		t.Fatalf("JSON encode error: %v", err)
	}

	// Encode with Cowrie
	var cowrieBuf bytes.Buffer
	cowrieCodec := codec.CowrieCodec{}
	if err := cowrieCodec.Encode(&cowrieBuf, data); err != nil {
		t.Fatalf("Cowrie encode error: %v", err)
	}

	jsonSize := jsonBuf.Len()
	cowrieSize := cowrieBuf.Len()
	savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

	t.Logf("JSON size:  %d bytes", jsonSize)
	t.Logf("Cowrie size: %d bytes", cowrieSize)
	t.Logf("Savings:    %.1f%%", savings)

	// Cowrie should be smaller (tensor encoding for float32)
	if cowrieSize >= jsonSize {
		t.Errorf("Expected Cowrie to be smaller: JSON=%d, Cowrie=%d", jsonSize, cowrieSize)
	}
}

// compareAny compares two any values, handling type differences.
func compareAny(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !compareAny(v, bv[k]) {
				return false
			}
		}
		return true

	case []any:
		switch bv := b.(type) {
		case []any:
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if !compareAny(av[i], bv[i]) {
					return false
				}
			}
			return true
		case []float64:
			// Handle JSON []any vs Cowrie []float64
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if !compareAny(av[i], bv[i]) {
					return false
				}
			}
			return true
		case []float32:
			// Handle JSON []any vs Cowrie []float32
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if !compareAny(av[i], float64(bv[i])) {
					return false
				}
			}
			return true
		}
		return false

	case []float64:
		switch bv := b.(type) {
		case []float64:
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if math.Abs(av[i]-bv[i]) > 0.0001 {
					return false
				}
			}
			return true
		case []any:
			// Handle Cowrie []float64 vs JSON []any
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if !compareAny(av[i], bv[i]) {
					return false
				}
			}
			return true
		}
		return false

	case []float32:
		switch bv := b.(type) {
		case []float32:
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if math.Abs(float64(av[i])-float64(bv[i])) > 0.0001 {
					return false
				}
			}
			return true
		case []any:
			// Handle Cowrie []float32 vs JSON []any
			if len(av) != len(bv) {
				return false
			}
			for i := range av {
				if !compareAny(float64(av[i]), bv[i]) {
					return false
				}
			}
			return true
		}
		return false

	case float64:
		switch bv := b.(type) {
		case float64:
			return math.Abs(av-bv) < 0.0001
		case float32:
			return math.Abs(av-float64(bv)) < 0.0001
		case int64:
			return av == float64(bv)
		}
		return false

	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
		return false

	case string:
		bv, ok := b.(string)
		return ok && av == bv

	default:
		return reflect.DeepEqual(a, b)
	}
}
