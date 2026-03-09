package codec_test

import (
	"net/http"
	"testing"

	"github.com/Neumenon/cowrie/go"
	"github.com/Neumenon/cowrie/go/codec"
)

func TestFromRequest(t *testing.T) {
	tests := []struct {
		name       string
		accept     string
		wantType   string
	}{
		{"json", "application/json", "application/json"},
		{"cowrie", "application/cowrie", "application/cowrie"},
		{"empty", "", "application/json"},
		{"unknown", "text/html", "application/json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "/", nil)
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}
			c := codec.FromRequest(req)
			if c.ContentType() != tt.wantType {
				t.Errorf("FromRequest(%q) = %q, want %q", tt.accept, c.ContentType(), tt.wantType)
			}
		})
	}
}

func TestDecodeStringArray(t *testing.T) {
	arr := cowrie.Array(
		cowrie.String("hello"),
		cowrie.String("world"),
		cowrie.Int64(42), // non-string, should be empty string
	)
	result := codec.DecodeStringArray(arr)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != "hello" || result[1] != "world" {
		t.Errorf("unexpected result: %v", result)
	}
	if result[2] != "" {
		t.Errorf("non-string should be empty, got %q", result[2])
	}

	// nil input
	if codec.DecodeStringArray(nil) != nil {
		t.Error("expected nil for nil input")
	}

	// non-array input
	if codec.DecodeStringArray(cowrie.Int64(42)) != nil {
		t.Error("expected nil for non-array input")
	}
}

func TestDecodeStringMapArray(t *testing.T) {
	obj1 := cowrie.Object(
		cowrie.Member{Key: "a", Value: cowrie.String("1")},
		cowrie.Member{Key: "b", Value: cowrie.String("2")},
	)
	obj2 := cowrie.Object(
		cowrie.Member{Key: "c", Value: cowrie.Int64(3)}, // non-string value
	)
	arr := cowrie.Array(obj1, obj2)

	result := codec.DecodeStringMapArray(arr)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0]["a"] != "1" || result[0]["b"] != "2" {
		t.Errorf("unexpected first map: %v", result[0])
	}
	if _, exists := result[1]["c"]; exists {
		t.Error("non-string value should not appear in map")
	}

	// nil input
	if codec.DecodeStringMapArray(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestGetString(t *testing.T) {
	obj := cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("alice")},
		cowrie.Member{Key: "count", Value: cowrie.Int64(42)},
	)

	if codec.GetString(obj, "name") != "alice" {
		t.Error("expected 'alice'")
	}
	if codec.GetString(obj, "count") != "" {
		t.Error("expected empty string for non-string value")
	}
	if codec.GetString(obj, "missing") != "" {
		t.Error("expected empty string for missing key")
	}
	if codec.GetString(nil, "name") != "" {
		t.Error("expected empty string for nil input")
	}
}

func TestGetStringArray(t *testing.T) {
	obj := cowrie.Object(
		cowrie.Member{Key: "ids", Value: cowrie.Array(cowrie.String("a"), cowrie.String("b"))},
	)

	result := codec.GetStringArray(obj, "ids")
	if len(result) != 2 || result[0] != "a" || result[1] != "b" {
		t.Errorf("unexpected result: %v", result)
	}

	if codec.GetStringArray(obj, "missing") != nil {
		t.Error("expected nil for missing key")
	}
	if codec.GetStringArray(nil, "ids") != nil {
		t.Error("expected nil for nil input")
	}
}

func TestGetFloat32Array(t *testing.T) {
	// From tensor
	tensor := codec.EncodeFloat32Tensor([]float32{1.0, 2.0, 3.0})
	obj := cowrie.Object(
		cowrie.Member{Key: "scores", Value: tensor},
	)
	result := codec.GetFloat32Array(obj, "scores")
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}

	// From regular array
	arr := cowrie.Array(cowrie.Float64(1.5), cowrie.Float64(2.5))
	obj2 := cowrie.Object(
		cowrie.Member{Key: "values", Value: arr},
	)
	result2 := codec.GetFloat32Array(obj2, "values")
	if len(result2) != 2 {
		t.Fatalf("expected 2, got %d", len(result2))
	}

	// Missing key
	if codec.GetFloat32Array(obj, "missing") != nil {
		t.Error("expected nil for missing key")
	}

	// Nil value for key
	obj3 := cowrie.Object(
		cowrie.Member{Key: "scores", Value: nil},
	)
	if codec.GetFloat32Array(obj3, "scores") != nil {
		t.Error("expected nil for nil value")
	}

	// nil input
	if codec.GetFloat32Array(nil, "scores") != nil {
		t.Error("expected nil for nil input")
	}
}

func TestIsFloat32Array(t *testing.T) {
	arr := []float32{1.0, 2.0}
	result, ok := codec.IsFloat32Array(arr)
	if !ok || len(result) != 2 {
		t.Error("expected true for []float32")
	}

	_, ok = codec.IsFloat32Array("not a float32 array")
	if ok {
		t.Error("expected false for string")
	}
}
