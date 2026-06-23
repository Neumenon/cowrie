package codec

import (
	"testing"
)

// TestMasterStream_Detection verifies IsMasterStream correctly identifies the
// preamble magic.
func TestMasterStream_Detection(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		isMaster bool
	}{
		{"master_preamble", []byte("SJST\x01\x00\x00\x00"), true},
		{"cowrie_doc", []byte("SJ\x02\x00"), false},
		{"empty", []byte{}, false},
		{"too_short", []byte("SJS"), false},
		{"wrong_magic", []byte("XXXX\x01\x00"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsMasterStream(tt.data)
			if got != tt.isMaster {
				t.Errorf("IsMasterStream = %v, want %v", got, tt.isMaster)
			}
		})
	}
}

// TestBackwardCompat_OldStructTags ensures old JSON tags still work.
func TestBackwardCompat_OldStructTags(t *testing.T) {
	type OldStyleStruct struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Hidden string `json:"-"`
		Empty  string `json:"empty,omitempty"`
	}

	original := OldStyleStruct{
		ID:     42,
		Name:   "test",
		Hidden: "should_not_appear",
		Empty:  "", // Should be omitted
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded OldStyleStruct
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify
	if decoded.ID != original.ID {
		t.Errorf("ID = %d, want %d", decoded.ID, original.ID)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Hidden != "" {
		t.Errorf("Hidden = %q, want empty (should not be serialized)", decoded.Hidden)
	}
	if decoded.Empty != "" {
		t.Errorf("Empty = %q, want empty", decoded.Empty)
	}
}

// TestBackwardCompat_PointerFields ensures pointer fields work correctly.
func TestBackwardCompat_PointerFields(t *testing.T) {
	type StructWithPointers struct {
		Name     *string `json:"name"`
		Age      *int64  `json:"age,omitempty"`
		Score    *float64 `json:"score"`
	}

	name := "Alice"
	score := 3.14

	original := StructWithPointers{
		Name:  &name,
		Age:   nil, // Should be omitted
		Score: &score,
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithPointers
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify
	if decoded.Name == nil || *decoded.Name != name {
		t.Errorf("Name = %v, want %q", decoded.Name, name)
	}
	if decoded.Age != nil {
		t.Errorf("Age = %v, want nil", decoded.Age)
	}
	if decoded.Score == nil || *decoded.Score != score {
		t.Errorf("Score = %v, want %f", decoded.Score, score)
	}
}

// TestBackwardCompat_SliceFields ensures slice fields work correctly.
func TestBackwardCompat_SliceFields(t *testing.T) {
	type StructWithSlices struct {
		Strings  []string  `json:"strings"`
		Ints     []int64   `json:"ints"`
		Floats   []float64 `json:"floats"`
		Empty    []string  `json:"empty,omitempty"`
		Nil      []string  `json:"nil,omitempty"`
	}

	original := StructWithSlices{
		Strings: []string{"a", "b", "c"},
		Ints:    []int64{1, 2, 3},
		Floats:  []float64{1.1, 2.2, 3.3},
		Empty:   []string{},
		Nil:     nil,
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithSlices
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify strings
	if len(decoded.Strings) != len(original.Strings) {
		t.Errorf("Strings len = %d, want %d", len(decoded.Strings), len(original.Strings))
	}

	// Verify ints
	if len(decoded.Ints) != len(original.Ints) {
		t.Errorf("Ints len = %d, want %d", len(decoded.Ints), len(original.Ints))
	}

	// Verify floats
	if len(decoded.Floats) != len(original.Floats) {
		t.Errorf("Floats len = %d, want %d", len(decoded.Floats), len(original.Floats))
	}
}

// TestBackwardCompat_MapFields ensures map fields work correctly.
func TestBackwardCompat_MapFields(t *testing.T) {
	type StructWithMaps struct {
		StringMap map[string]string `json:"string_map"`
		IntMap    map[string]int64  `json:"int_map"`
		AnyMap    map[string]any    `json:"any_map"`
	}

	original := StructWithMaps{
		StringMap: map[string]string{"a": "1", "b": "2"},
		IntMap:    map[string]int64{"x": 10, "y": 20},
		AnyMap:    map[string]any{"mixed": int64(1), "types": "here"},
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithMaps
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify string map
	if len(decoded.StringMap) != len(original.StringMap) {
		t.Errorf("StringMap len = %d, want %d", len(decoded.StringMap), len(original.StringMap))
	}
	for k, v := range original.StringMap {
		if decoded.StringMap[k] != v {
			t.Errorf("StringMap[%q] = %q, want %q", k, decoded.StringMap[k], v)
		}
	}

	// Verify int map
	if len(decoded.IntMap) != len(original.IntMap) {
		t.Errorf("IntMap len = %d, want %d", len(decoded.IntMap), len(original.IntMap))
	}
}
