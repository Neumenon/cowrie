package glyph

import (
	"math"
	"testing"
)

func TestCanonFloat_NegZero(t *testing.T) {
	if canonFloat(math.Float64frombits(0x8000000000000000)) != "0" {
		t.Error("canonFloat(-0) should be 0")
	}
}

func TestCanonFloat_LargeNum(t *testing.T) {
	result := canonFloat(1e7)
	if result == "" {
		t.Error("empty result for 1e7")
	}
}

func TestCanonRef_Coverage(t *testing.T) {
	r := RefID{Prefix: "m", Value: "123"}
	result := canonRef(r)
	if result != "^m:123" {
		t.Errorf("canonRef: got %q", result)
	}

	r2 := RefID{Prefix: "", Value: "abc"}
	result = canonRef(r2)
	if result != "^abc" {
		t.Errorf("canonRef no prefix: got %q", result)
	}
}

func TestIsBareSafeV2_Coverage(t *testing.T) {
	tests := []struct {
		s    string
		safe bool
	}{
		{"hello_world", true},
		{"camelCase", true},
		{"with-dash", true},
		{"with.dot", true},
		{"with/slash", true},
		{"_underscore", true},
		{"", false},
		{"hello world", false},
		{"123starts_with_digit", false},
		{"t", false},
		{"f", false},
		{"true", false},
		{"false", false},
		{"null", false},
		{"none", false},
		{"nil", false},
	}

	for _, tt := range tests {
		if got := isBareSafeV2(tt.s); got != tt.safe {
			t.Errorf("isBareSafeV2(%q) = %v, want %v", tt.s, got, tt.safe)
		}
	}
}

func TestIsRefSafe_Coverage(t *testing.T) {
	tests := []struct {
		s    string
		safe bool
	}{
		{"m:123", true},
		{"abc", true},
		{"", false},
		{"has space", false},
	}

	for _, tt := range tests {
		if got := isRefSafe(tt.s); got != tt.safe {
			t.Errorf("isRefSafe(%q) = %v, want %v", tt.s, got, tt.safe)
		}
	}
}
