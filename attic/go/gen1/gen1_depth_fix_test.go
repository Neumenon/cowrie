package gen1

import (
	"errors"
	"testing"
)

func TestDepthLimitEnforced(t *testing.T) {
	var nested any = int64(0)
	for i := 0; i < DefaultMaxDepth+5; i++ {
		nested = []any{nested}
	}
	data, err := Encode(nested)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := Decode(data); !errors.Is(err, ErrMaxDepthExceeded) {
		t.Fatalf("expected ErrMaxDepthExceeded for deep nesting, got %v", err)
	}
}

func TestDepthLimitAllowsNormalNesting(t *testing.T) {
	var nested any = int64(0)
	for i := 0; i < 50; i++ {
		nested = []any{nested}
	}
	data, _ := Encode(nested)
	if _, err := Decode(data); err != nil {
		t.Fatalf("50-deep nesting should decode fine, got %v", err)
	}
}
