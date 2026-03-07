package glyph

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseRejectsTrailingTokens(t *testing.T) {
	result, err := ParseWithOptions("{a:1} garbage", ParseOptions{Tolerant: false})
	if err == nil {
		t.Fatal("expected trailing-token error")
	}
	if result == nil {
		t.Fatal("expected parse result")
	}
	if result.Value != nil {
		t.Fatal("expected nil value when trailing tokens are present")
	}
}

func TestStrictParseDoesNotHangOnMalformedMap(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ParseWithOptions("{a b}", ParseOptions{Tolerant: false})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("strict parser did not make forward progress")
	}
}

func TestParsePackedParsesTrue(t *testing.T) {
	schema, err := ParseSchema(`@schema{
		Flag:v1 struct{ ok: bool }
	}`)
	if err != nil {
		t.Fatalf("ParseSchema error: %v", err)
	}

	got, err := ParsePacked("Flag@(true)", schema)
	if err != nil {
		t.Fatalf("ParsePacked error: %v", err)
	}
	if !mustAsBool(t, got.Get("ok")) {
		t.Fatal("expected ok=true")
	}
}

func TestParsePackedRejectsTrailingTokens(t *testing.T) {
	schema, err := ParseSchema(`@schema{
		Flag:v1 struct{ ok: bool }
	}`)
	if err != nil {
		t.Fatalf("ParseSchema error: %v", err)
	}

	if _, err := ParsePacked("Flag@(t) trailing", schema); err == nil {
		t.Fatal("expected trailing-token error")
	}
}

func TestBinaryToMaskRejectsOversizedBitmap(t *testing.T) {
	_, err := binaryToMask("0b" + strings.Repeat("1", maxBitmapBits+1))
	if err == nil {
		t.Fatal("expected oversized bitmap error")
	}
}

func TestParsePatchRejectsInvalidIndex(t *testing.T) {
	input := `@patch
+ items "x" @idx=abc
@end`

	if _, err := ParsePatch(input, nil); err == nil {
		t.Fatal("expected invalid @idx error")
	}
}

func TestTryParseNumberRejectsSuffixGarbage(t *testing.T) {
	if v, ok := tryParseNumber("1abc"); ok || v != nil {
		t.Fatal("expected suffixed number to be rejected")
	}
}

func TestIncrementalParserDoesNotRecurseOnErrorHandlerFailure(t *testing.T) {
	handlerCalls := 0
	handler := func(event ParseEvent) error {
		handlerCalls++
		if event.Type == EventError {
			return errors.New("handler failure")
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	if _, err := p.Feed([]byte("{incomplete")); err != nil {
		t.Fatalf("Feed error: %v", err)
	}
	if err := p.End(); err == nil {
		t.Fatal("expected end error")
	}
	if handlerCalls == 0 {
		t.Fatal("expected handler to be called")
	}
}
