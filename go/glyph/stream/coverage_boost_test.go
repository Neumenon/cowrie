package stream

import (
	"testing"
)

// ============================================================
// types.go: Frame methods, error types
// ============================================================

func TestFrameHasCRC(t *testing.T) {
	f := &Frame{}
	if f.HasCRC() {
		t.Fatal("expected no CRC")
	}
	crc := uint32(12345)
	f.CRC = &crc
	if !f.HasCRC() {
		t.Fatal("expected CRC")
	}
}

func TestFrameHasBase(t *testing.T) {
	f := &Frame{}
	if f.HasBase() {
		t.Fatal("expected no base")
	}
	base := [32]byte{1, 2, 3}
	f.Base = &base
	if !f.HasBase() {
		t.Fatal("expected base")
	}
}

func TestParseError(t *testing.T) {
	e := &ParseError{Reason: "bad frame", Offset: 42}
	s := e.Error()
	if s != "gs1: bad frame at offset 42" {
		t.Fatalf("unexpected error: %q", s)
	}
	e2 := &ParseError{Reason: "bad frame", Offset: -1}
	s2 := e2.Error()
	if s2 != "gs1: bad frame" {
		t.Fatalf("unexpected error: %q", s2)
	}
}

func TestCRCMismatchError(t *testing.T) {
	e := &CRCMismatchError{Expected: 0x12345678, Got: 0xdeadbeef}
	s := e.Error()
	if s == "" {
		t.Fatal("expected non-empty error")
	}
}

func TestBaseMismatchError(t *testing.T) {
	e := &BaseMismatchError{
		Expected: [32]byte{1, 2, 3},
		Got:      [32]byte{4, 5, 6},
	}
	s := e.Error()
	if s == "" {
		t.Fatal("expected non-empty error")
	}
}
