package stream

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go/glyph"
)

func TestVerifyCRC(t *testing.T) {
	data := []byte("hello world")
	crc := ComputeCRC(data)
	if !VerifyCRC(data, crc) {
		t.Error("VerifyCRC should match")
	}
	if VerifyCRC(data, crc+1) {
		t.Error("VerifyCRC should not match wrong CRC")
	}
}

func TestWriter_AllFrameTypes(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// WriteDoc
	if err := w.WriteDoc(1, 1, []byte("doc payload")); err != nil {
		t.Fatalf("WriteDoc: %v", err)
	}

	// WritePatch with base
	base := [32]byte{1, 2, 3}
	if err := w.WritePatch(1, 2, []byte("patch payload"), &base); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}

	// WritePatch without base
	if err := w.WritePatch(1, 3, []byte("patch2"), nil); err != nil {
		t.Fatalf("WritePatch nil base: %v", err)
	}

	// WriteRow
	if err := w.WriteRow(1, 4, []byte("row data")); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}

	// WriteUI
	if err := w.WriteUI(1, 5, []byte("ui event")); err != nil {
		t.Fatalf("WriteUI: %v", err)
	}

	// WriteAck
	if err := w.WriteAck(1, 6); err != nil {
		t.Fatalf("WriteAck: %v", err)
	}

	// WriteErr
	if err := w.WriteErr(1, 7, []byte("error msg")); err != nil {
		t.Fatalf("WriteErr: %v", err)
	}

	// WritePing
	if err := w.WritePing(1, 8); err != nil {
		t.Fatalf("WritePing: %v", err)
	}

	// WritePong
	if err := w.WritePong(1, 9); err != nil {
		t.Fatalf("WritePong: %v", err)
	}

	// WriteFinal
	if err := w.WriteFinal(1, 10, KindDoc, []byte("final doc")); err != nil {
		t.Fatalf("WriteFinal: %v", err)
	}

	// Verify frames can be read back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	count := 0
	for {
		_, err := reader.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 10 {
		t.Errorf("expected 10 frames, read %d", count)
	}
}

func TestWriterWithCRC(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriterWithCRC(&buf)

	if err := w.WriteDoc(1, 1, []byte("hello")); err != nil {
		t.Fatalf("WriteDoc with CRC: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()), WithCRCVerification())
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.CRC == nil {
		t.Error("expected CRC in frame")
	}
}

func TestStateHashEmit(t *testing.T) {
	v := glyph.Map(glyph.MapEntry{Key: "x", Value: glyph.Int(1)})
	h := StateHashEmit(v)
	// Should be non-zero
	zero := [32]byte{}
	if h == zero {
		t.Error("expected non-zero hash")
	}
}

func TestStateHashBytes(t *testing.T) {
	h := StateHashBytes([]byte("test"))
	zero := [32]byte{}
	if h == zero {
		t.Error("expected non-zero hash")
	}
}

func TestVerifyBase(t *testing.T) {
	h1 := StateHashBytes([]byte("a"))
	h2 := StateHashBytes([]byte("a"))
	h3 := StateHashBytes([]byte("b"))

	if !VerifyBase(h1, h2) {
		t.Error("same hash should match")
	}
	if VerifyBase(h1, h3) {
		t.Error("different hashes should not match")
	}
}

func TestStreamCursor_SetStateHash(t *testing.T) {
	cursor := NewStreamCursor()
	h := [32]byte{1, 2, 3}
	cursor.SetStateHash(1, h)
	state := cursor.GetReadOnly(1)
	if state == nil {
		t.Fatal("expected state")
	}
	if !state.HasState {
		t.Error("expected HasState=true")
	}
	if state.StateHash != h {
		t.Error("hash mismatch")
	}
}

func TestStreamCursor_NeedsResync(t *testing.T) {
	cursor := NewStreamCursor()

	// Unknown SID needs resync
	if !cursor.NeedsResync(99) {
		t.Error("unknown SID should need resync")
	}

	// SID without state needs resync
	cursor.Get(1)
	if !cursor.NeedsResync(1) {
		t.Error("SID without state should need resync")
	}

	// SID with state doesn't need resync
	cursor.SetState(1, glyph.Int(1))
	if cursor.NeedsResync(1) {
		t.Error("SID with state should not need resync")
	}
}

func TestHashToHex_HexToHash(t *testing.T) {
	h := StateHashBytes([]byte("test"))
	hex := HashToHex(h)
	if len(hex) != 64 {
		t.Errorf("expected 64 char hex, got %d", len(hex))
	}

	back, ok := HexToHash(hex)
	if !ok {
		t.Error("HexToHash failed")
	}
	if back != h {
		t.Error("round-trip failed")
	}
}

func TestHexToHash_Invalid(t *testing.T) {
	_, ok := HexToHash("too short")
	if ok {
		t.Error("expected failure for short string")
	}

	_, ok = HexToHash("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	if ok {
		t.Error("expected failure for invalid hex chars")
	}
}
