package stream

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// ============================================================
// Writer Tests
// ============================================================

func TestWriter_MinimalFrame(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	err := w.WriteFrame(&Frame{
		Version: 1,
		SID:     0,
		Seq:     0,
		Kind:    KindDoc,
		Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	got := buf.String()
	want := "@frame{v=1 sid=0 seq=0 kind=doc len=2}\n{}\n"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestWriter_WithCRC(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriterWithCRC(&buf)

	payload := []byte("{x=1}")
	err := w.WriteFrame(&Frame{
		Version: 1,
		SID:     1,
		Seq:     5,
		Kind:    KindPatch,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	got := buf.String()
	// Should contain crc=
	if !strings.Contains(got, "crc=") {
		t.Errorf("expected crc= in output: %s", got)
	}
}

func TestWriter_WithBase(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	base := [32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}

	err := w.WriteFrame(&Frame{
		Version: 1,
		SID:     1,
		Seq:     10,
		Kind:    KindPatch,
		Payload: []byte("@patch\nset .x 1\n@end"),
		Base:    &base,
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "base=sha256:") {
		t.Errorf("expected base=sha256: in output: %s", got)
	}
}

func TestWriter_FinalFlag(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	err := w.WriteFrame(&Frame{
		Version: 1,
		SID:     1,
		Seq:     100,
		Kind:    KindDoc,
		Payload: []byte("final"),
		Final:   true,
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "final=true") {
		t.Errorf("expected final=true in output: %s", got)
	}
}

func TestWriter_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	err := w.WriteAck(1, 42)
	if err != nil {
		t.Fatalf("WriteAck failed: %v", err)
	}

	got := buf.String()
	want := "@frame{v=1 sid=1 seq=42 kind=ack len=0}\n\n"
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

// ============================================================
// Reader Tests
// ============================================================

func TestReader_MinimalFrame(t *testing.T) {
	input := "@frame{v=1 sid=0 seq=0 kind=doc len=2}\n{}\n"
	r := NewReader(strings.NewReader(input))

	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if frame.Version != 1 {
		t.Errorf("Version = %d, want 1", frame.Version)
	}
	if frame.SID != 0 {
		t.Errorf("SID = %d, want 0", frame.SID)
	}
	if frame.Seq != 0 {
		t.Errorf("Seq = %d, want 0", frame.Seq)
	}
	if frame.Kind != KindDoc {
		t.Errorf("Kind = %v, want doc", frame.Kind)
	}
	if string(frame.Payload) != "{}" {
		t.Errorf("Payload = %q, want {}", string(frame.Payload))
	}
}

func TestReader_WithCRC(t *testing.T) {
	payload := []byte("hello")
	crc := ComputeCRC(payload)

	input := "@frame{v=1 sid=1 seq=5 kind=doc len=5 crc=" + crcToHex(crc) + "}\nhello\n"
	r := NewReader(strings.NewReader(input))

	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if frame.CRC == nil {
		t.Error("expected CRC to be present")
	} else if *frame.CRC != crc {
		t.Errorf("CRC = %08x, want %08x", *frame.CRC, crc)
	}
}

func TestReader_CRCMismatch(t *testing.T) {
	input := "@frame{v=1 sid=1 seq=5 kind=doc len=5 crc=deadbeef}\nhello\n"
	r := NewReader(strings.NewReader(input))

	_, err := r.Next()
	if err == nil {
		t.Error("expected CRC mismatch error")
	}
	if _, ok := err.(*CRCMismatchError); !ok {
		t.Errorf("expected CRCMismatchError, got %T: %v", err, err)
	}
}

func TestReader_WithBase(t *testing.T) {
	base := [32]byte{0xab, 0xcd}
	baseHex := HashToHex(base)

	input := "@frame{v=1 sid=1 seq=10 kind=patch len=4 base=sha256:" + baseHex + "}\ntest\n"
	r := NewReader(strings.NewReader(input))

	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if frame.Base == nil {
		t.Error("expected base to be present")
	} else if *frame.Base != base {
		t.Errorf("base mismatch")
	}
}

func TestReader_PayloadWithNewlines(t *testing.T) {
	// Payload contains newlines - reader must use len, not delimiters
	payload := "@patch\nset .x 1\nset .y 2\n@end"
	payloadLen := len(payload)

	input := "@frame{v=1 sid=1 seq=1 kind=patch len=" + itoa(payloadLen) + "}\n" + payload + "\n"
	r := NewReader(strings.NewReader(input))

	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if string(frame.Payload) != payload {
		t.Errorf("Payload = %q, want %q", string(frame.Payload), payload)
	}
}

func TestReader_PayloadWithBraces(t *testing.T) {
	// Payload contains braces - reader must use len, not delimiters
	payload := `{a={b={c=1}}}`
	payloadLen := len(payload)

	input := "@frame{v=1 sid=1 seq=1 kind=doc len=" + itoa(payloadLen) + "}\n" + payload + "\n"
	r := NewReader(strings.NewReader(input))

	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if string(frame.Payload) != payload {
		t.Errorf("Payload = %q, want %q", string(frame.Payload), payload)
	}
}

func TestReader_MultipleFrames(t *testing.T) {
	input := `@frame{v=1 sid=1 seq=0 kind=doc len=5}
hello
@frame{v=1 sid=1 seq=1 kind=patch len=6}
update
@frame{v=1 sid=1 seq=2 kind=ack len=0}

`
	r := NewReader(strings.NewReader(input))

	frames, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(frames))
	}

	if frames[0].Kind != KindDoc || string(frames[0].Payload) != "hello" {
		t.Errorf("frame 0: got %v/%q", frames[0].Kind, frames[0].Payload)
	}
	if frames[1].Kind != KindPatch || string(frames[1].Payload) != "update" {
		t.Errorf("frame 1: got %v/%q", frames[1].Kind, frames[1].Payload)
	}
	if frames[2].Kind != KindAck || len(frames[2].Payload) != 0 {
		t.Errorf("frame 2: got %v/%d bytes", frames[2].Kind, len(frames[2].Payload))
	}
}

func TestReader_AllKinds(t *testing.T) {
	kinds := []string{"doc", "patch", "row", "ui", "ack", "err", "ping", "pong"}
	for _, k := range kinds {
		input := "@frame{v=1 sid=0 seq=0 kind=" + k + " len=1}\nx\n"
		r := NewReader(strings.NewReader(input))
		frame, err := r.Next()
		if err != nil {
			t.Errorf("kind=%s: %v", k, err)
			continue
		}
		if frame.Kind.String() != k {
			t.Errorf("kind=%s: got %s", k, frame.Kind.String())
		}
	}
}

func TestReader_NumericKind(t *testing.T) {
	input := "@frame{v=1 sid=0 seq=0 kind=99 len=1}\nx\n"
	r := NewReader(strings.NewReader(input))
	frame, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if frame.Kind != FrameKind(99) {
		t.Errorf("Kind = %d, want 99", frame.Kind)
	}
}

func TestReader_HeaderVariations(t *testing.T) {
	// Test comma-separated
	input1 := "@frame{v=1,sid=1,seq=0,kind=doc,len=1}\nx\n"
	r1 := NewReader(strings.NewReader(input1))
	if _, err := r1.Next(); err != nil {
		t.Errorf("comma-separated: %v", err)
	}

	// Test with extra spaces
	input2 := "@frame{  v=1   sid=1  seq=0  kind=doc   len=1  }\nx\n"
	r2 := NewReader(strings.NewReader(input2))
	if _, err := r2.Next(); err != nil {
		t.Errorf("extra spaces: %v", err)
	}
}

func TestReader_EOF(t *testing.T) {
	r := NewReader(strings.NewReader(""))
	_, err := r.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestReader_PayloadTooLarge(t *testing.T) {
	input := "@frame{v=1 sid=0 seq=0 kind=doc len=999999999}\n"
	r := NewReader(strings.NewReader(input), WithMaxPayload(1024))

	_, err := r.Next()
	if err == nil {
		t.Error("expected error for large payload")
	}
}

// ============================================================
// Round-trip Tests
// ============================================================

func TestRoundtrip_AllFrameTypes(t *testing.T) {
	testCases := []struct {
		name  string
		frame Frame
	}{
		{"minimal doc", Frame{Version: 1, SID: 0, Seq: 0, Kind: KindDoc, Payload: []byte("{}")}},
		{"patch with base", Frame{Version: 1, SID: 1, Seq: 5, Kind: KindPatch, Payload: []byte("@patch\nset .x 1\n@end"), Base: &[32]byte{0x01, 0x02}}},
		{"row", Frame{Version: 1, SID: 2, Seq: 100, Kind: KindRow, Payload: []byte("Row@(id 1 name foo)")}},
		{"ui", Frame{Version: 1, SID: 1, Seq: 50, Kind: KindUI, Payload: []byte(`UIEvent@(type "progress" pct 0.5)`)}},
		{"ack", Frame{Version: 1, SID: 1, Seq: 10, Kind: KindAck, Payload: nil}},
		{"err", Frame{Version: 1, SID: 1, Seq: 11, Kind: KindErr, Payload: []byte(`Err@(code "FAIL")`)}},
		{"ping", Frame{Version: 1, SID: 0, Seq: 0, Kind: KindPing, Payload: nil}},
		{"pong", Frame{Version: 1, SID: 0, Seq: 0, Kind: KindPong, Payload: nil}},
		{"final", Frame{Version: 1, SID: 1, Seq: 999, Kind: KindDoc, Payload: []byte("done"), Final: true}},
		{"large seq", Frame{Version: 1, SID: 18446744073709551615, Seq: 18446744073709551615, Kind: KindDoc, Payload: []byte("x")}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.WriteFrame(&tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}

			r := NewReader(&buf)
			got, err := r.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}

			if got.Version != tc.frame.Version {
				t.Errorf("Version = %d, want %d", got.Version, tc.frame.Version)
			}
			if got.SID != tc.frame.SID {
				t.Errorf("SID = %d, want %d", got.SID, tc.frame.SID)
			}
			if got.Seq != tc.frame.Seq {
				t.Errorf("Seq = %d, want %d", got.Seq, tc.frame.Seq)
			}
			if got.Kind != tc.frame.Kind {
				t.Errorf("Kind = %v, want %v", got.Kind, tc.frame.Kind)
			}
			if !bytes.Equal(got.Payload, tc.frame.Payload) {
				t.Errorf("Payload = %q, want %q", got.Payload, tc.frame.Payload)
			}
			if got.Final != tc.frame.Final {
				t.Errorf("Final = %v, want %v", got.Final, tc.frame.Final)
			}
			if tc.frame.Base != nil {
				if got.Base == nil {
					t.Error("expected base to be present")
				} else if *got.Base != *tc.frame.Base {
					t.Error("base mismatch")
				}
			}
		})
	}
}

func TestRoundtrip_WithCRC(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriterWithCRC(&buf)

	original := &Frame{
		Version: 1,
		SID:     1,
		Seq:     5,
		Kind:    KindDoc,
		Payload: []byte("test payload with CRC"),
	}

	if err := w.WriteFrame(original); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	r := NewReader(&buf, WithCRCVerification())
	got, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	if got.CRC == nil {
		t.Error("expected CRC to be present")
	}
	if !bytes.Equal(got.Payload, original.Payload) {
		t.Errorf("Payload mismatch")
	}
}

// ============================================================
// CRC and Hash Tests
// ============================================================

func TestCRC_KnownValues(t *testing.T) {
	// Known CRC-32 IEEE test vectors
	testCases := []struct {
		input string
		crc   uint32
	}{
		{"", 0x00000000},
		{"a", 0xe8b7be43},
		{"abc", 0x352441c2},
		{"hello", 0x3610a686},
	}

	for _, tc := range testCases {
		got := ComputeCRC([]byte(tc.input))
		if got != tc.crc {
			t.Errorf("CRC(%q) = %08x, want %08x", tc.input, got, tc.crc)
		}
	}
}

func TestHash_RoundTrip(t *testing.T) {
	original := [32]byte{
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
	}

	hex := HashToHex(original)
	if len(hex) != 64 {
		t.Errorf("hex length = %d, want 64", len(hex))
	}

	parsed, ok := HexToHash(hex)
	if !ok {
		t.Fatal("HexToHash failed")
	}
	if parsed != original {
		t.Error("round-trip failed")
	}
}

// ============================================================
// Helpers
// ============================================================

func itoa(n int) string {
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

func crcToHex(crc uint32) string {
	const hextable = "0123456789abcdef"
	var buf [8]byte
	for i := 7; i >= 0; i-- {
		buf[i] = hextable[crc&0xf]
		crc >>= 4
	}
	return string(buf[:])
}
