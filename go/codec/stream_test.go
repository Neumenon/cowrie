package codec_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/Neumenon/cowrie/go/codec"
)

func TestStreamWriterReader_Roundtrip(t *testing.T) {
	type Record struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	var buf bytes.Buffer
	sw := codec.NewStreamWriter(&buf)

	// Write records
	records := []Record{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
	}
	for _, r := range records {
		if err := sw.Write(r); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Sync
	if err := sw.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Read back
	sr := codec.NewStreamReader(buf.Bytes())
	for i := 0; i < len(records); i++ {
		var got Record
		if err := sr.Next(&got); err != nil {
			t.Fatalf("Next(%d) failed: %v", i, err)
		}
		if got.Name != records[i].Name || got.Value != records[i].Value {
			t.Errorf("record %d: got %v, want %v", i, got, records[i])
		}
	}

	// Next read should return EOF
	var dummy Record
	if err := sr.Next(&dummy); err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestStreamReader_Position(t *testing.T) {
	var buf bytes.Buffer
	sw := codec.NewStreamWriter(&buf)
	_ = sw.Write(map[string]any{"x": int64(1)})
	_ = sw.Write(map[string]any{"y": int64(2)})

	sr := codec.NewStreamReader(buf.Bytes())
	if sr.Position() != 0 {
		t.Errorf("initial position should be 0, got %d", sr.Position())
	}
	if sr.Remaining() != buf.Len() {
		t.Errorf("initial remaining should be %d, got %d", buf.Len(), sr.Remaining())
	}

	var dummy map[string]any
	_ = sr.Next(&dummy)
	if sr.Position() == 0 {
		t.Error("position should advance after reading")
	}
	if sr.Remaining() >= buf.Len() {
		t.Error("remaining should decrease after reading")
	}
}

func TestStreamReader_TruncatedLength(t *testing.T) {
	// Only 2 bytes, not enough for a u32 length prefix
	sr := codec.NewStreamReader([]byte{0x01, 0x02})
	var dummy map[string]any
	err := sr.Next(&dummy)
	if err == nil {
		t.Error("expected error for truncated length prefix")
	}
}

func TestStreamReader_TruncatedFrame(t *testing.T) {
	// Valid length prefix (10 bytes) but not enough data
	data := make([]byte, 4)
	data[0] = 10 // frame length = 10
	sr := codec.NewStreamReader(data)
	var dummy map[string]any
	err := sr.Next(&dummy)
	if err == nil {
		t.Error("expected error for truncated frame data")
	}
}

func TestStreamReader_ZeroLengthEOF(t *testing.T) {
	// Zero length marker means EOF
	data := make([]byte, 4) // all zeros
	sr := codec.NewStreamReader(data)
	var dummy map[string]any
	err := sr.Next(&dummy)
	if err != io.EOF {
		t.Errorf("expected io.EOF for zero length marker, got %v", err)
	}
}

func TestReadAllStream(t *testing.T) {
	type Item struct {
		ID int `json:"id"`
	}

	var buf bytes.Buffer
	sw := codec.NewStreamWriter(&buf)
	for i := 0; i < 5; i++ {
		_ = sw.Write(Item{ID: i})
	}

	items, err := codec.ReadAllStream[Item](buf.Bytes())
	if err != nil {
		t.Fatalf("ReadAllStream failed: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	for i, item := range items {
		if item.ID != i {
			t.Errorf("item %d: got ID %d", i, item.ID)
		}
	}
}

func TestStreamWriter_SyncNonSyncer(t *testing.T) {
	// bytes.Buffer doesn't implement Sync(), so Sync should be a no-op
	var buf bytes.Buffer
	sw := codec.NewStreamWriter(&buf)
	if err := sw.Sync(); err != nil {
		t.Errorf("Sync on non-syncer should not error, got %v", err)
	}
}
