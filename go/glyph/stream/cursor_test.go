package stream

import (
	"testing"

	"github.com/Neumenon/cowrie/glyph"
)

func TestStreamCursor_Basic(t *testing.T) {
	cursor := NewStreamCursor()

	// Get creates state
	state := cursor.Get(1)
	if state == nil {
		t.Fatal("Get should create state")
	}
	if state.SID != 1 {
		t.Errorf("SID = %d, want 1", state.SID)
	}
	if state.LastSeq != 0 {
		t.Errorf("LastSeq = %d, want 0", state.LastSeq)
	}

	// GetReadOnly returns nil for unknown
	if cursor.GetReadOnly(99) != nil {
		t.Error("GetReadOnly should return nil for unknown SID")
	}

	// AllSIDs
	cursor.Get(2)
	cursor.Get(3)
	sids := cursor.AllSIDs()
	if len(sids) != 3 {
		t.Errorf("AllSIDs returned %d, want 3", len(sids))
	}

	// Delete
	cursor.Delete(2)
	if cursor.GetReadOnly(2) != nil {
		t.Error("Delete should remove SID")
	}
}

func TestStreamCursor_ProcessFrame(t *testing.T) {
	cursor := NewStreamCursor()

	// First frame
	err := cursor.ProcessFrame(&Frame{
		SID:     1,
		Seq:     1,
		Kind:    KindDoc,
		Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatalf("ProcessFrame failed: %v", err)
	}

	state := cursor.Get(1)
	if state.LastSeq != 1 {
		t.Errorf("LastSeq = %d, want 1", state.LastSeq)
	}

	// Sequential frame
	err = cursor.ProcessFrame(&Frame{SID: 1, Seq: 2, Kind: KindPatch})
	if err != nil {
		t.Fatalf("ProcessFrame failed: %v", err)
	}
	if state.LastSeq != 2 {
		t.Errorf("LastSeq = %d, want 2", state.LastSeq)
	}

	// Gap should fail
	err = cursor.ProcessFrame(&Frame{SID: 1, Seq: 5, Kind: KindPatch})
	if err == nil {
		t.Error("expected error for sequence gap")
	}

	// Duplicate should fail
	err = cursor.ProcessFrame(&Frame{SID: 1, Seq: 2, Kind: KindPatch})
	if err == nil {
		t.Error("expected error for duplicate sequence")
	}
}

func TestStreamCursor_PatchVerification(t *testing.T) {
	cursor := NewStreamCursor()

	// Set initial state
	doc := glyph.Map(
		glyph.MapEntry{Key: "x", Value: glyph.Int(1)},
	)
	cursor.SetState(1, doc)

	state := cursor.Get(1)
	if !state.HasState {
		t.Error("HasState should be true after SetState")
	}

	// Patch with correct base should succeed
	correctBase := state.StateHash
	err := cursor.ProcessFrame(&Frame{
		SID:     1,
		Seq:     1,
		Kind:    KindPatch,
		Payload: []byte("@patch\nset .x 2\n@end"),
		Base:    &correctBase,
	})
	if err != nil {
		t.Fatalf("ProcessFrame with correct base failed: %v", err)
	}

	// Update state after patch
	doc2 := glyph.Map(
		glyph.MapEntry{Key: "x", Value: glyph.Int(2)},
	)
	cursor.SetState(1, doc2)

	// Patch with wrong base should fail
	wrongBase := [32]byte{0xde, 0xad, 0xbe, 0xef}
	err = cursor.ProcessFrame(&Frame{
		SID:     1,
		Seq:     2,
		Kind:    KindPatch,
		Payload: []byte("@patch\nset .x 3\n@end"),
		Base:    &wrongBase,
	})
	if err == nil {
		t.Error("expected error for wrong base hash")
	}
	if _, ok := err.(*BaseMismatchError); !ok {
		t.Errorf("expected BaseMismatchError, got %T", err)
	}
}

func TestStreamCursor_Ack(t *testing.T) {
	cursor := NewStreamCursor()

	// Process some frames
	for seq := uint64(1); seq <= 5; seq++ {
		cursor.ProcessFrame(&Frame{SID: 1, Seq: seq, Kind: KindDoc, Payload: []byte("{}")})
	}

	// No acks yet
	pending := cursor.PendingAcks(1)
	if len(pending) != 5 {
		t.Errorf("PendingAcks = %d, want 5", len(pending))
	}

	// Ack some
	cursor.Ack(1, 3)

	pending = cursor.PendingAcks(1)
	if len(pending) != 2 { // 4, 5
		t.Errorf("PendingAcks after ack = %d, want 2", len(pending))
	}

	// Ack rest
	cursor.Ack(1, 5)
	pending = cursor.PendingAcks(1)
	if len(pending) != 0 {
		t.Errorf("PendingAcks after full ack = %d, want 0", len(pending))
	}
}

func TestStreamCursor_Final(t *testing.T) {
	cursor := NewStreamCursor()

	cursor.ProcessFrame(&Frame{
		SID:     1,
		Seq:     1,
		Kind:    KindDoc,
		Payload: []byte("done"),
		Final:   true,
	})

	state := cursor.Get(1)
	if !state.Final {
		t.Error("Final should be true")
	}
}

func TestFrameHandler_Basic(t *testing.T) {
	handler := NewFrameHandler()

	var docs []string
	var patches []string
	var uiEvents []string

	handler.OnDoc = func(sid, seq uint64, payload []byte, state *SIDState) error {
		docs = append(docs, string(payload))
		return nil
	}
	handler.OnPatch = func(sid, seq uint64, payload []byte, state *SIDState) error {
		patches = append(patches, string(payload))
		return nil
	}
	handler.OnUI = func(sid, seq uint64, payload []byte, state *SIDState) error {
		uiEvents = append(uiEvents, string(payload))
		return nil
	}

	// Send frames
	handler.Handle(&Frame{SID: 1, Seq: 1, Kind: KindDoc, Payload: []byte(`{x=1}`)})
	handler.Handle(&Frame{SID: 1, Seq: 2, Kind: KindPatch, Payload: []byte(`set .x 2`)})
	handler.Handle(&Frame{SID: 1, Seq: 3, Kind: KindUI, Payload: []byte(`progress 50%`)})
	handler.Handle(&Frame{SID: 1, Seq: 4, Kind: KindPatch, Payload: []byte(`set .x 3`)})

	if len(docs) != 1 || docs[0] != "{x=1}" {
		t.Errorf("docs = %v", docs)
	}
	if len(patches) != 2 {
		t.Errorf("patches = %v", patches)
	}
	if len(uiEvents) != 1 || uiEvents[0] != "progress 50%" {
		t.Errorf("uiEvents = %v", uiEvents)
	}
}

func TestFrameHandler_GapCallback(t *testing.T) {
	handler := NewFrameHandler()

	var gaps [][2]uint64
	handler.OnSeqGap = func(sid uint64, expected, got uint64) error {
		gaps = append(gaps, [2]uint64{expected, got})
		return nil // Allow gap
	}

	handler.Handle(&Frame{SID: 1, Seq: 1, Kind: KindDoc, Payload: []byte("a")})
	handler.Handle(&Frame{SID: 1, Seq: 5, Kind: KindDoc, Payload: []byte("b")}) // Gap!

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d", len(gaps))
	}
	if gaps[0][0] != 2 || gaps[0][1] != 5 {
		t.Errorf("gap = %v, want [2,5]", gaps[0])
	}

	// State should still update if callback allows
	state := handler.Cursor.Get(1)
	if state.LastSeq != 5 {
		t.Errorf("LastSeq = %d, want 5", state.LastSeq)
	}
}

func TestFrameHandler_FinalCallback(t *testing.T) {
	handler := NewFrameHandler()

	var finalSIDs []uint64
	handler.OnFinal = func(sid uint64, state *SIDState) error {
		finalSIDs = append(finalSIDs, sid)
		return nil
	}

	handler.Handle(&Frame{SID: 1, Seq: 1, Kind: KindDoc, Payload: []byte("a")})
	handler.Handle(&Frame{SID: 1, Seq: 2, Kind: KindDoc, Payload: []byte("done"), Final: true})

	if len(finalSIDs) != 1 || finalSIDs[0] != 1 {
		t.Errorf("finalSIDs = %v", finalSIDs)
	}
}

func TestFrameHandler_DuplicateSkipped(t *testing.T) {
	handler := NewFrameHandler()

	var count int
	handler.OnDoc = func(sid, seq uint64, payload []byte, state *SIDState) error {
		count++
		return nil
	}

	handler.Handle(&Frame{SID: 1, Seq: 1, Kind: KindDoc, Payload: []byte("a")})
	handler.Handle(&Frame{SID: 1, Seq: 1, Kind: KindDoc, Payload: []byte("b")}) // Duplicate
	handler.Handle(&Frame{SID: 1, Seq: 2, Kind: KindDoc, Payload: []byte("c")})

	if count != 2 {
		t.Errorf("count = %d, want 2 (duplicate should be skipped)", count)
	}
}
