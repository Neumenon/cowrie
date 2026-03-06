package stream

import (
	"fmt"
	"sync"

	"github.com/Neumenon/cowrie/go/glyph"
)

// StreamCursor tracks per-SID state for stream processing.
// It maintains sequence numbers, state hashes, and provides
// helpers for patch verification and acknowledgement.
type StreamCursor struct {
	mu sync.RWMutex

	// Per-SID state
	cursors map[uint64]*SIDState
}

// SIDState holds state for a single stream ID.
type SIDState struct {
	SID       uint64
	LastSeq   uint64        // Last sequence number seen
	LastAcked uint64        // Last sequence number acknowledged
	StateHash [32]byte      // Current state hash (for patch verification)
	HasState  bool          // Whether StateHash is valid
	State     *glyph.GValue // Current state document (optional)
	Final     bool          // Whether stream has ended
}

// NewStreamCursor creates a new stream cursor.
func NewStreamCursor() *StreamCursor {
	return &StreamCursor{
		cursors: make(map[uint64]*SIDState),
	}
}

// Get returns the state for a SID, creating it if needed.
func (sc *StreamCursor) Get(sid uint64) *SIDState {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	state, ok := sc.cursors[sid]
	if !ok {
		state = &SIDState{SID: sid}
		sc.cursors[sid] = state
	}
	return state
}

// GetReadOnly returns the state for a SID without creating it.
func (sc *StreamCursor) GetReadOnly(sid uint64) *SIDState {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.cursors[sid]
}

// Delete removes state for a SID.
func (sc *StreamCursor) Delete(sid uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.cursors, sid)
}

// AllSIDs returns all tracked SIDs.
func (sc *StreamCursor) AllSIDs() []uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	sids := make([]uint64, 0, len(sc.cursors))
	for sid := range sc.cursors {
		sids = append(sids, sid)
	}
	return sids
}

// ProcessFrame processes a frame and updates cursor state.
// Returns an error if:
//   - Sequence number is not monotonic (gap or duplicate)
//   - Base hash mismatch for patch frames
//
// On success, updates LastSeq and returns nil.
func (sc *StreamCursor) ProcessFrame(frame *Frame) error {
	state := sc.Get(frame.SID)

	// Check sequence monotonicity
	if frame.Seq != 0 && frame.Seq <= state.LastSeq {
		return fmt.Errorf("sequence not monotonic: got %d, last was %d", frame.Seq, state.LastSeq)
	}

	// Check for gaps
	if state.LastSeq > 0 && frame.Seq != state.LastSeq+1 {
		return fmt.Errorf("sequence gap: expected %d, got %d", state.LastSeq+1, frame.Seq)
	}

	// For patches with base, verify state hash
	if frame.Kind == KindPatch && frame.Base != nil {
		if !state.HasState {
			return fmt.Errorf("cannot verify base: no state hash for SID %d", frame.SID)
		}
		if !VerifyBase(state.StateHash, *frame.Base) {
			return &BaseMismatchError{Expected: *frame.Base, Got: state.StateHash}
		}
	}

	// Update sequence
	state.LastSeq = frame.Seq

	// Update final flag
	if frame.IsFinal() {
		state.Final = true
	}

	return nil
}

// SetState sets the current state and computes its hash.
// Use this after applying a doc snapshot or patch.
func (sc *StreamCursor) SetState(sid uint64, value *glyph.GValue) {
	state := sc.Get(sid)
	state.State = value
	state.StateHash = StateHashLoose(value)
	state.HasState = true
}

// SetStateHash sets the state hash directly.
// Use this when you have pre-computed the hash.
func (sc *StreamCursor) SetStateHash(sid uint64, hash [32]byte) {
	state := sc.Get(sid)
	state.StateHash = hash
	state.HasState = true
}

// Ack marks a sequence as acknowledged.
func (sc *StreamCursor) Ack(sid, seq uint64) {
	state := sc.Get(sid)
	if seq > state.LastAcked {
		state.LastAcked = seq
	}
}

// PendingAcks returns sequences that have been seen but not acked.
func (sc *StreamCursor) PendingAcks(sid uint64) []uint64 {
	state := sc.GetReadOnly(sid)
	if state == nil {
		return nil
	}

	if state.LastSeq <= state.LastAcked {
		return nil
	}

	pending := make([]uint64, 0, state.LastSeq-state.LastAcked)
	for seq := state.LastAcked + 1; seq <= state.LastSeq; seq++ {
		pending = append(pending, seq)
	}
	return pending
}

// NeedsResync returns true if there's a gap or state mismatch.
func (sc *StreamCursor) NeedsResync(sid uint64) bool {
	state := sc.GetReadOnly(sid)
	if state == nil {
		return true
	}
	return !state.HasState
}

// ============================================================
// Frame Handler - functional processing helper
// ============================================================

// FrameHandler processes frames with state tracking.
type FrameHandler struct {
	Cursor *StreamCursor

	// Callbacks (optional)
	OnDoc   func(sid uint64, seq uint64, payload []byte, state *SIDState) error
	OnPatch func(sid uint64, seq uint64, payload []byte, state *SIDState) error
	OnRow   func(sid uint64, seq uint64, payload []byte, state *SIDState) error
	OnUI    func(sid uint64, seq uint64, payload []byte, state *SIDState) error
	OnAck   func(sid uint64, seq uint64, state *SIDState) error
	OnErr   func(sid uint64, seq uint64, payload []byte, state *SIDState) error
	OnFinal func(sid uint64, state *SIDState) error

	// Error handling
	OnSeqGap       func(sid uint64, expected, got uint64) error // Called on sequence gap
	OnBaseMismatch func(sid uint64, frame *Frame) error         // Called on base hash mismatch
}

// NewFrameHandler creates a handler with default cursor.
func NewFrameHandler() *FrameHandler {
	return &FrameHandler{
		Cursor: NewStreamCursor(),
	}
}

// Handle processes a frame and calls the appropriate callback.
func (h *FrameHandler) Handle(frame *Frame) error {
	state := h.Cursor.Get(frame.SID)

	// Check sequence
	if frame.Seq != 0 && state.LastSeq > 0 {
		if frame.Seq <= state.LastSeq {
			// Duplicate or out of order - skip
			return nil
		}
		if frame.Seq != state.LastSeq+1 {
			// Gap detected
			if h.OnSeqGap != nil {
				if err := h.OnSeqGap(frame.SID, state.LastSeq+1, frame.Seq); err != nil {
					return err
				}
			}
		}
	}

	// Check base for patches
	if frame.Kind == KindPatch && frame.Base != nil && state.HasState {
		if !VerifyBase(state.StateHash, *frame.Base) {
			if h.OnBaseMismatch != nil {
				return h.OnBaseMismatch(frame.SID, frame)
			}
			return &BaseMismatchError{Expected: *frame.Base, Got: state.StateHash}
		}
	}

	// Update sequence
	state.LastSeq = frame.Seq

	// Dispatch to callback
	var err error
	switch frame.Kind {
	case KindDoc:
		if h.OnDoc != nil {
			err = h.OnDoc(frame.SID, frame.Seq, frame.Payload, state)
		}
	case KindPatch:
		if h.OnPatch != nil {
			err = h.OnPatch(frame.SID, frame.Seq, frame.Payload, state)
		}
	case KindRow:
		if h.OnRow != nil {
			err = h.OnRow(frame.SID, frame.Seq, frame.Payload, state)
		}
	case KindUI:
		if h.OnUI != nil {
			err = h.OnUI(frame.SID, frame.Seq, frame.Payload, state)
		}
	case KindAck:
		if h.OnAck != nil {
			err = h.OnAck(frame.SID, frame.Seq, state)
		}
	case KindErr:
		if h.OnErr != nil {
			err = h.OnErr(frame.SID, frame.Seq, frame.Payload, state)
		}
	}

	if err != nil {
		return err
	}

	// Handle final
	if frame.IsFinal() {
		state.Final = true
		if h.OnFinal != nil {
			return h.OnFinal(frame.SID, state)
		}
	}

	return nil
}
