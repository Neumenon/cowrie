// Package stream implements GS1 (GLYPH Stream v1) framing protocol.
//
// GS1 is a transport envelope for GLYPH payloads, providing:
//   - Message boundaries and resync
//   - Multiplexing via stream IDs (sid)
//   - Ordering via sequence numbers (seq)
//   - Integrity via optional CRC-32
//   - Patch safety via optional state hash (base)
//
// GS1 headers are NOT part of GLYPH canonicalization.
// The payload is standard GLYPH text passed to existing parsers unchanged.
package stream

import (
	"fmt"
)

// Version is the GS1 protocol version.
const Version uint8 = 1

// FrameKind indicates the semantic category of a frame's payload.
type FrameKind uint8

const (
	KindDoc   FrameKind = 0 // Snapshot or general GLYPH document
	KindPatch FrameKind = 1 // GLYPH patch doc (@patch ... @end)
	KindRow   FrameKind = 2 // Single row value (streaming tabular)
	KindUI    FrameKind = 3 // UI event (progress/log/artifact)
	KindAck   FrameKind = 4 // Acknowledgement
	KindErr   FrameKind = 5 // Error event
	KindPing  FrameKind = 6 // Keepalive
	KindPong  FrameKind = 7 // Ping response
)

// String returns the kind name.
func (k FrameKind) String() string {
	switch k {
	case KindDoc:
		return "doc"
	case KindPatch:
		return "patch"
	case KindRow:
		return "row"
	case KindUI:
		return "ui"
	case KindAck:
		return "ack"
	case KindErr:
		return "err"
	case KindPing:
		return "ping"
	case KindPong:
		return "pong"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// ParseKind parses a kind string or numeric value.
func ParseKind(s string) (FrameKind, bool) {
	switch s {
	case "doc", "0":
		return KindDoc, true
	case "patch", "1":
		return KindPatch, true
	case "row", "2":
		return KindRow, true
	case "ui", "3":
		return KindUI, true
	case "ack", "4":
		return KindAck, true
	case "err", "5":
		return KindErr, true
	case "ping", "6":
		return KindPing, true
	case "pong", "7":
		return KindPong, true
	default:
		// Try to parse as number
		var n int
		if _, err := fmt.Sscanf(s, "%d", &n); err == nil && n >= 0 && n <= 255 {
			return FrameKind(n), true
		}
		return 0, false
	}
}

// Flags for GS1 frames.
type Flags uint8

const (
	FlagHasCRC     Flags = 0x01 // CRC-32 is present
	FlagHasBase    Flags = 0x02 // Base hash is present
	FlagFinal      Flags = 0x04 // End-of-stream for this SID
	FlagCompressed Flags = 0x08 // Payload is compressed (reserved for GS1.1)
)

// Frame represents a single GS1 frame.
type Frame struct {
	// Required fields
	Version uint8     // Protocol version (must be 1)
	SID     uint64    // Stream identifier
	Seq     uint64    // Sequence number (per-SID, monotonic)
	Kind    FrameKind // Frame kind
	Payload []byte    // GLYPH payload bytes (UTF-8)

	// Optional fields
	CRC   *uint32   // CRC-32 of payload (nil if not present)
	Base  *[32]byte // SHA-256 state hash (nil if not present)
	Flags Flags     // Flag bits
	Final bool      // End-of-stream marker
}

// HasCRC returns true if CRC is present.
func (f *Frame) HasCRC() bool {
	return f.CRC != nil
}

// HasBase returns true if base hash is present.
func (f *Frame) HasBase() bool {
	return f.Base != nil
}

// IsFinal returns true if this is the final frame for this SID.
func (f *Frame) IsFinal() bool {
	return f.Final || f.Flags&FlagFinal != 0
}

// MaxPayloadSize is the default maximum payload size (64 MiB).
const MaxPayloadSize = 64 * 1024 * 1024

// Error types for GS1 parsing.
type ParseError struct {
	Reason string
	Offset int
}

func (e *ParseError) Error() string {
	if e.Offset >= 0 {
		return fmt.Sprintf("gs1: %s at offset %d", e.Reason, e.Offset)
	}
	return fmt.Sprintf("gs1: %s", e.Reason)
}

// CRCMismatchError is returned when CRC verification fails.
type CRCMismatchError struct {
	Expected uint32
	Got      uint32
}

func (e *CRCMismatchError) Error() string {
	return fmt.Sprintf("gs1: CRC mismatch: expected %08x, got %08x", e.Expected, e.Got)
}

// BaseMismatchError is returned when base hash verification fails.
type BaseMismatchError struct {
	Expected [32]byte
	Got      [32]byte
}

func (e *BaseMismatchError) Error() string {
	return fmt.Sprintf("gs1: base hash mismatch")
}
