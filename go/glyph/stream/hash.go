package stream

import (
	"crypto/sha256"

	"github.com/Neumenon/cowrie/go/glyph"
)

// StateHashLoose computes the state hash using CanonicalizeLoose.
// This is: sha256(CanonicalizeLoose(decoded value))
//
// Use this for schema-optional streaming where both sides operate
// in loose mode.
func StateHashLoose(value *glyph.GValue) [32]byte {
	canonical := glyph.CanonicalizeLoose(value)
	return sha256.Sum256([]byte(canonical))
}

// StateHashEmit computes the state hash using Emit (default emit).
// This is: sha256(Emit(decoded value))
//
// Use this when you need schema-aware output but don't have a schema
// for canonical packed encoding.
func StateHashEmit(value *glyph.GValue) [32]byte {
	emitted := glyph.Emit(value)
	return sha256.Sum256([]byte(emitted))
}

// StateHashBytes computes SHA-256 of raw bytes.
// Use this when you already have canonical bytes.
func StateHashBytes(data []byte) [32]byte {
	return sha256.Sum256(data)
}

// VerifyBase checks if the current state hash matches the expected base.
func VerifyBase(current, expected [32]byte) bool {
	return current == expected
}

// HashToHex converts a 32-byte hash to lowercase hex string.
func HashToHex(h [32]byte) string {
	const hextable = "0123456789abcdef"
	var buf [64]byte
	for i, b := range h {
		buf[i*2] = hextable[b>>4]
		buf[i*2+1] = hextable[b&0x0f]
	}
	return string(buf[:])
}

// HexToHash parses a 64-character hex string to a 32-byte hash.
func HexToHash(s string) ([32]byte, bool) {
	var h [32]byte
	if len(s) != 64 {
		return h, false
	}
	for i := 0; i < 32; i++ {
		hi := hexDigit(s[i*2])
		lo := hexDigit(s[i*2+1])
		if hi < 0 || lo < 0 {
			return h, false
		}
		h[i] = byte(hi<<4 | lo)
	}
	return h, true
}

func hexDigit(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10)
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10)
	default:
		return -1
	}
}
