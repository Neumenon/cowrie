package cowrie

import (
	"crypto/sha256"
	"encoding/hex"
)

// multihashSHA256Prefix is the multihash prefix for sha2-256 (code 0x12)
// with a 32-byte digest length (0x20), per SPEC-v1 §3.
var multihashSHA256Prefix = [2]byte{0x12, 0x20}

// AddressOfBytes computes the content address (§3) of already-canonical,
// uncompressed wire bytes: a multihash SHA-256 — the 2 prefix bytes
// 0x12 0x20 (sha2-256 code, 32-byte length) followed by the 32 raw
// SHA-256 digest bytes. The result is 34 raw bytes.
//
// The input MUST be the canonical, uncompressed wire bytes (exactly what
// Encode produces). Never pass compressed bytes or a decoded value.
func AddressOfBytes(canonical []byte) []byte {
	digest := sha256.Sum256(canonical)
	out := make([]byte, 0, 2+sha256.Size)
	out = append(out, multihashSHA256Prefix[0], multihashSHA256Prefix[1])
	out = append(out, digest[:]...)
	return out
}

// AddressOfBytesHex is AddressOfBytes rendered as lowercase hex (68 chars).
func AddressOfBytesHex(canonical []byte) string {
	return hex.EncodeToString(AddressOfBytes(canonical))
}

// ContentAddress computes the content address (§3) of a value: it encodes
// the value canonically, then returns AddressOfBytes of those bytes. This is
// the v1 per-value identity — equal canonical bytes ⇒ equal address, across
// every conforming implementation. Returns the 34 raw multihash bytes.
func ContentAddress(v *Value) ([]byte, error) {
	canonical, err := Encode(v)
	if err != nil {
		return nil, err
	}
	return AddressOfBytes(canonical), nil
}

// ContentAddressHex is ContentAddress rendered as lowercase hex (68 chars).
func ContentAddressHex(v *Value) (string, error) {
	addr, err := ContentAddress(v)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(addr), nil
}
