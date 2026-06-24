package cowrie

// Cowrie file / frame container and Merkle file identity (SPEC-v1 §7).
//
// A Cowrie file packages an ordered list of frames (each frame is one complete,
// canonical §2.2 COWR value's wire bytes) with a sealed footer index for O(1)
// random access, and a Merkle root over the frames that is the file's content
// identity.
//
// Layout (all multi-byte ints little-endian; uvarint = §2.1):
//
//	"CWRF" (0x43 0x57 0x52 0x46)        file magic
//	version  u8 (0x01)
//	reserved u8 (0x00)
//	uvarint  frame_count
//	repeat frame_count times:
//	    uvarint frame_len
//	    frame_bytes                      (a canonical COWR value, §2.2; uncompressed)
//	FOOTER (starts at footer_offset):
//	    uvarint frame_count              (repeated, footer is self-contained)
//	    repeat: uvarint frame_offset, uvarint frame_len   (offset = file-start to frame_bytes)
//	    merkle_root  34 bytes            (multihash SHA-256, §3)
//	u64 LE   footer_offset               (file-start to FOOTER; enables seek-from-end)
//	"CWRF"                               end magic
//
// Merkle (RFC 6962 domain separation, promote-odd — never duplicate; the count is
// bound into the root so two different frame lists can never share a root):
//
//	leaf(f)    = SHA-256(0x00 || f)
//	node(a, b) = SHA-256(0x01 || a || b)
//	tree_digest = promote-odd reduction of the leaves   (empty file: SHA-256(b""))
//	root_digest = SHA-256(0x02 || uvarint(frame_count) || tree_digest)
//	merkle_root = 0x12 0x20 || root_digest              (multihash sha2-256)

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

var (
	fileMagic = []byte{0x43, 0x57, 0x52, 0x46} // "CWRF"
)

const (
	fileVersion = 1
	// maxFrames is the §7 frame-count bound (DoS guard).
	maxFrames = uint64(1) << 32
)

// putUvarint appends the minimal LEB128 unsigned varint of n to dst.
func putUvarint(dst []byte, n uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	w := binary.PutUvarint(buf[:], n)
	return append(dst, buf[:w]...)
}

// ---------------------------------------------------------------- Merkle -----

func merkleLeaf(frame []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(frame)
	return h.Sum(nil)
}

func merkleNode(a, b []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x01})
	h.Write(a)
	h.Write(b)
	return h.Sum(nil)
}

func treeDigest(frames [][]byte) []byte {
	if len(frames) == 0 {
		d := sha256.Sum256(nil)
		return d[:]
	}
	level := make([][]byte, len(frames))
	for i, f := range frames {
		level[i] = merkleLeaf(f)
	}
	for len(level) > 1 {
		var nxt [][]byte
		for i := 0; i+1 < len(level); i += 2 {
			nxt = append(nxt, merkleNode(level[i], level[i+1]))
		}
		if len(level)%2 == 1 {
			// promote the odd node unchanged (RFC 6962, never duplicate)
			nxt = append(nxt, level[len(level)-1])
		}
		level = nxt
	}
	return level[0]
}

// MerkleRoot computes the multihash SHA-256 Merkle root over the ordered frame
// byte-blobs (§7), with the frame count bound into the root (0x02 domain) so
// distinct frame lists can never share a root. Returns the 34 raw multihash bytes.
func MerkleRoot(frames [][]byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x02})
	h.Write(putUvarint(nil, uint64(len(frames))))
	h.Write(treeDigest(frames))
	root := h.Sum(nil)
	out := make([]byte, 0, 2+sha256.Size)
	out = append(out, 0x12, 0x20)
	out = append(out, root...)
	return out
}

// ---------------------------------------------------------------- encode -----

// EncodeFile builds a Cowrie file from a list of values (each encoded as one
// canonical frame), per SPEC-v1 §7.
func EncodeFile(values []*Value) ([]byte, error) {
	frames := make([][]byte, len(values))
	for i, v := range values {
		f, err := Encode(v)
		if err != nil {
			return nil, err
		}
		frames[i] = f
	}
	return encodeFileFromFrames(frames), nil
}

// encodeFileFromFrames serializes already-encoded frame byte-blobs into the §7
// container. This is the re-encode path used by --file-recode.
func encodeFileFromFrames(frames [][]byte) []byte {
	out := make([]byte, 0, 64)
	out = append(out, fileMagic...)
	out = append(out, fileVersion)
	out = append(out, 0x00) // reserved
	out = putUvarint(out, uint64(len(frames)))

	offsets := make([]int, len(frames))
	for i, f := range frames {
		out = putUvarint(out, uint64(len(f)))
		// §7: each frame's bytes begin at a 64-byte FILE offset so in-frame
		// tensors stay 64B-aligned in the file. Pad = (-offset) mod 64 zero bytes
		// after frame_len; the footer offset points to the aligned frame start.
		pad := (-len(out)) & (TensorAlign - 1)
		for j := 0; j < pad; j++ {
			out = append(out, 0)
		}
		offsets[i] = len(out) // offset to frame_bytes (64B-aligned)
		out = append(out, f...)
	}
	footerOffset := len(out)

	// footer
	out = putUvarint(out, uint64(len(frames)))
	for i, f := range frames {
		out = putUvarint(out, uint64(offsets[i]))
		out = putUvarint(out, uint64(len(f)))
	}
	out = append(out, MerkleRoot(frames)...)

	var fo [8]byte
	binary.LittleEndian.PutUint64(fo[:], uint64(footerOffset))
	out = append(out, fo[:]...)
	out = append(out, fileMagic...)
	return out
}

// ---------------------------------------------------------------- decode -----

// errCanonical signals a non-canonical or invalid Cowrie file.
var errCanonical = errors.New("cowrie file: non-canonical or invalid")

// DecodeFile returns the ordered frame byte-blobs, fully validated (§7). The BODY
// is the source of truth: frames are read sequentially via their length prefixes,
// and the footer MUST exactly mirror that layout (count, contiguous offsets/lengths)
// — so there is exactly one canonical file byte-string per ordered value list. The
// stored Merkle root must also match.
func DecodeFile(data []byte) ([][]byte, error) {
	// minimum: magic(4)+ver(1)+reserved(1)+count(1)+footer_offset(8)+magic(4)
	if len(data) < 4+1+1+1+8+4 {
		return nil, errors.New("cowrie file: too short")
	}
	if !bytesEqual(data[:4], fileMagic) {
		return nil, errors.New("cowrie file: bad file magic")
	}
	if !bytesEqual(data[len(data)-4:], fileMagic) {
		return nil, errors.New("cowrie file: bad end magic")
	}
	if data[4] != fileVersion {
		return nil, errors.New("cowrie file: bad file version")
	}
	if data[5] != 0x00 {
		return nil, errors.New("cowrie file: reserved byte must be 0")
	}
	footerOffset := binary.LittleEndian.Uint64(data[len(data)-12 : len(data)-4])
	if !(footerOffset > 6 && footerOffset <= uint64(len(data))-12) {
		return nil, errors.New("cowrie file: footer offset out of range")
	}

	// --- body: read frames sequentially (the source of truth) ---
	pos := 6
	n, adv, err := readUvarint(data, pos)
	if err != nil {
		return nil, err
	}
	pos = adv
	if n > maxFrames {
		return nil, errors.New("cowrie file: frame count exceeds limit")
	}
	frames := make([][]byte, 0, n)
	type span struct{ off, flen int }
	layout := make([]span, 0, n)
	for i := uint64(0); i < n; i++ {
		flen, adv2, err := readUvarint(data, pos)
		if err != nil {
			return nil, err
		}
		pos = adv2
		// §7: frame bytes are 64-byte aligned in the file. Consume pad =
		// (-pos) mod 64 zero bytes after frame_len; reject non-zero padding.
		pad := (-pos) & (TensorAlign - 1)
		if pos+pad > len(data) {
			return nil, errors.New("cowrie file: truncated frame alignment padding")
		}
		for j := 0; j < pad; j++ {
			if data[pos+j] != 0 {
				return nil, errors.New("cowrie file: frame alignment padding not zero")
			}
		}
		pos += pad
		if uint64(pos)+flen > footerOffset {
			return nil, errors.New("cowrie file: frame extends past footer")
		}
		layout = append(layout, span{off: pos, flen: int(flen)})
		frames = append(frames, data[pos:pos+int(flen)])
		pos += int(flen)
	}
	if uint64(pos) != footerOffset {
		return nil, errors.New("cowrie file: body does not end exactly at the footer")
	}

	// --- footer: must mirror the body layout exactly (canonical) ---
	fcount, adv3, err := readUvarint(data, pos)
	if err != nil {
		return nil, err
	}
	pos = adv3
	if fcount != n {
		return nil, errors.New("cowrie file: footer frame count != header")
	}
	for _, s := range layout {
		foff, advA, err := readUvarint(data, pos)
		if err != nil {
			return nil, err
		}
		pos = advA
		flen2, advB, err := readUvarint(data, pos)
		if err != nil {
			return nil, err
		}
		pos = advB
		if foff != uint64(s.off) || flen2 != uint64(s.flen) {
			return nil, errors.New("cowrie file: footer index does not mirror body layout")
		}
	}
	if pos+34 > len(data) {
		return nil, errors.New("cowrie file: truncated footer root")
	}
	storedRoot := data[pos : pos+34]
	pos += 34
	if pos != len(data)-12 {
		return nil, errors.New("cowrie file: footer length not canonical")
	}
	if !bytesEqual(MerkleRoot(frames), storedRoot) {
		return nil, errors.New("cowrie file: merkle root mismatch (tampered or non-canonical)")
	}
	return frames, nil
}

// FileIdentity returns the file's content identity = its verified Merkle root
// (34-byte multihash).
func FileIdentity(data []byte) ([]byte, error) {
	frames, err := DecodeFile(data)
	if err != nil {
		return nil, err
	}
	return MerkleRoot(frames), nil
}

// FileIdentityHex is FileIdentity rendered as lowercase hex (68 chars).
func FileIdentityHex(data []byte) (string, error) {
	id, err := FileIdentity(data)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(id), nil
}

// RecodeFile decodes a canonical Cowrie file and re-encodes it from its frames,
// reproducing the input byte-for-byte for canonical files.
func RecodeFile(data []byte) ([]byte, error) {
	frames, err := DecodeFile(data)
	if err != nil {
		return nil, err
	}
	return encodeFileFromFrames(frames), nil
}

// ---------------------------------------------------------------- helpers ----

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// readUvarint reads a strict (minimal, ≤10-byte) LEB128 unsigned varint from data
// at pos, returning (value, newPos). Rejects truncation, over-length, non-minimal.
func readUvarint(data []byte, pos int) (uint64, int, error) {
	var result uint64
	var shift uint
	start := pos
	for {
		if pos-start >= 10 {
			return 0, pos, errors.New("cowrie file: varint too long")
		}
		if pos >= len(data) {
			return 0, pos, errors.New("cowrie file: truncated varint")
		}
		b := data[pos]
		pos++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			if b == 0 && pos-start > 1 {
				return 0, pos, errors.New("cowrie file: overlong varint")
			}
			return result, pos, nil
		}
		shift += 7
	}
}
