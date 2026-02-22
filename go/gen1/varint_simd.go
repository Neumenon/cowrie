// Package gen1 - SIMD Group Varint encoding for faster integer array processing.
//
// Group VByte encodes 4 integers at a time with a shared control byte,
// reducing branch mispredictions and enabling SIMD-friendly decoding.
//
// Wire format:
//   [CTRL: 1 byte] [INT0: 1-4B] [INT1: 1-4B] [INT2: 1-4B] [INT3: 1-4B]
//
// Control byte encodes byte counts: 2 bits per integer
//   00 = 1 byte, 01 = 2 bytes, 10 = 3 bytes, 11 = 4 bytes
package gen1

import (
	"encoding/binary"
)

// New tag for Group VByte encoded int32 arrays
const tagArrayInt32GroupVB byte = 0x0F

// byteCountTable maps control byte to total data bytes (precomputed)
var byteCountTable [256]byte

// bytesPerSlot maps 2-bit code to byte count
var bytesPerSlot = [4]byte{1, 2, 3, 4}

func init() {
	// Precompute byte counts for all 256 possible control bytes
	for ctrl := 0; ctrl < 256; ctrl++ {
		total := byte(0)
		for i := 0; i < 4; i++ {
			code := (ctrl >> (i * 2)) & 0x03
			total += bytesPerSlot[code]
		}
		byteCountTable[ctrl] = total
	}
}

// GroupVByteEncode encodes 4 uint32s using group varint.
// Returns number of bytes written (1 control + 4-16 data bytes).
// dst must have at least 17 bytes capacity.
func GroupVByteEncode(dst []byte, v0, v1, v2, v3 uint32) int {
	ctrl := byte(0)
	pos := 1 // Start after control byte

	// Encode v0
	switch {
	case v0 < 1<<8:
		dst[pos] = byte(v0)
		pos++
		// ctrl |= 0 << 0 // 00 = 1 byte (no-op)
	case v0 < 1<<16:
		binary.LittleEndian.PutUint16(dst[pos:], uint16(v0))
		pos += 2
		ctrl |= 1 << 0 // 01 = 2 bytes
	case v0 < 1<<24:
		dst[pos] = byte(v0)
		dst[pos+1] = byte(v0 >> 8)
		dst[pos+2] = byte(v0 >> 16)
		pos += 3
		ctrl |= 2 << 0 // 10 = 3 bytes
	default:
		binary.LittleEndian.PutUint32(dst[pos:], v0)
		pos += 4
		ctrl |= 3 << 0 // 11 = 4 bytes
	}

	// Encode v1
	switch {
	case v1 < 1<<8:
		dst[pos] = byte(v1)
		pos++
	case v1 < 1<<16:
		binary.LittleEndian.PutUint16(dst[pos:], uint16(v1))
		pos += 2
		ctrl |= 1 << 2
	case v1 < 1<<24:
		dst[pos] = byte(v1)
		dst[pos+1] = byte(v1 >> 8)
		dst[pos+2] = byte(v1 >> 16)
		pos += 3
		ctrl |= 2 << 2
	default:
		binary.LittleEndian.PutUint32(dst[pos:], v1)
		pos += 4
		ctrl |= 3 << 2
	}

	// Encode v2
	switch {
	case v2 < 1<<8:
		dst[pos] = byte(v2)
		pos++
	case v2 < 1<<16:
		binary.LittleEndian.PutUint16(dst[pos:], uint16(v2))
		pos += 2
		ctrl |= 1 << 4
	case v2 < 1<<24:
		dst[pos] = byte(v2)
		dst[pos+1] = byte(v2 >> 8)
		dst[pos+2] = byte(v2 >> 16)
		pos += 3
		ctrl |= 2 << 4
	default:
		binary.LittleEndian.PutUint32(dst[pos:], v2)
		pos += 4
		ctrl |= 3 << 4
	}

	// Encode v3
	switch {
	case v3 < 1<<8:
		dst[pos] = byte(v3)
		pos++
	case v3 < 1<<16:
		binary.LittleEndian.PutUint16(dst[pos:], uint16(v3))
		pos += 2
		ctrl |= 1 << 6
	case v3 < 1<<24:
		dst[pos] = byte(v3)
		dst[pos+1] = byte(v3 >> 8)
		dst[pos+2] = byte(v3 >> 16)
		pos += 3
		ctrl |= 2 << 6
	default:
		binary.LittleEndian.PutUint32(dst[pos:], v3)
		pos += 4
		ctrl |= 3 << 6
	}

	dst[0] = ctrl
	return pos
}

// GroupVByteDecode decodes 4 uint32s from group varint format.
// Returns the 4 values and number of bytes consumed.
func GroupVByteDecode(src []byte) (v0, v1, v2, v3 uint32, n int) {
	if len(src) < 5 { // Minimum: 1 ctrl + 4 data bytes
		return 0, 0, 0, 0, 0
	}

	ctrl := src[0]
	pos := 1

	// Decode v0
	switch ctrl & 0x03 {
	case 0: // 1 byte
		v0 = uint32(src[pos])
		pos++
	case 1: // 2 bytes
		v0 = uint32(binary.LittleEndian.Uint16(src[pos:]))
		pos += 2
	case 2: // 3 bytes
		v0 = uint32(src[pos]) | uint32(src[pos+1])<<8 | uint32(src[pos+2])<<16
		pos += 3
	case 3: // 4 bytes
		v0 = binary.LittleEndian.Uint32(src[pos:])
		pos += 4
	}

	// Decode v1
	switch (ctrl >> 2) & 0x03 {
	case 0:
		v1 = uint32(src[pos])
		pos++
	case 1:
		v1 = uint32(binary.LittleEndian.Uint16(src[pos:]))
		pos += 2
	case 2:
		v1 = uint32(src[pos]) | uint32(src[pos+1])<<8 | uint32(src[pos+2])<<16
		pos += 3
	case 3:
		v1 = binary.LittleEndian.Uint32(src[pos:])
		pos += 4
	}

	// Decode v2
	switch (ctrl >> 4) & 0x03 {
	case 0:
		v2 = uint32(src[pos])
		pos++
	case 1:
		v2 = uint32(binary.LittleEndian.Uint16(src[pos:]))
		pos += 2
	case 2:
		v2 = uint32(src[pos]) | uint32(src[pos+1])<<8 | uint32(src[pos+2])<<16
		pos += 3
	case 3:
		v2 = binary.LittleEndian.Uint32(src[pos:])
		pos += 4
	}

	// Decode v3
	switch (ctrl >> 6) & 0x03 {
	case 0:
		v3 = uint32(src[pos])
		pos++
	case 1:
		v3 = uint32(binary.LittleEndian.Uint16(src[pos:]))
		pos += 2
	case 2:
		v3 = uint32(src[pos]) | uint32(src[pos+1])<<8 | uint32(src[pos+2])<<16
		pos += 3
	case 3:
		v3 = binary.LittleEndian.Uint32(src[pos:])
		pos += 4
	}

	return v0, v1, v2, v3, pos
}

// EncodeInt32ArrayGroupVB encodes an int32 slice using group varint.
// Returns the encoded bytes with tag prefix.
func EncodeInt32ArrayGroupVB(values []int32) []byte {
	if len(values) == 0 {
		// Empty array: tag + count(0)
		return []byte{tagArrayInt32GroupVB, 0}
	}

	// Estimate output size: tag + count + data
	// Worst case: 4 bytes per value + 1 control byte per 4 values
	maxSize := 1 + 10 + len(values)*4 + (len(values)+3)/4
	buf := make([]byte, 0, maxSize)

	// Tag
	buf = append(buf, tagArrayInt32GroupVB)

	// Count (uvarint)
	buf = appendUvarint(buf, uint64(len(values)))

	// Encode in groups of 4
	tmp := make([]byte, 17) // Max size for one group
	fullGroups := len(values) / 4
	remainder := len(values) % 4

	for i := 0; i < fullGroups; i++ {
		idx := i * 4
		// Convert int32 to uint32 using zigzag encoding for signed values
		v0 := zigzagEncode32(values[idx])
		v1 := zigzagEncode32(values[idx+1])
		v2 := zigzagEncode32(values[idx+2])
		v3 := zigzagEncode32(values[idx+3])
		n := GroupVByteEncode(tmp, v0, v1, v2, v3)
		buf = append(buf, tmp[:n]...)
	}

	// Handle remainder (pad with zeros)
	if remainder > 0 {
		var v0, v1, v2, v3 uint32
		idx := fullGroups * 4
		if remainder >= 1 {
			v0 = zigzagEncode32(values[idx])
		}
		if remainder >= 2 {
			v1 = zigzagEncode32(values[idx+1])
		}
		if remainder >= 3 {
			v2 = zigzagEncode32(values[idx+2])
		}
		n := GroupVByteEncode(tmp, v0, v1, v2, v3)
		buf = append(buf, tmp[:n]...)
	}

	return buf
}

// DecodeInt32ArrayGroupVB decodes a group varint encoded int32 array.
// Input should start after the tag byte.
func DecodeInt32ArrayGroupVB(data []byte) ([]int32, int, error) {
	if len(data) == 0 {
		return nil, 0, nil
	}

	// Read count
	count, n, err := readUvarint(data, 0)
	if err != nil {
		return nil, 0, err
	}
	off := n

	if count == 0 {
		return []int32{}, off, nil
	}

	result := make([]int32, count)
	fullGroups := int(count) / 4
	remainder := int(count) % 4

	idx := 0
	for i := 0; i < fullGroups; i++ {
		v0, v1, v2, v3, consumed := GroupVByteDecode(data[off:])
		if consumed == 0 {
			return nil, 0, ErrMaxArrayLen
		}
		result[idx] = zigzagDecode32(v0)
		result[idx+1] = zigzagDecode32(v1)
		result[idx+2] = zigzagDecode32(v2)
		result[idx+3] = zigzagDecode32(v3)
		idx += 4
		off += consumed
	}

	// Handle remainder
	if remainder > 0 {
		v0, v1, v2, _, consumed := GroupVByteDecode(data[off:])
		if consumed == 0 {
			return nil, 0, ErrMaxArrayLen
		}
		if remainder >= 1 {
			result[idx] = zigzagDecode32(v0)
		}
		if remainder >= 2 {
			result[idx+1] = zigzagDecode32(v1)
		}
		if remainder >= 3 {
			result[idx+2] = zigzagDecode32(v2)
		}
		off += consumed
	}

	return result, off, nil
}

// zigzagEncode32 encodes a signed int32 to unsigned uint32 using zigzag encoding.
// This maps small magnitude values to small unsigned values.
func zigzagEncode32(n int32) uint32 {
	return uint32((n << 1) ^ (n >> 31))
}

// zigzagDecode32 decodes a zigzag-encoded uint32 back to int32.
func zigzagDecode32(n uint32) int32 {
	return int32((n >> 1) ^ -(n & 1))
}

// EncodeUint32ArrayGroupVB encodes an unsigned int32 slice using group varint.
// For unsigned values, no zigzag encoding is needed.
func EncodeUint32ArrayGroupVB(values []uint32) []byte {
	if len(values) == 0 {
		return []byte{tagArrayInt32GroupVB, 0}
	}

	maxSize := 1 + 10 + len(values)*4 + (len(values)+3)/4
	buf := make([]byte, 0, maxSize)
	buf = append(buf, tagArrayInt32GroupVB)
	buf = appendUvarint(buf, uint64(len(values)))

	tmp := make([]byte, 17)
	fullGroups := len(values) / 4
	remainder := len(values) % 4

	for i := 0; i < fullGroups; i++ {
		idx := i * 4
		n := GroupVByteEncode(tmp, values[idx], values[idx+1], values[idx+2], values[idx+3])
		buf = append(buf, tmp[:n]...)
	}

	if remainder > 0 {
		var v0, v1, v2, v3 uint32
		idx := fullGroups * 4
		if remainder >= 1 {
			v0 = values[idx]
		}
		if remainder >= 2 {
			v1 = values[idx+1]
		}
		if remainder >= 3 {
			v2 = values[idx+2]
		}
		n := GroupVByteEncode(tmp, v0, v1, v2, v3)
		buf = append(buf, tmp[:n]...)
	}

	return buf
}

// DecodeUint32ArrayGroupVB decodes a group varint encoded uint32 array.
func DecodeUint32ArrayGroupVB(data []byte) ([]uint32, int, error) {
	if len(data) == 0 {
		return nil, 0, nil
	}

	count, n, err := readUvarint(data, 0)
	if err != nil {
		return nil, 0, err
	}
	off := n

	if count == 0 {
		return []uint32{}, off, nil
	}

	result := make([]uint32, count)
	fullGroups := int(count) / 4
	remainder := int(count) % 4

	idx := 0
	for i := 0; i < fullGroups; i++ {
		v0, v1, v2, v3, consumed := GroupVByteDecode(data[off:])
		if consumed == 0 {
			return nil, 0, ErrMaxArrayLen
		}
		result[idx] = v0
		result[idx+1] = v1
		result[idx+2] = v2
		result[idx+3] = v3
		idx += 4
		off += consumed
	}

	if remainder > 0 {
		v0, v1, v2, _, consumed := GroupVByteDecode(data[off:])
		if consumed == 0 {
			return nil, 0, ErrMaxArrayLen
		}
		if remainder >= 1 {
			result[idx] = v0
		}
		if remainder >= 2 {
			result[idx+1] = v1
		}
		if remainder >= 3 {
			result[idx+2] = v2
		}
		off += consumed
	}

	return result, off, nil
}
