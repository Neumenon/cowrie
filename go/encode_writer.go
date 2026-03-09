package cowrie

import (
	"encoding/binary"
	"io"
	"net"
)

// EncodeToWriter encodes a value to an io.Writer using scatter-gather I/O
// when possible. For tensor values, the tensor data is written directly
// from the source buffer without copying into an intermediate buffer.
//
// When the writer implements net.Conn (or writev-capable), this uses
// net.Buffers for vectored I/O. Otherwise falls back to sequential writes.
func EncodeToWriter(w io.Writer, v *Value) error {
	// Build dictionary
	d := newDict()
	collectKeys(v, d)

	// Encode header + dictionary into a small buffer
	hdr := bufferPool.Get().(*buffer)
	hdr.data = hdr.data[:0]

	hdr.writeByte(Magic0)
	hdr.writeByte(Magic1)
	hdr.writeByte(Version)
	hdr.writeByte(0) // flags

	hdr.writeUvarint(uint64(len(d.keys)))
	for _, key := range d.keys {
		hdr.writeString(key)
	}

	// Collect write segments: header, then value segments
	segments := make(net.Buffers, 0, 8)
	segments = append(segments, hdr.bytes())

	if err := collectSegments(&segments, v, d); err != nil {
		putBuf(hdr)
		return err
	}

	// Write all segments
	_, err := segments.WriteTo(w)
	putBuf(hdr)
	return err
}

// collectSegments appends write segments for a value.
// For tensors, the data is added as a separate segment (zero-copy).
// For all other types, values are encoded into the last buffer segment.
func collectSegments(segments *net.Buffers, v *Value, d *dict) error {
	// For most values, just encode normally into a buffer
	buf := &buffer{data: make([]byte, 0, 256)}
	if err := encodeValueScatter(buf, segments, v, d); err != nil {
		return err
	}
	if len(buf.data) > 0 {
		*segments = append(*segments, buf.data)
	}
	return nil
}

// encodeValueScatter encodes a value, splitting tensor data into separate segments.
func encodeValueScatter(buf *buffer, segments *net.Buffers, v *Value, d *dict) error {
	if v == nil {
		buf.writeByte(TagNull)
		return nil
	}

	switch v.typ {
	case TypeTensor:
		// Write header to buf
		buf.writeByte(TagTensor)
		buf.writeByte(byte(v.tensorVal.DType))
		buf.writeByte(byte(len(v.tensorVal.Dims)))
		for _, dim := range v.tensorVal.Dims {
			buf.writeUvarint(dim)
		}
		buf.writeUvarint(uint64(len(v.tensorVal.Data)))

		// Flush current buf as a segment, then add tensor data as separate segment
		if len(buf.data) > 0 {
			*segments = append(*segments, buf.data)
			buf.data = make([]byte, 0, 64)
		}
		*segments = append(*segments, v.tensorVal.Data)
		return nil

	case TypeArray:
		n := len(v.arrayVal)
		if n <= 15 {
			buf.writeByte(byte(TagFixarrayBase + n))
		} else {
			buf.writeByte(TagArray)
			buf.writeUvarint(uint64(n))
		}
		for _, item := range v.arrayVal {
			if err := encodeValueScatter(buf, segments, item, d); err != nil {
				return err
			}
		}
		return nil

	case TypeObject:
		n := len(v.objectVal)
		if n <= 15 {
			buf.writeByte(byte(TagFixmapBase + n))
		} else {
			buf.writeByte(TagObject)
			buf.writeUvarint(uint64(n))
		}
		for _, m := range v.objectVal {
			idx := d.get(m.Key)
			buf.writeUvarint(uint64(idx))
			if err := encodeValueScatter(buf, segments, m.Value, d); err != nil {
				return err
			}
		}
		return nil

	default:
		// All other types: encode into buf normally
		return encodeValueToBuf(buf, v, d)
	}
}

// encodeValueToBuf encodes a non-container, non-tensor value into a buffer.
func encodeValueToBuf(buf *buffer, v *Value, d *dict) error {
	if v == nil {
		buf.writeByte(TagNull)
		return nil
	}

	switch v.typ {
	case TypeNull:
		buf.writeByte(TagNull)
	case TypeBool:
		if v.boolVal {
			buf.writeByte(TagTrue)
		} else {
			buf.writeByte(TagFalse)
		}
	case TypeInt64:
		if v.int64Val >= 0 && v.int64Val <= 127 {
			buf.writeByte(byte(TagFixintBase + v.int64Val))
		} else if v.int64Val >= -16 && v.int64Val <= -1 {
			buf.writeByte(byte(TagFixnegBase + (-1 - v.int64Val)))
		} else {
			buf.writeByte(TagInt64)
			buf.writeUvarint(zigzagEncode(v.int64Val))
		}
	case TypeUint64:
		buf.writeByte(TagUint64)
		buf.writeUvarint(v.uint64Val)
	case TypeFloat64:
		buf.writeByte(TagFloat64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], uint64FromFloat64(v.float64Val))
		buf.write(bits[:])
	case TypeDecimal128:
		buf.writeByte(TagDecimal128)
		buf.writeByte(byte(v.decimal128.Scale))
		buf.write(v.decimal128.Coef[:])
	case TypeString:
		buf.writeByte(TagString)
		buf.writeString(v.stringVal)
	case TypeBytes:
		buf.writeByte(TagBytes)
		buf.writeUvarint(uint64(len(v.bytesVal)))
		buf.write(v.bytesVal)
	case TypeDatetime64:
		buf.writeByte(TagDatetime64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], uint64(v.datetime64))
		buf.write(bits[:])
	case TypeUUID128:
		buf.writeByte(TagUUID128)
		buf.write(v.uuid128[:])
	case TypeBigInt:
		buf.writeByte(TagBigInt)
		buf.writeUvarint(uint64(len(v.bigintVal)))
		buf.write(v.bigintVal)
	case TypeBitmask:
		buf.writeByte(TagBitmask)
		buf.writeUvarint(v.bitmaskVal.Count)
		buf.write(v.bitmaskVal.Bits)
	default:
		// Fall back to full encode for complex types
		return encodeValue(buf, v, d)
	}
	return nil
}

