package gnn

import (
	"encoding/binary"
	"encoding/json"
	"math"
)

// SectionEncoder provides encoding helpers for section bodies.
type SectionEncoder struct {
	buf buffer
}

// NewSectionEncoder creates a new section encoder.
func NewSectionEncoder() *SectionEncoder {
	return &SectionEncoder{}
}

// Bytes returns the encoded section body.
func (e *SectionEncoder) Bytes() []byte {
	return e.buf.bytes()
}

// WriteUvarint writes a variable-length unsigned integer.
func (e *SectionEncoder) WriteUvarint(v uint64) {
	e.buf.writeUvarint(v)
}

// WriteInt64 writes an int64 as 8 bytes little-endian.
func (e *SectionEncoder) WriteInt64(v int64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	e.buf.write(b[:])
}

// WriteUint64 writes a uint64 as 8 bytes little-endian.
func (e *SectionEncoder) WriteUint64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	e.buf.write(b[:])
}

// WriteFloat32 writes a float32 as 4 bytes little-endian.
func (e *SectionEncoder) WriteFloat32(v float32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(v))
	e.buf.write(b[:])
}

// WriteFloat64 writes a float64 as 8 bytes little-endian.
func (e *SectionEncoder) WriteFloat64(v float64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	e.buf.write(b[:])
}

// WriteBytes writes raw bytes with length prefix.
func (e *SectionEncoder) WriteBytes(data []byte) {
	e.buf.writeUvarint(uint64(len(data)))
	e.buf.write(data)
}

// WriteRawBytes writes raw bytes without length prefix.
func (e *SectionEncoder) WriteRawBytes(data []byte) {
	e.buf.write(data)
}

// WriteString writes a string with length prefix.
func (e *SectionEncoder) WriteString(s string) {
	e.buf.writeString(s)
}

// WriteJSON writes a JSON-encoded value.
func (e *SectionEncoder) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.WriteBytes(data)
	return nil
}

// WriteShape writes a shape array (for tensors).
func (e *SectionEncoder) WriteShape(shape []int) {
	e.buf.writeUvarint(uint64(len(shape)))
	for _, dim := range shape {
		e.buf.writeUvarint(uint64(dim))
	}
}

// WriteDType writes a dtype byte.
func (e *SectionEncoder) WriteDType(dtype DType) {
	e.buf.writeByte(byte(dtype))
}

// WriteInt64Array writes an array of int64 values.
func (e *SectionEncoder) WriteInt64Array(values []int64) {
	e.buf.writeUvarint(uint64(len(values)))
	for _, v := range values {
		e.WriteInt64(v)
	}
}

// WriteFloat32Array writes an array of float32 values (contiguous tensor).
func (e *SectionEncoder) WriteFloat32Array(values []float32) {
	e.buf.writeUvarint(uint64(len(values)))
	for _, v := range values {
		e.WriteFloat32(v)
	}
}

// WriteFloat64Array writes an array of float64 values (contiguous tensor).
func (e *SectionEncoder) WriteFloat64Array(values []float64) {
	e.buf.writeUvarint(uint64(len(values)))
	for _, v := range values {
		e.WriteFloat64(v)
	}
}

// SectionDecoder provides decoding helpers for section bodies.
type SectionDecoder struct {
	r reader
}

// NewSectionDecoder creates a new section decoder.
func NewSectionDecoder(data []byte) *SectionDecoder {
	return &SectionDecoder{r: reader{data: data}}
}

// ReadUvarint reads a variable-length unsigned integer.
func (d *SectionDecoder) ReadUvarint() (uint64, error) {
	return d.r.readUvarint()
}

// ReadInt64 reads an int64 from 8 bytes little-endian.
func (d *SectionDecoder) ReadInt64() (int64, error) {
	b, err := d.r.read(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// ReadUint64 reads a uint64 from 8 bytes little-endian.
func (d *SectionDecoder) ReadUint64() (uint64, error) {
	b, err := d.r.read(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// ReadFloat32 reads a float32 from 4 bytes little-endian.
func (d *SectionDecoder) ReadFloat32() (float32, error) {
	b, err := d.r.read(4)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
}

// ReadFloat64 reads a float64 from 8 bytes little-endian.
func (d *SectionDecoder) ReadFloat64() (float64, error) {
	b, err := d.r.read(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}

// ReadBytes reads raw bytes with length prefix.
func (d *SectionDecoder) ReadBytes() ([]byte, error) {
	length, err := d.r.readUvarint()
	if err != nil {
		return nil, err
	}
	return d.r.read(int(length))
}

// ReadRawBytes reads a fixed number of raw bytes.
func (d *SectionDecoder) ReadRawBytes(n int) ([]byte, error) {
	return d.r.read(n)
}

// ReadString reads a string with length prefix.
func (d *SectionDecoder) ReadString() (string, error) {
	return d.r.readString()
}

// ReadJSON reads a JSON-encoded value.
func (d *SectionDecoder) ReadJSON(v any) error {
	data, err := d.ReadBytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// ReadShape reads a shape array.
func (d *SectionDecoder) ReadShape() ([]int, error) {
	length, err := d.r.readUvarint()
	if err != nil {
		return nil, err
	}
	shape := make([]int, length)
	for i := uint64(0); i < length; i++ {
		dim, err := d.r.readUvarint()
		if err != nil {
			return nil, err
		}
		shape[i] = int(dim)
	}
	return shape, nil
}

// ReadDType reads a dtype byte.
func (d *SectionDecoder) ReadDType() (DType, error) {
	b, err := d.r.readByte()
	if err != nil {
		return 0, err
	}
	return DType(b), nil
}

// ReadInt64Array reads an array of int64 values.
func (d *SectionDecoder) ReadInt64Array() ([]int64, error) {
	length, err := d.r.readUvarint()
	if err != nil {
		return nil, err
	}
	values := make([]int64, length)
	for i := uint64(0); i < length; i++ {
		v, err := d.ReadInt64()
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

// ReadFloat32Array reads an array of float32 values.
func (d *SectionDecoder) ReadFloat32Array() ([]float32, error) {
	length, err := d.r.readUvarint()
	if err != nil {
		return nil, err
	}
	values := make([]float32, length)
	for i := uint64(0); i < length; i++ {
		v, err := d.ReadFloat32()
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

// ReadFloat64Array reads an array of float64 values.
func (d *SectionDecoder) ReadFloat64Array() ([]float64, error) {
	length, err := d.r.readUvarint()
	if err != nil {
		return nil, err
	}
	values := make([]float64, length)
	for i := uint64(0); i < length; i++ {
		v, err := d.ReadFloat64()
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

// Remaining returns the remaining bytes.
func (d *SectionDecoder) Remaining() int {
	return len(d.r.data) - d.r.pos
}

// Position returns the current read position.
func (d *SectionDecoder) Position() int {
	return d.r.pos
}
