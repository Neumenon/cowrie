package ld

import (
	"encoding/binary"
	"io"
	"math"
	"sync"

	"github.com/Neumenon/cowrie"
)

// Buffer pool for encoding - reduces allocations
var bufferPool = sync.Pool{
	New: func() any {
		return &buffer{data: make([]byte, 0, 4096)}
	},
}

// Encode encodes an LDDocument to Cowrie-LD binary format.
func Encode(doc *LDDocument) ([]byte, error) {
	buf := bufferPool.Get().(*buffer)
	buf.data = buf.data[:0]

	if err := encode(buf, doc); err != nil {
		bufferPool.Put(buf)
		return nil, err
	}

	// Copy result (can't return pooled buffer)
	out := make([]byte, len(buf.data))
	copy(out, buf.data)
	bufferPool.Put(buf)
	return out, nil
}

// EncodeTo encodes an LDDocument to an io.Writer.
func EncodeTo(w io.Writer, doc *LDDocument) error {
	data, err := Encode(doc)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// EncodeValue encodes a single Cowrie value with an LDDocument context.
// This is useful when you have a pre-built document context.
func EncodeValue(v *cowrie.Value, doc *LDDocument) ([]byte, error) {
	doc.Root = v
	return Encode(doc)
}

// buffer is a simple byte buffer for encoding.
type buffer struct {
	data []byte
}

func (b *buffer) bytes() []byte {
	return b.data
}

func (b *buffer) writeByte(c byte) {
	b.data = append(b.data, c)
}

func (b *buffer) write(p []byte) {
	b.data = append(b.data, p...)
}

func (b *buffer) writeUvarint(v uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	b.write(buf[:n])
}

func (b *buffer) writeString(s string) {
	b.writeUvarint(uint64(len(s)))
	b.write([]byte(s))
}

// zigzagEncode encodes a signed integer using zigzag encoding.
func zigzagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// encoder holds encoding state
type encoder struct {
	buf      *buffer
	doc      *LDDocument
	fieldMap map[string]int // field name → index
	iriMap   map[IRI]int    // IRI → index in doc.IRIs
}

func newEncoder(doc *LDDocument) *encoder {
	e := &encoder{
		buf:      &buffer{},
		doc:      doc,
		fieldMap: make(map[string]int),
		iriMap:   make(map[IRI]int),
	}
	// Build lookup maps
	for i, iri := range doc.IRIs {
		e.iriMap[iri] = i
	}
	return e
}

// collectFields recursively collects all object keys.
func (e *encoder) collectFields(v *cowrie.Value) {
	if v == nil {
		return
	}
	switch v.Type() {
	case cowrie.TypeArray:
		for _, item := range v.Array() {
			e.collectFields(item)
		}
	case cowrie.TypeObject:
		for _, m := range v.Members() {
			if _, ok := e.fieldMap[m.Key]; !ok {
				idx := len(e.doc.FieldDict)
				e.doc.FieldDict = append(e.doc.FieldDict, m.Key)
				e.fieldMap[m.Key] = idx
			}
			e.collectFields(m.Value)
		}
	}
}

// encode writes the complete Cowrie-LD format to the buffer.
func encode(buf *buffer, doc *LDDocument) error {
	enc := newEncoder(doc)
	enc.buf = buf

	// Collect field names from root value
	if doc.Root != nil {
		enc.collectFields(doc.Root)
	}

	// Write header: 'SJLD' + version + flags
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Magic2)
	buf.writeByte(Magic3)
	buf.writeByte(Version)
	buf.writeByte(0) // flags = 0

	// Write field dictionary
	buf.writeUvarint(uint64(len(doc.FieldDict)))
	for _, key := range doc.FieldDict {
		buf.writeString(key)
	}

	// Write terms table
	buf.writeUvarint(uint64(len(doc.Terms)))
	for _, term := range doc.Terms {
		buf.writeString(term.Term)
		buf.writeString(string(term.IRI))
		buf.writeByte(byte(term.Flags))
	}

	// Write IRIs table
	buf.writeUvarint(uint64(len(doc.IRIs)))
	for _, iri := range doc.IRIs {
		buf.writeString(string(iri))
	}

	// Write datatypes table
	buf.writeUvarint(uint64(len(doc.Datatypes)))
	for _, dt := range doc.Datatypes {
		buf.writeString(string(dt))
	}

	// Write root value
	return enc.encodeValue(doc.Root)
}

// encodeValue writes a single value using Cowrie-LD encoding.
func (e *encoder) encodeValue(v *cowrie.Value) error {
	if v == nil {
		e.buf.writeByte(cowrie.TagNull)
		return nil
	}

	switch v.Type() {
	case cowrie.TypeNull:
		e.buf.writeByte(cowrie.TagNull)

	case cowrie.TypeBool:
		if v.Bool() {
			e.buf.writeByte(cowrie.TagTrue)
		} else {
			e.buf.writeByte(cowrie.TagFalse)
		}

	case cowrie.TypeInt64:
		e.buf.writeByte(cowrie.TagInt64)
		e.buf.writeUvarint(zigzagEncode(v.Int64()))

	case cowrie.TypeUint64:
		e.buf.writeByte(cowrie.TagUint64)
		e.buf.writeUvarint(v.Uint64())

	case cowrie.TypeFloat64:
		e.buf.writeByte(cowrie.TagFloat64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(v.Float64()))
		e.buf.write(bits[:])

	case cowrie.TypeDecimal128:
		e.buf.writeByte(cowrie.TagDecimal128)
		dec := v.Decimal128()
		e.buf.writeByte(byte(dec.Scale))
		e.buf.write(dec.Coef[:])

	case cowrie.TypeString:
		e.buf.writeByte(cowrie.TagString)
		e.buf.writeString(v.String())

	case cowrie.TypeBytes:
		e.buf.writeByte(cowrie.TagBytes)
		data := v.Bytes()
		e.buf.writeUvarint(uint64(len(data)))
		e.buf.write(data)

	case cowrie.TypeDatetime64:
		e.buf.writeByte(cowrie.TagDatetime64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], uint64(v.Datetime64()))
		e.buf.write(bits[:])

	case cowrie.TypeUUID128:
		e.buf.writeByte(cowrie.TagUUID128)
		uuid := v.UUID128()
		e.buf.write(uuid[:])

	case cowrie.TypeBigInt:
		e.buf.writeByte(cowrie.TagBigInt)
		data := v.BigInt()
		e.buf.writeUvarint(uint64(len(data)))
		e.buf.write(data)

	case cowrie.TypeArray:
		e.buf.writeByte(cowrie.TagArray)
		arr := v.Array()
		e.buf.writeUvarint(uint64(len(arr)))
		for _, item := range arr {
			if err := e.encodeValue(item); err != nil {
				return err
			}
		}

	case cowrie.TypeObject:
		e.buf.writeByte(cowrie.TagObject)
		members := v.Members()
		e.buf.writeUvarint(uint64(len(members)))
		for _, m := range members {
			idx := e.fieldMap[m.Key]
			e.buf.writeUvarint(uint64(idx))
			if err := e.encodeValue(m.Value); err != nil {
				return err
			}
		}
	}

	return nil
}

// EncodeIRI writes an IRI reference using the TagIRI tag.
func (e *encoder) EncodeIRI(iri IRI) error {
	idx, ok := e.iriMap[iri]
	if !ok {
		// Add to IRIs table
		idx = len(e.doc.IRIs)
		e.doc.IRIs = append(e.doc.IRIs, iri)
		e.iriMap[iri] = idx
	}
	e.buf.writeByte(TagIRI)
	e.buf.writeUvarint(uint64(idx))
	return nil
}

// EncodeBNode writes a blank node using the TagBNode tag.
func (e *encoder) EncodeBNode(id string) error {
	e.buf.writeByte(TagBNode)
	e.buf.writeString(id)
	return nil
}

// EncodeLDValue encodes an LDValue which may be an IRI, BNode, or regular value.
func (e *encoder) EncodeLDValue(ldv *LDValue) error {
	if ldv.IsIRI {
		return e.EncodeIRI(e.doc.IRIs[ldv.IRIID])
	}
	if ldv.IsBNode {
		return e.EncodeBNode(ldv.BNodeID)
	}
	return e.encodeValue(ldv.Value)
}
