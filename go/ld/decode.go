package ld

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/Neumenon/cowrie"
)

// Errors
var (
	ErrInvalidMagic    = errors.New("cowrie-ld: invalid magic bytes")
	ErrInvalidVersion  = errors.New("cowrie-ld: unsupported version")
	ErrUnexpectedEOF   = errors.New("cowrie-ld: unexpected end of data")
	ErrInvalidTag      = errors.New("cowrie-ld: invalid type tag")
	ErrInvalidFieldID  = errors.New("cowrie-ld: invalid field ID")
	ErrInvalidIRIID    = errors.New("cowrie-ld: invalid IRI ID")
	ErrMalformedLength = errors.New("cowrie-ld: malformed length exceeds remaining data")
)

// Decode decodes Cowrie-LD binary data into an LDDocument.
func Decode(data []byte) (*LDDocument, error) {
	r := &reader{data: data}
	return decode(r)
}

// DecodeFrom decodes from an io.Reader.
func DecodeFrom(rd io.Reader) (*LDDocument, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

// reader wraps a byte slice for reading.
type reader struct {
	data []byte
	pos  int
}

// remaining returns bytes left to read.
func (r *reader) remaining() int {
	return len(r.data) - r.pos
}

func (r *reader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, ErrUnexpectedEOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) read(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *reader) readUvarint() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.pos:])
	if n <= 0 {
		return 0, ErrUnexpectedEOF
	}
	r.pos += n
	return v, nil
}

func (r *reader) readString() (string, error) {
	length, err := r.readUvarint()
	if err != nil {
		return "", err
	}
	// Sanity check: length can't exceed remaining data
	if length > uint64(r.remaining()) {
		return "", ErrMalformedLength
	}
	b, err := r.read(int(length))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// zigzagDecode decodes a zigzag-encoded value.
func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

// decode reads the complete Cowrie-LD format.
func decode(r *reader) (*LDDocument, error) {
	// Read header: 'SJLD' + version + flags
	magic0, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic1, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic2, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic3, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if magic0 != Magic0 || magic1 != Magic1 || magic2 != Magic2 || magic3 != Magic3 {
		return nil, ErrInvalidMagic
	}

	version, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, ErrInvalidVersion
	}

	// Read flags (reserved for future use)
	_, err = r.readByte()
	if err != nil {
		return nil, err
	}

	doc := NewDocument()

	// Read field dictionary
	fieldDictLen, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each field needs at least 1 byte
	if fieldDictLen > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	doc.FieldDict = make([]string, fieldDictLen)
	for i := uint64(0); i < fieldDictLen; i++ {
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		doc.FieldDict[i] = s
	}

	// Read terms table
	termCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each term needs at least 3 bytes (term string len + IRI index + flags)
	if termCount*3 > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	doc.Terms = make([]TermEntry, termCount)
	for i := uint64(0); i < termCount; i++ {
		term, err := r.readString()
		if err != nil {
			return nil, err
		}
		iri, err := r.readString()
		if err != nil {
			return nil, err
		}
		flags, err := r.readByte()
		if err != nil {
			return nil, err
		}
		doc.Terms[i] = TermEntry{
			Term:  term,
			IRI:   IRI(iri),
			Flags: TermFlags(flags),
		}
	}

	// Read IRIs table
	iriCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each IRI needs at least 1 byte
	if iriCount > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	doc.IRIs = make([]IRI, iriCount)
	for i := uint64(0); i < iriCount; i++ {
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		doc.IRIs[i] = IRI(s)
	}

	// Read datatypes table
	dtCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each datatype needs at least 1 byte
	if dtCount > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	doc.Datatypes = make([]IRI, dtCount)
	for i := uint64(0); i < dtCount; i++ {
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		doc.Datatypes[i] = IRI(s)
	}

	// Decode root value
	doc.Root, err = decodeValue(r, doc)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// decodeValue reads a single value.
func decodeValue(r *reader, doc *LDDocument) (*cowrie.Value, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, err
	}

	switch tag {
	case cowrie.TagNull:
		return cowrie.Null(), nil

	case cowrie.TagFalse:
		return cowrie.Bool(false), nil

	case cowrie.TagTrue:
		return cowrie.Bool(true), nil

	case cowrie.TagInt64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return cowrie.Int64(zigzagDecode(u)), nil

	case cowrie.TagUint64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return cowrie.Uint64(u), nil

	case cowrie.TagFloat64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint64(b)
		return cowrie.Float64(math.Float64frombits(bits)), nil

	case cowrie.TagDecimal128:
		scale, err := r.readByte()
		if err != nil {
			return nil, err
		}
		coefBytes, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var coef [16]byte
		copy(coef[:], coefBytes)
		return cowrie.NewDecimal128(int8(scale), coef), nil

	case cowrie.TagString:
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		return cowrie.String(s), nil

	case cowrie.TagBytes:
		length, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check
		if length > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		b, err := r.read(int(length))
		if err != nil {
			return nil, err
		}
		return cowrie.Bytes(b), nil

	case cowrie.TagDatetime64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		nanos := int64(binary.LittleEndian.Uint64(b))
		return cowrie.Datetime64(nanos), nil

	case cowrie.TagUUID128:
		b, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var uuid [16]byte
		copy(uuid[:], b)
		return cowrie.UUID128(uuid), nil

	case cowrie.TagBigInt:
		length, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check
		if length > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		b, err := r.read(int(length))
		if err != nil {
			return nil, err
		}
		return cowrie.BigInt(b), nil

	case cowrie.TagArray:
		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: each element needs at least 1 byte
		if count > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		items := make([]*cowrie.Value, count)
		for i := uint64(0); i < count; i++ {
			v, err := decodeValue(r, doc)
			if err != nil {
				return nil, err
			}
			items[i] = v
		}
		return cowrie.Array(items...), nil

	case cowrie.TagObject:
		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: each field needs at least 2 bytes
		if count > uint64(r.remaining()/2) {
			return nil, ErrMalformedLength
		}
		members := make([]cowrie.Member, count)
		for i := uint64(0); i < count; i++ {
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			if fieldID >= uint64(len(doc.FieldDict)) {
				return nil, ErrInvalidFieldID
			}
			v, err := decodeValue(r, doc)
			if err != nil {
				return nil, err
			}
			members[i] = cowrie.Member{Key: doc.FieldDict[fieldID], Value: v}
		}
		return cowrie.Object(members...), nil

	case TagIRI:
		// IRI reference - decode ID and return as string value with IRI marker
		iriID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if iriID >= uint64(len(doc.IRIs)) {
			return nil, ErrInvalidIRIID
		}
		// Return the IRI as a string value
		// Callers can check the LDDocument to understand this is an IRI
		return cowrie.String(string(doc.IRIs[iriID])), nil

	case TagBNode:
		// Blank node - decode ID string
		id, err := r.readString()
		if err != nil {
			return nil, err
		}
		// Return as string value with _: prefix
		return cowrie.String("_:" + id), nil

	default:
		return nil, ErrInvalidTag
	}
}

// DecodeLDValue reads a value that may be an IRI, BNode, or regular value.
// Returns an LDValue with metadata about the value type.
func DecodeLDValue(r *reader, doc *LDDocument) (*LDValue, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Handle LD-specific tags
	switch tag {
	case TagIRI:
		iriID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if iriID >= uint64(len(doc.IRIs)) {
			return nil, ErrInvalidIRIID
		}
		return &LDValue{
			Value: cowrie.String(string(doc.IRIs[iriID])),
			IsIRI: true,
			IRIID: int(iriID),
		}, nil

	case TagBNode:
		id, err := r.readString()
		if err != nil {
			return nil, err
		}
		return &LDValue{
			Value:   cowrie.String(id),
			IsBNode: true,
			BNodeID: id,
		}, nil
	}

	// For non-LD tags, put the tag back and decode normally
	r.pos-- // unread the tag
	v, err := decodeValue(r, doc)
	if err != nil {
		return nil, err
	}
	return &LDValue{Value: v}, nil
}
