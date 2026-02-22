package gnn

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/Neumenon/cowrie"
	"github.com/klauspost/compress/zstd"
)

// Errors
var (
	ErrInvalidMagic    = errors.New("gnn: invalid magic bytes")
	ErrInvalidVersion  = errors.New("gnn: unsupported version")
	ErrUnexpectedEOF   = errors.New("gnn: unexpected end of data")
	ErrInvalidSection  = errors.New("gnn: invalid section")
	ErrMalformedLength = errors.New("gnn: malformed length exceeds remaining data")
)

// Encode writes the container to bytes.
// Uses v1.1 format if container.Version >= Version11, otherwise v1.0.
func (c *Container) Encode() ([]byte, error) {
	if c.IsV11() {
		return c.encodeV11()
	}
	return c.encodeV10()
}

// encodeV10 writes v1.0 format (backward compatible).
func (c *Container) encodeV10() ([]byte, error) {
	buf := getBuffer()

	// Write magic
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Magic2)
	buf.writeByte(Magic3)

	// Write version (1 byte for v1.0)
	buf.writeByte(Version)

	// Write flags
	buf.writeByte(c.Flags)

	// Encode meta section first if we have meta
	sections := c.Sections
	if c.meta != nil {
		metaBody, err := json.Marshal(c.meta)
		if err != nil {
			return nil, err
		}
		// Prepend meta section
		metaSection := Section{Kind: SectionMeta, Name: "meta", Body: metaBody}
		sections = append([]Section{metaSection}, sections...)
	}

	// Write section count
	buf.writeUvarint(uint64(len(sections)))

	// Write each section
	for _, s := range sections {
		// Kind
		buf.writeUvarint(uint64(s.Kind))
		// Name
		buf.writeString(s.Name)
		// Body length (fixed 8 bytes for u64)
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s.Body)))
		buf.write(lenBuf[:])
		// Body
		buf.write(s.Body)
	}

	// Copy result and return buffer to pool
	out := make([]byte, len(buf.data))
	copy(out, buf.data)
	putBuffer(buf)
	return out, nil
}

// encodeV11 writes v1.1 format with per-section encoding.
func (c *Container) encodeV11() ([]byte, error) {
	buf := getBuffer()

	// Write magic
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Magic2)
	buf.writeByte(Magic3)

	// Write version (2 bytes for v1.1: major.minor)
	buf.writeByte(byte(c.Version >> 8))   // Major
	buf.writeByte(byte(c.Version & 0xFF)) // Minor

	// Write flags
	buf.writeByte(c.Flags)

	// Encode meta section first if we have meta
	sections := c.Sections
	if c.meta != nil {
		metaBody, err := encodeMetaCowrie(c.meta)
		if err != nil {
			return nil, err
		}
		// Prepend meta section with Cowrie encoding
		metaSection := Section{
			Kind:     SectionMeta,
			Name:     "meta",
			Encoding: SectionEncodingCowrie,
			Body:     metaBody,
		}
		sections = append([]Section{metaSection}, sections...)
	}

	// Write section count
	buf.writeUvarint(uint64(len(sections)))

	// Write each section
	for _, s := range sections {
		// Kind
		buf.writeUvarint(uint64(s.Kind))
		// Encoding (v1.1 addition)
		buf.writeByte(byte(s.Encoding))
		// Name
		buf.writeString(s.Name)
		// Body length (fixed 8 bytes for u64)
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s.Body)))
		buf.write(lenBuf[:])
		// Body
		buf.write(s.Body)
	}

	// Copy result and return buffer to pool
	out := make([]byte, len(buf.data))
	copy(out, buf.data)
	putBuffer(buf)
	return out, nil
}

// encodeMetaCowrie encodes Meta to Cowrie binary format.
func encodeMetaCowrie(m *Meta) ([]byte, error) {
	// Convert Meta to cowrie.Value object
	members := []cowrie.Member{
		{Key: "dataset_name", Value: cowrie.String(m.DatasetName)},
		{Key: "directed", Value: cowrie.Bool(m.Directed)},
	}

	if m.Version != "" {
		members = append(members, cowrie.Member{Key: "version", Value: cowrie.String(m.Version)})
	}
	if m.Temporal {
		members = append(members, cowrie.Member{Key: "temporal", Value: cowrie.Bool(m.Temporal)})
	}
	if m.Heterogeneous {
		members = append(members, cowrie.Member{Key: "heterogeneous", Value: cowrie.Bool(m.Heterogeneous)})
	}

	// Node types
	if len(m.NodeTypes) > 0 {
		nodeTypes := make([]*cowrie.Value, len(m.NodeTypes))
		for i, nt := range m.NodeTypes {
			nodeTypes[i] = cowrie.String(nt)
		}
		members = append(members, cowrie.Member{Key: "node_types", Value: cowrie.Array(nodeTypes...)})
	}

	// Edge types
	if len(m.EdgeTypes) > 0 {
		edgeTypes := make([]*cowrie.Value, len(m.EdgeTypes))
		for i, et := range m.EdgeTypes {
			edgeTypes[i] = cowrie.Object(
				cowrie.Member{Key: "src_type", Value: cowrie.String(et.SrcType)},
				cowrie.Member{Key: "edge_type", Value: cowrie.String(et.EdgeType)},
				cowrie.Member{Key: "dst_type", Value: cowrie.String(et.DstType)},
			)
		}
		members = append(members, cowrie.Member{Key: "edge_types", Value: cowrie.Array(edgeTypes...)})
	}

	// ID spaces
	if len(m.IDSpaces) > 0 {
		idSpaceMembers := make([]cowrie.Member, 0, len(m.IDSpaces))
		for name, space := range m.IDSpaces {
			idSpaceMembers = append(idSpaceMembers, cowrie.Member{
				Key: name,
				Value: cowrie.Object(
					cowrie.Member{Key: "start", Value: cowrie.Int64(space.Start)},
					cowrie.Member{Key: "count", Value: cowrie.Int64(space.Count)},
				),
			})
		}
		members = append(members, cowrie.Member{Key: "id_spaces", Value: cowrie.Object(idSpaceMembers...)})
	}

	// Features
	if len(m.Features) > 0 {
		featureMembers := make([]cowrie.Member, 0, len(m.Features))
		for name, spec := range m.Features {
			shapeVals := make([]*cowrie.Value, len(spec.Shape))
			for i, s := range spec.Shape {
				shapeVals[i] = cowrie.Int64(int64(s))
			}
			featureMembers = append(featureMembers, cowrie.Member{
				Key: name,
				Value: cowrie.Object(
					cowrie.Member{Key: "shape", Value: cowrie.Array(shapeVals...)},
					cowrie.Member{Key: "dtype", Value: cowrie.String(spec.DType)},
				),
			})
		}
		members = append(members, cowrie.Member{Key: "features", Value: cowrie.Object(featureMembers...)})
	}

	// Labels
	if len(m.Labels) > 0 {
		labelMembers := make([]cowrie.Member, 0, len(m.Labels))
		for name, spec := range m.Labels {
			shapeVals := make([]*cowrie.Value, len(spec.Shape))
			for i, s := range spec.Shape {
				shapeVals[i] = cowrie.Int64(int64(s))
			}
			labelMembers = append(labelMembers, cowrie.Member{
				Key: name,
				Value: cowrie.Object(
					cowrie.Member{Key: "shape", Value: cowrie.Array(shapeVals...)},
					cowrie.Member{Key: "dtype", Value: cowrie.String(spec.DType)},
				),
			})
		}
		members = append(members, cowrie.Member{Key: "labels", Value: cowrie.Object(labelMembers...)})
	}

	return cowrie.Encode(cowrie.Object(members...))
}

// EncodeCompressed encodes the container and compresses it with zstd.
// This typically achieves 2-3x smaller file sizes compared to JSON+zstd.
func (c *Container) EncodeCompressed() ([]byte, error) {
	// Encode normally first
	data, err := c.Encode()
	if err != nil {
		return nil, err
	}

	// Compress with zstd at best compression level
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}
	compressed := enc.EncodeAll(data, nil)

	// Create wrapper with compression magic
	// Format: 'S' 'J' 'G' 'Z' + 4-byte original size + compressed data
	result := make([]byte, 8+len(compressed))
	result[0] = 'S'
	result[1] = 'J'
	result[2] = 'G'
	result[3] = 'Z' // Z for zstd compressed
	binary.LittleEndian.PutUint32(result[4:], uint32(len(data)))
	copy(result[8:], compressed)

	return result, nil
}

// DecodeCompressed decodes a zstd-compressed container.
// Automatically detects if the data is compressed (SJGZ magic) or not (SJGN magic).
func DecodeCompressed(data []byte) (*Container, error) {
	if len(data) < 8 {
		return nil, ErrUnexpectedEOF
	}

	// Check magic
	if data[0] == 'S' && data[1] == 'J' && data[2] == 'G' && data[3] == 'Z' {
		// Compressed format
		// originalSize := binary.LittleEndian.Uint32(data[4:8]) // For validation
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer dec.Close()

		decompressed, err := dec.DecodeAll(data[8:], nil)
		if err != nil {
			return nil, err
		}
		return Decode(decompressed)
	}

	// Not compressed, decode normally
	return Decode(data)
}

// Float32ToFloat16 converts float32 values to float16 bytes.
// This halves the storage size with minor precision loss (~0.1% relative error).
// Ideal for embeddings and features where full precision isn't needed.
func Float32ToFloat16(data []float32) []byte {
	result := make([]byte, len(data)*2)
	for i, v := range data {
		bits := math.Float32bits(v)
		sign := (bits >> 16) & 0x8000
		exp := int((bits>>23)&0xFF) - 127 + 15
		mantissa := (bits >> 13) & 0x3FF

		var f16 uint16
		if exp <= 0 {
			f16 = uint16(sign) // Underflow to zero
		} else if exp >= 31 {
			f16 = uint16(sign | 0x7C00) // Overflow to inf
		} else {
			f16 = uint16(sign | uint32(exp<<10) | mantissa)
		}
		binary.LittleEndian.PutUint16(result[i*2:], f16)
	}
	return result
}

// Float16ToFloat32 converts float16 bytes back to float32 values.
func Float16ToFloat32(data []byte) []float32 {
	result := make([]float32, len(data)/2)
	for i := range result {
		f16 := binary.LittleEndian.Uint16(data[i*2:])

		sign := uint32(f16&0x8000) << 16
		exp := int((f16 >> 10) & 0x1F)
		mantissa := uint32(f16 & 0x3FF)

		var bits uint32
		if exp == 0 {
			bits = sign // Zero or denormal (treat as zero)
		} else if exp == 31 {
			bits = sign | 0x7F800000 | (mantissa << 13) // Inf or NaN
		} else {
			bits = sign | uint32((exp-15+127)<<23) | (mantissa << 13)
		}
		result[i] = math.Float32frombits(bits)
	}
	return result
}

// ByteShuffle groups similar bytes together for better compression.
// For float32 data, groups byte 0s, byte 1s, byte 2s, byte 3s separately.
// This can improve zstd compression by 2-10x for numeric data.
func ByteShuffle(data []byte, bytesPerElement int) []byte {
	n := len(data) / bytesPerElement
	if n == 0 {
		return data
	}
	result := make([]byte, len(data))
	for elem := 0; elem < n; elem++ {
		for b := 0; b < bytesPerElement; b++ {
			result[b*n+elem] = data[elem*bytesPerElement+b]
		}
	}
	return result
}

// ByteUnshuffle reverses ByteShuffle.
func ByteUnshuffle(data []byte, bytesPerElement int) []byte {
	n := len(data) / bytesPerElement
	if n == 0 {
		return data
	}
	result := make([]byte, len(data))
	for elem := 0; elem < n; elem++ {
		for b := 0; b < bytesPerElement; b++ {
			result[elem*bytesPerElement+b] = data[b*n+elem]
		}
	}
	return result
}

// Save writes the container to a file.
func (c *Container) Save(filename string) error {
	data, err := c.Encode()
	if err != nil {
		return err
	}
	return writeFile(filename, data)
}

// EncodeTo writes the container to an io.Writer.
func (c *Container) EncodeTo(w io.Writer) error {
	data, err := c.Encode()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// Load reads a container from a file.
func Load(filename string) (*Container, error) {
	data, err := readFile(filename)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

// Decode decodes a container from bytes.
// Automatically detects v1.0 vs v1.1 format.
func Decode(data []byte) (*Container, error) {
	r := &reader{data: data}

	// Read magic
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

	// Read version byte 1
	version1, err := r.readByte()
	if err != nil {
		return nil, err
	}

	var version uint16
	var isV11 bool

	// Detect version format
	// v1.0: [0x01] [flags] ...
	// v1.1: [0x01] [0x10+] [flags] ... (minor version >= 0x10 to distinguish from flags)
	if version1 != 0x01 {
		return nil, ErrInvalidVersion
	}

	// Peek at next byte to determine if it's v1.0 flags or v1.1 minor version
	nextByte, err := r.readByte()
	if err != nil {
		return nil, err
	}

	if nextByte >= 0x10 {
		// v1.1: nextByte is minor version (>= 0x10)
		version = uint16(version1)<<8 | uint16(nextByte)
		isV11 = true
	} else {
		// v1.0: nextByte is flags (0x00-0x0F)
		// We need to "unread" this byte since it's actually the flags
		r.pos-- // Put back the flags byte
		version = uint16(version1)
		isV11 = false
	}

	// Read flags
	flags, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Read section count
	sectionCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each section needs at least ~10 bytes minimum
	if sectionCount > uint64(r.remaining()/10) {
		return nil, ErrMalformedLength
	}

	c := &Container{
		Flags:    flags,
		Version:  version,
		Sections: make([]Section, 0, sectionCount),
		features: make(map[string]*FeatureHeader),
	}

	// Read sections
	for i := uint64(0); i < sectionCount; i++ {
		// Kind
		kind, err := r.readUvarint()
		if err != nil {
			return nil, err
		}

		// Encoding (v1.1 only)
		var encoding SectionEncoding
		if isV11 {
			encByte, err := r.readByte()
			if err != nil {
				return nil, err
			}
			encoding = SectionEncoding(encByte)
		} else {
			// v1.0: derive encoding from section kind
			encoding = SectionKind(kind).LegacyEncoding()
		}

		// Name
		name, err := r.readString()
		if err != nil {
			return nil, err
		}

		// Body length (fixed 8 bytes)
		lenBytes, err := r.read(8)
		if err != nil {
			return nil, err
		}
		bodyLen := binary.LittleEndian.Uint64(lenBytes)

		// Sanity check: bodyLen can't exceed remaining data
		if bodyLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}

		// Body
		body, err := r.read(int(bodyLen))
		if err != nil {
			return nil, err
		}

		section := Section{
			Kind:     SectionKind(kind),
			Name:     name,
			Encoding: encoding,
			Body:     body,
		}

		// Parse meta section
		if section.Kind == SectionMeta && name == "meta" {
			var meta Meta
			if encoding == SectionEncodingCowrie {
				// v1.1 Cowrie-encoded meta
				if err := decodeMetaCowrie(body, &meta); err != nil {
					return nil, err
				}
			} else {
				// v1.0 JSON-encoded meta
				if err := json.Unmarshal(body, &meta); err != nil {
					return nil, err
				}
			}
			c.meta = &meta
		} else {
			c.Sections = append(c.Sections, section)
		}
	}

	return c, nil
}

// decodeMetaCowrie decodes Meta from Cowrie binary format.
func decodeMetaCowrie(data []byte, m *Meta) error {
	val, err := cowrie.Decode(data)
	if err != nil {
		return err
	}

	if val.Type() != cowrie.TypeObject {
		return errors.New("gnn: meta section is not an object")
	}

	// Initialize maps
	m.IDSpaces = make(map[string]IDSpace)
	m.Features = make(map[string]FeatureSpec)
	m.Labels = make(map[string]LabelSpec)

	for _, member := range val.Members() {
		switch member.Key {
		case "dataset_name":
			if member.Value.Type() == cowrie.TypeString {
				m.DatasetName = member.Value.String()
			}
		case "version":
			if member.Value.Type() == cowrie.TypeString {
				m.Version = member.Value.String()
			}
		case "directed":
			if member.Value.Type() == cowrie.TypeBool {
				m.Directed = member.Value.Bool()
			}
		case "temporal":
			if member.Value.Type() == cowrie.TypeBool {
				m.Temporal = member.Value.Bool()
			}
		case "heterogeneous":
			if member.Value.Type() == cowrie.TypeBool {
				m.Heterogeneous = member.Value.Bool()
			}
		case "node_types":
			if member.Value.Type() == cowrie.TypeArray {
				m.NodeTypes = make([]string, member.Value.Len())
				for i := 0; i < member.Value.Len(); i++ {
					if member.Value.Index(i).Type() == cowrie.TypeString {
						m.NodeTypes[i] = member.Value.Index(i).String()
					}
				}
			}
		case "edge_types":
			if member.Value.Type() == cowrie.TypeArray {
				m.EdgeTypes = make([]EdgeTypeTuple, member.Value.Len())
				for i := 0; i < member.Value.Len(); i++ {
					et := member.Value.Index(i)
					if et.Type() == cowrie.TypeObject {
						if srcType := et.Get("src_type"); srcType != nil && srcType.Type() == cowrie.TypeString {
							m.EdgeTypes[i].SrcType = srcType.String()
						}
						if edgeType := et.Get("edge_type"); edgeType != nil && edgeType.Type() == cowrie.TypeString {
							m.EdgeTypes[i].EdgeType = edgeType.String()
						}
						if dstType := et.Get("dst_type"); dstType != nil && dstType.Type() == cowrie.TypeString {
							m.EdgeTypes[i].DstType = dstType.String()
						}
					}
				}
			}
		case "id_spaces":
			if member.Value.Type() == cowrie.TypeObject {
				for _, spaceMember := range member.Value.Members() {
					space := IDSpace{}
					if start := spaceMember.Value.Get("start"); start != nil && start.Type() == cowrie.TypeInt64 {
						space.Start = start.Int64()
					}
					if count := spaceMember.Value.Get("count"); count != nil && count.Type() == cowrie.TypeInt64 {
						space.Count = count.Int64()
					}
					m.IDSpaces[spaceMember.Key] = space
				}
			}
		case "features":
			if member.Value.Type() == cowrie.TypeObject {
				for _, featMember := range member.Value.Members() {
					spec := FeatureSpec{}
					if shapeVal := featMember.Value.Get("shape"); shapeVal != nil && shapeVal.Type() == cowrie.TypeArray {
						spec.Shape = make([]int, shapeVal.Len())
						for i := 0; i < shapeVal.Len(); i++ {
							if shapeVal.Index(i).Type() == cowrie.TypeInt64 {
								spec.Shape[i] = int(shapeVal.Index(i).Int64())
							}
						}
					}
					if dtype := featMember.Value.Get("dtype"); dtype != nil && dtype.Type() == cowrie.TypeString {
						spec.DType = dtype.String()
					}
					m.Features[featMember.Key] = spec
				}
			}
		case "labels":
			if member.Value.Type() == cowrie.TypeObject {
				for _, labelMember := range member.Value.Members() {
					spec := LabelSpec{}
					if shapeVal := labelMember.Value.Get("shape"); shapeVal != nil && shapeVal.Type() == cowrie.TypeArray {
						spec.Shape = make([]int, shapeVal.Len())
						for i := 0; i < shapeVal.Len(); i++ {
							if shapeVal.Index(i).Type() == cowrie.TypeInt64 {
								spec.Shape[i] = int(shapeVal.Index(i).Int64())
							}
						}
					}
					if dtype := labelMember.Value.Get("dtype"); dtype != nil && dtype.Type() == cowrie.TypeString {
						spec.DType = dtype.String()
					}
					m.Labels[labelMember.Key] = spec
				}
			}
		}
	}

	return nil
}

// DecodeFrom decodes a container from an io.Reader.
func DecodeFrom(rd io.Reader) (*Container, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

// Buffer pool for encoding - reduces allocations in hot paths
var bufferPool = sync.Pool{
	New: func() any {
		return &buffer{data: make([]byte, 0, 4096)}
	},
}

func getBuffer() *buffer {
	return bufferPool.Get().(*buffer)
}

func putBuffer(buf *buffer) {
	buf.data = buf.data[:0]
	bufferPool.Put(buf)
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

// File helpers (simple implementations)
func writeFile(filename string, data []byte) error {
	// Using io package to avoid os import for portability
	// In real usage, use os.WriteFile
	return writeFileImpl(filename, data)
}

func readFile(filename string) ([]byte, error) {
	// Using io package to avoid os import for portability
	// In real usage, use os.ReadFile
	return readFileImpl(filename)
}
