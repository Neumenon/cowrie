package codec

import (
	"encoding/binary"

	"github.com/Neumenon/cowrie/go/v2"
	"lukechampine.com/blake3"
)

// ColumnarTensorBatch is the builder input for a fixed-shape columnar tensor
// batch: N tensors each of shape [..ShapeTail], laid out column-wise.
type ColumnarTensorBatch struct {
	// TypeID is the LOGICAL type_id (shares the type_id space with row tensors).
	TypeID uint32
	// DType is the element dtype of tensor.values.
	DType cowrie.DType
	// NumRows is N, the number of tensors in the batch.
	NumRows uint64
	// ShapeTail is the shared inner shape (suffix dims), e.g. [16] for [N,16].
	ShapeTail []uint64
	// Values is the contiguous PLAIN [N, ...inner] row-major value bytes.
	Values []byte
	// Validity is an optional LSB-first bitmap of length ceil(N/8); nil => all-valid.
	Validity []byte
	// CompressChunks enables per-chunk zstd (opaque output). When off, chunks are
	// PLAIN and zero-copy on read.
	CompressChunks bool
}

// prodU64 returns the product of dims (1 for an empty/scalar tail).
func prodU64(dims []uint64) uint64 {
	p := uint64(1)
	for _, d := range dims {
		p *= d
	}
	return p
}

// validateBatch checks dtype/shape/validity invariants and returns the element size.
func validateBatch(b *ColumnarTensorBatch) (uint64, error) {
	if b.NumRows > 0xFFFFFFFF {
		return 0, ErrColumnarRowOverflow
	}
	elem, ok := cowrie.DTypeElemSize(b.DType)
	if !ok || elem == 0 {
		// Sub-byte / packed dtypes are out of scope for v2 columnar values.
		return 0, ErrColumnarShapeMismatch
	}
	want := b.NumRows * prodU64(b.ShapeTail) * elem
	if uint64(len(b.Values)) != want {
		return 0, ErrColumnarShapeMismatch
	}
	if b.Validity != nil {
		needBytes := (b.NumRows + 7) / 8
		if uint64(len(b.Validity)) != needBytes {
			return 0, ErrColumnarShapeMismatch
		}
	}
	return elem, nil
}

// columnBuild is an intermediate per-column description used while laying out the
// payload, before chunk offsets are known.
type columnBuild struct {
	path     string
	dtype    uint8
	rawBytes []byte // uncompressed chunk bytes
}

// EncodeColumnarBatchV1 is the pure layout function: it builds the
// ColumnarBatchV1 payload (header + directory + path blob + K-aligned chunks),
// fills ChunkOff/ChunkLen/RawLen/ChunkDigest, and applies optional per-chunk
// zstd. It returns the payload bytes plus the decoded directory (for the writer
// to cross-check / tests to assert). encoding is always PLAIN; compression is
// per-chunk. All ChunkOff values are payload-relative and ≡ 0 mod AlignK.
func EncodeColumnarBatchV1(b *ColumnarTensorBatch) (payload []byte, dir []ColumnDesc, err error) {
	elem, err := validateBatch(b)
	if err != nil {
		return nil, nil, err
	}
	_ = elem

	// --- assemble logical columns in fixed order (determinism) ---
	cols := make([]columnBuild, 0, 3)
	cols = append(cols, columnBuild{
		path:     ColPathValues,
		dtype:    uint8(b.DType),
		rawBytes: b.Values,
	})
	// shape_tail: shared inner shape as LE u64[].
	shapeBytes := make([]byte, 0, len(b.ShapeTail)*8)
	for _, d := range b.ShapeTail {
		shapeBytes = binary.LittleEndian.AppendUint64(shapeBytes, d)
	}
	cols = append(cols, columnBuild{
		path:     ColPathShapeTail,
		dtype:    uint8(cowrie.DTypeUint64),
		rawBytes: shapeBytes,
	})
	if b.Validity != nil {
		cols = append(cols, columnBuild{
			path:     ColPathValidity,
			dtype:    uint8(cowrie.DTypeBool),
			rawBytes: b.Validity,
		})
	}

	numColumns := uint32(len(cols))

	// --- path blob ---
	var pathBlob []byte
	descs := make([]ColumnDesc, len(cols))
	for i := range cols {
		off := uint32(len(pathBlob))
		pathBlob = append(pathBlob, cols[i].path...)
		descs[i].PathID = fnv1a32(cols[i].path)
		descs[i].PathOff = off
		descs[i].PathLen = uint16(len(cols[i].path))
		descs[i].LogicalDType = cols[i].dtype
		descs[i].Encoding = ColumnarEncodingPlain
		descs[i].StatsOff = 0
	}

	// --- on-disk chunk bytes (optional per-chunk zstd) ---
	onDisk := make([][]byte, len(cols))
	for i := range cols {
		raw := cols[i].rawBytes
		descs[i].RawLen = uint64(len(raw))
		// BLAKE3-128 over RAW bytes (leading 16 of blake3.Sum256). Build-time
		// integrity only; never a read gate. (Computed even for empty chunks.)
		sum := blake3.Sum256(raw)
		copy(descs[i].ChunkDigest[:], sum[:16])

		if b.CompressChunks && len(raw) > 0 {
			comp, cerr := compressZstd(raw)
			if cerr == nil && len(comp) < len(raw) {
				onDisk[i] = comp
				descs[i].Compression = ColumnarCompressionZstd
			} else {
				onDisk[i] = raw
				descs[i].Compression = ColumnarCompressionNone
			}
		} else {
			onDisk[i] = raw
			descs[i].Compression = ColumnarCompressionNone
		}
		descs[i].ChunkLen = uint64(len(onDisk[i]))
	}

	// --- compute layout offsets ---
	// header (16) + directory (56*numColumns) + path blob, then pad to K, then
	// each chunk K-aligned with 0x00 padding between.
	dirEnd := uint64(ColumnarHeaderLen) + uint64(ColumnDescLen)*uint64(numColumns)
	blobEnd := dirEnd + uint64(len(pathBlob))
	cursor := align16(blobEnd)
	for i := range cols {
		descs[i].ChunkOff = cursor
		cursor += descs[i].ChunkLen
		cursor = align16(cursor)
	}
	// payload length: last chunk_off + last chunk_len (no trailing align needed,
	// but cursor already rounds up; trim to exact end of last chunk).
	var payloadLen uint64
	if len(cols) == 0 {
		payloadLen = blobEnd
	} else {
		last := descs[len(descs)-1]
		payloadLen = last.ChunkOff + last.ChunkLen
	}

	// --- materialize payload (0x00 pad fill for determinism) ---
	buf := make([]byte, payloadLen)
	binary.LittleEndian.PutUint32(buf[0:4], numColumns)
	binary.LittleEndian.PutUint64(buf[4:12], b.NumRows)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(pathBlob)))
	// directory
	doff := uint64(ColumnarHeaderLen)
	for i := range descs {
		descs[i].encodeInto(buf[doff : doff+ColumnDescLen])
		doff += ColumnDescLen
	}
	// path blob
	copy(buf[dirEnd:blobEnd], pathBlob)
	// chunks
	for i := range cols {
		off := descs[i].ChunkOff
		copy(buf[off:off+descs[i].ChunkLen], onDisk[i])
	}

	return buf, descs, nil
}

// WriteColumnarTensorBatch emits exactly one columnar tensor-batch frame and
// synthesizes one RowGroupEntry with LayoutID = LayoutColumnarV1 and a non-zero
// ChunkDirOff. Any open ROW group is flushed first (a columnar group is
// self-closing and gets its own group). The frame payload is opaque/PLAIN at the
// frame level (FlagMasterCompressed stays 0); per-chunk zstd lives INSIDE the
// payload. chunk_dir_off is the ABSOLUTE file offset of the payload byte 0 (the
// num_columns field) == frameStart + FrameHeaderLen; the directory sits at +16.
func (mw *MasterWriter) WriteColumnarTensorBatch(b *ColumnarTensorBatch) error {
	if mw.closed {
		return ErrMasterClosed
	}
	if err := mw.writePreamble(); err != nil {
		return err
	}

	// Build the payload first (validates shape/dtype/validity, rejects ragged via
	// the fixed-shape-only contract — there is no offsets input).
	payload, _, err := EncodeColumnarBatchV1(b)
	if err != nil {
		return err
	}

	// Close any pending ROW group; columnar gets its own group.
	if mw.groupOpen {
		if err := mw.flushGroup(); err != nil {
			return err
		}
	}

	// Frame abs_offset ≡ 0 mod AlignK.
	if err := mw.padToAlign(); err != nil {
		return err
	}
	groupStart := mw.absOffset

	// Frame flags: deterministic + optional CRC. NO frame-level compression and
	// NO meta (so payload starts at groupStart + FrameHeaderLen).
	var flags uint8
	if mw.opts.Deterministic {
		flags |= FlagMasterDeterministic
	}
	if mw.opts.EnableCRC {
		flags |= FlagMasterCRC
	}
	frame := buildFrame(b.TypeID, flags, 0 /*rawLen*/, nil /*meta*/, payload, mw.opts.EnableCRC)

	payloadAbsStart := groupStart + uint64(FrameHeaderLen)

	if err := mw.writeTracked(frame); err != nil {
		return err
	}

	// Synthesize the columnar group entry directly (NOT via groupOffsets).
	mw.groups = append(mw.groups, RowGroupEntry{
		OrdinalFirst: mw.globalOrdinal,
		RecordCount:  uint32(b.NumRows),
		AbsOffset:    groupStart,
		ByteLen:      mw.absOffset - groupStart,
		TypeID:       b.TypeID,
		LayoutID:     LayoutColumnarV1,
		Flags:        0,
		ChunkDirOff:  payloadAbsStart,
		// GroupDigest zeroed in v2.
	})
	mw.globalOrdinal += b.NumRows
	mw.groupOpen = false // a columnar group is self-closing.
	return nil
}
