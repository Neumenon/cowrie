package codec

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/Neumenon/cowrie/go/v2"
)

// buildF32Values builds N*D contiguous LE float32 bytes from a deterministic ramp.
func buildF32Values(n, d int) ([]byte, []float32) {
	vals := make([]float32, n*d)
	buf := make([]byte, n*d*4)
	for i := range vals {
		vals[i] = float32(i)*0.5 - 3.0
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(vals[i]))
	}
	return buf, vals
}

func writeColumnar(t *testing.T, b *ColumnarTensorBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true, EnableCRC: true})
	if err := mw.WriteColumnarTensorBatch(b); err != nil {
		t.Fatalf("WriteColumnarTensorBatch: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// TestColumnarRoundtripFloat32 — write N=1000 tensors shape_tail=[16], reopen
// indexed, assert columnar group, values byte-exact, shape_tail==[16].
func TestColumnarRoundtripFloat32(t *testing.T) {
	const N, D = 1000, 16
	vbytes, vals := buildF32Values(N, D)
	b := &ColumnarTensorBatch{
		TypeID:    0xABCD1234,
		DType:     cowrie.DTypeFloat32,
		NumRows:   N,
		ShapeTail: []uint64{D},
		Values:    vbytes,
	}
	data := writeColumnar(t, b)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	if len(r.Groups()) != 1 {
		t.Fatalf("want 1 group, got %d", len(r.Groups()))
	}
	if !r.IsColumnarGroup(0) {
		t.Fatal("group 0 should be columnar")
	}
	cv, err := r.OpenColumnarGroup(0)
	if err != nil {
		t.Fatalf("OpenColumnarGroup: %v", err)
	}
	if cv.NumRows() != N {
		t.Fatalf("NumRows: got %d want %d", cv.NumRows(), N)
	}
	if cv.NumColumns() != 2 {
		t.Fatalf("NumColumns: got %d want 2", cv.NumColumns())
	}
	st := cv.ShapeTail()
	if !reflect.DeepEqual(st, []uint64{D}) {
		t.Fatalf("ShapeTail: got %v want [16]", st)
	}
	col, ok := cv.Column(ColPathValues)
	if !ok {
		t.Fatal("missing tensor.values")
	}
	got, ok := col.ValuesFloat32()
	if !ok {
		t.Fatal("ValuesFloat32 not ok")
	}
	if !reflect.DeepEqual(got, vals) {
		t.Fatal("float32 values mismatch")
	}
	if !bytes.Equal(col.RawBytes(), vbytes) {
		t.Fatal("raw bytes mismatch")
	}
}

// TestColumnarZeroCopyAliasing — PLAIN (no compression): the returned []float32
// backing array pointer must lie inside the original file buffer (no copy).
func TestColumnarZeroCopyAliasing(t *testing.T) {
	const N, D = 256, 8
	vbytes, _ := buildF32Values(N, D)
	b := &ColumnarTensorBatch{
		TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes,
	}
	data := writeColumnar(t, b)
	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	cv, err := r.OpenColumnarGroup(0)
	if err != nil {
		t.Fatalf("OpenColumnarGroup: %v", err)
	}
	col, _ := cv.Column(ColPathValues)
	if !col.IsZeroCopy() {
		t.Fatal("PLAIN column should report IsZeroCopy")
	}
	got, ok := col.ValuesFloat32()
	if !ok || len(got) == 0 {
		t.Fatal("ValuesFloat32 failed")
	}
	// Pointer-in-range check: the []float32 base must point inside data[].
	base := uintptr(unsafe.Pointer(&got[0]))
	lo := uintptr(unsafe.Pointer(&data[0]))
	hi := lo + uintptr(len(data))
	if base < lo || base >= hi {
		t.Fatalf("float32 view is a COPY (base %x not in file buffer [%x,%x))", base, lo, hi)
	}
}

// TestColumnarCopyFallbackMisaligned — feed a deliberately misaligned chunk slice
// to the view helper and confirm it falls back (ok=false), exercising the
// alignment guard that the PLAIN path is engineered to avoid.
func TestColumnarCopyFallbackMisaligned(t *testing.T) {
	// Allocate a generous backing buffer and re-slice it at a base pointer that is
	// guaranteed NOT 4-aligned, so the LE fast-path alignment guard forces a
	// fallback (ok=false). We probe offsets until the base pointer is odd-by-4.
	backing := make([]byte, 64)
	var mis []byte
	for off := 0; off < 4; off++ {
		cand := backing[off : off+16]
		if uintptr(unsafe.Pointer(&cand[0]))%4 != 0 {
			mis = cand
			break
		}
	}
	if mis == nil {
		t.Skip("could not obtain a 4-misaligned slice base on this allocator")
	}
	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint32(mis[i*4:], math.Float32bits(float32(i)))
	}
	if _, ok := cowrie.ViewBytesFloat32(mis); ok {
		t.Fatal("expected misaligned view to fall back (ok=false)")
	}
}

// TestColumnarPerChunkZstd — CompressChunks=true => values chunk_len<raw_len, and
// ValuesFloat32 still correct (copy path); frame-level FlagMasterCompressed stays 0.
func TestColumnarPerChunkZstd(t *testing.T) {
	const N, D = 4096, 16
	// Highly compressible: all zeros.
	vbytes := make([]byte, N*D*4)
	b := &ColumnarTensorBatch{
		TypeID: 7, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D},
		Values: vbytes, CompressChunks: true,
	}
	payload, dir, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var valDesc *ColumnDesc
	for i := range dir {
		// values column is index 0
		if i == 0 {
			valDesc = &dir[i]
		}
	}
	if valDesc.Compression != ColumnarCompressionZstd {
		t.Fatalf("values column should be zstd, got compression=%d", valDesc.Compression)
	}
	if !(valDesc.ChunkLen < valDesc.RawLen) {
		t.Fatalf("zstd chunk_len %d should be < raw_len %d", valDesc.ChunkLen, valDesc.RawLen)
	}
	_ = payload

	data := writeColumnar(t, b)
	// Assert frame-level FlagMasterCompressed is NOT set on the columnar frame.
	// Frame starts after the 8-byte preamble (offset 0 of the file). The columnar
	// frame is the first frame; its flags byte is at preamble_end + 5.
	flagsByte := data[PreambleLen+5]
	if flagsByte&FlagMasterCompressed != 0 {
		t.Fatal("frame-level FlagMasterCompressed must stay 0 for columnar")
	}

	r, _ := OpenIndexed(data)
	cv, err := r.OpenColumnarGroup(0)
	if err != nil {
		t.Fatalf("OpenColumnarGroup: %v", err)
	}
	col, _ := cv.Column(ColPathValues)
	if col.IsZeroCopy() {
		t.Fatal("zstd column must NOT report zero-copy")
	}
	got, ok := col.ValuesFloat32()
	if !ok {
		t.Fatal("ValuesFloat32 not ok")
	}
	if len(got) != N*D {
		t.Fatalf("len got %d want %d", len(got), N*D)
	}
	for _, v := range got {
		if v != 0 {
			t.Fatal("decompressed values should all be 0")
		}
	}
}

// TestColumnDescGolden — encodeInto then decodeColumnDesc round-trips all fields;
// ColumnDescLen==64 (stats_off@56, _pad2@60); each field at its pinned offset via a byte-level golden.
func TestColumnDescGolden(t *testing.T) {
	if ColumnDescLen != 64 {
		t.Fatalf("ColumnDescLen must be 64, got %d", ColumnDescLen)
	}
	d := ColumnDesc{
		PathID:       0x11223344,
		PathOff:      0x55667788,
		PathLen:      0x99AA,
		LogicalDType: 0x01,
		Encoding:     0x00,
		Compression:  0x01,
		ChunkOff:     0x1122334455667788,
		ChunkLen:     0x0102030405060708,
		RawLen:       0xA1A2A3A4A5A6A7A8,
		ChunkDigest:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		StatsOff:     0xDEADBEEF,
	}
	var buf [ColumnDescLen]byte
	d.encodeInto(buf[:])

	// Byte-level offsets.
	if got := binary.LittleEndian.Uint32(buf[0:4]); got != d.PathID {
		t.Fatalf("path_id @0: %x", got)
	}
	if got := binary.LittleEndian.Uint32(buf[4:8]); got != d.PathOff {
		t.Fatalf("path_off @4: %x", got)
	}
	if got := binary.LittleEndian.Uint16(buf[8:10]); got != d.PathLen {
		t.Fatalf("path_len @8: %x", got)
	}
	if buf[10] != d.LogicalDType {
		t.Fatalf("logical_dtype @10")
	}
	if buf[11] != d.Encoding {
		t.Fatalf("encoding @11")
	}
	if buf[12] != d.Compression {
		t.Fatalf("compression @12")
	}
	if buf[13] != 0 || buf[14] != 0 || buf[15] != 0 {
		t.Fatalf("pad bytes @13..16 not zero: %v", buf[13:16])
	}
	if got := binary.LittleEndian.Uint64(buf[16:24]); got != d.ChunkOff {
		t.Fatalf("chunk_off @16: %x", got)
	}
	if got := binary.LittleEndian.Uint64(buf[24:32]); got != d.ChunkLen {
		t.Fatalf("chunk_len @24: %x", got)
	}
	if got := binary.LittleEndian.Uint64(buf[32:40]); got != d.RawLen {
		t.Fatalf("raw_len @32: %x", got)
	}
	if !bytes.Equal(buf[40:56], d.ChunkDigest[:]) {
		t.Fatalf("chunk_digest @40")
	}
	if got := binary.LittleEndian.Uint32(buf[56:60]); got != d.StatsOff {
		t.Fatalf("stats_off @56: %x want %x", got, d.StatsOff)
	}
	if buf[60] != 0 || buf[61] != 0 || buf[62] != 0 || buf[63] != 0 {
		t.Fatalf("pad2 bytes @60..64 not zero: %v", buf[60:64])
	}

	back := decodeColumnDesc(buf[:])
	if !reflect.DeepEqual(back, d) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", back, d)
	}
}

// TestColumnarAlignmentInvariant — every chunk_off ≡ 0 mod 16; group AbsOffset
// ≡ 0 mod 16; directory starts at payload+16 (== ChunkDirOff+16).
func TestColumnarAlignmentInvariant(t *testing.T) {
	const N, D = 333, 7
	vbytes, _ := buildF32Values(N, D)
	val := make([]byte, N*8) // validity needs ceil(N/8) bytes
	_ = val
	validity := make([]byte, (N+7)/8)
	for i := range validity {
		validity[i] = 0xFF
	}
	b := &ColumnarTensorBatch{
		TypeID: 2, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D},
		Values: vbytes, Validity: validity,
	}
	payload, dir, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := range dir {
		if dir[i].ChunkOff%uint64(AlignK) != 0 {
			t.Fatalf("col %d chunk_off %d not 16-aligned", i, dir[i].ChunkOff)
		}
	}
	// Directory starts at payload byte 16.
	got := binary.LittleEndian.Uint32(payload[0:4])
	if int(got) != len(dir) {
		t.Fatalf("num_columns mismatch")
	}

	data := writeColumnar(t, b)
	r, _ := OpenIndexed(data)
	g, _ := r.SeekGroup(0)
	if g.AbsOffset%uint64(AlignK) != 0 {
		t.Fatalf("group abs_offset %d not 16-aligned", g.AbsOffset)
	}
	if g.ChunkDirOff == 0 {
		t.Fatal("ChunkDirOff must be non-zero for columnar")
	}
	if g.ChunkDirOff != g.AbsOffset+uint64(FrameHeaderLen) {
		t.Fatalf("ChunkDirOff %d != AbsOffset+24 (%d)", g.ChunkDirOff, g.AbsOffset+uint64(FrameHeaderLen))
	}
}

// TestColumnarLayoutFooter — RowGroupEntry decodes with LayoutID=1, ChunkDirOff!=0,
// and OpenIndexed still verifies the footer digest (corrupting footer fails).
func TestColumnarLayoutFooter(t *testing.T) {
	const N, D = 64, 4
	vbytes, _ := buildF32Values(N, D)
	b := &ColumnarTensorBatch{TypeID: 9, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	data := writeColumnar(t, b)
	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	g := r.Groups()[0]
	if g.LayoutID != LayoutColumnarV1 {
		t.Fatalf("LayoutID got %d want 1", g.LayoutID)
	}
	if g.ChunkDirOff == 0 {
		t.Fatal("ChunkDirOff zero")
	}
	// Corrupt a footer byte => digest mismatch on reopen.
	corrupt := append([]byte(nil), data...)
	footerByte := len(corrupt) - LocatorLen - 1
	corrupt[footerByte] ^= 0xFF
	if _, err := OpenIndexed(corrupt); err != ErrFooterDigestMismatch {
		t.Fatalf("expected footer digest mismatch, got %v", err)
	}
}

// TestColumnarRejectEncodingNonPlain — hand-craft a directory with encoding=1;
// OpenColumnarGroup returns ErrColumnarUnsupportedEncoding.
func TestColumnarRejectEncodingNonPlain(t *testing.T) {
	const N, D = 8, 2
	vbytes, _ := buildF32Values(N, D)
	b := &ColumnarTensorBatch{TypeID: 3, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	payload, _, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Flip the first ColumnDesc.encoding (offset 11 within the desc; desc starts at 16).
	encOff := ColumnarHeaderLen + 11
	payload[encOff] = 1
	if _, err := parseColumnarPayload(payload); err != ErrColumnarUnsupportedEncoding {
		t.Fatalf("expected ErrColumnarUnsupportedEncoding, got %v", err)
	}
}

// TestColumnarRejectUnknownCompression — compression=2 => ErrColumnarUnsupportedCompression.
func TestColumnarRejectUnknownCompression(t *testing.T) {
	const N, D = 8, 2
	vbytes, _ := buildF32Values(N, D)
	b := &ColumnarTensorBatch{TypeID: 3, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	payload, _, _ := EncodeColumnarBatchV1(b)
	compOff := ColumnarHeaderLen + 12
	payload[compOff] = 2
	if _, err := parseColumnarPayload(payload); err != ErrColumnarUnsupportedCompression {
		t.Fatalf("expected ErrColumnarUnsupportedCompression, got %v", err)
	}
}

// TestTypeIDLayoutOrthogonality — write a ROW tensor group AND a COLUMNAR tensor
// group with the SAME type_id; FilterByType returns both; IsColumnarGroup
// distinguishes; the row group is still SeekRecord-able and the columnar one
// returns ErrRecordIsColumnar.
func TestTypeIDLayoutOrthogonality(t *testing.T) {
	const tid uint32 = 0xFEEDFACE
	const N, D = 100, 4
	vbytes, _ := buildF32Values(N, D)

	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true, EnableCRC: true})
	// Row group first.
	for i := 0; i < 3; i++ {
		td := &cowrie.TensorData{DType: cowrie.DTypeFloat32, Dims: []uint64{D}, Data: vbytes[:D*4]}
		if err := mw.Write(td); err != nil {
			t.Fatalf("row write: %v", err)
		}
	}
	// Columnar group with the SAME logical type_id as the row tensors.
	rowGroups := mw.groups
	_ = rowGroups
	// Derive the row tensor's type_id and reuse it for the columnar batch.
	rowTID := cowrie.SchemaFingerprint32(toCowrieValue(&cowrie.TensorData{DType: cowrie.DTypeFloat32, Dims: []uint64{D}, Data: vbytes[:D*4]}))
	cb := &ColumnarTensorBatch{TypeID: rowTID, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	if err := mw.WriteColumnarTensorBatch(cb); err != nil {
		t.Fatalf("columnar write: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := OpenIndexed(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	matches := r.FilterByType(rowTID)
	if len(matches) != 2 {
		t.Fatalf("FilterByType should return both row+columnar groups, got %d", len(matches))
	}
	var rowIdx, colIdx uint32
	colCount := 0
	for _, gi := range matches {
		if r.IsColumnarGroup(gi) {
			colIdx = gi
			colCount++
		} else {
			rowIdx = gi
		}
	}
	if colCount != 1 {
		t.Fatalf("exactly one columnar group expected, got %d", colCount)
	}
	// Row group still SeekRecord-able.
	rowG := r.Groups()[rowIdx]
	if _, err := r.SeekRecord(rowG.OrdinalFirst); err != nil {
		t.Fatalf("row SeekRecord: %v", err)
	}
	// Columnar group SeekRecord returns ErrRecordIsColumnar.
	colG := r.Groups()[colIdx]
	if _, err := r.SeekRecord(colG.OrdinalFirst); err != ErrRecordIsColumnar {
		t.Fatalf("columnar SeekRecord: expected ErrRecordIsColumnar, got %v", err)
	}
}

// TestColumnarValidityBitmap — provide a validity bitmap; Validity() wraps it as
// BitmaskData with correct Get(i) (LSB-first).
func TestColumnarValidityBitmap(t *testing.T) {
	const N, D = 20, 3
	vbytes, _ := buildF32Values(N, D)
	validity := make([]byte, (N+7)/8)
	// Mark even indices valid.
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			validity[i/8] |= 1 << (i % 8)
		}
	}
	b := &ColumnarTensorBatch{TypeID: 5, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes, Validity: validity}
	data := writeColumnar(t, b)
	r, _ := OpenIndexed(data)
	cv, err := r.OpenColumnarGroup(0)
	if err != nil {
		t.Fatalf("OpenColumnarGroup: %v", err)
	}
	bm, ok := cv.Validity()
	if !ok {
		t.Fatal("Validity not present")
	}
	if bm.Count != N {
		t.Fatalf("bitmask count %d want %d", bm.Count, N)
	}
	for i := uint64(0); i < N; i++ {
		want := i%2 == 0
		if bm.Get(i) != want {
			t.Fatalf("Get(%d) got %v want %v", i, bm.Get(i), want)
		}
	}
	// Absent validity => Validity() reports false.
	b2 := &ColumnarTensorBatch{TypeID: 5, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	data2 := writeColumnar(t, b2)
	r2, _ := OpenIndexed(data2)
	cv2, _ := r2.OpenColumnarGroup(0)
	if _, ok := cv2.Validity(); ok {
		t.Fatal("omitted validity column should report not-present")
	}
}

// TestColumnarShapeMismatch — Values length != rows*prod(shape_tail)*elemsize.
func TestColumnarShapeMismatch(t *testing.T) {
	b := &ColumnarTensorBatch{
		TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: 10, ShapeTail: []uint64{4},
		Values: make([]byte, 10*4*4-4), // one element short
	}
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true, EnableCRC: true})
	if err := mw.WriteColumnarTensorBatch(b); err != ErrColumnarShapeMismatch {
		t.Fatalf("expected ErrColumnarShapeMismatch, got %v", err)
	}
	// Wrong validity length.
	b2 := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: 10, ShapeTail: []uint64{4}, Values: make([]byte, 10*4*4), Validity: make([]byte, 1)}
	if _, _, err := EncodeColumnarBatchV1(b2); err != ErrColumnarShapeMismatch {
		t.Fatalf("expected validity-length ErrColumnarShapeMismatch, got %v", err)
	}
}

// TestColumnarDTypeCoverage — roundtrip one column per dtype; wrong-type accessor
// returns ok=false; right-type ok=true.
func TestColumnarDTypeCoverage(t *testing.T) {
	const N = 32

	// int32
	{
		raw := make([]byte, N*4)
		for i := 0; i < N; i++ {
			binary.LittleEndian.PutUint32(raw[i*4:], uint32(int32(i-10)))
		}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeInt32, NumRows: N, ShapeTail: nil, Values: raw}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		col, _ := cv.Column(ColPathValues)
		if _, ok := col.ValuesFloat32(); ok {
			t.Fatal("int32 column should not view as float32")
		}
		got, ok := col.ValuesInt32()
		if !ok || len(got) != N || got[0] != -10 {
			t.Fatalf("int32 view: ok=%v got=%v", ok, got[:1])
		}
	}
	// int64
	{
		raw := make([]byte, N*8)
		for i := 0; i < N; i++ {
			binary.LittleEndian.PutUint64(raw[i*8:], uint64(int64(i*1000)))
		}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeInt64, NumRows: N, Values: raw}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		col, _ := cv.Column(ColPathValues)
		got, ok := col.ValuesInt64()
		if !ok || got[5] != 5000 {
			t.Fatalf("int64 view: ok=%v got[5]=%d", ok, got[5])
		}
	}
	// float64
	{
		raw := make([]byte, N*8)
		for i := 0; i < N; i++ {
			binary.LittleEndian.PutUint64(raw[i*8:], math.Float64bits(float64(i)*0.25))
		}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat64, NumRows: N, Values: raw}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		col, _ := cv.Column(ColPathValues)
		if _, ok := col.ValuesInt64(); ok {
			t.Fatal("float64 should not view as int64")
		}
		got, ok := col.ValuesFloat64()
		if !ok || got[8] != 2.0 {
			t.Fatalf("float64 view: ok=%v got[8]=%v", ok, got[8])
		}
	}
	// uint8
	{
		raw := make([]byte, N)
		for i := 0; i < N; i++ {
			raw[i] = byte(i)
		}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeUint8, NumRows: N, Values: raw}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		col, _ := cv.Column(ColPathValues)
		got, ok := col.ValuesUint8()
		if !ok || got[7] != 7 {
			t.Fatalf("uint8 view: ok=%v got[7]=%d", ok, got[7])
		}
	}
}

// TestColumnarDeterminism — two independent writers, identical input, byte-identical
// payloads (PLAIN, pad=0x00, fixed field order, no timestamps).
func TestColumnarDeterminism(t *testing.T) {
	const N, D = 500, 12
	vbytes, _ := buildF32Values(N, D)
	validity := make([]byte, (N+7)/8)
	for i := range validity {
		validity[i] = 0xA5
	}
	b := &ColumnarTensorBatch{TypeID: 42, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes, Validity: validity}

	p1, _, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode1: %v", err)
	}
	p2, _, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode2: %v", err)
	}
	if !bytes.Equal(p1, p2) {
		t.Fatal("PLAIN payloads not byte-identical")
	}
	// Whole-file determinism too.
	d1 := writeColumnar(t, b)
	d2 := writeColumnar(t, b)
	if !bytes.Equal(d1, d2) {
		t.Fatal("whole-file output not byte-identical (PLAIN)")
	}
}

// TestColumnarRowOverflow — NumRows > MaxUint32 rejected at write.
func TestColumnarRowOverflow(t *testing.T) {
	b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeUint8, NumRows: uint64(math.MaxUint32) + 1, Values: nil}
	if _, _, err := EncodeColumnarBatchV1(b); err != ErrColumnarRowOverflow {
		t.Fatalf("expected ErrColumnarRowOverflow, got %v", err)
	}
}
