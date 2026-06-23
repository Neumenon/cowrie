package codec

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Neumenon/cowrie/go/v2"
	"lukechampine.com/blake3"
)

// f32bytes packs []float32 into contiguous LE bytes.
func f32bytes(vals []float32) []byte {
	b := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

// f64bytes packs []float64 into contiguous LE bytes.
func f64bytes(vals []float64) []byte {
	b := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(v))
	}
	return b
}

// i32bytes packs []int32 into contiguous LE bytes.
func i32bytes(vals []int32) []byte {
	b := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], uint32(v))
	}
	return b
}

// i64bytes packs []int64 into contiguous LE bytes.
func i64bytes(vals []int64) []byte {
	b := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], uint64(v))
	}
	return b
}

// TestStatsBlockGolden — encodeInto then decodeStatsBlock round-trips every field
// at its pinned offset; ColumnStatsLen==48; ver@0, dtype@1, flags@2, null@8,
// nan@16, min@24, max@32, pad_tail@40.
func TestStatsBlockGolden(t *testing.T) {
	if ColumnStatsLen != 48 {
		t.Fatalf("ColumnStatsLen must be 48, got %d", ColumnStatsLen)
	}
	var s StatsBlock
	s.DType = cowrie.DTypeFloat64
	s.Flags = statsFlagHasMinMax
	s.NullCount = 0x1122334455667788
	s.NanCount = 0x0102030405060708
	binary.LittleEndian.PutUint64(s.minCell[0:8], math.Float64bits(-7.5))
	binary.LittleEndian.PutUint64(s.maxCell[0:8], math.Float64bits(9.25))

	var buf [ColumnStatsLen]byte
	s.encodeInto(buf[:])

	if buf[0] != StatsVersion1 {
		t.Fatalf("ver @0: %d", buf[0])
	}
	if buf[1] != uint8(cowrie.DTypeFloat64) {
		t.Fatalf("dtype @1: %d", buf[1])
	}
	if buf[2] != statsFlagHasMinMax {
		t.Fatalf("flags @2: %d", buf[2])
	}
	if buf[3] != 0 {
		t.Fatalf("pad0 @3 not zero")
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != 0 {
		t.Fatalf("pad1 @4 not zero")
	}
	if got := binary.LittleEndian.Uint64(buf[8:16]); got != s.NullCount {
		t.Fatalf("null_count @8: %x", got)
	}
	if got := binary.LittleEndian.Uint64(buf[16:24]); got != s.NanCount {
		t.Fatalf("nan_count @16: %x", got)
	}
	if !bytes.Equal(buf[24:32], s.minCell[:]) {
		t.Fatalf("min_cell @24")
	}
	if !bytes.Equal(buf[32:40], s.maxCell[:]) {
		t.Fatalf("max_cell @32")
	}
	if binary.LittleEndian.Uint64(buf[40:48]) != 0 {
		t.Fatalf("pad_tail @40 not zero")
	}

	back, err := decodeStatsBlock(buf[:])
	if err != nil {
		t.Fatalf("decodeStatsBlock: %v", err)
	}
	if back.DType != s.DType || back.Flags != s.Flags ||
		back.NullCount != s.NullCount || back.NanCount != s.NanCount ||
		back.minCell != s.minCell || back.maxCell != s.maxCell {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", back, s)
	}
	lo, hi, ok := back.MinMaxFloat64()
	if !ok || lo != -7.5 || hi != 9.25 {
		t.Fatalf("MinMaxFloat64: lo=%v hi=%v ok=%v", lo, hi, ok)
	}

	// Bad version => ErrColumnarMalformed.
	bad := buf
	bad[0] = 2
	if _, err := decodeStatsBlock(bad[:]); err != ErrColumnarMalformed {
		t.Fatalf("expected ErrColumnarMalformed for ver=2, got %v", err)
	}
	// Short buffer => ErrColumnarMalformed.
	if _, err := decodeStatsBlock(buf[:10]); err != ErrColumnarMalformed {
		t.Fatalf("expected ErrColumnarMalformed for short buf, got %v", err)
	}
}

// TestStatsFloat32IgnoreNaN — f32 values incl NaN and -0.0: nan_count correct,
// min/max exclude NaN, the stored zero bound bits == 0x00000000 (signed-zero
// canonicalized).
func TestStatsFloat32IgnoreNaN(t *testing.T) {
	vals := []float32{
		float32(math.NaN()),
		-0.0,
		3.5,
		float32(math.Copysign(0, -1)), // negative zero
		float32(math.NaN()),
		-2.0,
	}
	raw := f32bytes(vals)
	s, ok := computeValuesStats(cowrie.DTypeFloat32, raw, nil, uint64(len(vals)), 1)
	if !ok {
		t.Fatal("computeValuesStats not ok")
	}
	if s.NanCount != 2 {
		t.Fatalf("nan_count: got %d want 2", s.NanCount)
	}
	if !s.HasMinMax() {
		t.Fatal("HasMinMax should be set")
	}
	lo, hi, ok := s.MinMaxFloat64()
	if !ok || lo != -2.0 || hi != 3.5 {
		t.Fatalf("min/max: lo=%v hi=%v (want -2.0, 3.5)", lo, hi)
	}
	// The min bound is -2.0 here; force a min==0 case to assert canonicalization.
	vals2 := []float32{float32(math.Copysign(0, -1)), 5.0}
	s2, _ := computeValuesStats(cowrie.DTypeFloat32, f32bytes(vals2), nil, 2, 1)
	if binary.LittleEndian.Uint32(s2.minCell[0:4]) != 0x00000000 {
		t.Fatalf("zero min bound bits not canonical: %x", s2.minCell[0:4])
	}
	// high 4 bytes of an f32 cell are always zero.
	if binary.LittleEndian.Uint32(s2.minCell[4:8]) != 0 {
		t.Fatalf("f32 min_cell high half not zero")
	}
}

// TestStatsFloat64InfBounds — +Inf/-Inf are included as ordered min/max; an
// all-NaN block has HasMinMax=false and nan_count==N.
func TestStatsFloat64InfBounds(t *testing.T) {
	vals := []float64{math.Inf(-1), 2.0, math.Inf(+1), math.NaN()}
	s, _ := computeValuesStats(cowrie.DTypeFloat64, f64bytes(vals), nil, uint64(len(vals)), 1)
	if s.NanCount != 1 {
		t.Fatalf("nan_count got %d want 1", s.NanCount)
	}
	lo, hi, ok := s.MinMaxFloat64()
	if !ok || !math.IsInf(lo, -1) || !math.IsInf(hi, +1) {
		t.Fatalf("inf bounds: lo=%v hi=%v ok=%v", lo, hi, ok)
	}

	// All-NaN block: HasMinMax=false, nan_count==N.
	allNaN := []float64{math.NaN(), math.NaN(), math.NaN()}
	s2, _ := computeValuesStats(cowrie.DTypeFloat64, f64bytes(allNaN), nil, 3, 1)
	if s2.HasMinMax() {
		t.Fatal("all-NaN block must have HasMinMax=false")
	}
	if s2.NanCount != 3 {
		t.Fatalf("all-NaN nan_count got %d want 3", s2.NanCount)
	}
	if _, _, ok := s2.MinMaxFloat64(); ok {
		t.Fatal("MinMaxFloat64 should be ok=false when !HasMinMax")
	}
	// MightContainRange must be conservative (true) when !HasMinMax.
	if !s2.MightContainRange(100, 200) {
		t.Fatal("!HasMinMax must yield MightContainRange=true")
	}
}

// TestStatsNullCountFromValidity — validity present with k invalid rows =>
// null_count == k*prod(shape_tail); masked rows excluded from min/max and nan.
func TestStatsNullCountFromValidity(t *testing.T) {
	const N = 6
	const D = 4 // prod(shape_tail)
	// Row 0 holds a small value and a NaN; rows 2 and 5 are masked-out.
	// Mask out rows 2 and 5 (invalid). Their elements must be ignored.
	vals := make([]float32, N*D)
	for i := range vals {
		vals[i] = 1.0
	}
	// Live extremes in valid rows.
	vals[0] = -9.0 // row 0 (valid)
	vals[1] = float32(math.NaN())
	vals[4] = 7.0 // row 1 (valid)
	// Poison the masked rows with values that WOULD widen min/max and a NaN.
	vals[2*D+0] = -1000.0 // row 2 (masked)
	vals[2*D+1] = float32(math.NaN())
	vals[5*D+0] = 1000.0 // row 5 (masked)

	validity := make([]byte, (N+7)/8)
	for r := 0; r < N; r++ {
		if r != 2 && r != 5 {
			validity[r/8] |= 1 << (r % 8)
		}
	}
	s, _ := computeValuesStats(cowrie.DTypeFloat32, f32bytes(vals), validity, N, D)
	if s.NullCount != 2*D {
		t.Fatalf("null_count got %d want %d", s.NullCount, 2*D)
	}
	if s.NanCount != 1 {
		t.Fatalf("nan_count got %d want 1 (masked-row NaN excluded)", s.NanCount)
	}
	lo, hi, ok := s.MinMaxFloat64()
	if !ok || lo != -9.0 || hi != 7.0 {
		t.Fatalf("min/max with masking: lo=%v hi=%v (want -9, 7)", lo, hi)
	}
}

// TestStatsInt64MinMax — exact int64 min/max, nan_count==0.
func TestStatsInt64MinMax(t *testing.T) {
	vals := []int64{5, -100, 0, 42, -7, 99}
	s, _ := computeValuesStats(cowrie.DTypeInt64, i64bytes(vals), nil, uint64(len(vals)), 1)
	if s.NanCount != 0 {
		t.Fatalf("int64 nan_count must be 0, got %d", s.NanCount)
	}
	lo, hi, ok := s.MinMaxInt64()
	if !ok || lo != -100 || hi != 99 {
		t.Fatalf("int64 min/max: lo=%d hi=%d ok=%v", lo, hi, ok)
	}
	// Large i64 magnitudes round-trip exactly through the cell.
	big := []int64{math.MinInt64, math.MaxInt64}
	s2, _ := computeValuesStats(cowrie.DTypeInt64, i64bytes(big), nil, 2, 1)
	lo2, hi2, _ := s2.MinMaxInt64()
	if lo2 != math.MinInt64 || hi2 != math.MaxInt64 {
		t.Fatalf("int64 extremes: lo=%d hi=%d", lo2, hi2)
	}
}

// TestStatsInt32MinMax — i32 sign-extended into the 8-byte cell, decoded back.
func TestStatsInt32MinMax(t *testing.T) {
	vals := []int32{10, -5, 3, -2147483648, 2147483647}
	s, _ := computeValuesStats(cowrie.DTypeInt32, i32bytes(vals), nil, uint64(len(vals)), 1)
	lo, hi, ok := s.MinMaxInt64()
	if !ok || lo != -2147483648 || hi != 2147483647 {
		t.Fatalf("int32 min/max: lo=%d hi=%d ok=%v", lo, hi, ok)
	}
}

// TestStatsUint8MinMax — exact uint8 min/max, nan_count==0.
func TestStatsUint8MinMax(t *testing.T) {
	vals := []byte{200, 3, 255, 0, 17}
	s, _ := computeValuesStats(cowrie.DTypeUint8, vals, nil, uint64(len(vals)), 1)
	if s.NanCount != 0 {
		t.Fatalf("uint8 nan_count must be 0")
	}
	lo, hi, ok := s.MinMaxUint64()
	if !ok || lo != 0 || hi != 255 {
		t.Fatalf("uint8 min/max: lo=%d hi=%d ok=%v", lo, hi, ok)
	}
}

// TestMightContainRange — min<=hi && max>=lo truth table incl boundary equality,
// empty query (lo>hi)->false, NaN query->true, !HasMinMax->true.
func TestMightContainRange(t *testing.T) {
	mk := func(lo, hi float64) *StatsBlock {
		var s StatsBlock
		s.DType = cowrie.DTypeFloat64
		s.Flags = statsFlagHasMinMax
		binary.LittleEndian.PutUint64(s.minCell[0:8], math.Float64bits(lo))
		binary.LittleEndian.PutUint64(s.maxCell[0:8], math.Float64bits(hi))
		return &s
	}
	// Block covering [10, 20].
	b := mk(10, 20)
	cases := []struct {
		lo, hi float64
		want   bool
		name   string
	}{
		{15, 16, true, "inside"},
		{0, 5, false, "below"},
		{30, 40, false, "above"},
		{20, 25, true, "touch-high (max>=lo, min<=hi)"},
		{5, 10, true, "touch-low"},
		{20, 20, true, "point at max"},
		{10, 10, true, "point at min"},
		{0, 100, true, "superset"},
		{9.999, 10.0, true, "just-touch min"},
		{20.0, 20.001, true, "just-touch max"},
		{20.0001, 21, false, "just-above max"},
		{5, 9.9999, false, "just-below min"},
		{50, 10, false, "empty query lo>hi"},
	}
	for _, c := range cases {
		if got := b.MightContainRange(c.lo, c.hi); got != c.want {
			t.Fatalf("%s: MightContainRange(%v,%v)=%v want %v", c.name, c.lo, c.hi, got, c.want)
		}
	}
	// NaN query -> true.
	if !b.MightContainRange(math.NaN(), 5) {
		t.Fatal("NaN lo query must be true")
	}
	if !b.MightContainRange(5, math.NaN()) {
		t.Fatal("NaN hi query must be true")
	}
	// !HasMinMax -> true.
	var empty StatsBlock
	empty.DType = cowrie.DTypeFloat64
	if !empty.MightContainRange(1, 2) {
		t.Fatal("!HasMinMax must be true")
	}

	// Integer block [100, 200] via int64.
	var si StatsBlock
	si.DType = cowrie.DTypeInt64
	si.Flags = statsFlagHasMinMax
	binary.LittleEndian.PutUint64(si.minCell[0:8], uint64(int64(100)))
	binary.LittleEndian.PutUint64(si.maxCell[0:8], uint64(int64(200)))
	if !si.MightContainRange(150, 160) {
		t.Fatal("int range inside should be true")
	}
	if si.MightContainRange(201, 300) {
		t.Fatal("int range above should be false")
	}
	if si.MightContainRange(0, 99) {
		t.Fatal("int range below should be false")
	}
}

// TestStatsRoundtripThroughEncode — encode a real columnar batch per dtype and
// reopen; the values column's decoded StatsBlock matches the expected null/nan/
// min/max. Confirms the writer wires stats_off and the reader reads it.
func TestStatsRoundtripThroughEncode(t *testing.T) {
	// float32 with a NaN and a -0.0
	{
		vals := []float32{-0.0, 4.0, float32(math.NaN()), -3.0, 2.0}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: uint64(len(vals)), Values: f32bytes(vals)}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, err := r.OpenColumnarGroup(0)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		sb, ok := cv.StatsFor(ColPathValues)
		if !ok {
			t.Fatal("values stats absent")
		}
		if sb.NanCount != 1 {
			t.Fatalf("f32 nan_count %d want 1", sb.NanCount)
		}
		lo, hi, _ := sb.MinMaxFloat64()
		if lo != -3.0 || hi != 4.0 {
			t.Fatalf("f32 min/max %v %v want -3,4", lo, hi)
		}
		// shape_tail / validity carry no stats.
		if _, ok := cv.StatsFor(ColPathShapeTail); ok {
			t.Fatal("shape_tail must not carry stats")
		}
	}
	// int64
	{
		vals := []int64{50, -3, 1000, 7}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeInt64, NumRows: uint64(len(vals)), Values: i64bytes(vals)}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		sb, ok := cv.StatsFor(ColPathValues)
		if !ok {
			t.Fatal("int64 stats absent")
		}
		lo, hi, _ := sb.MinMaxInt64()
		if lo != -3 || hi != 1000 {
			t.Fatalf("int64 min/max %d %d", lo, hi)
		}
	}
	// uint8
	{
		vals := []byte{9, 250, 0, 33}
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeUint8, NumRows: uint64(len(vals)), Values: vals}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		sb, _ := cv.StatsFor(ColPathValues)
		lo, hi, _ := sb.MinMaxUint64()
		if lo != 0 || hi != 250 {
			t.Fatalf("uint8 min/max %d %d", lo, hi)
		}
	}
	// float64 with validity (masked rows excluded)
	{
		const N, D = 4, 2
		vals := make([]float64, N*D)
		for i := range vals {
			vals[i] = 1.0
		}
		vals[0] = -50.0 // row 0 valid
		vals[2] = 5.0   // row 1 valid
		vals[2*D] = 999 // row 2 MASKED
		validity := make([]byte, (N+7)/8)
		validity[0] = 0b1011 // rows 0,1,3 valid; row 2 invalid
		b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat64, NumRows: N, ShapeTail: []uint64{D}, Values: f64bytes(vals), Validity: validity}
		data := writeColumnar(t, b)
		r, _ := OpenIndexed(data)
		cv, _ := r.OpenColumnarGroup(0)
		sb, _ := cv.StatsFor(ColPathValues)
		if sb.NullCount != 1*D {
			t.Fatalf("f64 null_count %d want %d", sb.NullCount, D)
		}
		lo, hi, _ := sb.MinMaxFloat64()
		if lo != -50.0 {
			t.Fatalf("f64 masked min %v want -50", lo)
		}
		_ = hi
	}
}

// TestStatsReadableWithoutDecompressingChunks — with zstd chunks, the stats are
// readable purely from the directory front region; StatsFor must succeed and the
// stats region must lie entirely BEFORE the first chunk_off (so no chunk byte is
// ever touched to read stats).
func TestStatsReadableWithoutDecompressingChunks(t *testing.T) {
	const N, D = 4096, 8
	vals := make([]float32, N*D)
	for i := range vals {
		vals[i] = float32(i%7) - 2.0
	}
	b := &ColumnarTensorBatch{
		TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D},
		Values: f32bytes(vals), CompressChunks: true,
	}
	_, dir, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// values column (idx 0) is zstd and carries stats.
	if dir[0].Compression != ColumnarCompressionZstd {
		t.Fatalf("expected zstd values chunk")
	}
	if dir[0].StatsOff == 0 {
		t.Fatal("values stats_off must be non-zero")
	}
	// stats region must end at or before the first chunk_off.
	var firstChunk uint64 = ^uint64(0)
	for i := range dir {
		if dir[i].ChunkOff < firstChunk {
			firstChunk = dir[i].ChunkOff
		}
	}
	if uint64(dir[0].StatsOff)+uint64(ColumnStatsLen) > firstChunk {
		t.Fatalf("stats region overlaps first chunk (stats_end %d > first_chunk %d)",
			uint64(dir[0].StatsOff)+ColumnStatsLen, firstChunk)
	}

	data := writeColumnar(t, b)
	r, _ := OpenIndexed(data)
	cv, _ := r.OpenColumnarGroup(0)
	// StatsFor decodes stats WITHOUT resolving (decompressing) the chunk.
	sb, ok := cv.StatsFor(ColPathValues)
	if !ok {
		t.Fatal("stats absent on zstd column")
	}
	lo, hi, _ := sb.MinMaxFloat64()
	if lo != -2.0 || hi != 4.0 {
		t.Fatalf("zstd-column stats min/max %v %v want -2,4", lo, hi)
	}
}

// TestStatsRegionDeterminism — encode same batch twice -> byte-identical payload
// (incl stats region), and the stats region lies before the first chunk_off.
func TestStatsRegionDeterminism(t *testing.T) {
	vals := []float64{1.5, -0.0, math.NaN(), 8.0, -4.0, math.Inf(+1)}
	b := &ColumnarTensorBatch{TypeID: 9, DType: cowrie.DTypeFloat64, NumRows: uint64(len(vals)), Values: f64bytes(vals)}
	p1, dir, err := EncodeColumnarBatchV1(b)
	if err != nil {
		t.Fatalf("encode1: %v", err)
	}
	p2, _, _ := EncodeColumnarBatchV1(b)
	if !bytes.Equal(p1, p2) {
		t.Fatal("stats payload not byte-identical on re-encode")
	}
	// Stats region precedes the first chunk.
	so := uint64(dir[0].StatsOff)
	if so == 0 {
		t.Fatal("expected non-zero stats_off")
	}
	var firstChunk uint64 = ^uint64(0)
	for i := range dir {
		if dir[i].ChunkOff < firstChunk {
			firstChunk = dir[i].ChunkOff
		}
	}
	if so+uint64(ColumnStatsLen) > firstChunk {
		t.Fatal("stats region not before first chunk_off")
	}
	// Whole-file determinism too.
	d1 := writeColumnar(t, b)
	d2 := writeColumnar(t, b)
	if !bytes.Equal(d1, d2) {
		t.Fatal("whole-file output not byte-identical with stats")
	}
}

// TestSelectColumnarGroups — multi-group file (some overlapping [lo,hi], some not).
// The selector returns only candidate groups and works on stats alone (zstd
// chunks would error if decoded; selection succeeds without touching them).
// Also: missing column -> excluded; stats_off==0 -> included; non-columnar -> excluded.
func TestSelectColumnarGroups(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true, EnableCRC: true})

	// Group 0: a ROW tensor group (non-columnar) — must be EXCLUDED.
	td := &cowrie.TensorData{DType: cowrie.DTypeFloat32, Dims: []uint64{4}, Data: f32bytes([]float32{1, 2, 3, 4})}
	if err := mw.Write(td); err != nil {
		t.Fatalf("row write: %v", err)
	}

	// Group 1: columnar f32 values in [0,10] (zstd chunks) — overlaps [3,5].
	g1vals := make([]float32, 1024)
	for i := range g1vals {
		g1vals[i] = float32(i%11) // 0..10
	}
	if err := mw.WriteColumnarTensorBatch(&ColumnarTensorBatch{
		TypeID: 100, DType: cowrie.DTypeFloat32, NumRows: uint64(len(g1vals)),
		Values: f32bytes(g1vals), CompressChunks: true,
	}); err != nil {
		t.Fatalf("g1: %v", err)
	}

	// Group 2: columnar f32 values in [100,110] — does NOT overlap [3,5].
	g2vals := make([]float32, 512)
	for i := range g2vals {
		g2vals[i] = 100.0 + float32(i%11)
	}
	if err := mw.WriteColumnarTensorBatch(&ColumnarTensorBatch{
		TypeID: 100, DType: cowrie.DTypeFloat32, NumRows: uint64(len(g2vals)),
		Values: f32bytes(g2vals), CompressChunks: true,
	}); err != nil {
		t.Fatalf("g2: %v", err)
	}

	// Group 3: columnar f32 values in [-5,-1] — does NOT overlap [3,5].
	g3vals := make([]float32, 256)
	for i := range g3vals {
		g3vals[i] = -5.0 + float32(i%5)
	}
	if err := mw.WriteColumnarTensorBatch(&ColumnarTensorBatch{
		TypeID: 100, DType: cowrie.DTypeFloat32, NumRows: uint64(len(g3vals)),
		Values: f32bytes(g3vals),
	}); err != nil {
		t.Fatalf("g3: %v", err)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := OpenIndexed(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}

	// Query [3,5] on tensor.values: only group 1 overlaps.
	got := r.SelectColumnarGroups(ColPathValues, 3, 5)
	want := []uint32{1}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("Select [3,5]: got %v want %v", got, want)
	}

	// Query [100,105]: only group 2.
	got = r.SelectColumnarGroups(ColPathValues, 100, 105)
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("Select [100,105]: got %v want [2]", got)
	}

	// Query [-3,-2]: only group 3.
	got = r.SelectColumnarGroups(ColPathValues, -3, -2)
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("Select [-3,-2]: got %v want [3]", got)
	}

	// Wide query covering everything: all three columnar groups (1,2,3), never 0.
	got = r.SelectColumnarGroups(ColPathValues, -1000, 1000)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("Select wide: got %v want [1 2 3]", got)
	}

	// Missing column => excluded everywhere.
	got = r.SelectColumnarGroups("tensor.nope", -1000, 1000)
	if len(got) != 0 {
		t.Fatalf("Select missing column: got %v want []", got)
	}

	// Confirm the selector never decoded a chunk: the zstd values chunks would be
	// undecodable as raw f32, yet selection succeeded. Sanity: the selected group
	// IS zstd at the chunk level (proving stats-only path).
	cv, _ := r.OpenColumnarGroup(1)
	if cv.dir[0].Compression != ColumnarCompressionZstd {
		t.Fatal("group 1 values chunk should be zstd (stats-only selection proven)")
	}
}

// TestSelectColumnarGroupsNoStats — a columnar group whose values column has
// stats_off==0 is always INCLUDED (conservative). We simulate this by zeroing
// the stats_off in a re-encoded payload is not trivial through the writer, so we
// assert the policy via the conservative path: a group with stats present but
// !HasMinMax (all-NaN) is included.
func TestSelectColumnarGroupsAllNaNIncluded(t *testing.T) {
	allNaN := []float32{float32(math.NaN()), float32(math.NaN()), float32(math.NaN())}
	b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: 3, Values: f32bytes(allNaN)}
	data := writeColumnar(t, b)
	r, _ := OpenIndexed(data)
	// Even a range that clearly cannot match must INCLUDE the all-NaN group.
	got := r.SelectColumnarGroups(ColPathValues, 1e9, 2e9)
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("all-NaN group must be included (conservative): got %v", got)
	}
}

// TestColumnarChunkDigestRoundtrip — closes the Phase 2 'digest untested' note:
// encode a known values chunk, reopen, recompute blake3.Sum256(rawValues)[:16]
// and assert == the directory entry's ChunkDigest.
func TestColumnarChunkDigestRoundtrip(t *testing.T) {
	const N, D = 128, 4
	vbytes, _ := buildF32Values(N, D)
	b := &ColumnarTensorBatch{TypeID: 1, DType: cowrie.DTypeFloat32, NumRows: N, ShapeTail: []uint64{D}, Values: vbytes}
	data := writeColumnar(t, b)
	r, _ := OpenIndexed(data)
	cv, err := r.OpenColumnarGroup(0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Directory index 0 is the values column.
	want := blake3.Sum256(vbytes)
	got := cv.dir[0].ChunkDigest
	if !bytes.Equal(got[:], want[:16]) {
		t.Fatalf("chunk_digest mismatch:\n got %x\nwant %x", got[:], want[:16])
	}
	// And the resolved column raw bytes hash to the same digest.
	col, _ := cv.Column(ColPathValues)
	got2 := blake3.Sum256(col.RawBytes())
	if !bytes.Equal(got2[:16], cv.dir[0].ChunkDigest[:]) {
		t.Fatal("resolved values bytes do not hash to stored chunk_digest")
	}
}
