package codec

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go/v2"
)

// noCompressNoCRCOpts disables compression and CRC so frame bytes are
// predictable and small (keeps the determinism/seek tests fast).
func noCompressNoCRCOpts() MasterWriterOptions {
	return MasterWriterOptions{
		Deterministic: true,
		Compression:   cowrie.CompressionNone,
		EnableCRC:     false,
	}
}

// writeClosed writes the given values with the given options and returns the
// finalized (Closed) byte stream.
func writeClosed(t *testing.T, opts MasterWriterOptions, vals []any) []byte {
	t.Helper()
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, opts)
	for _, v := range vals {
		if err := mw.Write(v); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// recOf builds a single-field record whose schema (hence type_id) depends only
// on the key, so changing the key forces a type change / group boundary.
func recOf(key string, n int) map[string]any {
	return map[string]any{key: int64(n)}
}

func TestIndexedEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, noCompressNoCRCOpts())
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data := buf.Bytes()

	// Empty stream = preamble (8) + locator (44), footer_len = 0.
	if len(data) != PreambleLen+LocatorLen {
		t.Fatalf("empty stream length = %d, want %d", len(data), PreambleLen+LocatorLen)
	}

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed(empty): %v", err)
	}
	if len(r.Groups()) != 0 {
		t.Errorf("empty stream groups = %d, want 0", len(r.Groups()))
	}
	if r.NumRecords() != 0 {
		t.Errorf("empty stream records = %d, want 0", r.NumRecords())
	}
	if _, err := r.SeekRecord(0); err != ErrRecordOutOfRange {
		t.Errorf("SeekRecord(0) on empty: got %v, want ErrRecordOutOfRange", err)
	}
}

func TestIndexedPartialGroup(t *testing.T) {
	const n = 10
	vals := make([]any, n)
	for i := 0; i < n; i++ {
		vals[i] = recOf("k", i)
	}
	data := writeClosed(t, noCompressNoCRCOpts(), vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	if len(r.Groups()) != 1 {
		t.Fatalf("groups = %d, want 1 (single partial group)", len(r.Groups()))
	}
	g := r.Groups()[0]
	if g.RecordCount != n {
		t.Errorf("record_count = %d, want %d", g.RecordCount, n)
	}
	if g.OrdinalFirst != 0 {
		t.Errorf("ordinal_first = %d, want 0", g.OrdinalFirst)
	}
	if g.LayoutID != LayoutRow {
		t.Errorf("layout_id = %d, want %d (row)", g.LayoutID, LayoutRow)
	}
	if g.ChunkDirOff != 0 {
		t.Errorf("chunk_dir_off = %d, want 0", g.ChunkDirOff)
	}
	if g.GroupDigest != ([16]byte{}) {
		t.Errorf("group_digest = %x, want zeroed", g.GroupDigest)
	}
	if g.AbsOffset%AlignK != 0 {
		t.Errorf("abs_offset = %d not %d-aligned", g.AbsOffset, AlignK)
	}
	if r.NumRecords() != n {
		t.Errorf("NumRecords = %d, want %d", r.NumRecords(), n)
	}

	for i := 0; i < n; i++ {
		assertRecordKeyVal(t, r, uint64(i), "k", i)
	}
}

func TestIndexedExactlyRowGroupRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 65536-record group test in -short mode")
	}
	const n = RowGroupRecords
	vals := make([]any, n)
	for i := 0; i < n; i++ {
		vals[i] = recOf("k", i)
	}
	data := writeClosed(t, noCompressNoCRCOpts(), vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	// Exactly one full group; no trailing empty group.
	if len(r.Groups()) != 1 {
		t.Fatalf("groups = %d, want exactly 1 full group", len(r.Groups()))
	}
	if r.Groups()[0].RecordCount != RowGroupRecords {
		t.Errorf("record_count = %d, want %d", r.Groups()[0].RecordCount, RowGroupRecords)
	}
	if r.NumRecords() != n {
		t.Errorf("NumRecords = %d, want %d", r.NumRecords(), n)
	}
	// Spot-check first, middle, last.
	for _, k := range []uint64{0, RowGroupRecords / 2, RowGroupRecords - 1} {
		assertRecordKeyVal(t, r, k, "k", int(k))
	}
}

func TestIndexedRowGroupRecordsPlusOne(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 65537-record group test in -short mode")
	}
	const n = RowGroupRecords + 1
	vals := make([]any, n)
	for i := 0; i < n; i++ {
		vals[i] = recOf("k", i)
	}
	data := writeClosed(t, noCompressNoCRCOpts(), vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	// Count overflow forces a second group even with one type_id.
	if len(r.Groups()) != 2 {
		t.Fatalf("groups = %d, want 2 (count overflow)", len(r.Groups()))
	}
	if r.Groups()[0].RecordCount != RowGroupRecords {
		t.Errorf("group0 record_count = %d, want %d", r.Groups()[0].RecordCount, RowGroupRecords)
	}
	if r.Groups()[1].RecordCount != 1 {
		t.Errorf("group1 record_count = %d, want 1", r.Groups()[1].RecordCount)
	}
	if r.Groups()[1].OrdinalFirst != RowGroupRecords {
		t.Errorf("group1 ordinal_first = %d, want %d", r.Groups()[1].OrdinalFirst, RowGroupRecords)
	}
	// The boundary record (ordinal 65536) must resolve to the second group.
	assertRecordKeyVal(t, r, RowGroupRecords, "k", RowGroupRecords)
}

func TestIndexedTypeChangeForcesTwoGroups(t *testing.T) {
	var vals []any
	for i := 0; i < 10; i++ {
		vals = append(vals, recOf("a", i)) // type A
	}
	for i := 0; i < 10; i++ {
		vals = append(vals, recOf("b", i)) // type B (different key => different type_id)
	}
	data := writeClosed(t, noCompressNoCRCOpts(), vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	if len(r.Groups()) != 2 {
		t.Fatalf("groups = %d, want 2 (type change boundary)", len(r.Groups()))
	}
	if r.Groups()[0].TypeID == r.Groups()[1].TypeID {
		t.Errorf("two groups share type_id %#x; expected distinct", r.Groups()[0].TypeID)
	}
	if r.Groups()[0].RecordCount != 10 || r.Groups()[1].RecordCount != 10 {
		t.Errorf("record counts = %d/%d, want 10/10",
			r.Groups()[0].RecordCount, r.Groups()[1].RecordCount)
	}

	// FilterByType returns matching group indices without reading payloads.
	tA := r.Groups()[0].TypeID
	tB := r.Groups()[1].TypeID
	if got := r.FilterByType(tA); len(got) != 1 || got[0] != 0 {
		t.Errorf("FilterByType(A) = %v, want [0]", got)
	}
	if got := r.FilterByType(tB); len(got) != 1 || got[0] != 1 {
		t.Errorf("FilterByType(B) = %v, want [1]", got)
	}
	if got := r.FilterByType(0xDEADBEEF); len(got) != 0 {
		t.Errorf("FilterByType(missing) = %v, want empty", got)
	}

	// Records 0..9 are key "a"=i; 10..19 are key "b"=i-10.
	for i := 0; i < 10; i++ {
		assertRecordKeyVal(t, r, uint64(i), "a", i)
	}
	for i := 0; i < 10; i++ {
		assertRecordKeyVal(t, r, uint64(10+i), "b", i)
	}
}

func TestIndexedMultiGroupSeekByOrdinal(t *testing.T) {
	// Alternate between three types in blocks so many small groups form, then
	// verify SeekRecord(k) returns the right payload for every k.
	var vals []any
	var wantKey []string
	var wantVal []int
	keys := []string{"x", "y", "z"}
	rec := 0
	for block := 0; block < 12; block++ {
		key := keys[block%len(keys)]
		blockLen := 1 + block%7 // varying group sizes
		for j := 0; j < blockLen; j++ {
			vals = append(vals, recOf(key, rec))
			wantKey = append(wantKey, key)
			wantVal = append(wantVal, rec)
			rec++
		}
	}
	data := writeClosed(t, noCompressNoCRCOpts(), vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	if r.NumRecords() != uint64(len(vals)) {
		t.Fatalf("NumRecords = %d, want %d", r.NumRecords(), len(vals))
	}

	// Every k, including a forward and reverse pass to exercise the binary
	// search from both directions.
	for k := 0; k < len(vals); k++ {
		assertRecordKeyVal(t, r, uint64(k), wantKey[k], wantVal[k])
	}
	for k := len(vals) - 1; k >= 0; k-- {
		assertRecordKeyVal(t, r, uint64(k), wantKey[k], wantVal[k])
	}

	// Out-of-range ordinal.
	if _, err := r.SeekRecord(uint64(len(vals))); err != ErrRecordOutOfRange {
		t.Errorf("SeekRecord(N): got %v, want ErrRecordOutOfRange", err)
	}

	// SeekGroup bounds.
	if _, err := r.SeekGroup(uint32(len(r.Groups()))); err != ErrGroupOutOfRange {
		t.Errorf("SeekGroup(out of range): got %v, want ErrGroupOutOfRange", err)
	}
	if _, err := r.SeekGroup(0); err != nil {
		t.Errorf("SeekGroup(0): %v", err)
	}
}

func TestIndexedSeekWithCompression(t *testing.T) {
	// SeekRecord must transparently decompress. Use a payload large enough to
	// trigger compression (the writer only compresses >= 256 bytes).
	var vals []any
	for i := 0; i < 5; i++ {
		vals = append(vals, map[string]any{
			"k":    int64(i),
			"blob": bytesField(600),
		})
	}
	opts := MasterWriterOptions{Deterministic: true, Compression: cowrie.CompressionZstd, EnableCRC: true}
	data := writeClosed(t, opts, vals)

	r, err := OpenIndexed(data)
	if err != nil {
		t.Fatalf("OpenIndexed: %v", err)
	}
	for i := 0; i < 5; i++ {
		payload, err := r.SeekRecord(uint64(i))
		if err != nil {
			t.Fatalf("SeekRecord(%d): %v", i, err)
		}
		val, err := cowrie.Decode(payload)
		if err != nil {
			t.Fatalf("Decode record %d: %v", i, err)
		}
		if got := val.Get("k").Int64(); got != int64(i) {
			t.Errorf("record %d: k = %d, want %d", i, got, i)
		}
	}
}

func TestIndexedTruncatedLocator(t *testing.T) {
	data := writeClosed(t, noCompressNoCRCOpts(), []any{recOf("k", 1)})

	// Drop the last byte of the locator: trailing magic no longer matches.
	truncated := data[:len(data)-1]
	if _, err := OpenIndexed(truncated); err != ErrNotCowrieFile {
		t.Errorf("dropped locator byte: got %v, want ErrNotCowrieFile", err)
	}
}

func TestIndexedFileShorterThanPreamblePlusLocator(t *testing.T) {
	for _, n := range []int{0, 1, PreambleLen, PreambleLen + LocatorLen - 1} {
		buf := make([]byte, n)
		if _, err := OpenIndexed(buf); err != ErrNotCowrieFile {
			t.Errorf("len=%d: got %v, want ErrNotCowrieFile", n, err)
		}
	}
}

func TestIndexedLocatorClaimsOversizedFooter(t *testing.T) {
	data := writeClosed(t, noCompressNoCRCOpts(), []any{recOf("k", 1)})
	corrupt := append([]byte(nil), data...)
	// Overwrite footer_len (first 8 bytes of the locator) with a huge value.
	locStart := len(corrupt) - LocatorLen
	for i := 0; i < 8; i++ {
		corrupt[locStart+i] = 0xFF
	}
	if _, err := OpenIndexed(corrupt); err != ErrTruncatedLocator {
		t.Errorf("oversized footer_len: got %v, want ErrTruncatedLocator", err)
	}
}

func TestIndexedFooterDigestMismatch(t *testing.T) {
	data := writeClosed(t, noCompressNoCRCOpts(), []any{recOf("k", 1), recOf("k", 2)})
	corrupt := append([]byte(nil), data...)

	// One group => 64-byte footer. footer_start = len - locator - footer_len.
	footerStart := len(corrupt) - LocatorLen - RowGroupEntryLen
	corrupt[footerStart] ^= 0xFF

	if _, err := OpenIndexed(corrupt); err != ErrFooterDigestMismatch {
		t.Errorf("corrupt footer byte: got %v, want ErrFooterDigestMismatch", err)
	}
}

func TestIndexedDeterminism(t *testing.T) {
	vals := []any{
		recOf("a", 1), recOf("a", 2),
		recOf("b", 1),
		recOf("a", 3),
	}
	a := writeClosed(t, noCompressNoCRCOpts(), vals)
	b := writeClosed(t, noCompressNoCRCOpts(), vals)
	if !bytes.Equal(a, b) {
		t.Fatalf("encoding not byte-identical: len %d vs %d", len(a), len(b))
	}
}

// ---- helpers ----

// assertRecordKeyVal seeks global ordinal k and asserts the decoded record has
// object field key == val.
func assertRecordKeyVal(t *testing.T, r *IndexedReader, k uint64, key string, val int) {
	t.Helper()
	payload, err := r.SeekRecord(k)
	if err != nil {
		t.Fatalf("SeekRecord(%d): %v", k, err)
	}
	v, err := cowrie.Decode(payload)
	if err != nil {
		t.Fatalf("Decode record %d: %v", k, err)
	}
	got := v.Get(key)
	if got == nil {
		t.Fatalf("record %d: missing key %q (type=%v)", k, key, v.Type())
	}
	if got.Int64() != int64(val) {
		t.Errorf("record %d: %s = %d, want %d", k, key, got.Int64(), val)
	}
}

// bytesField returns a deterministic filler string of length n.
func bytesField(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + i%26)
	}
	return string(b)
}
