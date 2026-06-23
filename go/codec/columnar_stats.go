package codec

import (
	"encoding/binary"
	"math"

	"github.com/Neumenon/cowrie/go/v2"
)

// ---------------------------------------------------------------------------
// Phase 3 — per-chunk statistics + stateless declarative skipping.
//
// One fixed 48-byte StatsBlock (ColumnStatsLen) is attached to the NUMERIC
// tensor.values column. It is UNCOMPRESSED, little-endian, and lives in the
// payload front (between the path blob and the first K-aligned chunk) so a skip
// pass reads ONLY directory + stats and never touches a chunk byte.
//
// Byte layout (offsets relative to stats_off):
//
//	off  size  field        notes
//	  0    1   stats_ver    u8  = StatsVersion1 (1); other => ErrColumnarMalformed
//	  1    1   stats_dtype  u8  cowrie.DType the min/max cells are typed as
//	  2    1   stats_flags  u8  bit0 HAS_MINMAX; bits1..7 reserved 0
//	  3    1   _pad0        u8  0x00
//	  4    4   _pad1        u32 0x00 (brings the two u64 counts to offset 8)
//	  8    8   null_count   u64 invalid ELEMENTS (invalid_rows * prod(shape_tail))
//	 16    8   nan_count    u64 IEEE-NaN value elements (floats only; 0 otherwise)
//	 24    8   min_cell     8 raw LE bytes; reinterpret per stats_dtype
//	 32    8   max_cell     8 raw LE bytes (same encoding as min_cell)
//	 40    8   _pad_tail    u64 0x00 (rounds the block to 48)
//	 48  = END.
//
// min_cell / max_cell encoding (pinned, deterministic):
//   - f32: math.Float32bits(v) -> u32 in [0:4] LE; [4:8] = 0x00.
//   - f64: math.Float64bits(v) -> u64 in [0:8] LE.
//   - i32: int64(v) sign-extended -> 8-byte LE two's-complement in [0:8].
//   - i64: v -> 8-byte LE two's-complement in [0:8].
//   - u8:  uint64(v) -> 8-byte LE in [0:8].
//
// Decoders read the 8 bytes then narrow per stats_dtype. This single 8-byte
// cell + dtype tag is why the block is fixed-width regardless of dtype.
// ---------------------------------------------------------------------------

// StatsBlock flag bits (stats_flags @2).
const statsFlagHasMinMax uint8 = 0x01

// StatsBlock is the decoded per-chunk statistics block for a numeric column.
// minCell/maxCell are raw 8-byte LE cells; use the typed accessors to decode
// them per DType.
type StatsBlock struct {
	DType     cowrie.DType
	Flags     uint8 // bit0 HasMinMax
	NullCount uint64
	NanCount  uint64
	minCell   [8]byte // raw LE, decode per DType
	maxCell   [8]byte
}

// HasMinMax reports whether the block carries valid min/max cells. It is false
// for an empty / all-null / all-NaN block, in which case min/max MUST be ignored.
func (s *StatsBlock) HasMinMax() bool { return s.Flags&statsFlagHasMinMax != 0 }

// encodeInto writes this block as a fixed ColumnStatsLen little-endian record
// into dst (>= ColumnStatsLen bytes). Field-by-field, never unsafe; all
// reserved/pad bytes are written as 0x00 for determinism.
func (s *StatsBlock) encodeInto(dst []byte) {
	_ = dst[ColumnStatsLen-1] // bounds-check hint
	for i := 0; i < ColumnStatsLen; i++ {
		dst[i] = 0
	}
	dst[0] = StatsVersion1
	dst[1] = uint8(s.DType)
	dst[2] = s.Flags & statsFlagHasMinMax
	// dst[3] _pad0, dst[4:8] _pad1 stay 0.
	binary.LittleEndian.PutUint64(dst[8:16], s.NullCount)
	binary.LittleEndian.PutUint64(dst[16:24], s.NanCount)
	copy(dst[24:32], s.minCell[:])
	copy(dst[32:40], s.maxCell[:])
	// dst[40:48] _pad_tail stays 0.
}

// decodeStatsBlock decodes a fixed ColumnStatsLen little-endian record from b.
// It requires len(b) >= ColumnStatsLen and stats_ver == StatsVersion1, returning
// ErrColumnarMalformed otherwise.
func decodeStatsBlock(b []byte) (StatsBlock, error) {
	if len(b) < ColumnStatsLen {
		return StatsBlock{}, ErrColumnarMalformed
	}
	if b[0] != StatsVersion1 {
		return StatsBlock{}, ErrColumnarMalformed
	}
	var s StatsBlock
	s.DType = cowrie.DType(b[1])
	s.Flags = b[2] & statsFlagHasMinMax
	s.NullCount = binary.LittleEndian.Uint64(b[8:16])
	s.NanCount = binary.LittleEndian.Uint64(b[16:24])
	copy(s.minCell[:], b[24:32])
	copy(s.maxCell[:], b[32:40])
	return s, nil
}

// ---- typed min/max accessors (ok=false when !HasMinMax or dtype mismatch) ----

// MinMaxFloat64 decodes the min/max cells as float64 (f32 widened to f64, f64
// native). ok=false for non-float dtypes or when HasMinMax is unset.
func (s *StatsBlock) MinMaxFloat64() (lo, hi float64, ok bool) {
	if !s.HasMinMax() {
		return 0, 0, false
	}
	switch s.DType {
	case cowrie.DTypeFloat32:
		lo = float64(math.Float32frombits(binary.LittleEndian.Uint32(s.minCell[0:4])))
		hi = float64(math.Float32frombits(binary.LittleEndian.Uint32(s.maxCell[0:4])))
		return lo, hi, true
	case cowrie.DTypeFloat64:
		lo = math.Float64frombits(binary.LittleEndian.Uint64(s.minCell[0:8]))
		hi = math.Float64frombits(binary.LittleEndian.Uint64(s.maxCell[0:8]))
		return lo, hi, true
	default:
		return 0, 0, false
	}
}

// MinMaxInt64 decodes the min/max cells as int64 (i32 sign-extended, i64 native).
func (s *StatsBlock) MinMaxInt64() (lo, hi int64, ok bool) {
	if !s.HasMinMax() {
		return 0, 0, false
	}
	switch s.DType {
	case cowrie.DTypeInt32, cowrie.DTypeInt64:
		lo = int64(binary.LittleEndian.Uint64(s.minCell[0:8]))
		hi = int64(binary.LittleEndian.Uint64(s.maxCell[0:8]))
		return lo, hi, true
	default:
		return 0, 0, false
	}
}

// MinMaxUint64 decodes the min/max cells as uint64 (u8).
func (s *StatsBlock) MinMaxUint64() (lo, hi uint64, ok bool) {
	if !s.HasMinMax() {
		return 0, 0, false
	}
	switch s.DType {
	case cowrie.DTypeUint8:
		lo = binary.LittleEndian.Uint64(s.minCell[0:8])
		hi = binary.LittleEndian.Uint64(s.maxCell[0:8])
		return lo, hi, true
	default:
		return 0, 0, false
	}
}

// MightContainRange reports, conservatively, whether the block could contain a
// value within the inclusive numeric range [lo, hi]. It is NaN-aware and works in
// the float domain. It NEVER returns false when absence cannot be proven, so a
// caller can safely skip blocks for which it returns false.
//
//   - !HasMinMax: return true (cannot prove absence).
//   - lo or hi is NaN: return true (unconstrained query, never skip).
//   - lo > hi (empty query range): return false.
//   - otherwise decode min/max into float64 (int/uint widened; i64 magnitudes
//     beyond 2^53 widen the window — min rounds toward -Inf, max toward +Inf —
//     so the test can only widen the candidate set, never false-skip), then
//     return min <= hi && max >= lo.
func (s *StatsBlock) MightContainRange(lo, hi float64) bool {
	if !s.HasMinMax() {
		return true
	}
	if math.IsNaN(lo) || math.IsNaN(hi) {
		return true
	}
	if lo > hi {
		return false
	}

	var mn, mx float64
	switch s.DType {
	case cowrie.DTypeFloat32, cowrie.DTypeFloat64:
		var ok bool
		mn, mx, ok = s.MinMaxFloat64()
		if !ok {
			return true
		}
	case cowrie.DTypeInt32, cowrie.DTypeInt64:
		imn, imx, ok := s.MinMaxInt64()
		if !ok {
			return true
		}
		// Round min toward -Inf and max toward +Inf so float widening can only
		// enlarge the [mn,mx] window (never narrow it -> never false-skip).
		mn = nextDownF64(float64(imn))
		mx = nextUpF64(float64(imx))
	case cowrie.DTypeUint8:
		umn, umx, ok := s.MinMaxUint64()
		if !ok {
			return true
		}
		mn = float64(umn)
		mx = float64(umx)
	default:
		// Unknown stats dtype: cannot prove absence.
		return true
	}

	return mn <= hi && mx >= lo
}

// nextDownF64 returns the previous representable float64 below x (toward -Inf),
// used to conservatively widen an integer min on the float boundary. Exact
// (and finite-magnitude) values within +/-2^53 are returned unchanged via the
// widening below, but for safety against int64->float64 rounding we always step
// down by one ULP unless x is already -Inf/NaN.
func nextDownF64(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, -1) {
		return x
	}
	return math.Nextafter(x, math.Inf(-1))
}

// nextUpF64 returns the next representable float64 above x (toward +Inf).
func nextUpF64(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, +1) {
		return x
	}
	return math.Nextafter(x, math.Inf(+1))
}

// ---------------------------------------------------------------------------
// Writer-side computation.
// ---------------------------------------------------------------------------

// computeValuesStats computes the StatsBlock for the tensor.values column over
// its RAW (pre-compression) bytes. dtype is the values DType; validity is the
// optional LSB-first per-ROW bitmap (nil => all valid); numRows is N;
// prodShapeTail is prod(shape_tail) (the inner element count per row).
//
// Rules (pinned):
//   - FLOATS (f32/f64): NaN excluded from min/max but counted in nan_count;
//     signed-zero canonicalized so a zero bound stores +0.0 bits; +/-Inf are
//     ordered bounds and ARE included. !seenFinite => HasMinMax=false.
//   - INTS/UINT (i32/i64/u8): exact element min/max; nan_count=0;
//     HasMinMax=true iff at least one LIVE element exists.
//   - Masked (invalid) rows are excluded ENTIRELY from min/max and nan scanning.
//   - null_count = invalid_rows * prodShapeTail (element domain).
//   - ok=false (no block) for any dtype that cannot be stat'd (e.g. f16/bf16).
func computeValuesStats(dtype cowrie.DType, raw []byte, validity []byte, numRows, prodShapeTail uint64) (StatsBlock, bool) {
	var s StatsBlock
	s.DType = dtype

	// rowValid reports whether row r (0-based) is live. nil validity => all valid.
	rowValid := func(r uint64) bool {
		if validity == nil {
			return true
		}
		byteIdx := r / 8
		if byteIdx >= uint64(len(validity)) {
			return true
		}
		return validity[byteIdx]&(1<<(r%8)) != 0
	}

	// null_count = invalid_rows * prodShapeTail (element domain).
	if validity != nil {
		var invalidRows uint64
		for r := uint64(0); r < numRows; r++ {
			if !rowValid(r) {
				invalidRows++
			}
		}
		s.NullCount = invalidRows * prodShapeTail
	}

	switch dtype {
	case cowrie.DTypeFloat32:
		vals, ok := cowrie.ViewBytesFloat32(raw)
		if !ok {
			return StatsBlock{}, false
		}
		mn := math.Inf(+1)
		mx := math.Inf(-1)
		seen := false
		forEachLive(len(vals), prodShapeTail, rowValid, func(i int) {
			v := float64(vals[i])
			if math.IsNaN(v) {
				s.NanCount++
				return
			}
			if v == 0 {
				v = 0 // canonicalize -0.0 -> +0.0
			}
			if v < mn {
				mn = v
			}
			if v > mx {
				mx = v
			}
			seen = true
		})
		if seen {
			s.Flags |= statsFlagHasMinMax
			binary.LittleEndian.PutUint32(s.minCell[0:4], math.Float32bits(float32(mn)))
			binary.LittleEndian.PutUint32(s.maxCell[0:4], math.Float32bits(float32(mx)))
		}
		return s, true

	case cowrie.DTypeFloat64:
		vals, ok := cowrie.ViewBytesFloat64(raw)
		if !ok {
			return StatsBlock{}, false
		}
		mn := math.Inf(+1)
		mx := math.Inf(-1)
		seen := false
		forEachLive(len(vals), prodShapeTail, rowValid, func(i int) {
			v := vals[i]
			if math.IsNaN(v) {
				s.NanCount++
				return
			}
			if v == 0 {
				v = 0
			}
			if v < mn {
				mn = v
			}
			if v > mx {
				mx = v
			}
			seen = true
		})
		if seen {
			s.Flags |= statsFlagHasMinMax
			binary.LittleEndian.PutUint64(s.minCell[0:8], math.Float64bits(mn))
			binary.LittleEndian.PutUint64(s.maxCell[0:8], math.Float64bits(mx))
		}
		return s, true

	case cowrie.DTypeInt32:
		vals, ok := cowrie.ViewBytesInt32(raw)
		if !ok {
			return StatsBlock{}, false
		}
		var mn, mx int64
		seen := false
		forEachLive(len(vals), prodShapeTail, rowValid, func(i int) {
			v := int64(vals[i])
			if !seen || v < mn {
				mn = v
			}
			if !seen || v > mx {
				mx = v
			}
			seen = true
		})
		if seen {
			s.Flags |= statsFlagHasMinMax
			binary.LittleEndian.PutUint64(s.minCell[0:8], uint64(mn))
			binary.LittleEndian.PutUint64(s.maxCell[0:8], uint64(mx))
		}
		return s, true

	case cowrie.DTypeInt64:
		vals, ok := cowrie.ViewBytesInt64(raw)
		if !ok {
			return StatsBlock{}, false
		}
		var mn, mx int64
		seen := false
		forEachLive(len(vals), prodShapeTail, rowValid, func(i int) {
			v := vals[i]
			if !seen || v < mn {
				mn = v
			}
			if !seen || v > mx {
				mx = v
			}
			seen = true
		})
		if seen {
			s.Flags |= statsFlagHasMinMax
			binary.LittleEndian.PutUint64(s.minCell[0:8], uint64(mn))
			binary.LittleEndian.PutUint64(s.maxCell[0:8], uint64(mx))
		}
		return s, true

	case cowrie.DTypeUint8:
		vals, ok := cowrie.ViewBytesUint8(raw)
		if !ok {
			return StatsBlock{}, false
		}
		var mn, mx uint64
		seen := false
		forEachLive(len(vals), prodShapeTail, rowValid, func(i int) {
			v := uint64(vals[i])
			if !seen || v < mn {
				mn = v
			}
			if !seen || v > mx {
				mx = v
			}
			seen = true
		})
		if seen {
			s.Flags |= statsFlagHasMinMax
			binary.LittleEndian.PutUint64(s.minCell[0:8], mn)
			binary.LittleEndian.PutUint64(s.maxCell[0:8], mx)
		}
		return s, true

	default:
		// Any other dtype (f16/bf16/quantized/...) is not stat'd in Phase 3.
		return StatsBlock{}, false
	}
}

// forEachLive invokes fn(i) for every element index i in [0,n) whose owning row
// is live per rowValid. Element i belongs to row i/prodShapeTail (prodShapeTail
// is clamped to >=1). When validity is implicitly all-valid the callback runs
// for every element.
func forEachLive(n int, prodShapeTail uint64, rowValid func(uint64) bool, fn func(int)) {
	inner := prodShapeTail
	if inner == 0 {
		inner = 1
	}
	for i := 0; i < n; i++ {
		row := uint64(i) / inner
		if !rowValid(row) {
			continue
		}
		fn(i)
	}
}
