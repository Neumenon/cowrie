package gen2

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"time"
)

// AnyOptions controls EncodeAny behavior.
//
// EncodeAny/DecodeAny provides a Gen-1-like API surface on top of the Gen-2 wire
// format (Cowrie v2). The key feature is optional tensorization of numeric slices
// so embeddings don’t degrade into per-element arrays.
//
// DecodeAny returns Go-friendly values (e.g. `[]byte`, `time.Time`), whereas
// `ToAny` is a JSON projection (e.g. base64 strings, RFC3339 timestamps).

type AnyOptions struct {
	HighPrecision   bool
	Enriched        bool
	TensorizeSlices bool
}

// DefaultAnyOptions returns recommended defaults for EncodeAny.
func DefaultAnyOptions() AnyOptions {
	return AnyOptions{TensorizeSlices: true}
}

// EncodeAny encodes v into Cowrie v2 bytes.
func EncodeAny(v any) ([]byte, error) {
	return EncodeAnyWithOptions(v, DefaultAnyOptions())
}

// EncodeAnyWithOptions encodes v into Cowrie v2 bytes with options.
func EncodeAnyWithOptions(v any, opts AnyOptions) ([]byte, error) {
	val := fromGoAny(v, opts)
	return Encode(val)
}

// DecodeAny decodes Cowrie v2 bytes into Go values.
func DecodeAny(data []byte) (any, error) {
	v, err := Decode(data)
	if err != nil {
		return nil, err
	}
	return ToGoAny(v), nil
}

// ToGoAny converts an Cowrie value to Go-friendly values.
//
// Differences vs ToAny:
// - bytes -> []byte (not base64 string)
// - datetime64 -> time.Time
// - uuid128 -> [16]byte
// - tensors -> []T for rank-1 tensors (common embedding case)
func ToGoAny(v *Value) any {
	if v == nil {
		return nil
	}

	switch v.typ {
	case TypeNull:
		return nil
	case TypeBool:
		return v.boolVal
	case TypeInt64:
		return v.int64Val
	case TypeUint64:
		return v.uint64Val
	case TypeFloat64:
		return v.float64Val
	case TypeDecimal128:
		return v.decimal128
	case TypeString:
		return v.stringVal
	case TypeBytes:
		out := make([]byte, len(v.bytesVal))
		copy(out, v.bytesVal)
		return out
	case TypeDatetime64:
		return time.Unix(0, v.datetime64).UTC()
	case TypeUUID128:
		return v.uuid128
	case TypeBigInt:
		out := make([]byte, len(v.bigintVal))
		copy(out, v.bigintVal)
		return out
	case TypeArray:
		arr := make([]any, len(v.arrayVal))
		for i, item := range v.arrayVal {
			arr[i] = ToGoAny(item)
		}
		return arr
	case TypeObject:
		obj := make(map[string]any, len(v.objectVal))
		for _, m := range v.objectVal {
			obj[m.Key] = ToGoAny(m.Value)
		}
		return obj

	case TypeTensor:
		// Preserve multi-dimensional tensors; flatten rank-1 (embedding case).
		td := v.tensorVal
		if len(td.Dims) == 1 {
			switch td.DType {
			case DTypeFloat32:
				return td.Float32Slice()
			case DTypeFloat64:
				return td.Float64Slice()
			case DTypeInt32:
				return td.Int32Slice()
			case DTypeInt64:
				return td.Int64Slice()
			case DTypeUint8:
				if view, ok := td.ViewUint8(); ok {
					return view
				}
				out := make([]byte, len(td.Data))
				copy(out, td.Data)
				return out
			}
		}
		// Return the structured tensor for higher-rank or unknown dtype.
		return td

	case TypeTensorRef:
		return v.tensorRefVal
	case TypeImage:
		return v.imageVal
	case TypeAudio:
		return v.audioVal
	case TypeAdjlist:
		return v.adjlistVal
	case TypeRichText:
		return v.richTextVal
	case TypeDelta:
		return v.deltaVal
	case TypeUnknownExt:
		return v.unknownExtVal
	default:
		return nil
	}
}

func decodeTensorFloat32(data []byte) []float32 {
	count := len(data) / 4
	out := make([]float32, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		out[i] = math.Float32frombits(bits)
	}
	return out
}

func decodeTensorFloat64(data []byte) []float64 {
	count := len(data) / 8
	out := make([]float64, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint64(data[i*8:])
		out[i] = math.Float64frombits(bits)
	}
	return out
}

func decodeTensorInt32(data []byte) []int32 {
	count := len(data) / 4
	out := make([]int32, count)
	for i := 0; i < count; i++ {
		out[i] = int32(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return out
}

func decodeTensorInt64(data []byte) []int64 {
	count := len(data) / 8
	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return out
}

func fromGoAny(v any, opts AnyOptions) *Value {
	if v == nil {
		return Null()
	}

	// Tensor fast paths
	if opts.TensorizeSlices {
		switch x := v.(type) {
		case []float32:
			return Tensor(DTypeFloat32, []uint64{uint64(len(x))}, encodeFloat32LE(x))
		case []float64:
			if opts.HighPrecision {
				return Tensor(DTypeFloat64, []uint64{uint64(len(x))}, encodeFloat64LE(x))
			}
			return Tensor(DTypeFloat32, []uint64{uint64(len(x))}, encodeFloat64AsFloat32LE(x))
		case []int32:
			return Tensor(DTypeInt32, []uint64{uint64(len(x))}, encodeInt32LE(x))
		case []int64:
			return Tensor(DTypeInt64, []uint64{uint64(len(x))}, encodeInt64LE(x))
		case [][]float32:
			if t := encodeFloat32MatrixTensor(x); t != nil {
				return t
			}
		case [][]float64:
			if t := encodeFloat64MatrixTensor(x, opts.HighPrecision); t != nil {
				return t
			}
		case []any:
			if t := tryTensorizeAnySlice(x, opts); t != nil {
				return t
			}
		}
	}

	// Fast path for most values.
	if opts.Enriched {
		return FromAnyEnriched(v)
	}
	val := FromAny(v)
	if val != nil {
		return val
	}

	// Last resort: JSON bridge. Kept here (not in FromAny) so existing semantics
	// don’t change.
	return fallbackViaJSON(v, opts)
}

func tryTensorizeAnySlice(arr []any, opts AnyOptions) *Value {
	if len(arr) < 8 {
		return nil
	}

	// JSON unmarshaled arrays of numbers are usually []any of float64.
	floats := make([]float64, len(arr))
	for i := range arr {
		switch x := arr[i].(type) {
		case float64:
			floats[i] = x
		case int64:
			floats[i] = float64(x)
		case int:
			floats[i] = float64(x)
		default:
			return nil
		}
	}

	if opts.HighPrecision {
		return Tensor(DTypeFloat64, []uint64{uint64(len(floats))}, encodeFloat64LE(floats))
	}
	return Tensor(DTypeFloat32, []uint64{uint64(len(floats))}, encodeFloat64AsFloat32LE(floats))
}

func encodeFloat32LE(values []float32) []byte {
	out := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
	}
	return out
}

func encodeFloat64LE(values []float64) []byte {
	out := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(out[i*8:], math.Float64bits(v))
	}
	return out
}

func encodeFloat64AsFloat32LE(values []float64) []byte {
	out := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(float32(v)))
	}
	return out
}

func encodeInt32LE(values []int32) []byte {
	out := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], uint32(v))
	}
	return out
}

func encodeInt64LE(values []int64) []byte {
	out := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(v))
	}
	return out
}

func encodeFloat32MatrixTensor(rows [][]float32) *Value {
	if len(rows) == 0 {
		return nil
	}
	cols := len(rows[0])
	if cols == 0 {
		return nil
	}
	for i := 1; i < len(rows); i++ {
		if len(rows[i]) != cols {
			return nil
		}
	}
	flat := make([]float32, 0, len(rows)*cols)
	for _, r := range rows {
		flat = append(flat, r...)
	}
	return Tensor(DTypeFloat32, []uint64{uint64(len(rows)), uint64(cols)}, encodeFloat32LE(flat))
}

func encodeFloat64MatrixTensor(rows [][]float64, highPrecision bool) *Value {
	if len(rows) == 0 {
		return nil
	}
	cols := len(rows[0])
	if cols == 0 {
		return nil
	}
	for i := 1; i < len(rows); i++ {
		if len(rows[i]) != cols {
			return nil
		}
	}

	flat := make([]float64, 0, len(rows)*cols)
	for _, r := range rows {
		flat = append(flat, r...)
	}

	if highPrecision {
		return Tensor(DTypeFloat64, []uint64{uint64(len(rows)), uint64(cols)}, encodeFloat64LE(flat))
	}
	return Tensor(DTypeFloat32, []uint64{uint64(len(rows)), uint64(cols)}, encodeFloat64AsFloat32LE(flat))
}

// Fallback helper used for "unknown" types when the caller asked for enriched
// behavior. It preserves the tensorization policy while using JSON as a bridge.
func fallbackViaJSON(v any, opts AnyOptions) *Value {
	data, err := json.Marshal(v)
	if err != nil {
		if opts.Enriched {
			return FromAnyEnriched(v)
		}
		return FromAny(v)
	}
	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		if opts.Enriched {
			return FromAnyEnriched(v)
		}
		return FromAny(v)
	}
	return fromGoAny(parsed, opts)
}
