package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/Neumenon/cowrie/go"
)

// CowrieCodec implements Codec using the Cowrie binary format.
// It provides native tensor encoding for float32 slices (~48% smaller).
type CowrieCodec struct {
	// HighPrecision preserves float64 precision instead of converting to float32.
	// When false (default), float64 slices are encoded as float32 for ~50% size reduction.
	// When true, float64 slices are encoded as float64 (larger but lossless).
	HighPrecision bool
}

// ContentType returns "application/cowrie".
func (CowrieCodec) ContentType() string {
	return ContentTypeCowrie
}

// Encode encodes v as Cowrie and writes to w.
// Float32 slices are automatically encoded as native tensors.
// If c.HighPrecision is true, float64 slices preserve full precision.
func (c CowrieCodec) Encode(w io.Writer, v any) error {
	cowrieVal := toCowrieValueWithOpts(v, c.HighPrecision)
	data, err := cowrie.Encode(cowrieVal)
	if err != nil {
		return fmt.Errorf("cowrie encode: %w", err)
	}
	_, err = w.Write(data)
	return err
}

// Decode decodes Cowrie from r into v.
// Uses a tiered approach for maximum performance:
//  1. Type registry (fastest) - pre-compiled unmarshalers for hot types
//  2. Direct reflection (fast) - struct fields mapped directly from Cowrie
//  3. Generic conversion (fallback) - for map[string]any and interface{}
func (c CowrieCodec) Decode(r io.Reader, v any) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("cowrie read: %w", err)
	}

	cowrieVal, err := cowrie.Decode(data)
	if err != nil {
		return fmt.Errorf("cowrie decode: %w", err)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("cowrie: decode requires non-nil pointer, got %T", v)
	}

	targetType := rv.Elem().Type()

	// Priority 1: Check type registry for pre-compiled fast unmarshaler
	if fast := getFastUnmarshaler(targetType); fast != nil {
		return fast(cowrieVal, v)
	}

	// Priority 2: Direct reflection unmarshaler for structs
	target := rv.Elem()
	if target.Kind() == reflect.Struct {
		return unmarshalValue(cowrieVal, target)
	}

	// Priority 3: Generic map/interface (original path, handles map[string]any)
	goVal := fromCowrieValue(cowrieVal)
	if goVal == nil {
		return nil
	}
	target.Set(reflect.ValueOf(goVal))
	return nil
}

// toCowrieValue converts a Go value to Cowrie, with special handling for float32 slices.
// For struct types, uses the fast cached encoder for 10-27x speedup.
// Uses default precision settings (converts float64 to float32 for size).
func toCowrieValue(v any) *cowrie.Value {
	return toCowrieValueWithOpts(v, false)
}

// toCowrieValueWithOpts converts a Go value to Cowrie with precision control.
// If highPrecision is true, float64 slices preserve full precision.
func toCowrieValueWithOpts(v any, highPrecision bool) *cowrie.Value {
	if v == nil {
		return cowrie.Null()
	}

	// Fast path for structs using cached encoder
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Struct {
		return encodeStruct(rv)
	}
	if rv.Kind() == reflect.Ptr && !rv.IsNil() && rv.Elem().Kind() == reflect.Struct {
		return encodeStruct(rv.Elem())
	}

	switch x := v.(type) {
	case []float32:
		// Optimize float32 slices as tensors (~60% smaller)
		return EncodeFloat32Tensor(x)

	case []float64:
		// HighPrecision: preserve float64, otherwise convert to float32 for ~55% savings
		if highPrecision {
			return EncodeFloat64Tensor(x)
		}
		return EncodeFloat64AsFloat32Tensor(x)

	case map[string]any:
		// Walk the map and convert float slices
		members := make([]cowrie.Member, 0, len(x))
		for key, val := range x {
			members = append(members, cowrie.Member{
				Key:   key,
				Value: toCowrieValueWithOpts(val, highPrecision),
			})
		}
		return cowrie.Object(members...)

	case []any:
		// Try to encode as numeric tensor first (handles JSON-decoded float arrays)
		if tensor := TryEncodeNumericSliceWithOpts(x, highPrecision); tensor != nil {
			return tensor
		}
		// Fall back to regular array
		items := make([]*cowrie.Value, len(x))
		for i, item := range x {
			items[i] = toCowrieValueWithOpts(item, highPrecision)
		}
		return cowrie.Array(items...)

	default:
		// Use standard conversion for everything else
		return cowrie.FromAny(x)
	}
}

// fromCowrieValue converts an Cowrie value to Go, with special handling for tensors.
func fromCowrieValue(v *cowrie.Value) any {
	if v == nil {
		return nil
	}

	switch v.Type() {
	case cowrie.TypeTensor:
		// Convert tensor to slice
		td := v.Tensor()
		return tensorDataToSlice(&td)

	case cowrie.TypeArray:
		// Walk the array
		arr := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			arr[i] = fromCowrieValue(v.Index(i))
		}
		return arr

	case cowrie.TypeObject:
		// Walk the object using Members()
		obj := make(map[string]any)
		for _, m := range v.Members() {
			obj[m.Key] = fromCowrieValue(m.Value)
		}
		return obj

	default:
		// Use standard conversion for everything else
		return cowrie.ToAny(v)
	}
}

// tensorDataToSlice converts TensorData to the appropriate Go slice type.
func tensorDataToSlice(td *cowrie.TensorData) any {
	if td == nil || len(td.Data) == 0 {
		return nil
	}

	switch td.DType {
	case cowrie.DTypeFloat32:
		count := len(td.Data) / 4
		result := make([]float32, count)
		for i := 0; i < count; i++ {
			bits := binary.LittleEndian.Uint32(td.Data[i*4:])
			result[i] = math.Float32frombits(bits)
		}
		return result

	case cowrie.DTypeFloat64:
		count := len(td.Data) / 8
		result := make([]float64, count)
		for i := 0; i < count; i++ {
			bits := binary.LittleEndian.Uint64(td.Data[i*8:])
			result[i] = math.Float64frombits(bits)
		}
		return result

	case cowrie.DTypeInt32:
		count := len(td.Data) / 4
		result := make([]int32, count)
		for i := 0; i < count; i++ {
			result[i] = int32(binary.LittleEndian.Uint32(td.Data[i*4:]))
		}
		return result

	case cowrie.DTypeInt64:
		count := len(td.Data) / 8
		result := make([]int64, count)
		for i := 0; i < count; i++ {
			result[i] = int64(binary.LittleEndian.Uint64(td.Data[i*8:]))
		}
		return result

	default:
		// Return raw bytes for unknown types
		return td.Data
	}
}

// DecodeBytes is a convenience function for decoding Cowrie bytes directly.
func DecodeBytes(data []byte, v any) error {
	return CowrieCodec{}.Decode(bytes.NewReader(data), v)
}

// EncodeBytes is a convenience function for encoding to Cowrie bytes directly.
func EncodeBytes(v any) ([]byte, error) {
	var buf bytes.Buffer
	c := CowrieCodec{}
	if err := c.Encode(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
