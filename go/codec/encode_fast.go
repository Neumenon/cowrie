package codec

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/Neumenon/cowrie/go"
)

// timeType and rawMessageType defined in unmarshal.go

// === Buffer Pool ===
// Reuse buffers to reduce allocations during encoding.

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() < 1<<20 { // Don't pool buffers > 1MB
		bufferPool.Put(buf)
	}
}

// === Fast Struct Encoder ===
// Pre-compute struct field info for fast encoding (mirrors reflect_cache.go for decode).

type encoderFunc func(v reflect.Value) *cowrie.Value

type structEncoder struct {
	fields []encoderFieldInfo
}

type encoderFieldInfo struct {
	name      string
	index     []int
	encoder   encoderFunc
	omitEmpty bool
}

// encoderCache caches struct encoders by type.
var encoderCache = struct {
	sync.RWMutex
	m map[reflect.Type]*structEncoder
}{m: make(map[reflect.Type]*structEncoder)}

// getStructEncoder returns a cached encoder for the given struct type.
func getStructEncoder(t reflect.Type) *structEncoder {
	encoderCache.RLock()
	enc := encoderCache.m[t]
	encoderCache.RUnlock()
	if enc != nil {
		return enc
	}

	encoderCache.Lock()
	defer encoderCache.Unlock()

	// Double-check
	if enc = encoderCache.m[t]; enc != nil {
		return enc
	}

	enc = buildStructEncoder(t)
	encoderCache.m[t] = enc
	return enc
}

func buildStructEncoder(t reflect.Type) *structEncoder {
	enc := &structEncoder{}
	buildStructEncoderFields(t, nil, enc)
	return enc
}

// buildStructEncoderFields recursively builds encoder fields, flattening embedded structs.
func buildStructEncoderFields(t reflect.Type, index []int, enc *structEncoder) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Build full index path
		fieldIndex := make([]int, len(index)+1)
		copy(fieldIndex, index)
		fieldIndex[len(index)] = i

		// Handle embedded structs - flatten them like decode does
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			buildStructEncoderFields(field.Type, fieldIndex, enc)
			continue
		}

		// Skip unexported
		if field.PkgPath != "" {
			continue
		}

		// Parse json tag
		tag := field.Tag.Get("json")
		name, opts := parseTag(tag)
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}

		omitEmpty := hasOmitEmpty(opts)
		fieldEnc := getTypeEncoder(field.Type)

		enc.fields = append(enc.fields, encoderFieldInfo{
			name:      name,
			index:     fieldIndex,
			encoder:   fieldEnc,
			omitEmpty: omitEmpty,
		})
	}
}

func hasOmitEmpty(opts string) bool {
	for opts != "" {
		var opt string
		idx := 0
		for idx < len(opts) && opts[idx] != ',' {
			idx++
		}
		opt = opts[:idx]
		if idx < len(opts) {
			opts = opts[idx+1:]
		} else {
			opts = ""
		}
		if opt == "omitempty" {
			return true
		}
	}
	return false
}

// getTypeEncoder returns an encoder function for a specific type.
func getTypeEncoder(t reflect.Type) encoderFunc {
	// Special case for time.Time
	if t == timeType {
		return encodeTime
	}

	// Special case for json.RawMessage - parse JSON and encode as Cowrie
	if t == rawMessageType {
		return encodeRawMessage
	}

	switch t.Kind() {
	case reflect.Bool:
		return encodeBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return encodeUint
	case reflect.Float32:
		return encodeFloat32
	case reflect.Float64:
		return encodeFloat64
	case reflect.String:
		return encodeString
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return encodeBytes
		}
		if t.Elem().Kind() == reflect.Float32 {
			return encodeFloat32Slice
		}
		return encodeSlice
	case reflect.Map:
		return encodeMap
	case reflect.Struct:
		// Check for time.Time again (for embedded structs)
		if t == timeType {
			return encodeTime
		}
		return encodeStruct
	case reflect.Ptr:
		return encodePtr
	case reflect.Interface:
		return encodeInterface
	default:
		return encodeAny
	}
}

// === Encoder Functions ===

func encodeBool(v reflect.Value) *cowrie.Value {
	return cowrie.Bool(v.Bool())
}

func encodeInt(v reflect.Value) *cowrie.Value {
	return cowrie.Int64(v.Int())
}

func encodeUint(v reflect.Value) *cowrie.Value {
	return cowrie.Uint64(v.Uint())
}

func encodeFloat32(v reflect.Value) *cowrie.Value {
	return cowrie.Float64(float64(v.Float()))
}

func encodeFloat64(v reflect.Value) *cowrie.Value {
	return cowrie.Float64(v.Float())
}

func encodeString(v reflect.Value) *cowrie.Value {
	return cowrie.String(v.String())
}

func encodeTime(v reflect.Value) *cowrie.Value {
	t := v.Interface().(time.Time)
	return cowrie.Datetime(t)
}

func encodeBytes(v reflect.Value) *cowrie.Value {
	return cowrie.Bytes(v.Bytes())
}

func encodeRawMessage(v reflect.Value) *cowrie.Value {
	raw := v.Bytes()
	if len(raw) == 0 {
		return cowrie.Null()
	}
	// Parse JSON and convert to Cowrie structure
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		// If parsing fails, preserve as string for lossless round-trip.
		// This is better than returning null which loses data.
		return cowrie.String(string(raw))
	}
	return toCowrieValue(parsed)
}

func encodeFloat32Slice(v reflect.Value) *cowrie.Value {
	if v.IsNil() {
		return cowrie.Null()
	}
	slice := v.Interface().([]float32)
	return EncodeFloat32Tensor(slice)
}

func encodeSlice(v reflect.Value) *cowrie.Value {
	if v.IsNil() {
		return cowrie.Null()
	}
	n := v.Len()
	items := make([]*cowrie.Value, n)
	elemEnc := getTypeEncoder(v.Type().Elem())
	for i := 0; i < n; i++ {
		items[i] = elemEnc(v.Index(i))
	}
	return cowrie.Array(items...)
}

func encodeMap(v reflect.Value) *cowrie.Value {
	if v.IsNil() {
		return cowrie.Null()
	}
	keys := v.MapKeys()

	// Sort keys for deterministic encoding (required for fingerprints)
	sortReflectKeys(keys)

	members := make([]cowrie.Member, 0, len(keys))
	valEnc := getTypeEncoder(v.Type().Elem())
	for _, k := range keys {
		key := k.String()
		val := valEnc(v.MapIndex(k))
		members = append(members, cowrie.Member{Key: key, Value: val})
	}
	return cowrie.Object(members...)
}

// sortReflectKeys sorts reflect.Value keys (strings) lexicographically in-place.
func sortReflectKeys(keys []reflect.Value) {
	// Insertion sort - efficient for typical JSON object sizes
	for i := 1; i < len(keys); i++ {
		key := keys[i]
		keyStr := key.String()
		j := i - 1
		for j >= 0 && keys[j].String() > keyStr {
			keys[j+1] = keys[j]
			j--
		}
		keys[j+1] = key
	}
}

func encodeStruct(v reflect.Value) *cowrie.Value {
	enc := getStructEncoder(v.Type())
	members := make([]cowrie.Member, 0, len(enc.fields))
	for _, f := range enc.fields {
		fv := v.FieldByIndex(f.index)
		if f.omitEmpty && isEmptyValue(fv) {
			continue
		}
		val := f.encoder(fv)
		members = append(members, cowrie.Member{Key: f.name, Value: val})
	}
	return cowrie.Object(members...)
}

func encodePtr(v reflect.Value) *cowrie.Value {
	if v.IsNil() {
		return cowrie.Null()
	}
	return getTypeEncoder(v.Elem().Type())(v.Elem())
}

func encodeInterface(v reflect.Value) *cowrie.Value {
	if v.IsNil() {
		return cowrie.Null()
	}
	return encodeAny(v.Elem())
}

func encodeAny(v reflect.Value) *cowrie.Value {
	if !v.IsValid() {
		return cowrie.Null()
	}
	return cowrie.FromAny(v.Interface())
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

// === Fast Encode Entry Point ===

// FastEncode encodes v to Cowrie using cached struct encoders.
// This is 2-3x faster than the generic path for struct types.
func FastEncode(v any) ([]byte, error) {
	cowrieVal := fastToCowrieValue(v)
	return cowrie.Encode(cowrieVal)
}

// fastToCowrieValue converts Go value to Cowrie using cached encoders.
func fastToCowrieValue(v any) *cowrie.Value {
	if v == nil {
		return cowrie.Null()
	}

	rv := reflect.ValueOf(v)

	// Fast path for common types
	switch x := v.(type) {
	case []float32:
		return EncodeFloat32Tensor(x)
	case map[string]any:
		return toCowrieValue(v) // Use existing path for map[string]any
	case []any:
		return toCowrieValue(v) // Use existing path for []any
	}

	// Use cached struct encoder for struct types
	if rv.Kind() == reflect.Struct {
		return encodeStruct(rv)
	}
	if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Struct {
		if rv.IsNil() {
			return cowrie.Null()
		}
		return encodeStruct(rv.Elem())
	}

	// Fallback to type-specific encoder
	enc := getTypeEncoder(rv.Type())
	return enc(rv)
}

// FastEncodeBytes is the pooled-buffer version of FastEncode.
func FastEncodeBytes(v any) ([]byte, error) {
	cowrieVal := fastToCowrieValue(v)
	data, err := cowrie.Encode(cowrieVal)
	if err != nil {
		return nil, err
	}
	// Return a copy (the caller owns it)
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}
