package cowrie

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// JavaScript safe integer range
const (
	maxSafeInt = 9007199254740991  // 2^53 - 1
	minSafeInt = -9007199254740991 // -(2^53 - 1)
)

// Regex patterns for type inference
var (
	iso8601Pattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	uuidPattern    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// FromJSON parses JSON bytes into an Cowrie value without type inference.
// Strings remain strings, preserving exact JSON round-trip fidelity.
// Use FromJSONEnriched if you want automatic type inference (dates, UUIDs, etc.).
// Large integers are preserved exactly via json.Number (no float64 precision loss).
func FromJSON(data []byte) (*Value, error) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return fromJSONTyped(v)
}

// FromJSONEnriched parses JSON bytes with automatic type inference.
// Detects and converts:
//   - ISO 8601 strings in date-like fields → TypeDatetime64
//   - UUID-formatted strings → TypeUUID128
//   - Base64 strings in data-like fields → TypeBytes
//
// Use this when ingesting external data into the Cowrie ecosystem.
// For strict JSON round-trip fidelity, use FromJSON instead.
// Large integers are preserved exactly via json.Number (no float64 precision loss).
func FromJSONEnriched(data []byte) (*Value, error) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return fromAnyEnrichedTyped(v, nil)
}

// FromAny converts a Go value to an Cowrie value without type inference.
// Strings remain strings, preserving exact round-trip fidelity.
// Use FromAnyEnriched if you want automatic type inference.
func FromAny(v any) *Value {
	return fromAnyStrict(v)
}

// FromAnyEnriched converts a Go value with automatic type inference.
// Detects and converts datetime strings, UUIDs, base64, etc.
func FromAnyEnriched(v any) *Value {
	return fromAnyEnriched(v, nil)
}

// fromJSONTyped converts decoded JSON to a Value, reconstructing typed projections
// (_type-tagged objects produced by ToAny) at EVERY nesting depth — top level, array
// elements, and nested object values — matching the Python/Rust/TS from_json bridges.
// A recognized-but-out-of-range typed value (e.g. audio rate > uint32 or channels
// outside 1..255) returns an error rather than being silently truncated or demoted to
// a plain object. Strict mode keeps strings as strings; scalar leaves go to fromAnyStrict.
func fromJSONTyped(v any) (*Value, error) {
	switch x := v.(type) {
	case map[string]any:
		typed, err := tryReconstructTyped(x, false)
		if err != nil {
			return nil, err
		}
		if typed != nil {
			return typed, nil
		}
		// Plain object: recurse so nested typed projections are reconstructed too.
		members := make([]Member, 0, len(x))
		for _, key := range sortedMapKeys(x) {
			mv, err := fromJSONTyped(x[key])
			if err != nil {
				return nil, err
			}
			members = append(members, Member{Key: key, Value: mv})
		}
		return Object(members...), nil
	case []any:
		items := make([]*Value, len(x))
		for i, item := range x {
			iv, err := fromJSONTyped(item)
			if err != nil {
				return nil, err
			}
			items[i] = iv
		}
		return Array(items...), nil
	default:
		return fromAnyStrict(v), nil
	}
}

// fromAnyEnrichedTyped reconstructs a top-level typed projection (propagating
// out-of-range errors) and otherwise applies enriched type inference. Unlike the
// strict fromJSONTyped it does not recurse typed reconstruction into nested values:
// enriched mode ingests raw external JSON (which carries no _type projections), and
// its field-name inference is preserved by delegating to fromAnyEnriched.
func fromAnyEnrichedTyped(v any, hints map[string]Type) (*Value, error) {
	if m, ok := v.(map[string]any); ok {
		typed, err := tryReconstructTyped(m, true)
		if err != nil {
			return nil, err
		}
		if typed != nil {
			return typed, nil
		}
	}
	return fromAnyEnriched(v, hints), nil
}

// jsonNumberToInt64 extracts an int64 from a value decoded by json.Decoder with
// UseNumber() (json.Number) or the plain Go numeric types. ok is false if v is
// not an integer-valued number.
func jsonNumberToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

// audioRangeError reports whether rate/channels fall outside the wire types'
// valid ranges (rate: u32; channels: 1..255). Single source of truth for the
// audio bounds, used by tryReconstructTyped's audio case to reject out-of-range
// values loudly rather than silently truncating them.
func audioRangeError(rate, channels int64) error {
	if rate < 0 || rate > math.MaxUint32 {
		return ErrInvalidAudioRate
	}
	if channels < 1 || channels > 255 {
		return ErrInvalidAudioCh
	}
	return nil
}

// tryReconstructTyped inspects a map[string]any for a "_type" key and, if the
// value matches a known ToAny projection, reconstructs the original typed Value.
// Returns (nil, nil) when the map is not a typed projection or is malformed (the
// caller falls back to a plain object); returns (nil, err) when the map is a
// recognized typed projection whose numeric fields are out of range (e.g. audio
// rate/channels), so the loud-rejection contract holds at every nesting depth.
func tryReconstructTyped(m map[string]any, enriched bool) (*Value, error) {
	typeStr, ok := m["_type"].(string)
	if !ok {
		return nil, nil
	}

	// Helper to get a string field
	strField := func(key string) (string, bool) {
		v, ok := m[key].(string)
		return v, ok
	}
	// Helper to decode a base64 string field
	b64Field := func(key string) ([]byte, bool) {
		s, ok := strField(key)
		if !ok {
			return nil, false
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, false
		}
		return b, true
	}
	// Helper to get an int from a json.Number or float64 field
	intField := func(key string) (int64, bool) {
		return jsonNumberToInt64(m[key])
	}

	switch typeStr {
	case "tensor":
		dtypeStr, ok1 := strField("dtype")
		dataBytes, ok2 := b64Field("data")
		dimsRaw, ok3 := m["dims"]
		if !ok1 || !ok2 || !ok3 {
			return nil, nil
		}
		dtype, ok := stringToDtype(dtypeStr)
		if !ok {
			return nil, nil
		}
		dimsSlice, ok := dimsRaw.([]any)
		if !ok {
			return nil, nil
		}
		dims := make([]uint64, len(dimsSlice))
		for i, d := range dimsSlice {
			switch v := d.(type) {
			case json.Number:
				i64, err := v.Int64()
				if err != nil {
					return nil, nil
				}
				dims[i] = uint64(i64)
			case float64:
				dims[i] = uint64(v)
			default:
				return nil, nil
			}
		}
		return Tensor(dtype, dims, dataBytes), nil

	case "tensor_ref":
		storeRaw, ok1 := intField("store")
		keyBytes, ok2 := b64Field("key")
		if !ok1 || !ok2 {
			return nil, nil
		}
		return TensorRef(uint8(storeRaw), keyBytes), nil

	case "image":
		fmtStr, ok1 := strField("format")
		w, ok2 := intField("width")
		h, ok3 := intField("height")
		dataBytes, ok4 := b64Field("data")
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return nil, nil
		}
		format, ok := stringToImageFormat(fmtStr)
		if !ok {
			return nil, nil
		}
		return Image(format, uint16(w), uint16(h), dataBytes), nil

	case "audio":
		encStr, ok1 := strField("encoding")
		rate, ok2 := intField("rate")
		ch, ok3 := intField("channels")
		dataBytes, ok4 := b64Field("data")
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return nil, nil
		}
		enc, ok := stringToAudioEncoding(encStr)
		if !ok {
			return nil, nil
		}
		// Out-of-range rate/channels must never be silently truncated into a
		// corrupt audio value — surface the specific error so the bridge rejects
		// it loudly at any nesting depth.
		if err := audioRangeError(rate, ch); err != nil {
			return nil, err
		}
		return Audio(enc, uint32(rate), uint8(ch), dataBytes), nil

	case "unknown_ext":
		extTypeRaw, ok1 := intField("ext_type")
		payload, ok2 := b64Field("payload")
		if !ok1 || !ok2 {
			return nil, nil
		}
		return UnknownExtension(uint64(extTypeRaw), payload), nil

	case "node":
		id, ok1 := strField("id")
		labelsRaw, ok2 := m["labels"]
		propsRaw := m["props"]
		if !ok1 || !ok2 {
			return nil, nil
		}
		labelsSlice, ok := labelsRaw.([]any)
		if !ok {
			return nil, nil
		}
		labels := make([]string, len(labelsSlice))
		for i, l := range labelsSlice {
			s, ok := l.(string)
			if !ok {
				return nil, nil
			}
			labels[i] = s
		}
		var props map[string]any
		if propsRaw != nil {
			pm, ok := propsRaw.(map[string]any)
			if !ok {
				return nil, nil
			}
			props = normalizePropsMap(pm)
		}
		return Node(id, labels, props), nil

	case "edge":
		fromID, ok1 := strField("fromId")
		toID, ok2 := strField("toId")
		edgeType, ok3 := strField("type")
		if !ok1 || !ok2 || !ok3 {
			return nil, nil
		}
		var props map[string]any
		if propsRaw := m["props"]; propsRaw != nil {
			pm, ok := propsRaw.(map[string]any)
			if !ok {
				return nil, nil
			}
			props = normalizePropsMap(pm)
		}
		return Edge(fromID, toID, edgeType, props), nil

	case "node_batch":
		nodesRaw, ok := m["nodes"]
		if !ok {
			return nil, nil
		}
		nodesSlice, ok := nodesRaw.([]any)
		if !ok {
			return nil, nil
		}
		nodes := make([]NodeData, 0, len(nodesSlice))
		for _, nr := range nodesSlice {
			nm, ok := nr.(map[string]any)
			if !ok {
				return nil, nil
			}
			nv, err := tryReconstructTyped(nm, enriched)
			if err != nil {
				return nil, err
			}
			if nv == nil || nv.typ != TypeNode {
				return nil, nil
			}
			nodes = append(nodes, nv.nodeVal)
		}
		return NodeBatch(nodes), nil

	case "edge_batch":
		edgesRaw, ok := m["edges"]
		if !ok {
			return nil, nil
		}
		edgesSlice, ok := edgesRaw.([]any)
		if !ok {
			return nil, nil
		}
		edges := make([]EdgeData, 0, len(edgesSlice))
		for _, er := range edgesSlice {
			em, ok := er.(map[string]any)
			if !ok {
				return nil, nil
			}
			ev, err := tryReconstructTyped(em, enriched)
			if err != nil {
				return nil, err
			}
			if ev == nil || ev.typ != TypeEdge {
				return nil, nil
			}
			edges = append(edges, ev.edgeVal)
		}
		return EdgeBatch(edges), nil

	case "bitmask":
		count, ok1 := intField("count")
		bits, ok2 := b64Field("bits")
		if !ok1 || !ok2 {
			return nil, nil
		}
		return Bitmask(uint64(count), bits), nil
	}

	return nil, nil
}

// normalizePropsMap converts a map[string]any decoded by json.Decoder.UseNumber()
// into codec-safe Go types: json.Number → int64 or float64, nested maps and
// slices are normalized recursively. This is needed because tryReconstructTyped
// receives raw decoded maps where all JSON numbers are json.Number, but the
// Encode property-type switch only accepts plain Go numerics.
func normalizePropsMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = normalizePropsValue(v)
	}
	return out
}

// normalizePropsValue normalizes a single props value recursively.
func normalizePropsValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		s := x.String()
		if strings.Contains(s, ".") || strings.Contains(s, "e") || strings.Contains(s, "E") {
			f, err := x.Float64()
			if err == nil {
				return f
			}
			return s
		}
		i, err := x.Int64()
		if err == nil {
			return i
		}
		// Doesn't fit in int64 — return as float64 (best effort)
		f, err := x.Float64()
		if err == nil {
			return f
		}
		return s
	case map[string]any:
		return normalizePropsMap(x)
	case []any:
		out := make([]any, len(x))
		for i, elem := range x {
			out[i] = normalizePropsValue(elem)
		}
		return out
	default:
		return v
	}
}

// stringToDtype converts a dtype string (as produced by dtypeToString) back to DType.
func stringToDtype(s string) (DType, bool) {
	switch s {
	case "float32":
		return DTypeFloat32, true
	case "float16":
		return DTypeFloat16, true
	case "bfloat16":
		return DTypeBFloat16, true
	case "float64":
		return DTypeFloat64, true
	case "int8":
		return DTypeInt8, true
	case "int16":
		return DTypeInt16, true
	case "int32":
		return DTypeInt32, true
	case "int64":
		return DTypeInt64, true
	case "uint8":
		return DTypeUint8, true
	case "uint16":
		return DTypeUint16, true
	case "uint32":
		return DTypeUint32, true
	case "uint64":
		return DTypeUint64, true
	default:
		return 0, false
	}
}

// stringToImageFormat converts an image format string back to ImageFormat.
func stringToImageFormat(s string) (ImageFormat, bool) {
	switch s {
	case "jpeg":
		return ImageFormatJPEG, true
	case "png":
		return ImageFormatPNG, true
	case "webp":
		return ImageFormatWEBP, true
	case "avif":
		return ImageFormatAVIF, true
	case "bmp":
		return ImageFormatBMP, true
	default:
		return 0, false
	}
}

// stringToAudioEncoding converts an audio encoding string back to AudioEncoding.
func stringToAudioEncoding(s string) (AudioEncoding, bool) {
	switch s {
	case "pcm_int16":
		return AudioEncodingPCMInt16, true
	case "pcm_float32":
		return AudioEncodingPCMFloat32, true
	case "opus":
		return AudioEncodingOPUS, true
	case "aac":
		return AudioEncodingAAC, true
	default:
		return 0, false
	}
}

// fromAnyStrict converts without any type inference - strings stay strings.
func fromAnyStrict(v any) *Value {
	if v == nil {
		return Null()
	}

	switch x := v.(type) {
	case bool:
		return Bool(x)

	case float64:
		// JSON numbers come as float64
		if x == math.Trunc(x) && x >= minSafeInt && x <= maxSafeInt {
			// It's an integer in safe range
			return Int64(int64(x))
		}
		return Float64(x)

	case json.Number:
		// If json.UseNumber was used
		if strings.Contains(x.String(), ".") || strings.Contains(x.String(), "e") {
			f, _ := x.Float64()
			return Float64(f)
		}
		i, err := x.Int64()
		if err == nil {
			return Int64(i)
		}
		// Non-negative values in (Int64Max, Uint64Max] are Uint (SPEC-v1 §1.1/§2.3.1)
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return Uint64(u)
		}
		// Try as big int
		bi := new(big.Int)
		bi.SetString(x.String(), 10)
		return BigInt(bigIntToTwosComplement(bi))

	case string:
		return String(x) // No inference - string stays string

	case []any:
		items := make([]*Value, len(x))
		for i, item := range x {
			items[i] = fromAnyStrict(item)
		}
		return Array(items...)

	case map[string]any:
		members := make([]Member, 0, len(x))
		// Sort keys by UTF-8 byte order so EncodeAny is deterministic
		// (Go map iteration order is randomized).
		for _, key := range sortedMapKeys(x) {
			members = append(members, Member{
				Key:   key,
				Value: fromAnyStrict(x[key]),
			})
		}
		return Object(members...)

	// Direct Go types (these are explicit, not inferred)
	case int:
		return Int64(int64(x))
	case int32:
		return Int64(int64(x))
	case int64:
		return Int64(x)
	case uint:
		return Uint64(uint64(x))
	case uint32:
		return Uint64(uint64(x))
	case uint64:
		return Uint64(x)
	case []byte:
		return Bytes(x)
	case time.Time:
		return Datetime(x)

	default:
		// Try to marshal and unmarshal as JSON
		data, err := json.Marshal(v)
		if err != nil {
			return String(fmt.Sprint(v))
		}
		var parsed any
		if err := json.Unmarshal(data, &parsed); err != nil {
			return String(fmt.Sprint(v))
		}
		return fromAnyStrict(parsed)
	}
}

// fromAnyEnriched converts with type inference for strings.
func fromAnyEnriched(v any, hints map[string]Type) *Value {
	if v == nil {
		return Null()
	}

	switch x := v.(type) {
	case bool:
		return Bool(x)

	case float64:
		// JSON numbers come as float64
		if x == math.Trunc(x) && x >= minSafeInt && x <= maxSafeInt {
			// It's an integer in safe range
			return Int64(int64(x))
		}
		return Float64(x)

	case json.Number:
		// If json.UseNumber was used
		if strings.Contains(x.String(), ".") || strings.Contains(x.String(), "e") {
			f, _ := x.Float64()
			return Float64(f)
		}
		i, err := x.Int64()
		if err == nil {
			return Int64(i)
		}
		// Non-negative values in (Int64Max, Uint64Max] are Uint (SPEC-v1 §1.1/§2.3.1)
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return Uint64(u)
		}
		// Try as big int
		bi := new(big.Int)
		bi.SetString(x.String(), 10)
		return BigInt(bigIntToTwosComplement(bi))

	case string:
		return inferStringType(x, hints, "")

	case []any:
		items := make([]*Value, len(x))
		for i, item := range x {
			items[i] = fromAnyEnriched(item, hints)
		}
		return Array(items...)

	case map[string]any:
		members := make([]Member, 0, len(x))
		// Sort keys by UTF-8 byte order for deterministic encoding.
		for _, key := range sortedMapKeys(x) {
			val := x[key]
			// Check hints for this field
			if hints != nil {
				if hint, ok := hints[key]; ok {
					members = append(members, Member{
						Key:   key,
						Value: fromAnyWithHint(val, hint),
					})
					continue
				}
			}
			members = append(members, Member{
				Key:   key,
				Value: fromAnyWithFieldHint(val, hints, key),
			})
		}
		return Object(members...)

	// Direct Go types
	case int:
		return Int64(int64(x))
	case int32:
		return Int64(int64(x))
	case int64:
		return Int64(x)
	case uint:
		return Uint64(uint64(x))
	case uint32:
		return Uint64(uint64(x))
	case uint64:
		return Uint64(x)
	case []byte:
		return Bytes(x)
	case time.Time:
		return Datetime(x)

	default:
		// Try to marshal and unmarshal as JSON
		data, err := json.Marshal(v)
		if err != nil {
			return String(fmt.Sprint(v))
		}
		var parsed any
		if err := json.Unmarshal(data, &parsed); err != nil {
			return String(fmt.Sprint(v))
		}
		return fromAnyEnriched(parsed, hints)
	}
}

func fromAnyWithFieldHint(v any, hints map[string]Type, fieldName string) *Value {
	if s, ok := v.(string); ok {
		return inferStringType(s, hints, fieldName)
	}
	return fromAnyEnriched(v, hints)
}

func fromAnyWithHint(v any, hint Type) *Value {
	switch hint {
	case TypeDatetime64:
		if s, ok := v.(string); ok {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err == nil {
				return Datetime(t)
			}
		}
	case TypeBytes:
		if s, ok := v.(string); ok {
			b, err := base64.StdEncoding.DecodeString(s)
			if err == nil {
				return Bytes(b)
			}
		}
	case TypeUUID128:
		if s, ok := v.(string); ok {
			uuid, err := parseUUID(s)
			if err == nil {
				return UUID128(uuid)
			}
		}
	}
	return fromAnyEnriched(v, nil)
}

// inferStringType tries to infer a more specific type from a string value.
func inferStringType(s string, hints map[string]Type, fieldName string) *Value {
	// Check field name hints
	if fieldName != "" {
		lowerField := strings.ToLower(fieldName)

		// Common datetime field names
		if strings.Contains(lowerField, "time") ||
			strings.Contains(lowerField, "date") ||
			strings.HasSuffix(lowerField, "_at") ||
			lowerField == "created" || lowerField == "updated" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return Datetime(t)
			}
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return Datetime(t)
			}
		}

		// Common binary field names
		if strings.Contains(lowerField, "data") ||
			strings.Contains(lowerField, "content") ||
			strings.Contains(lowerField, "payload") ||
			strings.Contains(lowerField, "blob") {
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				return Bytes(b)
			}
		}
	}

	// Pattern-based inference
	if iso8601Pattern.MatchString(s) {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return Datetime(t)
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return Datetime(t)
		}
	}

	if uuidPattern.MatchString(s) {
		if uuid, err := parseUUID(s); err == nil {
			return UUID128(uuid)
		}
	}

	return String(s)
}

// parseUUID parses a UUID string into bytes.
func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return uuid, fmt.Errorf("invalid UUID length")
	}
	for i := 0; i < 16; i++ {
		b, err := strconv.ParseUint(s[i*2:i*2+2], 16, 8)
		if err != nil {
			return uuid, err
		}
		uuid[i] = byte(b)
	}
	return uuid, nil
}

// ToJSON converts an Cowrie value to JSON bytes with canonical projections.
func ToJSON(v *Value) ([]byte, error) {
	return json.Marshal(ToAny(v))
}

// ToJSONIndent converts to indented JSON.
func ToJSONIndent(v *Value, indent string) ([]byte, error) {
	return json.MarshalIndent(ToAny(v), "", indent)
}

// ToAny converts an Cowrie value to a Go any value.
func ToAny(v *Value) any {
	if v == nil {
		return nil
	}

	switch v.typ {
	case TypeNull:
		return nil

	case TypeBool:
		return v.boolVal

	case TypeInt64:
		// Keep as number if in JS safe range
		if v.int64Val >= minSafeInt && v.int64Val <= maxSafeInt {
			return v.int64Val
		}
		// Convert to string for safety
		return strconv.FormatInt(v.int64Val, 10)

	case TypeUint64:
		// Keep as number if in JS safe range
		if v.uint64Val <= maxSafeInt {
			return v.uint64Val
		}
		// Convert to string for safety
		return strconv.FormatUint(v.uint64Val, 10)

	case TypeFloat64:
		return v.float64Val

	case TypeDecimal128:
		return formatDecimal128(v.decimal128)

	case TypeString:
		return v.stringVal

	case TypeBytes:
		return base64.StdEncoding.EncodeToString(v.bytesVal)

	case TypeDatetime64:
		t := time.Unix(0, v.datetime64).UTC()
		return t.Format(time.RFC3339Nano)

	case TypeUUID128:
		return formatUUID(v.uuid128)

	case TypeBigInt:
		bi := bigIntFromTwosComplement(v.bigintVal)
		return bi.String()

	case TypeArray:
		arr := make([]any, len(v.arrayVal))
		for i, item := range v.arrayVal {
			arr[i] = ToAny(item)
		}
		return arr

	case TypeObject:
		obj := make(map[string]any, len(v.objectVal))
		for _, m := range v.objectVal {
			obj[m.Key] = ToAny(m.Value)
		}
		return obj

	// v2.1 Extension Type JSON Projections
	case TypeTensor:
		t := v.tensorVal
		return map[string]any{
			"_type": "tensor",
			"dtype": dtypeToString(t.DType),
			"dims":  dimsToInts(t.Dims),
			"data":  base64.StdEncoding.EncodeToString(t.Data),
		}

	case TypeTensorRef:
		tr := v.tensorRefVal
		return map[string]any{
			"_type": "tensor_ref",
			"store": int(tr.StoreID),
			"key":   base64.StdEncoding.EncodeToString(tr.Key),
		}

	case TypeImage:
		img := v.imageVal
		return map[string]any{
			"_type":  "image",
			"format": imageFormatToString(img.Format),
			"width":  int(img.Width),
			"height": int(img.Height),
			"data":   base64.StdEncoding.EncodeToString(img.Data),
		}

	case TypeAudio:
		aud := v.audioVal
		return map[string]any{
			"_type":    "audio",
			"encoding": audioEncodingToString(aud.Encoding),
			"rate":     int(aud.SampleRate),
			"channels": int(aud.Channels),
			"data":     base64.StdEncoding.EncodeToString(aud.Data),
		}

	case TypeUnknownExt:
		ext := v.unknownExtVal
		return map[string]any{
			"_type":    "unknown_ext",
			"ext_type": ext.ExtType,
			"payload":  base64.StdEncoding.EncodeToString(ext.Payload),
		}

	case TypeNode:
		nd := v.nodeVal
		labels := make([]any, len(nd.Labels))
		for i, l := range nd.Labels {
			labels[i] = l
		}
		props := make(map[string]any, len(nd.Props))
		for k, pv := range nd.Props {
			props[k] = pv
		}
		return map[string]any{
			"_type":  "node",
			"id":     nd.ID,
			"labels": labels,
			"props":  props,
		}

	case TypeEdge:
		ed := v.edgeVal
		props := make(map[string]any, len(ed.Props))
		for k, pv := range ed.Props {
			props[k] = pv
		}
		return map[string]any{
			"_type":  "edge",
			"fromId": ed.From,
			"toId":   ed.To,
			"type":   ed.Type,
			"props":  props,
		}

	case TypeNodeBatch:
		nodes := v.nodeBatchVal.Nodes
		arr := make([]any, len(nodes))
		for i, nd := range nodes {
			labels := make([]any, len(nd.Labels))
			for j, l := range nd.Labels {
				labels[j] = l
			}
			props := make(map[string]any, len(nd.Props))
			for k, pv := range nd.Props {
				props[k] = pv
			}
			arr[i] = map[string]any{
				"_type":  "node",
				"id":     nd.ID,
				"labels": labels,
				"props":  props,
			}
		}
		return map[string]any{
			"_type": "node_batch",
			"nodes": arr,
		}

	case TypeEdgeBatch:
		edges := v.edgeBatchVal.Edges
		arr := make([]any, len(edges))
		for i, ed := range edges {
			props := make(map[string]any, len(ed.Props))
			for k, pv := range ed.Props {
				props[k] = pv
			}
			arr[i] = map[string]any{
				"_type":  "edge",
				"fromId": ed.From,
				"toId":   ed.To,
				"type":   ed.Type,
				"props":  props,
			}
		}
		return map[string]any{
			"_type": "edge_batch",
			"edges": arr,
		}

	case TypeBitmask:
		bm := v.bitmaskVal
		return map[string]any{
			"_type": "bitmask",
			"count": bm.Count,
			"bits":  base64.StdEncoding.EncodeToString(bm.Bits),
		}

	default:
		return nil
	}
}

// Helper functions for JSON projection

func dtypeToString(d DType) string {
	switch d {
	case DTypeFloat32:
		return "float32"
	case DTypeFloat16:
		return "float16"
	case DTypeBFloat16:
		return "bfloat16"
	case DTypeFloat64:
		return "float64"
	case DTypeInt8:
		return "int8"
	case DTypeInt16:
		return "int16"
	case DTypeInt32:
		return "int32"
	case DTypeInt64:
		return "int64"
	case DTypeUint8:
		return "uint8"
	case DTypeUint16:
		return "uint16"
	case DTypeUint32:
		return "uint32"
	case DTypeUint64:
		return "uint64"
	default:
		return "unknown"
	}
}

func dimsToInts(dims []uint64) []int {
	result := make([]int, len(dims))
	for i, d := range dims {
		result[i] = int(d)
	}
	return result
}

func imageFormatToString(f ImageFormat) string {
	switch f {
	case ImageFormatJPEG:
		return "jpeg"
	case ImageFormatPNG:
		return "png"
	case ImageFormatWEBP:
		return "webp"
	case ImageFormatAVIF:
		return "avif"
	case ImageFormatBMP:
		return "bmp"
	default:
		return "unknown"
	}
}

func audioEncodingToString(e AudioEncoding) string {
	switch e {
	case AudioEncodingPCMInt16:
		return "pcm_int16"
	case AudioEncodingPCMFloat32:
		return "pcm_float32"
	case AudioEncodingOPUS:
		return "opus"
	case AudioEncodingAAC:
		return "aac"
	default:
		return "unknown"
	}
}

// formatDecimal128 formats a Decimal128 as a decimal string.
func formatDecimal128(d Decimal128) string {
	// Coef is stored LITTLE-ENDIAN two's-complement (the §2.3 wire form). Convert
	// to big-endian unsigned magnitude, then apply the sign bit (the MSB lives in
	// the last LE byte, Coef[15]).
	be := make([]byte, 16)
	for i := 0; i < 16; i++ {
		be[i] = d.Coef[15-i]
	}
	bi := new(big.Int).SetBytes(be)
	if d.Coef[15]&0x80 != 0 {
		// Negative number (two's complement): subtract 2^128.
		bi.Sub(bi, new(big.Int).Lsh(big.NewInt(1), 128))
	}

	// Track sign separately so the decimal-point insertion below operates on the
	// unsigned magnitude string (a leading '-' would shift the digit positions).
	neg := bi.Sign() < 0
	bi.Abs(bi)
	s := bi.String()

	// Apply scale
	if d.Scale <= 0 {
		// No decimal point needed, or append zeros
		for i := int32(0); i > d.Scale; i-- {
			s += "0"
		}
		if neg && s != "0" {
			s = "-" + s
		}
		return s
	}

	// Insert decimal point
	scale := int(d.Scale)
	if scale >= len(s) {
		// Pad with leading zeros
		s = strings.Repeat("0", scale-len(s)+1) + s
	}

	pos := len(s) - scale
	out := s[:pos] + "." + s[pos:]
	if neg {
		out = "-" + out
	}
	return out
}

// formatUUID formats a UUID as a canonical string.
func formatUUID(uuid [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// bigIntToTwosComplement encodes a *big.Int as minimal-length two's-complement
// big-endian bytes, matching the SPEC wire format stored in bigintVal.
//
// Rules:
//   - Zero → []byte{0x00}
//   - Positive: big.Int.Bytes() gives the unsigned magnitude; if the top bit is
//     set we prepend 0x00 so the sign bit is clear.
//   - Negative: compute the two's-complement representation by taking the
//     absolute value, inverting all bits, and adding one. If the resulting top
//     bit is not set (would be misread as positive) we prepend 0xFF.
func reverseBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, v := range b {
		out[len(b)-1-i] = v
	}
	return out
}

// bigIntToTwosComplement returns minimal-length LITTLE-ENDIAN two's-complement (SPEC-v1 §2.3).
func bigIntToTwosComplement(n *big.Int) []byte {
	return reverseBytes(bigIntToTwosComplementBE(n))
}

func bigIntToTwosComplementBE(n *big.Int) []byte {
	switch n.Sign() {
	case 0:
		return []byte{0x00}
	case 1:
		b := n.Bytes() // unsigned magnitude, big-endian
		if b[0]&0x80 != 0 {
			// Top bit set — prepend 0x00 to keep sign positive
			return append([]byte{0x00}, b...)
		}
		return b
	default: // negative
		// Work with the absolute value
		abs := new(big.Int).Neg(n) // abs = -n (positive)
		b := abs.Bytes()           // unsigned big-endian magnitude

		// Two's complement: invert bits and add one.
		// We do this byte-by-byte with carry.
		result := make([]byte, len(b))
		for i, v := range b {
			result[i] = ^v
		}
		// Add 1 (carry from the right)
		carry := 1
		for i := len(result) - 1; i >= 0 && carry != 0; i-- {
			sum := int(result[i]) + carry
			result[i] = byte(sum)
			carry = sum >> 8
		}
		if carry != 0 {
			// Overflow: value was exactly -2^(8*len) — prepend 0x01 then zeros
			// This happens for e.g. -128 → {0x80} which is already correct,
			// but for exact powers of two we need an extra byte.
			result = append([]byte{0x01}, result...)
		}

		// If top bit is not set, the result would be misread as positive.
		// Prepend 0xFF to extend the sign.
		if result[0]&0x80 == 0 {
			result = append([]byte{0xFF}, result...)
		}
		return result
	}
}

// bigIntFromTwosComplement interprets b as a minimal-length two's-complement
// big-endian byte slice and returns the corresponding *big.Int.
//
// The high bit of b[0] is the sign bit: 0 = non-negative, 1 = negative.
// An empty slice is treated as zero.
func bigIntFromTwosComplement(b []byte) *big.Int {
	return bigIntFromTwosComplementBE(reverseBytes(b))
}

func bigIntFromTwosComplementBE(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}

	if b[0]&0x80 == 0 {
		// Non-negative: treat as unsigned magnitude
		result := new(big.Int).SetBytes(b)
		return result
	}

	// Negative: reverse the two's-complement to recover the magnitude.
	// Subtract one, then invert bits.
	tmp := make([]byte, len(b))
	copy(tmp, b)

	// Subtract 1 (borrow from right)
	borrow := 1
	for i := len(tmp) - 1; i >= 0 && borrow != 0; i-- {
		val := int(tmp[i]) - borrow
		if val < 0 {
			val += 256
			borrow = 1
		} else {
			borrow = 0
		}
		tmp[i] = byte(val)
	}

	// Invert all bits
	for i := range tmp {
		tmp[i] = ^tmp[i]
	}

	// The result is the positive magnitude; negate it
	magnitude := new(big.Int).SetBytes(tmp)
	return magnitude.Neg(magnitude)
}
