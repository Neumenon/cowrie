package gen2

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

// FromJSON parses JSON bytes into an SJSON value without type inference.
// Strings remain strings, preserving exact JSON round-trip fidelity.
// Use FromJSONEnriched if you want automatic type inference (dates, UUIDs, etc.).
func FromJSON(data []byte) (*Value, error) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return FromAny(v), nil
}

// FromJSONEnriched parses JSON bytes with automatic type inference.
// Detects and converts:
//   - ISO 8601 strings in date-like fields → TypeDatetime64
//   - UUID-formatted strings → TypeUUID128
//   - Base64 strings in data-like fields → TypeBytes
//
// Use this when ingesting external data into the SJSON ecosystem.
// For strict JSON round-trip fidelity, use FromJSON instead.
func FromJSONEnriched(data []byte) (*Value, error) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return FromAnyEnriched(v), nil
}

// FromAny converts a Go value to an SJSON value without type inference.
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
		// Try as big int
		bi := new(big.Int)
		bi.SetString(x.String(), 10)
		return BigInt(bi.Bytes())

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
		for key, val := range x {
			members = append(members, Member{
				Key:   key,
				Value: fromAnyStrict(val),
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
		// Try as big int
		bi := new(big.Int)
		bi.SetString(x.String(), 10)
		return BigInt(bi.Bytes())

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
		for key, val := range x {
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

// ToJSON converts an SJSON value to JSON bytes with canonical projections.
func ToJSON(v *Value) ([]byte, error) {
	return json.Marshal(ToAny(v))
}

// ToJSONIndent converts to indented JSON.
func ToJSONIndent(v *Value, indent string) ([]byte, error) {
	return json.MarshalIndent(ToAny(v), "", indent)
}

// ToAny converts an SJSON value to a Go any value.
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
		bi := new(big.Int)
		bi.SetBytes(v.bigintVal)
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

	case TypeAdjlist:
		adj := v.adjlistVal
		return map[string]any{
			"_type":       "adjlist",
			"id_width":    idWidthToString(adj.IDWidth),
			"node_count":  adj.NodeCount,
			"edge_count":  adj.EdgeCount,
			"row_offsets": adj.RowOffsets,
			"col_indices": base64.StdEncoding.EncodeToString(adj.ColIndices),
		}

	case TypeRichText:
		rt := v.richTextVal
		result := map[string]any{
			"_type": "rich_text",
			"text":  rt.Text,
		}
		if len(rt.Tokens) > 0 {
			tokens := make([]int, len(rt.Tokens))
			for i, t := range rt.Tokens {
				tokens[i] = int(t)
			}
			result["tokens"] = tokens
		}
		if len(rt.Spans) > 0 {
			spans := make([]map[string]any, len(rt.Spans))
			for i, s := range rt.Spans {
				spans[i] = map[string]any{
					"start":   s.Start,
					"end":     s.End,
					"kind_id": s.KindID,
				}
			}
			result["spans"] = spans
		}
		return result

	case TypeDelta:
		d := v.deltaVal
		ops := make([]map[string]any, len(d.Ops))
		for i, op := range d.Ops {
			opMap := map[string]any{
				"op":       deltaOpCodeToString(op.OpCode),
				"field_id": op.FieldID,
			}
			if op.Value != nil {
				opMap["value"] = ToAny(op.Value)
			}
			ops[i] = opMap
		}
		return map[string]any{
			"_type":   "delta",
			"base_id": d.BaseID,
			"ops":     ops,
		}

	case TypeUnknownExt:
		ext := v.unknownExtVal
		return map[string]any{
			"_type":    "unknown_ext",
			"ext_type": ext.ExtType,
			"payload":  base64.StdEncoding.EncodeToString(ext.Payload),
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

func idWidthToString(w IDWidth) string {
	switch w {
	case IDWidthInt32:
		return "int32"
	case IDWidthInt64:
		return "int64"
	default:
		return "unknown"
	}
}

func deltaOpCodeToString(op DeltaOpCode) string {
	switch op {
	case DeltaOpSetField:
		return "set_field"
	case DeltaOpDeleteField:
		return "delete_field"
	case DeltaOpAppendArray:
		return "append_array"
	default:
		return "unknown"
	}
}

// formatDecimal128 formats a Decimal128 as a decimal string.
func formatDecimal128(d Decimal128) string {
	// Convert coefficient to big.Int
	bi := new(big.Int)
	bi.SetBytes(d.Coef[:])

	// Handle negative (two's complement)
	if d.Coef[0]&0x80 != 0 {
		// Negative number
		bi.Sub(bi, new(big.Int).Lsh(big.NewInt(1), 128))
	}

	s := bi.String()

	// Apply scale
	if d.Scale <= 0 {
		// No decimal point needed, or append zeros
		for i := int8(0); i > d.Scale; i-- {
			s += "0"
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
	return s[:pos] + "." + s[pos:]
}

// formatUUID formats a UUID as a canonical string.
func formatUUID(uuid [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
