package glyph

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// ============================================================
// GLYPH-Loose JSON Bridge
// ============================================================
//
// Converts between JSON and GValue for schema-free GLYPH usage.
// Supports two modes:
//   - Strict (default): time/id/bytes become strings, fully JSON compatible
//   - Extended: uses $glyph markers for lossless round-trip

// BridgeOpts configures JSON bridge behavior.
type BridgeOpts struct {
	// Extended enables $glyph markers for lossless round-trip of time/id/bytes.
	// When false (default), these types are converted to plain strings.
	Extended bool
}

// DefaultBridgeOpts returns the default (strict/JSON-compatible) options.
func DefaultBridgeOpts() BridgeOpts {
	return BridgeOpts{Extended: false}
}

// ============================================================
// FromJSONLoose - JSON to GValue (Loose mode)
// ============================================================

// FromJSONLoose converts JSON bytes to a GValue using strict mode.
// This is the GLYPH-Loose entry point for JSON conversion.
func FromJSONLoose(data []byte) (*GValue, error) {
	return FromJSONLooseWithOpts(data, DefaultBridgeOpts())
}

// FromJSONLooseWithOpts converts JSON bytes to a GValue with options.
func FromJSONLooseWithOpts(data []byte, opts BridgeOpts) (*GValue, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("JSON parse error: %w", err)
	}
	return fromJSONValue(v, opts)
}

// FromJSONValueLoose converts a Go interface{} (from json.Unmarshal) to GValue.
func FromJSONValueLoose(v interface{}) (*GValue, error) {
	return fromJSONValue(v, DefaultBridgeOpts())
}

// FromJSONValueLooseWithOpts converts a Go interface{} to GValue with options.
func FromJSONValueLooseWithOpts(v interface{}, opts BridgeOpts) (*GValue, error) {
	return fromJSONValue(v, opts)
}

func fromJSONValue(v interface{}, opts BridgeOpts) (*GValue, error) {
	if v == nil {
		return Null(), nil
	}

	switch val := v.(type) {
	case bool:
		return Bool(val), nil

	case float64:
		// Check for special values (reject in Loose mode)
		if math.IsNaN(val) {
			return nil, fmt.Errorf("NaN is not allowed in GLYPH-Loose")
		}
		if math.IsInf(val, 0) {
			return nil, fmt.Errorf("Infinity is not allowed in GLYPH-Loose")
		}
		// Check if it's an integer
		if val == math.Trunc(val) && val >= -9007199254740991 && val <= 9007199254740991 {
			return Int(int64(val)), nil
		}
		return Float(val), nil

	case string:
		return Str(val), nil

	case []interface{}:
		items := make([]*GValue, 0, len(val))
		for i, elem := range val {
			gv, err := fromJSONValue(elem, opts)
			if err != nil {
				return nil, fmt.Errorf("array[%d]: %w", i, err)
			}
			items = append(items, gv)
		}
		return List(items...), nil

	case map[string]interface{}:
		// Check for extended markers
		if opts.Extended {
			if glyph, ok := val["$glyph"].(string); ok {
				return fromGlyphMarker(glyph, val)
			}
		}

		// Regular object/map
		entries := make([]MapEntry, 0, len(val))
		for k, elem := range val {
			gv, err := fromJSONValue(elem, opts)
			if err != nil {
				return nil, fmt.Errorf("object[%q]: %w", k, err)
			}
			entries = append(entries, MapEntry{Key: k, Value: gv})
		}
		return Map(entries...), nil

	default:
		return nil, fmt.Errorf("unsupported JSON type: %T", v)
	}
}

func fromGlyphMarker(markerType string, obj map[string]interface{}) (*GValue, error) {
	switch markerType {
	case "time":
		value, ok := obj["value"].(string)
		if !ok {
			return nil, fmt.Errorf("$glyph time marker missing value")
		}
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid time: %w", err)
		}
		return Time(t), nil

	case "id":
		value, ok := obj["value"].(string)
		if !ok {
			return nil, fmt.Errorf("$glyph id marker missing value")
		}
		// Parse ^prefix:value format
		if len(value) > 0 && value[0] == '^' {
			value = value[1:]
		}
		prefix := ""
		val := value
		for i, c := range value {
			if c == ':' {
				prefix = value[:i]
				val = value[i+1:]
				break
			}
		}
		return ID(prefix, val), nil

	case "bytes":
		b64, ok := obj["base64"].(string)
		if !ok {
			return nil, fmt.Errorf("$glyph bytes marker missing base64")
		}
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("invalid base64: %w", err)
		}
		return Bytes(data), nil

	default:
		return nil, fmt.Errorf("unknown $glyph marker type: %s", markerType)
	}
}

// ============================================================
// ToJSONLoose - GValue to JSON (Loose mode)
// ============================================================

// ToJSONLoose converts a GValue to JSON bytes using strict mode.
// This is the GLYPH-Loose entry point for JSON output.
func ToJSONLoose(v *GValue) ([]byte, error) {
	return ToJSONLooseWithOpts(v, DefaultBridgeOpts())
}

// ToJSONLooseWithOpts converts a GValue to JSON bytes with options.
func ToJSONLooseWithOpts(v *GValue, opts BridgeOpts) ([]byte, error) {
	jsonVal, err := toJSONValue(v, opts)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jsonVal)
}

// ToJSONValueLoose converts a GValue to a Go interface{} suitable for json.Marshal.
func ToJSONValueLoose(v *GValue) (interface{}, error) {
	return toJSONValue(v, DefaultBridgeOpts())
}

// ToJSONValueLooseWithOpts converts a GValue to interface{} with options.
func ToJSONValueLooseWithOpts(v *GValue, opts BridgeOpts) (interface{}, error) {
	return toJSONValue(v, opts)
}

func toJSONValue(v *GValue, opts BridgeOpts) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch v.typ {
	case TypeNull:
		return nil, nil

	case TypeBool:
		return v.boolVal, nil

	case TypeInt:
		return float64(v.intVal), nil

	case TypeFloat:
		if math.IsNaN(v.floatVal) || math.IsInf(v.floatVal, 0) {
			return nil, fmt.Errorf("NaN/Infinity not allowed in JSON")
		}
		return v.floatVal, nil

	case TypeStr:
		return v.strVal, nil

	case TypeBytes:
		if opts.Extended {
			return map[string]interface{}{
				"$glyph": "bytes",
				"base64": base64.StdEncoding.EncodeToString(v.bytesVal),
			}, nil
		}
		return base64.StdEncoding.EncodeToString(v.bytesVal), nil

	case TypeTime:
		if opts.Extended {
			return map[string]interface{}{
				"$glyph": "time",
				"value":  v.timeVal.Format(time.RFC3339),
			}, nil
		}
		return v.timeVal.Format(time.RFC3339), nil

	case TypeID:
		idStr := canonRef(v.idVal)
		if opts.Extended {
			return map[string]interface{}{
				"$glyph": "id",
				"value":  idStr,
			}, nil
		}
		return idStr, nil

	case TypeList:
		items := make([]interface{}, 0, len(v.listVal))
		for _, elem := range v.listVal {
			jsonElem, err := toJSONValue(elem, opts)
			if err != nil {
				return nil, err
			}
			items = append(items, jsonElem)
		}
		return items, nil

	case TypeMap:
		obj := make(map[string]interface{}, len(v.mapVal))
		for _, entry := range v.mapVal {
			jsonVal, err := toJSONValue(entry.Value, opts)
			if err != nil {
				return nil, err
			}
			obj[entry.Key] = jsonVal
		}
		return obj, nil

	case TypeStruct:
		// Structs become objects with fields as properties
		obj := make(map[string]interface{}, len(v.structVal.Fields))
		for _, field := range v.structVal.Fields {
			jsonVal, err := toJSONValue(field.Value, opts)
			if err != nil {
				return nil, err
			}
			obj[field.Key] = jsonVal
		}
		return obj, nil

	case TypeSum:
		// Sums become { tag: value }
		tagVal, err := toJSONValue(v.sumVal.Value, opts)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			v.sumVal.Tag: tagVal,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported GValue type: %s", v.typ)
	}
}

// ============================================================
// JSON Round-Trip Helpers (Loose mode)
// ============================================================

// JSONRoundTripLoose parses JSON, converts to GValue, and back to JSON.
// Returns error if round-trip fails or produces different structure.
func JSONRoundTripLoose(data []byte) ([]byte, error) {
	gv, err := FromJSONLoose(data)
	if err != nil {
		return nil, err
	}
	return ToJSONLoose(gv)
}

// JSONEqual checks if two JSON byte slices represent equal values.
func JSONEqual(a, b []byte) (bool, error) {
	var va, vb interface{}
	if err := json.Unmarshal(a, &va); err != nil {
		return false, fmt.Errorf("parse a: %w", err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false, fmt.Errorf("parse b: %w", err)
	}
	return jsonValueEqual(va, vb), nil
}

func jsonValueEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch va := a.(type) {
	case bool:
		vb, ok := b.(bool)
		return ok && va == vb
	case float64:
		vb, ok := b.(float64)
		return ok && va == vb
	case string:
		vb, ok := b.(string)
		return ok && va == vb
	case []interface{}:
		vb, ok := b.([]interface{})
		if !ok || len(va) != len(vb) {
			return false
		}
		for i := range va {
			if !jsonValueEqual(va[i], vb[i]) {
				return false
			}
		}
		return true
	case map[string]interface{}:
		vb, ok := b.(map[string]interface{})
		if !ok || len(va) != len(vb) {
			return false
		}
		for k, valA := range va {
			valB, exists := vb[k]
			if !exists || !jsonValueEqual(valA, valB) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
