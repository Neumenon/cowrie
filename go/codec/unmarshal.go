package codec

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/Neumenon/cowrie/go"
)

var (
	timeType       = reflect.TypeOf(time.Time{})
	rawMessageType = reflect.TypeOf(json.RawMessage{})
)

// unmarshalValue assigns an Cowrie value directly to a Go value via reflection.
// This eliminates the JSON bridge overhead by directly mapping Cowrie types to Go types.
func unmarshalValue(cowrieVal *cowrie.Value, target reflect.Value) error {
	// Handle nil Cowrie value
	if cowrieVal == nil || cowrieVal.Type() == cowrie.TypeNull {
		return setZero(target)
	}

	// Dereference pointers, allocating if necessary
	for target.Kind() == reflect.Ptr {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		target = target.Elem()
	}

	switch target.Kind() {
	case reflect.Struct:
		// Special case: time.Time from datetime64
		if target.Type() == timeType {
			return unmarshalTime(cowrieVal, target)
		}
		return unmarshalStruct(cowrieVal, target)
	case reflect.Slice:
		return unmarshalSlice(cowrieVal, target)
	case reflect.Map:
		return unmarshalMap(cowrieVal, target)
	case reflect.Interface:
		return unmarshalInterface(cowrieVal, target)
	default:
		return unmarshalPrimitive(cowrieVal, target)
	}
}

// unmarshalStruct handles struct fields using cached type info.
func unmarshalStruct(cowrieVal *cowrie.Value, target reflect.Value) error {
	if cowrieVal.Type() != cowrie.TypeObject {
		return fmt.Errorf("expected object for struct, got %s", cowrieVal.Type())
	}

	// Get cached struct info
	info := globalTypeCache.getStructInfo(target.Type())

	// Iterate Cowrie object members
	for _, member := range cowrieVal.Members() {
		idx, ok := info.fieldMap[member.Key]
		if !ok {
			continue // Unknown field, skip (like encoding/json default)
		}

		fi := info.fields[idx]
		fieldVal := target.FieldByIndex(fi.index)

		if !fieldVal.CanSet() {
			continue // Skip unsettable fields
		}

		if err := unmarshalValue(member.Value, fieldVal); err != nil {
			return fmt.Errorf("field %s: %w", fi.name, err)
		}
	}

	return nil
}

// unmarshalSlice handles slice types with special tensor optimization.
func unmarshalSlice(cowrieVal *cowrie.Value, target reflect.Value) error {
	targetType := target.Type()
	elemType := targetType.Elem()

	// Special case: json.RawMessage (convert Cowrie back to JSON)
	if targetType == rawMessageType {
		return unmarshalRawMessage(cowrieVal, target)
	}

	// Special case: []byte from Cowrie bytes or string
	if elemType.Kind() == reflect.Uint8 {
		return unmarshalBytes(cowrieVal, target)
	}

	// Special case: float32 slice from tensor (the key optimization!)
	if elemType.Kind() == reflect.Float32 && cowrieVal.Type() == cowrie.TypeTensor {
		floats := DecodeFloat32Tensor(cowrieVal)
		if floats != nil {
			target.Set(reflect.ValueOf(floats))
			return nil
		}
	}

	// Special case: float64 slice from tensor (convert from float32)
	if elemType.Kind() == reflect.Float64 && cowrieVal.Type() == cowrie.TypeTensor {
		floats := DecodeFloat32Tensor(cowrieVal)
		if floats != nil {
			f64s := make([]float64, len(floats))
			for i, f := range floats {
				f64s[i] = float64(f)
			}
			target.Set(reflect.ValueOf(f64s))
			return nil
		}
	}

	// Handle regular arrays
	if cowrieVal.Type() != cowrie.TypeArray {
		return fmt.Errorf("expected array for slice, got %s", cowrieVal.Type())
	}

	length := cowrieVal.Len()
	slice := reflect.MakeSlice(target.Type(), length, length)

	for i := 0; i < length; i++ {
		elem := slice.Index(i)
		if err := unmarshalValue(cowrieVal.Index(i), elem); err != nil {
			return fmt.Errorf("index %d: %w", i, err)
		}
	}

	target.Set(slice)
	return nil
}

// unmarshalMap handles map[string]T types.
func unmarshalMap(cowrieVal *cowrie.Value, target reflect.Value) error {
	if cowrieVal.Type() != cowrie.TypeObject {
		return fmt.Errorf("expected object for map, got %s", cowrieVal.Type())
	}

	if target.IsNil() {
		target.Set(reflect.MakeMap(target.Type()))
	}

	keyType := target.Type().Key()
	if keyType.Kind() != reflect.String {
		return fmt.Errorf("map key must be string, got %s", keyType)
	}

	elemType := target.Type().Elem()

	for _, member := range cowrieVal.Members() {
		elemVal := reflect.New(elemType).Elem()
		if err := unmarshalValue(member.Value, elemVal); err != nil {
			return fmt.Errorf("key %s: %w", member.Key, err)
		}
		target.SetMapIndex(reflect.ValueOf(member.Key), elemVal)
	}

	return nil
}

// unmarshalPrimitive handles basic types.
func unmarshalPrimitive(cowrieVal *cowrie.Value, target reflect.Value) error {
	cowrieType := cowrieVal.Type()

	switch target.Kind() {
	case reflect.String:
		if cowrieType == cowrie.TypeString {
			target.SetString(cowrieVal.String())
			return nil
		}
		// Allow other types to be converted to string
		target.SetString(fmt.Sprint(cowrie.ToAny(cowrieVal)))
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch cowrieType {
		case cowrie.TypeInt64:
			target.SetInt(cowrieVal.Int64())
			return nil
		case cowrie.TypeUint64:
			target.SetInt(int64(cowrieVal.Uint64()))
			return nil
		case cowrie.TypeFloat64:
			target.SetInt(int64(cowrieVal.Float64()))
			return nil
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch cowrieType {
		case cowrie.TypeUint64:
			target.SetUint(cowrieVal.Uint64())
			return nil
		case cowrie.TypeInt64:
			target.SetUint(uint64(cowrieVal.Int64()))
			return nil
		case cowrie.TypeFloat64:
			target.SetUint(uint64(cowrieVal.Float64()))
			return nil
		}

	case reflect.Float32, reflect.Float64:
		switch cowrieType {
		case cowrie.TypeFloat64:
			target.SetFloat(cowrieVal.Float64())
			return nil
		case cowrie.TypeInt64:
			target.SetFloat(float64(cowrieVal.Int64()))
			return nil
		case cowrie.TypeUint64:
			target.SetFloat(float64(cowrieVal.Uint64()))
			return nil
		}

	case reflect.Bool:
		if cowrieType == cowrie.TypeBool {
			target.SetBool(cowrieVal.Bool())
			return nil
		}
	}

	return fmt.Errorf("cannot unmarshal %s into %s", cowrieType, target.Type())
}

// unmarshalInterface handles interface{} targets.
// Returns the natural Go type (map[string]any, []any, etc.)
func unmarshalInterface(cowrieVal *cowrie.Value, target reflect.Value) error {
	goVal := fromCowrieValue(cowrieVal)
	if goVal == nil {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	target.Set(reflect.ValueOf(goVal))
	return nil
}

// setZero sets a reflect.Value to its zero value.
func setZero(v reflect.Value) error {
	if v.CanSet() {
		v.Set(reflect.Zero(v.Type()))
	}
	return nil
}

// unmarshalTime handles time.Time from datetime64 or string.
func unmarshalTime(cowrieVal *cowrie.Value, target reflect.Value) error {
	var t time.Time

	switch cowrieVal.Type() {
	case cowrie.TypeDatetime64:
		// datetime64 is nanoseconds since Unix epoch
		t = time.Unix(0, cowrieVal.Datetime64()).UTC()
	case cowrie.TypeString:
		// Try parsing as RFC3339
		var err error
		t, err = time.Parse(time.RFC3339Nano, cowrieVal.String())
		if err != nil {
			t, err = time.Parse(time.RFC3339, cowrieVal.String())
			if err != nil {
				return fmt.Errorf("cannot parse time from string: %s", cowrieVal.String())
			}
		}
	case cowrie.TypeInt64:
		// Unix timestamp in seconds
		t = time.Unix(cowrieVal.Int64(), 0).UTC()
	default:
		return fmt.Errorf("expected datetime64 for time.Time, got %s", cowrieVal.Type())
	}

	target.Set(reflect.ValueOf(t))
	return nil
}

// unmarshalRawMessage converts Cowrie value back to json.RawMessage (JSON bytes).
// This allows seamless round-tripping of embedded JSON data.
func unmarshalRawMessage(cowrieVal *cowrie.Value, target reflect.Value) error {
	// Convert Cowrie value to Go value, then marshal to JSON
	goVal := fromCowrieValue(cowrieVal)
	jsonBytes, err := json.Marshal(goVal)
	if err != nil {
		return fmt.Errorf("failed to marshal to JSON: %w", err)
	}
	target.SetBytes(jsonBytes)
	return nil
}

// unmarshalBytes handles []byte from Cowrie bytes type or string.
func unmarshalBytes(cowrieVal *cowrie.Value, target reflect.Value) error {
	switch cowrieVal.Type() {
	case cowrie.TypeBytes:
		target.SetBytes(cowrieVal.Bytes())
		return nil
	case cowrie.TypeString:
		// Allow string to []byte conversion
		target.SetBytes([]byte(cowrieVal.String()))
		return nil
	case cowrie.TypeNull:
		target.SetBytes(nil)
		return nil
	default:
		return fmt.Errorf("expected bytes for []byte, got %s", cowrieVal.Type())
	}
}
