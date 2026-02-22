package codec

import (
	"reflect"
	"sync"

	"github.com/Neumenon/cowrie"
)

// FastUnmarshaler is a type-specific unmarshaler that avoids reflection overhead.
// Use Register[T] to add fast unmarshalers for hot types.
type FastUnmarshaler func(*cowrie.Value, any) error

// typeRegistry holds pre-compiled unmarshalers for registered types.
type typeRegistry struct {
	mu           sync.RWMutex
	unmarshalers map[reflect.Type]FastUnmarshaler
}

var globalRegistry = &typeRegistry{
	unmarshalers: make(map[reflect.Type]FastUnmarshaler),
}

// Register adds a fast unmarshaler for a specific type.
// The unmarshaler function receives the Cowrie value and a pointer to the target type.
//
// Example:
//
//	codec.Register(func(v *cowrie.Value, resp *QueryResponse) error {
//	    if ids := v.Get("ids"); ids != nil {
//	        resp.IDs = decodeStringArray(ids)
//	    }
//	    if scores := v.Get("scores"); scores != nil {
//	        resp.Scores = codec.DecodeFloat32Tensor(scores)
//	    }
//	    return nil
//	})
func Register[T any](unmarshaler func(*cowrie.Value, *T) error) {
	// Use &zero pattern to get correct type even for pointer types.
	// reflect.TypeOf(nil pointer) returns nil, but reflect.TypeOf(&nilPtr).Elem() works.
	var zero T
	t := reflect.TypeOf(&zero).Elem()

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	globalRegistry.unmarshalers[t] = func(v *cowrie.Value, target any) error {
		return unmarshaler(v, target.(*T))
	}
}

// getFastUnmarshaler returns a registered unmarshaler for the given type, or nil.
func getFastUnmarshaler(t reflect.Type) FastUnmarshaler {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	return globalRegistry.unmarshalers[t]
}

// IsRegistered returns true if a fast unmarshaler is registered for the type.
func IsRegistered[T any]() bool {
	var zero T
	t := reflect.TypeOf(&zero).Elem()
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	_, ok := globalRegistry.unmarshalers[t]
	return ok
}

// Unregister removes a fast unmarshaler for a type.
// Useful for testing or dynamic reconfiguration.
func Unregister[T any]() {
	var zero T
	t := reflect.TypeOf(&zero).Elem()
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	delete(globalRegistry.unmarshalers, t)
}

// Helper functions for building fast unmarshalers

// DecodeStringArray decodes an Cowrie array to []string.
func DecodeStringArray(v *cowrie.Value) []string {
	if v == nil || v.Type() != cowrie.TypeArray {
		return nil
	}
	result := make([]string, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem != nil && elem.Type() == cowrie.TypeString {
			result[i] = elem.String()
		}
	}
	return result
}

// DecodeStringMapArray decodes an Cowrie array of objects to []map[string]string.
func DecodeStringMapArray(v *cowrie.Value) []map[string]string {
	if v == nil || v.Type() != cowrie.TypeArray {
		return nil
	}
	result := make([]map[string]string, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem != nil && elem.Type() == cowrie.TypeObject {
			m := make(map[string]string)
			for _, member := range elem.Members() {
				if member.Value != nil && member.Value.Type() == cowrie.TypeString {
					m[member.Key] = member.Value.String()
				}
			}
			result[i] = m
		}
	}
	return result
}

// GetString safely gets a string value from an object by key.
func GetString(v *cowrie.Value, key string) string {
	if v == nil || v.Type() != cowrie.TypeObject {
		return ""
	}
	for _, m := range v.Members() {
		if m.Key == key && m.Value != nil && m.Value.Type() == cowrie.TypeString {
			return m.Value.String()
		}
	}
	return ""
}

// GetStringArray safely gets a string array from an object by key.
func GetStringArray(v *cowrie.Value, key string) []string {
	if v == nil || v.Type() != cowrie.TypeObject {
		return nil
	}
	for _, m := range v.Members() {
		if m.Key == key {
			return DecodeStringArray(m.Value)
		}
	}
	return nil
}

// GetFloat32Array safely gets a float32 array from an object by key.
// Handles both tensor encoding and regular arrays.
func GetFloat32Array(v *cowrie.Value, key string) []float32 {
	if v == nil || v.Type() != cowrie.TypeObject {
		return nil
	}
	for _, m := range v.Members() {
		if m.Key == key {
			if m.Value == nil {
				return nil
			}
			// Try tensor first (optimized path)
			if m.Value.Type() == cowrie.TypeTensor {
				return DecodeFloat32Tensor(m.Value)
			}
			// Fall back to array
			if m.Value.Type() == cowrie.TypeArray {
				result := make([]float32, m.Value.Len())
				for i := 0; i < m.Value.Len(); i++ {
					elem := m.Value.Index(i)
					if elem != nil && elem.Type() == cowrie.TypeFloat64 {
						result[i] = float32(elem.Float64())
					}
				}
				return result
			}
		}
	}
	return nil
}
