package codec

import (
	"reflect"
	"strings"
	"sync"
)

// fieldInfo holds cached metadata about a struct field for fast unmarshaling.
type fieldInfo struct {
	name      string       // JSON tag name (or field name if no tag)
	index     []int        // Field index path for embedded structs
	typ       reflect.Type // Field type
	omitEmpty bool         // json:",omitempty" flag
	isPtr     bool         // Is pointer type
	isSlice   bool         // Is slice type
	elemType  reflect.Type // Element type for slices/pointers
}

// structInfo holds cached metadata about a struct type.
type structInfo struct {
	fields   []fieldInfo    // All exported fields
	fieldMap map[string]int // json_name -> index in fields slice
}

// typeCache is a concurrent-safe cache for struct type information.
type typeCache struct {
	mu    sync.RWMutex
	cache map[reflect.Type]*structInfo
}

var globalTypeCache = &typeCache{
	cache: make(map[reflect.Type]*structInfo),
}

// getStructInfo returns cached struct info, computing it if necessary.
func (tc *typeCache) getStructInfo(t reflect.Type) *structInfo {
	// Fast path: read lock
	tc.mu.RLock()
	info, ok := tc.cache[t]
	tc.mu.RUnlock()
	if ok {
		return info
	}

	// Slow path: compute and cache
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Double-check after acquiring write lock
	if info, ok := tc.cache[t]; ok {
		return info
	}

	info = buildStructInfo(t)
	tc.cache[t] = info
	return info
}

// buildStructInfo analyzes a struct type and extracts field metadata.
func buildStructInfo(t reflect.Type) *structInfo {
	info := &structInfo{
		fieldMap: make(map[string]int),
	}

	buildStructFields(t, nil, info)
	return info
}

// buildStructFields recursively builds field info, handling embedded structs.
func buildStructFields(t reflect.Type, index []int, info *structInfo) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Build full index path
		fieldIndex := make([]int, len(index)+1)
		copy(fieldIndex, index)
		fieldIndex[len(index)] = i

		// Handle embedded structs
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			buildStructFields(field.Type, fieldIndex, info)
			continue
		}

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Parse json tag
		tag := field.Tag.Get("json")
		name, opts := parseTag(tag)

		// Skip fields with json:"-"
		if name == "-" {
			continue
		}

		// Use field name if no json tag
		if name == "" {
			name = field.Name
		}

		fi := fieldInfo{
			name:      name,
			index:     fieldIndex,
			typ:       field.Type,
			omitEmpty: strings.Contains(opts, "omitempty"),
			isPtr:     field.Type.Kind() == reflect.Ptr,
			isSlice:   field.Type.Kind() == reflect.Slice,
		}

		if fi.isPtr && field.Type.Elem().Kind() != reflect.Invalid {
			fi.elemType = field.Type.Elem()
		} else if fi.isSlice && field.Type.Elem().Kind() != reflect.Invalid {
			fi.elemType = field.Type.Elem()
		}

		info.fieldMap[name] = len(info.fields)
		info.fields = append(info.fields, fi)
	}
}

// parseTag splits a json tag into name and options.
func parseTag(tag string) (name, opts string) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tag[idx+1:]
	}
	return tag, ""
}

// GetStructInfo is the public accessor for struct type information.
func GetStructInfo(t reflect.Type) *structInfo {
	return globalTypeCache.getStructInfo(t)
}
