package delta

import (
	"fmt"

	"github.com/Neumenon/cowrie/go"
)

// Apply applies a delta to a base value, producing the target value.
// The base value is not modified; a new value is returned.
func Apply(base *cowrie.Value, delta *cowrie.DeltaData, fieldDict []string) (*cowrie.Value, error) {
	if delta == nil || len(delta.Ops) == 0 {
		// No changes, return copy of base
		return base, nil
	}

	if base == nil {
		return nil, fmt.Errorf("cannot apply delta to nil base")
	}

	if base.Type() != cowrie.TypeObject {
		return nil, fmt.Errorf("delta application requires object base, got %s", base.Type())
	}

	// Build mutable copy of base object
	baseMembers := base.Members()
	result := make(map[string]*cowrie.Value, len(baseMembers))
	for _, m := range baseMembers {
		result[m.Key] = m.Value
	}

	// Apply operations in order
	for _, op := range delta.Ops {
		fieldName, err := resolveFieldID(op.FieldID, fieldDict)
		if err != nil {
			return nil, fmt.Errorf("op %d: %w", op.OpCode, err)
		}

		switch op.OpCode {
		case cowrie.DeltaOpSetField:
			result[fieldName] = op.Value

		case cowrie.DeltaOpDeleteField:
			delete(result, fieldName)

		case cowrie.DeltaOpAppendArray:
			existing, ok := result[fieldName]
			if !ok {
				// Create new array
				result[fieldName] = cowrie.Array(op.Value)
			} else if existing.Type() == cowrie.TypeArray {
				// Append to existing array
				items := make([]*cowrie.Value, existing.Len()+1)
				for i := 0; i < existing.Len(); i++ {
					items[i] = existing.Index(i)
				}
				items[existing.Len()] = op.Value
				result[fieldName] = cowrie.Array(items...)
			} else {
				return nil, fmt.Errorf("cannot append to non-array field %q", fieldName)
			}

		default:
			return nil, fmt.Errorf("unknown delta operation: %d", op.OpCode)
		}
	}

	// Convert back to Cowrie object
	members := make([]cowrie.Member, 0, len(result))
	for k, v := range result {
		members = append(members, cowrie.Member{Key: k, Value: v})
	}

	return cowrie.Object(members...), nil
}

// ApplyWithStore applies a delta using a base store for lookups.
func ApplyWithStore(delta *cowrie.DeltaData, store *BaseStore) (*cowrie.Value, error) {
	if delta == nil {
		return nil, fmt.Errorf("nil delta")
	}

	entry, ok := store.Get(delta.BaseID)
	if !ok {
		return nil, fmt.Errorf("base object %d not found in store", delta.BaseID)
	}

	return Apply(entry.Value, delta, entry.FieldDict)
}

// resolveFieldID converts a field ID to a field name using the dictionary.
// For simplicity in the initial implementation, fieldID is used as an index.
// If fieldDict is empty, we fall back to string conversion.
func resolveFieldID(fieldID uint64, fieldDict []string) (string, error) {
	if len(fieldDict) == 0 {
		// Fallback: use field ID as string (requires sender to include field names)
		return fmt.Sprintf("field_%d", fieldID), nil
	}

	if int(fieldID) >= len(fieldDict) {
		return "", fmt.Errorf("field ID %d out of bounds (dict size: %d)", fieldID, len(fieldDict))
	}

	return fieldDict[fieldID], nil
}

// ApplyConfig configures delta application behavior.
type ApplyConfig struct {
	// StrictMode returns error on unknown field IDs.
	// When false, unknown fields are skipped.
	StrictMode bool

	// PreserveOrder maintains field order from base object.
	PreserveOrder bool
}

// DefaultApplyConfig returns the default apply configuration.
func DefaultApplyConfig() ApplyConfig {
	return ApplyConfig{
		StrictMode:    false,
		PreserveOrder: false,
	}
}
