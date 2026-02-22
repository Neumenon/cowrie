package glyph

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidationError represents a validation failure.
type ValidationError struct {
	Path    string   // JSON-path style path to the error
	Message string   // Human-readable error message
	Code    string   // Machine-readable error code
	Pos     Position // Source position if available
}

func (e *ValidationError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s: %s", e.Path, e.Message)
	}
	return e.Message
}

// ValidationResult contains all validation errors and warnings.
type ValidationResult struct {
	Valid    bool
	Errors   []ValidationError
	Warnings []ValidationError
}

// Validator validates GValues against a Schema.
type Validator struct {
	schema           *Schema
	errors           []ValidationError
	warnings         []ValidationError
	compiledPatterns map[string]*regexp.Regexp
	strict           bool // If true, treat unknown fields as errors even for @open
}

// NewValidator creates a validator for the given schema.
func NewValidator(schema *Schema) *Validator {
	return &Validator{
		schema:           schema,
		compiledPatterns: make(map[string]*regexp.Regexp),
	}
}

// NewStrictValidator creates a validator that treats unknown fields as errors.
func NewStrictValidator(schema *Schema) *Validator {
	return &Validator{
		schema:           schema,
		compiledPatterns: make(map[string]*regexp.Regexp),
		strict:           true,
	}
}

// Validate validates a value against the schema.
func (v *Validator) Validate(value *GValue) *ValidationResult {
	v.errors = nil
	v.warnings = nil

	// If value is a struct, validate against its type
	if value.typ == TypeStruct {
		v.validateStruct(value, "", value.structVal.TypeName)
	} else {
		// Generic value validation without type context
		v.validateValue(value, "", TypeSpec{})
	}

	return &ValidationResult{
		Valid:    len(v.errors) == 0,
		Errors:   v.errors,
		Warnings: v.warnings,
	}
}

// ValidateAs validates a value as a specific type.
func (v *Validator) ValidateAs(value *GValue, typeName string) *ValidationResult {
	v.errors = nil
	v.warnings = nil

	td := v.schema.GetType(typeName)
	if td == nil {
		v.addError("", "type_not_found", "unknown type: %s", typeName)
		return &ValidationResult{Valid: false, Errors: v.errors}
	}

	switch td.Kind {
	case TypeDefStruct:
		if value.typ != TypeStruct && value.typ != TypeMap {
			v.addError("", "type_mismatch", "expected struct %s, got %s", typeName, value.typ)
		} else {
			v.validateStruct(value, "", typeName)
		}
	case TypeDefSum:
		if value.typ != TypeSum {
			v.addError("", "type_mismatch", "expected sum %s, got %s", typeName, value.typ)
		} else {
			v.validateSum(value, "", typeName)
		}
	}

	return &ValidationResult{
		Valid:    len(v.errors) == 0,
		Errors:   v.errors,
		Warnings: v.warnings,
	}
}

func (v *Validator) validateStruct(value *GValue, path, typeName string) {
	td := v.schema.GetType(typeName)
	if td == nil {
		v.addWarning(path, "unknown_type", "unknown type: %s", typeName)
		return
	}

	if td.Kind != TypeDefStruct || td.Struct == nil {
		v.addError(path, "type_mismatch", "%s is not a struct type", typeName)
		return
	}

	// Get fields from value
	var fields []MapEntry
	if value.typ == TypeStruct {
		fields = value.structVal.Fields
	} else if value.typ == TypeMap {
		fields = value.mapVal
	}

	// Build field lookup
	fieldValues := make(map[string]*GValue)
	for _, f := range fields {
		fieldValues[f.Key] = f.Value
	}

	// Validate each expected field
	for _, fieldDef := range td.Struct.Fields {
		fieldPath := joinPath(path, fieldDef.Name)
		fieldVal, exists := fieldValues[fieldDef.Name]

		// Also check wire key
		if !exists && fieldDef.WireKey != "" {
			if wkVal, ok := fieldValues[fieldDef.WireKey]; ok {
				fieldVal = wkVal
				exists = true
			}
		}

		if !exists {
			if fieldDef.Optional {
				continue
			}
			v.addError(fieldPath, "required_field", "required field missing: %s", fieldDef.Name)
			continue
		}

		// Validate field value against type spec
		v.validateValue(fieldVal, fieldPath, fieldDef.Type)

		// Validate constraints
		v.validateConstraints(fieldVal, fieldPath, fieldDef.Constraints)
	}

	// Check for unknown fields
	knownFields := make(map[string]bool)
	for _, fd := range td.Struct.Fields {
		knownFields[fd.Name] = true
		if fd.WireKey != "" {
			knownFields[fd.WireKey] = true
		}
	}

	for _, f := range fields {
		if !knownFields[f.Key] {
			if td.Open && !v.strict {
				// @open structs accept unknown fields (just a warning for info)
				v.addWarning(joinPath(path, f.Key), "unknown_field_captured", "unknown field captured: %s", f.Key)
			} else {
				// Strict mode or non-open structs reject unknown fields
				v.addError(joinPath(path, f.Key), "unknown_field", "unknown field: %s (type %s is not @open)", f.Key, typeName)
			}
		}
	}
}

func (v *Validator) validateSum(value *GValue, path, typeName string) {
	td := v.schema.GetType(typeName)
	if td == nil || td.Kind != TypeDefSum || td.Sum == nil {
		v.addError(path, "type_mismatch", "%s is not a sum type", typeName)
		return
	}

	sv := value.sumVal
	if sv == nil {
		v.addError(path, "invalid_sum", "invalid sum value")
		return
	}

	// Find matching variant
	var matchedVariant *VariantDef
	for _, variant := range td.Sum.Variants {
		if variant.Tag == sv.Tag {
			matchedVariant = variant
			break
		}
	}

	if matchedVariant == nil {
		validTags := make([]string, len(td.Sum.Variants))
		for i, vd := range td.Sum.Variants {
			validTags[i] = vd.Tag
		}
		v.addError(path, "invalid_variant", "unknown variant %s, expected one of: %s",
			sv.Tag, strings.Join(validTags, ", "))
		return
	}

	// Validate inner value
	v.validateValue(sv.Value, path, matchedVariant.Type)
}

func (v *Validator) validateValue(value *GValue, path string, spec TypeSpec) {
	if value == nil || value.IsNull() {
		// Null is generally valid unless spec says otherwise
		return
	}

	switch spec.Kind {
	case TypeSpecNull:
		if !value.IsNull() {
			v.addError(path, "type_mismatch", "expected null, got %s", value.typ)
		}

	case TypeSpecBool:
		if value.typ != TypeBool {
			v.addError(path, "type_mismatch", "expected bool, got %s", value.typ)
		}

	case TypeSpecInt:
		if value.typ != TypeInt {
			// Allow float that's actually an integer
			if value.typ == TypeFloat && isInteger(value.floatVal) {
				v.addWarning(path, "implicit_coercion", "float used as int")
			} else {
				v.addError(path, "type_mismatch", "expected int, got %s", value.typ)
			}
		}

	case TypeSpecFloat:
		if value.typ != TypeFloat && value.typ != TypeInt {
			v.addError(path, "type_mismatch", "expected float, got %s", value.typ)
		}

	case TypeSpecStr:
		if value.typ != TypeStr && value.typ != TypeID {
			v.addError(path, "type_mismatch", "expected str, got %s", value.typ)
		}

	case TypeSpecBytes:
		if value.typ != TypeBytes {
			v.addError(path, "type_mismatch", "expected bytes, got %s", value.typ)
		}

	case TypeSpecTime:
		if value.typ != TypeTime {
			v.addError(path, "type_mismatch", "expected time, got %s", value.typ)
		}

	case TypeSpecID:
		if value.typ != TypeID && value.typ != TypeStr {
			v.addError(path, "type_mismatch", "expected id, got %s", value.typ)
		}

	case TypeSpecList:
		if value.typ != TypeList {
			v.addError(path, "type_mismatch", "expected list, got %s", value.typ)
		} else if spec.Elem != nil {
			for i, elem := range value.listVal {
				v.validateValue(elem, fmt.Sprintf("%s[%d]", path, i), *spec.Elem)
			}
		}

	case TypeSpecMap:
		if value.typ != TypeMap {
			v.addError(path, "type_mismatch", "expected map, got %s", value.typ)
		} else if spec.ValType != nil {
			for _, entry := range value.mapVal {
				v.validateValue(entry.Value, joinPath(path, entry.Key), *spec.ValType)
			}
		}

	case TypeSpecRef:
		// Reference to named type
		td := v.schema.GetType(spec.Name)
		if td == nil {
			v.addWarning(path, "unknown_type", "unknown type reference: %s", spec.Name)
		} else if td.Kind == TypeDefStruct {
			if value.typ == TypeStruct || value.typ == TypeMap {
				v.validateStruct(value, path, spec.Name)
			} else {
				v.addError(path, "type_mismatch", "expected struct %s, got %s", spec.Name, value.typ)
			}
		} else if td.Kind == TypeDefSum {
			if value.typ == TypeSum {
				v.validateSum(value, path, spec.Name)
			} else {
				v.addError(path, "type_mismatch", "expected sum %s, got %s", spec.Name, value.typ)
			}
		}

	case TypeSpecInlineStruct:
		if value.typ != TypeStruct && value.typ != TypeMap {
			v.addError(path, "type_mismatch", "expected struct, got %s", value.typ)
		}
	}
}

func (v *Validator) validateConstraints(value *GValue, path string, constraints []Constraint) {
	for _, c := range constraints {
		switch c.Kind {
		case ConstraintMin:
			min := c.Value.(float64)
			if num, ok := value.Number(); ok {
				if num < min {
					v.addError(path, "constraint_min", "value %v is less than minimum %v", num, min)
				}
			}

		case ConstraintMax:
			max := c.Value.(float64)
			if num, ok := value.Number(); ok {
				if num > max {
					v.addError(path, "constraint_max", "value %v is greater than maximum %v", num, max)
				}
			}

		case ConstraintRange:
			r := c.Value.([2]float64)
			if num, ok := value.Number(); ok {
				if num < r[0] || num > r[1] {
					v.addError(path, "constraint_range", "value %v is outside range [%v, %v]", num, r[0], r[1])
				}
			}

		case ConstraintMinLen:
			minLen := c.Value.(int)
			if length := valueLength(value); length < minLen {
				v.addError(path, "constraint_min_len", "length %d is less than minimum %d", length, minLen)
			}

		case ConstraintMaxLen:
			maxLen := c.Value.(int)
			if length := valueLength(value); length > maxLen {
				v.addError(path, "constraint_max_len", "length %d is greater than maximum %d", length, maxLen)
			}

		case ConstraintLen:
			exactLen := c.Value.(int)
			if length := valueLength(value); length != exactLen {
				v.addError(path, "constraint_len", "length %d does not match required %d", length, exactLen)
			}

		case ConstraintNonEmpty:
			if valueLength(value) == 0 {
				v.addError(path, "constraint_nonempty", "value must not be empty")
			}

		case ConstraintRegex:
			pattern := c.Value.(string)
			if value.typ == TypeStr {
				re := v.getCompiledPattern(pattern)
				if re != nil && !re.MatchString(value.strVal) {
					v.addError(path, "constraint_regex", "value does not match pattern: %s", pattern)
				}
			}

		case ConstraintEnum:
			values := c.Value.([]string)
			if value.typ == TypeStr {
				found := false
				for _, ev := range values {
					if value.strVal == ev {
						found = true
						break
					}
				}
				if !found {
					v.addError(path, "constraint_enum", "value %q is not in allowed values: %v", value.strVal, values)
				}
			}

		case ConstraintUnique:
			if value.typ == TypeList {
				seen := make(map[string]bool)
				for i, elem := range value.listVal {
					key := Emit(elem)
					if seen[key] {
						v.addError(path, "constraint_unique", "duplicate value at index %d", i)
					}
					seen[key] = true
				}
			}
		}
	}
}

func (v *Validator) getCompiledPattern(pattern string) *regexp.Regexp {
	if re, ok := v.compiledPatterns[pattern]; ok {
		return re
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		v.addWarning("", "invalid_regex", "invalid regex pattern: %s", pattern)
		return nil
	}
	v.compiledPatterns[pattern] = re
	return re
}

func (v *Validator) addError(path, code, format string, args ...interface{}) {
	v.errors = append(v.errors, ValidationError{
		Path:    path,
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	})
}

func (v *Validator) addWarning(path, code, format string, args ...interface{}) {
	v.warnings = append(v.warnings, ValidationError{
		Path:    path,
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	})
}

// Helper functions

func joinPath(base, field string) string {
	if base == "" {
		return field
	}
	return base + "." + field
}

func valueLength(v *GValue) int {
	switch v.typ {
	case TypeStr:
		return len(v.strVal)
	case TypeBytes:
		return len(v.bytesVal)
	case TypeList:
		return len(v.listVal)
	case TypeMap:
		return len(v.mapVal)
	case TypeStruct:
		return len(v.structVal.Fields)
	default:
		return 0
	}
}

func isInteger(f float64) bool {
	return f == float64(int64(f))
}

// ============================================================
// Quick Validation Functions
// ============================================================

// ValidateWithSchema validates a value against a schema.
func ValidateWithSchema(value *GValue, schema *Schema) *ValidationResult {
	return NewValidator(schema).Validate(value)
}

// ValidateAs validates a value as a specific type.
func ValidateAs(value *GValue, schema *Schema, typeName string) *ValidationResult {
	return NewValidator(schema).ValidateAs(value, typeName)
}

// IsValid returns true if a value passes schema validation.
func IsValid(value *GValue, schema *Schema) bool {
	return NewValidator(schema).Validate(value).Valid
}

// ValidateStrict validates a value rejecting any unknown fields.
func ValidateStrict(value *GValue, schema *Schema) *ValidationResult {
	return NewStrictValidator(schema).Validate(value)
}
