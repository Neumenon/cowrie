package glyph

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ============================================================
// GLYPH Schema Evolution
// ============================================================
//
// Enables schemas to evolve safely without breaking clients. Supports:
//   - Adding new optional fields
//   - Renaming fields (with compatibility mapping)
//   - Deprecating fields
//   - Changing defaults
//   - Strict vs tolerant parsing modes
//
// Problem It Solves:
//   v1: Match{home=Arsenal away=Liverpool}
//   v2: Match{home=Arsenal away=Liverpool venue="Emirates"}
//       ↑ Parser fails on unknown field "venue"
//
// Solution:
//   Schema tracks versions and applies migrations automatically
//   v1 clients can read v2 data (missing fields get defaults)
//   v2 clients can read v1 data (new fields optional)

// EvolutionMode specifies how the schema handles version differences.
type EvolutionMode int

const (
	// ModeStrict fails on unknown fields
	ModeStrict EvolutionMode = iota
	// ModeTolerant ignores unknown fields (default)
	ModeTolerant
	// ModeMigrate auto-migrates between versions
	ModeMigrate
)

// String returns the mode name.
func (m EvolutionMode) String() string {
	switch m {
	case ModeStrict:
		return "strict"
	case ModeTolerant:
		return "tolerant"
	case ModeMigrate:
		return "migrate"
	default:
		return "unknown"
	}
}

// ============================================================
// Field Evolution Schema
// ============================================================

// EvolvingFieldType represents field types for schema evolution.
type EvolvingFieldType string

const (
	FieldTypeStr     EvolvingFieldType = "str"
	FieldTypeInt     EvolvingFieldType = "int"
	FieldTypeFloat   EvolvingFieldType = "float"
	FieldTypeBool    EvolvingFieldType = "bool"
	FieldTypeList    EvolvingFieldType = "list"
	FieldTypeDecimal EvolvingFieldType = "decimal"
)

// EvolvingField represents a field in a versioned schema.
type EvolvingField struct {
	Name         string            // Field name
	Type         EvolvingFieldType // Field type
	Required     bool              // Whether field is required
	Default      interface{}       // Default value if any
	AddedIn      string            // Version when field was added (e.g., "1.0")
	DeprecatedIn string            // Version when field was deprecated (empty if not deprecated)
	RenamedFrom  string            // Previous field name if renamed
	Validation   *regexp.Regexp    // Optional validation pattern
}

// IsAvailableIn checks if field is available in a given version.
func (f *EvolvingField) IsAvailableIn(version string) bool {
	if compareVersions(version, f.AddedIn) < 0 {
		return false // Field not added yet
	}

	if f.DeprecatedIn != "" && compareVersions(version, f.DeprecatedIn) >= 0 {
		return false // Field is deprecated
	}

	return true
}

// IsDeprecatedIn checks if field is deprecated in a given version.
func (f *EvolvingField) IsDeprecatedIn(version string) bool {
	if f.DeprecatedIn == "" {
		return false
	}
	return compareVersions(version, f.DeprecatedIn) >= 0
}

// ValidateValue validates a value against this field schema.
// Returns error message if invalid, empty string if valid.
func (f *EvolvingField) ValidateValue(value interface{}) string {
	if value == nil {
		if f.Required {
			return fmt.Sprintf("field %s is required", f.Name)
		}
		return ""
	}

	// Type checking
	switch f.Type {
	case FieldTypeStr:
		str, ok := value.(string)
		if !ok {
			return fmt.Sprintf("field %s must be string", f.Name)
		}
		if f.Validation != nil && !f.Validation.MatchString(str) {
			return fmt.Sprintf("field %s does not match pattern", f.Name)
		}
	case FieldTypeInt:
		switch value.(type) {
		case int, int32, int64:
			// OK
		default:
			return fmt.Sprintf("field %s must be int", f.Name)
		}
	case FieldTypeFloat:
		switch value.(type) {
		case float32, float64, int, int32, int64:
			// OK (allow int as float)
		default:
			return fmt.Sprintf("field %s must be float", f.Name)
		}
	case FieldTypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Sprintf("field %s must be bool", f.Name)
		}
	case FieldTypeList:
		switch value.(type) {
		case []interface{}, []string, []int, []int64, []float64:
			// OK
		default:
			return fmt.Sprintf("field %s must be list", f.Name)
		}
	}

	return ""
}

// ============================================================
// Version Schema
// ============================================================

// VersionSchema represents a schema for a specific version.
type VersionSchema struct {
	Name        string                    // Type name (e.g., "Match")
	Version     string                    // Version string (e.g., "1.0")
	Fields      map[string]*EvolvingField // Field name → schema
	Description string                    // Human-readable description
}

// NewVersionSchema creates a new version schema.
func NewVersionSchema(name, version string) *VersionSchema {
	return &VersionSchema{
		Name:    name,
		Version: version,
		Fields:  make(map[string]*EvolvingField),
	}
}

// AddField adds a field to this version.
func (vs *VersionSchema) AddField(field *EvolvingField) {
	vs.Fields[field.Name] = field
}

// GetField gets a field by name.
func (vs *VersionSchema) GetField(name string) *EvolvingField {
	return vs.Fields[name]
}

// Validate validates data against this schema version.
// Returns error message if invalid, empty string if valid.
func (vs *VersionSchema) Validate(data map[string]interface{}) string {
	// Check required fields
	for fieldName, fieldSchema := range vs.Fields {
		if fieldSchema.Required {
			if _, exists := data[fieldName]; !exists {
				return fmt.Sprintf("missing required field: %s", fieldName)
			}
		}
	}

	// Check field values
	for fieldName, value := range data {
		fieldSchema := vs.Fields[fieldName]
		if fieldSchema != nil {
			if err := fieldSchema.ValidateValue(value); err != "" {
				return err
			}
		}
	}

	return ""
}

// ============================================================
// Versioned Schema
// ============================================================

// VersionedSchema manages multiple versions of a schema.
type VersionedSchema struct {
	Name          string                    // Type name
	Versions      map[string]*VersionSchema // Version → schema
	LatestVersion string                    // Latest version string
	Mode          EvolutionMode             // How to handle version differences
}

// NewVersionedSchema creates a new versioned schema.
func NewVersionedSchema(name string) *VersionedSchema {
	return &VersionedSchema{
		Name:          name,
		Versions:      make(map[string]*VersionSchema),
		LatestVersion: "1.0",
		Mode:          ModeTolerant,
	}
}

// FieldConfig is used to configure fields when adding a version.
type FieldConfig struct {
	Type         EvolvingFieldType
	Required     bool
	Default      interface{}
	AddedIn      string
	DeprecatedIn string
	RenamedFrom  string
	Validation   string // Regex pattern
}

// AddVersion adds a version to the schema.
func (vs *VersionedSchema) AddVersion(version string, fields map[string]FieldConfig) error {
	versionSchema := NewVersionSchema(vs.Name, version)

	for fieldName, config := range fields {
		addedIn := config.AddedIn
		if addedIn == "" {
			addedIn = version
		}

		var validation *regexp.Regexp
		if config.Validation != "" {
			var err error
			validation, err = regexp.Compile(config.Validation)
			if err != nil {
				return fmt.Errorf("invalid validation pattern for field %s: %w", fieldName, err)
			}
		}

		field := &EvolvingField{
			Name:         fieldName,
			Type:         config.Type,
			Required:     config.Required,
			Default:      config.Default,
			AddedIn:      addedIn,
			DeprecatedIn: config.DeprecatedIn,
			RenamedFrom:  config.RenamedFrom,
			Validation:   validation,
		}

		versionSchema.AddField(field)
	}

	vs.Versions[version] = versionSchema
	vs.LatestVersion = vs.getLatestVersion()

	return nil
}

// GetVersion gets schema for a specific version.
func (vs *VersionedSchema) GetVersion(version string) *VersionSchema {
	return vs.Versions[version]
}

// EvolutionParseResult holds the result of parsing versioned data.
type EvolutionParseResult struct {
	Error string
	Data  map[string]interface{}
}

// Parse parses data from a specific version.
func (vs *VersionedSchema) Parse(data map[string]interface{}, fromVersion string) EvolutionParseResult {
	schema := vs.GetVersion(fromVersion)
	if schema == nil {
		return EvolutionParseResult{Error: fmt.Sprintf("unknown version: %s", fromVersion)}
	}

	// Validate
	if err := schema.Validate(data); err != "" && vs.Mode == ModeStrict {
		return EvolutionParseResult{Error: err}
	}

	result := copyMap(data)

	// Migrate to latest if needed
	if fromVersion != vs.LatestVersion {
		migrated, err := vs.migrate(data, fromVersion, vs.LatestVersion)
		if err != "" {
			return EvolutionParseResult{Error: err}
		}
		result = migrated
	}

	// Filter unknown fields in tolerant mode
	if vs.Mode == ModeTolerant {
		targetSchema := vs.GetVersion(vs.LatestVersion)
		if targetSchema != nil {
			filtered := make(map[string]interface{})
			for k, v := range result {
				if _, exists := targetSchema.Fields[k]; exists {
					filtered[k] = v
				}
			}
			result = filtered
		}
	}

	return EvolutionParseResult{Data: result}
}

// EvolutionEmitResult holds the result of emitting versioned data.
type EvolutionEmitResult struct {
	Error  string
	Header string
}

// Emit emits data for a specific version.
func (vs *VersionedSchema) Emit(data map[string]interface{}, version string) EvolutionEmitResult {
	if version == "" {
		version = vs.LatestVersion
	}

	schema := vs.GetVersion(version)
	if schema == nil {
		return EvolutionEmitResult{Error: fmt.Sprintf("unknown version: %s", version)}
	}

	// Validate
	if err := schema.Validate(data); err != "" {
		return EvolutionEmitResult{Error: err}
	}

	// Format version header
	header := fmt.Sprintf("@version %s", version)
	return EvolutionEmitResult{Header: header}
}

// migrate migrates data from one version to another.
func (vs *VersionedSchema) migrate(data map[string]interface{}, fromVersion, toVersion string) (map[string]interface{}, string) {
	currentVersion := fromVersion
	currentData := copyMap(data)

	// Get migration path
	path := vs.getMigrationPath(fromVersion, toVersion)
	if path == nil {
		return nil, fmt.Sprintf("cannot migrate from %s to %s", fromVersion, toVersion)
	}

	// Apply each migration step
	for _, nextVersion := range path {
		migrated, err := vs.migrateStep(currentData, currentVersion, nextVersion)
		if err != "" {
			return nil, err
		}
		currentData = migrated
		currentVersion = nextVersion
	}

	return currentData, ""
}

// migrateStep migrates data from one version to the next.
func (vs *VersionedSchema) migrateStep(data map[string]interface{}, fromVersion, toVersion string) (map[string]interface{}, string) {
	fromSchema := vs.GetVersion(fromVersion)
	toSchema := vs.GetVersion(toVersion)

	if fromSchema == nil || toSchema == nil {
		return nil, "invalid version"
	}

	result := copyMap(data)

	// 1. Handle field renames
	for fieldName, fieldSchema := range toSchema.Fields {
		if fieldSchema.RenamedFrom != "" {
			oldName := fieldSchema.RenamedFrom
			if oldVal, exists := result[oldName]; exists {
				if _, newExists := result[fieldName]; !newExists {
					result[fieldName] = oldVal
					delete(result, oldName)
				}
			}
		}
	}

	// 2. Handle new fields (add defaults)
	for fieldName, fieldSchema := range toSchema.Fields {
		if _, exists := result[fieldName]; !exists {
			if fieldSchema.Default != nil {
				result[fieldName] = fieldSchema.Default
			} else if !fieldSchema.Required {
				result[fieldName] = nil
			}
		}
	}

	// 3. Remove unknown fields (tolerant mode)
	if vs.Mode == ModeTolerant {
		filtered := make(map[string]interface{})
		for k, v := range result {
			if _, exists := toSchema.Fields[k]; exists {
				filtered[k] = v
			}
		}
		result = filtered
	}

	return result, ""
}

// getMigrationPath gets the migration path between versions.
func (vs *VersionedSchema) getMigrationPath(fromVersion, toVersion string) []string {
	// Get sorted versions
	versions := make([]string, 0, len(vs.Versions))
	for v := range vs.Versions {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool {
		return compareVersions(versions[i], versions[j]) < 0
	})

	// Find indices
	fromIdx := -1
	toIdx := -1
	for i, v := range versions {
		if v == fromVersion {
			fromIdx = i
		}
		if v == toVersion {
			toIdx = i
		}
	}

	if fromIdx == -1 || toIdx == -1 {
		return nil
	}

	if fromIdx < toIdx {
		return versions[fromIdx+1 : toIdx+1]
	} else if fromIdx > toIdx {
		// Downgrade not supported
		return nil
	}

	return []string{}
}

// getLatestVersion gets the latest version string.
func (vs *VersionedSchema) getLatestVersion() string {
	if len(vs.Versions) == 0 {
		return "1.0"
	}

	versions := make([]string, 0, len(vs.Versions))
	for v := range vs.Versions {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool {
		return compareVersions(versions[i], versions[j]) < 0
	})

	return versions[len(versions)-1]
}

// ChangelogEntry represents a changelog entry for a version.
type ChangelogEntry struct {
	Version          string
	Description      string
	AddedFields      []string
	DeprecatedFields []string
	RenamedFields    [][2]string // [old, new] pairs
}

// GetChangelog gets changelog of schema evolution.
func (vs *VersionedSchema) GetChangelog() []ChangelogEntry {
	// Get sorted versions
	versions := make([]string, 0, len(vs.Versions))
	for v := range vs.Versions {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool {
		return compareVersions(versions[i], versions[j]) < 0
	})

	changelog := make([]ChangelogEntry, 0, len(versions))

	for _, version := range versions {
		schema := vs.Versions[version]

		entry := ChangelogEntry{
			Version:          version,
			Description:      schema.Description,
			AddedFields:      []string{},
			DeprecatedFields: []string{},
			RenamedFields:    [][2]string{},
		}

		for _, field := range schema.Fields {
			if field.AddedIn == version {
				entry.AddedFields = append(entry.AddedFields, field.Name)
			}
			if field.DeprecatedIn == version {
				entry.DeprecatedFields = append(entry.DeprecatedFields, field.Name)
			}
			if field.RenamedFrom != "" {
				entry.RenamedFields = append(entry.RenamedFields, [2]string{field.RenamedFrom, field.Name})
			}
		}

		changelog = append(changelog, entry)
	}

	return changelog
}

// ============================================================
// Helper Functions
// ============================================================

// compareVersions compares two version strings.
// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2.
func compareVersions(v1, v2 string) int {
	parts1 := parseVersionParts(v1)
	parts2 := parseVersionParts(v2)

	// Pad shorter version
	for len(parts1) < len(parts2) {
		parts1 = append(parts1, 0)
	}
	for len(parts2) < len(parts1) {
		parts2 = append(parts2, 0)
	}

	for i := range parts1 {
		if parts1[i] < parts2[i] {
			return -1
		}
		if parts1[i] > parts2[i] {
			return 1
		}
	}

	return 0
}

// parseVersionParts parses a version string into integer parts.
func parseVersionParts(version string) []int {
	parts := strings.Split(version, ".")
	result := make([]int, len(parts))

	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			n = 0
		}
		result[i] = n
	}

	return result
}

// copyMap creates a shallow copy of a map.
func copyMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// ParseVersionHeader extracts version from a header string like "@version 2.0".
func ParseVersionHeader(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "@version ") {
		return "", false
	}
	version := strings.TrimSpace(text[9:])
	if version == "" {
		return "", false
	}
	return version, true
}

// FormatVersionHeader creates a version header string.
func FormatVersionHeader(version string) string {
	return fmt.Sprintf("@version %s", version)
}
