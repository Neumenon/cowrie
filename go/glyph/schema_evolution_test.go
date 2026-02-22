package glyph

import (
	"reflect"
	"testing"
)

func TestEvolvingFieldAvailability(t *testing.T) {
	field := &EvolvingField{
		Name:    "venue",
		Type:    FieldTypeStr,
		AddedIn: "2.0",
	}

	// Field not available before 2.0
	if field.IsAvailableIn("1.0") {
		t.Error("Field should not be available in 1.0")
	}

	// Field available in 2.0
	if !field.IsAvailableIn("2.0") {
		t.Error("Field should be available in 2.0")
	}

	// Field available in 2.1
	if !field.IsAvailableIn("2.1") {
		t.Error("Field should be available in 2.1")
	}
}

func TestEvolvingFieldDeprecation(t *testing.T) {
	field := &EvolvingField{
		Name:         "referee",
		Type:         FieldTypeStr,
		AddedIn:      "1.0",
		DeprecatedIn: "3.0",
	}

	// Field available in 2.0
	if !field.IsAvailableIn("2.0") {
		t.Error("Field should be available in 2.0")
	}

	// Field deprecated in 3.0
	if field.IsAvailableIn("3.0") {
		t.Error("Field should not be available in 3.0 (deprecated)")
	}

	// Check deprecated flag
	if !field.IsDeprecatedIn("3.0") {
		t.Error("Field should be deprecated in 3.0")
	}
	if field.IsDeprecatedIn("2.0") {
		t.Error("Field should not be deprecated in 2.0")
	}
}

func TestEvolvingFieldValidation(t *testing.T) {
	field := &EvolvingField{
		Name:     "email",
		Type:     FieldTypeStr,
		Required: true,
	}

	// Required field missing
	if err := field.ValidateValue(nil); err == "" {
		t.Error("Expected error for nil value on required field")
	}

	// Valid value
	if err := field.ValidateValue("test@example.com"); err != "" {
		t.Errorf("Unexpected error: %s", err)
	}

	// Wrong type
	if err := field.ValidateValue(123); err == "" {
		t.Error("Expected error for wrong type")
	}
}

func TestVersionedSchemaBasic(t *testing.T) {
	schema := NewVersionedSchema("Match")

	err := schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})
	if err != nil {
		t.Fatalf("AddVersion error: %v", err)
	}

	v := schema.GetVersion("1.0")
	if v == nil {
		t.Fatal("Expected version 1.0 to exist")
	}
	if v.GetField("home") == nil {
		t.Error("Expected 'home' field to exist")
	}
	if v.GetField("away") == nil {
		t.Error("Expected 'away' field to exist")
	}
}

func TestVersionedSchemaMultipleVersions(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	if schema.LatestVersion != "2.0" {
		t.Errorf("Expected latest version 2.0, got %s", schema.LatestVersion)
	}

	v2 := schema.GetVersion("2.0")
	if v2 == nil {
		t.Fatal("Expected version 2.0 to exist")
	}
	if v2.GetField("venue") == nil {
		t.Error("Expected 'venue' field in v2.0")
	}
}

func TestVersionedSchemaAddOptionalField(t *testing.T) {
	schema := NewVersionedSchema("Match")
	schema.Mode = ModeTolerant

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	// Parse v1 data - should auto-migrate to v2
	data := map[string]interface{}{
		"home": "Arsenal",
		"away": "Liverpool",
	}

	result := schema.Parse(data, "1.0")
	if result.Error != "" {
		t.Fatalf("Parse error: %s", result.Error)
	}

	// Check that venue was added with nil default
	if _, exists := result.Data["venue"]; !exists {
		t.Error("Expected 'venue' field to be added during migration")
	}
}

func TestVersionedSchemaFieldWithDefault(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":   {Type: FieldTypeStr, Required: true},
		"away":   {Type: FieldTypeStr, Required: true},
		"status": {Type: FieldTypeStr, Required: false, Default: "scheduled", AddedIn: "2.0"},
	})

	// Parse v1 data
	data := map[string]interface{}{
		"home": "Arsenal",
		"away": "Liverpool",
	}

	result := schema.Parse(data, "1.0")
	if result.Error != "" {
		t.Fatalf("Parse error: %s", result.Error)
	}

	// Check that status was added with default
	if result.Data["status"] != "scheduled" {
		t.Errorf("Expected status='scheduled', got %v", result.Data["status"])
	}
}

func TestVersionedSchemaFieldRenaming(t *testing.T) {
	schema := NewVersionedSchema("User")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"email": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"contact_email": {Type: FieldTypeStr, Required: true, RenamedFrom: "email", AddedIn: "2.0"},
	})

	// Parse v1 data with old field name
	data := map[string]interface{}{
		"email": "user@example.com",
	}

	result := schema.Parse(data, "1.0")
	if result.Error != "" {
		t.Fatalf("Parse error: %s", result.Error)
	}

	// Check that email was renamed to contact_email
	if result.Data["contact_email"] != "user@example.com" {
		t.Errorf("Expected contact_email='user@example.com', got %v", result.Data["contact_email"])
	}

	// Old field name should not exist
	if _, exists := result.Data["email"]; exists {
		t.Error("Old field name 'email' should not exist after rename")
	}
}

func TestVersionedSchemaStrictMode(t *testing.T) {
	schema := NewVersionedSchema("Match")
	schema.Mode = ModeStrict

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	// Parse with missing required field
	data := map[string]interface{}{
		"home": "Arsenal",
		// "away" missing
	}

	result := schema.Parse(data, "1.0")
	if result.Error == "" {
		t.Error("Expected error in strict mode for missing required field")
	}
}

func TestVersionedSchemaTolerantMode(t *testing.T) {
	schema := NewVersionedSchema("Match")
	schema.Mode = ModeTolerant

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	// Parse with extra unknown field
	data := map[string]interface{}{
		"home":    "Arsenal",
		"away":    "Liverpool",
		"unknown": "should be ignored",
	}

	result := schema.Parse(data, "1.0")
	if result.Error != "" {
		t.Fatalf("Unexpected error: %s", result.Error)
	}

	// Unknown field should be filtered
	if _, exists := result.Data["unknown"]; exists {
		t.Error("Unknown field should be filtered in tolerant mode")
	}
}

func TestVersionedSchemaEmit(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	data := map[string]interface{}{
		"home":  "Arsenal",
		"away":  "Liverpool",
		"venue": "Emirates",
	}

	result := schema.Emit(data, "2.0")
	if result.Error != "" {
		t.Fatalf("Emit error: %s", result.Error)
	}

	if result.Header != "@version 2.0" {
		t.Errorf("Expected '@version 2.0', got '%s'", result.Header)
	}
}

func TestVersionedSchemaEmitLatest(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	data := map[string]interface{}{
		"home":  "Arsenal",
		"away":  "Liverpool",
		"venue": "Emirates",
	}

	// Emit with empty version defaults to latest
	result := schema.Emit(data, "")
	if result.Error != "" {
		t.Fatalf("Emit error: %s", result.Error)
	}

	if result.Header != "@version 2.0" {
		t.Errorf("Expected '@version 2.0', got '%s'", result.Header)
	}
}

func TestVersionedSchemaChangelog(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})

	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	changelog := schema.GetChangelog()

	if len(changelog) != 2 {
		t.Fatalf("Expected 2 changelog entries, got %d", len(changelog))
	}

	// v1.0
	if changelog[0].Version != "1.0" {
		t.Errorf("Expected first entry version '1.0', got '%s'", changelog[0].Version)
	}

	// v2.0 should show venue as added
	if changelog[1].Version != "2.0" {
		t.Errorf("Expected second entry version '2.0', got '%s'", changelog[1].Version)
	}

	venueFound := false
	for _, f := range changelog[1].AddedFields {
		if f == "venue" {
			venueFound = true
			break
		}
	}
	if !venueFound {
		t.Error("Expected 'venue' in added fields for v2.0")
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		v1, v2   string
		expected int
	}{
		{"1.0", "1.0", 0},
		{"1.0", "2.0", -1},
		{"2.0", "1.0", 1},
		{"1.0", "1.1", -1},
		{"1.1", "1.0", 1},
		{"1.0.0", "1.0", 0},
		{"1.2.3", "1.2.4", -1},
	}

	for _, tt := range tests {
		result := compareVersions(tt.v1, tt.v2)
		if result != tt.expected {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tt.v1, tt.v2, result, tt.expected)
		}
	}
}

func TestParseVersionHeader(t *testing.T) {
	tests := []struct {
		input   string
		version string
		ok      bool
	}{
		{"@version 1.0", "1.0", true},
		{"@version 2.0", "2.0", true},
		{"  @version 1.5  ", "1.5", true},
		{"version 1.0", "", false},
		{"@version", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		version, ok := ParseVersionHeader(tt.input)
		if ok != tt.ok || version != tt.version {
			t.Errorf("ParseVersionHeader(%q) = (%q, %v), want (%q, %v)",
				tt.input, version, ok, tt.version, tt.ok)
		}
	}
}

func TestFormatVersionHeader(t *testing.T) {
	header := FormatVersionHeader("2.0")
	if header != "@version 2.0" {
		t.Errorf("FormatVersionHeader(\"2.0\") = %q, want \"@version 2.0\"", header)
	}
}

func TestEvolutionModeString(t *testing.T) {
	tests := []struct {
		mode     EvolutionMode
		expected string
	}{
		{ModeStrict, "strict"},
		{ModeTolerant, "tolerant"},
		{ModeMigrate, "migrate"},
	}

	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.expected {
			t.Errorf("EvolutionMode.String() = %q, want %q", got, tt.expected)
		}
	}
}

func TestVersionSchemaValidation(t *testing.T) {
	vs := NewVersionSchema("Match", "1.0")
	vs.AddField(&EvolvingField{
		Name:     "home",
		Type:     FieldTypeStr,
		Required: true,
		AddedIn:  "1.0",
	})
	vs.AddField(&EvolvingField{
		Name:     "score",
		Type:     FieldTypeInt,
		Required: false,
		AddedIn:  "1.0",
	})

	// Valid data
	data := map[string]interface{}{
		"home":  "Arsenal",
		"score": 3,
	}
	if err := vs.Validate(data); err != "" {
		t.Errorf("Unexpected validation error: %s", err)
	}

	// Missing required field
	data = map[string]interface{}{
		"score": 3,
	}
	if err := vs.Validate(data); err == "" {
		t.Error("Expected validation error for missing required field")
	}

	// Wrong type
	data = map[string]interface{}{
		"home":  "Arsenal",
		"score": "not a number",
	}
	if err := vs.Validate(data); err == "" {
		t.Error("Expected validation error for wrong type")
	}
}

func TestMigrationPath(t *testing.T) {
	schema := NewVersionedSchema("Match")

	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
	})
	schema.AddVersion("1.1", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"extra": {Type: FieldTypeStr, Required: false},
	})
	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"extra": {Type: FieldTypeStr, Required: false},
		"venue": {Type: FieldTypeStr, Required: false},
	})

	// Test migration from 1.0 to 2.0 (should go through 1.1)
	path := schema.getMigrationPath("1.0", "2.0")
	expected := []string{"1.1", "2.0"}
	if !reflect.DeepEqual(path, expected) {
		t.Errorf("getMigrationPath(1.0, 2.0) = %v, want %v", path, expected)
	}

	// Test no migration needed
	path = schema.getMigrationPath("2.0", "2.0")
	if len(path) != 0 {
		t.Errorf("getMigrationPath(2.0, 2.0) = %v, want empty", path)
	}

	// Test downgrade not supported
	path = schema.getMigrationPath("2.0", "1.0")
	if path != nil {
		t.Errorf("getMigrationPath(2.0, 1.0) = %v, want nil (downgrade not supported)", path)
	}
}

func TestFieldTypeValidation(t *testing.T) {
	tests := []struct {
		name      string
		fieldType EvolvingFieldType
		value     interface{}
		wantErr   bool
	}{
		{"str valid", FieldTypeStr, "hello", false},
		{"str invalid", FieldTypeStr, 123, true},
		{"int valid", FieldTypeInt, 42, false},
		{"int64 valid", FieldTypeInt, int64(42), false},
		{"int invalid", FieldTypeInt, "42", true},
		{"float valid", FieldTypeFloat, 3.14, false},
		{"float int valid", FieldTypeFloat, 42, false},
		{"float invalid", FieldTypeFloat, "3.14", true},
		{"bool valid", FieldTypeBool, true, false},
		{"bool invalid", FieldTypeBool, "true", true},
		{"list valid", FieldTypeList, []interface{}{1, 2, 3}, false},
		{"list strings valid", FieldTypeList, []string{"a", "b"}, false},
		{"list invalid", FieldTypeList, "not a list", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := &EvolvingField{
				Name:    "test",
				Type:    tt.fieldType,
				AddedIn: "1.0",
			}
			err := field.ValidateValue(tt.value)
			if (err != "") != tt.wantErr {
				t.Errorf("ValidateValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Benchmarks

func BenchmarkVersionedSchemaParse(b *testing.B) {
	schema := NewVersionedSchema("Match")
	schema.AddVersion("1.0", map[string]FieldConfig{
		"home": {Type: FieldTypeStr, Required: true},
		"away": {Type: FieldTypeStr, Required: true},
	})
	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false, AddedIn: "2.0"},
	})

	data := map[string]interface{}{
		"home": "Arsenal",
		"away": "Liverpool",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema.Parse(data, "1.0")
	}
}

func BenchmarkVersionedSchemaEmit(b *testing.B) {
	schema := NewVersionedSchema("Match")
	schema.AddVersion("2.0", map[string]FieldConfig{
		"home":  {Type: FieldTypeStr, Required: true},
		"away":  {Type: FieldTypeStr, Required: true},
		"venue": {Type: FieldTypeStr, Required: false},
	})

	data := map[string]interface{}{
		"home":  "Arsenal",
		"away":  "Liverpool",
		"venue": "Emirates",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema.Emit(data, "2.0")
	}
}

func BenchmarkCompareVersions(b *testing.B) {
	v1 := "1.2.3"
	v2 := "1.2.4"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compareVersions(v1, v2)
	}
}
