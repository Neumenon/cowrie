package glyph

import (
	"regexp"
	"testing"
)

func TestStreamingValidatorBasic(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&ToolSchema{
		Name:        "search",
		Description: "Search for information",
		Args: map[string]ArgSchema{
			"query":       {Type: "string", Required: true, MinLen: MinInt(1)},
			"max_results": {Type: "int", Required: false, Min: MinFloat64(1), Max: MaxFloat64(100)},
		},
	})

	v := NewStreamingValidator(registry)
	v.Start()

	// Simulate streaming tokens
	tokens := []string{
		`{`,
		`action=`,
		`"search"`,
		` `,
		`query=`,
		`"AI news"`,
		` `,
		`max_results=`,
		`10`,
		`}`,
	}

	var result *StreamValidationResult
	for _, tok := range tokens {
		result = v.PushToken(tok)
	}

	if !result.Complete {
		t.Errorf("Expected complete, got incomplete")
	}
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}
	if result.ToolName != "search" {
		t.Errorf("Expected tool 'search', got '%s'", result.ToolName)
	}
	if result.ToolDetectedAtToken == 0 {
		t.Errorf("Expected tool detection token > 0")
	}
}

func TestStreamingValidatorEarlyRejection(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&ToolSchema{
		Name: "search",
		Args: map[string]ArgSchema{
			"query": {Type: "string", Required: true},
		},
	})

	v := NewStreamingValidator(registry)
	v.Start()

	// Simulate streaming tokens - tool name becomes known after the space
	// This mimics real LLM streaming where tokens arrive incrementally
	tokens := []string{
		`{action="unknown_malicious_tool" `, // Space triggers field completion
	}

	var result *StreamValidationResult
	for _, tok := range tokens {
		result = v.PushToken(tok)
	}

	// Tool should be detected and rejected after the field is completed
	if result.ToolName != "unknown_malicious_tool" {
		t.Errorf("Expected tool 'unknown_malicious_tool', got '%s'", result.ToolName)
	}
	if result.ToolAllowed == nil || *result.ToolAllowed {
		t.Errorf("Expected tool to be NOT allowed")
	}
	if !v.ShouldStop() {
		t.Errorf("Expected ShouldStop() to return true")
	}

	// Verify we can stop before more tokens arrive
	// (simulating early stream cancellation)
	if len(result.Errors) == 0 {
		t.Errorf("Expected errors for unknown tool")
	}
}

func TestStreamingValidatorConstraintViolation(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&ToolSchema{
		Name: "search",
		Args: map[string]ArgSchema{
			"query":       {Type: "string", Required: true},
			"max_results": {Type: "int", Required: false, Min: MinFloat64(1), Max: MaxFloat64(100)},
		},
	})

	v := NewStreamingValidator(registry)
	v.Start()

	// Simulate constraint violation: max_results=500 exceeds max of 100
	tokens := []string{
		`{action="search" query="test" max_results=500}`,
	}

	var result *StreamValidationResult
	for _, tok := range tokens {
		result = v.PushToken(tok)
	}

	if result.Complete && result.Valid {
		t.Errorf("Expected validation error for max_results=500")
	}

	foundConstraintError := false
	for _, err := range result.Errors {
		if err.Code == ErrCodeConstraintMax {
			foundConstraintError = true
			break
		}
	}
	if !foundConstraintError {
		t.Errorf("Expected CONSTRAINT_MAX error, got: %v", result.Errors)
	}
}

func TestStreamingValidatorPatternViolation(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&ToolSchema{
		Name: "browse",
		Args: map[string]ArgSchema{
			"url": {Type: "string", Required: true, Pattern: regexp.MustCompile(`^https?://`)},
		},
	})

	v := NewStreamingValidator(registry)
	v.Start()

	// Simulate pattern violation: file:// URL not allowed
	tokens := []string{
		`{action="browse" url="file:///etc/passwd"}`,
	}

	var result *StreamValidationResult
	for _, tok := range tokens {
		result = v.PushToken(tok)
	}

	if result.Complete && result.Valid {
		t.Errorf("Expected validation error for file:// URL")
	}

	foundPatternError := false
	for _, err := range result.Errors {
		if err.Code == ErrCodeConstraintPat {
			foundPatternError = true
			break
		}
	}
	if !foundPatternError {
		t.Errorf("Expected CONSTRAINT_PATTERN error, got: %v", result.Errors)
	}
}

func TestStreamingValidatorMissingRequired(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&ToolSchema{
		Name: "search",
		Args: map[string]ArgSchema{
			"query": {Type: "string", Required: true},
		},
	})

	v := NewStreamingValidator(registry)
	v.Start()

	// Missing required 'query' field
	result := v.PushToken(`{action="search"}`)

	if result.Valid {
		t.Errorf("Expected validation error for missing required field")
	}

	foundMissingError := false
	for _, err := range result.Errors {
		if err.Code == ErrCodeMissingRequired {
			foundMissingError = true
			break
		}
	}
	if !foundMissingError {
		t.Errorf("Expected MISSING_REQUIRED error, got: %v", result.Errors)
	}
}

func TestStreamingValidatorTimeline(t *testing.T) {
	registry := DefaultToolRegistry()

	v := NewStreamingValidator(registry)
	v.Start()

	// Stream tokens
	tokens := []string{
		`{`,
		`action="search"`,
		` query="test"`,
		`}`,
	}

	var result *StreamValidationResult
	for _, tok := range tokens {
		result = v.PushToken(tok)
	}

	// Check timeline events
	if len(result.Timeline) == 0 {
		t.Errorf("Expected timeline events")
	}

	foundToolDetected := false
	foundComplete := false
	for _, event := range result.Timeline {
		switch event.Event {
		case "TOOL_DETECTED":
			foundToolDetected = true
		case "COMPLETE":
			foundComplete = true
		}
	}

	if !foundToolDetected {
		t.Errorf("Expected TOOL_DETECTED event in timeline")
	}
	if !foundComplete {
		t.Errorf("Expected COMPLETE event in timeline")
	}
}

func TestStreamingValidatorReset(t *testing.T) {
	registry := DefaultToolRegistry()
	v := NewStreamingValidator(registry)

	// First validation
	v.Start()
	v.PushToken(`{action="search" query="test"}`)

	// Reset and validate again
	v.Reset()
	v.Start()
	result := v.PushToken(`{action="calculate" expression="1+1"}`)

	if result.ToolName != "calculate" {
		t.Errorf("Expected tool 'calculate' after reset, got '%s'", result.ToolName)
	}
}

func TestDefaultToolRegistry(t *testing.T) {
	registry := DefaultToolRegistry()

	expectedTools := []string{"search", "calculate", "browse", "execute", "read_file", "write_file"}

	for _, tool := range expectedTools {
		if !registry.IsAllowed(tool) {
			t.Errorf("Expected tool '%s' to be allowed", tool)
		}
		if registry.Get(tool) == nil {
			t.Errorf("Expected tool schema for '%s'", tool)
		}
	}

	// Check that unknown tools are not allowed
	if registry.IsAllowed("unknown_tool") {
		t.Errorf("Expected 'unknown_tool' to not be allowed")
	}
}

func BenchmarkStreamingValidator(b *testing.B) {
	registry := DefaultToolRegistry()

	payload := `{action="search" query="artificial intelligence news" max_results=20}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := NewStreamingValidator(registry)
		v.Start()
		v.PushToken(payload)
	}
}

func BenchmarkStreamingValidatorStreamed(b *testing.B) {
	registry := DefaultToolRegistry()

	tokens := []string{
		`{`, `action=`, `"search"`, ` `, `query=`, `"artificial intelligence news"`, ` `, `max_results=`, `20`, `}`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := NewStreamingValidator(registry)
		v.Start()
		for _, tok := range tokens {
			v.PushToken(tok)
		}
	}
}
