package glyph

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ============================================================
// GLYPH Streaming Validator
// ============================================================
//
// Validates GLYPH tool calls incrementally as tokens arrive from
// an LLM. This enables:
//
//   - Early tool detection: Know the tool name before full response
//   - Early rejection: Stop on unknown tools mid-stream
//   - Incremental validation: Check constraints as tokens arrive
//   - Latency savings: Reject bad payloads without waiting for completion
//
// Reference: cowrie/benchmark/comparison/js/streaming_validation_test.mjs

// ============================================================
// Tool Registry
// ============================================================

// ArgSchema defines constraints for a tool argument.
type ArgSchema struct {
	Type     string         // "string", "int", "float", "bool"
	Required bool           // Whether the argument is required
	Min      *float64       // Minimum value (for numbers)
	Max      *float64       // Maximum value (for numbers)
	MinLen   *int           // Minimum string length
	MaxLen   *int           // Maximum string length
	Pattern  *regexp.Regexp // Regex pattern for strings
	Enum     []string       // Allowed values (for strings)
}

// ToolSchema defines a tool and its arguments.
type ToolSchema struct {
	Name        string               // Tool name
	Description string               // Human-readable description
	Args        map[string]ArgSchema // Argument schemas
}

// ToolRegistry holds registered tools for validation.
type ToolRegistry struct {
	mu       sync.RWMutex
	tools    map[string]*ToolSchema
	allowSet map[string]struct{}
}

// NewToolRegistry creates an empty tool registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools:    make(map[string]*ToolSchema),
		allowSet: make(map[string]struct{}),
	}
}

// Register adds a tool to the registry.
func (r *ToolRegistry) Register(tool *ToolSchema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[tool.Name] = tool
	r.allowSet[tool.Name] = struct{}{}
}

// IsAllowed checks if a tool name is in the allow list.
func (r *ToolRegistry) IsAllowed(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.allowSet[name]
	return ok
}

// Get returns the schema for a tool, or nil if not found.
func (r *ToolRegistry) Get(name string) *ToolSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tools[name]
}

// ============================================================
// Stream Validation Errors
// ============================================================

// StreamValidationError represents a validation failure during streaming.
type StreamValidationError struct {
	Code    string // Error code (UNKNOWN_TOOL, CONSTRAINT_MIN, etc.)
	Message string // Human-readable message
	Field   string // Field that caused the error (optional)
}

func (e StreamValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Field)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Error codes
const (
	ErrCodeUnknownTool     = "UNKNOWN_TOOL"
	ErrCodeMissingRequired = "MISSING_REQUIRED"
	ErrCodeMissingTool     = "MISSING_TOOL"
	ErrCodeConstraintMin   = "CONSTRAINT_MIN"
	ErrCodeConstraintMax   = "CONSTRAINT_MAX"
	ErrCodeConstraintLen   = "CONSTRAINT_LEN"
	ErrCodeConstraintPat   = "CONSTRAINT_PATTERN"
	ErrCodeConstraintEnum  = "CONSTRAINT_ENUM"
	ErrCodeInvalidType     = "INVALID_TYPE"
	ErrCodeLimitExceeded   = "LIMIT_EXCEEDED"
)

// Default limits
const (
	DefaultMaxBuffer = 1 << 20 // 1MB
	DefaultMaxFields = 1000
	DefaultMaxErrors = 100
)

// ============================================================
// Streaming Validator
// ============================================================

// ValidatorState represents the current parsing state.
type ValidatorState int

const (
	StateWaiting  ValidatorState = iota // Waiting for opening brace
	StateInObject                       // Inside the main object
	StateComplete                       // Object fully parsed
	StateError                          // Unrecoverable error
)

// StreamingValidator validates GLYPH tool calls incrementally.
type StreamingValidator struct {
	registry *ToolRegistry

	// Parser state
	buffer     strings.Builder
	state      ValidatorState
	depth      int
	inString   bool
	escapeNext bool
	currentKey string
	currentVal strings.Builder
	hasKey     bool

	// Parsed data
	toolName     string
	parsedFields map[string]interface{}
	errors       []StreamValidationError

	// Timing instrumentation
	tokenCount          int
	charCount           int
	startTime           time.Time
	toolDetectedAtToken int
	toolDetectedAtChar  int
	toolDetectedAtTime  time.Duration
	firstErrorAtToken   int
	firstErrorAtTime    time.Duration
	completeAtToken     int
	completeAtTime      time.Duration

	// Timeline events
	timeline []TimelineEvent

	// Hard limits to prevent OOM
	maxBufferSize int
	maxFieldCount int
	maxErrorCount int
}

// TimelineEvent records a significant event during validation.
type TimelineEvent struct {
	Event   string        // TOOL_DETECTED, ERROR, COMPLETE
	Token   int           // Token number when event occurred
	Char    int           // Character position
	Elapsed time.Duration // Time since start
	Detail  string        // Additional info (tool name, error, etc.)
}

// NewStreamingValidator creates a new validator with the given registry.
func NewStreamingValidator(registry *ToolRegistry) *StreamingValidator {
	return &StreamingValidator{
		registry:      registry,
		parsedFields:  make(map[string]interface{}),
		maxBufferSize: DefaultMaxBuffer,
		maxFieldCount: DefaultMaxFields,
		maxErrorCount: DefaultMaxErrors,
	}
}

// WithLimits sets custom limits. Returns self for chaining.
func (v *StreamingValidator) WithLimits(bufSize, fieldCount, errCount int) *StreamingValidator {
	if bufSize > 0 {
		v.maxBufferSize = bufSize
	}
	if fieldCount > 0 {
		v.maxFieldCount = fieldCount
	}
	if errCount > 0 {
		v.maxErrorCount = errCount
	}
	return v
}

// Reset clears the validator state for reuse.
func (v *StreamingValidator) Reset() {
	v.buffer.Reset()
	v.state = StateWaiting
	v.depth = 0
	v.inString = false
	v.escapeNext = false
	v.currentKey = ""
	v.currentVal.Reset()
	v.hasKey = false
	v.toolName = ""
	v.parsedFields = make(map[string]interface{})
	v.errors = nil
	v.tokenCount = 0
	v.charCount = 0
	v.startTime = time.Time{}
	v.toolDetectedAtToken = 0
	v.toolDetectedAtChar = 0
	v.toolDetectedAtTime = 0
	v.firstErrorAtToken = 0
	v.firstErrorAtTime = 0
	v.completeAtToken = 0
	v.completeAtTime = 0
	v.timeline = nil
}

// Start begins timing for the validation session.
func (v *StreamingValidator) Start() {
	v.startTime = time.Now()
}

// PushToken processes a token (string fragment) from the LLM.
// Returns the current validation state.
func (v *StreamingValidator) PushToken(token string) *StreamValidationResult {
	v.tokenCount++

	for _, char := range token {
		v.charCount++
		v.processChar(char)
	}

	elapsed := time.Since(v.startTime)

	// Record tool detection
	if v.toolName != "" && v.toolDetectedAtToken == 0 {
		v.toolDetectedAtToken = v.tokenCount
		v.toolDetectedAtChar = v.charCount
		v.toolDetectedAtTime = elapsed

		allowed := true
		if v.registry != nil {
			allowed = v.registry.IsAllowed(v.toolName)
		}

		v.timeline = append(v.timeline, TimelineEvent{
			Event:   "TOOL_DETECTED",
			Token:   v.tokenCount,
			Char:    v.charCount,
			Elapsed: elapsed,
			Detail:  fmt.Sprintf("tool=%s allowed=%v", v.toolName, allowed),
		})
	}

	// Record first error
	if len(v.errors) > 0 && v.firstErrorAtToken == 0 {
		v.firstErrorAtToken = v.tokenCount
		v.firstErrorAtTime = elapsed

		v.timeline = append(v.timeline, TimelineEvent{
			Event:   "ERROR",
			Token:   v.tokenCount,
			Char:    v.charCount,
			Elapsed: elapsed,
			Detail:  v.errors[0].Message,
		})
	}

	// Record completion
	if v.state == StateComplete && v.completeAtToken == 0 {
		v.completeAtToken = v.tokenCount
		v.completeAtTime = elapsed

		v.timeline = append(v.timeline, TimelineEvent{
			Event:   "COMPLETE",
			Token:   v.tokenCount,
			Char:    v.charCount,
			Elapsed: elapsed,
			Detail:  fmt.Sprintf("valid=%v", len(v.errors) == 0),
		})
	}

	return v.GetResult()
}

func (v *StreamingValidator) processChar(char rune) {
	// Check hard limits before processing
	if v.buffer.Len() >= v.maxBufferSize {
		v.state = StateError
		return
	}
	if len(v.parsedFields) >= v.maxFieldCount {
		v.state = StateError
		return
	}

	v.buffer.WriteRune(char)

	// Handle escape sequences
	if v.escapeNext {
		v.escapeNext = false
		v.currentVal.WriteRune(char)
		return
	}

	if char == '\\' && v.inString {
		v.escapeNext = true
		v.currentVal.WriteRune(char)
		return
	}

	// Handle quotes
	if char == '"' {
		if v.inString {
			v.inString = false
		} else {
			v.inString = true
			v.currentVal.Reset()
		}
		return
	}

	// Inside string - accumulate
	if v.inString {
		v.currentVal.WriteRune(char)
		return
	}

	// Handle structural characters
	switch char {
	case '{':
		if v.state == StateWaiting {
			v.state = StateInObject
		}
		v.depth++

	case '}':
		v.depth--
		if v.depth == 0 {
			v.finishField()
			v.state = StateComplete
			v.validateComplete()
		}

	case '[':
		v.depth++
		v.currentVal.WriteRune(char)

	case ']':
		v.depth--
		v.currentVal.WriteRune(char)

	case '=':
		if v.depth == 1 && !v.hasKey {
			v.currentKey = strings.TrimSpace(v.currentVal.String())
			v.currentVal.Reset()
			v.hasKey = true
		} else {
			v.currentVal.WriteRune(char)
		}

	case ' ', '\n', '\t', '\r':
		if v.depth == 1 && v.hasKey && v.currentVal.Len() > 0 {
			v.finishField()
		}

	default:
		v.currentVal.WriteRune(char)
	}
}

func (v *StreamingValidator) finishField() {
	if !v.hasKey {
		return
	}

	key := v.currentKey
	valStr := strings.TrimSpace(v.currentVal.String())

	// Parse value
	value := v.parseValue(valStr)
	v.parsedFields[key] = value

	// Check for tool/action field
	if key == "action" || key == "tool" {
		if strVal, ok := value.(string); ok {
			v.toolName = strVal

			// Validate against allow list
			if v.registry != nil && !v.registry.IsAllowed(v.toolName) {
				v.errors = append(v.errors, StreamValidationError{
					Code:    ErrCodeUnknownTool,
					Message: fmt.Sprintf("Unknown tool: %s", v.toolName),
					Field:   key,
				})
			}
		}
	}

	// Validate field constraints
	if v.toolName != "" && v.registry != nil {
		v.validateField(key, value)
	}

	// Reset for next field
	v.currentKey = ""
	v.currentVal.Reset()
	v.hasKey = false
}

func (v *StreamingValidator) parseValue(s string) interface{} {
	// Boolean
	if s == "t" || s == "true" {
		return true
	}
	if s == "f" || s == "false" {
		return false
	}

	// Null
	if s == "_" || s == "null" || s == "" {
		return nil
	}

	// Integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}

	// Float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	// String (remove quotes if present from accumulated value)
	return s
}

func (v *StreamingValidator) validateField(key string, value interface{}) {
	if key == "action" || key == "tool" {
		return
	}

	schema := v.registry.Get(v.toolName)
	if schema == nil {
		return
	}

	argSchema, exists := schema.Args[key]
	if !exists {
		return
	}

	// Numeric constraints
	if numVal, ok := toFloat64(value); ok {
		if argSchema.Min != nil && numVal < *argSchema.Min {
			v.errors = append(v.errors, StreamValidationError{
				Code:    ErrCodeConstraintMin,
				Message: fmt.Sprintf("%s < %.0f", key, *argSchema.Min),
				Field:   key,
			})
		}
		if argSchema.Max != nil && numVal > *argSchema.Max {
			v.errors = append(v.errors, StreamValidationError{
				Code:    ErrCodeConstraintMax,
				Message: fmt.Sprintf("%s > %.0f", key, *argSchema.Max),
				Field:   key,
			})
		}
	}

	// String constraints
	if strVal, ok := value.(string); ok {
		if argSchema.MinLen != nil && len(strVal) < *argSchema.MinLen {
			v.errors = append(v.errors, StreamValidationError{
				Code:    ErrCodeConstraintLen,
				Message: fmt.Sprintf("%s length < %d", key, *argSchema.MinLen),
				Field:   key,
			})
		}
		if argSchema.MaxLen != nil && len(strVal) > *argSchema.MaxLen {
			v.errors = append(v.errors, StreamValidationError{
				Code:    ErrCodeConstraintLen,
				Message: fmt.Sprintf("%s length > %d", key, *argSchema.MaxLen),
				Field:   key,
			})
		}
		if argSchema.Pattern != nil && !argSchema.Pattern.MatchString(strVal) {
			v.errors = append(v.errors, StreamValidationError{
				Code:    ErrCodeConstraintPat,
				Message: fmt.Sprintf("%s pattern mismatch", key),
				Field:   key,
			})
		}
		if len(argSchema.Enum) > 0 {
			found := false
			for _, allowed := range argSchema.Enum {
				if strVal == allowed {
					found = true
					break
				}
			}
			if !found {
				v.errors = append(v.errors, StreamValidationError{
					Code:    ErrCodeConstraintEnum,
					Message: fmt.Sprintf("%s not in allowed values", key),
					Field:   key,
				})
			}
		}
	}
}

func (v *StreamingValidator) validateComplete() {
	if v.toolName == "" {
		v.errors = append(v.errors, StreamValidationError{
			Code:    ErrCodeMissingTool,
			Message: "No action field found",
		})
		return
	}

	if v.registry == nil {
		return
	}

	schema := v.registry.Get(v.toolName)
	if schema == nil {
		return
	}

	// Check required fields
	for argName, argSchema := range schema.Args {
		if argSchema.Required {
			if _, exists := v.parsedFields[argName]; !exists {
				v.errors = append(v.errors, StreamValidationError{
					Code:    ErrCodeMissingRequired,
					Message: fmt.Sprintf("Missing required field: %s", argName),
					Field:   argName,
				})
			}
		}
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

// ============================================================
// Stream Validation Result
// ============================================================

// StreamValidationResult represents the current state of streaming validation.
type StreamValidationResult struct {
	Complete    bool                    // True when object is fully parsed
	Valid       bool                    // True when no errors
	ToolName    string                  // Detected tool name (may be empty)
	ToolAllowed *bool                   // Whether tool is in allow list (nil if unknown)
	Errors      []StreamValidationError // Accumulated errors
	Fields      map[string]interface{}  // Parsed fields so far
	TokenCount  int                     // Tokens processed
	CharCount   int                     // Characters processed
	Timeline    []TimelineEvent         // Significant events

	// Timing metrics
	ToolDetectedAtToken int
	ToolDetectedAtTime  time.Duration
	FirstErrorAtToken   int
	FirstErrorAtTime    time.Duration
	CompleteAtToken     int
	CompleteAtTime      time.Duration
}

// GetResult returns the current validation result.
func (v *StreamingValidator) GetResult() *StreamValidationResult {
	result := &StreamValidationResult{
		Complete:            v.state == StateComplete,
		Valid:               len(v.errors) == 0,
		ToolName:            v.toolName,
		Errors:              append([]StreamValidationError{}, v.errors...),
		Fields:              make(map[string]interface{}, len(v.parsedFields)),
		TokenCount:          v.tokenCount,
		CharCount:           v.charCount,
		Timeline:            append([]TimelineEvent{}, v.timeline...),
		ToolDetectedAtToken: v.toolDetectedAtToken,
		ToolDetectedAtTime:  v.toolDetectedAtTime,
		FirstErrorAtToken:   v.firstErrorAtToken,
		FirstErrorAtTime:    v.firstErrorAtTime,
		CompleteAtToken:     v.completeAtToken,
		CompleteAtTime:      v.completeAtTime,
	}

	// Copy fields
	for k, val := range v.parsedFields {
		result.Fields[k] = val
	}

	// Set ToolAllowed
	if v.toolName != "" && v.registry != nil {
		allowed := v.registry.IsAllowed(v.toolName)
		result.ToolAllowed = &allowed
	}

	return result
}

// IsToolAllowed returns whether the detected tool is in the allow list.
// Returns false if no tool detected or no registry configured.
func (v *StreamingValidator) IsToolAllowed() bool {
	if v.toolName == "" || v.registry == nil {
		return false
	}
	return v.registry.IsAllowed(v.toolName)
}

// ShouldStop returns true if validation has detected a fatal error
// that warrants stopping the LLM stream early.
func (v *StreamingValidator) ShouldStop() bool {
	for _, err := range v.errors {
		if err.Code == ErrCodeUnknownTool {
			return true
		}
	}
	return false
}

// ============================================================
// Helper Functions
// ============================================================

// MinFloat64 returns a pointer to a float64 for use in ArgSchema.Min/Max.
func MinFloat64(v float64) *float64 { return &v }

// MaxFloat64 returns a pointer to a float64 for use in ArgSchema.Min/Max.
func MaxFloat64(v float64) *float64 { return &v }

// MinInt returns a pointer to an int for use in ArgSchema.MinLen/MaxLen.
func MinInt(v int) *int { return &v }

// MaxInt returns a pointer to an int for use in ArgSchema.MinLen/MaxLen.
func MaxInt(v int) *int { return &v }

// ============================================================
// Default Tool Registry
// ============================================================

// DefaultToolRegistry returns a registry with common tools for agent use.
func DefaultToolRegistry() *ToolRegistry {
	r := NewToolRegistry()

	// Search tool
	r.Register(&ToolSchema{
		Name:        "search",
		Description: "Search for information",
		Args: map[string]ArgSchema{
			"query":       {Type: "string", Required: true, MinLen: MinInt(1)},
			"max_results": {Type: "int", Required: false, Min: MinFloat64(1), Max: MaxFloat64(100)},
		},
	})

	// Calculate tool
	r.Register(&ToolSchema{
		Name:        "calculate",
		Description: "Evaluate a mathematical expression",
		Args: map[string]ArgSchema{
			"expression": {Type: "string", Required: true},
			"precision":  {Type: "int", Required: false, Min: MinFloat64(0), Max: MaxFloat64(15)},
		},
	})

	// Browse tool
	r.Register(&ToolSchema{
		Name:        "browse",
		Description: "Fetch a web page",
		Args: map[string]ArgSchema{
			"url": {Type: "string", Required: true, Pattern: regexp.MustCompile(`^https?://`)},
		},
	})

	// Execute tool
	r.Register(&ToolSchema{
		Name:        "execute",
		Description: "Execute a shell command",
		Args: map[string]ArgSchema{
			"command": {Type: "string", Required: true},
		},
	})

	// Read file tool
	r.Register(&ToolSchema{
		Name:        "read_file",
		Description: "Read a file from disk",
		Args: map[string]ArgSchema{
			"path":  {Type: "string", Required: true},
			"limit": {Type: "int", Required: false, Min: MinFloat64(1)},
		},
	})

	// Write file tool
	r.Register(&ToolSchema{
		Name:        "write_file",
		Description: "Write content to a file",
		Args: map[string]ArgSchema{
			"path":    {Type: "string", Required: true},
			"content": {Type: "string", Required: true},
		},
	})

	return r
}
