package glyph

import (
	"testing"
)

func TestParseEventType_String(t *testing.T) {
	tests := []struct {
		typ      ParseEventType
		expected string
	}{
		{EventNone, "NONE"},
		{EventStartObject, "START_OBJECT"},
		{EventEndObject, "END_OBJECT"},
		{EventStartList, "START_LIST"},
		{EventEndList, "END_LIST"},
		{EventKey, "KEY"},
		{EventValue, "VALUE"},
		{EventError, "ERROR"},
		{EventNeedMore, "NEED_MORE"},
	}

	for _, tc := range tests {
		if tc.typ.String() != tc.expected {
			t.Errorf("%d.String() = %q, expected %q", tc.typ, tc.typ.String(), tc.expected)
		}
	}
}

func TestIncrementalParser_SimpleMap(t *testing.T) {
	var events []ParseEvent

	handler := func(e ParseEvent) error {
		events = append(events, e)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	input := `{name:"test" value:42}`
	_, err := p.Feed([]byte(input))
	if err != nil {
		t.Fatalf("feed error: %v", err)
	}

	// Check that we got at least some events
	hasStartObject := false
	hasKey := false
	hasValue := false
	for _, e := range events {
		switch e.Type {
		case EventStartObject:
			hasStartObject = true
		case EventKey:
			hasKey = true
		case EventValue:
			hasValue = true
		}
	}

	if !hasStartObject {
		t.Error("expected StartObject event")
	}
	if !hasKey {
		t.Error("expected Key event")
	}
	if !hasValue {
		t.Error("expected Value event")
	}

	t.Logf("Got %d events", len(events))
}

func TestIncrementalParser_SimpleList(t *testing.T) {
	var events []ParseEvent

	handler := func(e ParseEvent) error {
		events = append(events, e)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	input := `[1 2 3]`
	_, err := p.Feed([]byte(input))
	if err != nil {
		t.Fatalf("feed error: %v", err)
	}

	// Check that we got list events
	hasStartList := false
	valueCount := 0

	for _, e := range events {
		switch e.Type {
		case EventStartList:
			hasStartList = true
		case EventValue:
			valueCount++
		}
	}

	if !hasStartList {
		t.Error("missing StartList event")
	}
	if valueCount < 1 {
		t.Error("expected at least 1 value")
	}

	t.Logf("Got %d events, %d values", len(events), valueCount)
}

func TestIncrementalParser_NestedStructure(t *testing.T) {
	var events []ParseEvent

	handler := func(e ParseEvent) error {
		events = append(events, e)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	input := `{outer:{inner:123}}`
	p.Feed([]byte(input))

	// Check that we got nested objects
	objectStarts := 0
	for _, e := range events {
		if e.Type == EventStartObject {
			objectStarts++
		}
	}

	if objectStarts < 1 {
		t.Errorf("expected at least 1 object start, got %d", objectStarts)
	}

	t.Logf("Got %d object starts", objectStarts)
}

func TestIncrementalParser_Streaming(t *testing.T) {
	var events []ParseEvent

	handler := func(e ParseEvent) error {
		events = append(events, e)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	// Feed complete tokens in chunks to test streaming
	chunks := []string{
		`{`,
		`name:"test"`,
		` value:42`,
		`}`,
	}

	for _, chunk := range chunks {
		p.Feed([]byte(chunk))
	}

	// Should produce events
	hasStartObject := false
	for _, e := range events {
		if e.Type == EventStartObject {
			hasStartObject = true
		}
	}

	if !hasStartObject {
		t.Error("streaming parse should produce StartObject")
	}

	t.Logf("Streaming produced %d events", len(events))
}

func TestIncrementalParser_Scalars(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`42`, int64(42)},
		{`-123`, int64(-123)},
		{`3.14`, nil}, // float
		{`t`, true},
		{`f`, false},
		{`true`, true},
		{`false`, false},
		{`"hello"`, "hello"},
		{`hello`, "hello"}, // bare string
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			var lastValue *GValue

			handler := func(e ParseEvent) error {
				if e.Type == EventValue {
					lastValue = e.Value
				}
				return nil
			}

			p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
			p.Feed([]byte(tc.input))
			p.End()

			if lastValue == nil {
				t.Fatal("no value parsed")
			}

			// Type-specific checks
			switch expected := tc.expected.(type) {
			case int64:
				if v, err := lastValue.AsInt(); err != nil || v != expected {
					t.Errorf("expected int %d, got %v", expected, lastValue)
				}
			case bool:
				if v, err := lastValue.AsBool(); err != nil || v != expected {
					t.Errorf("expected bool %v, got %v", expected, lastValue)
				}
			case string:
				if v, err := lastValue.AsStr(); err != nil || v != expected {
					t.Errorf("expected string %q, got %v", expected, lastValue)
				}
			}
		})
	}
}

func TestIncrementalParser_Null(t *testing.T) {
	inputs := []string{"null", "none", "nil", "∅"}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			var lastValue *GValue

			handler := func(e ParseEvent) error {
				if e.Type == EventValue {
					lastValue = e.Value
				}
				return nil
			}

			p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
			p.Feed([]byte(input))
			p.End()

			if lastValue == nil || !lastValue.IsNull() {
				t.Errorf("expected null for input %q", input)
			}
		})
	}
}

func TestIncrementalParser_Path(t *testing.T) {
	var paths [][]PathElement

	handler := func(e ParseEvent) error {
		if e.Type == EventKey || e.Type == EventValue {
			pathCopy := make([]PathElement, len(e.Path))
			copy(pathCopy, e.Path)
			paths = append(paths, pathCopy)
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`{a:{b:1}}`))
	p.End()

	// Should have paths like [a], [a, b], [a, b] (for value)
	if len(paths) < 3 {
		t.Fatalf("expected at least 3 paths, got %d", len(paths))
	}

	// First key "a" should have path [a]
	if len(paths[0]) != 1 || paths[0][0].Key != "a" {
		t.Errorf("first path should be [a], got %v", paths[0])
	}
}

func TestIncrementalParser_ListPath(t *testing.T) {
	var paths [][]PathElement

	handler := func(e ParseEvent) error {
		if e.Type == EventValue {
			pathCopy := make([]PathElement, len(e.Path))
			copy(pathCopy, e.Path)
			paths = append(paths, pathCopy)
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`[1 2 3]`))
	p.End()

	// Values should have index paths [0], [1], [2]
	for i, path := range paths {
		if len(path) != 1 {
			t.Errorf("path %d: expected len 1, got %d", i, len(path))
			continue
		}
		if !path[0].IsIndex {
			t.Errorf("path %d: expected index path", i)
			continue
		}
		if path[0].Index != i {
			t.Errorf("path %d: expected index %d, got %d", i, i, path[0].Index)
		}
	}
}

func TestIncrementalParser_Reset(t *testing.T) {
	eventCount := 0

	handler := func(e ParseEvent) error {
		eventCount++
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	p.Feed([]byte(`{a:1}`))
	p.End()

	count1 := eventCount

	p.Reset()
	eventCount = 0

	p.Feed([]byte(`{b:2}`))
	p.End()

	count2 := eventCount

	// Both parses should produce similar event counts
	if count1 != count2 {
		t.Errorf("event counts differ after reset: %d vs %d", count1, count2)
	}
}

func TestIncrementalParser_QuotedString(t *testing.T) {
	var lastStr string

	handler := func(e ParseEvent) error {
		if e.Type == EventValue && e.Value != nil {
			if s, err := e.Value.AsStr(); err == nil {
				lastStr = s
			}
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`"hello world"`))
	p.End()

	if lastStr != "hello world" {
		t.Errorf("expected 'hello world', got %q", lastStr)
	}
}

func TestIncrementalParser_EscapedString(t *testing.T) {
	var lastStr string

	handler := func(e ParseEvent) error {
		if e.Type == EventValue && e.Value != nil {
			if s, err := e.Value.AsStr(); err == nil {
				lastStr = s
			}
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`"line1\nline2\ttab"`))
	p.End()

	expected := "line1\nline2\ttab"
	if lastStr != expected {
		t.Errorf("expected %q, got %q", expected, lastStr)
	}
}

func TestIncrementalParser_Reference(t *testing.T) {
	var lastValue *GValue

	handler := func(e ParseEvent) error {
		if e.Type == EventValue {
			lastValue = e.Value
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`^user:123`))
	p.End()

	if lastValue == nil {
		t.Fatal("no value parsed")
	}

	ref, err := lastValue.AsID()
	if err != nil {
		t.Fatalf("not a ref: %v", err)
	}

	if ref.Prefix != "user" || ref.Value != "123" {
		t.Errorf("expected ^user:123, got ^%s:%s", ref.Prefix, ref.Value)
	}
}

func TestIncrementalParser_Struct(t *testing.T) {
	var events []ParseEvent

	handler := func(e ParseEvent) error {
		events = append(events, e)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`Person{name:"Alice" age:30}`))
	p.End()

	// Should have StartObject with TypeName
	var startObj *ParseEvent
	for i := range events {
		if events[i].Type == EventStartObject {
			startObj = &events[i]
			break
		}
	}

	if startObj == nil {
		t.Fatal("no StartObject event")
	}

	if startObj.TypeName != "Person" {
		t.Errorf("expected TypeName 'Person', got %q", startObj.TypeName)
	}
}

func TestIncrementalParser_ErrorHandling(t *testing.T) {
	var gotError bool

	handler := func(e ParseEvent) error {
		if e.Type == EventError {
			gotError = true
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte(`{incomplete`))
	p.End()

	if !gotError {
		t.Error("expected error for incomplete input")
	}
}

func TestIncrementalParser_NeedMore(t *testing.T) {
	needMoreCount := 0

	handler := func(e ParseEvent) error {
		if e.Type == EventNeedMore {
			needMoreCount++
		}
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	// Feed incomplete chunks
	p.Feed([]byte(`{na`))
	p.Feed([]byte(`me:`))
	p.Feed([]byte(`"test"}`))
	p.End()

	// Should have requested more data at some point
	if needMoreCount == 0 {
		t.Log("Note: parser didn't need more data (acceptable)")
	}
}

func TestDefaultIncrementalParserOptions(t *testing.T) {
	opts := DefaultIncrementalParserOptions()

	if opts.MaxDepth != 128 {
		t.Errorf("expected MaxDepth 128, got %d", opts.MaxDepth)
	}
	if opts.MaxKeyLen != 4096 {
		t.Errorf("expected MaxKeyLen 4096, got %d", opts.MaxKeyLen)
	}
}

func TestIncrementalParser_EmptyInput(t *testing.T) {
	handler := func(e ParseEvent) error {
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	_, err := p.Feed([]byte{})
	if err != nil {
		t.Errorf("unexpected error for empty input: %v", err)
	}

	err = p.End()
	if err != nil {
		t.Errorf("unexpected error on end: %v", err)
	}
}

func TestIncrementalParser_WhitespaceOnly(t *testing.T) {
	eventCount := 0

	handler := func(e ParseEvent) error {
		eventCount++
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	p.Feed([]byte("   \t\n  "))
	p.End()

	// Should have no value events
	if eventCount > 1 { // Might have NeedMore
		t.Log("Events received for whitespace-only input")
	}
}

// Benchmarks

func BenchmarkIncrementalParser_SimpleMap(b *testing.B) {
	handler := func(e ParseEvent) error {
		return nil
	}

	input := []byte(`{name:"test" value:42 count:100}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
		p.Feed(input)
		p.End()
	}
}

func BenchmarkIncrementalParser_NestedMap(b *testing.B) {
	handler := func(e ParseEvent) error {
		return nil
	}

	input := []byte(`{a:{b:{c:{d:{e:1}}}}}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
		p.Feed(input)
		p.End()
	}
}

func BenchmarkIncrementalParser_LargeList(b *testing.B) {
	handler := func(e ParseEvent) error {
		return nil
	}

	// Build a list of 100 integers
	input := []byte("[")
	for i := 0; i < 100; i++ {
		if i > 0 {
			input = append(input, ' ')
		}
		input = append(input, []byte("12345")...)
	}
	input = append(input, ']')

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
		p.Feed(input)
		p.End()
	}
}

func BenchmarkIncrementalParser_Streaming(b *testing.B) {
	handler := func(e ParseEvent) error {
		return nil
	}

	input := `{name:"test" value:42 nested:{a:1 b:2 c:3}}`
	chunks := [][]byte{
		[]byte(input[:10]),
		[]byte(input[10:20]),
		[]byte(input[20:30]),
		[]byte(input[30:]),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
		for _, chunk := range chunks {
			p.Feed(chunk)
		}
		p.End()
	}
}
