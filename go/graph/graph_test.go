package graph

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go/ld"
)

func TestNodeEventRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	// Pre-populate header with labels
	sw.Header().AddLabel("Person")
	sw.Header().AddLabel("Employee")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write node event
	evt := &NodeEvent{
		Op:     OpUpsert,
		ID:     "node-1",
		Labels: []string{"Person", "Employee"},
		Props: map[string]any{
			"name": "Alice",
			"age":  int64(30),
		},
	}
	if err := sw.WriteNode(evt); err != nil {
		t.Fatalf("WriteNode failed: %v", err)
	}

	if err := sw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read back
	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	event, err := sr.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if event == nil {
		t.Fatal("Expected event, got nil")
	}

	if event.Kind != EventNode {
		t.Errorf("Expected EventNode, got %v", event.Kind)
	}
	if event.Node.ID != "node-1" {
		t.Errorf("Expected ID=node-1, got %s", event.Node.ID)
	}
	if event.Node.Op != OpUpsert {
		t.Errorf("Expected OpUpsert, got %v", event.Node.Op)
	}
	if len(event.Node.Labels) != 2 {
		t.Errorf("Expected 2 labels, got %d", len(event.Node.Labels))
	}
	if event.Node.Labels[0] != "Person" {
		t.Errorf("Expected label[0]=Person, got %s", event.Node.Labels[0])
	}
	if event.Node.Props["name"] != "Alice" {
		t.Errorf("Expected name=Alice, got %v", event.Node.Props["name"])
	}

	// Should be end of stream
	next, _ := sr.Next()
	if next != nil {
		t.Error("Expected nil at end of stream")
	}
}

func TestEdgeEventRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	sw.Header().AddLabel("KNOWS")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	evt := &EdgeEvent{
		Op:     OpUpsert,
		ID:     "edge-1",
		Label:  "KNOWS",
		FromID: "node-1",
		ToID:   "node-2",
		Props: map[string]any{
			"since": int64(2020),
		},
	}
	if err := sw.WriteEdge(evt); err != nil {
		t.Fatalf("WriteEdge failed: %v", err)
	}

	if err := sw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	event, err := sr.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if event.Kind != EventEdge {
		t.Errorf("Expected EventEdge, got %v", event.Kind)
	}
	if event.Edge.Label != "KNOWS" {
		t.Errorf("Expected label=KNOWS, got %s", event.Edge.Label)
	}
	if event.Edge.FromID != "node-1" {
		t.Errorf("Expected from=node-1, got %s", event.Edge.FromID)
	}
	if event.Edge.ToID != "node-2" {
		t.Errorf("Expected to=node-2, got %s", event.Edge.ToID)
	}
	if event.Edge.Props["since"] != int64(2020) {
		t.Errorf("Expected since=2020, got %v", event.Edge.Props["since"])
	}
}

func TestTripleEventRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	sw.Header().AddPredicate("http://schema.org/name")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	evt := &TripleEvent{
		Op:        OpUpsert,
		Subject:   NewIRITerm("http://example.org/alice"),
		Predicate: "http://schema.org/name",
		Object:    NewLangLiteral("Alice", "en"),
	}
	if err := sw.WriteTriple(evt); err != nil {
		t.Fatalf("WriteTriple failed: %v", err)
	}

	if err := sw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	event, err := sr.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if event.Kind != EventTriple {
		t.Errorf("Expected EventTriple, got %v", event.Kind)
	}
	if event.Triple.Predicate != "http://schema.org/name" {
		t.Errorf("Expected predicate, got %s", event.Triple.Predicate)
	}
	if event.Triple.Subject.Kind != TermIRI {
		t.Errorf("Expected subject IRI, got %v", event.Triple.Subject.Kind)
	}
	if event.Triple.Subject.Value != "http://example.org/alice" {
		t.Errorf("Expected subject value, got %s", event.Triple.Subject.Value)
	}
	if event.Triple.Object.Kind != TermLiteral {
		t.Errorf("Expected object literal, got %v", event.Triple.Object.Kind)
	}
	if event.Triple.Object.Value != "Alice" {
		t.Errorf("Expected object value=Alice, got %s", event.Triple.Object.Value)
	}
	if event.Triple.Object.Lang != "en" {
		t.Errorf("Expected lang=en, got %s", event.Triple.Object.Lang)
	}
}

func TestMultipleEvents(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	sw.Header().AddLabel("Person")
	sw.Header().AddLabel("KNOWS")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write multiple events
	sw.WriteNode(&NodeEvent{Op: OpUpsert, ID: "a", Labels: []string{"Person"}})
	sw.WriteNode(&NodeEvent{Op: OpUpsert, ID: "b", Labels: []string{"Person"}})
	sw.WriteEdge(&EdgeEvent{Op: OpUpsert, Label: "KNOWS", FromID: "a", ToID: "b"})
	sw.Close()

	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	events, err := sr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}

	if events[0].Kind != EventNode || events[0].Node.ID != "a" {
		t.Error("First event should be node 'a'")
	}
	if events[1].Kind != EventNode || events[1].Node.ID != "b" {
		t.Error("Second event should be node 'b'")
	}
	if events[2].Kind != EventEdge || events[2].Edge.Label != "KNOWS" {
		t.Error("Third event should be KNOWS edge")
	}
}

func TestDeleteOperation(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	sw.WriteNode(&NodeEvent{Op: OpDelete, ID: "node-to-delete"})
	sw.Close()

	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	event, _ := sr.Next()
	if event.Node.Op != OpDelete {
		t.Errorf("Expected OpDelete, got %v", event.Node.Op)
	}
}

func TestTypedLiteral(t *testing.T) {
	term := NewTypedLiteral("42", ld.XSDInteger)
	if term.Kind != TermLiteral {
		t.Errorf("Expected TermLiteral, got %v", term.Kind)
	}
	if term.Value != "42" {
		t.Errorf("Expected value=42, got %s", term.Value)
	}
	if term.Datatype != ld.XSDInteger {
		t.Errorf("Expected XSDInteger datatype, got %s", term.Datatype)
	}
}

func TestBNodeTerm(t *testing.T) {
	term := NewBNodeTerm("b1")
	if term.Kind != TermBNode {
		t.Errorf("Expected TermBNode, got %v", term.Kind)
	}
	if term.Value != "b1" {
		t.Errorf("Expected value=b1, got %s", term.Value)
	}
}

func TestStreamHeaderHelpers(t *testing.T) {
	h := NewStreamHeader()

	// Test deduplication
	idx1 := h.AddField("name")
	idx2 := h.AddField("age")
	idx3 := h.AddField("name") // duplicate

	if idx1 != 0 {
		t.Errorf("Expected idx1=0, got %d", idx1)
	}
	if idx2 != 1 {
		t.Errorf("Expected idx2=1, got %d", idx2)
	}
	if idx3 != 0 {
		t.Errorf("Expected idx3=0 (duplicate), got %d", idx3)
	}

	// Test GetField
	if h.GetField(0) != "name" {
		t.Errorf("Expected field[0]=name, got %s", h.GetField(0))
	}
	if h.GetField(100) != "" {
		t.Error("Expected empty string for out of bounds")
	}

	// Test AddLabel
	labelIdx1 := h.AddLabel("Person")
	labelIdx2 := h.AddLabel("Person") // duplicate
	if labelIdx1 != labelIdx2 {
		t.Error("Duplicate label should return same index")
	}

	// Test AddPredicate sets flag
	if h.Flags&FlagHasPredDict != 0 {
		t.Error("Flag should not be set before adding predicate")
	}
	h.AddPredicate("http://example.org/pred")
	if h.Flags&FlagHasPredDict == 0 {
		t.Error("Flag should be set after adding predicate")
	}
}

func TestEventKindString(t *testing.T) {
	tests := []struct {
		kind EventKind
		want string
	}{
		{EventNode, "node"},
		{EventEdge, "edge"},
		{EventTriple, "triple"},
		{EventKind(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("EventKind(%d).String() = %s, want %s", tt.kind, got, tt.want)
		}
	}
}

func TestOpString(t *testing.T) {
	if OpUpsert.String() != "upsert" {
		t.Errorf("Expected 'upsert', got %s", OpUpsert.String())
	}
	if OpDelete.String() != "delete" {
		t.Errorf("Expected 'delete', got %s", OpDelete.String())
	}
}

func TestTermKindString(t *testing.T) {
	tests := []struct {
		kind TermKind
		want string
	}{
		{TermIRI, "iri"},
		{TermBNode, "bnode"},
		{TermLiteral, "literal"},
		{TermKind(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("TermKind(%d).String() = %s, want %s", tt.kind, got, tt.want)
		}
	}
}

func TestEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	sw.WriteHeader()
	sw.Close()

	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	event, err := sr.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if event != nil {
		t.Error("Expected nil for empty stream")
	}
}

func TestAgentMessageScenario(t *testing.T) {
	// Simulate the agent message use case from the plan
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	sw.Header().AddLabel("Agent")
	sw.Header().AddLabel("SENT_MESSAGE")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Agent A sends message to Agent B
	sw.WriteNode(&NodeEvent{
		Op:     OpUpsert,
		ID:     "A",
		Labels: []string{"Agent"},
		Props:  map[string]any{"name": "Agent A"},
	})
	sw.WriteNode(&NodeEvent{
		Op:     OpUpsert,
		ID:     "B",
		Labels: []string{"Agent"},
		Props:  map[string]any{"name": "Agent B"},
	})
	sw.WriteEdge(&EdgeEvent{
		Op:     OpUpsert,
		Label:  "SENT_MESSAGE",
		FromID: "A",
		ToID:   "B",
		Props:  map[string]any{"task": "Process X", "priority": int64(1)},
	})
	sw.Close()

	// Read back and verify
	sr, err := NewStreamReader(buf.Bytes())
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}

	events, err := sr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(events))
	}

	// Verify edge properties
	edge := events[2].Edge
	if edge.Props["task"] != "Process X" {
		t.Errorf("Expected task='Process X', got %v", edge.Props["task"])
	}
	if edge.Props["priority"] != int64(1) {
		t.Errorf("Expected priority=1, got %v", edge.Props["priority"])
	}
}

func BenchmarkWriteNode(b *testing.B) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	sw.Header().AddLabel("Person")
	sw.WriteHeader()

	evt := &NodeEvent{
		Op:     OpUpsert,
		ID:     "node-1",
		Labels: []string{"Person"},
		Props:  map[string]any{"name": "Alice", "age": int64(30)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sw.WriteNode(evt)
	}
}

func BenchmarkReadEvents(b *testing.B) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	sw.Header().AddLabel("Person")
	sw.Header().AddLabel("KNOWS")
	sw.WriteHeader()

	for i := 0; i < 100; i++ {
		sw.WriteNode(&NodeEvent{Op: OpUpsert, ID: "node", Labels: []string{"Person"}})
	}
	sw.Close()

	data := buf.Bytes()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sr, _ := NewStreamReader(data)
		sr.ReadAll()
	}
}
