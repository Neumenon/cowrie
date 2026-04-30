package ld

import (
	"strings"
	"testing"
)

func TestToolIRI(t *testing.T) {
	iri := ToolIRI("calculator")
	expected := IRI("https://agentscope.io/tool/calculator")

	if iri != expected {
		t.Errorf("got %s, want %s", iri, expected)
	}
}

func TestAgentIRI(t *testing.T) {
	iri := AgentIRI("math_agent")
	expected := IRI("https://agentscope.io/agent/math_agent")

	if iri != expected {
		t.Errorf("got %s, want %s", iri, expected)
	}
}

func TestCapabilityIRI(t *testing.T) {
	iri := CapabilityIRI("math.add")
	expected := IRI("https://agentscope.io/capability/math.add")

	if iri != expected {
		t.Errorf("got %s, want %s", iri, expected)
	}
}

func TestServiceIRI(t *testing.T) {
	iri := ServiceIRI("vectordb")
	expected := IRI("https://agentscope.io/service/vectordb")

	if iri != expected {
		t.Errorf("got %s, want %s", iri, expected)
	}
}

func TestNodeIRI(t *testing.T) {
	iri := NodeIRI("node-1")
	expected := IRI("https://agentscope.io/node/node-1")

	if iri != expected {
		t.Errorf("got %s, want %s", iri, expected)
	}
}

func TestCapabilityCategories(t *testing.T) {
	// Verify all standard categories exist
	categories := []string{"math", "text", "code", "web", "file", "database"}

	for _, cat := range categories {
		iri, ok := CapabilityCategories[cat]
		if !ok {
			t.Errorf("missing capability category: %s", cat)
		}
		if !strings.HasPrefix(string(iri), string(ASCapabilityNS)) {
			t.Errorf("category %s has wrong namespace: %s", cat, iri)
		}
	}
}

func TestStandardCapabilities(t *testing.T) {
	// Verify some standard capabilities exist
	capabilities := []string{"math.add", "text.summarize", "code.execute", "web.fetch"}

	for _, cap := range capabilities {
		iri, ok := StandardCapabilities[cap]
		if !ok {
			t.Errorf("missing standard capability: %s", cap)
		}
		if !strings.HasPrefix(string(iri), string(ASCapabilityNS)) {
			t.Errorf("capability %s has wrong namespace: %s", cap, iri)
		}
	}
}

func TestOntologyClassIRIs(t *testing.T) {
	// Verify class IRIs are in the AS namespace
	classes := []IRI{ASTool, ASAgent, ASCapability, ASService, ASClusterNode}

	for _, class := range classes {
		if !strings.HasPrefix(string(class), string(ASNamespace)) {
			t.Errorf("class %s not in AS namespace", class)
		}
	}
}

func TestOntologyPropertyIRIs(t *testing.T) {
	// Verify property IRIs are in the AS namespace
	properties := []IRI{ASHasCapability, ASRequiresTool, ASProvidesService, ASHostedOn}

	for _, prop := range properties {
		if !strings.HasPrefix(string(prop), string(ASNamespace)) {
			t.Errorf("property %s not in AS namespace", prop)
		}
	}
}

func TestContextBuilder_ToolContext(t *testing.T) {
	ctx := ToolContext()
	doc := ctx.Build()

	// Should have AS prefix
	term := doc.LookupTerm("as")
	if term == nil {
		t.Error("missing 'as' prefix in tool context")
	}
	if term.IRI != ASNamespace {
		t.Errorf("wrong AS namespace: %s", term.IRI)
	}

	// Should have capabilities term
	caps := doc.LookupTerm("capabilities")
	if caps == nil {
		t.Error("missing 'capabilities' term")
	}
	if caps.IRI != ASHasCapability {
		t.Errorf("wrong capabilities IRI: %s", caps.IRI)
	}
}

func TestContextBuilder_AgentContext(t *testing.T) {
	ctx := AgentContext()
	doc := ctx.Build()

	// Should have tools term
	tools := doc.LookupTerm("tools")
	if tools == nil {
		t.Error("missing 'tools' term")
	}
	if tools.IRI != ASRequiresTool {
		t.Errorf("wrong tools IRI: %s", tools.IRI)
	}
}

func TestToolMetadata_ToLDDocument(t *testing.T) {
	meta := &ToolMetadata{
		IRI:         ToolIRI("calculator"),
		Name:        "Calculator",
		Description: "Performs math operations",
		Capabilities: []IRI{
			StandardCapabilities["math.add"],
			StandardCapabilities["math.multiply"],
		},
		Version: "1.0.0",
	}

	doc := meta.ToLDDocument()

	// Should have root value
	if doc.Root == nil {
		t.Fatal("document has no root")
	}

	// Verify IRI was added
	if len(doc.IRIs) < 3 { // tool IRI + 2 capability IRIs
		t.Errorf("expected at least 3 IRIs, got %d", len(doc.IRIs))
	}

	// Check root has expected fields
	found := map[string]bool{"@id": false, "@type": false, "name": false}
	for _, m := range doc.Root.Members() {
		if _, ok := found[m.Key]; ok {
			found[m.Key] = true
		}
	}

	for key, ok := range found {
		if !ok {
			t.Errorf("missing expected field: %s", key)
		}
	}
}

func TestAgentMetadata_ToLDDocument(t *testing.T) {
	meta := &AgentMetadata{
		IRI:         AgentIRI("math_agent"),
		Name:        "Math Agent",
		Description: "Agent that can do math",
		Capabilities: []IRI{
			CapabilityCategories["math"],
		},
		Tools: []IRI{
			ToolIRI("calculator"),
		},
		Version: "2.0.0",
	}

	doc := meta.ToLDDocument()

	if doc.Root == nil {
		t.Fatal("document has no root")
	}

	// Should have tools field
	hasTools := false
	for _, m := range doc.Root.Members() {
		if m.Key == "tools" {
			hasTools = true
			break
		}
	}
	if !hasTools {
		t.Error("agent document missing 'tools' field")
	}
}

func BenchmarkToolIRI(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ToolIRI("calculator")
	}
}

func BenchmarkToolMetadata_ToLDDocument(b *testing.B) {
	meta := &ToolMetadata{
		IRI:         ToolIRI("calculator"),
		Name:        "Calculator",
		Description: "Performs math operations",
		Capabilities: []IRI{
			StandardCapabilities["math.add"],
			StandardCapabilities["math.multiply"],
		},
		Version: "1.0.0",
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		meta.ToLDDocument()
	}
}
