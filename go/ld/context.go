package ld

import (
	"github.com/Neumenon/cowrie"
)

// ContextBuilder constructs JSON-LD context and term mappings for Cowrie-LD documents.
// It provides a fluent API for building type-safe context definitions.
type ContextBuilder struct {
	doc *LDDocument
}

// NewContextBuilder creates a new context builder.
func NewContextBuilder() *ContextBuilder {
	return &ContextBuilder{
		doc: NewDocument(),
	}
}

// AddPrefix registers a namespace prefix for IRI compression.
// Example: AddPrefix("as", "https://agentscope.io/ontology/2025/")
func (b *ContextBuilder) AddPrefix(prefix string, namespace IRI) *ContextBuilder {
	// Store prefix as a term with the namespace as IRI
	b.doc.AddTerm(prefix, namespace, TermFlagVocab)
	return b
}

// AddTerm maps a short term to a full IRI.
// Example: AddTerm("name", "http://schema.org/name")
func (b *ContextBuilder) AddTerm(term string, iri IRI) *ContextBuilder {
	b.doc.AddTerm(term, iri, TermFlagNone)
	return b
}

// AddTypedTerm maps a term with a specific type coercion.
// Example: AddTypedTerm("homepage", "http://schema.org/url", TermFlagID)
func (b *ContextBuilder) AddTypedTerm(term string, iri IRI, flags TermFlags) *ContextBuilder {
	b.doc.AddTerm(term, iri, flags)
	return b
}

// AddListTerm maps a term that should be treated as an ordered list.
func (b *ContextBuilder) AddListTerm(term string, iri IRI) *ContextBuilder {
	b.doc.AddTerm(term, iri, TermFlagList)
	return b
}

// AddSetTerm maps a term that should be treated as an unordered set.
func (b *ContextBuilder) AddSetTerm(term string, iri IRI) *ContextBuilder {
	b.doc.AddTerm(term, iri, TermFlagSet)
	return b
}

// AddIDTerm maps a term whose values are IRI references.
func (b *ContextBuilder) AddIDTerm(term string, iri IRI) *ContextBuilder {
	b.doc.AddTerm(term, iri, TermFlagID)
	return b
}

// Build returns the constructed LDDocument.
func (b *ContextBuilder) Build() *LDDocument {
	return b.doc
}

// WithRoot sets the root value for the document.
func (b *ContextBuilder) WithRoot(root *cowrie.Value) *ContextBuilder {
	b.doc.Root = root
	return b
}

// ToolContext returns a pre-configured context builder for tool metadata.
// Includes AgentScope vocabulary and schema.org mappings.
func ToolContext() *ContextBuilder {
	return NewContextBuilder().
		AddPrefix("as", ASNamespace).
		AddPrefix("schema", "http://schema.org/").
		AddTerm("@type", RDFType).
		AddTerm("name", "http://schema.org/name").
		AddTerm("description", ASDescription).
		AddIDTerm("capabilities", ASHasCapability).
		AddTerm("inputSchema", ASInputSchema).
		AddTerm("outputSchema", ASOutputSchema).
		AddTerm("version", ASVersion)
}

// AgentContext returns a pre-configured context builder for agent metadata.
func AgentContext() *ContextBuilder {
	return NewContextBuilder().
		AddPrefix("as", ASNamespace).
		AddPrefix("schema", "http://schema.org/").
		AddTerm("@type", RDFType).
		AddTerm("name", "http://schema.org/name").
		AddTerm("description", ASDescription).
		AddIDTerm("capabilities", ASHasCapability).
		AddIDTerm("tools", ASRequiresTool).
		AddTerm("version", ASVersion)
}

// ServiceContext returns a pre-configured context builder for service metadata.
func ServiceContext() *ContextBuilder {
	return NewContextBuilder().
		AddPrefix("as", ASNamespace).
		AddTerm("@type", RDFType).
		AddTerm("name", "http://schema.org/name").
		AddTerm("description", ASDescription).
		AddIDTerm("node", ASHostedOn).
		AddTerm("version", ASVersion)
}

// ToolMetadata represents semantic metadata for a tool.
type ToolMetadata struct {
	IRI          IRI    // Full IRI for this tool
	Name         string // Human-readable name
	Description  string // Tool description
	Capabilities []IRI  // Capability IRIs this tool provides
	Version      string // Version string
}

// ToLDDocument converts tool metadata to an Cowrie-LD document.
func (tm *ToolMetadata) ToLDDocument() *LDDocument {
	ctx := ToolContext()

	// Build the root object
	members := []cowrie.Member{
		{Key: "@id", Value: cowrie.String(string(tm.IRI))},
		{Key: "@type", Value: cowrie.String(string(ASTool))},
		{Key: "name", Value: cowrie.String(tm.Name)},
		{Key: "description", Value: cowrie.String(tm.Description)},
	}

	// Add capabilities as IRI array
	if len(tm.Capabilities) > 0 {
		caps := make([]*cowrie.Value, len(tm.Capabilities))
		for i, c := range tm.Capabilities {
			caps[i] = cowrie.String(string(c))
			ctx.doc.AddIRI(c)
		}
		members = append(members, cowrie.Member{
			Key:   "capabilities",
			Value: cowrie.Array(caps...),
		})
	}

	if tm.Version != "" {
		members = append(members, cowrie.Member{
			Key:   "version",
			Value: cowrie.String(tm.Version),
		})
	}

	ctx.doc.Root = cowrie.Object(members...)
	ctx.doc.AddIRI(tm.IRI)

	return ctx.doc
}

// AgentMetadata represents semantic metadata for an agent.
type AgentMetadata struct {
	IRI          IRI    // Full IRI for this agent
	Name         string // Human-readable name
	Description  string // Agent description
	Capabilities []IRI  // Capability IRIs this agent provides
	Tools        []IRI  // Tool IRIs this agent requires
	Version      string // Version string
}

// ToLDDocument converts agent metadata to an Cowrie-LD document.
func (am *AgentMetadata) ToLDDocument() *LDDocument {
	ctx := AgentContext()

	members := []cowrie.Member{
		{Key: "@id", Value: cowrie.String(string(am.IRI))},
		{Key: "@type", Value: cowrie.String(string(ASAgent))},
		{Key: "name", Value: cowrie.String(am.Name)},
		{Key: "description", Value: cowrie.String(am.Description)},
	}

	// Add capabilities
	if len(am.Capabilities) > 0 {
		caps := make([]*cowrie.Value, len(am.Capabilities))
		for i, c := range am.Capabilities {
			caps[i] = cowrie.String(string(c))
			ctx.doc.AddIRI(c)
		}
		members = append(members, cowrie.Member{
			Key:   "capabilities",
			Value: cowrie.Array(caps...),
		})
	}

	// Add tools
	if len(am.Tools) > 0 {
		tools := make([]*cowrie.Value, len(am.Tools))
		for i, t := range am.Tools {
			tools[i] = cowrie.String(string(t))
			ctx.doc.AddIRI(t)
		}
		members = append(members, cowrie.Member{
			Key:   "tools",
			Value: cowrie.Array(tools...),
		})
	}

	if am.Version != "" {
		members = append(members, cowrie.Member{
			Key:   "version",
			Value: cowrie.String(am.Version),
		})
	}

	ctx.doc.Root = cowrie.Object(members...)
	ctx.doc.AddIRI(am.IRI)

	return ctx.doc
}
