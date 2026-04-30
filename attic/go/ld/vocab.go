package ld

// AgentScope Vocabulary - IRI definitions for semantic tool and agent discovery.
//
// This vocabulary enables:
//   - Semantic discovery of tools by capability IRIs
//   - Knowledge graph construction for agents
//   - Federated reasoning across service boundaries
//
// Namespace: https://agentscope.io/ontology/2025/
//
// Example usage:
//
//	toolIRI := vocab.ToolIRI("calculator")
//	// Returns: https://agentscope.io/tool/calculator
//
//	capabilityIRI := vocab.CapabilityIRI("math.add")
//	// Returns: https://agentscope.io/capability/math.add

// Core namespace IRIs
const (
	// ASNamespace is the base namespace for AgentScope ontology.
	ASNamespace IRI = "https://agentscope.io/ontology/2025/"

	// ASToolNS is the namespace for tool identifiers.
	ASToolNS IRI = "https://agentscope.io/tool/"

	// ASAgentNS is the namespace for agent identifiers.
	ASAgentNS IRI = "https://agentscope.io/agent/"

	// ASCapabilityNS is the namespace for capability identifiers.
	ASCapabilityNS IRI = "https://agentscope.io/capability/"

	// ASServiceNS is the namespace for service identifiers.
	ASServiceNS IRI = "https://agentscope.io/service/"

	// ASNodeNS is the namespace for cluster node identifiers.
	ASNodeNS IRI = "https://agentscope.io/node/"
)

// Ontology class IRIs
const (
	// ASTool represents the Tool class in the ontology.
	ASTool IRI = ASNamespace + "Tool"

	// ASAgent represents the Agent class in the ontology.
	ASAgent IRI = ASNamespace + "Agent"

	// ASCapability represents the Capability class in the ontology.
	ASCapability IRI = ASNamespace + "Capability"

	// ASService represents the Service class in the ontology.
	ASService IRI = ASNamespace + "Service"

	// ASClusterNode represents the ClusterNode class in the ontology.
	ASClusterNode IRI = ASNamespace + "ClusterNode"

	// ASMessage represents the Message class in the ontology.
	ASMessage IRI = ASNamespace + "Message"

	// ASConversation represents the Conversation class in the ontology.
	ASConversation IRI = ASNamespace + "Conversation"
)

// Ontology property IRIs
const (
	// ASHasCapability links a tool or agent to its capabilities.
	ASHasCapability IRI = ASNamespace + "hasCapability"

	// ASRequiresTool links an agent to required tools.
	ASRequiresTool IRI = ASNamespace + "requiresTool"

	// ASProvidesService links a node to services it provides.
	ASProvidesService IRI = ASNamespace + "providesService"

	// ASHostedOn links a tool/agent to its hosting node.
	ASHostedOn IRI = ASNamespace + "hostedOn"

	// ASInputSchema links a tool to its input JSON schema.
	ASInputSchema IRI = ASNamespace + "inputSchema"

	// ASOutputSchema links a tool to its output JSON schema.
	ASOutputSchema IRI = ASNamespace + "outputSchema"

	// ASDescription provides a human-readable description.
	ASDescription IRI = ASNamespace + "description"

	// ASVersion indicates the version of a tool/agent/service.
	ASVersion IRI = ASNamespace + "version"

	// ASCreatedAt indicates when something was created.
	ASCreatedAt IRI = ASNamespace + "createdAt"

	// ASUpdatedAt indicates when something was last updated.
	ASUpdatedAt IRI = ASNamespace + "updatedAt"
)

// ToolIRI returns the full IRI for a tool with the given name.
func ToolIRI(name string) IRI {
	return IRI(string(ASToolNS) + name)
}

// AgentIRI returns the full IRI for an agent with the given name.
func AgentIRI(name string) IRI {
	return IRI(string(ASAgentNS) + name)
}

// CapabilityIRI returns the full IRI for a capability with the given name.
func CapabilityIRI(name string) IRI {
	return IRI(string(ASCapabilityNS) + name)
}

// ServiceIRI returns the full IRI for a service with the given name.
func ServiceIRI(name string) IRI {
	return IRI(string(ASServiceNS) + name)
}

// NodeIRI returns the full IRI for a cluster node with the given ID.
func NodeIRI(nodeID string) IRI {
	return IRI(string(ASNodeNS) + nodeID)
}

// CapabilityCategories defines standard capability categories.
var CapabilityCategories = map[string]IRI{
	"math":      CapabilityIRI("category/math"),
	"text":      CapabilityIRI("category/text"),
	"code":      CapabilityIRI("category/code"),
	"web":       CapabilityIRI("category/web"),
	"file":      CapabilityIRI("category/file"),
	"database":  CapabilityIRI("category/database"),
	"api":       CapabilityIRI("category/api"),
	"shell":     CapabilityIRI("category/shell"),
	"image":     CapabilityIRI("category/image"),
	"audio":     CapabilityIRI("category/audio"),
	"embedding": CapabilityIRI("category/embedding"),
	"reasoning": CapabilityIRI("category/reasoning"),
}

// StandardCapabilities defines commonly used capabilities.
var StandardCapabilities = map[string]IRI{
	// Math capabilities
	"math.add":       CapabilityIRI("math/add"),
	"math.subtract":  CapabilityIRI("math/subtract"),
	"math.multiply":  CapabilityIRI("math/multiply"),
	"math.divide":    CapabilityIRI("math/divide"),
	"math.calculate": CapabilityIRI("math/calculate"),

	// Text capabilities
	"text.summarize": CapabilityIRI("text/summarize"),
	"text.translate": CapabilityIRI("text/translate"),
	"text.extract":   CapabilityIRI("text/extract"),
	"text.generate":  CapabilityIRI("text/generate"),

	// Code capabilities
	"code.execute":  CapabilityIRI("code/execute"),
	"code.analyze":  CapabilityIRI("code/analyze"),
	"code.generate": CapabilityIRI("code/generate"),
	"code.format":   CapabilityIRI("code/format"),

	// Web capabilities
	"web.fetch":  CapabilityIRI("web/fetch"),
	"web.search": CapabilityIRI("web/search"),
	"web.scrape": CapabilityIRI("web/scrape"),

	// File capabilities
	"file.read":  CapabilityIRI("file/read"),
	"file.write": CapabilityIRI("file/write"),
	"file.list":  CapabilityIRI("file/list"),

	// Database capabilities
	"database.query":  CapabilityIRI("database/query"),
	"database.insert": CapabilityIRI("database/insert"),
	"database.update": CapabilityIRI("database/update"),
	"database.delete": CapabilityIRI("database/delete"),

	// String capabilities
	"string.format": CapabilityIRI("string/format"),
	"string.concat": CapabilityIRI("string/concat"),
	"string.split":  CapabilityIRI("string/split"),
}

// Capability constants for direct use in code
var (
	// Math capabilities
	CapabilityMathAdd       = CapabilityIRI("math/add")
	CapabilityMathSubtract  = CapabilityIRI("math/subtract")
	CapabilityMathMultiply  = CapabilityIRI("math/multiply")
	CapabilityMathDivide    = CapabilityIRI("math/divide")
	CapabilityMathCalculate = CapabilityIRI("math/calculate")

	// String capabilities
	CapabilityStringFormat = CapabilityIRI("string/format")
	CapabilityStringConcat = CapabilityIRI("string/concat")
	CapabilityStringSplit  = CapabilityIRI("string/split")

	// Text capabilities
	CapabilityTextSummarize = CapabilityIRI("text/summarize")
	CapabilityTextTranslate = CapabilityIRI("text/translate")
	CapabilityTextExtract   = CapabilityIRI("text/extract")
	CapabilityTextGenerate  = CapabilityIRI("text/generate")

	// Code capabilities
	CapabilityCodeExecute  = CapabilityIRI("code/execute")
	CapabilityCodeAnalyze  = CapabilityIRI("code/analyze")
	CapabilityCodeGenerate = CapabilityIRI("code/generate")
	CapabilityCodeFormat   = CapabilityIRI("code/format")

	// Web capabilities
	CapabilityWebFetch  = CapabilityIRI("web/fetch")
	CapabilityWebSearch = CapabilityIRI("web/search")
	CapabilityWebScrape = CapabilityIRI("web/scrape")

	// File capabilities
	CapabilityFileRead  = CapabilityIRI("file/read")
	CapabilityFileWrite = CapabilityIRI("file/write")
	CapabilityFileList  = CapabilityIRI("file/list")
)
