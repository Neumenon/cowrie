# Cowrie-LD - Binary JSON-LD Codec

A binary codec for JSON-LD built on Cowrie v2, providing efficient semantic data interchange with IRI references, blank nodes, and term tables.

## Why Cowrie-LD?

| Problem with JSON-LD | Cowrie-LD Solution |
|----------------------|-------------------|
| Verbose IRI strings | Dictionary-coded IRI table |
| Repeated `@context` | Terms table with semantic flags |
| Large blank node IDs | Compact blank node encoding |
| JSON overhead | Cowrie v2 binary efficiency |

## Quick Start

```go
import "agentscope/cowrie/ld"

// Build a context
doc := ld.NewContextBuilder().
    AddPrefix("schema", "http://schema.org/").
    AddTerm("name", "http://schema.org/name").
    AddIDTerm("knows", "http://schema.org/knows").
    Build()

// Add IRI references
personIRI := doc.AddIRI("http://example.org/people/alice")

// Encode
data, _ := ld.Encode(doc)

// Decode
decoded, _ := ld.Decode(data)
```

## Wire Format

```
Magic:     'S' 'J' 'L' 'D'  (4 bytes)
Version:   0x01              (1 byte)
Flags:     0x00              (1 byte)

FieldDict: count:uvarint + [len:uvarint + utf8]...
Terms:     count:uvarint + [TermEntry]...
IRIs:      count:uvarint + [len:uvarint + utf8]...
Datatypes: count:uvarint + [len:uvarint + utf8]...

RootValue: Cowrie v2 value tree (with extended tags)
```

### TermEntry Format

```
Term:  len:uvarint + utf8   (short name, e.g., "name")
IRI:   iriIndex:uvarint     (index into IRIs table)
Flags: 1 byte               (container/type semantics)
```

### Extended Type Tags

| Tag | Type | Format |
|-----|------|--------|
| 0x11 | IRI | iriIndex:uvarint |
| 0x12 | BNode | len:uvarint + utf8 |

All other tags use standard Cowrie v2 encoding.

## Term Flags

| Flag | Value | Description |
|------|-------|-------------|
| None | 0x00 | No special handling |
| List | 0x01 | `@container: @list` |
| Set | 0x02 | `@container: @set` |
| Language | 0x04 | `@container: @language` |
| Index | 0x08 | `@container: @index` |
| ID | 0x10 | `@type: @id` (values are IRIs) |
| Vocab | 0x20 | `@type: @vocab` (namespace prefix) |

## Standard Datatypes

Pre-defined XSD datatype IRIs:

| Constant | IRI |
|----------|-----|
| `XSDString` | `http://www.w3.org/2001/XMLSchema#string` |
| `XSDBoolean` | `http://www.w3.org/2001/XMLSchema#boolean` |
| `XSDInteger` | `http://www.w3.org/2001/XMLSchema#integer` |
| `XSDDouble` | `http://www.w3.org/2001/XMLSchema#double` |
| `XSDDecimal` | `http://www.w3.org/2001/XMLSchema#decimal` |
| `XSDDateTime` | `http://www.w3.org/2001/XMLSchema#dateTime` |
| `XSDDate` | `http://www.w3.org/2001/XMLSchema#date` |
| `XSDTime` | `http://www.w3.org/2001/XMLSchema#time` |
| `XSDAnyURI` | `http://www.w3.org/2001/XMLSchema#anyURI` |
| `XSDBase64` | `http://www.w3.org/2001/XMLSchema#base64Binary` |

## RDF Namespace Constants

| Constant | IRI |
|----------|-----|
| `RDFType` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#type` |
| `RDFFirst` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#first` |
| `RDFRest` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#rest` |
| `RDFNil` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#nil` |
| `RDFSLabel` | `http://www.w3.org/2000/01/rdf-schema#label` |

## AgentScope Vocabulary

Cowrie-LD includes a built-in vocabulary for semantic tool/agent discovery:

### Namespace

```
https://agentscope.io/ontology/2025/
```

### Classes

| Class | IRI |
|-------|-----|
| Tool | `as:Tool` |
| Agent | `as:Agent` |
| Capability | `as:Capability` |
| Service | `as:Service` |
| ClusterNode | `as:ClusterNode` |
| Message | `as:Message` |
| Conversation | `as:Conversation` |

### Properties

| Property | IRI | Description |
|----------|-----|-------------|
| hasCapability | `as:hasCapability` | Links tool/agent to capabilities |
| requiresTool | `as:requiresTool` | Links agent to required tools |
| providesService | `as:providesService` | Links node to services |
| hostedOn | `as:hostedOn` | Links tool/agent to hosting node |
| inputSchema | `as:inputSchema` | Tool input JSON schema |
| outputSchema | `as:outputSchema` | Tool output JSON schema |
| description | `as:description` | Human-readable description |
| version | `as:version` | Version string |

### Helper Functions

```go
// Generate IRIs for entities
ld.ToolIRI("calculator")       // https://agentscope.io/tool/calculator
ld.AgentIRI("assistant")       // https://agentscope.io/agent/assistant
ld.CapabilityIRI("math/add")   // https://agentscope.io/capability/math/add
ld.ServiceIRI("vectordb")      // https://agentscope.io/service/vectordb
ld.NodeIRI("node-1")           // https://agentscope.io/node/node-1
```

## Pre-built Contexts

```go
// Tool metadata context
ctx := ld.ToolContext()

// Agent metadata context
ctx := ld.AgentContext()

// Service metadata context
ctx := ld.ServiceContext()
```

## Example: Tool Metadata

```go
tool := &ld.ToolMetadata{
    IRI:          ld.ToolIRI("calculator"),
    Name:         "Calculator",
    Description:  "Performs basic arithmetic",
    Capabilities: []ld.IRI{
        ld.CapabilityIRI("math/add"),
        ld.CapabilityIRI("math/subtract"),
    },
    Version: "1.0.0",
}

doc := tool.ToLDDocument()
data, _ := ld.Encode(doc)
```

## JSON-LD Compatibility

Cowrie-LD documents can be converted to standard JSON-LD:

```go
// Cowrie-LD → JSON-LD
jsonLD, _ := ld.ToJSONLD(doc)
// {
//   "@context": {...},
//   "@id": "https://agentscope.io/tool/calculator",
//   "@type": "as:Tool",
//   "name": "Calculator",
//   ...
// }
```

## Building

```bash
go test ./cowrie/ld/...
```

## License

MIT
