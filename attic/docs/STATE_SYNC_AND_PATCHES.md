# Glyph Research: State Synchronization, Patches, and Collaborative Editing

**Date:** 2026-03-29
**Scope:** Market landscape for Glyph's unique features beyond token efficiency

---

## Executive Summary

Glyph has four structural advantages that go beyond "compact JSON": `@patch` state deltas with SHA-256 fingerprinting, `@pool` deduplication, `@schema` inline declarations, and streaming validation. This research maps those features against six market segments to identify where they create differentiated value.

**Key finding:** The biggest opportunity is not a single killer market, but the convergence of three trends:
1. **MCP token tax** -- tool definitions consume 40-50% of LLM context windows; compact formats are in active demand.
2. **TOON validation** -- a new format (Token-Oriented Object Notation) proves the market accepts non-JSON text for LLM workloads, but lacks schemas, patches, or references.
3. **Local-first / collaborative editing** -- CRDT metadata bloat (16-32 bytes per character) creates demand for compact delta wire formats.

Glyph is the only format that combines token efficiency, human readability, inline schemas, state-verified patching, and streaming validation in a single spec.

---

## 1. Real-Time Collaboration and Delta Sync

### How the leaders work

| Tool | Sync Algorithm | Delta Wire Format | Pain Points |
|------|---------------|-------------------|-------------|
| **Google Docs** | OT (Operational Transformation) | Proprietary JSON-like ops | Complex server, single authority |
| **Figma** | CRDT (custom, periodically compacted) | Binary deltas over WebSocket | 16-32 bytes metadata per character |
| **Notion** | Hybrid (CRDT structure + OT text) | JSON operations | Block-level granularity only |
| **Linear** | Sync engine with last-writer-wins | JSON deltas | No rich merge semantics |

**CRDT metadata overhead is real:** A basic string CRDT (RGA or YATA) adds 16-32 bytes per character. A 10,000-character document balloons from 10KB to 160-320KB. Automerge 2.0 reduced this to ~30% overhead via columnar encoding, but that is a binary format -- not human-readable or debuggable.

**Yjs** stores a 100KB document in ~160KB on disk (1.6x) or 3MB in memory. Automerge's naive JSON encoding historically reached 1,300 bytes per character before their Rust rewrite.

### Where Glyph fits

Glyph `@patch` is not a CRDT -- it is a **compact, human-readable delta format with state fingerprinting**. This positions it as:

- **A wire format for CRDT-based systems** that need to send deltas between server and client. Instead of JSON operations like `{"op": "replace", "path": "/users/0/name", "value": "Alice"}` (78 bytes), Glyph can express the same as `@patch{sha=abc123 ops=[{replace /users/0/name Alice}]}`.
- **A conflict detection layer** for systems that use last-writer-wins. The SHA-256 state hash in `@patch` catches concurrent modifications at the protocol level -- no separate ETag or version counter needed.

**Opportunity:** Pitch Glyph `@patch` as a transport encoding for local-first frameworks (Automerge, Yjs, Electric SQL) that need to send operations over constrained channels (WebSocket, SSE, MQTT).

### Sources
- [How Figma's multiplayer technology works](https://www.figma.com/blog/how-figmas-multiplayer-technology-works/)
- [Understanding sync engines: Figma, Linear, Google Docs](https://liveblocks.io/blog/understanding-sync-engines-how-figma-linear-and-google-docs-work)
- [Introducing Automerge 2.0](https://automerge.org/blog/automerge-2/)
- [CRDTs are the future](https://josephg.com/blog/crdts-are-the-future/)
- [Best CRDT Libraries 2025](https://velt.dev/blog/best-crdt-libraries-real-time-data-sync)
- [Peritext: A CRDT for Rich-Text Collaboration](https://www.inkandswitch.com/peritext/)

---

## 2. JSON Patch (RFC 6902) Verbosity Problem

### The problem is well-documented

JSON Patch (RFC 6902) is the IETF standard for describing mutations to JSON documents. It is verbose by design:

```json
[
  {"op": "replace", "path": "/name", "value": "Alice"},
  {"op": "add", "path": "/tags/-", "value": "premium"},
  {"op": "remove", "path": "/old_field"}
]
```

Three operations = 150+ bytes of JSON, with repeated structural keys (`op`, `path`, `value`) on every entry.

**JSON Merge Patch (RFC 7386)** is more compact but cannot express deletions (setting a key to `null` means "delete", so you can never set a key to actual null).

### Existing alternatives and their gaps

| Tool | Approach | Limitation |
|------|----------|------------|
| **Patchpack** | Binary serialization of JSON Patch via MessagePack | Not human-readable; requires schema sync |
| **jsondiffpatch** | Custom delta format balancing readability and size | Non-standard, JS-only ecosystem |
| **JSON Merge Patch** | Looks like a partial document | Cannot express null values or array ops |
| **Protobuf FieldMask** | Field-level update masking | Requires .proto schema files |

### Glyph @patch advantage

Glyph `@patch` combines the expressiveness of RFC 6902 with the compactness of merge patches:
- **Named operations** without repeated JSON boilerplate
- **SHA-256 base state hash** for optimistic concurrency (no separate ETag mechanism)
- **Human-readable** -- you can debug patches in a terminal
- **Null-safe** -- `_` represents null distinctly from deletion

**Key differentiator:** No existing patch format combines compact syntax + state verification + human readability. Patchpack is compact but binary. JSON Patch is readable but verbose. Glyph @patch is both compact and readable, with built-in concurrency control.

**Opportunity:** Position `@patch` as a drop-in improvement for any system currently using JSON Patch over HTTP or WebSocket, especially real-time dashboards, game state sync, and configuration management APIs.

### Sources
- [RFC 6902 JSON Patch](https://datatracker.ietf.org/doc/html/rfc6902)
- [JSON Patch vs Merge Patch](https://erosb.github.io/json-patch-vs-merge-patch/)
- [Patchpack - Binary JSON Patch serializer](https://github.com/udamir/patchpack)
- [jsondiffpatch delta format](https://github.com/benjamine/jsondiffpatch/blob/master/docs/deltas.md)
- [Synchronizing state with WebSockets and JSON Patch](https://cetra3.github.io/blog/synchronising-with-websocket/)

---

## 3. Game State Sync and Configuration

### The landscape

Game developers use text formats extensively for configuration, scene descriptions, and save data:

| Engine | Format | Known Frustrations |
|--------|--------|--------------------|
| **Unity** | YAML (serialized) | Merge conflicts are notorious; every object reference is a numeric fileID; "Git + Unity is full of spiders" |
| **Godot** | .tscn (INI-like) | Better than Unity for version control but still generates merge conflicts on concurrent edits |
| **Unreal** | .uasset (binary) + .ini | Binary assets are opaque; text configs lack structure |
| **Custom engines** | JSON/YAML | Verbose for large data tables; no incremental update support |

**Unity's YAML pain is acute:** When two developers move different objects in the same scene, merge tools produce unfixable conflicts because they lack semantic understanding of the YAML structure. Unity ships a `UnityYAMLMerge` tool, but it cannot resolve conflicts on the same GameObject.

**Patchpack was born from games:** The npm package was "developed for a game server framework that uses WebSockets to synchronize state between server and clients" -- proving that compact delta encoding for game state is an active need.

### Where Glyph fits

1. **Game config format:** Glyph's bare-word keys and compact syntax make it viable for data-driven game design files (item tables, AI rules, dialogue trees). `@schema` provides inline validation without separate schema files.

2. **Hot-reload patches:** Game engines that support hot-reload of config files could use `@patch` to send only the changed fields, with SHA-256 verification that the base state matches. This prevents applying a stale patch to a modified config.

3. **Scene delta format:** Instead of trying to merge entire Unity YAML scenes, a Glyph-based workflow could express scene changes as `@patch` operations, which are composable and conflict-detectable.

**Opportunity:** Build a Godot plugin or Unity editor extension that exports scene deltas as Glyph `@patch` documents, making version control diffs meaningful.

### Sources
- [TSCN file format - Godot 4.4 docs](https://docs.godotengine.org/en/4.4/contributing/development/file_formats/tscn.html)
- [Merge Conflicts in Unity - How to avoid them](https://manuel-rauber.com/2023/01/25/merge-conflicts-in-unity-how-to-avoid-them/)
- [YAML scene merge broke the scene - Unity Discussions](https://discussions.unity.com/t/yaml-scene-merge-broke-the-scene/823946)
- [Patchpack npm](https://www.npmjs.com/package/patchpack)

---

## 4. MCP Token Tax -- The Biggest Near-Term Opportunity

### The problem is severe and growing

The Model Context Protocol consumes **40-50% of available context windows** before agents perform any actual work:

- **Per-tool cost:** 550-1,400 tokens for name, description, JSON schema, field descriptions, enums, and system instructions
- **Real-world case:** 40 tools = 55,000 tokens of definitions before a single user message
- **Extreme case:** Three MCP servers consuming 143,000 of 200,000 tokens (72% burned on tool definitions)
- **Scalekit benchmark:** MCP costs 4-32x more tokens than CLI for identical operations

**Industry response is fragmented:**
- Anthropic published research showing code execution reduces context overhead by 98.7%
- Perplexity's CTO announced moving away from MCP toward APIs and CLI tools
- Progressive disclosure (CLI `--help` patterns) reduces initial overhead but adds round-trips

### Where Glyph fits -- and why it is different from current solutions

Current MCP responses are JSON. Tool output schemas (released 2025-06-18) help the LLM understand response shapes, but the responses themselves remain verbose JSON.

**Glyph as an MCP transport format provides:**

1. **30-50% token reduction on tool responses** -- the same data in fewer tokens, directly reducing context consumption
2. **`@schema` inline declarations** -- tool output shapes embedded in the response, not requiring separate schema registration
3. **Streaming validation** -- detect malformed tool responses mid-stream, cancel early instead of wasting tokens on garbage output
4. **`@pool` deduplication** -- when multiple tool responses share common structures (e.g., repeated user objects), pool references avoid redundant encoding

**This is not a competing protocol to MCP.** Glyph is a content encoding that MCP can adopt for its JSON-RPC payloads, just as HTTP can use gzip for content encoding without changing the HTTP spec.

**Opportunity:** Propose a Glyph content-type for MCP tool responses. Build a reference MCP server that returns Glyph-encoded responses and benchmark the token savings against JSON baselines.

### Sources
- [MCP Protocol Evolution 2026](https://blogs.versalence.ai/mcp-model-context-protocol-evolution-2026)
- [Your MCP Server Is Eating Your Context Window](https://www.apideck.com/blog/mcp-server-eating-context-window-cli-alternative)
- [The MCP Tax: Hidden Costs](https://www.mmntm.net/articles/mcp-context-tax)
- [Code execution with MCP - Anthropic](https://www.anthropic.com/engineering/code-execution-with-mcp)
- [2026 MCP Roadmap](http://blog.modelcontextprotocol.io/posts/2026-mcp-roadmap/)
- [MCP Tool Descriptions Are Smelly - arXiv](https://arxiv.org/html/2602.14878v1)

---

## 5. TOON -- The Direct Comparator

### What TOON is

TOON (Token-Oriented Object Notation) is a compact, human-readable serialization format designed specifically for LLM prompts. It launched in late 2025 and has gained significant attention:

**Syntax example:**
```
users[2]{id,name,role}:
  1,Alice,admin
  2,Bob,user
```

**Performance:** 30-60% token reduction vs JSON on uniform arrays. 39.9% fewer tokens across mixed datasets. 76.4% accuracy vs JSON's 75.0% in retrieval benchmarks.

### Feature comparison: Glyph vs TOON

| Feature | Glyph | TOON |
|---------|-------|------|
| Token reduction vs JSON | 30-50% | 30-60% (tabular), ~0% (nested) |
| Human-readable | Yes | Yes |
| Nested objects | Full support | Degrades; JSON-compact often wins |
| Inline schema (@schema) | Yes | No (field headers only) |
| State-verified patches (@patch) | Yes | No |
| Pool references (@pool) | Yes | No |
| Streaming validation | Yes (GS1 protocol) | No |
| SHA-256 state fingerprinting | Yes | No |
| Lossless JSON round-trip | Yes | Yes |
| Wikipedia page | No | Yes |
| npm ecosystem | Yes | Yes |
| Multi-language SDKs | Go, Python, TS, Rust, C | TypeScript, Python |

### What this means

TOON validates the market for compact LLM text formats. It proves developers will adopt non-JSON encoding to save tokens. But TOON is essentially "CSV for JSON arrays" -- it excels at flat tabular data and falls off for nested, irregular structures.

**Glyph's advantage is depth:** it handles the same token-efficiency use case as TOON but also provides schema validation, state management (patches + hashes), deduplication (pools), and streaming -- features TOON does not offer.

**Glyph's risk:** TOON already has Wikipedia coverage, a GitHub spec, FreeCodeCamp articles, and LogRocket tutorials. Glyph needs to establish awareness before TOON becomes the default "compact LLM format."

**Strategic response:**
1. Publish head-to-head benchmarks (Glyph vs TOON vs JSON) on mixed workloads where TOON degrades
2. Emphasize features TOON cannot match: `@patch`, `@pool`, `@schema`, streaming
3. Position Glyph as "TOON for the hard problems" -- when you need more than flat tables

### Sources
- [TOON Format official site](https://toonformat.dev/)
- [TOON GitHub - Spec, benchmarks, TypeScript SDK](https://github.com/toon-format/toon)
- [TOON vs JSON - Tensorlake](https://www.tensorlake.ai/blog-posts/toon-vs-json)
- [How to use TOON to reduce token usage by 60% - LogRocket](https://blog.logrocket.com/reduce-tokens-with-toon/)
- [TOON Wikipedia](https://en.wikipedia.org/wiki/Token-Oriented_Object_Notation)
- [TOON vs JSON: Supercharge Your LLM Prompts](https://www.thakurcoder.com/blog/2025-11-05-toon-vs-json-supercharge-your-llm-prompts-and-cut-token-costs)

---

## 6. IoT Device Configuration

### Landscape

IoT devices on constrained networks (6LoWPAN, LoRa, satellite) care about every byte:

| Format | Typical Payload Size | Human-Readable | Schema Support |
|--------|---------------------|----------------|----------------|
| JSON | Baseline (100%) | Yes | External (JSON Schema) |
| CBOR | 30-50% smaller | No (binary) | CDDL |
| MessagePack | 30-50% smaller | No (binary) | No |
| YAML | 5-10% larger | Yes | External |
| **Glyph** | **30-50% smaller** | **Yes** | **Inline (@schema)** |

**Real-world impact:** A 2025 IoT deployment switching from JSON to CBOR saw payloads shrink 72% and saved $28k/month in bandwidth costs processing 500M messages/day.

**AWS IoT Core** limits MQTT payloads to 128KB. CoAP (RFC 7252) is designed for microcontrollers with kilobytes of RAM.

### Where Glyph fits

Glyph occupies a unique position: **human-readable AND compact.** CBOR and MessagePack are compact but binary -- you cannot debug them in a terminal. JSON is readable but verbose.

For IoT config management specifically:
- **`@patch` for OTA config updates** -- send only changed fields, with SHA-256 verification that the device's current config matches expectations
- **`@schema` for self-describing payloads** -- device can validate config without a separate schema file
- **30-50% smaller than JSON** while remaining human-debuggable

**Limitation:** For the most constrained devices (sub-1KB RAM), binary formats like CBOR remain more appropriate. Glyph targets the "smart edge" tier -- devices with enough resources to process text but where bandwidth matters (e.g., cellular IoT, satellite backhaul).

### Sources
- [CBOR vs the Other Guys](https://cborbook.com/introduction/cbor_vs_the_other_guys.html)
- [Optimizing API Performance with Protocol Buffers, FlatBuffers, MessagePack, CBOR](https://www.cloudthat.com/resources/blog/optimizing-api-performance-with-protocol-buffers-flatbuffers-messagepack-and-cbor)
- [MQTT Protocol: Lightweight IoT Messaging 2026](https://calmops.com/network/mqtt-protocol-iot-messaging-2026/)
- [Optimizing IoT Protocols for Edge Microservices](https://blog.dreamfactory.com/optimizing-iot-protocols-for-edge-microservices)

---

## 7. API Versioning and Schema Evolution

### Landscape

Schema-heavy approaches dominate:
- **GraphQL:** Schema-first, continuous evolution via field deprecation, no versioning
- **gRPC/Protobuf:** Field numbers enable backward-compatible additions, efficient binary encoding
- **OpenAPI/Swagger:** JSON Schema for REST APIs, schemas live in separate files

All of these require **external schema files** -- .graphql, .proto, openapi.yaml. The schema is never in the payload.

### Where Glyph @schema fits

Glyph's `@schema` is **inline** -- the schema travels with the data:

```
@schema{
  user{name=str email=str age=int tags=[str]}
}
{name=Alice email=alice@example.com age=30 tags=[premium beta]}
```

This is valuable for:
- **Schema negotiation in multi-agent systems** -- agents can declare their expected input/output shapes without a shared schema registry
- **Self-describing API responses** -- no separate OpenAPI file needed for one-off or exploratory APIs
- **Configuration files** -- the config file carries its own validation rules

**Limitation:** For high-performance APIs where schema registration is already established (gRPC, GraphQL), inline schemas add overhead rather than reducing it. `@schema` is most valuable in ad-hoc, schema-optional environments.

### Sources
- [How to Handle Versioning in GraphQL APIs](https://oneuptime.com/blog/post/2026-01-24-graphql-api-versioning/view)
- [API Design Principles 2026: REST vs gRPC vs GraphQL vs tRPC](https://ruchitsuthar.com/blog/software-craftsmanship/api-design-principles-rest-grpc-graphql/)
- [Schema Design - GraphQL](https://graphql.org/learn/schema-design/)

---

## 8. Document Databases

### Landscape

Document databases store JSON:
- **MongoDB** uses BSON (binary JSON) internally, with key compression
- **CouchDB** stores raw JSON, optimizes replication with incremental sync
- **RxDB** offers key-compression to reduce document size
- **PostgreSQL JSONB** is a binary JSON representation for efficient queries

### Where Glyph fits

The opportunity here is **niche but real** for:
- **Embedded databases** (SQLite-backed, local-first) where document size directly impacts I/O
- **Replication payloads** -- CouchDB-style replication could use Glyph for 30-50% smaller sync payloads
- **Audit logs** -- append-only stores where compact text representation saves storage while remaining grep-able

**Honest assessment:** Major document databases are unlikely to adopt a new text format. The opportunity is in **new** database projects, particularly local-first databases (OctoBase, RxDB, Electric SQL) where the format is still flexible.

### Sources
- [MongoDB, CouchDB, PostgreSQL JSON compared](https://dasroot.net/posts/2026/01/mongodb-couchdb-and-postgresql-json/)
- [JSON-Based Databases - RxDB](https://rxdb.info/articles/json-based-database.html)
- [OctoBase - Local-first collaborative database](https://octobase.dev/)

---

## 9. Optimistic Concurrency: SHA-256 State Hashing

### Industry pattern

Optimistic concurrency control using content hashes is standard in APIs:
- **HTTP ETags** (RFC 7232) use entity hashes for cache validation and conflict detection
- **CouchDB** uses document revision IDs (_rev) for conflict detection
- **Square API** uses version tokens for optimistic concurrency
- **Event sourcing** systems use expected version numbers

In 2025-2026, optimistic concurrency is preferred "because it favors speed and scalability when conflicts are relatively rare."

### Glyph's integrated approach

Most systems bolt on concurrency control as a separate mechanism (ETag header, _rev field, version column). Glyph `@patch` embeds the state hash **in the patch document itself**:

```
@patch{
  base=sha256:a1b2c3d4...
  ops=[
    {set /config/timeout 30}
    {del /config/deprecated_field}
  ]
}
```

The base hash and operations travel together as a single atomic unit. This is cleaner than the HTTP pattern of `If-Match` header + separate JSON Patch body, because:
1. The hash cannot be accidentally omitted
2. The patch is self-contained and auditable
3. Multiple patches can be chained with intermediate hashes

### Sources
- [How to Implement API ETag Headers](https://oneuptime.com/blog/post/2026-01-30-api-etag-headers/view)
- [Optimistic Concurrency Control: A Practical Guide for 2025](https://www.shadecoder.com/topics/optimistic-concurrency-control-a-practical-guide-for-2025)
- [Square API - Optimistic Concurrency](https://developer.squareup.com/docs/build-basics/common-api-patterns/optimistic-concurrency)

---

## 10. Strategic Priorities

### Tier 1 -- Act now

**MCP integration.** The token tax problem is acute (40-50% context window waste), the industry is actively seeking solutions, and Glyph is ready today as a content encoding. Build a reference MCP server with Glyph-encoded responses, publish benchmarks, propose to the MCP spec as an optional content type.

**TOON competitive response.** TOON is gaining awareness fast. Publish comparison benchmarks emphasizing nested/irregular data (where TOON fails), and highlight features TOON lacks entirely: `@patch`, `@pool`, `@schema`, streaming validation.

### Tier 2 -- Build ecosystem

**Game config format.** Start with Godot (text-format-friendly community). Build a .glyph importer/exporter. Demonstrate `@patch` for hot-reload config updates. The Unity YAML pain is a strong narrative hook.

**Local-first / collaboration.** Propose Glyph as a delta wire format for Automerge or Yjs sync protocols. The CRDT metadata overhead problem creates natural demand for compact encodings.

### Tier 3 -- Long-term positioning

**IoT edge configuration.** Target the "smart edge" tier (cellular IoT, satellite backhaul) where human readability matters but bandwidth is constrained. Partner with an IoT platform for a case study.

**API schema evolution.** Position `@schema` for multi-agent and exploratory API scenarios. This is a slow burn -- established API ecosystems (GraphQL, gRPC) will not switch formats, but new agent-to-agent protocols might adopt inline schemas from the start.

---

## Appendix: LLM Token Efficiency Market Context

The market for token-efficient formats is validated and growing:

- **TOON** (2025): 30-60% token savings, Wikipedia page, active ecosystem
- **Poor serialization wastes 40-70% of available tokens** through formatting overhead (Redis blog, 2026)
- **Anthropic's own research** on code execution with MCP shows 98.7% context reduction -- the problem is real
- **SAP, LogRocket, FreeCodeCamp** all publishing guides on token-efficient data formats -- mainstream awareness is here
- **TOON benchmarks** show 86.6% accuracy vs 83.2% with JSON -- compact formats can improve model performance, not just reduce cost

Glyph is not entering an empty market. It is entering a market that TOON has warmed up, with features TOON cannot match.
