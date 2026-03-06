# Cowrie: Ideal Customer & Engineer Profile

Who needs Cowrie, how to find them, and what signals to look for.

---

## The Ideal Company

### Must-Have Traits (all three required)

1. **Polyglot backend** -- At least two of: Python, Go, Rust, C/C++, TypeScript. The pain Cowrie solves is *cross-language* data transfer. A pure-Python shop can use pickle; a pure-Go shop can use encoding/gob. The moment two languages need to talk, Cowrie's value appears.

2. **Moves structured + binary data between services** -- Not just JSON, not just raw bytes. They pass objects that contain tensors, embeddings, images, or numeric arrays alongside metadata. The "I need to send a JSON object that also has a float32 array in it" problem.

3. **Internal data volume > 100 GB/day** -- Below this threshold, serialization overhead is noise. Above it, every percentage point of compression saves real money. The sweet spot starts at 1 TB/day.

### Strong Signals (any two = high fit)

| Signal | Why It Matters | How to Detect |
|--------|---------------|---------------|
| **base64-encoded tensors in JSON** | 33% size overhead + CPU for encode/decode on every request | Search GitHub issues for "base64", "embedding", "numpy to json" in their repos |
| **cloudpickle or pickle for IPC** | Python-only, slow, insecure, breaks across versions | Search for `cloudpickle`, `pickle.dumps`, `torch.save` in their codebase |
| **Protobuf fatigue** | .proto file management, codegen pipeline, version conflicts | Search GitHub issues for "protobuf", "codegen", "schema", "breaking change" |
| **Repeated-schema event streams** | Logs/metrics/events where keys repeat millions of times (dictionary coding territory) | Their product processes logs, metrics, traces, telemetry, or IoT events |
| **Custom binary format maintained in-house** | They've already decided JSON isn't good enough but are paying maintenance tax | Search for "custom serialization", "binary format", "wire format" in eng blogs |
| **Graph-structured data** | No standard wire format exists for graphs -- everyone invents their own | They work with GNNs, knowledge graphs, social networks, or recommendation graphs |

### Disqualifying Traits

- **Single-language shop** -- pickle/gob/serde are good enough within one language
- **Columnar/analytical workloads only** -- Arrow/Parquet are better for batch analytics
- **Stable schemas with strong governance** -- Protobuf is genuinely better when schemas are frozen and managed by a platform team
- **< 50 GB/day internal data movement** -- The savings don't justify the integration effort
- **No binary data in payloads** -- If it's all strings and integers, msgpack or CBOR are simpler

---

## The Ideal Engineer

The person who evaluates and adopts Cowrie is not the CTO. It's the engineer who has *personally felt* the serialization pain.

### Title Patterns

- ML Infrastructure Engineer / ML Platform Engineer
- Backend Engineer (on a team that bridges Python ML and Go/Rust services)
- Data Infrastructure Engineer
- Observability / Pipeline Engineer
- GNN / Graph ML Engineer

### What They're Working On

They're in one of these situations right now:

1. **"I need to send embeddings from Python to Go"** -- They've tried base64 JSON, pickle over HTTP, or a custom binary format. None are satisfying. They want something that just works across languages without codegen.

2. **"Our JSON log pipeline is too expensive"** -- They process millions of events/sec with identical keys and are looking at compression, batching, or format changes to reduce costs.

3. **"Protobuf is slowing us down"** -- Not the encoding speed -- the schema management. Every new field requires a PR to the proto repo, regeneration, redeployment of all clients. They want to iterate faster.

4. **"Our graph data serialization is a mess"** -- They wrote custom pickle/JSON serialization for GNN mini-batches and it's fragile, slow, and Python-only.

5. **"We're building a multi-modal ML pipeline"** -- Images + tensors + metadata flowing between services. Currently using multipart HTTP or separate channels. They want one wire format.

### Behavioral Signals (how to find them online)

| Where to Look | What to Search |
|---------------|---------------|
| **GitHub Issues** (their company's repos) | "serialization slow", "pickle", "base64 overhead", "protobuf codegen", "JSON too large" |
| **GitHub Issues** (ecosystem repos) | Issues on numpy, torch, cloudpickle, protobuf, msgpack about cross-language transfer |
| **Hacker News** | Comments on posts about serialization formats, binary encoding, ML infrastructure. They're the ones saying "we tried X and it didn't work because..." |
| **Blog posts** | Engineering blogs about "reducing serialization overhead", "binary protocol", "replacing JSON", "cross-language ML serving" |
| **Conference talks** | MLOps, KubeCon, GopherCon, RustConf -- talks about data transfer, serving infrastructure, or pipeline optimization |
| **Stack Overflow** | Questions about "numpy array to bytes", "send tensor between services", "protobuf vs msgpack vs cbor" |
| **Twitter/X / Mastodon** | Complaints about pickle, protobuf codegen, JSON overhead. Follows accounts like @antirez, @burntsushi, MLOps community |
| **Discord / Slack** | PyTorch, Ray, MLOps Community, Rust, Go channels -- look for serialization discussions |

### Search Queries That Find Them

LinkedIn:
```
"ML infrastructure" AND ("serialization" OR "binary format" OR "protobuf" OR "wire format")
"platform engineer" AND ("Python" AND ("Go" OR "Rust")) AND ("ML" OR "inference" OR "embedding")
"data engineer" AND ("observability" OR "pipeline") AND ("JSON" OR "serialization")
```

GitHub code search:
```
language:go "base64" "embedding" "float32"
language:python "msgpack" "numpy" "serialize"
language:rust "serde" "tensor" "grpc"
"cloudpickle" "cross-language" OR "cross language"
"protobuf" "codegen" "slow" OR "overhead" OR "painful"
```

Google:
```
site:medium.com OR site:dev.to "replacing JSON" "binary format" "ML" "serving"
site:news.ycombinator.com "serialization" "protobuf" "msgpack" "ML infrastructure"
"engineering blog" "binary serialization" "embedding" "cross-language"
```

---

## Company Scoring Rubric

Score a prospect 0-3 on each dimension. Pursue if total >= 8.

| Dimension | 0 | 1 | 2 | 3 |
|-----------|---|---|---|---|
| **Language mix** | Single language | 2 languages, one dominant | 2-3 languages, active cross-language IPC | 3+ languages, all with significant traffic |
| **Data type complexity** | Strings/integers only | Some binary blobs | Tensors/embeddings regularly | Tensors + images + graphs in same pipeline |
| **Volume** | < 50 GB/day | 50-500 GB/day | 500 GB - 5 TB/day | > 5 TB/day |
| **Current pain** | Happy with JSON | Tried msgpack/CBOR | Using Protobuf but frustrated | Maintaining custom binary format |
| **Iteration speed** | Stable APIs, slow release cycle | Quarterly schema changes | Monthly schema changes | Weekly/daily schema changes (ML experiments) |

**8-9** = worth a conversation
**10-12** = strong fit, pursue actively
**13-15** = near-perfect fit, prioritize

---

## The Five Conversations

Each prospect falls into one of five opening conversations. Lead with their pain.

### Conversation 1: "The Tensor Bridge"
**For**: ML teams sending embeddings/tensors between Python and Go/Rust/C++
**Open with**: "How are you getting float32 arrays from your Python training code to your Go serving layer?"
**They'll say**: base64 JSON, pickle over HTTP, Arrow, or "we wrote something custom"
**Cowrie answer**: Zero-copy tensor encoding, receiver gets `&[f32]` / `[]float32` / `np.ndarray` directly from the buffer

### Conversation 2: "The Key Repeater"
**For**: Observability/pipeline teams processing millions of identically-shaped events
**Open with**: "What percentage of your event payload is key strings that repeat in every record?"
**They'll say**: "I never thought about it" or "probably 30-50%"
**Cowrie answer**: Dictionary coding stores keys once, references by 1-byte index. Gen2+zstd = 5-10% of JSON.

### Conversation 3: "The Codegen Tax"
**For**: Teams frustrated with Protobuf schema management
**Open with**: "How many .proto files do you maintain, and how often do you regenerate?"
**They'll say**: Dozens of files, regeneration breaks builds, version conflicts across services
**Cowrie answer**: Schemaless binary encoding, no codegen, same wire format across 5 languages

### Conversation 4: "The Graph Wire"
**For**: GNN/graph teams with no standard serialization
**Open with**: "How do you serialize graph mini-batches between your sampler and training workers?"
**They'll say**: pickle, custom CSR-to-JSON, torch.save
**Cowrie answer**: Native CSR adjacency lists + node features + edge properties in one GraphShard message

### Conversation 5: "The Multi-Modal Mess"
**For**: Teams sending images + tensors + metadata between services
**Open with**: "Are you using multipart HTTP or separate channels for binary + structured data?"
**They'll say**: Multipart with content-type headers, or separate REST calls, or base64 in JSON
**Cowrie answer**: One Cowrie message carries Image + Tensor + structured metadata natively, typed on the wire

---

## Quick Reference: Industry Verticals

| Vertical | Primary Conversation | Volume Threshold | Key Search Term |
|----------|---------------------|-----------------|-----------------|
| ML Infrastructure / Serving | Tensor Bridge | 500 GB/day | "embedding serving" "model inference" |
| Vector Databases | Tensor Bridge | 1 TB/day | "vector search" "similarity" |
| Observability / Logging | Key Repeater | 1 TB/day | "log pipeline" "metrics ingestion" |
| GNN / Graph ML | Graph Wire | 100 GB/day | "GNN training" "graph neural" |
| Multi-modal AI | Multi-Modal Mess | 500 GB/day | "vision-language" "multi-modal inference" |
| Data Streaming | Key Repeater | 5 TB/day | "Kafka" "event streaming" |
| Edge / IoT | Codegen Tax | 10 GB/day | "device telemetry" "edge inference" |
