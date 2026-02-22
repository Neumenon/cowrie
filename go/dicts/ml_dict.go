// Package dicts provides pre-trained compression dictionaries for common domains.
//
// Domain dictionaries encode frequently occurring keys and patterns for specific
// data types, reducing encoded size by 20-40% for domain-specific data.
//
// Supported domains:
//   - ML: Machine learning schemas (tensors, models, configs)
//   - LLM: Language model API schemas (chat, completions)
//   - JSON Schema: Schema definitions for tool calling
//   - GGUF: GGUF model format metadata keys
package dicts

import (
	"sync"
)

// Dict represents a compression dictionary mapping strings to compact indices.
type Dict struct {
	mu         sync.RWMutex
	name       string            // Dictionary name
	version    uint8             // Version for compatibility
	strToIdx   map[string]uint16 // String -> index
	idxToStr   []string          // Index -> string
	maxEntries uint16            // Maximum entries (default 65535)
}

// DictID identifies a well-known dictionary.
type DictID uint8

const (
	DictIDNone DictID = iota
	DictIDML
	DictIDLLM
	DictIDJSONSchema
	DictIDGGUF
	DictIDCustom = 0xFF
)

// NewDict creates an empty dictionary.
func NewDict(name string) *Dict {
	return &Dict{
		name:       name,
		version:    1,
		strToIdx:   make(map[string]uint16),
		idxToStr:   make([]string, 0, 256),
		maxEntries: 65535,
	}
}

// Name returns the dictionary name.
func (d *Dict) Name() string {
	return d.name
}

// Version returns the dictionary version.
func (d *Dict) Version() uint8 {
	return d.version
}

// Len returns the number of entries.
func (d *Dict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.idxToStr)
}

// Add registers a string and returns its index.
// Returns existing index if already present.
// Returns 0xFFFF if dictionary is full.
func (d *Dict) Add(s string) uint16 {
	d.mu.Lock()
	defer d.mu.Unlock()

	if idx, ok := d.strToIdx[s]; ok {
		return idx
	}

	if uint16(len(d.idxToStr)) >= d.maxEntries {
		return 0xFFFF // Dictionary full
	}

	idx := uint16(len(d.idxToStr))
	d.strToIdx[s] = idx
	d.idxToStr = append(d.idxToStr, s)
	return idx
}

// AddBatch adds multiple strings efficiently.
func (d *Dict) AddBatch(strings []string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, s := range strings {
		if _, ok := d.strToIdx[s]; ok {
			continue
		}
		if uint16(len(d.idxToStr)) >= d.maxEntries {
			return
		}
		idx := uint16(len(d.idxToStr))
		d.strToIdx[s] = idx
		d.idxToStr = append(d.idxToStr, s)
	}
}

// Lookup returns the index for a string, or -1 if not found.
func (d *Dict) Lookup(s string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx, ok := d.strToIdx[s]; ok {
		return int(idx)
	}
	return -1
}

// LookupMust returns the index, panics if not found.
func (d *Dict) LookupMust(s string) uint16 {
	idx := d.Lookup(s)
	if idx < 0 {
		panic("dict: string not found: " + s)
	}
	return uint16(idx)
}

// Get returns the string for an index, or "" if invalid.
func (d *Dict) Get(idx uint16) string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if int(idx) >= len(d.idxToStr) {
		return ""
	}
	return d.idxToStr[idx]
}

// Contains checks if a string is in the dictionary.
func (d *Dict) Contains(s string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.strToIdx[s]
	return ok
}

// Encode encodes a string using the dictionary.
// Returns (index, true) if found, (0, false) if not.
func (d *Dict) Encode(s string) (uint16, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	idx, ok := d.strToIdx[s]
	return idx, ok
}

// Decode decodes an index to its string.
// Returns empty string if index is invalid.
func (d *Dict) Decode(idx uint16) string {
	return d.Get(idx)
}

// Entries returns all dictionary entries in order.
func (d *Dict) Entries() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]string, len(d.idxToStr))
	copy(result, d.idxToStr)
	return result
}

// Clone creates a copy of the dictionary.
func (d *Dict) Clone() *Dict {
	d.mu.RLock()
	defer d.mu.RUnlock()

	clone := &Dict{
		name:       d.name,
		version:    d.version,
		strToIdx:   make(map[string]uint16, len(d.strToIdx)),
		idxToStr:   make([]string, len(d.idxToStr)),
		maxEntries: d.maxEntries,
	}

	for k, v := range d.strToIdx {
		clone.strToIdx[k] = v
	}
	copy(clone.idxToStr, d.idxToStr)

	return clone
}

// ============================================================
// Pre-built ML Dictionary
// ============================================================

// MLDict is a pre-built dictionary for ML schemas.
var MLDict = buildMLDict()

func buildMLDict() *Dict {
	d := NewDict("ml")

	// Tensor keys (high frequency)
	tensorKeys := []string{
		"shape", "dtype", "data", "strides", "offset", "device",
		"name", "op", "inputs", "outputs", "attrs",
		"dim", "dims", "ndim", "size", "numel",
		"requires_grad", "grad", "grad_fn",
	}

	// Data types (very common in ML)
	dtypes := []string{
		"float32", "float64", "float16", "bfloat16",
		"int32", "int64", "int16", "int8",
		"uint32", "uint64", "uint16", "uint8",
		"bool", "complex64", "complex128",
	}

	// Model architecture keys
	archKeys := []string{
		"model", "config", "state_dict", "weights", "bias",
		"layer", "layers", "block", "blocks",
		"encoder", "decoder", "attention", "self_attn", "cross_attn",
		"mlp", "ffn", "fc", "linear", "conv", "conv1d", "conv2d",
		"norm", "ln", "bn", "layer_norm", "batch_norm", "rms_norm",
		"embed", "embedding", "token_embedding", "position_embedding",
		"head", "heads", "num_heads", "head_dim",
		"hidden", "hidden_size", "intermediate_size",
		"dropout", "activation", "gelu", "relu", "silu", "swish",
	}

	// Training keys
	trainingKeys := []string{
		"optimizer", "scheduler", "loss", "metric", "metrics",
		"lr", "learning_rate", "weight_decay", "momentum",
		"beta1", "beta2", "eps", "epsilon",
		"step", "steps", "epoch", "epochs",
		"batch", "batch_size", "gradient", "gradients",
		"checkpoint", "save", "load", "resume",
	}

	// Common value strings
	valueStrings := []string{
		"cpu", "cuda", "mps", "xla",
		"train", "eval", "inference",
		"true", "false", "none", "null",
	}

	d.AddBatch(tensorKeys)
	d.AddBatch(dtypes)
	d.AddBatch(archKeys)
	d.AddBatch(trainingKeys)
	d.AddBatch(valueStrings)

	return d
}

// ============================================================
// Pre-built LLM API Dictionary
// ============================================================

// LLMDict is a pre-built dictionary for LLM API schemas.
var LLMDict = buildLLMDict()

func buildLLMDict() *Dict {
	d := NewDict("llm")

	// Chat message keys (very high frequency)
	chatKeys := []string{
		"role", "content", "name", "function_call", "tool_calls",
		"tool_call_id", "refusal",
		"system", "user", "assistant", "function", "tool",
	}

	// Request keys
	requestKeys := []string{
		"model", "messages", "temperature", "top_p", "top_k",
		"max_tokens", "max_completion_tokens", "stop", "stream",
		"n", "presence_penalty", "frequency_penalty", "logit_bias",
		"user", "seed", "response_format", "tools", "tool_choice",
		"parallel_tool_calls", "stream_options",
	}

	// Response keys
	responseKeys := []string{
		"id", "object", "created", "model", "choices",
		"index", "message", "delta", "finish_reason",
		"usage", "prompt_tokens", "completion_tokens", "total_tokens",
		"system_fingerprint", "logprobs",
	}

	// Tool/Function calling
	toolKeys := []string{
		"type", "function", "name", "arguments", "parameters",
		"description", "properties", "required", "enum",
		"items", "minimum", "maximum", "default",
	}

	// Common values
	commonValues := []string{
		"chat.completion", "chat.completion.chunk",
		"stop", "length", "content_filter", "tool_calls", "function_call",
		"auto", "none", "required",
		"json_object", "text",
	}

	// Model names (common ones)
	modelNames := []string{
		"gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini",
		"gpt-3.5-turbo", "gpt-3.5-turbo-16k",
		"claude-3-opus", "claude-3-sonnet", "claude-3-haiku",
		"claude-3.5-sonnet", "claude-3.5-haiku",
		"llama-3", "llama-3.1", "llama-3.2",
		"mistral", "mixtral", "gemini", "command-r",
	}

	d.AddBatch(chatKeys)
	d.AddBatch(requestKeys)
	d.AddBatch(responseKeys)
	d.AddBatch(toolKeys)
	d.AddBatch(commonValues)
	d.AddBatch(modelNames)

	return d
}

// ============================================================
// Pre-built JSON Schema Dictionary
// ============================================================

// JSONSchemaDict is a pre-built dictionary for JSON Schema.
var JSONSchemaDict = buildJSONSchemaDict()

func buildJSONSchemaDict() *Dict {
	d := NewDict("jsonschema")

	schemaKeys := []string{
		"$schema", "$id", "$ref", "$defs", "definitions",
		"type", "properties", "required", "additionalProperties",
		"items", "additionalItems", "contains",
		"minItems", "maxItems", "uniqueItems",
		"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
		"multipleOf",
		"minLength", "maxLength", "pattern", "format",
		"minProperties", "maxProperties", "propertyNames",
		"enum", "const", "default",
		"allOf", "anyOf", "oneOf", "not", "if", "then", "else",
		"title", "description", "examples", "deprecated", "readOnly", "writeOnly",
	}

	typeValues := []string{
		"string", "number", "integer", "boolean", "array", "object", "null",
	}

	formatValues := []string{
		"date-time", "date", "time", "duration",
		"email", "idn-email", "hostname", "idn-hostname",
		"ipv4", "ipv6", "uri", "uri-reference", "iri", "iri-reference",
		"uuid", "uri-template", "json-pointer", "relative-json-pointer",
		"regex",
	}

	d.AddBatch(schemaKeys)
	d.AddBatch(typeValues)
	d.AddBatch(formatValues)

	return d
}

// ============================================================
// Pre-built GGUF Dictionary
// ============================================================

// GGUFDict is a pre-built dictionary for GGUF model metadata.
var GGUFDict = buildGGUFDict()

func buildGGUFDict() *Dict {
	d := NewDict("gguf")

	// GGUF metadata keys
	ggufKeys := []string{
		"general.architecture", "general.name", "general.author",
		"general.version", "general.description", "general.file_type",
		"general.quantization_version",

		// Architecture-specific
		"llama.context_length", "llama.embedding_length", "llama.block_count",
		"llama.feed_forward_length", "llama.attention.head_count",
		"llama.attention.head_count_kv", "llama.attention.layer_norm_rms_epsilon",
		"llama.rope.freq_base", "llama.rope.dimension_count",

		// Tokenizer
		"tokenizer.ggml.model", "tokenizer.ggml.tokens", "tokenizer.ggml.scores",
		"tokenizer.ggml.token_type", "tokenizer.ggml.bos_token_id",
		"tokenizer.ggml.eos_token_id", "tokenizer.ggml.padding_token_id",
		"tokenizer.ggml.add_bos_token", "tokenizer.ggml.add_eos_token",
		"tokenizer.chat_template",

		// Tensor info
		"tensor_count", "kv_count",
	}

	// GGUF types
	ggufTypes := []string{
		"llama", "gpt2", "gptj", "gptneox", "falcon", "mpt",
		"starcoder", "refact", "bloom", "stablelm", "qwen",
		"phi2", "gemma", "command-r", "dbrx", "olmo",
	}

	// Quantization types
	quantTypes := []string{
		"f32", "f16", "q4_0", "q4_1", "q5_0", "q5_1",
		"q8_0", "q8_1", "q2_k", "q3_k", "q4_k", "q5_k", "q6_k",
		"q8_k", "iq2_xxs", "iq2_xs", "iq3_xxs", "iq1_s",
		"iq4_nl", "iq3_s", "iq2_s", "iq4_xs",
	}

	d.AddBatch(ggufKeys)
	d.AddBatch(ggufTypes)
	d.AddBatch(quantTypes)

	return d
}

// ============================================================
// Dictionary Registry
// ============================================================

var (
	registryMu sync.RWMutex
	registry   = map[DictID]*Dict{
		DictIDML:         MLDict,
		DictIDLLM:        LLMDict,
		DictIDJSONSchema: JSONSchemaDict,
		DictIDGGUF:       GGUFDict,
	}
)

// GetDict returns a dictionary by ID.
func GetDict(id DictID) *Dict {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return registry[id]
}

// RegisterDict registers a custom dictionary.
func RegisterDict(id DictID, d *Dict) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[id] = d
}
