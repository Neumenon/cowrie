/**
 * GLYPH Streaming Validator
 *
 * Validates GLYPH tool calls incrementally as tokens arrive from an LLM.
 *
 * This enables:
 * - Early tool detection: Know the tool name before full response
 * - Early rejection: Stop on unknown tools mid-stream
 * - Incremental validation: Check constraints as tokens arrive
 * - Latency savings: Reject bad payloads without waiting for completion
 */

#ifndef GLYPH_STREAM_VALIDATOR_H
#define GLYPH_STREAM_VALIDATOR_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Types
 * ============================================================ */

/** Validation error codes */
typedef enum {
    VERR_UNKNOWN_TOOL = 0,
    VERR_MISSING_REQUIRED,
    VERR_MISSING_TOOL,
    VERR_CONSTRAINT_MIN,
    VERR_CONSTRAINT_MAX,
    VERR_CONSTRAINT_LEN,
    VERR_CONSTRAINT_PATTERN,
    VERR_CONSTRAINT_ENUM,
    VERR_INVALID_TYPE,
} validation_error_code_t;

/** Validator state */
typedef enum {
    VALIDATOR_WAITING = 0,
    VALIDATOR_IN_OBJECT,
    VALIDATOR_COMPLETE,
    VALIDATOR_ERROR,
} validator_state_t;

/** Argument schema for a tool */
typedef struct {
    char *name;
    char *type;           /* "string", "int", "float", "bool" */
    bool required;
    int64_t min;          /* For numeric types (INT64_MIN if not set) */
    int64_t max;          /* For numeric types (INT64_MAX if not set) */
    size_t min_len;       /* For strings (0 if not set) */
    size_t max_len;       /* For strings (SIZE_MAX if not set) */
    char *pattern;        /* Regex pattern (NULL if not set) */
    char **enum_values;   /* Allowed values (NULL terminated, or NULL if not set) */
    size_t enum_count;
} arg_schema_t;

/** Tool schema */
typedef struct {
    char *name;
    char *description;
    arg_schema_t *args;
    size_t args_count;
    size_t args_capacity;
} tool_schema_t;

/** Tool registry */
typedef struct {
    tool_schema_t *tools;
    size_t tools_count;
    size_t tools_capacity;
} tool_registry_t;

/** Validation error */
typedef struct {
    validation_error_code_t code;
    char *message;
    char *field;
} validation_error_t;

/** Timeline event */
typedef struct {
    char *event;     /* "TOOL_DETECTED", "ERROR", "COMPLETE" */
    size_t token;
    size_t char_pos;
    uint64_t elapsed;
    char *detail;
} timeline_event_t;

/** Field value (simple variant for validation) */
typedef struct {
    enum {
        VFIELD_NULL = 0,
        VFIELD_BOOL,
        VFIELD_INT,
        VFIELD_FLOAT,
        VFIELD_STR,
    } type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        char *str_val;
    };
} validator_field_value_t;

/** Parsed field */
typedef struct {
    char *key;
    validator_field_value_t value;
} parsed_field_t;

/** Validation result */
typedef struct {
    bool complete;
    bool valid;
    char *tool_name;
    bool tool_allowed;
    bool tool_allowed_set;  /* true if tool_allowed is meaningful */

    validation_error_t *errors;
    size_t errors_count;

    parsed_field_t *fields;
    size_t fields_count;

    size_t token_count;
    size_t char_count;

    timeline_event_t *timeline;
    size_t timeline_count;

    size_t tool_detected_at_token;
    uint64_t tool_detected_at_time;
    size_t first_error_at_token;
    uint64_t first_error_at_time;
    size_t complete_at_token;
    uint64_t complete_at_time;
} validation_result_t;

/** Streaming validator */
typedef struct {
    tool_registry_t *registry;

    /* Parser state */
    char *buffer;
    size_t buffer_len;
    size_t buffer_cap;
    validator_state_t state;
    int depth;
    bool in_string;
    bool escape_next;
    char *current_key;
    char *current_val;
    size_t current_val_len;
    size_t current_val_cap;
    bool has_key;

    /* Parsed data */
    char *tool_name;
    parsed_field_t *fields;
    size_t fields_count;
    size_t fields_capacity;
    validation_error_t *errors;
    size_t errors_count;
    size_t errors_capacity;

    /* Timing */
    size_t token_count;
    size_t char_count;
    uint64_t start_time;
    size_t tool_detected_at_token;
    uint64_t tool_detected_at_time;
    size_t first_error_at_token;
    uint64_t first_error_at_time;
    size_t complete_at_token;
    uint64_t complete_at_time;

    /* Timeline */
    timeline_event_t *timeline;
    size_t timeline_count;
    size_t timeline_capacity;
} streaming_validator_t;

/* ============================================================
 * Argument Schema Functions
 * ============================================================ */

/** Create an argument schema */
arg_schema_t *arg_schema_new(const char *name, const char *type);

/** Set required flag */
void arg_schema_set_required(arg_schema_t *a, bool required);

/** Set numeric constraints */
void arg_schema_set_range(arg_schema_t *a, int64_t min, int64_t max);

/** Set string length constraints */
void arg_schema_set_length(arg_schema_t *a, size_t min_len, size_t max_len);

/** Set pattern constraint */
void arg_schema_set_pattern(arg_schema_t *a, const char *pattern);

/** Set enum values (copies the strings) */
void arg_schema_set_enum(arg_schema_t *a, const char **values, size_t count);

/** Free an argument schema */
void arg_schema_free(arg_schema_t *a);

/* ============================================================
 * Tool Schema Functions
 * ============================================================ */

/** Create a tool schema */
tool_schema_t *tool_schema_new(const char *name, const char *description);

/** Add an argument to the tool */
void tool_schema_add_arg(tool_schema_t *t, arg_schema_t *arg);

/** Free a tool schema */
void tool_schema_free(tool_schema_t *t);

/* ============================================================
 * Tool Registry Functions
 * ============================================================ */

/** Create a tool registry */
tool_registry_t *tool_registry_new(void);

/** Free a tool registry */
void tool_registry_free(tool_registry_t *r);

/** Register a tool (takes ownership of the tool schema) */
void tool_registry_register(tool_registry_t *r, tool_schema_t *tool);

/** Check if a tool is allowed */
bool tool_registry_is_allowed(const tool_registry_t *r, const char *name);

/** Get a tool schema (returns NULL if not found) */
const tool_schema_t *tool_registry_get(const tool_registry_t *r, const char *name);

/** Create a default tool registry with common tools */
tool_registry_t *tool_registry_default(void);

/* ============================================================
 * Streaming Validator Functions
 * ============================================================ */

/** Create a streaming validator */
streaming_validator_t *streaming_validator_new(tool_registry_t *registry);

/** Free a streaming validator */
void streaming_validator_free(streaming_validator_t *v);

/** Reset the validator for reuse */
void streaming_validator_reset(streaming_validator_t *v);

/** Start timing */
void streaming_validator_start(streaming_validator_t *v);

/** Process a token from the LLM */
validation_result_t *streaming_validator_push_token(streaming_validator_t *v, const char *token);

/** Get the current validation result */
validation_result_t *streaming_validator_get_result(const streaming_validator_t *v);

/** Check if the stream should be cancelled */
bool streaming_validator_should_stop(const streaming_validator_t *v);

/** Free a validation result */
void validation_result_free(validation_result_t *r);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/** Get string name for error code */
const char *validation_error_code_string(validation_error_code_t code);

/** Get string name for validator state */
const char *validator_state_string(validator_state_t state);

#ifdef __cplusplus
}
#endif

#endif /* GLYPH_STREAM_VALIDATOR_H */
