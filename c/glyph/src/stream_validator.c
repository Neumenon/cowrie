/**
 * GLYPH Streaming Validator - C Implementation
 */

#define _POSIX_C_SOURCE 199309L

#include "stream_validator.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>
#include <limits.h>

/* ============================================================
 * Internal Helpers
 * ============================================================ */

static char *strdup_safe(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (copy) memcpy(copy, s, len + 1);
    return copy;
}

static uint64_t current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* ============================================================
 * Argument Schema Functions
 * ============================================================ */

arg_schema_t *arg_schema_new(const char *name, const char *type) {
    arg_schema_t *a = calloc(1, sizeof(arg_schema_t));
    if (!a) return NULL;

    a->name = strdup_safe(name);
    a->type = strdup_safe(type);
    a->required = false;
    a->min = INT64_MIN;
    a->max = INT64_MAX;
    a->min_len = 0;
    a->max_len = SIZE_MAX;
    a->pattern = NULL;
    a->enum_values = NULL;
    a->enum_count = 0;

    return a;
}

void arg_schema_set_required(arg_schema_t *a, bool required) {
    if (a) a->required = required;
}

void arg_schema_set_range(arg_schema_t *a, int64_t min, int64_t max) {
    if (a) {
        a->min = min;
        a->max = max;
    }
}

void arg_schema_set_length(arg_schema_t *a, size_t min_len, size_t max_len) {
    if (a) {
        a->min_len = min_len;
        a->max_len = max_len;
    }
}

void arg_schema_set_pattern(arg_schema_t *a, const char *pattern) {
    if (a) {
        free(a->pattern);
        a->pattern = strdup_safe(pattern);
    }
}

void arg_schema_set_enum(arg_schema_t *a, const char **values, size_t count) {
    if (!a) return;

    /* Free existing */
    if (a->enum_values) {
        for (size_t i = 0; i < a->enum_count; i++) {
            free(a->enum_values[i]);
        }
        free(a->enum_values);
    }

    a->enum_values = calloc(count, sizeof(char *));
    a->enum_count = count;

    for (size_t i = 0; i < count; i++) {
        a->enum_values[i] = strdup_safe(values[i]);
    }
}

void arg_schema_free(arg_schema_t *a) {
    if (!a) return;

    free(a->name);
    free(a->type);
    free(a->pattern);

    if (a->enum_values) {
        for (size_t i = 0; i < a->enum_count; i++) {
            free(a->enum_values[i]);
        }
        free(a->enum_values);
    }

    free(a);
}

/* ============================================================
 * Tool Schema Functions
 * ============================================================ */

tool_schema_t *tool_schema_new(const char *name, const char *description) {
    tool_schema_t *t = calloc(1, sizeof(tool_schema_t));
    if (!t) return NULL;

    t->name = strdup_safe(name);
    t->description = strdup_safe(description);
    t->args = NULL;
    t->args_count = 0;
    t->args_capacity = 0;

    return t;
}

void tool_schema_add_arg(tool_schema_t *t, arg_schema_t *arg) {
    if (!t || !arg) return;

    if (t->args_count >= t->args_capacity) {
        size_t new_cap = t->args_capacity == 0 ? 8 : t->args_capacity * 2;
        arg_schema_t *new_args = realloc(t->args, new_cap * sizeof(arg_schema_t));
        if (!new_args) return;
        t->args = new_args;
        t->args_capacity = new_cap;
    }

    t->args[t->args_count++] = *arg;
    free(arg); /* Ownership transferred */
}

void tool_schema_free(tool_schema_t *t) {
    if (!t) return;

    free(t->name);
    free(t->description);

    for (size_t i = 0; i < t->args_count; i++) {
        free(t->args[i].name);
        free(t->args[i].type);
        free(t->args[i].pattern);
        if (t->args[i].enum_values) {
            for (size_t j = 0; j < t->args[i].enum_count; j++) {
                free(t->args[i].enum_values[j]);
            }
            free(t->args[i].enum_values);
        }
    }
    free(t->args);

    free(t);
}

/* ============================================================
 * Tool Registry Functions
 * ============================================================ */

tool_registry_t *tool_registry_new(void) {
    tool_registry_t *r = calloc(1, sizeof(tool_registry_t));
    if (!r) return NULL;

    r->tools = NULL;
    r->tools_count = 0;
    r->tools_capacity = 0;

    return r;
}

void tool_registry_free(tool_registry_t *r) {
    if (!r) return;

    for (size_t i = 0; i < r->tools_count; i++) {
        free(r->tools[i].name);
        free(r->tools[i].description);

        for (size_t j = 0; j < r->tools[i].args_count; j++) {
            free(r->tools[i].args[j].name);
            free(r->tools[i].args[j].type);
            free(r->tools[i].args[j].pattern);
            if (r->tools[i].args[j].enum_values) {
                for (size_t k = 0; k < r->tools[i].args[j].enum_count; k++) {
                    free(r->tools[i].args[j].enum_values[k]);
                }
                free(r->tools[i].args[j].enum_values);
            }
        }
        free(r->tools[i].args);
    }
    free(r->tools);

    free(r);
}

void tool_registry_register(tool_registry_t *r, tool_schema_t *tool) {
    if (!r || !tool) return;

    if (r->tools_count >= r->tools_capacity) {
        size_t new_cap = r->tools_capacity == 0 ? 8 : r->tools_capacity * 2;
        tool_schema_t *new_tools = realloc(r->tools, new_cap * sizeof(tool_schema_t));
        if (!new_tools) return;
        r->tools = new_tools;
        r->tools_capacity = new_cap;
    }

    r->tools[r->tools_count++] = *tool;
    free(tool); /* Ownership transferred */
}

bool tool_registry_is_allowed(const tool_registry_t *r, const char *name) {
    return tool_registry_get(r, name) != NULL;
}

const tool_schema_t *tool_registry_get(const tool_registry_t *r, const char *name) {
    if (!r || !name) return NULL;

    for (size_t i = 0; i < r->tools_count; i++) {
        if (strcmp(r->tools[i].name, name) == 0) {
            return &r->tools[i];
        }
    }
    return NULL;
}

tool_registry_t *tool_registry_default(void) {
    tool_registry_t *r = tool_registry_new();
    if (!r) return NULL;

    /* Register 'search' tool */
    tool_schema_t *search = tool_schema_new("search", "Search for information");
    arg_schema_t *query = arg_schema_new("query", "string");
    arg_schema_set_required(query, true);
    arg_schema_set_length(query, 1, SIZE_MAX);
    tool_schema_add_arg(search, query);

    arg_schema_t *max_results = arg_schema_new("max_results", "int");
    arg_schema_set_range(max_results, 1, 100);
    tool_schema_add_arg(search, max_results);
    tool_registry_register(r, search);

    /* Register 'calculate' tool */
    tool_schema_t *calculate = tool_schema_new("calculate", "Evaluate a mathematical expression");
    arg_schema_t *expression = arg_schema_new("expression", "string");
    arg_schema_set_required(expression, true);
    tool_schema_add_arg(calculate, expression);

    arg_schema_t *precision = arg_schema_new("precision", "int");
    arg_schema_set_range(precision, 0, 15);
    tool_schema_add_arg(calculate, precision);
    tool_registry_register(r, calculate);

    /* Register 'browse' tool */
    tool_schema_t *browse = tool_schema_new("browse", "Fetch a web page");
    arg_schema_t *url = arg_schema_new("url", "string");
    arg_schema_set_required(url, true);
    arg_schema_set_pattern(url, "^https?://");
    tool_schema_add_arg(browse, url);
    tool_registry_register(r, browse);

    /* Register 'execute' tool */
    tool_schema_t *execute = tool_schema_new("execute", "Execute a shell command");
    arg_schema_t *command = arg_schema_new("command", "string");
    arg_schema_set_required(command, true);
    tool_schema_add_arg(execute, command);
    tool_registry_register(r, execute);

    return r;
}

/* ============================================================
 * Streaming Validator Functions
 * ============================================================ */

streaming_validator_t *streaming_validator_new(tool_registry_t *registry) {
    streaming_validator_t *v = calloc(1, sizeof(streaming_validator_t));
    if (!v) return NULL;

    v->registry = registry;

    /* Initialize parser state */
    v->buffer = malloc(256);
    v->buffer_len = 0;
    v->buffer_cap = 256;
    v->state = VALIDATOR_WAITING;
    v->depth = 0;
    v->in_string = false;
    v->escape_next = false;
    v->current_key = NULL;
    v->current_val = malloc(256);
    v->current_val_len = 0;
    v->current_val_cap = 256;
    v->has_key = false;

    /* Initialize parsed data */
    v->tool_name = NULL;
    v->fields = NULL;
    v->fields_count = 0;
    v->fields_capacity = 0;
    v->errors = NULL;
    v->errors_count = 0;
    v->errors_capacity = 0;

    /* Initialize timing */
    v->token_count = 0;
    v->char_count = 0;
    v->start_time = 0;
    v->tool_detected_at_token = 0;
    v->tool_detected_at_time = 0;
    v->first_error_at_token = 0;
    v->first_error_at_time = 0;
    v->complete_at_token = 0;
    v->complete_at_time = 0;

    /* Initialize timeline */
    v->timeline = NULL;
    v->timeline_count = 0;
    v->timeline_capacity = 0;

    return v;
}

void streaming_validator_free(streaming_validator_t *v) {
    if (!v) return;

    free(v->buffer);
    free(v->current_key);
    free(v->current_val);
    free(v->tool_name);

    for (size_t i = 0; i < v->fields_count; i++) {
        free(v->fields[i].key);
        if (v->fields[i].value.type == VFIELD_STR) {
            free(v->fields[i].value.str_val);
        }
    }
    free(v->fields);

    for (size_t i = 0; i < v->errors_count; i++) {
        free(v->errors[i].message);
        free(v->errors[i].field);
    }
    free(v->errors);

    for (size_t i = 0; i < v->timeline_count; i++) {
        free(v->timeline[i].event);
        free(v->timeline[i].detail);
    }
    free(v->timeline);

    free(v);
}

void streaming_validator_reset(streaming_validator_t *v) {
    if (!v) return;

    /* Reset buffer */
    v->buffer_len = 0;
    v->state = VALIDATOR_WAITING;
    v->depth = 0;
    v->in_string = false;
    v->escape_next = false;
    free(v->current_key);
    v->current_key = NULL;
    v->current_val_len = 0;
    v->has_key = false;

    /* Reset parsed data */
    free(v->tool_name);
    v->tool_name = NULL;

    for (size_t i = 0; i < v->fields_count; i++) {
        free(v->fields[i].key);
        if (v->fields[i].value.type == VFIELD_STR) {
            free(v->fields[i].value.str_val);
        }
    }
    v->fields_count = 0;

    for (size_t i = 0; i < v->errors_count; i++) {
        free(v->errors[i].message);
        free(v->errors[i].field);
    }
    v->errors_count = 0;

    /* Reset timing */
    v->token_count = 0;
    v->char_count = 0;
    v->start_time = 0;
    v->tool_detected_at_token = 0;
    v->tool_detected_at_time = 0;
    v->first_error_at_token = 0;
    v->first_error_at_time = 0;
    v->complete_at_token = 0;
    v->complete_at_time = 0;

    /* Reset timeline */
    for (size_t i = 0; i < v->timeline_count; i++) {
        free(v->timeline[i].event);
        free(v->timeline[i].detail);
    }
    v->timeline_count = 0;
}

void streaming_validator_start(streaming_validator_t *v) {
    if (v) v->start_time = current_time_ms();
}

/* Add error to validator */
static void add_error(streaming_validator_t *v, validation_error_code_t code,
                      const char *message, const char *field) {
    if (v->errors_count >= v->errors_capacity) {
        size_t new_cap = v->errors_capacity == 0 ? 8 : v->errors_capacity * 2;
        validation_error_t *new_errors = realloc(v->errors, new_cap * sizeof(validation_error_t));
        if (!new_errors) return;
        v->errors = new_errors;
        v->errors_capacity = new_cap;
    }

    v->errors[v->errors_count].code = code;
    v->errors[v->errors_count].message = strdup_safe(message);
    v->errors[v->errors_count].field = strdup_safe(field);
    v->errors_count++;
}

/* Add timeline event */
static void add_timeline_event(streaming_validator_t *v, const char *event,
                                size_t token, size_t char_pos, uint64_t elapsed,
                                const char *detail) {
    if (v->timeline_count >= v->timeline_capacity) {
        size_t new_cap = v->timeline_capacity == 0 ? 8 : v->timeline_capacity * 2;
        timeline_event_t *new_timeline = realloc(v->timeline, new_cap * sizeof(timeline_event_t));
        if (!new_timeline) return;
        v->timeline = new_timeline;
        v->timeline_capacity = new_cap;
    }

    v->timeline[v->timeline_count].event = strdup_safe(event);
    v->timeline[v->timeline_count].token = token;
    v->timeline[v->timeline_count].char_pos = char_pos;
    v->timeline[v->timeline_count].elapsed = elapsed;
    v->timeline[v->timeline_count].detail = strdup_safe(detail);
    v->timeline_count++;
}

/* Parse a value string */
static validator_field_value_t parse_value(const char *s, size_t len) {
    validator_field_value_t v = {VFIELD_NULL, {0}};

    if (len == 0 || (len == 1 && s[0] == '_')) {
        return v;
    }

    /* Check for boolean */
    if ((len == 1 && s[0] == 't') || (len == 4 && strncmp(s, "true", 4) == 0)) {
        v.type = VFIELD_BOOL;
        v.bool_val = true;
        return v;
    }
    if ((len == 1 && s[0] == 'f') || (len == 5 && strncmp(s, "false", 5) == 0)) {
        v.type = VFIELD_BOOL;
        v.bool_val = false;
        return v;
    }

    /* Check for null */
    if (len == 4 && strncmp(s, "null", 4) == 0) {
        return v;
    }

    /* Check for integer */
    char *end;
    char buf[64];
    if (len < sizeof(buf)) {
        memcpy(buf, s, len);
        buf[len] = '\0';

        long long num = strtoll(buf, &end, 10);
        if (*end == '\0' || isspace(*end)) {
            v.type = VFIELD_INT;
            v.int_val = num;
            return v;
        }

        /* Check for float */
        double fnum = strtod(buf, &end);
        if (*end == '\0' || isspace(*end)) {
            v.type = VFIELD_FLOAT;
            v.float_val = fnum;
            return v;
        }
    }

    /* Default to string */
    v.type = VFIELD_STR;
    v.str_val = malloc(len + 1);
    if (v.str_val) {
        memcpy(v.str_val, s, len);
        v.str_val[len] = '\0';
    }
    return v;
}

/* Add parsed field */
static void add_field(streaming_validator_t *v, const char *key, validator_field_value_t value) {
    if (v->fields_count >= v->fields_capacity) {
        size_t new_cap = v->fields_capacity == 0 ? 16 : v->fields_capacity * 2;
        parsed_field_t *new_fields = realloc(v->fields, new_cap * sizeof(parsed_field_t));
        if (!new_fields) return;
        v->fields = new_fields;
        v->fields_capacity = new_cap;
    }

    v->fields[v->fields_count].key = strdup_safe(key);
    v->fields[v->fields_count].value = value;
    v->fields_count++;
}

/* Validate field against schema */
static void validate_field(streaming_validator_t *v, const char *key, validator_field_value_t value) {
    if (!v->tool_name) return;
    if (strcmp(key, "action") == 0 || strcmp(key, "tool") == 0) return;

    const tool_schema_t *tool = tool_registry_get(v->registry, v->tool_name);
    if (!tool) return;

    /* Find argument schema */
    const arg_schema_t *arg = NULL;
    for (size_t i = 0; i < tool->args_count; i++) {
        if (strcmp(tool->args[i].name, key) == 0) {
            arg = &tool->args[i];
            break;
        }
    }
    if (!arg) return;

    /* Numeric constraints */
    if (value.type == VFIELD_INT) {
        if (arg->min != INT64_MIN && value.int_val < arg->min) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s < %ld", key, (long)arg->min);
            add_error(v, VERR_CONSTRAINT_MIN, buf, key);
        }
        if (arg->max != INT64_MAX && value.int_val > arg->max) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s > %ld", key, (long)arg->max);
            add_error(v, VERR_CONSTRAINT_MAX, buf, key);
        }
    }

    /* String constraints */
    if (value.type == VFIELD_STR && value.str_val) {
        size_t len = strlen(value.str_val);
        if (arg->min_len > 0 && len < arg->min_len) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s length < %zu", key, arg->min_len);
            add_error(v, VERR_CONSTRAINT_LEN, buf, key);
        }
        if (arg->max_len != SIZE_MAX && len > arg->max_len) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s length > %zu", key, arg->max_len);
            add_error(v, VERR_CONSTRAINT_LEN, buf, key);
        }
        /* Note: Pattern matching would require regex library */
        if (arg->enum_values) {
            bool found = false;
            for (size_t i = 0; i < arg->enum_count; i++) {
                if (strcmp(value.str_val, arg->enum_values[i]) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                char buf[128];
                snprintf(buf, sizeof(buf), "%s not in allowed values", key);
                add_error(v, VERR_CONSTRAINT_ENUM, buf, key);
            }
        }
    }
}

/* Finish parsing a field */
static void finish_field(streaming_validator_t *v) {
    if (!v->has_key) return;

    /* Trim whitespace from value */
    while (v->current_val_len > 0 && isspace(v->current_val[v->current_val_len - 1])) {
        v->current_val_len--;
    }
    v->current_val[v->current_val_len] = '\0';

    validator_field_value_t value = parse_value(v->current_val, v->current_val_len);

    /* Check for tool/action field */
    if (strcmp(v->current_key, "action") == 0 || strcmp(v->current_key, "tool") == 0) {
        if (value.type == VFIELD_STR && value.str_val) {
            v->tool_name = strdup_safe(value.str_val);

            /* Validate against allow list */
            if (!tool_registry_is_allowed(v->registry, value.str_val)) {
                char buf[128];
                snprintf(buf, sizeof(buf), "Unknown tool: %s", value.str_val);
                add_error(v, VERR_UNKNOWN_TOOL, buf, v->current_key);
            }
        }
    }

    /* Validate field constraints */
    if (v->tool_name) {
        validate_field(v, v->current_key, value);
    }

    add_field(v, v->current_key, value);

    free(v->current_key);
    v->current_key = NULL;
    v->current_val_len = 0;
    v->has_key = false;
}

/* Validate on completion */
static void validate_complete(streaming_validator_t *v) {
    if (!v->tool_name) {
        add_error(v, VERR_MISSING_TOOL, "No action field found", NULL);
        return;
    }

    const tool_schema_t *tool = tool_registry_get(v->registry, v->tool_name);
    if (!tool) return;

    /* Check required fields */
    for (size_t i = 0; i < tool->args_count; i++) {
        if (tool->args[i].required) {
            bool found = false;
            for (size_t j = 0; j < v->fields_count; j++) {
                if (strcmp(v->fields[j].key, tool->args[i].name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                char buf[128];
                snprintf(buf, sizeof(buf), "Missing required field: %s", tool->args[i].name);
                add_error(v, VERR_MISSING_REQUIRED, buf, tool->args[i].name);
            }
        }
    }
}

/* Process a single character */
static void process_char(streaming_validator_t *v, char c) {
    /* Grow buffer if needed */
    if (v->buffer_len >= v->buffer_cap - 1) {
        size_t new_cap = v->buffer_cap * 2;
        char *new_buf = realloc(v->buffer, new_cap);
        if (new_buf) {
            v->buffer = new_buf;
            v->buffer_cap = new_cap;
        }
    }
    v->buffer[v->buffer_len++] = c;
    v->buffer[v->buffer_len] = '\0';

    /* Handle escape sequences */
    if (v->escape_next) {
        v->escape_next = false;
        if (v->current_val_len < v->current_val_cap - 1) {
            v->current_val[v->current_val_len++] = c;
        }
        return;
    }

    if (c == '\\' && v->in_string) {
        v->escape_next = true;
        if (v->current_val_len < v->current_val_cap - 1) {
            v->current_val[v->current_val_len++] = c;
        }
        return;
    }

    /* Handle quotes */
    if (c == '"') {
        if (v->in_string) {
            v->in_string = false;
        } else {
            v->in_string = true;
            v->current_val_len = 0;
        }
        return;
    }

    /* Inside string - accumulate */
    if (v->in_string) {
        if (v->current_val_len < v->current_val_cap - 1) {
            v->current_val[v->current_val_len++] = c;
        }
        return;
    }

    /* Handle structural characters */
    switch (c) {
        case '{':
            if (v->state == VALIDATOR_WAITING) {
                v->state = VALIDATOR_IN_OBJECT;
            }
            v->depth++;
            break;

        case '}':
            v->depth--;
            if (v->depth == 0) {
                finish_field(v);
                v->state = VALIDATOR_COMPLETE;
                validate_complete(v);
            }
            break;

        case '[':
            v->depth++;
            if (v->current_val_len < v->current_val_cap - 1) {
                v->current_val[v->current_val_len++] = c;
            }
            break;

        case ']':
            v->depth--;
            if (v->current_val_len < v->current_val_cap - 1) {
                v->current_val[v->current_val_len++] = c;
            }
            break;

        case '=':
            if (v->depth == 1 && !v->has_key) {
                /* Finish key */
                v->current_val[v->current_val_len] = '\0';
                /* Trim whitespace */
                while (v->current_val_len > 0 && isspace(v->current_val[v->current_val_len - 1])) {
                    v->current_val[--v->current_val_len] = '\0';
                }
                char *start = v->current_val;
                while (*start && isspace(*start)) start++;
                v->current_key = strdup_safe(start);
                v->current_val_len = 0;
                v->has_key = true;
            } else {
                if (v->current_val_len < v->current_val_cap - 1) {
                    v->current_val[v->current_val_len++] = c;
                }
            }
            break;

        case ' ':
        case '\n':
        case '\t':
        case '\r':
            if (v->depth == 1 && v->has_key && v->current_val_len > 0) {
                finish_field(v);
            }
            break;

        default:
            if (v->current_val_len < v->current_val_cap - 1) {
                v->current_val[v->current_val_len++] = c;
            }
    }
}

validation_result_t *streaming_validator_push_token(streaming_validator_t *v, const char *token) {
    if (!v || !token) return NULL;

    if (v->start_time == 0) {
        streaming_validator_start(v);
    }

    v->token_count++;

    for (const char *c = token; *c; c++) {
        v->char_count++;
        process_char(v, *c);
    }

    uint64_t elapsed = current_time_ms() - v->start_time;

    /* Record tool detection */
    if (v->tool_name && v->tool_detected_at_token == 0) {
        v->tool_detected_at_token = v->token_count;
        v->tool_detected_at_time = elapsed;

        bool allowed = tool_registry_is_allowed(v->registry, v->tool_name);
        char detail[256];
        snprintf(detail, sizeof(detail), "tool=%s allowed=%s", v->tool_name, allowed ? "true" : "false");
        add_timeline_event(v, "TOOL_DETECTED", v->token_count, v->char_count, elapsed, detail);
    }

    /* Record first error */
    if (v->errors_count > 0 && v->first_error_at_token == 0) {
        v->first_error_at_token = v->token_count;
        v->first_error_at_time = elapsed;

        add_timeline_event(v, "ERROR", v->token_count, v->char_count, elapsed, v->errors[0].message);
    }

    /* Record completion */
    if (v->state == VALIDATOR_COMPLETE && v->complete_at_token == 0) {
        v->complete_at_token = v->token_count;
        v->complete_at_time = elapsed;

        char detail[64];
        snprintf(detail, sizeof(detail), "valid=%s", v->errors_count == 0 ? "true" : "false");
        add_timeline_event(v, "COMPLETE", v->token_count, v->char_count, elapsed, detail);
    }

    return streaming_validator_get_result(v);
}

validation_result_t *streaming_validator_get_result(const streaming_validator_t *v) {
    if (!v) return NULL;

    validation_result_t *r = calloc(1, sizeof(validation_result_t));
    if (!r) return NULL;

    r->complete = (v->state == VALIDATOR_COMPLETE);
    r->valid = (v->errors_count == 0);
    r->tool_name = strdup_safe(v->tool_name);
    r->tool_allowed = v->tool_name ? tool_registry_is_allowed(v->registry, v->tool_name) : false;
    r->tool_allowed_set = (v->tool_name != NULL);

    /* Copy errors */
    r->errors_count = v->errors_count;
    if (v->errors_count > 0) {
        r->errors = calloc(v->errors_count, sizeof(validation_error_t));
        for (size_t i = 0; i < v->errors_count; i++) {
            r->errors[i].code = v->errors[i].code;
            r->errors[i].message = strdup_safe(v->errors[i].message);
            r->errors[i].field = strdup_safe(v->errors[i].field);
        }
    }

    /* Copy fields */
    r->fields_count = v->fields_count;
    if (v->fields_count > 0) {
        r->fields = calloc(v->fields_count, sizeof(parsed_field_t));
        for (size_t i = 0; i < v->fields_count; i++) {
            r->fields[i].key = strdup_safe(v->fields[i].key);
            r->fields[i].value = v->fields[i].value;
            if (v->fields[i].value.type == VFIELD_STR) {
                r->fields[i].value.str_val = strdup_safe(v->fields[i].value.str_val);
            }
        }
    }

    /* Copy timing */
    r->token_count = v->token_count;
    r->char_count = v->char_count;
    r->tool_detected_at_token = v->tool_detected_at_token;
    r->tool_detected_at_time = v->tool_detected_at_time;
    r->first_error_at_token = v->first_error_at_token;
    r->first_error_at_time = v->first_error_at_time;
    r->complete_at_token = v->complete_at_token;
    r->complete_at_time = v->complete_at_time;

    /* Copy timeline */
    r->timeline_count = v->timeline_count;
    if (v->timeline_count > 0) {
        r->timeline = calloc(v->timeline_count, sizeof(timeline_event_t));
        for (size_t i = 0; i < v->timeline_count; i++) {
            r->timeline[i].event = strdup_safe(v->timeline[i].event);
            r->timeline[i].token = v->timeline[i].token;
            r->timeline[i].char_pos = v->timeline[i].char_pos;
            r->timeline[i].elapsed = v->timeline[i].elapsed;
            r->timeline[i].detail = strdup_safe(v->timeline[i].detail);
        }
    }

    return r;
}

bool streaming_validator_should_stop(const streaming_validator_t *v) {
    if (!v) return false;

    for (size_t i = 0; i < v->errors_count; i++) {
        if (v->errors[i].code == VERR_UNKNOWN_TOOL) {
            return true;
        }
    }
    return false;
}

void validation_result_free(validation_result_t *r) {
    if (!r) return;

    free(r->tool_name);

    for (size_t i = 0; i < r->errors_count; i++) {
        free(r->errors[i].message);
        free(r->errors[i].field);
    }
    free(r->errors);

    for (size_t i = 0; i < r->fields_count; i++) {
        free(r->fields[i].key);
        if (r->fields[i].value.type == VFIELD_STR) {
            free(r->fields[i].value.str_val);
        }
    }
    free(r->fields);

    for (size_t i = 0; i < r->timeline_count; i++) {
        free(r->timeline[i].event);
        free(r->timeline[i].detail);
    }
    free(r->timeline);

    free(r);
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

const char *validation_error_code_string(validation_error_code_t code) {
    switch (code) {
        case VERR_UNKNOWN_TOOL: return "UNKNOWN_TOOL";
        case VERR_MISSING_REQUIRED: return "MISSING_REQUIRED";
        case VERR_MISSING_TOOL: return "MISSING_TOOL";
        case VERR_CONSTRAINT_MIN: return "CONSTRAINT_MIN";
        case VERR_CONSTRAINT_MAX: return "CONSTRAINT_MAX";
        case VERR_CONSTRAINT_LEN: return "CONSTRAINT_LEN";
        case VERR_CONSTRAINT_PATTERN: return "CONSTRAINT_PATTERN";
        case VERR_CONSTRAINT_ENUM: return "CONSTRAINT_ENUM";
        case VERR_INVALID_TYPE: return "INVALID_TYPE";
        default: return "UNKNOWN";
    }
}

const char *validator_state_string(validator_state_t state) {
    switch (state) {
        case VALIDATOR_WAITING: return "waiting";
        case VALIDATOR_IN_OBJECT: return "in_object";
        case VALIDATOR_COMPLETE: return "complete";
        case VALIDATOR_ERROR: return "error";
        default: return "unknown";
    }
}
