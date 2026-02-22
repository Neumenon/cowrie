/**
 * Schema Evolution - Safe API versioning for GLYPH
 *
 * Enables schemas to evolve safely without breaking clients. Supports:
 * - Adding new optional fields
 * - Renaming fields (with compatibility mapping)
 * - Deprecating fields
 * - Changing defaults
 * - Strict vs tolerant parsing modes
 */

#ifndef GLYPH_SCHEMA_EVOLUTION_H
#define GLYPH_SCHEMA_EVOLUTION_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Types
 * ============================================================ */

/** Evolution mode */
typedef enum {
    EVOLUTION_MODE_STRICT = 0,  /* Fail on unknown fields */
    EVOLUTION_MODE_TOLERANT,    /* Ignore unknown fields (default) */
    EVOLUTION_MODE_MIGRATE,     /* Auto-migrate between versions */
} evolution_mode_t;

/** Field type */
typedef enum {
    FIELD_TYPE_STR = 0,
    FIELD_TYPE_INT,
    FIELD_TYPE_FLOAT,
    FIELD_TYPE_BOOL,
    FIELD_TYPE_LIST,
    FIELD_TYPE_DECIMAL,
} field_type_t;

/** Field value (simple tagged union) */
typedef struct {
    enum {
        FIELD_VALUE_NULL = 0,
        FIELD_VALUE_BOOL,
        FIELD_VALUE_INT,
        FIELD_VALUE_FLOAT,
        FIELD_VALUE_STR,
    } type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        char *str_val;
    };
} field_value_t;

/** Evolving field configuration */
typedef struct {
    field_type_t type;
    bool required;
    field_value_t default_value;
    const char *added_in;      /* Version when field was added (e.g., "1.0") */
    const char *deprecated_in; /* Version when field was deprecated (NULL if not) */
    const char *renamed_from;  /* Original field name if renamed (NULL if not) */
    const char *validation;    /* Regex pattern for validation (NULL if none) */
} evolving_field_config_t;

/** Evolving field */
typedef struct evolving_field {
    char *name;
    field_type_t type;
    bool required;
    field_value_t default_value;
    char *added_in;
    char *deprecated_in;
    char *renamed_from;
    char *validation;
} evolving_field_t;

/** Version schema (single version) */
typedef struct {
    char *name;
    char *version;
    char *description;
    evolving_field_t *fields;
    size_t fields_count;
    size_t fields_capacity;
} version_schema_t;

/** Versioned schema (multiple versions) */
typedef struct {
    char *name;
    version_schema_t *versions;
    size_t versions_count;
    size_t versions_capacity;
    char *latest_version;
    evolution_mode_t mode;
} versioned_schema_t;

/** Parse result */
typedef struct {
    char *error;  /* NULL on success */
    field_value_t *data;
    char **keys;
    size_t data_count;
} evolution_parse_result_t;

/** Emit result */
typedef struct {
    char *error;  /* NULL on success */
    char *header;
} evolution_emit_result_t;

/** Changelog entry */
typedef struct {
    char *version;
    char *description;
    char **added_fields;
    size_t added_count;
    char **deprecated_fields;
    size_t deprecated_count;
    char **renamed_from;
    char **renamed_to;
    size_t renamed_count;
} changelog_entry_t;

/* ============================================================
 * Field Value Functions
 * ============================================================ */

/** Create a null field value */
field_value_t field_value_null(void);

/** Create a bool field value */
field_value_t field_value_bool(bool val);

/** Create an int field value */
field_value_t field_value_int(int64_t val);

/** Create a float field value */
field_value_t field_value_float(double val);

/** Create a string field value (copies the string) */
field_value_t field_value_str(const char *val);

/** Free a field value (only frees string if type is STR) */
void field_value_free(field_value_t *v);

/* ============================================================
 * Evolving Field Functions
 * ============================================================ */

/** Create an evolving field */
evolving_field_t *evolving_field_new(const char *name, const evolving_field_config_t *config);

/** Free an evolving field */
void evolving_field_free(evolving_field_t *f);

/** Check if field is available in a given version */
bool evolving_field_is_available_in(const evolving_field_t *f, const char *version);

/** Check if field is deprecated in a given version */
bool evolving_field_is_deprecated_in(const evolving_field_t *f, const char *version);

/** Validate a value against this field (returns NULL on success, error message on failure) */
char *evolving_field_validate(const evolving_field_t *f, const field_value_t *value);

/* ============================================================
 * Version Schema Functions
 * ============================================================ */

/** Create a version schema */
version_schema_t *version_schema_new(const char *name, const char *version);

/** Free a version schema */
void version_schema_free(version_schema_t *s);

/** Add a field to the schema */
void version_schema_add_field(version_schema_t *s, evolving_field_t *field);

/** Get a field by name (returns NULL if not found) */
const evolving_field_t *version_schema_get_field(const version_schema_t *s, const char *name);

/** Validate data against this schema (returns NULL on success, error message on failure) */
char *version_schema_validate(const version_schema_t *s,
                              const field_value_t *data,
                              const char **keys,
                              size_t count);

/* ============================================================
 * Versioned Schema Functions
 * ============================================================ */

/** Create a versioned schema */
versioned_schema_t *versioned_schema_new(const char *name);

/** Free a versioned schema */
void versioned_schema_free(versioned_schema_t *s);

/** Set evolution mode (returns self for chaining) */
versioned_schema_t *versioned_schema_with_mode(versioned_schema_t *s, evolution_mode_t mode);

/** Add a version with fields */
void versioned_schema_add_version(versioned_schema_t *s,
                                  const char *version,
                                  const evolving_field_config_t *fields,
                                  const char **field_names,
                                  size_t field_count);

/** Get schema for a specific version (returns NULL if not found) */
const version_schema_t *versioned_schema_get_version(const versioned_schema_t *s, const char *version);

/** Parse data from a specific version */
evolution_parse_result_t versioned_schema_parse(const versioned_schema_t *s,
                                                const field_value_t *data,
                                                const char **keys,
                                                size_t count,
                                                const char *from_version);

/** Emit version header for data */
evolution_emit_result_t versioned_schema_emit(const versioned_schema_t *s,
                                              const field_value_t *data,
                                              const char **keys,
                                              size_t count,
                                              const char *version);

/** Get changelog of schema evolution. Returns array of entries. Caller must free. */
changelog_entry_t *versioned_schema_get_changelog(const versioned_schema_t *s, size_t *count);

/** Free a changelog array */
void changelog_free(changelog_entry_t *entries, size_t count);

/** Free a parse result */
void evolution_parse_result_free(evolution_parse_result_t *r);

/** Free an emit result */
void evolution_emit_result_free(evolution_emit_result_t *r);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/**
 * Compare two version strings.
 * Returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
 */
int compare_versions(const char *v1, const char *v2);

/**
 * Parse a version header (e.g., "@version 2.0").
 * Returns malloc'd version string, or NULL if not a valid header.
 */
char *parse_version_header(const char *text);

/**
 * Format a version header.
 * Returns malloc'd string.
 */
char *format_version_header(const char *version);

#ifdef __cplusplus
}
#endif

#endif /* GLYPH_SCHEMA_EVOLUTION_H */
