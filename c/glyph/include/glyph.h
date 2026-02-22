/**
 * GLYPH Codec - Token-efficient serialization for AI agents
 *
 * GLYPH is a serialization format designed for LLM tool calls that provides
 * 30-50% token savings over JSON while remaining human-readable.
 *
 * Example:
 *   glyph_value_t *v = glyph_from_json("{\"action\": \"search\"}");
 *   char *glyph = glyph_canonicalize_loose(v);
 *   // glyph = "{action=search}"
 *   glyph_free(glyph);
 *   glyph_value_free(v);
 */

#ifndef GLYPH_H
#define GLYPH_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Types
 * ============================================================ */

/** GLYPH value type enumeration */
typedef enum {
    GLYPH_NULL = 0,
    GLYPH_BOOL,
    GLYPH_INT,
    GLYPH_FLOAT,
    GLYPH_STR,
    GLYPH_BYTES,
    GLYPH_TIME,
    GLYPH_ID,
    GLYPH_LIST,
    GLYPH_MAP,
    GLYPH_STRUCT,
    GLYPH_SUM,
} glyph_type_t;

/** Null style for canonicalization */
typedef enum {
    GLYPH_NULL_UNDERSCORE = 0, /* _ */
    GLYPH_NULL_SYMBOL,         /* ∅ */
} glyph_null_style_t;

/** Forward declarations */
typedef struct glyph_value glyph_value_t;
typedef struct glyph_map_entry glyph_map_entry_t;
typedef struct glyph_ref_id glyph_ref_id_t;
typedef struct glyph_struct_value glyph_struct_value_t;
typedef struct glyph_sum_value glyph_sum_value_t;

/** Map entry (key-value pair) */
struct glyph_map_entry {
    char *key;
    glyph_value_t *value;
};

/** Reference ID */
struct glyph_ref_id {
    char *prefix;
    char *value;
};

/** Struct value */
struct glyph_struct_value {
    char *type_name;
    glyph_map_entry_t *fields;
    size_t fields_count;
};

/** Sum type value */
struct glyph_sum_value {
    char *tag;
    glyph_value_t *value; /* May be NULL */
};

/** GLYPH value container */
struct glyph_value {
    glyph_type_t type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        char *str_val;
        struct {
            uint8_t *data;
            size_t len;
        } bytes_val;
        int64_t time_val; /* Unix timestamp in milliseconds */
        glyph_ref_id_t id_val;
        struct {
            glyph_value_t **items;
            size_t count;
        } list_val;
        struct {
            glyph_map_entry_t *entries;
            size_t count;
        } map_val;
        glyph_struct_value_t struct_val;
        glyph_sum_value_t sum_val;
    };
};

/** Canonicalization options */
typedef struct {
    bool auto_tabular;
    size_t min_rows;
    size_t max_cols;
    bool allow_missing;
    glyph_null_style_t null_style;
} glyph_canon_opts_t;

/* ============================================================
 * Constructors
 * ============================================================ */

/** Create a null value */
glyph_value_t *glyph_null(void);

/** Create a boolean value */
glyph_value_t *glyph_bool(bool val);

/** Create an integer value */
glyph_value_t *glyph_int(int64_t val);

/** Create a float value */
glyph_value_t *glyph_float(double val);

/** Create a string value (copies the string) */
glyph_value_t *glyph_str(const char *val);

/** Create a bytes value (copies the data) */
glyph_value_t *glyph_bytes(const uint8_t *data, size_t len);

/** Create a reference ID */
glyph_value_t *glyph_id(const char *prefix, const char *value);

/** Create an empty list */
glyph_value_t *glyph_list_new(void);

/** Append to a list (takes ownership of item) */
void glyph_list_append(glyph_value_t *list, glyph_value_t *item);

/** Create an empty map */
glyph_value_t *glyph_map_new(void);

/** Add to a map (copies key, takes ownership of value) */
void glyph_map_set(glyph_value_t *map, const char *key, glyph_value_t *value);

/** Create a struct */
glyph_value_t *glyph_struct_new(const char *type_name);

/** Add a field to a struct */
void glyph_struct_set(glyph_value_t *s, const char *key, glyph_value_t *value);

/** Create a sum type */
glyph_value_t *glyph_sum(const char *tag, glyph_value_t *value);

/* ============================================================
 * Accessors
 * ============================================================ */

/** Get value type */
glyph_type_t glyph_get_type(const glyph_value_t *v);

/** Get boolean value (returns false if not a bool) */
bool glyph_as_bool(const glyph_value_t *v);

/** Get integer value (returns 0 if not an int) */
int64_t glyph_as_int(const glyph_value_t *v);

/** Get float value (returns 0.0 if not a float) */
double glyph_as_float(const glyph_value_t *v);

/** Get string value (returns NULL if not a string) */
const char *glyph_as_str(const glyph_value_t *v);

/** Get list length (returns 0 if not a list) */
size_t glyph_list_len(const glyph_value_t *v);

/** Get list item by index */
glyph_value_t *glyph_list_get(const glyph_value_t *v, size_t index);

/** Get map/struct value by key */
glyph_value_t *glyph_get(const glyph_value_t *v, const char *key);

/* ============================================================
 * Canonicalization
 * ============================================================ */

/** Get default canonicalization options */
glyph_canon_opts_t glyph_canon_opts_default(void);

/** Get LLM-friendly options */
glyph_canon_opts_t glyph_canon_opts_llm(void);

/** Get pretty (unicode) options */
glyph_canon_opts_t glyph_canon_opts_pretty(void);

/** Get no-tabular options */
glyph_canon_opts_t glyph_canon_opts_no_tabular(void);

/** Canonicalize a value with default options. Returns malloc'd string. */
char *glyph_canonicalize_loose(const glyph_value_t *v);

/** Canonicalize without tabular mode. Returns malloc'd string. */
char *glyph_canonicalize_loose_no_tabular(const glyph_value_t *v);

/** Canonicalize with custom options. Returns malloc'd string. */
char *glyph_canonicalize_loose_with_opts(const glyph_value_t *v, const glyph_canon_opts_t *opts);

/** Get fingerprint (same as canonicalize). Returns malloc'd string. */
char *glyph_fingerprint_loose(const glyph_value_t *v);

/** Get SHA-256 hash (first 16 hex chars). Returns malloc'd string. */
char *glyph_hash_loose(const glyph_value_t *v);

/** Check if two values are equal */
bool glyph_equal_loose(const glyph_value_t *a, const glyph_value_t *b);

/* ============================================================
 * JSON Bridge
 * ============================================================ */

/** Parse JSON string to glyph value. Returns NULL on error. */
glyph_value_t *glyph_from_json(const char *json);

/** Convert glyph value to JSON string. Returns malloc'd string. */
char *glyph_to_json(const glyph_value_t *v);

/* ============================================================
 * Memory Management
 * ============================================================ */

/** Free a glyph value and all its children */
void glyph_value_free(glyph_value_t *v);

/** Free a string returned by glyph functions */
void glyph_free(char *s);

#ifdef __cplusplus
}
#endif

#endif /* GLYPH_H */
