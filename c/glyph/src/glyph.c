/**
 * GLYPH Codec - C Implementation
 */

#include "glyph.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ctype.h>
#include <time.h>

/* ============================================================
 * Internal helpers
 * ============================================================ */

static char *strdup_safe(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (copy) {
        memcpy(copy, s, len + 1);
    }
    return copy;
}

/* Dynamic string buffer */
typedef struct {
    char *data;
    size_t len;
    size_t cap;
} strbuf_t;

static void strbuf_init(strbuf_t *buf) {
    buf->data = malloc(256);
    buf->len = 0;
    buf->cap = 256;
    if (buf->data) buf->data[0] = '\0';
}

static void strbuf_grow(strbuf_t *buf, size_t need) {
    if (buf->len + need >= buf->cap) {
        size_t new_cap = buf->cap * 2;
        while (buf->len + need >= new_cap) new_cap *= 2;
        char *new_data = realloc(buf->data, new_cap);
        if (new_data) {
            buf->data = new_data;
            buf->cap = new_cap;
        }
    }
}

static void strbuf_append(strbuf_t *buf, const char *s) {
    size_t len = strlen(s);
    strbuf_grow(buf, len + 1);
    memcpy(buf->data + buf->len, s, len + 1);
    buf->len += len;
}

static void strbuf_append_char(strbuf_t *buf, char c) {
    strbuf_grow(buf, 2);
    buf->data[buf->len++] = c;
    buf->data[buf->len] = '\0';
}

static char *strbuf_finish(strbuf_t *buf) {
    return buf->data;
}

/* ============================================================
 * Constructors
 * ============================================================ */

glyph_value_t *glyph_null(void) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) v->type = GLYPH_NULL;
    return v;
}

glyph_value_t *glyph_bool(bool val) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_BOOL;
        v->bool_val = val;
    }
    return v;
}

glyph_value_t *glyph_int(int64_t val) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_INT;
        v->int_val = val;
    }
    return v;
}

glyph_value_t *glyph_float(double val) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_FLOAT;
        v->float_val = val;
    }
    return v;
}

glyph_value_t *glyph_str(const char *val) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_STR;
        v->str_val = strdup_safe(val);
    }
    return v;
}

glyph_value_t *glyph_bytes(const uint8_t *data, size_t len) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_BYTES;
        v->bytes_val.data = malloc(len);
        if (v->bytes_val.data) {
            memcpy(v->bytes_val.data, data, len);
            v->bytes_val.len = len;
        }
    }
    return v;
}

glyph_value_t *glyph_id(const char *prefix, const char *value) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_ID;
        v->id_val.prefix = strdup_safe(prefix ? prefix : "");
        v->id_val.value = strdup_safe(value);
    }
    return v;
}

glyph_value_t *glyph_list_new(void) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_LIST;
        v->list_val.items = NULL;
        v->list_val.count = 0;
    }
    return v;
}

void glyph_list_append(glyph_value_t *list, glyph_value_t *item) {
    if (!list || list->type != GLYPH_LIST || !item) return;

    size_t new_count = list->list_val.count + 1;
    glyph_value_t **new_items = realloc(list->list_val.items,
                                        new_count * sizeof(glyph_value_t *));
    if (new_items) {
        new_items[list->list_val.count] = item;
        list->list_val.items = new_items;
        list->list_val.count = new_count;
    }
}

glyph_value_t *glyph_map_new(void) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_MAP;
        v->map_val.entries = NULL;
        v->map_val.count = 0;
    }
    return v;
}

void glyph_map_set(glyph_value_t *map, const char *key, glyph_value_t *value) {
    if (!map || map->type != GLYPH_MAP || !key || !value) return;

    size_t new_count = map->map_val.count + 1;
    glyph_map_entry_t *new_entries = realloc(map->map_val.entries,
                                              new_count * sizeof(glyph_map_entry_t));
    if (new_entries) {
        new_entries[map->map_val.count].key = strdup_safe(key);
        new_entries[map->map_val.count].value = value;
        map->map_val.entries = new_entries;
        map->map_val.count = new_count;
    }
}

glyph_value_t *glyph_struct_new(const char *type_name) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_STRUCT;
        v->struct_val.type_name = strdup_safe(type_name);
        v->struct_val.fields = NULL;
        v->struct_val.fields_count = 0;
    }
    return v;
}

void glyph_struct_set(glyph_value_t *s, const char *key, glyph_value_t *value) {
    if (!s || s->type != GLYPH_STRUCT || !key || !value) return;

    size_t new_count = s->struct_val.fields_count + 1;
    glyph_map_entry_t *new_fields = realloc(s->struct_val.fields,
                                             new_count * sizeof(glyph_map_entry_t));
    if (new_fields) {
        new_fields[s->struct_val.fields_count].key = strdup_safe(key);
        new_fields[s->struct_val.fields_count].value = value;
        s->struct_val.fields = new_fields;
        s->struct_val.fields_count = new_count;
    }
}

glyph_value_t *glyph_sum(const char *tag, glyph_value_t *value) {
    glyph_value_t *v = calloc(1, sizeof(glyph_value_t));
    if (v) {
        v->type = GLYPH_SUM;
        v->sum_val.tag = strdup_safe(tag);
        v->sum_val.value = value;
    }
    return v;
}

/* ============================================================
 * Accessors
 * ============================================================ */

glyph_type_t glyph_get_type(const glyph_value_t *v) {
    return v ? v->type : GLYPH_NULL;
}

bool glyph_as_bool(const glyph_value_t *v) {
    return v && v->type == GLYPH_BOOL ? v->bool_val : false;
}

int64_t glyph_as_int(const glyph_value_t *v) {
    return v && v->type == GLYPH_INT ? v->int_val : 0;
}

double glyph_as_float(const glyph_value_t *v) {
    return v && v->type == GLYPH_FLOAT ? v->float_val : 0.0;
}

const char *glyph_as_str(const glyph_value_t *v) {
    return v && v->type == GLYPH_STR ? v->str_val : NULL;
}

size_t glyph_list_len(const glyph_value_t *v) {
    return v && v->type == GLYPH_LIST ? v->list_val.count : 0;
}

glyph_value_t *glyph_list_get(const glyph_value_t *v, size_t index) {
    if (!v || v->type != GLYPH_LIST || index >= v->list_val.count) return NULL;
    return v->list_val.items[index];
}

glyph_value_t *glyph_get(const glyph_value_t *v, const char *key) {
    if (!v || !key) return NULL;

    if (v->type == GLYPH_MAP) {
        for (size_t i = 0; i < v->map_val.count; i++) {
            if (strcmp(v->map_val.entries[i].key, key) == 0) {
                return v->map_val.entries[i].value;
            }
        }
    } else if (v->type == GLYPH_STRUCT) {
        for (size_t i = 0; i < v->struct_val.fields_count; i++) {
            if (strcmp(v->struct_val.fields[i].key, key) == 0) {
                return v->struct_val.fields[i].value;
            }
        }
    }
    return NULL;
}

/* ============================================================
 * Canonicalization Options
 * ============================================================ */

glyph_canon_opts_t glyph_canon_opts_default(void) {
    return (glyph_canon_opts_t){
        .auto_tabular = true,
        .min_rows = 3,
        .max_cols = 64,
        .allow_missing = true,
        .null_style = GLYPH_NULL_UNDERSCORE,
    };
}

glyph_canon_opts_t glyph_canon_opts_llm(void) {
    return glyph_canon_opts_default();
}

glyph_canon_opts_t glyph_canon_opts_pretty(void) {
    glyph_canon_opts_t opts = glyph_canon_opts_default();
    opts.null_style = GLYPH_NULL_SYMBOL;
    return opts;
}

glyph_canon_opts_t glyph_canon_opts_no_tabular(void) {
    glyph_canon_opts_t opts = glyph_canon_opts_default();
    opts.auto_tabular = false;
    return opts;
}

/* ============================================================
 * Canonicalization Helpers
 * ============================================================ */

static const char *canon_null(glyph_null_style_t style) {
    return style == GLYPH_NULL_SYMBOL ? "∅" : "_";
}

static bool is_bare_safe(const char *s) {
    if (!s || !*s) return false;

    /* Must not start with digit, quote, or dash */
    char first = s[0];
    if (isdigit((unsigned char)first) || first == '"' || first == '\'' || first == '-') {
        return false;
    }

    /* Reserved words */
    if (strcmp(s, "t") == 0 || strcmp(s, "f") == 0 ||
        strcmp(s, "true") == 0 || strcmp(s, "false") == 0 ||
        strcmp(s, "null") == 0 || strcmp(s, "_") == 0) {
        return false;
    }

    /* Must contain only safe characters */
    for (const char *p = s; *p; p++) {
        unsigned char c = *p;
        if (!(isalnum(c) || c == '_' || c == '-' || c == '.' ||
              c == '/' || c == '@' || c == ':' || c > 127)) {
            return false;
        }
    }
    return true;
}

static bool is_ref_bare_safe(const char *s) {
    if (!s || !*s) return false;
    for (const char *p = s; *p; p++) {
        unsigned char c = *p;
        if (!(isalnum(c) || c == '_' || c == '-' || c == '.' || c > 127)) {
            return false;
        }
    }
    return true;
}

static void write_quoted_string(strbuf_t *buf, const char *s) {
    strbuf_append_char(buf, '"');
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '\\': strbuf_append(buf, "\\\\"); break;
            case '"':  strbuf_append(buf, "\\\""); break;
            case '\n': strbuf_append(buf, "\\n"); break;
            case '\r': strbuf_append(buf, "\\r"); break;
            case '\t': strbuf_append(buf, "\\t"); break;
            default:
                if ((unsigned char)*p < 0x20) {
                    char hex[8];
                    snprintf(hex, sizeof(hex), "\\u%04x", (unsigned char)*p);
                    strbuf_append(buf, hex);
                } else {
                    strbuf_append_char(buf, *p);
                }
        }
    }
    strbuf_append_char(buf, '"');
}

static void write_canon_string(strbuf_t *buf, const char *s) {
    if (is_bare_safe(s)) {
        strbuf_append(buf, s);
    } else {
        write_quoted_string(buf, s);
    }
}

/* Forward declaration */
static void write_canon_value(strbuf_t *buf, const glyph_value_t *v,
                              const glyph_canon_opts_t *opts);

/* Compare entries by canonical key for sorting */
static int compare_entries(const void *a, const void *b) {
    const glyph_map_entry_t *ea = a;
    const glyph_map_entry_t *eb = b;
    return strcmp(ea->key, eb->key);
}

static void write_canon_map(strbuf_t *buf, const glyph_map_entry_t *entries,
                            size_t count, const glyph_canon_opts_t *opts) {
    strbuf_append_char(buf, '{');

    /* Sort entries by key */
    glyph_map_entry_t *sorted = NULL;
    if (count > 0) {
        sorted = malloc(count * sizeof(glyph_map_entry_t));
        if (sorted) {
            memcpy(sorted, entries, count * sizeof(glyph_map_entry_t));
            qsort(sorted, count, sizeof(glyph_map_entry_t), compare_entries);
        }
    }

    const glyph_map_entry_t *use_entries = sorted ? sorted : entries;
    for (size_t i = 0; i < count; i++) {
        if (i > 0) strbuf_append_char(buf, ' ');
        write_canon_string(buf, use_entries[i].key);
        strbuf_append_char(buf, '=');
        write_canon_value(buf, use_entries[i].value, opts);
    }

    free(sorted);
    strbuf_append_char(buf, '}');
}

/* Check if items have at least 50% common keys */
static bool check_homogeneous(glyph_value_t **items, size_t count,
                              char ***out_cols, size_t *out_col_count,
                              const glyph_canon_opts_t *opts) {
    if (count < opts->min_rows) return false;

    /* Collect all keys */
    size_t all_keys_cap = 64;
    char **all_keys = malloc(all_keys_cap * sizeof(char *));
    size_t all_keys_count = 0;

    for (size_t i = 0; i < count; i++) {
        glyph_value_t *item = items[i];
        glyph_map_entry_t *entries = NULL;
        size_t entry_count = 0;

        if (item->type == GLYPH_MAP) {
            entries = item->map_val.entries;
            entry_count = item->map_val.count;
        } else if (item->type == GLYPH_STRUCT) {
            entries = item->struct_val.fields;
            entry_count = item->struct_val.fields_count;
        } else {
            free(all_keys);
            return false;
        }

        for (size_t j = 0; j < entry_count; j++) {
            const char *key = entries[j].key;
            bool found = false;
            for (size_t k = 0; k < all_keys_count; k++) {
                if (strcmp(all_keys[k], key) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (all_keys_count >= all_keys_cap) {
                    all_keys_cap *= 2;
                    all_keys = realloc(all_keys, all_keys_cap * sizeof(char *));
                }
                all_keys[all_keys_count++] = strdup_safe(key);
            }
        }
    }

    /* Don't use tabular for empty objects or too many columns */
    if (all_keys_count == 0 || all_keys_count > opts->max_cols) {
        for (size_t i = 0; i < all_keys_count; i++) free(all_keys[i]);
        free(all_keys);
        return false;
    }

    /* Find common keys */
    size_t common_count = 0;
    for (size_t k = 0; k < all_keys_count; k++) {
        bool in_all = true;
        for (size_t i = 0; i < count && in_all; i++) {
            glyph_value_t *item = items[i];
            glyph_map_entry_t *entries = NULL;
            size_t entry_count = 0;

            if (item->type == GLYPH_MAP) {
                entries = item->map_val.entries;
                entry_count = item->map_val.count;
            } else if (item->type == GLYPH_STRUCT) {
                entries = item->struct_val.fields;
                entry_count = item->struct_val.fields_count;
            }

            bool found = false;
            for (size_t j = 0; j < entry_count; j++) {
                if (strcmp(entries[j].key, all_keys[k]) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) in_all = false;
        }
        if (in_all) common_count++;
    }

    /* Check 50% threshold */
    if (common_count * 2 < all_keys_count) {
        for (size_t i = 0; i < all_keys_count; i++) free(all_keys[i]);
        free(all_keys);
        return false;
    }

    /* Sort columns */
    qsort(all_keys, all_keys_count, sizeof(char *),
          (int (*)(const void *, const void *))strcmp);

    *out_cols = all_keys;
    *out_col_count = all_keys_count;
    return true;
}

static void write_tabular(strbuf_t *buf, glyph_value_t **items, size_t count,
                          char **cols, size_t col_count,
                          const glyph_canon_opts_t *opts) {
    /* Header */
    char header[256];
    snprintf(header, sizeof(header), "@tab _ rows=%zu cols=%zu [", count, col_count);
    strbuf_append(buf, header);

    for (size_t i = 0; i < col_count; i++) {
        if (i > 0) strbuf_append_char(buf, ' ');
        write_canon_string(buf, cols[i]);
    }
    strbuf_append(buf, "]\n");

    /* Rows */
    for (size_t i = 0; i < count; i++) {
        strbuf_append_char(buf, '|');
        glyph_value_t *item = items[i];
        glyph_map_entry_t *entries = NULL;
        size_t entry_count = 0;

        if (item->type == GLYPH_MAP) {
            entries = item->map_val.entries;
            entry_count = item->map_val.count;
        } else if (item->type == GLYPH_STRUCT) {
            entries = item->struct_val.fields;
            entry_count = item->struct_val.fields_count;
        }

        for (size_t c = 0; c < col_count; c++) {
            glyph_value_t *cell_val = NULL;
            for (size_t j = 0; j < entry_count; j++) {
                if (strcmp(entries[j].key, cols[c]) == 0) {
                    cell_val = entries[j].value;
                    break;
                }
            }
            if (cell_val) {
                write_canon_value(buf, cell_val, opts);
            } else {
                strbuf_append(buf, canon_null(opts->null_style));
            }
            strbuf_append_char(buf, '|');
        }
        strbuf_append_char(buf, '\n');
    }
    strbuf_append(buf, "@end");
}

static void write_canon_list(strbuf_t *buf, glyph_value_t **items, size_t count,
                             const glyph_canon_opts_t *opts) {
    /* Try tabular */
    if (opts->auto_tabular) {
        char **cols = NULL;
        size_t col_count = 0;
        if (check_homogeneous(items, count, &cols, &col_count, opts)) {
            write_tabular(buf, items, count, cols, col_count, opts);
            for (size_t i = 0; i < col_count; i++) free(cols[i]);
            free(cols);
            return;
        }
    }

    strbuf_append_char(buf, '[');
    for (size_t i = 0; i < count; i++) {
        if (i > 0) strbuf_append_char(buf, ' ');
        write_canon_value(buf, items[i], opts);
    }
    strbuf_append_char(buf, ']');
}

static void write_canon_value(strbuf_t *buf, const glyph_value_t *v,
                              const glyph_canon_opts_t *opts) {
    if (!v) {
        strbuf_append(buf, canon_null(opts->null_style));
        return;
    }

    switch (v->type) {
        case GLYPH_NULL:
            strbuf_append(buf, canon_null(opts->null_style));
            break;

        case GLYPH_BOOL:
            strbuf_append_char(buf, v->bool_val ? 't' : 'f');
            break;

        case GLYPH_INT: {
            char num[32];
            snprintf(num, sizeof(num), "%ld", (long)v->int_val);
            strbuf_append(buf, num);
            break;
        }

        case GLYPH_FLOAT: {
            double f = v->float_val;
            /* Handle negative zero */
            if (f == 0.0) f = 0.0;

            /* Check if whole number */
            if (f == floor(f) && fabs(f) < 1e15) {
                char num[32];
                snprintf(num, sizeof(num), "%ld", (long)f);
                strbuf_append(buf, num);
            } else {
                char num[64];
                snprintf(num, sizeof(num), "%.15g", f);
                strbuf_append(buf, num);
            }
            break;
        }

        case GLYPH_STR:
            write_canon_string(buf, v->str_val ? v->str_val : "");
            break;

        case GLYPH_BYTES: {
            /* Base64 encode */
            static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            strbuf_append(buf, "b64\"");
            const uint8_t *data = v->bytes_val.data;
            size_t len = v->bytes_val.len;
            for (size_t i = 0; i < len; i += 3) {
                uint32_t n = (uint32_t)data[i] << 16;
                if (i + 1 < len) n |= (uint32_t)data[i + 1] << 8;
                if (i + 2 < len) n |= data[i + 2];

                strbuf_append_char(buf, b64[(n >> 18) & 63]);
                strbuf_append_char(buf, b64[(n >> 12) & 63]);
                strbuf_append_char(buf, i + 1 < len ? b64[(n >> 6) & 63] : '=');
                strbuf_append_char(buf, i + 2 < len ? b64[n & 63] : '=');
            }
            strbuf_append_char(buf, '"');
            break;
        }

        case GLYPH_TIME: {
            /* Format as ISO-8601 */
            char ts[32];
            time_t t = v->time_val / 1000;
            struct tm *tm = gmtime(&t);
            strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", tm);
            strbuf_append(buf, ts);
            break;
        }

        case GLYPH_ID:
            strbuf_append_char(buf, '^');
            if (v->id_val.prefix && v->id_val.prefix[0]) {
                strbuf_append(buf, v->id_val.prefix);
                strbuf_append_char(buf, ':');
            }
            if (is_ref_bare_safe(v->id_val.value)) {
                strbuf_append(buf, v->id_val.value);
            } else {
                write_quoted_string(buf, v->id_val.value ? v->id_val.value : "");
            }
            break;

        case GLYPH_LIST:
            write_canon_list(buf, v->list_val.items, v->list_val.count, opts);
            break;

        case GLYPH_MAP:
            write_canon_map(buf, v->map_val.entries, v->map_val.count, opts);
            break;

        case GLYPH_STRUCT:
            strbuf_append(buf, v->struct_val.type_name ? v->struct_val.type_name : "");
            write_canon_map(buf, v->struct_val.fields, v->struct_val.fields_count, opts);
            break;

        case GLYPH_SUM:
            strbuf_append(buf, v->sum_val.tag ? v->sum_val.tag : "");
            strbuf_append_char(buf, '(');
            if (v->sum_val.value) {
                write_canon_value(buf, v->sum_val.value, opts);
            }
            strbuf_append_char(buf, ')');
            break;
    }
}

/* ============================================================
 * Public Canonicalization Functions
 * ============================================================ */

char *glyph_canonicalize_loose(const glyph_value_t *v) {
    glyph_canon_opts_t opts = glyph_canon_opts_default();
    return glyph_canonicalize_loose_with_opts(v, &opts);
}

char *glyph_canonicalize_loose_no_tabular(const glyph_value_t *v) {
    glyph_canon_opts_t opts = glyph_canon_opts_no_tabular();
    return glyph_canonicalize_loose_with_opts(v, &opts);
}

char *glyph_canonicalize_loose_with_opts(const glyph_value_t *v,
                                          const glyph_canon_opts_t *opts) {
    strbuf_t buf;
    strbuf_init(&buf);
    write_canon_value(&buf, v, opts);
    return strbuf_finish(&buf);
}

char *glyph_fingerprint_loose(const glyph_value_t *v) {
    return glyph_canonicalize_loose(v);
}

bool glyph_equal_loose(const glyph_value_t *a, const glyph_value_t *b) {
    char *ca = glyph_canonicalize_loose(a);
    char *cb = glyph_canonicalize_loose(b);
    bool eq = ca && cb && strcmp(ca, cb) == 0;
    free(ca);
    free(cb);
    return eq;
}

/* ============================================================
 * Memory Management
 * ============================================================ */

void glyph_value_free(glyph_value_t *v) {
    if (!v) return;

    switch (v->type) {
        case GLYPH_STR:
            free(v->str_val);
            break;
        case GLYPH_BYTES:
            free(v->bytes_val.data);
            break;
        case GLYPH_ID:
            free(v->id_val.prefix);
            free(v->id_val.value);
            break;
        case GLYPH_LIST:
            for (size_t i = 0; i < v->list_val.count; i++) {
                glyph_value_free(v->list_val.items[i]);
            }
            free(v->list_val.items);
            break;
        case GLYPH_MAP:
            for (size_t i = 0; i < v->map_val.count; i++) {
                free(v->map_val.entries[i].key);
                glyph_value_free(v->map_val.entries[i].value);
            }
            free(v->map_val.entries);
            break;
        case GLYPH_STRUCT:
            free(v->struct_val.type_name);
            for (size_t i = 0; i < v->struct_val.fields_count; i++) {
                free(v->struct_val.fields[i].key);
                glyph_value_free(v->struct_val.fields[i].value);
            }
            free(v->struct_val.fields);
            break;
        case GLYPH_SUM:
            free(v->sum_val.tag);
            glyph_value_free(v->sum_val.value);
            break;
        default:
            break;
    }
    free(v);
}

void glyph_free(char *s) {
    free(s);
}
