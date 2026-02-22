/**
 * Schema Evolution - C Implementation
 */

#include "schema_evolution.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

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

/* ============================================================
 * Field Value Functions
 * ============================================================ */

field_value_t field_value_null(void) {
    field_value_t v = {FIELD_VALUE_NULL, {.int_val = 0}};
    return v;
}

field_value_t field_value_bool(bool val) {
    field_value_t v = {FIELD_VALUE_BOOL, {.bool_val = val}};
    return v;
}

field_value_t field_value_int(int64_t val) {
    field_value_t v = {FIELD_VALUE_INT, {.int_val = val}};
    return v;
}

field_value_t field_value_float(double val) {
    field_value_t v = {FIELD_VALUE_FLOAT, {.float_val = val}};
    return v;
}

field_value_t field_value_str(const char *val) {
    field_value_t v = {FIELD_VALUE_STR, {.str_val = strdup_safe(val)}};
    return v;
}

void field_value_free(field_value_t *v) {
    if (v && v->type == FIELD_VALUE_STR && v->str_val) {
        free(v->str_val);
        v->str_val = NULL;
    }
}

/* ============================================================
 * Evolving Field Functions
 * ============================================================ */

evolving_field_t *evolving_field_new(const char *name, const evolving_field_config_t *config) {
    if (!name || !config) return NULL;

    evolving_field_t *f = calloc(1, sizeof(evolving_field_t));
    if (!f) return NULL;

    f->name = strdup_safe(name);
    f->type = config->type;
    f->required = config->required;
    f->default_value = config->default_value;
    if (f->default_value.type == FIELD_VALUE_STR && config->default_value.str_val) {
        f->default_value.str_val = strdup_safe(config->default_value.str_val);
    }
    f->added_in = strdup_safe(config->added_in ? config->added_in : "1.0");
    f->deprecated_in = strdup_safe(config->deprecated_in);
    f->renamed_from = strdup_safe(config->renamed_from);
    f->validation = strdup_safe(config->validation);

    return f;
}

void evolving_field_free(evolving_field_t *f) {
    if (!f) return;
    free(f->name);
    field_value_free(&f->default_value);
    free(f->added_in);
    free(f->deprecated_in);
    free(f->renamed_from);
    free(f->validation);
    free(f);
}

bool evolving_field_is_available_in(const evolving_field_t *f, const char *version) {
    if (!f || !version) return false;

    if (compare_versions(version, f->added_in) < 0) {
        return false;
    }

    if (f->deprecated_in && compare_versions(version, f->deprecated_in) >= 0) {
        return false;
    }

    return true;
}

bool evolving_field_is_deprecated_in(const evolving_field_t *f, const char *version) {
    if (!f || !version || !f->deprecated_in) return false;
    return compare_versions(version, f->deprecated_in) >= 0;
}

char *evolving_field_validate(const evolving_field_t *f, const field_value_t *value) {
    if (!f) return strdup_safe("invalid field");

    if (!value || value->type == FIELD_VALUE_NULL) {
        if (f->required) {
            char buf[256];
            snprintf(buf, sizeof(buf), "field %s is required", f->name);
            return strdup_safe(buf);
        }
        return NULL;
    }

    /* Type checking */
    switch (f->type) {
        case FIELD_TYPE_STR:
            if (value->type != FIELD_VALUE_STR) {
                char buf[256];
                snprintf(buf, sizeof(buf), "field %s must be string", f->name);
                return strdup_safe(buf);
            }
            break;
        case FIELD_TYPE_INT:
            if (value->type != FIELD_VALUE_INT) {
                char buf[256];
                snprintf(buf, sizeof(buf), "field %s must be int", f->name);
                return strdup_safe(buf);
            }
            break;
        case FIELD_TYPE_FLOAT:
            if (value->type != FIELD_VALUE_FLOAT && value->type != FIELD_VALUE_INT) {
                char buf[256];
                snprintf(buf, sizeof(buf), "field %s must be float", f->name);
                return strdup_safe(buf);
            }
            break;
        case FIELD_TYPE_BOOL:
            if (value->type != FIELD_VALUE_BOOL) {
                char buf[256];
                snprintf(buf, sizeof(buf), "field %s must be bool", f->name);
                return strdup_safe(buf);
            }
            break;
        default:
            break;
    }

    return NULL;
}

/* ============================================================
 * Version Schema Functions
 * ============================================================ */

version_schema_t *version_schema_new(const char *name, const char *version) {
    version_schema_t *s = calloc(1, sizeof(version_schema_t));
    if (!s) return NULL;

    s->name = strdup_safe(name);
    s->version = strdup_safe(version);
    s->description = NULL;
    s->fields = NULL;
    s->fields_count = 0;
    s->fields_capacity = 0;

    return s;
}

void version_schema_free(version_schema_t *s) {
    if (!s) return;

    free(s->name);
    free(s->version);
    free(s->description);

    for (size_t i = 0; i < s->fields_count; i++) {
        evolving_field_free(&s->fields[i]);
    }
    free(s->fields);
    free(s);
}

void version_schema_add_field(version_schema_t *s, evolving_field_t *field) {
    if (!s || !field) return;

    if (s->fields_count >= s->fields_capacity) {
        size_t new_cap = s->fields_capacity == 0 ? 8 : s->fields_capacity * 2;
        evolving_field_t *new_fields = realloc(s->fields, new_cap * sizeof(evolving_field_t));
        if (!new_fields) return;
        s->fields = new_fields;
        s->fields_capacity = new_cap;
    }

    s->fields[s->fields_count++] = *field;
    free(field); /* Ownership transferred */
}

const evolving_field_t *version_schema_get_field(const version_schema_t *s, const char *name) {
    if (!s || !name) return NULL;

    for (size_t i = 0; i < s->fields_count; i++) {
        if (strcmp(s->fields[i].name, name) == 0) {
            return &s->fields[i];
        }
    }
    return NULL;
}

char *version_schema_validate(const version_schema_t *s,
                              const field_value_t *data,
                              const char **keys,
                              size_t count) {
    if (!s) return strdup_safe("invalid schema");

    /* Check required fields */
    for (size_t i = 0; i < s->fields_count; i++) {
        if (s->fields[i].required) {
            bool found = false;
            for (size_t j = 0; j < count; j++) {
                if (strcmp(keys[j], s->fields[i].name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                char buf[256];
                snprintf(buf, sizeof(buf), "missing required field: %s", s->fields[i].name);
                return strdup_safe(buf);
            }
        }
    }

    /* Validate field values */
    for (size_t i = 0; i < count; i++) {
        const evolving_field_t *field = version_schema_get_field(s, keys[i]);
        if (field) {
            char *error = evolving_field_validate(field, &data[i]);
            if (error) return error;
        }
    }

    return NULL;
}

/* ============================================================
 * Versioned Schema Functions
 * ============================================================ */

versioned_schema_t *versioned_schema_new(const char *name) {
    versioned_schema_t *s = calloc(1, sizeof(versioned_schema_t));
    if (!s) return NULL;

    s->name = strdup_safe(name);
    s->versions = NULL;
    s->versions_count = 0;
    s->versions_capacity = 0;
    s->latest_version = strdup_safe("1.0");
    s->mode = EVOLUTION_MODE_TOLERANT;

    return s;
}

void versioned_schema_free(versioned_schema_t *s) {
    if (!s) return;

    free(s->name);
    free(s->latest_version);

    for (size_t i = 0; i < s->versions_count; i++) {
        free(s->versions[i].name);
        free(s->versions[i].version);
        free(s->versions[i].description);
        for (size_t j = 0; j < s->versions[i].fields_count; j++) {
            free(s->versions[i].fields[j].name);
            field_value_free(&s->versions[i].fields[j].default_value);
            free(s->versions[i].fields[j].added_in);
            free(s->versions[i].fields[j].deprecated_in);
            free(s->versions[i].fields[j].renamed_from);
            free(s->versions[i].fields[j].validation);
        }
        free(s->versions[i].fields);
    }
    free(s->versions);
    free(s);
}

versioned_schema_t *versioned_schema_with_mode(versioned_schema_t *s, evolution_mode_t mode) {
    if (s) s->mode = mode;
    return s;
}

static void update_latest_version(versioned_schema_t *s) {
    if (!s || s->versions_count == 0) return;

    const char *latest = s->versions[0].version;
    for (size_t i = 1; i < s->versions_count; i++) {
        if (compare_versions(s->versions[i].version, latest) > 0) {
            latest = s->versions[i].version;
        }
    }

    free(s->latest_version);
    s->latest_version = strdup_safe(latest);
}

void versioned_schema_add_version(versioned_schema_t *s,
                                  const char *version,
                                  const evolving_field_config_t *fields,
                                  const char **field_names,
                                  size_t field_count) {
    if (!s || !version) return;

    if (s->versions_count >= s->versions_capacity) {
        size_t new_cap = s->versions_capacity == 0 ? 4 : s->versions_capacity * 2;
        version_schema_t *new_versions = realloc(s->versions, new_cap * sizeof(version_schema_t));
        if (!new_versions) return;
        s->versions = new_versions;
        s->versions_capacity = new_cap;
    }

    version_schema_t *vs = &s->versions[s->versions_count++];
    memset(vs, 0, sizeof(version_schema_t));
    vs->name = strdup_safe(s->name);
    vs->version = strdup_safe(version);
    vs->description = NULL;
    vs->fields = NULL;
    vs->fields_count = 0;
    vs->fields_capacity = 0;

    for (size_t i = 0; i < field_count; i++) {
        evolving_field_config_t config = fields[i];
        if (!config.added_in) {
            config.added_in = version;
        }

        evolving_field_t *field = evolving_field_new(field_names[i], &config);
        if (field) {
            version_schema_add_field(vs, field);
        }
    }

    update_latest_version(s);
}

const version_schema_t *versioned_schema_get_version(const versioned_schema_t *s, const char *version) {
    if (!s || !version) return NULL;

    for (size_t i = 0; i < s->versions_count; i++) {
        if (strcmp(s->versions[i].version, version) == 0) {
            return &s->versions[i];
        }
    }
    return NULL;
}

/* Migration helper */
static evolution_parse_result_t migrate_step(const versioned_schema_t *s,
                                              const field_value_t *data,
                                              const char **keys,
                                              size_t count,
                                              const char *to_version) {
    evolution_parse_result_t result = {0};

    const version_schema_t *to_schema = versioned_schema_get_version(s, to_version);
    if (!to_schema) {
        result.error = strdup_safe("invalid version");
        return result;
    }

    /* Allocate result arrays */
    size_t max_count = count + to_schema->fields_count;
    result.data = calloc(max_count, sizeof(field_value_t));
    result.keys = calloc(max_count, sizeof(char *));
    result.data_count = 0;

    /* Copy existing data, handling renames */
    for (size_t i = 0; i < count; i++) {
        bool renamed = false;
        for (size_t j = 0; j < to_schema->fields_count; j++) {
            if (to_schema->fields[j].renamed_from &&
                strcmp(to_schema->fields[j].renamed_from, keys[i]) == 0) {
                /* Field was renamed */
                result.keys[result.data_count] = strdup_safe(to_schema->fields[j].name);
                result.data[result.data_count] = data[i];
                if (data[i].type == FIELD_VALUE_STR) {
                    result.data[result.data_count].str_val = strdup_safe(data[i].str_val);
                }
                result.data_count++;
                renamed = true;
                break;
            }
        }
        if (!renamed) {
            /* Copy as-is if field exists in target schema or in tolerant mode */
            if (s->mode == EVOLUTION_MODE_TOLERANT || version_schema_get_field(to_schema, keys[i])) {
                result.keys[result.data_count] = strdup_safe(keys[i]);
                result.data[result.data_count] = data[i];
                if (data[i].type == FIELD_VALUE_STR) {
                    result.data[result.data_count].str_val = strdup_safe(data[i].str_val);
                }
                result.data_count++;
            }
        }
    }

    /* Add new fields with defaults */
    for (size_t i = 0; i < to_schema->fields_count; i++) {
        const evolving_field_t *field = &to_schema->fields[i];
        bool exists = false;
        for (size_t j = 0; j < result.data_count; j++) {
            if (strcmp(result.keys[j], field->name) == 0) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            result.keys[result.data_count] = strdup_safe(field->name);
            if (field->default_value.type != FIELD_VALUE_NULL) {
                result.data[result.data_count] = field->default_value;
                if (field->default_value.type == FIELD_VALUE_STR) {
                    result.data[result.data_count].str_val = strdup_safe(field->default_value.str_val);
                }
            } else if (!field->required) {
                result.data[result.data_count] = field_value_null();
            }
            result.data_count++;
        }
    }

    /* Remove unknown fields in tolerant mode */
    if (s->mode == EVOLUTION_MODE_TOLERANT) {
        for (size_t i = 0; i < result.data_count; ) {
            if (!version_schema_get_field(to_schema, result.keys[i])) {
                field_value_free(&result.data[i]);
                free(result.keys[i]);
                /* Shift remaining items */
                for (size_t j = i; j < result.data_count - 1; j++) {
                    result.data[j] = result.data[j + 1];
                    result.keys[j] = result.keys[j + 1];
                }
                result.data_count--;
            } else {
                i++;
            }
        }
    }

    return result;
}

evolution_parse_result_t versioned_schema_parse(const versioned_schema_t *s,
                                                const field_value_t *data,
                                                const char **keys,
                                                size_t count,
                                                const char *from_version) {
    evolution_parse_result_t result = {0};

    const version_schema_t *schema = versioned_schema_get_version(s, from_version);
    if (!schema) {
        result.error = malloc(64);
        snprintf(result.error, 64, "unknown version: %s", from_version);
        return result;
    }

    /* Validate in strict mode */
    if (s->mode == EVOLUTION_MODE_STRICT) {
        char *error = version_schema_validate(schema, data, keys, count);
        if (error) {
            result.error = error;
            return result;
        }
    }

    /* If same version, just copy */
    if (strcmp(from_version, s->latest_version) == 0) {
        result.data = calloc(count, sizeof(field_value_t));
        result.keys = calloc(count, sizeof(char *));
        result.data_count = count;
        for (size_t i = 0; i < count; i++) {
            result.data[i] = data[i];
            if (data[i].type == FIELD_VALUE_STR) {
                result.data[i].str_val = strdup_safe(data[i].str_val);
            }
            result.keys[i] = strdup_safe(keys[i]);
        }
        return result;
    }

    /* Migrate to latest */
    return migrate_step(s, data, keys, count, s->latest_version);
}

evolution_emit_result_t versioned_schema_emit(const versioned_schema_t *s,
                                              const field_value_t *data,
                                              const char **keys,
                                              size_t count,
                                              const char *version) {
    evolution_emit_result_t result = {0};

    const char *target_version = version ? version : s->latest_version;
    const version_schema_t *schema = versioned_schema_get_version(s, target_version);
    if (!schema) {
        result.error = malloc(64);
        snprintf(result.error, 64, "unknown version: %s", target_version);
        return result;
    }

    char *error = version_schema_validate(schema, data, keys, count);
    if (error) {
        result.error = error;
        return result;
    }

    result.header = format_version_header(target_version);
    return result;
}

changelog_entry_t *versioned_schema_get_changelog(const versioned_schema_t *s, size_t *count) {
    if (!s || !count) return NULL;

    *count = s->versions_count;
    if (s->versions_count == 0) return NULL;

    changelog_entry_t *entries = calloc(s->versions_count, sizeof(changelog_entry_t));
    if (!entries) return NULL;

    for (size_t i = 0; i < s->versions_count; i++) {
        const version_schema_t *vs = &s->versions[i];
        entries[i].version = strdup_safe(vs->version);
        entries[i].description = strdup_safe(vs->description);

        /* Count fields for each category */
        size_t added = 0, deprecated = 0, renamed = 0;
        for (size_t j = 0; j < vs->fields_count; j++) {
            if (vs->fields[j].added_in && strcmp(vs->fields[j].added_in, vs->version) == 0) {
                added++;
            }
            if (vs->fields[j].deprecated_in && strcmp(vs->fields[j].deprecated_in, vs->version) == 0) {
                deprecated++;
            }
            if (vs->fields[j].renamed_from) {
                renamed++;
            }
        }

        entries[i].added_fields = added ? calloc(added + 1, sizeof(char *)) : NULL;
        entries[i].deprecated_fields = deprecated ? calloc(deprecated + 1, sizeof(char *)) : NULL;
        entries[i].renamed_from = renamed ? calloc(renamed + 1, sizeof(char *)) : NULL;
        entries[i].renamed_to = renamed ? calloc(renamed + 1, sizeof(char *)) : NULL;

        size_t ai = 0, di = 0, ri = 0;
        for (size_t j = 0; j < vs->fields_count; j++) {
            if (vs->fields[j].added_in && strcmp(vs->fields[j].added_in, vs->version) == 0) {
                entries[i].added_fields[ai++] = strdup_safe(vs->fields[j].name);
            }
            if (vs->fields[j].deprecated_in && strcmp(vs->fields[j].deprecated_in, vs->version) == 0) {
                entries[i].deprecated_fields[di++] = strdup_safe(vs->fields[j].name);
            }
            if (vs->fields[j].renamed_from) {
                entries[i].renamed_from[ri] = strdup_safe(vs->fields[j].renamed_from);
                entries[i].renamed_to[ri++] = strdup_safe(vs->fields[j].name);
            }
        }

        entries[i].added_count = added;
        entries[i].deprecated_count = deprecated;
        entries[i].renamed_count = renamed;
    }

    return entries;
}

void changelog_free(changelog_entry_t *entries, size_t count) {
    if (!entries) return;

    for (size_t i = 0; i < count; i++) {
        free(entries[i].version);
        free(entries[i].description);

        for (size_t j = 0; j < entries[i].added_count; j++) {
            free(entries[i].added_fields[j]);
        }
        free(entries[i].added_fields);

        for (size_t j = 0; j < entries[i].deprecated_count; j++) {
            free(entries[i].deprecated_fields[j]);
        }
        free(entries[i].deprecated_fields);

        for (size_t j = 0; j < entries[i].renamed_count; j++) {
            free(entries[i].renamed_from[j]);
            free(entries[i].renamed_to[j]);
        }
        free(entries[i].renamed_from);
        free(entries[i].renamed_to);
    }

    free(entries);
}

void evolution_parse_result_free(evolution_parse_result_t *r) {
    if (!r) return;

    free(r->error);
    for (size_t i = 0; i < r->data_count; i++) {
        field_value_free(&r->data[i]);
        free(r->keys[i]);
    }
    free(r->data);
    free(r->keys);
}

void evolution_emit_result_free(evolution_emit_result_t *r) {
    if (!r) return;
    free(r->error);
    free(r->header);
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

int compare_versions(const char *v1, const char *v2) {
    if (!v1 && !v2) return 0;
    if (!v1) return -1;
    if (!v2) return 1;

    /* Parse version parts */
    int parts1[10] = {0};
    int parts2[10] = {0};
    int count1 = 0, count2 = 0;

    const char *p = v1;
    while (*p && count1 < 10) {
        parts1[count1++] = atoi(p);
        while (*p && *p != '.') p++;
        if (*p == '.') p++;
    }

    p = v2;
    while (*p && count2 < 10) {
        parts2[count2++] = atoi(p);
        while (*p && *p != '.') p++;
        if (*p == '.') p++;
    }

    int max_len = count1 > count2 ? count1 : count2;
    for (int i = 0; i < max_len; i++) {
        int p1 = i < count1 ? parts1[i] : 0;
        int p2 = i < count2 ? parts2[i] : 0;

        if (p1 < p2) return -1;
        if (p1 > p2) return 1;
    }

    return 0;
}

char *parse_version_header(const char *text) {
    if (!text) return NULL;

    /* Skip whitespace */
    while (isspace(*text)) text++;

    if (strncmp(text, "@version ", 9) != 0) {
        return NULL;
    }

    text += 9;
    while (isspace(*text)) text++;

    if (!*text) return NULL;

    /* Find end of version string */
    const char *end = text;
    while (*end && !isspace(*end)) end++;

    size_t len = end - text;
    char *version = malloc(len + 1);
    if (version) {
        memcpy(version, text, len);
        version[len] = '\0';
    }

    return version;
}

char *format_version_header(const char *version) {
    if (!version) return NULL;

    size_t len = strlen(version) + 10; /* "@version " + version + null */
    char *header = malloc(len);
    if (header) {
        snprintf(header, len, "@version %s", version);
    }

    return header;
}
