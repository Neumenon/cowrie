/**
 * Simple JSON parser for GLYPH codec
 */

#include "glyph.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <math.h>
#include <time.h>

/* ============================================================
 * JSON Parser
 * ============================================================ */

typedef struct {
    const char *input;
    size_t pos;
    size_t len;
} json_parser_t;

static void skip_whitespace(json_parser_t *p) {
    while (p->pos < p->len && isspace((unsigned char)p->input[p->pos])) {
        p->pos++;
    }
}

static char peek(json_parser_t *p) {
    skip_whitespace(p);
    return p->pos < p->len ? p->input[p->pos] : '\0';
}

static char next(json_parser_t *p) {
    skip_whitespace(p);
    return p->pos < p->len ? p->input[p->pos++] : '\0';
}

static bool match(json_parser_t *p, const char *s) {
    skip_whitespace(p);
    size_t len = strlen(s);
    if (p->pos + len <= p->len && strncmp(p->input + p->pos, s, len) == 0) {
        p->pos += len;
        return true;
    }
    return false;
}

static glyph_value_t *parse_value(json_parser_t *p);

static char *parse_string(json_parser_t *p) {
    if (next(p) != '"') return NULL;

    size_t cap = 64;
    char *str = malloc(cap);
    size_t len = 0;

    while (p->pos < p->len) {
        char c = p->input[p->pos++];
        if (c == '"') {
            str[len] = '\0';
            return str;
        }
        if (c == '\\' && p->pos < p->len) {
            c = p->input[p->pos++];
            switch (c) {
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case '\\': c = '\\'; break;
                case '"': c = '"'; break;
                case 'u': {
                    /* Parse \uXXXX */
                    if (p->pos + 4 <= p->len) {
                        char hex[5] = {0};
                        memcpy(hex, p->input + p->pos, 4);
                        p->pos += 4;
                        unsigned int code = strtoul(hex, NULL, 16);
                        /* Simple UTF-8 encoding for BMP */
                        if (code < 0x80) {
                            c = (char)code;
                        } else if (code < 0x800) {
                            if (len + 2 >= cap) {
                                cap *= 2;
                                str = realloc(str, cap);
                            }
                            str[len++] = 0xC0 | (code >> 6);
                            c = 0x80 | (code & 0x3F);
                        } else {
                            if (len + 3 >= cap) {
                                cap *= 2;
                                str = realloc(str, cap);
                            }
                            str[len++] = 0xE0 | (code >> 12);
                            str[len++] = 0x80 | ((code >> 6) & 0x3F);
                            c = 0x80 | (code & 0x3F);
                        }
                    }
                    break;
                }
                default: break;
            }
        }
        if (len + 1 >= cap) {
            cap *= 2;
            str = realloc(str, cap);
        }
        str[len++] = c;
    }

    free(str);
    return NULL;
}

static glyph_value_t *parse_number(json_parser_t *p) {
    skip_whitespace(p);
    const char *start = p->input + p->pos;
    char *end;

    /* Try integer first */
    long long int_val = strtoll(start, &end, 10);
    if (end > start && (*end == '.' || *end == 'e' || *end == 'E')) {
        /* It's a float */
        double float_val = strtod(start, &end);
        p->pos = end - p->input;
        return glyph_float(float_val);
    } else if (end > start) {
        p->pos = end - p->input;
        return glyph_int(int_val);
    }

    return NULL;
}

static glyph_value_t *parse_array(json_parser_t *p) {
    if (next(p) != '[') return NULL;

    glyph_value_t *list = glyph_list_new();

    skip_whitespace(p);
    if (peek(p) == ']') {
        next(p);
        return list;
    }

    while (1) {
        glyph_value_t *item = parse_value(p);
        if (!item) {
            glyph_value_free(list);
            return NULL;
        }
        glyph_list_append(list, item);

        skip_whitespace(p);
        char c = peek(p);
        if (c == ']') {
            next(p);
            break;
        }
        if (c != ',') {
            glyph_value_free(list);
            return NULL;
        }
        next(p); /* consume comma */
    }

    return list;
}

static glyph_value_t *parse_object(json_parser_t *p) {
    if (next(p) != '{') return NULL;

    glyph_value_t *map = glyph_map_new();

    skip_whitespace(p);
    if (peek(p) == '}') {
        next(p);
        return map;
    }

    while (1) {
        skip_whitespace(p);
        if (peek(p) != '"') {
            glyph_value_free(map);
            return NULL;
        }

        char *key = parse_string(p);
        if (!key) {
            glyph_value_free(map);
            return NULL;
        }

        skip_whitespace(p);
        if (next(p) != ':') {
            free(key);
            glyph_value_free(map);
            return NULL;
        }

        glyph_value_t *value = parse_value(p);
        if (!value) {
            free(key);
            glyph_value_free(map);
            return NULL;
        }

        glyph_map_set(map, key, value);
        free(key);

        skip_whitespace(p);
        char c = peek(p);
        if (c == '}') {
            next(p);
            break;
        }
        if (c != ',') {
            glyph_value_free(map);
            return NULL;
        }
        next(p); /* consume comma */
    }

    return map;
}

static glyph_value_t *parse_value(json_parser_t *p) {
    skip_whitespace(p);
    char c = peek(p);

    if (c == 'n' && match(p, "null")) {
        return glyph_null();
    }
    if (c == 't' && match(p, "true")) {
        return glyph_bool(true);
    }
    if (c == 'f' && match(p, "false")) {
        return glyph_bool(false);
    }
    if (c == '"') {
        char *s = parse_string(p);
        if (!s) return NULL;
        glyph_value_t *v = glyph_str(s);
        free(s);
        return v;
    }
    if (c == '[') {
        return parse_array(p);
    }
    if (c == '{') {
        return parse_object(p);
    }
    if (c == '-' || isdigit((unsigned char)c)) {
        return parse_number(p);
    }

    return NULL;
}

glyph_value_t *glyph_from_json(const char *json) {
    if (!json) return NULL;

    json_parser_t p = {
        .input = json,
        .pos = 0,
        .len = strlen(json),
    };

    return parse_value(&p);
}

/* ============================================================
 * JSON Serialization
 * ============================================================ */

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} json_buf_t;

static void json_buf_init(json_buf_t *buf) {
    buf->cap = 256;
    buf->data = malloc(buf->cap);
    buf->len = 0;
    if (buf->data) buf->data[0] = '\0';
}

static void json_buf_grow(json_buf_t *buf, size_t need) {
    if (buf->len + need >= buf->cap) {
        while (buf->len + need >= buf->cap) buf->cap *= 2;
        buf->data = realloc(buf->data, buf->cap);
    }
}

static void json_buf_append(json_buf_t *buf, const char *s) {
    size_t len = strlen(s);
    json_buf_grow(buf, len + 1);
    memcpy(buf->data + buf->len, s, len + 1);
    buf->len += len;
}

static void json_buf_append_char(json_buf_t *buf, char c) {
    json_buf_grow(buf, 2);
    buf->data[buf->len++] = c;
    buf->data[buf->len] = '\0';
}

static void write_json_string(json_buf_t *buf, const char *s) {
    json_buf_append_char(buf, '"');
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '\\': json_buf_append(buf, "\\\\"); break;
            case '"':  json_buf_append(buf, "\\\""); break;
            case '\n': json_buf_append(buf, "\\n"); break;
            case '\r': json_buf_append(buf, "\\r"); break;
            case '\t': json_buf_append(buf, "\\t"); break;
            default:
                if ((unsigned char)*p < 0x20) {
                    char hex[8];
                    snprintf(hex, sizeof(hex), "\\u%04x", (unsigned char)*p);
                    json_buf_append(buf, hex);
                } else {
                    json_buf_append_char(buf, *p);
                }
        }
    }
    json_buf_append_char(buf, '"');
}

static void write_json_value(json_buf_t *buf, const glyph_value_t *v) {
    if (!v) {
        json_buf_append(buf, "null");
        return;
    }

    switch (v->type) {
        case GLYPH_NULL:
            json_buf_append(buf, "null");
            break;

        case GLYPH_BOOL:
            json_buf_append(buf, v->bool_val ? "true" : "false");
            break;

        case GLYPH_INT: {
            char num[32];
            snprintf(num, sizeof(num), "%ld", (long)v->int_val);
            json_buf_append(buf, num);
            break;
        }

        case GLYPH_FLOAT: {
            char num[64];
            snprintf(num, sizeof(num), "%.15g", v->float_val);
            json_buf_append(buf, num);
            break;
        }

        case GLYPH_STR:
            write_json_string(buf, v->str_val ? v->str_val : "");
            break;

        case GLYPH_BYTES: {
            /* Base64 encode */
            static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            json_buf_append_char(buf, '"');
            const uint8_t *data = v->bytes_val.data;
            size_t len = v->bytes_val.len;
            for (size_t i = 0; i < len; i += 3) {
                uint32_t n = (uint32_t)data[i] << 16;
                if (i + 1 < len) n |= (uint32_t)data[i + 1] << 8;
                if (i + 2 < len) n |= data[i + 2];

                json_buf_append_char(buf, b64[(n >> 18) & 63]);
                json_buf_append_char(buf, b64[(n >> 12) & 63]);
                json_buf_append_char(buf, i + 1 < len ? b64[(n >> 6) & 63] : '=');
                json_buf_append_char(buf, i + 2 < len ? b64[n & 63] : '=');
            }
            json_buf_append_char(buf, '"');
            break;
        }

        case GLYPH_ID: {
            json_buf_append_char(buf, '"');
            json_buf_append_char(buf, '^');
            if (v->id_val.prefix && v->id_val.prefix[0]) {
                json_buf_append(buf, v->id_val.prefix);
                json_buf_append_char(buf, ':');
            }
            json_buf_append(buf, v->id_val.value ? v->id_val.value : "");
            json_buf_append_char(buf, '"');
            break;
        }

        case GLYPH_LIST:
            json_buf_append_char(buf, '[');
            for (size_t i = 0; i < v->list_val.count; i++) {
                if (i > 0) json_buf_append_char(buf, ',');
                write_json_value(buf, v->list_val.items[i]);
            }
            json_buf_append_char(buf, ']');
            break;

        case GLYPH_MAP:
            json_buf_append_char(buf, '{');
            for (size_t i = 0; i < v->map_val.count; i++) {
                if (i > 0) json_buf_append_char(buf, ',');
                write_json_string(buf, v->map_val.entries[i].key);
                json_buf_append_char(buf, ':');
                write_json_value(buf, v->map_val.entries[i].value);
            }
            json_buf_append_char(buf, '}');
            break;

        case GLYPH_STRUCT:
            json_buf_append_char(buf, '{');
            json_buf_append(buf, "\"_type\":");
            write_json_string(buf, v->struct_val.type_name ? v->struct_val.type_name : "");
            for (size_t i = 0; i < v->struct_val.fields_count; i++) {
                json_buf_append_char(buf, ',');
                write_json_string(buf, v->struct_val.fields[i].key);
                json_buf_append_char(buf, ':');
                write_json_value(buf, v->struct_val.fields[i].value);
            }
            json_buf_append_char(buf, '}');
            break;

        case GLYPH_SUM:
            json_buf_append_char(buf, '{');
            json_buf_append(buf, "\"_tag\":");
            write_json_string(buf, v->sum_val.tag ? v->sum_val.tag : "");
            if (v->sum_val.value) {
                json_buf_append(buf, ",\"_value\":");
                write_json_value(buf, v->sum_val.value);
            }
            json_buf_append_char(buf, '}');
            break;

        case GLYPH_TIME: {
            /* Format as ISO-8601 string */
            char ts[32];
            time_t t = v->time_val / 1000;
            struct tm *tm = gmtime(&t);
            strftime(ts, sizeof(ts), "\"%Y-%m-%dT%H:%M:%SZ\"", tm);
            json_buf_append(buf, ts);
            break;
        }
    }
}

char *glyph_to_json(const glyph_value_t *v) {
    json_buf_t buf;
    json_buf_init(&buf);
    write_json_value(&buf, v);
    return buf.data;
}

/* ============================================================
 * Hash (simple implementation)
 * ============================================================ */

/* Simple SHA-256 implementation would be too long here.
   For production, link against a crypto library. */
char *glyph_hash_loose(const glyph_value_t *v) {
    char *canonical = glyph_canonicalize_loose(v);
    if (!canonical) return NULL;

    /* Simple hash for demonstration (not cryptographic!) */
    unsigned long hash = 5381;
    for (const char *p = canonical; *p; p++) {
        hash = ((hash << 5) + hash) + (unsigned char)*p;
    }

    char *result = malloc(17);
    if (result) {
        snprintf(result, 17, "%016lx", hash);
    }

    free(canonical);
    return result;
}
