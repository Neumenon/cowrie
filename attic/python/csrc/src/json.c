/*
 * COWRIE JSON Bridge Implementation
 */

#include "../include/cowrie_json.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <math.h>

/* ============================================================
 * Base64 Implementation
 * ============================================================ */

static const char b64_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const int8_t b64_decode_table[256] = {
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
    52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
    15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
    -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
};

int cowrie_base64_encode(const uint8_t *data, size_t len, COWRIEBuf *buf) {
    size_t out_len = ((len + 2) / 3) * 4;
    buf->data = malloc(out_len + 1);
    if (!buf->data) return -1;

    size_t i = 0, j = 0;
    while (i < len) {
        size_t remaining = len - i;
        uint32_t a = data[i++];
        uint32_t b = (remaining > 1) ? data[i++] : 0;
        uint32_t c = (remaining > 2) ? data[i++] : 0;
        uint32_t triple = (a << 16) | (b << 8) | c;

        buf->data[j++] = b64_chars[(triple >> 18) & 0x3F];
        buf->data[j++] = b64_chars[(triple >> 12) & 0x3F];
        buf->data[j++] = (remaining > 1) ? b64_chars[(triple >> 6) & 0x3F] : '=';
        buf->data[j++] = (remaining > 2) ? b64_chars[triple & 0x3F] : '=';
    }

    buf->data[j] = '\0';
    buf->len = j;
    buf->cap = out_len + 1;
    return 0;
}

int cowrie_base64_decode(const char *b64, size_t len, COWRIEBuf *buf) {
    if (len % 4 != 0) return -1;

    size_t out_len = (len / 4) * 3;
    if (len > 0 && b64[len-1] == '=') out_len--;
    if (len > 1 && b64[len-2] == '=') out_len--;

    buf->data = malloc(out_len);
    if (!buf->data) return -1;

    size_t i = 0, j = 0;
    while (i < len) {
        int8_t a = b64_decode_table[(unsigned char)b64[i++]];
        int8_t b = b64_decode_table[(unsigned char)b64[i++]];
        int8_t c = b64_decode_table[(unsigned char)b64[i++]];
        int8_t d = b64_decode_table[(unsigned char)b64[i++]];

        if (a < 0 || b < 0) { free(buf->data); return -1; }

        uint32_t triple = (a << 18) | (b << 12);
        if (c >= 0) triple |= (c << 6);
        if (d >= 0) triple |= d;

        if (j < out_len) buf->data[j++] = (triple >> 16) & 0xFF;
        if (j < out_len) buf->data[j++] = (triple >> 8) & 0xFF;
        if (j < out_len) buf->data[j++] = triple & 0xFF;
    }

    buf->len = out_len;
    buf->cap = out_len;
    return 0;
}

/* ============================================================
 * JSON Parser
 * ============================================================ */

typedef struct {
    const char *data;
    size_t len;
    size_t pos;
} JsonParser;

static void skip_whitespace(JsonParser *p) {
    while (p->pos < p->len && isspace((unsigned char)p->data[p->pos])) {
        p->pos++;
    }
}

static int peek(JsonParser *p) {
    if (p->pos >= p->len) return -1;
    return (unsigned char)p->data[p->pos];
}

static int next(JsonParser *p) {
    if (p->pos >= p->len) return -1;
    return (unsigned char)p->data[p->pos++];
}

static int expect(JsonParser *p, char c) {
    skip_whitespace(p);
    if (peek(p) != c) return -1;
    next(p);
    return 0;
}

static COWRIEValue* parse_value(JsonParser *p);

static char* parse_string_content(JsonParser *p, size_t *out_len) {
    if (expect(p, '"') != 0) return NULL;

    size_t start = p->pos;
    size_t cap = 64;
    char *result = malloc(cap);
    if (!result) return NULL;
    size_t len = 0;

    while (p->pos < p->len) {
        int c = next(p);
        if (c == '"') {
            result[len] = '\0';
            *out_len = len;
            return result;
        }
        if (c == '\\') {
            c = next(p);
            switch (c) {
                case '"':  c = '"'; break;
                case '\\': c = '\\'; break;
                case '/':  c = '/'; break;
                case 'b':  c = '\b'; break;
                case 'f':  c = '\f'; break;
                case 'n':  c = '\n'; break;
                case 'r':  c = '\r'; break;
                case 't':  c = '\t'; break;
                case 'u': {
                    /* Parse \uXXXX */
                    uint32_t cp = 0;
                    for (int i = 0; i < 4; i++) {
                        c = next(p);
                        if (c >= '0' && c <= '9') cp = (cp << 4) | (c - '0');
                        else if (c >= 'a' && c <= 'f') cp = (cp << 4) | (c - 'a' + 10);
                        else if (c >= 'A' && c <= 'F') cp = (cp << 4) | (c - 'A' + 10);
                        else { free(result); return NULL; }
                    }
                    /* Encode as UTF-8 */
                    if (len + 4 > cap) {
                        cap *= 2;
                        char *tmp = realloc(result, cap);
                        if (!tmp) { free(result); return NULL; }
                        result = tmp;
                    }
                    if (cp < 0x80) {
                        result[len++] = cp;
                    } else if (cp < 0x800) {
                        result[len++] = 0xC0 | (cp >> 6);
                        result[len++] = 0x80 | (cp & 0x3F);
                    } else {
                        result[len++] = 0xE0 | (cp >> 12);
                        result[len++] = 0x80 | ((cp >> 6) & 0x3F);
                        result[len++] = 0x80 | (cp & 0x3F);
                    }
                    continue;
                }
                default: free(result); return NULL;
            }
        }
        if (len + 1 >= cap) {
            cap *= 2;
            char *tmp = realloc(result, cap);
            if (!tmp) { free(result); return NULL; }
            result = tmp;
        }
        result[len++] = c;
    }

    free(result);
    return NULL;
}

static COWRIEValue* parse_string(JsonParser *p) {
    size_t len;
    char *s = parse_string_content(p, &len);
    if (!s) return NULL;
    return cowrie_new_string(s, len);
}

static COWRIEValue* parse_number(JsonParser *p) {
    skip_whitespace(p);
    size_t start = p->pos;
    int is_float = 0;
    int is_negative = 0;

    if (peek(p) == '-') {
        is_negative = 1;
        next(p);
    }

    while (p->pos < p->len) {
        int c = peek(p);
        if (c >= '0' && c <= '9') {
            next(p);
        } else if (c == '.' || c == 'e' || c == 'E') {
            is_float = 1;
            next(p);
            if (c != '.' && (peek(p) == '+' || peek(p) == '-')) {
                next(p);
            }
        } else {
            break;
        }
    }

    size_t len = p->pos - start;
    char *num_str = malloc(len + 1);
    if (!num_str) return NULL;
    memcpy(num_str, p->data + start, len);
    num_str[len] = '\0';

    COWRIEValue *result;
    if (is_float) {
        double d = strtod(num_str, NULL);
        result = cowrie_new_float64(d);
    } else if (is_negative) {
        int64_t i = strtoll(num_str, NULL, 10);
        result = cowrie_new_int64(i);
    } else {
        uint64_t u = strtoull(num_str, NULL, 10);
        if (u <= INT64_MAX) {
            result = cowrie_new_int64((int64_t)u);
        } else {
            result = cowrie_new_uint64(u);
        }
    }

    free(num_str);
    return result;
}

static COWRIEValue* parse_array(JsonParser *p) {
    if (expect(p, '[') != 0) return NULL;

    COWRIEValue *arr = cowrie_new_array();
    if (!arr) return NULL;

    skip_whitespace(p);
    if (peek(p) == ']') {
        next(p);
        return arr;
    }

    while (1) {
        COWRIEValue *item = parse_value(p);
        if (!item) {
            cowrie_free(arr);
            return NULL;
        }
        if (cowrie_array_append(arr, item) != 0) {
            cowrie_free(item);
            cowrie_free(arr);
            return NULL;
        }

        skip_whitespace(p);
        int c = peek(p);
        if (c == ']') {
            next(p);
            return arr;
        }
        if (c != ',') {
            cowrie_free(arr);
            return NULL;
        }
        next(p);
    }
}

/* Forward declaration for special type handling */
static COWRIEValue* handle_special_object(COWRIEValue *obj);

static COWRIEValue* parse_object(JsonParser *p) {
    if (expect(p, '{') != 0) return NULL;

    COWRIEValue *obj = cowrie_new_object();
    if (!obj) return NULL;

    skip_whitespace(p);
    if (peek(p) == '}') {
        next(p);
        return obj;
    }

    while (1) {
        size_t key_len;
        char *key = parse_string_content(p, &key_len);
        if (!key) {
            cowrie_free(obj);
            return NULL;
        }

        if (expect(p, ':') != 0) {
            free(key);
            cowrie_free(obj);
            return NULL;
        }

        COWRIEValue *value = parse_value(p);
        if (!value) {
            free(key);
            cowrie_free(obj);
            return NULL;
        }

        if (cowrie_object_set(obj, key, key_len, value) != 0) {
            free(key);
            cowrie_free(value);
            cowrie_free(obj);
            return NULL;
        }
        free(key);

        skip_whitespace(p);
        int c = peek(p);
        if (c == '}') {
            next(p);
            /* Check for special _type handling */
            COWRIEValue *special = handle_special_object(obj);
            if (special) {
                cowrie_free(obj);
                return special;
            }
            return obj;
        }
        if (c != ',') {
            cowrie_free(obj);
            return NULL;
        }
        next(p);
    }
}

static COWRIEValue* parse_value(JsonParser *p) {
    skip_whitespace(p);
    int c = peek(p);

    if (c == '"') return parse_string(p);
    if (c == '[') return parse_array(p);
    if (c == '{') return parse_object(p);
    if (c == '-' || (c >= '0' && c <= '9')) return parse_number(p);

    /* Keywords */
    if (p->len - p->pos >= 4 && strncmp(p->data + p->pos, "null", 4) == 0) {
        p->pos += 4;
        return cowrie_new_null();
    }
    if (p->len - p->pos >= 4 && strncmp(p->data + p->pos, "true", 4) == 0) {
        p->pos += 4;
        return cowrie_new_bool(1);
    }
    if (p->len - p->pos >= 5 && strncmp(p->data + p->pos, "false", 5) == 0) {
        p->pos += 5;
        return cowrie_new_bool(0);
    }

    return NULL;
}

/* Handle special _type objects */
static COWRIEValue* handle_special_object(COWRIEValue *obj) {
    if (obj->type != COWRIE_OBJECT) return NULL;

    COWRIEValue *type_val = cowrie_object_get(obj, "_type", 5);
    if (!type_val || type_val->type != COWRIE_STRING) return NULL;

    const char *type_name = type_val->as.str.data;
    size_t type_len = type_val->as.str.len;

    /* Tensor: {"_type":"tensor", "dtype":"float32", "dims":[...], "data":"base64"} */
    if (type_len == 6 && memcmp(type_name, "tensor", 6) == 0) {
        COWRIEValue *dtype_val = cowrie_object_get(obj, "dtype", 5);
        COWRIEValue *dims_val = cowrie_object_get(obj, "dims", 4);
        COWRIEValue *data_val = cowrie_object_get(obj, "data", 4);

        if (!dtype_val || dtype_val->type != COWRIE_STRING) return NULL;
        if (!dims_val || dims_val->type != COWRIE_ARRAY) return NULL;
        if (!data_val || data_val->type != COWRIE_STRING) return NULL;

        /* Parse dtype */
        uint8_t dtype;
        const char *ds = dtype_val->as.str.data;
        size_t dl = dtype_val->as.str.len;
        if (dl == 7 && memcmp(ds, "float32", 7) == 0) dtype = COWRIE_DTYPE_FLOAT32;
        else if (dl == 7 && memcmp(ds, "float64", 7) == 0) dtype = COWRIE_DTYPE_FLOAT64;
        else if (dl == 4 && memcmp(ds, "int8", 4) == 0) dtype = COWRIE_DTYPE_INT8;
        else if (dl == 5 && memcmp(ds, "int16", 5) == 0) dtype = COWRIE_DTYPE_INT16;
        else if (dl == 5 && memcmp(ds, "int32", 5) == 0) dtype = COWRIE_DTYPE_INT32;
        else if (dl == 5 && memcmp(ds, "int64", 5) == 0) dtype = COWRIE_DTYPE_INT64;
        else if (dl == 5 && memcmp(ds, "uint8", 5) == 0) dtype = COWRIE_DTYPE_UINT8;
        else if (dl == 6 && memcmp(ds, "uint16", 6) == 0) dtype = COWRIE_DTYPE_UINT16;
        else if (dl == 6 && memcmp(ds, "uint32", 6) == 0) dtype = COWRIE_DTYPE_UINT32;
        else if (dl == 6 && memcmp(ds, "uint64", 6) == 0) dtype = COWRIE_DTYPE_UINT64;
        else if (dl == 4 && memcmp(ds, "bool", 4) == 0) dtype = COWRIE_DTYPE_BOOL;
        else if (dl == 8 && memcmp(ds, "bfloat16", 8) == 0) dtype = COWRIE_DTYPE_BFLOAT16;
        else if (dl == 7 && memcmp(ds, "float16", 7) == 0) dtype = COWRIE_DTYPE_FLOAT16;
        else return NULL;

        /* Parse dims */
        uint8_t rank = (uint8_t)dims_val->as.array.len;
        size_t *dims = malloc(rank * sizeof(size_t));
        if (!dims) return NULL;
        for (uint8_t i = 0; i < rank; i++) {
            COWRIEValue *dim = dims_val->as.array.items[i];
            if (dim->type == COWRIE_INT64) dims[i] = (size_t)dim->as.i64;
            else if (dim->type == COWRIE_UINT64) dims[i] = (size_t)dim->as.u64;
            else { free(dims); return NULL; }
        }

        /* Decode base64 data */
        COWRIEBuf data_buf;
        if (cowrie_base64_decode(data_val->as.str.data, data_val->as.str.len, &data_buf) != 0) {
            free(dims);
            return NULL;
        }

        COWRIEValue *tensor = cowrie_new_tensor(dtype, rank, dims, data_buf.data, data_buf.len);
        free(dims);
        free(data_buf.data);
        return tensor;
    }

    /* Bytes: {"_type":"bytes", "data":"base64"} */
    if (type_len == 5 && memcmp(type_name, "bytes", 5) == 0) {
        COWRIEValue *data_val = cowrie_object_get(obj, "data", 4);
        if (!data_val || data_val->type != COWRIE_STRING) return NULL;

        COWRIEBuf data_buf;
        if (cowrie_base64_decode(data_val->as.str.data, data_val->as.str.len, &data_buf) != 0) {
            return NULL;
        }

        COWRIEValue *bytes = cowrie_new_bytes(data_buf.data, data_buf.len);
        free(data_buf.data);
        return bytes;
    }

    /* DateTime: {"_type":"datetime", "nanos":123456789} */
    if (type_len == 8 && memcmp(type_name, "datetime", 8) == 0) {
        COWRIEValue *nanos_val = cowrie_object_get(obj, "nanos", 5);
        if (!nanos_val) return NULL;

        int64_t nanos;
        if (nanos_val->type == COWRIE_INT64) nanos = nanos_val->as.i64;
        else if (nanos_val->type == COWRIE_UINT64) nanos = (int64_t)nanos_val->as.u64;
        else return NULL;

        return cowrie_new_datetime64(nanos);
    }

    /* UUID: {"_type":"uuid", "hex":"550e8400-e29b-41d4-a716-446655440000"} */
    if (type_len == 4 && memcmp(type_name, "uuid", 4) == 0) {
        COWRIEValue *hex_val = cowrie_object_get(obj, "hex", 3);
        if (!hex_val || hex_val->type != COWRIE_STRING) return NULL;

        /* Parse hex, ignoring dashes */
        uint8_t uuid[16];
        size_t uuid_idx = 0;
        const char *hex = hex_val->as.str.data;
        size_t hex_len = hex_val->as.str.len;

        for (size_t i = 0; i < hex_len && uuid_idx < 16; i++) {
            if (hex[i] == '-') continue;
            if (i + 1 >= hex_len) return NULL;

            int hi, lo;
            char c = hex[i];
            if (c >= '0' && c <= '9') hi = c - '0';
            else if (c >= 'a' && c <= 'f') hi = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') hi = c - 'A' + 10;
            else return NULL;

            c = hex[++i];
            if (c >= '0' && c <= '9') lo = c - '0';
            else if (c >= 'a' && c <= 'f') lo = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') lo = c - 'A' + 10;
            else return NULL;

            uuid[uuid_idx++] = (hi << 4) | lo;
        }

        if (uuid_idx != 16) return NULL;
        return cowrie_new_uuid128(uuid);
    }

    /* Ext: {"_type":"ext","ext_type":123,"payload":"base64"} */
    if (type_len == 3 && memcmp(type_name, "ext", 3) == 0) {
        COWRIEValue *type_id_val = cowrie_object_get(obj, "ext_type", 8);
        COWRIEValue *payload_val = cowrie_object_get(obj, "payload", 7);
        if (!type_id_val || !payload_val) return NULL;
        if (payload_val->type != COWRIE_STRING) return NULL;

        uint64_t ext_type = 0;
        if (type_id_val->type == COWRIE_INT64) ext_type = (uint64_t)type_id_val->as.i64;
        else if (type_id_val->type == COWRIE_UINT64) ext_type = type_id_val->as.u64;
        else return NULL;

        COWRIEBuf data_buf;
        if (cowrie_base64_decode(payload_val->as.str.data, payload_val->as.str.len, &data_buf) != 0) {
            return NULL;
        }

        COWRIEValue *ext = cowrie_new_ext(ext_type, data_buf.data, data_buf.len);
        free(data_buf.data);
        return ext;
    }

    return NULL;
}

int cowrie_from_json(const char *json, size_t len, COWRIEValue **out) {
    JsonParser p = { json, len, 0 };
    *out = parse_value(&p);
    return *out ? 0 : -1;
}

/* ============================================================
 * JSON Serializer
 * ============================================================ */

static int buf_grow(COWRIEBuf *buf, size_t need) {
    if (buf->len + need <= buf->cap) return 0;
    size_t new_cap = buf->cap * 2;
    if (new_cap < buf->len + need) new_cap = buf->len + need;
    uint8_t *new_data = realloc(buf->data, new_cap);
    if (!new_data) return -1;
    buf->data = new_data;
    buf->cap = new_cap;
    return 0;
}

static int buf_append(COWRIEBuf *buf, const char *s, size_t len) {
    if (buf_grow(buf, len) != 0) return -1;
    memcpy(buf->data + buf->len, s, len);
    buf->len += len;
    return 0;
}

static int buf_append_char(COWRIEBuf *buf, char c) {
    return buf_append(buf, &c, 1);
}

static int write_string_escaped(COWRIEBuf *buf, const char *s, size_t len) {
    if (buf_append_char(buf, '"') != 0) return -1;

    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        switch (c) {
            case '"':  if (buf_append(buf, "\\\"", 2) != 0) return -1; break;
            case '\\': if (buf_append(buf, "\\\\", 2) != 0) return -1; break;
            case '\b': if (buf_append(buf, "\\b", 2) != 0) return -1; break;
            case '\f': if (buf_append(buf, "\\f", 2) != 0) return -1; break;
            case '\n': if (buf_append(buf, "\\n", 2) != 0) return -1; break;
            case '\r': if (buf_append(buf, "\\r", 2) != 0) return -1; break;
            case '\t': if (buf_append(buf, "\\t", 2) != 0) return -1; break;
            default:
                if ((unsigned char)c < 0x20) {
                    char esc[7];
                    snprintf(esc, sizeof(esc), "\\u%04x", (unsigned char)c);
                    if (buf_append(buf, esc, 6) != 0) return -1;
                } else {
                    if (buf_append_char(buf, c) != 0) return -1;
                }
        }
    }

    return buf_append_char(buf, '"');
}

static int write_value(COWRIEBuf *buf, const COWRIEValue *v, int pretty, int depth);

static int write_indent(COWRIEBuf *buf, int depth) {
    for (int i = 0; i < depth; i++) {
        if (buf_append(buf, "  ", 2) != 0) return -1;
    }
    return 0;
}

static int write_value(COWRIEBuf *buf, const COWRIEValue *v, int pretty, int depth) {
    char num_buf[64];

    switch (v->type) {
        case COWRIE_NULL:
            return buf_append(buf, "null", 4);

        case COWRIE_BOOL:
            return v->as.boolean ? buf_append(buf, "true", 4) : buf_append(buf, "false", 5);

        case COWRIE_INT64:
            snprintf(num_buf, sizeof(num_buf), "%lld", (long long)v->as.i64);
            return buf_append(buf, num_buf, strlen(num_buf));

        case COWRIE_UINT64:
            snprintf(num_buf, sizeof(num_buf), "%llu", (unsigned long long)v->as.u64);
            return buf_append(buf, num_buf, strlen(num_buf));

        case COWRIE_FLOAT64:
            if (isnan(v->as.f64) || isinf(v->as.f64)) {
                return buf_append(buf, "null", 4);
            }
            snprintf(num_buf, sizeof(num_buf), "%.17g", v->as.f64);
            return buf_append(buf, num_buf, strlen(num_buf));

        case COWRIE_STRING:
            return write_string_escaped(buf, v->as.str.data, v->as.str.len);

        case COWRIE_BYTES: {
            /* {"_type":"bytes","data":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.bytes.data, v->as.bytes.len, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"bytes\",\"data\":\"", 25) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_DATETIME64: {
            /* {"_type":"datetime","nanos":123456789} */
            if (buf_append(buf, "{\"_type\":\"datetime\",\"nanos\":", 28) != 0) return -1;
            snprintf(num_buf, sizeof(num_buf), "%lld", (long long)v->as.datetime64);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) return -1;
            return buf_append_char(buf, '}');
        }

        case COWRIE_UUID128: {
            /* {"_type":"uuid","hex":"550e8400-e29b-41d4-a716-446655440000"} */
            char hex[64];
            snprintf(hex, sizeof(hex),
                "{\"_type\":\"uuid\",\"hex\":\"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x\"}",
                v->as.uuid[0], v->as.uuid[1], v->as.uuid[2], v->as.uuid[3],
                v->as.uuid[4], v->as.uuid[5], v->as.uuid[6], v->as.uuid[7],
                v->as.uuid[8], v->as.uuid[9], v->as.uuid[10], v->as.uuid[11],
                v->as.uuid[12], v->as.uuid[13], v->as.uuid[14], v->as.uuid[15]);
            return buf_append(buf, hex, strlen(hex));
        }

        case COWRIE_ARRAY: {
            if (buf_append_char(buf, '[') != 0) return -1;
            for (size_t i = 0; i < v->as.array.len; i++) {
                if (i > 0 && buf_append_char(buf, ',') != 0) return -1;
                if (pretty) {
                    if (buf_append_char(buf, '\n') != 0) return -1;
                    if (write_indent(buf, depth + 1) != 0) return -1;
                }
                if (write_value(buf, v->as.array.items[i], pretty, depth + 1) != 0) return -1;
            }
            if (pretty && v->as.array.len > 0) {
                if (buf_append_char(buf, '\n') != 0) return -1;
                if (write_indent(buf, depth) != 0) return -1;
            }
            return buf_append_char(buf, ']');
        }

        case COWRIE_OBJECT: {
            if (buf_append_char(buf, '{') != 0) return -1;
            for (size_t i = 0; i < v->as.object.len; i++) {
                if (i > 0 && buf_append_char(buf, ',') != 0) return -1;
                if (pretty) {
                    if (buf_append_char(buf, '\n') != 0) return -1;
                    if (write_indent(buf, depth + 1) != 0) return -1;
                }
                if (write_string_escaped(buf, v->as.object.members[i].key,
                                          v->as.object.members[i].key_len) != 0) return -1;
                if (buf_append_char(buf, ':') != 0) return -1;
                if (pretty && buf_append_char(buf, ' ') != 0) return -1;
                if (write_value(buf, v->as.object.members[i].value, pretty, depth + 1) != 0) return -1;
            }
            if (pretty && v->as.object.len > 0) {
                if (buf_append_char(buf, '\n') != 0) return -1;
                if (write_indent(buf, depth) != 0) return -1;
            }
            return buf_append_char(buf, '}');
        }

        case COWRIE_TENSOR: {
            /* {"_type":"tensor","dtype":"float32","dims":[...],"data":"base64"} */
            const char *dtype_str;
            switch (v->as.tensor.dtype) {
                case COWRIE_DTYPE_FLOAT32: dtype_str = "float32"; break;
                case COWRIE_DTYPE_FLOAT64: dtype_str = "float64"; break;
                case COWRIE_DTYPE_INT8: dtype_str = "int8"; break;
                case COWRIE_DTYPE_INT16: dtype_str = "int16"; break;
                case COWRIE_DTYPE_INT32: dtype_str = "int32"; break;
                case COWRIE_DTYPE_INT64: dtype_str = "int64"; break;
                case COWRIE_DTYPE_UINT8: dtype_str = "uint8"; break;
                case COWRIE_DTYPE_UINT16: dtype_str = "uint16"; break;
                case COWRIE_DTYPE_UINT32: dtype_str = "uint32"; break;
                case COWRIE_DTYPE_UINT64: dtype_str = "uint64"; break;
                case COWRIE_DTYPE_BOOL: dtype_str = "bool"; break;
                case COWRIE_DTYPE_BFLOAT16: dtype_str = "bfloat16"; break;
                case COWRIE_DTYPE_FLOAT16: dtype_str = "float16"; break;
                default: dtype_str = "unknown";
            }

            if (buf_append(buf, "{\"_type\":\"tensor\",\"dtype\":\"", 27) != 0) return -1;
            if (buf_append(buf, dtype_str, strlen(dtype_str)) != 0) return -1;
            if (buf_append(buf, "\",\"dims\":[", 10) != 0) return -1;

            for (uint8_t i = 0; i < v->as.tensor.rank; i++) {
                if (i > 0 && buf_append_char(buf, ',') != 0) return -1;
                snprintf(num_buf, sizeof(num_buf), "%zu", v->as.tensor.dims[i]);
                if (buf_append(buf, num_buf, strlen(num_buf)) != 0) return -1;
            }

            if (buf_append(buf, "],\"data\":\"", 10) != 0) return -1;

            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.tensor.data, v->as.tensor.data_len, &b64) != 0) return -1;
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);

            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_TENSOR_REF: {
            /* {"_type":"tensor_ref","store_id":0,"key":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.tensor_ref.key, v->as.tensor_ref.key_len, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"tensor_ref\",\"store_id\":", 33) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.tensor_ref.store_id);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"key\":\"", 8) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_IMAGE: {
            /* {"_type":"image","format":1,"width":100,"height":100,"data":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.image.data, v->as.image.data_len, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"image\",\"format\":", 26) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.image.format);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"width\":", 9) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.image.width);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"height\":", 10) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.image.height);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"data\":\"", 9) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_AUDIO: {
            /* {"_type":"audio","encoding":1,"sample_rate":44100,"channels":2,"data":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.audio.data, v->as.audio.data_len, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"audio\",\"encoding\":", 28) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.audio.encoding);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"sample_rate\":", 15) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.audio.sample_rate);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"channels\":", 12) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%u", v->as.audio.channels);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"data\":\"", 9) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_DECIMAL128: {
            /* {"_type":"decimal128","data":"base64"} */
            uint8_t dec_data[17];
            dec_data[0] = (uint8_t)v->as.decimal128.scale;
            memcpy(dec_data + 1, v->as.decimal128.coef, 16);

            COWRIEBuf b64;
            if (cowrie_base64_encode(dec_data, 17, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"decimal128\",\"data\":\"", 30) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_BIGINT: {
            /* {"_type":"bigint","data":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.bigint.data, v->as.bigint.len, &b64) != 0) return -1;

            if (buf_append(buf, "{\"_type\":\"bigint\",\"data\":\"", 26) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        case COWRIE_EXT: {
            /* {"_type":"ext","ext_type":123,"payload":"base64"} */
            COWRIEBuf b64;
            if (cowrie_base64_encode(v->as.ext.payload, v->as.ext.payload_len, &b64) != 0) return -1;
            if (buf_append(buf, "{\"_type\":\"ext\",\"ext_type\":", 26) != 0) { free(b64.data); return -1; }
            snprintf(num_buf, sizeof(num_buf), "%llu", (unsigned long long)v->as.ext.ext_type);
            if (buf_append(buf, num_buf, strlen(num_buf)) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, ",\"payload\":\"", 12) != 0) { free(b64.data); return -1; }
            if (buf_append(buf, (char*)b64.data, b64.len) != 0) { free(b64.data); return -1; }
            free(b64.data);
            return buf_append(buf, "\"}", 2);
        }

        default:
            /* For unsupported types, output null */
            return buf_append(buf, "null", 4);
    }
}

int cowrie_to_json(const COWRIEValue *value, COWRIEBuf *buf) {
    cowrie_buf_init(buf);
    buf->data = malloc(256);
    if (!buf->data) return -1;
    buf->cap = 256;
    buf->len = 0;

    if (write_value(buf, value, 0, 0) != 0) {
        free(buf->data);
        buf->data = NULL;
        return -1;
    }

    /* Null terminate */
    if (buf_append_char(buf, '\0') != 0) {
        free(buf->data);
        buf->data = NULL;
        return -1;
    }
    buf->len--; /* Don't include null in length */

    return 0;
}

int cowrie_to_json_pretty(const COWRIEValue *value, COWRIEBuf *buf) {
    cowrie_buf_init(buf);
    buf->data = malloc(256);
    if (!buf->data) return -1;
    buf->cap = 256;
    buf->len = 0;

    if (write_value(buf, value, 1, 0) != 0) {
        free(buf->data);
        buf->data = NULL;
        return -1;
    }

    /* Add trailing newline and null terminate */
    if (buf_append_char(buf, '\n') != 0 || buf_append_char(buf, '\0') != 0) {
        free(buf->data);
        buf->data = NULL;
        return -1;
    }
    buf->len--; /* Don't include null in length */

    return 0;
}
