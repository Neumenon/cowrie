/*
 * COWRIE Gen1 - Lightweight Binary JSON with Proto-Tensors
 * Implementation
 */

#include "../include/cowrie_gen1.h"
#include <stdlib.h>
#include <string.h>

/* ============================================================
 * Buffer Operations
 * ============================================================ */

void cowrie_g1_buf_init(cowrie_g1_buf_t *buf) {
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

void cowrie_g1_buf_free(cowrie_g1_buf_t *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

int cowrie_g1_buf_reserve(cowrie_g1_buf_t *buf, size_t extra) {
    /* Security: check for overflow before addition */
    if (extra > SIZE_MAX - buf->len) return COWRIE_G1_ERR_OVERFLOW;

    size_t needed = buf->len + extra;
    if (needed <= buf->cap) return COWRIE_G1_OK;

    size_t new_cap = buf->cap ? buf->cap * 2 : 256;
    /* Prevent infinite loop and overflow in capacity growth */
    while (new_cap < needed) {
        if (new_cap > SIZE_MAX / 2) {
            new_cap = needed; /* Can't double, use exact size */
            break;
        }
        new_cap *= 2;
    }

    uint8_t *new_data = realloc(buf->data, new_cap);
    if (!new_data) return COWRIE_G1_ERR_NOMEM;

    buf->data = new_data;
    buf->cap = new_cap;
    return COWRIE_G1_OK;
}

int cowrie_g1_buf_write(cowrie_g1_buf_t *buf, const void *data, size_t len) {
    int err = cowrie_g1_buf_reserve(buf, len);
    if (err) return err;
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    return COWRIE_G1_OK;
}

int cowrie_g1_buf_write_byte(cowrie_g1_buf_t *buf, uint8_t byte) {
    return cowrie_g1_buf_write(buf, &byte, 1);
}

int cowrie_g1_buf_write_uvarint(cowrie_g1_buf_t *buf, uint64_t val) {
    uint8_t bytes[10];
    int n = 0;
    while (val >= 0x80) {
        bytes[n++] = (uint8_t)(val | 0x80);
        val >>= 7;
    }
    bytes[n++] = (uint8_t)val;
    return cowrie_g1_buf_write(buf, bytes, n);
}

/* ============================================================
 * Value Constructors
 * ============================================================ */

static cowrie_g1_value_t *alloc_value(cowrie_g1_type_t type) {
    cowrie_g1_value_t *v = calloc(1, sizeof(cowrie_g1_value_t));
    if (v) v->type = type;
    return v;
}

cowrie_g1_value_t *cowrie_g1_null(void) {
    return alloc_value(COWRIE_G1_TYPE_NULL);
}

cowrie_g1_value_t *cowrie_g1_bool(bool val) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_BOOL);
    if (v) v->bool_val = val;
    return v;
}

cowrie_g1_value_t *cowrie_g1_int64(int64_t val) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_INT64);
    if (v) v->int64_val = val;
    return v;
}

cowrie_g1_value_t *cowrie_g1_float64(double val) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_FLOAT64);
    if (v) v->float64_val = val;
    return v;
}

cowrie_g1_value_t *cowrie_g1_string(const char *str, size_t len) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_STRING);
    if (!v) return NULL;

    v->string_val.data = malloc(len + 1);
    if (!v->string_val.data) {
        free(v);
        return NULL;
    }
    memcpy(v->string_val.data, str, len);
    v->string_val.data[len] = '\0';
    v->string_val.len = len;
    return v;
}

cowrie_g1_value_t *cowrie_g1_bytes(const uint8_t *data, size_t len) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_BYTES);
    if (!v) return NULL;

    v->bytes_val.data = malloc(len);
    if (!v->bytes_val.data) {
        free(v);
        return NULL;
    }
    memcpy(v->bytes_val.data, data, len);
    v->bytes_val.len = len;
    return v;
}

cowrie_g1_value_t *cowrie_g1_array(size_t capacity) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_ARRAY);
    if (!v) return NULL;

    if (capacity > 0) {
        v->array_val.items = calloc(capacity, sizeof(cowrie_g1_value_t *));
        if (!v->array_val.items) {
            free(v);
            return NULL;
        }
        v->array_val.cap = capacity;
    } else {
        v->array_val.items = NULL;
        v->array_val.cap = 0;
    }
    v->array_val.len = 0;
    return v;
}

cowrie_g1_value_t *cowrie_g1_object(size_t capacity) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_OBJECT);
    if (!v) return NULL;

    if (capacity > 0) {
        v->object_val.members = calloc(capacity, sizeof(cowrie_g1_member_t));
        if (!v->object_val.members) {
            free(v);
            return NULL;
        }
        v->object_val.cap = capacity;
    } else {
        v->object_val.members = NULL;
        v->object_val.cap = 0;
    }
    v->object_val.len = 0;
    return v;
}

cowrie_g1_value_t *cowrie_g1_int64_array(const int64_t *data, size_t len) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_INT64_ARRAY);
    if (!v) return NULL;

    v->int64_array_val.data = malloc(len * sizeof(int64_t));
    if (!v->int64_array_val.data) {
        free(v);
        return NULL;
    }
    memcpy(v->int64_array_val.data, data, len * sizeof(int64_t));
    v->int64_array_val.len = len;
    return v;
}

cowrie_g1_value_t *cowrie_g1_float64_array(const double *data, size_t len) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_FLOAT64_ARRAY);
    if (!v) return NULL;

    v->float64_array_val.data = malloc(len * sizeof(double));
    if (!v->float64_array_val.data) {
        free(v);
        return NULL;
    }
    memcpy(v->float64_array_val.data, data, len * sizeof(double));
    v->float64_array_val.len = len;
    return v;
}

cowrie_g1_value_t *cowrie_g1_string_array(const char **strings, size_t count) {
    cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_STRING_ARRAY);
    if (!v) return NULL;

    if (count > 0) {
        v->string_array_val.data = malloc(count * sizeof(char *));
        if (!v->string_array_val.data) {
            free(v);
            return NULL;
        }
        for (size_t i = 0; i < count; i++) {
            v->string_array_val.data[i] = strdup(strings[i]);
            if (!v->string_array_val.data[i]) {
                /* Rollback on allocation failure */
                for (size_t j = 0; j < i; j++) {
                    free(v->string_array_val.data[j]);
                }
                free(v->string_array_val.data);
                free(v);
                return NULL;
            }
        }
    } else {
        v->string_array_val.data = NULL;
    }
    v->string_array_val.len = count;
    return v;
}

int cowrie_g1_array_append(cowrie_g1_value_t *arr, cowrie_g1_value_t *val) {
    if (!arr || arr->type != COWRIE_G1_TYPE_ARRAY) return COWRIE_G1_ERR_INVALID;

    /* Check if we need to grow the array */
    if (arr->array_val.len >= arr->array_val.cap) {
        /* Exponential growth: double capacity, min 4 */
        size_t new_cap = arr->array_val.cap ? arr->array_val.cap * 2 : 4;
        cowrie_g1_value_t **new_items = realloc(arr->array_val.items, new_cap * sizeof(cowrie_g1_value_t *));
        if (!new_items) return COWRIE_G1_ERR_NOMEM;
        arr->array_val.items = new_items;
        arr->array_val.cap = new_cap;
    }

    arr->array_val.items[arr->array_val.len] = val;
    arr->array_val.len++;
    return COWRIE_G1_OK;
}

int cowrie_g1_object_set(cowrie_g1_value_t *obj, const char *key, cowrie_g1_value_t *val) {
    if (!obj || obj->type != COWRIE_G1_TYPE_OBJECT) return COWRIE_G1_ERR_INVALID;

    /* Check if key already exists */
    for (size_t i = 0; i < obj->object_val.len; i++) {
        if (strcmp(obj->object_val.members[i].key, key) == 0) {
            cowrie_g1_value_free(obj->object_val.members[i].value);
            obj->object_val.members[i].value = val;
            return COWRIE_G1_OK;
        }
    }

    /* Check if we need to grow the members array */
    if (obj->object_val.len >= obj->object_val.cap) {
        /* Exponential growth: double capacity, min 4 */
        size_t new_cap = obj->object_val.cap ? obj->object_val.cap * 2 : 4;
        cowrie_g1_member_t *new_members = realloc(obj->object_val.members, new_cap * sizeof(cowrie_g1_member_t));
        if (!new_members) return COWRIE_G1_ERR_NOMEM;
        obj->object_val.members = new_members;
        obj->object_val.cap = new_cap;
    }

    /* Add new member */
    char *key_copy = strdup(key);
    if (!key_copy) return COWRIE_G1_ERR_NOMEM;

    obj->object_val.members[obj->object_val.len].key = key_copy;
    obj->object_val.members[obj->object_val.len].value = val;
    obj->object_val.len++;
    return COWRIE_G1_OK;
}

void cowrie_g1_value_free(cowrie_g1_value_t *val) {
    if (!val) return;

    switch (val->type) {
    case COWRIE_G1_TYPE_STRING:
        free(val->string_val.data);
        break;
    case COWRIE_G1_TYPE_BYTES:
        free(val->bytes_val.data);
        break;
    case COWRIE_G1_TYPE_ARRAY:
        for (size_t i = 0; i < val->array_val.len; i++) {
            cowrie_g1_value_free(val->array_val.items[i]);
        }
        free(val->array_val.items);
        break;
    case COWRIE_G1_TYPE_OBJECT:
        for (size_t i = 0; i < val->object_val.len; i++) {
            free(val->object_val.members[i].key);
            cowrie_g1_value_free(val->object_val.members[i].value);
        }
        free(val->object_val.members);
        break;
    case COWRIE_G1_TYPE_INT64_ARRAY:
        free(val->int64_array_val.data);
        break;
    case COWRIE_G1_TYPE_FLOAT64_ARRAY:
        free(val->float64_array_val.data);
        break;
    case COWRIE_G1_TYPE_STRING_ARRAY:
        for (size_t i = 0; i < val->string_array_val.len; i++) {
            free(val->string_array_val.data[i]);
        }
        free(val->string_array_val.data);
        break;
    default:
        break;
    }

    free(val);
}

/* ============================================================
 * Encode
 * ============================================================ */

static int encode_value(const cowrie_g1_value_t *val, cowrie_g1_buf_t *buf);

static int encode_value(const cowrie_g1_value_t *val, cowrie_g1_buf_t *buf) {
    int err;

    if (!val) {
        return cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_NULL);
    }

    switch (val->type) {
    case COWRIE_G1_TYPE_NULL:
        return cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_NULL);

    case COWRIE_G1_TYPE_BOOL:
        return cowrie_g1_buf_write_byte(buf, val->bool_val ? COWRIE_G1_TAG_TRUE : COWRIE_G1_TAG_FALSE);

    case COWRIE_G1_TYPE_INT64:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_INT64);
        if (err) return err;
        return cowrie_g1_buf_write_uvarint(buf, cowrie_g1_zigzag_encode(val->int64_val));

    case COWRIE_G1_TYPE_FLOAT64:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_FLOAT64);
        if (err) return err;
        return cowrie_g1_buf_write(buf, &val->float64_val, 8);

    case COWRIE_G1_TYPE_STRING:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_STRING);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->string_val.len);
        if (err) return err;
        return cowrie_g1_buf_write(buf, val->string_val.data, val->string_val.len);

    case COWRIE_G1_TYPE_BYTES:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_BYTES);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->bytes_val.len);
        if (err) return err;
        return cowrie_g1_buf_write(buf, val->bytes_val.data, val->bytes_val.len);

    case COWRIE_G1_TYPE_ARRAY:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_ARRAY);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->array_val.len);
        if (err) return err;
        for (size_t i = 0; i < val->array_val.len; i++) {
            err = encode_value(val->array_val.items[i], buf);
            if (err) return err;
        }
        return COWRIE_G1_OK;

    case COWRIE_G1_TYPE_OBJECT:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_OBJECT);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->object_val.len);
        if (err) return err;
        for (size_t i = 0; i < val->object_val.len; i++) {
            size_t key_len = strlen(val->object_val.members[i].key);
            err = cowrie_g1_buf_write_uvarint(buf, key_len);
            if (err) return err;
            err = cowrie_g1_buf_write(buf, val->object_val.members[i].key, key_len);
            if (err) return err;
            err = encode_value(val->object_val.members[i].value, buf);
            if (err) return err;
        }
        return COWRIE_G1_OK;

    case COWRIE_G1_TYPE_INT64_ARRAY:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_INT64_ARRAY);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->int64_array_val.len);
        if (err) return err;
        return cowrie_g1_buf_write(buf, val->int64_array_val.data, val->int64_array_val.len * 8);

    case COWRIE_G1_TYPE_FLOAT64_ARRAY:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_FLOAT64_ARRAY);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->float64_array_val.len);
        if (err) return err;
        return cowrie_g1_buf_write(buf, val->float64_array_val.data, val->float64_array_val.len * 8);

    case COWRIE_G1_TYPE_STRING_ARRAY:
        err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_STRING_ARRAY);
        if (err) return err;
        err = cowrie_g1_buf_write_uvarint(buf, val->string_array_val.len);
        if (err) return err;
        for (size_t i = 0; i < val->string_array_val.len; i++) {
            size_t str_len = strlen(val->string_array_val.data[i]);
            err = cowrie_g1_buf_write_uvarint(buf, str_len);
            if (err) return err;
            err = cowrie_g1_buf_write(buf, val->string_array_val.data[i], str_len);
            if (err) return err;
        }
        return COWRIE_G1_OK;

    default:
        return COWRIE_G1_ERR_INVALID;
    }
}

int cowrie_g1_encode(const cowrie_g1_value_t *val, cowrie_g1_buf_t *buf) {
    cowrie_g1_buf_init(buf);
    return encode_value(val, buf);
}

/* ============================================================
 * Decode
 * ============================================================ */

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
} reader_t;

static int read_byte(reader_t *r, uint8_t *out) {
    if (r->pos >= r->len) return COWRIE_G1_ERR_EOF;
    *out = r->data[r->pos++];
    return COWRIE_G1_OK;
}

static int read_bytes(reader_t *r, void *out, size_t len) {
    if (r->pos + len > r->len) return COWRIE_G1_ERR_EOF;
    memcpy(out, r->data + r->pos, len);
    r->pos += len;
    return COWRIE_G1_OK;
}

static int read_uvarint(reader_t *r, uint64_t *out) {
    uint64_t result = 0;
    int shift = 0;
    uint8_t byte;

    while (1) {
        int err = read_byte(r, &byte);
        if (err) return err;
        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
        if (shift > 63) return COWRIE_G1_ERR_OVERFLOW;
    }

    *out = result;
    return COWRIE_G1_OK;
}

static int decode_value_depth(reader_t *r, cowrie_g1_value_t **out, int depth);

static int decode_value_depth(reader_t *r, cowrie_g1_value_t **out, int depth) {
    /* Security: check depth limit */
    if (depth > COWRIE_G1_MAX_DEPTH) return COWRIE_G1_ERR_DEPTH;

    uint8_t tag;
    int err = read_byte(r, &tag);
    if (err) return err;

    switch (tag) {
    case COWRIE_G1_TAG_NULL:
        *out = cowrie_g1_null();
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;

    case COWRIE_G1_TAG_FALSE:
        *out = cowrie_g1_bool(false);
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;

    case COWRIE_G1_TAG_TRUE:
        *out = cowrie_g1_bool(true);
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;

    case COWRIE_G1_TAG_INT64: {
        uint64_t zz;
        err = read_uvarint(r, &zz);
        if (err) return err;
        *out = cowrie_g1_int64(cowrie_g1_zigzag_decode(zz));
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_FLOAT64: {
        double val;
        err = read_bytes(r, &val, 8);
        if (err) return err;
        *out = cowrie_g1_float64(val);
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_STRING: {
        uint64_t len;
        err = read_uvarint(r, &len);
        if (err) return err;
        if (len > COWRIE_G1_MAX_STRING_LEN) return COWRIE_G1_ERR_STRING_LEN;
        if (r->pos + len > r->len) return COWRIE_G1_ERR_EOF;
        *out = cowrie_g1_string((const char *)(r->data + r->pos), len);
        r->pos += len;
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_BYTES: {
        uint64_t len;
        err = read_uvarint(r, &len);
        if (err) return err;
        if (len > COWRIE_G1_MAX_BYTES_LEN) return COWRIE_G1_ERR_BYTES_LEN;
        if (r->pos + len > r->len) return COWRIE_G1_ERR_EOF;
        *out = cowrie_g1_bytes(r->data + r->pos, len);
        r->pos += len;
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_ARRAY: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;
        cowrie_g1_value_t *arr = cowrie_g1_array(count);
        if (!arr) return COWRIE_G1_ERR_NOMEM;
        for (uint64_t i = 0; i < count; i++) {
            cowrie_g1_value_t *item;
            err = decode_value_depth(r, &item, depth + 1);
            if (err) {
                cowrie_g1_value_free(arr);
                return err;
            }
            cowrie_g1_array_append(arr, item);
        }
        *out = arr;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_OBJECT: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_OBJECT_LEN) return COWRIE_G1_ERR_OBJECT_LEN;
        cowrie_g1_value_t *obj = cowrie_g1_object(count);
        if (!obj) return COWRIE_G1_ERR_NOMEM;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t key_len;
            err = read_uvarint(r, &key_len);
            if (err) {
                cowrie_g1_value_free(obj);
                return err;
            }
            if (key_len > COWRIE_G1_MAX_STRING_LEN) {
                cowrie_g1_value_free(obj);
                return COWRIE_G1_ERR_STRING_LEN;
            }
            if (r->pos + key_len > r->len) {
                cowrie_g1_value_free(obj);
                return COWRIE_G1_ERR_EOF;
            }
            char *key = malloc(key_len + 1);
            if (!key) {
                cowrie_g1_value_free(obj);
                return COWRIE_G1_ERR_NOMEM;
            }
            memcpy(key, r->data + r->pos, key_len);
            key[key_len] = '\0';
            r->pos += key_len;

            cowrie_g1_value_t *val;
            err = decode_value_depth(r, &val, depth + 1);
            if (err) {
                free(key);
                cowrie_g1_value_free(obj);
                return err;
            }
            err = cowrie_g1_object_set(obj, key, val);
            free(key);
            if (err) {
                cowrie_g1_value_free(val);
                cowrie_g1_value_free(obj);
                return err;
            }
        }
        *out = obj;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_INT64_ARRAY: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;
        /* Security: check for overflow before multiplication */
        if (count > (SIZE_MAX - r->pos) / 8) return COWRIE_G1_ERR_OVERFLOW;
        size_t byte_len = (size_t)count * 8;
        if (r->pos + byte_len > r->len) return COWRIE_G1_ERR_EOF;
        *out = cowrie_g1_int64_array((const int64_t *)(r->data + r->pos), (size_t)count);
        r->pos += byte_len;
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_FLOAT64_ARRAY: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;
        /* Security: check for overflow before multiplication */
        if (count > (SIZE_MAX - r->pos) / 8) return COWRIE_G1_ERR_OVERFLOW;
        size_t byte_len = (size_t)count * 8;
        if (r->pos + byte_len > r->len) return COWRIE_G1_ERR_EOF;
        *out = cowrie_g1_float64_array((const double *)(r->data + r->pos), (size_t)count);
        r->pos += byte_len;
        return *out ? COWRIE_G1_OK : COWRIE_G1_ERR_NOMEM;
    }

    case COWRIE_G1_TAG_STRING_ARRAY: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;

        /* Allocate temporary array to collect strings */
        char **strings = NULL;
        if (count > 0) {
            strings = calloc(count, sizeof(char *));
            if (!strings) return COWRIE_G1_ERR_NOMEM;
        }

        for (uint64_t i = 0; i < count; i++) {
            uint64_t str_len;
            err = read_uvarint(r, &str_len);
            if (err) {
                for (uint64_t j = 0; j < i; j++) free(strings[j]);
                free(strings);
                return err;
            }
            if (str_len > COWRIE_G1_MAX_STRING_LEN) {
                for (uint64_t j = 0; j < i; j++) free(strings[j]);
                free(strings);
                return COWRIE_G1_ERR_STRING_LEN;
            }
            if (r->pos + str_len > r->len) {
                for (uint64_t j = 0; j < i; j++) free(strings[j]);
                free(strings);
                return COWRIE_G1_ERR_EOF;
            }
            strings[i] = malloc(str_len + 1);
            if (!strings[i]) {
                for (uint64_t j = 0; j < i; j++) free(strings[j]);
                free(strings);
                return COWRIE_G1_ERR_NOMEM;
            }
            memcpy(strings[i], r->data + r->pos, str_len);
            strings[i][str_len] = '\0';
            r->pos += str_len;
        }

        /* Create the string array value */
        cowrie_g1_value_t *v = alloc_value(COWRIE_G1_TYPE_STRING_ARRAY);
        if (!v) {
            for (uint64_t i = 0; i < count; i++) free(strings[i]);
            free(strings);
            return COWRIE_G1_ERR_NOMEM;
        }
        v->string_array_val.data = strings;
        v->string_array_val.len = count;
        *out = v;
        return COWRIE_G1_OK;
    }

    default:
        return COWRIE_G1_ERR_INVALID;
    }
}

int cowrie_g1_decode(const uint8_t *data, size_t len, cowrie_g1_value_t **out) {
    reader_t r = {data, len, 0};
    return decode_value_depth(&r, out, 0);
}
