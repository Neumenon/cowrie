/*
 * COWRIE Gen1 - Lightweight Binary JSON with Proto-Tensors
 * Implementation
 */

#include "../include/cowrie_gen1.h"
#include <limits.h>
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
static int encode_object_entries(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_node_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_edge_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_adjlist_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_node_batch_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_edge_batch_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);
static int encode_graph_shard_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf);

static int encode_raw_string(cowrie_g1_buf_t *buf, const char *data, size_t len) {
    int err = cowrie_g1_buf_write_uvarint(buf, len);
    if (err) return err;
    return cowrie_g1_buf_write(buf, data, len);
}

static const cowrie_g1_value_t *object_get_member(const cowrie_g1_value_t *obj, const char *key) {
    if (!obj || obj->type != COWRIE_G1_TYPE_OBJECT) return NULL;
    for (size_t i = 0; i < obj->object_val.len; i++) {
        if (strcmp(obj->object_val.members[i].key, key) == 0) {
            return obj->object_val.members[i].value;
        }
    }
    return NULL;
}

static bool object_has_exact_keys(const cowrie_g1_value_t *obj, const char **keys, size_t key_count) {
    if (!obj || obj->type != COWRIE_G1_TYPE_OBJECT) return false;
    if (obj->object_val.len != key_count) return false;
    for (size_t i = 0; i < key_count; i++) {
        if (!object_get_member(obj, keys[i])) return false;
    }
    return true;
}

static bool is_int_sequence(const cowrie_g1_value_t *v) {
    if (!v) return false;
    if (v->type == COWRIE_G1_TYPE_INT64_ARRAY) return true;
    if (v->type != COWRIE_G1_TYPE_ARRAY) return false;
    for (size_t i = 0; i < v->array_val.len; i++) {
        if (!v->array_val.items[i] || v->array_val.items[i]->type != COWRIE_G1_TYPE_INT64) return false;
    }
    return true;
}

static size_t int_sequence_len(const cowrie_g1_value_t *v) {
    if (v->type == COWRIE_G1_TYPE_INT64_ARRAY) return v->int64_array_val.len;
    return v->array_val.len;
}

static bool int_sequence_get(const cowrie_g1_value_t *v, size_t idx, int64_t *out) {
    if (!v || !out) return false;
    if (v->type == COWRIE_G1_TYPE_INT64_ARRAY) {
        if (idx >= v->int64_array_val.len) return false;
        *out = v->int64_array_val.data[idx];
        return true;
    }
    if (v->type == COWRIE_G1_TYPE_ARRAY) {
        if (idx >= v->array_val.len) return false;
        if (!v->array_val.items[idx] || v->array_val.items[idx]->type != COWRIE_G1_TYPE_INT64) return false;
        *out = v->array_val.items[idx]->int64_val;
        return true;
    }
    return false;
}

static bool is_byte_sequence(const cowrie_g1_value_t *v) {
    if (!v) return false;
    if (v->type == COWRIE_G1_TYPE_BYTES) return true;
    if (v->type != COWRIE_G1_TYPE_ARRAY) return false;
    for (size_t i = 0; i < v->array_val.len; i++) {
        if (!v->array_val.items[i] || v->array_val.items[i]->type != COWRIE_G1_TYPE_INT64) return false;
        int64_t b = v->array_val.items[i]->int64_val;
        if (b < 0 || b > 255) return false;
    }
    return true;
}

static size_t byte_sequence_len(const cowrie_g1_value_t *v) {
    if (v->type == COWRIE_G1_TYPE_BYTES) return v->bytes_val.len;
    return v->array_val.len;
}

static int byte_sequence_write(const cowrie_g1_value_t *v, cowrie_g1_buf_t *buf) {
    if (v->type == COWRIE_G1_TYPE_BYTES) {
        return cowrie_g1_buf_write(buf, v->bytes_val.data, v->bytes_val.len);
    }
    for (size_t i = 0; i < v->array_val.len; i++) {
        uint8_t b = (uint8_t)v->array_val.items[i]->int64_val;
        int err = cowrie_g1_buf_write_byte(buf, b);
        if (err) return err;
    }
    return COWRIE_G1_OK;
}

static bool is_node_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"id", "label", "properties"};
    if (!object_has_exact_keys(obj, keys, 3)) return false;
    const cowrie_g1_value_t *id = object_get_member(obj, "id");
    const cowrie_g1_value_t *label = object_get_member(obj, "label");
    const cowrie_g1_value_t *properties = object_get_member(obj, "properties");
    return id && id->type == COWRIE_G1_TYPE_INT64 &&
           label && label->type == COWRIE_G1_TYPE_STRING &&
           properties && properties->type == COWRIE_G1_TYPE_OBJECT;
}

static bool is_edge_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"src", "dst", "label", "properties"};
    if (!object_has_exact_keys(obj, keys, 4)) return false;
    const cowrie_g1_value_t *src = object_get_member(obj, "src");
    const cowrie_g1_value_t *dst = object_get_member(obj, "dst");
    const cowrie_g1_value_t *label = object_get_member(obj, "label");
    const cowrie_g1_value_t *properties = object_get_member(obj, "properties");
    return src && src->type == COWRIE_G1_TYPE_INT64 &&
           dst && dst->type == COWRIE_G1_TYPE_INT64 &&
           label && label->type == COWRIE_G1_TYPE_STRING &&
           properties && properties->type == COWRIE_G1_TYPE_OBJECT;
}

static bool is_adjlist_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"id_width", "node_count", "edge_count", "row_offsets", "col_indices"};
    if (!object_has_exact_keys(obj, keys, 5)) return false;
    const cowrie_g1_value_t *id_width = object_get_member(obj, "id_width");
    const cowrie_g1_value_t *node_count = object_get_member(obj, "node_count");
    const cowrie_g1_value_t *edge_count = object_get_member(obj, "edge_count");
    const cowrie_g1_value_t *row_offsets = object_get_member(obj, "row_offsets");
    const cowrie_g1_value_t *col_indices = object_get_member(obj, "col_indices");
    return id_width && id_width->type == COWRIE_G1_TYPE_INT64 &&
           node_count && node_count->type == COWRIE_G1_TYPE_INT64 &&
           edge_count && edge_count->type == COWRIE_G1_TYPE_INT64 &&
           row_offsets && is_int_sequence(row_offsets) &&
           col_indices && is_byte_sequence(col_indices);
}

static bool is_node_batch_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"nodes"};
    if (!object_has_exact_keys(obj, keys, 1)) return false;
    const cowrie_g1_value_t *nodes = object_get_member(obj, "nodes");
    if (!nodes || nodes->type != COWRIE_G1_TYPE_ARRAY) return false;
    for (size_t i = 0; i < nodes->array_val.len; i++) {
        if (!is_node_object(nodes->array_val.items[i])) return false;
    }
    return true;
}

static bool is_edge_batch_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"edges"};
    if (!object_has_exact_keys(obj, keys, 1)) return false;
    const cowrie_g1_value_t *edges = object_get_member(obj, "edges");
    if (!edges || edges->type != COWRIE_G1_TYPE_ARRAY) return false;
    for (size_t i = 0; i < edges->array_val.len; i++) {
        if (!is_edge_object(edges->array_val.items[i])) return false;
    }
    return true;
}

static bool is_graph_shard_object(const cowrie_g1_value_t *obj) {
    static const char *keys[] = {"nodes", "edges", "meta"};
    if (!object_has_exact_keys(obj, keys, 3)) return false;
    const cowrie_g1_value_t *nodes = object_get_member(obj, "nodes");
    const cowrie_g1_value_t *edges = object_get_member(obj, "edges");
    const cowrie_g1_value_t *meta = object_get_member(obj, "meta");
    if (!nodes || nodes->type != COWRIE_G1_TYPE_ARRAY) return false;
    if (!edges || edges->type != COWRIE_G1_TYPE_ARRAY) return false;
    if (!meta || meta->type != COWRIE_G1_TYPE_OBJECT) return false;
    for (size_t i = 0; i < nodes->array_val.len; i++) {
        if (!is_node_object(nodes->array_val.items[i])) return false;
    }
    for (size_t i = 0; i < edges->array_val.len; i++) {
        if (!is_edge_object(edges->array_val.items[i])) return false;
    }
    return true;
}

static int encode_object_entries(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    int err = cowrie_g1_buf_write_uvarint(buf, obj->object_val.len);
    if (err) return err;
    for (size_t i = 0; i < obj->object_val.len; i++) {
        size_t key_len = strlen(obj->object_val.members[i].key);
        err = encode_raw_string(buf, obj->object_val.members[i].key, key_len);
        if (err) return err;
        err = encode_value(obj->object_val.members[i].value, buf);
        if (err) return err;
    }
    return COWRIE_G1_OK;
}

static int encode_object_default(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_OBJECT);
    if (err) return err;
    return encode_object_entries(obj, buf);
}

static int encode_node_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *id = object_get_member(obj, "id");
    const cowrie_g1_value_t *label = object_get_member(obj, "label");
    const cowrie_g1_value_t *properties = object_get_member(obj, "properties");

    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_NODE);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, cowrie_g1_zigzag_encode(id->int64_val));
    if (err) return err;
    err = encode_raw_string(buf, label->string_val.data, label->string_val.len);
    if (err) return err;
    return encode_object_entries(properties, buf);
}

static int encode_edge_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *src = object_get_member(obj, "src");
    const cowrie_g1_value_t *dst = object_get_member(obj, "dst");
    const cowrie_g1_value_t *label = object_get_member(obj, "label");
    const cowrie_g1_value_t *properties = object_get_member(obj, "properties");

    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_EDGE);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, cowrie_g1_zigzag_encode(src->int64_val));
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, cowrie_g1_zigzag_encode(dst->int64_val));
    if (err) return err;
    err = encode_raw_string(buf, label->string_val.data, label->string_val.len);
    if (err) return err;
    return encode_object_entries(properties, buf);
}

static int encode_adjlist_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *id_width_val = object_get_member(obj, "id_width");
    const cowrie_g1_value_t *node_count_val = object_get_member(obj, "node_count");
    const cowrie_g1_value_t *edge_count_val = object_get_member(obj, "edge_count");
    const cowrie_g1_value_t *row_offsets = object_get_member(obj, "row_offsets");
    const cowrie_g1_value_t *col_indices = object_get_member(obj, "col_indices");

    if (id_width_val->int64_val < 0 || id_width_val->int64_val > 255) return COWRIE_G1_ERR_INVALID;
    if (node_count_val->int64_val < 0 || edge_count_val->int64_val < 0) return COWRIE_G1_ERR_INVALID;

    uint64_t node_count = (uint64_t)node_count_val->int64_val;
    uint64_t edge_count = (uint64_t)edge_count_val->int64_val;
    if (node_count > SIZE_MAX - 1) return COWRIE_G1_ERR_OVERFLOW;
    size_t expected_row_offsets = (size_t)(node_count + 1);
    if (int_sequence_len(row_offsets) != expected_row_offsets) return COWRIE_G1_ERR_INVALID;

    size_t id_size = ((uint8_t)id_width_val->int64_val == 1) ? 4 : 8;
    if (edge_count > SIZE_MAX / id_size) return COWRIE_G1_ERR_OVERFLOW;
    size_t expected_col_len = (size_t)edge_count * id_size;
    if (byte_sequence_len(col_indices) != expected_col_len) return COWRIE_G1_ERR_INVALID;

    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_ADJLIST);
    if (err) return err;
    err = cowrie_g1_buf_write_byte(buf, (uint8_t)id_width_val->int64_val);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, node_count);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, edge_count);
    if (err) return err;

    for (size_t i = 0; i < expected_row_offsets; i++) {
        int64_t offset;
        if (!int_sequence_get(row_offsets, i, &offset) || offset < 0) return COWRIE_G1_ERR_INVALID;
        err = cowrie_g1_buf_write_uvarint(buf, (uint64_t)offset);
        if (err) return err;
    }

    return byte_sequence_write(col_indices, buf);
}

static int encode_node_batch_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *nodes = object_get_member(obj, "nodes");
    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_NODE_BATCH);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, nodes->array_val.len);
    if (err) return err;
    for (size_t i = 0; i < nodes->array_val.len; i++) {
        err = encode_node_object(nodes->array_val.items[i], buf);
        if (err) return err;
    }
    return COWRIE_G1_OK;
}

static int encode_edge_batch_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *edges = object_get_member(obj, "edges");
    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_EDGE_BATCH);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, edges->array_val.len);
    if (err) return err;
    for (size_t i = 0; i < edges->array_val.len; i++) {
        err = encode_edge_object(edges->array_val.items[i], buf);
        if (err) return err;
    }
    return COWRIE_G1_OK;
}

static int encode_graph_shard_object(const cowrie_g1_value_t *obj, cowrie_g1_buf_t *buf) {
    const cowrie_g1_value_t *nodes = object_get_member(obj, "nodes");
    const cowrie_g1_value_t *edges = object_get_member(obj, "edges");
    const cowrie_g1_value_t *meta = object_get_member(obj, "meta");

    int err = cowrie_g1_buf_write_byte(buf, COWRIE_G1_TAG_GRAPH_SHARD);
    if (err) return err;
    err = cowrie_g1_buf_write_uvarint(buf, nodes->array_val.len);
    if (err) return err;
    for (size_t i = 0; i < nodes->array_val.len; i++) {
        err = encode_node_object(nodes->array_val.items[i], buf);
        if (err) return err;
    }
    err = cowrie_g1_buf_write_uvarint(buf, edges->array_val.len);
    if (err) return err;
    for (size_t i = 0; i < edges->array_val.len; i++) {
        err = encode_edge_object(edges->array_val.items[i], buf);
        if (err) return err;
    }
    return encode_object_entries(meta, buf);
}

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
        if (is_graph_shard_object(val)) return encode_graph_shard_object(val, buf);
        if (is_node_batch_object(val)) return encode_node_batch_object(val, buf);
        if (is_edge_batch_object(val)) return encode_edge_batch_object(val, buf);
        if (is_node_object(val)) return encode_node_object(val, buf);
        if (is_edge_object(val)) return encode_edge_object(val, buf);
        if (is_adjlist_object(val)) return encode_adjlist_object(val, buf);
        return encode_object_default(val, buf);

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

static int object_set_owned(cowrie_g1_value_t *obj, const char *key, cowrie_g1_value_t *val) {
    if (!val) return COWRIE_G1_ERR_NOMEM;
    int err = cowrie_g1_object_set(obj, key, val);
    if (err) cowrie_g1_value_free(val);
    return err;
}

static int decode_object_members(reader_t *r, cowrie_g1_value_t *obj, uint64_t count, int depth) {
    int err;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t key_len;
        err = read_uvarint(r, &key_len);
        if (err) return err;
        if (key_len > COWRIE_G1_MAX_STRING_LEN) return COWRIE_G1_ERR_STRING_LEN;
        if (r->pos + key_len > r->len) return COWRIE_G1_ERR_EOF;

        char *key = malloc((size_t)key_len + 1);
        if (!key) return COWRIE_G1_ERR_NOMEM;
        memcpy(key, r->data + r->pos, (size_t)key_len);
        key[key_len] = '\0';
        r->pos += (size_t)key_len;

        cowrie_g1_value_t *val;
        err = decode_value_depth(r, &val, depth + 1);
        if (err) {
            free(key);
            return err;
        }

        err = cowrie_g1_object_set(obj, key, val);
        free(key);
        if (err) {
            cowrie_g1_value_free(val);
            return err;
        }
    }
    return COWRIE_G1_OK;
}

static int decode_object_counted(reader_t *r, uint64_t count, int depth, cowrie_g1_value_t **out) {
    if (count > COWRIE_G1_MAX_OBJECT_LEN) return COWRIE_G1_ERR_OBJECT_LEN;
    cowrie_g1_value_t *obj = cowrie_g1_object((size_t)count);
    if (!obj) return COWRIE_G1_ERR_NOMEM;

    int err = decode_object_members(r, obj, count, depth);
    if (err) {
        cowrie_g1_value_free(obj);
        return err;
    }

    *out = obj;
    return COWRIE_G1_OK;
}

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
        return decode_object_counted(r, count, depth, out);
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

    case COWRIE_G1_TAG_NODE: {
        uint64_t zz;
        err = read_uvarint(r, &zz);
        if (err) return err;
        int64_t id = cowrie_g1_zigzag_decode(zz);

        uint64_t label_len;
        err = read_uvarint(r, &label_len);
        if (err) return err;
        if (label_len > COWRIE_G1_MAX_STRING_LEN) return COWRIE_G1_ERR_STRING_LEN;
        if (r->pos + label_len > r->len) return COWRIE_G1_ERR_EOF;
        cowrie_g1_value_t *label = cowrie_g1_string((const char *)(r->data + r->pos), (size_t)label_len);
        if (!label) return COWRIE_G1_ERR_NOMEM;
        r->pos += (size_t)label_len;

        uint64_t prop_count;
        err = read_uvarint(r, &prop_count);
        if (err) {
            cowrie_g1_value_free(label);
            return err;
        }
        cowrie_g1_value_t *properties;
        err = decode_object_counted(r, prop_count, depth, &properties);
        if (err) {
            cowrie_g1_value_free(label);
            return err;
        }

        cowrie_g1_value_t *node = cowrie_g1_object(3);
        if (!node) {
            cowrie_g1_value_free(label);
            cowrie_g1_value_free(properties);
            return COWRIE_G1_ERR_NOMEM;
        }

        err = object_set_owned(node, "id", cowrie_g1_int64(id));
        if (err) {
            cowrie_g1_value_free(label);
            cowrie_g1_value_free(properties);
            cowrie_g1_value_free(node);
            return err;
        }
        err = object_set_owned(node, "label", label);
        if (err) {
            cowrie_g1_value_free(properties);
            cowrie_g1_value_free(node);
            return err;
        }
        err = object_set_owned(node, "properties", properties);
        if (err) {
            cowrie_g1_value_free(node);
            return err;
        }
        *out = node;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_EDGE: {
        uint64_t src_zz;
        uint64_t dst_zz;
        err = read_uvarint(r, &src_zz);
        if (err) return err;
        err = read_uvarint(r, &dst_zz);
        if (err) return err;
        int64_t src = cowrie_g1_zigzag_decode(src_zz);
        int64_t dst = cowrie_g1_zigzag_decode(dst_zz);

        uint64_t label_len;
        err = read_uvarint(r, &label_len);
        if (err) return err;
        if (label_len > COWRIE_G1_MAX_STRING_LEN) return COWRIE_G1_ERR_STRING_LEN;
        if (r->pos + label_len > r->len) return COWRIE_G1_ERR_EOF;
        cowrie_g1_value_t *label = cowrie_g1_string((const char *)(r->data + r->pos), (size_t)label_len);
        if (!label) return COWRIE_G1_ERR_NOMEM;
        r->pos += (size_t)label_len;

        uint64_t prop_count;
        err = read_uvarint(r, &prop_count);
        if (err) {
            cowrie_g1_value_free(label);
            return err;
        }
        cowrie_g1_value_t *properties;
        err = decode_object_counted(r, prop_count, depth, &properties);
        if (err) {
            cowrie_g1_value_free(label);
            return err;
        }

        cowrie_g1_value_t *edge = cowrie_g1_object(4);
        if (!edge) {
            cowrie_g1_value_free(label);
            cowrie_g1_value_free(properties);
            return COWRIE_G1_ERR_NOMEM;
        }

        err = object_set_owned(edge, "src", cowrie_g1_int64(src));
        if (err) {
            cowrie_g1_value_free(label);
            cowrie_g1_value_free(properties);
            cowrie_g1_value_free(edge);
            return err;
        }
        err = object_set_owned(edge, "dst", cowrie_g1_int64(dst));
        if (err) {
            cowrie_g1_value_free(label);
            cowrie_g1_value_free(properties);
            cowrie_g1_value_free(edge);
            return err;
        }
        err = object_set_owned(edge, "label", label);
        if (err) {
            cowrie_g1_value_free(properties);
            cowrie_g1_value_free(edge);
            return err;
        }
        err = object_set_owned(edge, "properties", properties);
        if (err) {
            cowrie_g1_value_free(edge);
            return err;
        }
        *out = edge;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_ADJLIST: {
        uint8_t id_width;
        err = read_byte(r, &id_width);
        if (err) return err;

        uint64_t node_count_u64;
        uint64_t edge_count_u64;
        err = read_uvarint(r, &node_count_u64);
        if (err) return err;
        err = read_uvarint(r, &edge_count_u64);
        if (err) return err;
        if (node_count_u64 > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;
        if (edge_count_u64 > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;
        if (node_count_u64 > INT64_MAX || edge_count_u64 > INT64_MAX) return COWRIE_G1_ERR_OVERFLOW;
        if (node_count_u64 > SIZE_MAX - 1) return COWRIE_G1_ERR_OVERFLOW;

        size_t row_count = (size_t)(node_count_u64 + 1);
        cowrie_g1_value_t *row_offsets = cowrie_g1_array(row_count);
        if (!row_offsets) return COWRIE_G1_ERR_NOMEM;

        for (size_t i = 0; i < row_count; i++) {
            uint64_t offset_u64;
            err = read_uvarint(r, &offset_u64);
            if (err) {
                cowrie_g1_value_free(row_offsets);
                return err;
            }
            if (offset_u64 > INT64_MAX) {
                cowrie_g1_value_free(row_offsets);
                return COWRIE_G1_ERR_OVERFLOW;
            }
            cowrie_g1_value_t *offset_val = cowrie_g1_int64((int64_t)offset_u64);
            if (!offset_val) {
                cowrie_g1_value_free(row_offsets);
                return COWRIE_G1_ERR_NOMEM;
            }
            err = cowrie_g1_array_append(row_offsets, offset_val);
            if (err) {
                cowrie_g1_value_free(offset_val);
                cowrie_g1_value_free(row_offsets);
                return err;
            }
        }

        size_t col_width = id_width == 1 ? 4 : 8;
        if (edge_count_u64 > (SIZE_MAX - r->pos) / col_width) {
            cowrie_g1_value_free(row_offsets);
            return COWRIE_G1_ERR_OVERFLOW;
        }
        size_t col_len = (size_t)edge_count_u64 * col_width;
        if (r->pos + col_len > r->len) {
            cowrie_g1_value_free(row_offsets);
            return COWRIE_G1_ERR_EOF;
        }
        cowrie_g1_value_t *col_indices = cowrie_g1_bytes(r->data + r->pos, col_len);
        if (!col_indices) {
            cowrie_g1_value_free(row_offsets);
            return COWRIE_G1_ERR_NOMEM;
        }
        r->pos += col_len;

        cowrie_g1_value_t *adj = cowrie_g1_object(5);
        if (!adj) {
            cowrie_g1_value_free(row_offsets);
            cowrie_g1_value_free(col_indices);
            return COWRIE_G1_ERR_NOMEM;
        }
        err = object_set_owned(adj, "id_width", cowrie_g1_int64(id_width));
        if (err) {
            cowrie_g1_value_free(row_offsets);
            cowrie_g1_value_free(col_indices);
            cowrie_g1_value_free(adj);
            return err;
        }
        err = object_set_owned(adj, "node_count", cowrie_g1_int64((int64_t)node_count_u64));
        if (err) {
            cowrie_g1_value_free(row_offsets);
            cowrie_g1_value_free(col_indices);
            cowrie_g1_value_free(adj);
            return err;
        }
        err = object_set_owned(adj, "edge_count", cowrie_g1_int64((int64_t)edge_count_u64));
        if (err) {
            cowrie_g1_value_free(row_offsets);
            cowrie_g1_value_free(col_indices);
            cowrie_g1_value_free(adj);
            return err;
        }
        err = object_set_owned(adj, "row_offsets", row_offsets);
        if (err) {
            cowrie_g1_value_free(col_indices);
            cowrie_g1_value_free(adj);
            return err;
        }
        err = object_set_owned(adj, "col_indices", col_indices);
        if (err) {
            cowrie_g1_value_free(adj);
            return err;
        }
        *out = adj;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_NODE_BATCH: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;

        cowrie_g1_value_t *nodes = cowrie_g1_array((size_t)count);
        if (!nodes) return COWRIE_G1_ERR_NOMEM;
        for (uint64_t i = 0; i < count; i++) {
            cowrie_g1_value_t *node;
            err = decode_value_depth(r, &node, depth + 1);
            if (err) {
                cowrie_g1_value_free(nodes);
                return err;
            }
            if (!is_node_object(node)) {
                cowrie_g1_value_free(node);
                cowrie_g1_value_free(nodes);
                return COWRIE_G1_ERR_INVALID;
            }
            err = cowrie_g1_array_append(nodes, node);
            if (err) {
                cowrie_g1_value_free(node);
                cowrie_g1_value_free(nodes);
                return err;
            }
        }

        cowrie_g1_value_t *batch = cowrie_g1_object(1);
        if (!batch) {
            cowrie_g1_value_free(nodes);
            return COWRIE_G1_ERR_NOMEM;
        }
        err = object_set_owned(batch, "nodes", nodes);
        if (err) {
            cowrie_g1_value_free(batch);
            return err;
        }
        *out = batch;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_EDGE_BATCH: {
        uint64_t count;
        err = read_uvarint(r, &count);
        if (err) return err;
        if (count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;

        cowrie_g1_value_t *edges = cowrie_g1_array((size_t)count);
        if (!edges) return COWRIE_G1_ERR_NOMEM;
        for (uint64_t i = 0; i < count; i++) {
            cowrie_g1_value_t *edge;
            err = decode_value_depth(r, &edge, depth + 1);
            if (err) {
                cowrie_g1_value_free(edges);
                return err;
            }
            if (!is_edge_object(edge)) {
                cowrie_g1_value_free(edge);
                cowrie_g1_value_free(edges);
                return COWRIE_G1_ERR_INVALID;
            }
            err = cowrie_g1_array_append(edges, edge);
            if (err) {
                cowrie_g1_value_free(edge);
                cowrie_g1_value_free(edges);
                return err;
            }
        }

        cowrie_g1_value_t *batch = cowrie_g1_object(1);
        if (!batch) {
            cowrie_g1_value_free(edges);
            return COWRIE_G1_ERR_NOMEM;
        }
        err = object_set_owned(batch, "edges", edges);
        if (err) {
            cowrie_g1_value_free(batch);
            return err;
        }
        *out = batch;
        return COWRIE_G1_OK;
    }

    case COWRIE_G1_TAG_GRAPH_SHARD: {
        uint64_t node_count;
        err = read_uvarint(r, &node_count);
        if (err) return err;
        if (node_count > COWRIE_G1_MAX_ARRAY_LEN) return COWRIE_G1_ERR_ARRAY_LEN;

        cowrie_g1_value_t *nodes = cowrie_g1_array((size_t)node_count);
        if (!nodes) return COWRIE_G1_ERR_NOMEM;
        for (uint64_t i = 0; i < node_count; i++) {
            cowrie_g1_value_t *node;
            err = decode_value_depth(r, &node, depth + 1);
            if (err) {
                cowrie_g1_value_free(nodes);
                return err;
            }
            if (!is_node_object(node)) {
                cowrie_g1_value_free(node);
                cowrie_g1_value_free(nodes);
                return COWRIE_G1_ERR_INVALID;
            }
            err = cowrie_g1_array_append(nodes, node);
            if (err) {
                cowrie_g1_value_free(node);
                cowrie_g1_value_free(nodes);
                return err;
            }
        }

        uint64_t edge_count;
        err = read_uvarint(r, &edge_count);
        if (err) {
            cowrie_g1_value_free(nodes);
            return err;
        }
        if (edge_count > COWRIE_G1_MAX_ARRAY_LEN) {
            cowrie_g1_value_free(nodes);
            return COWRIE_G1_ERR_ARRAY_LEN;
        }

        cowrie_g1_value_t *edges = cowrie_g1_array((size_t)edge_count);
        if (!edges) {
            cowrie_g1_value_free(nodes);
            return COWRIE_G1_ERR_NOMEM;
        }
        for (uint64_t i = 0; i < edge_count; i++) {
            cowrie_g1_value_t *edge;
            err = decode_value_depth(r, &edge, depth + 1);
            if (err) {
                cowrie_g1_value_free(nodes);
                cowrie_g1_value_free(edges);
                return err;
            }
            if (!is_edge_object(edge)) {
                cowrie_g1_value_free(edge);
                cowrie_g1_value_free(nodes);
                cowrie_g1_value_free(edges);
                return COWRIE_G1_ERR_INVALID;
            }
            err = cowrie_g1_array_append(edges, edge);
            if (err) {
                cowrie_g1_value_free(edge);
                cowrie_g1_value_free(nodes);
                cowrie_g1_value_free(edges);
                return err;
            }
        }

        uint64_t meta_count;
        err = read_uvarint(r, &meta_count);
        if (err) {
            cowrie_g1_value_free(nodes);
            cowrie_g1_value_free(edges);
            return err;
        }
        cowrie_g1_value_t *meta;
        err = decode_object_counted(r, meta_count, depth, &meta);
        if (err) {
            cowrie_g1_value_free(nodes);
            cowrie_g1_value_free(edges);
            return err;
        }

        cowrie_g1_value_t *shard = cowrie_g1_object(3);
        if (!shard) {
            cowrie_g1_value_free(nodes);
            cowrie_g1_value_free(edges);
            cowrie_g1_value_free(meta);
            return COWRIE_G1_ERR_NOMEM;
        }
        err = object_set_owned(shard, "nodes", nodes);
        if (err) {
            cowrie_g1_value_free(edges);
            cowrie_g1_value_free(meta);
            cowrie_g1_value_free(shard);
            return err;
        }
        err = object_set_owned(shard, "edges", edges);
        if (err) {
            cowrie_g1_value_free(meta);
            cowrie_g1_value_free(shard);
            return err;
        }
        err = object_set_owned(shard, "meta", meta);
        if (err) {
            cowrie_g1_value_free(shard);
            return err;
        }
        *out = shard;
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
