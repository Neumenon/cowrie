/*
 * COWRIE v2 - Core Codec Implementation
 */

#include "../include/cowrie_gen2.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#ifdef COWRIE_HAS_ZSTD
#include <zstd.h>
#endif

/* ============================================================
 * Buffer Operations
 * ============================================================ */

void cowrie_buf_init(COWRIEBuf *buf) {
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

void cowrie_buf_free(COWRIEBuf *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

static int buf_grow(COWRIEBuf *buf, size_t need) {
    /* Check for overflow in addition */
    if (need > SIZE_MAX - buf->len) return -1;
    
    if (buf->len + need <= buf->cap) return 0;

    size_t new_cap = buf->cap ? buf->cap : 256;
    size_t target = buf->len + need;
    
    /* Grow with overflow protection */
    while (new_cap < target) {
        if (new_cap > SIZE_MAX / 2) {
            /* Can't double, try exact size */
            new_cap = target;
            break;
        }
        new_cap *= 2;
    }

    uint8_t *new_data = realloc(buf->data, new_cap);
    if (!new_data) return -1;

    buf->data = new_data;
    buf->cap = new_cap;
    return 0;
}

static int buf_put_byte(COWRIEBuf *buf, uint8_t b) {
    if (buf_grow(buf, 1) != 0) return -1;
    buf->data[buf->len++] = b;
    return 0;
}

static int buf_put(COWRIEBuf *buf, const void *data, size_t len) {
    if (buf_grow(buf, len) != 0) return -1;
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    return 0;
}

/* ============================================================
 * Varint Encoding/Decoding
 * ============================================================ */

int cowrie_put_uvarint(COWRIEBuf *buf, uint64_t v) {
    uint8_t tmp[10];
    int n = 0;
    while (v >= 0x80) {
        tmp[n++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    tmp[n++] = (uint8_t)v;
    return buf_put(buf, tmp, n);
}

int cowrie_get_uvarint(const uint8_t *data, size_t len, uint64_t *v, size_t *bytes_read) {
    uint64_t result = 0;
    size_t shift = 0;
    size_t i = 0;

    while (i < len && i < 10) {
        uint8_t b = data[i];
        result |= ((uint64_t)(b & 0x7F)) << shift;
        i++;
        if ((b & 0x80) == 0) {
            *v = result;
            *bytes_read = i;
            return 0;
        }
        shift += 7;
    }
    return -1; /* incomplete or overflow */
}

/* ============================================================
 * Memory Allocation
 * ============================================================ */

static COWRIEValue *alloc_value(COWRIEType type) {
    COWRIEValue *v = calloc(1, sizeof(COWRIEValue));
    if (v) v->type = type;
    return v;
}

/* ============================================================
 * Value Constructors
 * ============================================================ */

COWRIEValue *cowrie_new_null(void) {
    return alloc_value(COWRIE_NULL);
}

COWRIEValue *cowrie_new_bool(int b) {
    COWRIEValue *v = alloc_value(COWRIE_BOOL);
    if (v) v->as.boolean = b ? 1 : 0;
    return v;
}

COWRIEValue *cowrie_new_int64(int64_t i) {
    COWRIEValue *v = alloc_value(COWRIE_INT64);
    if (v) v->as.i64 = i;
    return v;
}

COWRIEValue *cowrie_new_uint64(uint64_t u) {
    COWRIEValue *v = alloc_value(COWRIE_UINT64);
    if (v) v->as.u64 = u;
    return v;
}

COWRIEValue *cowrie_new_float64(double f) {
    COWRIEValue *v = alloc_value(COWRIE_FLOAT64);
    if (v) v->as.f64 = f;
    return v;
}

COWRIEValue *cowrie_new_decimal128(int8_t scale, const uint8_t coef[16]) {
    COWRIEValue *v = alloc_value(COWRIE_DECIMAL128);
    if (v) {
        v->as.decimal128.scale = scale;
        memcpy(v->as.decimal128.coef, coef, 16);
    }
    return v;
}

COWRIEValue *cowrie_new_string(const char *s, size_t len) {
    COWRIEValue *v = alloc_value(COWRIE_STRING);
    if (!v) return NULL;

    v->as.str.data = malloc(len + 1);
    if (!v->as.str.data) {
        free(v);
        return NULL;
    }
    memcpy(v->as.str.data, s, len);
    v->as.str.data[len] = '\0';
    v->as.str.len = len;
    return v;
}

COWRIEValue *cowrie_new_bytes(const uint8_t *data, size_t len) {
    COWRIEValue *v = alloc_value(COWRIE_BYTES);
    if (!v) return NULL;

    v->as.bytes.data = malloc(len);
    if (!v->as.bytes.data && len > 0) {
        free(v);
        return NULL;
    }
    if (len > 0) memcpy(v->as.bytes.data, data, len);
    v->as.bytes.len = len;
    return v;
}

COWRIEValue *cowrie_new_datetime64(int64_t nanos) {
    COWRIEValue *v = alloc_value(COWRIE_DATETIME64);
    if (v) v->as.datetime64 = nanos;
    return v;
}

COWRIEValue *cowrie_new_uuid128(const uint8_t uuid[16]) {
    COWRIEValue *v = alloc_value(COWRIE_UUID128);
    if (v) memcpy(v->as.uuid, uuid, 16);
    return v;
}

COWRIEValue *cowrie_new_bigint(const uint8_t *data, size_t len) {
    COWRIEValue *v = alloc_value(COWRIE_BIGINT);
    if (!v) return NULL;

    v->as.bigint.data = malloc(len);
    if (!v->as.bigint.data && len > 0) {
        free(v);
        return NULL;
    }
    if (len > 0) memcpy(v->as.bigint.data, data, len);
    v->as.bigint.len = len;
    return v;
}

COWRIEValue *cowrie_new_ext(uint64_t ext_type, const uint8_t *payload, size_t payload_len) {
    COWRIEValue *v = alloc_value(COWRIE_EXT);
    if (!v) return NULL;

    v->as.ext.ext_type = ext_type;
    v->as.ext.payload = malloc(payload_len);
    if (!v->as.ext.payload && payload_len > 0) {
        free(v);
        return NULL;
    }
    if (payload_len > 0) memcpy(v->as.ext.payload, payload, payload_len);
    v->as.ext.payload_len = payload_len;
    return v;
}

COWRIEValue *cowrie_new_array(void) {
    return alloc_value(COWRIE_ARRAY);
}

COWRIEValue *cowrie_new_object(void) {
    return alloc_value(COWRIE_OBJECT);
}

/* ============================================================
 * v2.1 Extension Constructors
 * ============================================================ */

COWRIEValue *cowrie_new_tensor(uint8_t dtype, uint8_t rank, const size_t *dims,
                              const uint8_t *data, size_t data_len) {
    COWRIEValue *v = alloc_value(COWRIE_TENSOR);
    if (!v) return NULL;

    v->as.tensor.dtype = dtype;
    v->as.tensor.rank = rank;
    v->as.tensor.data_len = data_len;

    /* Allocate and copy dims */
    if (rank > 0) {
        v->as.tensor.dims = malloc(rank * sizeof(size_t));
        if (!v->as.tensor.dims) { free(v); return NULL; }
        memcpy(v->as.tensor.dims, dims, rank * sizeof(size_t));
    } else {
        v->as.tensor.dims = NULL;
    }

    /* Allocate and copy data */
    if (data_len > 0) {
        v->as.tensor.data = malloc(data_len);
        if (!v->as.tensor.data) { free(v->as.tensor.dims); free(v); return NULL; }
        memcpy(v->as.tensor.data, data, data_len);
    } else {
        v->as.tensor.data = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_tensor_ref(uint8_t store_id, const uint8_t *key, size_t key_len) {
    COWRIEValue *v = alloc_value(COWRIE_TENSOR_REF);
    if (!v) return NULL;

    v->as.tensor_ref.store_id = store_id;
    v->as.tensor_ref.key_len = key_len;

    if (key_len > 0) {
        v->as.tensor_ref.key = malloc(key_len);
        if (!v->as.tensor_ref.key) { free(v); return NULL; }
        memcpy(v->as.tensor_ref.key, key, key_len);
    } else {
        v->as.tensor_ref.key = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_image(uint8_t format, uint16_t width, uint16_t height,
                             const uint8_t *data, size_t data_len) {
    COWRIEValue *v = alloc_value(COWRIE_IMAGE);
    if (!v) return NULL;

    v->as.image.format = format;
    v->as.image.width = width;
    v->as.image.height = height;
    v->as.image.data_len = data_len;

    if (data_len > 0) {
        v->as.image.data = malloc(data_len);
        if (!v->as.image.data) { free(v); return NULL; }
        memcpy(v->as.image.data, data, data_len);
    } else {
        v->as.image.data = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_audio(uint8_t encoding, uint32_t sample_rate, uint8_t channels,
                             const uint8_t *data, size_t data_len) {
    COWRIEValue *v = alloc_value(COWRIE_AUDIO);
    if (!v) return NULL;

    v->as.audio.encoding = encoding;
    v->as.audio.sample_rate = sample_rate;
    v->as.audio.channels = channels;
    v->as.audio.data_len = data_len;

    if (data_len > 0) {
        v->as.audio.data = malloc(data_len);
        if (!v->as.audio.data) { free(v); return NULL; }
        memcpy(v->as.audio.data, data, data_len);
    } else {
        v->as.audio.data = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_adjlist(uint8_t id_width, size_t node_count, size_t edge_count,
                               const size_t *row_offsets, const void *col_indices) {
    COWRIEValue *v = alloc_value(COWRIE_ADJLIST);
    if (!v) return NULL;

    v->as.adjlist.id_width = id_width;
    v->as.adjlist.node_count = node_count;
    v->as.adjlist.edge_count = edge_count;

    /* Allocate and copy row_offsets */
    size_t ro_size = (node_count + 1) * sizeof(size_t);
    v->as.adjlist.row_offsets = malloc(ro_size);
    if (!v->as.adjlist.row_offsets) { free(v); return NULL; }
    memcpy(v->as.adjlist.row_offsets, row_offsets, ro_size);

    /* Allocate and copy col_indices */
    size_t ci_elem_size = (id_width == COWRIE_ID_INT64) ? sizeof(int64_t) : sizeof(int32_t);
    size_t ci_size = edge_count * ci_elem_size;
    if (edge_count > 0) {
        v->as.adjlist.col_indices = malloc(ci_size);
        if (!v->as.adjlist.col_indices) {
            free(v->as.adjlist.row_offsets);
            free(v);
            return NULL;
        }
        memcpy(v->as.adjlist.col_indices, col_indices, ci_size);
    } else {
        v->as.adjlist.col_indices = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_rich_text(const char *text, size_t text_len,
                                 const int32_t *tokens, size_t token_count,
                                 const COWRIERichTextSpan *spans, size_t span_count) {
    COWRIEValue *v = alloc_value(COWRIE_RICH_TEXT);
    if (!v) return NULL;

    v->as.rich_text.text_len = text_len;
    v->as.rich_text.token_count = token_count;
    v->as.rich_text.span_count = span_count;

    /* Allocate and copy text */
    v->as.rich_text.text = malloc(text_len + 1);
    if (!v->as.rich_text.text) { free(v); return NULL; }
    memcpy(v->as.rich_text.text, text, text_len);
    v->as.rich_text.text[text_len] = '\0';

    /* Allocate and copy tokens */
    if (tokens && token_count > 0) {
        v->as.rich_text.tokens = malloc(token_count * sizeof(int32_t));
        if (!v->as.rich_text.tokens) {
            free(v->as.rich_text.text);
            free(v);
            return NULL;
        }
        memcpy(v->as.rich_text.tokens, tokens, token_count * sizeof(int32_t));
    } else {
        v->as.rich_text.tokens = NULL;
    }

    /* Allocate and copy spans */
    if (spans && span_count > 0) {
        v->as.rich_text.spans = malloc(span_count * sizeof(COWRIERichTextSpan));
        if (!v->as.rich_text.spans) {
            free(v->as.rich_text.tokens);
            free(v->as.rich_text.text);
            free(v);
            return NULL;
        }
        memcpy(v->as.rich_text.spans, spans, span_count * sizeof(COWRIERichTextSpan));
    } else {
        v->as.rich_text.spans = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_delta(size_t base_id, const COWRIEDeltaOp_t *ops, size_t op_count) {
    COWRIEValue *v = alloc_value(COWRIE_DELTA);
    if (!v) return NULL;

    v->as.delta.base_id = base_id;
    v->as.delta.op_count = op_count;

    if (op_count > 0) {
        v->as.delta.ops = malloc(op_count * sizeof(COWRIEDeltaOp_t));
        if (!v->as.delta.ops) { free(v); return NULL; }

        /* 
         * Copy ops - takes OWNERSHIP of value pointers.
         * The caller should pass values that this delta will own.
         * cowrie_free() will free all op values when delta is freed.
         */
        for (size_t i = 0; i < op_count; i++) {
            v->as.delta.ops[i].op_code = ops[i].op_code;
            v->as.delta.ops[i].field_id = ops[i].field_id;
            v->as.delta.ops[i].value = ops[i].value;  /* Takes ownership */
        }
    } else {
        v->as.delta.ops = NULL;
    }

    return v;
}

/* ============================================================
 * v2.1 Graph Type Constructors
 * ============================================================ */

COWRIEValue *cowrie_new_node(const char *id, size_t id_len,
                           const char **labels, const size_t *label_lens, size_t label_count,
                           const COWRIEMember *props, size_t prop_count) {
    COWRIEValue *v = alloc_value(COWRIE_NODE);
    if (!v) return NULL;

    /* Copy ID */
    v->as.node.id = malloc(id_len + 1);
    if (!v->as.node.id) { free(v); return NULL; }
    memcpy(v->as.node.id, id, id_len);
    v->as.node.id[id_len] = '\0';
    v->as.node.id_len = id_len;

    /* Copy labels */
    v->as.node.label_count = label_count;
    if (label_count > 0) {
        v->as.node.labels = malloc(label_count * sizeof(char *));
        v->as.node.label_lens = malloc(label_count * sizeof(size_t));
        if (!v->as.node.labels || !v->as.node.label_lens) {
            free(v->as.node.id);
            free(v->as.node.labels);
            free(v->as.node.label_lens);
            free(v);
            return NULL;
        }
        for (size_t i = 0; i < label_count; i++) {
            v->as.node.labels[i] = malloc(label_lens[i] + 1);
            if (!v->as.node.labels[i]) {
                for (size_t j = 0; j < i; j++) free(v->as.node.labels[j]);
                free(v->as.node.labels);
                free(v->as.node.label_lens);
                free(v->as.node.id);
                free(v);
                return NULL;
            }
            memcpy(v->as.node.labels[i], labels[i], label_lens[i]);
            v->as.node.labels[i][label_lens[i]] = '\0';
            v->as.node.label_lens[i] = label_lens[i];
        }
    } else {
        v->as.node.labels = NULL;
        v->as.node.label_lens = NULL;
    }

    /* Copy properties */
    v->as.node.prop_count = prop_count;
    if (prop_count > 0) {
        v->as.node.props = malloc(prop_count * sizeof(COWRIEMember));
        if (!v->as.node.props) {
            for (size_t i = 0; i < label_count; i++) free(v->as.node.labels[i]);
            free(v->as.node.labels);
            free(v->as.node.label_lens);
            free(v->as.node.id);
            free(v);
            return NULL;
        }
        for (size_t i = 0; i < prop_count; i++) {
            v->as.node.props[i].key = malloc(props[i].key_len + 1);
            if (!v->as.node.props[i].key) {
                /* Cleanup on failure */
                for (size_t j = 0; j < i; j++) free(v->as.node.props[j].key);
                free(v->as.node.props);
                for (size_t j = 0; j < label_count; j++) free(v->as.node.labels[j]);
                free(v->as.node.labels);
                free(v->as.node.label_lens);
                free(v->as.node.id);
                free(v);
                return NULL;
            }
            memcpy(v->as.node.props[i].key, props[i].key, props[i].key_len);
            v->as.node.props[i].key[props[i].key_len] = '\0';
            v->as.node.props[i].key_len = props[i].key_len;
            v->as.node.props[i].value = props[i].value;  /* Takes ownership */
        }
    } else {
        v->as.node.props = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_edge(const char *from_id, size_t from_id_len,
                           const char *to_id, size_t to_id_len,
                           const char *edge_type, size_t edge_type_len,
                           const COWRIEMember *props, size_t prop_count) {
    COWRIEValue *v = alloc_value(COWRIE_EDGE);
    if (!v) return NULL;

    /* Copy from_id */
    v->as.edge.from_id = malloc(from_id_len + 1);
    if (!v->as.edge.from_id) { free(v); return NULL; }
    memcpy(v->as.edge.from_id, from_id, from_id_len);
    v->as.edge.from_id[from_id_len] = '\0';
    v->as.edge.from_id_len = from_id_len;

    /* Copy to_id */
    v->as.edge.to_id = malloc(to_id_len + 1);
    if (!v->as.edge.to_id) {
        free(v->as.edge.from_id);
        free(v);
        return NULL;
    }
    memcpy(v->as.edge.to_id, to_id, to_id_len);
    v->as.edge.to_id[to_id_len] = '\0';
    v->as.edge.to_id_len = to_id_len;

    /* Copy edge_type */
    v->as.edge.edge_type = malloc(edge_type_len + 1);
    if (!v->as.edge.edge_type) {
        free(v->as.edge.to_id);
        free(v->as.edge.from_id);
        free(v);
        return NULL;
    }
    memcpy(v->as.edge.edge_type, edge_type, edge_type_len);
    v->as.edge.edge_type[edge_type_len] = '\0';
    v->as.edge.edge_type_len = edge_type_len;

    /* Copy properties */
    v->as.edge.prop_count = prop_count;
    if (prop_count > 0) {
        v->as.edge.props = malloc(prop_count * sizeof(COWRIEMember));
        if (!v->as.edge.props) {
            free(v->as.edge.edge_type);
            free(v->as.edge.to_id);
            free(v->as.edge.from_id);
            free(v);
            return NULL;
        }
        for (size_t i = 0; i < prop_count; i++) {
            v->as.edge.props[i].key = malloc(props[i].key_len + 1);
            if (!v->as.edge.props[i].key) {
                for (size_t j = 0; j < i; j++) free(v->as.edge.props[j].key);
                free(v->as.edge.props);
                free(v->as.edge.edge_type);
                free(v->as.edge.to_id);
                free(v->as.edge.from_id);
                free(v);
                return NULL;
            }
            memcpy(v->as.edge.props[i].key, props[i].key, props[i].key_len);
            v->as.edge.props[i].key[props[i].key_len] = '\0';
            v->as.edge.props[i].key_len = props[i].key_len;
            v->as.edge.props[i].value = props[i].value;  /* Takes ownership */
        }
    } else {
        v->as.edge.props = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_node_batch(const COWRIENode *nodes, size_t node_count) {
    COWRIEValue *v = alloc_value(COWRIE_NODE_BATCH);
    if (!v) return NULL;

    v->as.node_batch.node_count = node_count;
    if (node_count > 0) {
        v->as.node_batch.nodes = malloc(node_count * sizeof(COWRIENode));
        if (!v->as.node_batch.nodes) { free(v); return NULL; }
        memcpy(v->as.node_batch.nodes, nodes, node_count * sizeof(COWRIENode));
    } else {
        v->as.node_batch.nodes = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_edge_batch(const COWRIEEdge *edges, size_t edge_count) {
    COWRIEValue *v = alloc_value(COWRIE_EDGE_BATCH);
    if (!v) return NULL;

    v->as.edge_batch.edge_count = edge_count;
    if (edge_count > 0) {
        v->as.edge_batch.edges = malloc(edge_count * sizeof(COWRIEEdge));
        if (!v->as.edge_batch.edges) { free(v); return NULL; }
        memcpy(v->as.edge_batch.edges, edges, edge_count * sizeof(COWRIEEdge));
    } else {
        v->as.edge_batch.edges = NULL;
    }

    return v;
}

COWRIEValue *cowrie_new_graph_shard(const COWRIENode *nodes, size_t node_count,
                                   const COWRIEEdge *edges, size_t edge_count,
                                   const COWRIEMember *metadata, size_t meta_count) {
    COWRIEValue *v = alloc_value(COWRIE_GRAPH_SHARD);
    if (!v) return NULL;

    /* Copy nodes */
    v->as.graph_shard.node_count = node_count;
    if (node_count > 0) {
        v->as.graph_shard.nodes = malloc(node_count * sizeof(COWRIENode));
        if (!v->as.graph_shard.nodes) { free(v); return NULL; }
        memcpy(v->as.graph_shard.nodes, nodes, node_count * sizeof(COWRIENode));
    } else {
        v->as.graph_shard.nodes = NULL;
    }

    /* Copy edges */
    v->as.graph_shard.edge_count = edge_count;
    if (edge_count > 0) {
        v->as.graph_shard.edges = malloc(edge_count * sizeof(COWRIEEdge));
        if (!v->as.graph_shard.edges) {
            free(v->as.graph_shard.nodes);
            free(v);
            return NULL;
        }
        memcpy(v->as.graph_shard.edges, edges, edge_count * sizeof(COWRIEEdge));
    } else {
        v->as.graph_shard.edges = NULL;
    }

    /* Copy metadata */
    v->as.graph_shard.meta_count = meta_count;
    if (meta_count > 0) {
        v->as.graph_shard.metadata = malloc(meta_count * sizeof(COWRIEMember));
        if (!v->as.graph_shard.metadata) {
            free(v->as.graph_shard.edges);
            free(v->as.graph_shard.nodes);
            free(v);
            return NULL;
        }
        for (size_t i = 0; i < meta_count; i++) {
            v->as.graph_shard.metadata[i].key = malloc(metadata[i].key_len + 1);
            if (!v->as.graph_shard.metadata[i].key) {
                for (size_t j = 0; j < i; j++) free(v->as.graph_shard.metadata[j].key);
                free(v->as.graph_shard.metadata);
                free(v->as.graph_shard.edges);
                free(v->as.graph_shard.nodes);
                free(v);
                return NULL;
            }
            memcpy(v->as.graph_shard.metadata[i].key, metadata[i].key, metadata[i].key_len);
            v->as.graph_shard.metadata[i].key[metadata[i].key_len] = '\0';
            v->as.graph_shard.metadata[i].key_len = metadata[i].key_len;
            v->as.graph_shard.metadata[i].value = metadata[i].value;  /* Takes ownership */
        }
    } else {
        v->as.graph_shard.metadata = NULL;
    }

    return v;
}

/* ============================================================
 * Value Manipulation
 * ============================================================ */

int cowrie_array_append(COWRIEValue *arr, COWRIEValue *item) {
    if (!arr || arr->type != COWRIE_ARRAY) return -1;

    size_t new_len = arr->as.array.len + 1;
    COWRIEValue **new_items = realloc(arr->as.array.items, new_len * sizeof(COWRIEValue *));
    if (!new_items) return -1;

    new_items[arr->as.array.len] = item;
    arr->as.array.items = new_items;
    arr->as.array.len = new_len;
    return 0;
}

COWRIEValue *cowrie_array_get(const COWRIEValue *arr, size_t index) {
    if (!arr || arr->type != COWRIE_ARRAY) return NULL;
    if (index >= arr->as.array.len) return NULL;
    return arr->as.array.items[index];
}

size_t cowrie_array_len(const COWRIEValue *arr) {
    if (!arr || arr->type != COWRIE_ARRAY) return 0;
    return arr->as.array.len;
}

int cowrie_object_set(COWRIEValue *obj, const char *key, size_t key_len, COWRIEValue *value) {
    if (!obj || obj->type != COWRIE_OBJECT) return -1;

    /* Check if key exists */
    for (size_t i = 0; i < obj->as.object.len; i++) {
        if (obj->as.object.members[i].key_len == key_len &&
            memcmp(obj->as.object.members[i].key, key, key_len) == 0) {
            cowrie_free(obj->as.object.members[i].value);
            obj->as.object.members[i].value = value;
            return 0;
        }
    }

    /* Add new member */
    size_t new_len = obj->as.object.len + 1;
    COWRIEMember *new_members = realloc(obj->as.object.members, new_len * sizeof(COWRIEMember));
    if (!new_members) return -1;

    COWRIEMember *m = &new_members[obj->as.object.len];
    m->key = malloc(key_len + 1);
    if (!m->key) return -1;
    memcpy(m->key, key, key_len);
    m->key[key_len] = '\0';
    m->key_len = key_len;
    m->value = value;

    obj->as.object.members = new_members;
    obj->as.object.len = new_len;
    return 0;
}

COWRIEValue *cowrie_object_get(const COWRIEValue *obj, const char *key, size_t key_len) {
    if (!obj || obj->type != COWRIE_OBJECT) return NULL;

    for (size_t i = 0; i < obj->as.object.len; i++) {
        if (obj->as.object.members[i].key_len == key_len &&
            memcmp(obj->as.object.members[i].key, key, key_len) == 0) {
            return obj->as.object.members[i].value;
        }
    }
    return NULL;
}

size_t cowrie_object_len(const COWRIEValue *obj) {
    if (!obj || obj->type != COWRIE_OBJECT) return 0;
    return obj->as.object.len;
}

/* ============================================================
 * Memory Management
 * ============================================================ */

void cowrie_free(COWRIEValue *v) {
    if (!v) return;

    switch (v->type) {
    case COWRIE_STRING:
        free(v->as.str.data);
        break;
    case COWRIE_BYTES:
        free(v->as.bytes.data);
        break;
    case COWRIE_BIGINT:
        free(v->as.bigint.data);
        break;
    case COWRIE_EXT:
        free(v->as.ext.payload);
        break;
    case COWRIE_ARRAY:
        for (size_t i = 0; i < v->as.array.len; i++) {
            cowrie_free(v->as.array.items[i]);
        }
        free(v->as.array.items);
        break;
    case COWRIE_OBJECT:
        for (size_t i = 0; i < v->as.object.len; i++) {
            free(v->as.object.members[i].key);
            cowrie_free(v->as.object.members[i].value);
        }
        free(v->as.object.members);
        break;
    /* v2.1 extension types */
    case COWRIE_TENSOR:
        free(v->as.tensor.dims);
        free(v->as.tensor.data);
        break;
    case COWRIE_TENSOR_REF:
        free(v->as.tensor_ref.key);
        break;
    case COWRIE_IMAGE:
        free(v->as.image.data);
        break;
    case COWRIE_AUDIO:
        free(v->as.audio.data);
        break;
    case COWRIE_ADJLIST:
        free(v->as.adjlist.row_offsets);
        free(v->as.adjlist.col_indices);
        break;
    case COWRIE_RICH_TEXT:
        free(v->as.rich_text.text);
        free(v->as.rich_text.tokens);
        free(v->as.rich_text.spans);
        break;
    case COWRIE_DELTA:
        /* Delta owns its op values - free them */
        for (size_t i = 0; i < v->as.delta.op_count; i++) {
            cowrie_free(v->as.delta.ops[i].value);
        }
        free(v->as.delta.ops);
        break;
    /* v2.1 Graph types */
    case COWRIE_NODE:
        free(v->as.node.id);
        for (size_t i = 0; i < v->as.node.label_count; i++) {
            free(v->as.node.labels[i]);
        }
        free(v->as.node.labels);
        free(v->as.node.label_lens);
        for (size_t i = 0; i < v->as.node.prop_count; i++) {
            free(v->as.node.props[i].key);
            cowrie_free(v->as.node.props[i].value);
        }
        free(v->as.node.props);
        break;
    case COWRIE_EDGE:
        free(v->as.edge.from_id);
        free(v->as.edge.to_id);
        free(v->as.edge.edge_type);
        for (size_t i = 0; i < v->as.edge.prop_count; i++) {
            free(v->as.edge.props[i].key);
            cowrie_free(v->as.edge.props[i].value);
        }
        free(v->as.edge.props);
        break;
    case COWRIE_NODE_BATCH:
        /* Note: shallow copy, don't free node internals */
        free(v->as.node_batch.nodes);
        break;
    case COWRIE_EDGE_BATCH:
        /* Note: shallow copy, don't free edge internals */
        free(v->as.edge_batch.edges);
        break;
    case COWRIE_GRAPH_SHARD:
        /* Note: shallow copy, don't free node/edge internals */
        free(v->as.graph_shard.nodes);
        free(v->as.graph_shard.edges);
        for (size_t i = 0; i < v->as.graph_shard.meta_count; i++) {
            free(v->as.graph_shard.metadata[i].key);
            cowrie_free(v->as.graph_shard.metadata[i].value);
        }
        free(v->as.graph_shard.metadata);
        break;
    default:
        break;
    }
    free(v);
}

/* ============================================================
 * Dictionary Building (for encoding) - with hash table for O(1) lookup
 * ============================================================ */

/* FNV-1a hash function */
static uint64_t fnv1a_hash(const char *data, size_t len) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)data[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

#define DICT_EMPTY ((size_t)-1)

typedef struct {
    char **keys;
    size_t *lens;
    size_t count;
    size_t cap;
    /* Hash table: stores indices into keys array, DICT_EMPTY = unused slot */
    size_t *htab;
    size_t htab_cap;
} Dict;

static void dict_init(Dict *d) {
    d->keys = NULL;
    d->lens = NULL;
    d->count = 0;
    d->cap = 0;
    d->htab = NULL;
    d->htab_cap = 0;
}

static void dict_free(Dict *d) {
    for (size_t i = 0; i < d->count; i++) {
        free(d->keys[i]);
    }
    free(d->keys);
    free(d->lens);
    free(d->htab);
}

static int dict_find(const Dict *d, const char *key, size_t len) {
    if (d->htab_cap == 0) return -1;

    uint64_t h = fnv1a_hash(key, len);
    size_t mask = d->htab_cap - 1;
    size_t slot = (size_t)(h & mask);

    /* Linear probing */
    for (size_t i = 0; i < d->htab_cap; i++) {
        size_t idx = d->htab[slot];
        if (idx == DICT_EMPTY) return -1;
        if (d->lens[idx] == len && memcmp(d->keys[idx], key, len) == 0) {
            return (int)idx;
        }
        slot = (slot + 1) & mask;
    }
    return -1;
}

static int dict_rehash(Dict *d, size_t new_htab_cap);

static int dict_add(Dict *d, const char *key, size_t len) {
    int idx = dict_find(d, key, len);
    if (idx >= 0) return idx;

    /* Grow keys array if needed */
    if (d->count >= d->cap) {
        size_t new_cap = d->cap ? d->cap * 2 : 16;
        char **new_keys = realloc(d->keys, new_cap * sizeof(char *));
        size_t *new_lens = realloc(d->lens, new_cap * sizeof(size_t));
        if (!new_keys || !new_lens) return -1;
        d->keys = new_keys;
        d->lens = new_lens;
        d->cap = new_cap;
    }

    /* Grow hash table if load factor > 0.7 */
    if (d->htab_cap == 0 || d->count * 10 >= d->htab_cap * 7) {
        size_t new_htab_cap = d->htab_cap ? d->htab_cap * 2 : 32;
        if (dict_rehash(d, new_htab_cap) != 0) return -1;
    }

    /* Add key */
    d->keys[d->count] = malloc(len + 1);
    if (!d->keys[d->count]) return -1;
    memcpy(d->keys[d->count], key, len);
    d->keys[d->count][len] = '\0';
    d->lens[d->count] = len;

    /* Insert into hash table */
    uint64_t h = fnv1a_hash(key, len);
    size_t mask = d->htab_cap - 1;
    size_t slot = (size_t)(h & mask);
    while (d->htab[slot] != DICT_EMPTY) {
        slot = (slot + 1) & mask;
    }
    d->htab[slot] = d->count;

    return (int)d->count++;
}

static int dict_rehash(Dict *d, size_t new_htab_cap) {
    size_t *new_htab = malloc(new_htab_cap * sizeof(size_t));
    if (!new_htab) return -1;

    /* Initialize all slots to empty */
    for (size_t i = 0; i < new_htab_cap; i++) {
        new_htab[i] = DICT_EMPTY;
    }

    /* Rehash existing entries */
    size_t mask = new_htab_cap - 1;
    for (size_t i = 0; i < d->count; i++) {
        uint64_t h = fnv1a_hash(d->keys[i], d->lens[i]);
        size_t slot = (size_t)(h & mask);
        while (new_htab[slot] != DICT_EMPTY) {
            slot = (slot + 1) & mask;
        }
        new_htab[slot] = i;
    }

    free(d->htab);
    d->htab = new_htab;
    d->htab_cap = new_htab_cap;
    return 0;
}

static void collect_keys(const COWRIEValue *v, Dict *d) {
    if (!v) return;

    switch (v->type) {
    case COWRIE_ARRAY:
        for (size_t i = 0; i < v->as.array.len; i++) {
            collect_keys(v->as.array.items[i], d);
        }
        break;
    case COWRIE_OBJECT:
        for (size_t i = 0; i < v->as.object.len; i++) {
            dict_add(d, v->as.object.members[i].key, v->as.object.members[i].key_len);
            collect_keys(v->as.object.members[i].value, d);
        }
        break;
    /* v2.1 Graph types - collect property keys */
    case COWRIE_NODE:
        for (size_t i = 0; i < v->as.node.prop_count; i++) {
            dict_add(d, v->as.node.props[i].key, v->as.node.props[i].key_len);
            collect_keys(v->as.node.props[i].value, d);
        }
        break;
    case COWRIE_EDGE:
        for (size_t i = 0; i < v->as.edge.prop_count; i++) {
            dict_add(d, v->as.edge.props[i].key, v->as.edge.props[i].key_len);
            collect_keys(v->as.edge.props[i].value, d);
        }
        break;
    case COWRIE_NODE_BATCH:
        for (size_t i = 0; i < v->as.node_batch.node_count; i++) {
            COWRIENode *n = &v->as.node_batch.nodes[i];
            for (size_t j = 0; j < n->prop_count; j++) {
                dict_add(d, n->props[j].key, n->props[j].key_len);
                collect_keys(n->props[j].value, d);
            }
        }
        break;
    case COWRIE_EDGE_BATCH:
        for (size_t i = 0; i < v->as.edge_batch.edge_count; i++) {
            COWRIEEdge *e = &v->as.edge_batch.edges[i];
            for (size_t j = 0; j < e->prop_count; j++) {
                dict_add(d, e->props[j].key, e->props[j].key_len);
                collect_keys(e->props[j].value, d);
            }
        }
        break;
    case COWRIE_GRAPH_SHARD:
        for (size_t i = 0; i < v->as.graph_shard.node_count; i++) {
            COWRIENode *n = &v->as.graph_shard.nodes[i];
            for (size_t j = 0; j < n->prop_count; j++) {
                dict_add(d, n->props[j].key, n->props[j].key_len);
                collect_keys(n->props[j].value, d);
            }
        }
        for (size_t i = 0; i < v->as.graph_shard.edge_count; i++) {
            COWRIEEdge *e = &v->as.graph_shard.edges[i];
            for (size_t j = 0; j < e->prop_count; j++) {
                dict_add(d, e->props[j].key, e->props[j].key_len);
                collect_keys(e->props[j].value, d);
            }
        }
        for (size_t i = 0; i < v->as.graph_shard.meta_count; i++) {
            dict_add(d, v->as.graph_shard.metadata[i].key, v->as.graph_shard.metadata[i].key_len);
            collect_keys(v->as.graph_shard.metadata[i].value, d);
        }
        break;
    default:
        break;
    }
}

/* ============================================================
 * Encoding
 * ============================================================ */

static int encode_value(COWRIEBuf *buf, const COWRIEValue *v, const Dict *dict);

static int encode_string_raw(COWRIEBuf *buf, const char *s, size_t len) {
    if (cowrie_put_uvarint(buf, len) != 0) return -1;
    return buf_put(buf, s, len);
}

static int encode_value(COWRIEBuf *buf, const COWRIEValue *v, const Dict *dict) {
    if (!v) return -1;

    switch (v->type) {
    case COWRIE_NULL:
        return buf_put_byte(buf, SJT_NULL);

    case COWRIE_BOOL:
        return buf_put_byte(buf, v->as.boolean ? SJT_TRUE : SJT_FALSE);

    case COWRIE_INT64:
        if (buf_put_byte(buf, SJT_INT64) != 0) return -1;
        return cowrie_put_uvarint(buf, cowrie_zigzag_encode(v->as.i64));

    case COWRIE_UINT64:
        if (buf_put_byte(buf, SJT_UINT64) != 0) return -1;
        return cowrie_put_uvarint(buf, v->as.u64);

    case COWRIE_FLOAT64: {
        if (buf_put_byte(buf, SJT_FLOAT64) != 0) return -1;
        uint64_t bits;
        memcpy(&bits, &v->as.f64, sizeof(bits));
        return buf_put(buf, &bits, sizeof(bits));
    }

    case COWRIE_DECIMAL128:
        if (buf_put_byte(buf, SJT_DECIMAL128) != 0) return -1;
        if (buf_put_byte(buf, (uint8_t)v->as.decimal128.scale) != 0) return -1;
        return buf_put(buf, v->as.decimal128.coef, 16);

    case COWRIE_STRING:
        if (buf_put_byte(buf, SJT_STRING) != 0) return -1;
        return encode_string_raw(buf, v->as.str.data, v->as.str.len);

    case COWRIE_BYTES:
        if (buf_put_byte(buf, SJT_BYTES) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.bytes.len) != 0) return -1;
        return buf_put(buf, v->as.bytes.data, v->as.bytes.len);

    case COWRIE_DATETIME64:
        if (buf_put_byte(buf, SJT_DATETIME64) != 0) return -1;
        return buf_put(buf, &v->as.datetime64, sizeof(int64_t));

    case COWRIE_UUID128:
        if (buf_put_byte(buf, SJT_UUID128) != 0) return -1;
        return buf_put(buf, v->as.uuid, 16);

    case COWRIE_BIGINT:
        if (buf_put_byte(buf, SJT_BIGINT) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.bigint.len) != 0) return -1;
        return buf_put(buf, v->as.bigint.data, v->as.bigint.len);

    case COWRIE_EXT:
        if (buf_put_byte(buf, SJT_EXT) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.ext.ext_type) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.ext.payload_len) != 0) return -1;
        return buf_put(buf, v->as.ext.payload, v->as.ext.payload_len);

    case COWRIE_ARRAY:
        if (buf_put_byte(buf, SJT_ARRAY) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.array.len) != 0) return -1;
        for (size_t i = 0; i < v->as.array.len; i++) {
            if (encode_value(buf, v->as.array.items[i], dict) != 0) return -1;
        }
        return 0;

    case COWRIE_OBJECT:
        if (buf_put_byte(buf, SJT_OBJECT) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.object.len) != 0) return -1;
        for (size_t i = 0; i < v->as.object.len; i++) {
            int idx = dict_find(dict, v->as.object.members[i].key, v->as.object.members[i].key_len);
            if (idx < 0) return -1;
            if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
            if (encode_value(buf, v->as.object.members[i].value, dict) != 0) return -1;
        }
        return 0;

    /* v2.1 extension types */
    case COWRIE_TENSOR:
        if (buf_put_byte(buf, SJT_TENSOR) != 0) return -1;
        if (buf_put_byte(buf, v->as.tensor.dtype) != 0) return -1;
        if (buf_put_byte(buf, v->as.tensor.rank) != 0) return -1;
        for (uint8_t i = 0; i < v->as.tensor.rank; i++) {
            if (cowrie_put_uvarint(buf, v->as.tensor.dims[i]) != 0) return -1;
        }
        if (cowrie_put_uvarint(buf, v->as.tensor.data_len) != 0) return -1;
        return buf_put(buf, v->as.tensor.data, v->as.tensor.data_len);

    case COWRIE_TENSOR_REF:
        if (buf_put_byte(buf, SJT_TENSOR_REF) != 0) return -1;
        if (buf_put_byte(buf, v->as.tensor_ref.store_id) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.tensor_ref.key_len) != 0) return -1;
        return buf_put(buf, v->as.tensor_ref.key, v->as.tensor_ref.key_len);

    case COWRIE_IMAGE: {
        if (buf_put_byte(buf, SJT_IMAGE) != 0) return -1;
        if (buf_put_byte(buf, v->as.image.format) != 0) return -1;
        if (buf_put(buf, &v->as.image.width, sizeof(uint16_t)) != 0) return -1;
        if (buf_put(buf, &v->as.image.height, sizeof(uint16_t)) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.image.data_len) != 0) return -1;
        return buf_put(buf, v->as.image.data, v->as.image.data_len);
    }

    case COWRIE_AUDIO: {
        if (buf_put_byte(buf, SJT_AUDIO) != 0) return -1;
        if (buf_put_byte(buf, v->as.audio.encoding) != 0) return -1;
        if (buf_put(buf, &v->as.audio.sample_rate, sizeof(uint32_t)) != 0) return -1;
        if (buf_put_byte(buf, v->as.audio.channels) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.audio.data_len) != 0) return -1;
        return buf_put(buf, v->as.audio.data, v->as.audio.data_len);
    }

    case COWRIE_ADJLIST: {
        if (buf_put_byte(buf, SJT_ADJLIST) != 0) return -1;
        if (buf_put_byte(buf, v->as.adjlist.id_width) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.adjlist.node_count) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.adjlist.edge_count) != 0) return -1;
        /* row_offsets as uvarint */
        for (size_t i = 0; i <= v->as.adjlist.node_count; i++) {
            if (cowrie_put_uvarint(buf, v->as.adjlist.row_offsets[i]) != 0) return -1;
        }
        /* col_indices as fixed-width LE */
        if (v->as.adjlist.id_width == COWRIE_ID_INT64) {
            int64_t *cols = (int64_t *)v->as.adjlist.col_indices;
            for (size_t i = 0; i < v->as.adjlist.edge_count; i++) {
                if (buf_put(buf, &cols[i], sizeof(int64_t)) != 0) return -1;
            }
        } else {
            int32_t *cols = (int32_t *)v->as.adjlist.col_indices;
            for (size_t i = 0; i < v->as.adjlist.edge_count; i++) {
                if (buf_put(buf, &cols[i], sizeof(int32_t)) != 0) return -1;
            }
        }
        return 0;
    }

    case COWRIE_RICH_TEXT: {
        if (buf_put_byte(buf, SJT_RICH_TEXT) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.rich_text.text_len) != 0) return -1;
        if (buf_put(buf, v->as.rich_text.text, v->as.rich_text.text_len) != 0) return -1;
        uint8_t flags = 0;
        if (v->as.rich_text.tokens && v->as.rich_text.token_count > 0) flags |= 0x01;
        if (v->as.rich_text.spans && v->as.rich_text.span_count > 0) flags |= 0x02;
        if (buf_put_byte(buf, flags) != 0) return -1;
        if (flags & 0x01) {
            if (cowrie_put_uvarint(buf, v->as.rich_text.token_count) != 0) return -1;
            for (size_t i = 0; i < v->as.rich_text.token_count; i++) {
                if (buf_put(buf, &v->as.rich_text.tokens[i], sizeof(int32_t)) != 0) return -1;
            }
        }
        if (flags & 0x02) {
            if (cowrie_put_uvarint(buf, v->as.rich_text.span_count) != 0) return -1;
            for (size_t i = 0; i < v->as.rich_text.span_count; i++) {
                if (cowrie_put_uvarint(buf, v->as.rich_text.spans[i].start) != 0) return -1;
                if (cowrie_put_uvarint(buf, v->as.rich_text.spans[i].end) != 0) return -1;
                if (cowrie_put_uvarint(buf, v->as.rich_text.spans[i].kind_id) != 0) return -1;
            }
        }
        return 0;
    }

    case COWRIE_DELTA: {
        if (buf_put_byte(buf, SJT_DELTA) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.delta.base_id) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.delta.op_count) != 0) return -1;
        for (size_t i = 0; i < v->as.delta.op_count; i++) {
            COWRIEDeltaOp_t *op = &v->as.delta.ops[i];
            if (buf_put_byte(buf, op->op_code) != 0) return -1;
            if (cowrie_put_uvarint(buf, op->field_id) != 0) return -1;
            if (op->op_code == COWRIE_DELTA_SET_FIELD || op->op_code == COWRIE_DELTA_APPEND_ARRAY) {
                if (encode_value(buf, op->value, dict) != 0) return -1;
            }
        }
        return 0;
    }

    /* v2.1 Graph types */
    case COWRIE_NODE: {
        if (buf_put_byte(buf, SJT_NODE) != 0) return -1;
        /* id:string */
        if (encode_string_raw(buf, v->as.node.id, v->as.node.id_len) != 0) return -1;
        /* labelCount:uvarint + labels:string* */
        if (cowrie_put_uvarint(buf, v->as.node.label_count) != 0) return -1;
        for (size_t i = 0; i < v->as.node.label_count; i++) {
            if (encode_string_raw(buf, v->as.node.labels[i], v->as.node.label_lens[i]) != 0) return -1;
        }
        /* propCount:uvarint + (dictIdx:uvarint + value)* */
        if (cowrie_put_uvarint(buf, v->as.node.prop_count) != 0) return -1;
        for (size_t i = 0; i < v->as.node.prop_count; i++) {
            int idx = dict_find(dict, v->as.node.props[i].key, v->as.node.props[i].key_len);
            if (idx < 0) return -1;
            if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
            if (encode_value(buf, v->as.node.props[i].value, dict) != 0) return -1;
        }
        return 0;
    }

    case COWRIE_EDGE: {
        if (buf_put_byte(buf, SJT_EDGE) != 0) return -1;
        /* srcId:string + dstId:string + type:string */
        if (encode_string_raw(buf, v->as.edge.from_id, v->as.edge.from_id_len) != 0) return -1;
        if (encode_string_raw(buf, v->as.edge.to_id, v->as.edge.to_id_len) != 0) return -1;
        if (encode_string_raw(buf, v->as.edge.edge_type, v->as.edge.edge_type_len) != 0) return -1;
        /* propCount:uvarint + (dictIdx:uvarint + value)* */
        if (cowrie_put_uvarint(buf, v->as.edge.prop_count) != 0) return -1;
        for (size_t i = 0; i < v->as.edge.prop_count; i++) {
            int idx = dict_find(dict, v->as.edge.props[i].key, v->as.edge.props[i].key_len);
            if (idx < 0) return -1;
            if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
            if (encode_value(buf, v->as.edge.props[i].value, dict) != 0) return -1;
        }
        return 0;
    }

    case COWRIE_NODE_BATCH: {
        if (buf_put_byte(buf, SJT_NODE_BATCH) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.node_batch.node_count) != 0) return -1;
        for (size_t i = 0; i < v->as.node_batch.node_count; i++) {
            COWRIENode *n = &v->as.node_batch.nodes[i];
            /* Inline node encoding (without tag) */
            if (encode_string_raw(buf, n->id, n->id_len) != 0) return -1;
            if (cowrie_put_uvarint(buf, n->label_count) != 0) return -1;
            for (size_t j = 0; j < n->label_count; j++) {
                if (encode_string_raw(buf, n->labels[j], n->label_lens[j]) != 0) return -1;
            }
            if (cowrie_put_uvarint(buf, n->prop_count) != 0) return -1;
            for (size_t j = 0; j < n->prop_count; j++) {
                int idx = dict_find(dict, n->props[j].key, n->props[j].key_len);
                if (idx < 0) return -1;
                if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
                if (encode_value(buf, n->props[j].value, dict) != 0) return -1;
            }
        }
        return 0;
    }

    case COWRIE_EDGE_BATCH: {
        if (buf_put_byte(buf, SJT_EDGE_BATCH) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.edge_batch.edge_count) != 0) return -1;
        for (size_t i = 0; i < v->as.edge_batch.edge_count; i++) {
            COWRIEEdge *e = &v->as.edge_batch.edges[i];
            /* Inline edge encoding (without tag) */
            if (encode_string_raw(buf, e->from_id, e->from_id_len) != 0) return -1;
            if (encode_string_raw(buf, e->to_id, e->to_id_len) != 0) return -1;
            if (encode_string_raw(buf, e->edge_type, e->edge_type_len) != 0) return -1;
            if (cowrie_put_uvarint(buf, e->prop_count) != 0) return -1;
            for (size_t j = 0; j < e->prop_count; j++) {
                int idx = dict_find(dict, e->props[j].key, e->props[j].key_len);
                if (idx < 0) return -1;
                if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
                if (encode_value(buf, e->props[j].value, dict) != 0) return -1;
            }
        }
        return 0;
    }

    case COWRIE_GRAPH_SHARD: {
        if (buf_put_byte(buf, SJT_GRAPH_SHARD) != 0) return -1;
        /* nodeCount:uvarint + Node* */
        if (cowrie_put_uvarint(buf, v->as.graph_shard.node_count) != 0) return -1;
        for (size_t i = 0; i < v->as.graph_shard.node_count; i++) {
            COWRIENode *n = &v->as.graph_shard.nodes[i];
            if (encode_string_raw(buf, n->id, n->id_len) != 0) return -1;
            if (cowrie_put_uvarint(buf, n->label_count) != 0) return -1;
            for (size_t j = 0; j < n->label_count; j++) {
                if (encode_string_raw(buf, n->labels[j], n->label_lens[j]) != 0) return -1;
            }
            if (cowrie_put_uvarint(buf, n->prop_count) != 0) return -1;
            for (size_t j = 0; j < n->prop_count; j++) {
                int idx = dict_find(dict, n->props[j].key, n->props[j].key_len);
                if (idx < 0) return -1;
                if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
                if (encode_value(buf, n->props[j].value, dict) != 0) return -1;
            }
        }
        /* edgeCount:uvarint + Edge* */
        if (cowrie_put_uvarint(buf, v->as.graph_shard.edge_count) != 0) return -1;
        for (size_t i = 0; i < v->as.graph_shard.edge_count; i++) {
            COWRIEEdge *e = &v->as.graph_shard.edges[i];
            if (encode_string_raw(buf, e->from_id, e->from_id_len) != 0) return -1;
            if (encode_string_raw(buf, e->to_id, e->to_id_len) != 0) return -1;
            if (encode_string_raw(buf, e->edge_type, e->edge_type_len) != 0) return -1;
            if (cowrie_put_uvarint(buf, e->prop_count) != 0) return -1;
            for (size_t j = 0; j < e->prop_count; j++) {
                int idx = dict_find(dict, e->props[j].key, e->props[j].key_len);
                if (idx < 0) return -1;
                if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
                if (encode_value(buf, e->props[j].value, dict) != 0) return -1;
            }
        }
        /* metaCount:uvarint + (dictIdx:uvarint + value)* */
        if (cowrie_put_uvarint(buf, v->as.graph_shard.meta_count) != 0) return -1;
        for (size_t i = 0; i < v->as.graph_shard.meta_count; i++) {
            int idx = dict_find(dict, v->as.graph_shard.metadata[i].key, v->as.graph_shard.metadata[i].key_len);
            if (idx < 0) return -1;
            if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) return -1;
            if (encode_value(buf, v->as.graph_shard.metadata[i].value, dict) != 0) return -1;
        }
        return 0;
    }
    }

    return -1;
}

/* Forward declaration for deterministic encoding */
static int encode_value_deterministic(COWRIEBuf *buf, const COWRIEValue *v, const Dict *dict, const COWRIEEncodeOpts *opts);

/* Compare function for sorting object members by key */
static int member_key_compare(const void *a, const void *b) {
    const COWRIEMember *ma = (const COWRIEMember *)a;
    const COWRIEMember *mb = (const COWRIEMember *)b;
    return strcmp(ma->key, mb->key);
}

/* Count non-null members in object */
static size_t count_non_null_members(const COWRIEValue *v) {
    size_t count = 0;
    for (size_t i = 0; i < v->as.object.len; i++) {
        if (v->as.object.members[i].value->type != COWRIE_NULL) {
            count++;
        }
    }
    return count;
}

/* Encode value with deterministic key ordering for objects */
static int encode_value_deterministic(COWRIEBuf *buf, const COWRIEValue *v, const Dict *dict, const COWRIEEncodeOpts *opts) {
    if (!v) return -1;

    switch (v->type) {
    case COWRIE_NULL:
        return buf_put_byte(buf, SJT_NULL);

    case COWRIE_BOOL:
        return buf_put_byte(buf, v->as.boolean ? SJT_TRUE : SJT_FALSE);

    case COWRIE_INT64: {
        if (buf_put_byte(buf, SJT_INT64) != 0) return -1;
        return cowrie_put_uvarint(buf, cowrie_zigzag_encode(v->as.i64));
    }

    case COWRIE_UINT64:
        if (buf_put_byte(buf, SJT_UINT64) != 0) return -1;
        return cowrie_put_uvarint(buf, v->as.u64);

    case COWRIE_FLOAT64: {
        if (buf_put_byte(buf, SJT_FLOAT64) != 0) return -1;
        /* Write float64 as 8 bytes LE - use direct memcpy (assumes LE platform) */
        return buf_put(buf, &v->as.f64, 8);
    }

    case COWRIE_STRING:
        if (buf_put_byte(buf, SJT_STRING) != 0) return -1;
        return encode_string_raw(buf, v->as.str.data, v->as.str.len);

    case COWRIE_BYTES:
        if (buf_put_byte(buf, SJT_BYTES) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.bytes.len) != 0) return -1;
        return buf_put(buf, v->as.bytes.data, v->as.bytes.len);

    case COWRIE_ARRAY:
        if (buf_put_byte(buf, SJT_ARRAY) != 0) return -1;
        if (cowrie_put_uvarint(buf, v->as.array.len) != 0) return -1;
        for (size_t i = 0; i < v->as.array.len; i++) {
            if (encode_value_deterministic(buf, v->as.array.items[i], dict, opts) != 0) return -1;
        }
        return 0;

    case COWRIE_OBJECT: {
        /* Sort members by key for deterministic output */
        size_t n = v->as.object.len;

        /* Count members to encode (filter nulls if omit_null) */
        size_t encode_count = (opts && opts->omit_null) ? count_non_null_members(v) : n;

        if (buf_put_byte(buf, SJT_OBJECT) != 0) return -1;
        if (cowrie_put_uvarint(buf, encode_count) != 0) return -1;

        if (n == 0) return 0;

        COWRIEMember *sorted = malloc(n * sizeof(COWRIEMember));
        if (!sorted) return -1;
        memcpy(sorted, v->as.object.members, n * sizeof(COWRIEMember));
        qsort(sorted, n, sizeof(COWRIEMember), member_key_compare);

        for (size_t i = 0; i < n; i++) {
            /* Skip null values if omit_null is set */
            if (opts && opts->omit_null && sorted[i].value->type == COWRIE_NULL) {
                continue;
            }
            int idx = dict_find(dict, sorted[i].key, sorted[i].key_len);
            if (idx < 0) { free(sorted); return -1; }
            if (cowrie_put_uvarint(buf, (uint64_t)idx) != 0) { free(sorted); return -1; }
            if (encode_value_deterministic(buf, sorted[i].value, dict, opts) != 0) { free(sorted); return -1; }
        }
        free(sorted);
        return 0;
    }

    /* For other types, fall through to regular encode_value */
    default:
        return encode_value(buf, v, dict);
    }
}

int cowrie_encode(const COWRIEValue *root, COWRIEBuf *buf) {
    cowrie_buf_init(buf);

    /* Build dictionary */
    Dict dict;
    dict_init(&dict);
    collect_keys(root, &dict);

    /* Write header */
    if (buf_put_byte(buf, COWRIE_MAGIC_0) != 0) goto fail;
    if (buf_put_byte(buf, COWRIE_MAGIC_1) != 0) goto fail;
    if (buf_put_byte(buf, COWRIE_VERSION) != 0) goto fail;
    if (buf_put_byte(buf, 0) != 0) goto fail; /* flags = 0 (no compression) */

    /* Write dictionary */
    if (cowrie_put_uvarint(buf, dict.count) != 0) goto fail;
    for (size_t i = 0; i < dict.count; i++) {
        if (encode_string_raw(buf, dict.keys[i], dict.lens[i]) != 0) goto fail;
    }

    /* Write root value */
    if (encode_value(buf, root, &dict) != 0) goto fail;

    dict_free(&dict);
    return 0;

fail:
    dict_free(&dict);
    cowrie_buf_free(buf);
    return -1;
}

int cowrie_encode_with_opts(const COWRIEValue *root, const COWRIEEncodeOpts *opts, COWRIEBuf *buf) {
    cowrie_buf_init(buf);

    /* Build dictionary - need to sort keys for deterministic dictionary order */
    Dict dict;
    dict_init(&dict);
    collect_keys(root, &dict);

    /* Sort dictionary keys for deterministic output */
    if (opts && opts->deterministic && dict.count > 1) {
        /* Simple bubble sort for dictionary (usually small) */
        for (size_t i = 0; i < dict.count - 1; i++) {
            for (size_t j = 0; j < dict.count - i - 1; j++) {
                if (strcmp(dict.keys[j], dict.keys[j + 1]) > 0) {
                    char *tmp_key = dict.keys[j];
                    dict.keys[j] = dict.keys[j + 1];
                    dict.keys[j + 1] = tmp_key;
                    size_t tmp_len = dict.lens[j];
                    dict.lens[j] = dict.lens[j + 1];
                    dict.lens[j + 1] = tmp_len;
                }
            }
        }
        /* Rebuild hash table after sorting - indices have changed */
        if (dict_rehash(&dict, dict.htab_cap) != 0) goto fail;
    }

    /* Write header */
    if (buf_put_byte(buf, COWRIE_MAGIC_0) != 0) goto fail;
    if (buf_put_byte(buf, COWRIE_MAGIC_1) != 0) goto fail;
    if (buf_put_byte(buf, COWRIE_VERSION) != 0) goto fail;
    if (buf_put_byte(buf, 0) != 0) goto fail; /* flags = 0 (no compression) */

    /* Write dictionary */
    if (cowrie_put_uvarint(buf, dict.count) != 0) goto fail;
    for (size_t i = 0; i < dict.count; i++) {
        if (encode_string_raw(buf, dict.keys[i], dict.lens[i]) != 0) goto fail;
    }

    /* Write root value */
    if (opts && opts->deterministic) {
        if (encode_value_deterministic(buf, root, &dict, opts) != 0) goto fail;
    } else {
        if (encode_value(buf, root, &dict) != 0) goto fail;
    }

    dict_free(&dict);
    return 0;

fail:
    dict_free(&dict);
    cowrie_buf_free(buf);
    return -1;
}

/* ============================================================
 * Decoding
 * ============================================================ */

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    int depth;              /* Current nesting depth */
    COWRIEDecodeOpts opts;   /* Security limits */
} Reader;

/* Check remaining bytes in reader */
static size_t rd_remaining(const Reader *r) {
    return r->len - r->pos;
}

/* Overflow-safe multiplication: returns 0 on success, -1 on overflow */
static int safe_mul(size_t a, size_t b, size_t *result) {
    if (a != 0 && b > SIZE_MAX / a) return -1;
    *result = a * b;
    return 0;
}

/* Overflow-safe addition: returns 0 on success, -1 on overflow */
static int safe_add(size_t a, size_t b, size_t *result) {
    if (a > SIZE_MAX - b) return -1;
    *result = a + b;
    return 0;
}

static int rd_get_byte(Reader *r, uint8_t *out) {
    if (r->pos >= r->len) return -1;
    *out = r->data[r->pos++];
    return 0;
}

static int rd_get(Reader *r, void *out, size_t n) {
    if (r->pos + n > r->len) return -1;
    memcpy(out, r->data + r->pos, n);
    r->pos += n;
    return 0;
}

static int rd_get_uvarint(Reader *r, uint64_t *out) {
    size_t bytes_read;
    if (cowrie_get_uvarint(r->data + r->pos, r->len - r->pos, out, &bytes_read) != 0) {
        return -1;
    }
    r->pos += bytes_read;
    return 0;
}

/* UTF-8 validation using state machine approach.
 * Returns 0 if valid UTF-8, -1 if invalid.
 * Rejects: overlong encodings, surrogate code points (0xD800-0xDFFF),
 * invalid bytes (0xC0, 0xC1, 0xF5-0xFF), truncated sequences.
 */
static int validate_utf8(const char *data, size_t len) {
    const uint8_t *s = (const uint8_t *)data;
    const uint8_t *end = s + len;

    while (s < end) {
        uint8_t b = *s++;

        if (b <= 0x7F) {
            /* ASCII: valid single byte */
            continue;
        }

        /* Multi-byte sequence */
        int bytes_remaining;
        uint32_t codepoint;
        uint32_t min_codepoint;

        if ((b & 0xE0) == 0xC0) {
            /* 2-byte sequence: 110xxxxx 10xxxxxx */
            if (b < 0xC2) return -1; /* Overlong (0xC0, 0xC1 are invalid) */
            bytes_remaining = 1;
            codepoint = b & 0x1F;
            min_codepoint = 0x80;
        } else if ((b & 0xF0) == 0xE0) {
            /* 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx */
            bytes_remaining = 2;
            codepoint = b & 0x0F;
            min_codepoint = 0x800;
        } else if ((b & 0xF8) == 0xF0) {
            /* 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx */
            if (b > 0xF4) return -1; /* Beyond Unicode range */
            bytes_remaining = 3;
            codepoint = b & 0x07;
            min_codepoint = 0x10000;
        } else {
            /* Invalid leading byte (0x80-0xBF, 0xF5-0xFF) */
            return -1;
        }

        /* Check we have enough bytes */
        if (s + bytes_remaining > end) return -1;

        /* Process continuation bytes */
        for (int i = 0; i < bytes_remaining; i++) {
            b = *s++;
            if ((b & 0xC0) != 0x80) return -1; /* Not a continuation byte */
            codepoint = (codepoint << 6) | (b & 0x3F);
        }

        /* Check for overlong encoding */
        if (codepoint < min_codepoint) return -1;

        /* Check for surrogate code points (0xD800-0xDFFF) */
        if (codepoint >= 0xD800 && codepoint <= 0xDFFF) return -1;

        /* Check for code points beyond Unicode max (0x10FFFF) */
        if (codepoint > 0x10FFFF) return -1;
    }

    return 0;
}

static int decode_value(Reader *r, char **dict, size_t dict_len, COWRIEValue **out);
static int decode_string_raw(Reader *r, char **out, size_t *out_len);

static int skip_hints(Reader *r) {
    uint64_t count;
    if (rd_get_uvarint(r, &count) != 0) return -1;

    /* Enforce hint count limit to prevent CPU spin attacks */
    if (r->opts.max_hint_count > 0 && count > r->opts.max_hint_count) return -1;

    /* Sanity check: each hint is at least 3 bytes (1 field name len + 1 type + 1 flags) */
    if (count > 0 && count * 3 > rd_remaining(r)) return -1;

    for (uint64_t i = 0; i < count; i++) {
        /* Skip field name */
        char *field = NULL;
        size_t field_len = 0;
        if (decode_string_raw(r, &field, &field_len) != 0) return -1;
        free(field);

        /* Skip type (1 byte) */
        uint8_t typ;
        if (rd_get_byte(r, &typ) != 0) return -1;

        /* Skip shape */
        uint64_t shape_len;
        if (rd_get_uvarint(r, &shape_len) != 0) return -1;
        if (r->opts.max_rank > 0 && shape_len > (uint64_t)r->opts.max_rank) return -1;
        for (uint64_t j = 0; j < shape_len; j++) {
            uint64_t dim;
            if (rd_get_uvarint(r, &dim) != 0) return -1;
        }

        /* Skip flags (1 byte) */
        uint8_t flags;
        if (rd_get_byte(r, &flags) != 0) return -1;
    }

    return 0;
}

static int decode_string_raw(Reader *r, char **out, size_t *out_len) {
    uint64_t len;
    if (rd_get_uvarint(r, &len) != 0) return -1;
    if (len > SIZE_MAX - 1) return -1;
    
    /* Sanity check: length can't exceed remaining data */
    if (len > rd_remaining(r)) return -1;
    
    /* Security limit check */
    if (r->opts.max_string_len > 0 && len > r->opts.max_string_len) return -1;

    char *s = malloc((size_t)len + 1);
    if (!s) return -1;

    if (rd_get(r, s, (size_t)len) != 0) {
        free(s);
        return -1;
    }
    s[len] = '\0';

    /* Validate UTF-8 encoding */
    if (validate_utf8(s, (size_t)len) != 0) {
        free(s);
        return -1;
    }

    *out = s;
    *out_len = (size_t)len;
    return 0;
}

static int decode_value(Reader *r, char **dict, size_t dict_len, COWRIEValue **out) {
    uint8_t tag;
    if (rd_get_byte(r, &tag) != 0) return -1;

    switch (tag) {
    case SJT_NULL:
        *out = cowrie_new_null();
        return *out ? 0 : -1;

    case SJT_FALSE:
        *out = cowrie_new_bool(0);
        return *out ? 0 : -1;

    case SJT_TRUE:
        *out = cowrie_new_bool(1);
        return *out ? 0 : -1;

    case SJT_INT64: {
        uint64_t ux;
        if (rd_get_uvarint(r, &ux) != 0) return -1;
        *out = cowrie_new_int64(cowrie_zigzag_decode(ux));
        return *out ? 0 : -1;
    }

    case SJT_UINT64: {
        uint64_t u;
        if (rd_get_uvarint(r, &u) != 0) return -1;
        *out = cowrie_new_uint64(u);
        return *out ? 0 : -1;
    }

    case SJT_FLOAT64: {
        uint64_t bits;
        if (rd_get(r, &bits, sizeof(bits)) != 0) return -1;
        double f;
        memcpy(&f, &bits, sizeof(f));
        *out = cowrie_new_float64(f);
        return *out ? 0 : -1;
    }

    case SJT_FLOAT32: {
        uint32_t bits;
        if (rd_get(r, &bits, sizeof(bits)) != 0) return -1;
        float f;
        memcpy(&f, &bits, sizeof(f));
        *out = cowrie_new_float64((double)f);
        return *out ? 0 : -1;
    }

    case SJT_DECIMAL128: {
        uint8_t scale;
        uint8_t coef[16];
        if (rd_get_byte(r, &scale) != 0) return -1;
        if (rd_get(r, coef, 16) != 0) return -1;
        *out = cowrie_new_decimal128((int8_t)scale, coef);
        return *out ? 0 : -1;
    }

    case SJT_STRING: {
        char *s;
        size_t len;
        if (decode_string_raw(r, &s, &len) != 0) return -1;
        COWRIEValue *v = alloc_value(COWRIE_STRING);
        if (!v) { free(s); return -1; }
        v->as.str.data = s;
        v->as.str.len = len;
        *out = v;
        return 0;
    }

    case SJT_BYTES: {
        uint64_t len;
        if (rd_get_uvarint(r, &len) != 0) return -1;
        if (len > SIZE_MAX) return -1;
        if (len > rd_remaining(r)) return -1;
        uint8_t *data = malloc((size_t)len);
        if (!data && len > 0) return -1;
        if (len > 0 && rd_get(r, data, (size_t)len) != 0) { free(data); return -1; }
        *out = cowrie_new_bytes(data, (size_t)len);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_DATETIME64: {
        int64_t nanos;
        if (rd_get(r, &nanos, sizeof(nanos)) != 0) return -1;
        *out = cowrie_new_datetime64(nanos);
        return *out ? 0 : -1;
    }

    case SJT_UUID128: {
        uint8_t uuid[16];
        if (rd_get(r, uuid, 16) != 0) return -1;
        *out = cowrie_new_uuid128(uuid);
        return *out ? 0 : -1;
    }

    case SJT_BIGINT: {
        uint64_t len;
        if (rd_get_uvarint(r, &len) != 0) return -1;
        if (len > SIZE_MAX) return -1;
        if (len > rd_remaining(r)) return -1;
        uint8_t *data = malloc((size_t)len);
        if (!data && len > 0) return -1;
        if (len > 0 && rd_get(r, data, (size_t)len) != 0) { free(data); return -1; }
        *out = cowrie_new_bigint(data, (size_t)len);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_EXT: {
        uint64_t ext_type;
        uint64_t len;
        if (rd_get_uvarint(r, &ext_type) != 0) return -1;
        if (rd_get_uvarint(r, &len) != 0) return -1;
        if (len > SIZE_MAX) return -1;
        if (r->opts.max_ext_len > 0 && len > r->opts.max_ext_len) return -1;
        if (r->opts.max_bytes_len > 0 && len > r->opts.max_bytes_len) return -1;
        uint8_t *data = malloc((size_t)len);
        if (!data && len > 0) return -1;
        if (len > 0 && rd_get(r, data, (size_t)len) != 0) { free(data); return -1; }
        if (r->opts.unknown_ext == COWRIE_UNKNOWN_EXT_ERROR) {
            free(data);
            return -1;
        }
        if (r->opts.unknown_ext == COWRIE_UNKNOWN_EXT_SKIP_AS_NULL) {
            free(data);
            *out = cowrie_new_null();
            return *out ? 0 : -1;
        }
        *out = cowrie_new_ext(ext_type, data, (size_t)len);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_ARRAY: {
        /* Depth tracking */
        r->depth++;
        if (r->opts.max_depth > 0 && r->depth > r->opts.max_depth) {
            r->depth--;
            return -1;
        }
        
        uint64_t count;
        if (rd_get_uvarint(r, &count) != 0) { r->depth--; return -1; }
        
        /* Sanity check: count can't exceed remaining bytes */
        if (count > rd_remaining(r)) { r->depth--; return -1; }
        
        /* Security limit check */
        if (r->opts.max_array_len > 0 && count > r->opts.max_array_len) {
            r->depth--;
            return -1;
        }

        COWRIEValue *arr = cowrie_new_array();
        if (!arr) { r->depth--; return -1; }

        for (uint64_t i = 0; i < count; i++) {
            COWRIEValue *item;
            if (decode_value(r, dict, dict_len, &item) != 0) {
                cowrie_free(arr);
                r->depth--;
                return -1;
            }
            if (cowrie_array_append(arr, item) != 0) {
                cowrie_free(item);
                cowrie_free(arr);
                r->depth--;
                return -1;
            }
        }
        r->depth--;
        *out = arr;
        return 0;
    }

    case SJT_OBJECT: {
        /* Depth tracking */
        r->depth++;
        if (r->opts.max_depth > 0 && r->depth > r->opts.max_depth) {
            r->depth--;
            return -1;
        }
        
        uint64_t count;
        if (rd_get_uvarint(r, &count) != 0) { r->depth--; return -1; }
        
        /* Sanity check: count can't exceed remaining bytes / 2 (fieldID + value tag) */
        if (count > rd_remaining(r) / 2) { r->depth--; return -1; }
        
        /* Security limit check */
        if (r->opts.max_object_len > 0 && count > r->opts.max_object_len) {
            r->depth--;
            return -1;
        }

        COWRIEValue *obj = cowrie_new_object();
        if (!obj) { r->depth--; return -1; }

        for (uint64_t i = 0; i < count; i++) {
            uint64_t field_id;
            if (rd_get_uvarint(r, &field_id) != 0) {
                cowrie_free(obj);
                r->depth--;
                return -1;
            }
            if (field_id >= dict_len) {
                cowrie_free(obj);
                r->depth--;
                return -1;
            }

            COWRIEValue *val;
            if (decode_value(r, dict, dict_len, &val) != 0) {
                cowrie_free(obj);
                r->depth--;
                return -1;
            }

            size_t key_len = strlen(dict[field_id]);
            if (cowrie_object_set(obj, dict[field_id], key_len, val) != 0) {
                cowrie_free(val);
                cowrie_free(obj);
                r->depth--;
                return -1;
            }
        }
        r->depth--;
        *out = obj;
        return 0;
    }

    /* v2.1 extension types */
    case SJT_TENSOR: {
        uint8_t dtype, rank;
        if (rd_get_byte(r, &dtype) != 0) return -1;
        if (rd_get_byte(r, &rank) != 0) return -1;
        if (r->opts.max_rank > 0 && rank > (uint8_t)r->opts.max_rank) return -1;

        size_t *dims = NULL;
        if (rank > 0) {
            dims = malloc(rank * sizeof(size_t));
            if (!dims) return -1;
            for (uint8_t i = 0; i < rank; i++) {
                uint64_t dim;
                if (rd_get_uvarint(r, &dim) != 0) { free(dims); return -1; }
                dims[i] = (size_t)dim;
            }
        }

        uint64_t data_len;
        if (rd_get_uvarint(r, &data_len) != 0) { free(dims); return -1; }
        
        /* Sanity check: data_len can't exceed remaining */
        if (data_len > rd_remaining(r)) { free(dims); return -1; }
        
        /* Security limit check (use bytes limit for tensor data) */
        if (r->opts.max_bytes_len > 0 && data_len > r->opts.max_bytes_len) {
            free(dims);
            return -1;
        }

        uint8_t *data = NULL;
        if (data_len > 0) {
            data = malloc((size_t)data_len);
            if (!data) { free(dims); return -1; }
            if (rd_get(r, data, (size_t)data_len) != 0) { free(data); free(dims); return -1; }
        }

        *out = cowrie_new_tensor(dtype, rank, dims, data, (size_t)data_len);
        free(dims);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_TENSOR_REF: {
        uint8_t store_id;
        if (rd_get_byte(r, &store_id) != 0) return -1;

        uint64_t key_len;
        if (rd_get_uvarint(r, &key_len) != 0) return -1;
        if (key_len > rd_remaining(r)) return -1;

        uint8_t *key = NULL;
        if (key_len > 0) {
            key = malloc((size_t)key_len);
            if (!key) return -1;
            if (rd_get(r, key, (size_t)key_len) != 0) { free(key); return -1; }
        }

        *out = cowrie_new_tensor_ref(store_id, key, (size_t)key_len);
        free(key);
        return *out ? 0 : -1;
    }

    case SJT_IMAGE: {
        uint8_t format;
        uint16_t width, height;
        if (rd_get_byte(r, &format) != 0) return -1;
        if (rd_get(r, &width, sizeof(uint16_t)) != 0) return -1;
        if (rd_get(r, &height, sizeof(uint16_t)) != 0) return -1;

        uint64_t data_len;
        if (rd_get_uvarint(r, &data_len) != 0) return -1;
        if (data_len > rd_remaining(r)) return -1;

        uint8_t *data = NULL;
        if (data_len > 0) {
            data = malloc((size_t)data_len);
            if (!data) return -1;
            if (rd_get(r, data, (size_t)data_len) != 0) { free(data); return -1; }
        }

        *out = cowrie_new_image(format, width, height, data, (size_t)data_len);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_AUDIO: {
        uint8_t encoding, channels;
        uint32_t sample_rate;
        if (rd_get_byte(r, &encoding) != 0) return -1;
        if (rd_get(r, &sample_rate, sizeof(uint32_t)) != 0) return -1;
        if (rd_get_byte(r, &channels) != 0) return -1;

        uint64_t data_len;
        if (rd_get_uvarint(r, &data_len) != 0) return -1;
        if (data_len > rd_remaining(r)) return -1;

        uint8_t *data = NULL;
        if (data_len > 0) {
            data = malloc((size_t)data_len);
            if (!data) return -1;
            if (rd_get(r, data, (size_t)data_len) != 0) { free(data); return -1; }
        }

        *out = cowrie_new_audio(encoding, sample_rate, channels, data, (size_t)data_len);
        free(data);
        return *out ? 0 : -1;
    }

    case SJT_ADJLIST: {
        uint8_t id_width;
        if (rd_get_byte(r, &id_width) != 0) return -1;

        uint64_t node_count, edge_count;
        if (rd_get_uvarint(r, &node_count) != 0) return -1;
        if (rd_get_uvarint(r, &edge_count) != 0) return -1;

        /* Overflow-safe allocation: node_count + 1 could wrap, then * sizeof could overflow */
        size_t ro_count;
        if (safe_add((size_t)node_count, 1, &ro_count) != 0) return -1;
        size_t ro_alloc;
        if (safe_mul(ro_count, sizeof(size_t), &ro_alloc) != 0) return -1;
        if (ro_alloc > rd_remaining(r)) return -1;
        size_t *row_offsets = malloc(ro_alloc);
        if (!row_offsets) return -1;
        for (size_t i = 0; i <= (size_t)node_count; i++) {
            uint64_t off;
            if (rd_get_uvarint(r, &off) != 0) { free(row_offsets); return -1; }
            row_offsets[i] = (size_t)off;
        }

        void *col_indices = NULL;
        if (edge_count > 0) {
            size_t elem_size = (id_width == COWRIE_ID_INT64) ? sizeof(int64_t) : sizeof(int32_t);
            size_t ci_alloc;
            if (safe_mul((size_t)edge_count, elem_size, &ci_alloc) != 0) { free(row_offsets); return -1; }
            if (ci_alloc > rd_remaining(r)) { free(row_offsets); return -1; }
            col_indices = malloc(ci_alloc);
            if (!col_indices) { free(row_offsets); return -1; }
            if (rd_get(r, col_indices, ci_alloc) != 0) {
                free(col_indices);
                free(row_offsets);
                return -1;
            }
        }

        *out = cowrie_new_adjlist(id_width, (size_t)node_count, (size_t)edge_count, row_offsets, col_indices);
        free(row_offsets);
        free(col_indices);
        return *out ? 0 : -1;
    }

    case SJT_RICH_TEXT: {
        uint64_t text_len;
        if (rd_get_uvarint(r, &text_len) != 0) return -1;
        if (text_len > SIZE_MAX - 1) return -1;
        if (text_len > rd_remaining(r)) return -1;

        char *text = malloc((size_t)text_len + 1);
        if (!text) return -1;
        if (text_len > 0 && rd_get(r, text, (size_t)text_len) != 0) { free(text); return -1; }
        text[text_len] = '\0';

        uint8_t flags;
        if (rd_get_byte(r, &flags) != 0) { free(text); return -1; }

        int32_t *tokens = NULL;
        size_t token_count = 0;
        if (flags & 0x01) {
            uint64_t tc;
            if (rd_get_uvarint(r, &tc) != 0) { free(text); return -1; }
            token_count = (size_t)tc;
            if (token_count > 0) {
                size_t tok_alloc;
                if (safe_mul(token_count, sizeof(int32_t), &tok_alloc) != 0) { free(text); return -1; }
                if (tok_alloc > rd_remaining(r)) { free(text); return -1; }
                tokens = malloc(tok_alloc);
                if (!tokens) { free(text); return -1; }
                if (rd_get(r, tokens, tok_alloc) != 0) {
                    free(tokens);
                    free(text);
                    return -1;
                }
            }
        }

        COWRIERichTextSpan *spans = NULL;
        size_t span_count = 0;
        if (flags & 0x02) {
            uint64_t sc;
            if (rd_get_uvarint(r, &sc) != 0) { free(tokens); free(text); return -1; }
            span_count = (size_t)sc;
            if (span_count > 0) {
                size_t span_alloc;
                if (safe_mul(span_count, sizeof(COWRIERichTextSpan), &span_alloc) != 0) { free(tokens); free(text); return -1; }
                spans = malloc(span_alloc);
                if (!spans) { free(tokens); free(text); return -1; }
                for (size_t i = 0; i < span_count; i++) {
                    uint64_t start, end, kind_id;
                    if (rd_get_uvarint(r, &start) != 0 ||
                        rd_get_uvarint(r, &end) != 0 ||
                        rd_get_uvarint(r, &kind_id) != 0) {
                        free(spans);
                        free(tokens);
                        free(text);
                        return -1;
                    }
                    spans[i].start = (size_t)start;
                    spans[i].end = (size_t)end;
                    spans[i].kind_id = (size_t)kind_id;
                }
            }
        }

        *out = cowrie_new_rich_text(text, (size_t)text_len, tokens, token_count, spans, span_count);
        free(spans);
        free(tokens);
        free(text);
        return *out ? 0 : -1;
    }

    case SJT_DELTA: {
        uint64_t base_id, op_count;
        if (rd_get_uvarint(r, &base_id) != 0) return -1;
        if (rd_get_uvarint(r, &op_count) != 0) return -1;

        COWRIEDeltaOp_t *ops = NULL;
        if (op_count > 0) {
            size_t ops_alloc;
            if (safe_mul((size_t)op_count, sizeof(COWRIEDeltaOp_t), &ops_alloc) != 0) return -1;
            ops = malloc(ops_alloc);
            if (!ops) return -1;

            for (size_t i = 0; i < (size_t)op_count; i++) {
                uint8_t op_code;
                uint64_t field_id;
                if (rd_get_byte(r, &op_code) != 0) { free(ops); return -1; }
                if (rd_get_uvarint(r, &field_id) != 0) { free(ops); return -1; }

                ops[i].op_code = op_code;
                ops[i].field_id = (size_t)field_id;
                ops[i].value = NULL;

                if (op_code == COWRIE_DELTA_SET_FIELD || op_code == COWRIE_DELTA_APPEND_ARRAY) {
                    if (decode_value(r, dict, dict_len, &ops[i].value) != 0) {
                        /* Free previously allocated values */
                        for (size_t j = 0; j < i; j++) {
                            cowrie_free(ops[j].value);
                        }
                        free(ops);
                        return -1;
                    }
                }
            }
        }

        *out = cowrie_new_delta((size_t)base_id, ops, (size_t)op_count);
        /* Note: cowrie_new_delta takes ownership of value pointers */
        free(ops);  /* Free the array but not the values */
        return *out ? 0 : -1;
    }

    /* v2.1 Graph types */
    case SJT_NODE: {
        /* id:string */
        char *id;
        size_t id_len;
        if (decode_string_raw(r, &id, &id_len) != 0) return -1;

        /* labelCount:uvarint + labels:string* */
        uint64_t label_count;
        if (rd_get_uvarint(r, &label_count) != 0) { free(id); return -1; }
        /* Security limit check */
        if (r->opts.max_array_len > 0 && label_count > r->opts.max_array_len) {
            free(id);
            return -1;
        }

        char **labels = NULL;
        size_t *label_lens = NULL;
        if (label_count > 0) {
            size_t lbl_alloc, lbllen_alloc;
            if (safe_mul((size_t)label_count, sizeof(char *), &lbl_alloc) != 0 ||
                safe_mul((size_t)label_count, sizeof(size_t), &lbllen_alloc) != 0) {
                free(id); return -1;
            }
            labels = malloc(lbl_alloc);
            label_lens = malloc(lbllen_alloc);
            if (!labels || !label_lens) {
                free(labels); free(label_lens); free(id);
                return -1;
            }
            for (size_t i = 0; i < (size_t)label_count; i++) {
                if (decode_string_raw(r, &labels[i], &label_lens[i]) != 0) {
                    for (size_t j = 0; j < i; j++) free(labels[j]);
                    free(labels); free(label_lens); free(id);
                    return -1;
                }
            }
        }

        /* propCount:uvarint + (dictIdx:uvarint + value)* */
        uint64_t prop_count;
        if (rd_get_uvarint(r, &prop_count) != 0) {
            for (size_t i = 0; i < (size_t)label_count; i++) free(labels[i]);
            free(labels); free(label_lens); free(id);
            return -1;
        }
        /* Security limit check */
        if (r->opts.max_object_len > 0 && prop_count > r->opts.max_object_len) {
            for (size_t i = 0; i < (size_t)label_count; i++) free(labels[i]);
            free(labels); free(label_lens); free(id);
            return -1;
        }

        COWRIEMember *props = NULL;
        if (prop_count > 0) {
            size_t props_alloc;
            if (safe_mul((size_t)prop_count, sizeof(COWRIEMember), &props_alloc) != 0) {
                for (size_t i = 0; i < (size_t)label_count; i++) free(labels[i]);
                free(labels); free(label_lens); free(id);
                return -1;
            }
            props = malloc(props_alloc);
            if (!props) {
                for (size_t i = 0; i < (size_t)label_count; i++) free(labels[i]);
                free(labels); free(label_lens); free(id);
                return -1;
            }
            for (size_t i = 0; i < (size_t)prop_count; i++) {
                uint64_t field_id;
                if (rd_get_uvarint(r, &field_id) != 0 || field_id >= dict_len) {
                    for (size_t j = 0; j < i; j++) {
                        free(props[j].key);
                        cowrie_free(props[j].value);
                    }
                    free(props);
                    for (size_t j = 0; j < (size_t)label_count; j++) free(labels[j]);
                    free(labels); free(label_lens); free(id);
                    return -1;
                }
                props[i].key = strdup(dict[field_id]);
                props[i].key_len = strlen(dict[field_id]);
                if (decode_value(r, dict, dict_len, &props[i].value) != 0) {
                    free(props[i].key);
                    for (size_t j = 0; j < i; j++) {
                        free(props[j].key);
                        cowrie_free(props[j].value);
                    }
                    free(props);
                    for (size_t j = 0; j < (size_t)label_count; j++) free(labels[j]);
                    free(labels); free(label_lens); free(id);
                    return -1;
                }
            }
        }

        *out = cowrie_new_node(id, id_len, (const char **)labels, label_lens, (size_t)label_count, props, (size_t)prop_count);
        /* Cleanup temps (constructor copies) */
        for (size_t i = 0; i < (size_t)label_count; i++) free(labels[i]);
        free(labels); free(label_lens); free(id);
        for (size_t i = 0; i < (size_t)prop_count; i++) free(props[i].key);
        free(props);
        return *out ? 0 : -1;
    }

    case SJT_EDGE: {
        /* srcId:string + dstId:string + type:string */
        char *from_id, *to_id, *edge_type;
        size_t from_id_len, to_id_len, edge_type_len;
        if (decode_string_raw(r, &from_id, &from_id_len) != 0) return -1;
        if (decode_string_raw(r, &to_id, &to_id_len) != 0) { free(from_id); return -1; }
        if (decode_string_raw(r, &edge_type, &edge_type_len) != 0) { free(to_id); free(from_id); return -1; }

        /* propCount:uvarint + (dictIdx:uvarint + value)* */
        uint64_t prop_count;
        if (rd_get_uvarint(r, &prop_count) != 0) {
            free(edge_type); free(to_id); free(from_id);
            return -1;
        }
        /* Security limit check */
        if (r->opts.max_object_len > 0 && prop_count > r->opts.max_object_len) {
            free(edge_type); free(to_id); free(from_id);
            return -1;
        }

        COWRIEMember *props = NULL;
        if (prop_count > 0) {
            size_t eprops_alloc;
            if (safe_mul((size_t)prop_count, sizeof(COWRIEMember), &eprops_alloc) != 0) {
                free(edge_type); free(to_id); free(from_id);
                return -1;
            }
            props = malloc(eprops_alloc);
            if (!props) {
                free(edge_type); free(to_id); free(from_id);
                return -1;
            }
            for (size_t i = 0; i < (size_t)prop_count; i++) {
                uint64_t field_id;
                if (rd_get_uvarint(r, &field_id) != 0 || field_id >= dict_len) {
                    for (size_t j = 0; j < i; j++) {
                        free(props[j].key);
                        cowrie_free(props[j].value);
                    }
                    free(props); free(edge_type); free(to_id); free(from_id);
                    return -1;
                }
                props[i].key = strdup(dict[field_id]);
                props[i].key_len = strlen(dict[field_id]);
                if (decode_value(r, dict, dict_len, &props[i].value) != 0) {
                    free(props[i].key);
                    for (size_t j = 0; j < i; j++) {
                        free(props[j].key);
                        cowrie_free(props[j].value);
                    }
                    free(props); free(edge_type); free(to_id); free(from_id);
                    return -1;
                }
            }
        }

        *out = cowrie_new_edge(from_id, from_id_len, to_id, to_id_len, edge_type, edge_type_len, props, (size_t)prop_count);
        free(from_id); free(to_id); free(edge_type);
        for (size_t i = 0; i < (size_t)prop_count; i++) free(props[i].key);
        free(props);
        return *out ? 0 : -1;
    }

    case SJT_NODE_BATCH: {
        uint64_t count;
        if (rd_get_uvarint(r, &count) != 0) return -1;
        /* Security limit check */
        if (r->opts.max_array_len > 0 && count > r->opts.max_array_len) return -1;

        COWRIENode *nodes = NULL;
        if (count > 0) {
            nodes = calloc((size_t)count, sizeof(COWRIENode));
            if (!nodes) return -1;
        }

        for (size_t i = 0; i < (size_t)count; i++) {
            /* Decode inline node */
            if (decode_string_raw(r, &nodes[i].id, &nodes[i].id_len) != 0) goto node_batch_fail;

            uint64_t lc;
            if (rd_get_uvarint(r, &lc) != 0) goto node_batch_fail;
            nodes[i].label_count = (size_t)lc;

            if (lc > 0) {
                size_t lbl_a, lbll_a;
                if (safe_mul((size_t)lc, sizeof(char *), &lbl_a) != 0 ||
                    safe_mul((size_t)lc, sizeof(size_t), &lbll_a) != 0) goto node_batch_fail;
                nodes[i].labels = malloc(lbl_a);
                nodes[i].label_lens = malloc(lbll_a);
                if (!nodes[i].labels || !nodes[i].label_lens) goto node_batch_fail;
                for (size_t j = 0; j < (size_t)lc; j++) {
                    if (decode_string_raw(r, &nodes[i].labels[j], &nodes[i].label_lens[j]) != 0) goto node_batch_fail;
                }
            }

            uint64_t pc;
            if (rd_get_uvarint(r, &pc) != 0) goto node_batch_fail;
            nodes[i].prop_count = (size_t)pc;

            if (pc > 0) {
                size_t p_a;
                if (safe_mul((size_t)pc, sizeof(COWRIEMember), &p_a) != 0) goto node_batch_fail;
                nodes[i].props = malloc(p_a);
                if (!nodes[i].props) goto node_batch_fail;
                for (size_t j = 0; j < (size_t)pc; j++) {
                    uint64_t fid;
                    if (rd_get_uvarint(r, &fid) != 0 || fid >= dict_len) goto node_batch_fail;
                    nodes[i].props[j].key = strdup(dict[fid]);
                    nodes[i].props[j].key_len = strlen(dict[fid]);
                    if (decode_value(r, dict, dict_len, &nodes[i].props[j].value) != 0) goto node_batch_fail;
                }
            }
        }

        *out = cowrie_new_node_batch(nodes, (size_t)count);
        free(nodes);  /* shallow copy in constructor */
        return *out ? 0 : -1;

    node_batch_fail:
        free(nodes);
        return -1;
    }

    case SJT_EDGE_BATCH: {
        uint64_t count;
        if (rd_get_uvarint(r, &count) != 0) return -1;
        /* Security limit check */
        if (r->opts.max_array_len > 0 && count > r->opts.max_array_len) return -1;

        COWRIEEdge *edges = NULL;
        if (count > 0) {
            edges = calloc((size_t)count, sizeof(COWRIEEdge));
            if (!edges) return -1;
        }

        for (size_t i = 0; i < (size_t)count; i++) {
            if (decode_string_raw(r, &edges[i].from_id, &edges[i].from_id_len) != 0) goto edge_batch_fail;
            if (decode_string_raw(r, &edges[i].to_id, &edges[i].to_id_len) != 0) goto edge_batch_fail;
            if (decode_string_raw(r, &edges[i].edge_type, &edges[i].edge_type_len) != 0) goto edge_batch_fail;

            uint64_t pc;
            if (rd_get_uvarint(r, &pc) != 0) goto edge_batch_fail;
            edges[i].prop_count = (size_t)pc;

            if (pc > 0) {
                size_t ep_a;
                if (safe_mul((size_t)pc, sizeof(COWRIEMember), &ep_a) != 0) goto edge_batch_fail;
                edges[i].props = malloc(ep_a);
                if (!edges[i].props) goto edge_batch_fail;
                for (size_t j = 0; j < (size_t)pc; j++) {
                    uint64_t fid;
                    if (rd_get_uvarint(r, &fid) != 0 || fid >= dict_len) goto edge_batch_fail;
                    edges[i].props[j].key = strdup(dict[fid]);
                    edges[i].props[j].key_len = strlen(dict[fid]);
                    if (decode_value(r, dict, dict_len, &edges[i].props[j].value) != 0) goto edge_batch_fail;
                }
            }
        }

        *out = cowrie_new_edge_batch(edges, (size_t)count);
        free(edges);  /* shallow copy in constructor */
        return *out ? 0 : -1;

    edge_batch_fail:
        free(edges);
        return -1;
    }

    case SJT_GRAPH_SHARD: {
        /* nodeCount:uvarint + Node* */
        uint64_t node_count;
        if (rd_get_uvarint(r, &node_count) != 0) return -1;
        /* Security limit check */
        if (r->opts.max_array_len > 0 && node_count > r->opts.max_array_len) return -1;

        COWRIENode *nodes = NULL;
        if (node_count > 0) {
            nodes = calloc((size_t)node_count, sizeof(COWRIENode));
            if (!nodes) return -1;
        }

        for (size_t i = 0; i < (size_t)node_count; i++) {
            if (decode_string_raw(r, &nodes[i].id, &nodes[i].id_len) != 0) goto shard_fail;
            uint64_t lc;
            if (rd_get_uvarint(r, &lc) != 0) goto shard_fail;
            nodes[i].label_count = (size_t)lc;
            if (lc > 0) {
                size_t sl_a, sll_a;
                if (safe_mul((size_t)lc, sizeof(char *), &sl_a) != 0 ||
                    safe_mul((size_t)lc, sizeof(size_t), &sll_a) != 0) goto shard_fail;
                nodes[i].labels = malloc(sl_a);
                nodes[i].label_lens = malloc(sll_a);
                if (!nodes[i].labels || !nodes[i].label_lens) goto shard_fail;
                for (size_t j = 0; j < (size_t)lc; j++) {
                    if (decode_string_raw(r, &nodes[i].labels[j], &nodes[i].label_lens[j]) != 0) goto shard_fail;
                }
            }
            uint64_t pc;
            if (rd_get_uvarint(r, &pc) != 0) goto shard_fail;
            nodes[i].prop_count = (size_t)pc;
            if (pc > 0) {
                size_t sp_a;
                if (safe_mul((size_t)pc, sizeof(COWRIEMember), &sp_a) != 0) goto shard_fail;
                nodes[i].props = malloc(sp_a);
                if (!nodes[i].props) goto shard_fail;
                for (size_t j = 0; j < (size_t)pc; j++) {
                    uint64_t fid;
                    if (rd_get_uvarint(r, &fid) != 0 || fid >= dict_len) goto shard_fail;
                    nodes[i].props[j].key = strdup(dict[fid]);
                    nodes[i].props[j].key_len = strlen(dict[fid]);
                    if (decode_value(r, dict, dict_len, &nodes[i].props[j].value) != 0) goto shard_fail;
                }
            }
        }

        /* edgeCount:uvarint + Edge* */
        uint64_t edge_count;
        if (rd_get_uvarint(r, &edge_count) != 0) goto shard_fail;
        /* Security limit check */
        if (r->opts.max_array_len > 0 && edge_count > r->opts.max_array_len) goto shard_fail;

        COWRIEEdge *edges = NULL;
        if (edge_count > 0) {
            edges = calloc((size_t)edge_count, sizeof(COWRIEEdge));
            if (!edges) goto shard_fail;
        }

        for (size_t i = 0; i < (size_t)edge_count; i++) {
            if (decode_string_raw(r, &edges[i].from_id, &edges[i].from_id_len) != 0) goto shard_fail2;
            if (decode_string_raw(r, &edges[i].to_id, &edges[i].to_id_len) != 0) goto shard_fail2;
            if (decode_string_raw(r, &edges[i].edge_type, &edges[i].edge_type_len) != 0) goto shard_fail2;
            uint64_t pc;
            if (rd_get_uvarint(r, &pc) != 0) goto shard_fail2;
            edges[i].prop_count = (size_t)pc;
            if (pc > 0) {
                size_t sep_a;
                if (safe_mul((size_t)pc, sizeof(COWRIEMember), &sep_a) != 0) goto shard_fail2;
                edges[i].props = malloc(sep_a);
                if (!edges[i].props) goto shard_fail2;
                for (size_t j = 0; j < (size_t)pc; j++) {
                    uint64_t fid;
                    if (rd_get_uvarint(r, &fid) != 0 || fid >= dict_len) goto shard_fail2;
                    edges[i].props[j].key = strdup(dict[fid]);
                    edges[i].props[j].key_len = strlen(dict[fid]);
                    if (decode_value(r, dict, dict_len, &edges[i].props[j].value) != 0) goto shard_fail2;
                }
            }
        }

        /* metaCount:uvarint + (dictIdx:uvarint + value)* */
        uint64_t meta_count;
        if (rd_get_uvarint(r, &meta_count) != 0) goto shard_fail2;

        COWRIEMember *metadata = NULL;
        if (meta_count > 0) {
            size_t meta_alloc;
            if (safe_mul((size_t)meta_count, sizeof(COWRIEMember), &meta_alloc) != 0) goto shard_fail2;
            metadata = malloc(meta_alloc);
            if (!metadata) goto shard_fail2;
            for (size_t i = 0; i < (size_t)meta_count; i++) {
                uint64_t fid;
                if (rd_get_uvarint(r, &fid) != 0 || fid >= dict_len) {
                    for (size_t j = 0; j < i; j++) {
                        free(metadata[j].key);
                        cowrie_free(metadata[j].value);
                    }
                    free(metadata);
                    goto shard_fail2;
                }
                metadata[i].key = strdup(dict[fid]);
                metadata[i].key_len = strlen(dict[fid]);
                if (decode_value(r, dict, dict_len, &metadata[i].value) != 0) {
                    free(metadata[i].key);
                    for (size_t j = 0; j < i; j++) {
                        free(metadata[j].key);
                        cowrie_free(metadata[j].value);
                    }
                    free(metadata);
                    goto shard_fail2;
                }
            }
        }

        *out = cowrie_new_graph_shard(nodes, (size_t)node_count, edges, (size_t)edge_count, metadata, (size_t)meta_count);
        free(nodes); free(edges);
        for (size_t i = 0; i < (size_t)meta_count; i++) free(metadata[i].key);
        free(metadata);
        return *out ? 0 : -1;

    shard_fail2:
        free(edges);
    shard_fail:
        free(nodes);
        return -1;
    }

    default:
        return -1;
    }
}

int cowrie_decode_with_opts(const uint8_t *data, size_t len, 
                           const COWRIEDecodeOpts *opts, COWRIEValue **out) {
    Reader r;
    r.data = data;
    r.len = len;
    r.pos = 0;
    r.depth = 0;
    
    /* Apply options or defaults */
    if (opts) {
        r.opts = *opts;
    } else {
        cowrie_decode_opts_init(&r.opts);
    }
    
    /* Apply defaults for zero values */
    if (r.opts.max_depth == 0) r.opts.max_depth = COWRIE_DEFAULT_MAX_DEPTH;
    if (r.opts.max_array_len == 0) r.opts.max_array_len = COWRIE_DEFAULT_MAX_ARRAY_LEN;
    if (r.opts.max_object_len == 0) r.opts.max_object_len = COWRIE_DEFAULT_MAX_OBJECT_LEN;
    if (r.opts.max_string_len == 0) r.opts.max_string_len = COWRIE_DEFAULT_MAX_STRING_LEN;
    if (r.opts.max_bytes_len == 0) r.opts.max_bytes_len = COWRIE_DEFAULT_MAX_BYTES_LEN;
    if (r.opts.max_ext_len == 0) r.opts.max_ext_len = COWRIE_DEFAULT_MAX_EXT_LEN;
    if (r.opts.max_dict_len == 0) r.opts.max_dict_len = COWRIE_DEFAULT_MAX_DICT_LEN;
    if (r.opts.max_hint_count == 0) r.opts.max_hint_count = COWRIE_DEFAULT_MAX_HINT_COUNT;
    if (r.opts.max_rank == 0) r.opts.max_rank = COWRIE_DEFAULT_MAX_RANK;

    /* Read header */
    uint8_t magic0, magic1, version, flags;
    if (rd_get_byte(&r, &magic0) != 0) return -1;
    if (rd_get_byte(&r, &magic1) != 0) return -1;
    if (magic0 != COWRIE_MAGIC_0 || magic1 != COWRIE_MAGIC_1) return -1;

    if (rd_get_byte(&r, &version) != 0) return -1;
    if (version != COWRIE_VERSION) return -1;

    if (rd_get_byte(&r, &flags) != 0) return -1;
    /* For now, we don't handle compression here - use cowrie_decode_framed */
    if (flags & COWRIE_FLAG_HAS_COLUMN_HINTS) {
        if (skip_hints(&r) != 0) return -1;
    }

    /* Read dictionary */
    uint64_t dict_len;
    if (rd_get_uvarint(&r, &dict_len) != 0) return -1;
    if (dict_len > SIZE_MAX / sizeof(char *)) return -1;
    if (dict_len > r.opts.max_dict_len) return -1;

    char **dict = NULL;
    if (dict_len > 0) {
        dict = calloc((size_t)dict_len, sizeof(char *));
        if (!dict) return -1;

        for (size_t i = 0; i < (size_t)dict_len; i++) {
            size_t slen;
            if (decode_string_raw(&r, &dict[i], &slen) != 0) {
                for (size_t j = 0; j < i; j++) free(dict[j]);
                free(dict);
                return -1;
            }
        }
    }

    /* Decode root value */
    int result = decode_value(&r, dict, (size_t)dict_len, out);

    /* Free dictionary */
    for (size_t i = 0; i < (size_t)dict_len; i++) {
        free(dict[i]);
    }
    free(dict);

    return result;
}

int cowrie_decode(const uint8_t *data, size_t len, COWRIEValue **out) {
    /* Use default options */
    return cowrie_decode_with_opts(data, len, NULL, out);
}

/* ============================================================
 * CRC32-IEEE Implementation
 * ============================================================ */

static const uint32_t crc32_table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7d43, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd706b3,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

uint32_t cowrie_crc32(const uint8_t *data, size_t len) {
    if (!data || len == 0) return 0;

    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

/* ============================================================
 * Schema Fingerprint (FNV-1a)
 * ============================================================ */

#define FNV_OFFSET_BASIS 14695981039346656037ULL
#define FNV_PRIME        1099511628211ULL

static uint64_t fnv1a_byte(uint64_t hash, uint8_t b) {
    hash ^= b;
    hash *= FNV_PRIME;
    return hash;
}

/* Hash a uint64 as 8 little-endian bytes (matches Go's fnvHashUint64) */
static uint64_t fnv1a_uint64(uint64_t hash, uint64_t v) {
    for (int i = 0; i < 8; i++) {
        hash ^= (v & 0xFF);
        hash *= FNV_PRIME;
        v >>= 8;
    }
    return hash;
}

/* Hash a string with length prefix (matches Go's fnvHashString) */
static uint64_t fnv1a_string(uint64_t hash, const char *s, size_t len) {
    /* Hash length first */
    hash = fnv1a_uint64(hash, len);
    /* Then hash bytes */
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)s[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

/* Compare function for sorting object keys */
static int key_compare(const void *a, const void *b) {
    const COWRIEMember *ma = (const COWRIEMember *)a;
    const COWRIEMember *mb = (const COWRIEMember *)b;
    return strcmp(ma->key, mb->key);
}

static uint64_t schema_fingerprint_impl(const COWRIEValue *v, uint64_t hash) {
    if (!v) {
        return fnv1a_byte(hash, (uint8_t)COWRIE_NULL);
    }

    /* Hash the type ordinal */
    hash = fnv1a_byte(hash, (uint8_t)v->type);

    switch (v->type) {
    case COWRIE_NULL:
    case COWRIE_BOOL:
    case COWRIE_INT64:
    case COWRIE_UINT64:
    case COWRIE_FLOAT64:
    case COWRIE_DECIMAL128:
    case COWRIE_STRING:
    case COWRIE_BYTES:
    case COWRIE_DATETIME64:
    case COWRIE_UUID128:
    case COWRIE_BIGINT:
        /* Scalar types: just the type ordinal */
        break;

    case COWRIE_ARRAY:
        /* Array: hash count + each element schema */
        hash = fnv1a_uint64(hash, v->as.array.len);
        for (size_t i = 0; i < v->as.array.len; i++) {
            hash = schema_fingerprint_impl(v->as.array.items[i], hash);
        }
        break;

    case COWRIE_OBJECT: {
        /* Object: hash count + sorted keys and their value schemas */
        size_t n = v->as.object.len;
        hash = fnv1a_uint64(hash, n);

        if (n == 0) break;

        /* Sort keys for deterministic hashing */
        COWRIEMember *sorted = malloc(n * sizeof(COWRIEMember));
        if (!sorted) break;
        memcpy(sorted, v->as.object.members, n * sizeof(COWRIEMember));
        qsort(sorted, n, sizeof(COWRIEMember), key_compare);

        for (size_t i = 0; i < n; i++) {
            /* Hash key name with length prefix */
            hash = fnv1a_string(hash, sorted[i].key, strlen(sorted[i].key));
            /* Hash value schema */
            hash = schema_fingerprint_impl(sorted[i].value, hash);
        }
        free(sorted);
        break;
    }

    case COWRIE_TENSOR:
        /* Tensor: include dtype and rank (as uint64) in schema */
        hash = fnv1a_byte(hash, v->as.tensor.dtype);
        hash = fnv1a_uint64(hash, v->as.tensor.rank);
        break;

    case COWRIE_TENSOR_REF:
        /* TensorRef: include store ID */
        hash = fnv1a_byte(hash, v->as.tensor_ref.store_id);
        break;

    case COWRIE_EXT:
        /* Ext: include ext_type */
        hash = fnv1a_uint64(hash, v->as.ext.ext_type);
        break;

    case COWRIE_IMAGE:
        /* Image: include format */
        hash = fnv1a_byte(hash, v->as.image.format);
        break;

    case COWRIE_AUDIO:
        /* Audio: include encoding and channels */
        hash = fnv1a_byte(hash, v->as.audio.encoding);
        hash = fnv1a_byte(hash, v->as.audio.channels);
        break;

    case COWRIE_ADJLIST:
        /* Adjlist: include ID width */
        hash = fnv1a_byte(hash, v->as.adjlist.id_width);
        break;

    case COWRIE_RICH_TEXT: {
        /* RichText: include presence of tokens and spans */
        uint8_t flags = 0;
        if (v->as.rich_text.token_count > 0) flags |= 0x01;
        if (v->as.rich_text.span_count > 0) flags |= 0x02;
        hash = fnv1a_byte(hash, flags);
        break;
    }

    case COWRIE_DELTA:
        /* Delta: include base ID and ops */
        hash = fnv1a_uint64(hash, v->as.delta.base_id);
        hash = fnv1a_uint64(hash, v->as.delta.op_count);
        for (size_t i = 0; i < v->as.delta.op_count; i++) {
            hash = fnv1a_byte(hash, v->as.delta.ops[i].op_code);
            hash = schema_fingerprint_impl(v->as.delta.ops[i].value, hash);
        }
        break;
    }

    return hash;
}

uint32_t cowrie_schema_fingerprint32(const COWRIEValue *v) {
    uint64_t hash = schema_fingerprint_impl(v, FNV_OFFSET_BASIS);
    return (uint32_t)(hash & 0xFFFFFFFF);
}

/* ============================================================
 * Master Stream Implementation
 * ============================================================ */

/* Buffer append helper */
static int cowrie_buf_append(COWRIEBuf *buf, const uint8_t *data, size_t len) {
    if (buf->len + len > buf->cap) {
        size_t new_cap = buf->cap ? buf->cap * 2 : 256;
        while (new_cap < buf->len + len) new_cap *= 2;
        uint8_t *new_data = realloc(buf->data, new_cap);
        if (!new_data) return -1;
        buf->data = new_data;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    return 0;
}

/* Helper to write little-endian uint16 */
static void write_u16_le(COWRIEBuf *buf, uint16_t v) {
    uint8_t bytes[2] = { (uint8_t)(v & 0xFF), (uint8_t)((v >> 8) & 0xFF) };
    cowrie_buf_append(buf, bytes, 2);
}

/* Helper to write little-endian uint32 */
static void write_u32_le(COWRIEBuf *buf, uint32_t v) {
    uint8_t bytes[4] = {
        (uint8_t)(v & 0xFF),
        (uint8_t)((v >> 8) & 0xFF),
        (uint8_t)((v >> 16) & 0xFF),
        (uint8_t)((v >> 24) & 0xFF)
    };
    cowrie_buf_append(buf, bytes, 4);
}

/* Helper to read little-endian uint16 */
static uint16_t read_u16_le(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

/* Helper to read little-endian uint32 */
static uint32_t read_u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

int cowrie_master_write_frame(const COWRIEValue *value, const COWRIEValue *meta,
                              const COWRIEMasterWriterOpts *opts, COWRIEBuf *buf) {
    if (!value || !opts || !buf) return -1;

    cowrie_buf_init(buf);

    /* Encode payload */
    COWRIEBuf payload_buf;
    cowrie_buf_init(&payload_buf);
    if (cowrie_encode(value, &payload_buf) != 0) {
        cowrie_buf_free(&payload_buf);
        return -1;
    }

    /* Encode metadata if present */
    COWRIEBuf meta_buf;
    cowrie_buf_init(&meta_buf);
    if (meta) {
        if (cowrie_encode(meta, &meta_buf) != 0) {
            cowrie_buf_free(&payload_buf);
            cowrie_buf_free(&meta_buf);
            return -1;
        }
    }

    /* Compute type ID from schema */
    uint32_t type_id = cowrie_schema_fingerprint32(value);

    /* Build flags */
    uint8_t frame_flags = 0;
    if (opts->deterministic) frame_flags |= COWRIE_MFLAG_DETERMINISTIC;
    if (opts->enable_crc) frame_flags |= COWRIE_MFLAG_CRC;
    if (meta_buf.len > 0) frame_flags |= COWRIE_MFLAG_META;

    /* Header length (fixed at 24 bytes for v2) */
    uint16_t header_len = 24;

    /* Write magic */
    uint8_t magic[4] = { COWRIE_MASTER_MAGIC_0, COWRIE_MASTER_MAGIC_1,
                         COWRIE_MASTER_MAGIC_2, COWRIE_MASTER_MAGIC_3 };
    cowrie_buf_append(buf, magic, 4);

    /* Write version */
    uint8_t version = COWRIE_MASTER_VERSION;
    cowrie_buf_append(buf, &version, 1);

    /* Write flags */
    cowrie_buf_append(buf, &frame_flags, 1);

    /* Write header length */
    write_u16_le(buf, header_len);

    /* Write type ID */
    write_u32_le(buf, type_id);

    /* Write payload length */
    write_u32_le(buf, (uint32_t)payload_buf.len);

    /* Write raw length (0 = not compressed) */
    write_u32_le(buf, 0);

    /* Write meta length */
    write_u32_le(buf, (uint32_t)meta_buf.len);

    /* Write metadata */
    if (meta_buf.len > 0) {
        cowrie_buf_append(buf, meta_buf.data, meta_buf.len);
    }

    /* Write payload */
    cowrie_buf_append(buf, payload_buf.data, payload_buf.len);

    /* Write CRC32 if enabled */
    if (opts->enable_crc) {
        uint32_t crc = cowrie_crc32(buf->data, buf->len);
        write_u32_le(buf, crc);
    }

    cowrie_buf_free(&payload_buf);
    cowrie_buf_free(&meta_buf);

    return 0;
}

int cowrie_master_read_frame(const uint8_t *data, size_t len, COWRIEMasterFrame *frame) {
    if (!data || len < 24 || !frame) return -1;

    /* Initialize frame */
    memset(frame, 0, sizeof(*frame));

    /* Check magic */
    if (!cowrie_is_master_stream(data, len)) {
        return -1;
    }

    /* Read header */
    uint8_t version = data[4];
    if (version != COWRIE_MASTER_VERSION) return -1;

    uint8_t flags = data[5];
    uint16_t header_len = read_u16_le(&data[6]);
    uint32_t type_id = read_u32_le(&data[8]);
    uint32_t payload_len = read_u32_le(&data[12]);
    uint32_t raw_len = read_u32_le(&data[16]);
    uint32_t meta_len = read_u32_le(&data[20]);

    /* Fill header struct */
    frame->header.version = version;
    frame->header.flags = flags;
    frame->header.header_len = header_len;
    frame->header.type_id = type_id;
    frame->header.payload_len = payload_len;
    frame->header.raw_len = raw_len;
    frame->header.meta_len = meta_len;
    frame->type_id = type_id;

    /* Calculate positions */
    size_t pos = (header_len > 24) ? header_len : 24;

    /* Read metadata if present */
    if ((flags & COWRIE_MFLAG_META) && meta_len > 0) {
        if (pos + meta_len > len) return -1;
        if (cowrie_decode(&data[pos], meta_len, &frame->meta) != 0) {
            return -1;
        }
        pos += meta_len;
    } else {
        frame->meta = NULL;
        pos += meta_len;  /* Skip even if not decoding */
    }

    /* Read payload */
    if (pos + payload_len > len) {
        if (frame->meta) cowrie_free(frame->meta);
        return -1;
    }
    if (cowrie_decode(&data[pos], payload_len, &frame->payload) != 0) {
        if (frame->meta) cowrie_free(frame->meta);
        return -1;
    }
    pos += payload_len;

    /* Verify CRC if enabled */
    if (flags & COWRIE_MFLAG_CRC) {
        if (pos + 4 > len) {
            cowrie_free(frame->payload);
            if (frame->meta) cowrie_free(frame->meta);
            return -1;
        }
        uint32_t expected_crc = read_u32_le(&data[pos]);
        uint32_t actual_crc = cowrie_crc32(data, pos);
        if (actual_crc != expected_crc) {
            cowrie_free(frame->payload);
            if (frame->meta) cowrie_free(frame->meta);
            return -1;
        }
        pos += 4;
    }

    return (int)pos;  /* Return bytes consumed */
}

void cowrie_master_frame_free(COWRIEMasterFrame *frame) {
    if (!frame) return;
    if (frame->payload) {
        cowrie_free(frame->payload);
        frame->payload = NULL;
    }
    if (frame->meta) {
        cowrie_free(frame->meta);
        frame->meta = NULL;
    }
}

/* ============================================================
 * Zero-Copy Tensor View Helpers (copy implementations)
 * ============================================================ */

float* cowrie_tensor_copy_float32(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_FLOAT32) return NULL;
    if (t->data_len == 0) {
        *count = 0;
        return NULL;
    }
    if (t->data_len % 4 != 0) return NULL;

    *count = t->data_len / 4;
    float *out = (float*)malloc(*count * sizeof(float));
    if (!out) return NULL;

    /* Copy with proper endianness (little-endian wire format) */
    for (size_t i = 0; i < *count; i++) {
        uint32_t bits = (uint32_t)t->data[i*4] |
                        ((uint32_t)t->data[i*4 + 1] << 8) |
                        ((uint32_t)t->data[i*4 + 2] << 16) |
                        ((uint32_t)t->data[i*4 + 3] << 24);
        memcpy(&out[i], &bits, sizeof(float));
    }
    return out;
}

double* cowrie_tensor_copy_float64(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    /* No FLOAT64 dtype currently, but support the operation */
    if (t->data_len == 0) {
        *count = 0;
        return NULL;
    }
    if (t->data_len % 8 != 0) return NULL;

    *count = t->data_len / 8;
    double *out = (double*)malloc(*count * sizeof(double));
    if (!out) return NULL;

    for (size_t i = 0; i < *count; i++) {
        uint64_t bits = (uint64_t)t->data[i*8] |
                        ((uint64_t)t->data[i*8 + 1] << 8) |
                        ((uint64_t)t->data[i*8 + 2] << 16) |
                        ((uint64_t)t->data[i*8 + 3] << 24) |
                        ((uint64_t)t->data[i*8 + 4] << 32) |
                        ((uint64_t)t->data[i*8 + 5] << 40) |
                        ((uint64_t)t->data[i*8 + 6] << 48) |
                        ((uint64_t)t->data[i*8 + 7] << 56);
        memcpy(&out[i], &bits, sizeof(double));
    }
    return out;
}

int32_t* cowrie_tensor_copy_int32(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_INT32) return NULL;
    if (t->data_len == 0) {
        *count = 0;
        return NULL;
    }
    if (t->data_len % 4 != 0) return NULL;

    *count = t->data_len / 4;
    int32_t *out = (int32_t*)malloc(*count * sizeof(int32_t));
    if (!out) return NULL;

    for (size_t i = 0; i < *count; i++) {
        out[i] = (int32_t)((uint32_t)t->data[i*4] |
                           ((uint32_t)t->data[i*4 + 1] << 8) |
                           ((uint32_t)t->data[i*4 + 2] << 16) |
                           ((uint32_t)t->data[i*4 + 3] << 24));
    }
    return out;
}

int64_t* cowrie_tensor_copy_int64(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_INT64) return NULL;
    if (t->data_len == 0) {
        *count = 0;
        return NULL;
    }
    if (t->data_len % 8 != 0) return NULL;

    *count = t->data_len / 8;
    int64_t *out = (int64_t*)malloc(*count * sizeof(int64_t));
    if (!out) return NULL;

    for (size_t i = 0; i < *count; i++) {
        out[i] = (int64_t)((uint64_t)t->data[i*8] |
                           ((uint64_t)t->data[i*8 + 1] << 8) |
                           ((uint64_t)t->data[i*8 + 2] << 16) |
                           ((uint64_t)t->data[i*8 + 3] << 24) |
                           ((uint64_t)t->data[i*8 + 4] << 32) |
                           ((uint64_t)t->data[i*8 + 5] << 40) |
                           ((uint64_t)t->data[i*8 + 6] << 48) |
                           ((uint64_t)t->data[i*8 + 7] << 56));
    }
    return out;
}

/* ============================================================
 * Framed Encode/Decode (with compression)
 * ============================================================
 *
 * Framed wire format:
 *   Magic:             "SJFR" (4 bytes)
 *   Version:           0x01   (1 byte)
 *   Flags:             1 byte
 *                        bit 0: compressed (1) or not (0)
 *                        bits 1-2: compression type
 *                          00 = none, 01 = gzip, 10 = zstd
 *   Uncompressed size: uint32 LE (4 bytes)
 *   [If compressed]:
 *     Compressed size:  uint32 LE (4 bytes)
 *   Payload bytes
 */

#define COWRIE_FRAMED_MAGIC_0 'S'
#define COWRIE_FRAMED_MAGIC_1 'J'
#define COWRIE_FRAMED_MAGIC_2 'F'
#define COWRIE_FRAMED_MAGIC_3 'R'
#define COWRIE_FRAMED_VERSION 0x01

/* Framed flags bits */
#define COWRIE_FRAMED_FLAG_COMPRESSED  0x01
#define COWRIE_FRAMED_COMP_SHIFT       1
#define COWRIE_FRAMED_COMP_MASK        0x06  /* bits 1-2 */

/* Helper to write u32 LE into a raw byte pointer (not COWRIEBuf) */
static void framed_write_u32_le(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}

/* Helper to read u32 LE from a raw byte pointer */
static uint32_t framed_read_u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

int cowrie_encode_framed(const COWRIEValue *root, int compression, COWRIEBuf *buf) {
    if (!root || !buf) return -1;

    cowrie_buf_init(buf);

    /* Encode the value to raw cowrie bytes first */
    COWRIEBuf raw;
    if (cowrie_encode(root, &raw) != 0) return -1;

    if (raw.len > UINT32_MAX) {
        cowrie_buf_free(&raw);
        return -1;
    }

    uint32_t uncomp_size = (uint32_t)raw.len;

    if (compression == COWRIE_COMP_NONE) {
        /* Uncompressed: header = 4 (magic) + 1 (version) + 1 (flags) + 4 (uncomp size) = 10 */
        size_t total = 10 + raw.len;
        if (buf_grow(buf, total) != 0) {
            cowrie_buf_free(&raw);
            return -1;
        }

        /* Write magic */
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_0;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_1;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_2;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_3;

        /* Version */
        buf->data[buf->len++] = COWRIE_FRAMED_VERSION;

        /* Flags: not compressed, comp type = none */
        buf->data[buf->len++] = 0x00;

        /* Uncompressed size */
        framed_write_u32_le(buf->data + buf->len, uncomp_size);
        buf->len += 4;

        /* Payload (raw cowrie bytes) */
        memcpy(buf->data + buf->len, raw.data, raw.len);
        buf->len += raw.len;

        cowrie_buf_free(&raw);
        return 0;

    } else if (compression == COWRIE_COMP_GZIP) {
        /* zlib's compress2() produces zlib-format; the framed header
         * identifies the compression type so the decoder knows what to use. */
        uLongf comp_bound = compressBound((uLong)raw.len);
        uint8_t *comp_buf = malloc((size_t)comp_bound);
        if (!comp_buf) {
            cowrie_buf_free(&raw);
            return -1;
        }

        uLongf comp_len = comp_bound;
        int zrc = compress2(comp_buf, &comp_len, raw.data, (uLong)raw.len, Z_DEFAULT_COMPRESSION);
        if (zrc != Z_OK) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        if (comp_len > UINT32_MAX) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        uint32_t compressed_size = (uint32_t)comp_len;

        /* Header: 4 (magic) + 1 (version) + 1 (flags) + 4 (uncomp) + 4 (comp) = 14 */
        size_t total = 14 + comp_len;
        if (buf_grow(buf, total) != 0) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        /* Write magic */
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_0;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_1;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_2;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_3;

        /* Version */
        buf->data[buf->len++] = COWRIE_FRAMED_VERSION;

        /* Flags: compressed=1, comp type=gzip (01 in bits 1-2) */
        uint8_t flags = COWRIE_FRAMED_FLAG_COMPRESSED | (COWRIE_COMP_GZIP << COWRIE_FRAMED_COMP_SHIFT);
        buf->data[buf->len++] = flags;

        /* Uncompressed size */
        framed_write_u32_le(buf->data + buf->len, uncomp_size);
        buf->len += 4;

        /* Compressed size */
        framed_write_u32_le(buf->data + buf->len, compressed_size);
        buf->len += 4;

        /* Compressed payload */
        memcpy(buf->data + buf->len, comp_buf, comp_len);
        buf->len += comp_len;

        free(comp_buf);
        cowrie_buf_free(&raw);
        return 0;

    } else if (compression == COWRIE_COMP_ZSTD) {
#ifdef COWRIE_HAS_ZSTD
        size_t comp_bound = ZSTD_compressBound(raw.len);
        uint8_t *comp_buf = malloc(comp_bound);
        if (!comp_buf) {
            cowrie_buf_free(&raw);
            return -1;
        }

        size_t comp_len = ZSTD_compress(comp_buf, comp_bound, raw.data, raw.len, 3);
        if (ZSTD_isError(comp_len)) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        if (comp_len > UINT32_MAX) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        uint32_t compressed_size = (uint32_t)comp_len;

        /* Header: 4 (magic) + 1 (version) + 1 (flags) + 4 (uncomp) + 4 (comp) = 14 */
        size_t total = 14 + comp_len;
        if (buf_grow(buf, total) != 0) {
            free(comp_buf);
            cowrie_buf_free(&raw);
            return -1;
        }

        /* Write magic */
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_0;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_1;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_2;
        buf->data[buf->len++] = COWRIE_FRAMED_MAGIC_3;

        /* Version */
        buf->data[buf->len++] = COWRIE_FRAMED_VERSION;

        /* Flags: compressed=1, comp type=zstd (10 in bits 1-2) */
        uint8_t flags = COWRIE_FRAMED_FLAG_COMPRESSED | (COWRIE_COMP_ZSTD << COWRIE_FRAMED_COMP_SHIFT);
        buf->data[buf->len++] = flags;

        /* Uncompressed size */
        framed_write_u32_le(buf->data + buf->len, uncomp_size);
        buf->len += 4;

        /* Compressed size */
        framed_write_u32_le(buf->data + buf->len, compressed_size);
        buf->len += 4;

        /* Compressed payload */
        memcpy(buf->data + buf->len, comp_buf, comp_len);
        buf->len += comp_len;

        free(comp_buf);
        cowrie_buf_free(&raw);
        return 0;
#else
        /* ZSTD not available */
        cowrie_buf_free(&raw);
        return -1;
#endif

    } else {
        /* Unknown compression type */
        cowrie_buf_free(&raw);
        return -1;
    }
}

int cowrie_decode_framed(const uint8_t *data, size_t len, COWRIEValue **out) {
    if (!data || !out) return -1;

    /* Minimum header: 4 (magic) + 1 (version) + 1 (flags) + 4 (uncomp size) = 10 */
    if (len < 10) return -1;

    /* Validate magic */
    if (data[0] != COWRIE_FRAMED_MAGIC_0 ||
        data[1] != COWRIE_FRAMED_MAGIC_1 ||
        data[2] != COWRIE_FRAMED_MAGIC_2 ||
        data[3] != COWRIE_FRAMED_MAGIC_3) {
        return -1;
    }

    /* Version check */
    uint8_t version = data[4];
    if (version != COWRIE_FRAMED_VERSION) return -1;

    /* Parse flags */
    uint8_t flags = data[5];
    int is_compressed = (flags & COWRIE_FRAMED_FLAG_COMPRESSED) != 0;
    int comp_type = (flags & COWRIE_FRAMED_COMP_MASK) >> COWRIE_FRAMED_COMP_SHIFT;

    /* Read uncompressed size */
    uint32_t uncomp_size = framed_read_u32_le(&data[6]);

    /* Decompression bomb protection */
    if (uncomp_size > COWRIE_MAX_DECOMPRESSED_SIZE) return -1;

    if (!is_compressed) {
        /* Uncompressed: payload starts at offset 10 */
        size_t payload_offset = 10;
        if (payload_offset + uncomp_size > len) return -1;

        return cowrie_decode(&data[payload_offset], uncomp_size, out);

    } else {
        /* Compressed: need additional 4 bytes for compressed size */
        if (len < 14) return -1;

        uint32_t comp_size = framed_read_u32_le(&data[10]);
        size_t payload_offset = 14;

        if (payload_offset + comp_size > len) return -1;

        const uint8_t *comp_data = &data[payload_offset];

        if (comp_type == COWRIE_COMP_GZIP) {
            /* Decompress with zlib */
            uint8_t *decomp = malloc(uncomp_size);
            if (!decomp) return -1;

            uLongf dest_len = (uLongf)uncomp_size;
            int zrc = uncompress(decomp, &dest_len, comp_data, (uLong)comp_size);
            if (zrc != Z_OK || dest_len != uncomp_size) {
                free(decomp);
                return -1;
            }

            int result = cowrie_decode(decomp, uncomp_size, out);
            free(decomp);
            return result;

        } else if (comp_type == COWRIE_COMP_ZSTD) {
#ifdef COWRIE_HAS_ZSTD
            uint8_t *decomp = malloc(uncomp_size);
            if (!decomp) return -1;

            size_t result_len = ZSTD_decompress(decomp, uncomp_size, comp_data, comp_size);
            if (ZSTD_isError(result_len) || result_len != uncomp_size) {
                free(decomp);
                return -1;
            }

            int result = cowrie_decode(decomp, uncomp_size, out);
            free(decomp);
            return result;
#else
            /* ZSTD not available */
            return -1;
#endif

        } else {
            /* Unknown compression type */
            return -1;
        }
    }
}
