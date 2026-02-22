/*
 * SJSON Gen1 - Lightweight Binary JSON with Proto-Tensors
 *
 * Gen1 is a simpler codec than Gen2, providing:
 * - 11 core types (null, bool, int64, float64, string, bytes, object, arrays)
 * - Proto-tensor types for efficient numeric arrays
 * - 6 graph types (Node, Edge, AdjList, batches)
 * - No dictionary coding
 * - No compression
 *
 * Use Gen1 when you need a lightweight codec without the full Gen2 complexity.
 */

#ifndef SJSON_GEN1_H
#define SJSON_GEN1_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Type Tags
 * ============================================================ */

typedef enum {
    SJSON_G1_TAG_NULL         = 0x00,
    SJSON_G1_TAG_FALSE        = 0x01,
    SJSON_G1_TAG_TRUE         = 0x02,
    SJSON_G1_TAG_INT64        = 0x03,
    SJSON_G1_TAG_FLOAT64      = 0x04,
    SJSON_G1_TAG_STRING       = 0x05,
    SJSON_G1_TAG_BYTES        = 0x06,
    SJSON_G1_TAG_ARRAY        = 0x07,
    SJSON_G1_TAG_OBJECT       = 0x08,
    /* Proto-tensor types */
    SJSON_G1_TAG_INT64_ARRAY  = 0x09,
    SJSON_G1_TAG_FLOAT64_ARRAY = 0x0A,
    SJSON_G1_TAG_STRING_ARRAY = 0x0B,
    /* Graph types */
    SJSON_G1_TAG_NODE         = 0x10,
    SJSON_G1_TAG_EDGE         = 0x11,
    SJSON_G1_TAG_ADJLIST      = 0x12,
    SJSON_G1_TAG_NODE_BATCH   = 0x13,
    SJSON_G1_TAG_EDGE_BATCH   = 0x14,
    SJSON_G1_TAG_GRAPH_SHARD  = 0x15,
} sjson_g1_tag_t;

/* ============================================================
 * Value Types
 * ============================================================ */

typedef enum {
    SJSON_G1_TYPE_NULL,
    SJSON_G1_TYPE_BOOL,
    SJSON_G1_TYPE_INT64,
    SJSON_G1_TYPE_FLOAT64,
    SJSON_G1_TYPE_STRING,
    SJSON_G1_TYPE_BYTES,
    SJSON_G1_TYPE_ARRAY,
    SJSON_G1_TYPE_OBJECT,
    SJSON_G1_TYPE_INT64_ARRAY,
    SJSON_G1_TYPE_FLOAT64_ARRAY,
    SJSON_G1_TYPE_STRING_ARRAY,
    SJSON_G1_TYPE_NODE,
    SJSON_G1_TYPE_EDGE,
    SJSON_G1_TYPE_ADJLIST,
    SJSON_G1_TYPE_NODE_BATCH,
    SJSON_G1_TYPE_EDGE_BATCH,
    SJSON_G1_TYPE_GRAPH_SHARD,
} sjson_g1_type_t;

/* ============================================================
 * Error Codes
 * ============================================================ */

typedef enum {
    SJSON_G1_OK             = 0,
    SJSON_G1_ERR_NOMEM      = -1,
    SJSON_G1_ERR_INVALID    = -2,
    SJSON_G1_ERR_EOF        = -3,
    SJSON_G1_ERR_OVERFLOW   = -4,
    SJSON_G1_ERR_DEPTH      = -5,  /* Maximum nesting depth exceeded */
    SJSON_G1_ERR_ARRAY_LEN  = -6,  /* Array too large */
    SJSON_G1_ERR_OBJECT_LEN = -7,  /* Object has too many fields */
    SJSON_G1_ERR_STRING_LEN = -8,  /* String too long */
    SJSON_G1_ERR_BYTES_LEN  = -9,  /* Bytes too long */
} sjson_g1_error_t;

/* Security limits - aligned with Go reference implementation */
#define SJSON_G1_MAX_DEPTH      1000
#define SJSON_G1_MAX_ARRAY_LEN  100000000   /* 100M elements */
#define SJSON_G1_MAX_OBJECT_LEN 10000000    /* 10M fields */
#define SJSON_G1_MAX_STRING_LEN 500000000   /* 500MB */
#define SJSON_G1_MAX_BYTES_LEN  1000000000  /* 1GB */

/* ============================================================
 * Buffer
 * ============================================================ */

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} sjson_g1_buf_t;

/* Buffer operations */
void sjson_g1_buf_init(sjson_g1_buf_t *buf);
void sjson_g1_buf_free(sjson_g1_buf_t *buf);
int  sjson_g1_buf_reserve(sjson_g1_buf_t *buf, size_t extra);
int  sjson_g1_buf_write(sjson_g1_buf_t *buf, const void *data, size_t len);
int  sjson_g1_buf_write_byte(sjson_g1_buf_t *buf, uint8_t byte);
int  sjson_g1_buf_write_uvarint(sjson_g1_buf_t *buf, uint64_t val);

/* ============================================================
 * Value (forward declaration)
 * ============================================================ */

typedef struct sjson_g1_value sjson_g1_value_t;
typedef struct sjson_g1_member sjson_g1_member_t;

struct sjson_g1_member {
    char *key;
    sjson_g1_value_t *value;
};

struct sjson_g1_value {
    sjson_g1_type_t type;
    union {
        bool            bool_val;
        int64_t         int64_val;
        double          float64_val;
        struct {
            char   *data;
            size_t  len;
        } string_val;
        struct {
            uint8_t *data;
            size_t   len;
        } bytes_val;
        struct {
            sjson_g1_value_t **items;
            size_t             len;
            size_t             cap;  /* capacity for O(1) amortized append */
        } array_val;
        struct {
            sjson_g1_member_t *members;
            size_t             len;
            size_t             cap;  /* capacity for O(1) amortized set */
        } object_val;
        struct {
            int64_t *data;
            size_t   len;
        } int64_array_val;
        struct {
            double *data;
            size_t  len;
        } float64_array_val;
        struct {
            char  **data;
            size_t  len;
        } string_array_val;
    };
};

/* ============================================================
 * Value Constructors
 * ============================================================ */

sjson_g1_value_t *sjson_g1_null(void);
sjson_g1_value_t *sjson_g1_bool(bool val);
sjson_g1_value_t *sjson_g1_int64(int64_t val);
sjson_g1_value_t *sjson_g1_float64(double val);
sjson_g1_value_t *sjson_g1_string(const char *str, size_t len);
sjson_g1_value_t *sjson_g1_bytes(const uint8_t *data, size_t len);
sjson_g1_value_t *sjson_g1_array(size_t capacity);
sjson_g1_value_t *sjson_g1_object(size_t capacity);
sjson_g1_value_t *sjson_g1_int64_array(const int64_t *data, size_t len);
sjson_g1_value_t *sjson_g1_float64_array(const double *data, size_t len);
sjson_g1_value_t *sjson_g1_string_array(const char **strings, size_t count);

/* Value operations */
int  sjson_g1_array_append(sjson_g1_value_t *arr, sjson_g1_value_t *val);
int  sjson_g1_object_set(sjson_g1_value_t *obj, const char *key, sjson_g1_value_t *val);
void sjson_g1_value_free(sjson_g1_value_t *val);

/* ============================================================
 * Encode/Decode
 * ============================================================ */

/* Encode a value to binary format */
int sjson_g1_encode(const sjson_g1_value_t *val, sjson_g1_buf_t *buf);

/* Decode binary data to a value (caller must free with sjson_g1_value_free) */
int sjson_g1_decode(const uint8_t *data, size_t len, sjson_g1_value_t **out);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/* Zigzag encode/decode */
static inline uint64_t sjson_g1_zigzag_encode(int64_t n) {
    return (uint64_t)((n << 1) ^ (n >> 63));
}

static inline int64_t sjson_g1_zigzag_decode(uint64_t n) {
    return (int64_t)((n >> 1) ^ (uint64_t)(-(int64_t)(n & 1)));
}

#ifdef __cplusplus
}
#endif

#endif /* SJSON_GEN1_H */
