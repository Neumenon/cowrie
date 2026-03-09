/*
 * Cowrie Gen1 - Lightweight Binary JSON with Proto-Tensors
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

#ifndef COWRIE_GEN1_H
#define COWRIE_GEN1_H

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
    COWRIE_G1_TAG_NULL         = 0x00,
    COWRIE_G1_TAG_FALSE        = 0x01,
    COWRIE_G1_TAG_TRUE         = 0x02,
    COWRIE_G1_TAG_INT64        = 0x03,
    COWRIE_G1_TAG_FLOAT64      = 0x04,
    COWRIE_G1_TAG_STRING       = 0x05,
    COWRIE_G1_TAG_ARRAY        = 0x06, /* v3: aligned with Gen2 */
    COWRIE_G1_TAG_OBJECT       = 0x07, /* v3: aligned with Gen2 */
    COWRIE_G1_TAG_BYTES        = 0x08, /* v3: aligned with Gen2 */
    /* Proto-tensor types */
    COWRIE_G1_TAG_INT64_ARRAY  = 0x09,
    COWRIE_G1_TAG_FLOAT64_ARRAY = 0x0A,
    COWRIE_G1_TAG_STRING_ARRAY = 0x0B,
    /* Graph types (v3: aligned with Gen2 at 0x30+0x35-0x39) */
    COWRIE_G1_TAG_ADJLIST      = 0x30,
    COWRIE_G1_TAG_NODE         = 0x35,
    COWRIE_G1_TAG_EDGE         = 0x36,
    COWRIE_G1_TAG_NODE_BATCH   = 0x37,
    COWRIE_G1_TAG_EDGE_BATCH   = 0x38,
    COWRIE_G1_TAG_GRAPH_SHARD  = 0x39,
} cowrie_g1_tag_t;

/* ============================================================
 * Value Types
 * ============================================================ */

typedef enum {
    COWRIE_G1_TYPE_NULL,
    COWRIE_G1_TYPE_BOOL,
    COWRIE_G1_TYPE_INT64,
    COWRIE_G1_TYPE_FLOAT64,
    COWRIE_G1_TYPE_STRING,
    COWRIE_G1_TYPE_BYTES,
    COWRIE_G1_TYPE_ARRAY,
    COWRIE_G1_TYPE_OBJECT,
    COWRIE_G1_TYPE_INT64_ARRAY,
    COWRIE_G1_TYPE_FLOAT64_ARRAY,
    COWRIE_G1_TYPE_STRING_ARRAY,
    COWRIE_G1_TYPE_NODE,
    COWRIE_G1_TYPE_EDGE,
    COWRIE_G1_TYPE_ADJLIST,
    COWRIE_G1_TYPE_NODE_BATCH,
    COWRIE_G1_TYPE_EDGE_BATCH,
    COWRIE_G1_TYPE_GRAPH_SHARD,
} cowrie_g1_type_t;

/* ============================================================
 * Error Codes
 * ============================================================ */

typedef enum {
    COWRIE_G1_OK             = 0,
    COWRIE_G1_ERR_NOMEM      = -1,
    COWRIE_G1_ERR_INVALID    = -2,
    COWRIE_G1_ERR_EOF        = -3,
    COWRIE_G1_ERR_OVERFLOW   = -4,
    COWRIE_G1_ERR_DEPTH      = -5,  /* Maximum nesting depth exceeded */
    COWRIE_G1_ERR_ARRAY_LEN  = -6,  /* Array too large */
    COWRIE_G1_ERR_OBJECT_LEN = -7,  /* Object has too many fields */
    COWRIE_G1_ERR_STRING_LEN = -8,  /* String too long */
    COWRIE_G1_ERR_BYTES_LEN  = -9,  /* Bytes too long */
} cowrie_g1_error_t;

/* Security limits - aligned with Go reference implementation */
#define COWRIE_G1_MAX_DEPTH      1000
#define COWRIE_G1_MAX_ARRAY_LEN  100000000   /* 100M elements */
#define COWRIE_G1_MAX_OBJECT_LEN 10000000    /* 10M fields */
#define COWRIE_G1_MAX_STRING_LEN 500000000   /* 500MB */
#define COWRIE_G1_MAX_BYTES_LEN  1000000000  /* 1GB */

/* ============================================================
 * Buffer
 * ============================================================ */

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} cowrie_g1_buf_t;

/* Buffer operations */
void cowrie_g1_buf_init(cowrie_g1_buf_t *buf);
void cowrie_g1_buf_free(cowrie_g1_buf_t *buf);
int  cowrie_g1_buf_reserve(cowrie_g1_buf_t *buf, size_t extra);
int  cowrie_g1_buf_write(cowrie_g1_buf_t *buf, const void *data, size_t len);
int  cowrie_g1_buf_write_byte(cowrie_g1_buf_t *buf, uint8_t byte);
int  cowrie_g1_buf_write_uvarint(cowrie_g1_buf_t *buf, uint64_t val);

/* ============================================================
 * Value (forward declaration)
 * ============================================================ */

typedef struct cowrie_g1_value cowrie_g1_value_t;
typedef struct cowrie_g1_member cowrie_g1_member_t;

struct cowrie_g1_member {
    char *key;
    cowrie_g1_value_t *value;
};

struct cowrie_g1_value {
    cowrie_g1_type_t type;
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
            cowrie_g1_value_t **items;
            size_t             len;
            size_t             cap;  /* capacity for O(1) amortized append */
        } array_val;
        struct {
            cowrie_g1_member_t *members;
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

cowrie_g1_value_t *cowrie_g1_null(void);
cowrie_g1_value_t *cowrie_g1_bool(bool val);
cowrie_g1_value_t *cowrie_g1_int64(int64_t val);
cowrie_g1_value_t *cowrie_g1_float64(double val);
cowrie_g1_value_t *cowrie_g1_string(const char *str, size_t len);
cowrie_g1_value_t *cowrie_g1_bytes(const uint8_t *data, size_t len);
cowrie_g1_value_t *cowrie_g1_array(size_t capacity);
cowrie_g1_value_t *cowrie_g1_object(size_t capacity);
cowrie_g1_value_t *cowrie_g1_int64_array(const int64_t *data, size_t len);
cowrie_g1_value_t *cowrie_g1_float64_array(const double *data, size_t len);
cowrie_g1_value_t *cowrie_g1_string_array(const char **strings, size_t count);

/* Value operations */
int  cowrie_g1_array_append(cowrie_g1_value_t *arr, cowrie_g1_value_t *val);
int  cowrie_g1_object_set(cowrie_g1_value_t *obj, const char *key, cowrie_g1_value_t *val);
void cowrie_g1_value_free(cowrie_g1_value_t *val);

/* ============================================================
 * Encode/Decode
 * ============================================================ */

/* Encode a value to binary format */
int cowrie_g1_encode(const cowrie_g1_value_t *val, cowrie_g1_buf_t *buf);

/* Decode binary data to a value (caller must free with cowrie_g1_value_free) */
int cowrie_g1_decode(const uint8_t *data, size_t len, cowrie_g1_value_t **out);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/* Zigzag encode/decode */
static inline uint64_t cowrie_g1_zigzag_encode(int64_t n) {
    return (uint64_t)((n << 1) ^ (n >> 63));
}

static inline int64_t cowrie_g1_zigzag_decode(uint64_t n) {
    return (int64_t)((n >> 1) ^ (uint64_t)(-(int64_t)(n & 1)));
}

#ifdef __cplusplus
}
#endif

#endif /* COWRIE_GEN1_H */
