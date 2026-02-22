/*
 * SJSON v2 - "JSON++" Binary Codec
 *
 * A binary format that extends JSON with better types:
 * - Explicit int64/uint64/float64/decimal128
 * - Native binary (no base64)
 * - datetime64, uuid128, bigint
 * - Dictionary-coded object keys
 * - Clean compression layering
 */

#ifndef SJSON_H
#define SJSON_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Wire format constants */
#define SJSON_MAGIC_0      'S'
#define SJSON_MAGIC_1      'J'
#define SJSON_VERSION      2

/* Header flags (in header byte 3) */
#define SJSON_FLAG_COMPRESSED       0x01
#define SJSON_FLAG_HAS_COLUMN_HINTS 0x08
#define SJSON_COMP_NONE         0
#define SJSON_COMP_GZIP         1
#define SJSON_COMP_ZSTD         2

/* Type tags - Core (0x00-0x1F) */
typedef enum {
    SJT_NULL        = 0x00,
    SJT_FALSE       = 0x01,
    SJT_TRUE        = 0x02,
    SJT_INT64       = 0x03,   /* zigzag varint */
    SJT_FLOAT64     = 0x04,   /* IEEE 754 LE */
    SJT_STRING      = 0x05,   /* len:uvarint + utf8 */
    SJT_ARRAY       = 0x06,   /* count:uvarint + values */
    SJT_OBJECT      = 0x07,   /* count:uvarint + (fieldID:uvarint + value)... */
    SJT_BYTES       = 0x08,   /* len:uvarint + raw bytes */
    SJT_UINT64      = 0x09,   /* uvarint */
    SJT_DECIMAL128  = 0x0A,   /* scale:1byte + 16 bytes */
    SJT_DATETIME64  = 0x0B,   /* int64 nanos LE */
    SJT_UUID128     = 0x0C,   /* 16 bytes */
    SJT_BIGINT      = 0x0D,   /* len:uvarint + two's complement BE */
    SJT_EXT         = 0x0E,   /* ext_type:uvarint + len:uvarint + payload */
    /* v2.1 ML/Multimodal extensions (0x20-0x2F) */
    SJT_TENSOR      = 0x20,   /* dtype:u8 + rank:u8 + dims[rank]:uvarint + data_len:uvarint + data */
    SJT_TENSOR_REF  = 0x21,   /* store_id:u8 + key_len:uvarint + key[] */
    SJT_IMAGE       = 0x22,   /* format:u8 + width:u16 + height:u16 + data_len:uvarint + data */
    SJT_AUDIO       = 0x23,   /* encoding:u8 + sample_rate:u32 + channels:u8 + data_len:uvarint + data */
    /* v2.1 Graph/Delta extensions (0x30-0x3F) */
    SJT_ADJLIST     = 0x30,   /* id_width:u8 + node_count:uvarint + edge_count:uvarint + CSR data */
    SJT_RICH_TEXT   = 0x31,   /* text_len:uvarint + text + flags:u8 + optional tokens/spans */
    SJT_DELTA       = 0x32,   /* base_id:uvarint + op_count:uvarint + ops[] */
    /* v2.1 Graph types (0x35-0x39) */
    SJT_NODE        = 0x35,   /* id:string + labelCount:uvarint + labels:string* + propCount:uvarint + (dictIdx:uvarint + value)* */
    SJT_EDGE        = 0x36,   /* srcId:string + dstId:string + type:string + propCount:uvarint + (dictIdx:uvarint + value)* */
    SJT_NODE_BATCH  = 0x37,   /* count:uvarint + Node[count] */
    SJT_EDGE_BATCH  = 0x38,   /* count:uvarint + Edge[count] */
    SJT_GRAPH_SHARD = 0x39    /* nodeCount:uvarint + Node* + edgeCount:uvarint + Edge* + metaCount:uvarint + (dictIdx:uvarint + value)* */
} SJSONTag;

/* DType enum for TENSOR - aligned with Go reference implementation */
typedef enum {
    SJSON_DTYPE_FLOAT32  = 0x01,
    SJSON_DTYPE_FLOAT16  = 0x02,
    SJSON_DTYPE_BFLOAT16 = 0x03,
    SJSON_DTYPE_INT8     = 0x04,
    SJSON_DTYPE_INT16    = 0x05,
    SJSON_DTYPE_INT32    = 0x06,
    SJSON_DTYPE_INT64    = 0x07,
    SJSON_DTYPE_UINT8    = 0x08,
    SJSON_DTYPE_UINT16   = 0x09,
    SJSON_DTYPE_UINT32   = 0x0A,
    SJSON_DTYPE_UINT64   = 0x0B,
    SJSON_DTYPE_FLOAT64  = 0x0C,
    SJSON_DTYPE_BOOL     = 0x0D,
    /* Quantized types */
    SJSON_DTYPE_QINT4    = 0x10,  /* 4-bit quantized integer */
    SJSON_DTYPE_QINT2    = 0x11,  /* 2-bit quantized integer */
    SJSON_DTYPE_QINT3    = 0x12,  /* 3-bit quantized integer */
    SJSON_DTYPE_TERNARY  = 0x13,  /* Ternary (-1, 0, 1) */
    SJSON_DTYPE_BINARY   = 0x14   /* Binary (0, 1) */
} SJSONDType;

/* Image format enum */
typedef enum {
    SJSON_IMG_JPEG = 0x01,
    SJSON_IMG_PNG  = 0x02,
    SJSON_IMG_WEBP = 0x03,
    SJSON_IMG_AVIF = 0x04,
    SJSON_IMG_BMP  = 0x05
} SJSONImageFormat;

/* Audio encoding enum */
typedef enum {
    SJSON_AUD_PCM_INT16   = 0x01,
    SJSON_AUD_PCM_FLOAT32 = 0x02,
    SJSON_AUD_OPUS        = 0x03,
    SJSON_AUD_AAC         = 0x04
} SJSONAudioEncoding;

/* ADJLIST id_width enum */
typedef enum {
    SJSON_ID_INT32 = 0x01,
    SJSON_ID_INT64 = 0x02
} SJSONIdWidth;

/* DELTA op codes */
typedef enum {
    SJSON_DELTA_SET_FIELD    = 0x01,
    SJSON_DELTA_DELETE_FIELD = 0x02,
    SJSON_DELTA_APPEND_ARRAY = 0x03
} SJSONDeltaOp;

/* Value types for the AST */
typedef enum {
    SJSON_NULL,
    SJSON_BOOL,
    SJSON_INT64,
    SJSON_UINT64,
    SJSON_FLOAT64,
    SJSON_DECIMAL128,
    SJSON_STRING,
    SJSON_BYTES,
    SJSON_DATETIME64,
    SJSON_UUID128,
    SJSON_BIGINT,
    SJSON_EXT,
    SJSON_ARRAY,
    SJSON_OBJECT,
    /* v2.1 extension types */
    SJSON_TENSOR,
    SJSON_TENSOR_REF,
    SJSON_IMAGE,
    SJSON_AUDIO,
    SJSON_ADJLIST,
    SJSON_RICH_TEXT,
    SJSON_DELTA,
    /* v2.1 Graph types */
    SJSON_NODE,
    SJSON_EDGE,
    SJSON_NODE_BATCH,
    SJSON_EDGE_BATCH,
    SJSON_GRAPH_SHARD
} SJSONType;

/* Forward declarations */
typedef struct SJSONValue SJSONValue;
typedef struct SJSONMember SJSONMember;
typedef struct SJSONDeltaOp_t SJSONDeltaOp_t;
typedef struct SJSONRichTextSpan SJSONRichTextSpan;

/* Decimal128: value = coef * 10^(-scale) */
typedef struct {
    int8_t scale;          /* -127 to +127 */
    uint8_t coef[16];      /* two's complement big-endian */
} SJSONDecimal128;

/* Unknown extension payload (TagExt) */
typedef struct {
    uint64_t ext_type;
    uint8_t *payload;
    size_t payload_len;
} SJSONExt;

/* ============================================================
 * v2.1 Extension Type Structs
 * ============================================================ */

/* Tensor: embeddings, feature vectors, model I/O */
typedef struct {
    uint8_t dtype;         /* SJSONDType enum */
    uint8_t rank;          /* number of dimensions */
    size_t *dims;          /* dimension sizes [rank] */
    uint8_t *data;         /* raw tensor bytes, row-major */
    size_t data_len;
} SJSONTensor;

/* TensorRef: reference to stored tensor (vector DB, KV cache) */
typedef struct {
    uint8_t store_id;      /* which store/shard */
    uint8_t *key;          /* lookup key (UUID, hash, etc.) */
    size_t key_len;
} SJSONTensorRef;

/* Image: raw images without base64 */
typedef struct {
    uint8_t format;        /* SJSONImageFormat enum */
    uint16_t width;
    uint16_t height;
    uint8_t *data;         /* encoded image bytes */
    size_t data_len;
} SJSONImage;

/* Audio: waveforms, voice clips */
typedef struct {
    uint8_t encoding;      /* SJSONAudioEncoding enum */
    uint32_t sample_rate;
    uint8_t channels;
    uint8_t *data;
    size_t data_len;
} SJSONAudio;

/* Adjlist: CSR adjacency list for graphs/GNNs */
typedef struct {
    uint8_t id_width;      /* SJSONIdWidth: 1=int32, 2=int64 */
    size_t node_count;
    size_t edge_count;
    size_t *row_offsets;   /* [node_count + 1] */
    void *col_indices;     /* int32_t* or int64_t* based on id_width */
} SJSONAdjlist;

/* RichTextSpan: annotated span within rich text */
struct SJSONRichTextSpan {
    size_t start;          /* byte offset */
    size_t end;            /* byte offset */
    size_t kind_id;        /* application-defined */
};

/* RichText: text with optional token IDs and spans */
typedef struct {
    char *text;
    size_t text_len;
    int32_t *tokens;       /* int32 token IDs, or NULL */
    size_t token_count;
    SJSONRichTextSpan *spans;  /* annotated spans, or NULL */
    size_t span_count;
} SJSONRichText;

/* DeltaOp: single operation in a delta patch */
struct SJSONDeltaOp_t {
    uint8_t op_code;       /* SJSONDeltaOp enum */
    size_t field_id;       /* dictionary-coded field ID */
    SJSONValue *value;     /* for SET_FIELD and APPEND_ARRAY */
};

/* Delta: semantic diff/patch vs previous state */
typedef struct {
    size_t base_id;        /* reference to base object */
    SJSONDeltaOp_t *ops;
    size_t op_count;
} SJSONDelta;

/* ============================================================
 * v2.1 Graph Type Structs
 * ============================================================ */

/* Node: graph node with ID, labels, and properties */
typedef struct {
    char *id;              /* node identifier */
    size_t id_len;
    char **labels;         /* label strings */
    size_t *label_lens;    /* label string lengths */
    size_t label_count;
    SJSONMember *props;    /* dictionary-coded properties */
    size_t prop_count;
} SJSONNode;

/* Edge: graph edge with source, destination, type, and properties */
typedef struct {
    char *from_id;         /* source node ID */
    size_t from_id_len;
    char *to_id;           /* destination node ID */
    size_t to_id_len;
    char *edge_type;       /* edge type/label */
    size_t edge_type_len;
    SJSONMember *props;    /* dictionary-coded properties */
    size_t prop_count;
} SJSONEdge;

/* NodeBatch: batch of nodes for streaming */
typedef struct {
    SJSONNode *nodes;
    size_t node_count;
} SJSONNodeBatch;

/* EdgeBatch: batch of edges for streaming */
typedef struct {
    SJSONEdge *edges;
    size_t edge_count;
} SJSONEdgeBatch;

/* GraphShard: self-contained subgraph */
typedef struct {
    SJSONNode *nodes;
    size_t node_count;
    SJSONEdge *edges;
    size_t edge_count;
    SJSONMember *metadata; /* dictionary-coded metadata */
    size_t meta_count;
} SJSONGraphShard;

/* Object member (key-value pair) */
struct SJSONMember {
    char *key;
    size_t key_len;
    SJSONValue *value;
};

/* The main value union */
struct SJSONValue {
    SJSONType type;
    union {
        int boolean;                    /* SJSON_BOOL */
        int64_t i64;                    /* SJSON_INT64 */
        uint64_t u64;                   /* SJSON_UINT64 */
        double f64;                     /* SJSON_FLOAT64 */
        SJSONDecimal128 decimal128;     /* SJSON_DECIMAL128 */
        struct {
            char *data;
            size_t len;
        } str;                          /* SJSON_STRING */
        struct {
            uint8_t *data;
            size_t len;
        } bytes;                        /* SJSON_BYTES */
        int64_t datetime64;             /* SJSON_DATETIME64 (nanos since epoch) */
        uint8_t uuid[16];               /* SJSON_UUID128 */
        struct {
            uint8_t *data;
            size_t len;
        } bigint;                       /* SJSON_BIGINT (two's complement BE) */
        SJSONExt ext;                   /* SJSON_EXT */
        struct {
            SJSONValue **items;
            size_t len;
        } array;                        /* SJSON_ARRAY */
        struct {
            SJSONMember *members;
            size_t len;
        } object;                       /* SJSON_OBJECT */
        /* v2.1 extension types */
        SJSONTensor tensor;             /* SJSON_TENSOR */
        SJSONTensorRef tensor_ref;      /* SJSON_TENSOR_REF */
        SJSONImage image;               /* SJSON_IMAGE */
        SJSONAudio audio;               /* SJSON_AUDIO */
        SJSONAdjlist adjlist;           /* SJSON_ADJLIST */
        SJSONRichText rich_text;        /* SJSON_RICH_TEXT */
        SJSONDelta delta;               /* SJSON_DELTA */
        /* v2.1 Graph types */
        SJSONNode node;                 /* SJSON_NODE */
        SJSONEdge edge;                 /* SJSON_EDGE */
        SJSONNodeBatch node_batch;      /* SJSON_NODE_BATCH */
        SJSONEdgeBatch edge_batch;      /* SJSON_EDGE_BATCH */
        SJSONGraphShard graph_shard;    /* SJSON_GRAPH_SHARD */
    } as;
};

/* Buffer for encoding */
typedef struct {
    uint8_t *data;
    size_t len;
    size_t cap;
} SJSONBuf;

/* ============================================================
 * Security Limits (mirrors Go's DecodeOptions)
 * ============================================================ */

/* Default security limits - generous for ML workloads but prevent exhaustion */
#define SJSON_DEFAULT_MAX_DEPTH      1000
#define SJSON_DEFAULT_MAX_ARRAY_LEN  100000000   /* 100M elements */
#define SJSON_DEFAULT_MAX_OBJECT_LEN 10000000    /* 10M fields */
#define SJSON_DEFAULT_MAX_STRING_LEN 500000000   /* 500MB */
#define SJSON_DEFAULT_MAX_BYTES_LEN  1000000000  /* 1GB */

/* Decode options for configurable security limits */
typedef struct {
    int max_depth;       /* Maximum nesting depth (0 = use default) */
    size_t max_array_len;   /* Maximum array element count */
    size_t max_object_len;  /* Maximum object field count */
    size_t max_string_len;  /* Maximum string byte length */
    size_t max_bytes_len;   /* Maximum bytes length */
    int unknown_ext;        /* Unknown ext behavior (0=keep,1=skip,2=error) */
} SJSONDecodeOpts;

/* Unknown extension behavior */
#define SJSON_UNKNOWN_EXT_KEEP        0
#define SJSON_UNKNOWN_EXT_SKIP_AS_NULL 1
#define SJSON_UNKNOWN_EXT_ERROR       2

/* Initialize decode options with defaults */
static inline void sjson_decode_opts_init(SJSONDecodeOpts *opts) {
    opts->max_depth = SJSON_DEFAULT_MAX_DEPTH;
    opts->max_array_len = SJSON_DEFAULT_MAX_ARRAY_LEN;
    opts->max_object_len = SJSON_DEFAULT_MAX_OBJECT_LEN;
    opts->max_string_len = SJSON_DEFAULT_MAX_STRING_LEN;
    opts->max_bytes_len = SJSON_DEFAULT_MAX_BYTES_LEN;
    opts->unknown_ext = SJSON_UNKNOWN_EXT_KEEP;
}

/* ============================================================
 * Value Constructors
 * ============================================================ */

SJSONValue *sjson_new_null(void);
SJSONValue *sjson_new_bool(int b);
SJSONValue *sjson_new_int64(int64_t i);
SJSONValue *sjson_new_uint64(uint64_t u);
SJSONValue *sjson_new_float64(double f);
SJSONValue *sjson_new_decimal128(int8_t scale, const uint8_t coef[16]);
SJSONValue *sjson_new_string(const char *s, size_t len);
SJSONValue *sjson_new_bytes(const uint8_t *data, size_t len);
SJSONValue *sjson_new_datetime64(int64_t nanos);
SJSONValue *sjson_new_uuid128(const uint8_t uuid[16]);
SJSONValue *sjson_new_bigint(const uint8_t *data, size_t len);
SJSONValue *sjson_new_ext(uint64_t ext_type, const uint8_t *payload, size_t payload_len);
SJSONValue *sjson_new_array(void);
SJSONValue *sjson_new_object(void);

/* v2.1 Extension Constructors */
SJSONValue *sjson_new_tensor(uint8_t dtype, uint8_t rank, const size_t *dims,
                              const uint8_t *data, size_t data_len);
SJSONValue *sjson_new_tensor_ref(uint8_t store_id, const uint8_t *key, size_t key_len);
SJSONValue *sjson_new_image(uint8_t format, uint16_t width, uint16_t height,
                             const uint8_t *data, size_t data_len);
SJSONValue *sjson_new_audio(uint8_t encoding, uint32_t sample_rate, uint8_t channels,
                             const uint8_t *data, size_t data_len);
SJSONValue *sjson_new_adjlist(uint8_t id_width, size_t node_count, size_t edge_count,
                               const size_t *row_offsets, const void *col_indices);
SJSONValue *sjson_new_rich_text(const char *text, size_t text_len,
                                 const int32_t *tokens, size_t token_count,
                                 const SJSONRichTextSpan *spans, size_t span_count);
SJSONValue *sjson_new_delta(size_t base_id, const SJSONDeltaOp_t *ops, size_t op_count);

/* v2.1 Graph Type Constructors */
SJSONValue *sjson_new_node(const char *id, size_t id_len,
                           const char **labels, const size_t *label_lens, size_t label_count,
                           const SJSONMember *props, size_t prop_count);
SJSONValue *sjson_new_edge(const char *from_id, size_t from_id_len,
                           const char *to_id, size_t to_id_len,
                           const char *edge_type, size_t edge_type_len,
                           const SJSONMember *props, size_t prop_count);
SJSONValue *sjson_new_node_batch(const SJSONNode *nodes, size_t node_count);
SJSONValue *sjson_new_edge_batch(const SJSONEdge *edges, size_t edge_count);
SJSONValue *sjson_new_graph_shard(const SJSONNode *nodes, size_t node_count,
                                   const SJSONEdge *edges, size_t edge_count,
                                   const SJSONMember *metadata, size_t meta_count);

/* ============================================================
 * Value Manipulation
 * ============================================================ */

/* Array operations */
int sjson_array_append(SJSONValue *arr, SJSONValue *item);
SJSONValue *sjson_array_get(const SJSONValue *arr, size_t index);
size_t sjson_array_len(const SJSONValue *arr);

/* Object operations */
int sjson_object_set(SJSONValue *obj, const char *key, size_t key_len, SJSONValue *value);
SJSONValue *sjson_object_get(const SJSONValue *obj, const char *key, size_t key_len);
size_t sjson_object_len(const SJSONValue *obj);

/* Memory management */
void sjson_free(SJSONValue *v);

/* ============================================================
 * Encoding / Decoding
 * ============================================================ */

/* Encode options */
typedef struct {
    int deterministic;  /* Sort object keys for deterministic output */
    int omit_null;      /* Omit null values from objects */
} SJSONEncodeOpts;

/* Initialize encode options with defaults */
static inline void sjson_encode_opts_init(SJSONEncodeOpts *opts) {
    opts->deterministic = 0;
    opts->omit_null = 0;
}

/*
 * Encode a value to SJSON v2 binary format.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int sjson_encode(const SJSONValue *root, SJSONBuf *buf);

/*
 * Encode with options (e.g., deterministic mode).
 * Returns 0 on success, -1 on error.
 */
int sjson_encode_with_opts(const SJSONValue *root, const SJSONEncodeOpts *opts, SJSONBuf *buf);

/*
 * Decode SJSON v2 binary format to a value.
 * Returns 0 on success, -1 on error.
 * Caller must sjson_free(*out) when done.
 */
int sjson_decode(const uint8_t *data, size_t len, SJSONValue **out);

/*
 * Decode SJSON v2 with custom security limits.
 * Use sjson_decode_opts_init() to initialize opts with defaults,
 * then modify as needed before calling.
 */
int sjson_decode_with_opts(const uint8_t *data, size_t len, 
                           const SJSONDecodeOpts *opts, SJSONValue **out);

/* ============================================================
 * Compression Layer
 * ============================================================ */

/*
 * Encode with compression framing.
 * compression: SJSON_COMP_NONE, SJSON_COMP_GZIP, or SJSON_COMP_ZSTD
 */
int sjson_encode_framed(const SJSONValue *root, int compression, SJSONBuf *buf);

/*
 * Decode with automatic decompression.
 * Detects compression from flags byte.
 */
int sjson_decode_framed(const uint8_t *data, size_t len, SJSONValue **out);

/* ============================================================
 * Utilities
 * ============================================================ */

/* Buffer helpers */
void sjson_buf_init(SJSONBuf *buf);
void sjson_buf_free(SJSONBuf *buf);

/* Varint encoding/decoding */
int sjson_put_uvarint(SJSONBuf *buf, uint64_t v);
int sjson_get_uvarint(const uint8_t *data, size_t len, uint64_t *v, size_t *bytes_read);

/* Zigzag encoding for signed integers */
static inline uint64_t sjson_zigzag_encode(int64_t n) {
    return ((uint64_t)n << 1) ^ (uint64_t)(n >> 63);
}

static inline int64_t sjson_zigzag_decode(uint64_t n) {
    return (int64_t)((n >> 1) ^ (-(n & 1)));
}

/* ============================================================
 * CRC32 and Schema Fingerprint
 * ============================================================ */

/* CRC32-IEEE checksum */
uint32_t sjson_crc32(const uint8_t *data, size_t len);

/* Schema fingerprint (FNV-1a hash of type structure) */
uint32_t sjson_schema_fingerprint32(const SJSONValue *v);

/* ============================================================
 * Master Stream Protocol
 * ============================================================ */

/* Master stream magic: "SJST" */
#define SJSON_MASTER_MAGIC_0  'S'
#define SJSON_MASTER_MAGIC_1  'J'
#define SJSON_MASTER_MAGIC_2  'S'
#define SJSON_MASTER_MAGIC_3  'T'
#define SJSON_MASTER_VERSION  0x02

/* Master stream frame flags */
#define SJSON_MFLAG_COMPRESSED    0x01
#define SJSON_MFLAG_CRC           0x02
#define SJSON_MFLAG_DETERMINISTIC 0x04
#define SJSON_MFLAG_META          0x08
#define SJSON_MFLAG_COMP_GZIP     0x10
#define SJSON_MFLAG_COMP_ZSTD     0x20

/* Master stream writer options */
typedef struct {
    int deterministic;   /* Sort object keys for deterministic output */
    int enable_crc;      /* Append CRC32 checksum */
    int compress;        /* 0=none, 1=gzip, 2=zstd */
} SJSONMasterWriterOpts;

/* Master stream frame header */
typedef struct {
    uint8_t version;
    uint8_t flags;
    uint16_t header_len;
    uint32_t type_id;
    uint32_t payload_len;
    uint32_t raw_len;
    uint32_t meta_len;
} SJSONMasterFrameHeader;

/* Decoded master stream frame */
typedef struct {
    SJSONMasterFrameHeader header;
    SJSONValue *meta;      /* NULL if no metadata */
    SJSONValue *payload;
    uint32_t type_id;
} SJSONMasterFrame;

/* Initialize master writer options with defaults */
static inline void sjson_master_writer_opts_init(SJSONMasterWriterOpts *opts) {
    opts->deterministic = 1;
    opts->enable_crc = 1;
    opts->compress = 0;
}

/*
 * Write a master stream frame.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int sjson_master_write_frame(const SJSONValue *value, const SJSONValue *meta,
                              const SJSONMasterWriterOpts *opts, SJSONBuf *buf);

/*
 * Read a master stream frame.
 * Returns bytes consumed on success, -1 on error.
 * Caller must call sjson_master_frame_free() when done.
 */
int sjson_master_read_frame(const uint8_t *data, size_t len, SJSONMasterFrame *frame);

/* Free a master frame's contents */
void sjson_master_frame_free(SJSONMasterFrame *frame);

/* Check if data starts with master stream magic "SJST" */
static inline int sjson_is_master_stream(const uint8_t *data, size_t len) {
    return len >= 4 &&
           data[0] == SJSON_MASTER_MAGIC_0 &&
           data[1] == SJSON_MASTER_MAGIC_1 &&
           data[2] == SJSON_MASTER_MAGIC_2 &&
           data[3] == SJSON_MASTER_MAGIC_3;
}

/* ============================================================
 * Zero-Copy Tensor Views
 * ============================================================
 *
 * These functions provide zero-copy access to tensor data when
 * the alignment and dtype requirements are met. They return NULL
 * if the view cannot be created safely.
 *
 * IMPORTANT: The returned pointer is only valid while the tensor
 * data remains allocated. Do not use after sjson_free().
 */

/*
 * Get zero-copy view of tensor as float32 array.
 * Returns NULL if:
 * - tensor is NULL
 * - dtype is not FLOAT32
 * - data is not 4-byte aligned
 * - data_len is not divisible by 4
 *
 * On success, *count is set to the number of float32 elements.
 */
static inline const float* sjson_tensor_view_float32(const SJSONTensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != SJSON_DTYPE_FLOAT32) return NULL;
    if (t->data_len == 0) { *count = 0; return (const float*)t->data; }
    if (t->data_len % 4 != 0) return NULL;
    if (((uintptr_t)t->data) % 4 != 0) return NULL;
    *count = t->data_len / 4;
    return (const float*)t->data;
}

/*
 * Get zero-copy view of tensor as float64 (double) array.
 * Returns NULL if dtype mismatch, alignment issue, or size issue.
 */
static inline const double* sjson_tensor_view_float64(const SJSONTensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != SJSON_DTYPE_FLOAT64) return NULL;
    if (t->data_len == 0) { *count = 0; return (const double*)t->data; }
    if (t->data_len % 8 != 0) return NULL;
    if (((uintptr_t)t->data) % 8 != 0) return NULL;
    *count = t->data_len / 8;
    return (const double*)t->data;
}

/*
 * Get zero-copy view of tensor as int32 array.
 */
static inline const int32_t* sjson_tensor_view_int32(const SJSONTensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != SJSON_DTYPE_INT32) return NULL;
    if (t->data_len == 0) { *count = 0; return (const int32_t*)t->data; }
    if (t->data_len % 4 != 0) return NULL;
    if (((uintptr_t)t->data) % 4 != 0) return NULL;
    *count = t->data_len / 4;
    return (const int32_t*)t->data;
}

/*
 * Get zero-copy view of tensor as int64 array.
 */
static inline const int64_t* sjson_tensor_view_int64(const SJSONTensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != SJSON_DTYPE_INT64) return NULL;
    if (t->data_len == 0) { *count = 0; return (const int64_t*)t->data; }
    if (t->data_len % 8 != 0) return NULL;
    if (((uintptr_t)t->data) % 8 != 0) return NULL;
    *count = t->data_len / 8;
    return (const int64_t*)t->data;
}

/*
 * Get zero-copy view of tensor as uint8 array.
 * Always succeeds for UINT8 tensors (no alignment requirement).
 */
static inline const uint8_t* sjson_tensor_view_uint8(const SJSONTensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != SJSON_DTYPE_UINT8) return NULL;
    *count = t->data_len;
    return t->data;
}

/*
 * Copy tensor data as float32 array.
 * Use this when zero-copy view is not possible (alignment issues).
 * Caller must free() the returned array.
 * Returns NULL on allocation failure or dtype mismatch.
 */
float* sjson_tensor_copy_float32(const SJSONTensor *t, size_t *count);

/*
 * Copy tensor data as float64 array.
 */
double* sjson_tensor_copy_float64(const SJSONTensor *t, size_t *count);

/*
 * Copy tensor data as int32 array.
 */
int32_t* sjson_tensor_copy_int32(const SJSONTensor *t, size_t *count);

/*
 * Copy tensor data as int64 array.
 */
int64_t* sjson_tensor_copy_int64(const SJSONTensor *t, size_t *count);

#ifdef __cplusplus
}
#endif

#endif /* SJSON_H */
