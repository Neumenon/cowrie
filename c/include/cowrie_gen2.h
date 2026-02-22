/*
 * Cowrie v2 - "JSON++" Binary Codec
 *
 * A binary format that extends JSON with better types:
 * - Explicit int64/uint64/float64/decimal128
 * - Native binary (no base64)
 * - datetime64, uuid128, bigint
 * - Dictionary-coded object keys
 * - Clean compression layering
 */

#ifndef COWRIE_H
#define COWRIE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Wire format constants */
#define COWRIE_MAGIC_0      'S'
#define COWRIE_MAGIC_1      'J'
#define COWRIE_VERSION      2

/* Header flags (in header byte 3) */
#define COWRIE_FLAG_COMPRESSED       0x01
#define COWRIE_FLAG_HAS_COLUMN_HINTS 0x08
#define COWRIE_COMP_NONE         0
#define COWRIE_COMP_GZIP         1
#define COWRIE_COMP_ZSTD         2

/* Compression types for framed encoding */
typedef enum {
    COWRIE_COMPRESS_NONE = 0,
    COWRIE_COMPRESS_GZIP = 1,
    COWRIE_COMPRESS_ZSTD = 2
} COWRIECompression;

/* Decompression bomb limit */
#define COWRIE_MAX_DECOMPRESSED_SIZE (256 * 1024 * 1024)

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
} COWRIETag;

/* DType enum for TENSOR - aligned with Go reference implementation */
typedef enum {
    COWRIE_DTYPE_FLOAT32  = 0x01,
    COWRIE_DTYPE_FLOAT16  = 0x02,
    COWRIE_DTYPE_BFLOAT16 = 0x03,
    COWRIE_DTYPE_INT8     = 0x04,
    COWRIE_DTYPE_INT16    = 0x05,
    COWRIE_DTYPE_INT32    = 0x06,
    COWRIE_DTYPE_INT64    = 0x07,
    COWRIE_DTYPE_UINT8    = 0x08,
    COWRIE_DTYPE_UINT16   = 0x09,
    COWRIE_DTYPE_UINT32   = 0x0A,
    COWRIE_DTYPE_UINT64   = 0x0B,
    COWRIE_DTYPE_FLOAT64  = 0x0C,
    COWRIE_DTYPE_BOOL     = 0x0D,
    /* Quantized types */
    COWRIE_DTYPE_QINT4    = 0x10,  /* 4-bit quantized integer */
    COWRIE_DTYPE_QINT2    = 0x11,  /* 2-bit quantized integer */
    COWRIE_DTYPE_QINT3    = 0x12,  /* 3-bit quantized integer */
    COWRIE_DTYPE_TERNARY  = 0x13,  /* Ternary (-1, 0, 1) */
    COWRIE_DTYPE_BINARY   = 0x14   /* Binary (0, 1) */
} COWRIEDType;

/* Image format enum */
typedef enum {
    COWRIE_IMG_JPEG = 0x01,
    COWRIE_IMG_PNG  = 0x02,
    COWRIE_IMG_WEBP = 0x03,
    COWRIE_IMG_AVIF = 0x04,
    COWRIE_IMG_BMP  = 0x05
} COWRIEImageFormat;

/* Audio encoding enum */
typedef enum {
    COWRIE_AUD_PCM_INT16   = 0x01,
    COWRIE_AUD_PCM_FLOAT32 = 0x02,
    COWRIE_AUD_OPUS        = 0x03,
    COWRIE_AUD_AAC         = 0x04
} COWRIEAudioEncoding;

/* ADJLIST id_width enum */
typedef enum {
    COWRIE_ID_INT32 = 0x01,
    COWRIE_ID_INT64 = 0x02
} COWRIEIdWidth;

/* DELTA op codes */
typedef enum {
    COWRIE_DELTA_SET_FIELD    = 0x01,
    COWRIE_DELTA_DELETE_FIELD = 0x02,
    COWRIE_DELTA_APPEND_ARRAY = 0x03
} COWRIEDeltaOp;

/* Value types for the AST */
typedef enum {
    COWRIE_NULL,
    COWRIE_BOOL,
    COWRIE_INT64,
    COWRIE_UINT64,
    COWRIE_FLOAT64,
    COWRIE_DECIMAL128,
    COWRIE_STRING,
    COWRIE_BYTES,
    COWRIE_DATETIME64,
    COWRIE_UUID128,
    COWRIE_BIGINT,
    COWRIE_EXT,
    COWRIE_ARRAY,
    COWRIE_OBJECT,
    /* v2.1 extension types */
    COWRIE_TENSOR,
    COWRIE_TENSOR_REF,
    COWRIE_IMAGE,
    COWRIE_AUDIO,
    COWRIE_ADJLIST,
    COWRIE_RICH_TEXT,
    COWRIE_DELTA,
    /* v2.1 Graph types */
    COWRIE_NODE,
    COWRIE_EDGE,
    COWRIE_NODE_BATCH,
    COWRIE_EDGE_BATCH,
    COWRIE_GRAPH_SHARD
} COWRIEType;

/* Forward declarations */
typedef struct COWRIEValue COWRIEValue;
typedef struct COWRIEMember COWRIEMember;
typedef struct COWRIEDeltaOp_t COWRIEDeltaOp_t;
typedef struct COWRIERichTextSpan COWRIERichTextSpan;

/* Decimal128: value = coef * 10^(-scale) */
typedef struct {
    int8_t scale;          /* -127 to +127 */
    uint8_t coef[16];      /* two's complement big-endian */
} COWRIEDecimal128;

/* Unknown extension payload (TagExt) */
typedef struct {
    uint64_t ext_type;
    uint8_t *payload;
    size_t payload_len;
} COWRIEExt;

/* ============================================================
 * v2.1 Extension Type Structs
 * ============================================================ */

/* Tensor: embeddings, feature vectors, model I/O */
typedef struct {
    uint8_t dtype;         /* COWRIEDType enum */
    uint8_t rank;          /* number of dimensions */
    size_t *dims;          /* dimension sizes [rank] */
    uint8_t *data;         /* raw tensor bytes, row-major */
    size_t data_len;
} COWRIETensor;

/* TensorRef: reference to stored tensor (vector DB, KV cache) */
typedef struct {
    uint8_t store_id;      /* which store/shard */
    uint8_t *key;          /* lookup key (UUID, hash, etc.) */
    size_t key_len;
} COWRIETensorRef;

/* Image: raw images without base64 */
typedef struct {
    uint8_t format;        /* COWRIEImageFormat enum */
    uint16_t width;
    uint16_t height;
    uint8_t *data;         /* encoded image bytes */
    size_t data_len;
} COWRIEImage;

/* Audio: waveforms, voice clips */
typedef struct {
    uint8_t encoding;      /* COWRIEAudioEncoding enum */
    uint32_t sample_rate;
    uint8_t channels;
    uint8_t *data;
    size_t data_len;
} COWRIEAudio;

/* Adjlist: CSR adjacency list for graphs/GNNs */
typedef struct {
    uint8_t id_width;      /* COWRIEIdWidth: 1=int32, 2=int64 */
    size_t node_count;
    size_t edge_count;
    size_t *row_offsets;   /* [node_count + 1] */
    void *col_indices;     /* int32_t* or int64_t* based on id_width */
} COWRIEAdjlist;

/* RichTextSpan: annotated span within rich text */
struct COWRIERichTextSpan {
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
    COWRIERichTextSpan *spans;  /* annotated spans, or NULL */
    size_t span_count;
} COWRIERichText;

/* DeltaOp: single operation in a delta patch */
struct COWRIEDeltaOp_t {
    uint8_t op_code;       /* COWRIEDeltaOp enum */
    size_t field_id;       /* dictionary-coded field ID */
    COWRIEValue *value;     /* for SET_FIELD and APPEND_ARRAY */
};

/* Delta: semantic diff/patch vs previous state */
typedef struct {
    size_t base_id;        /* reference to base object */
    COWRIEDeltaOp_t *ops;
    size_t op_count;
} COWRIEDelta;

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
    COWRIEMember *props;    /* dictionary-coded properties */
    size_t prop_count;
} COWRIENode;

/* Edge: graph edge with source, destination, type, and properties */
typedef struct {
    char *from_id;         /* source node ID */
    size_t from_id_len;
    char *to_id;           /* destination node ID */
    size_t to_id_len;
    char *edge_type;       /* edge type/label */
    size_t edge_type_len;
    COWRIEMember *props;    /* dictionary-coded properties */
    size_t prop_count;
} COWRIEEdge;

/* NodeBatch: batch of nodes for streaming */
typedef struct {
    COWRIENode *nodes;
    size_t node_count;
} COWRIENodeBatch;

/* EdgeBatch: batch of edges for streaming */
typedef struct {
    COWRIEEdge *edges;
    size_t edge_count;
} COWRIEEdgeBatch;

/* GraphShard: self-contained subgraph */
typedef struct {
    COWRIENode *nodes;
    size_t node_count;
    COWRIEEdge *edges;
    size_t edge_count;
    COWRIEMember *metadata; /* dictionary-coded metadata */
    size_t meta_count;
} COWRIEGraphShard;

/* Object member (key-value pair) */
struct COWRIEMember {
    char *key;
    size_t key_len;
    COWRIEValue *value;
};

/* The main value union */
struct COWRIEValue {
    COWRIEType type;
    union {
        int boolean;                    /* COWRIE_BOOL */
        int64_t i64;                    /* COWRIE_INT64 */
        uint64_t u64;                   /* COWRIE_UINT64 */
        double f64;                     /* COWRIE_FLOAT64 */
        COWRIEDecimal128 decimal128;     /* COWRIE_DECIMAL128 */
        struct {
            char *data;
            size_t len;
        } str;                          /* COWRIE_STRING */
        struct {
            uint8_t *data;
            size_t len;
        } bytes;                        /* COWRIE_BYTES */
        int64_t datetime64;             /* COWRIE_DATETIME64 (nanos since epoch) */
        uint8_t uuid[16];               /* COWRIE_UUID128 */
        struct {
            uint8_t *data;
            size_t len;
        } bigint;                       /* COWRIE_BIGINT (two's complement BE) */
        COWRIEExt ext;                   /* COWRIE_EXT */
        struct {
            COWRIEValue **items;
            size_t len;
        } array;                        /* COWRIE_ARRAY */
        struct {
            COWRIEMember *members;
            size_t len;
        } object;                       /* COWRIE_OBJECT */
        /* v2.1 extension types */
        COWRIETensor tensor;             /* COWRIE_TENSOR */
        COWRIETensorRef tensor_ref;      /* COWRIE_TENSOR_REF */
        COWRIEImage image;               /* COWRIE_IMAGE */
        COWRIEAudio audio;               /* COWRIE_AUDIO */
        COWRIEAdjlist adjlist;           /* COWRIE_ADJLIST */
        COWRIERichText rich_text;        /* COWRIE_RICH_TEXT */
        COWRIEDelta delta;               /* COWRIE_DELTA */
        /* v2.1 Graph types */
        COWRIENode node;                 /* COWRIE_NODE */
        COWRIEEdge edge;                 /* COWRIE_EDGE */
        COWRIENodeBatch node_batch;      /* COWRIE_NODE_BATCH */
        COWRIEEdgeBatch edge_batch;      /* COWRIE_EDGE_BATCH */
        COWRIEGraphShard graph_shard;    /* COWRIE_GRAPH_SHARD */
    } as;
};

/* Buffer for encoding */
typedef struct {
    uint8_t *data;
    size_t len;
    size_t cap;
} COWRIEBuf;

/* ============================================================
 * Security Limits (mirrors Go's DecodeOptions)
 * ============================================================ */

/* Default security limits - generous for ML workloads but prevent exhaustion */
#define COWRIE_DEFAULT_MAX_DEPTH      1000
#define COWRIE_DEFAULT_MAX_ARRAY_LEN  100000000   /* 100M elements */
#define COWRIE_DEFAULT_MAX_OBJECT_LEN 10000000    /* 10M fields */
#define COWRIE_DEFAULT_MAX_STRING_LEN 500000000   /* 500MB */
#define COWRIE_DEFAULT_MAX_BYTES_LEN  1000000000  /* 1GB */
#define COWRIE_DEFAULT_MAX_EXT_LEN    100000000   /* 100MB */
#define COWRIE_DEFAULT_MAX_RANK       32
#define COWRIE_DEFAULT_MAX_DICT_LEN   10000000    /* 10M */
#define COWRIE_DEFAULT_MAX_HINT_COUNT 10000       /* 10K column hints */

/* Decode options for configurable security limits */
typedef struct {
    int max_depth;       /* Maximum nesting depth (0 = use default) */
    size_t max_array_len;   /* Maximum array element count */
    size_t max_object_len;  /* Maximum object field count */
    size_t max_string_len;  /* Maximum string byte length */
    size_t max_bytes_len;   /* Maximum bytes length */
    size_t max_ext_len;     /* Maximum ext payload length */
    size_t max_dict_len;    /* Maximum dictionary entry count */
    size_t max_hint_count;  /* Maximum column hint count */
    int    max_rank;        /* Maximum tensor rank (dimensions) */
    int unknown_ext;        /* Unknown ext behavior (0=keep,1=skip,2=error) */
} COWRIEDecodeOpts;

/* Unknown extension behavior */
#define COWRIE_UNKNOWN_EXT_KEEP        0
#define COWRIE_UNKNOWN_EXT_SKIP_AS_NULL 1
#define COWRIE_UNKNOWN_EXT_ERROR       2

/* Initialize decode options with defaults */
static inline void cowrie_decode_opts_init(COWRIEDecodeOpts *opts) {
    opts->max_depth = COWRIE_DEFAULT_MAX_DEPTH;
    opts->max_array_len = COWRIE_DEFAULT_MAX_ARRAY_LEN;
    opts->max_object_len = COWRIE_DEFAULT_MAX_OBJECT_LEN;
    opts->max_string_len = COWRIE_DEFAULT_MAX_STRING_LEN;
    opts->max_bytes_len = COWRIE_DEFAULT_MAX_BYTES_LEN;
    opts->max_ext_len = COWRIE_DEFAULT_MAX_EXT_LEN;
    opts->max_dict_len = COWRIE_DEFAULT_MAX_DICT_LEN;
    opts->max_hint_count = COWRIE_DEFAULT_MAX_HINT_COUNT;
    opts->max_rank = COWRIE_DEFAULT_MAX_RANK;
    opts->unknown_ext = COWRIE_UNKNOWN_EXT_KEEP;
}

/* ============================================================
 * Value Constructors
 * ============================================================ */

COWRIEValue *cowrie_new_null(void);
COWRIEValue *cowrie_new_bool(int b);
COWRIEValue *cowrie_new_int64(int64_t i);
COWRIEValue *cowrie_new_uint64(uint64_t u);
COWRIEValue *cowrie_new_float64(double f);
COWRIEValue *cowrie_new_decimal128(int8_t scale, const uint8_t coef[16]);
COWRIEValue *cowrie_new_string(const char *s, size_t len);
COWRIEValue *cowrie_new_bytes(const uint8_t *data, size_t len);
COWRIEValue *cowrie_new_datetime64(int64_t nanos);
COWRIEValue *cowrie_new_uuid128(const uint8_t uuid[16]);
COWRIEValue *cowrie_new_bigint(const uint8_t *data, size_t len);
COWRIEValue *cowrie_new_ext(uint64_t ext_type, const uint8_t *payload, size_t payload_len);
COWRIEValue *cowrie_new_array(void);
COWRIEValue *cowrie_new_object(void);

/* v2.1 Extension Constructors */
COWRIEValue *cowrie_new_tensor(uint8_t dtype, uint8_t rank, const size_t *dims,
                              const uint8_t *data, size_t data_len);
COWRIEValue *cowrie_new_tensor_ref(uint8_t store_id, const uint8_t *key, size_t key_len);
COWRIEValue *cowrie_new_image(uint8_t format, uint16_t width, uint16_t height,
                             const uint8_t *data, size_t data_len);
COWRIEValue *cowrie_new_audio(uint8_t encoding, uint32_t sample_rate, uint8_t channels,
                             const uint8_t *data, size_t data_len);
COWRIEValue *cowrie_new_adjlist(uint8_t id_width, size_t node_count, size_t edge_count,
                               const size_t *row_offsets, const void *col_indices);
COWRIEValue *cowrie_new_rich_text(const char *text, size_t text_len,
                                 const int32_t *tokens, size_t token_count,
                                 const COWRIERichTextSpan *spans, size_t span_count);
COWRIEValue *cowrie_new_delta(size_t base_id, const COWRIEDeltaOp_t *ops, size_t op_count);

/* v2.1 Graph Type Constructors */
COWRIEValue *cowrie_new_node(const char *id, size_t id_len,
                           const char **labels, const size_t *label_lens, size_t label_count,
                           const COWRIEMember *props, size_t prop_count);
COWRIEValue *cowrie_new_edge(const char *from_id, size_t from_id_len,
                           const char *to_id, size_t to_id_len,
                           const char *edge_type, size_t edge_type_len,
                           const COWRIEMember *props, size_t prop_count);
COWRIEValue *cowrie_new_node_batch(const COWRIENode *nodes, size_t node_count);
COWRIEValue *cowrie_new_edge_batch(const COWRIEEdge *edges, size_t edge_count);
COWRIEValue *cowrie_new_graph_shard(const COWRIENode *nodes, size_t node_count,
                                   const COWRIEEdge *edges, size_t edge_count,
                                   const COWRIEMember *metadata, size_t meta_count);

/* ============================================================
 * Value Manipulation
 * ============================================================ */

/* Array operations */
int cowrie_array_append(COWRIEValue *arr, COWRIEValue *item);
COWRIEValue *cowrie_array_get(const COWRIEValue *arr, size_t index);
size_t cowrie_array_len(const COWRIEValue *arr);

/* Object operations */
int cowrie_object_set(COWRIEValue *obj, const char *key, size_t key_len, COWRIEValue *value);
COWRIEValue *cowrie_object_get(const COWRIEValue *obj, const char *key, size_t key_len);
size_t cowrie_object_len(const COWRIEValue *obj);

/* Memory management */
void cowrie_free(COWRIEValue *v);

/* ============================================================
 * Encoding / Decoding
 * ============================================================ */

/* Encode options */
typedef struct {
    int deterministic;  /* Sort object keys for deterministic output */
    int omit_null;      /* Omit null values from objects */
} COWRIEEncodeOpts;

/* Initialize encode options with defaults */
static inline void cowrie_encode_opts_init(COWRIEEncodeOpts *opts) {
    opts->deterministic = 0;
    opts->omit_null = 0;
}

/*
 * Encode a value to COWRIE v2 binary format.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int cowrie_encode(const COWRIEValue *root, COWRIEBuf *buf);

/*
 * Encode with options (e.g., deterministic mode).
 * Returns 0 on success, -1 on error.
 */
int cowrie_encode_with_opts(const COWRIEValue *root, const COWRIEEncodeOpts *opts, COWRIEBuf *buf);

/*
 * Decode COWRIE v2 binary format to a value.
 * Returns 0 on success, -1 on error.
 * Caller must cowrie_free(*out) when done.
 */
int cowrie_decode(const uint8_t *data, size_t len, COWRIEValue **out);

/*
 * Decode COWRIE v2 with custom security limits.
 * Use cowrie_decode_opts_init() to initialize opts with defaults,
 * then modify as needed before calling.
 */
int cowrie_decode_with_opts(const uint8_t *data, size_t len, 
                           const COWRIEDecodeOpts *opts, COWRIEValue **out);

/* ============================================================
 * Compression Layer
 * ============================================================ */

/*
 * Encode with compression framing.
 * compression: COWRIE_COMP_NONE, COWRIE_COMP_GZIP, or COWRIE_COMP_ZSTD
 */
int cowrie_encode_framed(const COWRIEValue *root, int compression, COWRIEBuf *buf);

/*
 * Decode with automatic decompression.
 * Detects compression from flags byte.
 */
int cowrie_decode_framed(const uint8_t *data, size_t len, COWRIEValue **out);

/* ============================================================
 * Utilities
 * ============================================================ */

/* Buffer helpers */
void cowrie_buf_init(COWRIEBuf *buf);
void cowrie_buf_free(COWRIEBuf *buf);

/* Varint encoding/decoding */
int cowrie_put_uvarint(COWRIEBuf *buf, uint64_t v);
int cowrie_get_uvarint(const uint8_t *data, size_t len, uint64_t *v, size_t *bytes_read);

/* Zigzag encoding for signed integers */
static inline uint64_t cowrie_zigzag_encode(int64_t n) {
    return ((uint64_t)n << 1) ^ (uint64_t)(n >> 63);
}

static inline int64_t cowrie_zigzag_decode(uint64_t n) {
    return (int64_t)((n >> 1) ^ (-(n & 1)));
}

/* ============================================================
 * CRC32 and Schema Fingerprint
 * ============================================================ */

/* CRC32-IEEE checksum */
uint32_t cowrie_crc32(const uint8_t *data, size_t len);

/* Schema fingerprint (FNV-1a hash of type structure) */
uint32_t cowrie_schema_fingerprint32(const COWRIEValue *v);

/* ============================================================
 * Master Stream Protocol
 * ============================================================ */

/* Master stream magic: "SJST" */
#define COWRIE_MASTER_MAGIC_0  'S'
#define COWRIE_MASTER_MAGIC_1  'J'
#define COWRIE_MASTER_MAGIC_2  'S'
#define COWRIE_MASTER_MAGIC_3  'T'
#define COWRIE_MASTER_VERSION  0x02

/* Master stream frame flags */
#define COWRIE_MFLAG_COMPRESSED    0x01
#define COWRIE_MFLAG_CRC           0x02
#define COWRIE_MFLAG_DETERMINISTIC 0x04
#define COWRIE_MFLAG_META          0x08
#define COWRIE_MFLAG_COMP_GZIP     0x10
#define COWRIE_MFLAG_COMP_ZSTD     0x20

/* Master stream writer options */
typedef struct {
    int deterministic;   /* Sort object keys for deterministic output */
    int enable_crc;      /* Append CRC32 checksum */
    int compress;        /* 0=none, 1=gzip, 2=zstd */
} COWRIEMasterWriterOpts;

/* Master stream frame header */
typedef struct {
    uint8_t version;
    uint8_t flags;
    uint16_t header_len;
    uint32_t type_id;
    uint32_t payload_len;
    uint32_t raw_len;
    uint32_t meta_len;
} COWRIEMasterFrameHeader;

/* Decoded master stream frame */
typedef struct {
    COWRIEMasterFrameHeader header;
    COWRIEValue *meta;      /* NULL if no metadata */
    COWRIEValue *payload;
    uint32_t type_id;
} COWRIEMasterFrame;

/* Initialize master writer options with defaults */
static inline void cowrie_master_writer_opts_init(COWRIEMasterWriterOpts *opts) {
    opts->deterministic = 1;
    opts->enable_crc = 1;
    opts->compress = 0;
}

/*
 * Write a master stream frame.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int cowrie_master_write_frame(const COWRIEValue *value, const COWRIEValue *meta,
                              const COWRIEMasterWriterOpts *opts, COWRIEBuf *buf);

/*
 * Read a master stream frame.
 * Returns bytes consumed on success, -1 on error.
 * Caller must call cowrie_master_frame_free() when done.
 */
int cowrie_master_read_frame(const uint8_t *data, size_t len, COWRIEMasterFrame *frame);

/* Free a master frame's contents */
void cowrie_master_frame_free(COWRIEMasterFrame *frame);

/* Check if data starts with master stream magic "SJST" */
static inline int cowrie_is_master_stream(const uint8_t *data, size_t len) {
    return len >= 4 &&
           data[0] == COWRIE_MASTER_MAGIC_0 &&
           data[1] == COWRIE_MASTER_MAGIC_1 &&
           data[2] == COWRIE_MASTER_MAGIC_2 &&
           data[3] == COWRIE_MASTER_MAGIC_3;
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
 * data remains allocated. Do not use after cowrie_free().
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
static inline const float* cowrie_tensor_view_float32(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_FLOAT32) return NULL;
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
static inline const double* cowrie_tensor_view_float64(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_FLOAT64) return NULL;
    if (t->data_len == 0) { *count = 0; return (const double*)t->data; }
    if (t->data_len % 8 != 0) return NULL;
    if (((uintptr_t)t->data) % 8 != 0) return NULL;
    *count = t->data_len / 8;
    return (const double*)t->data;
}

/*
 * Get zero-copy view of tensor as int32 array.
 */
static inline const int32_t* cowrie_tensor_view_int32(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_INT32) return NULL;
    if (t->data_len == 0) { *count = 0; return (const int32_t*)t->data; }
    if (t->data_len % 4 != 0) return NULL;
    if (((uintptr_t)t->data) % 4 != 0) return NULL;
    *count = t->data_len / 4;
    return (const int32_t*)t->data;
}

/*
 * Get zero-copy view of tensor as int64 array.
 */
static inline const int64_t* cowrie_tensor_view_int64(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_INT64) return NULL;
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
static inline const uint8_t* cowrie_tensor_view_uint8(const COWRIETensor *t, size_t *count) {
    if (t == NULL || count == NULL) return NULL;
    if (t->dtype != COWRIE_DTYPE_UINT8) return NULL;
    *count = t->data_len;
    return t->data;
}

/*
 * Copy tensor data as float32 array.
 * Use this when zero-copy view is not possible (alignment issues).
 * Caller must free() the returned array.
 * Returns NULL on allocation failure or dtype mismatch.
 */
float* cowrie_tensor_copy_float32(const COWRIETensor *t, size_t *count);

/*
 * Copy tensor data as float64 array.
 */
double* cowrie_tensor_copy_float64(const COWRIETensor *t, size_t *count);

/*
 * Copy tensor data as int32 array.
 */
int32_t* cowrie_tensor_copy_int32(const COWRIETensor *t, size_t *count);

/*
 * Copy tensor data as int64 array.
 */
int64_t* cowrie_tensor_copy_int64(const COWRIETensor *t, size_t *count);

#ifdef __cplusplus
}
#endif

#endif /* COWRIE_H */
