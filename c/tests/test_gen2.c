/*
 * SJSON Gen2 Test Suite
 */

#include "../include/sjson_gen2.h"
#include "../include/sjson_json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    printf("  Testing %s... ", #name); \
    tests_run++; \
    if (test_##name()) { \
        printf("PASS\n"); \
        tests_passed++; \
    } else { \
        printf("FAIL\n"); \
    } \
} while(0)

#define ASSERT(cond) do { if (!(cond)) { printf("ASSERT FAILED: %s\n", #cond); return 0; } } while(0)

static int read_file(const char *path, char **out, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return 0; }
    long len = ftell(f);
    if (len < 0) { fclose(f); return 0; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return 0; }

    char *buf = (char *)malloc((size_t)len + 1);
    if (!buf) { fclose(f); return 0; }
    size_t n = fread(buf, 1, (size_t)len, f);
    fclose(f);
    if (n != (size_t)len) { free(buf); return 0; }
    buf[len] = '\0';
    *out = buf;
    *out_len = (size_t)len;
    return 1;
}

static int build_repo_root(char *buf, size_t buf_len) {
    const char *file = __FILE__;
    const char *marker = "/c/tests/";
    const char *pos = strstr(file, marker);
    if (!pos) return 0;
    size_t len = (size_t)(pos - file);
    if (len + 1 > buf_len) return 0;
    memcpy(buf, file, len);
    buf[len] = '\0';
    return 1;
}

/* ============================================================
 * Basic Type Tests
 * ============================================================ */

static int test_null_roundtrip(void) {
    SJSONValue *v = sjson_new_null();
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_NULL);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);
    ASSERT(buf.len > 4);  /* Header + at least one byte */

    SJSONValue *decoded;
    ASSERT(sjson_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == SJSON_NULL);

    sjson_free(v);
    sjson_free(decoded);
    sjson_buf_free(&buf);
    return 1;
}

static int test_bool_roundtrip(void) {
    SJSONValue *t = sjson_new_bool(1);
    SJSONValue *f = sjson_new_bool(0);
    ASSERT(t != NULL && f != NULL);
    ASSERT(t->as.boolean == 1);
    ASSERT(f->as.boolean == 0);

    SJSONBuf buf_t, buf_f;
    ASSERT(sjson_encode(t, &buf_t) == 0);
    ASSERT(sjson_encode(f, &buf_f) == 0);

    SJSONValue *dec_t, *dec_f;
    ASSERT(sjson_decode(buf_t.data, buf_t.len, &dec_t) == 0);
    ASSERT(sjson_decode(buf_f.data, buf_f.len, &dec_f) == 0);
    ASSERT(dec_t->type == SJSON_BOOL && dec_t->as.boolean == 1);
    ASSERT(dec_f->type == SJSON_BOOL && dec_f->as.boolean == 0);

    sjson_free(t);
    sjson_free(f);
    sjson_free(dec_t);
    sjson_free(dec_f);
    sjson_buf_free(&buf_t);
    sjson_buf_free(&buf_f);
    return 1;
}

static int test_int64_roundtrip(void) {
    int64_t values[] = {0, 1, -1, 127, -128, 32767, -32768,
                        2147483647LL, -2147483648LL,
                        9223372036854775807LL, -9223372036854775807LL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        SJSONValue *v = sjson_new_int64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.i64 == values[i]);

        SJSONBuf buf;
        ASSERT(sjson_encode(v, &buf) == 0);

        SJSONValue *dec;
        ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == SJSON_INT64);
        ASSERT(dec->as.i64 == values[i]);

        sjson_free(v);
        sjson_free(dec);
        sjson_buf_free(&buf);
    }
    return 1;
}

static int test_uint64_roundtrip(void) {
    uint64_t values[] = {0, 1, 255, 65535, 4294967295ULL, 18446744073709551615ULL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        SJSONValue *v = sjson_new_uint64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.u64 == values[i]);

        SJSONBuf buf;
        ASSERT(sjson_encode(v, &buf) == 0);

        SJSONValue *dec;
        ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == SJSON_UINT64);
        ASSERT(dec->as.u64 == values[i]);

        sjson_free(v);
        sjson_free(dec);
        sjson_buf_free(&buf);
    }
    return 1;
}

static int test_float64_roundtrip(void) {
    double values[] = {0.0, 1.0, -1.0, 3.14159, -2.71828, 1e100, -1e-100};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        SJSONValue *v = sjson_new_float64(values[i]);
        ASSERT(v != NULL);

        SJSONBuf buf;
        ASSERT(sjson_encode(v, &buf) == 0);

        SJSONValue *dec;
        ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == SJSON_FLOAT64);
        ASSERT(dec->as.f64 == values[i]);

        sjson_free(v);
        sjson_free(dec);
        sjson_buf_free(&buf);
    }
    return 1;
}

static int test_string_roundtrip(void) {
    const char *test = "Hello, SJSON!";
    SJSONValue *v = sjson_new_string(test, strlen(test));
    ASSERT(v != NULL);
    ASSERT(v->as.str.len == strlen(test));
    ASSERT(strcmp(v->as.str.data, test) == 0);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_STRING);
    ASSERT(dec->as.str.len == strlen(test));
    ASSERT(strcmp(dec->as.str.data, test) == 0);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_bytes_roundtrip(void) {
    uint8_t data[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
    SJSONValue *v = sjson_new_bytes(data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->as.bytes.len == sizeof(data));
    ASSERT(memcmp(v->as.bytes.data, data, sizeof(data)) == 0);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_BYTES);
    ASSERT(dec->as.bytes.len == sizeof(data));
    ASSERT(memcmp(dec->as.bytes.data, data, sizeof(data)) == 0);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Composite Type Tests
 * ============================================================ */

static int test_array_roundtrip(void) {
    SJSONValue *arr = sjson_new_array();
    ASSERT(arr != NULL);

    sjson_array_append(arr, sjson_new_int64(1));
    sjson_array_append(arr, sjson_new_int64(2));
    sjson_array_append(arr, sjson_new_string("three", 5));
    ASSERT(sjson_array_len(arr) == 3);

    SJSONBuf buf;
    ASSERT(sjson_encode(arr, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_ARRAY);
    ASSERT(sjson_array_len(dec) == 3);
    ASSERT(sjson_array_get(dec, 0)->type == SJSON_INT64);
    ASSERT(sjson_array_get(dec, 0)->as.i64 == 1);
    ASSERT(sjson_array_get(dec, 2)->type == SJSON_STRING);

    sjson_free(arr);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_object_roundtrip(void) {
    SJSONValue *obj = sjson_new_object();
    ASSERT(obj != NULL);

    sjson_object_set(obj, "name", 4, sjson_new_string("Alice", 5));
    sjson_object_set(obj, "age", 3, sjson_new_int64(30));
    sjson_object_set(obj, "active", 6, sjson_new_bool(1));
    ASSERT(sjson_object_len(obj) == 3);

    SJSONBuf buf;
    ASSERT(sjson_encode(obj, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_OBJECT);
    ASSERT(sjson_object_len(dec) == 3);

    SJSONValue *name = sjson_object_get(dec, "name", 4);
    ASSERT(name != NULL);
    ASSERT(name->type == SJSON_STRING);
    ASSERT(strcmp(name->as.str.data, "Alice") == 0);

    SJSONValue *age = sjson_object_get(dec, "age", 3);
    ASSERT(age != NULL);
    ASSERT(age->type == SJSON_INT64);
    ASSERT(age->as.i64 == 30);

    sjson_free(obj);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_nested_object(void) {
    SJSONValue *inner = sjson_new_object();
    sjson_object_set(inner, "x", 1, sjson_new_int64(10));
    sjson_object_set(inner, "y", 1, sjson_new_int64(20));

    SJSONValue *outer = sjson_new_object();
    sjson_object_set(outer, "point", 5, inner);
    sjson_object_set(outer, "label", 5, sjson_new_string("origin", 6));

    SJSONBuf buf;
    ASSERT(sjson_encode(outer, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);

    SJSONValue *point = sjson_object_get(dec, "point", 5);
    ASSERT(point != NULL);
    ASSERT(point->type == SJSON_OBJECT);

    SJSONValue *x = sjson_object_get(point, "x", 1);
    ASSERT(x != NULL);
    ASSERT(x->as.i64 == 10);

    sjson_free(outer);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

/* ============================================================
 * ML Extension Type Tests
 * ============================================================ */

static int test_tensor_roundtrip(void) {
    /* Create a 2x3 float32 tensor */
    size_t dims[] = {2, 3};
    float data[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};

    SJSONValue *v = sjson_new_tensor(
        SJSON_DTYPE_FLOAT32, 2, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_TENSOR);
    ASSERT(v->as.tensor.dtype == SJSON_DTYPE_FLOAT32);
    ASSERT(v->as.tensor.rank == 2);
    ASSERT(v->as.tensor.dims[0] == 2);
    ASSERT(v->as.tensor.dims[1] == 3);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_TENSOR);
    ASSERT(dec->as.tensor.dtype == SJSON_DTYPE_FLOAT32);
    ASSERT(dec->as.tensor.rank == 2);
    ASSERT(dec->as.tensor.dims[0] == 2);
    ASSERT(dec->as.tensor.dims[1] == 3);
    ASSERT(dec->as.tensor.data_len == sizeof(data));

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_tensor_ref_roundtrip(void) {
    uint8_t key[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SJSONValue *v = sjson_new_tensor_ref(1, key, sizeof(key));
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_TENSOR_REF);
    ASSERT(v->as.tensor_ref.store_id == 1);
    ASSERT(v->as.tensor_ref.key_len == sizeof(key));

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_TENSOR_REF);
    ASSERT(dec->as.tensor_ref.store_id == 1);
    ASSERT(memcmp(dec->as.tensor_ref.key, key, sizeof(key)) == 0);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_image_roundtrip(void) {
    uint8_t data[] = {0xFF, 0xD8, 0xFF, 0xE0};  /* JPEG magic */
    SJSONValue *v = sjson_new_image(SJSON_IMG_JPEG, 1920, 1080, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_IMAGE);
    ASSERT(v->as.image.format == SJSON_IMG_JPEG);
    ASSERT(v->as.image.width == 1920);
    ASSERT(v->as.image.height == 1080);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_IMAGE);
    ASSERT(dec->as.image.format == SJSON_IMG_JPEG);
    ASSERT(dec->as.image.width == 1920);
    ASSERT(dec->as.image.height == 1080);
    ASSERT(dec->as.image.data_len == sizeof(data));

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_audio_roundtrip(void) {
    uint8_t data[] = {0x00, 0x00, 0x00, 0x00};  /* Silent PCM */
    SJSONValue *v = sjson_new_audio(SJSON_AUD_PCM_INT16, 44100, 2, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_AUDIO);
    ASSERT(v->as.audio.encoding == SJSON_AUD_PCM_INT16);
    ASSERT(v->as.audio.sample_rate == 44100);
    ASSERT(v->as.audio.channels == 2);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_AUDIO);
    ASSERT(dec->as.audio.encoding == SJSON_AUD_PCM_INT16);
    ASSERT(dec->as.audio.sample_rate == 44100);
    ASSERT(dec->as.audio.channels == 2);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_datetime64_roundtrip(void) {
    int64_t nanos = 1704067200000000000LL;  /* 2024-01-01 00:00:00 UTC */
    SJSONValue *v = sjson_new_datetime64(nanos);
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_DATETIME64);
    ASSERT(v->as.datetime64 == nanos);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_DATETIME64);
    ASSERT(dec->as.datetime64 == nanos);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

static int test_uuid128_roundtrip(void) {
    uint8_t uuid[16] = {
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00
    };
    SJSONValue *v = sjson_new_uuid128(uuid);
    ASSERT(v != NULL);
    ASSERT(v->type == SJSON_UUID128);
    ASSERT(memcmp(v->as.uuid, uuid, 16) == 0);

    SJSONBuf buf;
    ASSERT(sjson_encode(v, &buf) == 0);

    SJSONValue *dec;
    ASSERT(sjson_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == SJSON_UUID128);
    ASSERT(memcmp(dec->as.uuid, uuid, 16) == 0);

    sjson_free(v);
    sjson_free(dec);
    sjson_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Special Feature Tests
 * ============================================================ */

static int test_deterministic_encoding(void) {
    /* Create object with keys in different orders */
    SJSONValue *obj1 = sjson_new_object();
    sjson_object_set(obj1, "zebra", 5, sjson_new_int64(1));
    sjson_object_set(obj1, "alpha", 5, sjson_new_int64(2));
    sjson_object_set(obj1, "beta", 4, sjson_new_int64(3));

    SJSONValue *obj2 = sjson_new_object();
    sjson_object_set(obj2, "alpha", 5, sjson_new_int64(2));
    sjson_object_set(obj2, "beta", 4, sjson_new_int64(3));
    sjson_object_set(obj2, "zebra", 5, sjson_new_int64(1));

    SJSONEncodeOpts opts;
    sjson_encode_opts_init(&opts);
    opts.deterministic = 1;

    SJSONBuf buf1, buf2;
    ASSERT(sjson_encode_with_opts(obj1, &opts, &buf1) == 0);
    ASSERT(sjson_encode_with_opts(obj2, &opts, &buf2) == 0);

    /* Deterministic encoding should produce identical output */
    ASSERT(buf1.len == buf2.len);
    ASSERT(memcmp(buf1.data, buf2.data, buf1.len) == 0);

    sjson_free(obj1);
    sjson_free(obj2);
    sjson_buf_free(&buf1);
    sjson_buf_free(&buf2);
    return 1;
}

static int test_schema_fingerprint(void) {
    /* Same structure should produce same fingerprint */
    SJSONValue *obj1 = sjson_new_object();
    sjson_object_set(obj1, "x", 1, sjson_new_int64(1));
    sjson_object_set(obj1, "y", 1, sjson_new_int64(2));

    SJSONValue *obj2 = sjson_new_object();
    sjson_object_set(obj2, "x", 1, sjson_new_int64(100));
    sjson_object_set(obj2, "y", 1, sjson_new_int64(200));

    uint32_t fp1 = sjson_schema_fingerprint32(obj1);
    uint32_t fp2 = sjson_schema_fingerprint32(obj2);
    ASSERT(fp1 == fp2);  /* Same structure, different values */

    /* Different structure should produce different fingerprint */
    SJSONValue *obj3 = sjson_new_object();
    sjson_object_set(obj3, "x", 1, sjson_new_string("hello", 5));

    uint32_t fp3 = sjson_schema_fingerprint32(obj3);
    ASSERT(fp1 != fp3);

    sjson_free(obj1);
    sjson_free(obj2);
    sjson_free(obj3);
    return 1;
}

static int test_crc32(void) {
    const char *test = "Hello, World!";
    uint32_t crc = sjson_crc32((const uint8_t *)test, strlen(test));
    /* Known CRC32-IEEE value for "Hello, World!" */
    ASSERT(crc == 0xEC4AC3D0);
    return 1;
}

static int test_master_stream(void) {
    SJSONValue *value = sjson_new_object();
    sjson_object_set(value, "message", 7, sjson_new_string("test", 4));

    SJSONValue *meta = sjson_new_object();
    sjson_object_set(meta, "timestamp", 9, sjson_new_int64(1234567890));

    SJSONMasterWriterOpts opts;
    sjson_master_writer_opts_init(&opts);

    SJSONBuf buf;
    ASSERT(sjson_master_write_frame(value, meta, &opts, &buf) == 0);
    ASSERT(buf.len > 24);  /* Header + data */

    /* Check magic */
    ASSERT(sjson_is_master_stream(buf.data, buf.len));

    /* Read back */
    SJSONMasterFrame frame;
    int consumed = sjson_master_read_frame(buf.data, buf.len, &frame);
    ASSERT(consumed > 0);
    ASSERT(frame.payload != NULL);
    ASSERT(frame.meta != NULL);

    SJSONValue *msg = sjson_object_get(frame.payload, "message", 7);
    ASSERT(msg != NULL);
    ASSERT(strcmp(msg->as.str.data, "test") == 0);

    sjson_master_frame_free(&frame);
    sjson_free(value);
    sjson_free(meta);
    sjson_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Graph Type Tests
 * ============================================================ */

static int test_node_roundtrip(void) {
    const char *labels[] = {"Person", "Employee"};
    size_t label_lens[] = {6, 8};

    SJSONMember props[2];
    props[0].key = "name";
    props[0].key_len = 4;
    props[0].value = sjson_new_string("Alice", 5);
    props[1].key = "age";
    props[1].key_len = 3;
    props[1].value = sjson_new_int64(30);

    SJSONValue *node = sjson_new_node("node_42", 7, labels, label_lens, 2, props, 2);
    ASSERT(node != NULL);

    SJSONBuf buf;
    ASSERT(sjson_encode(node, &buf) == 0);

    SJSONValue *decoded;
    ASSERT(sjson_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == SJSON_NODE);
    ASSERT(strcmp(decoded->as.node.id, "node_42") == 0);
    ASSERT(decoded->as.node.label_count == 2);
    ASSERT(strcmp(decoded->as.node.labels[0], "Person") == 0);
    ASSERT(strcmp(decoded->as.node.labels[1], "Employee") == 0);
    ASSERT(decoded->as.node.prop_count == 2);

    sjson_free(node);
    sjson_free(decoded);
    sjson_buf_free(&buf);
    return 1;
}

static int test_edge_roundtrip(void) {
    SJSONMember props[1];
    props[0].key = "weight";
    props[0].key_len = 6;
    props[0].value = sjson_new_float64(0.75);

    SJSONValue *edge = sjson_new_edge("node_1", 6, "node_2", 6, "KNOWS", 5, props, 1);
    ASSERT(edge != NULL);

    SJSONBuf buf;
    ASSERT(sjson_encode(edge, &buf) == 0);

    SJSONValue *decoded;
    ASSERT(sjson_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == SJSON_EDGE);
    ASSERT(strcmp(decoded->as.edge.from_id, "node_1") == 0);
    ASSERT(strcmp(decoded->as.edge.to_id, "node_2") == 0);
    ASSERT(strcmp(decoded->as.edge.edge_type, "KNOWS") == 0);
    ASSERT(decoded->as.edge.prop_count == 1);

    sjson_free(edge);
    sjson_free(decoded);
    sjson_buf_free(&buf);
    return 1;
}

static int test_graph_shard_roundtrip(void) {
    /* Create a simple node */
    SJSONNode nodes[1];
    nodes[0].id = "n1";
    nodes[0].id_len = 2;
    nodes[0].labels = NULL;
    nodes[0].label_lens = NULL;
    nodes[0].label_count = 0;
    nodes[0].props = NULL;
    nodes[0].prop_count = 0;

    /* Create a simple edge */
    SJSONEdge edges[1];
    edges[0].from_id = "n1";
    edges[0].from_id_len = 2;
    edges[0].to_id = "n1";
    edges[0].to_id_len = 2;
    edges[0].edge_type = "SELF";
    edges[0].edge_type_len = 4;
    edges[0].props = NULL;
    edges[0].prop_count = 0;

    /* Create metadata */
    SJSONMember meta[1];
    meta[0].key = "version";
    meta[0].key_len = 7;
    meta[0].value = sjson_new_int64(1);

    SJSONValue *shard = sjson_new_graph_shard(nodes, 1, edges, 1, meta, 1);
    ASSERT(shard != NULL);

    SJSONBuf buf;
    ASSERT(sjson_encode(shard, &buf) == 0);

    SJSONValue *decoded;
    ASSERT(sjson_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == SJSON_GRAPH_SHARD);
    ASSERT(decoded->as.graph_shard.node_count == 1);
    ASSERT(decoded->as.graph_shard.edge_count == 1);
    ASSERT(decoded->as.graph_shard.meta_count == 1);

    sjson_free(shard);
    sjson_free(decoded);
    sjson_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Fixture Tests (Core)
 * ============================================================ */

static int test_fixtures_core(void) {
    char root[1024];
    if (!build_repo_root(root, sizeof(root))) return 0;

    const char *cases[][2] = {
        {"core/null.sjson", "core/null.json"},
        {"core/true.sjson", "core/true.json"},
        {"core/int.sjson", "core/int.json"},
        {"core/float.sjson", "core/float.json"},
        {"core/string.sjson", "core/string.json"},
        {"core/array.sjson", "core/array.json"},
        {"core/object.sjson", "core/object.json"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input_path[1200];
        char json_path[1200];
        snprintf(input_path, sizeof(input_path), "%s/testdata/fixtures/%s", root, cases[i][0]);
        snprintf(json_path, sizeof(json_path), "%s/testdata/fixtures/%s", root, cases[i][1]);

        char *input_data = NULL;
        size_t input_len = 0;
        if (!read_file(input_path, &input_data, &input_len)) return 0;

        SJSONValue *decoded = NULL;
        if (sjson_decode((const uint8_t *)input_data, input_len, &decoded) != 0) {
            free(input_data);
            return 0;
        }
        free(input_data);

        SJSONBuf actual_json;
        sjson_buf_init(&actual_json);
        if (sjson_to_json(decoded, &actual_json) != 0) {
            sjson_free(decoded);
            sjson_buf_free(&actual_json);
            return 0;
        }

        char *expected_json = NULL;
        size_t expected_len = 0;
        if (!read_file(json_path, &expected_json, &expected_len)) {
            sjson_free(decoded);
            sjson_buf_free(&actual_json);
            return 0;
        }

        SJSONValue *expected_val = NULL;
        if (sjson_from_json(expected_json, expected_len, &expected_val) != 0) {
            free(expected_json);
            sjson_free(decoded);
            sjson_buf_free(&actual_json);
            return 0;
        }
        free(expected_json);

        SJSONBuf expected_canon;
        sjson_buf_init(&expected_canon);
        if (sjson_to_json(expected_val, &expected_canon) != 0) {
            sjson_free(expected_val);
            sjson_free(decoded);
            sjson_buf_free(&actual_json);
            sjson_buf_free(&expected_canon);
            return 0;
        }

        int ok = (actual_json.len == expected_canon.len) &&
                 (memcmp(actual_json.data, expected_canon.data, actual_json.len) == 0);

        sjson_free(expected_val);
        sjson_free(decoded);
        sjson_buf_free(&actual_json);
        sjson_buf_free(&expected_canon);

        if (!ok) return 0;
    }

    return 1;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("SJSON Gen2 Test Suite\n");
    printf("=====================\n\n");

    printf("Basic Types:\n");
    TEST(null_roundtrip);
    TEST(bool_roundtrip);
    TEST(int64_roundtrip);
    TEST(uint64_roundtrip);
    TEST(float64_roundtrip);
    TEST(string_roundtrip);
    TEST(bytes_roundtrip);

    printf("\nComposite Types:\n");
    TEST(array_roundtrip);
    TEST(object_roundtrip);
    TEST(nested_object);

    printf("\nML Extension Types:\n");
    TEST(tensor_roundtrip);
    TEST(tensor_ref_roundtrip);
    TEST(image_roundtrip);
    TEST(audio_roundtrip);
    TEST(datetime64_roundtrip);
    TEST(uuid128_roundtrip);

    printf("\nGraph Types:\n");
    TEST(node_roundtrip);
    TEST(edge_roundtrip);
    TEST(graph_shard_roundtrip);

    printf("\nSpecial Features:\n");
    TEST(deterministic_encoding);
    TEST(schema_fingerprint);
    TEST(crc32);
    TEST(master_stream);

    printf("\nFixtures:\n");
    TEST(fixtures_core);

    printf("\n=====================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
