/*
 * COWRIE Gen2 Test Suite
 */

#include "../include/cowrie_gen2.h"
#include "../include/cowrie_json.h"
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
    COWRIEValue *v = cowrie_new_null();
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);
    ASSERT(buf.len > 4);  /* Header + at least one byte */

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_NULL);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bool_roundtrip(void) {
    COWRIEValue *t = cowrie_new_bool(1);
    COWRIEValue *f = cowrie_new_bool(0);
    ASSERT(t != NULL && f != NULL);
    ASSERT(t->as.boolean == 1);
    ASSERT(f->as.boolean == 0);

    COWRIEBuf buf_t, buf_f;
    ASSERT(cowrie_encode(t, &buf_t) == 0);
    ASSERT(cowrie_encode(f, &buf_f) == 0);

    COWRIEValue *dec_t, *dec_f;
    ASSERT(cowrie_decode(buf_t.data, buf_t.len, &dec_t) == 0);
    ASSERT(cowrie_decode(buf_f.data, buf_f.len, &dec_f) == 0);
    ASSERT(dec_t->type == COWRIE_BOOL && dec_t->as.boolean == 1);
    ASSERT(dec_f->type == COWRIE_BOOL && dec_f->as.boolean == 0);

    cowrie_free(t);
    cowrie_free(f);
    cowrie_free(dec_t);
    cowrie_free(dec_f);
    cowrie_buf_free(&buf_t);
    cowrie_buf_free(&buf_f);
    return 1;
}

static int test_int64_roundtrip(void) {
    int64_t values[] = {0, 1, -1, 127, -128, 32767, -32768,
                        2147483647LL, -2147483648LL,
                        9223372036854775807LL, -9223372036854775807LL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_int64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.i64 == values[i]);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_INT64);
        ASSERT(dec->as.i64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_uint64_roundtrip(void) {
    uint64_t values[] = {0, 1, 255, 65535, 4294967295ULL, 18446744073709551615ULL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_uint64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.u64 == values[i]);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_UINT64);
        ASSERT(dec->as.u64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_float64_roundtrip(void) {
    double values[] = {0.0, 1.0, -1.0, 3.14159, -2.71828, 1e100, -1e-100};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_float64(values[i]);
        ASSERT(v != NULL);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_FLOAT64);
        ASSERT(dec->as.f64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_string_roundtrip(void) {
    const char *test = "Hello, COWRIE!";
    COWRIEValue *v = cowrie_new_string(test, strlen(test));
    ASSERT(v != NULL);
    ASSERT(v->as.str.len == strlen(test));
    ASSERT(strcmp(v->as.str.data, test) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_STRING);
    ASSERT(dec->as.str.len == strlen(test));
    ASSERT(strcmp(dec->as.str.data, test) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bytes_roundtrip(void) {
    uint8_t data[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
    COWRIEValue *v = cowrie_new_bytes(data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->as.bytes.len == sizeof(data));
    ASSERT(memcmp(v->as.bytes.data, data, sizeof(data)) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BYTES);
    ASSERT(dec->as.bytes.len == sizeof(data));
    ASSERT(memcmp(dec->as.bytes.data, data, sizeof(data)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Composite Type Tests
 * ============================================================ */

static int test_array_roundtrip(void) {
    COWRIEValue *arr = cowrie_new_array();
    ASSERT(arr != NULL);

    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));
    cowrie_array_append(arr, cowrie_new_string("three", 5));
    ASSERT(cowrie_array_len(arr) == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(arr, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(dec) == 3);
    ASSERT(cowrie_array_get(dec, 0)->type == COWRIE_INT64);
    ASSERT(cowrie_array_get(dec, 0)->as.i64 == 1);
    ASSERT(cowrie_array_get(dec, 2)->type == COWRIE_STRING);

    cowrie_free(arr);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_object_roundtrip(void) {
    COWRIEValue *obj = cowrie_new_object();
    ASSERT(obj != NULL);

    cowrie_object_set(obj, "name", 4, cowrie_new_string("Alice", 5));
    cowrie_object_set(obj, "age", 3, cowrie_new_int64(30));
    cowrie_object_set(obj, "active", 6, cowrie_new_bool(1));
    ASSERT(cowrie_object_len(obj) == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(obj, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(dec) == 3);

    COWRIEValue *name = cowrie_object_get(dec, "name", 4);
    ASSERT(name != NULL);
    ASSERT(name->type == COWRIE_STRING);
    ASSERT(strcmp(name->as.str.data, "Alice") == 0);

    COWRIEValue *age = cowrie_object_get(dec, "age", 3);
    ASSERT(age != NULL);
    ASSERT(age->type == COWRIE_INT64);
    ASSERT(age->as.i64 == 30);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_nested_object(void) {
    COWRIEValue *inner = cowrie_new_object();
    cowrie_object_set(inner, "x", 1, cowrie_new_int64(10));
    cowrie_object_set(inner, "y", 1, cowrie_new_int64(20));

    COWRIEValue *outer = cowrie_new_object();
    cowrie_object_set(outer, "point", 5, inner);
    cowrie_object_set(outer, "label", 5, cowrie_new_string("origin", 6));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(outer, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);

    COWRIEValue *point = cowrie_object_get(dec, "point", 5);
    ASSERT(point != NULL);
    ASSERT(point->type == COWRIE_OBJECT);

    COWRIEValue *x = cowrie_object_get(point, "x", 1);
    ASSERT(x != NULL);
    ASSERT(x->as.i64 == 10);

    cowrie_free(outer);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * ML Extension Type Tests
 * ============================================================ */

static int test_tensor_roundtrip(void) {
    /* Create a 2x3 float32 tensor */
    size_t dims[] = {2, 3};
    float data[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_FLOAT32, 2, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_TENSOR);
    ASSERT(v->as.tensor.dtype == COWRIE_DTYPE_FLOAT32);
    ASSERT(v->as.tensor.rank == 2);
    ASSERT(v->as.tensor.dims[0] == 2);
    ASSERT(v->as.tensor.dims[1] == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_TENSOR);
    ASSERT(dec->as.tensor.dtype == COWRIE_DTYPE_FLOAT32);
    ASSERT(dec->as.tensor.rank == 2);
    ASSERT(dec->as.tensor.dims[0] == 2);
    ASSERT(dec->as.tensor.dims[1] == 3);
    ASSERT(dec->as.tensor.data_len == sizeof(data));

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_tensor_ref_roundtrip(void) {
    uint8_t key[] = {0xDE, 0xAD, 0xBE, 0xEF};
    COWRIEValue *v = cowrie_new_tensor_ref(1, key, sizeof(key));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_TENSOR_REF);
    ASSERT(v->as.tensor_ref.store_id == 1);
    ASSERT(v->as.tensor_ref.key_len == sizeof(key));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_TENSOR_REF);
    ASSERT(dec->as.tensor_ref.store_id == 1);
    ASSERT(memcmp(dec->as.tensor_ref.key, key, sizeof(key)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_image_roundtrip(void) {
    uint8_t data[] = {0xFF, 0xD8, 0xFF, 0xE0};  /* JPEG magic */
    COWRIEValue *v = cowrie_new_image(COWRIE_IMG_JPEG, 1920, 1080, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_IMAGE);
    ASSERT(v->as.image.format == COWRIE_IMG_JPEG);
    ASSERT(v->as.image.width == 1920);
    ASSERT(v->as.image.height == 1080);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_IMAGE);
    ASSERT(dec->as.image.format == COWRIE_IMG_JPEG);
    ASSERT(dec->as.image.width == 1920);
    ASSERT(dec->as.image.height == 1080);
    ASSERT(dec->as.image.data_len == sizeof(data));

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_audio_roundtrip(void) {
    uint8_t data[] = {0x00, 0x00, 0x00, 0x00};  /* Silent PCM */
    COWRIEValue *v = cowrie_new_audio(COWRIE_AUD_PCM_INT16, 44100, 2, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_AUDIO);
    ASSERT(v->as.audio.encoding == COWRIE_AUD_PCM_INT16);
    ASSERT(v->as.audio.sample_rate == 44100);
    ASSERT(v->as.audio.channels == 2);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_AUDIO);
    ASSERT(dec->as.audio.encoding == COWRIE_AUD_PCM_INT16);
    ASSERT(dec->as.audio.sample_rate == 44100);
    ASSERT(dec->as.audio.channels == 2);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_datetime64_roundtrip(void) {
    int64_t nanos = 1704067200000000000LL;  /* 2024-01-01 00:00:00 UTC */
    COWRIEValue *v = cowrie_new_datetime64(nanos);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_DATETIME64);
    ASSERT(v->as.datetime64 == nanos);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_DATETIME64);
    ASSERT(dec->as.datetime64 == nanos);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_uuid128_roundtrip(void) {
    uint8_t uuid[16] = {
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00
    };
    COWRIEValue *v = cowrie_new_uuid128(uuid);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_UUID128);
    ASSERT(memcmp(v->as.uuid, uuid, 16) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_UUID128);
    ASSERT(memcmp(dec->as.uuid, uuid, 16) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Special Feature Tests
 * ============================================================ */

static int test_deterministic_encoding(void) {
    /* Create object with keys in different orders */
    COWRIEValue *obj1 = cowrie_new_object();
    cowrie_object_set(obj1, "zebra", 5, cowrie_new_int64(1));
    cowrie_object_set(obj1, "alpha", 5, cowrie_new_int64(2));
    cowrie_object_set(obj1, "beta", 4, cowrie_new_int64(3));

    COWRIEValue *obj2 = cowrie_new_object();
    cowrie_object_set(obj2, "alpha", 5, cowrie_new_int64(2));
    cowrie_object_set(obj2, "beta", 4, cowrie_new_int64(3));
    cowrie_object_set(obj2, "zebra", 5, cowrie_new_int64(1));

    COWRIEEncodeOpts opts;
    cowrie_encode_opts_init(&opts);
    opts.deterministic = 1;

    COWRIEBuf buf1, buf2;
    ASSERT(cowrie_encode_with_opts(obj1, &opts, &buf1) == 0);
    ASSERT(cowrie_encode_with_opts(obj2, &opts, &buf2) == 0);

    /* Deterministic encoding should produce identical output */
    ASSERT(buf1.len == buf2.len);
    ASSERT(memcmp(buf1.data, buf2.data, buf1.len) == 0);

    cowrie_free(obj1);
    cowrie_free(obj2);
    cowrie_buf_free(&buf1);
    cowrie_buf_free(&buf2);
    return 1;
}

static int test_schema_fingerprint(void) {
    /* Same structure should produce same fingerprint */
    COWRIEValue *obj1 = cowrie_new_object();
    cowrie_object_set(obj1, "x", 1, cowrie_new_int64(1));
    cowrie_object_set(obj1, "y", 1, cowrie_new_int64(2));

    COWRIEValue *obj2 = cowrie_new_object();
    cowrie_object_set(obj2, "x", 1, cowrie_new_int64(100));
    cowrie_object_set(obj2, "y", 1, cowrie_new_int64(200));

    uint32_t fp1 = cowrie_schema_fingerprint32(obj1);
    uint32_t fp2 = cowrie_schema_fingerprint32(obj2);
    ASSERT(fp1 == fp2);  /* Same structure, different values */

    /* Different structure should produce different fingerprint */
    COWRIEValue *obj3 = cowrie_new_object();
    cowrie_object_set(obj3, "x", 1, cowrie_new_string("hello", 5));

    uint32_t fp3 = cowrie_schema_fingerprint32(obj3);
    ASSERT(fp1 != fp3);

    cowrie_free(obj1);
    cowrie_free(obj2);
    cowrie_free(obj3);
    return 1;
}

static int test_crc32(void) {
    const char *test = "Hello, World!";
    uint32_t crc = cowrie_crc32((const uint8_t *)test, strlen(test));
    /* Known CRC32-IEEE value for "Hello, World!" */
    ASSERT(crc == 0xEC4AC3D0);
    return 1;
}

static int test_master_stream(void) {
    COWRIEValue *value = cowrie_new_object();
    cowrie_object_set(value, "message", 7, cowrie_new_string("test", 4));

    COWRIEValue *meta = cowrie_new_object();
    cowrie_object_set(meta, "timestamp", 9, cowrie_new_int64(1234567890));

    COWRIEMasterWriterOpts opts;
    cowrie_master_writer_opts_init(&opts);

    COWRIEBuf buf;
    ASSERT(cowrie_master_write_frame(value, meta, &opts, &buf) == 0);
    ASSERT(buf.len > 24);  /* Header + data */

    /* Check magic */
    ASSERT(cowrie_is_master_stream(buf.data, buf.len));

    /* Read back */
    COWRIEMasterFrame frame;
    int consumed = cowrie_master_read_frame(buf.data, buf.len, &frame);
    ASSERT(consumed > 0);
    ASSERT(frame.payload != NULL);
    ASSERT(frame.meta != NULL);

    COWRIEValue *msg = cowrie_object_get(frame.payload, "message", 7);
    ASSERT(msg != NULL);
    ASSERT(strcmp(msg->as.str.data, "test") == 0);

    cowrie_master_frame_free(&frame);
    cowrie_free(value);
    cowrie_free(meta);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Graph Type Tests
 * ============================================================ */

static int test_node_roundtrip(void) {
    const char *labels[] = {"Person", "Employee"};
    size_t label_lens[] = {6, 8};

    COWRIEMember props[2];
    props[0].key = "name";
    props[0].key_len = 4;
    props[0].value = cowrie_new_string("Alice", 5);
    props[1].key = "age";
    props[1].key_len = 3;
    props[1].value = cowrie_new_int64(30);

    COWRIEValue *node = cowrie_new_node("node_42", 7, labels, label_lens, 2, props, 2);
    ASSERT(node != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(node, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_NODE);
    ASSERT(strcmp(decoded->as.node.id, "node_42") == 0);
    ASSERT(decoded->as.node.label_count == 2);
    ASSERT(strcmp(decoded->as.node.labels[0], "Person") == 0);
    ASSERT(strcmp(decoded->as.node.labels[1], "Employee") == 0);
    ASSERT(decoded->as.node.prop_count == 2);

    cowrie_free(node);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_edge_roundtrip(void) {
    COWRIEMember props[1];
    props[0].key = "weight";
    props[0].key_len = 6;
    props[0].value = cowrie_new_float64(0.75);

    COWRIEValue *edge = cowrie_new_edge("node_1", 6, "node_2", 6, "KNOWS", 5, props, 1);
    ASSERT(edge != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(edge, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_EDGE);
    ASSERT(strcmp(decoded->as.edge.from_id, "node_1") == 0);
    ASSERT(strcmp(decoded->as.edge.to_id, "node_2") == 0);
    ASSERT(strcmp(decoded->as.edge.edge_type, "KNOWS") == 0);
    ASSERT(decoded->as.edge.prop_count == 1);

    cowrie_free(edge);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_graph_shard_roundtrip(void) {
    /* Create a simple node */
    COWRIENode nodes[1];
    nodes[0].id = "n1";
    nodes[0].id_len = 2;
    nodes[0].labels = NULL;
    nodes[0].label_lens = NULL;
    nodes[0].label_count = 0;
    nodes[0].props = NULL;
    nodes[0].prop_count = 0;

    /* Create a simple edge */
    COWRIEEdge edges[1];
    edges[0].from_id = "n1";
    edges[0].from_id_len = 2;
    edges[0].to_id = "n1";
    edges[0].to_id_len = 2;
    edges[0].edge_type = "SELF";
    edges[0].edge_type_len = 4;
    edges[0].props = NULL;
    edges[0].prop_count = 0;

    /* Create metadata */
    COWRIEMember meta[1];
    meta[0].key = "version";
    meta[0].key_len = 7;
    meta[0].value = cowrie_new_int64(1);

    COWRIEValue *shard = cowrie_new_graph_shard(nodes, 1, edges, 1, meta, 1);
    ASSERT(shard != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(shard, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_GRAPH_SHARD);
    ASSERT(decoded->as.graph_shard.node_count == 1);
    ASSERT(decoded->as.graph_shard.edge_count == 1);
    ASSERT(decoded->as.graph_shard.meta_count == 1);

    cowrie_free(shard);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Fixture Tests (Core)
 * ============================================================ */

static int test_fixtures_core(void) {
    char root[1024];
    if (!build_repo_root(root, sizeof(root))) return 0;

    const char *cases[][2] = {
        {"core/null.cowrie", "core/null.json"},
        {"core/true.cowrie", "core/true.json"},
        {"core/int.cowrie", "core/int.json"},
        {"core/float.cowrie", "core/float.json"},
        {"core/string.cowrie", "core/string.json"},
        {"core/array.cowrie", "core/array.json"},
        {"core/object.cowrie", "core/object.json"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input_path[1200];
        char json_path[1200];
        snprintf(input_path, sizeof(input_path), "%s/testdata/fixtures/%s", root, cases[i][0]);
        snprintf(json_path, sizeof(json_path), "%s/testdata/fixtures/%s", root, cases[i][1]);

        char *input_data = NULL;
        size_t input_len = 0;
        if (!read_file(input_path, &input_data, &input_len)) return 0;

        COWRIEValue *decoded = NULL;
        if (cowrie_decode((const uint8_t *)input_data, input_len, &decoded) != 0) {
            free(input_data);
            return 0;
        }
        free(input_data);

        COWRIEBuf actual_json;
        cowrie_buf_init(&actual_json);
        if (cowrie_to_json(decoded, &actual_json) != 0) {
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }

        char *expected_json = NULL;
        size_t expected_len = 0;
        if (!read_file(json_path, &expected_json, &expected_len)) {
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }

        COWRIEValue *expected_val = NULL;
        if (cowrie_from_json(expected_json, expected_len, &expected_val) != 0) {
            free(expected_json);
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }
        free(expected_json);

        COWRIEBuf expected_canon;
        cowrie_buf_init(&expected_canon);
        if (cowrie_to_json(expected_val, &expected_canon) != 0) {
            cowrie_free(expected_val);
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            cowrie_buf_free(&expected_canon);
            return 0;
        }

        int ok = (actual_json.len == expected_canon.len) &&
                 (memcmp(actual_json.data, expected_canon.data, actual_json.len) == 0);

        cowrie_free(expected_val);
        cowrie_free(decoded);
        cowrie_buf_free(&actual_json);
        cowrie_buf_free(&expected_canon);

        if (!ok) return 0;
    }

    return 1;
}

/* ============================================================
 * Overflow Protection Tests
 * ============================================================ */

/* Helper: encode a uint64 as a protobuf-style uvarint into buf, return bytes written */
static size_t encode_uvarint(uint8_t *buf, uint64_t v) {
    size_t i = 0;
    while (v >= 0x80) {
        buf[i++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    buf[i++] = (uint8_t)v;
    return i;
}

/* Test: adjlist with node_count that would overflow (node_count+1)*sizeof(size_t) */
static int test_oversized_adjlist_rejects(void) {
    /* Build: header(4) + dict_count(1) + tag(1) + id_width(1) + node_count(varint) + edge_count(varint)
     * Set node_count to SIZE_MAX so (node_count+1) wraps to 0 */
    uint8_t payload[30];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x30; /* SJT_ADJLIST */
    payload[pos++] = 0x00; /* id_width = INT32 */
    /* node_count = 0xFFFFFFFFFFFFFFFF (max uint64) */
    pos += encode_uvarint(payload + pos, UINT64_MAX);
    /* edge_count = 0 */
    payload[pos++] = 0x00;

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* Test: richtext with huge token_count that would overflow token_count*sizeof(int32_t) */
static int test_oversized_richtext_tokens_rejects(void) {
    uint8_t payload[30];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x31; /* SJT_RICH_TEXT */
    payload[pos++] = 0x00; /* text_len = 0 */
    payload[pos++] = 0x01; /* flags: has tokens */
    /* token_count = 0x4000000000000000 — would overflow when * 4 */
    pos += encode_uvarint(payload + pos, (uint64_t)0x4000000000000000ULL);

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* Test: array with count exceeding remaining input */
static int test_oversized_array_rejects(void) {
    uint8_t payload[20];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x06; /* SJT_ARRAY */
    /* count = 999999999 but only 0 bytes of data follow */
    pos += encode_uvarint(payload + pos, 999999999ULL);

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("COWRIE Gen2 Test Suite\n");
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

    printf("\nOverflow Protection:\n");
    TEST(oversized_adjlist_rejects);
    TEST(oversized_richtext_tokens_rejects);
    TEST(oversized_array_rejects);

    printf("\n=====================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
