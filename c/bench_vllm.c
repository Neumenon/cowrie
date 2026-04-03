/*
 * vLLM Payload Shape Benchmark — C Cowrie encoder
 *
 * Compares Cowrie C encode/decode speed and sizes for real vLLM payload shapes.
 * This is the "speed of light" — what a Python cffi binding would achieve.
 *
 * Build:
 *   cd cowrie/c && mkdir -p build && cd build && cmake .. && make
 *   gcc -O2 -I../include -o bench_vllm ../bench_vllm.c ../src/gen2.c -lm
 *
 * Or standalone:
 *   gcc -O2 -Iinclude -o bench_vllm bench_vllm.c src/gen2.c -lm
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include "cowrie_gen2.h"

/* ── Timing ── */

static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

typedef struct {
    double enc_us;
    double dec_us;
    size_t enc_bytes;
} BenchResult;

/* ── Payload builders ── */

static COWRIEValue *make_sampling_params(void) {
    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "n", 1, cowrie_new_int64(1));
    cowrie_object_set(obj, "best_of", 7, cowrie_new_int64(1));
    cowrie_object_set(obj, "presence_penalty", 16, cowrie_new_float64(0.0));
    cowrie_object_set(obj, "frequency_penalty", 17, cowrie_new_float64(0.0));
    cowrie_object_set(obj, "repetition_penalty", 18, cowrie_new_float64(1.0));
    cowrie_object_set(obj, "temperature", 11, cowrie_new_float64(0.7));
    cowrie_object_set(obj, "top_p", 5, cowrie_new_float64(0.95));
    cowrie_object_set(obj, "top_k", 5, cowrie_new_int64(50));
    cowrie_object_set(obj, "min_p", 5, cowrie_new_float64(0.0));
    cowrie_object_set(obj, "use_beam_search", 15, cowrie_new_bool(0));
    cowrie_object_set(obj, "length_penalty", 14, cowrie_new_float64(1.0));
    cowrie_object_set(obj, "early_stopping", 14, cowrie_new_bool(0));
    cowrie_object_set(obj, "stop", 4, cowrie_new_array());
    cowrie_object_set(obj, "stop_token_ids", 14, cowrie_new_array());
    cowrie_object_set(obj, "max_tokens", 10, cowrie_new_int64(2048));
    cowrie_object_set(obj, "min_tokens", 10, cowrie_new_int64(0));
    cowrie_object_set(obj, "skip_special_tokens", 19, cowrie_new_bool(1));
    cowrie_object_set(obj, "spaces_between_special_tokens", 29, cowrie_new_bool(1));
    return obj;
}

static COWRIEValue *make_embedding(int dims) {
    /* Generate deterministic float32 data */
    uint8_t *data = (uint8_t *)malloc(dims * 4);
    srand(42);
    for (int i = 0; i < dims; i++) {
        float v = (float)rand() / (float)RAND_MAX * 2.0f - 1.0f;
        memcpy(data + i * 4, &v, 4);
    }
    size_t shape[1] = { (size_t)dims };

    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "object", 6, cowrie_new_string("embedding", 9));
    cowrie_object_set(obj, "index", 5, cowrie_new_int64(0));
    cowrie_object_set(obj, "embedding", 9,
        cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 1, shape, data, dims * 4));
    free(data);
    return obj;
}

static COWRIEValue *make_image_tensor(void) {
    /* 3×336×336 uint8 — real VLM preprocessed image shape */
    size_t total = 3 * 336 * 336;
    uint8_t *data = (uint8_t *)malloc(total);
    srand(42);
    for (size_t i = 0; i < total; i++) {
        data[i] = (uint8_t)(rand() % 256);
    }
    size_t shape[3] = { 3, 336, 336 };

    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "prompt", 6, cowrie_new_string("Describe this image.", 20));
    cowrie_object_set(obj, "model", 5, cowrie_new_string("Qwen2.5-VL-7B", 14));
    cowrie_object_set(obj, "image", 5,
        cowrie_new_tensor(COWRIE_DTYPE_UINT8, 3, shape, data, total));
    free(data);
    return obj;
}

static COWRIEValue *make_kv_cache(void) {
    /* 32 layers × 8 heads × 128 dim × 64 tokens = 2M float32s = 8MB */
    size_t count = 32 * 8 * 128 * 64;
    size_t total = count * 4;
    uint8_t *data = (uint8_t *)malloc(total);
    srand(42);
    for (size_t i = 0; i < total; i++) {
        data[i] = (uint8_t)(rand() % 256);
    }
    size_t shape[4] = { 32, 8, 128, 64 };

    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "layer_start", 11, cowrie_new_int64(0));
    cowrie_object_set(obj, "layer_end", 9, cowrie_new_int64(32));
    cowrie_object_set(obj, "seq_id", 6, cowrie_new_int64(42));
    cowrie_object_set(obj, "kv_cache", 8,
        cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 4, shape, data, total));
    free(data);
    return obj;
}

static COWRIEValue *make_embedding_1536(void) { return make_embedding(1536); }
static COWRIEValue *make_embedding_3072(void) { return make_embedding(3072); }

/* ── Benchmark runner ── */

static BenchResult bench_payload(const char *name, COWRIEValue *(*builder)(void), int iters) {
    BenchResult result = {0};

    /* Build once for size measurement */
    COWRIEValue *v = builder();
    COWRIEBuf buf = {0};
    cowrie_encode(v, &buf);
    result.enc_bytes = buf.len;
    free(buf.data);
    buf = (COWRIEBuf){0};
    cowrie_free(v);

    /* Encode benchmark */
    double t0 = now_us();
    for (int i = 0; i < iters; i++) {
        v = builder();
        buf = (COWRIEBuf){0};
        cowrie_encode(v, &buf);
        free(buf.data);
        cowrie_free(v);
    }
    double t1 = now_us();
    result.enc_us = (t1 - t0) / iters;

    /* Prepare data for decode benchmark */
    v = builder();
    buf = (COWRIEBuf){0};
    cowrie_encode(v, &buf);
    cowrie_free(v);

    /* Decode benchmark */
    t0 = now_us();
    for (int i = 0; i < iters; i++) {
        COWRIEValue *decoded = NULL;
        cowrie_decode(buf.data, buf.len, &decoded);
        cowrie_free(decoded);
    }
    t1 = now_us();
    result.dec_us = (t1 - t0) / iters;

    free(buf.data);
    return result;
}

/* ── Tensor-only encode/decode (pure tensor, no object wrapper) ── */

static BenchResult bench_tensor_only(const char *name, int dims, int iters) {
    BenchResult result = {0};
    size_t total = (size_t)dims * 4;
    uint8_t *data = (uint8_t *)malloc(total);
    srand(42);
    for (size_t i = 0; i < total; i++) data[i] = (uint8_t)(rand() % 256);
    size_t shape[1] = { (size_t)dims };

    /* Size */
    COWRIEValue *v = cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 1, shape, data, total);
    COWRIEBuf buf = {0};
    cowrie_encode(v, &buf);
    result.enc_bytes = buf.len;
    free(buf.data);
    cowrie_free(v);

    /* Encode */
    double t0 = now_us();
    for (int i = 0; i < iters; i++) {
        v = cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 1, shape, data, total);
        buf = (COWRIEBuf){0};
        cowrie_encode(v, &buf);
        free(buf.data);
        cowrie_free(v);
    }
    double t1 = now_us();
    result.enc_us = (t1 - t0) / iters;

    /* Decode */
    v = cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 1, shape, data, total);
    buf = (COWRIEBuf){0};
    cowrie_encode(v, &buf);
    cowrie_free(v);

    t0 = now_us();
    for (int i = 0; i < iters; i++) {
        COWRIEValue *decoded = NULL;
        cowrie_decode(buf.data, buf.len, &decoded);
        /* Zero-copy view test */
        if (decoded && decoded->type == COWRIE_TENSOR) {
            size_t count = 0;
            const float *view = cowrie_tensor_view_float32(&decoded->as.tensor, &count);
            (void)view; /* Use the view to prevent optimization */
        }
        cowrie_free(decoded);
    }
    t1 = now_us();
    result.dec_us = (t1 - t0) / iters;

    free(buf.data);
    free(data);
    return result;
}

/* ── Main ── */

int main(void) {
    printf("========================================================================\n");
    printf("  Cowrie C Encoder — vLLM Payload Shapes\n");
    printf("  This is the 'speed of light' for a Python cffi binding\n");
    printf("========================================================================\n\n");

    printf("%-45s %10s %12s %12s\n", "Payload", "Size", "Encode", "Decode");
    printf("%-45s %10s %12s %12s\n",
           "---------------------------------------------", "----------", "------------", "------------");

    /* SamplingParams */
    BenchResult r = bench_payload("SamplingParams", make_sampling_params, 10000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "SamplingParams (metadata)", r.enc_bytes, r.enc_us, r.dec_us);

    /* Embedding 1536 (in object wrapper) */
    r = bench_payload("Embed 1536", make_embedding_1536, 5000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Embed 1536-dim (in object)", r.enc_bytes, r.enc_us, r.dec_us);

    /* Embedding 3072 (in object wrapper) */
    r = bench_payload("Embed 3072", make_embedding_3072, 5000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Embed 3072-dim (in object)", r.enc_bytes, r.enc_us, r.dec_us);

    /* Image tensor */
    r = bench_payload("Image", make_image_tensor, 1000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "VLM image (3x336x336 uint8, in object)", r.enc_bytes, r.enc_us, r.dec_us);

    /* KV cache */
    r = bench_payload("KV cache", make_kv_cache, 100);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "KV cache (32x8x128x64 float32, in object)", r.enc_bytes, r.enc_us, r.dec_us);

    printf("\n--- Pure tensor encode/decode (no object wrapper) ---\n\n");

    /* Pure tensor benchmarks */
    r = bench_tensor_only("Tensor 768", 768, 10000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Tensor 768-dim float32 (raw)", r.enc_bytes, r.enc_us, r.dec_us);

    r = bench_tensor_only("Tensor 1536", 1536, 10000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Tensor 1536-dim float32 (raw)", r.enc_bytes, r.enc_us, r.dec_us);

    r = bench_tensor_only("Tensor 3072", 3072, 5000);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Tensor 3072-dim float32 (raw)", r.enc_bytes, r.enc_us, r.dec_us);

    int big = 32 * 8 * 128 * 64; /* 2M floats = 8MB */
    r = bench_tensor_only("Tensor 8MB", big, 100);
    printf("%-45s %8zu B %9.1f us %9.1f us\n",
           "Tensor 8MB float32 (KV cache raw)", r.enc_bytes, r.enc_us, r.dec_us);

    printf("\n--- Direct encode (Go-style, zero malloc) ---\n\n");

    /* Direct tensor encode — no value tree */
    {
        int dims_list[] = {768, 1536, 3072};
        const char *names[] = {"768-dim", "1536-dim", "3072-dim"};
        for (int d = 0; d < 3; d++) {
            int ndims = dims_list[d];
            size_t total = (size_t)ndims * 4;
            uint8_t *tdata = (uint8_t *)malloc(total);
            srand(42);
            for (size_t i = 0; i < total; i++) tdata[i] = (uint8_t)(rand() % 256);
            size_t shape[1] = { (size_t)ndims };
            int iters = 50000;

            /* Direct tensor (no object) */
            COWRIEBuf dbuf = {0};
            cowrie_buf_init(&dbuf);
            cowrie_direct_encode_tensor(&dbuf, COWRIE_DTYPE_FLOAT32, 1, shape, tdata, total);
            size_t direct_size = dbuf.len;
            free(dbuf.data);

            double t0 = now_us();
            for (int i = 0; i < iters; i++) {
                COWRIEBuf b = {0};
                cowrie_buf_init(&b);
                cowrie_direct_encode_tensor(&b, COWRIE_DTYPE_FLOAT32, 1, shape, tdata, total);
                free(b.data);
            }
            double t1 = now_us();
            double enc_direct = (t1 - t0) / iters;

            /* Direct tensor field (in object) */
            COWRIEBuf fbuf = {0};
            cowrie_buf_init(&fbuf);
            cowrie_direct_encode_tensor_field(&fbuf, "embedding", 9,
                COWRIE_DTYPE_FLOAT32, 1, shape, tdata, total);
            size_t field_size = fbuf.len;
            free(fbuf.data);

            t0 = now_us();
            for (int i = 0; i < iters; i++) {
                COWRIEBuf b = {0};
                cowrie_buf_init(&b);
                cowrie_direct_encode_tensor_field(&b, "embedding", 9,
                    COWRIE_DTYPE_FLOAT32, 1, shape, tdata, total);
                free(b.data);
            }
            t1 = now_us();
            double enc_field = (t1 - t0) / iters;

            char label1[80], label2[80];
            snprintf(label1, sizeof(label1), "Direct tensor %s (raw)", names[d]);
            snprintf(label2, sizeof(label2), "Direct tensor_field %s (object)", names[d]);
            printf("%-45s %8zu B %9.2f us\n", label1, direct_size, enc_direct);
            printf("%-45s %8zu B %9.2f us\n", label2, field_size, enc_field);
            free(tdata);
        }

        /* 8MB KV cache direct */
        {
            int ndims = 32 * 8 * 128 * 64;
            size_t total = (size_t)ndims * 4;
            uint8_t *tdata = (uint8_t *)malloc(total);
            srand(42);
            for (size_t i = 0; i < total; i++) tdata[i] = (uint8_t)(rand() % 256);
            size_t shape[4] = {32, 8, 128, 64};

            COWRIEBuf dbuf = {0};
            cowrie_buf_init(&dbuf);
            cowrie_direct_encode_tensor(&dbuf, COWRIE_DTYPE_FLOAT32, 4, shape, tdata, total);
            size_t direct_size = dbuf.len;
            free(dbuf.data);

            double t0 = now_us();
            for (int i = 0; i < 200; i++) {
                COWRIEBuf b = {0};
                cowrie_buf_init(&b);
                cowrie_direct_encode_tensor(&b, COWRIE_DTYPE_FLOAT32, 4, shape, tdata, total);
                free(b.data);
            }
            double t1 = now_us();
            printf("%-45s %8zu B %9.1f us\n", "Direct tensor 8MB (KV cache)", direct_size, (t1 - t0) / 200);
            free(tdata);
        }
    }

    printf("\n");
    printf("  Notes:\n");
    printf("  - Encode includes value construction + serialization + free\n");
    printf("  - Decode includes deserialization + zero-copy view access + free\n");
    printf("  - This is what Python cffi/ctypes binding would achieve\n");
    printf("  - Compare to: Python pure = 10-40x slower, Python msgpack(C) = ~1us for 6KB\n");
    printf("  - refs: vLLM issue #6241, PR #12918, PR #16860\n\n");

    return 0;
}
