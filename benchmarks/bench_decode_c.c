/*
 * Cowrie Gen1 vs Gen2 Decode Benchmark (C)
 * Build: cd c/build && cmake .. && make && cd ../..
 * Run:   ./benchmarks/bench_decode_c
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "cowrie_gen1.h"
#include "cowrie_gen2.h"

static uint8_t *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path); exit(1); }
    fseek(f, 0, SEEK_END);
    *out_len = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *buf = malloc(*out_len);
    fread(buf, 1, *out_len, f);
    fclose(f);
    return buf;
}

static void bench_gen1(const char *label, const uint8_t *data, size_t len, int iterations) {
    /* warmup */
    for (int i = 0; i < (iterations < 100 ? iterations : 100); i++) {
        cowrie_g1_value_t *v = NULL;
        cowrie_g1_decode(data, len, &v);
        if (v) cowrie_g1_value_free(v);
    }

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations; i++) {
        cowrie_g1_value_t *v = NULL;
        cowrie_g1_decode(data, len, &v);
        if (v) cowrie_g1_value_free(v);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double ops = iterations / elapsed;
    double us = (elapsed / iterations) * 1e6;
    double mbps = (len * iterations / elapsed) / 1e6;

    printf("%-10s %7zuB %10.0f %10.1f %10.1f\n", label, len, ops, us, mbps);
}

static void bench_gen2(const char *label, const uint8_t *data, size_t len, int iterations) {
    /* warmup */
    for (int i = 0; i < (iterations < 100 ? iterations : 100); i++) {
        COWRIEValue *v = NULL;
        cowrie_decode(data, len, &v);
        if (v) cowrie_free(v);
    }

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations; i++) {
        COWRIEValue *v = NULL;
        cowrie_decode(data, len, &v);
        if (v) cowrie_free(v);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double ops = iterations / elapsed;
    double us = (elapsed / iterations) * 1e6;
    double mbps = (len * iterations / elapsed) / 1e6;

    printf("%-10s %7zuB %10.0f %10.1f %10.1f\n", label, len, ops, us, mbps);
}

int main(void) {
    printf("========================================================================\n");
    printf("Cowrie Decode Benchmark — C\n");
    printf("========================================================================\n");
    printf("%-10s %8s %10s %10s %10s\n", "Payload", "Size", "ops/s", "us/op", "MB/s");
    printf("------------------------------------------------------------------------\n");

    const char *names[] = {"small", "medium", "large", "floats"};
    int name_count = 4;

    for (int n = 0; n < name_count; n++) {
        char path1[256], path2[256];
        snprintf(path1, sizeof(path1), "benchmarks/fixtures/%s.gen1", names[n]);
        snprintf(path2, sizeof(path2), "benchmarks/fixtures/%s.gen2", names[n]);

        size_t len1, len2;
        uint8_t *g1 = read_file(path1, &len1);
        uint8_t *g2 = read_file(path2, &len2);

        int iters = (len1 < 1000) ? 500000 : 10000;

        char label1[32], label2[32];
        snprintf(label1, sizeof(label1), "%s/g1", names[n]);
        snprintf(label2, sizeof(label2), "%s/g2", names[n]);

        bench_gen1(label1, g1, len1, iters);
        bench_gen2(label2, g2, len2, iters);
        printf("\n");

        free(g1);
        free(g2);
    }

    return 0;
}
