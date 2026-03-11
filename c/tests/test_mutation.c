/*
 * COWRIE Mutation Test Suite
 *
 * Loads core fixtures and applies three mutation strategies:
 * 1. Truncation at every byte offset
 * 2. Byte flip (XOR 0xFF) at every position
 * 3. Random corruption of header bytes (all 256 values)
 *
 * The test passes if no crashes occur. Decode errors are expected and fine.
 */

#include "../include/cowrie_gen2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
 * Fixture loading
 * ============================================================ */

typedef struct {
    const char *name;
    uint8_t *data;
    size_t len;
} Fixture;

static Fixture fixtures[7];
static int fixture_count = 0;

static const char *fixture_names[] = {
    "null", "true", "int", "float", "string", "array", "object"
};

static int load_fixtures(void) {
    char root[1024];
    if (!build_repo_root(root, sizeof(root))) {
        printf("SKIP: cannot determine repo root from __FILE__\n");
        return 0;
    }

    for (int i = 0; i < 7; i++) {
        char path[2048];
        snprintf(path, sizeof(path), "%s/testdata/fixtures/core/%s.cowrie",
                 root, fixture_names[i]);

        char *data = NULL;
        size_t len = 0;
        if (!read_file(path, &data, &len)) {
            printf("SKIP: cannot read %s\n", path);
            return 0;
        }
        fixtures[i].name = fixture_names[i];
        fixtures[i].data = (uint8_t *)data;
        fixtures[i].len = len;
        fixture_count++;
    }
    return 1;
}

static void free_fixtures(void) {
    for (int i = 0; i < fixture_count; i++) {
        free(fixtures[i].data);
        fixtures[i].data = NULL;
    }
    fixture_count = 0;
}

/* ============================================================
 * Strategy 1: Truncation
 *
 * For each fixture, try decoding at every byte offset 0..len-1.
 * Must not crash. Errors are expected.
 * ============================================================ */

static int test_truncation(void) {
    for (int f = 0; f < fixture_count; f++) {
        const Fixture *fix = &fixtures[f];
        for (size_t cut = 0; cut < fix->len; cut++) {
            COWRIEValue *decoded = NULL;
            int rc = cowrie_decode(fix->data, cut, &decoded);
            if (rc == 0) {
                cowrie_free(decoded);
            }
        }
    }
    return 1;
}

/* ============================================================
 * Strategy 2: Byte flip (XOR 0xFF)
 *
 * For each byte position, flip it, decode, restore.
 * Must not crash.
 * ============================================================ */

static int test_byte_flip(void) {
    for (int f = 0; f < fixture_count; f++) {
        Fixture *fix = &fixtures[f];
        for (size_t pos = 0; pos < fix->len; pos++) {
            fix->data[pos] ^= 0xFF;

            COWRIEValue *decoded = NULL;
            int rc = cowrie_decode(fix->data, fix->len, &decoded);
            if (rc == 0) {
                cowrie_free(decoded);
            }

            fix->data[pos] ^= 0xFF; /* restore */
        }
    }
    return 1;
}

/* ============================================================
 * Strategy 3: Random corruption of header bytes
 *
 * For the first 4 header bytes, try all 256 values.
 * Must not crash.
 * ============================================================ */

static int test_header_corruption(void) {
    for (int f = 0; f < fixture_count; f++) {
        Fixture *fix = &fixtures[f];
        size_t header_bytes = fix->len < 4 ? fix->len : 4;

        for (size_t pos = 0; pos < header_bytes; pos++) {
            uint8_t original = fix->data[pos];

            for (int val = 0; val < 256; val++) {
                fix->data[pos] = (uint8_t)val;

                COWRIEValue *decoded = NULL;
                int rc = cowrie_decode(fix->data, fix->len, &decoded);
                if (rc == 0) {
                    cowrie_free(decoded);
                }
            }

            fix->data[pos] = original; /* restore */
        }
    }
    return 1;
}

/* ============================================================
 * main
 * ============================================================ */

int main(void) {
    printf("=== Cowrie Mutation Tests ===\n");

    if (!load_fixtures()) {
        printf("Failed to load fixtures, skipping mutation tests.\n");
        free_fixtures();
        return 1;
    }

    printf("Loaded %d fixtures\n", fixture_count);

    TEST(truncation);
    TEST(byte_flip);
    TEST(header_corruption);

    free_fixtures();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
