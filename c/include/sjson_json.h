/*
 * SJSON JSON Bridge
 *
 * Provides conversion between JSON strings and SJSON values.
 */

#ifndef SJSON_JSON_H
#define SJSON_JSON_H

#include "sjson_gen2.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * JSON Bridge Functions
 * ============================================================ */

/*
 * Parse a JSON string into an SJSON value.
 * Returns 0 on success, -1 on error.
 * Caller must sjson_free(*out) when done.
 *
 * Special object handling:
 * - {"_type":"tensor", "dtype":"float32", "dims":[...], "data":"base64..."}
 * - {"_type":"bytes", "data":"base64..."}
 * - {"_type":"datetime", "nanos":123456789}
 * - {"_type":"uuid", "hex":"550e8400-e29b-41d4-a716-446655440000"}
 */
int sjson_from_json(const char *json, size_t len, SJSONValue **out);

/*
 * Convert an SJSON value to a JSON string.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 *
 * Extension types are encoded as special objects:
 * - Tensor: {"_type":"tensor", "dtype":"...", "dims":[...], "data":"base64"}
 * - Bytes: {"_type":"bytes", "data":"base64"}
 * - DateTime: {"_type":"datetime", "nanos":...}
 * - UUID: {"_type":"uuid", "hex":"..."}
 */
int sjson_to_json(const SJSONValue *value, SJSONBuf *buf);

/*
 * Convert an SJSON value to a pretty-printed JSON string.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int sjson_to_json_pretty(const SJSONValue *value, SJSONBuf *buf);

/* ============================================================
 * Base64 Utilities (used internally, exposed for convenience)
 * ============================================================ */

/*
 * Encode binary data to base64.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int sjson_base64_encode(const uint8_t *data, size_t len, SJSONBuf *buf);

/*
 * Decode base64 to binary data.
 * Returns 0 on success, -1 on error.
 * Caller must free buf->data when done.
 */
int sjson_base64_decode(const char *b64, size_t len, SJSONBuf *buf);

#ifdef __cplusplus
}
#endif

#endif /* SJSON_JSON_H */
