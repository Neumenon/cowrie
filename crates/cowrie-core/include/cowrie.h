#ifndef COWRIE_H
#define COWRIE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Out-of-band authority & causal taint bitmask (Plane D) */
#define COWRIE_AUTH_SYS_POLICY     0x01
#define COWRIE_AUTH_VERIFIED_TOOL  0x02
#define COWRIE_AUTH_UNTRUSTED_EXT  0x04
#define COWRIE_AUTH_MODEL_SYNTH    0x08
#define COWRIE_BIT_TAINTED         0x80

/* Epistemic classes */
#define COWRIE_EPI_OBSERVED        0x01
#define COWRIE_EPI_DERIVED         0x02
#define COWRIE_EPI_ESTIMATED       0x03

/* StatePage KV quantization / SSM dtype tags */
#define COWRIE_KV_FP16             0
#define COWRIE_KV_Q4_0             1
#define COWRIE_KV_Q8_0             2
#define COWRIE_KV_F32              3
#define COWRIE_SSM_FP16            0
#define COWRIE_SSM_BF16            1
#define COWRIE_SSM_FP32            2

/* Plane B lens tiers & Plane D action codes (`COWRIE_DECLASSIFY_V1`) */
#define COWRIE_LENS_R0             0
#define COWRIE_LENS_R1             1
#define COWRIE_LENS_R2             2
#define COWRIE_LENS_R3             3
#define COWRIE_ACT_READ_ONLY       0
#define COWRIE_ACT_SHELL_EXEC      1
#define COWRIE_ACT_CRED_READ       2
#define COWRIE_ACT_FS_WRITE        3
#define COWRIE_ACT_NET_EXFIL       4

typedef struct CowrieBuf {
    uint8_t* data;
    size_t   len;
    size_t   cap;
} CowrieBuf;

typedef struct CowrieStatePageHandle CowrieStatePageHandle;
typedef struct CowrieIvfHandle       CowrieIvfHandle;
typedef struct CowrieWalHandle       CowrieWalHandle;
typedef struct CowrieZonedHandle     CowrieZonedHandle;

typedef struct CowrieIvfHit {
    uint32_t orig_id;
    float    score;
    float    conf;
    uint8_t  auth;
} CowrieIvfHit;

uint8_t*    cowrie_wasm_alloc(size_t len);
void        cowrie_wasm_free(uint8_t* data, size_t len);
void        cowrie_buf_free(CowrieBuf buf);
const char* cowrie_err_name(int code);

/* Plane A: Canonical COWR frames (cowrie_frame_verify is zero-allocation) */
int cowrie_frame_verify(const uint8_t* data, size_t len, uint8_t* out_auth, uint8_t* out_epi, uint8_t* out_cid_34);
int cowrie_encode_json(const uint8_t* json_utf8, size_t json_len, uint8_t authority, uint8_t epistemic, CowrieBuf* out_buf);
int cowrie_decode_to_json(const uint8_t* data, size_t len, CowrieBuf* out_buf);

/* Plane B: Deterministic progressive lenses (tier: 0=R0, 1=R1, 2=R2, 3=R3 lossless; >3 -> ERR_INVALID_CONTRACT) */
int cowrie_project_lens(const uint8_t* frame_data, size_t frame_len, uint8_t tier, size_t tensor_count, CowrieBuf* out_buf);
int cowrie_project_r2_compact(const uint8_t* frame_data, size_t frame_len, CowrieBuf* out_buf);
int cowrie_extract_field(const uint8_t* frame_data, size_t frame_len, const uint8_t* path_utf8, size_t path_len, CowrieBuf* out_buf);

/* Plane C/D: 16 KB CWRF shard verification (Plane A + Plane C Merkle) & causal taint */
int     cowrie_shard_pack_single(const uint8_t* frame_data, size_t frame_len, CowrieBuf* out_buf);
int     cowrie_shard_verify(const uint8_t* shard_data, size_t shard_len, uint8_t* out_root_34);
int     cowrie_shard_resolved_frame_cid(const uint8_t* shard_data, size_t shard_len, size_t frame_idx, uint8_t* out_cid_34);
int     cowrie_statepage_verify_bytes(const uint8_t* data, size_t len, const uint8_t* hmac_key, size_t hmac_len, uint8_t verify_hash, uint32_t* out_ctx_tokens);
uint8_t cowrie_propagate_authority(uint8_t base_auth, const uint8_t* parent_auths, size_t num_parents);
uint8_t cowrie_is_tainted(uint8_t authority);
int     cowrie_sha256_hex(const uint8_t* data, size_t len, char* out_hex_65);

/* Unified sub-ms strided model fingerprinter (file_size + 64 x 64 KiB windows across full weights) */
int     cowrie_model_fingerprint(const char* path_cstr, char* out_hex_65);

/* Plane C Profile A: 16 KB-aligned Hybrid SSM+KV StatePages (atomic O_EXCL 0600 + zero-copy mmap) */
int cowrie_statepage_save(
    const char*    path_cstr,
    const char*    model_sha_cstr,
    const char*    doc_cid_cstr,
    uint32_t       ctx_tokens,
    uint8_t        kv_quant,
    uint8_t        ssm_dtype,
    const uint8_t* state_data,
    size_t         state_len
);

int cowrie_statepage_save_ext(
    const char*    path_cstr,
    const char*    model_sha_cstr,
    const char*    doc_cid_cstr,
    const char*    runtime_id_cstr,
    uint16_t       layout_ver,
    uint32_t       ctx_tokens,
    uint8_t        kv_quant,
    uint8_t        ssm_dtype,
    const uint8_t* state_data,
    size_t         state_len
);

int cowrie_statepage_open(
    const char*             path_cstr,
    const char*             expected_model_sha_cstr,
    uint8_t                 verify_hash,
    CowrieStatePageHandle** out_handle
);

/* Open StatePage enforcing both model_sha AND kv_quant (-1 to accept any kv_quant). */
int cowrie_statepage_open_checked(
    const char*             path_cstr,
    const char*             expected_model_sha_cstr,
    int                     expected_kv_quant,
    uint8_t                 verify_hash,
    CowrieStatePageHandle** out_handle
);

/* Open StatePage enforcing model_sha, runtime_id, kv_quant (-1=any), ssm_dtype (-1=any), ownership, and HMAC/Merkle. */
int cowrie_statepage_open_full(
    const char*             path_cstr,
    const char*             expected_model_sha_cstr,
    const char*             expected_runtime_id_cstr,
    int                     expected_kv_quant,
    int                     expected_ssm_dtype,
    uint8_t                 verify_hash,
    CowrieStatePageHandle** out_handle
);

/* Open StatePage enforcing model_sha, doc_cid, runtime_id, kv_quant (-1=any), ssm_dtype (-1=any), ownership, and HMAC/Merkle. */
int cowrie_statepage_open_strict(
    const char*             path_cstr,
    const char*             expected_model_sha_cstr,
    const char*             expected_doc_cid_cstr,
    const char*             expected_runtime_id_cstr,
    int                     expected_kv_quant,
    int                     expected_ssm_dtype,
    uint8_t                 verify_hash,
    CowrieStatePageHandle** out_handle
);

const uint8_t* cowrie_statepage_data(const CowrieStatePageHandle* handle);
size_t         cowrie_statepage_len(const CowrieStatePageHandle* handle);
uint32_t       cowrie_statepage_ctx_tokens(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_kv_quant(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_ssm_dtype(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_hmac_verified(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_hash_mode(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_model_sha(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_doc_cid(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_state_sha(const CowrieStatePageHandle* handle);
void           cowrie_statepage_close(CowrieStatePageHandle* handle);
uint8_t        cowrie_has_shani(void);

/* Plane C Profile B: Co-located 16 KB IVF-INT8 (`CWIV`) out-of-core SIMD search */
int cowrie_ivf_open(const char* path_cstr, CowrieIvfHandle** out_handle);
int cowrie_ivf_search_trusted(
    const CowrieIvfHandle* handle,
    const int8_t*          query_i8,
    size_t                 query_dim,
    size_t                 nprobe,
    size_t                 top_k,
    float                  conf_thresh,
    CowrieIvfHit*          out_hits,
    size_t                 max_hits,
    size_t*                out_hit_count,
    size_t*                out_bytes_read
);
void cowrie_ivf_close(CowrieIvfHandle* handle);

/* Plane D: RFC 8032 Ed25519 owner-declassification signatures (`COWRIE_DECLASSIFY_V1`) */
int cowrie_ed25519_pubkey(const uint8_t* seed_32, uint8_t* out_pubkey_32);
int cowrie_declassify_sign(
    const uint8_t* seed_32,
    const uint8_t* target_cid_34,
    uint8_t        action_code,
    uint64_t       expiry_unix_secs,
    uint8_t*       out_sig_64
);
int cowrie_declassify_verify(
    const uint8_t* pubkey_32,
    const uint8_t* target_cid_34,
    uint8_t        action_code,
    uint64_t       expiry_unix_secs,
    const uint8_t* sig_64,
    uint64_t       now_unix_secs
);

/* Streaming Hash-Chained Framed WAL (`.cwwal`) & Compaction */
int    cowrie_wal_open(const char* path_cstr, uint8_t recover_torn_tail, CowrieWalHandle** out_handle);
int    cowrie_wal_append_frame(CowrieWalHandle* handle, const uint8_t* frame_data, size_t frame_len, uint8_t fsync_now, uint64_t* out_seq, uint8_t* out_cid_34);
int    cowrie_wal_append_tombstone(CowrieWalHandle* handle, const uint8_t* cid_34, uint8_t fsync_now, uint64_t* out_seq);
size_t cowrie_wal_len(const CowrieWalHandle* handle);
size_t cowrie_wal_live_len(const CowrieWalHandle* handle);
int    cowrie_wal_live_frame_at(const CowrieWalHandle* handle, size_t idx, CowrieBuf* out_buf);
int    cowrie_wal_compact_shard(const CowrieWalHandle* handle, const char* out_shard_path_cstr, uint8_t* out_root_34);
void   cowrie_wal_close(CowrieWalHandle* handle);

/* Plane D: 16 KB Page-Targeted Zone-Map Shards (`CWZM`) */
typedef struct {
    int64_t        primary_key;
    int64_t        secondary_val;
    const uint8_t* frame_data;
    size_t         frame_len;
} CowrieZonedInput;

int  cowrie_zoned_write(const char* path_cstr, const CowrieZonedInput* inputs, size_t count, uint64_t* out_total_bytes, uint32_t* out_page_count, uint8_t* out_root_34);
int  cowrie_zoned_open(const char* path_cstr, CowrieZonedHandle** out_handle);
int  cowrie_zoned_query_keys(const CowrieZonedHandle* handle, int64_t lo, int64_t hi, uint8_t trusted_only, uint8_t verify_merkle, size_t* out_hit_count, size_t* out_bytes_read);
int  cowrie_zoned_query_secondary(const CowrieZonedHandle* handle, int64_t lo, int64_t hi, uint8_t trusted_only, uint8_t verify_merkle, size_t* out_hit_count, size_t* out_bytes_read);
void cowrie_zoned_close(CowrieZonedHandle* handle);

#ifdef __cplusplus
}
#endif

#endif /* COWRIE_H */
