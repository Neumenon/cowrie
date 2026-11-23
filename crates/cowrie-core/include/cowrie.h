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

typedef struct CowrieBuf {
    uint8_t* data;
    size_t   len;
    size_t   cap;
} CowrieBuf;

typedef struct CowrieStatePageHandle CowrieStatePageHandle;

void        cowrie_buf_free(CowrieBuf buf);
const char* cowrie_err_name(int code);

/* Plane A: Canonical COWR frames */
int cowrie_frame_verify(const uint8_t* data, size_t len, uint8_t* out_auth, uint8_t* out_epi, uint8_t* out_cid_34);
int cowrie_encode_json(const uint8_t* json_utf8, size_t json_len, uint8_t authority, uint8_t epistemic, CowrieBuf* out_buf);
int cowrie_decode_to_json(const uint8_t* data, size_t len, CowrieBuf* out_buf);

/* Plane B: Deterministic progressive lenses (tier: 0=R0, 1=R1, 2=R2) */
int cowrie_project_lens(const uint8_t* frame_data, size_t frame_len, uint8_t tier, size_t tensor_count, CowrieBuf* out_buf);

/* Plane C/D: 16 KB CWRF shard verification & causal taint */
int     cowrie_shard_verify(const uint8_t* shard_data, size_t shard_len, uint8_t* out_root_34);
uint8_t cowrie_propagate_authority(uint8_t base_auth, const uint8_t* parent_auths, size_t num_parents);
uint8_t cowrie_is_tainted(uint8_t authority);
int     cowrie_sha256_hex(const uint8_t* data, size_t len, char* out_hex_65);

/* Plane C: 16 KB-aligned Hybrid SSM+KV StatePages (atomic write + zero-copy verified mmap) */
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

const uint8_t* cowrie_statepage_data(const CowrieStatePageHandle* handle);
size_t         cowrie_statepage_len(const CowrieStatePageHandle* handle);
uint32_t       cowrie_statepage_ctx_tokens(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_kv_quant(const CowrieStatePageHandle* handle);
uint8_t        cowrie_statepage_hash_mode(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_model_sha(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_doc_cid(const CowrieStatePageHandle* handle);
const char*    cowrie_statepage_state_sha(const CowrieStatePageHandle* handle);
void           cowrie_statepage_close(CowrieStatePageHandle* handle);
uint8_t        cowrie_has_shani(void);

#ifdef __cplusplus
}
#endif

#endif /* COWRIE_H */
