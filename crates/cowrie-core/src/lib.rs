//! Cowrie Core Library
//!
//! 16 KB-aligned, content-addressed, trust-tagged binary container for local AI runtimes.

pub mod canonical;
pub mod ffi;
pub mod ivf;
pub mod lens;
pub mod sha256;
pub mod shard;
pub mod trust;

use std::fmt;

/// Deterministic strict error ABI (`ERR_*` codes shared across Rust, C-ABI, and Python).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CowrieError {
    Truncated,
    BadMagic,
    UnsupportedVer,
    NonCanonicalFlags,
    NonCanonicalVarint,
    NonCanonicalInt,
    NonCanonicalUint,
    NonCanonicalFloat,
    DuplicateKey,
    UnsortedDict,
    UnsortedMapKeys,
    InvalidUtf8,
    BadDictIndex,
    UnusedDictKey,
    ReservedTag,
    TrailingBytes,
    MaxDepth,
    AllocLimit,
    UnalignedArena,
    NonZeroPad,
    CorruptTrailer,
    RootMismatch,
    ModelMismatch,
    StatePageHashMismatch,
    TaintedExecutionBlocked,
    InvalidJson,
    InvalidContract,
    IoError,
    KvQuantMismatch,
}

impl CowrieError {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Truncated => "ERR_TRUNCATED",
            Self::BadMagic => "ERR_BAD_MAGIC",
            Self::UnsupportedVer => "ERR_UNSUPPORTED_VER",
            Self::NonCanonicalFlags => "ERR_NON_CANONICAL_FLAGS",
            Self::NonCanonicalVarint => "ERR_NON_CANONICAL_VARINT",
            Self::NonCanonicalInt => "ERR_NON_CANONICAL_INT",
            Self::NonCanonicalUint => "ERR_NON_CANONICAL_UINT",
            Self::NonCanonicalFloat => "ERR_NON_CANONICAL_FLOAT",
            Self::DuplicateKey => "ERR_DUPLICATE_KEY",
            Self::UnsortedDict => "ERR_UNSORTED_DICT",
            Self::UnsortedMapKeys => "ERR_UNSORTED_MAP_KEYS",
            Self::InvalidUtf8 => "ERR_INVALID_UTF8",
            Self::BadDictIndex => "ERR_BAD_DICT_INDEX",
            Self::UnusedDictKey => "ERR_UNUSED_DICT_KEY",
            Self::ReservedTag => "ERR_RESERVED_TAG",
            Self::TrailingBytes => "ERR_TRAILING_BYTES",
            Self::MaxDepth => "ERR_MAX_DEPTH",
            Self::AllocLimit => "ERR_ALLOC_LIMIT",
            Self::UnalignedArena => "ERR_UNALIGNED_ARENA",
            Self::NonZeroPad => "ERR_NON_ZERO_PAD",
            Self::CorruptTrailer => "ERR_CORRUPT_TRAILER",
            Self::RootMismatch => "ERR_ROOT_MISMATCH",
            Self::ModelMismatch => "ERR_MODEL_MISMATCH",
            Self::StatePageHashMismatch => "ERR_STATEPAGE_HASH_MISMATCH",
            Self::TaintedExecutionBlocked => "ERR_TAINTED_EXECUTION_BLOCKED",
            Self::InvalidJson => "ERR_INVALID_JSON",
            Self::InvalidContract => "ERR_INVALID_CONTRACT",
            Self::IoError => "ERR_IO",
            Self::KvQuantMismatch => "ERR_KV_QUANT_MISMATCH",
        }
    }

    pub fn code(self) -> i32 {
        match self {
            Self::Truncated => 1,
            Self::BadMagic => 2,
            Self::UnsupportedVer => 3,
            Self::NonCanonicalFlags => 4,
            Self::NonCanonicalVarint => 5,
            Self::NonCanonicalInt => 6,
            Self::NonCanonicalUint => 7,
            Self::NonCanonicalFloat => 8,
            Self::DuplicateKey => 9,
            Self::UnsortedDict => 10,
            Self::UnsortedMapKeys => 11,
            Self::InvalidUtf8 => 12,
            Self::BadDictIndex => 13,
            Self::UnusedDictKey => 14,
            Self::ReservedTag => 15,
            Self::TrailingBytes => 16,
            Self::MaxDepth => 17,
            Self::AllocLimit => 18,
            Self::UnalignedArena => 19,
            Self::NonZeroPad => 20,
            Self::CorruptTrailer => 21,
            Self::RootMismatch => 22,
            Self::ModelMismatch => 23,
            Self::StatePageHashMismatch => 24,
            Self::TaintedExecutionBlocked => 25,
            Self::InvalidJson => 26,
            Self::InvalidContract => 27,
            Self::IoError => 28,
            Self::KvQuantMismatch => 29,
        }
    }
}

impl fmt::Display for CowrieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for CowrieError {}

#[cfg(test)]
mod tests {
    use super::*;
    use canonical::*;
    use lens::*;
    use sha256::*;
    use shard::*;
    use trust::*;

    #[test]
    fn bijective_roundtrip_and_key_order_invariance() {
        let a = parse_json_to_value(r#"{"z": 1, "a": [0, 127, 128, -1, -0.0, true, null], "m": {"k": "hello"}}"#).unwrap();
        let b = parse_json_to_value(r#"{"m": {"k": "hello"}, "a": [0, 127, 128, -1, 0.0, true, null], "z": 1}"#).unwrap();
        let wa = encode_frame(&a, AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        let wb = encode_frame(&b, AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        assert_eq!(wa, wb);
        assert_eq!(content_address(&wa), content_address(&wb));
        let dec = decode_frame_strict(&wa).unwrap();
        assert_eq!(encode_frame(&dec.value, dec.authority, dec.epistemic).unwrap(), wa);
    }

    #[test]
    fn rejects_dos_depth_and_alloc_bombs_and_all_truncations() {
        // Depth 65 array bomb
        let mut bomb = Vec::from(&b"COWR\x01\x02\x01\x00\x00"[..]);
        for _ in 0..66 {
            bomb.push(TAG_ARRAY);
            bomb.push(1);
        }
        bomb.push(TAG_NULL);
        assert_eq!(decode_frame_strict(&bomb).unwrap_err(), CowrieError::MaxDepth);

        // 1B element array OOM bomb
        let mut oom = Vec::from(&b"COWR\x01\x02\x01\x00\x00"[..]);
        oom.push(TAG_ARRAY);
        encode_uvarint(1_000_000_000, &mut oom);
        assert_eq!(decode_frame_strict(&oom).unwrap_err(), CowrieError::AllocLimit);

        // Exhaustive truncation check
        let v = parse_json_to_value(r#"{"title":"Cowrie","rows":[{"a":1,"b":2},{"a":3,"b":4},{"a":5,"b":6}]}"#).unwrap();
        let wire = encode_frame(&v, AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        for cut in 0..wire.len() {
            let err = decode_frame_strict(&wire[..cut]).unwrap_err();
            assert!(matches!(err, CowrieError::Truncated | CowrieError::AllocLimit));
        }
    }

    #[test]
    fn shard_16kb_merkle_localization_and_cve_2012_2459() {
        let f0 = encode_frame(&Value::Str("alpha".into()), AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        let f1 = encode_frame(&Value::Str("beta".into()), AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        let f2 = encode_frame(&Value::Str("gamma".into()), AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        let r_abc = merkle_root_rfc6962(&[&f0, &f1, &f2]);
        let r_abcc = merkle_root_rfc6962(&[&f0, &f1, &f2, &f2]);
        assert_ne!(r_abc, r_abcc);

        let tensor = vec![42u8; 4096];
        let mut shard = build_shard_16kb(&[&f0, &f1, &f2], &[&tensor], 0);
        let rep = verify_shard_and_localize(&shard).unwrap();
        assert!(rep.ok);
        assert_eq!(rep.header.plane_c_off as usize % PAGE_ALIGN, 0);

        // Flip 1 bit in frame 1 -> exact localization
        let f1_mid = rep.frames[1].offset as usize + (rep.frames[1].len as usize / 2);
        shard[f1_mid] ^= 0x01;
        let rep_bad = verify_shard_and_localize(&shard).unwrap();
        assert!(!rep_bad.ok);
        assert_eq!(rep_bad.bad_frames, vec![1]);
    }

    #[test]
    fn adaptive_r2_lens_and_causal_taint_membrane() {
        let v = parse_json_to_value(
            r#"{"title":"Agent Trace","rows":[{"id":1,"ms":10},{"id":2,"ms":12},{"id":3,"ms":9}],"sparse":[{"a":1},{"b":2},{"c":3}]}"#
        ).unwrap();
        let w = encode_frame(&v, AUTH_VERIFIED_TOOL, EPI_OBSERVED).unwrap();
        let dec = decode_frame_strict(&w).unwrap();
        let r0 = project_r0(&w, &dec, 0);
        let r2 = project_r2(&w, &dec);
        assert!(r0.starts_with('@') && r0.contains("Agent Trace"));
        assert!(r2.contains("rows[3]{id,ms}:")); // dense >= 3 hoisted
        assert!(r2.contains("sparse: [{\"a\":1},{\"b\":2},{\"c\":3}]")); // sparse fell back to JSON

        // Multi-hop taint laundering blocked
        let mut ledger = TrustLedger::new();
        let cid_ext = content_address(b"hostile-web-html");
        let cid_clean = content_address(b"trusted-config");
        let cid_sum = content_address(b"llm-summary-hop1");
        let cid_arg = content_address(b"tool-arg-hop2");
        let cid_clean_arg = content_address(b"clean-tool-arg");

        ledger.record(cid_ext, AUTH_UNTRUSTED_EXT, &[]);
        ledger.record(cid_clean, AUTH_VERIFIED_TOOL, &[]);
        let a1 = ledger.record(cid_sum, AUTH_MODEL_SYNTH, &[cid_ext, cid_clean]);
        assert!(is_tainted(a1));
        let a2 = ledger.record(cid_arg, AUTH_VERIFIED_TOOL, &[cid_sum]);
        assert!(is_tainted(a2));
        let ac = ledger.record(cid_clean_arg, AUTH_VERIFIED_TOOL, &[cid_clean]);
        assert!(!is_tainted(ac));

        assert_eq!(ledger.check_tool_call(ToolAction::ShellExec, &[cid_arg], false).unwrap_err(), CowrieError::TaintedExecutionBlocked);
        assert!(ledger.check_tool_call(ToolAction::ShellExec, &[cid_clean_arg], false).is_ok());
    }

    #[test]
    fn statepage_atomic_save_mmap_and_negative_controls() {
        let dir = std::env::temp_dir().join("cowrie_unit_statepage_test.cowrie");
        let model_a = sha256_hex(b"Qwen3.5-9B-Q4_K_M");
        let model_b = sha256_hex(b"Qwen3.5-0.8B-Q4_K_M");
        let doc_cid = sha256_hex(b"workspace-doc");
        let payload: Vec<u8> = (0..65537).map(|i| (i * 31) as u8).collect();

        save_statepage_atomic(&dir, &model_a, &doc_cid, 13800, 0, 0, &payload).unwrap();

        // Valid verified mmap
        let mp = MappedStatePage::open_verified(&dir, Some(&model_a), true).unwrap();
        assert_eq!(mp.meta.ctx_tokens, 13800);
        assert_eq!(mp.meta.state_offset as usize % PAGE_ALIGN, 0);
        assert_eq!(mp.state_slice(), payload.as_slice());
        drop(mp);

        // Wrong model_sha rejected
        assert_eq!(
            MappedStatePage::open_verified(&dir, Some(&model_b), true).err(),
            Some(CowrieError::ModelMismatch)
        );

        // Flip 1 bit on disk -> caught before restore
        let mut raw = std::fs::read(&dir).unwrap();
        raw[PAGE_ALIGN + 1234] ^= 0x01;
        std::fs::write(&dir, &raw).unwrap();
        assert_eq!(
            MappedStatePage::open_verified(&dir, Some(&model_a), true).err(),
            Some(CowrieError::StatePageHashMismatch)
        );

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn shani_matches_scalar_and_parallel_merkle_and_kv_quant_gate() {
        // Official NIST FIPS 180-4 test vectors
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );

        // Bit-for-bit parity between SHA-NI and pure scalar across boundary sizes + 1 MiB
        for &sz in &[0usize, 1, 55, 56, 63, 64, 65, 127, 128, 129, 4096, 1_048_576] {
            let buf: Vec<u8> = (0..sz).map(|i| ((i.wrapping_mul(131) ^ (i >> 3)) & 0xFF) as u8).collect();
            let fast = sha256(&buf);
            let mut sc = Sha256::new_scalar();
            sc.update(&buf);
            assert_eq!(fast, sc.finalize(), "SHA-NI vs scalar mismatch at size {}", sz);
        }

        // Parallel 4 MiB Merkle catches 1-bit flip in chunk 2 of a 9 MiB StatePage + enforces kv_quant
        let path = std::env::temp_dir().join("cowrie_shani_merkle_test.cowrie");
        let model_a = sha256_hex(b"Qwen3.5-9B");
        let doc_cid = sha256_hex(b"doc-9mb");
        let big: Vec<u8> = (0usize..(9 * 1024 * 1024 + 17)).map(|i| (i.wrapping_mul(29) & 0xFF) as u8).collect();
        save_statepage_atomic(&path, &model_a, &doc_cid, 13800, 1 /* q4_0 */, 0, &big).unwrap();

        // Matching kv_quant = 1 passes
        let mp = MappedStatePage::open_verified_checked(&path, Some(&model_a), Some(1), true).unwrap();
        assert_eq!(mp.meta.hash_mode, HASH_MODE_PARALLEL_MERKLE_4M);
        assert_eq!(mp.meta.kv_quant, 1);
        drop(mp);

        // Wrong kv_quant = 0 (FP16 context trying to load Q4_0 StatePage) rejected!
        assert_eq!(
            MappedStatePage::open_verified_checked(&path, Some(&model_a), Some(0), true).err(),
            Some(CowrieError::KvQuantMismatch)
        );

        // 1-bit flip inside chunk 2 (offset > 8 MiB) caught by parallel SHA-NI Merkle!
        let mut raw = std::fs::read(&path).unwrap();
        raw[PAGE_ALIGN + 8 * 1024 * 1024 + 77] ^= 0x01;
        std::fs::write(&path, &raw).unwrap();
        assert_eq!(
            MappedStatePage::open_verified_checked(&path, Some(&model_a), Some(1), true).err(),
            Some(CowrieError::StatePageHashMismatch)
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn colocated_16kb_ivf_blocks_untrusted_traps_and_returns_exact_top_hits() {
        use ivf::*;
        let path = std::env::temp_dir().join("cowrie_colocated_ivf_test.cwiv");
        let dim = 16usize;
        // 4 clusters, cluster 2 aligned with query [100, 100, ...]
        let mut centroids = vec![0i8; 4 * dim];
        for d in 0..dim {
            centroids[2 * dim + d] = 100;
            centroids[0 * dim + d] = -50;
        }
        let mut c2_embs = Vec::new();
        let mut c2_meta = Vec::new();
        // Plant max-score UNTRUSTED 0x04 trap (id=999) + clean top match (id=42)
        c2_embs.extend(std::iter::repeat(127i8).take(dim));
        c2_meta.push(VecMeta { scale: 1.0, conf: 0.99, orig_id: 999, auth: AUTH_UNTRUSTED_EXT, pad: [0; 3] });
        c2_embs.extend(std::iter::repeat(90i8).take(dim));
        c2_meta.push(VecMeta { scale: 1.0, conf: 0.95, orig_id: 42, auth: AUTH_VERIFIED_TOOL, pad: [0; 3] });

        let dummy_e = vec![-20i8; dim];
        let dummy_m = vec![VecMeta { scale: 1.0, conf: 0.9, orig_id: 1, auth: AUTH_VERIFIED_TOOL, pad: [0; 3] }];
        let clusters: Vec<(&[i8], &[VecMeta])> = vec![
            (&dummy_e, &dummy_m),
            (&dummy_e, &dummy_m),
            (&c2_embs, &c2_meta),
            (&dummy_e, &dummy_m),
        ];
        let total = write_colocated_ivf_shard(&path, dim as u32, &centroids, &clusters).unwrap();
        assert_eq!(total as usize % PAGE_ALIGN, 0);

        let reader = ColocatedIvfReader::open(&path).unwrap();
        let q = vec![100i8; dim];
        let (hits, bytes_read) = reader.search_trusted(&q, 2, 5, 0.70).unwrap();
        assert!(bytes_read < total as usize);
        assert_eq!(hits[0].orig_id, 42);
        assert!(hits.iter().all(|h| !is_tainted(h.auth)));
        let _ = std::fs::remove_file(&path);
    }
}
