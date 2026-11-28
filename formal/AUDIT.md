# Formal Verification Audit, Multi-Agent Critique & Mutation Log (`F1`–`F18`, `M01`–`M34`, `LM01`–`LM26`, `R1`–`R16`)

Complete audit trail of real implementation defects uncovered during formal modeling and multi-agent adversarial review (`F1`–`F18`), the two rounds of independent multi-agent critique that hardened both formal suites, all `60` planted-false kernel mutation controls (`34` Bend + `26` Lean 4), and all `16` Lean 4 constructive semantic refutations.

---

## 1. Real Implementation & Oracle Defects Found & Fixed (`F1`–`F18`)

### Round 0 — Original Bend Formalization (`F1`–`F8`)
1. **`F1` (`lens.rs` `sanitize_r2_fences`):** Multi-bracket inputs such as `"<<<<END:COWRIE>>>>"` reformed `"<<<"` under single-pass `.replace("<<<", "<\\<<")`. Replaced with the Mealy transducer proved in `L07`, `L08`, `L20`, and `L39`.
2. **`F2` (`zonemap.rs` `CWZM` directory root & fence monotonicity):** Bound full 64-byte `(min_key, max_key, min_sec, max_sec, leaf_sha256)` entries into `merkle_root` and enforced monotonic sorted fences (`L12`, `L23`).
3. **`F3` (`shard.rs` `CWRF` contiguous plane partitioning):** Enforced exact contiguous partitioning across `FrameEntry` and `TensorEntry` tables with zero unhashed slack bytes inside `Plane A` or `Plane C`.
4. **`F4` (`lens.rs` `R2`/`R1` hoisted table column headers):** Applied `sanitize_r2_inline_key` to hoisted table column names (`cols.join(",")`), proved in `L21` (`R2Table`).
5. **`F5` (`wal.rs` `.cwwal` sequential tombstone replay & atomic compaction reset):** Replaced unordered `HashSet` tombstone filtering with sequential log replay so re-inserted frames at higher `seq` become live again (`L24`), validated `kind <= 2`, and made `LiveIvfStore::compact_to_cwiv` WAL resets atomic via `rename()`.
6. **`F6` (`canonical.rs` strict `$tensor` / `$bytes` JSON sentinels):** Made malformed or partial objects containing reserved `"$tensor"` or `"$bytes"` keys fail closed with `Err(InvalidContract)` (`L32`).
7. **`F7` (`shard.rs` `StatePage` `expected_runtime_id` check):** Enforced `Err(RuntimeMismatch)` whenever a caller specifies a non-empty `expected_runtime_id` and the file's `runtime_id` is empty (`""`) or mismatched (`L24`).
8. **`F8` (`lens.rs` `TensorRef` `<T:...@space>` closing bracket):** Wrapped formatted `<T:dtype[dims]@space>` strings in `sanitize_r2_fences` so `space` values ending in `">>"` cannot combine with the closing `>` to emit `">>>"` (`L21` `R2TensorRef`).

### Round 1 — Multi-Agent Spec/Code Drift & Law Validity Audit (`F9`–`F14`)
9. **`F9` (`lens.rs` multi-cell ChatML breakout & hidden-char cell marker spoofing):** Adjacent `R2Table` cells ending in `<` and starting with `|` or `>` could form `<|` / `|>` across the `|` delimiter, and invisible Unicode/control spoofing characters (`\u{200B}`, `\u{202E}`, etc.) inside string cells could spoof `null` / `<missing>` markers. Fixed by stripping invisible spoofing chars before marker classification and re-sanitizing the full joined row (`L21`, `L29`, `L39`).
10. **`F10` (`wal.rs` `LiveIvfStore` crash-safe `.cwgen` compaction watermark):** A crash after writing the new `.cwgen` base shard but before resetting `.cwwal` replayed already-compacted WAL records a second time. Fixed via `CWAL v2` `wal_base_seq` + `applied_wal_seq_exclusive` watermark filtering (`L35`).
11. **`F11` (`tests/independent_cowr_oracle.py` `dtype 5..=8` alignment):** Independent Python reference verifier had drifted from Rust's extended tensor `dtype` codes `5..=8` (`F8_E4M3`, `F8_E5M2`, `I4`, `U4`). Re-aligned oracle and added explicit `TAG_TENSOR_REF` test vectors (hardened further in `F18`).
12. **`F12` (`ffi.rs` null `parent_auths` with `parent_len > 0`):** FFI callers passing `(NULL, len > 0)` could misclassify or trigger invalid slice construction. Fixed to fail closed before dereference (`L01`, `L04`, `L37`).
13. **`F13` (`shard.rs` `StatePage` HMAC-first verification precedence & flat footer):** Enforced strict verification precedence (key/flag mismatch and HMAC-SHA256 verified before trusting unauthenticated body claims) across all `9` canonical state classes (`L36`).
14. **`F14` (`zonemap.rs` `CWZM` page-0 and EOF trailing-padding hardening):** Hardened `CWZM` header/page-0 bounds validation and rejected unauthenticated trailing EOF padding (`L12`, `L23`).

### Round 2 — Post-Proof Rust Fidelity & Lockstep Audit (`F15`–`F18`)
15. **`F15` (`ffi.rs` `cowrie_statepage_verify_bytes` NULL key with `hmac_key_len > 0` & missing `catch_unwind`):** Passing `(hmac_key = NULL, hmac_key_len > 0)` silently fell back to `key_opt = None` (unkeyed verification) instead of failing closed with `InvalidContract`; `cowrie_wal_live_len` also lacked a `catch_unwind` FFI guard. Fixed both and added Rust regression tests.
16. **`F16` (`wal.rs` pre-write `seq` overflow, `LiveIvfStore` forward-gap check & compaction `flock` preservation):** `next_seq.checked_add(1)` ran *after* `write_all` + `sync_all` (leaving a committed record on disk if `u64::MAX` overflowed); `LiveIvfStore::open` did not reject a forward gap (`wal_base_seq > applied_wal_seq_exclusive`); compaction renamed over the WAL file while dropping the advisory `flock`. Fixed all three in `wal.rs` with Rust regression tests.
17. **`F17` (`lens.rs` `query_frame_slice` overflow / post-cap fence expansion & `zonemap.rs` impossible record/page counts):** `offset + cap` could overflow `usize` and post-slice fence escaping could grow output past `cap` (now proved safe to truncate in `L39` prefix-closed safety); removed redundant double spoof scan in `cell_escape`; `zonemap.rs` now rejects impossible `record_count < page_count` or `(page_count == 0 && record_count != 0)`.
18. **`F18` (`tests/independent_cowr_oracle.py` zero `TAG_TENSOR_REF` coverage + lockstep `SP_PRECEDENCE` parity bug):** `tests/vectors.json` contained zero `TAG_TENSOR_REF` frames so `F11`'s `dtype 5..=8` branch was never executed; added `12` explicit `TAG_TENSOR_REF` positive/negative checks in the independent oracle. Also fixed correlated modulo sampling (`hk = t%2===0` vs `hf = t%4`) in both `differential_lockstep.mjs` and `differential_3way.mjs` and wired real `HMAC-SHA256` + WASM `verifyStatePageBytes` for true 3-way coverage of all `16` `StatePage` flag/key/validity combinations.

---

## 2. Multi-Agent Adversarial Critique Summary

### Round 1 (Pre-Upgrade Critique — 3 Independent Subagents)
- **Spec & Code Drift Auditor:** Identified `11` subsystems added to Cowrie `v1.2` since the original 26 Bend laws (`is_valid_header_meta`, `Ed25519` action codes & MCP classifier, 6-symbol ChatML/hidden sanitizer, `R2-Compact`/Virtual Atlas, table cell markers, lens tiers & `extract_field`, canonical `Float64`, JSON sentinels, `resolved_frame_cid`, `LiveIvfStore` compaction watermark, and `StatePage` HMAC precedence), leading to `L26`–`L36` and real catches `F9`–`F14`.
- **Adversarial Law Validity Critic:** Exposed **6 "smoking gun" underspecifications** in the original 26 Bend laws (`is_tainted` could ignore `0x04`, `any_tainted` could inspect head only, `derive_chain` could be a no-op, token check could ignore `target_cid`, `merkle_tree_digest` could return `HEmpty` or duplicate odd leaves CVE-2012-2459, and `OpFrame` could omit dedup), motivating the `SPEC.bend` TCB split and mutants `M14`–`M28`.
- **Lean 4 Formalization Architect:** Designed the zero-dependency Lean 4.34.0 mirror (`Cowrie/{Trust,Fence,Canonical,Storage,Laws,Proofs,Refutations,AxiomAudit}.lean`), Lean-exclusive machine-word/universal laws `L37`–`L39`, and 3-way native oracle architecture.

### Round 2 (Post-Proof Adversarial Critique — 2 Independent Subagents)
- **Post-Proof Law Validity Critic:** Audited all `37` Bend laws, `40` Lean laws, and both harnesses after initial green builds and caught:
  - **4 CRITICALs (all fixed):** `L37` machine-word circularity (now pinned against frozen literal masks, arithmetic `specZigzag64`, and `isValidHeaderMetaByte` using pure constructive bit-level proofs with zero `Classical.choice`); Bend `L36` missing 9th case `(False, 0, mv, False) == SpInvalidContract` (killed by `M29`); finite-length Merkle gap where `fuel = 2n` or dropping `f_tree` survived (replaced with universal `flatten_leaves` injectivity across all lengths `0..∞` + `M30`/`M31`); and TCB helper leaks (`K.auth_clean_bits`/`K.auth_ext_bit` in Bend `L15`, `stripSpoof`/`stripEsc` in Lean `L20`/`L21`/`L39`, all moved to frozen `SPEC.bend` / `Laws.lean` + `M34`).
  - **7 HIGHs (all fixed):** De-circularized `L31` (`Float64`); closed `L27` MCP policy override fail-open (`M32`/`LM23`/`R15`); generalized `L33` `resolved_frame_cid` from single `[0]` ref to universal `refs`/`ts ++ extra` + OOB + sensitivity; generalized `L29` from 3 examples to universal `raw`; generalized `L14` WAL recovery to arbitrary `start_seq` + rollback `bad_seq < exp_seq` (`M33`/`LM24`/`R16`); generalized `L30` `extract_field` to deep map lookup, arbitrary array indexing, and path composition; hardened `verify_lean.py` with `lake clean && lake build --wfail`, lexical comment/string-stripping static scanner, explicit `_bindCowrieSpec : Cowrie.Laws.CowrieSpec` binding in `AxiomAudit.lean`, and `4` new Lean mutants (`LM23`–`LM26`).
  - **5 MEDIUMs (all fixed):** Pinned 5-arg `checkGateSigned` wrapper in `L17`; added `hi < lo → []` to `L23`; removed dead `prune_sorted_zones_at` and wired `isValidHeaderMetaByte` & multi-step `ledgerInsert` into `L37`/`L16`; pinned bit-7 `tainted` in `L01`/`L25`/`L37`; added `List.Sublist` on all `R2Field` variants (`L21`) and `≤ 2×` expansion bound (`L20`, `L39`).
- **Rust Fidelity & Lockstep Auditor:** Uncovered real bugs `F15`–`F18`, mixed-authority lockstep coverage gaps (`AUTH_BASES` expanded to `14` bytes), correlated `SP_PRECEDENCE` sampling, and two-way vs true three-way accounting in `differential_3way.mjs` — all fixed and verified.

---

## 3. Bend 2.0.35 Planted-False Mutation Controls (`34/34` Rejected)

`formal/verify_formal.py` Stage 3 injects all `34` mutations into scratch copies of `CowrieKernel.bend` against unchanged `SPEC.bend`, `LAWS.bend`, and `PROOF.bend` and asserts `34/34` return `SOME PROOFS FAIL`:

- `M01_empty_args_fail_open`
- `M02_four_bracket_fence_bypass_F1`
- `M03_non_canonical_int_accepted`
- `M04_zonemap_strict_lt_false_negative`
- `M05_wal_mid_tamper_silently_truncated`
- `M06_underspec_deny_all_gate`
- `M07_underspec_taint_all_ledger`
- `M08_underspec_head_only_ledger_lookup`
- `M09_underspec_wipe_string_fence_sanitizer`
- `M10_wrongspec_unsanitized_table_col_header_F4`
- `M11_wrongspec_tensor_ref_boundary_breakout_F8`
- `M12_wal_tombstone_wipes_bystanders_F5`
- `M13_statepage_empty_runtime_bypass_F7`
- `M14_smoking_gun_is_tainted_ignores_ext`
- `M15_smoking_gun_head_only_any_tainted`
- `M16_smoking_gun_noop_derive_chain`
- `M17_unknown_parent_fail_open_nonempty`
- `M18_credread_unprivileged_bypass`
- `M19_token_ignores_target_cid`
- `M20_r2_table_multicell_chatml_breakout_F9`
- `M21_r2_compact_allows_model_synth`
- `M22_smoking_gun_merkle_const_empty`
- `M23_smoking_gun_merkle_cve_2012_2459_dup_odd`
- `M24_smoking_gun_wal_opframe_no_dedup`
- `M25_live_ivf_ignores_watermark_F10`
- `M26_statepage_hmac_flag_precedence_bypass_F13`
- `M27_missing_owner_pubkey_allows_declass`
- `M28_table_cell_hidden_spoof_marker_F9`
- `M29_statepage_nokey_bad_hdr_accept`
- `M30_merkle_fuel_truncated_to_2n`
- `M31_shard_merkle_drops_frames`
- `M32_mcp_policy_override_fail_open`
- `M33_wal_rollback_seq_le_replay`
- `M34_l15_tcb_clobber_origin_bits`

---

## 4. Lean 4.34.0 Planted-False Mutation Controls (`26/26` Rejected)

`formal/lean/verify_lean.py` Stage 3 mutates `Cowrie/{Trust,Fence,Canonical,Storage}.lean` against unchanged `Cowrie/Laws.lean` and `Cowrie/Proofs.lean` and asserts `26/26` fail Lean proof typechecking (distinct from syntax/build errors):

- `LM01_empty_args_fail_open`
- `LM02_four_bracket_fence_bypass_F1`
- `LM03_non_canonical_int_accepted`
- `LM04_zonemap_strict_lt_false_negative`
- `LM05_wal_mid_tamper_silently_truncated`
- `LM06_smoking_gun_isTainted_ignores_ext`
- `LM07_smoking_gun_head_only_anyTainted`
- `LM08_smoking_gun_noop_deriveChain`
- `LM09_unknown_parent_fail_open`
- `LM10_credRead_unprivileged_bypass`
- `LM11_token_ignores_target_cid`
- `LM12_missing_owner_pubkey_allows_declass`
- `LM13_r2_table_multicell_chatml_breakout_F9`
- `LM14_r2_compact_allows_model_synth`
- `LM15_table_cell_hidden_spoof_marker_F9`
- `LM16_smoking_gun_merkle_const_empty`
- `LM17_smoking_gun_merkle_cve_2012_2459_dup_odd`
- `LM18_smoking_gun_wal_opframe_no_dedup`
- `LM19_live_ivf_ignores_watermark_F10`
- `LM20_statepage_hmac_flag_precedence_bypass_F13`
- `LM21_lean_uint8_propagate_clobbers_origin`
- `LM22_lean_i64_zigzag_drops_sign_xor`
- `LM23_mcp_policy_override_fail_open`
- `LM24_wal_rollback_seq_le_replay`
- `LM25_f64_bits_drops_nan_canon`
- `LM26_uint8_isTainted_ignores_ext`

---

## 5. Lean 4.34.0 Constructive Semantic Refutations (`16/16` Proved)

In addition to external mutant tests, [`formal/lean/Cowrie/Refutations.lean`](lean/Cowrie/Refutations.lean) proves `16` internal theorems of the form `¬ (broken_impl satisfies law)` under the same `[propext, Quot.sound]` constructive axiom budget:

1. `refute_M14_ignore_ext`
2. `refute_M15_head_only_anyTainted`
3. `refute_M16_noop_deriveChain`
4. `refute_M19_token_ignores_cid`
5. `refute_M20_F9_multicell_chatml_breakout`
6. `refute_M28_F9_hidden_cell_marker_spoof`
7. `refute_M21_r2_compact_allows_synth`
8. `refute_M22_merkle_const_empty`
9. `refute_M23_merkle_cve_2012_2459`
10. `refute_M24_wal_opframe_no_dedup`
11. `refute_M25_F10_live_ivf_watermark_ignored`
12. `refute_M26_F13_statepage_hmac_flag_precedence`
13. `refute_M27_missing_owner_pubkey`
14. `refute_L23_unsorted_zonemap_needs_disjoint_precondition`
15. `refute_M32_mcp_policy_override_bypass`
16. `refute_M33_wal_rollback_seq_le_replay`
