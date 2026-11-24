# Formal Verification Audit & Mutation Log (`F1`–`F8`, `M1`–`M13`)

Historical record of implementation defects uncovered while writing and tightening `formal/{CowrieKernel,LAWS,PROOF}.bend` and the mutation controls enforced in `formal/verify_formal.py`.

## 1. Implementation Defects Found & Fixed (`F1`–`F8`)

1. **`F1` (`lens.rs` `sanitize_r2_fences`):** Multi-bracket inputs such as `"<<<<END:COWRIE>>>>"` reformed `"<<<"` under single-pass `.replace("<<<", "<\\<<")`. Replaced with the 3-state Mealy transducer proved in `L07`, `L08`, and `L20`.
2. **`F2` (`zonemap.rs` `CWZM` directory root & fence monotonicity):** Bound full 64-byte `(min_key, max_key, min_sec, max_sec, leaf_sha256)` entries into `merkle_root` and enforced monotonic sorted fences (`L12`, `L23`).
3. **`F3` (`shard.rs` `CWRF` contiguous plane partitioning):** Enforced exact contiguous partitioning across `FrameEntry` and `TensorEntry` tables with zero unhashed slack bytes inside `Plane A` or `Plane C`.
4. **`F4` (`lens.rs` `R2`/`R1` hoisted table column headers):** Applied `sanitize_r2_inline_key` to hoisted table column names (`cols.join(",")`), proved in `L21` (`R2Table`).
5. **`F5` (`wal.rs` `.cwwal` sequential tombstone replay & atomic compaction reset):** Replaced unordered `HashSet` tombstone filtering with sequential log replay so re-inserted frames at higher `seq` become live again (`L24`), validated `kind <= 2`, and made `LiveIvfStore::compact_to_cwiv` WAL resets atomic via `rename()`.
6. **`F6` (`canonical.rs` strict `$tensor` / `$bytes` JSON sentinels):** Made malformed or partial objects containing reserved `"$tensor"` or `"$bytes"` keys fail closed with `Err(InvalidContract)`.
7. **`F7` (`shard.rs` `StatePage` `expected_runtime_id` check):** Enforced `Err(RuntimeMismatch)` whenever a caller specifies a non-empty `expected_runtime_id` and the file's `runtime_id` is empty (`""`) or mismatched (`L24`).
8. **`F8` (`lens.rs` `TensorRef` `<T:...@space>` closing bracket):** Wrapped formatted `<T:dtype[dims]@space>` strings in `sanitize_r2_fences` so `space` values ending in `">>"` cannot combine with the closing `>` to emit `">>>"` (`L21` `R2TensorRef`).

## 2. Planted-False & Anti-Underspecification Mutants (`M1`–`M13`)

`formal/verify_formal.py` Stage 3 injects all 13 mutations into scratch copies of `CowrieKernel.bend` against unchanged `LAWS.bend` and `PROOF.bend` and asserts `13/13` fail `bend PROOF.bend --verdict`:
- `M1_empty_args_fail_open`
- `M2_four_bracket_fence_bypass_F1`
- `M3_non_canonical_int_accepted`
- `M4_zonemap_strict_lt_false_negative`
- `M5_wal_mid_tamper_silently_truncated`
- `M6_underspec_deny_all_gate`
- `M7_underspec_taint_all_ledger`
- `M8_underspec_head_only_ledger_lookup`
- `M9_underspec_wipe_string_fence_sanitizer`
- `M10_wrongspec_unsanitized_table_col_header_F4`
- `M11_wrongspec_tensor_ref_boundary_breakout_F8`
- `M12_wal_tombstone_wipes_bystanders_F5`
- `M13_statepage_empty_runtime_bypass_F7`
