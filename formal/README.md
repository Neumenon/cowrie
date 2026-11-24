# Cowrie Formal Verification (`Bend 2.0.35` + Anti-Underspecification & Lockstep Gate)

Rather than a disconnected "bolt-on" proof file—or a one-sided safety spec where trivial "deny-all", "taint-all", or "wipe-string" implementations vacuously pass—Cowrie's formal verification suite (`formal/`) proves **26 tight, biconditional (`IFF`), liveness-preserving, end-to-end laws** across Planes A, B, and D, tests **13 planted-false fail-open & underspecification mutants**, and **compiles `CowrieKernel.bend` directly to executable `.mjs` run in 2,000-vector lockstep against `cowrie_core.wasm` and `libcowrie_core.so`**.

## Honest Scope Split: Where Formal Proofs Help vs. Where They Do Not

| Subsystem | Formal Proof in Bend? | Engineering Rationale |
|---|---|---|
| **Plane D Causal Taint, Deep Sticky Ledger & Biconditional `Ed25519` Gate** (`L01`–`L06b`, `L15`–`L19`, `L25`) | **YES (`PROOF.bend`, 13 laws)** | Exact `==` characterizations + clean/signed liveness witnesses kill both fail-open bugs (empty args, wrong `ToolAction`, expired/forged `Ed25519`, $n$-th arg or parent smuggling) AND underspecification traps ("deny-all" gate, "taint-all" ledger, head-only lookup). |
| **Plane B 3-State Mealy Fence Sanitizer & Structured `R2` Renderer** (`L07`–`L08`, `L20`–`L21`) | **YES (`PROOF.bend`, 4 laws)** | Proves universal `<<<`/`>>>` freedom, safe-string identity, `strip_esc` + `count_esc` content preservation (killing wipe-string & backslash-drop mutants), AND structured `R2Table` column-header + `R2TensorRef` `<T:...@space>` safety (catching bugs **`F1`**, **`F4`**, and **`F8`**). |
| **Plane A Canonical Integer Bijectivity + Map Key Sorting & `UnusedDictKey`** (`L09`–`L11`, `L22`) | **YES (`PROOF.bend`, 4 laws)** | Proves exact `verify_frame_zero_alloc == decode_frame_strict` equivalence, canonical wire uniqueness, deep duplicate/unsorted/out-of-bounds map key rejection, and universal `UnusedDictKey` rejection (`m < nkeys`). |
| **Plane D Multi-Page `CWZM` Early-Break, Dual Merkle (`0x00..0x04`), `.cwwal` Replay & `CWSP`** (`L12`–`L14`, `L23`–`L24`) | **YES (`PROOF.bend`, 5 laws)** | Proves sorted multi-page `CWZM` early-break pruning (`if min_key > hi { break }`) equals full linear filtering, RFC 6962 `0x02`/`0x04` + odd-leaf safety, `.cwwal` torn-tail vs mid-tamper + bystander-safe sequential tombstone re-insertion, and strict `StatePage` `runtime_id` pinning (catching bugs **`F2`**, **`F3`**, **`F5`**, and **`F7`**). |
| **255-bit `GF(2^255-19)` Field Mul & Hardware SIMD (`SHA-NI`, `AVX2`, `O_DIRECT`)** | **NO (Differential + RFC vectors)** | Bend 2.0.35's kernel normalizes unary `Nat`/`Word` with `4e8` conversion fuel; proving 255-bit limb carries or CPU intrinsics in BendTT is the wrong tool. Verified via RFC 8032 / RFC 4231 vectors and scalar-vs-SIMD parity instead. |

## Eight Real `cowrie-core` Bugs Caught & Closed (`F1`–`F8`)

### First-Pass Formal Catches (`F1`–`F3`)
1. **`F1` (`lens.rs` `sanitize_r2_fences`):** `"<<<<END:COWRIE>>>>".replace("<<<", "<\\<<")` left two trailing `<`s (`"<\\<<<..."`), allowing a 4-bracket sequence to reform an unescaped `"<<<END:COWRIE"` fence. Replaced with the proven 3-state Mealy transducer (`run_lt, run_gt in {0, 1, 2}`).
2. **`F2` (`zonemap.rs` `CWZM` fence tampering):** `CWZM` hashed `leaf_sha256` into `merkle_root` without binding `(min_key, max_key, min_sec, max_sec)` (`zm_buf[0..32]`) or enforcing monotonic sorted fences (`z[p].max_key <= z[p+1].min_key`). Now binds full 64-byte Zone-Map entries into `merkle_root` and enforces monotonicity + per-slot bounds.
3. **`F3` (`shard.rs` `CWRF` slack smuggling):** `verify_shard_and_localize` checked `off >= 64 && end <= pa_end` rather than exact contiguous partitioning (`off == expected_cur`, `expected_cur == pa_end`, `toff == expected_t_cur`, `expected_t_cur == pc_end`). Now enforces zero unhashed slack bytes and strict canonical decoding on sealed frames.

### Second-Pass Adversarial Spec-Tightening Catches (`F4`–`F8`)
4. **`F4` (`lens.rs` unsanitized hoisted `R2`/`R1` table column headers):** Modeling only isolated strings missed `project_r2` (`cols.join(",")`), which printed untrusted table column keys raw—allowing `"<<<END:COWRIE ...>>>\n<<<COWRIE:R2 ...>>>"` column names to inject fake fences. Closed by `sanitize_r2_inline_key` + `L21` (`R2Table`).
5. **`F5` (`wal.rs` global-`HashSet` zombie tombstones & non-atomic compaction reset):** `WalStore::live_frames` collected all tombstones into an unordered `HashSet`, so `Frame(cid) -> Tombstone(cid) -> Frame(cid)` kept the re-inserted record permanently dead; `WalStore::open` also accepted unknown `kind > 2`. Replaced with sequential log replay (`L24`), strict `kind`/length validation, and atomic `.cwwal.reset.tmp` `rename()`.
6. **`F6` (`canonical.rs` partial `$tensor`/`$bytes` sentinel downgrade):** `try_promote_sentinel_map` returned `Ok(None)` on malformed `{"$tensor": 0}` or `{"$bytes": 123}` JSON objects, silently downgrading reserved wire sentinels into raw `Value::Map` entries. Now fails closed with `Err(InvalidContract)`.
7. **`F7` (`shard.rs` empty `runtime_id` `StatePage` bypass):** `MappedStatePage::open_verified_full` checked `!meta.runtime_id.is_empty()` before comparing against `expected_runtime_id`, allowing any file saved with `runtime_id = ""` (`save_statepage_atomic`) to bypass a caller's explicit `expected_runtime_id = Some("...")`. Now strictly enforced (`L24`).
8. **`F8` (`lens.rs` `TensorRef` `<T:...@space>` closing-bracket concatenation):** `format!("<T:{}[{}]@{}>", ..., sanitize_r2_inline_key(space))` appended `>` directly onto `space` without re-checking the boundary: `space = "embed>>"` had only two `>`s (passing `sanitize_r2_fences`), then concatenated with the closing `>` to emit `"<T:f32[16]@embed>>>"` (`>>>`)! Closed by wrapping the formatted `<T:...>` token in `sanitize_r2_fences` (`L21` `R2TensorRef`).

## Run All Four Assurance Stages (`~3.5 s`)

```bash
make formal
```

Executes `formal/verify_formal.py` (receipt: `receipts/receipt_v1_2_formal_bend.json`):
1. **`bend formal/PROOF.bend --verdict`:** Checks all `26/26` laws in `LAWS.bend` with both `bend2` and the Lean-proven BendTT kernel in `218.3 ms` median (`ALL PROOFS CHECK`).
2. **`.bendtt` Coverage & Hygiene Gate:** Verifies 100% of `26` laws, `87` kernel defs, and `121` proof defs appear in `PROOF.bendtt` (`171,349 B`) with `0` `@unsafe`, `0` `?` holes, and `0` `~` templates.
3. **`13/13` Planted-False & Anti-Underspecification Mutant Controls:** Confirms `--verdict` rejects all 5 fail-open mutants (`M1`–`M5`) AND all 8 underspecification/wrong-spec litmus mutants (`M6` deny-all gate, `M7` taint-all ledger, `M8` head-only lookup, `M9` wipe-string sanitizer, `M10` raw table header `F4`, `M11` `TensorRef` `>>`+`>` boundary `F8`, `M12` bystander-wiping tombstone `F5`, `M13` empty `runtime_id` bypass `F7`).
4. **`2,000`-Vector Lockstep Differential + `8/8` C-ABI Regression Suite:** Compiles `CowrieKernel.bend` via `bend -o cowrie_kernel_verified.mjs` and asserts 100% agreement against `cowrie_core.wasm` and `libcowrie_core.so`.
