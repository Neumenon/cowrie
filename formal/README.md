# Cowrie Formal Verification (`Bend 2.0.35`)

`formal/` contains the executable formal reference kernel (`CowrieKernel.bend`), machine-checked laws (`LAWS.bend`), constructive proofs (`PROOF.bend`), and differential lockstep runner (`differential_lockstep.mjs` + `verify_formal.py`) for Cowrie `v1.2.0`.

For the historical audit log of defects found during formal modeling (`F1`–`F8`) and mutation controls (`M1`–`M13`), see [`formal/AUDIT.md`](AUDIT.md).

## Verified Subsystems (`26/26` Laws)

| Subsystem | Laws (`LAWS.bend`) | Properties Proved |
|---|---|---|
| **Plane D Causal Taint, Sticky Ledger & `Ed25519` Gate** | `L01`–`L06b`, `L15`–`L19`, `L25` (`13` laws) | Biconditional taint propagation, origin-bit preservation, arbitrary-depth sticky taint, clean derivation liveness, exact biconditional `check_gate_signed` characterization, expiry/signature rejection, and $n$-th argument/parent fail-closed behavior. |
| **Plane B 3-State Mealy Fence Escaper & `R2` Renderer** | `L07`–`L08`, `L20`–`L21` (`4` laws) | Universal `<<<`/`>>>` freedom across scalar fields, hoisted `R2Table` column headers, and `R2TensorRef` `<T:...@space>` wrappers; identity on safe strings; `strip_esc` and `count_esc` content preservation. |
| **Plane A Canonical Wire Bijectivity & Map/Dict Rules** | `L09`–`L11`, `L22` (`4` laws) | Round-trip identity, `verify_frame_zero_alloc == decode_frame_strict` equivalence, canonical wire uniqueness, deep duplicate/unsorted/out-of-bounds map key rejection, and universal `UnusedDictKey` rejection. |
| **Plane D `CWZM` Early-Break, Dual Merkle, `.cwwal` & `CWSP`** | `L12`–`L14`, `L23`–`L24` (`5` laws) | Single-page zero false-negative overlap, exact equivalence of sorted multi-page `CWZM` early-break pruning and full linear scan, RFC 6962 `0x02`/`0x04` and odd-leaf non-collision, `.cwwal` torn-tail vs mid-tamper recovery, sequential tombstone/re-insertion replay, and `StatePage` `runtime_id` enforcement. |

*Scope note:* 255-bit `GF(2^255 - 19)` Curve25519 limb arithmetic and hardware SIMD intrinsics (`SHA-NI`, `ARMv8 SHA2`, `AVX2`, `NEON`) are verified via RFC 8032 / RFC 4231 test vectors and scalar-vs-SIMD parity tests.

## Running the Verification Suite (`~3.5 s`)

```bash
make formal
```

Runs `formal/verify_formal.py` (writing `receipts/receipt_v1_2_formal_bend.json`):
1. **`bend formal/PROOF.bend --verdict`:** Checks all `26` laws with both `bend2` and the Lean-proven BendTT kernel (`218.3 ms` median, `ALL PROOFS CHECK`).
2. **`.bendtt` Kernel Hygiene:** Confirms all `26` laws, `87` kernel definitions, and `121` proof definitions appear in `PROOF.bendtt` with `0` `@unsafe`, `0` `?` holes, and `0` `~` templates.
3. **Mutation Controls (`13/13`):** Confirms `--verdict` rejects all `13` mutated kernels (`SOME PROOFS FAIL`).
4. **Differential Lockstep (`2,000` vectors):** Compiles `CowrieKernel.bend` via `bend -o cowrie_kernel_verified.mjs` and checks behavioral equivalence against `cowrie_core.wasm` and `libcowrie_core.so`.
