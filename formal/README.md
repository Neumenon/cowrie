# Cowrie Formal Verification (Dual-Prover: `Bend 2.0.35` + `Lean 4.34.0`)

`formal/` contains two independent, zero-external-dependency formal verification suites for **Cowrie `v1.2.0`**, cross-checked against each other and against the production Rust core (`libcowrie_core.so` & `cowrie_core.wasm`) via 2-way and 3-way differential lockstep:

1. **Bend 2.0.35 (`formal/{SPEC,CowrieKernel,LAWS,PROOF}.bend`):**
   - Clean TCB split: frozen specification predicates live in [`SPEC.bend`](SPEC.bend) (`111` defs), the executable kernel lives in [`CowrieKernel.bend`](CowrieKernel.bend) (`96` defs), law statements live in [`LAWS.bend`](LAWS.bend) (`37` laws, zero mutable `K.` specification helpers), and constructive proofs live in [`PROOF.bend`](PROOF.bend) (`278` defs).
   - Verified by `bend formal/PROOF.bend --verdict` (`bend2` + the Lean-proven BendTT kernel, `467.19 ms` median, `0` `@unsafe`, `0` `?` holes, `0` `~` templates).
   - Mutation-tested against **`34/34` planted-false & anti-underspecification mutants (`M01`–`M34`)**, all rejected with `SOME PROOFS FAIL`.
   - Differential lockstep (`2,600` WASM/Bend vectors + `100` native `libcowrie_core.so` C-ABI trials).
   - Receipt: [`receipts/receipt_v1_2_formal_bend.json`](../receipts/receipt_v1_2_formal_bend.json).

2. **Lean 4.34.0 (`formal/lean/`):**
   - Pure Lean 4 `Init` — **zero external Lake dependencies** (`external_dependencies: 0`), built cleanly with `lake clean && lake build --wfail` (`-DwarningAsError=true -DautoImplicit=false`).
   - Proves **`40/40` laws** (`37` Bend-parity laws `L01`–`L36` + `3` Lean-exclusive bit-exact/universal laws `L37`–`L39`) bundled into `Cowrie.Proofs.all_laws_proved : Cowrie.Laws.CowrieSpec`, explicitly type-bound and audited in [`Cowrie/AxiomAudit.lean`](lean/Cowrie/AxiomAudit.lean).
   - **100% pure constructive axioms:** `AXIOM_AUDIT_PASS laws=40 refutations=16 axioms=[propext, Quot.sound]` — **not even `Classical.choice`**, zero `sorry`, `admit`, `axiom`, `native_decide`, `bv_decide`, `unsafe`, `extern`, `implemented_by`, `set_option`, or `partial` in proof modules.
   - Independently re-checked from scratch by Lean's standalone kernel verifier **`leanchecker --fresh Cowrie`** (`PASS`).
   - Proves **`16/16` constructive semantic refutations (`R1`–`R16`)** in [`Cowrie/Refutations.lean`](lean/Cowrie/Refutations.lean), demonstrating inside Lean's logic that concrete broken kernels violate the frozen `Laws.lean` contract (plus proving precondition necessity for `L23`).
   - Mutation-tested against **`26/26` Lean kernel mutants (`LM01`–`LM26`)**, all rejected with genuine Lean proof typecheck failures.
   - **3-way native differential lockstep (`2,110` vectors):** compiles [`Main.lean`](lean/Main.lean) to a native Linux binary (`cowrie_lean_oracle`, `7.35 ms` runtime) and cross-checks **`1,544` true 3-way vectors** (`Lean 4 native` × `Bend .mjs` × `Rust WASM`, including all `16` unbiased `StatePage` HMAC/header states with real HMAC-SHA256) plus **`566` 2-way vectors** (`ZigZag64`, `CWZM`, `LiveIvfStore`, `MCP` classifier).
   - Receipt: [`receipts/receipt_v1_2_formal_lean.json`](../receipts/receipt_v1_2_formal_lean.json).

For the full multi-agent adversarial critique record, real Rust/oracle bug ledger (`F1`–`F18`), all `60` mutants (`M01`–`M34` + `LM01`–`LM26`), and `16` Lean semantic refutations, see [`formal/AUDIT.md`](AUDIT.md) and the interactive monograph [`docs/formal-laws.html`](../docs/formal-laws.html).

## Verified Subsystems (`37` Bend Laws · `40` Lean 4 Laws)

| Subsystem | Laws | Provers | Properties Proved |
|---|---|---|---|
| **Plane D Causal Taint, Sticky Ledger, Header Meta, `Ed25519` Gate & MCP Classifier** | `L01`–`L06b`, `L15`–`L19`, `L25`–`L27` (`15` laws) | Bend + Lean 4 | Non-circular bit-7 `tainted` + origin-bit preservation, irrevocable multi-hop propagation, multi-step stateful sticky ledger (`ledgerInsert`), unknown-parent & empty-args fail-closed, exact 5-arg `checkGateSigned` / 6-arg `checkGateSignedPk` biconditional, expiry/forged-sig/missing-pubkey rejection, $n$-th arg fail-closed, `is_valid_header_meta` boundaries, `Ed25519` action-code `0..=4` bijection, and full 6-arg MCP tool classification biconditional (override precedence + untrusted hint invariance). |
| **Plane B 6-Symbol Mealy Sanitizer, Multi-Cell `R2`, `R2-Compact`, Cell Markers & `extract_field`** | `L07`–`L08`, `L20`–`L21`, `L28`–`L30` (`7` laws) | Bend + Lean 4 | Universal elimination of `<<<`, `>>>`, `<\|`, `\|>`, and `Hidden` spoof chars across scalars, multi-cell `R2Table` (`F4`+`F9`), and `R2TensorRef` (`F8`); identity on safe inputs; `strip_esc` + order-preserving `Sublist`/`subseq` + `≤ 2×` expansion bound; `R2-Compact` / Virtual Atlas fail-closed to `FullMealyFence` on untrusted/tainted/synth/mixed authorities + truncation safety; universal `TableCell` marker injectivity & `Hidden` spoof invariance; strict lens tier `0..=3` bounds & universal `extract_field` composition/lookup/OOB/type-mismatch rules. |
| **Plane A Canonical Wire, `Float64`, Map/Dict, JSON Sentinels & `resolved_frame_cid`** | `L09`–`L11`, `L22`, `L31`–`L33` (`7` laws) | Bend + Lean 4 | Canonical int round-trip, `verify_zero_alloc == decode_strict` equivalence, canonical uniqueness, exact strictly-ascending map keys + universal `UnusedDictKey` rejection, de-circularized canonical `Float64` (`-0.0 → +0.0`, single quiet NaN), lossless `$map`/`$float`/`$bytes`/`$tensor` sentinel round-trip + malformed fail-closed (`F6`), universal `resolved_frame_cid` repack invariance across arbitrary `refs`/`ts ++ extra`, universal OOB rejection, and arbitrary-index tensor binding. |
| **Plane C–D `CWZM` Binary-Skip, Universal Merkle, `.cwwal`, `LiveIvfStore` & `CWSP`** | `L12`–`L14`, `L23`–`L24`, `L34`–`L36` (`8` laws) | Bend + Lean 4 | Single-page zero false-negative overlap, exact `CWZM` `partition_point`-skip + early-break equivalence with linear scan for disjoint zones (plus `hi < lo → []`), universal RFC 6962 `flatten_leaves` injectivity across all lengths `0..∞` + `0x02`/`0x04` count/tree binding + leaf/frame/tensor sensitivity, complete `.cwwal` recovery incl. rollback `bad_seq < exp_seq` and forward gap `bad_seq > exp_seq`, sequential tombstone/re-insert replay, `StatePage` `runtime_id` & all 9 HMAC-first precedence outcomes (`F13`), and `LiveIvfStore` `.cwgen` crash-safe compaction watermark (`F10`). |
| **Lean-Exclusive Machine-Word, Universal Merkle & Declarative Infix Laws** | `L37`–`L39` (`3` laws) | Lean 4 | Exact `UInt8` authority/header-meta bitmasking + arithmetic `Int64` ZigZag `(i <<< 1) ^^^ (i >>> 63)` bijection + IEEE-754 `UInt64` float bit canonicalization (`L37`); universal structural RFC 6962 `flattenLeaves` injectivity & `odd-leaf ≠ duplicated-last` (`L38`); declarative `List.Infix` forbidden-pattern absence (zero shared FSM with sanitizer), prefix-closed truncation safety (`F17`), idempotence, and `≤ 2×` expansion bound (`L39`). |

## Running the Verification Suites

```bash
make formal       # Runs both Bend (verify_formal.py) and Lean 4 (verify_lean.py)
make formal-bend  # Bend 2.0.35 suite only (~25 s incl. 34 mutants + 2,600 lockstep)
make formal-lean  # Lean 4.34.0 suite only (~65 s incl. clean build + leanchecker --fresh + 26 mutants + 2,110 3-way lockstep)
```

## Honest Trust Boundary (What Is NOT Formally Proved)

The formal models in `formal/` and `formal/lean/` prove algebraic, state-machine, and machine-word invariants over clean functional reference models and bind them empirically to the production Rust binaries via `4,710+` differential lockstep vectors (`2,600` Bend + `2,110` Lean 3-way + `100` C-ABI trials). The following layers sit **outside** the mechanized proof boundary and rely instead on conformance vectors, fuzz/negative suites, and code review:

- **Hand-rolled cryptography & hardware SIMD:** `SHA-256` (`SHA-NI`, `ARMv8 SHA2`), `HMAC-SHA256`, `Ed25519` (`GF(2^255 - 19)` limb arithmetic), and `AVX2`/`NEON` dot-product kernels are verified by RFC 8032 / RFC 4231 test vectors and scalar-vs-SIMD parity tests, not formalized in Lean or Bend.
- **Rust `unsafe`, C-ABI pointer safety & OS durability:** Raw pointer dereferences in `ffi.rs`, `mmap` lifetime safety, `flock(2)` / `rename(2)` / `fsync(2)` crash semantics on real filesystems, and panic-unwinding boundaries are tested by Rust unit/negative suites (`F12`, `F15`, `F16`, `F17`) rather than a Rust-level verifier (e.g., Kani/Verus).
- **External runtime connectors:** Out-of-core adapters (`wasm/mcp-mediator.mjs`, `adapters/cloudflare-workers/`, `python/cowrie/integrations/`) consume the Rust/WASM core over FFI/WASM and are tested via integration suites rather than mechanized proofs.
