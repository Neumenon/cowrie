# Cowrie v1 — Conformance Status

**Authoritative status:** ✅ **GREEN.** All four implementations conform to Cowrie SPEC‑v1.
Every standing gate passes; CI gates the merge on the same runner used locally.

This document is the single source of truth for the conformance state of the codebase.
Every number below was verified by reading the actual files and running the gates (see
[How the counts were verified](#how-the-counts-were-verified)).

---

## 1. The four implementations

Cowrie v1 has one **reference** and three **production** codecs. Conformance means byte‑level
agreement: given the same logical value, every implementation produces the exact same canonical
byte string, and every implementation rejects the same malformed / non‑canonical bytes.

| Implementation | Role | Location | Conforms? |
| --- | --- | --- | --- |
| **Python** | Reference / oracle — the executable form of the spec; generated the golden vectors | `tools/cowrie_ref/` (package `cowrie_ref`) | ✅ |
| **Go** | Production codec | `go/` (CLI `go/cmd/cowrie`; prebuilt `/tmp/cowrie-cli`) | ✅ |
| **Rust** | Production codec | `rust/` (example `rust/examples/recode.rs`; prebuilt `rust/target/release/examples/recode`) | ✅ |
| **TypeScript** | Production codec | `typescript/` (entry `typescript/recode.ts`, run via `tsx`) | ✅ |

The Python reference is **the oracle**: the conformance gate re‑derives each golden vector from
`cowrie_ref.encode(...)`, and the other three decode the golden bytes and re‑encode them. All four
must produce byte‑identical output.

**Recode contract.** Go / Rust / TS each expose a `recode` operation — read Cowrie bytes on stdin,
decode to the logical value, re‑encode canonically, write bytes to stdout. In **lenient** mode
(default) a decoder may normalize a non‑canonical encoding to its canonical form; in **strict**
mode (`--strict`) it must instead reject non‑canonical input (SPEC‑v1 §5.3).

---

## 2. Standing gates

All gates run from the repo root. They honour `GO_CLI` (default `/tmp/cowrie-cli`) and
`RUST_RECODE` (default `rust/target/release/examples/recode`); TS runs via
`typescript/node_modules/.bin/tsx`. Python must use the dedicated venv because PEP‑668 blocks
system pip:

```sh
PY=/tmp/cowrie-venv/bin/python
export PYTHONPATH=tools          # so pytest and the gates can import cowrie_ref
```

### (a) Positive conformance — `tools/conformance_gate.py`

**What it checks.** 68 curated golden vectors (`testdata/v1_golden.json`) × 4 languages.
The Python reference re‑derives each vector; Go / Rust / TS decode the golden bytes and re‑encode.
Every cell must equal the stored `canonical_hex`. Any mismatch is a hard fail.

The 68 vectors span every wire type and its canonicalization boundaries: null / bool; the full
integer ladder (FIXINT / FIXNEG, Int64 min/max, Uint64 max, BigInt above/below those); floats
(NaN / ±inf / subnormal / max / neg‑zero); strings (empty, 4‑byte UTF‑8, unicode); bytes;
Decimal128, Datetime, UUID, Tensor (f32 / f64 / int8 / bool / qint4 / binary / empty‑dim), Bitmask,
and Extension Core types; array/object length boundaries (15↔16); and `dfs_global_sort` — the
vector that pins **global byte‑sorted** dictionary ordering (see Bug 1).

```sh
$PY tools/conformance_gate.py
```

Expected tail:

```
Cowrie v1 conformance gate — 68 golden vectors
impl        pass  fail   status
python        68     0   ✅ GREEN
go            68     0   ✅ GREEN
rust          68     0   ✅ GREEN
ts            68     0   ✅ GREEN
✅ ALL IMPLEMENTATIONS CONFORM
```

### (b) Negative / anti‑malleability — `tools/negative_gate.py [--strict]`

**What it checks.** 16 adversarial fixtures (`testdata/v1_negative.json`), each fed to the
Go / Rust / TS `recode` and classified as `REJECT`, `NORMALIZE` (accepted, re‑emitted canonical),
or `ACCEPT` (accepted, echoed non‑canonical bytes back — always a bug). The 16 split into two tiers:

- **7 `malformed`** (`bad_magic`, `bad_version`, `truncated_string`, `reserved_tag_0f`,
  `varint_overflow`, `dictidx_oob`, `trailing_data`) — **every** decoder MUST `REJECT`, in both
  modes. Otherwise hard fail.
- **9 `non-canonical`** (`int_not_fixint`, `uint_fits_int`, `bigint_small`, `array_not_fixarr`,
  `neg_zero`, `noncanonical_nan`, `unsorted_dict`, `nonascending_idx`, `bitmask_trailing`) — in
  **lenient** mode `NORMALIZE` is allowed (reported as a strict‑mode gap, not failed) but `ACCEPT`
  is a hard fail; in **strict** mode every one MUST `REJECT`.

```sh
$PY tools/negative_gate.py            # lenient: all 7 malformed rejected
$PY tools/negative_gate.py --strict   # strict §5.3: all 16 rejected by all three
```

Lenient tail: `✅ malformed fixtures rejected by every decoder.` (followed by a strict‑mode‑gap
notice for the non‑canonical cases that lenient decode normalizes/accepts).
Strict tail: `✅ STRICT: every malformed AND non-canonical fixture rejected by all three decoders (§5.3).`

### (c) Differential fuzz — `tools/fuzz_differential.py [N]`

**What it checks.** Generative cross‑language determinism. The Python reference generates `N`
random canonical values — random integer magnitudes across FIXINT / Int64 / Uint64 / BigInt
boundaries, unicode object keys (to stress UTF‑8 byte‑sorting), deep nesting, floats including
NaN / ±inf, and the Core wrapper types — encodes them, and Go / Rust / TS must each recode to
byte‑identical output. Exits non‑zero on the first divergence with a repro seed (default seed
`1234`). This covers inputs the curated goldens don't.

```sh
$PY tools/fuzz_differential.py 500    # or any N; run_all_gates uses 150
```

Expected: `✅ <N> random values — Go/Rust/TS all byte-identical to the Python reference. No divergence.`

### (d) Python reference unit tests — pytest

**What it checks.** 109 tests in `tools/cowrie_ref/tests/` (`test_conformance.py` +
`test_negatives.py`) that lock the reference itself against the golden and negative corpora — the
executable spec. Must run in the venv with `cowrie_ref` importable.

```sh
PYTHONPATH=tools /tmp/cowrie-venv/bin/python -m pytest tools/cowrie_ref/tests -q
```

Expected: `109 passed`.

---

## 3. §5.3 strict‑decode status

✅ **All four implementations honour SPEC‑v1 §5.3.** Canonical input is accepted and round‑trips
byte‑identically; non‑canonical input is **rejected** in strict mode and **normalized** in lenient
mode (never echoed back). Verified by `negative_gate.py --strict`: all 16 fixtures (7 malformed +
9 non‑canonical) `REJECT` across Go, Rust, and TypeScript. The Python reference is canonical‑only
by construction — its encoder emits exactly one encoding per value, and its tests assert decode
rejects the non‑canonical corpus.

| Tier | Lenient mode | Strict mode (§5.3) |
| --- | --- | --- |
| `malformed` (7) | REJECT (all) | REJECT (all) |
| `non-canonical` (9) | NORMALIZE / canonicalize (never ACCEPT) | REJECT (all) |

---

## 4. Bugs the gates caught

Each of these was a real cross‑language divergence surfaced by adding a gate or a golden vector,
then fixed; the corresponding gate now guards against regression.

| Bug | Where | Caught by | Fix |
| --- | --- | --- | --- |
| **Dictionary ordering: DFS‑discovery vs global byte‑sort** | Go, Rust, TS | Positive gate after expanding goldens 32→68 (the `dfs_global_sort` vector) | Sort dictionary entries by global UTF‑8 byte order, not depth‑first discovery order (§2.4) |
| **uvarint 64‑bit overflow wrap** | Rust | Negative gate (`varint_overflow`) + fuzz | Detect and reject varints that overflow 64 bits instead of silently wrapping |
| **Missing strict mode** | Go, Rust, TS | Negative gate `--strict` (§5.3) | Add `--strict` decode that rejects non‑canonical input instead of normalizing it |
| **Object fields re‑ordered by JS integer keys** | TS | Differential fuzz + negative gate | Emit object fields in `dictIdx` order, not the order JS reorders integer‑like string keys |

---

## 5. How CI enforces it

There is **one** gate runner and **one** conformance workflow, and they run the same thing:

- **`tools/run_all_gates.sh`** — the local single‑source‑of‑truth runner. Runs every gate in order
  and exits non‑zero on the first failure:
  1. pytest `tools/cowrie_ref/tests` (109 tests)
  2. `conformance_gate.py` (68 vectors × 4 langs)
  3. `negative_gate.py` (lenient — 16 fixtures)
  4. `negative_gate.py --strict` (strict §5.3)
  5. `fuzz_differential.py 150`

  It prefers `/tmp/cowrie-venv/bin/python`, exports `PYTHONPATH=tools` itself, builds the Go CLI
  only if `/tmp/cowrie-cli` is missing, and fails loudly (rather than rebuilding) if the prebuilt
  Rust `recode` binary is absent.

  ```sh
  bash tools/run_all_gates.sh
  # ✅✅✅  ALL GATES PASS  ✅✅✅
  ```

- **`.github/workflows/conformance.yml`** (workflow name `conformance`, job `run_all_gates`) — runs
  on every push and pull request. It sets up Go 1.21, Rust stable, Node 22, and Python 3.12; builds
  the Go CLI and the Rust `recode` example; `npm ci` for TS; creates `/tmp/cowrie-venv` and
  `pip install -e "tools[test]"`; then runs **exactly** `bash tools/run_all_gates.sh`. If the script
  exits non‑zero the job fails. The gates are law: no merge while this is red.

> The repo also has `.github/workflows/ci.yml` (per‑language build / test / coverage matrices plus a
> `fixtures` cross‑language fixture‑validation job and a publish gate) and
> `.github/workflows/fuzz.yml` (scheduled Go / Rust / Python native fuzzing). `conformance.yml` is
> the workflow that gates the cross‑language byte‑level parity described in this document.

---

## How the counts were verified

This is a documentation task; "verified green" means the numbers were confirmed against the files
and the gates were run.

- Golden vector count (**68**) and negative fixture count (**16**) — parsed `testdata/v1_golden.json`
  and `testdata/v1_negative.json` with Python (`len(...)`).
- Negative tier split (**7 malformed / 9 non‑canonical**) — `Counter(v['tier'] ...)` over
  `v1_negative.json`.
- Reference test count (**109 passed**) —
  `PYTHONPATH=tools /tmp/cowrie-venv/bin/python -m pytest tools/cowrie_ref/tests -q`.
- All gates green — ran `conformance_gate.py`, `negative_gate.py`, `negative_gate.py --strict`,
  `fuzz_differential.py`, and `bash tools/run_all_gates.sh` (full runner ended `✅✅✅ ALL GATES PASS`).
- Gate semantics, runner steps, and CI wiring — read `tools/conformance_gate.py`,
  `tools/negative_gate.py`, `tools/fuzz_differential.py`, `tools/run_all_gates.sh`, and
  `.github/workflows/conformance.yml`.
