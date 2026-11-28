# CowrieKernel.bend — Upgraded Executable Formal Reference Kernel for Cowrie v1.2 (37-Law Edition)
#
# Upgraded after multi-agent adversarial validity & spec-drift audit (`F1`–`F14`):
#   - Clean TCB separation: ALL frozen specification predicates live in `SPEC.bend`.
#   - Plane D: Causal taint (`ext | tainted`), deep sticky ledger, `owner_pubkey` fail-closed Ed25519 gate,
#              header metadata (`is_valid_header_meta`), Ed25519 action codes, default-deny MCP tool classifier.
#   - Plane B: Full 6-symbol Mealy transducer (`<<<`, `>>>`, `<|`, `|>`, `Hidden` strip), multi-cell `R2Table`
#              pipe-boundary sanitization (`F9`), marker injectivity under hidden chars (`F9`), `R2-Compact` &
#              `project_virtual_atlas` fail-closed trust tiering, strict `R0..R3` lens tier bounds, `extract_field`.
#   - Plane A: Canonical wire integer bijection, `Float64` canonicalization, Map key sorting & `UnusedDictKey`,
#              lossless `$map`/`$float`/`$bytes`/`$tensor` JSON sentinels, `resolved_frame_cid` repack invariance.
#   - Plane C/D: `CWZM` disjoint-monotonic binary-search skip + early-break (`zonemap.rs`), RFC 6962 dual-tree
#              Merkle (`0x00`..`0x04`), `.cwwal` strict/torn-tail + `seq` monotonicity + sequential tombstone/re-insert,
#              `LiveIvfStore` `.cwgen` crash-safe watermark compaction (`F10`), `CWSP` `runtime_id` & HMAC-first precedence (`F13`).

import Base
import ./SPEC.bend as S

# =============================================================================
# 1. PLANE D — CAUSAL TAINT, STICKY LEDGER, HEADER META, ED25519 GATE & MCP CLASSIFIER
# =============================================================================

def is_tainted(a: S.Auth) -> Bool:
  match a:
    case S.Auth{_, True{}, _}:
      True{}
    case S.Auth{_, False{}, t}:
      t

def auth_clean_bits(a: S.Auth) -> Nat:
  match a:
    case S.Auth{c, _, _}:
      c

def auth_ext_bit(a: S.Auth) -> Bool:
  match a:
    case S.Auth{_, e, _}:
      e

def set_tainted(a: S.Auth) -> S.Auth:
  match a:
    case S.Auth{c, e, _}:
      S.Auth{c, e, True{}}

def prop_apply(flag: Bool, base: S.Auth) -> S.Auth:
  match flag:
    case True{}:
      set_tainted(base)
    case False{}:
      base

def any_tainted(ps: List<&2, S.Auth>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+h, +t}:
      S.bor(is_tainted(h), any_tainted(t))

def propagate(+base: S.Auth, ps: List<&2, S.Auth>) -> S.Auth:
  prop_apply(S.bor(is_tainted(base), any_tainted(ps)), base)

def derive_chain(hops: List<&2, S.Auth>, cur: S.Auth) -> S.Auth:
  match hops:
    case Nil{}:
      cur
    case Con{+step_base, +rest}:
      derive_chain(rest, propagate(step_base, [cur]))

def select_lookup(eq: Bool, a: S.Auth, fallback: Maybe<&2, S.Auth>) -> Maybe<&2, S.Auth>:
  match eq:
    case True{}:
      Some{a}
    case False{}:
      fallback

def ledger_lookup(lg: List<&2, S.LedgerNode>, +cid: Nat) -> Maybe<&2, S.Auth>:
  match lg:
    case Nil{}:
      None{}
    case Con{S.LNode{+k, +a}, +rest}:
      select_lookup(S.nat_eq(k, cid), a, ledger_lookup(rest, cid))

def opt_auth_tainted(ma: Maybe<&2, S.Auth>) -> Bool:
  match ma:
    case None{}:
      True{}
    case Some{a}:
      is_tainted(a)

def existing_tainted(ma: Maybe<&2, S.Auth>) -> Bool:
  match ma:
    case None{}:
      False{}
    case Some{a}:
      is_tainted(a)

def parents_tainted(ps: List<&2, Nat>, +lg: List<&2, S.LedgerNode>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+p, +rest}:
      S.bor(opt_auth_tainted(ledger_lookup(lg, p)), parents_tainted(rest, lg))

def ledger_record(+lg: List<&2, S.LedgerNode>, +cid: Nat, +base: S.Auth, ps: List<&2, Nat>) -> S.Auth:
  old_t = existing_tainted(ledger_lookup(lg, cid))
  par_t = parents_tainted(ps, lg)
  prop_apply(S.bor(is_tainted(base), S.bor(old_t, par_t)), base)

def is_privileged(act: S.Action) -> Bool:
  match act:
    case S.ReadOnly{}:
      False{}
    case S.ShellExec{}:
      True{}
    case S.CredRead{}:
      True{}
    case S.FsWrite{}:
      True{}
    case S.NetExfil{}:
      True{}

def action_eq(a: S.Action, b: S.Action) -> Bool:
  match a b:
    case S.ReadOnly{} S.ReadOnly{}:
      True{}
    case S.ShellExec{} S.ShellExec{}:
      True{}
    case S.CredRead{} S.CredRead{}:
      True{}
    case S.FsWrite{} S.FsWrite{}:
      True{}
    case S.NetExfil{} S.NetExfil{}:
      True{}
    case _ _:
      False{}

def token_matches(tok: S.DeclassToken, +cid: Nat, +act: S.Action, +now: Nat) -> Bool:
  match tok:
    case S.Tok{+tcid, +tact, +texp, +tsig}:
      S.band(tsig, S.band(S.nat_eq(tcid, cid), S.band(action_eq(tact, act), S.nat_le(now, texp))))

def has_valid_token(toks: List<&2, S.DeclassToken>, +cid: Nat, +act: S.Action, +now: Nat) -> Bool:
  match toks:
    case Nil{}:
      False{}
    case Con{+tk, +rest}:
      S.bor(token_matches(tk, cid, act, now), has_valid_token(rest, cid, act, now))

def arg_auth_branch(tainted: Bool, +cid: Nat, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match tainted:
    case False{}:
      True{}
    case True{}:
      S.band(has_pk, has_valid_token(toks, cid, act, now))

def arg_authorized(+cid: Nat, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  arg_auth_branch(opt_auth_tainted(ledger_lookup(lg, cid)), cid, act, toks, has_pk, now)

def all_args_authorized(args: List<&2, Nat>, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match args:
    case Nil{}:
      True{}
    case Con{+c, +rest}:
      S.band(arg_authorized(c, lg, act, toks, has_pk, now), all_args_authorized(rest, lg, act, toks, has_pk, now))

def check_priv_args(args: List<&2, Nat>, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match args:
    case Nil{}:
      False{}
    case Con{+c, +rest}:
      S.band(arg_authorized(c, lg, act, toks, has_pk, now), all_args_authorized(rest, lg, act, toks, has_pk, now))

def check_gate_priv(priv: Bool, +lg: List<&2, S.LedgerNode>, +act: S.Action, args: List<&2, Nat>, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match priv:
    case False{}:
      True{}
    case True{}:
      check_priv_args(args, lg, act, toks, has_pk, now)

# Signature with explicit `has_pk: Bool` (models `owner_pubkey: Option<&[u8; 32]>` in `trust.rs:146,160`)
def check_gate_signed_pk(+lg: List<&2, S.LedgerNode>, +act: S.Action, args: List<&2, Nat>, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  check_gate_priv(is_privileged(act), lg, act, args, toks, has_pk, now)

# Backwards-compatible 5-arg wrapper (pinned owner pubkey present)
def check_gate_signed(+lg: List<&2, S.LedgerNode>, +act: S.Action, args: List<&2, Nat>, +toks: List<&2, S.DeclassToken>, +now: Nat) -> Bool:
  check_gate_signed_pk(lg, act, args, toks, True{}, now)

# Plane A/D Header Metadata Validator (`canonical.rs:16-18`)
# `low4`: low 4 bits of authority (must be in 1..=15), `res3`: reserved bits 4..6 (must be 0),
# `epi`: epistemic class (must be in 1..=3), `flags`: must be 0.
def is_valid_header_meta(+low4: Nat, +res3: Nat, +epi: Nat, +flags: Nat) -> Bool:
  S.band(
    S.nat_eq(flags, 0n),
    S.band(
      S.nat_eq(res3, 0n),
      S.band(
        S.band(S.nat_le(1n, low4), S.nat_le(low4, 15n)),
        S.band(S.nat_le(1n, epi), S.nat_le(epi, 3n))
      )
    )
  )

# RFC 8032 `COWRIE_DECLASSIFY_V1` Action Codes (`ed25519.rs:618-637`)
def action_to_code(act: S.Action) -> Nat:
  match act:
    case S.ReadOnly{}:
      0n
    case S.ShellExec{}:
      1n
    case S.CredRead{}:
      2n
    case S.FsWrite{}:
      3n
    case S.NetExfil{}:
      4n

def action_from_code(code: Nat) -> Maybe<&2, S.Action>:
  match code:
    case 0n:
      Some{S.ReadOnly{}}
    case 1n:
      Some{S.ShellExec{}}
    case 2n:
      Some{S.CredRead{}}
    case 3n:
      Some{S.FsWrite{}}
    case 4n:
      Some{S.NetExfil{}}
    case 1n+(1n+(1n+(1n+(1n+_)))):
      None{}

# Default-Deny MCP Tool Classifier (`crates/cowrie-cli/src/main.rs:244-306`)
def classify_ro_branch(is_ro: Bool) -> S.Action:
  match is_ro:
    case True{}:
      S.ReadOnly{}
    case False{}:
      S.ShellExec{}

def classify_mcp_tool(
  pol_override: Maybe<&2, S.Action>,
  verb_priv: Maybe<&2, S.Action>,
  allow_ro: Bool,
  builtin_ro: Bool,
  trust_ann: Bool,
  server_ro_hint: Bool
) -> S.Action:
  match pol_override verb_priv:
    case Some{act} _:
      act
    case None{} Some{vact}:
      vact
    case None{} None{}:
      classify_ro_branch(S.bor(allow_ro, S.bor(builtin_ro, S.band(trust_ann, server_ro_hint))))

# =============================================================================
# 2. PLANE B — 6-SYMBOL MEALY TRANSDUCER, MULTI-CELL R2 TABLE (`F9`), R2-COMPACT & EXTRACT
# =============================================================================

# Full v1.2 Mealy transducer (`lens.rs:68-132`):
#   - Strips `Hidden{}` (zero-width, Bidi, Unicode Tags, C0/C1) BEFORE run/prev transitions
#   - Escapes 3rd consecutive `<` or `>` (`<<<`, `>>>`)
#   - Escapes ChatML `<|` (`prev == PLt` on `Pipe`) and `|>` (`prev == PPipe` on `Gt`)
def sanitize_step(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> List<&2, S.Sym>:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      Nil{}
    case Con{S.Hidden{}, +rest} r_l r_g p:
      sanitize_step(rest, r_l, r_g, p)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      S.Lt{} <> sanitize_step(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      S.Lt{} <> sanitize_step(rest, S.R2{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R2{} _ _:
      S.Esc{} <> (S.Lt{} <> sanitize_step(rest, S.R1{}, S.R0{}, S.PLt{}))
    case Con{S.Gt{}, +rest} _ _ S.PPipe{}:
      S.Esc{} <> (S.Gt{} <> sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}))
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      S.Gt{} <> sanitize_step(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      S.Gt{} <> sanitize_step(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      S.Gt{} <> sanitize_step(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      S.Gt{} <> sanitize_step(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R2{} S.PLt{}:
      S.Esc{} <> (S.Gt{} <> sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}))
    case Con{S.Gt{}, +rest} _ S.R2{} S.POther{}:
      S.Esc{} <> (S.Gt{} <> sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}))
    case Con{S.Pipe{}, +rest} _ _ S.PLt{}:
      S.Esc{} <> (S.Pipe{} <> sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{}))
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      S.Pipe{} <> sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      S.Pipe{} <> sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Esc{}, +rest} _ _ _:
      S.Esc{} <> sanitize_step(rest, S.R0{}, S.R0{}, S.POther{})
    case Con{S.Other{}, +rest} _ _ _:
      S.Other{} <> sanitize_step(rest, S.R0{}, S.R0{}, S.POther{})

def sanitize_fences(xs: List<&2, S.Sym>) -> List<&2, S.Sym>:
  sanitize_step(xs, S.R0{}, S.R0{}, S.POther{})

# Structured `R2` field renderer covering scalar key/values, hoisted multi-cell `R2Table`
# joined by `Pipe{}` (`F4` + `F9` ChatML boundary fix!), AND `<T:...@space>` `R2TensorRef` (`F8`)!
def render_r2_field(f: S.R2Field) -> List<&2, S.Sym>:
  match f:
    case S.R2Scalar{k, v}:
      S.append_sym(sanitize_fences(k), S.Other{} <> sanitize_fences(v))
    case S.R2Table{k, col, c1, c2}:
      row = sanitize_fences(S.append_sym(sanitize_fences(c1), S.Pipe{} <> sanitize_fences(c2)))
      S.append_sym(sanitize_fences(k), S.Other{} <> S.append_sym(sanitize_fences(col), S.Other{} <> row))
    case S.R2TensorRef{k, sp}:
      S.append_sym(sanitize_fences(k), S.Other{} <> sanitize_fences(S.Lt{} <> (S.Other{} <> S.append_sym(sanitize_fences(sp), [S.Gt{}]))))

# `R2-Compact` & `project_virtual_atlas` Trust-Tiered Fail-Closed Gate (`lens.rs:395-407, 688`)
# `clean_bits`: 1 = SYS_POLICY, 2 = VERIFIED_TOOL, 8 = MODEL_SYNTH, other = mixed/invalid.
def r2_compact_mode(a: S.Auth) -> S.LensMode:
  match a:
    case S.Auth{1n, False{}, False{}}:
      S.CompactHeader{}
    case S.Auth{2n, False{}, False{}}:
      S.CompactHeader{}
    case _:
      S.FullMealyFence{}

def virtual_atlas_mode(a: S.Auth) -> S.LensMode:
  r2_compact_mode(a)

# R2 Table Cell Marker Encoding (`lens.rs:283-317` + `F9` hidden-char strip before marker match!)
def is_null_or_missing_str(xs: List<&2, S.Sym>) -> Bool:
  match xs:
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Gt{}, Nil{}}}}:
      True{}
    case Con{S.Other{}, Nil{}}:
      True{}
    case _:
      False{}

def encode_nonempty_cell(is_nm: Bool, eff: List<&2, S.Sym>) -> S.CellTag:
  match is_nm:
    case True{}:
      S.OutQuotedStr{sanitize_fences(eff)}
    case False{}:
      S.OutPlainStr{sanitize_fences(eff)}

def encode_cleaned_str(eff: List<&2, S.Sym>) -> S.CellTag:
  match eff:
    case Nil{}:
      S.OutEmptyQuoted{}
    case Con{+h, +t}:
      encode_nonempty_cell(is_null_or_missing_str(h <> t), h <> t)

def encode_table_cell(c: S.TableCell) -> S.CellTag:
  match c:
    case S.CellNull{}:
      S.OutNull{}
    case S.CellMissing{}:
      S.OutMissing{}
    case S.CellStr{raw}:
      encode_cleaned_str(S.strip_hidden(raw))

# Strict Lens Tier Bounds (`ffi.rs:258`, `SPEC.md` §4: R0=0, R1=1, R2=2, R3=3; >=4 rejected!)
def is_valid_lens_tier(tier: Nat) -> Bool:
  S.nat_le(tier, 3n)

# `extract_field` exact typed path resolution (`lens.rs:497-524`)
def select_val(eq: Bool, v: S.ValTree, fb: Maybe<&2, S.ValTree>) -> Maybe<&2, S.ValTree>:
  match eq:
    case True{}:
      Some{v}
    case False{}:
      fb

def map_find_val(entries: List<&2, S.MapEntry>, +key: Nat) -> Maybe<&2, S.ValTree>:
  match entries:
    case Nil{}:
      None{}
    case Con{S.MEntry{+ek, v}, rest}:
      select_val(S.nat_eq(ek, key), v, map_find_val(rest, key))

def list_nth_val(items: List<&2, S.ValTree>, idx: Nat) -> Maybe<&2, S.ValTree>:
  match items idx:
    case Nil{} _:
      None{}
    case Con{h, _} 0n:
      Some{h}
    case Con{_, t} 1n+pi:
      list_nth_val(t, pi)

def extract_step(cur: S.ValTree, seg: S.PathSeg) -> Maybe<&2, S.ValTree>:
  match cur seg:
    case S.VMap{es} S.PKey{k}:
      map_find_val(es, k)
    case S.VArr{xs} S.PIdx{i}:
      list_nth_val(xs, i)
    case _ _:
      None{}

def extract_opt_step(cur: Maybe<&2, S.ValTree>, seg: S.PathSeg) -> Maybe<&2, S.ValTree>:
  match cur:
    case None{}:
      None{}
    case Some{v}:
      extract_step(v, seg)

def extract_field_opt(path: List<&2, S.PathSeg>, cur: Maybe<&2, S.ValTree>) -> Maybe<&2, S.ValTree>:
  match path:
    case Nil{}:
      cur
    case Con{seg, rest}:
      extract_field_opt(rest, extract_opt_step(cur, seg))

def extract_field(cur: S.ValTree, path: List<&2, S.PathSeg>) -> Maybe<&2, S.ValTree>:
  extract_field_opt(path, Some{cur})

# =============================================================================
# 3. PLANE A — CANONICAL WIRE, FLOAT64, MAP/DICT, JSON SENTINELS & RESOLVED CID
# =============================================================================

def encode_pos_branch(is_small: Bool, n: Nat) -> S.WireInt:
  match is_small:
    case True{}:
      S.WFix{n}
    case False{}:
      S.WTagIntPos{n}

def encode_canon_int(+lim: Nat, +ulim: Nat, v: S.LogicalInt) -> S.WireInt:
  match v:
    case S.LPos{+n}:
      encode_pos_branch(S.nat_lt(n, lim), n)
    case S.LNeg{m}:
      S.WTagIntNeg{m}
    case S.LBigUint{d}:
      S.WTagUint{S.nat_add(ulim, d)}

def decode_fix_branch(ok: Bool, n: Nat) -> Maybe<&2, S.LogicalInt>:
  match ok:
    case True{}:
      Some{S.LPos{n}}
    case False{}:
      None{}

def decode_pos_branch(too_small: Bool, n: Nat) -> Maybe<&2, S.LogicalInt>:
  match too_small:
    case True{}:
      None{}
    case False{}:
      Some{S.LPos{n}}

def decode_uint_branch(too_small: Bool, +ulim: Nat, +u: Nat) -> Maybe<&2, S.LogicalInt>:
  match too_small:
    case True{}:
      None{}
    case False{}:
      Some{S.LBigUint{S.nat_sub(u, ulim)}}

def decode_canon_int(+lim: Nat, +ulim: Nat, w: S.WireInt) -> Maybe<&2, S.LogicalInt>:
  match w:
    case S.WFix{+n}:
      decode_fix_branch(S.nat_lt(n, lim), n)
    case S.WTagIntPos{+n}:
      decode_pos_branch(S.nat_lt(n, lim), n)
    case S.WTagIntNeg{m}:
      Some{S.LNeg{m}}
    case S.WTagUint{+u}:
      decode_uint_branch(S.nat_lt(u, ulim), ulim, u)

def verify_canon_int_zero_alloc(+lim: Nat, +ulim: Nat, w: S.WireInt) -> Bool:
  match w:
    case S.WFix{n}:
      S.nat_lt(n, lim)
    case S.WTagIntPos{n}:
      S.bnot(S.nat_lt(n, lim))
    case S.WTagIntNeg{_}:
      True{}
    case S.WTagUint{u}:
      S.bnot(S.nat_lt(u, ulim))

def is_some_int(m: Maybe<&2, S.LogicalInt>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{_}:
      True{}

# Plane A Canonical `Float64` (`canonical.rs:168-178, 564-580`)
def canonicalize_f64(f: S.FloatKind) -> S.FloatKind:
  match f:
    case S.FPosZero{}:
      S.FPosZero{}
    case S.FNegZero{}:
      S.FPosZero{}
    case S.FFinite{b}:
      S.FFinite{b}
    case S.FInf{p}:
      S.FInf{p}
    case S.FCanonQNaN{}:
      S.FCanonQNaN{}
    case S.FNonCanonNaN{_}:
      S.FCanonQNaN{}

def verify_canon_f64(f: S.FloatKind) -> Bool:
  match f:
    case S.FPosZero{}:
      True{}
    case S.FNegZero{}:
      False{}
    case S.FFinite{_}:
      True{}
    case S.FInf{_}:
      True{}
    case S.FCanonQNaN{}:
      True{}
    case S.FNonCanonNaN{_}:
      False{}

# Plane A Map Key Strictly-Ascending Index Verifier & `KeyBitset` Unused-Key Verifier
def verify_map_keys_from(ks: List<&2, Nat>, +min_allowed: Nat, +nkeys: Nat) -> Bool:
  match ks:
    case Nil{}:
      True{}
    case Con{+k, +rest}:
      S.band(S.nat_le(min_allowed, k), S.band(S.nat_lt(k, nkeys), verify_map_keys_from(rest, 1n+k, nkeys)))

def verify_map_keys(+nkeys: Nat, ks: List<&2, Nat>) -> Bool:
  verify_map_keys_from(ks, 0n, nkeys)

def list_contains_nat(xs: List<&2, Nat>, +target: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+x, +rest}:
      S.bor(S.nat_eq(x, target), list_contains_nat(rest, target))

def all_dict_keys_used(+nkeys: Nat, +refs: List<&2, Nat>) -> Bool:
  match nkeys:
    case 0n:
      True{}
    case 1n+pk:
      S.band(list_contains_nat(refs, pk), all_dict_keys_used(pk, refs))

# Plane A Lossless JSON Sentinels (`$map`, `$float`, `$bytes`, `$tensor`, `canonical.rs:989-1509`)
def emit_sentinel_json(v: S.SentinelVal) -> S.JsonSentinel:
  match v:
    case S.SVPlain{t}:
      S.JPlain{t}
    case S.SVBytes{r}:
      S.JBytesObj{True{}, r}
    case S.SVFloatSpecial{c}:
      S.JFloatObj{True{}, c}
    case S.SVTensor{a, dt}:
      S.JTensorObj{True{}, a, dt}
    case S.SVMapPlain{p}:
      S.JMapPlain{p}
    case S.SVMapReserved{kk, p}:
      S.JMapWrap{True{}, kk, p}

def parse_sentinel_json(j: S.JsonSentinel) -> Maybe<&2, S.SentinelVal>:
  match j:
    case S.JPlain{t}:
      Some{S.SVPlain{t}}
    case S.JBytesObj{True{}, r}:
      Some{S.SVBytes{r}}
    case S.JBytesObj{False{}, _}:
      None{}
    case S.JFloatObj{True{}, c}:
      Some{S.SVFloatSpecial{c}}
    case S.JFloatObj{False{}, _}:
      None{}
    case S.JTensorObj{True{}, a, dt}:
      Some{S.SVTensor{a, dt}}
    case S.JTensorObj{False{}, _, _}:
      None{}
    case S.JMapPlain{p}:
      Some{S.SVMapPlain{p}}
    case S.JMapWrap{True{}, kk, p}:
      Some{S.SVMapReserved{kk, p}}
    case S.JMapWrap{False{}, _, _}:
      None{}

# Plane A/C `resolved_frame_cid` (`shard.rs:432-474`)
def lookup_tensor(ts: List<&2, S.TensorDesc>, idx: Nat) -> Maybe<&2, S.TensorDesc>:
  match ts idx:
    case Nil{} _:
      None{}
    case Con{h, _} 0n:
      Some{h}
    case Con{_, t} 1n+pi:
      lookup_tensor(t, pi)

def cons_resolved(
  idx: Nat,
  td_opt: Maybe<&2, S.TensorDesc>,
  tail_opt: Maybe<&2, List<&2, S.BoundTensor>>
) -> Maybe<&2, List<&2, S.BoundTensor>>:
  match td_opt tail_opt:
    case Some{td} Some{more}:
      Some{S.BTensor{idx, td} <> more}
    case _ _:
      None{}

def resolve_tensor_refs(refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>) -> Maybe<&2, List<&2, S.BoundTensor>>:
  match refs:
    case Nil{}:
      Some{Nil{}}
    case Con{+idx, +rest}:
      cons_resolved(idx, lookup_tensor(ts, idx), resolve_tensor_refs(rest, ts))

def wrap_rcid(frame_cid: Nat, bound_opt: Maybe<&2, List<&2, S.BoundTensor>>) -> Maybe<&2, S.ResolvedCid>:
  match bound_opt:
    case None{}:
      None{}
    case Some{bound}:
      Some{S.RCid{frame_cid, bound}}

def resolved_frame_cid(+frame_cid: Nat, refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>) -> Maybe<&2, S.ResolvedCid>:
  wrap_rcid(frame_cid, resolve_tensor_refs(refs, ts))

# =============================================================================
# 4. PLANE C/D — `CWZM` PARTITION-SKIP + EARLY-BREAK, MERKLE, `.cwwal`, `LiveIvfStore` & `CWSP`
# =============================================================================

def zone_extend(z: S.Zone, +k: Nat) -> S.Zone:
  match z:
    case S.Zone{+mn, +mx}:
      S.Zone{S.nat_min(mn, k), S.nat_max(mx, k)}

def build_zone_loop(ks: List<&2, Nat>, z: S.Zone) -> S.Zone:
  match ks:
    case Nil{}:
      z
    case Con{+k, +rest}:
      build_zone_loop(rest, zone_extend(z, k))

def build_zone(+k0: Nat, ks: List<&2, Nat>) -> S.Zone:
  build_zone_loop(ks, S.Zone{k0, k0})

def zone_overlaps(z: S.Zone, +lo: Nat, +hi: Nat) -> Bool:
  match z:
    case S.Zone{+mn, +mx}:
      S.band(S.nat_le(lo, mx), S.nat_le(mn, hi))

# Models `zonemap.rs:430-437`:
#   Phase 1 (`skip_below_lo`): skip all pages with `max_key < lo` (equivalent to `partition_point(|z| z.max_key < lo)`)
#   Phase 2 (`take_until_gt_hi`): emit every page until `min_key > hi`, then break!
#   Notice Phase 2 omits `max_key >= lo` — SOUND ONLY under `spec_zones_disjoint_from` (`prev_max <= next_min <= next_max`)!
def take_gt_branch(le_hi: Bool, idx: Nat, tail_res: List<&2, Nat>) -> List<&2, Nat>:
  match le_hi:
    case False{}:
      Nil{}
    case True{}:
      idx <> tail_res

def take_until_gt_hi(zs: List<&2, S.Zone>, +idx: Nat, +hi: Nat) -> List<&2, Nat>:
  match zs:
    case Nil{}:
      Nil{}
    case Con{S.Zone{+mn, _}, +rest}:
      take_gt_branch(S.nat_le(mn, hi), idx, take_until_gt_hi(rest, 1n+idx, hi))

def qkr_branch(lt_lo: Bool, rest_res: List<&2, Nat>, cur_res: List<&2, Nat>) -> List<&2, Nat>:
  match lt_lo:
    case True{}:
      rest_res
    case False{}:
      cur_res

def query_key_range_at(zs: List<&2, S.Zone>, +idx: Nat, +lo: Nat, +hi: Nat) -> List<&2, Nat>:
  match zs:
    case Nil{}:
      Nil{}
    case Con{S.Zone{+mn, +mx}, +rest}:
      qkr_branch(S.nat_lt(mx, lo), query_key_range_at(rest, 1n+idx, lo, hi), take_until_gt_hi(S.Zone{mn, mx} <> rest, idx, hi))

def qkr_le_branch(ok: Bool, res: List<&2, Nat>) -> List<&2, Nat>:
  match ok:
    case False{}:
      Nil{}
    case True{}:
      res

def query_key_range_pages(zs: List<&2, S.Zone>, +lo: Nat, +hi: Nat) -> List<&2, Nat>:
  qkr_le_branch(S.nat_le(lo, hi), query_key_range_at(zs, 0n, lo, hi))

# Domain-separated RFC 6962 Dual-Tree Merkle Term Algebra (`0x00`..`0x04`)
def list_len_d(xs: List<&2, S.Digest>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_d(rest)

def merkle_pair_level(xs: List<&2, S.Digest>) -> List<&2, S.Digest>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{a, Nil{}}:
      [a]
    case Con{a, Con{b, rest}}:
      S.HNode01{a, b} <> merkle_pair_level(rest)

def merkle_reduce_fuel(fuel: Nat, xs: List<&2, S.Digest>) -> S.Digest:
  match fuel xs:
    case _ Nil{}:
      S.HEmpty{}
    case _ Con{only, Nil{}}:
      only
    case 0n Con{first, Con{_, _}}:
      first
    case 1n+pf Con{a, Con{b, rest}}:
      merkle_reduce_fuel(pf, S.HNode01{a, b} <> merkle_pair_level(rest))

def merkle_tree_digest(+xs: List<&2, S.Digest>) -> S.Digest:
  merkle_reduce_fuel(list_len_d(xs), xs)

def shard_merkle_branch(tensors: List<&2, S.Digest>, +frames: List<&2, S.Digest>) -> S.Digest:
  match tensors:
    case Nil{}:
      S.HRoot02{list_len_d(frames), merkle_tree_digest(frames)}
    case Con{+t0, +trest}:
      S.HRoot04{list_len_d(frames), merkle_tree_digest(frames), list_len_d(t0 <> trest), merkle_tree_digest(t0 <> trest)}

def shard_merkle_root(+frames: List<&2, S.Digest>, +tensors: List<&2, S.Digest>) -> S.Digest:
  shard_merkle_branch(tensors, frames)

# `.cwwal` Crash-Safe Recovery (`recover_torn_tail` flag + strict `seq` monotonicity!)
def wal_prepend(p: Nat, r: S.WalResult) -> S.WalResult:
  match r:
    case S.WalOk{ps}:
      S.WalOk{p <> ps}
    case S.WalErr{}:
      S.WalErr{}

def wal_seq_branch(ok: Bool, p: Nat, rest_res: S.WalResult) -> S.WalResult:
  match ok:
    case False{}:
      S.WalErr{}
    case True{}:
      wal_prepend(p, rest_res)

def wal_torn_branch(recover_torn: Bool) -> S.WalResult:
  match recover_torn:
    case True{}:
      S.WalOk{Nil{}}
    case False{}:
      S.WalErr{}

def wal_recover_mode(recs: List<&2, S.WalRec>, +exp_seq: Nat, +recover_torn: Bool) -> S.WalResult:
  match recs:
    case Nil{}:
      S.WalOk{Nil{}}
    case Con{S.WalRec{True{}, +seq, +p}, +rest}:
      wal_seq_branch(S.nat_eq(seq, exp_seq), p, wal_recover_mode(rest, 1n+exp_seq, recover_torn))
    case Con{S.WalRec{False{}, _, _}, Nil{}}:
      wal_torn_branch(recover_torn)
    case Con{S.WalRec{False{}, _, _}, Con{_, _}}:
      S.WalErr{}

# Sequential Tombstone & Re-Insertion Replay (`wal.rs` Fix F5)
def remove_cid_keep(eq: Bool, +x: Nat, rest: List<&2, Nat>) -> List<&2, Nat>:
  match eq:
    case True{}:
      rest
    case False{}:
      x <> rest

def remove_cid(xs: List<&2, Nat>, +target: Nat) -> List<&2, Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+x, +rest}:
      remove_cid_keep(S.nat_eq(x, target), x, remove_cid(rest, target))

def wal_replay_live(ops: List<&2, S.WalOp>, live: List<&2, Nat>) -> List<&2, Nat>:
  match ops:
    case Nil{}:
      live
    case Con{S.OpFrame{+c}, +rest}:
      wal_replay_live(rest, S.append_nat(remove_cid(live, c), [c]))
    case Con{S.OpTomb{+c}, +rest}:
      wal_replay_live(rest, remove_cid(live, c))

# `LiveIvfStore` Crash-Safe `.cwgen` Generation Compaction & Watermark Filter (`wal.rs:584-797`, Fix F10!)
def ivf_push_if(ge: Bool, v: Nat, rest: List<&2, Nat>) -> List<&2, Nat>:
  match ge:
    case True{}:
      v <> rest
    case False{}:
      rest

def ivf_wal_filter_from(recs: List<&2, S.IvfWalRec>, +applied_seq: Nat) -> List<&2, Nat>:
  match recs:
    case Nil{}:
      Nil{}
    case Con{S.IvfRec{+s, +v}, +rest}:
      ivf_push_if(S.nat_le(applied_seq, s), v, ivf_wal_filter_from(rest, applied_seq))

def live_ivf_recover(base_vecs: List<&2, Nat>, +applied_seq: Nat, wal_recs: List<&2, S.IvfWalRec>) -> List<&2, Nat>:
  S.append_nat(base_vecs, ivf_wal_filter_from(wal_recs, applied_seq))

# `StatePage` Strict `runtime_id` Pinning (`F7`) & HMAC-First Error Precedence (`shard.rs:758-785`, Fix F13!)
def check_statepage_runtime(expected_rt: Maybe<&2, Nat>, file_rt: Maybe<&2, Nat>) -> Bool:
  match expected_rt file_rt:
    case None{} _:
      True{}
    case Some{_} None{}:
      False{}
    case Some{exp} Some{got}:
      S.nat_eq(exp, got)

def statepage_parse_precedence(has_key: Bool, hmac_flag: Nat, mac_valid: Bool, hdr_valid: Bool) -> S.SpError:
  match has_key hmac_flag mac_valid hdr_valid:
    case True{} 1n True{} True{}:
      S.SpOk{}
    case True{} 1n True{} False{}:
      S.SpInvalidContract{}
    case True{} 1n False{} _:
      S.SpAuthMacMismatch{}
    case True{} 0n _ _:
      S.SpAuthMacMismatch{}
    case True{} 1n+(1n+_) _ _:
      S.SpAuthMacMismatch{}
    case False{} 1n _ _:
      S.SpAuthMacMismatch{}
    case False{} 0n _ True{}:
      S.SpOk{}
    case False{} 0n _ False{}:
      S.SpInvalidContract{}
    case False{} 1n+(1n+_) _ _:
      S.SpNonCanonicalFlags{}
