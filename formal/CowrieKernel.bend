# CowrieKernel.bend — Complete Executable Formal Reference Kernel for Cowrie v1.2
#
# Strengthened after adversarial underspecification & wrong-spec audit:
#   1. Plane D Causal Taint, Deep-Ledger Sticky Rule, Clean Liveness & Exact Biconditional Ed25519 Gate
#   2. Plane B 3-State Mealy Fence Sanitizer (`strip_esc` content-preserving) + Structured `R2` Table/Key Renderer
#   3. Plane A Canonical Integer Bijectivity (`verify == decode` + uniqueness) + Map Key Sorting & `UnusedDictKey`
#   4. Plane D `CWZM` Monotonic Early-Break Pruning, Dual-Tree Merkle Injectivity, `.cwwal` Sequential Tombstones
#      (fixing zombie tombstones on re-insert), and `StatePage` Strict `runtime_id` Pinning.
#
# Checked via `bend PROOF.bend --verdict` (Bend 2.0.35 + Lean-proven BendTT kernel) AND compiled
# via `bend CowrieKernel.bend -o cowrie_kernel_verified.mjs` for lockstep differential execution
# against `cowrie_core.wasm` and `libcowrie_core.so`.

import Base

# =============================================================================
# 0. BOOLEAN, NAT & LIST HELPERS
# =============================================================================

def bor(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def band(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def bnot(a: Bool) -> Bool:
  match a:
    case True{}:
      False{}
    case False{}:
      True{}

def implies(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      True{}

def bool_eq(a: Bool, b: Bool) -> Bool:
  match a b:
    case True{} True{}:
      True{}
    case False{} False{}:
      True{}
    case _ _:
      False{}

def nat_eq(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+_:
      False{}
    case 1n+_ 0n:
      False{}
    case 1n+pa 1n+pb:
      nat_eq(pa, pb)

def nat_le(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n _:
      True{}
    case 1n+_ 0n:
      False{}
    case 1n+pa 1n+pb:
      nat_le(pa, pb)

def nat_lt(a: Nat, b: Nat) -> Bool:
  nat_le(1n+a, b)

def nat_add(a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+nat_add(p, b)

def nat_sub(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n _:
      0n
    case a0 0n:
      a0
    case 1n+pa 1n+pb:
      nat_sub(pa, pb)

def nat_min(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n _:
      0n
    case 1n+_ 0n:
      0n
    case 1n+pa 1n+pb:
      1n+nat_min(pa, pb)

def nat_max(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n b0:
      b0
    case 1n+pa 0n:
      1n+pa
    case 1n+pa 1n+pb:
      1n+nat_max(pa, pb)

def is_nonempty_nat(xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{_, _}:
      True{}

def append_nat(xs: List<&2, Nat>, ys: List<&2, Nat>) -> List<&2, Nat>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_nat(t, ys)

def nat_list_eq(xs: List<&2, Nat>, ys: List<&2, Nat>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Con{hx, tx} Con{hy, ty}:
      band(nat_eq(hx, hy), nat_list_eq(tx, ty))
    case _ _:
      False{}

# =============================================================================
# 1. PLANE D — CAUSAL TAINT, DEEP STICKY LEDGER & SCOPED ED25519 GATE
# =============================================================================

type Auth is Data:
  Auth{clean_bits: Nat, ext: Bool, tainted: Bool}

def auth_clean_bits(a: Auth) -> Nat:
  match a:
    case Auth{c, _, _}:
      c

def auth_ext_bit(a: Auth) -> Bool:
  match a:
    case Auth{_, e, _}:
      e

def is_tainted(a: Auth) -> Bool:
  match a:
    case Auth{_, True{}, _}:
      True{}
    case Auth{_, False{}, t}:
      t

def set_tainted(a: Auth) -> Auth:
  match a:
    case Auth{c, e, _}:
      Auth{c, e, True{}}

def prop_apply(flag: Bool, base: Auth) -> Auth:
  match flag:
    case True{}:
      set_tainted(base)
    case False{}:
      base

def any_tainted(ps: List<&2, Auth>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+h, +t}:
      bor(is_tainted(h), any_tainted(t))

def propagate(+base: Auth, ps: List<&2, Auth>) -> Auth:
  prop_apply(bor(is_tainted(base), any_tainted(ps)), base)

def derive_chain(hops: List<&2, Auth>, cur: Auth) -> Auth:
  match hops:
    case Nil{}:
      cur
    case Con{+step_base, +rest}:
      derive_chain(rest, propagate(step_base, [cur]))

type LedgerNode is Data:
  LNode{cid: Nat, auth: Auth}

def append_lg(xs: List<&2, LedgerNode>, ys: List<&2, LedgerNode>) -> List<&2, LedgerNode>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_lg(t, ys)

def ledger_has_cid(lg: List<&2, LedgerNode>, +cid: Nat) -> Bool:
  match lg:
    case Nil{}:
      False{}
    case Con{LNode{+k, _}, +rest}:
      bor(nat_eq(k, cid), ledger_has_cid(rest, cid))

def select_lookup(eq: Bool, a: Auth, fallback: Maybe<&2, Auth>) -> Maybe<&2, Auth>:
  match eq:
    case True{}:
      Some{a}
    case False{}:
      fallback

def ledger_lookup(lg: List<&2, LedgerNode>, +cid: Nat) -> Maybe<&2, Auth>:
  match lg:
    case Nil{}:
      None{}
    case Con{LNode{+k, +a}, +rest}:
      select_lookup(nat_eq(k, cid), a, ledger_lookup(rest, cid))

def opt_auth_tainted(ma: Maybe<&2, Auth>) -> Bool:
  match ma:
    case None{}:
      True{}
    case Some{a}:
      is_tainted(a)

def existing_tainted(ma: Maybe<&2, Auth>) -> Bool:
  match ma:
    case None{}:
      False{}
    case Some{a}:
      is_tainted(a)

def parents_tainted(ps: List<&2, Nat>, +lg: List<&2, LedgerNode>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+p, +rest}:
      bor(opt_auth_tainted(ledger_lookup(lg, p)), parents_tainted(rest, lg))

def ledger_record(+lg: List<&2, LedgerNode>, +cid: Nat, +base: Auth, ps: List<&2, Nat>) -> Auth:
  old_t = existing_tainted(ledger_lookup(lg, cid))
  par_t = parents_tainted(ps, lg)
  prop_apply(bor(is_tainted(base), bor(old_t, par_t)), base)

type Action is Data:
  ReadOnly{}
  ShellExec{}
  CredRead{}
  FsWrite{}
  NetExfil{}

def is_privileged(act: Action) -> Bool:
  match act:
    case ReadOnly{}:
      False{}
    case ShellExec{}:
      True{}
    case CredRead{}:
      True{}
    case FsWrite{}:
      True{}
    case NetExfil{}:
      True{}

def action_eq(a: Action, b: Action) -> Bool:
  match a b:
    case ReadOnly{} ReadOnly{}:
      True{}
    case ShellExec{} ShellExec{}:
      True{}
    case CredRead{} CredRead{}:
      True{}
    case FsWrite{} FsWrite{}:
      True{}
    case NetExfil{} NetExfil{}:
      True{}
    case _ _:
      False{}

type DeclassToken is Data:
  Tok{cid: Nat, act: Action, expiry: Nat, sig_valid: Bool}

def token_matches(tok: DeclassToken, +cid: Nat, +act: Action, +now: Nat) -> Bool:
  match tok:
    case Tok{+tcid, +tact, +texp, +tsig}:
      band(tsig, band(nat_eq(tcid, cid), band(action_eq(tact, act), nat_le(now, texp))))

def has_valid_token(toks: List<&2, DeclassToken>, +cid: Nat, +act: Action, +now: Nat) -> Bool:
  match toks:
    case Nil{}:
      False{}
    case Con{+tk, +rest}:
      bor(token_matches(tk, cid, act, now), has_valid_token(rest, cid, act, now))

def arg_auth_branch(tainted: Bool, +cid: Nat, +act: Action, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  match tainted:
    case False{}:
      True{}
    case True{}:
      has_valid_token(toks, cid, act, now)

def arg_authorized(+cid: Nat, +lg: List<&2, LedgerNode>, +act: Action, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  arg_auth_branch(opt_auth_tainted(ledger_lookup(lg, cid)), cid, act, toks, now)

def all_args_authorized(args: List<&2, Nat>, +lg: List<&2, LedgerNode>, +act: Action, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  match args:
    case Nil{}:
      True{}
    case Con{+c, +rest}:
      band(arg_authorized(c, lg, act, toks, now), all_args_authorized(rest, lg, act, toks, now))

def check_priv_args(args: List<&2, Nat>, +lg: List<&2, LedgerNode>, +act: Action, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  match args:
    case Nil{}:
      False{}
    case Con{+c, +rest}:
      band(arg_authorized(c, lg, act, toks, now), all_args_authorized(rest, lg, act, toks, now))

def check_gate_priv(priv: Bool, +lg: List<&2, LedgerNode>, +act: Action, args: List<&2, Nat>, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  match priv:
    case False{}:
      True{}
    case True{}:
      check_priv_args(args, lg, act, toks, now)

def check_gate_signed(+lg: List<&2, LedgerNode>, +act: Action, args: List<&2, Nat>, +toks: List<&2, DeclassToken>, +now: Nat) -> Bool:
  check_gate_priv(is_privileged(act), lg, act, args, toks, now)

# =============================================================================
# 2. PLANE B — CONTENT-PRESERVING FENCE SANITIZER & STRUCTURED R2 RENDERER
# =============================================================================

type Sym is Data:
  Lt{}
  Gt{}
  Esc{}
  Other{}

type Run2 is Data:
  R0{}
  R1{}
  R2{}

def append_sym(xs: List<&2, Sym>, ys: List<&2, Sym>) -> List<&2, Sym>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_sym(t, ys)

def strip_esc(xs: List<&2, Sym>) -> List<&2, Sym>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{Esc{}, rest}:
      strip_esc(rest)
    case Con{Lt{}, rest}:
      Lt{} <> strip_esc(rest)
    case Con{Gt{}, rest}:
      Gt{} <> strip_esc(rest)
    case Con{Other{}, rest}:
      Other{} <> strip_esc(rest)

def count_esc(xs: List<&2, Sym>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{Esc{}, rest}:
      1n+count_esc(rest)
    case Con{Lt{}, rest}:
      count_esc(rest)
    case Con{Gt{}, rest}:
      count_esc(rest)
    case Con{Other{}, rest}:
      count_esc(rest)

def sanitize_step(xs: List<&2, Sym>, r_lt: Run2, r_gt: Run2) -> List<&2, Sym>:
  match xs r_lt r_gt:
    case Nil{} _ _:
      Nil{}
    case Con{Lt{}, +rest} R0{} _:
      Lt{} <> sanitize_step(rest, R1{}, R0{})
    case Con{Lt{}, +rest} R1{} _:
      Lt{} <> sanitize_step(rest, R2{}, R0{})
    case Con{Lt{}, +rest} R2{} _:
      Esc{} <> (Lt{} <> sanitize_step(rest, R1{}, R0{}))
    case Con{Gt{}, +rest} _ R0{}:
      Gt{} <> sanitize_step(rest, R0{}, R1{})
    case Con{Gt{}, +rest} _ R1{}:
      Gt{} <> sanitize_step(rest, R0{}, R2{})
    case Con{Gt{}, +rest} _ R2{}:
      Esc{} <> (Gt{} <> sanitize_step(rest, R0{}, R1{}))
    case Con{Esc{}, +rest} _ _:
      Esc{} <> sanitize_step(rest, R0{}, R0{})
    case Con{Other{}, +rest} _ _:
      Other{} <> sanitize_step(rest, R0{}, R0{})

def sanitize_fences(xs: List<&2, Sym>) -> List<&2, Sym>:
  sanitize_step(xs, R0{}, R0{})

def has_triple_step(ys: List<&2, Sym>, r_lt: Run2, r_gt: Run2) -> Bool:
  match ys r_lt r_gt:
    case Nil{} _ _:
      False{}
    case Con{Lt{}, +rest} R0{} _:
      has_triple_step(rest, R1{}, R0{})
    case Con{Lt{}, +rest} R1{} _:
      has_triple_step(rest, R2{}, R0{})
    case Con{Lt{}, _} R2{} _:
      True{}
    case Con{Gt{}, +rest} _ R0{}:
      has_triple_step(rest, R0{}, R1{})
    case Con{Gt{}, +rest} _ R1{}:
      has_triple_step(rest, R0{}, R2{})
    case Con{Gt{}, _} _ R2{}:
      True{}
    case Con{Esc{}, +rest} _ _:
      has_triple_step(rest, R0{}, R0{})
    case Con{Other{}, +rest} _ _:
      has_triple_step(rest, R0{}, R0{})

def has_triple_angle(ys: List<&2, Sym>) -> Bool:
  has_triple_step(ys, R0{}, R0{})

# Structured `R2` field renderer covering scalar key/values, hoisted Table headers (`cols`, Fix F4),
# AND `<T:dtype[dims]@space>` TensorRef wrappers (`space = "embed>>"` + closing `>`, Fix F8)!
type R2Field is Data:
  R2Scalar{key: List<&2, Sym>, val: List<&2, Sym>}
  R2Table{key: List<&2, Sym>, col_hdr: List<&2, Sym>, cell: List<&2, Sym>}
  R2TensorRef{key: List<&2, Sym>, space: List<&2, Sym>}

def render_r2_field(f: R2Field) -> List<&2, Sym>:
  match f:
    case R2Scalar{k, v}:
      append_sym(sanitize_fences(k), Other{} <> sanitize_fences(v))
    case R2Table{k, col, cell}:
      append_sym(sanitize_fences(k), Other{} <> append_sym(sanitize_fences(col), Other{} <> sanitize_fences(cell)))
    case R2TensorRef{k, sp}:
      append_sym(sanitize_fences(k), Other{} <> sanitize_fences(Lt{} <> (Other{} <> append_sym(sanitize_fences(sp), [Gt{}]))))

# =============================================================================
# 3. PLANE A — CANONICAL WIRE BIJECTIVITY + MAP SORTING & `UnusedDictKey`
# =============================================================================

type LogicalInt is Data:
  LPos{n: Nat}
  LNeg{m: Nat}
  LBigUint{delta: Nat}

type WireInt is Data:
  WFix{n: Nat}
  WTagIntPos{n: Nat}
  WTagIntNeg{m: Nat}
  WTagUint{u: Nat}

def encode_pos_branch(is_small: Bool, n: Nat) -> WireInt:
  match is_small:
    case True{}:
      WFix{n}
    case False{}:
      WTagIntPos{n}

def encode_canon_int(+lim: Nat, +ulim: Nat, v: LogicalInt) -> WireInt:
  match v:
    case LPos{+n}:
      encode_pos_branch(nat_lt(n, lim), n)
    case LNeg{m}:
      WTagIntNeg{m}
    case LBigUint{d}:
      WTagUint{nat_add(ulim, d)}

def decode_fix_branch(ok: Bool, n: Nat) -> Maybe<&2, LogicalInt>:
  match ok:
    case True{}:
      Some{LPos{n}}
    case False{}:
      None{}

def decode_pos_branch(too_small: Bool, n: Nat) -> Maybe<&2, LogicalInt>:
  match too_small:
    case True{}:
      None{}
    case False{}:
      Some{LPos{n}}

def decode_uint_branch(too_small: Bool, +ulim: Nat, +u: Nat) -> Maybe<&2, LogicalInt>:
  match too_small:
    case True{}:
      None{}
    case False{}:
      Some{LBigUint{nat_sub(u, ulim)}}

def decode_canon_int(+lim: Nat, +ulim: Nat, w: WireInt) -> Maybe<&2, LogicalInt>:
  match w:
    case WFix{+n}:
      decode_fix_branch(nat_lt(n, lim), n)
    case WTagIntPos{+n}:
      decode_pos_branch(nat_lt(n, lim), n)
    case WTagIntNeg{m}:
      Some{LNeg{m}}
    case WTagUint{+u}:
      decode_uint_branch(nat_lt(u, ulim), ulim, u)

def verify_canon_int_zero_alloc(+lim: Nat, +ulim: Nat, w: WireInt) -> Bool:
  match w:
    case WFix{n}:
      nat_lt(n, lim)
    case WTagIntPos{n}:
      bnot(nat_lt(n, lim))
    case WTagIntNeg{_}:
      True{}
    case WTagUint{u}:
      bnot(nat_lt(u, ulim))

def is_some_int(m: Maybe<&2, LogicalInt>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{_}:
      True{}

# Plane A Map Key Strictly-Ascending Index Verifier (`ERR_DUPLICATE_KEY` / `ERR_UNSORTED_MAP_KEYS` / `ERR_BAD_DICT_INDEX`)
def verify_map_keys_from(ks: List<&2, Nat>, +min_allowed: Nat, +nkeys: Nat) -> Bool:
  match ks:
    case Nil{}:
      True{}
    case Con{+k, +rest}:
      band(nat_le(min_allowed, k), band(nat_lt(k, nkeys), verify_map_keys_from(rest, 1n+k, nkeys)))

def verify_map_keys(+nkeys: Nat, ks: List<&2, Nat>) -> Bool:
  verify_map_keys_from(ks, 0n, nkeys)

# Plane A `KeyBitset` Unused-Key Verifier (`ERR_UNUSED_DICT_KEY`)
def list_contains_nat(xs: List<&2, Nat>, +target: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+x, +rest}:
      bor(nat_eq(x, target), list_contains_nat(rest, target))

def all_dict_keys_used(+nkeys: Nat, +refs: List<&2, Nat>) -> Bool:
  match nkeys:
    case 0n:
      True{}
    case 1n+pk:
      band(list_contains_nat(refs, pk), all_dict_keys_used(pk, refs))

# =============================================================================
# 4. PLANE D — `CWZM` EARLY-BREAK PRUNING, MERKLE INJECTIVITY, `.cwwal` & `CWSP`
# =============================================================================

type Zone is Data:
  Zone{min_k: Nat, max_k: Nat}

def zone_extend(z: Zone, +k: Nat) -> Zone:
  match z:
    case Zone{+mn, +mx}:
      Zone{nat_min(mn, k), nat_max(mx, k)}

def build_zone_loop(ks: List<&2, Nat>, z: Zone) -> Zone:
  match ks:
    case Nil{}:
      z
    case Con{+k, +rest}:
      build_zone_loop(rest, zone_extend(z, k))

def build_zone(+k0: Nat, ks: List<&2, Nat>) -> Zone:
  build_zone_loop(ks, Zone{k0, k0})

def in_range(+x: Nat, +lo: Nat, +hi: Nat) -> Bool:
  band(nat_le(lo, x), nat_le(x, hi))

def zone_overlaps(z: Zone, +lo: Nat, +hi: Nat) -> Bool:
  match z:
    case Zone{+mn, +mx}:
      band(nat_le(lo, mx), nat_le(mn, hi))

def list_has_in_range(ks: List<&2, Nat>, +lo: Nat, +hi: Nat) -> Bool:
  match ks:
    case Nil{}:
      False{}
    case Con{+k, +rest}:
      bor(in_range(k, lo, hi), list_has_in_range(rest, lo, hi))

def page_has_in_range(+k0: Nat, ks: List<&2, Nat>, +lo: Nat, +hi: Nat) -> Bool:
  bor(in_range(k0, lo, hi), list_has_in_range(ks, lo, hi))

# Multi-page `CWZM` sorted fence monotonicity & early-break pruning (`zonemap.rs:334-340`)
def zones_min_sorted(zs: List<&2, Zone>, +prev_min: Nat) -> Bool:
  match zs:
    case Nil{}:
      True{}
    case Con{Zone{+mn, _}, +rest}:
      band(nat_le(prev_min, mn), zones_min_sorted(rest, mn))

def push_if(cond: Bool, +idx: Nat, rest: List<&2, Nat>) -> List<&2, Nat>:
  match cond:
    case True{}:
      idx <> rest
    case False{}:
      rest

def filter_all_zones_at(zs: List<&2, Zone>, +idx: Nat, +lo: Nat, +hi: Nat) -> List<&2, Nat>:
  match zs:
    case Nil{}:
      Nil{}
    case Con{Zone{+mn, +mx}, +rest}:
      push_if(band(nat_le(lo, mx), nat_le(mn, hi)), idx, filter_all_zones_at(rest, 1n+idx, lo, hi))

# Note: Bend forbids mutual recursion, so inline the 2-way `le_hi` branch via non-recursive `prune_branch_nonrec`.
def prune_branch_nonrec(le_hi: Bool, ge_lo: Bool, +idx: Nat, tail_res: List<&2, Nat>) -> List<&2, Nat>:
  match le_hi:
    case False{}:
      Nil{}
    case True{}:
      push_if(ge_lo, idx, tail_res)

def prune_sorted_zones_at(zs: List<&2, Zone>, +idx: Nat, +lo: Nat, +hi: Nat) -> List<&2, Nat>:
  match zs:
    case Nil{}:
      Nil{}
    case Con{Zone{+mn, +mx}, +rest}:
      prune_branch_nonrec(nat_le(mn, hi), nat_le(lo, mx), idx, prune_sorted_zones_at(rest, 1n+idx, lo, hi))

# Domain-separated RFC 6962 Dual-Tree Merkle Term Algebra (`0x00`..`0x04`)
type Digest is Data:
  HEmpty{}
  HLeaf00{payload: Nat}
  HNode01{left: Digest, right: Digest}
  HRoot02{f_cnt: Nat, f_tree: Digest}
  HTensor03{byte_len: Nat, m4m: Nat}
  HRoot04{f_cnt: Nat, f_tree: Digest, t_cnt: Nat, t_tree: Digest}

def digest_eq(a: Digest, b: Digest) -> Bool:
  match a b:
    case HEmpty{} HEmpty{}:
      True{}
    case HLeaf00{x} HLeaf00{y}:
      nat_eq(x, y)
    case HNode01{l1, r1} HNode01{l2, r2}:
      band(digest_eq(l1, l2), digest_eq(r1, r2))
    case HRoot02{n1, t1} HRoot02{n2, t2}:
      band(nat_eq(n1, n2), digest_eq(t1, t2))
    case HTensor03{b1, m1} HTensor03{b2, m2}:
      band(nat_eq(b1, b2), nat_eq(m1, m2))
    case HRoot04{f1, ft1, tc1, tt1} HRoot04{f2, ft2, tc2, tt2}:
      band(nat_eq(f1, f2), band(digest_eq(ft1, ft2), band(nat_eq(tc1, tc2), digest_eq(tt1, tt2))))
    case _ _:
      False{}

def list_len_d(xs: List<&2, Digest>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_d(rest)

def merkle_pair_level(xs: List<&2, Digest>) -> List<&2, Digest>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{a, Nil{}}:
      [a]
    case Con{a, Con{b, rest}}:
      HNode01{a, b} <> merkle_pair_level(rest)

def merkle_reduce_fuel(fuel: Nat, xs: List<&2, Digest>) -> Digest:
  match fuel xs:
    case _ Nil{}:
      HEmpty{}
    case _ Con{only, Nil{}}:
      only
    case 0n Con{first, Con{_, _}}:
      first
    case 1n+pf Con{a, Con{b, rest}}:
      merkle_reduce_fuel(pf, HNode01{a, b} <> merkle_pair_level(rest))

def merkle_tree_digest(+xs: List<&2, Digest>) -> Digest:
  merkle_reduce_fuel(list_len_d(xs), xs)

def shard_merkle_branch(tensors: List<&2, Digest>, +frames: List<&2, Digest>) -> Digest:
  match tensors:
    case Nil{}:
      HRoot02{list_len_d(frames), merkle_tree_digest(frames)}
    case Con{+t0, +trest}:
      HRoot04{list_len_d(frames), merkle_tree_digest(frames), list_len_d(t0 <> trest), merkle_tree_digest(t0 <> trest)}

def shard_merkle_root(+frames: List<&2, Digest>, +tensors: List<&2, Digest>) -> Digest:
  shard_merkle_branch(tensors, frames)

# `.cwwal` Crash-Safe Recovery + Sequential Tombstone & Re-Insertion Replay (`wal.rs` Fix F5)
type WalRec is Data:
  WalRec{valid: Bool, payload: Nat}

type WalResult is Data:
  WalOk{live: List<&2, Nat>}
  WalErr{}

def wal_prepend(p: Nat, r: WalResult) -> WalResult:
  match r:
    case WalOk{ps}:
      WalOk{p <> ps}
    case WalErr{}:
      WalErr{}

def wal_recover(recs: List<&2, WalRec>) -> WalResult:
  match recs:
    case Nil{}:
      WalOk{Nil{}}
    case Con{WalRec{True{}, +p}, +rest}:
      wal_prepend(p, wal_recover(rest))
    case Con{WalRec{False{}, _}, Nil{}}:
      WalOk{Nil{}}
    case Con{WalRec{False{}, _}, Con{_, _}}:
      WalErr{}

def append_valid_prefix(payloads: List<&2, Nat>, tail: List<&2, WalRec>) -> List<&2, WalRec>:
  match payloads:
    case Nil{}:
      tail
    case Con{+p, +rest}:
      WalRec{True{}, p} <> append_valid_prefix(rest, tail)

type WalOp is Data:
  OpFrame{cid: Nat}
  OpTomb{cid: Nat}

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
      remove_cid_keep(nat_eq(x, target), x, remove_cid(rest, target))

def wal_replay_live(ops: List<&2, WalOp>, live: List<&2, Nat>) -> List<&2, Nat>:
  match ops:
    case Nil{}:
      live
    case Con{OpFrame{+c}, +rest}:
      wal_replay_live(rest, append_nat(remove_cid(live, c), [c]))
    case Con{OpTomb{+c}, +rest}:
      wal_replay_live(rest, remove_cid(live, c))

# `StatePage` Strict `runtime_id` Contract Pinning (`shard.rs` Fix F7)
def check_statepage_runtime(expected_rt: Maybe<&2, Nat>, file_rt: Maybe<&2, Nat>) -> Bool:
  match expected_rt file_rt:
    case None{} _:
      True{}
    case Some{_} None{}:
      # Empty/missing runtime_id in StatePage MUST fail closed when caller specifies `expected_rt`!
      False{}
    case Some{exp} Some{got}:
      nat_eq(exp, got)
