# SPEC.bend — Frozen Independent Specification Types, Connectives & Predicates for Cowrie v1.2
#
# Architectural anti-TCB-contamination boundary:
#   ALL logical connectives, arithmetic relations, list membership/subsequence predicates,
#   forbidden-sequence detectors, wire-boundary predicates, sorted-zone predicates, and
#   structural digest equalities live HERE in frozen `SPEC.bend` (NEVER inside `CowrieKernel.bend`).
#   Mutating `CowrieKernel.bend` can NEVER vacuously alter any law's specification predicate!

import Base

# =============================================================================
# 0. FROZEN DATA TYPES SHARED BY SPECIFICATION AND KERNEL
# =============================================================================

type Auth is Data:
  Auth{clean_bits: Nat, ext: Bool, tainted: Bool}

type LedgerNode is Data:
  LNode{cid: Nat, auth: Auth}

type Action is Data:
  ReadOnly{}
  ShellExec{}
  CredRead{}
  FsWrite{}
  NetExfil{}

type DeclassToken is Data:
  Tok{cid: Nat, act: Action, expiry: Nat, sig_valid: Bool}

type Sym is Data:
  Lt{}
  Gt{}
  Pipe{}
  Esc{}
  Hidden{}
  Other{}

type Run2 is Data:
  R0{}
  R1{}
  R2{}

type PrevSym is Data:
  PLt{}
  PPipe{}
  POther{}

type R2Field is Data:
  R2Scalar{key: List<&2, Sym>, val: List<&2, Sym>}
  R2Table{key: List<&2, Sym>, col_hdr: List<&2, Sym>, cell1: List<&2, Sym>, cell2: List<&2, Sym>}
  R2TensorRef{key: List<&2, Sym>, space: List<&2, Sym>}

type TableCell is Data:
  CellNull{}
  CellMissing{}
  CellStr{raw: List<&2, Sym>}

type CellTag is Data:
  OutNull{}
  OutMissing{}
  OutEmptyQuoted{}
  OutQuotedStr{s: List<&2, Sym>}
  OutPlainStr{s: List<&2, Sym>}

type LensMode is Data:
  CompactHeader{}
  FullMealyFence{}

type PathSeg is Data:
  PKey{k: Nat}
  PIdx{i: Nat}

type ValTree is Data:
  VAtom{n: Nat}
  VArr{items: List<&2, ValTree>}
  VMap{entries: List<&2, MapEntry>}

type MapEntry is Data:
  MEntry{k: Nat, v: ValTree}

type LogicalInt is Data:
  LPos{n: Nat}
  LNeg{m: Nat}
  LBigUint{delta: Nat}

type WireInt is Data:
  WFix{n: Nat}
  WTagIntPos{n: Nat}
  WTagIntNeg{m: Nat}
  WTagUint{u: Nat}

type FloatKind is Data:
  FPosZero{}
  FNegZero{}
  FFinite{bits: Nat}
  FInf{pos: Bool}
  FCanonQNaN{}
  FNonCanonNaN{payload: Nat}

type SentinelVal is Data:
  SVPlain{tag: Nat}
  SVBytes{raw: Nat}
  SVFloatSpecial{code: Nat}
  SVTensor{arena: Nat, dtype: Nat}
  SVMapPlain{payload: Nat}
  SVMapReserved{key_kind: Nat, payload: Nat}

type JsonSentinel is Data:
  JPlain{tag: Nat}
  JBytesObj{ok: Bool, raw: Nat}
  JFloatObj{ok: Bool, code: Nat}
  JTensorObj{ok: Bool, arena: Nat, dtype: Nat}
  JMapPlain{payload: Nat}
  JMapWrap{ok: Bool, key_kind: Nat, payload: Nat}

type TensorDesc is Data:
  TDesc{byte_len: Nat, merkle4m: Nat}

type BoundTensor is Data:
  BTensor{idx: Nat, td: TensorDesc}

type ResolvedCid is Data:
  RCid{frame_cid: Nat, bound_tensors: List<&2, BoundTensor>}

type Zone is Data:
  Zone{min_k: Nat, max_k: Nat}

type Digest is Data:
  HEmpty{}
  HLeaf00{payload: Nat}
  HNode01{left: Digest, right: Digest}
  HRoot02{f_cnt: Nat, f_tree: Digest}
  HTensor03{byte_len: Nat, m4m: Nat}
  HRoot04{f_cnt: Nat, f_tree: Digest, t_cnt: Nat, t_tree: Digest}

type WalRec is Data:
  WalRec{valid: Bool, seq: Nat, payload: Nat}

type WalResult is Data:
  WalOk{live: List<&2, Nat>}
  WalErr{}

type WalOp is Data:
  OpFrame{cid: Nat}
  OpTomb{cid: Nat}

type IvfWalRec is Data:
  IvfRec{seq: Nat, vec_id: Nat}

type SpError is Data:
  SpOk{}
  SpAuthMacMismatch{}
  SpNonCanonicalFlags{}
  SpInvalidContract{}

# =============================================================================
# 1. FROZEN BOOLEAN, NAT & LIST CONNECTIVES
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

def nat_mem(xs: List<&2, Nat>, +target: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+x, +rest}:
      bor(nat_eq(x, target), nat_mem(rest, target))

def nat_count_step(eq: Bool, rest_cnt: Nat) -> Nat:
  match eq:
    case True{}:
      1n+rest_cnt
    case False{}:
      rest_cnt

def nat_count(xs: List<&2, Nat>, +target: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{+x, +rest}:
      nat_count_step(nat_eq(x, target), nat_count(rest, target))

def append_lg(xs: List<&2, LedgerNode>, ys: List<&2, LedgerNode>) -> List<&2, LedgerNode>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_lg(t, ys)

def append_toks(xs: List<&2, DeclassToken>, ys: List<&2, DeclassToken>) -> List<&2, DeclassToken>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_toks(t, ys)

def append_ops(xs: List<&2, WalOp>, ys: List<&2, WalOp>) -> List<&2, WalOp>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_ops(t, ys)

def append_sym(xs: List<&2, Sym>, ys: List<&2, Sym>) -> List<&2, Sym>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_sym(t, ys)

def append_ivf(xs: List<&2, IvfWalRec>, ys: List<&2, IvfWalRec>) -> List<&2, IvfWalRec>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append_ivf(t, ys)

# =============================================================================
# 2. FROZEN PLANE D TRUST & GATE SPECIFICATIONS
# =============================================================================

def spec_tainted(a: Auth) -> Bool:
  match a:
    case Auth{_, e, t}:
      bor(e, t)

def spec_auth_clean_bits(a: Auth) -> Nat:
  match a:
    case Auth{c, _, _}:
      c

def spec_auth_ext_bit(a: Auth) -> Bool:
  match a:
    case Auth{_, e, _}:
      e

def spec_auth_tainted_bit(a: Auth) -> Bool:
  match a:
    case Auth{_, _, t}:
      t

def spec_set_tainted(a: Auth) -> Auth:
  match a:
    case Auth{c, e, _}:
      Auth{c, e, True{}}

def spec_any_tainted(ps: List<&2, Auth>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+h, +t}:
      bor(spec_tainted(h), spec_any_tainted(t))

def spec_ledger_has_cid(lg: List<&2, LedgerNode>, +cid: Nat) -> Bool:
  match lg:
    case Nil{}:
      False{}
    case Con{LNode{+k, _}, +rest}:
      bor(nat_eq(k, cid), spec_ledger_has_cid(rest, cid))

def spec_select_lookup(eq: Bool, a: Auth, fb: Maybe<&2, Auth>) -> Maybe<&2, Auth>:
  match eq:
    case True{}:
      Some{a}
    case False{}:
      fb

def spec_ledger_lookup(lg: List<&2, LedgerNode>, +cid: Nat) -> Maybe<&2, Auth>:
  match lg:
    case Nil{}:
      None{}
    case Con{LNode{+k, +a}, +rest}:
      spec_select_lookup(nat_eq(k, cid), a, spec_ledger_lookup(rest, cid))

def spec_opt_tainted(ma: Maybe<&2, Auth>) -> Bool:
  match ma:
    case None{}:
      True{}
    case Some{a}:
      spec_tainted(a)

def spec_existing_tainted(ma: Maybe<&2, Auth>) -> Bool:
  match ma:
    case None{}:
      False{}
    case Some{a}:
      spec_tainted(a)

def spec_parents_tainted(ps: List<&2, Nat>, +lg: List<&2, LedgerNode>) -> Bool:
  match ps:
    case Nil{}:
      False{}
    case Con{+p, +rest}:
      bor(spec_opt_tainted(spec_ledger_lookup(lg, p)), spec_parents_tainted(rest, lg))

def spec_is_privileged(act: Action) -> Bool:
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

def spec_action_eq(a: Action, b: Action) -> Bool:
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

def spec_tok_valid(tok: DeclassToken, +cid: Nat, +act: Action, +now: Nat) -> Bool:
  match tok:
    case Tok{+tcid, +tact, +texp, +tsig}:
      band(tsig, band(nat_eq(tcid, cid), band(spec_action_eq(tact, act), nat_le(now, texp))))

def spec_has_valid_tok(toks: List<&2, DeclassToken>, +cid: Nat, +act: Action, +now: Nat) -> Bool:
  match toks:
    case Nil{}:
      False{}
    case Con{+tk, +rest}:
      bor(spec_tok_valid(tk, cid, act, now), spec_has_valid_tok(rest, cid, act, now))

def spec_arg_ok_branch(tainted: Bool, +cid: Nat, +act: Action, +toks: List<&2, DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match tainted:
    case False{}:
      True{}
    case True{}:
      band(has_pk, spec_has_valid_tok(toks, cid, act, now))

def spec_arg_ok(+cid: Nat, +lg: List<&2, LedgerNode>, +act: Action, +toks: List<&2, DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  spec_arg_ok_branch(spec_opt_tainted(spec_ledger_lookup(lg, cid)), cid, act, toks, has_pk, now)

def spec_all_args_ok(args: List<&2, Nat>, +lg: List<&2, LedgerNode>, +act: Action, +toks: List<&2, DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  match args:
    case Nil{}:
      True{}
    case Con{+c, +rest}:
      band(spec_arg_ok(c, lg, act, toks, has_pk, now), spec_all_args_ok(rest, lg, act, toks, has_pk, now))

def spec_valid_header_meta(+low4: Nat, +res3: Nat, +epi: Nat, +flags: Nat) -> Bool:
  band(
    nat_eq(flags, 0n),
    band(
      nat_eq(res3, 0n),
      band(
        band(nat_le(1n, low4), nat_le(low4, 15n)),
        band(nat_le(1n, epi), nat_le(epi, 3n))
      )
    )
  )

def spec_gate(+lg: List<&2, LedgerNode>, +act: Action, +args: List<&2, Nat>, +toks: List<&2, DeclassToken>, +has_pk: Bool, +now: Nat) -> Bool:
  bor(bnot(spec_is_privileged(act)), band(is_nonempty_nat(args), spec_all_args_ok(args, lg, act, toks, has_pk, now)))

def spec_action_to_code(act: Action) -> Nat:
  match act:
    case ReadOnly{}:
      0n
    case ShellExec{}:
      1n
    case CredRead{}:
      2n
    case FsWrite{}:
      3n
    case NetExfil{}:
      4n

def spec_classify_ro_branch(is_ro: Bool) -> Action:
  match is_ro:
    case True{}:
      ReadOnly{}
    case False{}:
      ShellExec{}

def spec_classify_mcp_tool(
  pol: Maybe<&2, Action>,
  verb: Maybe<&2, Action>,
  allow_ro: Bool,
  builtin_ro: Bool,
  trust_ann: Bool,
  hint: Bool
) -> Action:
  match pol verb:
    case Some{act} _:
      act
    case None{} Some{vact}:
      vact
    case None{} None{}:
      spec_classify_ro_branch(bor(allow_ro, bor(builtin_ro, band(trust_ann, hint))))

# =============================================================================
# 3. FROZEN PLANE B FORBIDDEN-SEQUENCE & SUBSEQUENCE SPECIFICATIONS
# =============================================================================

def sym_eq(a: Sym, b: Sym) -> Bool:
  match a b:
    case Lt{} Lt{}:
      True{}
    case Gt{} Gt{}:
      True{}
    case Pipe{} Pipe{}:
      True{}
    case Esc{} Esc{}:
      True{}
    case Hidden{} Hidden{}:
      True{}
    case Other{} Other{}:
      True{}
    case _ _:
      False{}

def sym_list_eq(xs: List<&2, Sym>, ys: List<&2, Sym>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Con{hx, tx} Con{hy, ty}:
      band(sym_eq(hx, hy), sym_list_eq(tx, ty))
    case _ _:
      False{}

def strip_hidden(xs: List<&2, Sym>) -> List<&2, Sym>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{Hidden{}, rest}:
      strip_hidden(rest)
    case Con{Lt{}, rest}:
      Lt{} <> strip_hidden(rest)
    case Con{Gt{}, rest}:
      Gt{} <> strip_hidden(rest)
    case Con{Pipe{}, rest}:
      Pipe{} <> strip_hidden(rest)
    case Con{Esc{}, rest}:
      Esc{} <> strip_hidden(rest)
    case Con{Other{}, rest}:
      Other{} <> strip_hidden(rest)

def strip_esc(xs: List<&2, Sym>) -> List<&2, Sym>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{Esc{}, rest}:
      strip_esc(rest)
    case Con{Hidden{}, rest}:
      Hidden{} <> strip_esc(rest)
    case Con{Lt{}, rest}:
      Lt{} <> strip_esc(rest)
    case Con{Gt{}, rest}:
      Gt{} <> strip_esc(rest)
    case Con{Pipe{}, rest}:
      Pipe{} <> strip_esc(rest)
    case Con{Other{}, rest}:
      Other{} <> strip_esc(rest)

def sym_subseq_branch(eq: Bool, r_hit: Bool, r_skip: Bool) -> Bool:
  match eq:
    case True{}:
      r_hit
    case False{}:
      r_skip

def sym_subseq(sub: List<&2, Sym>, sup: List<&2, Sym>) -> Bool:
  match sub sup:
    case Nil{} _:
      True{}
    case Con{_, _} Nil{}:
      False{}
    case Con{+x, +xt} Con{+y, +yt}:
      sym_subseq_branch(sym_eq(x, y), sym_subseq(xt, yt), sym_subseq(x <> xt, yt))

# Independent frozen forbidden-pattern detector:
# True iff `ys` contains `<<<`, `>>>`, `<|`, `|>`, OR any `Hidden{}` spoof char!
def has_forbidden_step(ys: List<&2, Sym>, r_lt: Run2, r_gt: Run2, prev: PrevSym) -> Bool:
  match ys r_lt r_gt prev:
    case Nil{} _ _ _:
      False{}
    case Con{Hidden{}, _} _ _ _:
      True{}
    case Con{Lt{}, +rest} R0{} _ _:
      has_forbidden_step(rest, R1{}, R0{}, PLt{})
    case Con{Lt{}, +rest} R1{} _ _:
      has_forbidden_step(rest, R2{}, R0{}, PLt{})
    case Con{Lt{}, _} R2{} _ _:
      True{}
    case Con{Gt{}, _} _ _ PPipe{}:
      True{}
    case Con{Gt{}, +rest} _ R0{} PLt{}:
      has_forbidden_step(rest, R0{}, R1{}, POther{})
    case Con{Gt{}, +rest} _ R0{} POther{}:
      has_forbidden_step(rest, R0{}, R1{}, POther{})
    case Con{Gt{}, +rest} _ R1{} PLt{}:
      has_forbidden_step(rest, R0{}, R2{}, POther{})
    case Con{Gt{}, +rest} _ R1{} POther{}:
      has_forbidden_step(rest, R0{}, R2{}, POther{})
    case Con{Gt{}, _} _ R2{} PLt{}:
      True{}
    case Con{Gt{}, _} _ R2{} POther{}:
      True{}
    case Con{Pipe{}, _} _ _ PLt{}:
      True{}
    case Con{Pipe{}, +rest} _ _ PPipe{}:
      has_forbidden_step(rest, R0{}, R0{}, PPipe{})
    case Con{Pipe{}, +rest} _ _ POther{}:
      has_forbidden_step(rest, R0{}, R0{}, PPipe{})
    case Con{Esc{}, +rest} _ _ _:
      has_forbidden_step(rest, R0{}, R0{}, POther{})
    case Con{Other{}, +rest} _ _ _:
      has_forbidden_step(rest, R0{}, R0{}, POther{})

def has_forbidden(ys: List<&2, Sym>) -> Bool:
  has_forbidden_step(ys, R0{}, R0{}, POther{})

def take_sym(n: Nat, xs: List<&2, Sym>) -> List<&2, Sym>:
  match n xs:
    case 0n _:
      Nil{}
    case 1n+_ Nil{}:
      Nil{}
    case 1n+pn Con{h, t}:
      h <> take_sym(pn, t)

def cell_tag_eq(a: CellTag, b: CellTag) -> Bool:
  match a b:
    case OutNull{} OutNull{}:
      True{}
    case OutMissing{} OutMissing{}:
      True{}
    case OutEmptyQuoted{} OutEmptyQuoted{}:
      True{}
    case OutQuotedStr{x} OutQuotedStr{y}:
      sym_list_eq(x, y)
    case OutPlainStr{x} OutPlainStr{y}:
      sym_list_eq(x, y)
    case _ _:
      False{}

def cell_tag_has_forbidden(ct: CellTag) -> Bool:
  match ct:
    case OutNull{}:
      False{}
    case OutMissing{}:
      False{}
    case OutEmptyQuoted{}:
      False{}
    case OutQuotedStr{s}:
      has_forbidden(s)
    case OutPlainStr{s}:
      has_forbidden(s)

def cell_tag_content(ct: CellTag) -> List<&2, Sym>:
  match ct:
    case OutNull{}:
      Nil{}
    case OutMissing{}:
      Nil{}
    case OutEmptyQuoted{}:
      Nil{}
    case OutQuotedStr{s}:
      s
    case OutPlainStr{s}:
      s

def append_path(a: List<&2, PathSeg>, b: List<&2, PathSeg>) -> List<&2, PathSeg>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> append_path(t, b)

def append_map(a: List<&2, MapEntry>, b: List<&2, MapEntry>) -> List<&2, MapEntry>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> append_map(t, b)

def append_val(a: List<&2, ValTree>, b: List<&2, ValTree>) -> List<&2, ValTree>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> append_val(t, b)

def list_len_val(xs: List<&2, ValTree>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_val(rest)

def spec_map_has_key(es: List<&2, MapEntry>, +key: Nat) -> Bool:
  match es:
    case Nil{}:
      False{}
    case Con{MEntry{+ek, _}, +rest}:
      bor(nat_eq(ek, key), spec_map_has_key(rest, key))

def spec_select_val(eq: Bool, v: ValTree, fb: Maybe<&2, ValTree>) -> Maybe<&2, ValTree>:
  match eq:
    case True{}:
      Some{v}
    case False{}:
      fb

def spec_map_find_val(es: List<&2, MapEntry>, +key: Nat) -> Maybe<&2, ValTree>:
  match es:
    case Nil{}:
      None{}
    case Con{MEntry{+ek, v}, rest}:
      spec_select_val(nat_eq(ek, key), v, spec_map_find_val(rest, key))

def spec_list_nth_val(items: List<&2, ValTree>, idx: Nat) -> Maybe<&2, ValTree>:
  match items idx:
    case Nil{} _:
      None{}
    case Con{h, _} 0n:
      Some{h}
    case Con{_, t} 1n+pi:
      spec_list_nth_val(t, pi)

def spec_extract_step(cur: ValTree, seg: PathSeg) -> Maybe<&2, ValTree>:
  match cur seg:
    case VMap{es} PKey{k}:
      spec_map_find_val(es, k)
    case VArr{xs} PIdx{i}:
      spec_list_nth_val(xs, i)
    case _ _:
      None{}

def spec_extract_opt_step(cur: Maybe<&2, ValTree>, seg: PathSeg) -> Maybe<&2, ValTree>:
  match cur:
    case None{}:
      None{}
    case Some{v}:
      spec_extract_step(v, seg)

def spec_extract_field_opt(path: List<&2, PathSeg>, cur: Maybe<&2, ValTree>) -> Maybe<&2, ValTree>:
  match path:
    case Nil{}:
      cur
    case Con{seg, rest}:
      spec_extract_field_opt(rest, spec_extract_opt_step(cur, seg))

def spec_extract_field(cur: ValTree, path: List<&2, PathSeg>) -> Maybe<&2, ValTree>:
  spec_extract_field_opt(path, Some{cur})

def spec_maybe_val_is_atom(m: Maybe<&2, ValTree>, +a: Nat) -> Bool:
  match m:
    case Some{VAtom{x}}:
      nat_eq(x, a)
    case _:
      False{}

# =============================================================================
# 4. FROZEN PLANE A CANONICAL WIRE & MAP SPECIFICATIONS
# =============================================================================

def wire_int_eq(a: WireInt, b: WireInt) -> Bool:
  match a b:
    case WFix{x} WFix{y}:
      nat_eq(x, y)
    case WTagIntPos{x} WTagIntPos{y}:
      nat_eq(x, y)
    case WTagIntNeg{x} WTagIntNeg{y}:
      nat_eq(x, y)
    case WTagUint{x} WTagUint{y}:
      nat_eq(x, y)
    case _ _:
      False{}

def spec_valid_wire(+lim: Nat, +ulim: Nat, w: WireInt) -> Bool:
  match w:
    case WFix{n}:
      nat_lt(n, lim)
    case WTagIntPos{n}:
      bnot(nat_lt(n, lim))
    case WTagIntNeg{_}:
      True{}
    case WTagUint{u}:
      bnot(nat_lt(u, ulim))

def spec_is_some_int(m: Maybe<&2, LogicalInt>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{_}:
      True{}

def spec_canonicalize_f64(f: FloatKind) -> FloatKind:
  match f:
    case FPosZero{}:
      FPosZero{}
    case FNegZero{}:
      FPosZero{}
    case FFinite{b}:
      FFinite{b}
    case FInf{p}:
      FInf{p}
    case FCanonQNaN{}:
      FCanonQNaN{}
    case FNonCanonNaN{_}:
      FCanonQNaN{}

def spec_verify_canon_f64(f: FloatKind) -> Bool:
  match f:
    case FPosZero{}:
      True{}
    case FNegZero{}:
      False{}
    case FFinite{_}:
      True{}
    case FInf{_}:
      True{}
    case FCanonQNaN{}:
      True{}
    case FNonCanonNaN{_}:
      False{}

def append_tdesc(a: List<&2, TensorDesc>, b: List<&2, TensorDesc>) -> List<&2, TensorDesc>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> append_tdesc(t, b)

def list_len_tdesc(xs: List<&2, TensorDesc>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_tdesc(rest)

def spec_lookup_tensor(ts: List<&2, TensorDesc>, idx: Nat) -> Maybe<&2, TensorDesc>:
  match ts idx:
    case Nil{} _:
      None{}
    case Con{h, _} 0n:
      Some{h}
    case Con{_, t} 1n+pi:
      spec_lookup_tensor(t, pi)

def spec_cons_resolved(idx: Nat, td_opt: Maybe<&2, TensorDesc>, tail_opt: Maybe<&2, List<&2, BoundTensor>>) -> Maybe<&2, List<&2, BoundTensor>>:
  match td_opt tail_opt:
    case Some{td} Some{more}:
      Some{BTensor{idx, td} <> more}
    case _ _:
      None{}

def spec_resolve_tensor_refs(refs: List<&2, Nat>, +ts: List<&2, TensorDesc>) -> Maybe<&2, List<&2, BoundTensor>>:
  match refs:
    case Nil{}:
      Some{Nil{}}
    case Con{+idx, +rest}:
      spec_cons_resolved(idx, spec_lookup_tensor(ts, idx), spec_resolve_tensor_refs(rest, ts))

def spec_wrap_rcid(fcid: Nat, bound_opt: Maybe<&2, List<&2, BoundTensor>>) -> Maybe<&2, ResolvedCid>:
  match bound_opt:
    case None{}:
      None{}
    case Some{bound}:
      Some{RCid{fcid, bound}}

def spec_resolved_frame_cid(+fcid: Nat, refs: List<&2, Nat>, +ts: List<&2, TensorDesc>) -> Maybe<&2, ResolvedCid>:
  spec_wrap_rcid(fcid, spec_resolve_tensor_refs(refs, ts))

def rcid_is_some(m: Maybe<&2, ResolvedCid>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{_}:
      True{}

def spec_strictly_asc_from(ks: List<&2, Nat>, +min_a: Nat, +nkeys: Nat) -> Bool:
  match ks:
    case Nil{}:
      True{}
    case Con{+k, +rest}:
      band(nat_le(min_a, k), band(nat_lt(k, nkeys), spec_strictly_asc_from(rest, 1n+k, nkeys)))

def spec_all_lt_in(+nkeys: Nat, +refs: List<&2, Nat>) -> Bool:
  match nkeys:
    case 0n:
      True{}
    case 1n+pk:
      band(nat_mem(refs, pk), spec_all_lt_in(pk, refs))

def fkind_eq(a: FloatKind, b: FloatKind) -> Bool:
  match a b:
    case FPosZero{} FPosZero{}:
      True{}
    case FNegZero{} FNegZero{}:
      True{}
    case FFinite{x} FFinite{y}:
      nat_eq(x, y)
    case FInf{x} FInf{y}:
      bool_eq(x, y)
    case FCanonQNaN{} FCanonQNaN{}:
      True{}
    case FNonCanonNaN{x} FNonCanonNaN{y}:
      nat_eq(x, y)
    case _ _:
      False{}

def rcid_list_eq(xs: List<&2, BoundTensor>, ys: List<&2, BoundTensor>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Con{BTensor{i1, TDesc{l1, m1}}, t1} Con{BTensor{i2, TDesc{l2, m2}}, t2}:
      band(nat_eq(i1, i2), band(nat_eq(l1, l2), band(nat_eq(m1, m2), rcid_list_eq(t1, t2))))
    case _ _:
      False{}

def rcid_eq(a: Maybe<&2, ResolvedCid>, b: Maybe<&2, ResolvedCid>) -> Bool:
  match a b:
    case None{} None{}:
      True{}
    case Some{RCid{f1, ts1}} Some{RCid{f2, ts2}}:
      band(nat_eq(f1, f2), rcid_list_eq(ts1, ts2))
    case _ _:
      False{}

# =============================================================================
# 5. FROZEN PLANE C/D ZONEMAP, MERKLE, WAL & STATEPAGE SPECIFICATIONS
# =============================================================================

def spec_in_range(+x: Nat, +lo: Nat, +hi: Nat) -> Bool:
  band(nat_le(lo, x), nat_le(x, hi))

def spec_list_has_in_range(ks: List<&2, Nat>, +lo: Nat, +hi: Nat) -> Bool:
  match ks:
    case Nil{}:
      False{}
    case Con{+k, +rest}:
      bor(spec_in_range(k, lo, hi), spec_list_has_in_range(rest, lo, hi))

def spec_page_has_in_range(+k0: Nat, ks: List<&2, Nat>, +lo: Nat, +hi: Nat) -> Bool:
  bor(spec_in_range(k0, lo, hi), spec_list_has_in_range(ks, lo, hi))

def spec_zones_disjoint_from(zs: List<&2, Zone>, +prev_max: Nat) -> Bool:
  match zs:
    case Nil{}:
      True{}
    case Con{Zone{+mn, +mx}, +rest}:
      band(nat_le(prev_max, mn), band(nat_le(mn, mx), spec_zones_disjoint_from(rest, mx)))

def spec_push_if(cond: Bool, +idx: Nat, rest: List<&2, Nat>) -> List<&2, Nat>:
  match cond:
    case True{}:
      idx <> rest
    case False{}:
      rest

def spec_filter_zones_at(zs: List<&2, Zone>, +idx: Nat, +lo: Nat, +hi: Nat) -> List<&2, Nat>:
  match zs:
    case Nil{}:
      Nil{}
    case Con{Zone{+mn, +mx}, +rest}:
      spec_push_if(band(nat_le(lo, mx), nat_le(mn, hi)), idx, spec_filter_zones_at(rest, 1n+idx, lo, hi))

def spec_digest_eq(a: Digest, b: Digest) -> Bool:
  match a b:
    case HEmpty{} HEmpty{}:
      True{}
    case HLeaf00{x} HLeaf00{y}:
      nat_eq(x, y)
    case HNode01{l1, r1} HNode01{l2, r2}:
      band(spec_digest_eq(l1, l2), spec_digest_eq(r1, r2))
    case HRoot02{n1, t1} HRoot02{n2, t2}:
      band(nat_eq(n1, n2), spec_digest_eq(t1, t2))
    case HTensor03{b1, m1} HTensor03{b2, m2}:
      band(nat_eq(b1, b2), nat_eq(m1, m2))
    case HRoot04{f1, ft1, tc1, tt1} HRoot04{f2, ft2, tc2, tt2}:
      band(nat_eq(f1, f2), band(spec_digest_eq(ft1, ft2), band(nat_eq(tc1, tc2), spec_digest_eq(tt1, tt2))))
    case _ _:
      False{}

def append_dig(a: List<&2, Digest>, b: List<&2, Digest>) -> List<&2, Digest>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> append_dig(t, b)

def list_len_dig(xs: List<&2, Digest>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_dig(rest)

def flatten_leaves(d: Digest) -> List<&2, Digest>:
  match d:
    case HEmpty{}:
      Nil{}
    case HLeaf00{p}:
      [HLeaf00{p}]
    case HNode01{l, r}:
      append_dig(flatten_leaves(l), flatten_leaves(r))
    case HRoot02{fc, ft}:
      [HRoot02{fc, ft}]
    case HTensor03{bl, m}:
      [HTensor03{bl, m}]
    case HRoot04{fc, ft, tc, tt}:
      [HRoot04{fc, ft, tc, tt}]

def flat_map_leaves(xs: List<&2, Digest>) -> List<&2, Digest>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{h, t}:
      append_dig(flatten_leaves(h), flat_map_leaves(t))

def spec_wal_valid_prefix(payloads: List<&2, Nat>, +start_seq: Nat, tail: List<&2, WalRec>) -> List<&2, WalRec>:
  match payloads:
    case Nil{}:
      tail
    case Con{+p, +rest}:
      WalRec{True{}, start_seq, p} <> spec_wal_valid_prefix(rest, 1n+start_seq, tail)

def spec_wal_prepend(p: Nat, r: WalResult) -> WalResult:
  match r:
    case WalOk{ps}:
      WalOk{p <> ps}
    case WalErr{}:
      WalErr{}

def spec_wal_seq_branch(ok: Bool, p: Nat, rest_res: WalResult) -> WalResult:
  match ok:
    case False{}:
      WalErr{}
    case True{}:
      spec_wal_prepend(p, rest_res)

def spec_wal_torn_branch(recover_torn: Bool) -> WalResult:
  match recover_torn:
    case True{}:
      WalOk{Nil{}}
    case False{}:
      WalErr{}

def spec_wal_is_err(r: WalResult) -> Bool:
  match r:
    case WalOk{_}:
      False{}
    case WalErr{}:
      True{}

def spec_wal_recover_mode(recs: List<&2, WalRec>, +exp_seq: Nat, +recover_torn: Bool) -> WalResult:
  match recs:
    case Nil{}:
      WalOk{Nil{}}
    case Con{WalRec{True{}, +seq, +p}, +rest}:
      spec_wal_seq_branch(nat_eq(seq, exp_seq), p, spec_wal_recover_mode(rest, 1n+exp_seq, recover_torn))
    case Con{WalRec{False{}, _, _}, Nil{}}:
      spec_wal_torn_branch(recover_torn)
    case Con{WalRec{False{}, _, _}, Con{_, _}}:
      WalErr{}

def spec_sp_runtime(exp: Maybe<&2, Nat>, file: Maybe<&2, Nat>) -> Bool:
  match exp file:
    case None{} _:
      True{}
    case Some{_} None{}:
      False{}
    case Some{e} Some{g}:
      nat_eq(e, g)

def ivf_make_seq_recs(ids: List<&2, Nat>, +start_seq: Nat) -> List<&2, IvfWalRec>:
  match ids:
    case Nil{}:
      Nil{}
    case Con{+v, +rest}:
      IvfRec{start_seq, v} <> ivf_make_seq_recs(rest, 1n+start_seq)

def list_len_nat(xs: List<&2, Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{_, rest}:
      1n+list_len_nat(rest)

def sp_err_eq(a: SpError, b: SpError) -> Bool:
  match a b:
    case SpOk{} SpOk{}:
      True{}
    case SpAuthMacMismatch{} SpAuthMacMismatch{}:
      True{}
    case SpNonCanonicalFlags{} SpNonCanonicalFlags{}:
      True{}
    case SpInvalidContract{} SpInvalidContract{}:
      True{}
    case _ _:
      False{}
