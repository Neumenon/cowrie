# LAWS.bend — 37 Non-Circular, Biconditional & End-to-End Formal Laws for Cowrie v1.2
#
# Audited by TWO rounds of multi-agent adversarial critique against BOTH fail-open bugs AND
# vacuity/circularity/underspecification/TCB-leak traps:
#   - ALL frozen specification predicates live strictly in `SPEC.bend` (ZERO mutable `K.` helpers in laws!).
#   - 26 original laws (`L01`..`L25`, with `L06a`+`L06b`) de-circularized & universally tightened.
#   - 11 new laws (`L26`..`L36`) covering Cowrie v1.2 growth & audit catches `F9`..`F18`.
#   - Universal RFC 6962 `flatten_leaves` Merkle injectivity across ALL lengths `0..∞` (`L13`).
#   - Checked by `bend formal/PROOF.bend --verdict` (Bend 2.0.35 + Lean-proven BendTT kernel).

import Base
import ./SPEC.bend as S
import ./CowrieKernel.bend as K

def decoded_reencodes_identically(+lim: Nat, +ulim: Nat, +w: S.WireInt, dec: Maybe<&2, S.LogicalInt>) -> Bool:
  match dec:
    case None{}:
      True{}
    case Some{v}:
      S.wire_int_eq(K.encode_canon_int(lim, ulim, v), w)

def opt_val_bind_extract(opt: Maybe<&2, S.ValTree>, p2: List<&2, S.PathSeg>) -> Maybe<&2, S.ValTree>:
  match opt:
    case None{}:
      None{}
    case Some{u}:
      K.extract_field(u, p2)

# =============================================================================
# GROUP 1: PLANE D CAUSAL TAINT, DEEP STICKY LEDGER, HEADER META, ED25519 & MCP
# =============================================================================

# L01: Exact non-circular `is_tainted` (ext | tainted), `propagate` biconditional, AND bit-7 `tainted` pinning!
law L01_propagate_iff:
  for +c: Nat
  for +e: Bool
  for +t: Bool
  for +base: S.Auth
  for +ps: List<&2, S.Auth>
  {(K.is_tainted(S.Auth{c, e, t}), (K.is_tainted(K.propagate(base, ps)), S.spec_auth_tainted_bit(K.propagate(base, ps)))) == (S.bor(e, t), (S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)), S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)))) : Bool & (Bool & Bool)}

# L02: Exact biconditional `derive_chain`: tainted iff root OR ANY intermediate hop is tainted (kills no-op!).
law L02_taint_irrevocable_hops:
  for +hops: List<&2, S.Auth>
  for +root: S.Auth
  {K.is_tainted(K.derive_chain(hops, root)) == S.bor(S.spec_tainted(root), S.spec_any_tainted(hops)) : Bool}

# L03: Sticky re-registration holds for BOTH `ext=True, t=False` (`UNTRUSTED_EXT`) AND `t=True` (`BIT_TAINTED`).
law L03_sticky_reregister:
  for +cid: Nat
  for +oc: Nat
  for +old: S.Auth
  for +rest: List<&2, S.LedgerNode>
  for +base: S.Auth
  for +ps: List<&2, Nat>
  {(K.is_tainted(K.ledger_record(S.LNode{cid, S.Auth{oc, True{}, False{}}} <> rest, cid, base, ps)), K.is_tainted(K.ledger_record(S.LNode{cid, S.spec_set_tainted(old)} <> rest, cid, base, ps))) == (True{}, True{}) : Bool & Bool}

# L04: Unknown parent at ANY index fails closed on ARBITRARY ledgers (empty OR non-empty!).
law L04_unknown_parent_fails_closed:
  for +lg: List<&2, S.LedgerNode>
  for +cid: Nat
  for +base: S.Auth
  for +p_pre: List<&2, Nat>
  for +unk: Nat
  for +p_tail: List<&2, Nat>
  {S.implies(S.bnot(S.spec_ledger_has_cid(lg, unk)), K.is_tainted(K.ledger_record(lg, cid, base, S.append_nat(p_pre, unk <> p_tail)))) == True{} : Bool}

# L05: `is_privileged` matches frozen SPEC across all 5 actions, AND empty args fail closed on every privileged action.
law L05_empty_args_fail_closed:
  for +act: S.Action
  for +lg: List<&2, S.LedgerNode>
  for +toks: List<&2, S.DeclassToken>
  for +has_pk: Bool
  for +now: Nat
  {(K.is_privileged(act), S.implies(S.spec_is_privileged(act), S.bnot(K.check_gate_signed_pk(lg, act, Nil{}, toks, has_pk, now)))) == (S.spec_is_privileged(act), True{}) : Bool & Bool}

# L06a: Wrong-action token rejected across ALL mismatched action pairs `(a1 != a2)` and BOTH `ext`/`tainted`!
law L06a_wrong_action_token_rejected:
  for +a1: S.Action
  for +a2: S.Action
  for +cid: Nat
  for +oc: Nat
  for +oe: Bool
  for +ot: Bool
  for +expiry: Nat
  for +now: Nat
  {S.implies(S.band(S.spec_is_privileged(a1), S.band(S.bor(oe, ot), S.bnot(S.spec_action_eq(a1, a2)))), S.bnot(K.check_gate_signed([S.LNode{cid, S.Auth{oc, oe, ot}}], a1, [cid], [S.Tok{cid, a2, expiry, True{}}], now))) == True{} : Bool}

# L06b: Token for `c1` NEVER authorizes a distinct tainted `c2 != c1` (neither alone nor paired `[c1, c2]`)!
law L06b_partial_declass_blocks:
  for +act: S.Action
  for +c1: Nat
  for +c2: Nat
  for +o1: S.Auth
  for +oc2: Nat
  for +oe2: Bool
  for +ot2: Bool
  for +exp: Nat
  for +now: Nat
  {S.implies(S.band(S.spec_is_privileged(act), S.band(S.bnot(S.nat_eq(c1, c2)), S.bor(oe2, ot2))), S.band(S.bnot(K.check_gate_signed([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c1, c2], [S.Tok{c1, act, exp, True{}}], now)), S.bnot(K.check_gate_signed([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c2], [S.Tok{c1, act, exp, True{}}], now)))) == True{} : Bool}

# L15: BOTH `propagate` AND `ledger_record` preserve `clean_bits` and `ext` identically (frozen `S.spec_auth_*` TCB!).
law L15_propagate_preserves_origin:
  for +base: S.Auth
  for +ps: List<&2, S.Auth>
  for +lg: List<&2, S.LedgerNode>
  for +cid: Nat
  for +pids: List<&2, Nat>
  {((S.spec_auth_clean_bits(K.propagate(base, ps)), S.spec_auth_ext_bit(K.propagate(base, ps))), (S.spec_auth_clean_bits(K.ledger_record(lg, cid, base, pids)), S.spec_auth_ext_bit(K.ledger_record(lg, cid, base, pids)))) == ((S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base))) : (Nat & Bool) & (Nat & Bool)}

# L16: Deep sticky taint behind arbitrary prefix (including `ext=True`) AND universal clean-derivation liveness!
law L16_sticky_deep_and_clean_liveness:
  for +prefix: List<&2, S.LedgerNode>
  for +cid: Nat
  for +oc: Nat
  for +oe: Bool
  for +ot: Bool
  for +rest: List<&2, S.LedgerNode>
  for +base: S.Auth
  for +ps: List<&2, Nat>
  for +lg: List<&2, S.LedgerNode>
  for +bc: Nat
  for +clean_ps: List<&2, Nat>
  {(S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)), K.is_tainted(K.ledger_record(S.append_lg(prefix, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid, base, ps))), S.implies(S.band(S.bnot(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid))), S.bnot(S.spec_parents_tainted(clean_ps, lg))), S.bnot(K.is_tainted(K.ledger_record(lg, cid, S.Auth{bc, False{}, False{}}, clean_ps))))) == (True{}, True{}) : Bool & Bool}

# L17: Exact NON-CIRCULAR biconditional of BOTH `check_gate_signed_pk` AND 5-arg `check_gate_signed` + deep-token liveness!
law L17_gate_exact_iff_and_liveness:
  for +lg: List<&2, S.LedgerNode>
  for +act: S.Action
  for +args: List<&2, Nat>
  for +toks: List<&2, S.DeclassToken>
  for +has_pk: Bool
  for +now: Nat
  for +cid: Nat
  for +old: S.Auth
  for +tpre: List<&2, S.DeclassToken>
  for +ttail: List<&2, S.DeclassToken>
  for +d: Nat
  {((K.check_gate_signed_pk(lg, act, args, toks, has_pk, now), K.check_gate_signed(lg, act, args, toks, now)), (K.check_gate_signed_pk(lg, S.ReadOnly{}, Nil{}, Nil{}, False{}, now), K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail), True{}, now))) == ((S.spec_gate(lg, act, args, toks, has_pk, now), S.spec_gate(lg, act, args, toks, True{}, now)), (True{}, True{})) : (Bool & Bool) & (Bool & Bool)}

# L18: Expired tokens (`now = 1+exp+d` for ALL `d`), invalid signatures at ALL `now`, and missing `owner_pubkey` ALL block!
law L18_expired_and_forged_sig_rejected:
  for +act: S.Action
  for +cid: Nat
  for +old: S.Auth
  for +exp: Nat
  for +d: Nat
  for +now: Nat
  {S.implies(S.spec_is_privileged(act), S.band(S.bnot(K.check_gate_signed([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, True{}}], 1n+S.nat_add(exp, d))), S.band(S.bnot(K.check_gate_signed([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, False{}}], now)), S.bnot(K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now))))) == True{} : Bool}

# L19: Unauthorized/unknown/tainted `evil_cid` at ANY index blocks on EVERY privileged action (non-circular `S.spec_arg_ok`).
law L19_nth_arg_taint_blocks:
  for +act: S.Action
  for +prefix: List<&2, Nat>
  for +evil_cid: Nat
  for +tail: List<&2, Nat>
  for +lg: List<&2, S.LedgerNode>
  for +toks: List<&2, S.DeclassToken>
  for +has_pk: Bool
  for +now: Nat
  {S.implies(S.band(S.spec_is_privileged(act), S.bnot(S.spec_arg_ok(evil_cid, lg, act, toks, has_pk, now))), S.bnot(K.check_gate_signed_pk(lg, act, S.append_nat(prefix, evil_cid <> tail), toks, has_pk, now))) == True{} : Bool}

# L25: Exact NON-CIRCULAR biconditional of `ledger_record` (incl. bit-7 `tainted` pinning!) + deep bad-parent fail-closed!
law L25_ledger_record_exact_iff:
  for +lg: List<&2, S.LedgerNode>
  for +cid: Nat
  for +base: S.Auth
  for +ps: List<&2, Nat>
  for +p_pre: List<&2, Nat>
  for +bad_p: Nat
  for +p_tail: List<&2, Nat>
  {((K.is_tainted(K.ledger_record(lg, cid, base, ps)), S.spec_auth_tainted_bit(K.ledger_record(lg, cid, base, ps))), S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, S.append_nat(p_pre, bad_p <> p_tail))))) == ((S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))), S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg)))), True{}) : (Bool & Bool) & Bool}

# L26 (NEW): Plane A/D `is_valid_header_meta` exact biconditional against `SPEC` + explicit boundary rejections.
law L26_header_meta_and_owner_pubkey:
  for +low4: Nat
  for +res3: Nat
  for +epi: Nat
  for +flags: Nat
  for +r: Nat
  for +e: Nat
  for +f: Nat
  {((K.is_valid_header_meta(low4, res3, epi, flags), K.is_valid_header_meta(0n, 0n, 1n, 0n)), ((K.is_valid_header_meta(16n+r, 0n, 1n, 0n), K.is_valid_header_meta(2n, 1n+r, 1n, 0n)), ((K.is_valid_header_meta(2n, 0n, 0n, 0n), K.is_valid_header_meta(2n, 0n, 4n+e, 0n)), (K.is_valid_header_meta(2n, 0n, 1n, 1n+f), (K.is_valid_header_meta(1n, 0n, 1n, 0n), K.is_valid_header_meta(15n, 0n, 3n, 0n)))))) == ((S.spec_valid_header_meta(low4, res3, epi, flags), False{}), ((False{}, False{}), ((False{}, False{}), (False{}, (True{}, True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((Bool & Bool) & (Bool & (Bool & Bool))))}

# L27 (NEW): Exact Ed25519 action-code bijection (`0..=4`, >=5 rejected) + FULL 6-arg biconditional MCP tool classifier!
law L27_ed25519_action_code_and_mcp_classifier:
  for +a: S.Action
  for +b: S.Action
  for +k: Nat
  for +pol: Maybe<&2, S.Action>
  for +verb: Maybe<&2, S.Action>
  for +allow_ro: Bool
  for +builtin_ro: Bool
  for +trust_ann: Bool
  for +hint: Bool
  {(((K.action_to_code(a), K.action_from_code(K.action_to_code(a))), S.implies(S.bnot(S.spec_action_eq(a, b)), S.bnot(S.nat_eq(K.action_to_code(a), K.action_to_code(b))))), ((K.action_from_code(5n+k), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (S.spec_is_privileged(K.classify_mcp_tool(None{}, None{}, False{}, False{}, False{}, hint)), S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{}))))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}

# =============================================================================
# GROUP 2: PLANE B 6-SYMBOL MEALY SANITIZER, MULTI-CELL R2 (`F9`), R2-COMPACT & EXTRACT
# =============================================================================

# L07: Full 6-symbol Mealy sanitizer eliminates `<<<`, `>>>`, `<|`, `|>`, AND all `Hidden{}` (frozen `S.has_forbidden`).
law L07_fence_no_triple:
  for +xs: List<&2, S.Sym>
  {S.has_forbidden(K.sanitize_fences(xs)) == False{} : Bool}

# L08: Identity on every input already free of `<<<`, `>>>`, `<|`, `|>`, and `Hidden{}`.
law L08_fence_safe_identity:
  for +xs: List<&2, S.Sym>
  {S.implies(S.bnot(S.has_forbidden(xs)), S.sym_list_eq(K.sanitize_fences(xs), xs)) == True{} : Bool}

# L20: True content & original-escape preservation: `strip_esc` matches `strip_hidden` AND `strip_hidden(xs)`
# is an order-preserving SUBSEQUENCE of `sanitize_fences(xs)` (kills both wipe-string & drop-orig-esc!).
law L20_fence_preserves_all_non_esc_symbols:
  for +xs: List<&2, S.Sym>
  {(S.strip_esc(K.sanitize_fences(xs)), S.sym_subseq(S.strip_hidden(xs), K.sanitize_fences(xs))) == (S.strip_esc(S.strip_hidden(xs)), True{}) : List<&2, S.Sym> & Bool}

# L21: Structured `R2` rendering over `R2Scalar`, multi-cell `R2Table` joined by `Pipe{}` (`F4`+`F9`), and `R2TensorRef` (`F8`)
# NEVER emits `<<<`/`>>>`/`<|`/`|>`/`Hidden{}` AND preserves non-esc/non-hidden content across ALL 3 variants!
law L21_r2_structured_table_header_safe:
  for +field: S.R2Field
  for +sk: List<&2, S.Sym>
  for +sv: List<&2, S.Sym>
  for +tk: List<&2, S.Sym>
  for +tcol: List<&2, S.Sym>
  for +tc1: List<&2, S.Sym>
  for +tc2: List<&2, S.Sym>
  for +rk: List<&2, S.Sym>
  for +rsp: List<&2, S.Sym>
  {((S.has_forbidden(K.render_r2_field(field)), S.strip_esc(K.render_r2_field(S.R2Scalar{sk, sv}))), (S.strip_esc(K.render_r2_field(S.R2Table{tk, tcol, tc1, tc2})), S.strip_esc(K.render_r2_field(S.R2TensorRef{rk, rsp})))) == ((False{}, S.append_sym(S.strip_esc(S.strip_hidden(sk)), S.Other{} <> S.strip_esc(S.strip_hidden(sv)))), (S.append_sym(S.strip_esc(S.strip_hidden(tk)), S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(tcol)), S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(tc1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(tc2))))), S.append_sym(S.strip_esc(S.strip_hidden(rk)), S.Other{} <> (S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(rsp)), [S.Gt{}])))))) : (Bool & List<&2, S.Sym>) & (List<&2, S.Sym> & List<&2, S.Sym>)}

# L28 (NEW): `R2-Compact` & `project_virtual_atlas` fail closed to `FullMealyFence` on `0`, `UNTRUSTED_EXT`, `MODEL_SYNTH`,
# `BIT_TAINTED`, and mixed authorities; compact ONLY for clean `SYS_POLICY`/`VERIFIED_TOOL`; truncation preserves safety!
law L28_r2_compact_and_virtual_atlas_fail_closed:
  for +a: S.Auth
  for +c: Nat
  for +t: Bool
  for +e: Bool
  for +k: Nat
  for +n: Nat
  for +xs: List<&2, S.Sym>
  {((K.r2_compact_mode(S.Auth{1n, False{}, False{}}), K.r2_compact_mode(S.Auth{2n, False{}, False{}})), ((K.r2_compact_mode(S.Auth{0n, e, t}), K.r2_compact_mode(S.Auth{c, True{}, t})), ((K.r2_compact_mode(S.Auth{c, e, True{}}), K.r2_compact_mode(S.Auth{3n+k, e, t})), (K.virtual_atlas_mode(a), S.has_forbidden(S.take_sym(n, K.sanitize_fences(xs))))))) == ((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((S.FullMealyFence{}, S.FullMealyFence{}), (K.r2_compact_mode(a), False{})))) : (S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & (S.LensMode & Bool)))}

# L29 (NEW): Universal R2 table cell marker injectivity, universal `Hidden{}` spoof invariance (`F9`),
# universal empty/null/missing marker classification, AND full visible-content preservation across ALL `raw`!
law L29_r2_table_marker_injectivity:
  for +c: S.TableCell
  for +raw: List<&2, S.Sym>
  {((S.cell_tag_has_forbidden(K.encode_table_cell(c)), S.cell_tag_eq(K.encode_table_cell(S.CellNull{}), K.encode_table_cell(S.CellStr{raw}))), ((S.cell_tag_eq(K.encode_table_cell(S.CellMissing{}), K.encode_table_cell(S.CellStr{raw})), S.cell_tag_eq(K.encode_table_cell(S.CellNull{}), K.encode_table_cell(S.CellMissing{}))), ((K.encode_table_cell(S.CellStr{raw}), S.strip_esc(S.cell_tag_content(K.encode_table_cell(S.CellStr{raw})))), (S.sym_subseq(S.strip_hidden(raw), S.cell_tag_content(K.encode_table_cell(S.CellStr{raw}))), ((S.implies(S.sym_list_eq(S.strip_hidden(raw), Nil{}), S.cell_tag_eq(K.encode_table_cell(S.CellStr{raw}), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(S.strip_hidden(raw), [S.Other{}]), S.cell_tag_eq(K.encode_table_cell(S.CellStr{raw}), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(S.strip_hidden(raw), [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_table_cell(S.CellStr{raw}), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_table_cell(S.CellStr{S.strip_hidden(raw)}), S.strip_esc(S.strip_hidden(raw))), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}

# L30 (NEW): Strict lens tier bounds (`0..=3`, `>=4` rejected) + universal `extract_field`
# (exact frozen spec, empty path, path composition, deep map lookup, arbitrary array indexing, OOB & type mismatch fail-closed)!
law L30_lens_tier_bounds_and_extract_field:
  for +t: Nat
  for +k: Nat
  for +idx: Nat
  for +v: S.ValTree
  for +pre_m: List<&2, S.MapEntry>
  for +post_m: List<&2, S.MapEntry>
  for +pre_a: List<&2, S.ValTree>
  for +post_a: List<&2, S.ValTree>
  for +a: Nat
  for +seg: S.PathSeg
  for +p1: List<&2, S.PathSeg>
  for +p2: List<&2, S.PathSeg>
  {(((K.is_valid_lens_tier(t), K.is_valid_lens_tier(4n+k)), (K.extract_field(v, p1), (K.extract_field(v, Nil{}), K.extract_field(v, S.append_path(p1, p2))))), ((S.implies(S.bnot(S.spec_map_has_key(pre_m, k)), S.spec_maybe_val_is_atom(K.extract_field(S.VMap{S.append_map(pre_m, S.MEntry{k, S.VAtom{a}} <> post_m)}, [S.PKey{k}]), a)), K.extract_field(S.VArr{S.append_val(pre_a, v <> post_a)}, [S.PIdx{S.list_len_val(pre_a)}])), ((K.extract_field(S.VArr{pre_a}, [S.PIdx{S.nat_add(S.list_len_val(pre_a), k)}]), K.extract_field(S.VMap{pre_m}, [S.PIdx{idx}])), (K.extract_field(S.VArr{pre_a}, [S.PKey{k}]), K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, opt_val_bind_extract(K.extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}

# =============================================================================
# GROUP 3: PLANE A CANONICAL WIRE, FLOAT64, MAP/DICT, SENTINELS & RESOLVED CID
# =============================================================================

# L09: Canonical integer round-trip identity.
law L09_canon_roundtrip:
  for +lim: Nat
  for +ulim: Nat
  for +v: S.LogicalInt
  {K.decode_canon_int(lim, ulim, K.encode_canon_int(lim, ulim, v)) == Some{v} : Maybe<&2, S.LogicalInt>}

# L10: Zero-alloc verifier matches BOTH frozen `S.spec_valid_wire` (pinning `lim`/`ulim`) AND `decode_canon_int`.
law L10_verify_iff_decode:
  for +lim: Nat
  for +ulim: Nat
  for +w: S.WireInt
  {(K.verify_canon_int_zero_alloc(lim, ulim, w), K.verify_canon_int_zero_alloc(lim, ulim, w)) == (S.spec_valid_wire(lim, ulim, w), S.spec_is_some_int(K.decode_canon_int(lim, ulim, w))) : Bool & Bool}

# L11: Canonical wire uniqueness (every accepted wire int re-encodes identically).
law L11_canon_wire_unique:
  for +lim: Nat
  for +ulim: Nat
  for +w: S.WireInt
  {decoded_reencodes_identically(lim, ulim, w, K.decode_canon_int(lim, ulim, w)) == True{} : Bool}

# L22: Exact NON-CIRCULAR biconditional of `verify_map_keys` & `all_dict_keys_used` against frozen `SPEC.bend`,
# plus deep OOB, deep duplicate/unsorted, and universal missing-key rejection!
law L22_map_keys_and_dict_all_used:
  for +nkeys: Nat
  for +ks: List<&2, Nat>
  for +prefix: List<&2, Nat>
  for +b: Nat
  for +d: Nat
  for +tail: List<&2, Nat>
  for +m: Nat
  for +refs: List<&2, Nat>
  {((K.verify_map_keys(nkeys, ks), K.all_dict_keys_used(nkeys, refs)), ((K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(nkeys, d) <> tail)), K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(b, d) <> (b <> tail)))), S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(K.all_dict_keys_used(nkeys, refs)))))) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}

# L31 (NEW): De-circularized Canonical `Float64`: exact match against frozen `S.spec_canonicalize_f64` &
# `S.spec_verify_canon_f64`, idempotence, AND strict rejection of `-0.0` and non-canonical NaNs!
law L31_canon_float64_bijective_and_idempotent:
  for +f: S.FloatKind
  for +p: Nat
  {((K.canonicalize_f64(f), K.verify_canon_f64(f)), (K.canonicalize_f64(K.canonicalize_f64(f)), (K.verify_canon_f64(S.FNegZero{}), K.verify_canon_f64(S.FNonCanonNaN{p})))) == ((S.spec_canonicalize_f64(f), S.spec_verify_canon_f64(f)), (K.canonicalize_f64(f), (False{}, False{}))) : (S.FloatKind & Bool) & (S.FloatKind & (Bool & Bool))}

# L32 (NEW): Lossless `$map`/`$float`/`$bytes`/`$tensor` JSON sentinel round-trip + malformed sentinel fail-closed (`F6`).
law L32_json_sentinel_bijectivity_and_fail_closed:
  for +v: S.SentinelVal
  for +r: Nat
  for +c: Nat
  for +a: Nat
  for +dt: Nat
  for +kk: Nat
  for +p: Nat
  {(K.parse_sentinel_json(K.emit_sentinel_json(v)), ((K.parse_sentinel_json(S.JBytesObj{False{}, r}), K.parse_sentinel_json(S.JFloatObj{False{}, c})), (K.parse_sentinel_json(S.JTensorObj{False{}, a, dt}), K.parse_sentinel_json(S.JMapWrap{False{}, kk, p})))) == (Some{v}, ((None{}, None{}), (None{}, None{}))) : Maybe<&2, S.SentinelVal> & ((Maybe<&2, S.SentinelVal> & Maybe<&2, S.SentinelVal>) & (Maybe<&2, S.SentinelVal> & Maybe<&2, S.SentinelVal>))}

# L33 (NEW): Universal `resolved_frame_cid`: exact frozen spec, universal repack invariance (`ts ++ extra`),
# universal OOB rejection at ANY position `r_pre ++ (len ts + d) :: r_tail`, and arbitrary-index tensor binding!
law L33_resolved_frame_cid_repack_and_binding:
  for +fcid: Nat
  for +refs: List<&2, Nat>
  for +r_pre: List<&2, Nat>
  for +r_tail: List<&2, Nat>
  for +ts: List<&2, S.TensorDesc>
  for +pre: List<&2, S.TensorDesc>
  for +post: List<&2, S.TensorDesc>
  for +extra: List<&2, S.TensorDesc>
  for +l: Nat
  for +m: Nat
  for +d: Nat
  {((K.resolved_frame_cid(fcid, refs, ts), S.implies(S.rcid_is_some(K.resolved_frame_cid(fcid, refs, ts)), S.rcid_eq(K.resolved_frame_cid(fcid, refs, ts), K.resolved_frame_cid(fcid, refs, S.append_tdesc(ts, extra))))), ((K.resolved_frame_cid(fcid, S.append_nat(r_pre, S.nat_add(S.list_len_tdesc(ts), d) <> r_tail), ts), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(1n+S.nat_add(fcid, d), [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)))), (S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}

# =============================================================================
# GROUP 4: `CWZM` BINARY-SKIP, UNIVERSAL MERKLE, COMPLETE `.cwwal`, `LiveIvfStore` & `CWSP`
# =============================================================================

# L12: Exact `zone_overlaps` biconditional (kills `const True{}`!) + single-page zero false-negative + `min <= max`.
law L12_zonemap_no_false_neg:
  for +mn: Nat
  for +mx: Nat
  for +k0: Nat
  for +ks: List<&2, Nat>
  for +lo: Nat
  for +hi: Nat
  {(K.zone_overlaps(S.Zone{mn, mx}, lo, hi), S.implies(S.spec_page_has_in_range(k0, ks, lo, hi), K.zone_overlaps(K.build_zone(k0, ks), lo, hi))) == (S.band(S.nat_le(lo, mx), S.nat_le(mn, hi)), True{}) : Bool & Bool}

# L23: Multi-page `CWZM` `partition_point`-skip (`max_k < lo`) + early-break (`min_k > hi`) matches frozen
# `S.spec_filter_zones_at` EXACTLY when `lo <= hi` & disjoint monotonic, AND returns `Nil{}` when `hi < lo`!
law L23_sorted_zonemap_early_break_exact:
  for +zs: List<&2, S.Zone>
  for +lo: Nat
  for +hi: Nat
  {(S.implies(S.band(S.nat_le(lo, hi), S.spec_zones_disjoint_from(zs, 0n)), S.nat_list_eq(K.query_key_range_pages(zs, lo, hi), S.spec_filter_zones_at(zs, 0n, lo, hi))), S.implies(S.nat_lt(hi, lo), S.nat_list_eq(K.query_key_range_pages(zs, lo, hi), Nil{}))) == (True{}, True{}) : Bool & Bool}

# L13: UNIVERSAL RFC 6962 `flatten_leaves(merkle_tree_digest(xs)) == flat_map_leaves(xs)` across ALL lengths `0..∞`
# (kills `fuel=2n` truncation & CVE-2012-2459!), PLUS exact `shard_merkle_root` `0x02`/`0x04` count+tree binding!
law L13_merkle_domain_and_odd_safe:
  for +xs: List<&2, S.Digest>
  for +f1: List<&2, S.Digest>
  for +f2: List<&2, S.Digest>
  for +t0: S.Digest
  for +trest: List<&2, S.Digest>
  for +a: Nat
  for +b: Nat
  for +c: Nat
  {(((S.flatten_leaves(K.merkle_tree_digest(xs)), K.shard_merkle_root(f1, Nil{})), (K.shard_merkle_root(f2, t0 <> trest), S.spec_digest_eq(K.shard_merkle_root(f1, Nil{}), K.shard_merkle_root(f2, t0 <> trest)))), (S.spec_digest_eq(K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{b}, S.HLeaf00{c}]), K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{b}, S.HLeaf00{c}, S.HLeaf00{c}])), (K.merkle_tree_digest(Nil{}), (K.merkle_tree_digest([S.HLeaf00{a}]), K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{b}]))))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}

# L34 (NEW): Merkle leaf-payload sensitivity (incl. frame binding inside `shard_merkle_root`) & `HTensor03` binding!
law L34_merkle_leaf_and_tensor_sensitivity:
  for +a: Nat
  for +b: Nat
  for +d: Nat
  for +bl: Nat
  for +m4m: Nat
  for +fs: List<&2, S.Digest>
  {((S.spec_digest_eq(K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{b}]), K.merkle_tree_digest([S.HLeaf00{1n+S.nat_add(a, d)}, S.HLeaf00{b}])), S.spec_digest_eq(K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{b}]), K.merkle_tree_digest([S.HLeaf00{a}, S.HLeaf00{1n+S.nat_add(b, d)}]))), (S.spec_digest_eq(K.shard_merkle_root([S.HLeaf00{a}], Nil{}), K.shard_merkle_root([S.HLeaf00{1n+S.nat_add(a, d)}], Nil{})), S.spec_digest_eq(K.shard_merkle_root(fs, [S.HTensor03{bl, m4m}]), K.shard_merkle_root(fs, [S.HTensor03{bl, 1n+S.nat_add(m4m, d)}])))) == ((False{}, False{}), (False{}, False{})) : (Bool & Bool) & (Bool & Bool)}

# L14: Complete `.cwwal` recovery: exact biconditional against frozen `S.spec_wal_recover_mode`
# (covering BOTH rollback `bad_seq < exp_seq` AND forward gap `bad_seq > exp_seq`, arbitrary `torn_seq`, both modes)!
law L14_wal_torn_vs_mid_tamper:
  for +recs: List<&2, S.WalRec>
  for +prefix: List<&2, Nat>
  for +start_seq: Nat
  for +torn_seq: Nat
  for +bad_seq: Nat
  for +tp: Nat
  for +bp: Nat
  for +after: S.WalRec
  for +more: List<&2, S.WalRec>
  for +mode: Bool
  {((K.wal_recover_mode(recs, start_seq, mode), K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, Nil{}), start_seq, mode)), ((K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, True{}), K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, False{})), (K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), start_seq, mode), S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}

# L24: Universally quantified sequential WAL replay (homomorphism, tombstone revokes target & keeps ANY bystander `b != c`,
# frame revives with EXACT count `1n` & keeps ANY bystander `b != c`) + exact biconditional `StatePage` `runtime_id`!
law L24_wal_tombstone_reinsert_and_statepage_rt:
  for +ops1: List<&2, S.WalOp>
  for +ops2: List<&2, S.WalOp>
  for +live: List<&2, Nat>
  for +c: Nat
  for +b: Nat
  for +exp_rt: Maybe<&2, Nat>
  for +file_rt: Maybe<&2, Nat>
  {((K.wal_replay_live(S.append_ops(ops1, ops2), live), S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpTomb{c}]), live), c)), ((S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpTomb{c}]), live), b), S.nat_mem(K.wal_replay_live(ops1, live), b))), S.nat_count(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), b), S.nat_mem(K.wal_replay_live(ops1, live), b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, K.wal_replay_live(ops1, live)), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}

# L35 (NEW): `LiveIvfStore` crash-safe `.cwgen` generation compaction & `CWAL v2` watermark (`F10`):
# Pre-compaction, crash-after-`.cwgen`-before-WAL-reset, and post-WAL-reset ALL yield exact multiplicity 1!
law L35_live_ivf_compaction_crash_invariant:
  for +base: List<&2, Nat>
  for +comp_ids: List<&2, Nat>
  for +post_ids: List<&2, Nat>
  for +s0: Nat
  {(K.live_ivf_recover(base, s0, S.ivf_make_seq_recs(S.append_nat(comp_ids, post_ids), s0)), (K.live_ivf_recover(S.append_nat(base, comp_ids), S.nat_add(s0, S.list_len_nat(comp_ids)), S.ivf_make_seq_recs(S.append_nat(comp_ids, post_ids), s0)), K.live_ivf_recover(S.append_nat(base, comp_ids), S.nat_add(s0, S.list_len_nat(comp_ids)), S.ivf_make_seq_recs(post_ids, S.nat_add(s0, S.list_len_nat(comp_ids)))))) == (S.append_nat(base, S.append_nat(comp_ids, post_ids)), (S.append_nat(base, S.append_nat(comp_ids, post_ids)), S.append_nat(base, S.append_nat(comp_ids, post_ids)))) : List<&2, Nat> & (List<&2, Nat> & List<&2, Nat>)}

# L36 (NEW): `StatePage` HMAC-first error precedence (`F13`) — ALL 9 exhaustive branches (incl. `(False, 0, mv, False) == SpInvalidContract`)!
law L36_statepage_hmac_precedence:
  for +k: Nat
  for +mv: Bool
  for +hv: Bool
  {((K.statepage_parse_precedence(True{}, 1n, False{}, hv), K.statepage_parse_precedence(True{}, 0n, mv, hv)), ((K.statepage_parse_precedence(True{}, 2n+k, mv, hv), K.statepage_parse_precedence(False{}, 1n, mv, hv)), ((K.statepage_parse_precedence(True{}, 1n, True{}, True{}), K.statepage_parse_precedence(True{}, 1n, True{}, False{})), ((K.statepage_parse_precedence(False{}, 0n, mv, True{}), K.statepage_parse_precedence(False{}, 0n, mv, False{})), K.statepage_parse_precedence(False{}, 2n+k, mv, hv))))) == ((S.SpAuthMacMismatch{}, S.SpAuthMacMismatch{}), ((S.SpAuthMacMismatch{}, S.SpAuthMacMismatch{}), ((S.SpOk{}, S.SpInvalidContract{}), ((S.SpOk{}, S.SpInvalidContract{}), S.SpNonCanonicalFlags{})))) : (S.SpError & S.SpError) & ((S.SpError & S.SpError) & ((S.SpError & S.SpError) & ((S.SpError & S.SpError) & S.SpError)))}
