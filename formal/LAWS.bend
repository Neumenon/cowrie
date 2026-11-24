# LAWS.bend — Complete, Biconditional & End-to-End Formal Laws for Cowrie v1.2
#
# Audited against BOTH fail-open bugs AND underspecification traps ("deny-all",
# "taint-all", "wipe-string", "drop-backslash", "head-only lookup/parents",
# "table-header & TensorRef-boundary fence bypass", "zombie/wipe tombstones",
# and "empty runtime_id bypass"):
#   - 26 machine-checked laws across Planes A, B, and D (`L01`..`L25`, with `L06a`+`L06b`)
#   - Checked by `bend PROOF.bend --verdict` (Bend 2.0.35 + Lean-proven BendTT kernel)

import Base
import ./CowrieKernel.bend as K

def sym_eq(a: K.Sym, b: K.Sym) -> Bool:
  match a b:
    case K.Lt{} K.Lt{}:
      True{}
    case K.Gt{} K.Gt{}:
      True{}
    case K.Esc{} K.Esc{}:
      True{}
    case K.Other{} K.Other{}:
      True{}
    case _ _:
      False{}

def sym_list_eq(xs: List<&2, K.Sym>, ys: List<&2, K.Sym>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Con{hx, tx} Con{hy, ty}:
      K.band(sym_eq(hx, hy), sym_list_eq(tx, ty))
    case _ _:
      False{}

def wire_int_eq(a: K.WireInt, b: K.WireInt) -> Bool:
  match a b:
    case K.WFix{x} K.WFix{y}:
      K.nat_eq(x, y)
    case K.WTagIntPos{x} K.WTagIntPos{y}:
      K.nat_eq(x, y)
    case K.WTagIntNeg{x} K.WTagIntNeg{y}:
      K.nat_eq(x, y)
    case K.WTagUint{x} K.WTagUint{y}:
      K.nat_eq(x, y)
    case _ _:
      False{}

def decoded_reencodes_identically(+lim: Nat, +ulim: Nat, +w: K.WireInt, dec: Maybe<&2, K.LogicalInt>) -> Bool:
  match dec:
    case None{}:
      True{}
    case Some{v}:
      wire_int_eq(K.encode_canon_int(lim, ulim, v), w)

# -----------------------------------------------------------------------------
# GROUP 1: PLANE D CAUSAL TAINT, DEEP STICKY LEDGER & BICONDITIONAL ED25519 GATE
# -----------------------------------------------------------------------------

law L01_propagate_iff:
  for +base: K.Auth
  for +ps: List<&2, K.Auth>
  {K.is_tainted(K.propagate(base, ps)) == K.bor(K.is_tainted(base), K.any_tainted(ps)) : Bool}

law L02_taint_irrevocable_hops:
  for +hops: List<&2, K.Auth>
  for +root: K.Auth
  {K.is_tainted(K.derive_chain(hops, K.set_tainted(root))) == True{} : Bool}

law L03_sticky_reregister:
  for +cid: Nat
  for +old: K.Auth
  for +rest: List<&2, K.LedgerNode>
  for +base: K.Auth
  for +ps: List<&2, Nat>
  {K.is_tainted(K.ledger_record(K.LNode{cid, K.set_tainted(old)} <> rest, cid, base, ps)) == True{} : Bool}

law L04_unknown_parent_fails_closed:
  for +cid: Nat
  for +base: K.Auth
  for +unk: Nat
  for +more_ps: List<&2, Nat>
  {K.is_tainted(K.ledger_record(Nil{}, cid, base, unk <> more_ps)) == True{} : Bool}

law L05_empty_args_fail_closed:
  for +act: K.Action
  for +lg: List<&2, K.LedgerNode>
  for +toks: List<&2, K.DeclassToken>
  for +now: Nat
  {K.implies(K.is_privileged(act), K.bnot(K.check_gate_signed(lg, act, Nil{}, toks, now))) == True{} : Bool}

law L06a_wrong_action_token_rejected:
  for +cid: Nat
  for +old: K.Auth
  for +expiry: Nat
  for +now: Nat
  {K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.FsWrite{}, [cid], [K.Tok{cid, K.ShellExec{}, expiry, True{}}], now) == False{} : Bool}

law L06b_partial_declass_blocks:
  for +lg: List<&2, K.LedgerNode>
  for +c1: Nat
  for +c2: Nat
  for +toks: List<&2, K.DeclassToken>
  for +now: Nat
  {K.implies(K.bnot(K.arg_authorized(c2, lg, K.ShellExec{}, toks, now)), K.bnot(K.check_gate_signed(lg, K.ShellExec{}, [c1, c2], toks, now))) == True{} : Bool}

# NEW S1-1: `propagate` preserves clean origin bits and `ext` flag identically (never corrupts provenance).
law L15_propagate_preserves_origin:
  for +base: K.Auth
  for +ps: List<&2, K.Auth>
  {(K.auth_clean_bits(K.propagate(base, ps)), K.auth_ext_bit(K.propagate(base, ps))) == (K.auth_clean_bits(base), K.auth_ext_bit(base)) : Nat & Bool}

# NEW S1-2: Sticky taint holds at ARBITRARY depth behind any non-matching ledger prefix (kills head-only lookup),
# AND clean derivations stay untainted `False` (kills trivial "taint-everything" implementations!).
law L16_sticky_deep_and_clean_liveness:
  for +prefix: List<&2, K.LedgerNode>
  for +cid: Nat
  for +old: K.Auth
  for +rest: List<&2, K.LedgerNode>
  for +base: K.Auth
  for +ps: List<&2, Nat>
  for +pc: Nat
  for +bc: Nat
  for +k: Nat
  {(K.implies(K.bnot(K.ledger_has_cid(prefix, cid)), K.is_tainted(K.ledger_record(K.append_lg(prefix, K.LNode{cid, K.set_tainted(old)} <> rest), cid, base, ps))), K.is_tainted(K.ledger_record([K.LNode{0n, K.Auth{pc, False{}, False{}}}], 1n+k, K.Auth{bc, False{}, False{}}, [0n]))) == (True{}, False{}) : Bool & Bool}

# NEW S1-3: Exact BICONDITIONAL (`==`) characterization of `check_gate_signed` + LIVENESS on clean & validly-signed calls
# (kills trivial "deny-all" `return False{}` implementations!).
law L17_gate_exact_iff_and_liveness:
  for +lg: List<&2, K.LedgerNode>
  for +act: K.Action
  for +args: List<&2, Nat>
  for +toks: List<&2, K.DeclassToken>
  for +now: Nat
  for +cid: Nat
  for +cb: Nat
  for +old: K.Auth
  for +exp: Nat
  {(K.bool_eq(K.check_gate_signed(lg, act, args, toks, now), K.bor(K.bnot(K.is_privileged(act)), K.band(K.is_nonempty_nat(args), K.all_args_authorized(args, lg, act, toks, now)))), (K.check_gate_signed([K.LNode{cid, K.Auth{cb, False{}, False{}}}], K.ShellExec{}, [cid], Nil{}, 0n), K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp))) == (True{}, (True{}, True{})) : Bool & (Bool & Bool)}

# NEW S1-4: Expired Ed25519 tokens (`now = 1n+exp`) AND invalid signatures (`sig_valid = False`) ALWAYS block.
law L18_expired_and_forged_sig_rejected:
  for +cid: Nat
  for +old: K.Auth
  for +exp: Nat
  {(K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], 1n+exp), K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, False{}}], 0n)) == (False{}, False{}) : Bool & Bool}

# NEW S1-5: An unapproved tainted `evil_cid` at ANY index `k` (behind an arbitrary prefix of clean/approved args) ALWAYS blocks.
law L19_nth_arg_taint_blocks:
  for +prefix: List<&2, Nat>
  for +evil_cid: Nat
  for +tail: List<&2, Nat>
  for +lg: List<&2, K.LedgerNode>
  for +toks: List<&2, K.DeclassToken>
  for +now: Nat
  {K.implies(K.bnot(K.arg_authorized(evil_cid, lg, K.ShellExec{}, toks, now)), K.bnot(K.check_gate_signed(lg, K.ShellExec{}, K.append_nat(prefix, evil_cid <> tail), toks, now))) == True{} : Bool}

# NEW S1-6: Exact biconditional (`==`) characterization of `ledger_record` + deep parent fail-closed at ANY index in `ps`!
law L25_ledger_record_exact_iff:
  for +lg: List<&2, K.LedgerNode>
  for +cid: Nat
  for +base: K.Auth
  for +ps: List<&2, Nat>
  for +p_prefix: List<&2, Nat>
  for +bad_p: Nat
  for +p_tail: List<&2, Nat>
  {(K.bool_eq(K.is_tainted(K.ledger_record(lg, cid, base, ps)), K.bor(K.is_tainted(base), K.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg)))), K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, K.append_nat(p_prefix, bad_p <> p_tail))))) == (True{}, True{}) : Bool & Bool}

# -----------------------------------------------------------------------------
# GROUP 2: PLANE B CONTENT-PRESERVING FENCE SANITIZER & STRUCTURED R2 TABLE LAWS
# -----------------------------------------------------------------------------

law L07_fence_no_triple:
  for +xs: List<&2, K.Sym>
  {K.has_triple_angle(K.sanitize_fences(xs)) == False{} : Bool}

law L08_fence_safe_identity:
  for +xs: List<&2, K.Sym>
  {K.implies(K.bnot(K.has_triple_angle(xs)), sym_list_eq(K.sanitize_fences(xs), xs)) == True{} : Bool}

# NEW S2-1: Content preservation on ALL inputs (including attacks!) — `strip_esc(sanitize_fences(xs)) == strip_esc(xs)`
# AND `count_esc(xs) <= count_esc(sanitize_fences(xs))` (kills both wipe-string AND backslash-deletion mutants!).
law L20_fence_preserves_all_non_esc_symbols:
  for +xs: List<&2, K.Sym>
  {(K.strip_esc(K.sanitize_fences(xs)), K.nat_le(K.count_esc(xs), K.count_esc(K.sanitize_fences(xs)))) == (K.strip_esc(xs), True{}) : List<&2, K.Sym> & Bool}

# NEW S2-2: Structured `R2` rendering over `Map` scalars, hoisted `Table` column headers (`F4`), AND `TensorRef`
# `<T:...@space>` boundaries (`F8`) NEVER emits `<<<`/`>>>` AND preserves `key`, `col_hdr`, and `cell` non-esc content!
law L21_r2_structured_table_header_safe:
  for +field: K.R2Field
  for +tk: List<&2, K.Sym>
  for +tcol: List<&2, K.Sym>
  for +tcell: List<&2, K.Sym>
  {(K.has_triple_angle(K.render_r2_field(field)), K.strip_esc(K.render_r2_field(K.R2Table{tk, tcol, tcell}))) == (False{}, K.append_sym(K.strip_esc(tk), K.Other{} <> K.append_sym(K.strip_esc(tcol), K.Other{} <> K.strip_esc(tcell)))) : Bool & List<&2, K.Sym>}

# -----------------------------------------------------------------------------
# GROUP 3: PLANE A CANONICAL WIRE BIJECTIVITY + MAP SORTING & `UnusedDictKey`
# -----------------------------------------------------------------------------

law L09_canon_roundtrip:
  for +lim: Nat
  for +ulim: Nat
  for +v: K.LogicalInt
  {K.decode_canon_int(lim, ulim, K.encode_canon_int(lim, ulim, v)) == Some{v} : Maybe<&2, K.LogicalInt>}

law L10_verify_iff_decode:
  for +lim: Nat
  for +ulim: Nat
  for +w: K.WireInt
  {K.verify_canon_int_zero_alloc(lim, ulim, w) == K.is_some_int(K.decode_canon_int(lim, ulim, w)) : Bool}

law L11_canon_wire_unique:
  for +lim: Nat
  for +ulim: Nat
  for +w: K.WireInt
  {decoded_reencodes_identically(lim, ulim, w, K.decode_canon_int(lim, ulim, w)) == True{} : Bool}

# NEW S3: Deep Plane A Map out-of-range dict index rejection + deep duplicate/unsorted adjacent key rejection
# + universal `UnusedDictKey` rejection for ANY missing key `m < nkeys` + clean liveness!
law L22_map_keys_and_dict_all_used:
  for +nkeys: Nat
  for +prefix: List<&2, Nat>
  for +b: Nat
  for +d: Nat
  for +tail: List<&2, Nat>
  for +m: Nat
  for +refs: List<&2, Nat>
  {((K.verify_map_keys(nkeys, K.append_nat(prefix, K.nat_add(nkeys, d) <> tail)), K.verify_map_keys(nkeys, K.append_nat(prefix, K.nat_add(b, d) <> (b <> tail)))), (K.implies(K.nat_lt(m, nkeys), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.all_dict_keys_used(nkeys, refs)))), (K.verify_map_keys(2n, [0n, 1n]), K.all_dict_keys_used(2n, [1n, 0n])))) == ((False{}, False{}), (True{}, (True{}, True{}))) : (Bool & Bool) & (Bool & (Bool & Bool))}

# -----------------------------------------------------------------------------
# GROUP 4: `CWZM` EARLY-BREAK EQUIVALENCE, MERKLE, `.cwwal` RE-INSERT & `CWSP`
# -----------------------------------------------------------------------------

law L12_zonemap_no_false_neg:
  for +k0: Nat
  for +ks: List<&2, Nat>
  for +lo: Nat
  for +hi: Nat
  {K.implies(K.page_has_in_range(k0, ks, lo, hi), K.zone_overlaps(K.build_zone(k0, ks), lo, hi)) == True{} : Bool}

# NEW S4: Multi-page `CWZM` early-break pruning (`if z.min_key > hi { break; }`) returns the EXACT SAME page list
# as full linear filtering whenever `zones_min_sorted` holds (proving both 0 false negatives and 0 extra pages!).
law L23_sorted_zonemap_early_break_exact:
  for +zs: List<&2, K.Zone>
  for +lo: Nat
  for +hi: Nat
  {K.implies(K.zones_min_sorted(zs, 0n), K.nat_list_eq(K.prune_sorted_zones_at(zs, 0n, lo, hi), K.filter_all_zones_at(zs, 0n, lo, hi))) == True{} : Bool}

law L13_merkle_domain_and_odd_safe:
  for +f1: List<&2, K.Digest>
  for +f2: List<&2, K.Digest>
  for +t0: K.Digest
  for +trest: List<&2, K.Digest>
  for +a: K.Digest
  for +b: K.Digest
  for +c: K.Digest
  {K.bor(K.digest_eq(K.shard_merkle_root(f1, Nil{}), K.shard_merkle_root(f2, t0 <> trest)), K.digest_eq(K.shard_merkle_root([a, b, c], Nil{}), K.shard_merkle_root([a, b, c, c], Nil{}))) == False{} : Bool}

law L14_wal_torn_vs_mid_tamper:
  for +prefix: List<&2, Nat>
  for +torn_payload: Nat
  for +bad_payload: Nat
  for +after: K.WalRec
  for +more: List<&2, K.WalRec>
  {(K.wal_recover(K.append_valid_prefix(prefix, [K.WalRec{False{}, torn_payload}])), K.wal_recover(K.append_valid_prefix(prefix, K.WalRec{False{}, bad_payload} <> (after <> more)))) == (K.WalOk{prefix}, K.WalErr{}) : K.WalResult & K.WalResult}

# NEW S5 & S6: `.cwwal` sequential tombstone revokes target WITHOUT wiping bystander (`[0n]`) AND allows subsequent
# re-insertion (`[cid]`, killing global-HashSet zombie tombstones `F5`), AND `StatePage` `expected_runtime_id`
# rejects empty/mismatched `runtime_id` (`F7`) while allowing matching/unspecified `runtime_id`!
law L24_wal_tombstone_reinsert_and_statepage_rt:
  for +cid: Nat
  for +rt: Nat
  for +k: Nat
  for +file_rt: Maybe<&2, Nat>
  {((K.wal_replay_live([K.OpFrame{0n}, K.OpFrame{1n+k}, K.OpTomb{1n+k}], Nil{}), K.wal_replay_live([K.OpFrame{cid}, K.OpTomb{cid}, K.OpFrame{cid}], Nil{})), (K.check_statepage_runtime(Some{rt}, None{}), (K.check_statepage_runtime(Some{0n}, Some{1n+k}), (K.check_statepage_runtime(Some{rt}, Some{rt}), K.check_statepage_runtime(None{}, file_rt))))) == (([0n], [cid]), (False{}, (False{}, (True{}, True{})))) : (List<&2, Nat> & List<&2, Nat>) & (Bool & (Bool & (Bool & Bool)))}
