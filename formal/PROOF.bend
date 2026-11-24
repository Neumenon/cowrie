# PROOF.bend — Machine-Checked Proofs of All 26 Tight Cowrie v1.2 Formal Laws
#
# Checked by `/home/omen/toolchains/bend-2.0.35/rel/bend/bin/bend PROOF.bend --verdict`
# (both `bend2` and the Lean-proven BendTT kernel). Zero `@unsafe`, zero templates, zero holes.

import Base
import ./CowrieKernel.bend as K
import ./LAWS.bend as Laws

# =============================================================================
# ARITHMETIC, BOOLEAN & CONTRADICTION LEMMAS
# =============================================================================

def lem_ex_falso_true(b: Bool, h: {False{} == True{} : Bool}) -> {b == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      h

def lem_ex_falso_false(b: Bool, h: {False{} == True{} : Bool}) -> {b == False{} : Bool}:
  match b:
    case False{}:
      {==}
    case True{}:
      Equal.sym(Bool, False{}, True{}, h)

def lem_bool_eq_refl(b: Bool) -> {K.bool_eq(b, b) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_nat_eq_refl(n: Nat) -> {K.nat_eq(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_eq_refl(p)

def lem_nat_eq_refl_rw(n: Nat) -> {True{} == K.nat_eq(n, n) : Bool}:
  Equal.sym(Bool, K.nat_eq(n, n), True{}, lem_nat_eq_refl(n))

def lem_nat_le_refl(n: Nat) -> {K.nat_le(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_le_refl(p)

def lem_nat_le_refl_rw(n: Nat) -> {True{} == K.nat_le(n, n) : Bool}:
  Equal.sym(Bool, K.nat_le(n, n), True{}, lem_nat_le_refl(n))

def lem_le_succ_false_rw(n: Nat) -> {False{} == K.nat_le(1n+n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_le_succ_false_rw(p)

def lem_le_succ2_false_rw(n: Nat) -> {False{} == K.nat_le(1n+(1n+n), n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_le_succ2_false_rw(p)

def lem_implies_true_r(x: Bool) -> {K.implies(x, True{}) == True{} : Bool}:
  match x:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_nat_le_trans(a: Nat, b: Nat, c: Nat) -> {K.implies(K.nat_le(a, b), K.implies(K.nat_le(b, c), K.nat_le(a, c))) == True{} : Bool}:
  match a b c:
    case 0n b0 c0:
      lem_implies_true_r(K.nat_le(b0, c0))
    case 1n+_ 0n _:
      {==}
    case 1n+pa 1n+pb 0n:
      lem_implies_true_r(K.nat_le(pa, pb))
    case 1n+pa 1n+pb 1n+pc:
      lem_nat_le_trans(pa, pb, pc)

def lem_lt_add_false_rw(ulim: Nat, d: Nat) -> {False{} == K.nat_lt(K.nat_add(ulim, d), ulim) : Bool}:
  match ulim:
    case 0n:
      {==}
    case 1n+p:
      lem_lt_add_false_rw(p, d)

def lem_add_sub_cancel_rw(ulim: Nat, d: Nat) -> {d == K.nat_sub(K.nat_add(ulim, d), ulim) : Nat}:
  match ulim d:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+p d0:
      lem_add_sub_cancel_rw(p, d0)

def lem_sub_add_restore(ulim: Nat, u: Nat) -> {K.implies(K.bnot(K.nat_lt(u, ulim)), K.nat_eq(K.nat_add(ulim, K.nat_sub(u, ulim)), u)) == True{} : Bool}:
  match ulim u:
    case 0n 0n:
      {==}
    case 0n 1n+pu:
      lem_nat_eq_refl(1n+pu)
    case 1n+_ 0n:
      {==}
    case 1n+pl 1n+pu:
      lem_sub_add_restore(pl, pu)

def lem_mp(p: Bool, q: Bool, hp: {p == True{} : Bool}, hpq: {K.implies(p, q) == True{} : Bool}) -> {q == True{} : Bool}:
  match p q:
    case True{} True{}:
      {==}
    case True{} False{}:
      hpq
    case False{} True{}:
      {==}
    case False{} False{}:
      hp

def lem_le_succ_r_rw(n: Nat) -> {True{} == K.nat_le(n, 1n+n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_le_succ_r_rw(p)

def lem_le_step_r_top(+a: Nat, +b: Nat, +c: Nat, hbc_rw: {True{} == K.nat_le(b, c) : Bool}) -> {K.implies(K.nat_le(a, b), K.nat_le(a, c)) == True{} : Bool}:
  %Equal.sym(Bool, True{}, K.nat_le(b, c), hbc_rw) : {K.implies(K.nat_le(a, b), K.implies(_, K.nat_le(a, c))) == True{} : Bool}
  lem_nat_le_trans(a, b, c)

def lem_le_inc_r(+a: Nat, +b: Nat, ih: {K.nat_le(a, b) == True{} : Bool}) -> {K.nat_le(a, 1n+b) == True{} : Bool}:
  lem_mp(K.nat_le(a, b), K.nat_le(a, 1n+b), ih, lem_le_step_r_top(a, b, 1n+b, lem_le_succ_r_rw(b)))

# =============================================================================
# GROUP 1 PROOFS: PLANE D TAINT, DEEP STICKY LEDGER & BICONDITIONAL GATE
# =============================================================================

def lem_prop_bool(base: K.Auth, x: Bool) -> {K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), x), base)) == K.bor(K.is_tainted(base), x) : Bool}:
  match base x:
    case K.Auth{_, True{}, True{}} True{}:
      {==}
    case K.Auth{_, True{}, True{}} False{}:
      {==}
    case K.Auth{_, True{}, False{}} True{}:
      {==}
    case K.Auth{_, True{}, False{}} False{}:
      {==}
    case K.Auth{_, False{}, True{}} True{}:
      {==}
    case K.Auth{_, False{}, True{}} False{}:
      {==}
    case K.Auth{_, False{}, False{}} True{}:
      {==}
    case K.Auth{_, False{}, False{}} False{}:
      {==}

def Laws.L01_propagate_iff(base, ps):
  lem_prop_bool(base, K.any_tainted(ps))

def lem_prop_origin(base: K.Auth, x: Bool) -> {(K.auth_clean_bits(K.prop_apply(K.bor(K.is_tainted(base), x), base)), K.auth_ext_bit(K.prop_apply(K.bor(K.is_tainted(base), x), base))) == (K.auth_clean_bits(base), K.auth_ext_bit(base)) : Nat & Bool}:
  match base x:
    case K.Auth{_, True{}, True{}} True{}:
      {==}
    case K.Auth{_, True{}, True{}} False{}:
      {==}
    case K.Auth{_, True{}, False{}} True{}:
      {==}
    case K.Auth{_, True{}, False{}} False{}:
      {==}
    case K.Auth{_, False{}, True{}} True{}:
      {==}
    case K.Auth{_, False{}, True{}} False{}:
      {==}
    case K.Auth{_, False{}, False{}} True{}:
      {==}
    case K.Auth{_, False{}, False{}} False{}:
      {==}

def Laws.L15_propagate_preserves_origin(base, ps):
  lem_prop_origin(base, K.any_tainted(ps))

def lem_set_tainted_true(a: K.Auth) -> {K.is_tainted(K.set_tainted(a)) == True{} : Bool}:
  match a:
    case K.Auth{_, True{}, _}:
      {==}
    case K.Auth{_, False{}, _}:
      {==}

def lem_set_tainted_true_rw(a: K.Auth) -> {True{} == K.is_tainted(K.set_tainted(a)) : Bool}:
  Equal.sym(Bool, K.is_tainted(K.set_tainted(a)), True{}, lem_set_tainted_true(a))

def lem_prop_from_tainted_rw(step: K.Auth, root: K.Auth) -> {K.set_tainted(step) == K.propagate(step, [K.set_tainted(root)]) : K.Auth}:
  match step root:
    case K.Auth{_, True{}, True{}} K.Auth{_, True{}, _}:
      {==}
    case K.Auth{_, True{}, True{}} K.Auth{_, False{}, _}:
      {==}
    case K.Auth{_, True{}, False{}} K.Auth{_, True{}, _}:
      {==}
    case K.Auth{_, True{}, False{}} K.Auth{_, False{}, _}:
      {==}
    case K.Auth{_, False{}, True{}} K.Auth{_, True{}, _}:
      {==}
    case K.Auth{_, False{}, True{}} K.Auth{_, False{}, _}:
      {==}
    case K.Auth{_, False{}, False{}} K.Auth{_, True{}, _}:
      {==}
    case K.Auth{_, False{}, False{}} K.Auth{_, False{}, _}:
      {==}

def Laws.L02_taint_irrevocable_hops(hops, root):
  match hops:
    case Nil{}:
      lem_set_tainted_true(root)
    case Con{+h, +rest}:
      %lem_prop_from_tainted_rw(h, root) : {K.is_tainted(K.derive_chain(rest, _)) == True{} : Bool}
      Laws.L02_taint_irrevocable_hops(rest, h)

def lem_prop_forced_true(base: K.Auth, p: Bool) -> {K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), K.bor(True{}, p)), base)) == True{} : Bool}:
  match base:
    case K.Auth{_, True{}, True{}}:
      {==}
    case K.Auth{_, True{}, False{}}:
      {==}
    case K.Auth{_, False{}, True{}}:
      {==}
    case K.Auth{_, False{}, False{}}:
      {==}

def Laws.L03_sticky_reregister(cid, old, rest, base, ps):
  %lem_nat_eq_refl_rw(cid) : {K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), K.bor(K.existing_tainted(K.select_lookup(_, K.set_tainted(old), K.ledger_lookup(rest, cid))), K.parents_tainted(ps, K.LNode{cid, K.set_tainted(old)} <> rest))), base)) == True{} : Bool}
  %lem_set_tainted_true_rw(old) : {K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), K.bor(_, K.parents_tainted(ps, K.LNode{cid, K.set_tainted(old)} <> rest))), base)) == True{} : Bool}
  lem_prop_forced_true(base, K.parents_tainted(ps, K.LNode{cid, K.set_tainted(old)} <> rest))

def lem_deep_lookup_step(
  eq: Bool, +k: Nat, +a: K.Auth, +prest: List<&2, K.LedgerNode>, +cid: Nat, +old: K.Auth, +rest: List<&2, K.LedgerNode>,
  eq_pf: {eq == K.nat_eq(k, cid) : Bool},
  ih: {K.implies(K.bnot(K.ledger_has_cid(prest, cid)), K.existing_tainted(K.ledger_lookup(K.append_lg(prest, K.LNode{cid, K.set_tainted(old)} <> rest), cid))) == True{} : Bool}
) -> {K.implies(K.bnot(K.bor(K.nat_eq(k, cid), K.ledger_has_cid(prest, cid))), K.existing_tainted(K.select_lookup(K.nat_eq(k, cid), a, K.ledger_lookup(K.append_lg(prest, K.LNode{cid, K.set_tainted(old)} <> rest), cid)))) == True{} : Bool}:
  match eq:
    case True{}:
      %eq_pf : {K.implies(K.bnot(K.bor(_, K.ledger_has_cid(prest, cid))), K.existing_tainted(K.select_lookup(_, a, K.ledger_lookup(K.append_lg(prest, K.LNode{cid, K.set_tainted(old)} <> rest), cid)))) == True{} : Bool}
      {==}
    case False{}:
      %eq_pf : {K.implies(K.bnot(K.bor(_, K.ledger_has_cid(prest, cid))), K.existing_tainted(K.select_lookup(_, a, K.ledger_lookup(K.append_lg(prest, K.LNode{cid, K.set_tainted(old)} <> rest), cid)))) == True{} : Bool}
      ih

def lem_deep_lookup_tainted(prefix: List<&2, K.LedgerNode>, +cid: Nat, +old: K.Auth, +rest: List<&2, K.LedgerNode>) -> {K.implies(K.bnot(K.ledger_has_cid(prefix, cid)), K.existing_tainted(K.ledger_lookup(K.append_lg(prefix, K.LNode{cid, K.set_tainted(old)} <> rest), cid))) == True{} : Bool}:
  match prefix:
    case Nil{}:
      %lem_nat_eq_refl_rw(cid) : {K.existing_tainted(K.select_lookup(_, K.set_tainted(old), K.ledger_lookup(rest, cid))) == True{} : Bool}
      lem_set_tainted_true(old)
    case Con{K.LNode{+k, +a}, +prest}:
      lem_deep_lookup_step(
        K.nat_eq(k, cid), k, a, prest, cid, old, rest, {==},
        lem_deep_lookup_tainted(prest, cid, old, rest)
      )

def lem_imp_old_forces_record(
  cond: Bool, old_t: Bool, +base: K.Auth, +par_t: Bool,
  h: {K.implies(cond, old_t) == True{} : Bool}
) -> {K.implies(cond, K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), K.bor(old_t, par_t)), base))) == True{} : Bool}:
  match cond old_t:
    case False{} _:
      {==}
    case True{} True{}:
      lem_prop_forced_true(base, par_t)
    case True{} False{}:
      lem_ex_falso_true(K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), par_t), base)), h)

def Laws.L16_sticky_deep_and_clean_liveness(prefix, cid, old, rest, base, ps, _pc, _bc, _k):
  pf_deep = lem_imp_old_forces_record(
    K.bnot(K.ledger_has_cid(prefix, cid)),
    K.existing_tainted(K.ledger_lookup(K.append_lg(prefix, K.LNode{cid, K.set_tainted(old)} <> rest), cid)),
    base,
    K.parents_tainted(ps, K.append_lg(prefix, K.LNode{cid, K.set_tainted(old)} <> rest)),
    lem_deep_lookup_tainted(prefix, cid, old, rest)
  )
  %Equal.sym(Bool, K.implies(K.bnot(K.ledger_has_cid(prefix, cid)), K.is_tainted(K.ledger_record(K.append_lg(prefix, K.LNode{cid, K.set_tainted(old)} <> rest), cid, base, ps))), True{}, pf_deep) : {(_, False{}) == (True{}, False{}) : Bool & Bool}
  {==}

def lem_prop_unknown_true(base: K.Auth, p: Bool) -> {K.is_tainted(K.prop_apply(K.bor(K.is_tainted(base), K.bor(False{}, K.bor(True{}, p))), base)) == True{} : Bool}:
  match base:
    case K.Auth{_, True{}, True{}}:
      {==}
    case K.Auth{_, True{}, False{}}:
      {==}
    case K.Auth{_, False{}, True{}}:
      {==}
    case K.Auth{_, False{}, False{}}:
      {==}

def Laws.L04_unknown_parent_fails_closed(_cid, base, _unk, more_ps):
  lem_prop_unknown_true(base, K.parents_tainted(more_ps, Nil{}))

def Laws.L05_empty_args_fail_closed(act, _lg, _toks, _now):
  match act:
    case K.ReadOnly{}:
      {==}
    case K.ShellExec{}:
      {==}
    case K.CredRead{}:
      {==}
    case K.FsWrite{}:
      {==}
    case K.NetExfil{}:
      {==}

def lem_band_false_3_rw(a: Bool) -> {False{} == K.bor(K.band(True{}, K.band(a, False{})), False{}) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def Laws.L06a_wrong_action_token_rejected(cid, old, expiry, now):
  %lem_nat_eq_refl_rw(cid) : {K.band(K.arg_auth_branch(K.opt_auth_tainted(K.select_lookup(_, K.set_tainted(old), None{})), cid, K.FsWrite{}, [K.Tok{cid, K.ShellExec{}, expiry, True{}}], now), True{}) == False{} : Bool}
  %lem_set_tainted_true_rw(old) : {K.band(K.arg_auth_branch(_, cid, K.FsWrite{}, [K.Tok{cid, K.ShellExec{}, expiry, True{}}], now), True{}) == False{} : Bool}
  %lem_band_false_3_rw(K.nat_eq(cid, cid)) : {K.band(_, True{}) == False{} : Bool}
  {==}

def lem_partial_block_bool(b1: Bool, b2: Bool) -> {K.implies(K.bnot(b2), K.bnot(K.band(b1, K.band(b2, True{})))) == True{} : Bool}:
  match b1 b2:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}

def Laws.L06b_partial_declass_blocks(lg, c1, c2, toks, now):
  lem_partial_block_bool(K.arg_authorized(c1, lg, K.ShellExec{}, toks, now), K.arg_authorized(c2, lg, K.ShellExec{}, toks, now))

def lem_gate_iff(act: K.Action, args: List<&2, Nat>, +lg: List<&2, K.LedgerNode>, +toks: List<&2, K.DeclassToken>, +now: Nat) -> {K.bool_eq(K.check_gate_signed(lg, act, args, toks, now), K.bor(K.bnot(K.is_privileged(act)), K.band(K.is_nonempty_nat(args), K.all_args_authorized(args, lg, act, toks, now)))) == True{} : Bool}:
  match act args:
    case K.ReadOnly{} _:
      {==}
    case K.ShellExec{} Nil{}:
      {==}
    case K.ShellExec{} Con{+c, +rest}:
      lem_bool_eq_refl(K.band(K.arg_authorized(c, lg, K.ShellExec{}, toks, now), K.all_args_authorized(rest, lg, K.ShellExec{}, toks, now)))
    case K.CredRead{} Nil{}:
      {==}
    case K.CredRead{} Con{+c, +rest}:
      lem_bool_eq_refl(K.band(K.arg_authorized(c, lg, K.CredRead{}, toks, now), K.all_args_authorized(rest, lg, K.CredRead{}, toks, now)))
    case K.FsWrite{} Nil{}:
      {==}
    case K.FsWrite{} Con{+c, +rest}:
      lem_bool_eq_refl(K.band(K.arg_authorized(c, lg, K.FsWrite{}, toks, now), K.all_args_authorized(rest, lg, K.FsWrite{}, toks, now)))
    case K.NetExfil{} Nil{}:
      {==}
    case K.NetExfil{} Con{+c, +rest}:
      lem_bool_eq_refl(K.band(K.arg_authorized(c, lg, K.NetExfil{}, toks, now), K.all_args_authorized(rest, lg, K.NetExfil{}, toks, now)))

def lem_gate_clean_allows(+cid: Nat, +cb: Nat) -> {True{} == K.check_gate_signed([K.LNode{cid, K.Auth{cb, False{}, False{}}}], K.ShellExec{}, [cid], Nil{}, 0n) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {True{} == K.band(K.arg_auth_branch(K.opt_auth_tainted(K.select_lookup(_, K.Auth{cb, False{}, False{}}, None{})), cid, K.ShellExec{}, Nil{}, 0n), True{}) : Bool}
  {==}

def lem_gate_signed_allows(+cid: Nat, +old: K.Auth, +exp: Nat) -> {True{} == K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {True{} == K.band(K.arg_auth_branch(K.opt_auth_tainted(K.select_lookup(_, K.set_tainted(old), None{})), cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp), True{}) : Bool}
  %lem_set_tainted_true_rw(old) : {True{} == K.band(K.arg_auth_branch(_, cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp), True{}) : Bool}
  %lem_nat_eq_refl_rw(cid) : {True{} == K.band(K.bor(K.band(True{}, K.band(_, K.nat_le(exp, exp))), False{}), True{}) : Bool}
  %lem_nat_le_refl_rw(exp) : {True{} == K.band(K.bor(_, False{}), True{}) : Bool}
  {==}

def Laws.L17_gate_exact_iff_and_liveness(lg, act, args, toks, now, cid, cb, old, exp):
  pf_iff = lem_gate_iff(act, args, lg, toks, now)
  %Equal.sym(Bool, K.bool_eq(K.check_gate_signed(lg, act, args, toks, now), K.bor(K.bnot(K.is_privileged(act)), K.band(K.is_nonempty_nat(args), K.all_args_authorized(args, lg, act, toks, now)))), True{}, pf_iff) : {(_, (K.check_gate_signed([K.LNode{cid, K.Auth{cb, False{}, False{}}}], K.ShellExec{}, [cid], Nil{}, 0n), K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp))) == (True{}, (True{}, True{})) : Bool & (Bool & Bool)}
  %lem_gate_clean_allows(cid, cb) : {(True{}, (_, K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], exp))) == (True{}, (True{}, True{})) : Bool & (Bool & Bool)}
  %lem_gate_signed_allows(cid, old, exp) : {(True{}, (True{}, _)) == (True{}, (True{}, True{})) : Bool & (Bool & Bool)}
  {==}

def lem_gate_expired_blocks(+cid: Nat, +old: K.Auth, +exp: Nat) -> {False{} == K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, True{}}], 1n+exp) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {False{} == K.band(K.arg_auth_branch(K.opt_auth_tainted(K.select_lookup(_, K.set_tainted(old), None{})), cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, True{}}], 1n+exp), True{}) : Bool}
  %lem_set_tainted_true_rw(old) : {False{} == K.band(K.arg_auth_branch(_, cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, True{}}], 1n+exp), True{}) : Bool}
  %lem_nat_eq_refl_rw(cid) : {False{} == K.band(K.bor(K.band(True{}, K.band(_, K.nat_le(1n+exp, exp))), False{}), True{}) : Bool}
  %lem_le_succ_false_rw(exp) : {False{} == K.band(K.bor(_, False{}), True{}) : Bool}
  {==}

def lem_gate_badsig_blocks(+cid: Nat, +old: K.Auth, +exp: Nat) -> {False{} == K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, False{}}], 0n) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {False{} == K.band(K.arg_auth_branch(K.opt_auth_tainted(K.select_lookup(_, K.set_tainted(old), None{})), cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, False{}}], 0n), True{}) : Bool}
  %lem_set_tainted_true_rw(old) : {False{} == K.band(K.arg_auth_branch(_, cid, K.ShellExec{}, [K.Tok{cid, K.ShellExec{}, exp, False{}}], 0n), True{}) : Bool}
  {==}

def Laws.L18_expired_and_forged_sig_rejected(cid, old, exp):
  %lem_gate_expired_blocks(cid, old, exp) : {(_, K.check_gate_signed([K.LNode{cid, K.set_tainted(old)}], K.ShellExec{}, [cid], [K.Tok{cid, K.ShellExec{}, exp, False{}}], 0n)) == (False{}, False{}) : Bool & Bool}
  %lem_gate_badsig_blocks(cid, old, exp) : {(False{}, _) == (False{}, False{}) : Bool & Bool}
  {==}

def lem_evil_head_bool(e: Bool, t: Bool) -> {K.implies(K.bnot(e), K.bnot(K.band(e, t))) == True{} : Bool}:
  match e t:
    case False{} _:
      {==}
    case True{} _:
      {==}

def lem_evil_step_bool(
  h_ok: Bool, e_ok: Bool, rest_ok: Bool,
  ih: {K.implies(K.bnot(e_ok), K.bnot(rest_ok)) == True{} : Bool}
) -> {K.implies(K.bnot(e_ok), K.bnot(K.band(h_ok, rest_ok))) == True{} : Bool}:
  match h_ok e_ok rest_ok:
    case False{} False{} _:
      {==}
    case False{} True{} _:
      {==}
    case True{} True{} _:
      {==}
    case True{} False{} False{}:
      {==}
    case True{} False{} True{}:
      ih

def lem_all_args_nth_blocks(
  prefix: List<&2, Nat>, +evil: Nat, +tail: List<&2, Nat>, +lg: List<&2, K.LedgerNode>, +toks: List<&2, K.DeclassToken>, +now: Nat
) -> {K.implies(K.bnot(K.arg_authorized(evil, lg, K.ShellExec{}, toks, now)), K.bnot(K.all_args_authorized(K.append_nat(prefix, evil <> tail), lg, K.ShellExec{}, toks, now))) == True{} : Bool}:
  match prefix:
    case Nil{}:
      lem_evil_head_bool(K.arg_authorized(evil, lg, K.ShellExec{}, toks, now), K.all_args_authorized(tail, lg, K.ShellExec{}, toks, now))
    case Con{+h, +prest}:
      lem_evil_step_bool(
        K.arg_authorized(h, lg, K.ShellExec{}, toks, now),
        K.arg_authorized(evil, lg, K.ShellExec{}, toks, now),
        K.all_args_authorized(K.append_nat(prest, evil <> tail), lg, K.ShellExec{}, toks, now),
        lem_all_args_nth_blocks(prest, evil, tail, lg, toks, now)
      )

def Laws.L19_nth_arg_taint_blocks(prefix, evil_cid, tail, lg, toks, now):
  match prefix:
    case Nil{}:
      lem_evil_head_bool(K.arg_authorized(evil_cid, lg, K.ShellExec{}, toks, now), K.all_args_authorized(tail, lg, K.ShellExec{}, toks, now))
    case Con{+h, +prest}:
      lem_evil_step_bool(
        K.arg_authorized(h, lg, K.ShellExec{}, toks, now),
        K.arg_authorized(evil_cid, lg, K.ShellExec{}, toks, now),
        K.all_args_authorized(K.append_nat(prest, evil_cid <> tail), lg, K.ShellExec{}, toks, now),
        lem_all_args_nth_blocks(prest, evil_cid, tail, lg, toks, now)
      )

def lem_imp_bor_l(x: Bool, y: Bool) -> {K.implies(x, K.bor(x, y)) == True{} : Bool}:
  match x:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_imp_bor_r(h: Bool, a: Bool, b: Bool, ih: {K.implies(h, b) == True{} : Bool}) -> {K.implies(h, K.bor(a, b)) == True{} : Bool}:
  match h a b:
    case False{} _ _:
      {==}
    case True{} True{} _:
      {==}
    case True{} False{} True{}:
      {==}
    case True{} False{} False{}:
      ih

def lem_parents_nth_tainted(
  p_prefix: List<&2, Nat>, +bad_p: Nat, +p_tail: List<&2, Nat>, +lg: List<&2, K.LedgerNode>
) -> {K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), K.parents_tainted(K.append_nat(p_prefix, bad_p <> p_tail), lg)) == True{} : Bool}:
  match p_prefix:
    case Nil{}:
      lem_imp_bor_l(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), K.parents_tainted(p_tail, lg))
    case Con{+p, +rest}:
      lem_imp_bor_r(
        K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)),
        K.opt_auth_tainted(K.ledger_lookup(lg, p)),
        K.parents_tainted(K.append_nat(rest, bad_p <> p_tail), lg),
        lem_parents_nth_tainted(rest, bad_p, p_tail, lg)
      )

def lem_imp_bor3(h: Bool, b_t: Bool, old_t: Bool, par_t: Bool, p_par: {K.implies(h, par_t) == True{} : Bool}) -> {K.implies(h, K.bor(b_t, K.bor(old_t, par_t))) == True{} : Bool}:
  match h b_t old_t par_t:
    case False{} _ _ _:
      {==}
    case True{} True{} _ _:
      {==}
    case True{} False{} True{} _:
      {==}
    case True{} False{} False{} True{}:
      {==}
    case True{} False{} False{} False{}:
      p_par

def Laws.L25_ledger_record_exact_iff(lg, cid, base, ps, p_prefix, bad_p, p_tail):
  +rhs1 = K.bor(K.is_tainted(base), K.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg)))
  eq1 = Equal.sym(Bool, K.is_tainted(K.ledger_record(lg, cid, base, ps)), rhs1, lem_prop_bool(base, K.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg))))
  pf1 = Equal.sym(Bool, K.bool_eq(rhs1, rhs1), True{}, lem_bool_eq_refl(rhs1))
  +ps2 = K.append_nat(p_prefix, bad_p <> p_tail)
  +rhs2 = K.bor(K.is_tainted(base), K.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps2, lg)))
  eq2 = Equal.sym(Bool, K.is_tainted(K.ledger_record(lg, cid, base, ps2)), rhs2, lem_prop_bool(base, K.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps2, lg))))
  pf2 = Equal.sym(
    Bool,
    K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), rhs2),
    True{},
    lem_imp_bor3(
      K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)),
      K.is_tainted(base),
      K.existing_tainted(K.ledger_lookup(lg, cid)),
      K.parents_tainted(ps2, lg),
      lem_parents_nth_tainted(p_prefix, bad_p, p_tail, lg)
    )
  )
  %eq1 : {(K.bool_eq(_, rhs1), K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, ps2)))) == (True{}, True{}) : Bool & Bool}
  %pf1 : {(_, K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, ps2)))) == (True{}, True{}) : Bool & Bool}
  %eq2 : {(True{}, K.implies(K.opt_auth_tainted(K.ledger_lookup(lg, bad_p)), _)) == (True{}, True{}) : Bool & Bool}
  %pf2 : {(True{}, _) == (True{}, True{}) : Bool & Bool}
  {==}

# =============================================================================
# GROUP 2 PROOFS: PLANE B FENCE SANITIZER + CONTENT PRESERVATION + R2 TABLES
# =============================================================================

def lem_no_triple_inv(xs: List<&2, K.Sym>, r_lt: K.Run2, r_gt: K.Run2) -> {K.has_triple_step(K.sanitize_step(xs, r_lt, r_gt), r_lt, r_gt) == False{} : Bool}:
  match xs r_lt r_gt:
    case Nil{} _ _:
      {==}
    case Con{K.Lt{}, +rest} K.R0{} K.R0{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R0{} K.R1{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R0{} K.R2{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R0{}:
      lem_no_triple_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R1{}:
      lem_no_triple_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R2{}:
      lem_no_triple_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R2{} K.R0{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R2{} K.R1{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R2{} K.R2{}:
      lem_no_triple_inv(rest, K.R1{}, K.R0{})
    case Con{K.Gt{}, +rest} K.R0{} K.R0{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R1{} K.R0{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R2{} K.R0{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R0{} K.R1{}:
      lem_no_triple_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R1{} K.R1{}:
      lem_no_triple_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R2{} K.R1{}:
      lem_no_triple_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R0{} K.R2{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R1{} K.R2{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R2{} K.R2{}:
      lem_no_triple_inv(rest, K.R0{}, K.R1{})
    case Con{K.Esc{}, +rest} K.R0{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R1{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R2{} _:
      lem_no_triple_inv(rest, K.R0{}, K.R0{})

def Laws.L07_fence_no_triple(xs):
  lem_no_triple_inv(xs, K.R0{}, K.R0{})

def lem_band_true_l_rw(b: Bool) -> {b == K.band(True{}, b) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_safe_id_inv(xs: List<&2, K.Sym>, r_lt: K.Run2, r_gt: K.Run2) -> {K.implies(K.bnot(K.has_triple_step(xs, r_lt, r_gt)), Laws.sym_list_eq(K.sanitize_step(xs, r_lt, r_gt), xs)) == True{} : Bool}:
  match xs r_lt r_gt:
    case Nil{} _ _:
      {==}
    case Con{K.Lt{}, +rest} K.R0{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R1{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R1{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R0{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R1{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R1{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R0{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R1{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R1{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R2{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R2{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R2{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R2{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R2{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R2{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, _} K.R2{} K.R0{}:
      {==}
    case Con{K.Lt{}, _} K.R2{} K.R1{}:
      {==}
    case Con{K.Lt{}, _} K.R2{} K.R2{}:
      {==}
    case Con{K.Gt{}, +rest} K.R0{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R1{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R1{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R1{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R1{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R1{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R2{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R1{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R1{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R0{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R2{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R2{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R1{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R2{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R2{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R2{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R2{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R2{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, _} K.R0{} K.R2{}:
      {==}
    case Con{K.Gt{}, _} K.R1{} K.R2{}:
      {==}
    case Con{K.Gt{}, _} K.R2{} K.R2{}:
      {==}
    case Con{K.Esc{}, +rest} K.R0{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R0{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R0{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R1{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R1{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R1{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R2{} K.R0{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R2{} K.R1{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R2{} K.R2{}:
      %lem_band_true_l_rw(Laws.sym_list_eq(K.sanitize_step(rest, K.R0{}, K.R0{}), rest)) : {K.implies(K.bnot(K.has_triple_step(rest, K.R0{}, K.R0{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, K.R0{}, K.R0{})

def Laws.L08_fence_safe_identity(xs):
  lem_safe_id_inv(xs, K.R0{}, K.R0{})

def lem_strip_esc_inv_rw(xs: List<&2, K.Sym>, r_lt: K.Run2, r_gt: K.Run2) -> {K.strip_esc(xs) == K.strip_esc(K.sanitize_step(xs, r_lt, r_gt)) : List<&2, K.Sym>}:
  match xs r_lt r_gt:
    case Nil{} _ _:
      {==}
    case Con{K.Lt{}, +rest} K.R0{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R0{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R0{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R1{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R2{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R1{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R2{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R1{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R2{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R2{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R2{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Lt{}, +rest} K.R2{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R1{}, K.R0{}) : {K.Lt{} <> K.strip_esc(rest) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R0{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R1{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R2{} K.R0{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R0{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R2{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R1{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R2{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R2{} K.R1{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R2{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R0{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R1{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest} K.R2{} K.R2{}:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R1{}) : {K.Gt{} <> K.strip_esc(rest) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Esc{}, +rest} K.R0{} _:
      lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} _:
      lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} _:
      lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} _:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{}) : {K.Other{} <> K.strip_esc(rest) == K.Other{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Other{}, +rest} K.R1{} _:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{}) : {K.Other{} <> K.strip_esc(rest) == K.Other{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Other{}, +rest} K.R2{} _:
      %lem_strip_esc_inv_rw(rest, K.R0{}, K.R0{}) : {K.Other{} <> K.strip_esc(rest) == K.Other{} <> _ : List<&2, K.Sym>}
      {==}

def lem_count_esc_mono(xs: List<&2, K.Sym>, r_lt: K.Run2, r_gt: K.Run2) -> {K.nat_le(K.count_esc(xs), K.count_esc(K.sanitize_step(xs, r_lt, r_gt))) == True{} : Bool}:
  match xs r_lt r_gt:
    case Nil{} _ _:
      {==}
    case Con{K.Lt{}, +rest} K.R0{} _:
      lem_count_esc_mono(rest, K.R1{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R1{} _:
      lem_count_esc_mono(rest, K.R2{}, K.R0{})
    case Con{K.Lt{}, +rest} K.R2{} _:
      lem_le_inc_r(K.count_esc(rest), K.count_esc(K.sanitize_step(rest, K.R1{}, K.R0{})), lem_count_esc_mono(rest, K.R1{}, K.R0{}))
    case Con{K.Gt{}, +rest} K.R0{} K.R0{}:
      lem_count_esc_mono(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R1{} K.R0{}:
      lem_count_esc_mono(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R2{} K.R0{}:
      lem_count_esc_mono(rest, K.R0{}, K.R1{})
    case Con{K.Gt{}, +rest} K.R0{} K.R1{}:
      lem_count_esc_mono(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R1{} K.R1{}:
      lem_count_esc_mono(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R2{} K.R1{}:
      lem_count_esc_mono(rest, K.R0{}, K.R2{})
    case Con{K.Gt{}, +rest} K.R0{} K.R2{}:
      lem_le_inc_r(K.count_esc(rest), K.count_esc(K.sanitize_step(rest, K.R0{}, K.R1{})), lem_count_esc_mono(rest, K.R0{}, K.R1{}))
    case Con{K.Gt{}, +rest} K.R1{} K.R2{}:
      lem_le_inc_r(K.count_esc(rest), K.count_esc(K.sanitize_step(rest, K.R0{}, K.R1{})), lem_count_esc_mono(rest, K.R0{}, K.R1{}))
    case Con{K.Gt{}, +rest} K.R2{} K.R2{}:
      lem_le_inc_r(K.count_esc(rest), K.count_esc(K.sanitize_step(rest, K.R0{}, K.R1{})), lem_count_esc_mono(rest, K.R0{}, K.R1{}))
    case Con{K.Esc{}, +rest} K.R0{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R1{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})
    case Con{K.Esc{}, +rest} K.R2{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R0{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R1{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})
    case Con{K.Other{}, +rest} K.R2{} _:
      lem_count_esc_mono(rest, K.R0{}, K.R0{})

def Laws.L20_fence_preserves_all_non_esc_symbols(xs):
  %lem_strip_esc_inv_rw(xs, K.R0{}, K.R0{}) : {(_, K.nat_le(K.count_esc(xs), K.count_esc(K.sanitize_fences(xs)))) == (K.strip_esc(xs), True{}) : List<&2, K.Sym> & Bool}
  %Equal.sym(Bool, K.nat_le(K.count_esc(xs), K.count_esc(K.sanitize_fences(xs))), True{}, lem_count_esc_mono(xs, K.R0{}, K.R0{})) : {(K.strip_esc(xs), _) == (K.strip_esc(xs), True{}) : List<&2, K.Sym> & Bool}
  {==}

def lem_append_other_safe(
  a: List<&2, K.Sym>, +b: List<&2, K.Sym>, r_lt: K.Run2, r_gt: K.Run2,
  ha: {False{} == K.has_triple_step(a, r_lt, r_gt) : Bool},
  hb: {K.has_triple_step(b, K.R0{}, K.R0{}) == False{} : Bool}
) -> {K.has_triple_step(K.append_sym(a, K.Other{} <> b), r_lt, r_gt) == False{} : Bool}:
  match a r_lt r_gt:
    case Nil{} K.R0{} K.R0{}:
      hb
    case Nil{} K.R0{} K.R1{}:
      hb
    case Nil{} K.R0{} K.R2{}:
      hb
    case Nil{} K.R1{} K.R0{}:
      hb
    case Nil{} K.R1{} K.R1{}:
      hb
    case Nil{} K.R1{} K.R2{}:
      hb
    case Nil{} K.R2{} K.R0{}:
      hb
    case Nil{} K.R2{} K.R1{}:
      hb
    case Nil{} K.R2{} K.R2{}:
      hb
    case Con{K.Lt{}, +rest} K.R0{} K.R0{}:
      lem_append_other_safe(rest, b, K.R1{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R0{} K.R1{}:
      lem_append_other_safe(rest, b, K.R1{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R0{} K.R2{}:
      lem_append_other_safe(rest, b, K.R1{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R1{} K.R0{}:
      lem_append_other_safe(rest, b, K.R2{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R1{} K.R1{}:
      lem_append_other_safe(rest, b, K.R2{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R1{} K.R2{}:
      lem_append_other_safe(rest, b, K.R2{}, K.R0{}, ha, hb)
    case Con{K.Lt{}, +rest} K.R2{} K.R0{}:
      lem_ex_falso_false(K.has_triple_step(K.Lt{} <> K.append_sym(rest, K.Other{} <> b), K.R2{}, K.R0{}), ha)
    case Con{K.Lt{}, +rest} K.R2{} K.R1{}:
      lem_ex_falso_false(K.has_triple_step(K.Lt{} <> K.append_sym(rest, K.Other{} <> b), K.R2{}, K.R1{}), ha)
    case Con{K.Lt{}, +rest} K.R2{} K.R2{}:
      lem_ex_falso_false(K.has_triple_step(K.Lt{} <> K.append_sym(rest, K.Other{} <> b), K.R2{}, K.R2{}), ha)
    case Con{K.Gt{}, +rest} K.R0{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R1{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R1{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R1{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R2{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R1{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R0{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R2{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R1{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R2{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R2{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R2{}, ha, hb)
    case Con{K.Gt{}, +rest} K.R0{} K.R2{}:
      lem_ex_falso_false(K.has_triple_step(K.Gt{} <> K.append_sym(rest, K.Other{} <> b), K.R0{}, K.R2{}), ha)
    case Con{K.Gt{}, +rest} K.R1{} K.R2{}:
      lem_ex_falso_false(K.has_triple_step(K.Gt{} <> K.append_sym(rest, K.Other{} <> b), K.R1{}, K.R2{}), ha)
    case Con{K.Gt{}, +rest} K.R2{} K.R2{}:
      lem_ex_falso_false(K.has_triple_step(K.Gt{} <> K.append_sym(rest, K.Other{} <> b), K.R2{}, K.R2{}), ha)
    case Con{K.Esc{}, +rest} K.R0{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R0{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R0{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R1{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R1{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R1{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R2{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R2{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Esc{}, +rest} K.R2{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R0{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R0{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R0{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R1{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R1{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R1{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R2{} K.R0{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R2{} K.R1{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)
    case Con{K.Other{}, +rest} K.R2{} K.R2{}:
      lem_append_other_safe(rest, b, K.R0{}, K.R0{}, ha, hb)

def lem_strip_append_rw(+a: List<&2, K.Sym>, +b: List<&2, K.Sym>) -> {K.append_sym(K.strip_esc(a), K.strip_esc(b)) == K.strip_esc(K.append_sym(a, b)) : List<&2, K.Sym>}:
  match a:
    case Nil{}:
      {==}
    case Con{K.Esc{}, +rest}:
      lem_strip_append_rw(rest, b)
    case Con{K.Lt{}, +rest}:
      %lem_strip_append_rw(rest, b) : {K.Lt{} <> K.append_sym(K.strip_esc(rest), K.strip_esc(b)) == K.Lt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Gt{}, +rest}:
      %lem_strip_append_rw(rest, b) : {K.Gt{} <> K.append_sym(K.strip_esc(rest), K.strip_esc(b)) == K.Gt{} <> _ : List<&2, K.Sym>}
      {==}
    case Con{K.Other{}, +rest}:
      %lem_strip_append_rw(rest, b) : {K.Other{} <> K.append_sym(K.strip_esc(rest), K.strip_esc(b)) == K.Other{} <> _ : List<&2, K.Sym>}
      {==}

def lem_strip_pair_rw(
  +a: List<&2, K.Sym>, +b_san: List<&2, K.Sym>, +b_raw: List<&2, K.Sym>,
  hb: {b_raw == K.strip_esc(b_san) : List<&2, K.Sym>}
) -> {K.append_sym(K.strip_esc(a), K.Other{} <> b_raw) == K.strip_esc(K.append_sym(K.sanitize_fences(a), K.Other{} <> b_san)) : List<&2, K.Sym>}:
  e1 = Equal.sym(List<&2, K.Sym>, K.strip_esc(a), K.strip_esc(K.sanitize_fences(a)), lem_strip_esc_inv_rw(a, K.R0{}, K.R0{}))
  e2 = Equal.sym(List<&2, K.Sym>, b_raw, K.strip_esc(b_san), hb)
  %e1 : {K.append_sym(_, K.Other{} <> b_raw) == K.strip_esc(K.append_sym(K.sanitize_fences(a), K.Other{} <> b_san)) : List<&2, K.Sym>}
  %e2 : {K.append_sym(K.strip_esc(K.sanitize_fences(a)), K.Other{} <> _) == K.strip_esc(K.append_sym(K.sanitize_fences(a), K.Other{} <> b_san)) : List<&2, K.Sym>}
  lem_strip_append_rw(K.sanitize_fences(a), K.Other{} <> b_san)

def lem_r2_field_safe(field: K.R2Field) -> {K.has_triple_angle(K.render_r2_field(field)) == False{} : Bool}:
  match field:
    case K.R2Scalar{+k, +v}:
      lem_append_other_safe(
        K.sanitize_fences(k),
        K.sanitize_fences(v),
        K.R0{}, K.R0{},
        Equal.sym(Bool, K.has_triple_angle(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        Laws.L07_fence_no_triple(v)
      )
    case K.R2Table{+k, +col, +cell}:
      pf_col_cell = lem_append_other_safe(
        K.sanitize_fences(col),
        K.sanitize_fences(cell),
        K.R0{}, K.R0{},
        Equal.sym(Bool, K.has_triple_angle(K.sanitize_fences(col)), False{}, Laws.L07_fence_no_triple(col)),
        Laws.L07_fence_no_triple(cell)
      )
      lem_append_other_safe(
        K.sanitize_fences(k),
        K.append_sym(K.sanitize_fences(col), K.Other{} <> K.sanitize_fences(cell)),
        K.R0{}, K.R0{},
        Equal.sym(Bool, K.has_triple_angle(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        pf_col_cell
      )
    case K.R2TensorRef{+k, +sp}:
      lem_append_other_safe(
        K.sanitize_fences(k),
        K.sanitize_fences(K.Lt{} <> (K.Other{} <> K.append_sym(K.sanitize_fences(sp), [K.Gt{}]))),
        K.R0{}, K.R0{},
        Equal.sym(Bool, K.has_triple_angle(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        Laws.L07_fence_no_triple(K.Lt{} <> (K.Other{} <> K.append_sym(K.sanitize_fences(sp), [K.Gt{}])))
      )

def Laws.L21_r2_structured_table_header_safe(field, tk, tcol, tcell):
  pf_safe = Equal.sym(Bool, K.has_triple_angle(K.render_r2_field(field)), False{}, lem_r2_field_safe(field))
  pf_cc = lem_strip_pair_rw(tcol, K.sanitize_fences(tcell), K.strip_esc(tcell), lem_strip_esc_inv_rw(tcell, K.R0{}, K.R0{}))
  pf_tbl = lem_strip_pair_rw(
    tk,
    K.append_sym(K.sanitize_fences(tcol), K.Other{} <> K.sanitize_fences(tcell)),
    K.append_sym(K.strip_esc(tcol), K.Other{} <> K.strip_esc(tcell)),
    pf_cc
  )
  %pf_safe : {(_, K.strip_esc(K.render_r2_field(K.R2Table{tk, tcol, tcell}))) == (False{}, K.append_sym(K.strip_esc(tk), K.Other{} <> K.append_sym(K.strip_esc(tcol), K.Other{} <> K.strip_esc(tcell)))) : Bool & List<&2, K.Sym>}
  %pf_tbl : {(False{}, _) == (False{}, K.append_sym(K.strip_esc(tk), K.Other{} <> K.append_sym(K.strip_esc(tcol), K.Other{} <> K.strip_esc(tcell)))) : Bool & List<&2, K.Sym>}
  {==}

# =============================================================================
# GROUP 3 PROOFS: PLANE A CANONICAL INTEGER + MAP SORTING & `UnusedDictKey`
# =============================================================================

def lem_roundtrip_pos(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == K.nat_lt(n, lim) : Bool}) -> {K.decode_canon_int(lim, ulim, K.encode_pos_branch(b, n)) == Some{K.LPos{n}} : Maybe<&2, K.LogicalInt>}:
  match b:
    case True{}:
      %eq : {K.decode_fix_branch(_, n) == Some{K.LPos{n}} : Maybe<&2, K.LogicalInt>}
      {==}
    case False{}:
      %eq : {K.decode_pos_branch(_, n) == Some{K.LPos{n}} : Maybe<&2, K.LogicalInt>}
      {==}

def Laws.L09_canon_roundtrip(lim, ulim, v):
  match v:
    case K.LPos{+n}:
      lem_roundtrip_pos(K.nat_lt(n, lim), lim, ulim, n, {==})
    case K.LNeg{_}:
      {==}
    case K.LBigUint{+d}:
      %lem_lt_add_false_rw(ulim, d) : {K.decode_uint_branch(_, ulim, K.nat_add(ulim, d)) == Some{K.LBigUint{d}} : Maybe<&2, K.LogicalInt>}
      %lem_add_sub_cancel_rw(ulim, d) : {Some{K.LBigUint{_}} == Some{K.LBigUint{d}} : Maybe<&2, K.LogicalInt>}
      {==}

def lem_ver_fix(b: Bool, n: Nat) -> {b == K.is_some_int(K.decode_fix_branch(b, n)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ver_pos(b: Bool, n: Nat) -> {K.bnot(b) == K.is_some_int(K.decode_pos_branch(b, n)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ver_uint(b: Bool, ulim: Nat, u: Nat) -> {K.bnot(b) == K.is_some_int(K.decode_uint_branch(b, ulim, u)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def Laws.L10_verify_iff_decode(lim, ulim, w):
  match w:
    case K.WFix{+n}:
      lem_ver_fix(K.nat_lt(n, lim), n)
    case K.WTagIntPos{+n}:
      lem_ver_pos(K.nat_lt(n, lim), n)
    case K.WTagIntNeg{_}:
      {==}
    case K.WTagUint{+u}:
      lem_ver_uint(K.nat_lt(u, ulim), ulim, u)

def lem_unique_fix(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == K.nat_lt(n, lim) : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, K.WFix{n}, K.decode_fix_branch(b, n)) == True{} : Bool}:
  match b:
    case False{}:
      {==}
    case True{}:
      %eq : {Laws.wire_int_eq(K.encode_pos_branch(_, n), K.WFix{n}) == True{} : Bool}
      lem_nat_eq_refl(n)

def lem_unique_pos(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == K.nat_lt(n, lim) : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, K.WTagIntPos{n}, K.decode_pos_branch(b, n)) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      %eq : {Laws.wire_int_eq(K.encode_pos_branch(_, n), K.WTagIntPos{n}) == True{} : Bool}
      lem_nat_eq_refl(n)

def lem_unique_uint(b: Bool, +lim: Nat, +ulim: Nat, +u: Nat, _eq: {b == K.nat_lt(u, ulim) : Bool}, restore: {K.implies(K.bnot(b), K.nat_eq(K.nat_add(ulim, K.nat_sub(u, ulim)), u)) == True{} : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, K.WTagUint{u}, K.decode_uint_branch(b, ulim, u)) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      restore

def Laws.L11_canon_wire_unique(lim, ulim, w):
  match w:
    case K.WFix{+n}:
      lem_unique_fix(K.nat_lt(n, lim), lim, ulim, n, {==})
    case K.WTagIntPos{+n}:
      lem_unique_pos(K.nat_lt(n, lim), lim, ulim, n, {==})
    case K.WTagIntNeg{+m}:
      lem_nat_eq_refl(m)
    case K.WTagUint{+u}:
      lem_unique_uint(K.nat_lt(u, ulim), lim, ulim, u, {==}, lem_sub_add_restore(ulim, u))

def lem_band_false_r_rw(x: Bool) -> {False{} == K.band(x, False{}) : Bool}:
  match x:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band3_false_m_rw(a: Bool, c: Bool) -> {False{} == K.band(a, K.band(False{}, c)) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band3_false_r_rw(a: Bool, b: Bool) -> {False{} == K.band(a, K.band(b, False{})) : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} _:
      {==}

def lem_oob_head_false_rw(+nkeys: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(K.nat_add(nkeys, d) <> tail, min_a, nkeys) : Bool}:
  +bad = K.nat_add(nkeys, d)
  %lem_lt_add_false_rw(nkeys, d) : {False{} == K.band(K.nat_le(min_a, bad), K.band(_, K.verify_map_keys_from(tail, 1n+bad, nkeys))) : Bool}
  lem_band3_false_m_rw(K.nat_le(min_a, bad), K.verify_map_keys_from(tail, 1n+bad, nkeys))

def lem_oob_deep_false_rw(prefix: List<&2, Nat>, +nkeys: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(K.append_nat(prefix, K.nat_add(nkeys, d) <> tail), min_a, nkeys) : Bool}:
  match prefix:
    case Nil{}:
      lem_oob_head_false_rw(nkeys, d, tail, min_a)
    case Con{+p, +rest}:
      %lem_oob_deep_false_rw(rest, nkeys, d, tail, 1n+p) : {False{} == K.band(K.nat_le(min_a, p), K.band(K.nat_lt(p, nkeys), _)) : Bool}
      lem_band3_false_r_rw(K.nat_le(min_a, p), K.nat_lt(p, nkeys))

def lem_le_succ_add_false_rw(b: Nat, d: Nat) -> {False{} == K.nat_le(1n+K.nat_add(b, d), b) : Bool}:
  match b:
    case 0n:
      {==}
    case 1n+pb:
      lem_le_succ_add_false_rw(pb, d)

def lem_unsorted_pair_head_rw(+nkeys: Nat, +b: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(K.nat_add(b, d) <> (b <> tail), min_a, nkeys) : Bool}:
  +hi_k = K.nat_add(b, d)
  %lem_le_succ_add_false_rw(b, d) : {False{} == K.band(K.nat_le(min_a, hi_k), K.band(K.nat_lt(hi_k, nkeys), K.band(_, K.band(K.nat_lt(b, nkeys), K.verify_map_keys_from(tail, 1n+b, nkeys))))) : Bool}
  lem_band3_false_r_rw(K.nat_le(min_a, hi_k), K.nat_lt(hi_k, nkeys))

def lem_unsorted_deep_false_rw(prefix: List<&2, Nat>, +nkeys: Nat, +b: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(K.append_nat(prefix, K.nat_add(b, d) <> (b <> tail)), min_a, nkeys) : Bool}:
  match prefix:
    case Nil{}:
      lem_unsorted_pair_head_rw(nkeys, b, d, tail, min_a)
    case Con{+p, +rest}:
      %lem_unsorted_deep_false_rw(rest, nkeys, b, d, tail, 1n+p) : {False{} == K.band(K.nat_le(min_a, p), K.band(K.nat_lt(p, nkeys), _)) : Bool}
      lem_band3_false_r_rw(K.nat_le(min_a, p), K.nat_lt(p, nkeys))

def lem_le_split(m: Nat, pk: Nat) -> {K.implies(K.nat_le(m, pk), K.bor(K.nat_eq(m, pk), K.nat_lt(m, pk))) == True{} : Bool}:
  match m pk:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pm 1n+ppk:
      lem_le_split(pm, ppk)

def lem_nat_eq_cong_r(x: Nat, m: Nat, pk: Nat) -> {K.implies(K.nat_eq(m, pk), K.implies(K.bnot(K.nat_eq(x, m)), K.bnot(K.nat_eq(x, pk)))) == True{} : Bool}:
  match x m pk:
    case 0n 0n 0n:
      {==}
    case 0n 0n 1n+_:
      {==}
    case 0n 1n+_ 0n:
      {==}
    case 0n 1n+pm 1n+ppk:
      lem_implies_true_r(K.nat_eq(pm, ppk))
    case 1n+_ 0n 0n:
      {==}
    case 1n+_ 0n 1n+_:
      {==}
    case 1n+_ 1n+_ 0n:
      {==}
    case 1n+px 1n+pm 1n+ppk:
      lem_nat_eq_cong_r(px, pm, ppk)

def lem_contains_step_bool(
  eq_m: Bool, eq_xm: Bool, eq_xp: Bool, cm: Bool, cp: Bool,
  p_hd: {K.implies(eq_m, K.implies(K.bnot(eq_xm), K.bnot(eq_xp))) == True{} : Bool},
  p_tl: {K.implies(eq_m, K.implies(K.bnot(cm), K.bnot(cp))) == True{} : Bool}
) -> {K.implies(eq_m, K.implies(K.bnot(K.bor(eq_xm, cm)), K.bnot(K.bor(eq_xp, cp)))) == True{} : Bool}:
  match eq_m eq_xm eq_xp cm cp:
    case False{} _ _ _ _:
      {==}
    case True{} True{} _ _ _:
      {==}
    case True{} False{} _ True{} _:
      {==}
    case True{} False{} False{} False{} False{}:
      {==}
    case True{} False{} True{} False{} _:
      p_hd
    case True{} False{} False{} False{} True{}:
      p_tl

def lem_contains_cong(refs: List<&2, Nat>, +m: Nat, +pk: Nat) -> {K.implies(K.nat_eq(m, pk), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.list_contains_nat(refs, pk)))) == True{} : Bool}:
  match refs:
    case Nil{}:
      lem_implies_true_r(K.nat_eq(m, pk))
    case Con{+x, +rest}:
      lem_contains_step_bool(
        K.nat_eq(m, pk),
        K.nat_eq(x, m),
        K.nat_eq(x, pk),
        K.list_contains_nat(rest, m),
        K.list_contains_nat(rest, pk),
        lem_nat_eq_cong_r(x, m, pk),
        lem_contains_cong(rest, m, pk)
      )

def lem_unused_step_bool(
  le_mp: Bool, not_cm: Bool, cp: Bool, all_p: Bool, eq_mp: Bool, lt_mp: Bool,
  p_split: {K.implies(le_mp, K.bor(eq_mp, lt_mp)) == True{} : Bool},
  p_eq: {K.implies(eq_mp, K.implies(not_cm, K.bnot(cp))) == True{} : Bool},
  p_ih: {K.implies(lt_mp, K.implies(not_cm, K.bnot(all_p))) == True{} : Bool}
) -> {K.implies(le_mp, K.implies(not_cm, K.bnot(K.band(cp, all_p)))) == True{} : Bool}:
  match le_mp not_cm cp all_p eq_mp lt_mp:
    case False{} _ _ _ _ _:
      {==}
    case True{} False{} _ _ _ _:
      {==}
    case True{} True{} False{} _ _ _:
      {==}
    case True{} True{} True{} False{} _ _:
      {==}
    case True{} True{} True{} True{} False{} False{}:
      p_split
    case True{} True{} True{} True{} True{} _:
      p_eq
    case True{} True{} True{} True{} False{} True{}:
      p_ih

def lem_unused_key_blocks(+nkeys: Nat, +m: Nat, +refs: List<&2, Nat>) -> {K.implies(K.nat_lt(m, nkeys), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.all_dict_keys_used(nkeys, refs)))) == True{} : Bool}:
  match nkeys:
    case 0n:
      {==}
    case 1n+pk:
      lem_unused_step_bool(
        K.nat_le(m, pk),
        K.bnot(K.list_contains_nat(refs, m)),
        K.list_contains_nat(refs, pk),
        K.all_dict_keys_used(pk, refs),
        K.nat_eq(m, pk),
        K.nat_lt(m, pk),
        lem_le_split(m, pk),
        lem_contains_cong(refs, m, pk),
        lem_unused_key_blocks(pk, m, refs)
      )

def Laws.L22_map_keys_and_dict_all_used(nkeys, prefix, b, d, tail, m, refs):
  pf_unused = Equal.sym(
    Bool,
    K.implies(K.nat_lt(m, nkeys), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.all_dict_keys_used(nkeys, refs)))),
    True{},
    lem_unused_key_blocks(nkeys, m, refs)
  )
  %lem_oob_deep_false_rw(prefix, nkeys, d, tail, 0n) : {((_, K.verify_map_keys(nkeys, K.append_nat(prefix, K.nat_add(b, d) <> (b <> tail)))), (K.implies(K.nat_lt(m, nkeys), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.all_dict_keys_used(nkeys, refs)))), (True{}, True{}))) == ((False{}, False{}), (True{}, (True{}, True{}))) : (Bool & Bool) & (Bool & (Bool & Bool))}
  %lem_unsorted_deep_false_rw(prefix, nkeys, b, d, tail, 0n) : {((False{}, _), (K.implies(K.nat_lt(m, nkeys), K.implies(K.bnot(K.list_contains_nat(refs, m)), K.bnot(K.all_dict_keys_used(nkeys, refs)))), (True{}, True{}))) == ((False{}, False{}), (True{}, (True{}, True{}))) : (Bool & Bool) & (Bool & (Bool & Bool))}
  %pf_unused : {((False{}, False{}), (_, (True{}, True{}))) == ((False{}, False{}), (True{}, (True{}, True{}))) : (Bool & Bool) & (Bool & (Bool & Bool))}
  {==}

# =============================================================================
# GROUP 4 PROOFS: `CWZM` EARLY-BREAK, MERKLE, `.cwwal` RE-INSERT & `StatePage`
# =============================================================================

def lem_min_le_l_rw(a: Nat, b: Nat) -> {True{} == K.nat_le(K.nat_min(a, b), a) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_min_le_l_rw(pa, pb)

def lem_min_le_r_rw(a: Nat, b: Nat) -> {True{} == K.nat_le(K.nat_min(a, b), b) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_min_le_r_rw(pa, pb)

def lem_le_max_l_rw(a: Nat, b: Nat) -> {True{} == K.nat_le(a, K.nat_max(a, b)) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+pa 0n:
      lem_nat_le_refl_rw(pa)
    case 1n+pa 1n+pb:
      lem_le_max_l_rw(pa, pb)

def lem_le_max_r_rw(a: Nat, b: Nat) -> {True{} == K.nat_le(b, K.nat_max(a, b)) : Bool}:
  match a b:
    case 0n b0:
      lem_nat_le_refl_rw(b0)
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_le_max_r_rw(pa, pb)

def lem_le_step_r(+a: Nat, +b: Nat, +c: Nat, hbc_rw: {True{} == K.nat_le(b, c) : Bool}) -> {K.implies(K.nat_le(a, b), K.nat_le(a, c)) == True{} : Bool}:
  %Equal.sym(Bool, True{}, K.nat_le(b, c), hbc_rw) : {K.implies(K.nat_le(a, b), K.implies(_, K.nat_le(a, c))) == True{} : Bool}
  lem_nat_le_trans(a, b, c)

def lem_le_step_l(+a: Nat, +b: Nat, +c: Nat, hab_rw: {True{} == K.nat_le(a, b) : Bool}) -> {K.implies(K.nat_le(b, c), K.nat_le(a, c)) == True{} : Bool}:
  %Equal.sym(Bool, True{}, K.nat_le(a, b), hab_rw) : {K.implies(_, K.implies(K.nat_le(b, c), K.nat_le(a, c))) == True{} : Bool}
  lem_nat_le_trans(a, b, c)

def lem_band_mono_bool(
  a1: Bool, a2: Bool, c1: Bool, c2: Bool,
  p1: {K.implies(a1, c1) == True{} : Bool},
  p2: {K.implies(a2, c2) == True{} : Bool}
) -> {K.implies(K.band(a1, a2), K.band(c1, c2)) == True{} : Bool}:
  match a1 a2 c1 c2:
    case False{} _ _ _:
      {==}
    case True{} False{} _ _:
      {==}
    case True{} True{} True{} True{}:
      {==}
    case True{} True{} True{} False{}:
      p2
    case True{} True{} False{} _:
      p1

def lem_overlap_preserve(z: K.Zone, +k: Nat, +lo: Nat, +hi: Nat) -> {K.implies(K.zone_overlaps(z, lo, hi), K.zone_overlaps(K.zone_extend(z, k), lo, hi)) == True{} : Bool}:
  match z:
    case K.Zone{+mn, +mx}:
      lem_band_mono_bool(
        K.nat_le(lo, mx),
        K.nat_le(mn, hi),
        K.nat_le(lo, K.nat_max(mx, k)),
        K.nat_le(K.nat_min(mn, k), hi),
        lem_le_step_r(lo, mx, K.nat_max(mx, k), lem_le_max_l_rw(mx, k)),
        lem_le_step_l(K.nat_min(mn, k), mn, hi, lem_min_le_l_rw(mn, k))
      )

def lem_hit_gives_overlap(z: K.Zone, +k: Nat, +lo: Nat, +hi: Nat) -> {K.implies(K.in_range(k, lo, hi), K.zone_overlaps(K.zone_extend(z, k), lo, hi)) == True{} : Bool}:
  match z:
    case K.Zone{+mn, +mx}:
      lem_band_mono_bool(
        K.nat_le(lo, k),
        K.nat_le(k, hi),
        K.nat_le(lo, K.nat_max(mx, k)),
        K.nat_le(K.nat_min(mn, k), hi),
        lem_le_step_r(lo, k, K.nat_max(mx, k), lem_le_max_r_rw(mx, k)),
        lem_le_step_l(K.nat_min(mn, k), k, hi, lem_min_le_r_rw(mn, k))
      )

def lem_bor_false_imp(a: Bool) -> {K.implies(K.bor(a, False{}), a) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_loop_step_bool(
  ov: Bool, hit_k: Bool, hit_rest: Bool, ov_next: Bool, ov_final: Bool,
  p_pres: {K.implies(ov, ov_next) == True{} : Bool},
  p_hit: {K.implies(hit_k, ov_next) == True{} : Bool},
  p_ih: {K.implies(K.bor(ov_next, hit_rest), ov_final) == True{} : Bool}
) -> {K.implies(K.bor(ov, K.bor(hit_k, hit_rest)), ov_final) == True{} : Bool}:
  match ov hit_k hit_rest ov_next ov_final:
    case False{} False{} False{} _ _:
      {==}
    case _ _ _ _ True{}:
      {==}
    case _ _ True{} True{} False{}:
      p_ih
    case _ _ True{} False{} False{}:
      p_ih
    case True{} _ False{} True{} False{}:
      p_ih
    case True{} _ False{} False{} False{}:
      p_pres
    case False{} True{} False{} True{} False{}:
      p_ih
    case False{} True{} False{} False{} False{}:
      p_hit

def lem_zone_loop_mono(ks: List<&2, Nat>, +z: K.Zone, +lo: Nat, +hi: Nat) -> {K.implies(K.bor(K.zone_overlaps(z, lo, hi), K.list_has_in_range(ks, lo, hi)), K.zone_overlaps(K.build_zone_loop(ks, z), lo, hi)) == True{} : Bool}:
  match ks:
    case Nil{}:
      lem_bor_false_imp(K.zone_overlaps(z, lo, hi))
    case Con{+k, +rest}:
      lem_loop_step_bool(
        K.zone_overlaps(z, lo, hi),
        K.in_range(k, lo, hi),
        K.list_has_in_range(rest, lo, hi),
        K.zone_overlaps(K.zone_extend(z, k), lo, hi),
        K.zone_overlaps(K.build_zone_loop(rest, K.zone_extend(z, k)), lo, hi),
        lem_overlap_preserve(z, k, lo, hi),
        lem_hit_gives_overlap(z, k, lo, hi),
        lem_zone_loop_mono(rest, K.zone_extend(z, k), lo, hi)
      )

def Laws.L12_zonemap_no_false_neg(k0, ks, lo, hi):
  lem_zone_loop_mono(ks, K.Zone{k0, k0}, lo, hi)

# Multi-page `CWZM` sorted early-break equivalence (`L23`)
def lem_band_true_r_rw(b: Bool) -> {b == K.band(b, True{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_push_if_same(cond: Bool, +idx: Nat, +l1: List<&2, Nat>, +l2: List<&2, Nat>, ih_eq: {True{} == K.nat_list_eq(l1, l2) : Bool}) -> {K.nat_list_eq(K.push_if(cond, idx, l1), K.push_if(cond, idx, l2)) == True{} : Bool}:
  match cond:
    case False{}:
      Equal.sym(Bool, True{}, K.nat_list_eq(l1, l2), ih_eq)
    case True{}:
      %lem_nat_eq_refl_rw(idx) : {K.band(_, K.nat_list_eq(l1, l2)) == True{} : Bool}
      %ih_eq : {K.band(True{}, _) == True{} : Bool}
      {==}

def lem_above_hi_bool(
  le_pm_mn: Bool, srt_rest: Bool, le_pm_hi: Bool, le_mn_hi: Bool, +ge_lo: Bool, +idx: Nat, +tail_list: List<&2, Nat>,
  p_trans: {K.implies(le_pm_mn, K.implies(le_mn_hi, le_pm_hi)) == True{} : Bool},
  p_ih: {K.implies(srt_rest, K.implies(K.bnot(le_mn_hi), K.nat_list_eq(Nil{}, tail_list))) == True{} : Bool}
) -> {K.implies(K.band(le_pm_mn, srt_rest), K.implies(K.bnot(le_pm_hi), K.nat_list_eq(Nil{}, K.push_if(K.band(ge_lo, le_mn_hi), idx, tail_list)))) == True{} : Bool}:
  match le_pm_mn srt_rest le_pm_hi le_mn_hi:
    case False{} _ _ _:
      {==}
    case True{} False{} _ _:
      {==}
    case True{} True{} True{} _:
      {==}
    case True{} True{} False{} True{}:
      lem_ex_falso_true(K.nat_list_eq(Nil{}, K.push_if(K.band(ge_lo, True{}), idx, tail_list)), p_trans)
    case True{} True{} False{} False{}:
      %lem_band_false_r_rw(ge_lo) : {K.nat_list_eq(Nil{}, K.push_if(_, idx, tail_list)) == True{} : Bool}
      p_ih

def lem_above_hi_nil(zs: List<&2, K.Zone>, +idx: Nat, +prev_min: Nat, +lo: Nat, +hi: Nat) -> {K.implies(K.zones_min_sorted(zs, prev_min), K.implies(K.bnot(K.nat_le(prev_min, hi)), K.nat_list_eq(Nil{}, K.filter_all_zones_at(zs, idx, lo, hi)))) == True{} : Bool}:
  match zs:
    case Nil{}:
      lem_implies_true_r(K.bnot(K.nat_le(prev_min, hi)))
    case Con{K.Zone{+mn, +mx}, +rest}:
      lem_above_hi_bool(
        K.nat_le(prev_min, mn),
        K.zones_min_sorted(rest, mn),
        K.nat_le(prev_min, hi),
        K.nat_le(mn, hi),
        K.nat_le(lo, mx),
        idx,
        K.filter_all_zones_at(rest, 1n+idx, lo, hi),
        lem_nat_le_trans(prev_min, mn, hi),
        lem_above_hi_nil(rest, 1n+idx, mn, lo, hi)
      )

def lem_prune_step_bool(
  le_pm_mn: Bool, srt_rest: Bool, le_mn_hi: Bool, +ge_lo: Bool, +idx: Nat,
  +prune_rest: List<&2, Nat>, +filt_rest: List<&2, Nat>,
  p_nil: {K.implies(srt_rest, K.implies(K.bnot(le_mn_hi), K.nat_list_eq(Nil{}, filt_rest))) == True{} : Bool},
  p_ih: {K.implies(srt_rest, K.nat_list_eq(prune_rest, filt_rest)) == True{} : Bool}
) -> {K.implies(K.band(le_pm_mn, srt_rest), K.nat_list_eq(K.prune_branch_nonrec(le_mn_hi, ge_lo, idx, prune_rest), K.push_if(K.band(ge_lo, le_mn_hi), idx, filt_rest))) == True{} : Bool}:
  match le_pm_mn srt_rest le_mn_hi:
    case False{} _ _:
      {==}
    case True{} False{} _:
      {==}
    case True{} True{} False{}:
      %lem_band_false_r_rw(ge_lo) : {K.nat_list_eq(Nil{}, K.push_if(_, idx, filt_rest)) == True{} : Bool}
      p_nil
    case True{} True{} True{}:
      %lem_band_true_r_rw(ge_lo) : {K.nat_list_eq(K.push_if(ge_lo, idx, prune_rest), K.push_if(_, idx, filt_rest)) == True{} : Bool}
      lem_push_if_same(ge_lo, idx, prune_rest, filt_rest, Equal.sym(Bool, K.nat_list_eq(prune_rest, filt_rest), True{}, p_ih))

def lem_prune_eq_filter(zs: List<&2, K.Zone>, +idx: Nat, +prev_min: Nat, +lo: Nat, +hi: Nat) -> {K.implies(K.zones_min_sorted(zs, prev_min), K.nat_list_eq(K.prune_sorted_zones_at(zs, idx, lo, hi), K.filter_all_zones_at(zs, idx, lo, hi))) == True{} : Bool}:
  match zs:
    case Nil{}:
      {==}
    case Con{K.Zone{+mn, +mx}, +rest}:
      lem_prune_step_bool(
        K.nat_le(prev_min, mn),
        K.zones_min_sorted(rest, mn),
        K.nat_le(mn, hi),
        K.nat_le(lo, mx),
        idx,
        K.prune_sorted_zones_at(rest, 1n+idx, lo, hi),
        K.filter_all_zones_at(rest, 1n+idx, lo, hi),
        lem_above_hi_nil(rest, 1n+idx, mn, lo, hi),
        lem_prune_eq_filter(rest, 1n+idx, mn, lo, hi)
      )

def Laws.L23_sorted_zonemap_early_break_exact(zs, lo, hi):
  lem_prune_eq_filter(zs, 0n, 0n, lo, hi)

def Laws.L13_merkle_domain_and_odd_safe(_f1, _f2, _t0, _trest, _a, _b, _c):
  {==}

def lem_wal_torn_rw(prefix: List<&2, Nat>, +tp: Nat) -> {K.WalOk{prefix} == K.wal_recover(K.append_valid_prefix(prefix, [K.WalRec{False{}, tp}])) : K.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_wal_torn_rw(rest, tp) : {K.WalOk{p <> rest} == K.wal_prepend(p, _) : K.WalResult}
      {==}

def lem_wal_mid_rw(prefix: List<&2, Nat>, +bp: Nat, +after: K.WalRec, +more: List<&2, K.WalRec>) -> {K.WalErr{} == K.wal_recover(K.append_valid_prefix(prefix, K.WalRec{False{}, bp} <> (after <> more))) : K.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_wal_mid_rw(rest, bp, after, more) : {K.WalErr{} == K.wal_prepend(p, _) : K.WalResult}
      {==}

def Laws.L14_wal_torn_vs_mid_tamper(prefix, torn_payload, bad_payload, after, more):
  %lem_wal_torn_rw(prefix, torn_payload) : {(_, K.wal_recover(K.append_valid_prefix(prefix, K.WalRec{False{}, bad_payload} <> (after <> more)))) == (K.WalOk{prefix}, K.WalErr{}) : K.WalResult & K.WalResult}
  %lem_wal_mid_rw(prefix, bad_payload, after, more) : {(K.WalOk{prefix}, _) == (K.WalOk{prefix}, K.WalErr{}) : K.WalResult & K.WalResult}
  {==}

def lem_wal_bystander_rw(+k: Nat) -> {[0n] == K.wal_replay_live([K.OpFrame{0n}, K.OpFrame{1n+k}, K.OpTomb{1n+k}], Nil{}) : List<&2, Nat>}:
  %lem_nat_eq_refl_rw(k) : {[0n] == 0n <> K.remove_cid_keep(_, 1n+k, Nil{}) : List<&2, Nat>}
  {==}

def lem_wal_reinsert_rw(+cid: Nat) -> {[cid] == K.wal_replay_live([K.OpFrame{cid}, K.OpTomb{cid}, K.OpFrame{cid}], Nil{}) : List<&2, Nat>}:
  %lem_nat_eq_refl_rw(cid) : {[cid] == K.wal_replay_live([K.OpFrame{cid}], K.remove_cid_keep(_, cid, Nil{})) : List<&2, Nat>}
  {==}

def lem_rt_none_allows_rw(file_rt: Maybe<&2, Nat>) -> {True{} == K.check_statepage_runtime(None{}, file_rt) : Bool}:
  match file_rt:
    case None{}:
      {==}
    case Some{_}:
      {==}

def Laws.L24_wal_tombstone_reinsert_and_statepage_rt(cid, rt, k, file_rt):
  %lem_wal_bystander_rw(k) : {((_, K.wal_replay_live([K.OpFrame{cid}, K.OpTomb{cid}, K.OpFrame{cid}], Nil{})), (False{}, (False{}, (K.nat_eq(rt, rt), K.check_statepage_runtime(None{}, file_rt))))) == (([0n], [cid]), (False{}, (False{}, (True{}, True{})))) : (List<&2, Nat> & List<&2, Nat>) & (Bool & (Bool & (Bool & Bool)))}
  %lem_wal_reinsert_rw(cid) : {(([0n], _), (False{}, (False{}, (K.nat_eq(rt, rt), K.check_statepage_runtime(None{}, file_rt))))) == (([0n], [cid]), (False{}, (False{}, (True{}, True{})))) : (List<&2, Nat> & List<&2, Nat>) & (Bool & (Bool & (Bool & Bool)))}
  %lem_nat_eq_refl_rw(rt) : {(([0n], [cid]), (False{}, (False{}, (_, K.check_statepage_runtime(None{}, file_rt))))) == (([0n], [cid]), (False{}, (False{}, (True{}, True{})))) : (List<&2, Nat> & List<&2, Nat>) & (Bool & (Bool & (Bool & Bool)))}
  %lem_rt_none_allows_rw(file_rt) : {(([0n], [cid]), (False{}, (False{}, (True{}, _)))) == (([0n], [cid]), (False{}, (False{}, (True{}, True{})))) : (List<&2, Nat> & List<&2, Nat>) & (Bool & (Bool & (Bool & Bool)))}
  {==}
