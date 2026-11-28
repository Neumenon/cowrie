# PROOF.bend — Machine-Checked Proofs of All 37 Tight Cowrie v1.2 Formal Laws
#
# Checked by `/home/omen/toolchains/bend-2.0.35/rel/bend/bin/bend PROOF.bend --verdict`
# (both `bend2` and the Lean-proven BendTT kernel). Zero `@unsafe`, zero `?` holes, zero `~` templates.

import Base
import ./SPEC.bend as S
import ./CowrieKernel.bend as K
import ./LAWS.bend as Laws

# =============================================================================
# 0. GENERAL BOOLEAN, NAT & LIST LEMMAS
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

def lem_bool_eq_refl(b: Bool) -> {S.bool_eq(b, b) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_bor_false_r_rw(b: Bool) -> {b == S.bor(b, False{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_bor_true_r_rw(b: Bool) -> {True{} == S.bor(b, True{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_bor_comm_rw(a: Bool, b: Bool) -> {S.bor(a, b) == S.bor(b, a) : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}

def lem_bor_assoc_rw(a: Bool, b: Bool, c: Bool) -> {S.bor(a, S.bor(b, c)) == S.bor(S.bor(a, b), c) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band_true_l_rw(b: Bool) -> {b == S.band(True{}, b) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band_true_r_rw(b: Bool) -> {b == S.band(b, True{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band_false_r_rw(b: Bool) -> {False{} == S.band(b, False{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band3_false_m_rw(a: Bool, c: Bool) -> {False{} == S.band(a, S.band(False{}, c)) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_band3_false_r_rw(a: Bool, b: Bool) -> {False{} == S.band(a, S.band(b, False{})) : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} _:
      {==}

def lem_implies_self(b: Bool) -> {S.implies(b, b) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_implies_true_r(x: Bool) -> {S.implies(x, True{}) == True{} : Bool}:
  match x:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_mp(p: Bool, q: Bool, hp: {p == True{} : Bool}, hpq: {S.implies(p, q) == True{} : Bool}) -> {q == True{} : Bool}:
  match p q:
    case True{} True{}:
      {==}
    case True{} False{}:
      hpq
    case False{} True{}:
      {==}
    case False{} False{}:
      hp

def lem_nat_eq_refl(n: Nat) -> {S.nat_eq(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_eq_refl(p)

def lem_nat_eq_refl_rw(n: Nat) -> {True{} == S.nat_eq(n, n) : Bool}:
  Equal.sym(Bool, S.nat_eq(n, n), True{}, lem_nat_eq_refl(n))

def lem_nat_eq_sym_rw(a: Nat, b: Nat) -> {S.nat_eq(a, b) == S.nat_eq(b, a) : Bool}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_nat_eq_sym_rw(pa, pb)

def lem_nat_eq_succ_add_false_rw(a: Nat, d: Nat) -> {False{} == S.nat_eq(a, 1n+S.nat_add(a, d)) : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_eq_succ_add_false_rw(p, d)

def lem_nat_eq_succ_add_l_false_rw(a: Nat, d: Nat) -> {False{} == S.nat_eq(1n+S.nat_add(a, d), a) : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_eq_succ_add_l_false_rw(p, d)

def lem_nat_le_refl(n: Nat) -> {S.nat_le(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_nat_le_refl(p)

def lem_nat_le_refl_rw(n: Nat) -> {True{} == S.nat_le(n, n) : Bool}:
  Equal.sym(Bool, S.nat_le(n, n), True{}, lem_nat_le_refl(n))

def lem_le_succ_r_rw(n: Nat) -> {True{} == S.nat_le(n, 1n+n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lem_le_succ_r_rw(p)

def lem_le_add_r_rw(a: Nat, d: Nat) -> {True{} == S.nat_le(a, S.nat_add(a, d)) : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      lem_le_add_r_rw(p, d)

def lem_le_succ_add_false_rw(b: Nat, d: Nat) -> {False{} == S.nat_le(1n+S.nat_add(b, d), b) : Bool}:
  match b:
    case 0n:
      {==}
    case 1n+pb:
      lem_le_succ_add_false_rw(pb, d)

def lem_lt_add_false_rw(ulim: Nat, d: Nat) -> {False{} == S.nat_lt(S.nat_add(ulim, d), ulim) : Bool}:
  match ulim:
    case 0n:
      {==}
    case 1n+p:
      lem_lt_add_false_rw(p, d)

def lem_add_succ_r_rw(a: Nat, b: Nat) -> {1n+S.nat_add(a, b) == S.nat_add(a, 1n+b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %lem_add_succ_r_rw(p, b) : {1n+(1n+S.nat_add(p, b)) == 1n+_ : Nat}
      {==}

def lem_add_sub_cancel_rw(ulim: Nat, d: Nat) -> {d == S.nat_sub(S.nat_add(ulim, d), ulim) : Nat}:
  match ulim d:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+p d0:
      lem_add_sub_cancel_rw(p, d0)

def lem_sub_add_restore(ulim: Nat, u: Nat) -> {S.implies(S.bnot(S.nat_lt(u, ulim)), S.nat_eq(S.nat_add(ulim, S.nat_sub(u, ulim)), u)) == True{} : Bool}:
  match ulim u:
    case 0n 0n:
      {==}
    case 0n 1n+pu:
      lem_nat_eq_refl(1n+pu)
    case 1n+_ 0n:
      {==}
    case 1n+pl 1n+pu:
      lem_sub_add_restore(pl, pu)

def lem_nat_le_trans(a: Nat, b: Nat, c: Nat) -> {S.implies(S.nat_le(a, b), S.implies(S.nat_le(b, c), S.nat_le(a, c))) == True{} : Bool}:
  match a b c:
    case 0n b0 c0:
      lem_implies_true_r(S.nat_le(b0, c0))
    case 1n+_ 0n _:
      {==}
    case 1n+pa 1n+pb 0n:
      lem_implies_true_r(S.nat_le(pa, pb))
    case 1n+pa 1n+pb 1n+pc:
      lem_nat_le_trans(pa, pb, pc)

def lem_le_step_r(+a: Nat, +b: Nat, +c: Nat, hbc_rw: {True{} == S.nat_le(b, c) : Bool}) -> {S.implies(S.nat_le(a, b), S.nat_le(a, c)) == True{} : Bool}:
  %Equal.sym(Bool, True{}, S.nat_le(b, c), hbc_rw) : {S.implies(S.nat_le(a, b), S.implies(_, S.nat_le(a, c))) == True{} : Bool}
  lem_nat_le_trans(a, b, c)

def lem_le_step_l(+a: Nat, +b: Nat, +c: Nat, hab_rw: {True{} == S.nat_le(a, b) : Bool}) -> {S.implies(S.nat_le(b, c), S.nat_le(a, c)) == True{} : Bool}:
  %Equal.sym(Bool, True{}, S.nat_le(a, b), hab_rw) : {S.implies(_, S.implies(S.nat_le(b, c), S.nat_le(a, c))) == True{} : Bool}
  lem_nat_le_trans(a, b, c)

def lem_le_inc_r(+a: Nat, +b: Nat, ih: {S.nat_le(a, b) == True{} : Bool}) -> {S.nat_le(a, 1n+b) == True{} : Bool}:
  lem_mp(S.nat_le(a, b), S.nat_le(a, 1n+b), ih, lem_le_step_r(a, b, 1n+b, lem_le_succ_r_rw(b)))

def lem_lt_implies_not_le(mx: Nat, lo: Nat) -> {S.implies(S.nat_lt(mx, lo), S.bnot(S.nat_le(lo, mx))) == True{} : Bool}:
  match mx lo:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pm 1n+pl:
      lem_lt_implies_not_le(pm, pl)

def lem_not_lt_implies_le(mx: Nat, lo: Nat) -> {S.implies(S.bnot(S.nat_lt(mx, lo)), S.nat_le(lo, mx)) == True{} : Bool}:
  match mx lo:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pm 1n+pl:
      lem_not_lt_implies_le(pm, pl)

def lem_append_nat_assoc_rw(a: List<&2, Nat>, +b: List<&2, Nat>, +c: List<&2, Nat>) -> {S.append_nat(a, S.append_nat(b, c)) == S.append_nat(S.append_nat(a, b), c) : List<&2, Nat>}:
  match a:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      %lem_append_nat_assoc_rw(t, b, c) : {h <> S.append_nat(t, S.append_nat(b, c)) == h <> _ : List<&2, Nat>}
      {==}

def lem_append_nat_nil_r_rw(a: List<&2, Nat>) -> {a == S.append_nat(a, Nil{}) : List<&2, Nat>}:
  match a:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      %lem_append_nat_nil_r_rw(t) : {h <> t == h <> _ : List<&2, Nat>}
      {==}

def lem_sym_eq_refl(x: S.Sym) -> {S.sym_eq(x, x) == True{} : Bool}:
  match x:
    case S.Lt{}:
      {==}
    case S.Gt{}:
      {==}
    case S.Pipe{}:
      {==}
    case S.Esc{}:
      {==}
    case S.Hidden{}:
      {==}
    case S.Other{}:
      {==}

# =============================================================================
# 1. GROUP 1 PROOFS: PLANE D TRUST, DEEP LEDGER, GATE, HEADER META & MCP
# =============================================================================

def lem_is_tainted_spec_rw(a: S.Auth) -> {S.spec_tainted(a) == K.is_tainted(a) : Bool}:
  match a:
    case S.Auth{_, True{}, _}:
      {==}
    case S.Auth{_, False{}, _}:
      {==}

def lem_any_tainted_spec_rw(ps: List<&2, S.Auth>) -> {S.spec_any_tainted(ps) == K.any_tainted(ps) : Bool}:
  match ps:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      %lem_is_tainted_spec_rw(h) : {S.bor(S.spec_tainted(h), S.spec_any_tainted(t)) == S.bor(_, K.any_tainted(t)) : Bool}
      %lem_any_tainted_spec_rw(t) : {S.bor(S.spec_tainted(h), S.spec_any_tainted(t)) == S.bor(S.spec_tainted(h), _) : Bool}
      {==}

def lem_prop_bool(base: S.Auth, x: Bool) -> {K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), x), base)) == S.bor(K.is_tainted(base), x) : Bool}:
  match base x:
    case S.Auth{_, True{}, True{}} True{}:
      {==}
    case S.Auth{_, True{}, True{}} False{}:
      {==}
    case S.Auth{_, True{}, False{}} True{}:
      {==}
    case S.Auth{_, True{}, False{}} False{}:
      {==}
    case S.Auth{_, False{}, True{}} True{}:
      {==}
    case S.Auth{_, False{}, True{}} False{}:
      {==}
    case S.Auth{_, False{}, False{}} True{}:
      {==}
    case S.Auth{_, False{}, False{}} False{}:
      {==}

def lem_prop_spec_rw(+base: S.Auth, +ps: List<&2, S.Auth>) -> {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == K.is_tainted(K.propagate(base, ps)) : Bool}:
  pf = Equal.sym(Bool, K.is_tainted(K.propagate(base, ps)), S.bor(K.is_tainted(base), K.any_tainted(ps)), lem_prop_bool(base, K.any_tainted(ps)))
  %pf : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == _ : Bool}
  %lem_is_tainted_spec_rw(base) : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == S.bor(_, K.any_tainted(ps)) : Bool}
  %lem_any_tainted_spec_rw(ps) : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == S.bor(S.spec_tainted(base), _) : Bool}
  {==}

def lem_prop_tbit_bool(base: S.Auth, x: Bool) -> {S.spec_auth_tainted_bit(K.prop_apply(S.bor(K.is_tainted(base), x), base)) == S.bor(K.is_tainted(base), x) : Bool}:
  match base x:
    case S.Auth{_, True{}, True{}} True{}:
      {==}
    case S.Auth{_, True{}, True{}} False{}:
      {==}
    case S.Auth{_, True{}, False{}} True{}:
      {==}
    case S.Auth{_, True{}, False{}} False{}:
      {==}
    case S.Auth{_, False{}, True{}} True{}:
      {==}
    case S.Auth{_, False{}, True{}} False{}:
      {==}
    case S.Auth{_, False{}, False{}} True{}:
      {==}
    case S.Auth{_, False{}, False{}} False{}:
      {==}

def lem_prop_tbit_spec_rw(+base: S.Auth, +ps: List<&2, S.Auth>) -> {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == S.spec_auth_tainted_bit(K.propagate(base, ps)) : Bool}:
  pf = Equal.sym(Bool, S.spec_auth_tainted_bit(K.propagate(base, ps)), S.bor(K.is_tainted(base), K.any_tainted(ps)), lem_prop_tbit_bool(base, K.any_tainted(ps)))
  %pf : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == _ : Bool}
  %lem_is_tainted_spec_rw(base) : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == S.bor(_, K.any_tainted(ps)) : Bool}
  %lem_any_tainted_spec_rw(ps) : {S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)) == S.bor(S.spec_tainted(base), _) : Bool}
  {==}

def Laws.L01_propagate_iff(c, e, t, base, ps):
  %lem_is_tainted_spec_rw(S.Auth{c, e, t}) : {(_, (K.is_tainted(K.propagate(base, ps)), S.spec_auth_tainted_bit(K.propagate(base, ps)))) == (S.bor(e, t), (S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)), S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)))) : Bool & (Bool & Bool)}
  %lem_prop_spec_rw(base, ps) : {(S.bor(e, t), (_, S.spec_auth_tainted_bit(K.propagate(base, ps)))) == (S.bor(e, t), (S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)), S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)))) : Bool & (Bool & Bool)}
  %lem_prop_tbit_spec_rw(base, ps) : {(S.bor(e, t), (S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)), _)) == (S.bor(e, t), (S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)), S.bor(S.spec_tainted(base), S.spec_any_tainted(ps)))) : Bool & (Bool & Bool)}
  {==}

def lem_prop_one_spec_rw(+h: S.Auth, +root: S.Auth) -> {S.bor(S.spec_tainted(root), S.spec_tainted(h)) == S.spec_tainted(K.propagate(h, [root])) : Bool}:
  p1 = Equal.sym(Bool, S.spec_tainted(K.propagate(h, [root])), K.is_tainted(K.propagate(h, [root])), lem_is_tainted_spec_rw(K.propagate(h, [root])))
  %p1 : {S.bor(S.spec_tainted(root), S.spec_tainted(h)) == _ : Bool}
  %lem_prop_spec_rw(h, [root]) : {S.bor(S.spec_tainted(root), S.spec_tainted(h)) == _ : Bool}
  %lem_bor_false_r_rw(S.spec_tainted(root)) : {S.bor(S.spec_tainted(root), S.spec_tainted(h)) == S.bor(S.spec_tainted(h), _) : Bool}
  lem_bor_comm_rw(S.spec_tainted(root), S.spec_tainted(h))

def Laws.L02_taint_irrevocable_hops(hops, root):
  match hops:
    case Nil{}:
      %lem_is_tainted_spec_rw(root) : {_ == S.bor(S.spec_tainted(root), False{}) : Bool}
      lem_bor_false_r_rw(S.spec_tainted(root))
    case Con{+h, +rest}:
      ih = Equal.sym(
        Bool,
        K.is_tainted(K.derive_chain(rest, K.propagate(h, [root]))),
        S.bor(S.spec_tainted(K.propagate(h, [root])), S.spec_any_tainted(rest)),
        Laws.L02_taint_irrevocable_hops(rest, K.propagate(h, [root]))
      )
      %ih : {_ == S.bor(S.spec_tainted(root), S.bor(S.spec_tainted(h), S.spec_any_tainted(rest))) : Bool}
      %lem_prop_one_spec_rw(h, root) : {S.bor(_, S.spec_any_tainted(rest)) == S.bor(S.spec_tainted(root), S.bor(S.spec_tainted(h), S.spec_any_tainted(rest))) : Bool}
      Equal.sym(Bool, S.bor(S.spec_tainted(root), S.bor(S.spec_tainted(h), S.spec_any_tainted(rest))), S.bor(S.bor(S.spec_tainted(root), S.spec_tainted(h)), S.spec_any_tainted(rest)), lem_bor_assoc_rw(S.spec_tainted(root), S.spec_tainted(h), S.spec_any_tainted(rest)))

def lem_set_tainted_true(a: S.Auth) -> {K.is_tainted(S.spec_set_tainted(a)) == True{} : Bool}:
  match a:
    case S.Auth{_, True{}, _}:
      {==}
    case S.Auth{_, False{}, _}:
      {==}

def lem_set_tainted_true_rw(a: S.Auth) -> {True{} == K.is_tainted(S.spec_set_tainted(a)) : Bool}:
  Equal.sym(Bool, K.is_tainted(S.spec_set_tainted(a)), True{}, lem_set_tainted_true(a))

def lem_prop_forced_true(base: S.Auth, p: Bool) -> {K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(True{}, p)), base)) == True{} : Bool}:
  match base:
    case S.Auth{_, True{}, True{}}:
      {==}
    case S.Auth{_, True{}, False{}}:
      {==}
    case S.Auth{_, False{}, True{}}:
      {==}
    case S.Auth{_, False{}, False{}}:
      {==}

def lem_sticky_ext(+cid: Nat, +oc: Nat, +rest: List<&2, S.LedgerNode>, +base: S.Auth, +ps: List<&2, Nat>) -> {True{} == K.is_tainted(K.ledger_record(S.LNode{cid, S.Auth{oc, True{}, False{}}} <> rest, cid, base, ps)) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {True{} == K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(K.existing_tainted(K.select_lookup(_, S.Auth{oc, True{}, False{}}, K.ledger_lookup(rest, cid))), K.parents_tainted(ps, S.LNode{cid, S.Auth{oc, True{}, False{}}} <> rest))), base)) : Bool}
  Equal.sym(Bool, K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(True{}, K.parents_tainted(ps, S.LNode{cid, S.Auth{oc, True{}, False{}}} <> rest))), base)), True{}, lem_prop_forced_true(base, K.parents_tainted(ps, S.LNode{cid, S.Auth{oc, True{}, False{}}} <> rest)))

def lem_sticky_tbit(+cid: Nat, +old: S.Auth, +rest: List<&2, S.LedgerNode>, +base: S.Auth, +ps: List<&2, Nat>) -> {True{} == K.is_tainted(K.ledger_record(S.LNode{cid, S.spec_set_tainted(old)} <> rest, cid, base, ps)) : Bool}:
  %lem_nat_eq_refl_rw(cid) : {True{} == K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(K.existing_tainted(K.select_lookup(_, S.spec_set_tainted(old), K.ledger_lookup(rest, cid))), K.parents_tainted(ps, S.LNode{cid, S.spec_set_tainted(old)} <> rest))), base)) : Bool}
  %lem_set_tainted_true_rw(old) : {True{} == K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(_, K.parents_tainted(ps, S.LNode{cid, S.spec_set_tainted(old)} <> rest))), base)) : Bool}
  Equal.sym(Bool, K.is_tainted(K.prop_apply(S.bor(K.is_tainted(base), S.bor(True{}, K.parents_tainted(ps, S.LNode{cid, S.spec_set_tainted(old)} <> rest))), base)), True{}, lem_prop_forced_true(base, K.parents_tainted(ps, S.LNode{cid, S.spec_set_tainted(old)} <> rest)))

def Laws.L03_sticky_reregister(cid, oc, old, rest, base, ps):
  %lem_sticky_ext(cid, oc, rest, base, ps) : {(_, K.is_tainted(K.ledger_record(S.LNode{cid, S.spec_set_tainted(old)} <> rest, cid, base, ps))) == (True{}, True{}) : Bool & Bool}
  %lem_sticky_tbit(cid, old, rest, base, ps) : {(True{}, _) == (True{}, True{}) : Bool & Bool}
  {==}

# Bridge lemmas for Ledger & Gate between frozen `SPEC.bend` and `CowrieKernel.bend`
def lem_select_lookup_spec_rw(eq: Bool, a: S.Auth, fb: Maybe<&2, S.Auth>) -> {S.spec_select_lookup(eq, a, fb) == K.select_lookup(eq, a, fb) : Maybe<&2, S.Auth>}:
  match eq:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ledger_lookup_spec_rw(lg: List<&2, S.LedgerNode>, +cid: Nat) -> {S.spec_ledger_lookup(lg, cid) == K.ledger_lookup(lg, cid) : Maybe<&2, S.Auth>}:
  match lg:
    case Nil{}:
      {==}
    case Con{S.LNode{+k, +a}, +rest}:
      %lem_ledger_lookup_spec_rw(rest, cid) : {S.spec_select_lookup(S.nat_eq(k, cid), a, S.spec_ledger_lookup(rest, cid)) == K.select_lookup(S.nat_eq(k, cid), a, _) : Maybe<&2, S.Auth>}
      lem_select_lookup_spec_rw(S.nat_eq(k, cid), a, S.spec_ledger_lookup(rest, cid))

def lem_opt_tainted_spec_rw(ma: Maybe<&2, S.Auth>) -> {S.spec_opt_tainted(ma) == K.opt_auth_tainted(ma) : Bool}:
  match ma:
    case None{}:
      {==}
    case Some{a}:
      lem_is_tainted_spec_rw(a)

def lem_existing_tainted_spec_rw(ma: Maybe<&2, S.Auth>) -> {S.spec_existing_tainted(ma) == K.existing_tainted(ma) : Bool}:
  match ma:
    case None{}:
      {==}
    case Some{a}:
      lem_is_tainted_spec_rw(a)

def lem_parents_tainted_spec_rw(ps: List<&2, Nat>, +lg: List<&2, S.LedgerNode>) -> {S.spec_parents_tainted(ps, lg) == K.parents_tainted(ps, lg) : Bool}:
  match ps:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_ledger_lookup_spec_rw(lg, p) : {S.bor(S.spec_opt_tainted(S.spec_ledger_lookup(lg, p)), S.spec_parents_tainted(rest, lg)) == S.bor(K.opt_auth_tainted(_), K.parents_tainted(rest, lg)) : Bool}
      %lem_opt_tainted_spec_rw(S.spec_ledger_lookup(lg, p)) : {S.bor(S.spec_opt_tainted(S.spec_ledger_lookup(lg, p)), S.spec_parents_tainted(rest, lg)) == S.bor(_, K.parents_tainted(rest, lg)) : Bool}
      %lem_parents_tainted_spec_rw(rest, lg) : {S.bor(S.spec_opt_tainted(S.spec_ledger_lookup(lg, p)), S.spec_parents_tainted(rest, lg)) == S.bor(S.spec_opt_tainted(S.spec_ledger_lookup(lg, p)), _) : Bool}
      {==}

def lem_ledger_record_spec_rw(+lg: List<&2, S.LedgerNode>, +cid: Nat, +base: S.Auth, +ps: List<&2, Nat>) -> {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == K.is_tainted(K.ledger_record(lg, cid, base, ps)) : Bool}:
  pf = Equal.sym(
    Bool,
    K.is_tainted(K.ledger_record(lg, cid, base, ps)),
    S.bor(K.is_tainted(base), S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg))),
    lem_prop_bool(base, S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg)))
  )
  %pf : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == _ : Bool}
  %lem_is_tainted_spec_rw(base) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(_, S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg))) : Bool}
  %lem_ledger_lookup_spec_rw(lg, cid) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(K.existing_tainted(_), K.parents_tainted(ps, lg))) : Bool}
  %lem_existing_tainted_spec_rw(S.spec_ledger_lookup(lg, cid)) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(_, K.parents_tainted(ps, lg))) : Bool}
  %lem_parents_tainted_spec_rw(ps, lg) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), _)) : Bool}
  {==}

def lem_ledger_record_tbit_spec_rw(+lg: List<&2, S.LedgerNode>, +cid: Nat, +base: S.Auth, +ps: List<&2, Nat>) -> {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.spec_auth_tainted_bit(K.ledger_record(lg, cid, base, ps)) : Bool}:
  pf = Equal.sym(
    Bool,
    S.spec_auth_tainted_bit(K.ledger_record(lg, cid, base, ps)),
    S.bor(K.is_tainted(base), S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg))),
    lem_prop_tbit_bool(base, S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg)))
  )
  %pf : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == _ : Bool}
  %lem_is_tainted_spec_rw(base) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(_, S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(ps, lg))) : Bool}
  %lem_ledger_lookup_spec_rw(lg, cid) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(K.existing_tainted(_), K.parents_tainted(ps, lg))) : Bool}
  %lem_existing_tainted_spec_rw(S.spec_ledger_lookup(lg, cid)) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(_, K.parents_tainted(ps, lg))) : Bool}
  %lem_parents_tainted_spec_rw(ps, lg) : {S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg))) == S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), _)) : Bool}
  {==}

def lem_imp_bor_l(x: Bool, y: Bool) -> {S.implies(x, S.bor(x, y)) == True{} : Bool}:
  match x:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_imp_bor_r(h: Bool, a: Bool, b: Bool, ih: {S.implies(h, b) == True{} : Bool}) -> {S.implies(h, S.bor(a, b)) == True{} : Bool}:
  match h a b:
    case False{} _ _:
      {==}
    case True{} True{} _:
      {==}
    case True{} False{} True{}:
      {==}
    case True{} False{} False{}:
      ih

def lem_imp_trans(a: Bool, b: Bool, c: Bool, p1: {S.implies(a, b) == True{} : Bool}, p2: {S.implies(b, c) == True{} : Bool}) -> {S.implies(a, c) == True{} : Bool}:
  match a b c:
    case False{} _ _:
      {==}
    case True{} _ True{}:
      {==}
    case True{} False{} False{}:
      p1
    case True{} True{} False{}:
      p2

def lem_not_has_cid_step(
  eq: Bool, +k: Nat, +a: S.Auth, +rest: List<&2, S.LedgerNode>, +unk: Nat,
  eq_pf: {eq == S.nat_eq(k, unk) : Bool},
  ih: {S.implies(S.bnot(S.spec_ledger_has_cid(rest, unk)), S.spec_opt_tainted(S.spec_ledger_lookup(rest, unk))) == True{} : Bool}
) -> {S.implies(S.bnot(S.bor(S.nat_eq(k, unk), S.spec_ledger_has_cid(rest, unk))), S.spec_opt_tainted(S.spec_select_lookup(S.nat_eq(k, unk), a, S.spec_ledger_lookup(rest, unk)))) == True{} : Bool}:
  match eq:
    case True{}:
      %eq_pf : {S.implies(S.bnot(S.bor(_, S.spec_ledger_has_cid(rest, unk))), S.spec_opt_tainted(S.spec_select_lookup(_, a, S.spec_ledger_lookup(rest, unk)))) == True{} : Bool}
      {==}
    case False{}:
      %eq_pf : {S.implies(S.bnot(S.bor(_, S.spec_ledger_has_cid(rest, unk))), S.spec_opt_tainted(S.spec_select_lookup(_, a, S.spec_ledger_lookup(rest, unk)))) == True{} : Bool}
      ih

def lem_not_has_cid_tainted(lg: List<&2, S.LedgerNode>, +unk: Nat) -> {S.implies(S.bnot(S.spec_ledger_has_cid(lg, unk)), S.spec_opt_tainted(S.spec_ledger_lookup(lg, unk))) == True{} : Bool}:
  match lg:
    case Nil{}:
      {==}
    case Con{S.LNode{+k, +a}, +rest}:
      lem_not_has_cid_step(S.nat_eq(k, unk), k, a, rest, unk, {==}, lem_not_has_cid_tainted(rest, unk))

def lem_spec_parents_nth_tainted(
  p_prefix: List<&2, Nat>, +bad_p: Nat, +p_tail: List<&2, Nat>, +lg: List<&2, S.LedgerNode>
) -> {S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), S.spec_parents_tainted(S.append_nat(p_prefix, bad_p <> p_tail), lg)) == True{} : Bool}:
  match p_prefix:
    case Nil{}:
      lem_imp_bor_l(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), S.spec_parents_tainted(p_tail, lg))
    case Con{+p, +rest}:
      lem_imp_bor_r(
        S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)),
        S.spec_opt_tainted(S.spec_ledger_lookup(lg, p)),
        S.spec_parents_tainted(S.append_nat(rest, bad_p <> p_tail), lg),
        lem_spec_parents_nth_tainted(rest, bad_p, p_tail, lg)
      )

def lem_imp_bor3(h: Bool, b_t: Bool, old_t: Bool, par_t: Bool, p_par: {S.implies(h, par_t) == True{} : Bool}) -> {S.implies(h, S.bor(b_t, S.bor(old_t, par_t))) == True{} : Bool}:
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

def Laws.L04_unknown_parent_fails_closed(lg, cid, base, p_pre, unk, p_tail):
  +ps = S.append_nat(p_pre, unk <> p_tail)
  %lem_ledger_record_spec_rw(lg, cid, base, ps) : {S.implies(S.bnot(S.spec_ledger_has_cid(lg, unk)), _) == True{} : Bool}
  p_unk = lem_imp_trans(
    S.bnot(S.spec_ledger_has_cid(lg, unk)),
    S.spec_opt_tainted(S.spec_ledger_lookup(lg, unk)),
    S.spec_parents_tainted(ps, lg),
    lem_not_has_cid_tainted(lg, unk),
    lem_spec_parents_nth_tainted(p_pre, unk, p_tail, lg)
  )
  lem_imp_bor3(
    S.bnot(S.spec_ledger_has_cid(lg, unk)),
    S.spec_tainted(base),
    S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)),
    S.spec_parents_tainted(ps, lg),
    p_unk
  )

def Laws.L05_empty_args_fail_closed(act, _lg, _toks, _has_pk, _now):
  match act:
    case S.ReadOnly{}:
      {==}
    case S.ShellExec{}:
      {==}
    case S.CredRead{}:
      {==}
    case S.FsWrite{}:
      {==}
    case S.NetExfil{}:
      {==}

def lem_is_priv_spec_rw(act: S.Action) -> {S.spec_is_privileged(act) == K.is_privileged(act) : Bool}:
  match act:
    case S.ReadOnly{}:
      {==}
    case S.ShellExec{}:
      {==}
    case S.CredRead{}:
      {==}
    case S.FsWrite{}:
      {==}
    case S.NetExfil{}:
      {==}

def lem_action_eq_spec_rw(a: S.Action, b: S.Action) -> {S.spec_action_eq(a, b) == K.action_eq(a, b) : Bool}:
  match a b:
    case S.ReadOnly{} S.ReadOnly{}:
      {==}
    case S.ReadOnly{} S.ShellExec{}:
      {==}
    case S.ReadOnly{} S.CredRead{}:
      {==}
    case S.ReadOnly{} S.FsWrite{}:
      {==}
    case S.ReadOnly{} S.NetExfil{}:
      {==}
    case S.ShellExec{} S.ReadOnly{}:
      {==}
    case S.ShellExec{} S.ShellExec{}:
      {==}
    case S.ShellExec{} S.CredRead{}:
      {==}
    case S.ShellExec{} S.FsWrite{}:
      {==}
    case S.ShellExec{} S.NetExfil{}:
      {==}
    case S.CredRead{} S.ReadOnly{}:
      {==}
    case S.CredRead{} S.ShellExec{}:
      {==}
    case S.CredRead{} S.CredRead{}:
      {==}
    case S.CredRead{} S.FsWrite{}:
      {==}
    case S.CredRead{} S.NetExfil{}:
      {==}
    case S.FsWrite{} S.ReadOnly{}:
      {==}
    case S.FsWrite{} S.ShellExec{}:
      {==}
    case S.FsWrite{} S.CredRead{}:
      {==}
    case S.FsWrite{} S.FsWrite{}:
      {==}
    case S.FsWrite{} S.NetExfil{}:
      {==}
    case S.NetExfil{} S.ReadOnly{}:
      {==}
    case S.NetExfil{} S.ShellExec{}:
      {==}
    case S.NetExfil{} S.CredRead{}:
      {==}
    case S.NetExfil{} S.FsWrite{}:
      {==}
    case S.NetExfil{} S.NetExfil{}:
      {==}

def lem_tok_valid_spec_rw(tok: S.DeclassToken, +cid: Nat, +act: S.Action, +now: Nat) -> {S.spec_tok_valid(tok, cid, act, now) == K.token_matches(tok, cid, act, now) : Bool}:
  match tok:
    case S.Tok{+tcid, +tact, +texp, +tsig}:
      %lem_action_eq_spec_rw(tact, act) : {S.band(tsig, S.band(S.nat_eq(tcid, cid), S.band(S.spec_action_eq(tact, act), S.nat_le(now, texp)))) == S.band(tsig, S.band(S.nat_eq(tcid, cid), S.band(_, S.nat_le(now, texp)))) : Bool}
      {==}

def lem_has_tok_spec_rw(toks: List<&2, S.DeclassToken>, +cid: Nat, +act: S.Action, +now: Nat) -> {S.spec_has_valid_tok(toks, cid, act, now) == K.has_valid_token(toks, cid, act, now) : Bool}:
  match toks:
    case Nil{}:
      {==}
    case Con{+tk, +rest}:
      %lem_tok_valid_spec_rw(tk, cid, act, now) : {S.bor(S.spec_tok_valid(tk, cid, act, now), S.spec_has_valid_tok(rest, cid, act, now)) == S.bor(_, K.has_valid_token(rest, cid, act, now)) : Bool}
      %lem_has_tok_spec_rw(rest, cid, act, now) : {S.bor(S.spec_tok_valid(tk, cid, act, now), S.spec_has_valid_tok(rest, cid, act, now)) == S.bor(S.spec_tok_valid(tk, cid, act, now), _) : Bool}
      {==}

def lem_arg_branch_spec_rw(t: Bool, +cid: Nat, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> {S.spec_arg_ok_branch(t, cid, act, toks, has_pk, now) == K.arg_auth_branch(t, cid, act, toks, has_pk, now) : Bool}:
  match t:
    case False{}:
      {==}
    case True{}:
      %lem_has_tok_spec_rw(toks, cid, act, now) : {S.band(has_pk, S.spec_has_valid_tok(toks, cid, act, now)) == S.band(has_pk, _) : Bool}
      {==}

def lem_arg_ok_spec_rw(+cid: Nat, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> {S.spec_arg_ok(cid, lg, act, toks, has_pk, now) == K.arg_authorized(cid, lg, act, toks, has_pk, now) : Bool}:
  %lem_ledger_lookup_spec_rw(lg, cid) : {S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_ledger_lookup(lg, cid)), cid, act, toks, has_pk, now) == K.arg_auth_branch(K.opt_auth_tainted(_), cid, act, toks, has_pk, now) : Bool}
  %lem_opt_tainted_spec_rw(S.spec_ledger_lookup(lg, cid)) : {S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_ledger_lookup(lg, cid)), cid, act, toks, has_pk, now) == K.arg_auth_branch(_, cid, act, toks, has_pk, now) : Bool}
  lem_arg_branch_spec_rw(S.spec_opt_tainted(S.spec_ledger_lookup(lg, cid)), cid, act, toks, has_pk, now)

def lem_all_args_spec_rw(args: List<&2, Nat>, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> {S.spec_all_args_ok(args, lg, act, toks, has_pk, now) == K.all_args_authorized(args, lg, act, toks, has_pk, now) : Bool}:
  match args:
    case Nil{}:
      {==}
    case Con{+c, +rest}:
      %lem_arg_ok_spec_rw(c, lg, act, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, act, toks, has_pk, now), S.spec_all_args_ok(rest, lg, act, toks, has_pk, now)) == S.band(_, K.all_args_authorized(rest, lg, act, toks, has_pk, now)) : Bool}
      %lem_all_args_spec_rw(rest, lg, act, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, act, toks, has_pk, now), S.spec_all_args_ok(rest, lg, act, toks, has_pk, now)) == S.band(S.spec_arg_ok(c, lg, act, toks, has_pk, now), _) : Bool}
      {==}

def lem_gate_spec_rw(+lg: List<&2, S.LedgerNode>, act: S.Action, args: List<&2, Nat>, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat) -> {S.spec_gate(lg, act, args, toks, has_pk, now) == K.check_gate_signed_pk(lg, act, args, toks, has_pk, now) : Bool}:
  match act args:
    case S.ReadOnly{} _:
      {==}
    case S.ShellExec{} Nil{}:
      {==}
    case S.ShellExec{} Con{+c, +rest}:
      %lem_arg_ok_spec_rw(c, lg, S.ShellExec{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.ShellExec{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.ShellExec{}, toks, has_pk, now)) == S.band(_, K.all_args_authorized(rest, lg, S.ShellExec{}, toks, has_pk, now)) : Bool}
      %lem_all_args_spec_rw(rest, lg, S.ShellExec{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.ShellExec{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.ShellExec{}, toks, has_pk, now)) == S.band(S.spec_arg_ok(c, lg, S.ShellExec{}, toks, has_pk, now), _) : Bool}
      {==}
    case S.CredRead{} Nil{}:
      {==}
    case S.CredRead{} Con{+c, +rest}:
      %lem_arg_ok_spec_rw(c, lg, S.CredRead{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.CredRead{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.CredRead{}, toks, has_pk, now)) == S.band(_, K.all_args_authorized(rest, lg, S.CredRead{}, toks, has_pk, now)) : Bool}
      %lem_all_args_spec_rw(rest, lg, S.CredRead{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.CredRead{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.CredRead{}, toks, has_pk, now)) == S.band(S.spec_arg_ok(c, lg, S.CredRead{}, toks, has_pk, now), _) : Bool}
      {==}
    case S.FsWrite{} Nil{}:
      {==}
    case S.FsWrite{} Con{+c, +rest}:
      %lem_arg_ok_spec_rw(c, lg, S.FsWrite{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.FsWrite{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.FsWrite{}, toks, has_pk, now)) == S.band(_, K.all_args_authorized(rest, lg, S.FsWrite{}, toks, has_pk, now)) : Bool}
      %lem_all_args_spec_rw(rest, lg, S.FsWrite{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.FsWrite{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.FsWrite{}, toks, has_pk, now)) == S.band(S.spec_arg_ok(c, lg, S.FsWrite{}, toks, has_pk, now), _) : Bool}
      {==}
    case S.NetExfil{} Nil{}:
      {==}
    case S.NetExfil{} Con{+c, +rest}:
      %lem_arg_ok_spec_rw(c, lg, S.NetExfil{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.NetExfil{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.NetExfil{}, toks, has_pk, now)) == S.band(_, K.all_args_authorized(rest, lg, S.NetExfil{}, toks, has_pk, now)) : Bool}
      %lem_all_args_spec_rw(rest, lg, S.NetExfil{}, toks, has_pk, now) : {S.band(S.spec_arg_ok(c, lg, S.NetExfil{}, toks, has_pk, now), S.spec_all_args_ok(rest, lg, S.NetExfil{}, toks, has_pk, now)) == S.band(S.spec_arg_ok(c, lg, S.NetExfil{}, toks, has_pk, now), _) : Bool}
      {==}

def lem_action_eq_sym_rw(a: S.Action, b: S.Action) -> {S.spec_action_eq(a, b) == S.spec_action_eq(b, a) : Bool}:
  match a b:
    case S.ReadOnly{} S.ReadOnly{}:
      {==}
    case S.ReadOnly{} S.ShellExec{}:
      {==}
    case S.ReadOnly{} S.CredRead{}:
      {==}
    case S.ReadOnly{} S.FsWrite{}:
      {==}
    case S.ReadOnly{} S.NetExfil{}:
      {==}
    case S.ShellExec{} S.ReadOnly{}:
      {==}
    case S.ShellExec{} S.ShellExec{}:
      {==}
    case S.ShellExec{} S.CredRead{}:
      {==}
    case S.ShellExec{} S.FsWrite{}:
      {==}
    case S.ShellExec{} S.NetExfil{}:
      {==}
    case S.CredRead{} S.ReadOnly{}:
      {==}
    case S.CredRead{} S.ShellExec{}:
      {==}
    case S.CredRead{} S.CredRead{}:
      {==}
    case S.CredRead{} S.FsWrite{}:
      {==}
    case S.CredRead{} S.NetExfil{}:
      {==}
    case S.FsWrite{} S.ReadOnly{}:
      {==}
    case S.FsWrite{} S.ShellExec{}:
      {==}
    case S.FsWrite{} S.CredRead{}:
      {==}
    case S.FsWrite{} S.FsWrite{}:
      {==}
    case S.FsWrite{} S.NetExfil{}:
      {==}
    case S.NetExfil{} S.ReadOnly{}:
      {==}
    case S.NetExfil{} S.ShellExec{}:
      {==}
    case S.NetExfil{} S.CredRead{}:
      {==}
    case S.NetExfil{} S.FsWrite{}:
      {==}
    case S.NetExfil{} S.NetExfil{}:
      {==}

def lem_l06a_direct(priv: Bool, t: Bool, eq12: Bool, le_t: Bool, +cid: Nat, +a1: S.Action, +a2: S.Action, +exp: Nat, +now: Nat, h_eq: {eq12 == S.spec_action_eq(a2, a1) : Bool}) -> {S.implies(S.band(priv, S.band(t, S.bnot(eq12))), S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok_branch(t, cid, a1, [S.Tok{cid, a2, exp, True{}}], True{}, now), True{})))) == True{} : Bool}:
  match priv t eq12:
    case False{} _ _:
      {==}
    case True{} False{} _:
      {==}
    case True{} True{} True{}:
      {==}
    case True{} True{} False{}:
      %lem_nat_eq_refl_rw(cid) : {S.bnot(S.band(S.bor(S.band(True{}, S.band(_, S.band(S.spec_action_eq(a2, a1), S.nat_le(now, exp)))), False{}), True{})) == True{} : Bool}
      %h_eq : {S.bnot(S.band(S.bor(S.band(_, S.nat_le(now, exp)), False{}), True{})) == True{} : Bool}
      {==}

def Laws.L06a_wrong_action_token_rejected(a1, a2, cid, oc, oe, ot, expiry, now):
  %lem_gate_spec_rw([S.LNode{cid, S.Auth{oc, oe, ot}}], a1, [cid], [S.Tok{cid, a2, expiry, True{}}], True{}, now) : {S.implies(S.band(S.spec_is_privileged(a1), S.band(S.bor(oe, ot), S.bnot(S.spec_action_eq(a1, a2)))), S.bnot(_)) == True{} : Bool}
  %lem_nat_eq_refl_rw(cid) : {S.implies(S.band(S.spec_is_privileged(a1), S.band(S.bor(oe, ot), S.bnot(S.spec_action_eq(a1, a2)))), S.bnot(S.bor(S.bnot(S.spec_is_privileged(a1)), S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.Auth{oc, oe, ot}, None{})), cid, a1, [S.Tok{cid, a2, expiry, True{}}], True{}, now), True{})))) == True{} : Bool}
  lem_l06a_direct(S.spec_is_privileged(a1), S.bor(oe, ot), S.spec_action_eq(a1, a2), S.nat_le(now, expiry), cid, a1, a2, expiry, now, lem_action_eq_sym_rw(a1, a2))

def lem_l06b_direct(
  priv: Bool, eq12: Bool, t2: Bool, +c1: Nat, +c2: Nat, +o1: S.Auth, +oc2: Nat, +oe2: Bool, +ot2: Bool, +act: S.Action, +exp: Nat, +now: Nat,
  +h_eq: {eq12 == S.nat_eq(c1, c2) : Bool},
  h_t2: {t2 == S.bor(oe2, ot2) : Bool}
) -> {S.implies(S.band(priv, S.band(S.bnot(eq12), t2)), S.band(S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), S.band(S.spec_arg_ok(c2, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{})))), S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok(c2, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))))) == True{} : Bool}:
  match priv eq12 t2:
    case False{} _ _:
      {==}
    case True{} True{} _:
      {==}
    case True{} False{} False{}:
      {==}
    case True{} False{} True{}:
      %h_eq : {S.band(S.bnot(S.band(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(o1), S.spec_select_lookup(S.nat_eq(c2, c2), S.Auth{oc2, oe2, ot2}, None{}))), c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))), S.bnot(S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(o1), S.spec_select_lookup(S.nat_eq(c2, c2), S.Auth{oc2, oe2, ot2}, None{}))), c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))) == True{} : Bool}
      %lem_nat_eq_refl_rw(c2) : {S.band(S.bnot(S.band(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.Auth{oc2, oe2, ot2}, None{})), c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))), S.bnot(S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.Auth{oc2, oe2, ot2}, None{})), c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))) == True{} : Bool}
      %h_t2 : {S.band(S.bnot(S.band(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), S.band(S.spec_arg_ok_branch(_, c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))), S.bnot(S.band(S.spec_arg_ok_branch(_, c2, act, [S.Tok{c1, act, exp, True{}}], True{}, now), True{}))) == True{} : Bool}
      %h_eq : {S.band(S.bnot(S.band(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now), S.band(S.bor(S.band(True{}, S.band(_, S.band(S.spec_action_eq(act, act), S.nat_le(now, exp)))), False{}), True{}))), S.bnot(S.band(S.bor(S.band(True{}, S.band(_, S.band(S.spec_action_eq(act, act), S.nat_le(now, exp)))), False{}), True{}))) == True{} : Bool}
      %lem_band_false_r_rw(S.spec_arg_ok(c1, [S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [S.Tok{c1, act, exp, True{}}], True{}, now)) : {S.band(S.bnot(_), True{}) == True{} : Bool}
      {==}

def Laws.L06b_partial_declass_blocks(act, c1, c2, o1, oc2, oe2, ot2, exp, now):
  %lem_gate_spec_rw([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c1, c2], [S.Tok{c1, act, exp, True{}}], True{}, now) : {S.implies(S.band(S.spec_is_privileged(act), S.band(S.bnot(S.nat_eq(c1, c2)), S.bor(oe2, ot2))), S.band(S.bnot(_), S.bnot(K.check_gate_signed([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c2], [S.Tok{c1, act, exp, True{}}], now)))) == True{} : Bool}
  %lem_gate_spec_rw([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c2], [S.Tok{c1, act, exp, True{}}], True{}, now) : {S.implies(S.band(S.spec_is_privileged(act), S.band(S.bnot(S.nat_eq(c1, c2)), S.bor(oe2, ot2))), S.band(S.bnot(S.spec_gate([S.LNode{c1, S.spec_set_tainted(o1)}, S.LNode{c2, S.Auth{oc2, oe2, ot2}}], act, [c1, c2], [S.Tok{c1, act, exp, True{}}], True{}, now)), S.bnot(_))) == True{} : Bool}
  lem_l06b_direct(S.spec_is_privileged(act), S.nat_eq(c1, c2), S.bor(oe2, ot2), c1, c2, o1, oc2, oe2, ot2, act, exp, now, {==}, {==})

def lem_prop_origin(base: S.Auth, x: Bool) -> {(S.spec_auth_clean_bits(K.prop_apply(S.bor(K.is_tainted(base), x), base)), S.spec_auth_ext_bit(K.prop_apply(S.bor(K.is_tainted(base), x), base))) == (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)) : Nat & Bool}:
  match base x:
    case S.Auth{_, True{}, True{}} True{}:
      {==}
    case S.Auth{_, True{}, True{}} False{}:
      {==}
    case S.Auth{_, True{}, False{}} True{}:
      {==}
    case S.Auth{_, True{}, False{}} False{}:
      {==}
    case S.Auth{_, False{}, True{}} True{}:
      {==}
    case S.Auth{_, False{}, True{}} False{}:
      {==}
    case S.Auth{_, False{}, False{}} True{}:
      {==}
    case S.Auth{_, False{}, False{}} False{}:
      {==}

def Laws.L15_propagate_preserves_origin(base, ps, lg, cid, pids):
  p1 = Equal.sym(Nat & Bool, (S.spec_auth_clean_bits(K.propagate(base, ps)), S.spec_auth_ext_bit(K.propagate(base, ps))), (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), lem_prop_origin(base, K.any_tainted(ps)))
  p2 = Equal.sym(Nat & Bool, (S.spec_auth_clean_bits(K.ledger_record(lg, cid, base, pids)), S.spec_auth_ext_bit(K.ledger_record(lg, cid, base, pids))), (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), lem_prop_origin(base, S.bor(K.existing_tainted(K.ledger_lookup(lg, cid)), K.parents_tainted(pids, lg))))
  %p1 : {(_, (S.spec_auth_clean_bits(K.ledger_record(lg, cid, base, pids)), S.spec_auth_ext_bit(K.ledger_record(lg, cid, base, pids)))) == ((S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base))) : (Nat & Bool) & (Nat & Bool)}
  %p2 : {((S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), _) == ((S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base)), (S.spec_auth_clean_bits(base), S.spec_auth_ext_bit(base))) : (Nat & Bool) & (Nat & Bool)}
  {==}

def lem_deep_lookup_step(
  eq: Bool, +k: Nat, +a: S.Auth, +prest: List<&2, S.LedgerNode>, +cid: Nat, +oc: Nat, +oe: Bool, +ot: Bool, +rest: List<&2, S.LedgerNode>,
  eq_pf: {eq == S.nat_eq(k, cid) : Bool},
  ih: {S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prest, cid)), S.bor(oe, ot)), S.spec_existing_tainted(S.spec_ledger_lookup(S.append_lg(prest, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid))) == True{} : Bool}
) -> {S.implies(S.band(S.bnot(S.bor(S.nat_eq(k, cid), S.spec_ledger_has_cid(prest, cid))), S.bor(oe, ot)), S.spec_existing_tainted(S.spec_select_lookup(S.nat_eq(k, cid), a, S.spec_ledger_lookup(S.append_lg(prest, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid)))) == True{} : Bool}:
  match eq:
    case True{}:
      %eq_pf : {S.implies(S.band(S.bnot(S.bor(_, S.spec_ledger_has_cid(prest, cid))), S.bor(oe, ot)), S.spec_existing_tainted(S.spec_select_lookup(_, a, S.spec_ledger_lookup(S.append_lg(prest, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid)))) == True{} : Bool}
      {==}
    case False{}:
      %eq_pf : {S.implies(S.band(S.bnot(S.bor(_, S.spec_ledger_has_cid(prest, cid))), S.bor(oe, ot)), S.spec_existing_tainted(S.spec_select_lookup(_, a, S.spec_ledger_lookup(S.append_lg(prest, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid)))) == True{} : Bool}
      ih

def lem_deep_lookup_tainted(prefix: List<&2, S.LedgerNode>, +cid: Nat, +oc: Nat, +oe: Bool, +ot: Bool, +rest: List<&2, S.LedgerNode>) -> {S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)), S.spec_existing_tainted(S.spec_ledger_lookup(S.append_lg(prefix, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest), cid))) == True{} : Bool}:
  match prefix:
    case Nil{}:
      %lem_nat_eq_refl_rw(cid) : {S.implies(S.bor(oe, ot), S.spec_existing_tainted(S.spec_select_lookup(_, S.Auth{oc, oe, ot}, S.spec_ledger_lookup(rest, cid)))) == True{} : Bool}
      lem_implies_self(S.bor(oe, ot))
    case Con{S.LNode{+k, +a}, +prest}:
      lem_deep_lookup_step(
        S.nat_eq(k, cid), k, a, prest, cid, oc, oe, ot, rest, {==},
        lem_deep_lookup_tainted(prest, cid, oc, oe, ot, rest)
      )

def lem_imp_old_bor3(h: Bool, b_t: Bool, old_t: Bool, par_t: Bool, p_old: {S.implies(h, old_t) == True{} : Bool}) -> {S.implies(h, S.bor(b_t, S.bor(old_t, par_t))) == True{} : Bool}:
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
      p_old

def lem_clean_live_bool(o_t: Bool, p_t: Bool) -> {S.implies(S.band(S.bnot(o_t), S.bnot(p_t)), S.bnot(S.bor(False{}, S.bor(o_t, p_t)))) == True{} : Bool}:
  match o_t p_t:
    case False{} False{}:
      {==}
    case False{} True{}:
      {==}
    case True{} _:
      {==}

def Laws.L16_sticky_deep_and_clean_liveness(prefix, cid, oc, oe, ot, rest, base, ps, lg, bc, clean_ps):
  +lg1 = S.append_lg(prefix, S.LNode{cid, S.Auth{oc, oe, ot}} <> rest)
  %lem_ledger_record_spec_rw(lg1, cid, base, ps) : {(S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)), _), S.implies(S.band(S.bnot(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid))), S.bnot(S.spec_parents_tainted(clean_ps, lg))), S.bnot(K.is_tainted(K.ledger_record(lg, cid, S.Auth{bc, False{}, False{}}, clean_ps))))) == (True{}, True{}) : Bool & Bool}
  %lem_ledger_record_spec_rw(lg, cid, S.Auth{bc, False{}, False{}}, clean_ps) : {(S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)), S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg1, cid)), S.spec_parents_tainted(ps, lg1)))), S.implies(S.band(S.bnot(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid))), S.bnot(S.spec_parents_tainted(clean_ps, lg))), S.bnot(_))) == (True{}, True{}) : Bool & Bool}
  p1 = Equal.sym(
    Bool,
    S.implies(S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)), S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg1, cid)), S.spec_parents_tainted(ps, lg1)))),
    True{},
    lem_imp_old_bor3(
      S.band(S.bnot(S.spec_ledger_has_cid(prefix, cid)), S.bor(oe, ot)),
      S.spec_tainted(base),
      S.spec_existing_tainted(S.spec_ledger_lookup(lg1, cid)),
      S.spec_parents_tainted(ps, lg1),
      lem_deep_lookup_tainted(prefix, cid, oc, oe, ot, rest)
    )
  )
  p2 = Equal.sym(
    Bool,
    S.implies(S.band(S.bnot(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid))), S.bnot(S.spec_parents_tainted(clean_ps, lg))), S.bnot(S.bor(False{}, S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(clean_ps, lg))))),
    True{},
    lem_clean_live_bool(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(clean_ps, lg))
  )
  %p1 : {(_, S.implies(S.band(S.bnot(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid))), S.bnot(S.spec_parents_tainted(clean_ps, lg))), S.bnot(S.bor(False{}, S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(clean_ps, lg)))))) == (True{}, True{}) : Bool & Bool}
  %p2 : {(True{}, _) == (True{}, True{}) : Bool & Bool}
  {==}

def lem_action_eq_refl_rw(a: S.Action) -> {True{} == S.spec_action_eq(a, a) : Bool}:
  match a:
    case S.ReadOnly{}:
      {==}
    case S.ShellExec{}:
      {==}
    case S.CredRead{}:
      {==}
    case S.FsWrite{}:
      {==}
    case S.NetExfil{}:
      {==}

def lem_deep_tok_found(tpre: List<&2, S.DeclassToken>, +cid: Nat, +act: S.Action, +now: Nat, +d: Nat, +ttail: List<&2, S.DeclassToken>) -> {True{} == S.spec_has_valid_tok(S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail), cid, act, now) : Bool}:
  match tpre:
    case Nil{}:
      %lem_nat_eq_refl_rw(cid) : {True{} == S.bor(S.band(_, S.band(S.spec_action_eq(act, act), S.nat_le(now, S.nat_add(now, d)))), S.spec_has_valid_tok(ttail, cid, act, now)) : Bool}
      %lem_action_eq_refl_rw(act) : {True{} == S.bor(S.band(_, S.nat_le(now, S.nat_add(now, d))), S.spec_has_valid_tok(ttail, cid, act, now)) : Bool}
      %lem_le_add_r_rw(now, d) : {True{} == S.bor(_, S.spec_has_valid_tok(ttail, cid, act, now)) : Bool}
      {==}
    case Con{+tk, +rest}:
      %lem_deep_tok_found(rest, cid, act, now, d, ttail) : {True{} == S.bor(S.spec_tok_valid(tk, cid, act, now), _) : Bool}
      lem_bor_true_r_rw(S.spec_tok_valid(tk, cid, act, now))

def lem_spec_set_tainted_rw(old: S.Auth) -> {True{} == S.spec_tainted(S.spec_set_tainted(old)) : Bool}:
  match old:
    case S.Auth{_, True{}, _}:
      {==}
    case S.Auth{_, False{}, _}:
      {==}

def lem_deep_tok_gate(+cid: Nat, +old: S.Auth, +act: S.Action, +tpre: List<&2, S.DeclassToken>, +ttail: List<&2, S.DeclassToken>, +now: Nat, +d: Nat) -> {True{} == K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail), True{}, now) : Bool}:
  +toks = S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail)
  %lem_gate_spec_rw([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], toks, True{}, now) : {True{} == _ : Bool}
  %lem_nat_eq_refl_rw(cid) : {True{} == S.bor(S.bnot(S.spec_is_privileged(act)), S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(old), None{})), cid, act, toks, True{}, now), True{})) : Bool}
  %lem_spec_set_tainted_rw(old) : {True{} == S.bor(S.bnot(S.spec_is_privileged(act)), S.band(S.spec_arg_ok_branch(_, cid, act, toks, True{}, now), True{})) : Bool}
  %lem_deep_tok_found(tpre, cid, act, now, d, ttail) : {True{} == S.bor(S.bnot(S.spec_is_privileged(act)), S.band(_, True{})) : Bool}
  lem_bor_true_r_rw(S.bnot(S.spec_is_privileged(act)))

def Laws.L17_gate_exact_iff_and_liveness(lg, act, args, toks, has_pk, now, cid, old, tpre, ttail, d):
  %lem_gate_spec_rw(lg, act, args, toks, has_pk, now) : {((_, K.check_gate_signed(lg, act, args, toks, now)), (True{}, K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail), True{}, now))) == ((S.spec_gate(lg, act, args, toks, has_pk, now), S.spec_gate(lg, act, args, toks, True{}, now)), (True{}, True{})) : (Bool & Bool) & (Bool & Bool)}
  %lem_gate_spec_rw(lg, act, args, toks, True{}, now) : {((S.spec_gate(lg, act, args, toks, has_pk, now), _), (True{}, K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], S.append_toks(tpre, S.Tok{cid, act, S.nat_add(now, d), True{}} <> ttail), True{}, now))) == ((S.spec_gate(lg, act, args, toks, has_pk, now), S.spec_gate(lg, act, args, toks, True{}, now)), (True{}, True{})) : (Bool & Bool) & (Bool & Bool)}
  %lem_deep_tok_gate(cid, old, act, tpre, ttail, now, d) : {((S.spec_gate(lg, act, args, toks, has_pk, now), S.spec_gate(lg, act, args, toks, True{}, now)), (True{}, _)) == ((S.spec_gate(lg, act, args, toks, has_pk, now), S.spec_gate(lg, act, args, toks, True{}, now)), (True{}, True{})) : (Bool & Bool) & (Bool & Bool)}
  {==}

def lem_l18_priv_case(priv: Bool, +act: S.Action, +cid: Nat, +old: S.Auth, +exp: Nat, +d: Nat, +now: Nat, h_p: {priv == S.spec_is_privileged(act) : Bool}) -> {S.implies(priv, S.band(S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok(cid, [S.LNode{cid, S.spec_set_tainted(old)}], act, [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d)), True{}))), S.band(S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok(cid, [S.LNode{cid, S.spec_set_tainted(old)}], act, [S.Tok{cid, act, exp, False{}}], True{}, now), True{}))), S.bnot(S.bor(S.bnot(priv), S.band(S.spec_arg_ok(cid, [S.LNode{cid, S.spec_set_tainted(old)}], act, [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now), True{})))))) == True{} : Bool}:
  match priv:
    case False{}:
      {==}
    case True{}:
      %lem_nat_eq_refl_rw(cid) : {S.band(S.bnot(S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(old), None{})), cid, act, [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d)), True{})), S.band(S.bnot(S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(old), None{})), cid, act, [S.Tok{cid, act, exp, False{}}], True{}, now), True{})), S.bnot(S.band(S.spec_arg_ok_branch(S.spec_opt_tainted(S.spec_select_lookup(_, S.spec_set_tainted(old), None{})), cid, act, [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now), True{})))) == True{} : Bool}
      %lem_spec_set_tainted_rw(old) : {S.band(S.bnot(S.band(S.spec_arg_ok_branch(_, cid, act, [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d)), True{})), S.band(S.bnot(S.band(S.spec_arg_ok_branch(_, cid, act, [S.Tok{cid, act, exp, False{}}], True{}, now), True{})), S.bnot(S.band(S.spec_arg_ok_branch(_, cid, act, [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now), True{})))) == True{} : Bool}
      %lem_nat_eq_refl_rw(cid) : {S.band(S.bnot(S.band(S.bor(S.band(_, S.band(S.spec_action_eq(act, act), S.nat_le(1n+S.nat_add(exp, d), exp))), False{}), True{})), True{}) == True{} : Bool}
      %lem_action_eq_refl_rw(act) : {S.band(S.bnot(S.band(S.bor(S.band(_, S.nat_le(1n+S.nat_add(exp, d), exp)), False{}), True{})), True{}) == True{} : Bool}
      %lem_le_succ_add_false_rw(exp, d) : {S.band(S.bnot(S.band(S.bor(_, False{}), True{})), True{}) == True{} : Bool}
      {==}

def Laws.L18_expired_and_forged_sig_rejected(act, cid, old, exp, d, now):
  %lem_gate_spec_rw([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d)) : {S.implies(S.spec_is_privileged(act), S.band(S.bnot(_), S.band(S.bnot(K.check_gate_signed([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, False{}}], now)), S.bnot(K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now))))) == True{} : Bool}
  %lem_gate_spec_rw([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, False{}}], True{}, now) : {S.implies(S.spec_is_privileged(act), S.band(S.bnot(S.spec_gate([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d))), S.band(S.bnot(_), S.bnot(K.check_gate_signed_pk([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now))))) == True{} : Bool}
  %lem_gate_spec_rw([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, S.nat_add(now, d), True{}}], False{}, now) : {S.implies(S.spec_is_privileged(act), S.band(S.bnot(S.spec_gate([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, True{}}], True{}, 1n+S.nat_add(exp, d))), S.band(S.bnot(S.spec_gate([S.LNode{cid, S.spec_set_tainted(old)}], act, [cid], [S.Tok{cid, act, exp, False{}}], True{}, now)), S.bnot(_)))) == True{} : Bool}
  lem_l18_priv_case(S.spec_is_privileged(act), act, cid, old, exp, d, now, {==})

def lem_evil_head_bool(e: Bool, t: Bool) -> {S.implies(S.bnot(e), S.bnot(S.band(e, t))) == True{} : Bool}:
  match e:
    case False{}:
      {==}
    case True{}:
      {==}

def lem_evil_step_bool(
  h_ok: Bool, e_ok: Bool, rest_ok: Bool,
  ih: {S.implies(S.bnot(e_ok), S.bnot(rest_ok)) == True{} : Bool}
) -> {S.implies(S.bnot(e_ok), S.bnot(S.band(h_ok, rest_ok))) == True{} : Bool}:
  match h_ok e_ok rest_ok:
    case False{} _ _:
      lem_implies_true_r(S.bnot(e_ok))
    case True{} True{} _:
      {==}
    case True{} False{} False{}:
      {==}
    case True{} False{} True{}:
      ih

def lem_spec_all_args_nth_blocks(
  prefix: List<&2, Nat>, +evil: Nat, +tail: List<&2, Nat>, +lg: List<&2, S.LedgerNode>, +act: S.Action, +toks: List<&2, S.DeclassToken>, +has_pk: Bool, +now: Nat
) -> {S.implies(S.bnot(S.spec_arg_ok(evil, lg, act, toks, has_pk, now)), S.bnot(S.spec_all_args_ok(S.append_nat(prefix, evil <> tail), lg, act, toks, has_pk, now))) == True{} : Bool}:
  match prefix:
    case Nil{}:
      lem_evil_head_bool(S.spec_arg_ok(evil, lg, act, toks, has_pk, now), S.spec_all_args_ok(tail, lg, act, toks, has_pk, now))
    case Con{+h, +prest}:
      lem_evil_step_bool(
        S.spec_arg_ok(h, lg, act, toks, has_pk, now),
        S.spec_arg_ok(evil, lg, act, toks, has_pk, now),
        S.spec_all_args_ok(S.append_nat(prest, evil <> tail), lg, act, toks, has_pk, now),
        lem_spec_all_args_nth_blocks(prest, evil, tail, lg, act, toks, has_pk, now)
      )

def lem_l19_bool(priv: Bool, ne: Bool, e_ok: Bool, all_ok: Bool, ih: {S.implies(S.bnot(e_ok), S.bnot(all_ok)) == True{} : Bool}) -> {S.implies(S.band(priv, S.bnot(e_ok)), S.bnot(S.bor(S.bnot(priv), S.band(ne, all_ok)))) == True{} : Bool}:
  match priv ne e_ok all_ok:
    case False{} _ _ _:
      {==}
    case True{} _ True{} _:
      {==}
    case True{} False{} False{} _:
      {==}
    case True{} True{} False{} False{}:
      {==}
    case True{} True{} False{} True{}:
      ih

def Laws.L19_nth_arg_taint_blocks(act, prefix, evil_cid, tail, lg, toks, has_pk, now):
  +args = S.append_nat(prefix, evil_cid <> tail)
  %lem_gate_spec_rw(lg, act, args, toks, has_pk, now) : {S.implies(S.band(S.spec_is_privileged(act), S.bnot(S.spec_arg_ok(evil_cid, lg, act, toks, has_pk, now))), S.bnot(_)) == True{} : Bool}
  lem_l19_bool(
    S.spec_is_privileged(act),
    S.is_nonempty_nat(args),
    S.spec_arg_ok(evil_cid, lg, act, toks, has_pk, now),
    S.spec_all_args_ok(args, lg, act, toks, has_pk, now),
    lem_spec_all_args_nth_blocks(prefix, evil_cid, tail, lg, act, toks, has_pk, now)
  )

def Laws.L25_ledger_record_exact_iff(lg, cid, base, ps, p_pre, bad_p, p_tail):
  +ps2 = S.append_nat(p_pre, bad_p <> p_tail)
  +exp = S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps, lg)))
  %lem_ledger_record_spec_rw(lg, cid, base, ps) : {((_, S.spec_auth_tainted_bit(K.ledger_record(lg, cid, base, ps))), S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, ps2)))) == ((exp, exp), True{}) : (Bool & Bool) & Bool}
  %lem_ledger_record_tbit_spec_rw(lg, cid, base, ps) : {((exp, _), S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), K.is_tainted(K.ledger_record(lg, cid, base, ps2)))) == ((exp, exp), True{}) : (Bool & Bool) & Bool}
  %lem_ledger_record_spec_rw(lg, cid, base, ps2) : {((exp, exp), S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), _)) == ((exp, exp), True{}) : (Bool & Bool) & Bool}
  pf2 = Equal.sym(
    Bool,
    S.implies(S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)), S.bor(S.spec_tainted(base), S.bor(S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)), S.spec_parents_tainted(ps2, lg)))),
    True{},
    lem_imp_bor3(
      S.spec_opt_tainted(S.spec_ledger_lookup(lg, bad_p)),
      S.spec_tainted(base),
      S.spec_existing_tainted(S.spec_ledger_lookup(lg, cid)),
      S.spec_parents_tainted(ps2, lg),
      lem_spec_parents_nth_tainted(p_pre, bad_p, p_tail, lg)
    )
  )
  %pf2 : {((exp, exp), _) == ((exp, exp), True{}) : (Bool & Bool) & Bool}
  {==}

def Laws.L26_header_meta_and_owner_pubkey(_low4, _res3, _epi, _flags, _r, _e, _f):
  {==}

def lem_action_to_code_spec_rw(a: S.Action) -> {S.spec_action_to_code(a) == K.action_to_code(a) : Nat}:
  match a:
    case S.ReadOnly{}:
      {==}
    case S.ShellExec{}:
      {==}
    case S.CredRead{}:
      {==}
    case S.FsWrite{}:
      {==}
    case S.NetExfil{}:
      {==}

def lem_action_roundtrip_rw(a: S.Action) -> {Some{a} == K.action_from_code(K.action_to_code(a)) : Maybe<&2, S.Action>}:
  match a:
    case S.ReadOnly{}:
      {==}
    case S.ShellExec{}:
      {==}
    case S.CredRead{}:
      {==}
    case S.FsWrite{}:
      {==}
    case S.NetExfil{}:
      {==}

def lem_action_inj(a: S.Action, b: S.Action) -> {True{} == S.implies(S.bnot(S.spec_action_eq(a, b)), S.bnot(S.nat_eq(K.action_to_code(a), K.action_to_code(b)))) : Bool}:
  match a b:
    case S.ReadOnly{} S.ReadOnly{}:
      {==}
    case S.ReadOnly{} S.ShellExec{}:
      {==}
    case S.ReadOnly{} S.CredRead{}:
      {==}
    case S.ReadOnly{} S.FsWrite{}:
      {==}
    case S.ReadOnly{} S.NetExfil{}:
      {==}
    case S.ShellExec{} S.ReadOnly{}:
      {==}
    case S.ShellExec{} S.ShellExec{}:
      {==}
    case S.ShellExec{} S.CredRead{}:
      {==}
    case S.ShellExec{} S.FsWrite{}:
      {==}
    case S.ShellExec{} S.NetExfil{}:
      {==}
    case S.CredRead{} S.ReadOnly{}:
      {==}
    case S.CredRead{} S.ShellExec{}:
      {==}
    case S.CredRead{} S.CredRead{}:
      {==}
    case S.CredRead{} S.FsWrite{}:
      {==}
    case S.CredRead{} S.NetExfil{}:
      {==}
    case S.FsWrite{} S.ReadOnly{}:
      {==}
    case S.FsWrite{} S.ShellExec{}:
      {==}
    case S.FsWrite{} S.CredRead{}:
      {==}
    case S.FsWrite{} S.FsWrite{}:
      {==}
    case S.FsWrite{} S.NetExfil{}:
      {==}
    case S.NetExfil{} S.ReadOnly{}:
      {==}
    case S.NetExfil{} S.ShellExec{}:
      {==}
    case S.NetExfil{} S.CredRead{}:
      {==}
    case S.NetExfil{} S.FsWrite{}:
      {==}
    case S.NetExfil{} S.NetExfil{}:
      {==}

def lem_classify_ro_spec_rw(b: Bool) -> {S.spec_classify_ro_branch(b) == K.classify_ro_branch(b) : S.Action}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_mcp_spec_rw(pol: Maybe<&2, S.Action>, verb: Maybe<&2, S.Action>, allow_ro: Bool, builtin_ro: Bool, trust_ann: Bool, hint: Bool) -> {S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint) == K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint) : S.Action}:
  match pol verb:
    case Some{_} _:
      {==}
    case None{} Some{_}:
      {==}
    case None{} None{}:
      lem_classify_ro_spec_rw(S.bor(allow_ro, S.bor(builtin_ro, S.band(trust_ann, hint))))

def lem_mcp_untrusted_hint(pol: Maybe<&2, S.Action>, verb: Maybe<&2, S.Action>, allow_ro: Bool, builtin_ro: Bool, hint: Bool) -> {True{} == S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{})) : Bool}:
  match pol verb:
    case Some{a} _:
      lem_action_eq_refl_rw(a)
    case None{} Some{v}:
      lem_action_eq_refl_rw(v)
    case None{} None{}:
      match allow_ro builtin_ro:
        case True{} _:
          {==}
        case False{} True{}:
          {==}
        case False{} False{}:
          {==}

def Laws.L27_ed25519_action_code_and_mcp_classifier(a, b, k, pol, verb, allow_ro, builtin_ro, trust_ann, hint):
  %lem_action_to_code_spec_rw(a) : {(((_, K.action_from_code(K.action_to_code(a))), S.implies(S.bnot(S.spec_action_eq(a, b)), S.bnot(S.nat_eq(K.action_to_code(a), K.action_to_code(b))))), ((None{}, K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{}))))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}
  %lem_action_roundtrip_rw(a) : {(((S.spec_action_to_code(a), _), S.implies(S.bnot(S.spec_action_eq(a, b)), S.bnot(S.nat_eq(K.action_to_code(a), K.action_to_code(b))))), ((None{}, K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{}))))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}
  %lem_action_inj(a, b) : {(((S.spec_action_to_code(a), Some{a}), _), ((None{}, K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{}))))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}
  %lem_mcp_spec_rw(pol, verb, allow_ro, builtin_ro, trust_ann, hint) : {(((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, _), (True{}, S.spec_action_eq(K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, hint), K.classify_mcp_tool(pol, verb, allow_ro, builtin_ro, False{}, False{}))))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}
  %lem_mcp_untrusted_hint(pol, verb, allow_ro, builtin_ro, hint) : {(((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, _))) == (((S.spec_action_to_code(a), Some{a}), True{}), ((None{}, S.spec_classify_mcp_tool(pol, verb, allow_ro, builtin_ro, trust_ann, hint)), (True{}, True{}))) : ((Nat & Maybe<&2, S.Action>) & Bool) & ((Maybe<&2, S.Action> & S.Action) & (Bool & Bool))}
  {==}

# =============================================================================
# 2. GROUP 2 PROOFS: PLANE B 6-SYMBOL MEALY TRANSDUCER, MULTI-CELL R2 & EXTRACT
# =============================================================================

def lem_no_forb_inv(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> {S.has_forbidden_step(K.sanitize_step(xs, r_lt, r_gt, prev), r_lt, r_gt, prev) == False{} : Bool}:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      {==}
    case Con{S.Hidden{}, +rest} r_l r_g p:
      lem_no_forb_inv(rest, r_l, r_g, p)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      lem_no_forb_inv(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      lem_no_forb_inv(rest, S.R2{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R2{} _ _:
      lem_no_forb_inv(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Gt{}, +rest} _ _ S.PPipe{}:
      lem_no_forb_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      lem_no_forb_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      lem_no_forb_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      lem_no_forb_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      lem_no_forb_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R2{} S.PLt{}:
      lem_no_forb_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R2{} S.POther{}:
      lem_no_forb_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Pipe{}, +rest} _ _ S.PLt{}:
      lem_no_forb_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      lem_no_forb_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      lem_no_forb_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Esc{}, +rest} _ _ _:
      lem_no_forb_inv(rest, S.R0{}, S.R0{}, S.POther{})
    case Con{S.Other{}, +rest} _ _ _:
      lem_no_forb_inv(rest, S.R0{}, S.R0{}, S.POther{})

def Laws.L07_fence_no_triple(xs):
  lem_no_forb_inv(xs, S.R0{}, S.R0{}, S.POther{})

def lem_safe_id_inv(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> {S.implies(S.bnot(S.has_forbidden_step(xs, r_lt, r_gt, prev)), S.sym_list_eq(K.sanitize_step(xs, r_lt, r_gt, prev), xs)) == True{} : Bool}:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      {==}
    case Con{S.Hidden{}, _} _ _ _:
      {==}
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R1{}, S.R0{}, S.PLt{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R1{}, S.R0{}, S.PLt{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R2{}, S.R0{}, S.PLt{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R2{}, S.R0{}, S.PLt{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R2{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, _} S.R2{} _ _:
      {==}
    case Con{S.Gt{}, _} _ _ S.PPipe{}:
      {==}
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R1{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R1{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R2{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R2{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R2{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R2{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, _} _ S.R2{} S.PLt{}:
      {==}
    case Con{S.Gt{}, _} _ S.R2{} S.POther{}:
      {==}
    case Con{S.Pipe{}, _} _ _ S.PLt{}:
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R0{}, S.PPipe{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R0{}, S.PPipe{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Esc{}, +rest} _ _ _:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R0{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R0{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R0{}, S.POther{})
    case Con{S.Other{}, +rest} _ _ _:
      %lem_band_true_l_rw(S.sym_list_eq(K.sanitize_step(rest, S.R0{}, S.R0{}, S.POther{}), rest)) : {S.implies(S.bnot(S.has_forbidden_step(rest, S.R0{}, S.R0{}, S.POther{})), _) == True{} : Bool}
      lem_safe_id_inv(rest, S.R0{}, S.R0{}, S.POther{})

def Laws.L08_fence_safe_identity(xs):
  lem_safe_id_inv(xs, S.R0{}, S.R0{}, S.POther{})

def lem_strip_esc_inv_rw(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> {S.strip_esc(S.strip_hidden(xs)) == S.strip_esc(K.sanitize_step(xs, r_lt, r_gt, prev)) : List<&2, S.Sym>}:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      {==}
    case Con{S.Hidden{}, +rest} r_l r_g p:
      lem_strip_esc_inv_rw(rest, r_l, r_g, p)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      %lem_strip_esc_inv_rw(rest, S.R1{}, S.R0{}, S.PLt{}) : {S.Lt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      %lem_strip_esc_inv_rw(rest, S.R2{}, S.R0{}, S.PLt{}) : {S.Lt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Lt{}, +rest} S.R2{} _ _:
      %lem_strip_esc_inv_rw(rest, S.R1{}, S.R0{}, S.PLt{}) : {S.Lt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ _ S.PPipe{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R2{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R2{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R2{} S.PLt{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R2{} S.POther{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> S.strip_esc(S.strip_hidden(rest)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.PLt{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Pipe{} <> S.strip_esc(S.strip_hidden(rest)) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Pipe{} <> S.strip_esc(S.strip_hidden(rest)) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Pipe{} <> S.strip_esc(S.strip_hidden(rest)) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Esc{}, +rest} _ _ _:
      lem_strip_esc_inv_rw(rest, S.R0{}, S.R0{}, S.POther{})
    case Con{S.Other{}, +rest} _ _ _:
      %lem_strip_esc_inv_rw(rest, S.R0{}, S.R0{}, S.POther{}) : {S.Other{} <> S.strip_esc(S.strip_hidden(rest)) == S.Other{} <> _ : List<&2, S.Sym>}
      {==}

def lem_subseq_inv(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> {S.sym_subseq(S.strip_hidden(xs), K.sanitize_step(xs, r_lt, r_gt, prev)) == True{} : Bool}:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      {==}
    case Con{S.Hidden{}, +rest} r_l r_g p:
      lem_subseq_inv(rest, r_l, r_g, p)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      lem_subseq_inv(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      lem_subseq_inv(rest, S.R2{}, S.R0{}, S.PLt{})
    case Con{S.Lt{}, +rest} S.R2{} _ _:
      lem_subseq_inv(rest, S.R1{}, S.R0{}, S.PLt{})
    case Con{S.Gt{}, +rest} _ _ S.PPipe{}:
      lem_subseq_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      lem_subseq_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      lem_subseq_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      lem_subseq_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      lem_subseq_inv(rest, S.R0{}, S.R2{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R2{} S.PLt{}:
      lem_subseq_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Gt{}, +rest} _ S.R2{} S.POther{}:
      lem_subseq_inv(rest, S.R0{}, S.R1{}, S.POther{})
    case Con{S.Pipe{}, +rest} _ _ S.PLt{}:
      lem_subseq_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      lem_subseq_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      lem_subseq_inv(rest, S.R0{}, S.R0{}, S.PPipe{})
    case Con{S.Esc{}, +rest} _ _ _:
      lem_subseq_inv(rest, S.R0{}, S.R0{}, S.POther{})
    case Con{S.Other{}, +rest} _ _ _:
      lem_subseq_inv(rest, S.R0{}, S.R0{}, S.POther{})

def Laws.L20_fence_preserves_all_non_esc_symbols(xs):
  %lem_strip_esc_inv_rw(xs, S.R0{}, S.R0{}, S.POther{}) : {(_, S.sym_subseq(S.strip_hidden(xs), K.sanitize_fences(xs))) == (S.strip_esc(S.strip_hidden(xs)), True{}) : List<&2, S.Sym> & Bool}
  %Equal.sym(Bool, S.sym_subseq(S.strip_hidden(xs), K.sanitize_fences(xs)), True{}, lem_subseq_inv(xs, S.R0{}, S.R0{}, S.POther{})) : {(S.strip_esc(S.strip_hidden(xs)), _) == (S.strip_esc(S.strip_hidden(xs)), True{}) : List<&2, S.Sym> & Bool}
  {==}

def lem_append_other_safe(
  a: List<&2, S.Sym>, +b: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym,
  ha: {False{} == S.has_forbidden_step(a, r_lt, r_gt, prev) : Bool},
  hb: {S.has_forbidden_step(b, S.R0{}, S.R0{}, S.POther{}) == False{} : Bool}
) -> {S.has_forbidden_step(S.append_sym(a, S.Other{} <> b), r_lt, r_gt, prev) == False{} : Bool}:
  match a r_lt r_gt prev:
    case Nil{} _ _ _:
      hb
    case Con{S.Hidden{}, +rest} r_l r_g p:
      lem_ex_falso_false(S.has_forbidden_step(S.Hidden{} <> S.append_sym(rest, S.Other{} <> b), r_l, r_g, p), ha)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      lem_append_other_safe(rest, b, S.R1{}, S.R0{}, S.PLt{}, ha, hb)
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      lem_append_other_safe(rest, b, S.R2{}, S.R0{}, S.PLt{}, ha, hb)
    case Con{S.Lt{}, +rest} S.R2{} r_g p:
      lem_ex_falso_false(S.has_forbidden_step(S.Lt{} <> S.append_sym(rest, S.Other{} <> b), S.R2{}, r_g, p), ha)
    case Con{S.Gt{}, +rest} r_l r_g S.PPipe{}:
      lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.append_sym(rest, S.Other{} <> b), r_l, r_g, S.PPipe{}), ha)
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R1{}, S.POther{}, ha, hb)
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R1{}, S.POther{}, ha, hb)
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R2{}, S.POther{}, ha, hb)
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R2{}, S.POther{}, ha, hb)
    case Con{S.Gt{}, +rest} r_l S.R2{} S.PLt{}:
      lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.append_sym(rest, S.Other{} <> b), r_l, S.R2{}, S.PLt{}), ha)
    case Con{S.Gt{}, +rest} r_l S.R2{} S.POther{}:
      lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.append_sym(rest, S.Other{} <> b), r_l, S.R2{}, S.POther{}), ha)
    case Con{S.Pipe{}, +rest} r_l r_g S.PLt{}:
      lem_ex_falso_false(S.has_forbidden_step(S.Pipe{} <> S.append_sym(rest, S.Other{} <> b), r_l, r_g, S.PLt{}), ha)
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R0{}, S.PPipe{}, ha, hb)
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      lem_append_other_safe(rest, b, S.R0{}, S.R0{}, S.PPipe{}, ha, hb)
    case Con{S.Esc{}, +rest} _ _ _:
      lem_append_other_safe(rest, b, S.R0{}, S.R0{}, S.POther{}, ha, hb)
    case Con{S.Other{}, +rest} _ _ _:
      lem_append_other_safe(rest, b, S.R0{}, S.R0{}, S.POther{}, ha, hb)

def lem_r2_field_safe(field: S.R2Field) -> {S.has_forbidden(K.render_r2_field(field)) == False{} : Bool}:
  match field:
    case S.R2Scalar{+k, +v}:
      lem_append_other_safe(
        K.sanitize_fences(k),
        K.sanitize_fences(v),
        S.R0{}, S.R0{}, S.POther{},
        Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        Laws.L07_fence_no_triple(v)
      )
    case S.R2Table{+k, +col, +c1, +c2}:
      +row = K.sanitize_fences(S.append_sym(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2)))
      pf_col_row = lem_append_other_safe(
        K.sanitize_fences(col),
        row,
        S.R0{}, S.R0{}, S.POther{},
        Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(col)), False{}, Laws.L07_fence_no_triple(col)),
        Laws.L07_fence_no_triple(S.append_sym(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2)))
      )
      lem_append_other_safe(
        K.sanitize_fences(k),
        S.append_sym(K.sanitize_fences(col), S.Other{} <> row),
        S.R0{}, S.R0{}, S.POther{},
        Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        pf_col_row
      )
    case S.R2TensorRef{+k, +sp}:
      lem_append_other_safe(
        K.sanitize_fences(k),
        K.sanitize_fences(S.Lt{} <> (S.Other{} <> S.append_sym(K.sanitize_fences(sp), [S.Gt{}]))),
        S.R0{}, S.R0{}, S.POther{},
        Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(k)), False{}, Laws.L07_fence_no_triple(k)),
        Laws.L07_fence_no_triple(S.Lt{} <> (S.Other{} <> S.append_sym(K.sanitize_fences(sp), [S.Gt{}])))
      )

def lem_strip_esc_append_rw(+a: List<&2, S.Sym>, +b: List<&2, S.Sym>) -> {S.append_sym(S.strip_esc(a), S.strip_esc(b)) == S.strip_esc(S.append_sym(a, b)) : List<&2, S.Sym>}:
  match a:
    case Nil{}:
      {==}
    case Con{S.Esc{}, +rest}:
      lem_strip_esc_append_rw(rest, b)
    case Con{S.Hidden{}, +rest}:
      %lem_strip_esc_append_rw(rest, b) : {S.Hidden{} <> S.append_sym(S.strip_esc(rest), S.strip_esc(b)) == S.Hidden{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Lt{}, +rest}:
      %lem_strip_esc_append_rw(rest, b) : {S.Lt{} <> S.append_sym(S.strip_esc(rest), S.strip_esc(b)) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest}:
      %lem_strip_esc_append_rw(rest, b) : {S.Gt{} <> S.append_sym(S.strip_esc(rest), S.strip_esc(b)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest}:
      %lem_strip_esc_append_rw(rest, b) : {S.Pipe{} <> S.append_sym(S.strip_esc(rest), S.strip_esc(b)) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Other{}, +rest}:
      %lem_strip_esc_append_rw(rest, b) : {S.Other{} <> S.append_sym(S.strip_esc(rest), S.strip_esc(b)) == S.Other{} <> _ : List<&2, S.Sym>}
      {==}

def lem_strip_hidden_append_rw(+a: List<&2, S.Sym>, +b: List<&2, S.Sym>) -> {S.append_sym(S.strip_hidden(a), S.strip_hidden(b)) == S.strip_hidden(S.append_sym(a, b)) : List<&2, S.Sym>}:
  match a:
    case Nil{}:
      {==}
    case Con{S.Hidden{}, +rest}:
      lem_strip_hidden_append_rw(rest, b)
    case Con{S.Lt{}, +rest}:
      %lem_strip_hidden_append_rw(rest, b) : {S.Lt{} <> S.append_sym(S.strip_hidden(rest), S.strip_hidden(b)) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest}:
      %lem_strip_hidden_append_rw(rest, b) : {S.Gt{} <> S.append_sym(S.strip_hidden(rest), S.strip_hidden(b)) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest}:
      %lem_strip_hidden_append_rw(rest, b) : {S.Pipe{} <> S.append_sym(S.strip_hidden(rest), S.strip_hidden(b)) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Esc{}, +rest}:
      %lem_strip_hidden_append_rw(rest, b) : {S.Esc{} <> S.append_sym(S.strip_hidden(rest), S.strip_hidden(b)) == S.Esc{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Other{}, +rest}:
      %lem_strip_hidden_append_rw(rest, b) : {S.Other{} <> S.append_sym(S.strip_hidden(rest), S.strip_hidden(b)) == S.Other{} <> _ : List<&2, S.Sym>}
      {==}

def lem_strip_hidden_sanitize_rw(xs: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym) -> {K.sanitize_step(xs, r_lt, r_gt, prev) == S.strip_hidden(K.sanitize_step(xs, r_lt, r_gt, prev)) : List<&2, S.Sym>}:
  match xs r_lt r_gt prev:
    case Nil{} _ _ _:
      {==}
    case Con{S.Hidden{}, +rest} r_l r_g p:
      lem_strip_hidden_sanitize_rw(rest, r_l, r_g, p)
    case Con{S.Lt{}, +rest} S.R0{} _ _:
      %lem_strip_hidden_sanitize_rw(rest, S.R1{}, S.R0{}, S.PLt{}) : {S.Lt{} <> K.sanitize_step(rest, S.R1{}, S.R0{}, S.PLt{}) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Lt{}, +rest} S.R1{} _ _:
      %lem_strip_hidden_sanitize_rw(rest, S.R2{}, S.R0{}, S.PLt{}) : {S.Lt{} <> K.sanitize_step(rest, S.R2{}, S.R0{}, S.PLt{}) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Lt{}, +rest} S.R2{} _ _:
      %lem_strip_hidden_sanitize_rw(rest, S.R1{}, S.R0{}, S.PLt{}) : {S.Esc{} <> (S.Lt{} <> K.sanitize_step(rest, S.R1{}, S.R0{}, S.PLt{})) == S.Esc{} <> (S.Lt{} <> _) : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ _ S.PPipe{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Esc{} <> (S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{})) == S.Esc{} <> (S.Gt{} <> _) : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{}) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R2{}, S.POther{}) : {S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R2{}, S.POther{}) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R2{}, S.POther{}) : {S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R2{}, S.POther{}) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R2{} S.PLt{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Esc{} <> (S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{})) == S.Esc{} <> (S.Gt{} <> _) : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, +rest} _ S.R2{} S.POther{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R1{}, S.POther{}) : {S.Esc{} <> (S.Gt{} <> K.sanitize_step(rest, S.R0{}, S.R1{}, S.POther{})) == S.Esc{} <> (S.Gt{} <> _) : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.PLt{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Esc{} <> (S.Pipe{} <> K.sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{})) == S.Esc{} <> (S.Pipe{} <> _) : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Pipe{} <> K.sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{}) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, +rest} _ _ S.POther{}:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R0{}, S.PPipe{}) : {S.Pipe{} <> K.sanitize_step(rest, S.R0{}, S.R0{}, S.PPipe{}) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Esc{}, +rest} _ _ _:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R0{}, S.POther{}) : {S.Esc{} <> K.sanitize_step(rest, S.R0{}, S.R0{}, S.POther{}) == S.Esc{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Other{}, +rest} _ _ _:
      %lem_strip_hidden_sanitize_rw(rest, S.R0{}, S.R0{}, S.POther{}) : {S.Other{} <> K.sanitize_step(rest, S.R0{}, S.R0{}, S.POther{}) == S.Other{} <> _ : List<&2, S.Sym>}
      {==}

def lem_strip_pair_other_rw(
  +a: List<&2, S.Sym>, +b_san: List<&2, S.Sym>, +b_raw: List<&2, S.Sym>,
  hb: {b_raw == S.strip_esc(b_san) : List<&2, S.Sym>}
) -> {S.append_sym(S.strip_esc(S.strip_hidden(a)), S.Other{} <> b_raw) == S.strip_esc(S.append_sym(K.sanitize_fences(a), S.Other{} <> b_san)) : List<&2, S.Sym>}:
  %lem_strip_esc_append_rw(K.sanitize_fences(a), S.Other{} <> b_san) : {S.append_sym(S.strip_esc(S.strip_hidden(a)), S.Other{} <> b_raw) == _ : List<&2, S.Sym>}
  %lem_strip_esc_inv_rw(a, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(a)), S.Other{} <> b_raw) == S.append_sym(_, S.Other{} <> S.strip_esc(b_san)) : List<&2, S.Sym>}
  %hb : {S.append_sym(S.strip_esc(S.strip_hidden(a)), S.Other{} <> b_raw) == S.append_sym(S.strip_esc(S.strip_hidden(a)), S.Other{} <> _) : List<&2, S.Sym>}
  {==}

def lem_strip_row_rw(+c1: List<&2, S.Sym>, +c2: List<&2, S.Sym>) -> {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.strip_esc(K.sanitize_fences(S.append_sym(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2)))) : List<&2, S.Sym>}:
  +joined = S.append_sym(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2))
  %lem_strip_esc_inv_rw(joined, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == _ : List<&2, S.Sym>}
  %lem_strip_hidden_append_rw(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2)) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.strip_esc(_) : List<&2, S.Sym>}
  %lem_strip_hidden_sanitize_rw(c1, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.strip_esc(S.append_sym(_, S.Pipe{} <> S.strip_hidden(K.sanitize_fences(c2)))) : List<&2, S.Sym>}
  %lem_strip_hidden_sanitize_rw(c2, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.strip_esc(S.append_sym(K.sanitize_fences(c1), S.Pipe{} <> _)) : List<&2, S.Sym>}
  %lem_strip_esc_append_rw(K.sanitize_fences(c1), S.Pipe{} <> K.sanitize_fences(c2)) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == _ : List<&2, S.Sym>}
  %lem_strip_esc_inv_rw(c1, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.append_sym(_, S.Pipe{} <> S.strip_esc(K.sanitize_fences(c2))) : List<&2, S.Sym>}
  %lem_strip_esc_inv_rw(c2, S.R0{}, S.R0{}, S.POther{}) : {S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(c2))) == S.append_sym(S.strip_esc(S.strip_hidden(c1)), S.Pipe{} <> _) : List<&2, S.Sym>}
  {==}

def lem_strip_tref_body_rw(+sp: List<&2, S.Sym>) -> {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == S.strip_esc(K.sanitize_fences(S.Lt{} <> (S.Other{} <> S.append_sym(K.sanitize_fences(sp), [S.Gt{}])))) : List<&2, S.Sym>}:
  %lem_strip_esc_inv_rw(S.Lt{} <> (S.Other{} <> S.append_sym(K.sanitize_fences(sp), [S.Gt{}])), S.R0{}, S.R0{}, S.POther{}) : {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == _ : List<&2, S.Sym>}
  %lem_strip_hidden_append_rw(K.sanitize_fences(sp), [S.Gt{}]) : {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == S.Lt{} <> (S.Other{} <> S.strip_esc(_)) : List<&2, S.Sym>}
  %lem_strip_hidden_sanitize_rw(sp, S.R0{}, S.R0{}, S.POther{}) : {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == S.Lt{} <> (S.Other{} <> S.strip_esc(S.append_sym(_, [S.Gt{}]))) : List<&2, S.Sym>}
  %lem_strip_esc_append_rw(K.sanitize_fences(sp), [S.Gt{}]) : {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == S.Lt{} <> (S.Other{} <> _) : List<&2, S.Sym>}
  %lem_strip_esc_inv_rw(sp, S.R0{}, S.R0{}, S.POther{}) : {S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(sp)), [S.Gt{}])) == S.Lt{} <> (S.Other{} <> S.append_sym(_, [S.Gt{}])) : List<&2, S.Sym>}
  {==}

def Laws.L21_r2_structured_table_header_safe(field, sk, sv, tk, tcol, tc1, tc2, rk, rsp):
  +row = K.sanitize_fences(S.append_sym(K.sanitize_fences(tc1), S.Pipe{} <> K.sanitize_fences(tc2)))
  +row_raw = S.append_sym(S.strip_esc(S.strip_hidden(tc1)), S.Pipe{} <> S.strip_esc(S.strip_hidden(tc2)))
  pf_safe = Equal.sym(Bool, S.has_forbidden(K.render_r2_field(field)), False{}, lem_r2_field_safe(field))
  pf_sc = lem_strip_pair_other_rw(sk, K.sanitize_fences(sv), S.strip_esc(S.strip_hidden(sv)), lem_strip_esc_inv_rw(sv, S.R0{}, S.R0{}, S.POther{}))
  pf_cr = lem_strip_pair_other_rw(tcol, row, row_raw, lem_strip_row_rw(tc1, tc2))
  pf_tbl = lem_strip_pair_other_rw(tk, S.append_sym(K.sanitize_fences(tcol), S.Other{} <> row), S.append_sym(S.strip_esc(S.strip_hidden(tcol)), S.Other{} <> row_raw), pf_cr)
  pf_tr = lem_strip_pair_other_rw(
    rk,
    K.sanitize_fences(S.Lt{} <> (S.Other{} <> S.append_sym(K.sanitize_fences(rsp), [S.Gt{}]))),
    S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(rsp)), [S.Gt{}])),
    lem_strip_tref_body_rw(rsp)
  )
  +e_sc = S.append_sym(S.strip_esc(S.strip_hidden(sk)), S.Other{} <> S.strip_esc(S.strip_hidden(sv)))
  +e_tb = S.append_sym(S.strip_esc(S.strip_hidden(tk)), S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(tcol)), S.Other{} <> row_raw))
  +e_tr = S.append_sym(S.strip_esc(S.strip_hidden(rk)), S.Other{} <> (S.Lt{} <> (S.Other{} <> S.append_sym(S.strip_esc(S.strip_hidden(rsp)), [S.Gt{}]))))
  %pf_safe : {((_, S.strip_esc(K.render_r2_field(S.R2Scalar{sk, sv}))), (S.strip_esc(K.render_r2_field(S.R2Table{tk, tcol, tc1, tc2})), S.strip_esc(K.render_r2_field(S.R2TensorRef{rk, rsp})))) == ((False{}, e_sc), (e_tb, e_tr)) : (Bool & List<&2, S.Sym>) & (List<&2, S.Sym> & List<&2, S.Sym>)}
  %pf_sc : {((False{}, _), (S.strip_esc(K.render_r2_field(S.R2Table{tk, tcol, tc1, tc2})), S.strip_esc(K.render_r2_field(S.R2TensorRef{rk, rsp})))) == ((False{}, e_sc), (e_tb, e_tr)) : (Bool & List<&2, S.Sym>) & (List<&2, S.Sym> & List<&2, S.Sym>)}
  %pf_tbl : {((False{}, e_sc), (_, S.strip_esc(K.render_r2_field(S.R2TensorRef{rk, rsp})))) == ((False{}, e_sc), (e_tb, e_tr)) : (Bool & List<&2, S.Sym>) & (List<&2, S.Sym> & List<&2, S.Sym>)}
  %pf_tr : {((False{}, e_sc), (e_tb, _)) == ((False{}, e_sc), (e_tb, e_tr)) : (Bool & List<&2, S.Sym>) & (List<&2, S.Sym> & List<&2, S.Sym>)}
  {==}

def lem_take_safe_step(
  n: Nat, ys: List<&2, S.Sym>, r_lt: S.Run2, r_gt: S.Run2, prev: S.PrevSym,
  h_safe: {False{} == S.has_forbidden_step(ys, r_lt, r_gt, prev) : Bool}
) -> {S.has_forbidden_step(S.take_sym(n, ys), r_lt, r_gt, prev) == False{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+pn:
      match ys r_lt r_gt prev:
        case Nil{} _ _ _:
          {==}
        case Con{S.Hidden{}, +rest} r_l r_g p:
          lem_ex_falso_false(S.has_forbidden_step(S.Hidden{} <> S.take_sym(pn, rest), r_l, r_g, p), h_safe)
        case Con{S.Lt{}, +rest} S.R0{} _ _:
          lem_take_safe_step(pn, rest, S.R1{}, S.R0{}, S.PLt{}, h_safe)
        case Con{S.Lt{}, +rest} S.R1{} _ _:
          lem_take_safe_step(pn, rest, S.R2{}, S.R0{}, S.PLt{}, h_safe)
        case Con{S.Lt{}, +rest} S.R2{} r_g p:
          lem_ex_falso_false(S.has_forbidden_step(S.Lt{} <> S.take_sym(pn, rest), S.R2{}, r_g, p), h_safe)
        case Con{S.Gt{}, +rest} r_l r_g S.PPipe{}:
          lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.take_sym(pn, rest), r_l, r_g, S.PPipe{}), h_safe)
        case Con{S.Gt{}, +rest} _ S.R0{} S.PLt{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R1{}, S.POther{}, h_safe)
        case Con{S.Gt{}, +rest} _ S.R0{} S.POther{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R1{}, S.POther{}, h_safe)
        case Con{S.Gt{}, +rest} _ S.R1{} S.PLt{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R2{}, S.POther{}, h_safe)
        case Con{S.Gt{}, +rest} _ S.R1{} S.POther{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R2{}, S.POther{}, h_safe)
        case Con{S.Gt{}, +rest} r_l S.R2{} S.PLt{}:
          lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.take_sym(pn, rest), r_l, S.R2{}, S.PLt{}), h_safe)
        case Con{S.Gt{}, +rest} r_l S.R2{} S.POther{}:
          lem_ex_falso_false(S.has_forbidden_step(S.Gt{} <> S.take_sym(pn, rest), r_l, S.R2{}, S.POther{}), h_safe)
        case Con{S.Pipe{}, +rest} r_l r_g S.PLt{}:
          lem_ex_falso_false(S.has_forbidden_step(S.Pipe{} <> S.take_sym(pn, rest), r_l, r_g, S.PLt{}), h_safe)
        case Con{S.Pipe{}, +rest} _ _ S.PPipe{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R0{}, S.PPipe{}, h_safe)
        case Con{S.Pipe{}, +rest} _ _ S.POther{}:
          lem_take_safe_step(pn, rest, S.R0{}, S.R0{}, S.PPipe{}, h_safe)
        case Con{S.Esc{}, +rest} _ _ _:
          lem_take_safe_step(pn, rest, S.R0{}, S.R0{}, S.POther{}, h_safe)
        case Con{S.Other{}, +rest} _ _ _:
          lem_take_safe_step(pn, rest, S.R0{}, S.R0{}, S.POther{}, h_safe)

def lem_r2_compact_ext_rw(c: Nat, t: Bool) -> {S.FullMealyFence{} == K.r2_compact_mode(S.Auth{c, True{}, t}) : S.LensMode}:
  match c:
    case 0n:
      {==}
    case 1n:
      {==}
    case 2n:
      {==}
    case 1n+(1n+(1n+_)):
      {==}

def lem_r2_compact_tbit_rw(c: Nat, e: Bool) -> {S.FullMealyFence{} == K.r2_compact_mode(S.Auth{c, e, True{}}) : S.LensMode}:
  match c e:
    case 0n _:
      {==}
    case 1n True{}:
      {==}
    case 1n False{}:
      {==}
    case 2n True{}:
      {==}
    case 2n False{}:
      {==}
    case 1n+(1n+(1n+_)) _:
      {==}

def Laws.L28_r2_compact_and_virtual_atlas_fail_closed(a, c, t, e, k, n, xs):
  pf_take = Equal.sym(
    Bool,
    S.has_forbidden(S.take_sym(n, K.sanitize_fences(xs))),
    False{},
    lem_take_safe_step(n, K.sanitize_fences(xs), S.R0{}, S.R0{}, S.POther{}, Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(xs)), False{}, Laws.L07_fence_no_triple(xs)))
  )
  %lem_r2_compact_ext_rw(c, t) : {((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, _), ((K.r2_compact_mode(S.Auth{c, e, True{}}), S.FullMealyFence{}), (K.r2_compact_mode(a), S.has_forbidden(S.take_sym(n, K.sanitize_fences(xs))))))) == ((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((S.FullMealyFence{}, S.FullMealyFence{}), (K.r2_compact_mode(a), False{})))) : (S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & (S.LensMode & Bool)))}
  %lem_r2_compact_tbit_rw(c, e) : {((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((_, S.FullMealyFence{}), (K.r2_compact_mode(a), S.has_forbidden(S.take_sym(n, K.sanitize_fences(xs))))))) == ((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((S.FullMealyFence{}, S.FullMealyFence{}), (K.r2_compact_mode(a), False{})))) : (S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & (S.LensMode & Bool)))}
  %pf_take : {((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((S.FullMealyFence{}, S.FullMealyFence{}), (K.r2_compact_mode(a), _)))) == ((S.CompactHeader{}, S.CompactHeader{}), ((S.FullMealyFence{}, S.FullMealyFence{}), ((S.FullMealyFence{}, S.FullMealyFence{}), (K.r2_compact_mode(a), False{})))) : (S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & ((S.LensMode & S.LensMode) & (S.LensMode & Bool)))}
  {==}

def lem_cell_nonempty_safe(is_nm: Bool, +eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_has_forbidden(K.encode_nonempty_cell(is_nm, eff)) : Bool}:
  match is_nm:
    case True{}:
      Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(eff)), False{}, Laws.L07_fence_no_triple(eff))
    case False{}:
      Equal.sym(Bool, S.has_forbidden(K.sanitize_fences(eff)), False{}, Laws.L07_fence_no_triple(eff))

def lem_cell_cleaned_safe(eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_has_forbidden(K.encode_cleaned_str(eff)) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      lem_cell_nonempty_safe(K.is_null_or_missing_str(h <> t), h <> t)

def lem_cell_safe(c: S.TableCell) -> {False{} == S.cell_tag_has_forbidden(K.encode_table_cell(c)) : Bool}:
  match c:
    case S.CellNull{}:
      {==}
    case S.CellMissing{}:
      {==}
    case S.CellStr{raw}:
      lem_cell_cleaned_safe(S.strip_hidden(raw))

def lem_cell_ne_null_nonempty(is_nm: Bool, eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_eq(S.OutNull{}, K.encode_nonempty_cell(is_nm, eff)) : Bool}:
  match is_nm:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_cell_ne_null_clean(eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_eq(S.OutNull{}, K.encode_cleaned_str(eff)) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      lem_cell_ne_null_nonempty(K.is_null_or_missing_str(h <> t), h <> t)

def lem_cell_ne_miss_nonempty(is_nm: Bool, eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_eq(S.OutMissing{}, K.encode_nonempty_cell(is_nm, eff)) : Bool}:
  match is_nm:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_cell_ne_miss_clean(eff: List<&2, S.Sym>) -> {False{} == S.cell_tag_eq(S.OutMissing{}, K.encode_cleaned_str(eff)) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      lem_cell_ne_miss_nonempty(K.is_null_or_missing_str(h <> t), h <> t)

def lem_strip_hidden_idem_rw(xs: List<&2, S.Sym>) -> {S.strip_hidden(xs) == S.strip_hidden(S.strip_hidden(xs)) : List<&2, S.Sym>}:
  match xs:
    case Nil{}:
      {==}
    case Con{S.Hidden{}, rest}:
      lem_strip_hidden_idem_rw(rest)
    case Con{S.Lt{}, rest}:
      %lem_strip_hidden_idem_rw(rest) : {S.Lt{} <> S.strip_hidden(rest) == S.Lt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Gt{}, rest}:
      %lem_strip_hidden_idem_rw(rest) : {S.Gt{} <> S.strip_hidden(rest) == S.Gt{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Pipe{}, rest}:
      %lem_strip_hidden_idem_rw(rest) : {S.Pipe{} <> S.strip_hidden(rest) == S.Pipe{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Esc{}, rest}:
      %lem_strip_hidden_idem_rw(rest) : {S.Esc{} <> S.strip_hidden(rest) == S.Esc{} <> _ : List<&2, S.Sym>}
      {==}
    case Con{S.Other{}, rest}:
      %lem_strip_hidden_idem_rw(rest) : {S.Other{} <> S.strip_hidden(rest) == S.Other{} <> _ : List<&2, S.Sym>}
      {==}

def lem_cell_nonempty_strip_esc_rw(is_nm: Bool, +eff: List<&2, S.Sym>) -> {S.strip_esc(S.strip_hidden(eff)) == S.strip_esc(S.cell_tag_content(K.encode_nonempty_cell(is_nm, eff))) : List<&2, S.Sym>}:
  match is_nm:
    case True{}:
      lem_strip_esc_inv_rw(eff, S.R0{}, S.R0{}, S.POther{})
    case False{}:
      lem_strip_esc_inv_rw(eff, S.R0{}, S.R0{}, S.POther{})

def lem_cell_cleaned_strip_esc_rw(eff: List<&2, S.Sym>) -> {S.strip_esc(S.strip_hidden(eff)) == S.strip_esc(S.cell_tag_content(K.encode_cleaned_str(eff))) : List<&2, S.Sym>}:
  match eff:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      lem_cell_nonempty_strip_esc_rw(K.is_null_or_missing_str(h <> t), h <> t)

def lem_cell_nonempty_subseq_rw(is_nm: Bool, +eff: List<&2, S.Sym>) -> {True{} == S.sym_subseq(S.strip_hidden(eff), S.cell_tag_content(K.encode_nonempty_cell(is_nm, eff))) : Bool}:
  match is_nm:
    case True{}:
      Equal.sym(Bool, S.sym_subseq(S.strip_hidden(eff), K.sanitize_fences(eff)), True{}, lem_subseq_inv(eff, S.R0{}, S.R0{}, S.POther{}))
    case False{}:
      Equal.sym(Bool, S.sym_subseq(S.strip_hidden(eff), K.sanitize_fences(eff)), True{}, lem_subseq_inv(eff, S.R0{}, S.R0{}, S.POther{}))

def lem_cell_cleaned_subseq_rw(eff: List<&2, S.Sym>) -> {True{} == S.sym_subseq(S.strip_hidden(eff), S.cell_tag_content(K.encode_cleaned_str(eff))) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      lem_cell_nonempty_subseq_rw(K.is_null_or_missing_str(h <> t), h <> t)

def lem_cell_nil_marker_rw(eff: List<&2, S.Sym>) -> {True{} == S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{_, _}:
      {==}

def lem_cell_null_marker_rw(eff: List<&2, S.Sym>) -> {True{} == S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]})) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{S.Other{}, Nil{}}:
      {==}
    case Con{S.Other{}, Con{_, _}}:
      {==}
    case Con{S.Lt{}, _}:
      {==}
    case Con{S.Gt{}, _}:
      {==}
    case Con{S.Pipe{}, _}:
      {==}
    case Con{S.Esc{}, _}:
      {==}
    case Con{S.Hidden{}, _}:
      {==}

def lem_cell_missing_marker_rw(eff: List<&2, S.Sym>) -> {True{} == S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]})) : Bool}:
  match eff:
    case Nil{}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Gt{}, Nil{}}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Gt{}, Con{_, _}}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Lt{}, _}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Pipe{}, _}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Esc{}, _}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Hidden{}, _}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Con{S.Other{}, _}}}:
      {==}
    case Con{S.Lt{}, Con{S.Other{}, Nil{}}}:
      {==}
    case Con{S.Lt{}, Con{S.Lt{}, _}}:
      {==}
    case Con{S.Lt{}, Con{S.Gt{}, _}}:
      {==}
    case Con{S.Lt{}, Con{S.Pipe{}, _}}:
      {==}
    case Con{S.Lt{}, Con{S.Esc{}, _}}:
      {==}
    case Con{S.Lt{}, Con{S.Hidden{}, _}}:
      {==}
    case Con{S.Lt{}, Nil{}}:
      {==}
    case Con{S.Gt{}, _}:
      {==}
    case Con{S.Pipe{}, _}:
      {==}
    case Con{S.Esc{}, _}:
      {==}
    case Con{S.Hidden{}, _}:
      {==}
    case Con{S.Other{}, _}:
      {==}

def Laws.L29_r2_table_marker_injectivity(c, raw):
  +eff = S.strip_hidden(raw)
  %lem_cell_safe(c) : {((_, S.cell_tag_eq(S.OutNull{}, K.encode_cleaned_str(eff))), ((S.cell_tag_eq(S.OutMissing{}, K.encode_cleaned_str(eff)), False{}), ((K.encode_cleaned_str(eff), S.strip_esc(S.cell_tag_content(K.encode_cleaned_str(eff)))), (S.sym_subseq(eff, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(S.strip_hidden(eff)), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_ne_null_clean(eff) : {((False{}, _), ((S.cell_tag_eq(S.OutMissing{}, K.encode_cleaned_str(eff)), False{}), ((K.encode_cleaned_str(eff), S.strip_esc(S.cell_tag_content(K.encode_cleaned_str(eff)))), (S.sym_subseq(eff, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(S.strip_hidden(eff)), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_ne_miss_clean(eff) : {((False{}, False{}), ((_, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(S.cell_tag_content(K.encode_cleaned_str(eff)))), (S.sym_subseq(eff, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(S.strip_hidden(eff)), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_cleaned_strip_esc_rw(eff) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), _), (S.sym_subseq(eff, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(S.strip_hidden(eff)), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_strip_hidden_idem_rw(raw) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(_)), (S.sym_subseq(eff, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(_), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %Equal.sym(List<&2, S.Sym>, eff, S.strip_hidden(eff), lem_strip_hidden_idem_rw(raw)) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (S.sym_subseq(_, S.cell_tag_content(K.encode_cleaned_str(eff))), ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_cleaned_subseq_rw(eff) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (_, ((S.implies(S.sym_list_eq(eff, Nil{}), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutEmptyQuoted{})), S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_nil_marker_rw(eff) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((_, S.implies(S.sym_list_eq(eff, [S.Other{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Other{}]}))), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_null_marker_rw(eff) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, _), S.implies(S.sym_list_eq(eff, [S.Lt{}, S.Other{}, S.Gt{}]), S.cell_tag_eq(K.encode_cleaned_str(eff), S.OutQuotedStr{[S.Lt{}, S.Other{}, S.Gt{}]}))))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  %lem_cell_missing_marker_rw(eff) : {((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), _))))) == ((False{}, False{}), ((False{}, False{}), ((K.encode_cleaned_str(eff), S.strip_esc(eff)), (True{}, ((True{}, True{}), True{}))))) : (Bool & Bool) & ((Bool & Bool) & ((S.CellTag & List<&2, S.Sym>) & (Bool & ((Bool & Bool) & Bool))))}
  {==}

def lem_select_val_spec_rw(eq: Bool, v: S.ValTree, fb: Maybe<&2, S.ValTree>) -> {S.spec_select_val(eq, v, fb) == K.select_val(eq, v, fb) : Maybe<&2, S.ValTree>}:
  match eq:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_map_find_spec_rw(es: List<&2, S.MapEntry>, +k: Nat) -> {S.spec_map_find_val(es, k) == K.map_find_val(es, k) : Maybe<&2, S.ValTree>}:
  match es:
    case Nil{}:
      {==}
    case Con{S.MEntry{+ek, +v}, +rest}:
      %lem_map_find_spec_rw(rest, k) : {S.spec_select_val(S.nat_eq(ek, k), v, S.spec_map_find_val(rest, k)) == K.select_val(S.nat_eq(ek, k), v, _) : Maybe<&2, S.ValTree>}
      lem_select_val_spec_rw(S.nat_eq(ek, k), v, S.spec_map_find_val(rest, k))

def lem_list_nth_spec_rw(xs: List<&2, S.ValTree>, idx: Nat) -> {S.spec_list_nth_val(xs, idx) == K.list_nth_val(xs, idx) : Maybe<&2, S.ValTree>}:
  match xs idx:
    case Nil{} _:
      {==}
    case Con{_, _} 0n:
      {==}
    case Con{_, t} 1n+pi:
      lem_list_nth_spec_rw(t, pi)

def lem_extract_step_spec_rw(cur: S.ValTree, seg: S.PathSeg) -> {S.spec_extract_step(cur, seg) == K.extract_step(cur, seg) : Maybe<&2, S.ValTree>}:
  match cur seg:
    case S.VMap{es} S.PKey{k}:
      lem_map_find_spec_rw(es, k)
    case S.VArr{xs} S.PIdx{i}:
      lem_list_nth_spec_rw(xs, i)
    case S.VMap{_} S.PIdx{_}:
      {==}
    case S.VArr{_} S.PKey{_}:
      {==}
    case S.VAtom{_} S.PKey{_}:
      {==}
    case S.VAtom{_} S.PIdx{_}:
      {==}

def lem_extract_opt_step_spec_rw(cur: Maybe<&2, S.ValTree>, seg: S.PathSeg) -> {S.spec_extract_opt_step(cur, seg) == K.extract_opt_step(cur, seg) : Maybe<&2, S.ValTree>}:
  match cur:
    case None{}:
      {==}
    case Some{v}:
      lem_extract_step_spec_rw(v, seg)

def lem_extract_field_opt_spec_rw(path: List<&2, S.PathSeg>, +cur: Maybe<&2, S.ValTree>) -> {S.spec_extract_field_opt(path, cur) == K.extract_field_opt(path, cur) : Maybe<&2, S.ValTree>}:
  match path:
    case Nil{}:
      {==}
    case Con{+seg, +rest}:
      %lem_extract_opt_step_spec_rw(cur, seg) : {S.spec_extract_field_opt(rest, S.spec_extract_opt_step(cur, seg)) == K.extract_field_opt(rest, _) : Maybe<&2, S.ValTree>}
      lem_extract_field_opt_spec_rw(rest, S.spec_extract_opt_step(cur, seg))

def lem_extract_none_rw(rest: List<&2, S.PathSeg>) -> {None{} == K.extract_field_opt(rest, None{}) : Maybe<&2, S.ValTree>}:
  match rest:
    case Nil{}:
      {==}
    case Con{_, t}:
      lem_extract_none_rw(t)

def lem_extract_append_rw(p1: List<&2, S.PathSeg>, +p2: List<&2, S.PathSeg>, cur: Maybe<&2, S.ValTree>) -> {Laws.opt_val_bind_extract(K.extract_field_opt(p1, cur), p2) == K.extract_field_opt(S.append_path(p1, p2), cur) : Maybe<&2, S.ValTree>}:
  match p1:
    case Nil{}:
      match cur:
        case None{}:
          lem_extract_none_rw(p2)
        case Some{_}:
          {==}
    case Con{seg, rest}:
      lem_extract_append_rw(rest, p2, K.extract_opt_step(cur, seg))

def lem_map_find_deep_step(
  eq: Bool, +ek: Nat, +ev: S.ValTree, +rest: List<&2, S.MapEntry>, +k: Nat, +a: Nat, +post_m: List<&2, S.MapEntry>,
  eq_pf: {eq == S.nat_eq(ek, k) : Bool},
  ih: {True{} == S.implies(S.bnot(S.spec_map_has_key(rest, k)), S.spec_maybe_val_is_atom(K.map_find_val(S.append_map(rest, S.MEntry{k, S.VAtom{a}} <> post_m), k), a)) : Bool}
) -> {True{} == S.implies(S.bnot(S.bor(S.nat_eq(ek, k), S.spec_map_has_key(rest, k))), S.spec_maybe_val_is_atom(K.select_val(S.nat_eq(ek, k), ev, K.map_find_val(S.append_map(rest, S.MEntry{k, S.VAtom{a}} <> post_m), k)), a)) : Bool}:
  match eq:
    case True{}:
      %eq_pf : {True{} == S.implies(S.bnot(S.bor(_, S.spec_map_has_key(rest, k))), S.spec_maybe_val_is_atom(K.select_val(_, ev, K.map_find_val(S.append_map(rest, S.MEntry{k, S.VAtom{a}} <> post_m), k)), a)) : Bool}
      {==}
    case False{}:
      %eq_pf : {True{} == S.implies(S.bnot(S.bor(_, S.spec_map_has_key(rest, k))), S.spec_maybe_val_is_atom(K.select_val(_, ev, K.map_find_val(S.append_map(rest, S.MEntry{k, S.VAtom{a}} <> post_m), k)), a)) : Bool}
      ih

def lem_map_find_deep_rw(pre_m: List<&2, S.MapEntry>, +k: Nat, +a: Nat, +post_m: List<&2, S.MapEntry>) -> {True{} == S.implies(S.bnot(S.spec_map_has_key(pre_m, k)), S.spec_maybe_val_is_atom(K.map_find_val(S.append_map(pre_m, S.MEntry{k, S.VAtom{a}} <> post_m), k), a)) : Bool}:
  match pre_m:
    case Nil{}:
      %lem_nat_eq_refl_rw(k) : {True{} == S.spec_maybe_val_is_atom(K.select_val(_, S.VAtom{a}, K.map_find_val(post_m, k)), a) : Bool}
      lem_nat_eq_refl_rw(a)
    case Con{S.MEntry{+ek, +ev}, +rest}:
      lem_map_find_deep_step(S.nat_eq(ek, k), ek, ev, rest, k, a, post_m, {==}, lem_map_find_deep_rw(rest, k, a, post_m))

def lem_list_nth_hit_rw(pre_a: List<&2, S.ValTree>, +v: S.ValTree, +post_a: List<&2, S.ValTree>) -> {Some{v} == K.list_nth_val(S.append_val(pre_a, v <> post_a), S.list_len_val(pre_a)) : Maybe<&2, S.ValTree>}:
  match pre_a:
    case Nil{}:
      {==}
    case Con{_, rest}:
      lem_list_nth_hit_rw(rest, v, post_a)

def lem_list_nth_oob_rw(pre_a: List<&2, S.ValTree>, +k: Nat) -> {None{} == K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)) : Maybe<&2, S.ValTree>}:
  match pre_a:
    case Nil{}:
      {==}
    case Con{_, rest}:
      lem_list_nth_oob_rw(rest, k)

def lem_extract_atom_rw(a: Nat, seg: S.PathSeg, rest: List<&2, S.PathSeg>) -> {None{} == K.extract_field(S.VAtom{a}, seg <> rest) : Maybe<&2, S.ValTree>}:
  match seg:
    case S.PKey{_}:
      lem_extract_none_rw(rest)
    case S.PIdx{_}:
      lem_extract_none_rw(rest)

def Laws.L30_lens_tier_bounds_and_extract_field(t, k, idx, v, pre_m, post_m, pre_a, post_a, a, seg, p1, p2):
  %lem_extract_field_opt_spec_rw(p1, Some{v}) : {(((S.nat_le(t, 3n), False{}), (_, (Some{v}, K.extract_field(v, S.append_path(p1, p2))))), ((S.implies(S.bnot(S.spec_map_has_key(pre_m, k)), S.spec_maybe_val_is_atom(K.map_find_val(S.append_map(pre_m, S.MEntry{k, S.VAtom{a}} <> post_m), k), a)), K.list_nth_val(S.append_val(pre_a, v <> post_a), S.list_len_val(pre_a))), ((K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)), None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(_, p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_extract_append_rw(p1, p2, Some{v}) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, _))), ((S.implies(S.bnot(S.spec_map_has_key(pre_m, k)), S.spec_maybe_val_is_atom(K.map_find_val(S.append_map(pre_m, S.MEntry{k, S.VAtom{a}} <> post_m), k), a)), K.list_nth_val(S.append_val(pre_a, v <> post_a), S.list_len_val(pre_a))), ((K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)), None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_extract_field_opt_spec_rw(p1, Some{v}) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(_, p2)))), ((S.implies(S.bnot(S.spec_map_has_key(pre_m, k)), S.spec_maybe_val_is_atom(K.map_find_val(S.append_map(pre_m, S.MEntry{k, S.VAtom{a}} <> post_m), k), a)), K.list_nth_val(S.append_val(pre_a, v <> post_a), S.list_len_val(pre_a))), ((K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)), None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_map_find_deep_rw(pre_m, k, a, post_m) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((_, K.list_nth_val(S.append_val(pre_a, v <> post_a), S.list_len_val(pre_a))), ((K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)), None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_list_nth_hit_rw(pre_a, v, post_a) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, _), ((K.list_nth_val(pre_a, S.nat_add(S.list_len_val(pre_a), k)), None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_list_nth_oob_rw(pre_a, k) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((_, None{}), (None{}, K.extract_field(S.VAtom{a}, seg <> p1))))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  %lem_extract_atom_rw(a, seg, p1) : {(((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, _)))) == (((S.nat_le(t, 3n), False{}), (S.spec_extract_field(v, p1), (Some{v}, Laws.opt_val_bind_extract(S.spec_extract_field(v, p1), p2)))), ((True{}, Some{v}), ((None{}, None{}), (None{}, None{})))) : ((Bool & Bool) & (Maybe<&2, S.ValTree> & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>))) & ((Bool & Maybe<&2, S.ValTree>) & ((Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>) & (Maybe<&2, S.ValTree> & Maybe<&2, S.ValTree>)))}
  {==}

# =============================================================================
# 3. GROUP 3 PROOFS: PLANE A CANONICAL WIRE, FLOAT64, MAP/DICT, SENTINELS, RCID
# =============================================================================

def lem_roundtrip_pos(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == S.nat_lt(n, lim) : Bool}) -> {K.decode_canon_int(lim, ulim, K.encode_pos_branch(b, n)) == Some{S.LPos{n}} : Maybe<&2, S.LogicalInt>}:
  match b:
    case True{}:
      %eq : {K.decode_fix_branch(_, n) == Some{S.LPos{n}} : Maybe<&2, S.LogicalInt>}
      {==}
    case False{}:
      %eq : {K.decode_pos_branch(_, n) == Some{S.LPos{n}} : Maybe<&2, S.LogicalInt>}
      {==}

def Laws.L09_canon_roundtrip(lim, ulim, v):
  match v:
    case S.LPos{+n}:
      lem_roundtrip_pos(S.nat_lt(n, lim), lim, ulim, n, {==})
    case S.LNeg{_}:
      {==}
    case S.LBigUint{+d}:
      %lem_lt_add_false_rw(ulim, d) : {K.decode_uint_branch(_, ulim, S.nat_add(ulim, d)) == Some{S.LBigUint{d}} : Maybe<&2, S.LogicalInt>}
      %lem_add_sub_cancel_rw(ulim, d) : {Some{S.LBigUint{_}} == Some{S.LBigUint{d}} : Maybe<&2, S.LogicalInt>}
      {==}

def lem_ver_fix(b: Bool, n: Nat) -> {b == S.spec_is_some_int(K.decode_fix_branch(b, n)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ver_pos(b: Bool, n: Nat) -> {S.bnot(b) == S.spec_is_some_int(K.decode_pos_branch(b, n)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ver_uint(b: Bool, ulim: Nat, u: Nat) -> {S.bnot(b) == S.spec_is_some_int(K.decode_uint_branch(b, ulim, u)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def Laws.L10_verify_iff_decode(lim, ulim, w):
  match w:
    case S.WFix{+n}:
      %lem_ver_fix(S.nat_lt(n, lim), n) : {(S.nat_lt(n, lim), S.nat_lt(n, lim)) == (S.nat_lt(n, lim), _) : Bool & Bool}
      {==}
    case S.WTagIntPos{+n}:
      %lem_ver_pos(S.nat_lt(n, lim), n) : {(S.bnot(S.nat_lt(n, lim)), S.bnot(S.nat_lt(n, lim))) == (S.bnot(S.nat_lt(n, lim)), _) : Bool & Bool}
      {==}
    case S.WTagIntNeg{_}:
      {==}
    case S.WTagUint{+u}:
      %lem_ver_uint(S.nat_lt(u, ulim), ulim, u) : {(S.bnot(S.nat_lt(u, ulim)), S.bnot(S.nat_lt(u, ulim))) == (S.bnot(S.nat_lt(u, ulim)), _) : Bool & Bool}
      {==}

def lem_unique_fix(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == S.nat_lt(n, lim) : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, S.WFix{n}, K.decode_fix_branch(b, n)) == True{} : Bool}:
  match b:
    case False{}:
      {==}
    case True{}:
      %eq : {S.wire_int_eq(K.encode_pos_branch(_, n), S.WFix{n}) == True{} : Bool}
      lem_nat_eq_refl(n)

def lem_unique_pos(b: Bool, +lim: Nat, +ulim: Nat, +n: Nat, eq: {b == S.nat_lt(n, lim) : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, S.WTagIntPos{n}, K.decode_pos_branch(b, n)) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      %eq : {S.wire_int_eq(K.encode_pos_branch(_, n), S.WTagIntPos{n}) == True{} : Bool}
      lem_nat_eq_refl(n)

def lem_unique_uint(b: Bool, +lim: Nat, +ulim: Nat, +u: Nat, _eq: {b == S.nat_lt(u, ulim) : Bool}, restore: {S.implies(S.bnot(b), S.nat_eq(S.nat_add(ulim, S.nat_sub(u, ulim)), u)) == True{} : Bool}) -> {Laws.decoded_reencodes_identically(lim, ulim, S.WTagUint{u}, K.decode_uint_branch(b, ulim, u)) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      restore

def Laws.L11_canon_wire_unique(lim, ulim, w):
  match w:
    case S.WFix{+n}:
      lem_unique_fix(S.nat_lt(n, lim), lim, ulim, n, {==})
    case S.WTagIntPos{+n}:
      lem_unique_pos(S.nat_lt(n, lim), lim, ulim, n, {==})
    case S.WTagIntNeg{+m}:
      lem_nat_eq_refl(m)
    case S.WTagUint{+u}:
      lem_unique_uint(S.nat_lt(u, ulim), lim, ulim, u, {==}, lem_sub_add_restore(ulim, u))

def lem_verify_map_keys_spec_rw(ks: List<&2, Nat>, +min_a: Nat, +nkeys: Nat) -> {S.spec_strictly_asc_from(ks, min_a, nkeys) == K.verify_map_keys_from(ks, min_a, nkeys) : Bool}:
  match ks:
    case Nil{}:
      {==}
    case Con{+k, +rest}:
      %lem_verify_map_keys_spec_rw(rest, 1n+k, nkeys) : {S.band(S.nat_le(min_a, k), S.band(S.nat_lt(k, nkeys), S.spec_strictly_asc_from(rest, 1n+k, nkeys))) == S.band(S.nat_le(min_a, k), S.band(S.nat_lt(k, nkeys), _)) : Bool}
      {==}

def lem_list_contains_spec_rw(xs: List<&2, Nat>, +target: Nat) -> {S.nat_mem(xs, target) == K.list_contains_nat(xs, target) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, +rest}:
      %lem_list_contains_spec_rw(rest, target) : {S.bor(S.nat_eq(x, target), S.nat_mem(rest, target)) == S.bor(S.nat_eq(x, target), _) : Bool}
      {==}

def lem_all_dict_used_spec_rw(+nkeys: Nat, +refs: List<&2, Nat>) -> {S.spec_all_lt_in(nkeys, refs) == K.all_dict_keys_used(nkeys, refs) : Bool}:
  match nkeys:
    case 0n:
      {==}
    case 1n+pk:
      %lem_list_contains_spec_rw(refs, pk) : {S.band(S.nat_mem(refs, pk), S.spec_all_lt_in(pk, refs)) == S.band(_, K.all_dict_keys_used(pk, refs)) : Bool}
      %lem_all_dict_used_spec_rw(pk, refs) : {S.band(S.nat_mem(refs, pk), S.spec_all_lt_in(pk, refs)) == S.band(S.nat_mem(refs, pk), _) : Bool}
      {==}

def lem_oob_head_false_rw(+nkeys: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(S.nat_add(nkeys, d) <> tail, min_a, nkeys) : Bool}:
  +bad = S.nat_add(nkeys, d)
  %lem_lt_add_false_rw(nkeys, d) : {False{} == S.band(S.nat_le(min_a, bad), S.band(_, K.verify_map_keys_from(tail, 1n+bad, nkeys))) : Bool}
  lem_band3_false_m_rw(S.nat_le(min_a, bad), K.verify_map_keys_from(tail, 1n+bad, nkeys))

def lem_oob_deep_false_rw(prefix: List<&2, Nat>, +nkeys: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(S.append_nat(prefix, S.nat_add(nkeys, d) <> tail), min_a, nkeys) : Bool}:
  match prefix:
    case Nil{}:
      lem_oob_head_false_rw(nkeys, d, tail, min_a)
    case Con{+p, +rest}:
      %lem_oob_deep_false_rw(rest, nkeys, d, tail, 1n+p) : {False{} == S.band(S.nat_le(min_a, p), S.band(S.nat_lt(p, nkeys), _)) : Bool}
      lem_band3_false_r_rw(S.nat_le(min_a, p), S.nat_lt(p, nkeys))

def lem_unsorted_pair_head_rw(+nkeys: Nat, +b: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(S.nat_add(b, d) <> (b <> tail), min_a, nkeys) : Bool}:
  +hi_k = S.nat_add(b, d)
  %lem_le_succ_add_false_rw(b, d) : {False{} == S.band(S.nat_le(min_a, hi_k), S.band(S.nat_lt(hi_k, nkeys), S.band(_, S.band(S.nat_lt(b, nkeys), K.verify_map_keys_from(tail, 1n+b, nkeys))))) : Bool}
  lem_band3_false_r_rw(S.nat_le(min_a, hi_k), S.nat_lt(hi_k, nkeys))

def lem_unsorted_deep_false_rw(prefix: List<&2, Nat>, +nkeys: Nat, +b: Nat, +d: Nat, +tail: List<&2, Nat>, +min_a: Nat) -> {False{} == K.verify_map_keys_from(S.append_nat(prefix, S.nat_add(b, d) <> (b <> tail)), min_a, nkeys) : Bool}:
  match prefix:
    case Nil{}:
      lem_unsorted_pair_head_rw(nkeys, b, d, tail, min_a)
    case Con{+p, +rest}:
      %lem_unsorted_deep_false_rw(rest, nkeys, b, d, tail, 1n+p) : {False{} == S.band(S.nat_le(min_a, p), S.band(S.nat_lt(p, nkeys), _)) : Bool}
      lem_band3_false_r_rw(S.nat_le(min_a, p), S.nat_lt(p, nkeys))

def lem_le_split(m: Nat, pk: Nat) -> {S.implies(S.nat_le(m, pk), S.bor(S.nat_eq(m, pk), S.nat_lt(m, pk))) == True{} : Bool}:
  match m pk:
    case 0n 0n:
      {==}
    case 0n 1n+_:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pm 1n+ppk:
      lem_le_split(pm, ppk)

def lem_nat_eq_cong_r(x: Nat, m: Nat, pk: Nat) -> {S.implies(S.nat_eq(m, pk), S.implies(S.bnot(S.nat_eq(x, m)), S.bnot(S.nat_eq(x, pk)))) == True{} : Bool}:
  match x m pk:
    case 0n 0n 0n:
      {==}
    case 0n 0n 1n+_:
      {==}
    case 0n 1n+_ 0n:
      {==}
    case 0n 1n+pm 1n+ppk:
      lem_implies_true_r(S.nat_eq(pm, ppk))
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
  p_hd: {S.implies(eq_m, S.implies(S.bnot(eq_xm), S.bnot(eq_xp))) == True{} : Bool},
  p_tl: {S.implies(eq_m, S.implies(S.bnot(cm), S.bnot(cp))) == True{} : Bool}
) -> {S.implies(eq_m, S.implies(S.bnot(S.bor(eq_xm, cm)), S.bnot(S.bor(eq_xp, cp)))) == True{} : Bool}:
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

def lem_mem_cong(refs: List<&2, Nat>, +m: Nat, +pk: Nat) -> {S.implies(S.nat_eq(m, pk), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(S.nat_mem(refs, pk)))) == True{} : Bool}:
  match refs:
    case Nil{}:
      lem_implies_true_r(S.nat_eq(m, pk))
    case Con{+x, +rest}:
      lem_contains_step_bool(
        S.nat_eq(m, pk),
        S.nat_eq(x, m),
        S.nat_eq(x, pk),
        S.nat_mem(rest, m),
        S.nat_mem(rest, pk),
        lem_nat_eq_cong_r(x, m, pk),
        lem_mem_cong(rest, m, pk)
      )

def lem_unused_step_bool(
  le_mp: Bool, not_cm: Bool, cp: Bool, all_p: Bool, eq_mp: Bool, lt_mp: Bool,
  p_split: {S.implies(le_mp, S.bor(eq_mp, lt_mp)) == True{} : Bool},
  p_eq: {S.implies(eq_mp, S.implies(not_cm, S.bnot(cp))) == True{} : Bool},
  p_ih: {S.implies(lt_mp, S.implies(not_cm, S.bnot(all_p))) == True{} : Bool}
) -> {S.implies(le_mp, S.implies(not_cm, S.bnot(S.band(cp, all_p)))) == True{} : Bool}:
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

def lem_unused_key_blocks(+nkeys: Nat, +m: Nat, +refs: List<&2, Nat>) -> {S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(S.spec_all_lt_in(nkeys, refs)))) == True{} : Bool}:
  match nkeys:
    case 0n:
      {==}
    case 1n+pk:
      lem_unused_step_bool(
        S.nat_le(m, pk),
        S.bnot(S.nat_mem(refs, m)),
        S.nat_mem(refs, pk),
        S.spec_all_lt_in(pk, refs),
        S.nat_eq(m, pk),
        S.nat_lt(m, pk),
        lem_le_split(m, pk),
        lem_mem_cong(refs, m, pk),
        lem_unused_key_blocks(pk, m, refs)
      )

def Laws.L22_map_keys_and_dict_all_used(nkeys, ks, prefix, b, d, tail, m, refs):
  %lem_verify_map_keys_spec_rw(ks, 0n, nkeys) : {((_, K.all_dict_keys_used(nkeys, refs)), ((K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(nkeys, d) <> tail)), K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(b, d) <> (b <> tail)))), S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(K.all_dict_keys_used(nkeys, refs)))))) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}
  %lem_all_dict_used_spec_rw(nkeys, refs) : {((S.spec_strictly_asc_from(ks, 0n, nkeys), _), ((K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(nkeys, d) <> tail)), K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(b, d) <> (b <> tail)))), S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(_))))) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}
  %lem_oob_deep_false_rw(prefix, nkeys, d, tail, 0n) : {((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((_, K.verify_map_keys(nkeys, S.append_nat(prefix, S.nat_add(b, d) <> (b <> tail)))), S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(S.spec_all_lt_in(nkeys, refs)))))) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}
  %lem_unsorted_deep_false_rw(prefix, nkeys, b, d, tail, 0n) : {((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, _), S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(S.spec_all_lt_in(nkeys, refs)))))) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}
  pf_unused = Equal.sym(Bool, S.implies(S.nat_lt(m, nkeys), S.implies(S.bnot(S.nat_mem(refs, m)), S.bnot(S.spec_all_lt_in(nkeys, refs)))), True{}, lem_unused_key_blocks(nkeys, m, refs))
  %pf_unused : {((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), _)) == ((S.spec_strictly_asc_from(ks, 0n, nkeys), S.spec_all_lt_in(nkeys, refs)), ((False{}, False{}), True{})) : (Bool & Bool) & ((Bool & Bool) & Bool)}
  {==}

def Laws.L31_canon_float64_bijective_and_idempotent(f, _p):
  match f:
    case S.FPosZero{}:
      {==}
    case S.FNegZero{}:
      {==}
    case S.FFinite{_}:
      {==}
    case S.FInf{_}:
      {==}
    case S.FCanonQNaN{}:
      {==}
    case S.FNonCanonNaN{_}:
      {==}

def Laws.L32_json_sentinel_bijectivity_and_fail_closed(v, _r, _c, _a, _dt, _kk, _p):
  match v:
    case S.SVPlain{_}:
      {==}
    case S.SVBytes{_}:
      {==}
    case S.SVFloatSpecial{_}:
      {==}
    case S.SVTensor{_, _}:
      {==}
    case S.SVMapPlain{_}:
      {==}
    case S.SVMapReserved{_, _}:
      {==}

def lem_lookup_tensor_spec_rw(ts: List<&2, S.TensorDesc>, idx: Nat) -> {S.spec_lookup_tensor(ts, idx) == K.lookup_tensor(ts, idx) : Maybe<&2, S.TensorDesc>}:
  match ts idx:
    case Nil{} _:
      {==}
    case Con{_, _} 0n:
      {==}
    case Con{_, t} 1n+pi:
      lem_lookup_tensor_spec_rw(t, pi)

def lem_cons_resolved_spec_rw(idx: Nat, td_opt: Maybe<&2, S.TensorDesc>, tl_opt: Maybe<&2, List<&2, S.BoundTensor>>) -> {S.spec_cons_resolved(idx, td_opt, tl_opt) == K.cons_resolved(idx, td_opt, tl_opt) : Maybe<&2, List<&2, S.BoundTensor>>}:
  match td_opt tl_opt:
    case Some{_} Some{_}:
      {==}
    case Some{_} None{}:
      {==}
    case None{} _:
      {==}

def lem_resolve_refs_spec_rw(refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>) -> {S.spec_resolve_tensor_refs(refs, ts) == K.resolve_tensor_refs(refs, ts) : Maybe<&2, List<&2, S.BoundTensor>>}:
  match refs:
    case Nil{}:
      {==}
    case Con{+idx, +rest}:
      %lem_lookup_tensor_spec_rw(ts, idx) : {S.spec_cons_resolved(idx, S.spec_lookup_tensor(ts, idx), S.spec_resolve_tensor_refs(rest, ts)) == K.cons_resolved(idx, _, K.resolve_tensor_refs(rest, ts)) : Maybe<&2, List<&2, S.BoundTensor>>}
      %lem_resolve_refs_spec_rw(rest, ts) : {S.spec_cons_resolved(idx, S.spec_lookup_tensor(ts, idx), S.spec_resolve_tensor_refs(rest, ts)) == K.cons_resolved(idx, S.spec_lookup_tensor(ts, idx), _) : Maybe<&2, List<&2, S.BoundTensor>>}
      lem_cons_resolved_spec_rw(idx, S.spec_lookup_tensor(ts, idx), S.spec_resolve_tensor_refs(rest, ts))

def lem_wrap_rcid_spec_rw(fcid: Nat, bopt: Maybe<&2, List<&2, S.BoundTensor>>) -> {S.spec_wrap_rcid(fcid, bopt) == K.wrap_rcid(fcid, bopt) : Maybe<&2, S.ResolvedCid>}:
  match bopt:
    case None{}:
      {==}
    case Some{_}:
      {==}

def lem_resolved_cid_spec_rw(+fcid: Nat, +refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>) -> {S.spec_resolved_frame_cid(fcid, refs, ts) == K.resolved_frame_cid(fcid, refs, ts) : Maybe<&2, S.ResolvedCid>}:
  %lem_resolve_refs_spec_rw(refs, ts) : {S.spec_wrap_rcid(fcid, S.spec_resolve_tensor_refs(refs, ts)) == K.wrap_rcid(fcid, _) : Maybe<&2, S.ResolvedCid>}
  lem_wrap_rcid_spec_rw(fcid, S.spec_resolve_tensor_refs(refs, ts))

def lem_rcid_list_eq_refl_rw(xs: List<&2, S.BoundTensor>) -> {True{} == S.rcid_list_eq(xs, xs) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{S.BTensor{+i, S.TDesc{+l, +m}}, +rest}:
      %lem_nat_eq_refl_rw(i) : {True{} == S.band(_, S.band(S.nat_eq(l, l), S.band(S.nat_eq(m, m), S.rcid_list_eq(rest, rest)))) : Bool}
      %lem_nat_eq_refl_rw(l) : {True{} == S.band(_, S.band(S.nat_eq(m, m), S.rcid_list_eq(rest, rest))) : Bool}
      %lem_nat_eq_refl_rw(m) : {True{} == S.band(_, S.rcid_list_eq(rest, rest)) : Bool}
      lem_rcid_list_eq_refl_rw(rest)

def lookup_repack_prop(a: Maybe<&2, S.TensorDesc>, b: Maybe<&2, S.TensorDesc>) -> Bool:
  match a:
    case None{}:
      True{}
    case Some{S.TDesc{l1, m1}}:
      match b:
        case None{}:
          False{}
        case Some{S.TDesc{l2, m2}}:
          S.band(S.nat_eq(l1, l2), S.nat_eq(m1, m2))

def resolve_repack_prop(a: Maybe<&2, List<&2, S.BoundTensor>>, b: Maybe<&2, List<&2, S.BoundTensor>>) -> Bool:
  match a:
    case None{}:
      True{}
    case Some{xs}:
      match b:
        case None{}:
          False{}
        case Some{ys}:
          S.rcid_list_eq(xs, ys)

def lem_lookup_repack_ok(ts: List<&2, S.TensorDesc>, +extra: List<&2, S.TensorDesc>, idx: Nat) -> {True{} == lookup_repack_prop(K.lookup_tensor(ts, idx), K.lookup_tensor(S.append_tdesc(ts, extra), idx)) : Bool}:
  match ts idx:
    case Nil{} _:
      {==}
    case Con{S.TDesc{+l, +m}, _} 0n:
      %lem_nat_eq_refl_rw(l) : {True{} == S.band(_, S.nat_eq(m, m)) : Bool}
      lem_nat_eq_refl_rw(m)
    case Con{_, t} 1n+pi:
      lem_lookup_repack_ok(t, extra, pi)

def lem_band_split_rw(
  a: Bool, b: Bool, c: Bool,
  pab: {True{} == S.band(a, b) : Bool},
  pc: {True{} == c : Bool}
) -> {True{} == S.band(a, S.band(b, c)) : Bool}:
  match a b c:
    case True{} True{} True{}:
      {==}
    case True{} True{} False{}:
      pc
    case True{} False{} _:
      pab
    case False{} _ _:
      pab

def lem_resolve_repack_step(
  +idx: Nat,
  td1: Maybe<&2, S.TensorDesc>, td2: Maybe<&2, S.TensorDesc>,
  tl1: Maybe<&2, List<&2, S.BoundTensor>>, tl2: Maybe<&2, List<&2, S.BoundTensor>>,
  p_td: {True{} == lookup_repack_prop(td1, td2) : Bool},
  p_tl: {True{} == resolve_repack_prop(tl1, tl2) : Bool}
) -> {True{} == resolve_repack_prop(K.cons_resolved(idx, td1, tl1), K.cons_resolved(idx, td2, tl2)) : Bool}:
  match td1 td2 tl1 tl2:
    case None{} _ _ _:
      {==}
    case Some{_} _ None{} _:
      {==}
    case Some{S.TDesc{_, _}} None{} Some{_} _:
      p_td
    case Some{_} Some{_} Some{_} None{}:
      p_tl
    case Some{S.TDesc{+l1, +m1}} Some{S.TDesc{+l2, +m2}} Some{+xs} Some{+ys}:
      %lem_nat_eq_refl_rw(idx) : {True{} == S.band(_, S.band(S.nat_eq(l1, l2), S.band(S.nat_eq(m1, m2), S.rcid_list_eq(xs, ys)))) : Bool}
      lem_band_split_rw(S.nat_eq(l1, l2), S.nat_eq(m1, m2), S.rcid_list_eq(xs, ys), p_td, p_tl)

def lem_resolve_refs_append_ok(refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>, +extra: List<&2, S.TensorDesc>) -> {True{} == resolve_repack_prop(K.resolve_tensor_refs(refs, ts), K.resolve_tensor_refs(refs, S.append_tdesc(ts, extra))) : Bool}:
  match refs:
    case Nil{}:
      {==}
    case Con{+idx, +rest}:
      lem_resolve_repack_step(
        idx,
        K.lookup_tensor(ts, idx),
        K.lookup_tensor(S.append_tdesc(ts, extra), idx),
        K.resolve_tensor_refs(rest, ts),
        K.resolve_tensor_refs(rest, S.append_tdesc(ts, extra)),
        lem_lookup_repack_ok(ts, extra, idx),
        lem_resolve_refs_append_ok(rest, ts, extra)
      )

def lem_rcid_repack_from_prop(
  +fcid: Nat,
  tl1: Maybe<&2, List<&2, S.BoundTensor>>,
  tl2: Maybe<&2, List<&2, S.BoundTensor>>,
  p_tl: {True{} == resolve_repack_prop(tl1, tl2) : Bool}
) -> {True{} == S.implies(S.rcid_is_some(K.wrap_rcid(fcid, tl1)), S.rcid_eq(K.wrap_rcid(fcid, tl1), K.wrap_rcid(fcid, tl2))) : Bool}:
  match tl1 tl2:
    case None{} _:
      {==}
    case Some{_} None{}:
      p_tl
    case Some{+xs} Some{+ys}:
      %lem_nat_eq_refl_rw(fcid) : {True{} == S.band(_, S.rcid_list_eq(xs, ys)) : Bool}
      p_tl

def lem_rcid_repack_rw(+fcid: Nat, +refs: List<&2, Nat>, +ts: List<&2, S.TensorDesc>, +extra: List<&2, S.TensorDesc>) -> {True{} == S.implies(S.rcid_is_some(K.resolved_frame_cid(fcid, refs, ts)), S.rcid_eq(K.resolved_frame_cid(fcid, refs, ts), K.resolved_frame_cid(fcid, refs, S.append_tdesc(ts, extra)))) : Bool}:
  lem_rcid_repack_from_prop(
    fcid,
    K.resolve_tensor_refs(refs, ts),
    K.resolve_tensor_refs(refs, S.append_tdesc(ts, extra)),
    lem_resolve_refs_append_ok(refs, ts, extra)
  )

def lem_lookup_tensor_oob_rw(ts: List<&2, S.TensorDesc>, +d: Nat) -> {None{} == K.lookup_tensor(ts, S.nat_add(S.list_len_tdesc(ts), d)) : Maybe<&2, S.TensorDesc>}:
  match ts:
    case Nil{}:
      {==}
    case Con{_, t}:
      lem_lookup_tensor_oob_rw(t, d)

def lem_cons_resolved_none_r_rw(idx: Nat, td_opt: Maybe<&2, S.TensorDesc>) -> {None{} == K.cons_resolved(idx, td_opt, None{}) : Maybe<&2, List<&2, S.BoundTensor>>}:
  match td_opt:
    case None{}:
      {==}
    case Some{_}:
      {==}

def lem_resolve_refs_oob_rw(r_pre: List<&2, Nat>, +ts: List<&2, S.TensorDesc>, +d: Nat, +r_tail: List<&2, Nat>) -> {None{} == K.resolve_tensor_refs(S.append_nat(r_pre, S.nat_add(S.list_len_tdesc(ts), d) <> r_tail), ts) : Maybe<&2, List<&2, S.BoundTensor>>}:
  match r_pre:
    case Nil{}:
      %lem_lookup_tensor_oob_rw(ts, d) : {None{} == K.cons_resolved(S.nat_add(S.list_len_tdesc(ts), d), _, K.resolve_tensor_refs(r_tail, ts)) : Maybe<&2, List<&2, S.BoundTensor>>}
      {==}
    case Con{+idx, +rest}:
      %lem_resolve_refs_oob_rw(rest, ts, d, r_tail) : {None{} == K.cons_resolved(idx, K.lookup_tensor(ts, idx), _) : Maybe<&2, List<&2, S.BoundTensor>>}
      lem_cons_resolved_none_r_rw(idx, K.lookup_tensor(ts, idx))

def lem_lookup_tensor_hit_rw(pre: List<&2, S.TensorDesc>, +td: S.TensorDesc, +post: List<&2, S.TensorDesc>) -> {Some{td} == K.lookup_tensor(S.append_tdesc(pre, td <> post), S.list_len_tdesc(pre)) : Maybe<&2, S.TensorDesc>}:
  match pre:
    case Nil{}:
      {==}
    case Con{_, t}:
      lem_lookup_tensor_hit_rw(t, td, post)

def lem_l33_fcid_sens_rw(+fcid: Nat, +pre: List<&2, S.TensorDesc>, +l: Nat, +m: Nat, +post: List<&2, S.TensorDesc>, +d: Nat) -> {False{} == S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(1n+S.nat_add(fcid, d), [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post))) : Bool}:
  %lem_lookup_tensor_hit_rw(pre, S.TDesc{l, m}, post) : {False{} == S.rcid_eq(K.wrap_rcid(fcid, K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}})), K.wrap_rcid(1n+S.nat_add(fcid, d), K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}}))) : Bool}
  %lem_nat_eq_succ_add_false_rw(fcid, d) : {False{} == S.band(_, S.rcid_list_eq([S.BTensor{S.list_len_tdesc(pre), S.TDesc{l, m}}], [S.BTensor{S.list_len_tdesc(pre), S.TDesc{l, m}}])) : Bool}
  {==}

def lem_l33_len_sens_rw(+fcid: Nat, +pre: List<&2, S.TensorDesc>, +l: Nat, +m: Nat, +post: List<&2, S.TensorDesc>, +d: Nat) -> {False{} == S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))) : Bool}:
  %lem_lookup_tensor_hit_rw(pre, S.TDesc{l, m}, post) : {False{} == S.rcid_eq(K.wrap_rcid(fcid, K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}})), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))) : Bool}
  %lem_lookup_tensor_hit_rw(pre, S.TDesc{1n+S.nat_add(l, d), m}, post) : {False{} == S.rcid_eq(Some{S.RCid{fcid, [S.BTensor{S.list_len_tdesc(pre), S.TDesc{l, m}}]}}, K.wrap_rcid(fcid, K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}}))) : Bool}
  %lem_nat_eq_refl_rw(fcid) : {False{} == S.band(_, S.band(S.nat_eq(S.list_len_tdesc(pre), S.list_len_tdesc(pre)), S.band(S.nat_eq(l, 1n+S.nat_add(l, d)), S.band(S.nat_eq(m, m), True{})))) : Bool}
  %lem_nat_eq_refl_rw(S.list_len_tdesc(pre)) : {False{} == S.band(_, S.band(S.nat_eq(l, 1n+S.nat_add(l, d)), S.band(S.nat_eq(m, m), True{}))) : Bool}
  %lem_nat_eq_succ_add_false_rw(l, d) : {False{} == S.band(_, S.band(S.nat_eq(m, m), True{})) : Bool}
  {==}

def lem_l33_merkle_sens_rw(+fcid: Nat, +pre: List<&2, S.TensorDesc>, +l: Nat, +m: Nat, +post: List<&2, S.TensorDesc>, +d: Nat) -> {False{} == S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post))) : Bool}:
  %lem_lookup_tensor_hit_rw(pre, S.TDesc{l, m}, post) : {False{} == S.rcid_eq(K.wrap_rcid(fcid, K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}})), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post))) : Bool}
  %lem_lookup_tensor_hit_rw(pre, S.TDesc{l, 1n+S.nat_add(m, d)}, post) : {False{} == S.rcid_eq(Some{S.RCid{fcid, [S.BTensor{S.list_len_tdesc(pre), S.TDesc{l, m}}]}}, K.wrap_rcid(fcid, K.cons_resolved(S.list_len_tdesc(pre), _, Some{Nil{}}))) : Bool}
  %lem_nat_eq_refl_rw(fcid) : {False{} == S.band(_, S.band(S.nat_eq(S.list_len_tdesc(pre), S.list_len_tdesc(pre)), S.band(S.nat_eq(l, l), S.band(S.nat_eq(m, 1n+S.nat_add(m, d)), True{})))) : Bool}
  %lem_nat_eq_refl_rw(S.list_len_tdesc(pre)) : {False{} == S.band(_, S.band(S.nat_eq(l, l), S.band(S.nat_eq(m, 1n+S.nat_add(m, d)), True{}))) : Bool}
  %lem_nat_eq_refl_rw(l) : {False{} == S.band(_, S.band(S.nat_eq(m, 1n+S.nat_add(m, d)), True{})) : Bool}
  %lem_nat_eq_succ_add_false_rw(m, d) : {False{} == S.band(_, True{}) : Bool}
  {==}

def Laws.L33_resolved_frame_cid_repack_and_binding(fcid, refs, r_pre, r_tail, ts, pre, post, extra, l, m, d):
  %lem_resolved_cid_spec_rw(fcid, refs, ts) : {((_, S.implies(S.rcid_is_some(K.resolved_frame_cid(fcid, refs, ts)), S.rcid_eq(K.resolved_frame_cid(fcid, refs, ts), K.resolved_frame_cid(fcid, refs, S.append_tdesc(ts, extra))))), ((K.wrap_rcid(fcid, K.resolve_tensor_refs(S.append_nat(r_pre, S.nat_add(S.list_len_tdesc(ts), d) <> r_tail), ts)), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(1n+S.nat_add(fcid, d), [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)))), (S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  %lem_rcid_repack_rw(fcid, refs, ts, extra) : {((S.spec_resolved_frame_cid(fcid, refs, ts), _), ((K.wrap_rcid(fcid, K.resolve_tensor_refs(S.append_nat(r_pre, S.nat_add(S.list_len_tdesc(ts), d) <> r_tail), ts)), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(1n+S.nat_add(fcid, d), [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)))), (S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  %lem_resolve_refs_oob_rw(r_pre, ts, d, r_tail) : {((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((K.wrap_rcid(fcid, _), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(1n+S.nat_add(fcid, d), [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)))), (S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  %lem_l33_fcid_sens_rw(fcid, pre, l, m, post, d) : {((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, _), (S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{1n+S.nat_add(l, d), m} <> post))), S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  %lem_l33_len_sens_rw(fcid, pre, l, m, post, d) : {((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (_, S.rcid_eq(K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, m} <> post)), K.resolved_frame_cid(fcid, [S.list_len_tdesc(pre)], S.append_tdesc(pre, S.TDesc{l, 1n+S.nat_add(m, d)} <> post)))))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  %lem_l33_merkle_sens_rw(fcid, pre, l, m, post, d) : {((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, _))) == ((S.spec_resolved_frame_cid(fcid, refs, ts), True{}), ((None{}, False{}), (False{}, False{}))) : (Maybe<&2, S.ResolvedCid> & Bool) & ((Maybe<&2, S.ResolvedCid> & Bool) & (Bool & Bool))}
  {==}

# =============================================================================
# 4. GROUP 4 PROOFS: CWZM BINARY-SKIP, MERKLE, WAL, LIVEIVFSTORE & CWSP
# =============================================================================

def lem_min_le_l_rw(a: Nat, b: Nat) -> {True{} == S.nat_le(S.nat_min(a, b), a) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_min_le_l_rw(pa, pb)

def lem_min_le_r_rw(a: Nat, b: Nat) -> {True{} == S.nat_le(S.nat_min(a, b), b) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_min_le_r_rw(pa, pb)

def lem_le_max_l_rw(a: Nat, b: Nat) -> {True{} == S.nat_le(a, S.nat_max(a, b)) : Bool}:
  match a b:
    case 0n _:
      {==}
    case 1n+pa 0n:
      lem_nat_le_refl_rw(pa)
    case 1n+pa 1n+pb:
      lem_le_max_l_rw(pa, pb)

def lem_le_max_r_rw(a: Nat, b: Nat) -> {True{} == S.nat_le(b, S.nat_max(a, b)) : Bool}:
  match a b:
    case 0n b0:
      lem_nat_le_refl_rw(b0)
    case 1n+_ 0n:
      {==}
    case 1n+pa 1n+pb:
      lem_le_max_r_rw(pa, pb)

def lem_band_mono_bool(
  a1: Bool, a2: Bool, c1: Bool, c2: Bool,
  p1: {S.implies(a1, c1) == True{} : Bool},
  p2: {S.implies(a2, c2) == True{} : Bool}
) -> {S.implies(S.band(a1, a2), S.band(c1, c2)) == True{} : Bool}:
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

def lem_overlap_preserve(z: S.Zone, +k: Nat, +lo: Nat, +hi: Nat) -> {S.implies(K.zone_overlaps(z, lo, hi), K.zone_overlaps(K.zone_extend(z, k), lo, hi)) == True{} : Bool}:
  match z:
    case S.Zone{+mn, +mx}:
      lem_band_mono_bool(
        S.nat_le(lo, mx),
        S.nat_le(mn, hi),
        S.nat_le(lo, S.nat_max(mx, k)),
        S.nat_le(S.nat_min(mn, k), hi),
        lem_le_step_r(lo, mx, S.nat_max(mx, k), lem_le_max_l_rw(mx, k)),
        lem_le_step_l(S.nat_min(mn, k), mn, hi, lem_min_le_l_rw(mn, k))
      )

def lem_hit_gives_overlap(z: S.Zone, +k: Nat, +lo: Nat, +hi: Nat) -> {S.implies(S.spec_in_range(k, lo, hi), K.zone_overlaps(K.zone_extend(z, k), lo, hi)) == True{} : Bool}:
  match z:
    case S.Zone{+mn, +mx}:
      lem_band_mono_bool(
        S.nat_le(lo, k),
        S.nat_le(k, hi),
        S.nat_le(lo, S.nat_max(mx, k)),
        S.nat_le(S.nat_min(mn, k), hi),
        lem_le_step_r(lo, k, S.nat_max(mx, k), lem_le_max_r_rw(mx, k)),
        lem_le_step_l(S.nat_min(mn, k), k, hi, lem_min_le_r_rw(mn, k))
      )

def lem_bor_false_imp(a: Bool) -> {S.implies(S.bor(a, False{}), a) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_loop_step_bool(
  ov: Bool, hit_k: Bool, hit_rest: Bool, ov_next: Bool, ov_final: Bool,
  p_pres: {S.implies(ov, ov_next) == True{} : Bool},
  p_hit: {S.implies(hit_k, ov_next) == True{} : Bool},
  p_ih: {S.implies(S.bor(ov_next, hit_rest), ov_final) == True{} : Bool}
) -> {S.implies(S.bor(ov, S.bor(hit_k, hit_rest)), ov_final) == True{} : Bool}:
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

def lem_zone_loop_mono(ks: List<&2, Nat>, +z: S.Zone, +lo: Nat, +hi: Nat) -> {S.implies(S.bor(K.zone_overlaps(z, lo, hi), S.spec_list_has_in_range(ks, lo, hi)), K.zone_overlaps(K.build_zone_loop(ks, z), lo, hi)) == True{} : Bool}:
  match ks:
    case Nil{}:
      lem_bor_false_imp(K.zone_overlaps(z, lo, hi))
    case Con{+k, +rest}:
      lem_loop_step_bool(
        K.zone_overlaps(z, lo, hi),
        S.spec_in_range(k, lo, hi),
        S.spec_list_has_in_range(rest, lo, hi),
        K.zone_overlaps(K.zone_extend(z, k), lo, hi),
        K.zone_overlaps(K.build_zone_loop(rest, K.zone_extend(z, k)), lo, hi),
        lem_overlap_preserve(z, k, lo, hi),
        lem_hit_gives_overlap(z, k, lo, hi),
        lem_zone_loop_mono(rest, K.zone_extend(z, k), lo, hi)
      )

def Laws.L12_zonemap_no_false_neg(mn, mx, k0, ks, lo, hi):
  pf = Equal.sym(
    Bool,
    S.implies(S.spec_page_has_in_range(k0, ks, lo, hi), K.zone_overlaps(K.build_zone(k0, ks), lo, hi)),
    True{},
    lem_zone_loop_mono(ks, S.Zone{k0, k0}, lo, hi)
  )
  %pf : {(S.band(S.nat_le(lo, mx), S.nat_le(mn, hi)), _) == (S.band(S.nat_le(lo, mx), S.nat_le(mn, hi)), True{}) : Bool & Bool}
  {==}

# Multi-page CWZM `partition_point`-skip + early-break (`L23`)
def lem_disjoint_above_hi_bool(
  le_pm_mn: Bool, le_mn_mx: Bool, dj_rest: Bool, le_pm_hi: Bool, le_mn_hi: Bool, le_mx_hi: Bool, +ge_lo: Bool, +idx: Nat, +tail_list: List<&2, Nat>,
  p_pm_mn: {S.implies(le_pm_mn, S.implies(le_mn_hi, le_pm_hi)) == True{} : Bool},
  p_mn_mx: {S.implies(le_mn_mx, S.implies(le_mx_hi, le_mn_hi)) == True{} : Bool},
  p_ih: {S.implies(dj_rest, S.implies(S.bnot(le_mx_hi), S.nat_list_eq(Nil{}, tail_list))) == True{} : Bool}
) -> {S.implies(S.band(le_pm_mn, S.band(le_mn_mx, dj_rest)), S.implies(S.bnot(le_pm_hi), S.nat_list_eq(Nil{}, S.spec_push_if(S.band(ge_lo, le_mn_hi), idx, tail_list)))) == True{} : Bool}:
  match le_pm_mn le_mn_mx dj_rest le_pm_hi le_mn_hi le_mx_hi:
    case False{} _ _ _ _ _:
      {==}
    case True{} False{} _ _ _ _:
      {==}
    case True{} True{} False{} _ _ _:
      {==}
    case True{} True{} True{} True{} _ _:
      {==}
    case True{} True{} True{} False{} True{} _:
      lem_ex_falso_true(S.nat_list_eq(Nil{}, S.spec_push_if(S.band(ge_lo, True{}), idx, tail_list)), p_pm_mn)
    case True{} True{} True{} False{} False{} True{}:
      lem_ex_falso_true(S.nat_list_eq(Nil{}, S.spec_push_if(S.band(ge_lo, False{}), idx, tail_list)), p_mn_mx)
    case True{} True{} True{} False{} False{} False{}:
      %lem_band_false_r_rw(ge_lo) : {S.nat_list_eq(Nil{}, S.spec_push_if(_, idx, tail_list)) == True{} : Bool}
      p_ih

def lem_disjoint_above_hi_nil(zs: List<&2, S.Zone>, +idx: Nat, +prev_max: Nat, +lo: Nat, +hi: Nat) -> {S.implies(S.spec_zones_disjoint_from(zs, prev_max), S.implies(S.bnot(S.nat_le(prev_max, hi)), S.nat_list_eq(Nil{}, S.spec_filter_zones_at(zs, idx, lo, hi)))) == True{} : Bool}:
  match zs:
    case Nil{}:
      lem_implies_true_r(S.bnot(S.nat_le(prev_max, hi)))
    case Con{S.Zone{+mn, +mx}, +rest}:
      lem_disjoint_above_hi_bool(
        S.nat_le(prev_max, mn),
        S.nat_le(mn, mx),
        S.spec_zones_disjoint_from(rest, mx),
        S.nat_le(prev_max, hi),
        S.nat_le(mn, hi),
        S.nat_le(mx, hi),
        S.nat_le(lo, mx),
        idx,
        S.spec_filter_zones_at(rest, 1n+idx, lo, hi),
        lem_nat_le_trans(prev_max, mn, hi),
        lem_nat_le_trans(mn, mx, hi),
        lem_disjoint_above_hi_nil(rest, 1n+idx, mx, lo, hi)
      )

def lem_take_step_bool(
  le_pm_mn: Bool, le_mn_mx: Bool, dj_rest: Bool, le_lo_pm: Bool, le_lo_mn: Bool, +le_lo_mx: Bool, +le_mn_hi: Bool, le_mx_hi: Bool,
  +idx: Nat, +take_rest: List<&2, Nat>, +filt_rest: List<&2, Nat>,
  p_lo_mn: {S.implies(le_lo_pm, S.implies(le_pm_mn, le_lo_mn)) == True{} : Bool},
  p_lo_mx: {S.implies(le_lo_mn, S.implies(le_mn_mx, le_lo_mx)) == True{} : Bool},
  p_mn_mx: {S.implies(le_mn_mx, S.implies(le_mx_hi, le_mn_hi)) == True{} : Bool},
  p_nil: {S.implies(dj_rest, S.implies(S.bnot(le_mx_hi), S.nat_list_eq(Nil{}, filt_rest))) == True{} : Bool},
  p_ih: {S.implies(dj_rest, S.implies(le_lo_mx, S.nat_list_eq(take_rest, filt_rest))) == True{} : Bool}
) -> {S.implies(S.band(le_pm_mn, S.band(le_mn_mx, dj_rest)), S.implies(le_lo_pm, S.nat_list_eq(K.take_gt_branch(le_mn_hi, idx, take_rest), S.spec_push_if(S.band(le_lo_mx, le_mn_hi), idx, filt_rest)))) == True{} : Bool}:
  match le_pm_mn le_mn_mx dj_rest le_lo_pm le_lo_mn le_lo_mx le_mn_hi le_mx_hi:
    case False{} _ _ _ _ _ _ _:
      {==}
    case True{} False{} _ _ _ _ _ _:
      {==}
    case True{} True{} False{} _ _ _ _ _:
      {==}
    case True{} True{} True{} False{} _ _ _ _:
      {==}
    case True{} True{} True{} True{} False{} _ _ _:
      lem_ex_falso_true(S.nat_list_eq(K.take_gt_branch(le_mn_hi, idx, take_rest), S.spec_push_if(S.band(le_lo_mx, le_mn_hi), idx, filt_rest)), p_lo_mn)
    case True{} True{} True{} True{} True{} False{} _ _:
      lem_ex_falso_true(S.nat_list_eq(K.take_gt_branch(le_mn_hi, idx, take_rest), S.spec_push_if(False{}, idx, filt_rest)), p_lo_mx)
    case True{} True{} True{} True{} True{} True{} False{} True{}:
      lem_ex_falso_true(S.nat_list_eq(Nil{}, filt_rest), p_mn_mx)
    case True{} True{} True{} True{} True{} True{} False{} False{}:
      p_nil
    case True{} True{} True{} True{} True{} True{} True{} _:
      %lem_nat_eq_refl_rw(idx) : {S.band(_, S.nat_list_eq(take_rest, filt_rest)) == True{} : Bool}
      p_ih

def lem_take_until_eq_filter(zs: List<&2, S.Zone>, +idx: Nat, +prev_max: Nat, +lo: Nat, +hi: Nat) -> {S.implies(S.spec_zones_disjoint_from(zs, prev_max), S.implies(S.nat_le(lo, prev_max), S.nat_list_eq(K.take_until_gt_hi(zs, idx, hi), S.spec_filter_zones_at(zs, idx, lo, hi)))) == True{} : Bool}:
  match zs:
    case Nil{}:
      lem_implies_true_r(S.nat_le(lo, prev_max))
    case Con{S.Zone{+mn, +mx}, +rest}:
      lem_take_step_bool(
        S.nat_le(prev_max, mn),
        S.nat_le(mn, mx),
        S.spec_zones_disjoint_from(rest, mx),
        S.nat_le(lo, prev_max),
        S.nat_le(lo, mn),
        S.nat_le(lo, mx),
        S.nat_le(mn, hi),
        S.nat_le(mx, hi),
        idx,
        K.take_until_gt_hi(rest, 1n+idx, hi),
        S.spec_filter_zones_at(rest, 1n+idx, lo, hi),
        lem_nat_le_trans(lo, prev_max, mn),
        lem_nat_le_trans(lo, mn, mx),
        lem_nat_le_trans(mn, mx, hi),
        lem_disjoint_above_hi_nil(rest, 1n+idx, mx, lo, hi),
        lem_take_until_eq_filter(rest, 1n+idx, mx, lo, hi)
      )

def lem_qkr_step_bool(
  le_pm_mn: Bool, le_mn_mx: Bool, dj_rest: Bool, lt_mx_lo: Bool, +le_lo_mx: Bool, +le_mn_hi: Bool, le_mx_hi: Bool,
  +idx: Nat, +qkr_rest: List<&2, Nat>, +take_rest: List<&2, Nat>, +filt_rest: List<&2, Nat>,
  p_lt_nle: {S.implies(lt_mx_lo, S.bnot(le_lo_mx)) == True{} : Bool},
  p_nlt_le: {S.implies(S.bnot(lt_mx_lo), le_lo_mx) == True{} : Bool},
  p_mn_mx: {S.implies(le_mn_mx, S.implies(le_mx_hi, le_mn_hi)) == True{} : Bool},
  p_nil: {S.implies(dj_rest, S.implies(S.bnot(le_mx_hi), S.nat_list_eq(Nil{}, filt_rest))) == True{} : Bool},
  p_take: {S.implies(dj_rest, S.implies(le_lo_mx, S.nat_list_eq(take_rest, filt_rest))) == True{} : Bool},
  p_ih: {S.implies(dj_rest, S.nat_list_eq(qkr_rest, filt_rest)) == True{} : Bool}
) -> {S.implies(S.band(le_pm_mn, S.band(le_mn_mx, dj_rest)), S.nat_list_eq(K.qkr_branch(lt_mx_lo, qkr_rest, K.take_gt_branch(le_mn_hi, idx, take_rest)), S.spec_push_if(S.band(le_lo_mx, le_mn_hi), idx, filt_rest))) == True{} : Bool}:
  match le_pm_mn le_mn_mx dj_rest lt_mx_lo le_lo_mx le_mn_hi le_mx_hi:
    case False{} _ _ _ _ _ _:
      {==}
    case True{} False{} _ _ _ _ _:
      {==}
    case True{} True{} False{} _ _ _ _:
      {==}
    case True{} True{} True{} True{} True{} _ _:
      lem_ex_falso_true(S.nat_list_eq(qkr_rest, S.spec_push_if(le_mn_hi, idx, filt_rest)), p_lt_nle)
    case True{} True{} True{} True{} False{} _ _:
      p_ih
    case True{} True{} True{} False{} False{} _ _:
      lem_ex_falso_true(S.nat_list_eq(K.take_gt_branch(le_mn_hi, idx, take_rest), filt_rest), p_nlt_le)
    case True{} True{} True{} False{} True{} False{} True{}:
      lem_ex_falso_true(S.nat_list_eq(Nil{}, filt_rest), p_mn_mx)
    case True{} True{} True{} False{} True{} False{} False{}:
      p_nil
    case True{} True{} True{} False{} True{} True{} _:
      %lem_nat_eq_refl_rw(idx) : {S.band(_, S.nat_list_eq(take_rest, filt_rest)) == True{} : Bool}
      p_take

def lem_qkr_eq_filter(zs: List<&2, S.Zone>, +idx: Nat, +prev_max: Nat, +lo: Nat, +hi: Nat) -> {S.implies(S.spec_zones_disjoint_from(zs, prev_max), S.nat_list_eq(K.query_key_range_at(zs, idx, lo, hi), S.spec_filter_zones_at(zs, idx, lo, hi))) == True{} : Bool}:
  match zs:
    case Nil{}:
      {==}
    case Con{S.Zone{+mn, +mx}, +rest}:
      lem_qkr_step_bool(
        S.nat_le(prev_max, mn),
        S.nat_le(mn, mx),
        S.spec_zones_disjoint_from(rest, mx),
        S.nat_lt(mx, lo),
        S.nat_le(lo, mx),
        S.nat_le(mn, hi),
        S.nat_le(mx, hi),
        idx,
        K.query_key_range_at(rest, 1n+idx, lo, hi),
        K.take_until_gt_hi(rest, 1n+idx, hi),
        S.spec_filter_zones_at(rest, 1n+idx, lo, hi),
        lem_lt_implies_not_le(mx, lo),
        lem_not_lt_implies_le(mx, lo),
        lem_nat_le_trans(mn, mx, hi),
        lem_disjoint_above_hi_nil(rest, 1n+idx, mx, lo, hi),
        lem_take_until_eq_filter(rest, 1n+idx, mx, lo, hi),
        lem_qkr_eq_filter(rest, 1n+idx, mx, lo, hi)
      )

def lem_l23_wrap(le_lh: Bool, +zs: List<&2, S.Zone>, +lo: Nat, +hi: Nat) -> {S.implies(S.band(le_lh, S.spec_zones_disjoint_from(zs, 0n)), S.nat_list_eq(K.qkr_le_branch(le_lh, K.query_key_range_at(zs, 0n, lo, hi)), S.spec_filter_zones_at(zs, 0n, lo, hi))) == True{} : Bool}:
  match le_lh:
    case False{}:
      {==}
    case True{}:
      lem_qkr_eq_filter(zs, 0n, 0n, lo, hi)

def lem_l23_inv_bool(lt_hl: Bool, le_lh: Bool, +res: List<&2, Nat>, p: {S.implies(lt_hl, S.bnot(le_lh)) == True{} : Bool}) -> {True{} == S.implies(lt_hl, S.nat_list_eq(K.qkr_le_branch(le_lh, res), Nil{})) : Bool}:
  match lt_hl le_lh:
    case False{} _:
      {==}
    case True{} False{}:
      {==}
    case True{} True{}:
      Equal.sym(Bool, S.nat_list_eq(res, Nil{}), True{}, lem_ex_falso_true(S.nat_list_eq(res, Nil{}), p))

def Laws.L23_sorted_zonemap_early_break_exact(zs, lo, hi):
  p1 = Equal.sym(Bool, S.implies(S.band(S.nat_le(lo, hi), S.spec_zones_disjoint_from(zs, 0n)), S.nat_list_eq(K.query_key_range_pages(zs, lo, hi), S.spec_filter_zones_at(zs, 0n, lo, hi))), True{}, lem_l23_wrap(S.nat_le(lo, hi), zs, lo, hi))
  p2 = lem_l23_inv_bool(S.nat_lt(hi, lo), S.nat_le(lo, hi), K.query_key_range_at(zs, 0n, lo, hi), lem_lt_implies_not_le(hi, lo))
  %p1 : {(_, S.implies(S.nat_lt(hi, lo), S.nat_list_eq(K.query_key_range_pages(zs, lo, hi), Nil{}))) == (True{}, True{}) : Bool & Bool}
  %p2 : {(True{}, _) == (True{}, True{}) : Bool & Bool}
  {==}

# UNIVERSAL RFC 6962 `flatten_leaves(merkle_tree_digest(xs)) == flat_map_leaves(xs)` across ALL lengths `0..∞`!
def lem_append_dig_nil_r_rw(xs: List<&2, S.Digest>) -> {xs == S.append_dig(xs, Nil{}) : List<&2, S.Digest>}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      %lem_append_dig_nil_r_rw(t) : {h <> t == h <> _ : List<&2, S.Digest>}
      {==}

def lem_append_dig_assoc_rw(a: List<&2, S.Digest>, +b: List<&2, S.Digest>, +c: List<&2, S.Digest>) -> {S.append_dig(a, S.append_dig(b, c)) == S.append_dig(S.append_dig(a, b), c) : List<&2, S.Digest>}:
  match a:
    case Nil{}:
      {==}
    case Con{h, t}:
      %lem_append_dig_assoc_rw(t, b, c) : {h <> S.append_dig(t, S.append_dig(b, c)) == h <> _ : List<&2, S.Digest>}
      {==}

def lem_list_len_d_spec_rw(xs: List<&2, S.Digest>) -> {S.list_len_dig(xs) == K.list_len_d(xs) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{_, rest}:
      %lem_list_len_d_spec_rw(rest) : {1n+S.list_len_dig(rest) == 1n+_ : Nat}
      {==}

def lem_pair_len_le(xs: List<&2, S.Digest>) -> {S.nat_le(K.list_len_d(K.merkle_pair_level(xs)), K.list_len_d(xs)) == True{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{_, Nil{}}:
      {==}
    case Con{_, Con{_, +rest}}:
      lem_le_inc_r(K.list_len_d(K.merkle_pair_level(rest)), K.list_len_d(rest), lem_pair_len_le(rest))

def lem_flat_pair_rw(xs: List<&2, S.Digest>) -> {S.flat_map_leaves(xs) == S.flat_map_leaves(K.merkle_pair_level(xs)) : List<&2, S.Digest>}:
  match xs:
    case Nil{}:
      {==}
    case Con{_, Nil{}}:
      {==}
    case Con{+a, Con{+b, +rest}}:
      %lem_flat_pair_rw(rest) : {S.append_dig(S.flatten_leaves(a), S.append_dig(S.flatten_leaves(b), S.flat_map_leaves(rest))) == S.append_dig(S.append_dig(S.flatten_leaves(a), S.flatten_leaves(b)), _) : List<&2, S.Digest>}
      lem_append_dig_assoc_rw(S.flatten_leaves(a), S.flatten_leaves(b), S.flat_map_leaves(rest))

def pick_digs(b: Bool, x: List<&2, S.Digest>, y: List<&2, S.Digest>) -> List<&2, S.Digest>:
  match b:
    case True{}:
      x
    case False{}:
      y

def lem_absurd_digs(
  x: List<&2, S.Digest>,
  +y: List<&2, S.Digest>,
  h_inv: {False{} == True{} : Bool}
) -> {x == y : List<&2, S.Digest>}:
  %h_inv : {pick_digs(_, x, y) == y : List<&2, S.Digest>}
  {==}

def lem_flatten_reduce_rw(
  +fuel: Nat, xs: List<&2, S.Digest>,
  h_ge: {S.nat_le(K.list_len_d(xs), fuel) == True{} : Bool}
) -> {S.flat_map_leaves(xs) == S.flatten_leaves(K.merkle_reduce_fuel(fuel, xs)) : List<&2, S.Digest>}:
  match fuel xs:
    case _ Nil{}:
      {==}
    case _ Con{+only, Nil{}}:
      Equal.sym(List<&2, S.Digest>, S.flatten_leaves(only), S.append_dig(S.flatten_leaves(only), Nil{}), lem_append_dig_nil_r_rw(S.flatten_leaves(only)))
    case 0n Con{+a, Con{+b, +rest}}:
      lem_absurd_digs(
        S.flat_map_leaves(a <> (b <> rest)),
        S.flatten_leaves(K.merkle_reduce_fuel(0n, a <> (b <> rest))),
        h_ge
      )
    case 1n+pf Con{+a, Con{+b, +rest}}:
      h_sub = lem_mp(
        S.nat_le(1n+K.list_len_d(rest), pf),
        S.nat_le(1n+K.list_len_d(K.merkle_pair_level(rest)), pf),
        h_ge,
        lem_le_step_l(
          1n+K.list_len_d(K.merkle_pair_level(rest)),
          1n+K.list_len_d(rest),
          pf,
          Equal.sym(Bool, S.nat_le(K.list_len_d(K.merkle_pair_level(rest)), K.list_len_d(rest)), True{}, lem_pair_len_le(rest))
        )
      )
      %lem_flatten_reduce_rw(pf, S.HNode01{a, b} <> K.merkle_pair_level(rest), h_sub) : {S.append_dig(S.flatten_leaves(a), S.append_dig(S.flatten_leaves(b), S.flat_map_leaves(rest))) == _ : List<&2, S.Digest>}
      %lem_flat_pair_rw(rest) : {S.append_dig(S.flatten_leaves(a), S.append_dig(S.flatten_leaves(b), S.flat_map_leaves(rest))) == S.append_dig(S.append_dig(S.flatten_leaves(a), S.flatten_leaves(b)), _) : List<&2, S.Digest>}
      lem_append_dig_assoc_rw(S.flatten_leaves(a), S.flatten_leaves(b), S.flat_map_leaves(rest))

def Laws.L13_merkle_domain_and_odd_safe(xs, f1, f2, t0, trest, a, b, _c):
  %lem_flatten_reduce_rw(K.list_len_d(xs), xs, lem_nat_le_refl(K.list_len_d(xs))) : {(((_, K.shard_merkle_root(f1, Nil{})), (K.shard_merkle_root(f2, t0 <> trest), False{})), (S.band(S.band(S.nat_eq(a, a), S.nat_eq(b, b)), False{}), (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}
  %lem_list_len_d_spec_rw(f1) : {(((S.flat_map_leaves(xs), S.HRoot02{_, K.merkle_tree_digest(f1)}), (S.HRoot04{K.list_len_d(f2), K.merkle_tree_digest(f2), 1n+K.list_len_d(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (S.band(S.band(S.nat_eq(a, a), S.nat_eq(b, b)), False{}), (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}
  %lem_list_len_d_spec_rw(f2) : {(((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{_, K.merkle_tree_digest(f2), 1n+K.list_len_d(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (S.band(S.band(S.nat_eq(a, a), S.nat_eq(b, b)), False{}), (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}
  %lem_list_len_d_spec_rw(trest) : {(((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+_, K.merkle_tree_digest(t0 <> trest)}, False{})), (S.band(S.band(S.nat_eq(a, a), S.nat_eq(b, b)), False{}), (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}
  %lem_band_false_r_rw(S.band(S.nat_eq(a, a), S.nat_eq(b, b))) : {(((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (_, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) == (((S.flat_map_leaves(xs), S.HRoot02{S.list_len_dig(f1), K.merkle_tree_digest(f1)}), (S.HRoot04{S.list_len_dig(f2), K.merkle_tree_digest(f2), 1n+S.list_len_dig(trest), K.merkle_tree_digest(t0 <> trest)}, False{})), (False{}, (S.HEmpty{}, (S.HLeaf00{a}, S.HNode01{S.HLeaf00{a}, S.HLeaf00{b}})))) : ((List<&2, S.Digest> & S.Digest) & (S.Digest & Bool)) & (Bool & (S.Digest & (S.Digest & S.Digest)))}
  {==}

def lem_l34_tensor_sens(+fs: List<&2, S.Digest>, +bl: Nat, +m4m: Nat, +d: Nat) -> {False{} == S.spec_digest_eq(K.shard_merkle_root(fs, [S.HTensor03{bl, m4m}]), K.shard_merkle_root(fs, [S.HTensor03{bl, 1n+S.nat_add(m4m, d)}])) : Bool}:
  %lem_nat_eq_succ_add_false_rw(m4m, d) : {False{} == S.band(S.nat_eq(K.list_len_d(fs), K.list_len_d(fs)), S.band(S.spec_digest_eq(K.merkle_tree_digest(fs), K.merkle_tree_digest(fs)), S.band(True{}, S.band(S.nat_eq(bl, bl), _)))) : Bool}
  %lem_band_false_r_rw(S.nat_eq(bl, bl)) : {False{} == S.band(S.nat_eq(K.list_len_d(fs), K.list_len_d(fs)), S.band(S.spec_digest_eq(K.merkle_tree_digest(fs), K.merkle_tree_digest(fs)), S.band(True{}, _))) : Bool}
  lem_band3_false_r_rw(S.nat_eq(K.list_len_d(fs), K.list_len_d(fs)), S.spec_digest_eq(K.merkle_tree_digest(fs), K.merkle_tree_digest(fs)))

def Laws.L34_merkle_leaf_and_tensor_sensitivity(a, b, d, bl, m4m, fs):
  %lem_nat_eq_succ_add_false_rw(a, d) : {((S.band(_, S.nat_eq(b, b)), S.band(S.nat_eq(a, a), S.nat_eq(b, 1n+S.nat_add(b, d)))), (_, S.spec_digest_eq(K.shard_merkle_root(fs, [S.HTensor03{bl, m4m}]), K.shard_merkle_root(fs, [S.HTensor03{bl, 1n+S.nat_add(m4m, d)}])))) == ((False{}, False{}), (False{}, False{})) : (Bool & Bool) & (Bool & Bool)}
  %lem_nat_eq_succ_add_false_rw(b, d) : {((False{}, S.band(S.nat_eq(a, a), _)), (False{}, S.spec_digest_eq(K.shard_merkle_root(fs, [S.HTensor03{bl, m4m}]), K.shard_merkle_root(fs, [S.HTensor03{bl, 1n+S.nat_add(m4m, d)}])))) == ((False{}, False{}), (False{}, False{})) : (Bool & Bool) & (Bool & Bool)}
  %lem_band_false_r_rw(S.nat_eq(a, a)) : {((False{}, _), (False{}, S.spec_digest_eq(K.shard_merkle_root(fs, [S.HTensor03{bl, m4m}]), K.shard_merkle_root(fs, [S.HTensor03{bl, 1n+S.nat_add(m4m, d)}])))) == ((False{}, False{}), (False{}, False{})) : (Bool & Bool) & (Bool & Bool)}
  %lem_l34_tensor_sens(fs, bl, m4m, d) : {((False{}, False{}), (False{}, _)) == ((False{}, False{}), (False{}, False{})) : (Bool & Bool) & (Bool & Bool)}
  {==}

# Complete `.cwwal` recovery proofs (`L14`)
def lem_wal_prepend_spec_rw(p: Nat, r: S.WalResult) -> {S.spec_wal_prepend(p, r) == K.wal_prepend(p, r) : S.WalResult}:
  match r:
    case S.WalOk{_}:
      {==}
    case S.WalErr{}:
      {==}

def lem_wal_seq_branch_spec_rw(ok: Bool, p: Nat, r: S.WalResult) -> {S.spec_wal_seq_branch(ok, p, r) == K.wal_seq_branch(ok, p, r) : S.WalResult}:
  match ok:
    case False{}:
      {==}
    case True{}:
      lem_wal_prepend_spec_rw(p, r)

def lem_wal_torn_branch_spec_rw(m: Bool) -> {S.spec_wal_torn_branch(m) == K.wal_torn_branch(m) : S.WalResult}:
  match m:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_wal_recover_spec_rw(recs: List<&2, S.WalRec>, +s: Nat, +mode: Bool) -> {S.spec_wal_recover_mode(recs, s, mode) == K.wal_recover_mode(recs, s, mode) : S.WalResult}:
  match recs:
    case Nil{}:
      {==}
    case Con{S.WalRec{True{}, +seq, +p}, +rest}:
      %lem_wal_recover_spec_rw(rest, 1n+s, mode) : {S.spec_wal_seq_branch(S.nat_eq(seq, s), p, S.spec_wal_recover_mode(rest, 1n+s, mode)) == K.wal_seq_branch(S.nat_eq(seq, s), p, _) : S.WalResult}
      lem_wal_seq_branch_spec_rw(S.nat_eq(seq, s), p, S.spec_wal_recover_mode(rest, 1n+s, mode))
    case Con{S.WalRec{False{}, _, _}, Nil{}}:
      lem_wal_torn_branch_spec_rw(mode)
    case Con{S.WalRec{False{}, _, _}, Con{_, _}}:
      {==}

def lem_wal_clean_rw(prefix: List<&2, Nat>, +s: Nat, +mode: Bool) -> {S.WalOk{prefix} == K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, s, Nil{}), s, mode) : S.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_nat_eq_refl_rw(s) : {S.WalOk{p <> rest} == K.wal_seq_branch(_, p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, Nil{}), 1n+s, mode)) : S.WalResult}
      %lem_wal_clean_rw(rest, 1n+s, mode) : {S.WalOk{p <> rest} == K.wal_prepend(p, _) : S.WalResult}
      {==}

def lem_wal_torn_ok_rw(prefix: List<&2, Nat>, +s: Nat, +torn_seq: Nat, +tp: Nat) -> {S.WalOk{prefix} == K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, s, [S.WalRec{False{}, torn_seq, tp}]), s, True{}) : S.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_nat_eq_refl_rw(s) : {S.WalOk{p <> rest} == K.wal_seq_branch(_, p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, [S.WalRec{False{}, torn_seq, tp}]), 1n+s, True{})) : S.WalResult}
      %lem_wal_torn_ok_rw(rest, 1n+s, torn_seq, tp) : {S.WalOk{p <> rest} == K.wal_prepend(p, _) : S.WalResult}
      {==}

def lem_wal_torn_strict_rw(prefix: List<&2, Nat>, +s: Nat, +torn_seq: Nat, +tp: Nat) -> {S.WalErr{} == K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, s, [S.WalRec{False{}, torn_seq, tp}]), s, False{}) : S.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_nat_eq_refl_rw(s) : {S.WalErr{} == K.wal_seq_branch(_, p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, [S.WalRec{False{}, torn_seq, tp}]), 1n+s, False{})) : S.WalResult}
      %lem_wal_torn_strict_rw(rest, 1n+s, torn_seq, tp) : {S.WalErr{} == K.wal_prepend(p, _) : S.WalResult}
      {==}

def lem_wal_mid_err_rw(prefix: List<&2, Nat>, +s: Nat, +torn_seq: Nat, +bp: Nat, +after: S.WalRec, +more: List<&2, S.WalRec>, +mode: Bool) -> {S.WalErr{} == K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, s, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), s, mode) : S.WalResult}:
  match prefix:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %lem_nat_eq_refl_rw(s) : {S.WalErr{} == K.wal_seq_branch(_, p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), 1n+s, mode)) : S.WalResult}
      %lem_wal_mid_err_rw(rest, 1n+s, torn_seq, bp, after, more, mode) : {S.WalErr{} == K.wal_prepend(p, _) : S.WalResult}
      {==}

def lem_add_zero_r_rw(+a: Nat) -> {a == S.nat_add(a, 0n) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %lem_add_zero_r_rw(p) : {1n+p == 1n+_ : Nat}
      {==}

def lem_wal_is_err_prepend_rw(p: Nat, r: S.WalResult) -> {S.spec_wal_is_err(r) == S.spec_wal_is_err(K.wal_prepend(p, r)) : Bool}:
  match r:
    case S.WalOk{_}:
      {==}
    case S.WalErr{}:
      {==}

def lem_wal_bad_seq_nil(eq: Bool, tp: Nat) -> {True{} == S.implies(S.bnot(eq), S.spec_wal_is_err(K.wal_seq_branch(eq, tp, S.WalOk{Nil{}}))) : Bool}:
  match eq:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_wal_bad_seq_err_rw(prefix: List<&2, Nat>, +s: Nat, +bad_seq: Nat, +tp: Nat, +mode: Bool) -> {True{} == S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(s, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, s, [S.WalRec{True{}, bad_seq, tp}]), s, mode))) : Bool}:
  match prefix:
    case Nil{}:
      %lem_add_zero_r_rw(s) : {True{} == S.implies(S.bnot(S.nat_eq(bad_seq, _)), S.spec_wal_is_err(K.wal_seq_branch(S.nat_eq(bad_seq, s), tp, S.WalOk{Nil{}}))) : Bool}
      lem_wal_bad_seq_nil(S.nat_eq(bad_seq, s), tp)
    case Con{+p, +rest}:
      %lem_nat_eq_refl_rw(s) : {True{} == S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(s, 1n+S.list_len_nat(rest)))), S.spec_wal_is_err(K.wal_seq_branch(_, p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, [S.WalRec{True{}, bad_seq, tp}]), 1n+s, mode)))) : Bool}
      %lem_wal_is_err_prepend_rw(p, K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, [S.WalRec{True{}, bad_seq, tp}]), 1n+s, mode)) : {True{} == S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(s, 1n+S.list_len_nat(rest)))), _) : Bool}
      %lem_add_succ_r_rw(s, S.list_len_nat(rest)) : {True{} == S.implies(S.bnot(S.nat_eq(bad_seq, _)), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(rest, 1n+s, [S.WalRec{True{}, bad_seq, tp}]), 1n+s, mode))) : Bool}
      lem_wal_bad_seq_err_rw(rest, 1n+s, bad_seq, tp, mode)

def Laws.L14_wal_torn_vs_mid_tamper(recs, prefix, start_seq, torn_seq, bad_seq, tp, bp, after, more, mode):
  %lem_wal_recover_spec_rw(recs, start_seq, mode) : {((_, K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, Nil{}), start_seq, mode)), ((K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, True{}), K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, False{})), (K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), start_seq, mode), S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  %lem_wal_clean_rw(prefix, start_seq, mode) : {((S.spec_wal_recover_mode(recs, start_seq, mode), _), ((K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, True{}), K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, False{})), (K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), start_seq, mode), S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  %lem_wal_torn_ok_rw(prefix, start_seq, torn_seq, tp) : {((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((_, K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{False{}, torn_seq, tp}]), start_seq, False{})), (K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), start_seq, mode), S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  %lem_wal_torn_strict_rw(prefix, start_seq, torn_seq, tp) : {((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, _), (K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, S.WalRec{False{}, torn_seq, bp} <> (after <> more)), start_seq, mode), S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  %lem_wal_mid_err_rw(prefix, start_seq, torn_seq, bp, after, more, mode) : {((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (_, S.implies(S.bnot(S.nat_eq(bad_seq, S.nat_add(start_seq, S.list_len_nat(prefix)))), S.spec_wal_is_err(K.wal_recover_mode(S.spec_wal_valid_prefix(prefix, start_seq, [S.WalRec{True{}, bad_seq, tp}]), start_seq, mode)))))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  %lem_wal_bad_seq_err_rw(prefix, start_seq, bad_seq, tp, mode) : {((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, _))) == ((S.spec_wal_recover_mode(recs, start_seq, mode), S.WalOk{prefix}), ((S.WalOk{prefix}, S.WalErr{}), (S.WalErr{}, True{}))) : (S.WalResult & S.WalResult) & ((S.WalResult & S.WalResult) & (S.WalResult & Bool))}
  {==}

# Universal sequential WAL replay & StatePage runtime (`L24`)
def lem_wal_replay_hom_rw(ops1: List<&2, S.WalOp>, +ops2: List<&2, S.WalOp>, +live: List<&2, Nat>) -> {K.wal_replay_live(ops2, K.wal_replay_live(ops1, live)) == K.wal_replay_live(S.append_ops(ops1, ops2), live) : List<&2, Nat>}:
  match ops1:
    case Nil{}:
      {==}
    case Con{S.OpFrame{+c}, +rest}:
      lem_wal_replay_hom_rw(rest, ops2, S.append_nat(K.remove_cid(live, c), [c]))
    case Con{S.OpTomb{+c}, +rest}:
      lem_wal_replay_hom_rw(rest, ops2, K.remove_cid(live, c))

def lem_remove_mem_self_step(
  eq: Bool, +x: Nat, +rest: List<&2, Nat>, +c: Nat,
  h_eq: {eq == S.nat_eq(x, c) : Bool},
  ih: {False{} == S.nat_mem(K.remove_cid(rest, c), c) : Bool}
) -> {False{} == S.nat_mem(K.remove_cid_keep(eq, x, K.remove_cid(rest, c)), c) : Bool}:
  match eq:
    case True{}:
      ih
    case False{}:
      %h_eq : {False{} == S.bor(_, S.nat_mem(K.remove_cid(rest, c), c)) : Bool}
      ih

def lem_remove_mem_self_rw(xs: List<&2, Nat>, +c: Nat) -> {False{} == S.nat_mem(K.remove_cid(xs, c), c) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, +rest}:
      lem_remove_mem_self_step(S.nat_eq(x, c), x, rest, c, {==}, lem_remove_mem_self_rw(rest, c))

def lem_remove_count_self_step(
  eq: Bool, +x: Nat, +rest: List<&2, Nat>, +c: Nat,
  h_eq: {eq == S.nat_eq(x, c) : Bool},
  ih: {0n == S.nat_count(K.remove_cid(rest, c), c) : Nat}
) -> {0n == S.nat_count(K.remove_cid_keep(eq, x, K.remove_cid(rest, c)), c) : Nat}:
  match eq:
    case True{}:
      ih
    case False{}:
      %h_eq : {0n == S.nat_count_step(_, S.nat_count(K.remove_cid(rest, c), c)) : Nat}
      ih

def lem_remove_count_self_rw(xs: List<&2, Nat>, +c: Nat) -> {0n == S.nat_count(K.remove_cid(xs, c), c) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, +rest}:
      lem_remove_count_self_step(S.nat_eq(x, c), x, rest, c, {==}, lem_remove_count_self_rw(rest, c))

def lem_count_step_add_rw(eq: Bool, n: Nat) -> {1n+S.nat_count_step(eq, n) == S.nat_count_step(eq, 1n+n) : Nat}:
  match eq:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_count_append_self_rw(xs: List<&2, Nat>, +c: Nat) -> {1n+S.nat_count(xs, c) == S.nat_count(S.append_nat(xs, [c]), c) : Nat}:
  match xs:
    case Nil{}:
      %lem_nat_eq_refl_rw(c) : {1n == S.nat_count_step(_, 0n) : Nat}
      {==}
    case Con{+x, +rest}:
      %lem_count_append_self_rw(rest, c) : {1n+S.nat_count_step(S.nat_eq(x, c), S.nat_count(rest, c)) == S.nat_count_step(S.nat_eq(x, c), _) : Nat}
      lem_count_step_add_rw(S.nat_eq(x, c), S.nat_count(rest, c))

def lem_frame_count_one_rw(+xs: List<&2, Nat>, +c: Nat) -> {1n == S.nat_count(S.append_nat(K.remove_cid(xs, c), [c]), c) : Nat}:
  %lem_count_append_self_rw(K.remove_cid(xs, c), c) : {1n == _ : Nat}
  %lem_remove_count_self_rw(xs, c) : {1n == 1n+_ : Nat}
  {==}

def lem_nat_eq_trans_contra(x: Nat, b: Nat, c: Nat) -> {S.implies(S.bnot(S.nat_eq(b, c)), S.implies(S.nat_eq(x, c), S.bnot(S.nat_eq(x, b)))) == True{} : Bool}:
  match x b c:
    case 0n 0n 0n:
      {==}
    case 0n 0n 1n+_:
      {==}
    case 0n 1n+_ 0n:
      {==}
    case 0n 1n+pb 1n+pc:
      lem_implies_true_r(S.bnot(S.nat_eq(pb, pc)))
    case 1n+_ 0n 0n:
      {==}
    case 1n+px 0n 1n+pc:
      lem_implies_true_r(S.nat_eq(px, pc))
    case 1n+_ 1n+_ 0n:
      {==}
    case 1n+px 1n+pb 1n+pc:
      lem_nat_eq_trans_contra(px, pb, pc)

def lem_remove_other_step(
  eq_bc: Bool, eq_xc: Bool, eq_xb: Bool, mr: Bool, mb: Bool,
  h_contra: {S.implies(S.bnot(eq_bc), S.implies(eq_xc, S.bnot(eq_xb))) == True{} : Bool},
  ih: {S.implies(S.bnot(eq_bc), S.bool_eq(mr, mb)) == True{} : Bool},
  +x: Nat, +b: Nat, +rest: List<&2, Nat>, +c: Nat,
  h_eq_xb: {eq_xb == S.nat_eq(x, b) : Bool},
  h_mr: {mr == S.nat_mem(K.remove_cid(rest, c), b) : Bool}
) -> {S.implies(S.bnot(eq_bc), S.bool_eq(S.nat_mem(K.remove_cid_keep(eq_xc, x, K.remove_cid(rest, c)), b), S.bor(eq_xb, mb))) == True{} : Bool}:
  match eq_bc eq_xc eq_xb mr mb:
    case True{} _ _ _ _:
      {==}
    case False{} True{} True{} _ _:
      lem_ex_falso_true(S.bool_eq(S.nat_mem(K.remove_cid(rest, c), b), True{}), h_contra)
    case False{} True{} False{} _ _:
      %h_mr : {S.bool_eq(_, mb) == True{} : Bool}
      ih
    case False{} False{} True{} _ _:
      %h_eq_xb : {S.bool_eq(S.bor(_, S.nat_mem(K.remove_cid(rest, c), b)), True{}) == True{} : Bool}
      {==}
    case False{} False{} False{} _ _:
      %h_eq_xb : {S.bool_eq(S.bor(_, S.nat_mem(K.remove_cid(rest, c), b)), mb) == True{} : Bool}
      %h_mr : {S.bool_eq(_, mb) == True{} : Bool}
      ih

def lem_remove_other_mem(xs: List<&2, Nat>, +b: Nat, +c: Nat) -> {S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.remove_cid(xs, c), b), S.nat_mem(xs, b))) == True{} : Bool}:
  match xs:
    case Nil{}:
      lem_implies_true_r(S.bnot(S.nat_eq(b, c)))
    case Con{+x, +rest}:
      lem_remove_other_step(
        S.nat_eq(b, c),
        S.nat_eq(x, c),
        S.nat_eq(x, b),
        S.nat_mem(K.remove_cid(rest, c), b),
        S.nat_mem(rest, b),
        lem_nat_eq_trans_contra(x, b, c),
        lem_remove_other_mem(rest, b, c),
        x, b, rest, c, {==}, {==}
      )

def lem_mem_append_single_rw(xs: List<&2, Nat>, +c: Nat, +b: Nat) -> {S.bor(S.nat_mem(xs, b), S.nat_eq(c, b)) == S.nat_mem(S.append_nat(xs, [c]), b) : Bool}:
  match xs:
    case Nil{}:
      lem_bor_false_r_rw(S.nat_eq(c, b))
    case Con{+x, +rest}:
      %lem_mem_append_single_rw(rest, c, b) : {S.bor(S.bor(S.nat_eq(x, b), S.nat_mem(rest, b)), S.nat_eq(c, b)) == S.bor(S.nat_eq(x, b), _) : Bool}
      Equal.sym(Bool, S.bor(S.nat_eq(x, b), S.bor(S.nat_mem(rest, b), S.nat_eq(c, b))), S.bor(S.bor(S.nat_eq(x, b), S.nat_mem(rest, b)), S.nat_eq(c, b)), lem_bor_assoc_rw(S.nat_eq(x, b), S.nat_mem(rest, b), S.nat_eq(c, b)))

def lem_frame_other_bool(eq_bc: Bool, m1: Bool, m2: Bool, h: {S.implies(S.bnot(eq_bc), S.bool_eq(m1, m2)) == True{} : Bool}) -> {S.implies(S.bnot(eq_bc), S.bool_eq(S.bor(m1, eq_bc), m2)) == True{} : Bool}:
  match eq_bc:
    case True{}:
      {==}
    case False{}:
      %lem_bor_false_r_rw(m1) : {S.bool_eq(_, m2) == True{} : Bool}
      h

def lem_frame_other_mem(+xs: List<&2, Nat>, +b: Nat, +c: Nat) -> {S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(S.append_nat(K.remove_cid(xs, c), [c]), b), S.nat_mem(xs, b))) == True{} : Bool}:
  %lem_mem_append_single_rw(K.remove_cid(xs, c), c, b) : {S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(_, S.nat_mem(xs, b))) == True{} : Bool}
  %lem_nat_eq_sym_rw(b, c) : {S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.bor(S.nat_mem(K.remove_cid(xs, c), b), _), S.nat_mem(xs, b))) == True{} : Bool}
  lem_frame_other_bool(S.nat_eq(b, c), S.nat_mem(K.remove_cid(xs, c), b), S.nat_mem(xs, b), lem_remove_other_mem(xs, b, c))

def lem_sp_rt_spec_rw(exp_rt: Maybe<&2, Nat>, file_rt: Maybe<&2, Nat>) -> {S.spec_sp_runtime(exp_rt, file_rt) == K.check_statepage_runtime(exp_rt, file_rt) : Bool}:
  match exp_rt file_rt:
    case None{} _:
      {==}
    case Some{_} None{}:
      {==}
    case Some{_} Some{_}:
      {==}

def Laws.L24_wal_tombstone_reinsert_and_statepage_rt(ops1, ops2, live, c, b, exp_rt, file_rt):
  +l1 = K.wal_replay_live(ops1, live)
  %lem_wal_replay_hom_rw(ops1, ops2, live) : {((_, S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpTomb{c}]), live), c)), ((S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpTomb{c}]), live), b), S.nat_mem(l1, b))), S.nat_count(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %lem_wal_replay_hom_rw(ops1, [S.OpTomb{c}], live) : {((K.wal_replay_live(ops2, l1), S.nat_mem(_, c)), ((S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(_, b), S.nat_mem(l1, b))), S.nat_count(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.wal_replay_live(S.append_ops(ops1, [S.OpFrame{c}]), live), b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %lem_wal_replay_hom_rw(ops1, [S.OpFrame{c}], live) : {((K.wal_replay_live(ops2, l1), S.nat_mem(K.remove_cid(l1, c), c)), ((S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.remove_cid(l1, c), b), S.nat_mem(l1, b))), S.nat_count(_, c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(_, b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %lem_remove_mem_self_rw(l1, c) : {((K.wal_replay_live(ops2, l1), _), ((S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.remove_cid(l1, c), b), S.nat_mem(l1, b))), S.nat_count(S.append_nat(K.remove_cid(l1, c), [c]), c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(S.append_nat(K.remove_cid(l1, c), [c]), b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %Equal.sym(Bool, S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(K.remove_cid(l1, c), b), S.nat_mem(l1, b))), True{}, lem_remove_other_mem(l1, b, c)) : {((K.wal_replay_live(ops2, l1), False{}), ((_, S.nat_count(S.append_nat(K.remove_cid(l1, c), [c]), c)), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(S.append_nat(K.remove_cid(l1, c), [c]), b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %lem_frame_count_one_rw(l1, c) : {((K.wal_replay_live(ops2, l1), False{}), ((True{}, _), (S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(S.append_nat(K.remove_cid(l1, c), [c]), b), S.nat_mem(l1, b))), K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %Equal.sym(Bool, S.implies(S.bnot(S.nat_eq(b, c)), S.bool_eq(S.nat_mem(S.append_nat(K.remove_cid(l1, c), [c]), b), S.nat_mem(l1, b))), True{}, lem_frame_other_mem(l1, b, c)) : {((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (_, K.check_statepage_runtime(exp_rt, file_rt)))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  %lem_sp_rt_spec_rw(exp_rt, file_rt) : {((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, _))) == ((K.wal_replay_live(ops2, l1), False{}), ((True{}, 1n), (True{}, S.spec_sp_runtime(exp_rt, file_rt)))) : (List<&2, Nat> & Bool) & ((Bool & Nat) & (Bool & Bool))}
  {==}

# `LiveIvfStore` crash-safe compaction watermark proofs (`L35`, Fix F10)
def lem_ivf_make_append_rw(a: List<&2, Nat>, +b: List<&2, Nat>, +s0: Nat) -> {S.append_ivf(S.ivf_make_seq_recs(a, s0), S.ivf_make_seq_recs(b, S.nat_add(s0, S.list_len_nat(a)))) == S.ivf_make_seq_recs(S.append_nat(a, b), s0) : List<&2, S.IvfWalRec>}:
  match a:
    case Nil{}:
      %lem_add_zero_r_rw(s0) : {S.ivf_make_seq_recs(b, _) == S.ivf_make_seq_recs(b, s0) : List<&2, S.IvfWalRec>}
      {==}
    case Con{+v, +rest}:
      %lem_add_succ_r_rw(s0, S.list_len_nat(rest)) : {S.IvfRec{s0, v} <> S.append_ivf(S.ivf_make_seq_recs(rest, 1n+s0), S.ivf_make_seq_recs(b, _)) == S.IvfRec{s0, v} <> S.ivf_make_seq_recs(S.append_nat(rest, b), 1n+s0) : List<&2, S.IvfWalRec>}
      %lem_ivf_make_append_rw(rest, b, 1n+s0) : {S.IvfRec{s0, v} <> S.append_ivf(S.ivf_make_seq_recs(rest, 1n+s0), S.ivf_make_seq_recs(b, 1n+S.nat_add(s0, S.list_len_nat(rest)))) == S.IvfRec{s0, v} <> _ : List<&2, S.IvfWalRec>}
      {==}

def lem_ivf_push_append_rw(ge: Bool, +v: Nat, +xs: List<&2, Nat>, +ys: List<&2, Nat>) -> {S.append_nat(K.ivf_push_if(ge, v, xs), ys) == K.ivf_push_if(ge, v, S.append_nat(xs, ys)) : List<&2, Nat>}:
  match ge:
    case True{}:
      {==}
    case False{}:
      {==}

def lem_ivf_filter_append_rw(xs: List<&2, S.IvfWalRec>, +ys: List<&2, S.IvfWalRec>, +w: Nat) -> {S.append_nat(K.ivf_wal_filter_from(xs, w), K.ivf_wal_filter_from(ys, w)) == K.ivf_wal_filter_from(S.append_ivf(xs, ys), w) : List<&2, Nat>}:
  match xs:
    case Nil{}:
      {==}
    case Con{S.IvfRec{+s, +v}, +rest}:
      %lem_ivf_filter_append_rw(rest, ys, w) : {S.append_nat(K.ivf_push_if(S.nat_le(w, s), v, K.ivf_wal_filter_from(rest, w)), K.ivf_wal_filter_from(ys, w)) == K.ivf_push_if(S.nat_le(w, s), v, _) : List<&2, Nat>}
      lem_ivf_push_append_rw(S.nat_le(w, s), v, K.ivf_wal_filter_from(rest, w), K.ivf_wal_filter_from(ys, w))

def lem_ivf_filter_all_ge_rw(ids: List<&2, Nat>, +s: Nat, +w: Nat, +h: {S.nat_le(w, s) == True{} : Bool}) -> {ids == K.ivf_wal_filter_from(S.ivf_make_seq_recs(ids, s), w) : List<&2, Nat>}:
  match ids:
    case Nil{}:
      {==}
    case Con{+v, +rest}:
      %Equal.sym(Bool, S.nat_le(w, s), True{}, h) : {v <> rest == K.ivf_push_if(_, v, K.ivf_wal_filter_from(S.ivf_make_seq_recs(rest, 1n+s), w)) : List<&2, Nat>}
      %lem_ivf_filter_all_ge_rw(rest, 1n+s, w, lem_le_inc_r(w, s, h)) : {v <> rest == v <> _ : List<&2, Nat>}
      {==}

def lem_nat_add_assoc_rw(+a: Nat, +b: Nat, +c: Nat) -> {S.nat_add(a, S.nat_add(b, c)) == S.nat_add(S.nat_add(a, b), c) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %lem_nat_add_assoc_rw(p, b, c) : {1n+S.nat_add(p, S.nat_add(b, c)) == 1n+_ : Nat}
      {==}

def lem_ivf_filter_all_lt_exact_rw(ids: List<&2, Nat>, +s: Nat, +extra: Nat) -> {Nil{} == K.ivf_wal_filter_from(S.ivf_make_seq_recs(ids, s), S.nat_add(S.nat_add(s, S.list_len_nat(ids)), extra)) : List<&2, Nat>}:
  match ids:
    case Nil{}:
      {==}
    case Con{+v, +rest}:
      %lem_add_succ_r_rw(s, S.list_len_nat(rest)) : {Nil{} == K.ivf_push_if(S.nat_le(S.nat_add(_, extra), s), v, K.ivf_wal_filter_from(S.ivf_make_seq_recs(rest, 1n+s), S.nat_add(_, extra))) : List<&2, Nat>}
      %lem_nat_add_assoc_rw(s, S.list_len_nat(rest), extra) : {Nil{} == K.ivf_push_if(S.nat_le(1n+_, s), v, K.ivf_wal_filter_from(S.ivf_make_seq_recs(rest, 1n+s), 1n+S.nat_add(S.nat_add(s, S.list_len_nat(rest)), extra))) : List<&2, Nat>}
      %lem_le_succ_add_false_rw(s, S.nat_add(S.list_len_nat(rest), extra)) : {Nil{} == K.ivf_push_if(_, v, K.ivf_wal_filter_from(S.ivf_make_seq_recs(rest, 1n+s), 1n+S.nat_add(S.nat_add(s, S.list_len_nat(rest)), extra))) : List<&2, Nat>}
      lem_ivf_filter_all_lt_exact_rw(rest, 1n+s, extra)

def lem_ivf_crash_rw(+base: List<&2, Nat>, +comp_ids: List<&2, Nat>, +post_ids: List<&2, Nat>, +s0: Nat) -> {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == K.live_ivf_recover(S.append_nat(base, comp_ids), S.nat_add(s0, S.list_len_nat(comp_ids)), S.ivf_make_seq_recs(S.append_nat(comp_ids, post_ids), s0)) : List<&2, Nat>}:
  +w = S.nat_add(s0, S.list_len_nat(comp_ids))
  %lem_ivf_make_append_rw(comp_ids, post_ids, s0) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), K.ivf_wal_filter_from(_, w)) : List<&2, Nat>}
  %lem_ivf_filter_append_rw(S.ivf_make_seq_recs(comp_ids, s0), S.ivf_make_seq_recs(post_ids, w), w) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), _) : List<&2, Nat>}
  %Equal.sym(Nat, w, S.nat_add(w, 0n), lem_add_zero_r_rw(w)) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), S.append_nat(K.ivf_wal_filter_from(S.ivf_make_seq_recs(comp_ids, s0), _), K.ivf_wal_filter_from(S.ivf_make_seq_recs(post_ids, w), w))) : List<&2, Nat>}
  %lem_ivf_filter_all_lt_exact_rw(comp_ids, s0, 0n) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), S.append_nat(_, K.ivf_wal_filter_from(S.ivf_make_seq_recs(post_ids, w), w))) : List<&2, Nat>}
  %lem_ivf_filter_all_ge_rw(post_ids, w, w, lem_nat_le_refl(w)) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), _) : List<&2, Nat>}
  lem_append_nat_assoc_rw(base, comp_ids, post_ids)

def lem_ivf_post_rw(+base: List<&2, Nat>, +comp_ids: List<&2, Nat>, +post_ids: List<&2, Nat>, +s0: Nat) -> {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == K.live_ivf_recover(S.append_nat(base, comp_ids), S.nat_add(s0, S.list_len_nat(comp_ids)), S.ivf_make_seq_recs(post_ids, S.nat_add(s0, S.list_len_nat(comp_ids)))) : List<&2, Nat>}:
  +w = S.nat_add(s0, S.list_len_nat(comp_ids))
  %lem_ivf_filter_all_ge_rw(post_ids, w, w, lem_nat_le_refl(w)) : {S.append_nat(base, S.append_nat(comp_ids, post_ids)) == S.append_nat(S.append_nat(base, comp_ids), _) : List<&2, Nat>}
  lem_append_nat_assoc_rw(base, comp_ids, post_ids)

def Laws.L35_live_ivf_compaction_crash_invariant(base, comp_ids, post_ids, s0):
  +exp = S.append_nat(base, S.append_nat(comp_ids, post_ids))
  +w = S.nat_add(s0, S.list_len_nat(comp_ids))
  %lem_ivf_filter_all_ge_rw(S.append_nat(comp_ids, post_ids), s0, s0, lem_nat_le_refl(s0)) : {(S.append_nat(base, _), (K.live_ivf_recover(S.append_nat(base, comp_ids), w, S.ivf_make_seq_recs(S.append_nat(comp_ids, post_ids), s0)), K.live_ivf_recover(S.append_nat(base, comp_ids), w, S.ivf_make_seq_recs(post_ids, w)))) == (exp, (exp, exp)) : List<&2, Nat> & (List<&2, Nat> & List<&2, Nat>)}
  %lem_ivf_crash_rw(base, comp_ids, post_ids, s0) : {(exp, (_, K.live_ivf_recover(S.append_nat(base, comp_ids), w, S.ivf_make_seq_recs(post_ids, w)))) == (exp, (exp, exp)) : List<&2, Nat> & (List<&2, Nat> & List<&2, Nat>)}
  %lem_ivf_post_rw(base, comp_ids, post_ids, s0) : {(exp, (exp, _)) == (exp, (exp, exp)) : List<&2, Nat> & (List<&2, Nat> & List<&2, Nat>)}
  {==}

def Laws.L36_statepage_hmac_precedence(_k, mv, hv):
  match mv hv:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}
