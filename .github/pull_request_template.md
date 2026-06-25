## Summary

-

## Test plan

- [ ] **Cross-language gates:** `bash tools/run_all_gates.sh` (the 11-gate v1 conformance suite — CI runs this)
- [ ] Native suites: `cd go && go test ./...` · `cd rust && cargo test` · `cd typescript && npm test`
- [ ] Python reference: `pytest tools/cowrie_ref/tests`

## Wire-format impact

- [ ] No wire-format change
- [ ] If the wire/canonical form changed: golden regenerated (`python -m cowrie_ref gen`), all 4 langs
      re-verified, and the coverage floor (`tools/count_guard.py`) bumped — never lowered
- [ ] Cross-language byte-identity + content-address/fingerprint parity confirmed green
