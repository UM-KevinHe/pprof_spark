# Parity matrix (PROJECT_CONTEXT §9.8)

Status values: planned, implemented, parity-verified, scale-verified, stable.

| Feature | Specification | Reference functions | Fixtures | Tests | Tolerance classes | Status |
|---|---|---|---|---|---|---|
| Cox PH, first slice: stratified, right-censored, Breslow, model-based variance | [cox/first-slice.md](../spec/cox/first-slice.md) (draft, D-20) | `CoxPH`, `cox_partial_likelihood`, `newton_raphson`; R `coxph` | `fixtures/cox`: tiny-ties, rc-unstratified, rc-stratified | Not written | T-fn, T-iter, T-coef, T-var, T-p | planned |

The CI check that fails when a public API has no entry here is not implemented yet (OI-15).
Reference fixtures for the Phase 1a Cox features are in `fixtures/cox`; their tolerance
calibration is in [cox-calibration.md](cox-calibration.md).
