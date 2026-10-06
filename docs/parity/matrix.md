# Parity matrix (PROJECT_CONTEXT §9.8)

Status values: planned, implemented, parity-verified, scale-verified, stable.

| Feature | Specification | Reference functions | Fixtures | Tests | Tolerance classes | Status |
|---|---|---|---|---|---|---|
| Cox PH, first slice: stratified, right-censored, Breslow, model-based variance | [cox/first-slice.md](../spec/cox/first-slice.md) (approved, D-20) | `CoxPH`, `cox_partial_likelihood`, `newton_raphson`; R `coxph` | `fixtures/cox`: tiny-ties, rc-unstratified, rc-stratified | `pprof.spark.engine.cox.CoxPH`; tests `CoxPHSuite` | T-fn, T-iter, T-coef, T-var, T-test, T-p, T-part | parity in the sandbox (round 10); worst ratios: T-coef 0.995 (tiny-ties vs pprof_py, X-010) and 1.2e-7 vs R, T-fn 0.011, T-iter 2.1e-4, T-var 2.9e-4, T-test 1.3e-7, T-part 1.6e-7; bitwise identical across JDK 17 and 21 and Classic and Connect (R2) |

The CI check that fails when a public API has no entry here is not implemented yet (OI-15).
Reference fixtures for the Phase 1a Cox features are in `fixtures/cox`; their tolerance
calibration is in [cox-calibration.md](cox-calibration.md).
