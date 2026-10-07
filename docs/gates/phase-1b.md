# Phase 1b gate review: counting process and baseline

- Date: 2026-10-06 (round 16). Reviewer: the assistant; sign-off: the maintainer (D-24).
- Exit criterion (§13): the parity gate passed for (start, stop] data and left truncation; the
  per-stratum baseline cumulative hazard and survival; prediction of the linear predictor, relative
  hazard, cumulative hazard and survival.
- Specification: [counting-process-baseline.md](../spec/cox/counting-process-baseline.md) (D-23).

## Evidence

| Requirement (§9.8) | Status | Evidence |
|---|---|---|
| Function-level, lockstep and end-to-end parity of fits with entry times | Met | `CoxPHSuite` on lt-stratified and lt-weights-offset, both tie methods, against pprof_py and R |
| Baseline parity | Met | Against pprof_py's raw and public baselines at its estimates, worst T-base ratio 3.2e-6; against R's `basehaz` end to end, 3.8e-8; six fixtures, both tie methods |
| Prediction parity | Met | Cumulative hazard 3.2e-6, survival 8.5e-7, relative hazard 1.6e-8 and linear predictor 0 of T-base, on pprof_py's grids; right-continuous steps between grid points |
| Metamorphic and edge cases | Met | Entry 0 equals right-censored fits bit for bit; interval splitting; shifting every time below zero is bitwise neutral; invalid entries, unknown strata and invalid prediction inputs fail with counts |
| Reproducibility | Met | R0 bitwise for fits and the baseline; R1 within T-part; Classic and Connect, JDK 17 and 21 |
| API-3 and DIST-1 | Met | `CoxPH.baseline` checks the data fingerprint; the baseline table and predictions stay distributed |
| Persistence | Met | Format version 2 saves the baseline as Parquet and round-trips it bit for bit; version 1 loads |
| Documentation | Met | Specification, parity matrix, X-014 |
| Scale test | **Not met** | As in Phase 1a: no cluster (D-14), targets after benchmarks (D-04); OI-43 (unstratified prediction) is part of it |
| CI | Met | Rounds 11 to 14 green. Round 15's Spark Connect job failed: one CoxBaselineSuite test passed munit's 30-second limit (run 37533252860). Round 16.1 splits the long tests and gives Spark suites two minutes |

## Outcome

**Closed on 2026-10-06** (D-24): round 16.1's CI passed and the maintainer signed off.

## Recommendation

Close Phase 1b at parity-verified on the same terms as Phase 1a (D-22): the features stay
`Experimental`, and their scale verification moves to the Phase 1d gate (D-24, needs the
maintainer's sign-off). Phase 1c starts with the specification `docs/spec/cox/residuals-robust.md`.
