# Status

Updated 2026-10-06, round 17.

**Phase 0 closed on 2026-10-06** (docs/gates/phase-0.md). **Phase 1a, Cox estimation core: closed** (D-22; CI green through round 14).
**Phase 1b, counting process and baseline: closed** (D-24). **Phase 1c, residuals and robust variance:
in progress** (D-25): residuals done; robust variance next.

## Phase 0 exit criteria (§4, as amended by D-11 and D-14)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the platform skeleton, under Classic Spark and Spark Connect | Met | Run 37458038911 (`main` at 7f31717): every job passed; the test jobs counted engine 34, numerics 29, testkit 6 |
| Every spike recorded as an ADR | Met with round 7 | S-02 local: ADR-0002; S-05: ADR-0006; S-06: ADR-0007 (trial pending); deferred spikes: ADR-0008 |
| Phase 0 decisions resolved | Met | D-01 to D-17 are approved or delegated (2026-10-06) |

## Rounds

| Round | Scope | Status |
|---|---|---|
| 1 | Repository bootstrap | Merged (434ce01); tests failed in CI (OI-21) |
| 1.1 | Classic test-classpath fix, public CI failure details, LICENSE | Merged (0c0bdd8); CI green, run 37325078166 |
| 2 | Engine suites under Spark Connect (layer T8), test counts, ADR-0002 | Merged (c89fcee). Run 37351537662: tests and T8 passed; the linkage compile failed (OI-28) |
| 3 | Maintainer decisions (D-05, D-10, D-11, D-13, D-14); reference pinned to pprof_py v0.7.0; T8 required; linkage fix | Merged (35f5a7c); CI green, run 37361715287 |
| 4 | `numerics`: deterministic summation and the StrictMath rule (ADR-0003) | Merged (08b378f); CI green, run 37361715287 (40 tests per JDK) |
| 5 | Platform skeleton: data contract, layout plan, working set, kernels, ordered reduction, result table, persistence (ADR-0004) | Merged (7f31717); CI green, run 37458038911 |
| 6 | Reference fixtures for Cox (pprof_py v0.7.0, R 4.3.3, survival 3.5-8), calibration, R-comparison triage (ADR-0005) | Merged (0be6e54); CI green, run 37477285324 |
| 7 | S-05 (ADR-0006), S-06 (ADR-0007), deferred spikes (ADR-0008), Phase 0 gate review | Merged (d47532c); CI green, run 37484837090; Phase 0 closed |
| 8 | PROJECT_CONTEXT v2.2: Phase 0 decisions and review fixes folded in | Merged (3565b66); CI green, run 37487451760 |
| 9 | Phase 1a: Cox specification for the first slice (D-20) | Merged (697e5df); approved 2026-10-06 |
| 10 | First Cox slice: numerics (normal distribution, Cholesky, Newton, Breslow kernel), engine estimator, lockstep fixtures, parity tests | Merged (099fefe); CI green, run 37502839703 |
| 11 | Efron ties, case weights and offsets (D-21, X-013) | Merged (f619e9b); CI green |
| 12 | `CoxFit` persistence (OI-37), information matrix in the result, warning logging (OI-36), Phase 1a gate review (D-22) | Merged (094fd5b); CI green |
| 13 | Phase 1b specification: (start, stop] data, left truncation, baseline hazard, prediction (D-23, X-014) | Merged (f7aed80); approved 2026-10-06 |
| 14 | Phase 1b fixtures (left truncation, baselines, predictions) and fitting with entry times | Merged (ac55216); CI green |
| 15 | Baseline hazard table, prediction, and persisting the baseline (format version 2) | Merged (1453442); CI: the Spark Connect job failed on a test timeout (run 37533252860), fixed in round 16.1 |
| 16 | Phase 1b gate review (D-24) and the Phase 1c specification (D-25, X-015) | Merged with 16.1 (04ea163); signed off and approved |
| 16.1 | Fix round 15's CI failure: one test per fixture case and tie method in the long Cox tests; Spark suites allow two minutes per test | Merged (04ea163); CI green |
| 17 | Phase 1c fixtures (residuals, robust variances) and residuals | Delivered as a patch; CI to confirm |
| 18 | Robust variance, per row and clustered; Phase 1c gate review | Planned |

## Decisions and spikes

See the table above and DECISIONS.md. S-02's local part is complete; the remaining spikes are
listed under the exit criteria.
