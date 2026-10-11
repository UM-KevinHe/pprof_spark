# Phase 3 gate review: linear fixed effects

- Date: 2026-10-11 (round 74). Reviewer: the assistant; sign-off: the maintainer (D-48, proposed).
- Exit criterion (PROJECT_CONTEXT §4): the parity gate passed for the profile (within) estimation, standardization
  and inference of pprof_py's `LinearFixedEffectModel`.
- Plan: [plan.md](../spec/linear/plan.md) (D-45), slices 3a to 3c. Scale verification is the maintainer's, once the
  package is done (D-28).

## Evidence by slice

The parity suites were run again for this review in the sandbox (round 74: Classic Spark, JDK 17); every test
passed. Each suite also passed under Spark Connect in its own round.

| Slice | Specification | Fixtures and calibration | Parity tests | Status |
|---|---|---|---|---|
| 3a, estimation and covariate inference | D-45, with X-031 to X-033; X-034 | `fixtures/linear`, six cases (rounds 63 and 68); [linear-calibration.md](../parity/linear-calibration.md) | numerics `LinearKernelSuite` (against exact arithmetic) and `StudentTSuite` (against mpmath, 1 to 10⁹ degrees of freedom); `LinearFESuite` 8/8 (end to end against pprof_py and R's `lm`, bitwise invariance to row order and partitioning, edge cases); `LinearBehaviourSuite` 5/5 (metamorphic, block sizes); `LinearFitIOSuite` 3/3 (bitwise round trip) | Met, with one difference below |
| 3b, provider tests, intervals, standardized differences | D-46, with X-035 and X-036 | `inference_*.json` for six cases (round 68); [linear-inference-calibration.md](../parity/linear-inference-calibration.md) | `LinearProviderTestsSuite` 6/6 (against base R and pprof_py; every provider tested on lin-extreme); `LinearStandardizationSuite` 3/3 and `LinearStandardizationMoreSuite` 4/4 (against base R, pprof_py and R pprof; API-3) | Met |
| 3c, job runner and Python | D-47 | lin-base | `LinearJobSuite` 2/2 (every output equal to the library's bit for bit); a `PythonApiSuite` test (the facade equal to the engine bit for bit); `python/tests/test_linear.py` 4 (against the fixtures) | Met |

The difference in 3a: spec 3a §11 compares the within cross-products with the fixtures' NumPy values at function
level. They are verified instead against exact BigDecimal arithmetic (`LinearKernelSuite`, a stronger check on
synthetic data) and through the end-to-end estimates (β̂ within 6.4e-5 of T-coef); the fixtures' function-level
values are calibrated against R (`calibrate.py`, within T-fn). D-48 proposes accepting this.

## Measured worst ratios

A ratio is the observed difference over the allowed one under the class's element-wise rule (PROJECT_CONTEXT §8.4),
so 1 is at the tolerance. The six linear engine suites compute 3,544 ratios and none exceeds 1.

| Slice | Suite | Worst ratios |
|---|---|---|
| 3a | `LinearFESuite` | T-fn 0.0496 (lin-shifted), T-test 2.5e-4, T-meas 8.7e-5, T-coef 6.4e-5, T-var 5.2e-7 |
| 3a | `LinearBehaviourSuite` | T-part.parameters 4.2e-6, T-part.objective 0 |
| 3b | `LinearProviderTestsSuite` | T-test 7.3e-4, T-coef 6.0e-6 |
| 3b | `LinearStandardizationSuite` | T-meas 0.887 (lin-shifted; lin-base 7.2e-6, lin-singletons 1.7e-7) |
| 3b | `LinearStandardizationMoreSuite` | T-meas 9.4e-6 |

The largest, T-meas 0.887, is lin-shifted's (features near ±10⁴), where the measures are differences of sums of
γ₀ + xᵢᵀβ̂ with xᵢᵀβ̂ near 10⁴·β, so rounding in the sums dominates. On the same case the references disagree among
themselves by up to 0.504 of T-meas (R pprof's indirect expected values against pprof_py's; base R and pprof_py within
7.1e-4), so the ratio lies within the references' own spread. It is the narrowest margin in Phase 3.

Method: the copy of the testkit that prints every ratio `Tolerance.worstRatio` and `Tolerance.accepts` compute
(round 60, sandbox only); the suites ran unchanged against it. `LinearFitIOSuite`, `LinearJobSuite` and the Python
facade compare bit for bit and compute no ratios.

## Cross-cutting requirements

| Requirement | Status | Evidence |
|---|---|---|
| Specifications approved (NN-2) | Met | D-45, D-46, D-47 |
| Fixtures and calibration (§9.3) | Met | Rounds 63 and 68; `FixturesSuite` checks them and their negative controls (testkit 23/23 under Classic Spark and Spark Connect, round 68); every calibration passes |
| Three parity levels (§9.2) | Met, with 3a's difference above | Function level: `StudentT` against mpmath, the kernel against exact arithmetic. Lockstep: not applicable, the estimator does not iterate (spec 3a §11). End to end: every slice |
| Discrepancies (PAR-3, ROAD-2) | Met | Every discrepancy affecting Phase 3 has an approved decision: X-031 to X-033 (D-45), X-034 (round 64), X-035 and X-036 (D-46). The class B items, X-031 and X-035, are corrected behavior with a documented divergence; their upstream reports wait with OI-33, as Phase 2's did |
| Metamorphic and edge cases (§9.4, §9.5) | Met | Bitwise invariance to row order and partitioning; y + c, a scaled feature, duplicated records, block sizes within T-part; aliased features (X-032), n − m − p ≤ 0 (X-033), string keys and `score` on them (X-034), singletons, an underflowing t tail (X-035), unknown providers, the API-3 refusal |
| Persistence | Met | `LinearFitIO` (format version 1): bitwise round trips on integral and string keys, never overwriting, other kinds and versions refused |
| Job runner and Python access | Met | `LinearJobSuite` and the `PythonApiSuite` test: equal to the library bit for bit; `test_linear.py` against the fixtures |
| Platforms (§5) | Met in the sandbox | Every linear suite under Classic Spark and Spark Connect in its round; numerics on JDK 17 and 21 (round 69) |
| CI | **Pending** | The maintainer reported CI green after round 60; rounds 61 to 73 have no CI report. CI runs JDK 17 and 21 and Spark Connect |
| Documentation | Met | Specifications 3a to 3c; guides `linear-job.md` and `python.md`; the parity matrix's 3a to 3c rows; two calibration documents. PROJECT_CONTEXT §4 and §7.5 to be updated when Phase 3 closes |
| Scale (§9.7) | **Deferred** | The maintainer's package-level test (D-28) |

## Open items carried forward

OI-61 (§7.5's TSQR path, deferred from slice 3a), OI-62 (the three-stage `glmer` blocks not reproduced in round 63's
sandbox), OI-33 and OI-42 (upstream reports, now with X-031, X-035 and X-036), OI-40 (a CI check of the parity matrix),
OI-53 (Databricks access modes), OI-55 (a wheel bundling the JARs), and Phase 2's items listed in
[phase-2.md](phase-2.md). Closed during Phase 3: OI-60 (round 67) and OI-63 (round 73).

## Proposal (D-48)

Close Phase 3 at parity-verified once the maintainer reports CI green for `main` at this round, on the terms of D-32
and D-44, accepting 3a's function-level verification against exact arithmetic in place of the fixtures' NumPy values;
every Phase 3 feature stays `Experimental` until the package-level scale test (D-28). PROJECT_CONTEXT v2.6 then records
Phase 3.

## Outcome

Awaiting the maintainer's sign-off.
