# Phase 2 gate review: logistic provider models

- Date: 2026-10-09 (round 60). Reviewer: the assistant; sign-off: the maintainer (D-44, approved 2026-10-09).
- Exit criterion (§4): the parity gate passed for large-m fixed effects (SerBIN-type blocked Newton),
  provider tests (Wald, score, exact Poisson-binomial, bootstrap), direct and indirect standardization, and
  the three-stage SRR pipeline with the stage 2 variance estimation it needs.
- Plan: [plan.md](../spec/logistic/plan.md) (D-30), slices 2a to 2f; slice 2a's own review:
  [phase-2a.md](phase-2a.md) (D-32). Scale verification is the maintainer's, once the package is done (D-28).

## Evidence by slice

The parity suites were run again for this review in the sandbox (round 60: Classic Spark, JDK 17); every
test passed.

| Slice | Specification | Fixtures and calibration | Parity tests | Status |
|---|---|---|---|---|
| 2a, estimation | D-30 | `fixtures/logistic`, five cases (round 26) | `LogisticKernelParitySuite` 18/18 (function level, T-fn; lockstep, T-iter); `LogisticFESuite` 6/6 (end to end against pprof_py, `glm` and R pprof's SerBIN); `LogisticBehaviourSuite` 7/7 (metamorphic and edge cases) | Met; closed at parity-verified (D-32), CI green from run 37945209150 on |
| 2b, covariate tests, robust variance, prediction, AUC | D-34 | lfe-clustered (round 32) | `LogisticInferenceSuite` 8/8 against pprof_py and R | Met |
| 2c, provider tests | D-35 | Round 35, with R pprof's `test.logis_fe` and mpmath tails | `LogisticProviderTestsSuite` 5/5, `LogisticProviderTestsMoreSuite` 3/3; numerics `PoissonBinomialSuite` | Met |
| 2d, standardization | D-36 | Round 38, with R pprof's `SM_output.logis_fe` | `LogisticStandardizationSuite` 4/4, `LogisticStandardizationMoreSuite` 3/3; numerics `TaylorBinsSuite` | Met |
| 2e, `LogisticJob` | D-37 | lfe-clustered | `LogisticJobSuite` 5/5: every output equals the library's bit for bit | Met |
| 2f-1, preparation and stage 1 | D-39 | `fixtures/three-stage` (round 44) | `ThreeStageSuite` 6/6 against pprof_py and R | Met |
| 2f-2, stage 3 | D-40 | Round 47, with Gauss–Hermite rules | `ThreeStageStage3Suite` 5/5: ℓ and score at fixed effects (T-fn), iterates (T-iter), fits, and the executor path equal to the driver path bit for bit | Met |
| 2f-3, stage 2 | D-41, D-42 | Round 51, with `glmer` and ts-shuffled | `ThreeStageStage2Suite` 4/4: the Laplace deviance at fixed parameters (T-fn) and the optimum (T-opt) | Met |
| 2f-4, pipeline, tests, measures, intervals, σ sensitivity, persistence, job, Python | D-43 | Round 54 | `ThreeStagePipelineSuite` 2/2 and `ThreeStagePipelineMoreSuite` 2/2; `ThreeStageSensitivitySuite` 1/1 and `ThreeStageSensitivityMoreSuite` 2/2; `ThreeStageJobSuite` (round 57); `PythonApiSuite` and the Python tests (round 59) | Met |

## Measured worst ratios

A ratio is the observed difference over the allowed one under the class's element-wise rule (§8.4), so 1
is at the tolerance. The 15 engine suites above compute 10,042 ratios and none exceeds 1. The worst per
slice:

| Slice | Worst ratios |
|---|---|
| 2a | T-coef 0.431 (lfe-clustered), T-var 0.0253 (lfe-many), T-iter 0.0176 (lfe-shifted), T-fn 3.2e-3, T-test 1.2e-3, T-part 1.5e-6 |
| 2b | T-test 4.9e-4 (lfe-shifted), T-coef 1.4e-6, T-meas 5.2e-7, T-var 1.3e-7 |
| 2c | T-test 0.0528 (lfe-binomial), T-coef 2.8e-3 |
| 2d | T-meas 0.169 (lfe-clustered), T-test 1.2e-5, T-coef 2.8e-6, T-part 2.6e-6 (binned against exact direct sums) |
| 2f-1 | T-coef 0.683 (ts-golden-prep), T-var 1.1e-7 |
| 2f-2 | T-fn 0.142 (ts-golden), T-iter 1.9e-3, T-coef 5.2e-5, T-meas 1.2e-5 |
| 2f-3 | T-opt 0.331 (ts-shuffled), T-fn 4.7e-4, T-coef 1.3e-4 |
| 2f-4 | T-test 0.0108 (ts-synthetic), T-opt 5.2e-3 (end to end and σ sensitivity, ts-golden), T-coef 2.4e-3, T-meas 1.8e-3 |

Method: a copy of the testkit, compiled in the sandbox only, prints every ratio that `Tolerance.worstRatio`
and `Tolerance.accepts` compute; the suites ran unchanged against it, and each figure is the largest ratio of
its class over the slice's suites. The repository's testkit is unchanged.

## Cross-cutting requirements

| Requirement | Status | Evidence |
|---|---|---|
| Specifications approved (NN-2) | Met | D-30, D-34 to D-37, D-38 to D-43 |
| Fixtures and calibration (§9.3) | Met | Every slice's fixtures were calibrated in their round; `FixturesSuite` checks them and their negative controls (testkit 20/20 under Classic Spark and Spark Connect, round 60) |
| Three parity levels (§9.2) | Met | Function level: SerBIN's ℓ, scores and information, stage 3's ℓ and score, stage 2's deviance at fixed parameters (T-fn). Lockstep: SerBIN's steps 1 to 5 and stage 3's iterates (T-iter). End to end: every slice |
| Discrepancies (PAR-3, ROAD-2) | Met | Every discrepancy affecting Phase 2 has an approved decision: X-016 to X-020 (D-30), X-021 (D-32), X-022 and X-023 (D-34), X-024 and X-025 (D-35), X-026 (D-36), X-005 (D-38), X-028 (D-39), X-029 (D-40), X-030 (D-41, D-42). The class B items, X-024 and X-005, are corrected behavior with a documented divergence; their upstream reports wait with OI-33 and OI-42, as Phase 1's did. X-006 concerns later features |
| Metamorphic and edge cases (§9.4, §9.5) | Met | Bitwise invariance to row order and partitioning (2a, 2c's bootstrap, 2f-1 to 2f-3); block sizes within T-part; binomial rows against expanded Bernoulli rows; validation failures with counts; degenerate and screened providers; the σ = 0 limit (X-005); stage 3's executor path equal to its driver path bit for bit |
| Persistence | Met | `LogisticFitIO` (format version 3): bitwise round trips, no overwriting, other kinds and versions refused. `ThreeStageFitIO` (version 1): never overwriting; the saved and re-attached fit gives the library's outputs bit for bit, and changed data are refused |
| Job runners and Python access | Met | `LogisticJobSuite` and `ThreeStageJobSuite`: outputs equal to the library's bit for bit; `PythonApiSuite` and the Python tests: wrapper results equal to the engine's (ADR-0009) |
| Platforms (§5) | Met | Every Phase 2 suite ran under Classic Spark and Spark Connect in its round; CI run 37945209150 (round 56) on JDK 17 and 21 and Spark Connect; in round 60's sandbox, numerics 83 on JDK 17 and 21 |
| CI | Met | Run 37945209150 (round 56) passed every Scala job (engine 295, numerics 83, testkit 20, app 12 tests); the maintainer reported CI green after round 57.1, the `python` job included. Rounds 31 to 55 were not reported individually and are part of round 56's run. Round 59 (b876d51: the three-stage Python wrapper and a `PythonApiSuite` test): green, as the maintainer reported on 2026-10-09 |
| Documentation | Met, with one gap | Specifications; guides `logistic-job.md`, `three-stage-job.md` and `python.md`; the parity matrix, whose Phase 2 rows this round brings up to date. Gap: PROJECT_CONTEXT v2.4 §7.4 still describes stage 2 without the crossed GLMM (OI-48) |
| Scale (§9.7) | **Deferred** | The maintainer's package-level test (D-28) |

## Open items carried forward

OI-48 (PROJECT_CONTEXT §7.4's three-stage description, for the next revision), OI-50 (γ on the driver up to
`maxProvidersOnDriver`), OI-53 (Databricks access modes for the Python wrappers, the maintainer's check),
OI-55 (a wheel bundling the JARs), OI-56 (PySpark CSV pushdown with a literal provider column), OI-57 (exact
tests with unbalanced blocks), OI-58 (external standard populations), OI-59 (stage 2's dense Schur
complement), OI-29 (distributed result tables at large m), OI-40 (a CI check of the parity matrix; its Phase 2
rows had drifted until this round), and OI-33 and OI-42 (upstream reports, now with X-005 and X-024).
Closed in this round: OI-49 (D-36), OI-51 (D-38 and slice 2f) and OI-52 (D-35).

## Proposal (D-44)

Close Phase 2 at parity-verified once the maintainer reports CI green for round 59, on the terms of D-32;
every Phase 2 feature stays `Experimental` until the package-level scale test (D-28). OI-48 goes into the
next PROJECT_CONTEXT revision.

## Outcome

**Closed on 2026-10-09** (D-44): the maintainer reported CI green and signed off. Every Phase 2 feature is
parity-verified and stays `Experimental` until the package-level scale test (D-28). PROJECT_CONTEXT v2.5
(round 61) folds in OI-48, the documentation gap above.
