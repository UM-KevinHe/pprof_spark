# Status

Updated 2026-10-09, round 63.

**Phase 0 closed on 2026-10-06** (docs/gates/phase-0.md). **Phase 1a, Cox estimation core: closed** (D-22; CI green through round 14).
**Phase 1b, counting process and baseline: closed** (D-24). **Phase 1c, residuals and robust variance: closed** (D-26). **Phase 1d, provider workflows: closed** (D-29). **Phase 1 is complete.** **Phase 2, logistic provider
models: closed** (D-44, 2026-10-09, after CI green as reported by the maintainer; docs/gates/phase-2.md): plan and slice 2a specification approved (D-30); 2a fixtures and calibration in
round 26; the 2a estimator in round 27; persistence and the slice review in round 28 (D-32, approved); the correlation warning and ADR-0009 (D-33, approved) in round 29; the Cox py4j wrappers in round 30; the slice 2b specification in round 31 (D-34, approved); its fixtures in round 32; its code and the logistic Python wrappers in round 33; the slice 2c specification in round 34 (D-35, approved); its fixtures in round 35; its code in round 36; the slice 2d specification in round 37 (D-36, approved); its fixtures in round 38; its code in round 39; the slice 2e specification in round 40 (D-37, approved); `LogisticJob` in round 41; the three-stage spike and 2f direction in round 42 (D-38, approved); the 2f-1 specification in round 43 (D-39, approved); its fixtures in round 44; `ThreeStage.prepare` in round 45; the 2f-2 specification in round 46 (D-40, approved); its fixtures in round 47; its driver path in round 48 and its executor path in round 49; the 2f-3 specification in round 50 (D-41, approved); its fixtures and T-opt in round 51 (D-42, approved); its code in round 52; the 2f-4 specification in round 53 (D-43, approved); 2f-4a's fixtures in round 54; the pipeline, stage 3's tests, measures and intervals in round 55; `sigma_sensitivity` in round 56; persistence and the job runner in round 57; the Python three-stage wrapper in round 59; the Phase 2 gate review in round 60 (D-44, approved); PROJECT_CONTEXT v2.5 in round 61. **Phase 3, linear fixed effects: in progress**: the plan and slice 3a's specification approved (D-45, round 62); slice 3a's fixtures and calibration in round 63. Python access: py4j wrappers (D-31). The maintainer runs the
scale test on his Databricks workspace once the whole package is done (D-28).

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
| 17 | Phase 1c fixtures (residuals, robust variances) and residuals | Merged (09167b4) |
| 18 | Robust variance, per row and clustered; format version 3; Phase 1c gate review (D-26) | Merged (1f2ed5c); CI green |
| 19 | Phase 1d specification (D-27) and the scale-test site question (D-28) | Merged (3ebf680); approved; site decided |
| 20 | Phase 1d fixtures and standardized measures (1d-1) | Merged (bafd12f); covered by round 22's green CI |
| 21 | Provider tests (1d-2): Poisson numerics, exact and mid-p tests, flags, result tables | Merged (7d17f65); covered by round 22's green CI |
| 22 | Job runner (1d-3): `app` module, JSON run specification, outputs and run record | Merged (1b1971a); CI green |
| 23 | Phase 1d gate review (D-29); PROJECT_CONTEXT v2.3 | Merged; Phase 1d closed |
| 24 | Phase 1 closed; session handoff for Phase 2 (HANDOFF.md) | Merged (c34eab7) |
| 25 | Phase 2 plan and the 2a specification (D-30, X-016 to X-020); D-03 revisited | Merged (2a3f813); D-30 approved 2026-10-07; CI not reported |
| 26 | Phase 2a fixtures (`fixtures/logistic`), calibration, R pprof pin; D-31 (py4j wrappers) | Merged (fcbe03c); CI not reported |
| 27 | Slice 2a: SerBIN kernels and iteration (numerics), the distributed estimator (engine), parity tests | Merged (c9640f3); CI not reported |
| 28 | Slice 2a: persistence (`LogisticFitIO`), slice review (D-32), X-021 | Merged (1022efc); CI not reported |
| 29 | The correlation warning (X-021, a); `LogisticFitIO` format version 2; ADR-0009 (D-33) with a py4j spike | Merged (7391a78); CI not reported |
| 30 | Python access: the `PythonApi` facade, the `pprof_spark` package with the Cox wrappers, the `python` CI job, a user guide | Merged (c277147); CI not reported; the `python` job failed in every run from here to round 56 (relative JAR paths; fixed in round 57.1) |
| 31 | Slice 2b specification (D-34, X-022, X-023) | Merged (45e38b1); D-34 approved |
| 32 | Slice 2b fixtures (`lfe-clustered`; tests, AUC, predictions, robust variances) and calibration | Merged (a956da7); CI not reported |
| 33 | Slice 2b code (robust variances, Wald variants, LR and score tests, prediction, AUC, `LogisticFitIO` v3); logistic Python wrappers | Merged (e917a68); CI not reported |
| 34 | Slice 2c specification: provider tests (D-35, X-024, X-025) | Merged (c493084); D-35 approved |
| 35 | Slice 2c fixtures (provider tests, R pprof's `test.logis_fe`, mpmath tails) and calibration | Merged (db5d49a); CI not reported |
| 36 | Slice 2c code: exact, score, Wald and bootstrap provider tests; Python `provider_tests` | Merged (32715b6); CI not reported |
| 37 | Slice 2d specification: standardization (D-36, X-026, OI-49) | Merged (90a2f04); D-36 approved |
| 38 | Slice 2d fixtures (standardized measures, R pprof's `SM_output.logis_fe`) and calibration | Merged (a01c824); CI not reported |
| 39 | Slice 2d code: indirect and direct measures, binned direct sums, tests on measures; Python | Merged (8c651d9); CI not reported |
| 40 | Slice 2e specification: the logistic job runner (D-37) | Merged (27fefc8); D-37 approved |
| 41 | Slice 2e code: `LogisticJob`, the run specification's `model`, the guide | Merged (953ee52); CI not reported |
| 42 | Three-stage spike (OI-51) and the 2f direction (D-38, X-005) | Merged (73e6ef0); D-38 approved |
| 43 | Slice 2f-1 specification: preparation and stage 1 (D-39, X-028) | Merged (9563028); D-39 approved |
| 44 | Slice 2f-1 fixtures (`fixtures/three-stage`) and calibration | Merged (e1a2e78); CI not reported |
| 45 | Slice 2f-1 code: `ThreeStage.prepare`; excluded providers corrected in the fixtures | Merged (1f1ea56); CI not reported |
| 46 | Slice 2f-2 specification: stage 3 on compressed cells (D-40, X-029) | Merged (dabaeb4); D-40 approved |
| 47 | Slice 2f-2 fixtures (stage 3, Gauss–Hermite rules) and calibration | Merged (f11dd9d); CI not reported |
| 48 | Slice 2f-2 code, driver path: compression, Gauss–Hermite rules, sparse CG, `ThreeStage.stage3` | Merged (91f9347); CI not reported |
| 49 | Slice 2f-2 code, executor path for large compressed tables; fitted probabilities | Merged (a81ca26); CI not reported |
| 50 | Slice 2f-3 specification: stage 2, the crossed random-intercept GLMM (D-41, X-030, OI-59) | Merged (c1ea317); D-41 approved |
| 51 | Slice 2f-3 fixtures (stage 2, `glmer`, ts-shuffled) and calibration; T-opt (D-42) | Merged (77283db); D-42 approved |
| 52 | Slice 2f-3 code: PIRLS on compressed cells, the Schur-complement Cholesky, the bounded quasi-Newton | Merged (47c4048); CI not reported |
| 53 | Slice 2f-4 specification: the pipeline, `sigma_sensitivity`, stage 3's tests, measures and intervals, persistence, job and Python (D-43) | Merged (d6f2527); D-43 approved |
| 54 | Slice 2f-4a fixtures (stage 3 tests, measures, intervals, σ sensitivity) and calibration | Merged (0416194); CI not reported |
| 55 | Slice 2f-4a code: the pipeline, stage 3's tests, measures and intervals | Merged (4015d6c); CI not reported |
| 56 | Slice 2f-4a code: `sigma_sensitivity` | Merged (a215676); CI run 37945209150: checks, JDK 17 and 21 and Spark Connect passed (engine 295, numerics 83, testkit 20, app 12 tests); the `python` job failed |
| 57 | Slice 2f-4b: persistence (`ThreeStageFitIO`) and the job runner (`ThreeStageJob`) | Merged (e0a635c) |
| 57.1 | Fix of the `python` CI job (absolute JAR paths; conftest resolves and checks them; pytest's report feeds the annotations) | Merged (7d5190a); CI green as reported by the maintainer |
| 58 | PROJECT_CONTEXT v2.4 and the session handoff (documents) | Merged (2fa916b) |
| 59 | Slice 2f-4b: the Python three-stage wrapper (`ThreeStage`, `ThreeStageModel`) | Merged (b876d51); CI green as reported by the maintainer |
| 60 | Phase 2 gate review (`docs/gates/phase-2.md`, D-44); worst ratios measured; the parity matrix's Phase 2 rows brought up to date | Merged (7f8b49f); D-44 approved 2026-10-09 |
| 61 | D-44 approved: Phase 2 closed at parity-verified; PROJECT_CONTEXT v2.5 (§7.4's three-stage model, OI-48); README status | Merged (da6aa4e) |
| 62 | Phase 3 plan and the slice 3a specification (D-45, X-031 to X-033, OI-60, OI-61) | Merged (fb9cecf); D-45 approved |
| 63 | Slice 3a fixtures (`fixtures/linear`) and calibration; X-034 proposed | Delivered as a patch |
| 60 | Phase 2 gate review | Planned |

## Decisions and spikes

See the table above and DECISIONS.md. S-02's local part is complete; the remaining spikes are
listed under the exit criteria.
