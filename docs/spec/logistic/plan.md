# Phase 2 plan: logistic provider models

- Status: Approved by the maintainer, 2026-10-07 (D-30). Python access: D-31 (§6).
- Scope: PROJECT_CONTEXT §4 (Phase 2) and §7.4, against pprof_py v0.7.0 (`9320766`).

## 1. What the reference contains at the pin

Read in round 25 (`models/logistic/`, `algorithms/logistic/`, `inference/logistic/fixed_effect/`,
`inference/{effect_tests,count_tests,decision,standardized}.py`, `measures/logistic/`,
`data/{validation,preparation,glmm_prep}.py`). pprof_py's logistic suites and three inference
suites pass at the pin: 175 passed in 82 s (sandbox, without the optional `nlopt`).

| Component | pprof_py v0.7.0 |
|---|---|
| Fixed effects | `LogisticFixedEffectModel`: SerBIN (default) or BAN; Bernoulli or binomial rows; screening (more than 10 records); median-relative bound ±10 |
| Covariate inference | Wald (model-based or cluster-robust), likelihood-ratio and score tests by refitting without the covariate (C32, C33) |
| Variances | Var(β̂) = S⁻¹; Var(γ̂ⱼ) (R's `logis_fe_var`); Var(γ̂ⱼ + x̄ᵀβ̂) at the average case mix (C34); cluster-robust sandwiches by observation identifier (C25) |
| Provider tests | `test()`: exact Poisson-binomial (default; two-sided mid-p; limits by inverting the test), score, Wald, bootstrap; reference median, size-weighted mean or a number; shared `PROVIDER_TEST_COLUMNS` (as the Cox tests) |
| Standardization | Indirect O/E (expected at γ₀) and direct (each provider's effect over the whole population); `standardized_measure` with delta-method or Poisson-binomial SEs; `test_standardized` on identity, logit or log scales |
| Three-stage SRR | `LogisticThreeStageModel` (He et al. 2013, R's `glmm.fac.hosp`): data preparation with an adjusted outcome and provider × cluster cells; stage 1 = fixed effects per cell; stage 2 = `LogisticRandomEffectModel`, an lme4-style GLMM with **crossed** provider and cluster random intercepts (nAGQ = 0 then Laplace; BOBYQA from `nlopt` when installed, otherwise Powell); stage 3 = `LogisticFERandomClusterModel`, provider fixed effects with cluster random effects integrated out by adaptive Gauss-Hermite quadrature and a sparse Newton step in which providers interact through shared clusters |

PROJECT_CONTEXT §7.4 describes stage 2 as a provider-local random-intercept fit; the reference's is
crossed and global, and its stage 3 couples providers through clusters. §7.4 and §3.4 are to be
corrected (OI-48).

## 2. Slices

| Slice | Content | pprof_py | R references | Fixtures | Tolerance classes |
|---|---|---|---|---|---|
| 2a | Fixed-effect estimation: SerBIN, screening, degenerate providers, model-based variances, Wald for β, ℓ/AIC/BIC, persistence | `LogisticFixedEffectModel`, `SerbinAlgorithm`, `_estimate_variances` | `glm` (MLE, `vcov`); R pprof `logis_BIN_fe_prov` | lfe-base, lfe-degenerate, lfe-shifted, lfe-binomial, lfe-many | T-fn, T-iter, T-coef, T-var, T-test, T-p, T-part |
| 2b | Covariate likelihood-ratio and score tests (refits); cluster-robust variances (β; γ at the average case mix; γ with β fixed); prediction; AUC. Specification: [covariate-inference.md](covariate-inference.md) (D-34) | `_compute_lr_beta`, `_compute_score_beta`, `_compute_robust_variances`, `predict` | R pprof `summary.logis_fe` (C32/C33 goldens), `test_aoh`/`robust_wald_gamma` (AOH goldens); `anova(glm)` | 2a cases plus observation identifiers | T-test, T-var, T-p |
| 2c | Provider tests on γ: exact Poisson-binomial with test-inversion limits, score, Wald, bootstrap; flags and result tables. Specification: [provider-tests.md](provider-tests.md) (D-35) | `test`, `count_test`, `poibin_tails`, `invert_decreasing`, `effect_test`, `provider_test` | R pprof `test.logis_fe` (`poibin`) | 2a cases | T-test, T-p (log₁₀ p below 1e-10), T-flag, T-coef for limits |
| 2d | Standardization: indirect and direct measures, their SEs, `test_standardized` | `calculate_standardized_measures`, `standardized_measure`, `z_statistic` | R pprof `SM_output.logis_fe` | 2a cases | T-meas, T-test, T-p |
| 2e | Job runner for logistic fixed effects (the `CoxJob` pattern) | — | — | lfe-base | Bitwise against the library |
| 2f | Three-stage SRR: data preparation; stage 1 on cells (reuses 2a); stage 2 crossed GLMM; stage 3; its tests and measures; `sigma_sensitivity` (X-005) | `glmm_data_prep`, `LogisticThreeStageModel`, `LogisticRandomEffectModel`, `LogisticFERandomClusterModel` | lme4 1.1-35.1 `glmer(nAGQ = 1)` (Ubuntu's `r-cran-lme4`); R pprof `glmm.data.prep`, `glmm.fac.hosp` (pprof_py's `three_stage` goldens) | Three-stage cases with crossed clusters | To calibrate; pprof_py reports about 2e-5 against `glmer` |

Each slice follows the Phase 1 pattern: specification approved first, then fixtures and
calibration, then code and parity tests, under Classic Spark and Spark Connect. BAN, penalized and
random-effect-only models stay out of Phase 2 (§4). The Phase 2 gate closes at parity-verified, as
D-22 to D-29; the scale test is the maintainer's, once the package is done (D-28).

## 3. Distributed design for up to 10⁶ providers

| Topic | Design |
|---|---|
| Layout | ProviderLocal blocks for 2a to 2e: whole providers bin-packed by record count (exact tests: by estimated cost); canonical order within providers. Stage 3 of 2f needs cluster-local blocks, since a cluster's integral spans its providers |
| On the driver | p- and p²-scale partials; γ (m doubles, 8 MB at 10⁶) under `maxProvidersOnDriver`; the median for the bound (a sort of m values) |
| Distributed | Every n-scale and m-scale table: provider tables, test results, standardized measures |
| Per Newton step (2a) | Two passes: Schur-complement partials, then Δγ with ℓ at eight step lengths (spec §11) |
| Reductions | Provider sums in canonical row order; block partials in block order with compensated summation; two levels beyond the driver budget (§6.8); no floating-point SQL aggregates (DIST-7) |
| Exact tests (2c) | Provider-local, cost-packed; limits by root finding with pprof_py's bracketing. pprof_py's Poisson-binomial is FFT-based (`fast_poibin`, absolute error near ε), so extreme tails follow T-p's log rule (OI-52) |
| Bootstrap (2c) | A counter-based generator keyed on (seed, provider, replicate) (STAT-2); parity with numpy's generator is distributional only |
| Direct standardization (2d) | Every provider's effect over every record: O(n·m) evaluations for the estimate and its SE, 10¹⁵ at the top of the envelope. A kernel over the working set with γ shipped (§6.7) and m-length partials reduced by provider ranges; feasible to about n·m = 10¹³. Beyond that, an approximation needs approval (OI-49) |
| Three-stage (2f) | Stage 2's crossed GLMM has m_providers + m_clusters random effects in one sparse system; stage 3's Newton system is m×m and sparse through shared clusters. Both need a spike before their specification (OI-51) |

## 4. Tolerance classes

Phase 2 uses the existing classes of `tolerances.conf`: T-fn, T-iter, T-coef, T-var, T-test, T-p,
T-flag, T-meas and T-part. Each slice's fixtures are calibrated against both references before
they enter CI, as for Cox (`docs/parity/cox-calibration.md`). The round 25 probe (spec §10) puts
pprof_py and `glm` within 1.4e-13 of each other for estimates and variances. Stage 2 of the
three-stage model may need a class of its own: pprof_py reports about 2e-5 against `glmer`, and
its optimizer path depends on whether `nlopt` is installed; that is decided with slice 2f's
calibration (OI-51), never by relaxing a class (NN-9).

## 5. Decisions

| When | Decision |
|---|---|
| Now (D-30) | This plan; the 2a specification with X-004 (resolved), X-016 to X-018 (follow the reference), X-019 (fail at validation) and X-020 (README) |
| Before 2c | Exact Poisson-binomial algorithm and accuracy (OI-52) |
| Before 2d | Direct standardization beyond n·m = 10¹³ (OI-49) |
| Before 2f | Stage 2 and stage 3 designs after the spike (OI-51); X-005 at the σ̂ = 0 boundary |
| Before the Python wrappers | ADR-0009: facade, packaging and CI for the py4j wrappers (D-31, OI-53) |

## 6. Python access (D-31)

The maintainer chose py4j wrappers for Python access (D-31, superseding D-03). Python code calls
the Scala engine through PySpark's JVM gateway and never reimplements statistics (§6.12). The design is ADR-0009 (`docs/adr/0009-py4j-wrappers.md`, D-33, proposed); its summary:

| Topic | Proposal |
|---|---|
| JVM side | A Java-friendly facade (strings, numbers, `java.util` collections, Datasets), so Python never builds Scala `Option`, `Seq` or case classes; options travel as the job runner's JSON specification |
| Python side | A `pprof_spark` package in `python/`, versioned with the JAR; results as pandas-free dataclasses plus PySpark DataFrames |
| Modes | Classic sessions only: py4j needs the driver JVM in the Python process's gateway, which Spark Connect clients do not have; Connect users keep the job runner |
| Databricks | The maintainer's test: py4j access to library classes is expected on dedicated (single-user) compute and blocked in standard access mode (to re-verify, OI-53) |
| CI | A job with PySpark 4.1.0 and the built JARs running the Python tests in local Classic mode |
| Distribution | A wheel with the JAR on GitHub Releases (D-08) |
| Schedule | After slice 2a's estimator: ADR-0009, then wrappers for the Cox API (round 30), then each logistic slice as it lands (2a and 2b in round 33) |
