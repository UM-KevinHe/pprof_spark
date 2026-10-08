# Specification: three-stage model — pipeline, σ sensitivity, stage 3 inference, job (Phase 2f-4)

- Status: Draft, awaiting approval (D-43). No 2f-4 code is written before approval (NN-2).
- Builds on: 2f-1 to 2f-3 ([three-stage-spike.md](three-stage-spike.md), D-38 to D-42); slices 2c to 2e for
  the shared test, measure, job and Python machinery.
- Decision references: D-43; X-005, X-024, X-025, X-026, X-030.
- Reference: pprof_py v0.7.0 (`9320766`): `models/logistic/three_stage.py` (`fit`, `sigma_sensitivity`,
  `test`, `calculate_standardized_measures`, `calculate_confidence_intervals`, `summary`),
  `inference/logistic/fe_random_cluster.py` (`test`, `calculate_confidence_intervals`),
  `measures/logistic/fe_random_cluster.py`, `inference/effect_tests.py` (`clustered_poibin_tails`,
  `_clustered_pmf`, `_pmf_tails`, `resample_tails`), `models/logistic/random_effect.py` (`profile_sigma`).

Probe results are sandbox evidence from round 53 (pprof_py on `three_stage/raw.csv`).

## 1. Pipeline

`ThreeStage.fit(df, spec, options)` runs 2f-1's preparation and stage 1, compresses the records (2f-2),
fits stage 2 (2f-3), and fits stage 3 from stage 2's start with stage 1's β and stage 2's σ_c, returning
`ThreeStageFit` (the preparation, the three stages' results, and the options). pprof_py's `summary` is stage
1's Wald table (slice 2b's `waldTests` on the stage 1 fit).

## 2. Stage 3 provider tests

The observed count is each provider's raw outcomes (pprof_py's `obs_var`); the null distributions use stage
3's β, γ₀ and the clusters' posterior means and variances E_post[aₕ], Var_post[aₕ].

| Method | Null distribution of the count | Tails | Limits |
|---|---|---|---|
| `exact` (default) | Each cluster's effect is drawn once from N(E_post[aₕ], Var_post[aₕ]) and shared by the provider's records there: per cluster a Gauss–Hermite mixture (`nNodes`, standard normal nodes) of Poisson-binomial distributions, convolved across the provider's clusters | Sums of the count distribution's entries above and below the observed count, as pprof_py's `_pmf_tails` (no subtraction from 1, so X-024 does not arise) | Inversion, as slice 2c |
| `poibin_exact` | Poisson-binomial with each record's probability at E_post[aₕ] | Slice 2c's smaller-tail recursion (X-024) | Inversion |
| `resampling` | Monte Carlo with a separate cluster draw per record; tails at the floor 0.5/`nResample` replaced by the exact tails of the posterior-integrated probabilities, as pprof_py | Counter-based draws (X-025's rule: distributional parity) | None |

Reference γ₀ (median, size-weighted mean or a number), alternatives, the theoretical null, flags and the
result table are slice 2c's.

## 3. Standardized measures and intervals

| Measure | Definition (pprof_py) |
|---|---|
| Indirect | Eⱼ = Σᵢ∈ⱼ σ(γ₀ + E_post[aₕ₍ᵢ₎] + xᵢᵀβ̂); ratio Oⱼ/Eⱼ (NaN when Eⱼ = 0); rate clip(ratio × 100·O/N, 0, 100) |
| Direct | Dⱼ = Σ over every record of σ(γ̂ⱼ + E_post[aₕ₍ᵢ₎] + xᵢᵀβ̂); ratio Dⱼ/O; rate 100·Dⱼ/N |
| Reference γ₀ for measures | `median`, `mean` (pprof_py's unweighted mean here, unlike its tests' size-weighted mean) or a number |
| Intervals | `option = gamma`: the inverted test's limits; `option = SM`: the measure at those limits (the indirect ratio's predicted count at a limit over the expected count; the direct measure's prediction at a limit) |

Direct sums run per cluster: the records of cluster h share E_post[aₕ], so each cluster's offset-bin moments
(X-026's expansion) give Σᵢ∈ₕ σ(γ̂ⱼ + E_post[aₕ] + oᵢ) for every provider at cost m × (clusters × bins).

## 4. σ sensitivity (pprof_py's `sigma_sensitivity`)

The profile interval of σ_c at `level` (0.95): with σ_c fixed at s, P(s) is D minimized over (σₚ, μ) (2f-3's
exact D and bounded quasi-Newton, warm-started along the profile); the interval holds the s with
P(s) − D_min ≤ χ²₁(level). The upper limit is bracketed by doubling from σ̂_c + max(0.1, σ̂_c) and found by
Brent's method; the lower limit is 0 when P(0) − D_min ≤ χ²₁(level), and found by Brent's method on
[0, σ̂_c] otherwise. Stage 3 is refitted at both ends with stage 1's β and stage 2's start, using the σ = 0
limit where the interval reaches 0 (X-005, where pprof_py raises); each fit is tested as requested; the
result is the three σ values, the three test tables and each provider's flags with `stable` (the same flag
at all three). pprof_py minimizes the nuisance parameters by Nelder–Mead (`xatol` 1e-5, `fatol` 1e-8), so
its limits are optimizer-limited: they are compared under T-opt (X-030).

## 5. Persistence, job runner and Python

| Part | Content |
|---|---|
| `ThreeStageFitIO` (format version 1) | A directory: `metadata.json` (specification, options, counts, stage 2's parameters and convergence, stage 3's convergence, doubles as 64-bit patterns), `stage1/` (`LogisticFitIO`), `providers` (key, γ̂, stage 2 BLUP, start) and `clusters` (key, posterior mean and variance, stage 2 BLUP) as Parquet; never overwriting; bit-for-bit round trip |
| Job runner | The run specification (version 1) with `model` `three-stage`: `columns` (outcome, features, provider, cluster, rowId), `fit` (cutoff; stage 1's `LogisticOptions`; stage 2's and stage 3's options), `outputs` (fit, providers, clusters, tests, measures, intervals, sensitivity, fitted probabilities); `ThreeStageJob` as `LogisticJob` (slice 2e) |
| Python | `pprof_spark.ThreeStageModel` (fit, providers, clusters, test, standardized_measures, confidence_intervals, sigma_sensitivity, fitted, save, load) through `PythonApi.threeStage*` |

## 6. Distributed plan

| Step | Where |
|---|---|
| Stages 1 to 3 | As 2f-1 to 2f-3 (driver for stages 2 and 3 when the compressed cells fit, executors otherwise) |
| Tests | Provider-local on 2f-1's records grouped by provider (one block per set of whole providers), with E_post[aₕ] and Var_post[aₕ] for the provider's clusters shipped by closure (cluster-scale); results distributed |
| Indirect measures | Provider-local sums; direct measures from per-cluster offset-bin moments on the driver (m × clusters × bins) |
| Sensitivity | On the driver, from the compressed cells: the profile's evaluations of D, then two stage 3 refits |

## 7. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| Profile interval reaching σ_c = 0 | `ZeroDivisionError` in the stage 3 refit | The σ = 0 limit (X-005) |
| No upper profile limit below 1000 | Raises | Fails with a message |
| `resampling` limits | NaN | Same |
| Expected count 0 (indirect) | NaN ratio | Same |
| Unknown method, option, measure or reference | Raises | Fails with a message |

## 8. Validation plan

Fixtures (next round), on ts-golden, ts-synthetic and ts-shuffled, from pprof_py's pipeline: stage 3's
`test` for `exact` (two-sided, `greater`, `less`), `poibin_exact` and `resampling` (seeded);
`calculate_standardized_measures` (indirect and direct; median, mean, a number); `calculate_confidence_
intervals` (`gamma`, and `SM` for both measures); `sigma_sensitivity` (the three σ values, flags, stability);
the pipeline's γ, posterior moments and fitted probabilities. Tolerance classes: T-test (z), T-p, T-flag,
T-coef (limits), T-meas (measures, posterior moments, fitted probabilities) where the inputs are pprof_py's
own stage outputs, and T-opt for the profile limits and everything computed through σ̂ when pprof_spark's own
stages run end to end. Bootstrap-style checks for `resampling` (within four Monte Carlo standard errors of
`exact`'s posterior-integrated counterpart). Negative controls: γ₀ shifted by 0.01, one outcome flipped, the
`level` changed to 0.9.

Round 53 probes (golden data, pprof_py): `exact` 1.1 s for 40 providers (6 flagged high, 5 low, smallest p
3.3e-4), `poibin_exact` 0.16 s (7 and 6), `resampling` 2.2 s (6 and 6, no limits); measures and intervals
under 1.1 s; `sigma_sensitivity` 5.1 s, with σ_c's profile interval (0.2458, 0.7280) around 0.4229 and 34 of
40 flags stable.

## 9. Sub-rounds

2f-4a: the pipeline, stage 3's tests, measures and intervals, and `sigma_sensitivity` (fixtures, then code);
2f-4b: persistence, the job runner and Python.
