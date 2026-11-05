# Specification: fixed provider effects with random cluster effects (slice 4c-1)

- Status: proposed with D-63 (round 123, 2026-11-05), awaiting the maintainer's approval.
- Reference: pprof_py v0.7.0 (`9320766`), `LogisticFERandomClusterModel` (`models/logistic/fe_random_cluster.py`,
  `inference/logistic/fe_random_cluster.py`, `measures/logistic/fe_random_cluster.py`).
- Plan: [../random-effects/plan.md](../random-effects/plan.md) §1.3 and part 4c (D-51 item 4).
- Builds on the three-stage model's stage 3 and its inference: `ThreeStage.compress` and `ThreeStage.stage3`
  (spec 2f-2, [three-stage-stage3.md](three-stage-stage3.md)), and `ThreeStagePipeline`'s tests, measures and
  intervals (spec 2f-3, [three-stage-pipeline.md](three-stage-pipeline.md)). pprof_py's `LogisticThreeStageModel`
  fits its stage 3 as this model and delegates its inference to it, so the statistics are those slices'.
- Out of scope:
  - `estimator = "he2013"` (slice 4c-2, OI-94);
  - persistence, the job runner and Python (slice 4c-3);
  - `funnel_limits` (OI-88) and `null_model` (OI-87).

## 1. Model

For record i of provider j(i) in cluster h(i), with features xᵢ and outcome yᵢ ∈ [0, 1]:

    logit P(yᵢ = 1 | αₕ) = γⱼ₍ᵢ₎ + xᵢᵀβ + αₕ₍ᵢ₎,   αₕ ~ N(0, σ²),

with β and σ held fixed. The provider effects γ maximize the marginal likelihood, the clusters' effects integrated
out. This is the three-stage model's stage 3 (spec 2f-2 §1), as a model of its own.

## 2. Inputs

| Role | Rule |
|---|---|
| outcome `y` | The likelihood's outcome, in [0, 1]. The three-stage model passes its adjusted outcome here |
| observed `obs` (optional) | The outcome counted by the tests, measures and intervals; `y` when absent (pprof_py's `obs_var`) |
| features | The columns of xβ, numeric; no intercept (γ carries it) |
| provider, cluster | Integral or string keys, non-null; ordered by key |

β, σ and the start of γ come from exactly one of two sources, as pprof_py's `_stage_values`:
- **Given:** β by feature (one value each), σ ≥ 0, and the start, one value per provider in key order.
- **Stages:** stage 1 is a `LogisticFE` fit on the same features. Its β is taken by feature name, and other names
  are refused. Stage 2 is a three-stage stage-2 fit (`ThreeStageStage2`) on the same provider and cluster keys:
  σ is its cluster SD, and the start is its provider BLUPs plus its intercept, matched by key. A provider it lacks
  is refused.

Passing one stage alone, or both stages and given values, is refused. pprof_py takes stage 2 from a
`LogisticRandomEffectModel` with the provider and cluster factors. pprof_spark's logistic random-effect model gets
cluster factors in part 4d (OI-93), so until then stage 2 is the three-stage model's own fit (D-41), the same model by
the same code.

Rows with a missing or non-finite value fail with counts per column, as X-038 set. The model uses every row: the
three-stage model's preparation, which excludes small providers and cells, is not part of it.

## 3. Options

| Option | Default | Use |
|---|---|---|
| `nNodes` | 20 | Gauss–Hermite nodes per cluster, adaptive at each cluster's posterior mode |
| `maxIter` | 10,000 | Newton iterations |
| `tol` | 10⁻⁵ | The convergence bound on the largest free component of the score |
| `bound`, `boundMode` | 10, `relative` | γ held within ±`bound` of its median (`relative`) or of 0 (`absolute`) |
| `estimator` | `marginal` | `he2013` fails, naming slice 4c-2 |

pprof_py's `convergence_criterion` applies to `he2013` alone. The wrappers accept it and ignore it for `marginal`, as
pprof_py does.

## 4. Estimation

`marginal`, as stage 3 (spec 2f-2 §2 to §4):
- the records are compressed once into provider × cluster cells, with offset-bin moments of xβ;
- the kernel runs a projected Newton with the sparse Hessian and a line search on the adaptive Gauss–Hermite
  marginal likelihood;
- cells go to the driver or, when large, to cluster-local executors, as stage 3 does.

The outputs:
- γ̂ by provider;
- the clusters' posterior means and variances of α;
- β and σ as used;
- the fitted values expit(γ̂ⱼ + E[αₕ | y] + xᵢᵀβ);
- the marginal log-likelihood;
- the iterations, the final largest score, convergence, and warnings.

## 5. Inference

| Method | pprof_spark |
|---|---|
| `summary(stage1, covariates, level, null, alternative)` | Stage 1's Wald summary of β (slice 2b). It needs the stage-1 fit whose β was used, compared within 10⁻¹⁰ relative and 10⁻¹² absolute; without one it fails, as pprof_py |
| `test` | Stage 3's tests (spec 2f-3 §3): `exact` (the default; the cluster mixture), `poibin_exact` and `resampling`, at `reference` `median` by default, in slice 2c's table, with the observed outcome's counts |
| `calculate_confidence_intervals` | Option `gamma` (the inverted test's limits on γ) and option `SM` (indirect and direct ratios and rates through those limits), with `test_method` `exact` or `poibin_exact` (spec 2f-3 §5) |
| `calculate_standardized_measures` | Spec 2f-3 §4 |

## 6. Edge cases

| Case | pprof_spark |
|---|---|
| σ = 0 | The clusters' effects are 0, and γ maximizes the fixed-effect likelihood with offset xβ |
| A provider whose outcomes are all 0 or all 1 | γ̂ stops at the bound, as pprof_py |
| One provider, or one cluster | Fits, as pprof_py; the tests need no second level |
| Fractional `y` | Fitted as stage 3 fits the adjusted outcome; the tests count `obs`, rounded as `poibin_tails` |

## 7. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `LogisticFERandomClusterModel(n_nodes, max_iter, tol, bound, bound_mode, estimator, convergence_criterion)` | §3's options |
| `fit(data, y_var, x_vars, provider_var, cluster_var, stage1, stage2, beta, sigma, gamma_init, obs_var)` | §2 |
| `gamma_`, `alpha_mean_cluster_`, `alpha_var_cluster_`, `fitted_`, `loglik_`, `iterations_`, `convergence_`, `converged_` | §4's outputs |
| `summary`, `test`, `calculate_confidence_intervals`, `calculate_standardized_measures` | §5 |
| `funnel_limits` | Not in this slice (OI-88) |

## 8. Validation plan

- The three-stage fixtures (`fixtures/three-stage`) already hold stage 3's fit and inference from pprof_py's
  `LogisticThreeStageModel`, which is this model. The standalone model, given the fixtures' β, σ and start, must
  reproduce stage 3's γ̂, log-likelihood, tests, measures and intervals at the three-stage classes (D-40, D-43).
- A new fixture family, `fixtures/fe-random-cluster`, comes from pprof_py's `LogisticFERandomClusterModel` directly:
  - given values different from any stage's: β shifted and σ chosen;
  - an `obs_var` different from `y_var`;
  - `boundMode` `absolute`;
  - text keys.
  Its fixture round proposes the classes.
- The given and stage sources agree bit for bit when their values agree, and the fit is invariant to row order and
  partitioning bit for bit, as stage 3 is.

## 9. Known discrepancies

None new: the three-stage model's approved discrepancies on stage 3 and its inference apply (specs 2f-2 and 2f-3).
