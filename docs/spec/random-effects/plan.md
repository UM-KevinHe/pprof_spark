# Phase 4 plan: random-effect models (milestone M4)

Status: proposed as D-51 (round 94, 2026-10-21), awaiting the maintainer's approval.

D-50, as amended in round 94, puts M4 (priority P1) before M2 (P0's inference parity). This plan covers the three
random-effect models of pprof_py v0.7.0 in the maintainer's order: linear random effects, logistic random effects,
then fixed provider effects with random clusters. It maps each onto what pprof_spark already has, so that the
three-stage and fixed-effect code is reused rather than duplicated, and lists the decisions the work needs (§8).
Each slice then gets its own specification (NN-2) in `docs/spec/linear/` or `docs/spec/logistic/`.

## 1. What the references contain at the pin

Round 94 read the three model files with their inference and measures mixins, and ran the probes of §1.5.

### 1.1 `LinearRandomEffectModel`

`models/linear/random_effect.py`, with `inference/linear/random_effect.py` and `measures/linear/random_effect.py`:
a Gaussian linear mixed model with random intercepts, in lme4's formulation,

    y = offset + Xβ + Zb + ε,   b = σΛ(θ)u,   u ~ N(0, I),   ε ~ N(0, σ²W⁻¹),

with prior weights W and θ_k = σ_k/σ for each grouping factor k: the provider first, then `cluster_vars`.

- Constructor: `max_iter_inner` = 100 (kept for symmetry; the inner problem is one linear solve),
  `max_iter_outer` = 200, `tol_outer` = 1e-8, `theta_upper` = ∞, `reml` = True, `optimizer` = `"powell"` or
  `"nelder-mead"`.
- `fit(X, y_var, x_vars, provider_var, cluster_vars, offset_var, weights_var, include_intercept=True, reml=None)`:
  complete cases only (a row with a missing value in a used column is dropped); finite outcomes and covariates;
  weights finite and positive; REML needs n > p. θ starts at 1, β at weighted least squares.
- Objective: lme4's profiled deviance. With H(θ) = I + Λ′Z′WZΛ, C = X′WZΛ and PWRSS(θ) the penalized weighted
  residual sum of squares at the joint solution for (β, u):
  - ML: n(1 + log 2πσ̂²) + log|H| − Σ log w, with σ̂² = PWRSS/n;
  - REML: (n − p)(1 + log 2πσ̂²) + log|H| + log|X′WX − CH⁻¹C′| − Σ log w, with σ̂² = PWRSS/(n − p).

  It is minimized over θ ∈ [0, `theta_upper`] by scipy's Powell method, with xtol = ftol = `tol_outer`.
- Outputs: β̂ with covariance σ̂²(X′WX − CH⁻¹C′)⁻¹; σ̂, θ̂ and σ̂_k = θ̂_kσ̂; the BLUPs b̂; fitted values and
  residuals; `xbeta_` (offset + Xβ̂); the log-likelihood (under REML, −½ times the REML criterion), AIC and BIC with
  p + K + 1 parameters; `converged_`.
- Methods: `get_random_effects`, `get_sigma`, `get_random_effect_sd`, `standard_errors`, `fixed_effects_table` and
  `conf_int` (t with n − p degrees of freedom), `residual_standard_error`, `fitted_values`, `predict` (fixed effects
  by default; with `use_re`, the BLUPs of known levels and 0 for unknown ones, as lme4).
- Inference, for a fit with the provider factor alone (every profiling method refuses a fit with `cluster_vars`):
  - `summary(covariates, level, null, alternative)`: t with n − p − m degrees of freedom, as the fixed-effect
    model's, and p-values rounded to seven decimals;
  - `test(providers, reference=0.0, null_model, alternative, level, critical, interval)`: z = (b̂_j − γ₀)/se_j with
    se_j = √(s_j·σ̂²/n_j), s_j = σ̂²_α/(σ̂²_α + σ̂²/n_j) and n_j the provider's row count, a normal reference and the
    shared provider-test table (`effect_test`); γ₀ is 0, the random-effect mean, by default, or the median, the
    size-weighted mean or a number;
  - `calculate_confidence_intervals(providers, level, option, stdz, reference="median", alternative)`: `alpha`,
    b̂_j ± z·se_j (two-sided only); `SM`, the standardized differences at those bounds;
  - `calculate_standardized_measures(providers, stdz, reference="median")`: the indirect difference
    (Σ fitted − Σ(xβ̂ + γ₀))/n_j, which is b̂_j − γ₀, and the direct difference b̂_j − Σ_k n_k·b̂_k/n.

### 1.2 `LogisticRandomEffectModel`

A Bernoulli-logit GLMM with random intercepts, as `glmer`: logit P(y = 1 | u) = offset + Xβ + ZΛ(σ)u, u ~ N(0, I).

- Constructor: `max_iter_pirls` = 100, `tol_pirls` = 1e-8, `max_iter_outer` = 200, `tol_outer` = 1e-7,
  `sigma_upper` = ∞, `stage2` = True, `optimizer_stage1` = `"bobyqa"` (through nlopt; without it, as in the
  fixture environment, a warning and Powell), `optimizer_stage2` = `"nelder-mead"`.
- `fit(X, y_var, x_vars, provider_var, cluster_vars, offset_var, include_intercept=True)`: no weights; outcomes in
  [0, 1], fractional values included; complete cases.
- Two stages, as `glmer`: stage 1 (nAGQ = 0) optimizes σ with PIRLS over (β, u); stage 2 (nAGQ = 1, the default)
  runs Nelder–Mead over (σ, β) on the Laplace deviance, with PIRLS over u at each evaluation.
- Outputs: β̂ and its covariance, σ̂_k (also as `theta_`), the BLUPs of each factor, fitted values, residuals
  (deviance and Pearson residuals by method), `xbeta_`, the deviance; `predict`.
- Inference:
  - `summary(level)`: Wald z;
  - posterior variances σ̂²_k·diag(H⁻¹) at the mode (lme4's conditional variances);
  - `test(providers, test_method, reference=0.0, null_model, …)`: `wald` by default, (b̂_j − γ₀) over the
    posterior SD; `exact`, which needs exactly one cluster factor (Gauss–Hermite mixtures over the cluster effects,
    as stage 3's); `poibin_exact` (the other factors at their posterior means); `resampling`. The count methods
    compare the provider's observed count with its null distribution;
  - `calculate_confidence_intervals` (`alpha` or `SM`) and `calculate_standardized_measures`: the indirect ratio
    O_j/E_j with E_j = Σ expit(γ₀ + xβ̂) over the provider's rows, the indirect rate (the ratio times the overall
    rate, in percent, clipped to [0, 100]), and direct sums Σ expit(b̂_j + xβ̂) over every row;
  - `profile_sigma(var, level)`: profile-deviance limits for each σ_k, with the other parameters re-optimized by
    Nelder–Mead and the limits found by Brent's method at the χ²₁ quantile;
  - `funnel_limits`.

### 1.3 `LogisticFERandomClusterModel`

Stage 3 of the three-stage model as a model of its own: fixed provider effects γ with random cluster effects, β and
the cluster SD σ held fixed.

- Constructor: `n_nodes` = 20, `max_iter` = 10000, `tol` = 1e-5, `bound` = 10 with `bound_mode` `"relative"`
  (around the median) or `"absolute"`, `convergence_criterion` (used by `he2013` only), and `estimator` =
  `"marginal"` (the default) or `"he2013"`.
- `fit(data, y_var, x_vars, provider_var, cluster_var, stage1=None, stage2=None, beta=None, sigma=None,
  gamma_init=None, obs_var=None)`: β from a fitted `LogisticFixedEffectModel`, by covariate name; σ (its cluster SD)
  and the start (its provider BLUPs plus its intercept) from a fitted `LogisticRandomEffectModel` whose factors are
  exactly the provider and the cluster; or the three values given explicitly. `obs_var` names the observed outcome
  for the counts when `y_var` is an adjusted one.
- Estimators: `marginal` (adaptive Gauss–Hermite quadrature at each cluster's posterior mode, projected Newton with
  a sparse Hessian and a line search) and `he2013` (He et al. 2013: posterior moments at fixed nodes and a
  per-provider Newton step with second-order terms).
- Methods: `summary(stage1, …)`, `test`, `calculate_confidence_intervals`, `calculate_standardized_measures`,
  `funnel_limits`.
- pprof_py's `LogisticThreeStageModel` fits its stage 2 as a `LogisticRandomEffectModel` without covariates, with
  the cluster factor and stage 1's offset, and its stage 3 as this model with `stage1` and `stage2`; it delegates
  its tests, measures and funnel limits to stage 3.

### 1.4 R

- lme4 1.1-35.1 (`reference/REFERENCE.lock`): `lmer` (REML by default), `glmer` (nAGQ = 1 by default) and
  `ranef(condVar = TRUE)`.
- R pprof 1.0.3 at `1efc404`: `linear_re` and `logis_re` call `lmer` and `glmer`, and each has `summary`, `test`,
  `confint` and `SM_output` methods. `linear_cre` and `logis_cre`, which take within–between covariates
  (`wb.char`), have no pprof_py counterpart.

### 1.5 Probes (round 94, sandbox; not fixtures)

- Linear: 30 providers of 5 to 60 rows, two covariates, and no weights or weights uniform on [0.5, 2]; pprof_py
  against `lmer`, REML and ML. θ̂, σ̂, β̂ and the BLUPs agree within 8·10⁻⁸ relative and the deviances within 10⁻⁸.
  Without weights the BLUP standard errors agree within 3·10⁻⁸; with weights, pprof_py's are 18% larger than lme4's
  conditional SDs (0.304 against 0.256 for one provider, REML), because n_j counts rows where the model's
  conditional variance uses the provider's weight total (X-037).
- Logistic: 40 providers of 20 to 150 rows (n = 3,641), two covariates; no nlopt, so stage 1 ran by Powell.
  pprof_py's default fit against `glmer`'s (nAGQ = 1): σ̂, β̂ and the first BLUP within 4.2·10⁻⁵ absolute, the
  first posterior SD within 2.2·10⁻⁵ relative, and pprof_py's Laplace deviance 1.9·10⁻⁴ below `glmer`'s: both stop
  short of the optimum. `stage2=False` against nAGQ = 0: σ̂ within 2.6·10⁻⁶ absolute, β̂ and the first BLUP within
  2·10⁻⁷. The two stages' intercepts differ by 1.5%, so parity needs the Laplace optimum itself.

## 2. What pprof_spark already has

| Need | Existing code | Use in Phase 4 |
|---|---|---|
| Within co-moments and provider means in one pass | `LinearFE` (PROJECT_CONTEXT §7.5) | 4a, with prior weights added (the linear fixed-effect models of both packages have none) |
| Provider tests, the shared result table, reference effects | `LinearProviderTests`, `LogisticProviderTests` (slice 2c's flags, `critical` and columns) | 4a-2, 4b-2 |
| Indirect and direct logistic measures; binned Taylor sums (X-026) | `LogisticStandardization`, `TaylorBins` | 4b-2's direct sums over every row |
| Blocked Newton on provider effects and β, with a Schur complement | `LogisticFE`, `LogisticBlocks` | 4b's PIRLS over (β, u) when rows are not grouped by provider |
| Bounded quasi-Newton, PIRLS, the Laplace deviance, profile limits | `BoundedQuasiNewton`, `ThreeStageGlmm` (`evaluate`, `fit`, `profile`, `profileInterval`) | 4b-1's optimizer, 4b-2's `profile_sigma`, 4d's elimination |
| Stage 3, `marginal`, on compressed cells | `ThreeStage.compress`, `ThreeStage.stage3`, `ThreeStageKernel`, `GaussHermite`, `SparseSymmetric` | 4c-1 |
| Stage 3's tests (`exact`, `poibin_exact`, `resampling`), measures and intervals | `ThreeStagePipeline`, `ClusteredPoissonBinomial`, `PoissonBinomial` | 4c-1; 4b-2's count tests |
| Persistence, job runners, the py4j facade | the `FitIO` formats, the `Job` runners, `PythonApi`, `python/` | each part's third slice |
| Synthetic data whose provider effects are γ̄ + σ_γ·Z | `pprof.spark.bench.synthetic` (round 92) | σ̂-recovery tests and the S1 smokes |

New code: the linear profiled deviance on provider summaries with weights, the BLUPs and their conditional
variances; the logistic Laplace fit with covariates, where β moves inside the optimization (§4.2); a standalone
entry point for stage 3; `he2013`.

## 3. Parts and slices

Each part closes at parity-verified with its own gate review (`docs/gates/phase-4a.md` and so on) and stays
`Experimental` until it is scale-verified (D-28, D-50). Each slice runs, in order: specification, fixtures with
calibration, code, parity tests, the Python wrapper for its methods, documents. An S1 smoke closes each part.

| Slice | Content | References |
|---|---|---|
| 4a-1 | Linear estimation: one provider intercept, offsets, prior weights, REML and ML; β̂ and its covariance, σ̂, θ̂, σ̂_α, the BLUPs, the deviance, log-likelihood, AIC and BIC, fitted values, residuals, `predict` | pprof_py; `lmer` |
| 4a-2 | `summary`, `fixed_effects_table`, `conf_int`; provider tests, intervals and standardized differences | pprof_py; R pprof's `linear_re` family where it follows pprof_py |
| 4a-3 | Persistence, the job runner, Python | — |
| 4b-1 | Logistic estimation: one provider intercept with covariates and offsets; nAGQ = 0, then the Laplace fit (`stage2`); β̂ and its covariance, σ̂, the BLUPs and their posterior variances, the deviance, residuals, `predict` | pprof_py; `glmer` |
| 4b-2 | `summary`; tests `wald`, `poibin_exact` and `resampling`; intervals; standardized ratios and rates; `profile_sigma` | pprof_py; R pprof's `logis_re` family |
| 4b-3 | Persistence, the job runner, Python | — |
| 4c-1 | Fixed effects with random clusters: β, σ and the start given, or taken from a `LogisticFE` fit and a three-stage stage-2 fit; `marginal`; `summary`; tests, measures and intervals by the three-stage code | pprof_py |
| 4c-2 | `estimator = "he2013"` and its convergence criteria (OI-94) | pprof_py |
| 4c-3 | Persistence, the job runner, Python | — |
| 4d | Cluster terms (OI-93): crossed random intercepts in both models (estimates only for the linear model, whose profiling pprof_py refuses), the `exact` test, and a general logistic fit with one cluster factor as 4c's stage 2 | pprof_py; `lmer`; `glmer` |

## 4. Distributed design

### 4.1 Linear: one pass, then the driver

With a single provider intercept, the fit depends on the data only through, for each provider j, the weight total
W_j = Σw and the weighted means x̄_j and ȳ_j of x and of y − offset, and through the pooled weighted within
co-moments W_xx, W_xy and W_yy, Σ log w and n. For each θ, with h_j = 1 + θ²W_j:

- A(θ) = W_xx + Σ_j (W_j/h_j)·x̄_j·x̄_j′, which equals X′WX − CH⁻¹C′;
- β̂(θ) solves A(θ)β = W_xy + Σ_j (W_j/h_j)·x̄_j·ȳ_j;
- PWRSS(θ) = (W_yy − 2β̂′W_xy + β̂′W_xx·β̂) + Σ_j (W_j/h_j)(ȳ_j − x̄_j′β̂)², a sum of non-negative terms;
- log|H| = Σ_j log h_j, and REML adds log|A(θ)|;
- b̂_j = θ²W_j(ȳ_j − x̄_j′β̂)/h_j, with conditional variance σ̂²θ²/h_j.

So the fit takes one pass over the rows (`LinearFE`'s, with weights), then O(mp²) work for each θ: on the driver
when the provider table fits its budget, otherwise one deterministic aggregation over the cached table per
evaluation. θ is one-dimensional, so a bracketed method (Brent's) on [0, `theta_upper`], with the boundary θ = 0
checked, reaches the optimum well inside the references' optimizer limits. A pass at the optimum gives fitted
values and residuals.

### 4.2 Logistic: the rows at every evaluation

Stage 2 of the three-stage model iterates on compressed cells because its offset is fixed. With covariates, β moves
inside the optimization and x′β varies within a cell, so the offset-bin compression no longer applies: every
evaluation of the Laplace deviance reads the rows. With a single provider intercept, the deviance and its gradient
in (σ, β) are sums over providers: given (σ, β), each u_j is a one-dimensional Newton problem on that provider's
rows, and differentiating its stationarity condition gives the gradient. 4b-1's specification chooses between
grouping each provider's rows in one partition, so that one pass gives the deviance and its gradient, and
`LogisticFE`'s layout with one pass per PIRLS iteration; its S1 smoke measures the cost. `BoundedQuasiNewton` then
minimizes over (σ, β), starting from the nAGQ = 0 optimum.

### 4.3 Fixed effects with random clusters: cells

As the three-stage model's stage 3 (D-38, D-40): the records are compressed once into provider × cluster cells with
offset-bin moments of xβ, and the kernel iterates on cells, on the driver or on cluster-local executors.

### 4.4 Cluster terms (4d)

Crossed intercepts make H non-diagonal. The linear model then needs provider × cluster sums of the weights and a
sparse joint system, eliminated onto the smaller set of levels as stage 2 does (OI-59 for very large level sets).
The logistic model needs the same elimination inside its row-level PIRLS, and diag(H⁻¹) for the posterior
variances.

## 5. Discrepancies and points to verify

Proposed with this plan:

- X-037 (class B): the linear model's BLUP standard errors use the row counts n_j also when the fit has prior
  weights, where the fitted model's conditional variance, lme4's, uses the weight totals W_j. Proposed: the
  corrected behavior, σ̂²θ̂²/(1 + θ̂²W_j), which equals pprof_py's without weights, documented as a divergence.

For the slice specifications to settle, after running both references:

- the logistic intervals' `reference = "mean"` takes the unweighted mean of the BLUPs, where the model's tests and
  measures take the size-weighted mean (0 against −0.0028 in round 94's probe);
- the linear model's two degrees of freedom for covariates (n − p − m in `summary`, n − p in
  `fixed_effects_table` and `conf_int`), which pprof_spark follows method by method;
- the logistic covariance of β̂: pprof_py's against `vcov` of the `glmer` fit;
- the boundary σ = 0 in both models (zero standard errors, degenerate tests), and providers whose outcomes are all
  0 or all 1;
- fractional outcomes, which lme4 treats differently (informational only, as in spec 2f-3 §9).

## 6. Fixtures and tolerance classes

- New fixture families `fixtures/linear-re` and `fixtures/logistic-re`, made by `reference/fixtures/generate.py`
  and R scripts (lme4, R pprof), with byte-identical regeneration and `calibrate.py`. The designs cover weights,
  offsets, σ near 0, unbalanced and Zipf sizes, and text keys. 4c reuses `fixtures/three-stage` and adds a case
  with explicit β, σ and start values.
- The estimates are compared with references at tight settings (pprof_py's tolerances tightened; `lmer` and
  `glmer` with tightened optimizer control, as round 51 did for stage 2); default settings give informational
  comparisons.
- Classes:
  - 4a's estimates against tight references: T-coef for β̂ and the BLUPs, T-var for the variances and θ̂, T-fn for
    the deviance, as calibration confirms; otherwise a class proposed under NN-9.
  - 4b's estimates (σ̂, β̂, the BLUPs, posterior SDs, the deviance): T-opt (D-42), re-calibrated with covariates in
    4b-1's fixture round.
  - Inference and measures in lockstep, from the references' own estimates, at T-meas and T-test; end to end at the
    class of the estimates.
  - 4c: the three-stage classes (D-40, D-43).

## 7. Validation and S1

Parity at the function level (the profiled deviance, PIRLS, gradients), in lockstep and end to end; reproducibility
R0 to R2; layer T8 (Spark Connect); σ̂ recovery on `bench.synthetic` data at large m. Each part's S1 smoke is one
point near 10⁷ rows with S0's checks (`docs/spec/scale/plan.md` §9), prepared here and run by the maintainer.

## 8. Decisions proposed (D-51)

1. Parts and order: 4a, 4b and 4c in the maintainer's order, then 4d. Each closes at parity-verified with its own
   gate and an S1 smoke, and stays `Experimental` until it is scale-verified.
2. 4a's scope: one provider random intercept with offsets and prior weights; `cluster_vars` in 4d.
3. 4b's scope: one provider random intercept with covariates and offsets; the Laplace fit by default
   (`stage2 = True`) and nAGQ = 0 as an option; tests `wald`, `poibin_exact` and `resampling`; `profile_sigma`.
   `exact`, which needs a cluster factor, comes in 4d.
4. 4c's scope: stage 3 as a model of its own on the three-stage code, given β, σ and the start or taken from
   pprof_spark's fits; `he2013` as slice 4c-2. The three-stage spike (D-38) planned `he2013` for slice 2f-4, whose
   specification (D-43) left it out without a decision; OI-94 records it.
5. Inference beyond the models' own: the theoretical null only, as in the existing models. Null models, the
   empirical null and funnel limits come with P0, for every model (OI-87, OI-78, OI-88); no plots.
6. References: pprof_py v0.7.0 at default and tight settings; lme4 1.1-35.1 with tightened control for the
   fixtures; R pprof 1.0.3's `linear_re` and `logis_re` families for inference where they follow pprof_py's
   definitions, as X-036 set for the linear fixed-effect model.
7. Estimation targets: the references' optima, not their optimizer paths (class D, as X-030): a bracketed
   one-dimensional method for the linear θ; `BoundedQuasiNewton` with analytic gradients of the Laplace deviance
   for the logistic (σ, β), with PIRLS converged at every evaluation.
8. Tolerance classes as in §6.
9. REML by default for the linear model, as pprof_py and `lmer`, with ML as an option; the logistic model has the
   Laplace (maximum-likelihood) fit only.
10. X-037: the corrected behavior (§5).

## 9. Out of scope for M4

Plots; funnel limits, null models and the empirical null (P0); R pprof's `linear_cre` and `logis_cre`; random
slopes, which pprof_py does not fit; `FrailtyCoxPH` (P2).
