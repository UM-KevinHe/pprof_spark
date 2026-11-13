# Specification: the logistic random-effect model with cluster factors (slice 4d-2)

- Status: approved with D-71 (2026-11-05); fixtures and calibration in round 140 (D-72, approved); the kernel in round 141, the engine's fit and tests in round 142, the measures, intervals and profiles in round 143.
- Plan: [../random-effects/cluster-terms-plan.md](../random-effects/cluster-terms-plan.md) (D-68), slice 4d-2.
- References: pprof_py v0.7.0 (`9320766`) `LogisticRandomEffectModel(...).fit(..., cluster_vars=...)`
  (`models/logistic/random_effect.py`; `inference/logistic/random_effect.py`: `test`, `_get_posterior_var`;
  `measures/logistic/random_effect.py`); lme4 1.1-35.1 `glmer(y ~ x + (1 | provider) + (1 | cluster), family =
  binomial, nAGQ = 1)`.
- Builds on slices 4b-1 and 4b-2 (one provider factor), on the three-stage model's crossed elimination
  (`ThreeStageGlmm`, which fits the provider and one cluster factor with no features), and on 4d-1's elimination.

## 1. Model

For record i with outcome yᵢ ∈ [0, 1], offset oᵢ, features xᵢ, provider j(i) and level h_k(i) of each cluster
factor k:

    logit P(yᵢ = 1 | b) = oᵢ + xᵢᵀβ + b⁽⁰⁾ⱼ₍ᵢ₎ + Σₖ b⁽ᵏ⁾ₕₖ₍ᵢ₎,   b⁽ᵏ⁾ ~ N(0, σₖ²) independently.

Factor 0 is the provider. σ = (σ₀, σ₁, …) and β are estimated by pprof_py's two stages:
1. **nAGQ = 0:** σ is searched with β and the modes u updated together by penalized IRLS;
2. **nAGQ = 1 (the default):** the Laplace deviance is minimized over (σ, β), with the modes found by PIRLS at each
   point.

The Laplace deviance is lme4's: −2 log p(y | û) + ‖û‖² + log det H, with H = I + ΛZᵀWZΛ at the modes and Λ the
diagonal of σ expanded to each factor's levels.

## 2. The penalized system

H has 4d-1's block structure, with the PIRLS weights μ(1 − μ) in place of the prior weights. Every solve
eliminates the provider block onto the cluster levels and factors their Schur complement densely (4d-1 §2). The
posterior variance of a factor's effects is σₖ²·diag(H⁻¹) at the modes, as pprof_py's `_get_posterior_var`:
- for a provider, 1/D₀ⱼ + D₀ⱼ⁻²·eⱼᵀA⁻¹eⱼ, with eⱼ its couplings to the cluster levels and A the Schur complement;
- for a cluster level, the diagonal of A⁻¹.

## 3. Passes and paths

Each PIRLS iteration needs, from the rows at the current η:
- each provider × cluster cell's weight total;
- each factor level's sums of the weights times the working response;
- XᵀWX, XᵀW times the working response, and XᵀWZ by level.

They come from the blocks on the driver's copy when the rows fit half the driver budget (round 130, OI-99), or
else from one pass. Either way the same functions run in the same order, so both paths give the same bits. The
cells and the cluster block must fit the driver budget (OI-59).

## 4. The searches

- Stage 1 (nAGQ = 0) searches σ with `BoundedQuasiNewton` and central differences, from pprof_py's start.
- Stage 2 searches (σ, β) from stage 1's result, as 4b-1 does, with central differences in σ and the analytic β
  gradient of the Laplace deviance at the modes.

With one factor, 4b-1's searches run unchanged, so a fit without cluster factors stays bit for bit what 4b gives.
The bound `sigmaUpper` applies to every factor. A factor whose σ̂ is 0 is reported at the boundary.

## 5. Outputs

| pprof_py | pprof_spark |
|---|---|
| `coefficients_`, `summary` (Wald z) | β̂ and Var(β̂), the information conditional on the modes (X-043's rule) |
| `sigma_` (by factor) | σ̂ by factor |
| `get_random_effects(var)`, `_get_posterior_se(var)` | Each factor's table: key, `records`, `blup`, `variance`, `se` |
| `fitted_`, `predict(..., use_re, re_vars)`, the deviance, AIC, BIC | As 4b-1, with every factor's BLUPs in the fitted values and the named ones in predictions |

## 6. Inference

As pprof_py's `test` with cluster factors, for the provider factor:

| Method | Null for provider j |
|---|---|
| `wald` | The provider's BLUP and posterior SE |
| `poibin_exact` | The plug-in Poisson-binomial with pᵢ = expit(γ + mᵢ + xᵢᵀβ), mᵢ the sum of the row's cluster BLUPs |
| `resampling` | Monte Carlo draws of the row's cluster effects from their posteriors (means mᵢ, variances vᵢ). At the Monte Carlo floor, the row mixture's exact tails, as 4b-2 and pprof_py do |
| `exact` | He et al.'s cluster mixture over the one cluster factor's posteriors, as the three-stage model's; refused with two or more cluster factors, as pprof_py refuses it |

The measures and `SM` intervals keep 4b-2's definitions, which use the provider BLUPs and xβ alone, as pprof_py's
measures do. Option `alpha` uses the provider's posterior SEs. `profile_sigma(var)` profiles any factor's σ by the
Laplace deviance, minimizing over β and the other σ's, as 4b-2's profile does for one σ.

## 7. Validation plan

- Fixtures: a new family, `fixtures/logistic-re-clusters`, as 4d-1's. Its cases:
  - one cluster factor; two cluster factors;
  - text keys; an offset;
  - clusters nested in providers;
  - a cluster factor whose σ̂ is 0.
  Each has pprof_py's fits at its defaults and tightened, `glmer`'s Laplace fit, the tests (all four methods),
  the measures, the intervals and the profile limits on the tightened fit, and negative controls. The fixture round
  proposes the classes.
- The kernel against both references; a fit at given (σ, β) for lockstep inference checks; the optimality check of
  D-59; invariance bit for bit; the refusals.

## 8. Known discrepancies

None expected.

## 9. Fixture findings (round 140)

- `fixtures/logistic-re-clusters` holds six cases (D-72). Regenerating them reproduces every file byte for byte.
- pprof_py's tightened Laplace fits and glmer's agree:
  - σ within 4·10⁻⁶ relative and β within 6·10⁻⁷;
  - the log-likelihoods within 10⁻⁹ absolute;
  - every factor's BLUPs and posterior SEs at T-opt.
  On lgc-boundary both put the cluster factor's σ at 0.
- pprof_py's Laplace deviance is its `stage2_objective_`, −2 log-likelihood with every constant; glmer's
  `deviance()` differs, so the comparisons use the log-likelihood.
- pprof_py's stage 1 falls back from BOBYQA to Powell without nlopt, as in 4b's fixtures; its default fits still lie
  within 0.005 of T-opt of the tightened ones.

## 10. Implementation notes (round 141, the kernel; no statistical change)

- `numerics.kernels.CrossedSystem` is the elimination of §2, built from each level's diagonal value and the level
  pairs' off-diagonal values. It gives log det, solves and the inverse's diagonal. 4d-1's linear kernel keeps its
  own copy of the same steps, so that its tested code stays unchanged.
- `numerics.kernels.LogisticREClusters` holds the rows in memory, with each factor pair's level pairs precomputed.
  - `modes` runs PIRLS for u at (σ, β), or for (β, u) jointly for nAGQ = 0 through the β system's Schur
    complement, with step halving on the penalized deviance and a relative tolerance of 10⁻¹³.
  - `laplace` gives lme4's Laplace deviance.
  - `fit` runs the two stages: σ by the nAGQ = 0 deviance from σ = 1, then (σ, β) by the Laplace deviance. Both use
    `BoundedQuasiNewton` with central differences in every coordinate. §4 planned the analytic β gradient; central
    differences reach the same optimum within the fixtures' classes.
  - The outputs are the BLUPs σₖuₖ, the posterior variances σₖ²·diag(H⁻¹) and Var(β̂) from the β system
    conditional on the modes.
- `LogisticREClustersKernelSuite` builds every case's rows without Spark. All six cases passed on the first run:
  σ̂, β̂, every factor's BLUPs and posterior SEs (T-opt), and the log-likelihood (T-fn), against both pprof_py and
  glmer; D-59's optimality check against both optima.

## 11. Implementation notes (round 142, the engine's fit and tests; no statistical change)

- `engine.logistic.LogisticREClusters.fit(df, LogisticREClusterSpec, LogisticREClusterOptions)`:
  - it validates, fingerprints and counts in one pass, then collects the rows to the driver in canonical order (the
    row identifier, else the levels and values);
  - it runs the kernel's two stages from the outcome's logit;
  - its outputs are the coefficients, σ̂ by factor, each factor's table (key, `records`, `blup`, `variance`, `se`),
    the deviances, AIC and BIC (p + factors parameters, as pprof_py's) and the boundary factors.
  This is §3's driver path only. A fit whose rows exceed half the driver budget fails, naming OI-102, until the
  passes over distributed blocks are written.
- `fitAt` gives the same outputs at given (σ, β) through the kernel's new `at`, which `fit` now also ends with.
- `summary` gives Wald z for β. `test` covers `wald`, `poibin_exact`, `resampling` and `exact` as §6 defines them,
  with the clustered Poisson-binomial distributions of the three-stage model, `invertDecreasing` for the limits,
  and the row mixture at the Monte Carlo floor. `reference` is 0 by default, as pprof_py's.
- `LogisticREClustersSuite` (10 tests):
  - three fits against pprof_py and glmer;
  - every case in lockstep at pprof_py's tightened (σ, β): `summary` and every test method at D-72's classes;
  - `exact` refused with two cluster factors;
  - invariance bit for bit.
  The lockstep's first run failed on every case because the engine's default reference was the median, where
  pprof_py's is 0. The generator had also described the fixtures' tests as at the median; the description now says
  0, and the fixture data are unchanged.
- The measures, the intervals and the profile come next.

## 12. Implementation notes (round 143, the measures, intervals and profiles; no statistical change)

- `measures(fit, kinds, reference)`, `alphaIntervals(fit, level)` and `smIntervals(fit, kinds, measures, reference,
  alternative, level)` keep 4b-2's definitions on the driver's rows, as §6 says: the provider BLUPs and pprof_py's
  `xbeta_` (offset and xβ̂) alone.
  - The direct sums are exact, since the rows are on the driver; 4b-2's binned sums (X-026) serve its distributed
    path.
  - `reference = mean` is the record-weighted mean in both the measures and the intervals, as 4b-2 decided (X-046).
  - One-sided indirect intervals are one-sided, as X-045 decided.
- `profileSigma(fit, factor, level)` follows pprof_py's `profile_sigma(var)`:
  - at each candidate σ of the factor, the Laplace deviance is minimized over β and the other factors' σ's by
    `BoundedQuasiNewton`, warm-started from the previous point;
  - the limits are found by Brent's method to 10⁻¹⁰, the upper bracket doubling from σ̂ + max(0.1, σ̂), and the lower
    limit is 0 when the deviance at σ = 0 stays within χ²₁(level).
- `LogisticREClustersSuite` gains a lockstep test on all six cases at pprof_py's tightened (σ, β). The measures and
  options `alpha` and `SM` agree at T-meas, and every factor's profile limits at T-opt. It passed on the first run.
- Slice 4d-2's estimation and inference are complete. Persistence, the jobs and Python come with slice 4d-3 (D-68).

## 13. Round 144: the fitted values and predictions §5 promised

Round 143 called this slice's estimation and inference complete, but §5's fitted values and predictions had not
been written.
- `LogisticREClusters.fitted(df, fit)` gives 4b's columns, with every factor's BLUP in η: `xbeta`, `eta`, `fitted`,
  `residual`, `pearson` and `deviance_residual`.
- `LogisticREClusters.predict(df, fit, reVars, withOffset, type)` uses the BLUPs of the named factors, unseen levels
  contributing 0, on the response or link scale. Keys match by value, as 4b's predictions do after X-044.
- `LogisticREClustersSuite` gains a test on four cases:
  - the fitted values against pprof_py's at its tightened (σ, β) at T-meas;
  - predictions with every factor equal to the fitted values within a few ulps, and without effects equal to
    expit(xβ̂);
  - an unknown factor refused.
