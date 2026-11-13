# Specification: the linear random-effect model with cluster factors (slice 4d-1)

- Status: approved with D-69 (2026-11-05); fixtures and calibration in round 136 (D-70, proposed).
- Plan: [../random-effects/cluster-terms-plan.md](../random-effects/cluster-terms-plan.md) (D-68), slice 4d-1.
- References: pprof_py v0.7.0 (`9320766`) `LinearRandomEffectModel(...).fit(..., cluster_vars=...)`
  (`models/linear/random_effect.py`: `_build_H_C`, `_solve_inner`, `_objective_from_state`, `predict`); lme4
  1.1-35.1 `lmer(y ~ x + (1 | provider) + (1 | cluster), REML = TRUE or FALSE)`.
- Builds on slice 4a-1 ([random-effect-estimation.md](random-effect-estimation.md)): the same model with one more
  random intercept for each cluster factor. A fit without cluster factors is 4a-1's, unchanged bit for bit.

## 1. Model and estimand

For record i, with prior weight wᵢ, offset oᵢ, provider j(i) and level h_k(i) of each cluster factor k:

    yᵢ = oᵢ + xᵢᵀβ + b⁽⁰⁾ⱼ₍ᵢ₎ + Σₖ b⁽ᵏ⁾ₕₖ₍ᵢ₎ + εᵢ,   b⁽ᵏ⁾ ~ N(0, σ²θₖ²) independently,   εᵢ ~ N(0, σ²/wᵢ).

Factor 0 is the provider. θ = (θ₀, θ₁, …) maximizes the profiled REML (by default) or ML criterion of pprof_py's
`_objective_from_state`, which is lme4's up to constants, over [0, `thetaUpper`] for each factor. β, σ and the
penalized modes u follow at θ̂, and the BLUPs are b⁽ᵏ⁾ = θₖ·u⁽ᵏ⁾, as pprof_py and lme4 report them.

## 2. The penalized system

With Λ = diag(θ expanded to each factor's levels) and Z the indicator matrix of all factors' levels:
- H = I + ΛZᵀWZΛ has a diagonal block for each factor, with entries 1 + θₖ²·(the level's weight total). It also
  has off-diagonal blocks θₖθₗ·(the weight total of each pair of levels that share records), for every pair of
  factors.
- C = XᵀWZΛ, d = ΛZᵀW(y − o), and the β equations' Schur complement is XᵀWX − CH⁻¹Cᵀ.

pprof_spark solves the same system by block elimination:
- the provider block, which is diagonal, is eliminated onto the cluster levels;
- the Schur complement on the cluster levels is factored densely by Cholesky's method.
This gives log det H, H⁻¹d and H⁻¹Cᵀ, and from them β, u, the penalized residual sum of squares and the REML
term's log det.

## 3. Sufficient statistics and passes

One pass over the records computes everything the search needs:
- for every factor's levels: the sums of w, wx and w(y − o);
- for every pair of factors' level pairs that share records: the sum of w;
- XᵀWX, XᵀW(y − o), (y − o)ᵀW(y − o), Σ log w and n.
The penalized residual sum of squares at any θ expands into these sums, so the search runs on the driver without
further passes, as slice 4a-1's does on its provider sums.

The driver holds the level pairs, at most the number of distinct record cells, and the dense cluster block. A fit
whose cluster levels or level pairs exceed the driver budget fails, naming OI-59. Every sum is compensated and taken
in canonical order, so the fit is invariant to row order and partitioning bit for bit, as 4a's is.

## 4. The search

With one factor, slice 4a-1's search runs unchanged. With several, θ is found by `BoundedQuasiNewton` on [0,
`thetaUpper`]ᵏ. The gradient is taken by central differences of the profiled criterion, which is smooth inside the
box. The search starts from pprof_py's start, θ = 1 for every factor. A factor whose θ̂ is 0 is reported at the
boundary; its BLUPs are 0 and its off-diagonal blocks vanish, as in pprof_py's `_build_H_C`.

pprof_py searches with Nelder–Mead (or Powell) to its default tolerance, so its optimum is a reference within its
tolerance. The fixtures' tightened pprof_py and lme4 fits are the targets, as in 4a-1 (D-53).

## 5. Outputs and methods

| pprof_py | pprof_spark |
|---|---|
| `coefficients_["beta"]`, `standard_errors`, `fixed_effects_table`, `conf_int`, `summary` | As slice 4a-2's fixed-effect tables, from the Schur complement's inverse times σ̂² |
| `theta_`, `sigma_`, `random_effect_sd_` (σ̂θ̂ per factor), `get_random_effect_sd(var)` | θ̂ and σ̂θ̂ₖ by factor name |
| `get_random_effects(var)` | Each factor's BLUP table: key and `blup`; the provider's keeps 4a's table name |
| `fitted_values`, `predict(X, use_re, re_vars)` | Fitted values with every factor's BLUP; predictions with the factors `reVars` names, unseen levels contributing 0 |
| `loglike_`, `aic_`, `bic_`, `objective_`, `converged_` | The same, with one variance parameter per factor in AIC and BIC |
| `test`, `calculate_standardized_measures`, `calculate_confidence_intervals`, provider SEs | Refused for a fit with cluster factors, as pprof_py refuses them, naming its rule |

## 6. Inputs and validation

`LinearRESpec` gains `clusters: Seq[String]`, empty by default. Each cluster column is integral or string and
non-null; it must differ from the provider column and from the other cluster columns. The validation of 4a-1 (X-038)
covers the new columns. A cluster factor with one level is fitted, with its θ̂ typically at 0.

## 7. Validation plan

- Fixtures (the fixture round proposes the classes): new cases in `fixtures/linear-re` with
  - one cluster factor;
  - two cluster factors;
  - text keys;
  - prior weights;
  - an offset;
  - clusters nested in providers;
  - a cluster factor whose θ̂ is 0.
  Each has pprof_py's fit at its defaults and tightened, and `lmer`'s REML and ML fits.
- The kernel against both at the fixture round's classes, and the optimality check of D-59.
- Invariance to row order and partitioning bit for bit; a fit without cluster factors equal to 4a-1's bit for bit.
- The profiling refusals.

## 8. Known discrepancies

None expected.

## 9. Fixture findings (round 136)

- `fixtures/linear-re-clusters` holds seven cases (D-70). Regenerating them reproduces every file byte for byte.
- pprof_py's tightened REML and ML optima and `lmer`'s agree:
  - θ within 1·10⁻⁶ relative (0.004 of T-opt), β within 2·10⁻⁹ and σ within 3·10⁻⁹;
  - the criteria within 2·10⁻¹¹ absolute;
  - every factor's BLUPs at T-opt.
  On lrc-boundary both put the cluster factor's θ at 0, lme4 reporting a singular fit.
- pprof_py's default Powell search (tol 10⁻⁸) stops within 0.0012 of T-opt of its tightened optimum.
