# Specification: the logistic random-effect model with cluster factors (slice 4d-2)

- Status: approved with D-71 (2026-11-05); fixtures and calibration in round 140 (D-72, proposed).
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
