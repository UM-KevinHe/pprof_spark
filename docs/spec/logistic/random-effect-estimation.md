# Specification: logistic random effects — estimation (slice 4b-1)

- Status: approved with D-57 (2026-10-22), with X-043; fixtures and calibration in round 110 (D-58, approved with X-044); the numerics kernel in round 111.
- Reference: pprof_py v0.7.0 (`9320766`), `LogisticRandomEffectModel` (`models/logistic/random_effect.py`), and
  lme4 1.1-35.1's `glmer` (family binomial, nAGQ 1 and 0). Plan: [../random-effects/plan.md](../random-effects/plan.md)
  (D-51, part 4b). Reused: the three-stage model's stage 2 (`ThreeStageGlmm`, D-41) and `BoundedQuasiNewton`, the
  provider-local layout and the summaries of slice 4a-1.
- Parity matrix: entries arrive with the code (`docs/parity/matrix.md`).

Inference (`summary`, the tests, intervals, measures, `profile_sigma`) is slice 4b-2's; persistence and the job runner
4b-3's; further grouping factors (`cluster_vars`) part 4d's (OI-93).

## 1. Model and estimand

For record i of provider j(i), with outcome yᵢ ∈ [0, 1], offset oᵢ (0 by default) and p fixed effects xᵢ (the
intercept's constant included unless the fit has none):

    logit P(yᵢ = 1 | uⱼ) = ηᵢ = oᵢ + xᵢᵀβ + σuⱼ₍ᵢ₎,   uⱼ ~ N(0, 1),

so the provider effects are bⱼ = σuⱼ with SD σ ≥ 0. pprof_py's log-likelihood term is ℓᵢ(η) = yᵢη − log(1 + exp(η)),
defined for fractional yᵢ as well.

The estimand is lme4's for `glmer(y ~ x + offset(o) + (1 | provider), family = binomial)`:
- by default (`stage2`, nAGQ = 1), (σ̂, β̂) minimizing the Laplace deviance D(σ, β) of §2;
- with `stage2 = false` (nAGQ = 0), σ̂ minimizing the criterion D₀(σ) of §2, with β̂ inside the inner problem;
- in both, the conditional modes ûⱼ at the estimate, and b̂ⱼ = σ̂ûⱼ.

## 2. Criteria

For provider j at (σ, β), the penalized deviance Pⱼ(u) = −2Σᵢ∈ⱼ ℓᵢ(ηᵢ) + u² is strictly convex in u, so its
minimizer uⱼ* is unique: the root of gⱼ(u) = σΣᵢ∈ⱼ(yᵢ − μᵢ) − u, with μᵢ = expit(ηᵢ). With wᵢ = μᵢ(1 − μᵢ) and
Wⱼ = Σᵢ∈ⱼ wᵢ at uⱼ*, and hⱼ = 1 + σ²Wⱼ:

- the Laplace deviance is D(σ, β) = Σⱼ [Pⱼ(uⱼ*) + log hⱼ], pprof_py's `_laplace_deviance` (PWRSS + log det H) and
  lme4's deviance for nAGQ = 1 on binary outcomes;
- the nAGQ = 0 criterion is D₀(σ) = Σⱼ [Pⱼ(ûⱼ) + log hⱼ] at the joint minimizer (β̂(σ), û(σ)) of Σⱼ Pⱼ, pprof_py's
  stage-1 objective and lme4's nAGQ = 0 deviance.

### 2.1 Gradient of D

By the envelope theorem for Pⱼ and implicit differentiation of gⱼ(uⱼ*) = 0 (duⱼ*/dβ = −(σ/hⱼ)Σᵢ wᵢxᵢ,
duⱼ*/dσ = (Σᵢ(yᵢ − μᵢ) − σuⱼ*Wⱼ)/hⱼ), with rᵢ = yᵢ − μᵢ and vᵢ = wᵢ(1 − 2μᵢ):

    ∂D/∂β = Σⱼ [−2Σᵢ rᵢxᵢ + (σ²/hⱼ)·Σᵢ vᵢ(xᵢ + σ·duⱼ*/dβ)]
    ∂D/∂σ = Σⱼ [−2uⱼ*Σᵢ rᵢ + (2σWⱼ + σ²Σᵢ vᵢ(uⱼ* + σ·duⱼ*/dσ))/hⱼ]

so each provider contributes sums over its rows of rᵢ, rᵢxᵢ, wᵢ, wᵢxᵢ, vᵢ and vᵢxᵢ at uⱼ*.

## 3. Algorithm (D-51 item 7)

- **Inner problem.** For each provider, uⱼ* by Newton's method on gⱼ from u = 0 at every evaluation, as stage 2 of
  the three-stage model (D-41), with a bracket that keeps each step inside the interval where gⱼ changes sign, to
  |Δu| ≤ 10⁻¹² (1 + |u|).
- **nAGQ = 0 (`stage2 = false`, and the start of nAGQ = 1).** For a given σ, Newton's method on Σⱼ Pⱼ over (β, u)
  jointly, the providers' block of the Hessian diagonal and eliminated through the Schur complement, as the
  fixed-effect model's Newton (`LogisticFE`), with step halving when Σⱼ Pⱼ rises, to a relative change of 10⁻¹²;
  then σ̂ minimizes D₀(σ) over [0, σ_upper] by a grid on σ (σ = 0 and 10ᵏ/4 for k = −4, …, 2) bracketing Brent's
  minimization to 10⁻¹⁰ in σ, with the boundary σ = 0 checked as slice 4a-1's §3.
- **nAGQ = 1 (the default).** `BoundedQuasiNewton` over (σ, β) with σ ∈ [0, σ_upper], from the nAGQ = 0 optimum,
  with the analytic gradient of §2.1, until the projected gradient's largest component is at most 10⁻⁸·(1 + |D|);
  a boundary fit at σ = 0 when the projected gradient vanishes there.
- **Conditioning.** With an intercept, the features enter centred at their means over rows, a change of variables
  that moves only the intercept, transformed back as in slice 4a-1's §14.

pprof_py's stage 1 uses BOBYQA (or Powell without nlopt) and its stage 2 Nelder–Mead; `glmer` uses BOBYQA for both.
All three stop where their tolerances let them; pprof_spark reaches the optimum to the precision of its gradient, as
stage 2 of the three-stage model does (X-030).

## 4. Estimates and fit statistics

| Quantity | Definition | pprof_py | lme4 |
|---|---|---|---|
| β̂, σ̂ | §1, in the original coordinates | `coefficients_["beta"]`, `sigma_` | `fixef`, `getME(fit, "theta")` |
| Var(β̂) | (X′WX − CH⁻¹C′)⁻¹ at the estimate, with C = X′WZσ and H = I + σ²Z′WZ: the information conditional on the modes | `variances_["beta"]` | `vcov(fit, use.hessian = FALSE)` (X-043) |
| BLUP b̂ⱼ | σ̂ûⱼ | `coefficients_["alpha"]` | `ranef` |
| Conditional variance of bⱼ | σ̂²/hⱼ | `_get_posterior_var` | `ranef(condVar = TRUE)` |
| Deviance, ℓ | D at the estimate (D₀ for nAGQ = 0), and ℓ = −D/2 | `−2·loglike_`, `loglike_` | `-2 * logLik`, `logLik` |
| AIC, BIC | D + 2k and D + k·log n, with k = p + 1 | `aic_`, `bic_` | `AIC`, `BIC` |
| PWRSS, log det H, ‖u‖² | Σⱼ Pⱼ, Σⱼ log hⱼ, Σⱼ ûⱼ² | `pwrss_`, `ldL2_`, `ussq_` | `drsum + ussq`, `ldL2`, `ussq` of `getME(fit, "devcomp")$cmp` |
| Fitted values, residuals | μ̂ᵢ with the BLUPs; yᵢ − μ̂ᵢ; Pearson (yᵢ − μ̂ᵢ)/√(μ̂ᵢ(1 − μ̂ᵢ)); deviance residuals with the saturated terms of fractional yᵢ | `fitted_`, `residuals_`, `pearson_residuals`, `deviance_residuals` | `fitted`, `residuals` |
| Search record | Evaluations of each stage, the final projected gradient, the boundary | `converged_`, `stage2_iterations_` | `optinfo` |

lme4's `deviance` of a `glmer` fit is the conditional deviance (`drsum`), not −2ℓ; the table compares −2·`logLik`.

## 5. Data contract

| Column | Type | Rule |
|---|---|---|
| outcome | numeric | Finite, in [0, 1]; fractional values allowed, as pprof_py (lme4 warns on them, and its deviance adds their saturated terms: informational, as spec 2f-3 §9) |
| features | numeric, zero or more | Finite; aliasing checked as `LogisticFE` does, on the full design, so a feature constant within providers stays identified |
| provider | integral or string | Non-null; ordered by key |
| offset | numeric, optional | Finite |
| row identifier | integral, optional | Unique; the canonical order |

Missing or non-finite values fail with counts per column, as X-038 set for the linear model; outcomes outside
[0, 1] fail with their count; fewer than two providers fail (X-040's rule). pprof_py's logistic model takes no
weights or trials, nor does this slice.

## 6. Conventions

| Item | Convention |
|---|---|
| Stages | `stage2` true (nAGQ = 1) by default; false gives the nAGQ = 0 fit |
| σ | σ ∈ [0, σ_upper], σ_upper = ∞ by default |
| Boundary fits | σ̂ = 0 gives b̂ = 0, conditional variances of 0 and a warning, as slice 4a-1 |
| Prediction | `type` response (default) or link; fixed effects by default, with `use_re` the BLUPs of providers in the fit and 0 for others; the offset when the fit has one, unless `withOffset` is false (pprof_py adds it only with `offset_var`) |
| Provider sizes | The provider table carries the row count and Wⱼ at the estimate |

## 7. Distributed algorithm

1. Validation and the canonical order; ProviderLocal blocks, each provider's rows in one block.
2. One pass for the features' means over rows (the centring) and the design's aliasing check.
3. nAGQ = 0: each Newton iteration over (β, u) is one pass whose blocks emit, per provider, Σᵢ wᵢ, Σᵢ wᵢxᵢ and Σᵢ rᵢ,
   and their shares of X′WX and Σᵢ rᵢxᵢ; the driver forms the Schur complement in the providers' key order
   (m summaries of p + 2 values, under the driver budget, as slice 4a-1's search) and broadcasts the step.
4. nAGQ = 1: each evaluation of D and its gradient is one pass: every block solves its providers' uⱼ* on their
   rows and emits the shares of D and ∇D, combined pairwise in block order (§6.8). No provider summary is collected.
5. At the estimate: Var(β̂) from one more pass (X′WX and the providers' Σᵢ wᵢxᵢ and Wⱼ), the provider table
   (key, `records`, `weight`, `blup`, `variance`, `se`) computed in the blocks, and the fit's fingerprint.

Cost: each evaluation reads the rows once; the inner Newton steps stay inside the blocks. On the driver: O(p²) per
block per pass, and in nAGQ = 0 the m providers' summaries.

## 8. Edge cases

| Case | pprof_spark |
|---|---|
| σ̂ = 0 | The boundary fit of §6; on round 109's probe without a provider effect, pprof_py and `glmer` both gave an interior σ̂ = 0.0312 |
| A provider whose outcomes are all 0 or all 1 | uⱼ* finite, held by the penalty |
| Separation of the outcomes by the features | β̂ diverges: the search stops at its evaluation cap without convergence, with a warning naming separation as the likely cause |
| Fractional outcomes | Fitted as pprof_py; lme4's deviance differs by the saturated terms (informational) |
| One provider | Fails at validation (X-040's rule) |

## 9. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `stage2`, `sigma_upper` | The same options |
| `max_iter_outer` | The cap on the quasi-Newton's iterations (and on stage 1's evaluations) |
| `max_iter_pirls` | The cap on each inner Newton |
| `tol_outer`, `tol_pirls`, `optimizer_stage1`, `optimizer_stage2`, `verbose` | Not used by pprof_spark's searches; the Python wrapper accepts them and warns when they differ from pprof_py's defaults |
| `fit(X, y_var, x_vars, provider_var, cluster_vars, offset_var, include_intercept)` | The same roles; `cluster_vars` fails until part 4d (OI-93) |
| `get_random_effects`, `get_sigma`, `predict`, `pearson_residuals`, `deviance_residuals`, `fitted_` | The provider table, the fit, `predict`, `fitted` |

## 10. Validation plan

Fixtures in `fixtures/logistic-re`, each fitted with nAGQ = 1 and nAGQ = 0:

| Case | Content |
|---|---|
| lgre-base | 40 providers of 20 to 150 rows, two features (round 94's design) |
| lgre-offset | An offset |
| lgre-nointercept | No intercept |
| lgre-provider-level | A feature constant within providers |
| lgre-extreme | Providers whose outcomes are all 0 or all 1 |
| lgre-small-sigma | A provider SD near 0 |
| lgre-shifted | Features far from zero |
| lgre-text | String provider keys |
| lgre-many | About 2,000 providers |
| lgre-fractional | Fractional outcomes (lme4 informational) |

References: pprof_py at its defaults and with its tolerances tightened; `glmer` with its default control and with
BOBYQA's tolerances tightened.
- Function level: D(σ, β) at fixed points against pprof_py's `_laplace_deviance` (with its inner problem converged)
  and lme4's deviance function for nAGQ = 1 (`devFunOnly = TRUE`), at T-fn; the gradient of §2.1 against central
  differences of those references.
- End to end: σ̂, β̂, the BLUPs, conditional variances, Var(β̂), the deviance, ℓ, AIC, BIC, PWRSS, log det H and ‖u‖²
  at T-opt (D-42), re-calibrated with covariates in this fixture round, as D-51 item 8 says; fitted values and
  predictions likewise.
- Metamorphic: bitwise invariance to row order and partitioning; block sizes within T-part; the driver's and the
  distributed nAGQ = 0 search agree bit for bit; shifting a feature with an intercept moves only the intercept.
- Recovery on `bench.synthetic`'s logistic data (γⱼ = −1 + 0.5Zⱼ), and the edge cases of §8.

## 11. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-030 (precedent) | D | The optimum by a gradient method instead of the references' derivative-free paths | As D-41 |
| X-038, X-040 (precedents) | C | Incomplete rows fail; fewer than two providers fail | As D-52 |
| X-043 | — | Var(β̂) is pprof_py's information conditional on the modes, lme4's `vcov(use.hessian = FALSE)`; `glmer`'s default `vcov` uses the Hessian of the deviance in (θ, β) and differs by about 0.25% in standard errors on round 109's probe | Approved (D-57) |
| X-044 | B | Predictions with the BLUPs carry them under any key type; pprof_py's carry none under integer keys | Approved (D-58) |

## 12. Fixture findings (round 110)

- `fixtures/logistic-re` (`reference/fixtures/logistic_re_fixtures.py`, `logistic_re_r.R`) holds the ten cases of
  §10, each with nAGQ = 1 and nAGQ = 0: pprof_py and `glmer` at their defaults and with their tolerances tightened
  (pprof_py's `tol_outer` 10⁻¹⁰ and `tol_pirls` 10⁻¹³; BOBYQA's `rhoend` 10⁻¹² and `tolPwrss` 10⁻¹³). It also
  holds the Laplace deviance at four fixed points within two standard errors of the optimum, with pprof_py's
  Richardson central differences at two of them, the nAGQ = 0 criterion at three values of σ, and negative controls.
- Calibration (`docs/parity/logistic-re-calibration.md`): every strict check passes.
  - The tightened optima agree within 0.017 of T-opt: σ̂, β̂, the BLUPs and their variances, Var(β̂), the deviance,
    the fitted values, the predictions without the BLUPs and the residuals.
  - The deviance and the nAGQ = 0 criterion at fixed parameters agree within 0.32 of T-coef. T-fn is too strict for
    them, because both references stop their inner PIRLS at a relative change of the penalized deviance.
  - The weakest negative control misses by 12.7.
- `glmer` fits another model to fractional outcomes: on lgre-fractional it returns σ̂ = 0 and β̂ = (−2.31, 1.28,
  −0.75), where pprof_py's quasi-likelihood gives σ̂ = 0.562 and (−0.958, 0.687, −0.381). The case is compared with
  pprof_py alone (D-58).
- lgre-shifted is lgre-base's draws with x₁ + 1000; there both references lose precision. pprof_spark is checked by
  invariance against lgre-base, as lre-shifted (D-53, D-58).
- pprof_py's `predict(use_re=True)` adds no BLUP under integer keys (X-044). Its predictions with the BLUPs are
  compared on lgre-text only, and `glmer`'s everywhere.
- A single flipped row moves an optimizer-limited fit by less than T-opt allows. The negative controls therefore
  flip a whole provider, which must move σ̂, β̂, the BLUPs and the deviance, and the first row of every provider,
  which must move β̂ and the deviance.

## 13. Implementation notes (round 111, numerics; no statistical change)

- `numerics.kernels.LogisticRE`: `Rows` (a block's providers in key order, each row its features, offset and
  outcome). `laplace` gives a block's shares of D and ∇D (§2.1), with each provider's mode by Newton's method from 0
  in its bracket; it stops at an exact root, where the bracket would otherwise move it. `stage1` gives the nAGQ = 0
  sums: per provider Wⱼ, Σ wx and Σ r, and per block X′WX, Σ rx, the penalized deviance and Σ log hⱼ. `modes` gives
  the modes and Wⱼ. The `Blocks` trait has `LocalBlocks` on the driver; the engine's distributed blocks follow.
- `stage1Solve` is the nAGQ = 0 Newton through the Schur complement, with step halving. `fit` runs nAGQ = 0 by the
  grid of §3 and Brent's minimization, warm-starting each σ from the last solution, and then, for nAGQ = 1,
  `BoundedQuasiNewton.minimizeWithGradient`, a new variant that takes the analytic gradient with the value, one
  evaluation per point. Var(β̂) is the inverse of the same Schur complement at the estimate. The features are centred
  only with an intercept, and the estimates are transformed back as slice 4a-1's §14.
- An upper bound on σ clips the search and reports a boundary fit; `BoundedQuasiNewton` has only lower bounds.
- `LogisticREKernelSuite` (5 tests) checks:
  - that D equals the nAGQ = 0 sums at the modes, and that every mode solves its equation;
  - the gradient against fourth-order central differences at three points, within 10⁻⁶;
  - the nAGQ = 0 Newton's scores;
  - that both fits are optimal along σ and invariant to shifting a feature by 1,000;
  - the upper bound and the refusals.
- An out-of-repository harness (round 111's evidence log) checks the kernel against `fixtures/logistic-re` on every
  case:
  - D at the fixed points against pprof_py within 2.1·10⁻⁷ of T-coef, and against lme4 within 0.21;
  - the gradient against pprof_py's central differences within 0.22 of 10⁻⁶ relative;
  - D₀ against pprof_py within 1.5·10⁻⁶ of T-coef, and against lme4 within 0.32;
  - both fits against pprof_py's and lme4's tightened optima within 0.075 of T-opt;
  - lgre-shifted against lgre-base within 10⁻¹² relative for nAGQ = 1.
- `FixturesSuite` mirrors the calibration's strict checks.
