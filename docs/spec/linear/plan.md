# Phase 3 plan: linear fixed effects

- Status: approved with D-45 (2026-10-09).
- Scope (PROJECT_CONTEXT §4): profile (within) estimation, standardization and inference for pprof_py's
  `LinearFixedEffectModel`, read at the pin (v0.7.0, `9320766`).

## 1. What the reference contains at the pin

| Part | pprof_py | Statistics |
|---|---|---|
| Model | `models/linear/fixed_effect.py`: `LinearFixedEffectModel(gamma_var_option)` | yᵢ = γⱼ₍ᵢ₎ + xᵢᵀβ + εᵢ, εᵢ independent N(0, σ²); no global intercept, no case weights, no offset; every provider kept (no screening or cutoff) |
| Estimation | `algorithms/linear/fixed_effect.py` | The within transformation: β̂ solves X̃ᵀX̃β = X̃ᵀỹ with X̃ and ỹ demeaned within providers; γ̂ⱼ = ȳⱼ − x̄ⱼᵀβ̂ |
| Variances and fit statistics | `inference/linear/fixed_effect.py` | σ̂² = RSS/(n − m − p); Var(β̂) = σ̂²(X̃ᵀX̃)⁻¹; Var(γ̂ⱼ) = σ̂²(1/nⱼ + x̄ⱼᵀ(X̃ᵀX̃)⁻¹x̄ⱼ) (`complete`, the default) or σ̂²/nⱼ (`simplified`); AIC and BIC from the Gaussian log-likelihood at σ² = RSS/n with m + p + 1 parameters |
| Covariate inference | `summary` | t tests and intervals for β on n − m − p degrees of freedom; two-sided, `greater`, `less` |
| Provider inference | `test`, `calculate_confidence_intervals` | t statistics (γ̂ⱼ − γ₀)/se mapped to z by equal tail probability (`t_to_z`), then the shared `effect_test` (p-values, flags, `critical`); limits γ̂ⱼ ± t·se; γ₀ is the median of γ̂, their size-weighted mean, or a number |
| Standardization | `calculate_standardized_measures` | The indirect difference (Oⱼ − Eⱼ)/nⱼ with Eⱼ = Σᵢ(γ₀ + xᵢᵀβ̂), and the direct difference; both equal γ̂ⱼ − γ₀ |
| Other | `predict`, `score` (R²), `get_fitted_params`; `funnel_limits` and the plots | Funnel limits and plotting stay out of scope (§2.5; §4 lists funnel limits under "Later") |

pprof_py has no parity test of its own for this model. R's `lm(y ~ 0 + factor(provider) + x)` is the
external reference: round 62's probe (2,592 rows, 40 providers of 1 to 119 records, three features) agrees
with it within 5.4e-14 relative in β̂, γ̂, σ̂, both variances, AIC and BIC. R pprof 1.0.3 (MIT) carries the
same model (`linear_fe`, `summary.linear_fe`, `test.linear_fe`, `confint.linear_fe`,
`SM_output.linear_fe`), a second reference for slices 3b and 3c (OI-60).

## 2. Slices

| Slice | Content | pprof_py | R references | Fixtures | Tolerance classes |
|---|---|---|---|---|---|
| 3a | Estimation and covariate inference: β̂, γ̂, σ̂, both variance options, t tests and intervals for β, ℓ, AIC and BIC, prediction and R², persistence. Specification: [fixed-effect-estimation.md](fixed-effect-estimation.md) (D-45) | `fit`, `summary`, `predict`, `score`, `get_fitted_params` | `lm` with a provider factor | lin-base, lin-singletons, lin-shifted, lin-text, lin-many | T-fn, T-coef, T-var, T-test, T-p, T-part |
| 3b | Provider tests, limits and intervals (`gamma` and `SM`), standardized differences | `test`, `calculate_confidence_intervals`, `calculate_standardized_measures` | R pprof `test.linear_fe`, `confint.linear_fe`, `SM_output.linear_fe` | 3a cases | T-test, T-p, T-coef, T-meas |
| 3c | The job runner (`model` `linear`, as `LogisticJob`) and the Python wrapper (`LinearFixedEffect`, ADR-0009) | — | — | lin-base | Bitwise against the library |

Each slice follows the pattern of Phases 1 and 2: specification approved first, then fixtures and
calibration, then code and parity tests under Classic Spark and Spark Connect. The Phase 3 gate closes at
parity-verified; the scale test is the maintainer's, once the package is done (D-28).

## 3. Distributed design for up to 10⁶ providers

The estimator is closed form, with no iterations. PROJECT_CONTEXT §7.5's design holds, with one refinement
and one deferral:

1. Pass 1, in ProviderLocal blocks: per provider nⱼ, x̄ⱼ, ȳⱼ and the centred co-moments by Chan, Golub and
   LeVeque's pairwise updates; each block emits its totals of X̃ᵀX̃ (p×p) and X̃ᵀỹ (p), reduced in block
   order (§6.8). The per-provider means stay in a distributed provider table.
2. Driver: Cholesky of X̃ᵀX̃ with R's aliasing rule (X-032); β̂ and (X̃ᵀX̃)⁻¹ are broadcast.
3. Pass 2 over the rows: γ̂ⱼ = ȳⱼ − x̄ⱼᵀβ̂ from the provider table, the residuals and their sum of squares
   per block, reduced in block order. The provider table gains qⱼ = 1/nⱼ + x̄ⱼᵀ(X̃ᵀX̃)⁻¹x̄ⱼ, so
   Var(γ̂ⱼ) = σ̂²qⱼ needs no further pass.

The refinement: the residual sum of squares by the co-moment identity, ỹᵀỹ − β̂ᵀX̃ᵀỹ, cancels when the within
fit is close, so pass 2 computes it from the residuals, as the reference does (one more scan, no shuffle).
The driver holds p×p values and scalars; γ̂ and its variances stay distributed (DIST-1). The deferral:
§7.5's TSQR path for ill-conditioned cross-products; slice 3a records a condition estimate, and aliasing
fails cleanly (OI-61).

## 4. Tolerance classes

Closed-form estimates against closed-form references: β̂, γ̂ and the variances under T-coef and T-var; σ̂,
ℓ, AIC and BIC under T-fn; t statistics under T-test; p-values under T-p where pprof_py's are accurate
(X-031); layouts under T-part. Slice 3a's calibration decides whether the existing classes suffice; the
probe's 5.4e-14 against R suggests they do.

## 5. Decisions proposed (D-45)

| Item | Proposal |
|---|---|
| Slices | 3a to 3c as in §2 |
| Distributed design | §3: one pass for the co-moments and provider means, a second for the residuals; TSQR deferred (OI-61) |
| X-031 (class B) | `summary`'s p-values from the upper tail directly, as R and pprof_py's own `test` compute them |
| X-032 (class C) | Aliased features fail, named, as X-011 and X-019 |
| X-033 (class C) | n − m − p ≤ 0 fails at validation with the counts |
| Out of scope | `funnel_limits`, plots, and null models other than the theoretical null (§4 "Later": funnel limits, empirical-null calibration) |

## 6. Python access (D-31)

The `pprof_spark` package gains `LinearFixedEffect` and `LinearFixedEffectModel` in slice 3c, through
`PythonApi.linear*`, as for the logistic models.
