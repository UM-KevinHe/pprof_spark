# Specification: linear random effects — inference and standardized measures (slice 4a-2)

- Status: approved with D-54 (2026-10-21), with X-041 and X-042; fixtures and calibration in round 102 (D-55,
  approved); the engine in round 103 and Python in round 104.
- Reference: pprof_py v0.7.0's `LinearRandomEffectModel.summary`, `fixed_effects_table`, `conf_int`, `test`,
  `calculate_confidence_intervals` and `calculate_standardized_measures`, with the shared `reference_effect` and
  `effect_test`; R pprof 1.0.3's `test.linear_re` and `confint.linear_re` (option `alpha`) where they follow
  pprof_py's definitions (X-042); lme4 1.1-35.1's conditional variances for weighted fits (X-037); base R for t and
  normal tails and quantiles. The fit: [random-effect-estimation.md](random-effect-estimation.md) (slice 4a-1).
- Plan: [../random-effects/plan.md](../random-effects/plan.md) (D-51). Persistence and the job runner are 4a-3's.

Every quantity below comes from the fit of slice 4a-1: β̂, Var(β̂), σ̂, θ̂ and σ̂_α, and the provider table's BLUPs b̂ⱼ,
row counts nⱼ, weight totals Wⱼ and conditional variances vⱼ = σ̂²τ̂/(1 + τ̂Wⱼ) (X-037). n is the number of rows, p the
number of fixed effects (the intercept included) and m the number of providers. As in pprof_py, every method here
needs a fit with the provider factor alone (`cluster_vars` comes with part 4d).

## 1. Reference effect

γ₀ is 0 (the random effects' mean), the median of the BLUPs over all providers (unweighted; for even m the mean of
the two middle values, as NumPy's `median`), their size-weighted mean Σⱼ nⱼb̂ⱼ/n (pprof_py's `reference_effect`
with the row counts, also under prior weights), or a given number. The defaults are pprof_py's: 0 for `test`, the
median for `calculate_confidence_intervals` and `calculate_standardized_measures`. One γ₀ serves a call.

## 2. Covariate inference

| Method | Definition (pprof_py) |
|---|---|
| `summary(covariates, level, null, alternative)` | tₖ = (β̂ₖ − null)/seₖ with seₖ from Var(β̂) and ν = n − p − m, as the fixed-effect model's; p-values two-sided 2F̄ν(\|tₖ\|), `greater` F̄ν(tₖ), `less` Fν(tₖ), computed from the upper tail directly and not rounded (X-041); limits β̂ₖ ∓ t₁₋α/₂,ν·seₖ, one-sided limits t₁₋α,ν and open on the other side; columns estimate, std_error, stat, p_value, ci_lower, ci_upper; `covariates` selects rows by name |
| `fixed_effects_table()` | Estimate, Std. Error, t value = β̂ₖ/seₖ (NaN when seₖ = 0), df = max(n − p, 1), Pr(>\|t\|) = 2F̄df(\|t\|) |
| `conf_int(level)` | β̂ₖ ∓ t at (1 + level)/2 with max(n − p, 1) degrees of freedom; columns lower, upper |

The two degrees of freedom are pprof_py's, method by method: n − p − m in `summary`, n − p in the other two.

## 3. Provider tests (`test`)

For provider j, zⱼ = (b̂ⱼ − γ₀)/seⱼ with seⱼ = √vⱼ, the BLUP's conditional SD. Without prior weights seⱼ equals
pprof_py's and R pprof's √(sⱼσ̂²/nⱼ), sⱼ = σ̂²_α/(σ̂²_α + σ̂²/nⱼ); with them it is lme4's (X-037).

| Item | Definition |
|---|---|
| p-value | Two-sided 2Φ̄(\|zⱼ\|), `greater` Φ̄(zⱼ), `less` Φ(zⱼ), from the normal tails directly |
| Flags | Slice 2c §2: +1, −1 or 0 by zⱼ against c = Φ⁻¹(1 − α/2) (two-sided) or Φ⁻¹(1 − α) (one-sided), or a given `critical` c; one-sided flags are 0 or +1 (`greater`) and 0 or −1 (`less`) |
| Limits | b̂ⱼ ∓ c·seⱼ, one-sided limits open on the other side (±∞); `interval` `inversion` and `scale_only` coincide under the theoretical null |
| Null model | The theoretical null only (D-51 item 5): null_mean 0, null_sd 1, z_adjusted = z_raw |
| Providers | All, or the subset given (keys compared as text) |

Output: one row per provider, keyed by provider with `records`, and slice 2c's columns: estimate (b̂ⱼ), se,
null_value (γ₀), transformed (b̂ⱼ), se_transformed (seⱼ), null_transformed (γ₀), z_raw, null_mean, null_sd,
null_group, z_adjusted, p_value, flag, ci_lower, ci_upper.

## 4. Intervals (`calculate_confidence_intervals`)

| Option | Definition (pprof_py) |
|---|---|
| `alpha` | b̂ⱼ ∓ Φ⁻¹(1 − α/2)·seⱼ, two-sided only (pprof_py and R pprof refuse one-sided ones); columns key, alpha, alpha_lower, alpha_upper |
| `SM`, indirect | (Σᵢ∈ⱼ(Lⱼ + ηᵢ) − Eⱼ)/nⱼ and (Σᵢ∈ⱼ(Uⱼ + ηᵢ) − Eⱼ)/nⱼ, with ηᵢ = oᵢ + xᵢᵀβ̂, Eⱼ as in §5, and Lⱼ and Uⱼ the effect's limits at c = Φ⁻¹(1 − α/2), or Φ⁻¹(1 − α) and ±∞ for one-sided alternatives; columns: §5's indirect table with lower and upper |
| `SM`, direct | (Σᵢ ηᵢ + nLⱼ − O)/n and (Σᵢ ηᵢ + nUⱼ − O)/n, with O as in §5; columns: §5's direct table with lower and upper |

`level` (0.95), `alternative` (two_sided, greater, less), `reference` (the median) and `providers` as in §1 and §3.

## 5. Standardized differences (`calculate_standardized_measures`)

| Measure | Definition (pprof_py) |
|---|---|
| Indirect | Oⱼ = Σᵢ∈ⱼ ŷᵢ with the fitted values ŷᵢ = ηᵢ + b̂ⱼ, and Eⱼ = Σᵢ∈ⱼ(ηᵢ + γ₀); difference (Oⱼ − Eⱼ)/nⱼ; columns key, indirect_difference, observed, expected |
| Direct | O = Σᵢ ŷᵢ over all rows (one value) and expectedⱼ = Σᵢ ηᵢ + n·b̂ⱼ; difference (expectedⱼ − O)/n; columns key, direct_difference, observed, expected |

Algebraically the indirect difference is b̂ⱼ − γ₀ and the direct one b̂ⱼ − Σₖ nₖb̂ₖ/n, which does not depend on γ₀;
the sums are computed as defined, as in the fixed-effect model. Unlike the fixed-effect model's, the observed values
are sums of fitted values, not of outcomes: with an intercept and no weights the two totals agree, since the fixed
effects' equations make Σᵢ(yᵢ − ŷᵢ) = 0.

## 6. Data and API

| Function | Needs |
|---|---|
| `LinearREInference.summary`, `fixedEffectsTable`, `confInt` | The fit alone: β̂, Var(β̂), n, p and m |
| `LinearREProviderTests.test`, intervals with option `alpha` | The fit alone: its provider table (b̂ⱼ, nⱼ, seⱼ) |
| `LinearREStandardization.measures`, intervals with option `SM` | The fit and its training data, checked against the fit's fingerprint (API-3), for Σᵢ∈ⱼ ηᵢ and Σᵢ ηᵢ |

The Python wrapper's `LinearRandomEffectModel` gains `summary`, `fixed_effects_table`, `conf_int`, `test`,
`confidence_intervals` and `standardized_measures`, as the fixed-effect wrapper names them.

## 7. Distributed plan

1. γ₀: the provider table's `records` and `blup` collected to the driver in key order under `DriverGuards` (m values
   of each); the median by sorting, the size-weighted mean by a compensated sum in key order.
2. Tests and `alpha` intervals: a projection of the distributed provider table, with γ₀ and the critical values as
   literals.
3. Measures and `SM` intervals: one pass over ProviderLocal blocks of the training data, fingerprint checked: per
   provider Σᵢ∈ⱼ ηᵢ by Neumaier sums in canonical order, and Σᵢ ηᵢ reduced in block order (§6.8); the result is a
   distributed table joined to the provider table by key.

## 8. Edge cases

| Case | pprof_spark |
|---|---|
| A boundary fit (τ̂ = 0) | seⱼ = 0 and b̂ⱼ = 0. As pprof_py (round 102), every provider is untested whatever γ₀: zⱼ, the p-value, the flag and the test's limits are NaN, with the fit's warning (NN-10); option `alpha` gives limits of 0 (D-55) |
| Large \|zⱼ\| | Normal tails from the complementary functions, so no provider loses its p-value |
| Even m | The median is the mean of the two middle values |
| Providers with one record | seⱼ from vⱼ, as any other |
| Prior weights | seⱼ and the limits follow X-037; the measures' sums are unweighted, as pprof_py's |

## 9. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `summary(covariates, level, null, alternative)` | `LinearREInference.summary(fit, level, null, alternative, covariates)` |
| `fixed_effects_table()`, `conf_int(level)` | `LinearREInference.fixedEffectsTable(fit)`, `confInt(fit, level)` |
| `test(providers, reference, null_model=None, alternative, level, critical, interval)` | `LinearREProviderTests.test(fit, reference, alternative, level, critical, providers)` |
| `calculate_confidence_intervals(providers, level, option, stdz, reference, alternative)` | `LinearREStandardization.intervals(fit, …)` for `alpha`; `intervals(df, fit, …)` for `SM` |
| `calculate_standardized_measures(providers, stdz, reference)` | `LinearREStandardization.measures(df, fit, kinds, reference, providers)` |
| `funnel_limits`, the plots, `null_model` other than the theoretical one | P0 (D-51 item 5) |

## 10. Validation plan

The fixtures of `fixtures/linear-re` gain an inference section for every case under REML, and for lre-base and
lre-weights under ML, all at the tight optima of slice 4a-1:

- pprof_py: `summary` (two-sided, `greater` and `less`; level 0.90), `fixed_effects_table`, `conf_int`; `test` at
  γ₀ = 0, the median, the size-weighted mean and 0.1, two-sided, `greater` and `less`, with `critical` 2.5 and a
  providers subset; `calculate_confidence_intervals` (`alpha`; `SM` indirect and direct, two-sided and one-sided);
  `calculate_standardized_measures` at the median, the mean and 0.
- R pprof: `test.linear_re` (z, p-values, flags) and `confint.linear_re` with option `alpha`, on the unweighted cases.
- Base R: t tails and quantiles for `summary` with ν = n − p − m, from lme4's estimates; for the weighted cases, the
  tests and `alpha` intervals from lme4's conditional variances (X-037).

Layers: in lockstep, pprof_spark's inference computed from each reference's own estimates against that reference's
outputs; end to end, from pprof_spark's fit, at the estimates' classes. Classes: T-test (t and z), T-p (p-values
against base R everywhere; pprof_py's `summary` p-values within their rounding, 5·10⁻⁸ absolute, X-041 and D-55), T-flag (flags), T-coef
(limits), T-meas (measures and their intervals). Negative controls as D-09 requires.

## 11. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-037 | B | The tests' and intervals' seⱼ use the weight totals Wⱼ under prior weights | Approved (D-51) |
| X-041 | B | pprof_py's `summary` computes p-values as 1 − F and rounds them to seven decimals; pprof_spark computes the upper tail directly and does not round | Proposed (D-54) |
| X-042 | — | R pprof 1.0.3's linear random-effect inference departs from pprof_py's definitions in places (`summary`'s degrees of freedom and Wald intervals; BLUPs labelled in text order, which misaligns its tests, intervals and direct differences under integer keys) | Approved (D-54), as corrected (D-55) |

## 12. Fixture findings (round 102)

- `fixtures/linear-re` gains `inference_pprof_py.json`: every case under REML, lre-base and lre-weights also under
  ML, at slice 4a-1's tight optima, with the outputs of §10 and the negative controls' z at γ₀ = 0. `r_lme4.json`
  gains base R's references on lme4's tight fit (`inference`) and R pprof's tests and option `alpha` on its own default
  lmer fit (`rpprof`) for the cases without weights, offsets or a removed intercept, since R pprof's `linear_re`
  builds its data from the formula's columns. REFERENCE.lock pins R pprof's three files by SHA-256.
- Calibration (`docs/parity/linear-re-inference-calibration.md`): every strict check passes; worst 0.0016 of T-coef,
  6.2·10⁻⁴ of T-test, 0.0084 of T-p and 1.3·10⁻⁷ of T-meas; no flag differs; the controls miss by at least 5.7·10⁵.
  The measures equal their algebraic forms of §5. Weighted tests differ by X-037, as expected, and lre-shifted's
  comparisons are informational (D-53).
- pprof_py's `summary` p-values are rounded to seven decimals, so even p near 0.01 differs from base R's by more
  than T-p; they are compared within their rounding, 5·10⁻⁸ absolute (D-55).
- At the boundary fit (lre-boundary), pprof_py's `test` returns NaN for z, the p-value, the flag and the limits of
  every provider, whatever γ₀, and option `alpha` returns limits of 0; §8 now follows it (D-55).
- R pprof's BLUPs are labelled in text order over values in numeric key order: with integer keys its tests pair one
  provider's BLUP with another's size (provider 10: z 0.589 against pprof_py's 1.704, same se), and its direct
  differences and intervals inherit the labels; its indirect differences are right. X-042 is corrected accordingly.
- Regenerating the family in a new sandbox session moved the optimizer-limited default runs by up to 1.1·10⁻⁷ and the
  tight optima by 1.3·10⁻¹⁰, beyond T-part: `compare_outputs.py` takes T-opt and 10⁻⁸ for the family (D-55, OI-96).

## 13. Implementation notes (round 103, engine; no statistical change)

- `engine.linear.LinearREInference`: `summary`, `fixedEffectsTable` and `confInt` from the fit's coefficients;
  `reference` and `test` (slice 2c's table, normal tails from `Normal`, every provider untested when its se is 0);
  `alphaIntervals` from the provider table; `measures` and `smIntervals` from one pass over the training data, whose
  fingerprint and row count must match the fit's (API-3), with Σᵢ∈ⱼ ηᵢ by Neumaier sums in canonical order and Σᵢ ηᵢ
  reduced in block order; Σᵢ ŷᵢ is Σᵢ ηᵢ + Σⱼ nⱼb̂ⱼ, added in key order.
- The fit's working set now carries the offset as one more column after the weight, so the measures read ηᵢ = oᵢ +
  xᵢᵀβ̂ from the same blocks as the fit and check the same fingerprint; the fit's estimates are unchanged, and its
  fingerprint now covers the offset.
- `LinearREInferenceSuite` (engine, 11 tests): every case but lre-shifted, under REML, and lre-base and lre-weights
  under ML, end to end against base R on lme4's tight fit (covariate tables, the tests at γ₀ = 0 and the median,
  weighted fits included through lme4's conditional variances) and pprof_py (its rounded `summary` p-values within
  5·10⁻⁸; the other test variants, option `alpha` and the SM bounds on unweighted fits; the SM differences and the
  measures everywhere); the boundary fit untested; the measures' refusal of other data. `FixturesSuite` mirrors the
  calibration's checks.

## 14. Implementation notes (round 104, Python; no statistical change)

- The facade (`PythonApi`): `linearRECovariateTests`, `linearREFixedEffectsTable` and `linearREConfInt` (JSON,
  doubles in hexadecimal), `linearRETest`, `linearREAlphaIntervals`, `linearREMeasures` and `linearREIntervals`
  (DataFrames), with γ₀ given as "median", "mean" or a number, as for the fixed-effect model.
- `LinearRandomEffectModel` gains `summary(level, null, alternative, covariates)`, `fixed_effects_table()` (entries
  of `LinearREFixedEffect`), `conf_int(level)` (feature to its limits), `test(reference=0.0, …)`,
  `confidence_intervals(df, option, …)` (`alpha_ci`; `indirect_ci` and `direct_ci` for option `SM`, which needs the
  training data) and `standardized_measures(df, …)`, with the fixed-effect wrapper's names.
- `python/tests/test_linear_re.py` gains 2 tests on lre-offset: the covariate tables against base R, and the
  tests, both interval options and the measures against base R and pprof_py.
