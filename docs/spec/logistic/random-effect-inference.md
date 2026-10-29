# Specification: logistic random effects — inference, measures and σ's profile (slice 4b-2)

- Status: approved with D-60 (2026-10-23), with X-045 to X-048; fixtures and calibration in round 116 (D-61,
  approved); the engine in round 117.
- Reference: pprof_py v0.7.0 (`9320766`): `LogisticRandomEffectModel.summary`, `test`, `calculate_confidence_intervals`
  (`inference/logistic/random_effect.py`), `calculate_standardized_measures` (`measures/logistic/random_effect.py`)
  and `profile_sigma` (`models/logistic/random_effect.py`), with the shared `effect_test`, `count_test`,
  `poibin_tails` and `resample_tails` (`inference/effect_tests.py`). R pprof 1.0.3's `summary.logis_re`,
  `test.logis_re`, `SM_output.logis_re` and `confint.logis_re`, and lme4's `confint(method = "profile")`.
- Builds on slice 4b-1 (`docs/spec/logistic/random-effect-estimation.md`): the fit's β̂, Var(β̂), σ̂, the BLUPs b̂ⱼ
  and their conditional SDs seⱼ = σ̂/√hⱼ, and the Laplace deviance D. Reused: slice 2c's provider-test table and
  count tests (`LogisticProviderTests`, `PoissonBinomial`), the three-stage model's resampling test
  (`ThreeStagePipeline`, D-43), and the binned direct sums (`LogisticStandardization`, X-026).
- Out of scope: `null_model` (OI-87), `funnel_limits` (OI-88), the `exact` test, which needs a cluster factor
  (part 4d, OI-93), and plots.

## 1. Notation

ηᵢ = oᵢ + xᵢᵀβ̂ is a record's fixed part (pprof_py's `xbeta_`, with the offset); nⱼ, Oⱼ = Σᵢ∈ⱼ yᵢ and O = Σᵢ yᵢ are
a provider's size, its observed count and the total; n is the number of records. The reference effect γ₀ is a
number, `median` (the median of the BLUPs) or `mean` (their mean weighted by nⱼ, as pprof_py's `reference_effect`).

## 2. `summary(level)`

Wald tests of the fixed effects on the normal: zₖ = β̂ₖ/seₖ with seₖ from Var(β̂) (X-043), p = 2Φ̄(|zₖ|), limits
β̂ₖ ∓ Φ⁻¹(1 − α/2)·seₖ; z and the limits are NaN where seₖ = 0. Columns as pprof_py: estimate, standard error,
z, p, lower and upper limits.

## 3. `test`: provider tests

The result is slice 2c's shared table, one row per provider in key order: `estimate` (b̂ⱼ), `se`, `null_value`
(γ₀), `transformed`, `se_transformed`, `null_transformed`, `z_raw`, `null_mean` (0), `null_sd` (1), `null_group`,
`z_adjusted`, `p_value`, `flag`, `ci_lower` and `ci_upper`. Options: `providers`, `test_method`, `reference` (0
by default), `alternative`, `level`, `critical`, `interval` (`inversion`), `n_resample` (10,000) and `seed`.

| `test_method` | Statistic | Interval |
|---|---|---|
| `wald` (default) | z = (b̂ⱼ − γ₀)/seⱼ | b̂ⱼ ∓ c·seⱼ |
| `poibin_exact` | Oⱼ against the Poisson-binomial count of Bernoulli(expit(γ₀ + ηᵢ)) over the provider's records, rounded to the nearest integer and with probabilities clipped to [10⁻¹⁰, 1 − 10⁻¹⁰], as `poibin_tails`; z from the mid-p tails, as slice 2c | The test inverted in γ |
| `resampling` | The same count's tails estimated from `n_resample` simulated counts under γ₀ | None (NaN), as pprof_py |

`exact` fails, naming part 4d. At a boundary fit (σ̂ = 0, every seⱼ = 0) the Wald test leaves every provider
untested (z, p and the limits NaN, no flag), as slice 4a-2 does (D-55); pprof_py divides by max(seⱼ, 10⁻¹⁵)
instead (X-048). The count tests need no seⱼ and run as usual.

## 4. `calculate_confidence_intervals`

- **Option `alpha`** (two-sided only): b̂ⱼ ∓ Φ⁻¹(1 − α/2)·seⱼ; columns `alpha`, `alpha_lower`, `alpha_upper`.
- **Option `SM`**, with `stdz` (`indirect`, `direct`), `measure` (`ratio`, `rate`), `reference` (`median` by default)
  and `alternative`. The BLUP's limits are Lⱼ = b̂ⱼ − c·seⱼ and Uⱼ = b̂ⱼ + c·seⱼ, with c = Φ⁻¹(1 − α/2) two-sided,
  or Φ⁻¹(1 − α) with Uⱼ = ∞ for `greater` and Lⱼ = −∞ for `less`.
  - Indirect ratio: Rⱼ = Oⱼ/Eⱼ with Eⱼ = Σᵢ∈ⱼ expit(γ₀ + ηᵢ), and limits exp(log Rⱼ ∓ c·seⱼ), NaN when Oⱼ or Eⱼ
    is 0, as pprof_py. One-sided alternatives give one-sided limits (upper ∞ for `greater`, lower 0 for `less`);
    pprof_py keeps both limits finite there (X-045).
  - Indirect rate: the ratio and its limits times the population rate 100·O/n, clipped to [0, 100].
  - Direct ratio: Dⱼ/O with Dⱼ = Σᵢ expit(b̂ⱼ + ηᵢ) over every record, limits Σᵢ expit(Lⱼ + ηᵢ)/O and
    Σᵢ expit(Uⱼ + ηᵢ)/O (expit(±∞) = 1 or 0).
  - Direct rate: 100·Dⱼ/n and its limits, clipped to [0, 100].
- `reference = "mean"` is the size-weighted mean of the BLUPs here too; pprof_py's intervals take the unweighted
  mean, its tests and measures the weighted one (X-046).

## 5. `calculate_standardized_measures`

With `stdz` and `reference` (`median` by default), tables per kind in key order, columns as pprof_py:
- `indirect`: `indirect_ratio` Oⱼ/Eⱼ (NaN when Eⱼ = 0), `indirect_rate` (times 100·O/n, clipped to [0, 100]),
  `observed` Oⱼ and `expected` Eⱼ;
- `direct`: `direct_ratio` Dⱼ/O (NaN when O = 0), `direct_rate` 100·Dⱼ/n (clipped), `observed` O and `expected` Dⱼ.

## 6. `profile_sigma(level)`

The profiled deviance is D_p(σ) = min_β D(σ, β). The interval holds the σ where D_p(σ) − D_min ≤ χ²₁(level), with
D_min = min(D(σ̂, β̂), D_p(σ̂)), as pprof_py and lme4's `confint(method = "profile")`:
- the upper limit by Brent's method on a bracket from σ̂ + max(0.1, σ̂), doubled until it holds, failing beyond 1,000;
- the lower limit likewise on [0, σ̂], or 0 when D_p(0) − D_min ≤ χ²₁(level) or σ̂ = 0.
pprof_spark minimizes over β with the bounded quasi-Newton and D's analytic gradient in β (4b-1 §2.1), warm-started
from the previous point, and finds the limits to 10⁻¹⁰ in σ. pprof_py minimizes by Nelder–Mead (`xatol` 10⁻⁵,
`fatol` 10⁻⁸) and stops Brent's method at 10⁻⁷, so its limits are optimizer-limited (T-opt, D-42). Only nAGQ = 1 fits
have a profile, as pprof_py; the output is σ̂ with its lower and upper limits.

## 7. Distributed algorithm

- `summary`, the Wald test and option `alpha`: from the fit (β̂, Var(β̂)) and its provider table; no data pass.
- Measures and option `SM`: one pass over the training data, checked against the fit's fingerprint (API-3), as slice
  4a-2. Per provider Oⱼ and Eⱼ (Neumaier sums in canonical order). The direct sums Σᵢ expit(s + ηᵢ), for every
  provider's s ∈ {b̂ⱼ, Lⱼ, Uⱼ}, come from X-026's binned Taylor moments of η over all records, reduced in block order;
  pprof_py sums over every record for every provider (O(n·m)).
- `poibin_exact` and `resampling`: within the provider-local blocks, each provider's probabilities from its rows,
  with slice 2c's Poisson-binomial and inversion and the three-stage model's resampling (its per-provider seeded
  generator, D-43); the table is assembled by key.
- `profile_sigma`: every evaluation of D and its gradient is one pass (4b-1 §7); a profile takes some hundreds
  (OI-99).

## 8. Edge cases

| Case | pprof_spark |
|---|---|
| σ̂ = 0 | Wald tests untested (X-048), `alpha` limits b̂ⱼ = 0 ∓ 0; measures and count tests as usual |
| A provider with Oⱼ = 0 | Indirect ratio 0 with NaN limits, as pprof_py |
| Fractional outcomes | Oⱼ rounded for the count tests, as `poibin_tails`; measures use the sums as they are |
| `null_model` | Fails, naming OI-87 |
| `exact` | Fails, naming part 4d (OI-93) |
| nAGQ = 0 fit and `profile_sigma` | Fails, as pprof_py |

## 9. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `summary(level)` | §2 |
| `test(providers, test_method, reference, alternative, level, critical, interval, n_resample, seed)` | §3; `null_model` fails (OI-87) |
| `calculate_confidence_intervals(providers, level, option, stdz, reference, measure, alternative)` | §4, keyed as pprof_py (`alpha_ci`, `indirect_ratio`, `indirect_rate`, `direct_ratio`, `direct_rate`) |
| `calculate_standardized_measures(providers, stdz, reference)` | §5, keyed `indirect` and `direct` |
| `profile_sigma(var, level)` | §6; `var` names the provider (one grouping factor until part 4d) |

## 10. Validation plan

Fixtures `inference_pprof_py.json` for `fixtures/logistic-re`, computed on pprof_py's tightened nAGQ = 1 fit:
`summary`; `test` with every method, reference (0, `median`, `mean`, a number) and alternative, `critical` and a
subset; both interval options with every `stdz`, `measure` and alternative; the measures at each reference; and
`profile_sigma`. With them, R pprof's outputs on its own default `glmer` fit, and lme4's profile interval.
- Lockstep: pprof_spark's inference evaluated at pprof_py's tightened (σ, β), against pprof_py at T-test, T-p,
  T-flag, T-coef and T-meas.
- End to end: from pprof_spark's own fit, at T-est (D-59).
- R pprof: its Wald tests, option `alpha`, direct measures and direct SM intervals, at T-opt; its summary and
  indirect quantities are informational (X-047).
- `profile_sigma` against pprof_py and lme4 at T-opt. In round 115's probe on lgre-base: pprof_py (0.422371,
  0.743147), lme4 (0.422368, 0.743168).
- `resampling`: each p-value within 4.5 Monte Carlo standard errors of pprof_py's.

The fixture round proposes the classes (D-61).

## 11. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-026 (precedent) | B | Direct sums by binned Taylor expansions | Approved (slice 2d) |
| X-043 (precedent) | — | Var(β̂) conditional on the modes | Approved (D-57) |
| X-045 | B | One-sided SM intervals for indirect measures are one-sided; pprof_py keeps both limits finite at the one-sided critical value (round 115: for `greater`, provider 1 of lgre-base gets the upper limit 2.11) | Approved (D-60) |
| X-046 | B | `reference = "mean"` is size-weighted in the intervals as in the tests and measures; pprof_py's intervals take the unweighted mean (round 115, lgre-base: 0.0562 weighted, 0.0091 unweighted) | Approved (D-60) |
| X-047 | — | R pprof's `logis_re` family differs from pprof_py, and is a reference only for the Wald tests, option `alpha` and the direct measures and intervals:<br>• its summary uses `glmer`'s default `vcov` (X-043), and its p-value 2(1 − Φ(z)) exceeds 1 for negative estimates (round 115: 2.000000 and 1.999991);<br>• its indirect ratio is Σ μ̂ᵢ/Σ expit(ηᵢ), with the BLUPs and γ₀ = 0, where pprof_py's is Oⱼ/Eⱼ (lgre-base's provider 1: 1.0217 against 1.1327);<br>• its indirect SM limits transform the BLUP's limits | Approved (D-60), informational |
| X-048 | B | Wald tests at a boundary fit leave every provider untested, as slice 4a-2 (D-55); pprof_py divides by max(seⱼ, 10⁻¹⁵), giving z = 0 at γ₀ = 0 and ±10¹⁵γ₀ otherwise | Approved (D-60) |

## 12. Fixture findings (round 116)

- `inference_pprof_py.json` in every case of `fixtures/logistic-re`, from pprof_py's tightened fit. R pprof's
  `logis_re` family runs on `glmer`'s tightened fit in the seven cases it can fit, with `computeDirectExp` written in
  R. lme4's profile interval comes from `profile(delta = 0.02)` and `confint` on the tightened fit.
- `docs/parity/logistic-re-inference-calibration.md` passes every strict check:
  - pprof_py's Wald z, `alpha` limits and ratios satisfy their definitions exactly;
  - R pprof agrees at T-opt (worst 0.034, option `alpha` on lgre-small-sigma);
  - lme4's profile agrees within 0.37 of T-opt (lgre-nointercept);
  - every negative control misses by at least 4.3·10⁶.
- Before the references were tightened, the first calibration failed 14 checks:
  - `glmer`'s default fit inside R pprof stopped early on lgre-small-sigma and lgre-many;
  - lme4's default profile grid put lgre-base's upper limit at 0.742990 on the tightened fit, against 0.743168 on
    the default fit and pprof_py's 0.743147.
- lgre-small-sigma is a boundary fit (lme4's σ̂ = 0, pprof_py's 1.7·10⁻⁸). R pprof's Wald z there are 0/0,
  pprof_py's are ±4·10⁻⁸, and pprof_spark leaves them untested (X-048).
- `resampling` warned on 4 of lgre-base's 40 providers that simulated tails reached the floor. Its p-values are
  checked against `poibin_exact`'s only informationally, since mid-p and exact tails differ.

## 13. Implementation notes (round 117, engine; no statistical change)

- The kernel's `at`, and the engine's `LogisticRE.fitAt(df, spec, options, σ, β)`, give the fit's quantities at given
  parameters without a search: the Laplace deviance, the modes, Var(β̂) and the provider table. `fit` and `fitAt`
  share one pipeline. The provider table's `records` was 0 for nAGQ = 0 fits since round 112, because only the
  nAGQ = 1 path counted them; both paths count them now.
- `LogisticRE.training` rebuilds the training data's working set with the fit's centring and checks its fingerprint
  and row count (API-3); `LogisticREInference` reads rows only through it.
- `engine.logistic.LogisticREInference`:
  - `summary`;
  - `test`: `wald` from the provider table, untested at a boundary fit (X-048); `poibin_exact` and `resampling`
    within the provider-local blocks, through `PoissonBinomial`'s tails, inversion and seeded bootstrap, as slice 2c;
  - `alphaIntervals`;
  - `measures` and `smIntervals`, one pass with per-provider sums and Taylor-bin moments of η (X-026);
  - `profileSigma`, minimizing over β with `BoundedQuasiNewton.minimizeWithGradient` and Brent's method to 10⁻¹⁰.
- `resampling` follows pprof_py's `count_test`: a simulated tail at the floor 0.5/`n_resample` is replaced by the
  exact Poisson-binomial tail. Before this was found, round 117's first run gave lgre-base 5·10⁻⁵ where pprof_py
  gave 2.3·10⁻¹².
- `LogisticREInferenceSuite` (9 tests):
  - lockstep at pprof_py's tightened (σ, β) on seven cases, the full battery on lgre-base, lgre-text and lgre-offset:
    `summary`, eleven test variants, option `alpha`, the measures, and the `SM` intervals for each alternative;
  - `resampling` against pprof_py within 4.5 Monte Carlo standard errors of a two-sided p, which is twice a
    simulated tail;
  - `profileSigma` at T-opt against pprof_py and lme4;
  - the refusals.
  `poibin_exact` is compared under X-024: z where pprof_py's p ≥ 10⁻⁶, and p within 10⁻¹⁴ absolute. pprof_py's FFT
  tails are exact only to about 10⁻¹⁶, and lgre-base's and lgre-offset's small p-values differed by up to
  1.5·10⁻⁶ relative.
