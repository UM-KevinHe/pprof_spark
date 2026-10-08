# Specification: logistic fixed-effect model — standardized measures (Phase 2d)

- Status: Draft, awaiting approval (D-36). No 2d code is written before approval (NN-2).
- Builds on: slices 2a to 2c ([plan.md](plan.md)).
- Decision references: D-36; X-026; OI-49, OI-58.
- Reference: pprof_py v0.7.0 (`9320766`): `measures/logistic/fixed_effect.py`
  (`calculate_standardized_measures`), `inference/standardized.py` (`standardized_measure`,
  `_reference_gamma`, `_gamma_se`), `inference/zstat.py` (`z_statistic`, transforms),
  `inference/logistic/fixed_effect/provider_tests.py` (`test_standardized`). R pprof 1.0.3's
  `SM_output.logis_fe` as the R reference.

Probe results are sandbox evidence from round 37.

## 1. Estimands

With ηᵢ = xᵢᵀβ̂, wᵢ the trials (1 for Bernoulli rows), σ the logistic function and γ₀ the reference
effect of 2c (median, record-weighted mean, or a number), over the fitted rows (the population):

| Measure | Estimate | Standard error (`standardized_measure`) | Reference value |
|---|---|---|---|
| Indirect ratio | Oⱼ/Eⱼ, Eⱼ = Σᵢ∈ⱼ wᵢσ(γ₀ + ηᵢ) | √Var(Oⱼ)/Eⱼ, Var(Oⱼ) = Σᵢ∈ⱼ wᵢπᵢ(1 − πᵢ) with π at γ₀ (`null`, default) or at γ̂ⱼ (`fitted`) | 1 |
| Indirect rate | the ratio times the crude rate O/W | the ratio's SE times O/W | O/W |
| Direct rate | Dⱼ/W, Dⱼ = Σᵢ wᵢσ(γ̂ⱼ + ηᵢ) over the whole population, W = Σ wᵢ | Sⱼ/W·SE(γ̂ⱼ), Sⱼ = Σᵢ wᵢσ(1 − σ) at γ̂ⱼ | D(γ₀)/W |
| Direct ratio | Dⱼ/O, O the population's events | Sⱼ/O·SE(γ̂ⱼ) | D(γ₀)/O |
| Provider effect | γ̂ⱼ | SE(γ̂ⱼ) | γ₀ |

SE(γ̂ⱼ) is the variance at the average case mix (2a, `model`, default), the cluster-robust one
(2b, `robust`) or the β-known one (2b, `robust_fixed_beta`); the robust ones apply to direct
measures and the provider effect only, as in pprof_py. Indirect measures with Eⱼ ≤ 1e-10 are NaN.

`calculate_standardized_measures` reports the same ratios with rates in percent clipped to [0, 100]
(indirect: ratio × 100·O/W; direct: 100·Dⱼ/W) and, optionally, R's extreme observations: a total of
`extremeTrials` trials with η = 0, which add `extremeTrials`·σ(γ̂ⱼ) to Dⱼ and to W.

## 2. Tests on a measure (`test_standardized`)

z = (T(x) − T(x₀))/(T′(x)·SE) on a working scale T: `identity`, `logit` (SE/(x(1 − x))) or `log`
(SE/x); `auto` takes logit for the direct rate, log for the direct ratio and the identity otherwise.
The null value x₀ is the measure's reference value (default, so the test agrees with a test of
γⱼ = γ₀) or a given number. p-values, flags, `level` and `critical` follow 2c's theoretical null.
Intervals: x ± c·SE on the working scale, transformed back, clipped to [0, 1] for identity-scale
rates and [0, ∞) for identity-scale ratios (`bounds = auto`). On the log scale a provider without
events cannot be tested (NaN), as in pprof_py.

## 3. Direct sums at scale (OI-49)

Dⱼ and Sⱼ sum over every row for every provider: O(n·m) evaluations, 10¹⁵ at the top of the
envelope. Both depend on the rows only through the distribution of (ηᵢ, wᵢ), so pprof_spark groups the
rows into bins of width h = 0.1 on η, with centres c_b = h·round(η/h), and expands σ around each
centre to order K = 8:

Σᵢ wᵢσ(γ + ηᵢ) = Σ_b Σ_{k ≤ K} σ⁽ᵏ⁾(γ + c_b)·M_{b,k} + remainder, with M_{b,k} = Σᵢ∈b wᵢ(ηᵢ − c_b)ᵏ/k!,

and the same with σ′ = σ(1 − σ) for Sⱼ. Since abs(σ⁽⁹⁾) ≤ 7.75 and abs(σ⁽¹⁰⁾) ≤ 22.3, the remainder per
unit weight is below 4.2e-17 for Dⱼ and 1.2e-16 for Sⱼ: below the rounding of a double. The cost is one
pass over the rows for the moments, then O(m × bins × K) for the providers.

| Population | Relative difference from the exact sums (h = 0.1, K = 8): direct, variance | Time, exact against binned (numpy) |
|---|---|---|
| lfe-many: 14,750 rows, 500 providers | 8.2e-16, 4.9e-16 | 0.04 s, 0.08 s |
| synthetic: 200,000 rows, 2,000 providers | 7.9e-16, 6.6e-16 | 4.6 s, 0.4 s |

pprof_py sums exactly. The binned sums agree with it to rounding (class C, X-026); an `exact` method
(O(n·m), for small problems and verification) stays available.

## 4. Algorithm and distributed plan

| Output | Passes | Driver | Executors |
|---|---|---|---|
| γ₀, x₀, crude rate | none | from the fit (m values, guarded as 2a) | — |
| Indirect measures | 1 | — | Provider-local Oⱼ, Eⱼ and Var(Oⱼ) on the fit's working set (2c's score pass) |
| Direct moments | 1 + a shuffle by bin | none of n-scale | Per block, the moments of each bin in canonical order; a shuffle by bin; per bin, block partials in block order with compensated sums |
| Direct sums | 1 over providers | the bins' moments (bins × (K + 1) doubles), broadcast | Per provider Dⱼ and Sⱼ from the moments |
| Tables and tests | — | — | One row per provider, distributed |

Bins are keyed by the integer round(η/h), with η from the kernels' arithmetic, so results are bitwise
invariant to partitioning (R0). The `exact` method runs the direct sums as a block kernel with γ
shipped per block (§6.7), m-length partials reduced in block order, under a driver budget.

## 5. Outputs

`LogisticStandardization.measures(df, fit, kinds, reference, providers, extremeTrials)` gives
pprof_py's two tables (indirect: provider, `indirect_ratio`, `indirect_rate`, `observed`,
`expected`; direct: provider, `direct_ratio`, `direct_rate`, `observed`, `expected`, `n_pop`);
`measure(df, fit, measure, reference, variance, indirectVariance)` the estimates, standard errors and
reference value; `test(...)` the provider-test table of 2c (`PROVIDER_TEST_COLUMNS`). The data must be
the fit's (fingerprint, API-3). Python: `LogisticFixedEffectModel.standardized_measures`,
`standardized_measure` and `test_standardized`.

## 6. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| Robust variance for an indirect measure | Raises | Fails with a message |
| Robust variance without a cluster column | Raises | Fails with a message |
| Eⱼ ≤ 1e-10 | NaN ratio and SE | Same |
| Log scale, provider without events | NaN | Same |
| External standard population (`StandardPopulation.extend`) | Supported | Not in this slice (OI-58) |
| Unknown measure, transform, variance or reference | Raises | Fails with a message |

## 7. Validation plan

Fixtures (next round), every case: `calculate_standardized_measures` (indirect and direct; median,
mean and a number; with and without extreme observations), `standardized_measure` for the five
measures (`model`; `robust` and `robust_fixed_beta` on lfe-clustered; indirect `null` and
`fitted`), and `test_standardized` for each measure with its automatic scale and on the identity
scale; R pprof's `SM_output.logis_fe` (indirect and direct, rate and ratio) where its sourcing works.
Tolerance classes: T-meas (measures, expected counts, standard errors), T-test (z), T-p, T-flag,
T-coef (limits). The binned direct sums are also compared with the `exact` method under T-part.
Negative controls: γ₀ shifted by 0.01, one outcome flipped, the logit and log scales swapped.

## 8. Known discrepancies

| ID | Summary | Class | Proposed decision (D-36) |
|---|---|---|---|
| X-026 | Direct sums by binned Taylor expansions (h = 0.1, order 8) instead of pprof_py's exact sum over every row: a remainder below 1.2e-16 per unit weight; probe differences up to 8.8e-16 relative | C | Use the binned sums by default, record the method and bound with the result, keep `exact` as an option; §7.4's rule (approximations only where the reference uses them) yields here because the remainder is below rounding |
