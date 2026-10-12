# 5. Standardized measures

[Tutorial home](README.md) · previous: [4. Provider tests](04-provider-tests.md) ·
next: [6. Distributed execution](06-distributed-execution.md)

Standardized measures put each provider's performance on an interpretable scale: a ratio of observed
to expected events, or the event rate the whole population would have if treated by that provider.
This chapter covers slice 2d ([standardization.md](../../spec/logistic/standardization.md)): the two
kinds of standardization, their standard errors and tests, and why the direct kind needs a numerical
idea of its own at scale.

## 5.1 Indirect standardization

**Definition.** With ηᵢ = xᵢᵀβ̂ (case mix only), wᵢ the trials (1 for Bernoulli records) and γ₀ the
reference of chapter 4, provider j's expected events are what its own patients would have had at a
reference provider:

```math
E_j = \sum_{i \in j} w_i\,\sigma(\gamma_0 + \eta_i), \qquad
\text{ratio}_j = \frac{O_j}{E_j}, \qquad
\text{rate}_j = \text{ratio}_j \cdot \frac{O}{W},
```

with O/W the crude rate of the population. The standard error of the ratio is √Var(Oⱼ)/Eⱼ, with
Var(Oⱼ) = Σᵢ∈ⱼ wᵢπᵢ(1 − πᵢ) at γ₀ (`null`, default) or at γ̂ⱼ (`fitted`); a provider with Eⱼ ≤ 1e-10 gets
NaN. Indirect measures answer "how did this provider do with its own patients?" and need one
provider-local pass, the same as the score test's.

**lfe-base, provider 22**: 17 observed against 28.196 expected at the median effect, a ratio of 0.603.
R pprof's `SM_output.logis_fe` reports 0.60292, and the probe's provider test has the same observed
and expected counts.

## 5.2 Direct standardization

**Definition.** Provider j's direct measure applies its effect to **every** record of the population:

```math
D_j = \sum_{i=1}^{n} w_i\,\sigma(\hat\gamma_j + \eta_i), \qquad
\text{rate}_j = \frac{D_j}{W}, \qquad \text{ratio}_j = \frac{D_j}{O},
```

with reference value D(γ₀)/W or D(γ₀)/O. By the delta method its standard error is Sⱼ/W·SE(γ̂ⱼ) for the
rate, with Sⱼ = Σᵢ wᵢσ′(γ̂ⱼ + ηᵢ) and σ′ = σ(1 − σ). SE(γ̂ⱼ) is the variance at the average case mix
(`model`, default), or one of the robust variances of chapter 3 when the fit has clusters. Direct
measures answer "how would the whole population fare at this provider?", which makes providers with
different case mixes directly comparable.

**lfe-base, provider 22**: R's direct rate is 30.49%, against a crude rate of 47.72% (513 of 1,075). By
the formulas, that is a direct ratio of about 0.64.

`calculate_standardized_measures`-style tables report rates in percent clipped to [0, 100], and can add
R's "extreme observations": a number of extra trials with η = 0 that add `extremeTrials`·σ(γ̂ⱼ) to Dⱼ
and to W.

## 5.3 Why direct standardization is expensive

Dⱼ and Sⱼ sum over every record for every provider: n·m evaluations of σ. At the top of the project's
envelope, 10⁹ records and 10⁶ providers, that is 10¹⁵ evaluations for one measure. pprof_py computes the
sum exactly; that cannot scale.

## 5.4 Binned Taylor sums (X-026)

Both sums depend on the records only through the distribution of (ηᵢ, wᵢ). pprof_spark groups records
into bins of width h = 0.1 on η, with centres c_b = h·rint(η/h), and expands σ around each centre to
order K = 8:

```math
\sum_i w_i\,\sigma(\gamma + \eta_i) = \sum_b \sum_{k=0}^{8} \sigma^{(k)}(\gamma + c_b)\, M_{b,k} + R,
\qquad M_{b,k} = \sum_{i \in b} w_i \frac{(\eta_i - c_b)^k}{k!} ,
```

and the same with σ′ for Sⱼ. Within a bin |ηᵢ − c_b| ≤ 0.05, and |σ⁽⁹⁾| ≤ 7.75, so the remainder per
unit weight is below 7.75·0.05⁹/9! ≈ 4.2e-17; with |σ⁽¹⁰⁾| ≤ 22.3 the bound for Sⱼ is 1.2e-16. Both are
below the rounding of a double. The derivatives are polynomials in σ, P₀(s) = s and
Pₖ₊₁(s) = Pₖ′(s)(s − s²), evaluated by Horner's rule
([TaylorBins.scala](../../../numerics/src/main/scala/pprof/spark/numerics/TaylorBins.scala#L13-L78)).

The cost becomes one pass over the records for the moments, then O(m × bins × 9) for the providers,
independent of n. The specification's probe found relative differences from the exact sums of 8.2e-16
on `lfe-many` and 7.9e-16 on 200,000 records and 2,000 providers, where numpy's exact sum took 4.6 s and
the binned one 0.4 s. pprof_spark records the method and the bound with the result and keeps an
`exact` method for small problems and verification. This is an approximation pprof_py does not make,
accepted by D-36 because its error is below rounding (class C, X-026).

## 5.5 How it runs

| Step | Executors | Driver |
|---|---|---|
| Indirect measures | Provider-local Oⱼ, Eⱼ and Var(Oⱼ) on the fit's working set | — |
| Bin moments | Per block, the moments of each bin in canonical row order; a shuffle by bin key; per bin, block partials added in block order with compensated sums | Collects the bins' moments: bins × 9 doubles |
| Direct sums | Providers in chunks of 10,000 effects, with the moments captured by closure: each provider's Dⱼ and Sⱼ | — |
| Tables and tests | One row per provider | — |

Bins are keyed by the integer rint(η/0.1), with η computed by the kernels' arithmetic, so results are
bitwise invariant to partitioning (R0). The data must be the fit's (fingerprint, API-3)
([LogisticStandardization.scala lines 58–200](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticStandardization.scala#L58-L200)).

## 5.6 Tests on a measure

`LogisticStandardization.test(...)` tests a measure against its reference value on a working scale T:
z = (T(x) − T(x₀))/(T′(x)·SE), with T the identity, the logit (direct rates) or the log (direct ratios)
under `auto`. With the default null value, the test agrees with a test of γⱼ = γ₀. Intervals are formed
on the working scale and transformed back, clipped to [0, 1] for identity-scale rates and [0, ∞) for
ratios; p-values and flags follow chapter 4. A provider without events cannot be tested on the log
scale (NaN), as in pprof_py.

## 5.7 Where to look

| Piece | Code |
|---|---|
| Bins, moments, derivative polynomials, sums | [`TaylorBins`](../../../numerics/src/main/scala/pprof/spark/numerics/TaylorBins.scala) |
| Indirect and direct passes | [`LogisticStandardization`, lines 58–200](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticStandardization.scala#L58-L200) |
| Estimates, standard errors, reference value; pprof_py's tables; tests | [`measure`, `measures`, `test`](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticStandardization.scala#L216-L484) |

Tests: `LogisticStandardizationSuite` compares measures, standard errors and tests with pprof_py and R
pprof's `SM_output.logis_fe`, and the binned direct sums with the exact ones under T-part.
