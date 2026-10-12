# 4. Provider tests

[Tutorial home](README.md) · previous: [3. Covariate inference](03-covariate-inference.md) ·
next: [5. Standardized measures](05-standardized-measures.md)

Provider profiling ends in a decision per provider: is its effect above, below or consistent with a
reference? This chapter covers the four tests of slice 2c
([provider-tests.md](../../spec/logistic/provider-tests.md)), how the exact test is computed so that
its tails keep full relative accuracy, how its confidence limits are found, and how the tests run in
Spark.

## 4.1 The hypothesis and the reference

For provider j and a reference effect γ₀, the hypothesis is γⱼ = γ₀, with β fixed at β̂: case mix is
adjusted by the national model, and only the provider's own effect is tested. γ₀ is the median of the
fitted effects (default; numpy's median, degenerate providers included), their average weighted by
records, or a given number ([LogisticProviderTests.scala lines 59–71](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticProviderTests.scala#L59-L71)).

## 4.2 The exact Poisson-binomial test (default)

**Theory.** Under the hypothesis, provider j's observed events Oⱼ are a sum of independent Bernoulli
outcomes with probabilities πᵢ⁰ = σ(γ₀ + xᵢᵀβ̂), one per trial: a **Poisson-binomial** distribution.
The test compares Oⱼ with it, with mid-p tails for two-sided tests:

```math
\text{upper} = P(X > o) + \tfrac12 P(X = o), \qquad \text{lower} = P(X < o) + \tfrac12 P(X = o),
```

and P(X ≥ o) or P(X ≤ o) for one-sided tests. The tails become a z-statistic: two-sided,
z = Φ̄⁻¹(upper) when upper ≤ lower and −Φ̄⁻¹(lower) otherwise; tails below 1e-300 are raised to it and
|z| is capped at Φ̄⁻¹(1e-300). p-values and flags then use the standard normal: p = 2Φ̄(|z|), and a flag
of +1 or −1 when z is beyond the critical value c = Φ⁻¹(1 − α/2), 1.96 at level 0.95
([`decide`, lines 181–186](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticProviderTests.scala#L181-L186)).
Probabilities are clipped to [1e-10, 1 − 1e-10] and binomial rows are expanded into their trials, up to
20,000 trials per provider, as in pprof_py.

## 4.3 Computing the tails exactly (X-024)

pprof_py computes the whole distribution by FFT and reads the upper tail as 1 − cdf + pmf/2. That is
accurate to about 1e-16 **absolute**, so an upper tail of 6.4e-11 carries a relative error of 1.4e-5,
and one of 1.3e-25 is lost entirely (specification §3).

pprof_spark computes the **smaller** tail directly. The exact recursion over records,
pmfₖ ← pmfₖ(1 − πᵢ) + pmfₖ₋₁πᵢ, is truncated at the observed count: it runs over the events when o lies
at or below the expected count Σπᵢ, at cost n·o, and over the non-events with probabilities 1 − πᵢ when
o lies above it, at cost n·(n − o). Every term is non-negative, so the tail keeps relative accuracy near
machine precision; the other tail is one minus it, and one-sided z-statistics read a tail above one
half from its small complement
([`PoissonBinomial.tails` and `truncated`, lines 36–91](../../../numerics/src/main/scala/pprof/spark/numerics/PoissonBinomial.scala#L36-L91);
[`z`, lines 108–121](../../../numerics/src/main/scala/pprof/spark/numerics/PoissonBinomial.scala#L108-L121)).
`LogisticProviderTestsSuite` checks the tails against mpmath at 60 digits in both directions, down to
1e-40. Where pprof_py's upper tail is below 1e-7, the parity tests let mpmath decide, as X-024 records.

## 4.4 Confidence limits by inverting the test

The exact test gives an interval for γⱼ: the set of values g that the test would not reject. pprof_spark
solves z(g) = c and z(g) = −c, since z decreases as g increases, starting from γ̂ⱼ and stepping outward
by 0.5, growing by 1.6, until the sign changes, then refines by Brent's method to 1e-10; beyond a span of
40 the limit is ±∞, and one-sided tests have one open side (pprof_py's `invert_decreasing`;
[`invertDecreasing`, lines 131–163](../../../numerics/src/main/scala/pprof/spark/numerics/PoissonBinomial.scala#L131-L163)).
Each trial g recomputes the tail at that effect, so a limit costs about 20 to 40 tail evaluations.

## 4.5 The other three methods

| Method | Statistic | Notes |
|---|---|---|
| `score` | z = (Oⱼ − Eⱼ)/√Vⱼ with Eⱼ = Σnπ and Vⱼ = Σnπ(1 − π) at γ₀, π clipped | z = 0 when Vⱼ < 1e-14; no limits |
| `wald` | z = (γ̂ⱼ − γ₀)/SE, SE² = Var(γ̂ⱼ + x̄ᵀβ̂) of chapter 2 (pprof_py C34) | No pass over the data; limits γ̂ⱼ ± c·SE; a warning for degenerate providers, whose effects sit at the bound |
| `bootstrap_exact` | The exact test's tails estimated from `nResample` = 10,000 simulated counts under γ₀ | Draws from a counter-based generator keyed on (seed, provider position, replicate, trial), SplitMix64's finalizer: deterministic and partition-invariant (STAT-2); parity with numpy's draws is distributional only (X-025); no limits |

## 4.6 Worked example: lfe-base

The exact two-sided test against the median, at level 0.95 (probe):

- γ₀ = median(γ̂) = −0.364694.
- Of the 30 providers, 2 are flagged above and 4 below. The flags equal pprof_py's and R pprof's;
  z agrees with pprof_py to 1.9e-15 and with R to 1.1e-13; the limits agree with pprof_py to 2.5e-13.
- The most extreme is provider 22: 17 observed events against 28.196 expected under γ₀, so
  z = −3.110 and p = 0.0019. Its exact 95% interval for γ₂₂ is [−1.890, −0.692], around γ̂₂₂ = −1.275,
  and excludes γ₀; the provider is flagged −1.

## 4.7 How the tests run in Spark

`LogisticProviderTests.test(df, fit, method, reference, alternative, level, critical, providers,
nResample, seed)` ([lines 76–178](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticProviderTests.scala#L76-L178)):

1. γ₀ on the driver, from the fit's effects.
2. For every method but `wald`: rebuild the working set with the fit's provider plan and check the
   fingerprint (`LogisticFE.trainingWorkingSet`); ship the effects to the blocks as a broadcast-joined
   table (`withEffects`); on the executors, for each tested provider of each block, compute πᵢ⁰, the
   statistic, and for the exact test the limits.
3. Collect the result rows (m rows), recreate them as a Dataset, join the provider keys with a
   broadcast hint, and order by provider.

The probe recorded 15 Spark jobs for the exact test on `lfe-base`: nine jobs of preparation (the
fit's eleven without the two that collect provider sizes, since the plan comes from the fit), the effects' broadcast, the test pass,
and a small sort. The per-provider cost is O(nⱼ·min(oⱼ, nⱼ − oⱼ)) per tail evaluation, times the tens of
evaluations of the limits, so blocks packed by record count can be unbalanced for exact tests (OI-57).
The results pass through the driver as m rows before becoming a table again (review finding L-02).
Exact, score and bootstrap results are bitwise invariant to row order and partitioning
(`LogisticProviderTestsSuite`).
