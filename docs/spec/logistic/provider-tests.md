# Specification: logistic fixed-effect model — provider tests (Phase 2c)

- Status: Approved by the maintainer, 2026-10-08 (D-35), with X-024 and X-025 as recommended.
- Builds on: [fixed-effect-estimation.md](fixed-effect-estimation.md) (2a) and
  [covariate-inference.md](covariate-inference.md) (2b); plan [plan.md](plan.md) (slice 2c).
- Decision references: D-35; X-024 and X-025; OI-52.
- Reference: pprof_py v0.7.0 (`9320766`): `inference/logistic/fixed_effect/provider_tests.py`
  (`test`), `inference/count_tests.py` (`count_test`, `PlugIn`, `MonteCarlo`),
  `inference/effect_tests.py` (`reference_effect`, `poibin_tails`, `bootstrap_tails`,
  `z_from_tails`, `effect_test`, `invert_decreasing`), `inference/decision.py` (`provider_test`,
  `PROVIDER_TEST_COLUMNS`). R pprof 1.0.3's `test.logis_fe` (with `poibin` 1.6) as a second
  reference, and mpmath for exact tails.

Probe results are sandbox evidence from round 34.

## 1. Estimands

For provider j and a reference effect γ₀, the hypothesis γⱼ = γ₀, tested by one of four methods,
all at the fit's β̂ (pprof_py's `test`):

| Method | Statistic |
|---|---|
| `poibin_exact` (default) | The observed count Oⱼ against its Poisson-binomial distribution under γ₀: rows independent with πᵢ = σ(γ₀ + xᵢᵀβ̂), clipped to [1e-10, 1 − 1e-10], binomial rows expanded into their trials |
| `score` | z = (Oⱼ − Eⱼ)/√Vⱼ with Eⱼ = Σ nᵢπᵢ and Vⱼ = Σ nᵢπᵢ(1 − πᵢ) at γ₀ (π clipped); z = 0 when Vⱼ < 1e-14 |
| `wald` | z = (γ̂ⱼ − γ₀)/SE, SE from Var(γ̂ⱼ + x̄ᵀβ̂) of 2a (pprof_py C34), with a warning for providers that have no events or only events |
| `bootstrap_exact` | The exact test's tails estimated from `nResample` (10,000) simulated counts under γ₀ |

## 2. Formulas and conventions

| Convention | Value |
|---|---|
| Reference γ₀ | `median` (default): numpy's median of γ̂ over the fitted providers, degenerate ones included; `mean`: their average weighted by records; or a number |
| Exact tails | At the observed count o: mid-p tails upper = P(X > o) + P(X = o)/2 and lower = P(X < o) + P(X = o)/2 for two-sided tests; P(X ≥ o) or P(X ≤ o) for one-sided |
| Tails to z | Two-sided: z = Φ̄⁻¹(upper) when upper ≤ lower, otherwise −Φ̄⁻¹(lower); `greater`: Φ̄⁻¹(upper); `less`: −Φ̄⁻¹(lower); tails below 1e-300 are raised to it and abs(z) is capped at Φ̄⁻¹(1e-300) |
| Null | The theoretical N(0, 1): p two-sided 2Φ̄(abs(z)), one-sided Φ̄(z) or Φ(z); `null_mean` 0, `null_sd` 1. Fixed and empirical nulls are not in this slice |
| Flags | +1 when significant above γ₀, −1 below, 0 otherwise, at `level` (0.95) or a given `critical` value c (default Φ⁻¹(1 − α/2), or Φ⁻¹(1 − α) one-sided) |
| Intervals | Wald: γ̂ⱼ ± c·SE. Exact: by inverting the test, solving z(g) = c and z(g) = −c from γ̂ⱼ outward (steps 0.5 growing by 1.6 to a span of 40, then Brent's method with xtol 1e-10 and rtol 1e-12); ±∞ beyond the span; one side open for one-sided tests. Score and bootstrap: none |
| Binomial trials | The exact test expands each row's trials; above 20,000 trials in one provider it fails, as pprof_py (score or bootstrap instead) |
| Bootstrap | Draws by a counter-based generator keyed on (seed, provider key, replicate, row) (STAT-2); mid-p tails from the draws; no exact replacement at the floor (pprof_py's logistic bootstrap has none) (X-025) |
| Providers | Every fitted provider, or a requested subset (its limits only) |

## 3. Exact Poisson-binomial algorithm (OI-52)

pprof_py computes the distribution with `fast_poibin` (FFT) and the upper tail as 1 − cdf + pmf/2,
which loses relative accuracy as the upper tail shrinks (X-024). pprof_spark computes the smaller
tail directly by the exact recursion pmfₖ ← pmfₖ(1 − πᵢ) + pmfₖ₋₁πᵢ, truncated at the observed count:
over the events when o lies at or below the expected count Σπᵢ (cost n·o), over the non-events with
probabilities 1 − πᵢ when it lies above (cost n·(n − o)); the other tail is one minus it. All terms
are non-negative, so both tails keep relative accuracy near machine precision.

| Observed count (500 rows, π from 0.1 to 0.3) | Smaller tail (mpmath, 60 digits) | pprof_py's relative error | Recursion's relative error |
|---|---|---|---|
| 5 | lower 2.36e-41 | 1.5e-15 | 1.5e-15 |
| 40 | lower 5.51e-14 | 7.0e-16 | 7.0e-16 |
| 100 | upper 0.480 | 1.1e-15 | 9.8e-17 |
| 160 | upper 6.37e-11 | 1.4e-5 | 4.2e-16 |
| 200 | upper 1.33e-25 | 6.7e9 | 1.5e-15 |

## 4. Algorithm and distributed plan

γ₀ is computed on the driver from the fit's provider effects (m values, guarded as in 2a). The
tests then run provider-locally on the ProviderLocal working set, with γ₀, β̂ and the provider
effects shipped as in 2a (§6.7):

| Method | Per provider (executors) | Cost |
|---|---|---|
| `poibin_exact` | π at γ₀, the smaller tail, z; for the limits, the same at each trial g of the inversion (about 20 to 40 evaluations per limit) | O(nⱼ·min(oⱼ, nⱼ − oⱼ)) per evaluation |
| `score` | Oⱼ, Eⱼ, Vⱼ | O(nⱼ) |
| `wald` | none: from the fit | O(1) |
| `bootstrap_exact` | `nResample` simulated counts | O(nResample·nⱼ) |

Results are a distributed table (one row per provider, `PROVIDER_TEST_COLUMNS` plus observed,
expected, trials and records); nothing n-scale reaches the driver. Exact tests are provider-local
and deterministic, so results are bitwise invariant to partitioning (R0). Packing blocks by
estimated cost rather than by records (§6.6) is OI-57.

## 5. Outputs

`LogisticFE.providerTests(df, fit, method, reference, alternative, level, critical, providers,
nResample, seed)` returns the table above with pprof_py's column names and meanings (as the Cox
provider tests, round 21); data are checked against the fit's fingerprint (API-3). The Python
wrapper gains `provider_tests(...)`.

## 6. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| Provider with no events or only events | Exact and score tests are defined; the Wald test warns | Same |
| Binomial provider above 20,000 trials, exact test | Raises for the whole call | Same, naming the provider count |
| Vⱼ < 1e-14 (score) | z = 0 | Same |
| Tail below 1e-300 | Raised to 1e-300; z capped | Same |
| Limit beyond the span of 40 | ±∞ | Same |
| Unknown `method`, `alternative` or `reference`; `level` outside (0, 1) | Raises | Fails with a message |

## 7. Validation plan

Fixtures (round 35, calibrated in [logistic-calibration.md](../../parity/logistic-calibration.md)), for every case: pprof_py's `test` for `poibin_exact` (two-sided, `greater`,
`less`; reference `median`, `mean` and a number), `score`, `wald` and `bootstrap_exact` (seeded);
R pprof's `test.logis_fe` for `exact.poisbinom`, `score` and `wald` where its sourcing works; and,
function-level, Poisson-binomial tails of fixed probability vectors from mpmath at 60 digits,
including tails down to 1e-40 in both directions. Tolerance classes: T-test (z), T-p (p-values;
where pprof_py's upper tail is below 1e-7, mpmath decides, X-024), T-flag (flags, near-threshold
cases reported), T-coef (limits). Bootstrap: p-values within four Monte Carlo standard errors of
the exact test's, over every provider (STAT-2), and bitwise reproducible across partitionings.
Negative controls: γ₀ shifted by 0.01, one outcome flipped, the one-sided alternatives swapped.

## 8. Known discrepancies

| ID | Summary | Class | Proposed decision (D-35) |
|---|---|---|---|
| X-024 | pprof_py's exact upper tails are 1 − cdf + pmf/2 from an FFT distribution, accurate only to about 1e-16 absolute: relative error 1.4e-5 at 6.4e-11, none left at 1.3e-25 (§3); lower tails and the flags at usual levels are unaffected | B | Correct it: compute the smaller tail directly; compare with mpmath where pprof_py's upper tail is below 1e-7 |
| X-025 | The bootstrap draws cannot be numpy's: pprof_spark's generator is counter-based and partition-invariant (STAT-2) | C | Distributional parity only, as planned (plan §3) |
