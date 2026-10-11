# Specification: linear fixed effects — provider tests, intervals and standardized differences (Phase 3b)

- Status: approved with D-46 (2026-10-09).
- Reference: pprof_py v0.7.0's `LinearFixedEffectModel.test`, `calculate_confidence_intervals` and
  `calculate_standardized_measures`, with the shared `reference_effect`, `t_to_z`, `effect_test` and
  `provider_test`; R pprof 1.0.3's `test.linear_fe`, `confint.linear_fe` and `SM_output.linear_fe` (pure R, OI-60)
  where they follow pprof_py's definitions (X-036); base R for t tails and t-based limits. Plan: [plan.md](plan.md);
  the fit: [fixed-effect-estimation.md](fixed-effect-estimation.md).

## 1. Reference effect

γ₀ is the median of the fitted effects γ̂ⱼ over all providers (unweighted; for even m the mean of the two middle
values, as NumPy's `median`), their size-weighted mean Σⱼ nⱼγ̂ⱼ / n (pprof_py's `reference_effect` with sizes; R
pprof's `sum(n.prov * gamma) / n`), or a given number. One γ₀ serves the tests, the intervals and the measures.

## 2. Provider tests (`test`)

For provider j, tⱼ = (γ̂ⱼ − γ₀)/seⱼ, with seⱼ from the fit's variance option (slice 3a §3) and ν = n − m − p degrees
of freedom; zⱼ = sign(tⱼ)·Φ⁻¹(1 − F̄ν(|tⱼ|)) maps t to the normal scale by equal tail probability (pprof_py's
`t_to_z`), with F̄ν from `StudentT`.

| Item | Definition |
|---|---|
| p-value | Two-sided 2F̄ν(\|tⱼ\|), `greater` F̄ν(tⱼ), `less` Fν(tⱼ): the t test's, equal to the normal tails of zⱼ |
| Flags | Slice 2c §2: +1, −1 or 0 by zⱼ against c = Φ⁻¹(1 − α/2) (two-sided) or Φ⁻¹(1 − α) (one-sided), or a given `critical` c; one-sided flags are 0 or +1 (`greater`) and 0 or −1 (`less`) |
| Limits | γ̂ⱼ ∓ q·seⱼ with q the t quantile at Φ(c), so q = t₁₋α/₂,ν by default; one-sided limits are open on the other side (±∞); `interval` `inversion` and `scale_only` coincide under the theoretical null |
| Null model | The theoretical null only: null_mean 0, null_sd 1, z_adjusted = z_raw; empirical nulls are a later feature (PROJECT_CONTEXT §4) |
| Providers | All, or the subset given (keys compared as text) |

Output: one row per provider, keyed by provider with `records`, and slice 2c's columns: estimate (γ̂ⱼ), se,
null_value (γ₀), transformed (γ̂ⱼ), se_transformed (seⱼ), null_transformed (γ₀), z_raw, null_mean, null_sd,
null_group, z_adjusted, p_value, flag, ci_lower, ci_upper. When |tⱼ| is large enough for F̄ν to underflow, zⱼ comes
from the tail's logarithm, so the provider keeps its p-value (0 when it underflows), flag and limits (X-035).

## 3. Intervals (`calculate_confidence_intervals`)

| Option | Definition |
|---|---|
| `gamma` | γ̂ⱼ ∓ t₁₋α/₂,ν·seⱼ, two-sided only (pprof_py and R pprof refuse one-sided ones); columns: key, gamma, lower, upper |
| `SM`, indirect | (nⱼLⱼ + Sⱼ − Eⱼ)/nⱼ and (nⱼUⱼ + Sⱼ − Eⱼ)/nⱼ, where Lⱼ and Uⱼ are the effect's limits for the alternative (one of them ±∞ when one-sided), Sⱼ = Σᵢ∈ⱼ xᵢᵀβ̂ and Eⱼ as in §4; columns: §4's indirect table with lower and upper |
| `SM`, direct | Lⱼ − γ₀ and Uⱼ − γ₀ (pprof_py's `lower_gamma − gamma_null`); columns: §4's direct table with lower and upper |

`level` (0.95), `alternative` (two_sided, greater, less), `reference` and `providers` as in §1 and §2.

## 4. Standardized differences (`calculate_standardized_measures`)

| Measure | Definition (pprof_py, and R pprof's `SM_output.linear_fe`) |
|---|---|
| Indirect | Oⱼ = Σᵢ∈ⱼ yᵢ and Eⱼ = Σᵢ∈ⱼ (γ₀ + xᵢᵀβ̂); difference (Oⱼ − Eⱼ)/nⱼ; columns: key, indirect_difference, observed, expected |
| Direct | observed = Σᵢ (γ₀ + xᵢᵀβ̂) over all records (one value) and expectedⱼ = nγ̂ⱼ + Σᵢ xᵢᵀβ̂; difference (expectedⱼ − observed)/n; columns: key, direct_difference, observed, expected |

Both differences equal γ̂ⱼ − γ₀ algebraically (round 62's probe: within 7.5e-16); the sums are computed as defined.

## 5. Data and API

| Function | Needs |
|---|---|
| `LinearProviderTests.test`, intervals with option `gamma` | The fit alone: γ̂, se and ν from its provider table and summary |
| `LinearStandardization.measures`, intervals with option `SM` | The fit and its training data, checked against the fit's fingerprint (API-3), for Oⱼ, Sⱼ and Σᵢ xᵢᵀβ̂ |

## 6. Distributed plan

1. γ₀: the provider table's `records` and `gamma`, collected to the driver in key order under `DriverGuards`
   (provider-level values, m of each; NN-3 concerns patient-level data); the median by sorting, the size-weighted
   mean by a compensated sum in key order, so γ₀ is deterministic.
2. Tests and `gamma` intervals: a projection of the distributed provider table (tⱼ, zⱼ, p-value, flag and limits per
   row), with γ₀, ν and the critical values as literals.
3. Measures and `SM` intervals: one pass over ProviderLocal blocks of the training data (fingerprint checked):
   per provider Oⱼ and Sⱼ by Neumaier sums in canonical order, and Σᵢ xᵢᵀβ̂ reduced in block order (§6.8); the result
   is a distributed table joined to the provider table by key.

## 7. Edge cases

| Case | pprof_py (round 67 probe) | pprof_spark |
|---|---|---|
| A provider whose t tail underflows (t = 549 in the probe) | z = ∞ becomes NaN: p-value, flag and limits NaN, the provider untested | z from the log tail; p-value (0 when it underflows), flag ±1 and limits γ̂ⱼ ∓ q·seⱼ (X-035) |
| σ̂ = 0 (an exact within fit) | Not probed | tⱼ = ±∞ gives p-value 0 and flag ±1; tⱼ = 0/0 (γ̂ⱼ = γ₀) is untested (NaN), with the fit's warning (NN-10) |
| Even m | NumPy's median | The mean of the two middle values |
| Providers with one record | seⱼ from the fit's variance option | The same |

## 8. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `test(providers, reference, null_model=None, alternative, level, critical, interval)` | `LinearProviderTests.test(fit, reference, alternative, level, critical, providers)` |
| `calculate_confidence_intervals(providers, level, option, stdz, reference, alternative)` | `LinearStandardization.intervals(fit, …)` for `gamma`; `LinearStandardization.intervals(df, fit, …)` for `SM` |
| `calculate_standardized_measures(providers, stdz, reference)` | `LinearStandardization.measures(df, fit, kinds, reference, providers)` |
| `funnel_limits` and the plots | Out of scope (plan §5) |

## 9. Validation plan

Fixtures extend `fixtures/linear` (all five cases) with pprof_py's `test` (two-sided at the median; `greater` and
`less`; the size-weighted mean and a given reference; `critical` 2.5; a providers subset),
`calculate_confidence_intervals` (`gamma`; `SM` indirect and direct, two-sided and one-sided) and
`calculate_standardized_measures` (median and mean); R pprof's `test.linear_fe` and `SM_output.linear_fe` with the
`full` variance; base R's tails 2·pt(−|t|, ν) and limits γ̂ ∓ qt(1 − α/2, ν)·se from `lm`'s variances (R pprof's
`confint` is not a reference, X-036); and a new case, lin-extreme, with one provider shifted far enough for its t
tail to underflow (X-035). Classes: T-test (t and z), T-p (p-values against base R everywhere, against pprof_py
where its p-values are defined), T-flag (flags), T-coef (limits), T-meas (measures and their intervals). Negative
controls as D-09 requires.

## 10. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-035 | B | pprof_py leaves a provider untested when its t tail underflows | Approved (D-46) |
| X-036 | — | R pprof 1.0.3's linear inference departs from pprof_py's definitions in places (normal quantiles in `confint` with the `full` variance, normal tails with the `simplified` one, lower-tail p-values by subtraction) | Approved (D-46) |

## 11. Fixture findings (round 68)

1. `fixtures/linear` gains `inference_pprof_py.json` and `inference_r.json` for every case, and a sixth case,
   lin-extreme (lin-base with provider 10's outcomes + 60), which also enters slice 3a's calibration. lin-many keeps
   the two-sided test, the `gamma` and two-sided `SM` intervals and the median measures; the other five cases carry
   all of §9. R pprof's three linear files are pinned in REFERENCE.lock by SHA-256. Regeneration is byte-identical
   from scratch and from the committed inputs.
2. Calibration (`docs/parity/linear-inference-calibration.md`, 386 checks) passes: pprof_py agrees with base
   R within the classes, worst T-meas 0.5 (lin-shifted: R pprof SM_output indirect expected vs pprof_py), T-test 0.31 (lin-shifted: R pprof test.linear_fe statistic vs pprof_py's t), T-p 3.1e-05 (lin-shifted: test number p_value vs base R), T-coef 6e-06 (lin-shifted: test critical ci_upper vs base R); R pprof's statistics, flags and standardized differences agree where X-036 allows;
   every negative control misses by at least 2.63e+03. No new tolerance class is needed.
3. X-035 on lin-extreme: pprof_py leaves one provider untested (z, p-value, flag and limits NaN) where base R's
   log-scale tail gives z, the p-value 0, the flag and the limits; the calibration counts such positions instead of
   comparing them.
4. X-036: R pprof's two-sided p-values differ from pprof_py's by up to 1e-4 relative on lin-base and 2.6e-3 on
   lin-extreme (its lower tails by subtraction); its flags agree everywhere.

## 12. Implementation notes (round 69; no statistical change)

1. numerics: `Normal.logUpperTail` (log of the Mills ratio plus log φ for z ≥ 1.5, so it never underflows),
   `Normal.upperQuantileOfLog` (Newton on the log scale below e⁻⁶⁹⁰), `StudentT.logTwoSided` (the continued fraction
   with its prefactor kept as a logarithm when the tail underflows) and `StudentT.toZ` (pprof_py's `t_to_z`, finite
   for every finite t: X-035). A log tail accurate to ε·ν/(1 + t²) gives z to about that over z², since
   dz = −d(log p)/z. Measured against mpmath at 50 digits: z within 1.6e-15 relative down to log tails of −88,754.
2. engine `LinearProviderTests`: `reference` (γ₀ on the driver from the provider table in key order under
   `DriverGuards`: NumPy's median, or the size-weighted mean by a compensated sum), `test` (a typed map over the
   distributed provider table, keys carried as text and cast back; slice 2c's columns, `records` added) and
   `gammaIntervals` (column expressions). Measures and `SM` intervals follow in round 70.
