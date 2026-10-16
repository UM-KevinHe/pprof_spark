# Production validation, October 2026: diagnosis and proposed fixes

The maintainer's team validated build `b670dbb` (round 81) against R and pprof_py v0.7.0 on production
data in five provider-profiling pipelines, at about 5 million rows and 60 to 142 features, and reported
ten issues: four for `CoxPH` (C1 to C4) and six for `LogisticFixedEffect` (L1 to L6). The three-stage
model showed none. This document gives, for each issue, the diagnosis as confirmed or refined in the source
and in pprof_py's, the code involved, a proposed fix, and its risks. No code has changed yet; the fixes that
change statistical behaviour need the maintainer's decision first (section 3).

## 1. Summary

| ID | Reported | Finding | Proposed fix | Default changes? |
|---|---|---|---|---|
| C1 | Baseline survival up to ~6% from R's `survfit.coxph` (High) | **Refined: a reference-point convention, not an estimator error.** pprof_spark's baseline is at x = 0 and offset 0 (X-014); R's and pprof_py's include exp(weighted mean offset), and `survfit` also centres x. Fit, increments and residuals match R's definitions to 10⁻¹⁵ on counting-process data | `baseline(reference=...)`: `raw`, `mean_offset`, `means`; `linear_predictor(centered=...)` | No |
| C2 | `row_id` must be unique; synthetic ids need materializing (Low) | Confirmed: residuals need a row id; the fingerprint hashes it, so ids that change on recomputation break later calls | Composite row keys, hashed deterministically; meanwhile `xxhash64` of natural keys | No |
| C3 | `max_stratum_rows` = 10⁶ too low for unstratified fits (Low) | Confirmed: a row cap that ignores p | A byte budget per stratum, the row cap kept as an override | Yes: larger strata pass |
| C4 | ~4.7× slower than pprof_py at 4.8 M rows (Info) | Expected below the crossover (PERF-1); likely dominated by compensated scalar kernels and rebuilt working sets | Measure (scale-test kit); reuse working sets; kernel and block-size work | — |
| L1 | No `add_providers()` (Medium) | Confirmed: pprof_py has it; pprof_spark's provider table holds fitted providers only | `add_providers(ids, gamma, se_gamma=0.01, group_sizes=None)` returning a new model | No |
| L2 | No null model on `test_standardized()` (High) | Confirmed: theoretical null only; pprof_py has `TheoreticalNull`, `FixedNull`, `EmpiricalNull` | `null_model`: theoretical and fixed first; empirical later | No |
| L3 | No `interval`, `bounds` on `test_standardized()` (Low) | Confirmed: intervals by inversion, always clipped | pprof_py's `interval` and `bounds` | No |
| L4 | `reference="mean"` gives expit(xβ + E[γ]) (Medium) | **Refined:** that is pprof_py's `null_value="reference"`; the pipelines need `null_value="mean"` (the mean of the providers' measures), which pprof_spark's wrapper cannot express | `null_value` = `reference`, `mean`, `median` or a number | No |
| L5 | No `blocks` for the logistic model; p = 142 exceeds the driver budget (Medium) | Confirmed: the facade drops block options; 4 MiB blocks give ~1,400 partials of 82 KB against a 64 MiB budget | Expose `blocks`; grow blocks automatically to fit the budget | Yes, for large p only |
| L6 | `variance="robust"` rejected for indirect measures (Low) | Confirmed, by design; pprof_py ignores `variance` for indirect measures | Accept and ignore with a warning | No (an error becomes a warning) |

## 2. The issues

### C1. Baseline survival differs from R's `survfit.coxph`

**Diagnosis.** A probe built data like the pipelines' Stage 2: 3,662 counting-process rows of 1,500
patients, recurrent events at interval ends, 2,505 rows starting exactly at an event time, daily ties, left
truncation, and an offset with weighted mean 0.294. It fitted them with pprof_spark (local Spark, round 81
code) and checked the results against an independent NumPy implementation of R's definitions. Under both tie
methods:

| Quantity | Result |
|---|---|
| Score at pprof_spark's β̂ under R's risk set (aᵢ < t ≤ bᵢ) | 1.2 × 10⁻¹³ (Breslow), 1.5 × 10⁻¹³ (Efron) |
| Baseline hazard increments, 314 event times | Largest relative difference 4.3 × 10⁻¹⁶ and 4.9 × 10⁻¹⁶ |
| Martingale residuals (Efron with R's tied-death rule, as in `agmart2`) | Largest difference 1.3 × 10⁻¹⁵ and 1.6 × 10⁻¹⁵ |

So the estimator is right, including risk sets at shared interval boundaries and Efron ties. What differs
is the reference point. pprof_spark's `baseline()` returns the hazard at x = 0 and offset 0, pprof_py's raw
baseline (X-014, approved in D-23). pprof_py's public `baseline_hazard_` multiplies it by exp of the weighted
mean offset (`models/survival/coxph.py`, lines 335 to 338), as does R's `basehaz(fit, centered = FALSE)`; R's
`survfit.coxph(fit)` without `newdata` describes a subject at the covariate means as well. So

  Λ_R(t) = Λ_pprof_spark(t) · exp(ō + x̄ᵀβ̂), with ō the weighted mean offset and x̄ the mean covariates.

In Stage 2 the offset is the Stage 1 linear predictor, so ō is not zero: ō = 0.058 alone gives the reported
6%. In the probe, ō = 0.294 made the cumulative hazards differ by a factor 1.342.

**What does not differ.** pprof_spark's own predictions pair the raw baseline with the uncentred linear
predictor η = xᵀβ̂ + o, so its expected values and residuals do not depend on the reference point and match
R's (the probe's residuals). Downstream differences arise only when pprof_spark's baseline meets a centred
quantity from R, such as `predict(fit, type = "lp")`, which R centres by default.

**Check on production data.** Multiply pprof_spark's `cumulative_hazard` by exp(ō), with ō the weighted
mean of the offset over all rows, and compare with `basehaz(fit, centered = FALSE)`; also by exp(x̄ᵀβ̂) to
compare with `survfit(fit)`.

**Code.** `CoxPH.baseline` (`engine/.../cox/CoxPH.scala`) and `CoxKernel.baseline`; `CoxPrediction`
(uncentred linear predictor); `python/pprof_spark/cox.py` `baseline`, `linear_predictor`; specification
`docs/spec/cox/counting-process-baseline.md` §2; DISCREPANCIES X-014.

**Proposed fix.** `baseline(df, reference="raw" | "mean_offset" | "means")`: `raw` as now;
`mean_offset` = pprof_py's `baseline_hazard_` and R's `basehaz(centered = FALSE)`; `means` = R's
`survfit.coxph` default and `basehaz(centered = TRUE)`. And `linear_predictor(df, centered=False)`, with
`True` matching R's default. The weighted means come from the block summaries, which already carry column
sums. Fixtures gain R's `basehaz` both ways and `survfit`.

**Risks.** None by default. R computes `fit$means` over rows weighted by case weights; this is to be
confirmed in the fixtures, including for counting-process rows and strata (R uses overall means).

### C2. `row_id` for counting-process data

**Diagnosis, refined.** A row identifier is optional for fitting, baselines and predictions: without one,
validation uses 0 for every row (`engine/.../data/Validation.scala`, `rowId`), and the fit's fingerprint
(`Fingerprint.row` of stratum, row id and values, summed over rows in `CoxKernel`) then depends on content
only, which survives recomputation. Residuals need one, to join back to the input
(`CoxPH.residuals`). Validation rejects duplicates (`DuplicateRowIds`). With
`monotonically_increasing_id()` the identifiers depend on partitioning, so a recomputed DataFrame has
other identifiers, another fingerprint, and the later call refuses it (API-3), as reported. Without an
identifier, rows that tie on every sort key keep input order, so bitwise reproducibility (R0) across
partitionings is not guaranteed (results stay within T-part).

**Workaround today, no materialization:** a deterministic identifier from natural keys, for example
`F.xxhash64("patient_id", "start")`. Validation reports any duplicate; a 64-bit collision among 5 × 10⁶
rows has probability about n²/2⁶⁵ ≈ 7 × 10⁻⁷.

**Proposed fix.** `row_id` accepts a list of columns (`["patient_id", "start"]`): the engine derives the
64-bit identifier with `xxhash64`, keeps the duplicate check, and returns the original key columns in
residual output.

**Risks.** `CoxSpec.rowId` becomes a list (the Python argument accepts both forms); saved models record the
list (`CoxFitIO`); a collision is reported, never silent.

**Status.** The workaround is documented in `docs/guide/python.md` (round 83); composite keys remain (OI-74).

### C3. `max_stratum_rows` for unstratified fits

**Diagnosis.** `CoxOptions.maxStratumRows` defaults to 1,000,000 (`CoxPH.scala`); the guard exists because
a stratum is never split across tasks (no TimeRange layout, OI-03), so an unstratified fit holds every row
in one task. The cap counts rows and ignores p: 4.8 M rows with p = 57 need about 4.8 M × (p + 6) × 8 B ≈ 2.4
GB in that task, while an offset-heavy model with few features needs far less.

**Proposed fix.** A byte budget, `maxStratumBytes` (default 4 GiB, an estimate of rows × 8 × (p + 6)),
replaces the row default; an explicit `max_stratum_rows` still applies; the error states the estimated size
and both options.

**Risks.** A larger default trades a clean error for a possible executor out-of-memory on small workers;
the guide states the executor memory needed. The single-task fit stays slow (one core); TimeRange (OI-03,
OI-30) remains the real fix.

**Status.** Resolved in round 83: `CoxOptions.maxStratumBytes` (4 GiB) with `maxStratumRows` unlimited by default; Python `max_stratum_bytes`; saved models at format version 4, older versions read with the default.

### C4. Slower than pprof_py at 4.8 M rows

**Diagnosis.** Expected below the crossover that PERF-1 asks us to measure. Likely contributors, largest
first: per-row Neumaier-compensated scalar kernels, O(n·p²) per pass, against pprof_py's vectorised NumPy
(OI-38); every downstream call rebuilding the working set from the DataFrame (OI-68); about 560 tasks per pass
at p = 57 with 4 MiB blocks; a driver round trip per pass.

**Proposed fix.** Measure with the scale-test kit (its kernel benchmarks and phase timings), then: reuse a
prepared working set across calls; larger blocks for large p (shared with L5); faster kernels if the
measurements justify them (NUM-2 evidence). Meanwhile the decision guide: when the data fit in driver memory,
pprof_py is the faster tool.

### L1. `add_providers()`

**Diagnosis.** Confirmed. pprof_py's `LogisticFixedEffectModel.add_providers(provider_ids, gamma,
se_gamma=0.01, group_sizes=None)` (`models/logistic/fixed_effect.py`, line 325) reproduces R's PPPW step:
providers excluded from the fit (zero-event, all-event) are appended with fixed γ (−17, +17) and standard
error, or replace existing ones; their direct measures use the same patient-level xβ,
direct rateⱼ = Σᵢ σ(γⱼ + xᵢᵀβ̂)/N, so they enter every mean over providers. pprof_spark's provider table
(`LogisticFE.providerTable`) has fitted providers only.

**Proposed fix.** Engine: `LogisticFit.withProviders(ids, gamma, seGamma, groupSizes)`, returning a new fit
whose providers carry γ, variances se² (model, case-mix and robust), group sizes, and a flag `added`; used by
standardized measures, tests and null values. Python: `add_providers(...)`, returning the new model.

**Risks.** Saved models must keep added providers; provider keys must match the fit's type; tests that need
the provider's rows (exact, score) have none for added providers and return missing values, as is to be
confirmed against pprof_py.

### L2. Null model for `test_standardized()`

**Diagnosis.** Confirmed: `LogisticStandardization.testStandardized` implements pprof_py's test with the
theoretical null only (its documentation says so; so does the Python docstring). pprof_py's
`test_standardized` takes `null_model` (`TheoreticalNull`, `FixedNull`, `EmpiricalNull` in
`inference/empirical_null/models.py`), resolved by `resolve_null_model` (`inference/decision.py`).

**Proposed fix.** Stage one: a null model for standardized tests and provider tests, theoretical (default)
or fixed with a location and scale (z* = (z − location)/scale, p-values and flags from z*), as pprof_py's
`FixedNull`. Stage two: pprof_py's empirical null (location-scale and M-estimators, by provider-size groups)
under its own specification.

**Risks.** None by default. The team's R `FixedNull(sd = 1.81)` should be checked against pprof_py's
definition (location, scale, and how intervals use them) before fixtures are made.

### L3. `interval` and `bounds`

**Diagnosis.** Confirmed: intervals come from inverting the test on the working scale and are always
clipped, rates to [0, 1] and ratios to [0, ∞) (`LogisticStandardization`). pprof_py has
`interval="inversion"` (default) or `"scale_only"`, and `bounds="auto"` (the same clipping), a pair, or
`None`.

**Proposed fix.** Both options with pprof_py's defaults, with L2 (`scale_only` matters only for a
non-theoretical null).

### L4. `reference="mean"`

**Diagnosis, refined.** pprof_spark's null value is the measure at the reference effect g₀, the mean or
median of γ̂ (`LogisticProviderTests.reference`, then `LogisticStandardization.measure`): expit-scale
f(E[γ]) for `reference="mean"`. That is pprof_py's `null_value="reference"` (its default). The pipelines
use pprof_py's `null_value="mean"`, the mean of the providers' measure values, E[f(γ)], which pprof_spark's
wrapper cannot express: its `null_value` takes a number only. For a median the two agree when m is odd and
differ slightly when m is even.

**Proposed fix.** `null_value` = `"reference"` (default), `"mean"`, `"median"` or a number, computed over
all providers including those added by L1, as pprof_py documents.

**Risks.** None by default; fixtures from pprof_py for each mode.

### L5. Block options for the logistic model

**Diagnosis.** Confirmed: `LogisticFixedEffect` has no `blocks` argument, and the facade's accepted fit
options (`PythonApi.logisticFit`) leave `blocks` out, though `LogisticOptions` has them and the Cox facade
parses them. At p = 142 a block's partial is 8 × (p(p+1)/2 + p + 2) = 82,376 bytes, so the 64 MiB driver
budget allows 814 blocks, while 4 MiB blocks hold 3,591 rows: about 1,400 blocks at 5 M rows, hence
`DriverLimitExceededException` (`BlockOptions`, `LogisticFE`).

**Proposed fix.** Parse `blocks` in the facade and add the argument to the wrapper; and when a plan's
partials would exceed the driver budget, grow the block size (up to a ceiling, recorded in the fit) before
failing, for Cox as well.

**Risks.** Automatic growth changes the block layout for large p: results move within T-part (R1), never
beyond; recorded in the fit's layout summary.

**Status.** Resolved in round 83: `blocks` in the facade and in `LogisticFixedEffect`; `BudgetedLayout` grows blocks for the Cox, logistic and linear models, in the fit and in every later call.

### L6. `variance="robust"` for indirect measures

**Diagnosis.** Confirmed, by design: `LogisticStandardization` rejects it with an explanation, because
indirect measures use the variance of the observed count (`indirect_variance`). pprof_py does not use
`variance` for indirect measures; its result records `indirect_variance` instead.

**Proposed fix.** Accept and ignore it for indirect measures with a warning, as pprof_py does; record as a
discrepancy entry.

## 3. Decisions needed and a proposed order

Changes to statistical behaviour need the maintainer's approval as specification addenda: C1 (an addendum to
X-014), L1 to L4 and L6 (the 2c and 2d specifications), C3's default. Proposed order, by what unblocks the
pipelines first:

1. L5 and C3: blocks and the stratum budget, small and mechanical; the C2 workaround documented.
2. C1: the baseline's reference options.
3. L2, L3, L4, L6: null models (fixed), interval and bounds, null values.
4. L1: `add_providers()`.
5. C2's composite keys; L2's empirical null; C4 with the scale-test kit, which follows.
