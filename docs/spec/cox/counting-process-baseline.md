# Specification: counting-process data, baseline hazard and prediction (Phase 1b)

- Status: **Approved on 2026-10-06 (D-23), including X-014.** Round 14 implements §1 (fitting with
  entry times) and generates the fixtures for §2 and §3; round 15 implements the baseline,
  prediction and persistence of the baseline.
- Extends [first-slice.md](first-slice.md) and [efron-weights-offsets.md](efron-weights-offsets.md);
  everything not stated here is unchanged.
- Reference: pprof_py v0.7.0 (`CoxPH.fit(start=, stop=)`, `inference/survival/baseline.py`
  `compute_baseline_hazard`, `_efron_baseline_numba`, `predict_linear`, `predict_partial_hazard`,
  `predict_cumulative_hazard`, `predict_survival_function`), R 4.3.3 survival 3.5-8
  (`coxph(Surv(start, stop, event))`, `basehaz`, `survfit.coxph`).
- Scope (§13, Phase 1b): (start, stop] data and left truncation; per-stratum baseline cumulative
  hazard and survival; prediction of the linear predictor, relative hazard, cumulative hazard and
  survival. Residuals and robust variance are Phase 1c.

## 1. Model and risk sets

Each row has an entry time aᵢ and an exit time bᵢ with aᵢ < bᵢ, and is at risk at t when aᵢ < t ≤ bᵢ.
Right-censored data are the case aᵢ = 0. Entry times may be zero or negative, as pprof_py and R
accept. The likelihood, score and information keep the formulas of the earlier specifications
with this risk set; ties, weights, offsets, strata and X-013 are unchanged.

## 2. Baseline hazard

At β̂, for each stratum and each distinct time t with at least one event of positive weight, with
S₀, d, d_w, m and Aₖ as in the addendum §2, the increment is:

- Breslow fits, or Efron fits with d = 1: dΛ₀(t) = d_w / S₀(t).
- Efron fits with d ≥ 2: dΛ₀(t) = Σ_{k=1}^{d} m / Aₖ (pprof_py's `_efron_baseline_numba`).

Λ₀(t) is the running total of increments in ascending time within the stratum, and the baseline
survival is S₀(t) = exp(−Λ₀(t)), as in pprof_py (R's `survfit.coxph` default). The reference point
is x = 0 with offset 0, pprof_py's raw baseline. pprof_py's public `baseline_hazard_` and R's
`basehaz(fit, centered = FALSE)` equal this baseline times exp of the weighted mean offset; pprof_spark
reports the raw baseline and documents the factor (X-014).

## 3. Prediction

For a row with covariates x, offset o, stratum s and time t:

| Quantity | Definition |
|---|---|
| Linear predictor | η = xᵀβ̂ + o (uncentered), as `predict_linear` |
| Relative hazard | exp(min(η, 700)), as `predict_partial_hazard` (pprof_py's `safe_exp` clips at 700) |
| Cumulative hazard | Λ₀ₛ(t)·exp(min(η, 700)), with Λ₀ₛ a right-continuous step function: 0 before the stratum's first event time and constant after its last |
| Survival | exp(−cumulative hazard) |

pprof_py returns a dense matrix over one stratum's event times; pprof_spark adds columns to a
DataFrame and evaluates each row at its own time and stratum (API freedom, §3.6). Parity tests
evaluate pprof_spark at pprof_py's grid. Rows with a stratum absent from the fit, or with missing or
non-finite covariates, offsets or times, fail with counts, as pprof_py raises for an unknown stratum.

## 4. Data contract additions

| Column | Type | Rule |
|---|---|---|
| entry, optional | numeric | Finite and below the exit time; reported with counts otherwise |
| exit (the time column) | numeric | Finite; above the entry time. Without an entry column it must be positive, as before |

## 5. API

Following API-3, methods that need the training data take it explicitly and check its fingerprint
against the fit's:

- `CoxPH.fit` accepts `CoxSpec.entry`; `CoxFit` is unchanged apart from recording the entry column.
- `CoxPH.baseline(df, fit)`: a DataFrame of (stratum key, time, increment, cumulative hazard,
  survival), computed on the executors and never collected (DIST-1).
- `CoxPrediction.linearPredictor`, `relativeHazard`, `cumulativeHazard` and `survival` add a
  column to a DataFrame; the last two also take the baseline table and a time column.
- `CoxFitIO` stores the baseline table as Parquet beside `metadata` when given one (§6.10),
  format version 2; version 1 directories still load.

## 6. Algorithm and distributed plan

| Item | Plan |
|---|---|
| Blocks | As before, plus each row's entry time and, per stratum, the row order by entry time descending |
| Risk-set sweep | Exit times descending: add rows at their exit time, then remove rows whose entry time is at or above the current event time, before that time's events contribute; removals are Neumaier additions of the negated terms |
| Baseline | One extra pass at β̂: each block emits its strata's rows; the cumulative sum runs ascending within the stratum with a Neumaier sum. m-scale output stays distributed |
| Prediction | Spark expressions for η; an as-of join of each row's time to its stratum's baseline (a union with the baseline rows, a window partitioned by stratum and ordered by time, an event time counting as reached) |
| Cost | Fit unchanged in passes and driver traffic; baseline one pass; prediction one shuffle of the prediction rows and the baseline table |
| Reproducibility | R0 bitwise per layout; R1 within T-part for the fit and T-base for the baseline |

Subtracting entries can lose relative accuracy when the risk set shrinks far below the mass removed
from it. The Neumaier compensation bounds this, and a heavily truncated fixture tests it.

## 7. Edge cases

| Case | pprof_py v0.7.0 (probed) | pprof_spark |
|---|---|---|
| Entry not below exit | Raises | Fails at validation with counts |
| Negative entry | Accepted | Accepted |
| An event whose row is not at risk | Cannot happen once entry < exit | Same |
| Event times with only zero-weight events | (see X-013) | Not an event time; absent from the baseline |
| Prediction for an unknown stratum | Raises | Fails with the count of such rows |
| η above 700 in prediction | Clipped by `safe_exp` | Clipped the same way |

## 8. Validation plan

| Level | Test | Class |
|---|---|---|
| Fixtures | New cases from the generator: left-truncated stratified data with ties, and left truncation with weights and offsets; pprof_py fits (both methods, function-level, iterates), raw and public baselines, and predictions for three covariate profiles; R fits, `basehaz(centered = FALSE)` and `survfit` cumulative hazards | — |
| Function, lockstep, end to end | As in Phase 1a, for the new cases | T-fn, T-iter, T-coef, T-var |
| Baseline | Increments and cumulative hazards against pprof_py's raw baseline and, after the X-014 factor, its public baseline and R's | T-base |
| Prediction | All four quantities on pprof_py's grid and between grid points | T-base |
| Metamorphic | Entry 0 everywhere gives the Phase 1a results bit for bit; splitting (a, b] at c into (a, c] without event and (c, b] leaves the fit unchanged; shifting every time by a constant leaves β̂ unchanged and shifts the baseline; entries below every event time change nothing | bitwise or T-coef |
| Reproducibility | R0, R1 (T-base), R2 | as stated |

## 9. Known discrepancies

| ID | Class | Summary | Decision |
|---|---|---|---|
| X-014 | C | Baseline reference point: pprof_spark reports the hazard at x = 0, offset 0 (pprof_py's raw baseline, used by its predictions); pprof_py's public `baseline_hazard_` and R's `basehaz(centered = FALSE)` include the factor exp(weighted mean offset) | Proposed: report the raw baseline and document the factor |
| — | D | Summation order, compensated removal of entries | Within the tolerance classes by design |
