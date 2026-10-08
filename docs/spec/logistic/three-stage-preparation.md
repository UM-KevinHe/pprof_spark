# Specification: three-stage model — preparation and stage 1 (Phase 2f-1)

- Status: Draft, awaiting approval (D-39). No 2f-1 code is written before approval (NN-2).
- Builds on: [three-stage-spike.md](three-stage-spike.md) (D-38) and slice 2a
  ([fixed-effect-estimation.md](fixed-effect-estimation.md)).
- Decision references: D-39; X-028.
- Reference: pprof_py v0.7.0 (`9320766`): `data/glmm_prep.py` (`glmm_data_prep`, as R's
  `glmm.data.prep`) and stage 1 of `models/logistic/three_stage.py`; R golden data in pprof_py's
  `tests/data/glmm_prep` (R's `glmm.data.prep`) and `tests/data/three_stage` (stage 1's β).

Probe results are sandbox evidence from round 43.

## 1. Purpose and outputs

The first part of He et al. (2013)'s three-stage model: prepare the records, then estimate β from
fixed effects for provider × cluster cells. Outputs: the prepared records, the stage 1 fit, and the
offset xβ̂ that stages 2 and 3 use.

## 2. Data contract

| Column | Contract |
|---|---|
| Outcome | Integral, 0 or 1 (no binomial trials, as pprof_py) |
| Features | Numeric, finite (slice 2a's rules) |
| Provider, cluster | Integral or string keys, no nulls |
| Row identifier | Optional, as slice 2a |
| Reserved names | `y_adj`, `cell`, `included` and `stage1_offset` are the outputs' names: an input column with one of them fails (pprof_py refuses `stage1_offset`) |

Missing or invalid values fail with counts per column, never values (pprof_py raises on any missing
value and on a non-binary outcome).

## 3. Preparation (pprof_py's `glmm_data_prep`)

| Step | Rule |
|---|---|
| Screening | Providers with more than `cutoff` (default 10) records are kept; the others are excluded and reported with their sizes ("at most `cutoff` records") |
| Adjusted outcome | `y_adj` = y + 0.01/nⱼ for providers without events, y − 0.01/nⱼ for providers with only events, y otherwise (nⱼ the provider's records), so every provider's effect is finite in stages 2 and 3 |
| Cells | Each (cluster, provider) pair present in the kept records; `included` when the cell has more than `cutoff` records |
| Stage 1 | Slice 2a's estimator on the included records, with the cell as the provider, the raw outcome y, no screening (pprof_py's `screen_providers=False`) and slice 2a's defaults |
| Offset | `stage1_offset` = xᵢᵀβ̂ for every kept record |

pprof_py numbers cells 1, 2, … in (cluster, provider) order of pandas' sorted keys; pprof_spark keys
each cell by the pair itself. Numbering does not change the fits: stage 1's cell effects are
per cell, and β changes only by the summation order of its reductions (class D).

## 4. Outputs

`ThreeStage.prepare(df, spec, options)` returns `ThreeStagePreparation`: `data`, the kept records with
`y_adj`, `cell` (the pair as a string `cluster`␟`provider`, ␟ = U+001F), `included` and
`stage1_offset`; `stage1`, the slice 2a `LogisticFit` (its providers are the included cells); counts
of providers, clusters, cells and included cells; and the excluded providers. `ThreeStageSpec` names
the outcome, features, provider, cluster and optional row identifier; `ThreeStageOptions` holds
`cutoff` and stage 1's `LogisticOptions`.

R's `glmm.data.prep` also returns `n.fac.hosp`, the size of every provider × cluster combination as a
dense table; pprof_spark returns only the cells present (X-028).

## 5. Distributed plan

| Step | Work |
|---|---|
| Validation | One aggregation of counts (slice 2a's pattern) |
| Provider sizes and events | One aggregation by provider (m rows), joined back (broadcast below `maxProvidersOnDriver`, a shuffle join above) |
| Cell sizes | One aggregation by (cluster, provider), joined back the same way |
| Stage 1 | Slice 2a on the included records, cells as providers |
| Offset | A column expression with slice 2a's summation order (Σⱼ xⱼβⱼ in feature order) |

Nothing n-scale reaches the driver; results are bitwise invariant to partitioning (R0).

## 6. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| Provider with exactly `cutoff` records | Excluded (strict >) | Same |
| Non-binary outcome; missing values | Raises | Fails with counts |
| No kept providers, or no included cells | Fails later, in stage 1 | Fails with a message naming the step |
| Input column named like an output | Raises for `stage1_offset` | Fails for any of the four |
| Text keys | Sorted by pandas; R's `factor()` can order them differently, renumbering cells but not changing fits | Keyed by the pair, not numbered |

## 7. Validation plan

Fixtures (next round): pprof_py's golden `glmm_prep/raw.csv` (903 records, 23 providers, 8 clusters)
with R's `glmm.data.prep` output, and `three_stage/raw.csv` (2,637 records, 40 providers, 12 clusters)
with R's stage 1 β, copied with their provenance (pprof_py's MIT-licensed synthetic test data); and a
generated case with text keys, providers without events and with only events, and providers at the
cutoff. References: pprof_py's preparation (`y_adj`, cells, `included`, excluded providers) and stage
1 (β, its covariance, the cell effects), and R where available. Tolerance classes: counts, cells and
`included` exact; `y_adj` T-meas; β, cell effects and the offset T-coef; covariance T-var. Negative
controls: `cutoff` 11 instead of 10, one outcome flipped. Tests also check R0 under repartitioning and
Classic Spark and Spark Connect.

Round 43 probes: on the `glmm_prep` data, pprof_py keeps 854 records like R, with `y_adj` within
3.3e-16 of R's, the same cells, the same `included`, and the same dense table (23 × 8); on the
three-stage data, stage 1's β is within 3.9e-16 of R's. A provider with 11 records is kept and one
with 6 is excluded at `cutoff` 10; `y_adj` is 0.000909 and 0.999091 for an 11-record provider without
events and with only events.

## 8. Known discrepancies

| ID | Summary | Class | Proposed decision (D-39) |
|---|---|---|---|
| X-028 | R's `glmm.data.prep` (and pprof_py's `cell_sizes`) returns a dense provider × cluster table of every combination's size; at the envelope that is 10⁷ to 10⁸ entries, nearly all zero | C | Return the cells present only; the dense table is derivable from them |
