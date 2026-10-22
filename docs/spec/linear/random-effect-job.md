# Specification: linear random effects — persistence, job runner and Python (slice 4a-3)

- Status: approved with D-56 (2026-10-22); persistence and the job runner in round 106, Python in round 107.
- References: the linear fixed-effect job runner and Python access ([job-and-python.md](job-and-python.md), D-47)
  and `LinearFitIO`, applied to slices 4a-1 and 4a-2 ([random-effect-estimation.md](random-effect-estimation.md),
  [random-effect-inference.md](random-effect-inference.md)). Plan: [../random-effects/plan.md](../random-effects/plan.md).
  No new statistics.

## 1. Purpose

Slices 4a-1 and 4a-2 run as a `spark-submit` job from a JSON run specification, a fit is saved and loaded bit for
bit, and Python saves and loads fits, with results equal to the library's bit for bit. As in slice 4a-2, the fit, its
tables, the provider tests and option `alpha` need only the fit; the measures and option `SM` need the training data
(API-3).

## 2. Persistence (`LinearREFitIO`, format version 1)

As `LinearFitIO`, a fit is a directory with two parts, written once (mode `errorifexists`):

| Part | Content |
|---|---|
| `metadata` | One JSON line with a fixed schema: `formatVersion` (1), `kind` (`LinearREFit`), the column roles and options (blocks included), the coefficients with standard errors, Var(β̂) packed, σ̂, θ̂, σ̂_α, the criterion, rows, providers, the deviance, ℓ, AIC, BIC, PWRSS, log det H, ‖u‖², the search record (evaluations, bracket, \|d′(τ̂)\|, boundary, path), warnings, software, the layout summary, the fingerprint and the status; every double as its 64-bit pattern |
| `providers` | The provider table in Parquet (key, `records`, `weight`, `blup`, `variance`, `se`), whose doubles Parquet keeps exactly |

`LinearREFitIO.load(spark, path)` refuses another `kind` or a format version it cannot read, checks the provider
table's row count against the metadata, and persists the table as `fit` does. A loaded fit gives the same tables,
tests, intervals, predictions and measures as the fit that was saved, bit for bit; the measures still need the
training data, checked against the stored fingerprint.

## 3. Run specification

```json
{
  "version": 1,
  "model": "linear-re",
  "input": {"path": "/data/lengths-of-stay", "format": "parquet"},
  "columns": {"outcome": "length_of_stay", "features": ["age", "comorbidity"], "provider": "facility",
              "offset": "log_exposure", "weights": "w", "rowId": "id"},
  "fit": {"reml": true, "includeIntercept": true, "thetaUpper": null, "maxEvaluations": 200,
          "blocks": {"targetBlockBytes": 4194304}},
  "outputs": {
    "path": "/results/run-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95, "null": 0.0, "alternative": "two_sided"},
    "fixedEffects": true,
    "confInt": {"level": 0.95},
    "providerTests": {"reference": 0.0, "alternative": "two_sided", "level": 0.95, "critical": null},
    "alphaIntervals": {"level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "measureIntervals": {"kinds": ["indirect", "direct"], "reference": "median", "alternative": "two_sided",
                         "level": 0.95},
    "predictions": {"useRe": false},
    "fitted": false
  }
}
```

| Section | Content |
|---|---|
| `input` | As the other jobs: exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | `LinearRESpec`'s roles: `outcome`, `features` (possibly empty) and `provider` required; `offset`, `weights` and `rowId` optional |
| `fit` | `LinearREOptions`: `reml`, `includeIntercept`, `thetaUpper` (null for none), `maxEvaluations` and `blocks`; omitted fields take the library defaults |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `providers` | The fit (`LinearREFitIO`) and the provider table |
| `outputs.summary`, `fixedEffects`, `confInt` | `LinearREInference.summary`'s arguments; the `fixed_effects_table`; `confInt`'s `level` |
| `outputs.providerTests` | `LinearREInference.test`'s arguments: `reference` (`median`, `mean` or a number; 0 by default), `alternative`, `level`, `critical` |
| `outputs.alphaIntervals` | `alphaIntervals`' `level` (two-sided, as pprof_py's option `alpha`) |
| `outputs.measures`, `measureIntervals` | `measures`' `kinds` and `reference`; `smIntervals`' `kinds`, `reference`, `alternative` and `level` |
| `outputs.predictions` | The input rows with `prediction`: `useRe` adds the BLUPs, and the fit's offset column is included |
| `outputs.fitted` | The input rows with `xbeta`, `fitted` and `residual` |

Every problem in a specification is reported at once, and a field of another model is a problem (for example
`fit.varianceOption`, `columns.trials` or `columns.cluster` with `model` `linear-re`).

## 4. Entry point and outputs

`pprof.spark.app.LinearREJob` takes `--spec <path>` (read through Spark) or `--spec-json <text>`, as the other jobs;
`LinearREJob.run(spark, spec)` is the library form used by tests. `CoxJob`, `LogisticJob`, `ThreeStageJob` and
`LinearJob` refuse a `linear-re` specification, naming `LinearREJob`, and `LinearREJob` refuses theirs, naming the
right entry point.

| Directory under `outputs.path` | Content |
|---|---|
| `fit` | The fitted model, readable with `LinearREFitIO.load` |
| `providers` | Parquet: the provider table |
| `summary` | Parquet: feature, estimate, std_error, stat, p_value, ci_lower, ci_upper (slice 4a-2 §2) |
| `fixed_effects` | Parquet: feature, estimate, std_error, t_value, df, p_value |
| `conf_int` | Parquet: feature, lower, upper |
| `provider_tests` | Parquet: provider, `records` and slice 2c's provider-test columns |
| `alpha_intervals` | Parquet: provider, alpha, alpha_lower, alpha_upper |
| `measures/indirect`, `measures/direct` | Parquet: slice 4a-2 §5's tables |
| `measure_intervals/indirect`, `measure_intervals/direct` | Parquet: the same tables with `lower` and `upper` |
| `predictions`, `fitted` | Parquet: the input rows with their added columns |
| `run` | The run record, one JSON line, written last: the specification as given and the options used, software versions, the fingerprint, counts (rows, providers, fixed effects), the criterion, σ̂, θ̂, σ̂_α, the deviance, ℓ, AIC, BIC, the search record, warnings, and the outputs written |

## 5. Behaviour

As the linear fixed-effect job (spec 3c §4): an existing `outputs.path` is detected at the first write, after the fit
(OI-63), and nothing is overwritten; a stage that fails stops the job and leaves no run record; the measures use the
job's own input, the fit's training data; the job uses the engine's Spark Connect-compatible API. The input is read
once, and nothing n-scale reaches the driver.

## 6. Python access (ADR-0009)

`LinearRandomEffectModel` gains `save(path)` and the class method `load(spark, path)`, through the facade's
`linearRESave` and `linearRELoad`. The facade's `linearREModel` and `linearREFit` parse the column roles and options
with `LinearRERunSpec`'s parser, as the fixed-effect facade uses `LinearRunSpec`'s, so the wrapper and the job report
the same problems; the round-99 parser and its detour through the fixed-effect parser for `blocks` go.

## 7. Validation plan

- `LinearREFitIOSuite`: lre-offset and lre-weights saved and loaded, with every field and the provider table equal
  bit for bit, and the loaded fit's tests, intervals and measures equal to the saved fit's; another kind and an
  unknown format version refused; a provider table of the wrong length refused.
- `LinearREJobSuite`: on lre-offset with every output requested, each written table equals the library's result bit
  for bit and the fit loads back bit for bit; the run record holds the specification, counts and every output; a
  second run to the same path fails rather than overwriting; an invalid specification lists all its problems; the
  five jobs refuse each other's specifications, naming the right entry point; argument handling. Classic Spark and
  Spark Connect.
- `PythonApiSuite` and `python/tests/test_linear_re.py`: the facade's parser reports invalid roles at once; save and
  load from Python give the same estimates and provider table.
- Guides: `docs/guide/linear-re-job.md`, and the Python guide's section.

## 8. Known discrepancies

None new: persistence, the job and the wrappers add no statistics.

## 9. Implementation notes (round 106; no statistical change)

1. `engine.linear.LinearREFitIO`, format version 1, as §2: the metadata's fixed schema stores the coefficient names
   (the intercept included), every double as its bit pattern (θ_upper included, so an unbounded search round-trips),
   the search's record with its path, and the layout; `load` restores the provider table persisted and checks its
   length.
2. `app.LinearRERunSpec` parses as `LinearRunSpec` does, every problem at once, with its own field readers; `model`
   (`columns` and `fit` alone) is the form the Python facade will use (round 107). `RunSpec`, `LogisticRunSpec`,
   `ThreeStageRunSpec` and `LinearRunSpec` refuse `model` `linear-re`, naming `LinearREJob`.
3. `app.LinearREJob` reads the input once and calls the library for each requested output; the summary table is
   `LinearJob.summaryTable`'s, and `fixedEffectsFrame` and `confIntFrame` build the two covariate tables. The run
   record writes infinite or undefined doubles (an unbounded θ_upper) as null.
4. `LinearREFitIOSuite` (3 tests) and `LinearREJobSuite` (4 tests), as §7; the guide is `docs/guide/linear-re-job.md`.

## 10. Implementation notes (round 107; no statistical change)

1. `PythonApi.linearREModel` delegates to `LinearRERunSpec.model`, so the facade and the job report the same problems
   in the same words; round 99's parser and its detour through the fixed-effect parser for `blocks` are gone.
   `linearRESave` and `linearRELoad` wrap `LinearREFitIO`.
2. `LinearRandomEffectModel.save(path)` and `LinearRandomEffectModel.load(spark, path)`.
3. `PythonApiLinearRESuite` (2 tests: the parser's problems at once; the facade's fit bit for bit with the library's,
   and a save and load whose summaries match), and a Python test of save, load, overwriting and a parser problem.
