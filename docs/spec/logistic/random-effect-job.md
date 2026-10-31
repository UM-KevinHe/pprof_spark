# Specification: logistic random effects — persistence, job runner and Python (slice 4b-3)

- Status: approved with D-62 (2026-10-23); persistence and the job runner in round 120, Python in round 121.
- References: slice 4a-3 ([../linear/random-effect-job.md](../linear/random-effect-job.md), D-56), applied to slices
  4b-1 and 4b-2 ([random-effect-estimation.md](random-effect-estimation.md),
  [random-effect-inference.md](random-effect-inference.md)). No new statistics.

## 1. Purpose

Slices 4b-1 and 4b-2 run as a `spark-submit` job from a JSON run specification, a fit is saved and loaded bit for
bit, and Python saves and loads fits. Every output equals the library's result bit for bit. As in slice 4b-2,
`summary`, the Wald test and option `alpha` need only the fit; the count tests, the measures, option `SM` and the
profile need the training data (API-3).

## 2. Persistence (`LogisticREFitIO`, format version 1)

As `LinearREFitIO`, a fit is a directory of two parts, written once (mode `errorifexists`):

| Part | Content |
|---|---|
| `metadata` | One JSON line with a fixed schema. It holds `formatVersion` (1), `kind` (`LogisticREFit`), the column roles and options (blocks included, σ_upper as a bit pattern) and the coefficients with their standard errors. It also holds Var(β̂) packed, σ̂, the criterion, rows, providers, the deviance, ℓ, AIC, BIC, PWRSS, log det H and ‖u‖². Then the search's record (both stages' evaluations, the projected gradient, convergence, the boundary), warnings, software, the layout summary, the fingerprint and the status. Every double is stored as its 64-bit pattern |
| `providers` | The provider table in Parquet (key, `records`, `weight`, `blup`, `variance`, `se`) |

`LogisticREFitIO.load(spark, path)` refuses another `kind` or a format version it cannot read. It checks the provider
table's row count against the metadata and persists the table as `fit` does. A loaded fit gives the same tables,
tests, intervals, measures, profile, predictions and fitted values as the fit that was saved, bit for bit. What reads
rows still needs the training data, checked against the stored fingerprint.

## 3. Run specification

```json
{
  "version": 1,
  "model": "logistic-re",
  "input": {"table": "main.profiling.readmissions"},
  "columns": {"outcome": "readmitted", "features": ["age", "comorbidity"], "provider": "facility",
              "offset": "log_exposure", "rowId": "id"},
  "fit": {"stage2": true, "includeIntercept": true, "sigmaUpper": null, "maxIterations": 200,
          "blocks": {"targetBlockBytes": 4194304}},
  "outputs": {
    "path": "/results/readmissions-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95},
    "providerTests": [
      {"method": "wald", "reference": 0.0, "alternative": "two_sided", "level": 0.95},
      {"method": "poibin_exact", "reference": "median"},
      {"method": "resampling", "nResample": 10000, "seed": 7}
    ],
    "alphaIntervals": {"level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "measureIntervals": {"kinds": ["indirect", "direct"], "measures": ["ratio", "rate"], "reference": "median",
                         "alternative": "two_sided", "level": 0.95},
    "profile": {"level": 0.95},
    "predictions": {"useRe": true, "type": "response"},
    "fitted": true
  }
}
```

| Section | Content |
|---|---|
| `input` | As the other jobs: exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` and `fit` | As `LogisticRERunSpec.model` reads them (spec 4b-1 §15): `stage2` is nAGQ = 1 (true by default), `sigmaUpper` null for none |
| `outputs.path` | A directory that must not exist yet |
| `outputs.fit`, `providers` | The fit (`LogisticREFitIO`) and the provider table |
| `outputs.summary` | `level` |
| `outputs.providerTests` | One test, or a list with distinct `method`s: `method` (`wald` by default, `poibin_exact` or `resampling`), `reference` (0 by default; `median`, `mean` or a number), `alternative`, `level`, `critical`, `nResample` (10,000) and `seed` (0). `exact` and `nullModel` fail, naming part 4d and OI-87 |
| `outputs.alphaIntervals` | `level` (two-sided) |
| `outputs.measures`, `measureIntervals` | `kinds` and `reference`; with `measures` (`ratio`, `rate`), `alternative` and `level` for the intervals |
| `outputs.profile` | `level`; a nAGQ = 0 fit fails here at parse time |
| `outputs.predictions` | `useRe` (false by default) and `type` (`response` or `link`); the fit's offset column included |
| `outputs.fitted` | The input rows with `xbeta`, `eta`, `fitted`, `residual`, `pearson` and `deviance_residual` |

Every problem in a specification is reported at once. A field of another model counts as a problem, for example
`columns.weights`, `columns.trials` or `fit.reml` with `model` `logistic-re`.

## 4. Entry point and outputs

`pprof.spark.app.LogisticREJob` takes `--spec <path>` (read through Spark) or `--spec-json <text>`, as the other jobs.
`LogisticREJob.run(spark, spec, json)` is the library form. The six jobs (`CoxJob`, `LogisticJob`, `ThreeStageJob`,
`LinearJob`, `LinearREJob`, `LogisticREJob`) refuse each other's specifications, naming the right entry point.

| Directory under `outputs.path` | Content |
|---|---|
| `fit` | The fitted model, readable with `LogisticREFitIO.load` |
| `providers` | Parquet: the provider table |
| `summary` | Parquet: feature, estimate, std_error, z_value, p_value, ci_lower, ci_upper |
| `provider_tests/<method>` | Parquet: the provider-test table of slice 4b-2 §3, one directory per method |
| `alpha_intervals` | Parquet: provider, alpha, alpha_lower, alpha_upper |
| `measures/indirect`, `measures/direct` | Parquet: slice 4b-2 §5's tables |
| `measure_intervals/<key>` | Parquet: `indirect_ratio`, `indirect_rate`, `direct_ratio`, `direct_rate`, each with its value, `lower` and `upper` |
| `profile` | Parquet: one row of sigma, lower, upper |
| `predictions`, `fitted` | Parquet: the input rows with their added columns |
| `run` | The run record, written last: the specification as given, the options used, software versions, the fingerprint, counts, the criterion, σ̂, the deviance, ℓ, AIC, BIC, the search's record, warnings, the outputs written and the stages' timings |

## 5. Behaviour

As the linear random-effect job (spec 4a-3 §5):
- an existing `outputs.path` is detected at the first write, after the fit (OI-63), and nothing is overwritten;
- a stage that fails stops the job and leaves no run record;
- what reads rows uses the job's own input, the fit's training data;
- the input is read once per pass, and nothing n-scale reaches the driver.

The profile and the count tests are the slow outputs: each evaluation is a Spark job (OI-99).

## 6. Python access

`LogisticRandomEffectModel` gains `save(path)` and the class method `load(spark, path)`, through the facade's
`logisticRESave` and `logisticRELoad`. The facade already parses with `LogisticRERunSpec.model` (round 113), which
gains `parse` for whole specifications.

## 7. Validation plan

- `LogisticREFitIOSuite`:
  - lgre-offset (nAGQ = 1) and lgre-text (nAGQ = 0) saved and loaded, every field and the provider table equal bit
    for bit;
  - the loaded fit's tests, intervals and measures equal the saved fit's;
  - another kind, an unknown format version and a provider table of the wrong length refused; nothing overwritten.
- `LogisticREJobSuite`:
  - on lgre-offset with every output requested, every table equals the library's bit for bit, and the fit loads
    back;
  - the run record lists every output;
  - a second run to the same path fails;
  - an invalid specification lists all its problems;
  - the six jobs refuse each other's specifications;
  - argument handling.
- `PythonApiLogisticRESuite` and `test_logistic_re.py`: save and load from Python.
- Guides: `docs/guide/logistic-re-job.md`, and the Python guide's section.

## 8. Known discrepancies

None new.

## 9. Implementation notes (round 120; no statistical change)

1. `engine.logistic.LogisticREFitIO`, format version 1, as §2, follows `LinearREFitIO` with the logistic fit's
   fields: `nagq1`, σ_upper's bit pattern, and both stages' evaluations, the projected gradient, convergence and the
   boundary.
2. `app.LogisticRERunSpec.parse` reads whole specifications. `model`, used by the facade since round 113, shares its
   readers and keeps its messages. `providerTests` takes one object or a list with distinct methods; `exact`,
   `nullModel` and a profile of a nAGQ = 0 fit are problems at parse time. `RunSpec`, `LogisticRunSpec`,
   `ThreeStageRunSpec`, `LinearRunSpec` and `LinearRERunSpec` refuse `model` `logistic-re`, naming `LogisticREJob`.
3. `app.LogisticREJob` reads the input once and calls the library for each output. `summaryFrame` and `profileFrame`
   build the two driver-side tables.
4. `LogisticREFitIOSuite` (3 tests) and `LogisticREJobSuite` (4 tests), as §7. The job's outputs are compared with
   the library's results on the fit the job saved, which the persistence tests show is the job's fit bit for bit, so
   the test fits once. The guide is `docs/guide/logistic-re-job.md`.

## 10. Implementation notes (round 121; no statistical change)

- The facade gains `logisticRESave` and `logisticRELoad`, which wrap `LogisticREFitIO`.
- `LogisticRandomEffectModel` gains `save(path)` and `LogisticRandomEffectModel.load(spark, path)`.
- Tests: `PythonApiLogisticRESuite` gains a round trip whose summaries match, on a fit at given parameters. A Python
  test saves the fit, loads it, compares its estimates, BLUPs and Wald tests, and checks that saving does not overwrite.
