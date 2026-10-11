# Specification: linear fixed effects — job runner and Python access (Phase 3c)

- Status: approved with D-47 (2026-10-09).
- References: the logistic job runner ([job-runner.md](../logistic/job-runner.md), D-37) and the py4j wrappers
  (ADR-0009, D-31, D-33), applied to slices 3a and 3b ([fixed-effect-estimation.md](fixed-effect-estimation.md),
  [provider-inference.md](provider-inference.md)). Plan: [plan.md](plan.md). No new statistics.

## 1. Purpose

Slices 3a and 3b run as a `spark-submit` job from a JSON run specification, and from Python through py4j, with
results equal to the library's bit for bit. The model's split stays visible: the fit, its summary, the provider tests
and the effect intervals need only the fit; the standardized measures and their intervals need the training data
(API-3), as in slice 3b §5.

## 2. Run specification

```json
{
  "version": 1,
  "model": "linear",
  "input": {"path": "/data/lengths-of-stay", "format": "parquet"},
  "columns": {"outcome": "length_of_stay", "features": ["age", "comorbidity"], "provider": "facility", "rowId": "id"},
  "fit": {"varianceOption": "complete", "blocks": {"targetBlockBytes": 4194304}},
  "outputs": {
    "path": "/results/run-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95, "null": 0.0, "alternative": "two_sided"},
    "providerTests": {"reference": "median", "alternative": "two_sided", "level": 0.95, "critical": null},
    "effectIntervals": {"level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "measureIntervals": {"kinds": ["indirect", "direct"], "reference": "median", "alternative": "two_sided",
                         "level": 0.95},
    "predictions": false
  }
}
```

| Section | Content |
|---|---|
| `input` | As Cox and logistic: exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | `LinearSpec`'s roles: `outcome`, `features` and `provider` required; `rowId` optional |
| `fit` | `LinearOptions`: `varianceOption` (`complete`, the default, or `simplified`) and `blocks`; omitted fields take the library defaults |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `providers` | The fit (`LinearFitIO`) and the provider table |
| `outputs.summary` | `LinearFE.summary`'s arguments: `level`, `null`, `alternative` |
| `outputs.providerTests` | `LinearProviderTests.test`'s arguments: `reference` (`median`, `mean` or a number), `alternative`, `level`, `critical` |
| `outputs.effectIntervals` | `gammaIntervals`' `level` (two-sided, as pprof_py's option `gamma`) |
| `outputs.measures` | `LinearStandardization.measures`' `kinds` and `reference` |
| `outputs.measureIntervals` | `LinearStandardization.intervals`' `kinds`, `reference`, `alternative` and `level` |
| `outputs.predictions` | The input rows with `prediction` |

Every problem in a specification is reported at once. A field that belongs to another model is a problem, for
example `columns.trials`, `columns.cluster` or `fit.tol` with `model` `linear`.

## 3. Entry point and outputs

`pprof.spark.app.LinearJob` takes `--spec <path>` (read through Spark) or `--spec-json <text>`, as the other jobs.
`CoxJob`, `LogisticJob` and `ThreeStageJob` refuse a linear specification, naming `LinearJob`, and `LinearJob` refuses
theirs, naming the right entry point. `LinearJob.run(spark, spec)` is the library form used by tests.

| Directory under `outputs.path` | Content |
|---|---|
| `fit` | The fitted model, readable with `LinearFitIO.load` |
| `providers` | Parquet: the provider table (key, `records`, `gamma`, `variance`, `se`) |
| `summary` | Parquet: feature, estimate, std_error, stat, p_value, ci_lower, ci_upper (slice 3a §4) |
| `provider_tests` | Parquet: provider, `records` and slice 2c's provider-test columns |
| `effect_intervals` | Parquet: provider, gamma, lower, upper |
| `measures/indirect`, `measures/direct` | Parquet: slice 3b §4's tables |
| `measure_intervals/indirect`, `measure_intervals/direct` | Parquet: the same tables with `lower` and `upper` |
| `predictions` | Parquet: the input rows with `prediction` |
| `run` | The run record, one JSON line, written last: the specification as parsed (defaults filled in), software versions, data fingerprint, counts (rows, providers, features, degrees of freedom), σ̂, RSS, R² on the training rows, ℓ, AIC, BIC, the condition estimate, warnings, and the outputs written |

## 4. Behaviour

| Case | Behaviour |
|---|---|
| `outputs.path` exists | Fails before reading any data; nothing is written |
| A stage fails after others wrote | The job fails; directories already written stay, and the run record is not written, so an incomplete run is recognizable |
| Measures and their intervals | Use the job's own input, which is the fit's training data; the fingerprint check passes by construction |
| Spark Connect | The job uses the engine's Connect-compatible API; tests run under Classic Spark and Spark Connect |

The input is read once; each stage is the library call it names, and nothing n-scale reaches the driver.

## 5. Python access (ADR-0009)

| `pprof_spark` (Python) | Engine | pprof_py |
|---|---|---|
| `LinearFixedEffect(outcome, features, provider, *, row_id=None, variance_option="complete")`, then `.fit(df)` | `LinearFE.fit` | `LinearFixedEffectModel(gamma_var_option).fit` |
| `LinearFixedEffectModel` attributes: `coefficients`, `sigma`, `rss`, `r2`, `loglik`, `aic`, `bic`, `degrees_of_freedom`, `n_providers`, `condition`, `warnings`, `summary_json` | The fit's summary | `coefficients_["beta"]`, `sigma_`, `aic_`, `bic_` |
| `.providers()` | The provider table | `coefficients_["gamma"]`, `variances_["gamma"]` |
| `.summary(level=0.95, null=0.0, alternative="two_sided")`, a list of `Test` | `LinearFE.summary` | `summary` |
| `.test(reference="median", alternative="two_sided", level=0.95, critical=None, providers=None)` | `LinearProviderTests.test` | `test` |
| `.confidence_intervals(df=None, option="SM", kinds=("indirect",), reference="median", alternative="two_sided", level=0.95, providers=None)`; `df` required for `SM` only | `gammaIntervals`, `LinearStandardization.intervals` | `calculate_confidence_intervals` |
| `.standardized_measures(df, kinds=("indirect", "direct"), reference="median", providers=None)` | `LinearStandardization.measures` | `calculate_standardized_measures` |
| `.predict(df)`, `.score(df)` | `LinearFE.predict`, `LinearFE.score` | `predict`, `score` (any key type, X-034) |
| `.save(path)`, `LinearFixedEffectModel.load(spark, path)` | `LinearFitIO` | — |

The facade `PythonApi.linear*` takes and returns Java types only, as for the other models: `linearModel` and
`linearFit` (column roles and options in the job runner's `columns` and `fit` objects, through `LinearRunSpec`'s
parser, every problem at once), `linearSummary` (JSON, doubles as `Double.toHexString`), `linearProviders`,
`linearCovariateTests`, `linearTest`, `linearGammaIntervals`, `linearMeasures`, `linearIntervals`, `linearPredict`,
`linearScore`, `linearSave` and `linearLoad`. Spark Connect sessions are refused as for the other wrappers.

## 6. Validation plan

- `LinearJobSuite`: on lin-base with every output requested, each written table equals the library's result bit for
  bit and the fit loads back bit for bit; the run record holds the parsed specification, counts and every output; a
  second run to the same path fails rather than overwriting; an invalid specification lists all its problems; the
  four jobs refuse each other's specifications; argument handling. Classic Spark and Spark Connect.
- `PythonApiSuite`: the linear facade's results equal the engine's bit for bit; invalid roles are reported at once.
- `python/tests/test_linear.py`: on lin-base, the wrapper's β̂, γ̂, σ̂, summary, provider tests, intervals and measures
  against pprof_py's fixtures within their classes; save and load; data other than the training data refused for the
  measures (API-3).
- Guides: `docs/guide/linear-job.md`, and a section in `docs/guide/python.md`.

## 7. Known discrepancies

None new: the job and the wrappers add no statistics.

## 8. Implementation notes (round 72; no statistical change)

1. `LinearRunSpec` parses as `ThreeStageRunSpec` does (every problem at once; a field of another model "does not
   belong to a linear specification"), with `fit.blocks` as `LogisticRunSpec`. `RunSpec`, `LogisticRunSpec` and
   `ThreeStageRunSpec` refuse `model` `linear`, naming `LinearJob`; `LinearRunSpec` refuses the other three models,
   naming their jobs.
2. `LinearJob.run` reads the input once and calls the library for each requested output: `LinearFE.fit`,
   `LinearFitIO.save`, the provider table, `LinearFE.summary` (as a table, `LinearJob.summaryTable`),
   `LinearProviderTests.test` and `gammaIntervals`, `LinearStandardization.measures` and `intervals` on the job's own
   input (the training data), and `LinearFE.predict`. The run record holds the specification as given and, under
   `fitOptions`, the options used with their defaults filled in.
3. An existing `outputs.path` is detected, as by the other three jobs, at the first write (mode `errorifexists`),
   after the fit, not before reading the data as §4 and D-37 say: nothing is overwritten and no run record is
   written, but the fit is computed in vain (OI-63).
4. The Python facade and wrapper (§5) follow in round 73.
