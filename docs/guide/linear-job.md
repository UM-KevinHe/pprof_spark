# Running linear provider profiling as a Spark job

`pprof.spark.app.LinearJob` fits pprof_py's linear fixed-effect model (`LinearFixedEffectModel`) from a JSON run
specification and writes the requested results as Parquet tables, never overwriting. The specification is
`docs/spec/linear/job-and-python.md` (D-47); the statistics are slices 3a and 3b
(`docs/spec/linear/fixed-effect-estimation.md`, `docs/spec/linear/provider-inference.md`). Every feature is
`Experimental` until the package-level scale test (D-28).

## Running it

```bash
spark-submit --class pprof.spark.app.LinearJob pprof-spark-app_2.13-<version>.jar --spec /path/to/run.json
spark-submit --class pprof.spark.app.LinearJob pprof-spark-app_2.13-<version>.jar --spec-json '{"version": 1, ...}'
```

`--spec` is read through Spark, so any file system Spark reads works. The job prints the run record when it ends.

## Run specification

```json
{
  "version": 1,
  "model": "linear",
  "input": {"table": "main.profiling.lengths_of_stay"},
  "columns": {"outcome": "length_of_stay", "features": ["age", "comorbidity"], "provider": "facility", "rowId": "id"},
  "fit": {"varianceOption": "complete"},
  "outputs": {
    "path": "/results/run-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95, "null": 0.0, "alternative": "two_sided"},
    "providerTests": {"reference": "median", "alternative": "two_sided", "level": 0.95},
    "effectIntervals": {"level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "measureIntervals": {"kinds": ["indirect"], "reference": "median", "alternative": "two_sided", "level": 0.95},
    "predictions": false
  }
}
```

| Field | Meaning |
|---|---|
| `input` | Exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | `outcome`, `features` and `provider` are required; `rowId` (unique, for a canonical order) is optional |
| `fit` | `varianceOption`: `complete` (the exact variance of each provider effect, the default) or `simplified` (σ̂²/nⱼ); `blocks` |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `providers`, `predictions` | The fitted model, the provider table, and the input rows with `prediction` |
| `outputs.summary` | t tests and intervals for the covariates: `level`, `null`, `alternative` |
| `outputs.providerTests` | `reference` (`median`, `mean` or a number), `alternative`, `level`, `critical` |
| `outputs.effectIntervals` | Two-sided intervals for the provider effects at `level` |
| `outputs.measures` | `kinds` (`indirect`, `direct`) and `reference` |
| `outputs.measureIntervals` | `kinds`, `reference`, `alternative` and `level` |

Every problem in a specification is reported at once. Fields of other models (`trials`, `cluster`, `tol`) are
problems; a specification for another model is refused, naming its job (`CoxJob`, `LogisticJob`, `ThreeStageJob`).

## Outputs

| Directory | Content |
|---|---|
| `fit` | The fitted model, readable with `LinearFitIO.load` |
| `providers` | Parquet: provider, records, gamma, variance, se |
| `summary` | Parquet: feature, estimate, std_error, stat, p_value, ci_lower, ci_upper |
| `provider_tests` | Parquet: provider, records and pprof_py's provider-test columns |
| `effect_intervals` | Parquet: provider, gamma, lower, upper |
| `measures/indirect`, `measures/direct` | Parquet: the standardized differences with observed and expected values |
| `measure_intervals/indirect`, `measure_intervals/direct` | Parquet: the same with lower and upper limits |
| `predictions` | Parquet: the input rows with `prediction` |
| `run` | The run record, one JSON line, written last: the specification, the fit options used, software versions, data fingerprint, counts, σ̂, RSS, R², ℓ, AIC, BIC, the condition estimate, warnings, outputs and timings |

If a stage fails, the directories already written stay and the run record is not written, so an incomplete run is
recognizable. An existing `outputs.path` is detected at the first write, after the fit (OI-63).
