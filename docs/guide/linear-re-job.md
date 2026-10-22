# Running linear random-effect provider profiling as a Spark job

`pprof.spark.app.LinearREJob` fits pprof_py's linear random-intercept model (`LinearRandomEffectModel`) from a JSON
run specification and writes the requested results as Parquet tables, never overwriting. The specification is
`docs/spec/linear/random-effect-job.md` (D-56); the statistics are slices 4a-1 and 4a-2
(`docs/spec/linear/random-effect-estimation.md`, `docs/spec/linear/random-effect-inference.md`). Every feature is
`Experimental` until it is scale-verified (D-28, D-50).

## Running it

```bash
spark-submit --class pprof.spark.app.LinearREJob pprof-spark-app_2.13-<version>.jar --spec /path/to/run.json
spark-submit --class pprof.spark.app.LinearREJob pprof-spark-app_2.13-<version>.jar --spec-json '{"version": 1, ...}'
```

`--spec` is read through Spark, so any file system Spark reads works. The job prints the run record when it ends.

## Run specification

```json
{
  "version": 1,
  "model": "linear-re",
  "input": {"table": "main.profiling.lengths_of_stay"},
  "columns": {"outcome": "length_of_stay", "features": ["age", "comorbidity"], "provider": "facility",
              "weights": "w", "rowId": "id"},
  "fit": {"reml": true},
  "outputs": {
    "path": "/results/run-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95},
    "fixedEffects": true,
    "providerTests": {"reference": 0.0},
    "alphaIntervals": {"level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "predictions": {"useRe": true}
  }
}
```

- `columns`: `outcome`, `features` (possibly empty) and `provider` are required; `offset`, `weights` (positive) and
  `rowId` are optional. Rows with a missing value fail with counts per column rather than being dropped (X-038).
- `fit`: `reml` (true, the default, or false for ML), `includeIntercept` (true), `thetaUpper` (none by default),
  `maxEvaluations` (200, at least 20) and `blocks`, as in the other jobs.
- `outputs`: `fit` and `providers`; `summary`, `fixedEffects` and `confInt` for the covariates; `providerTests`
  (reference 0 by default, or `median`, `mean` or a number), `alphaIntervals`; `measures` and `measureIntervals`,
  which use the job's own input as the training data; `predictions` (`{"useRe": true}` adds the BLUPs) and `fitted`.
  An unknown field, or a field of another model, is reported with every other problem at once.

## Outputs

Each output is its own Parquet directory under `outputs.path`: `fit` (readable with `LinearREFitIO.load`),
`providers`, `summary`, `fixed_effects`, `conf_int`, `provider_tests`, `alpha_intervals`, `measures/<kind>`,
`measure_intervals/<kind>`, `predictions` and `fitted`. The run record, `run`, is written last: the specification
as given, the options used, software versions, the data fingerprint, counts, the criterion, σ̂, θ̂, σ̂_α, the deviance,
ℓ, AIC, BIC, the search's record (its `path` says whether the search summed the providers on the driver or across the
cluster), warnings and the outputs written. A run without a run record did not finish.

The other jobs refuse a `linear-re` specification and name this one; this job refuses theirs the same way.
