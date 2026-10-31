# Running logistic random-effect provider profiling as a Spark job

`pprof.spark.app.LogisticREJob` fits pprof_py's logistic random-intercept model (`LogisticRandomEffectModel`) from a
JSON run specification and writes the requested results as Parquet tables, never overwriting. The specification is
`docs/spec/logistic/random-effect-job.md` (D-62); the statistics are slices 4b-1 and 4b-2
(`docs/spec/logistic/random-effect-estimation.md`, `docs/spec/logistic/random-effect-inference.md`). Every feature
is `Experimental` until it is scale-verified (D-28, D-50).

## Running it

```bash
spark-submit --class pprof.spark.app.LogisticREJob pprof-spark-app_2.13-<version>.jar --spec /path/to/run.json
spark-submit --class pprof.spark.app.LogisticREJob pprof-spark-app_2.13-<version>.jar --spec-json '{"version": 1, ...}'
```

`--spec` is read through Spark, so any file system Spark reads works. The job prints the run record when it ends.

## Run specification

```json
{
  "version": 1,
  "model": "logistic-re",
  "input": {"table": "main.profiling.readmissions"},
  "columns": {"outcome": "readmitted", "features": ["age", "comorbidity"], "provider": "facility", "rowId": "id"},
  "fit": {"stage2": true},
  "outputs": {
    "path": "/results/readmissions-2026-10",
    "fit": true,
    "providers": true,
    "summary": {"level": 0.95},
    "providerTests": [{"method": "wald"}, {"method": "poibin_exact", "reference": "median"}],
    "measures": {"kinds": ["indirect", "direct"]},
    "measureIntervals": {"kinds": ["indirect", "direct"], "measures": ["ratio", "rate"]},
    "profile": {"level": 0.95},
    "predictions": {"useRe": true}
  }
}
```

- `columns`: `outcome` (in [0, 1]), `features` and `provider` are required; `offset` and `rowId` are optional. Rows
  with a missing value fail with counts per column rather than being dropped.
- `fit`: `stage2` (true, the Laplace fit, as lme4's nAGQ = 1; false for nAGQ = 0), `includeIntercept` (true),
  `sigmaUpper` (none by default), `maxIterations` (200, at least 20) and `blocks`.
- `outputs`:
  - `fit` and `providers`; `summary`;
  - `providerTests`, one test or a list with distinct `method`s (`wald`, `poibin_exact`, `resampling`), each with
    `reference` (0 by default, or `median`, `mean` or a number), `alternative`, `level`, `critical`, `nResample` and
    `seed`;
  - `alphaIntervals`; `measures` and `measureIntervals`, which use the job's input as the training data;
  - `profile`, the Laplace fit only; `predictions` (`useRe`, `type` response or link); `fitted`.

  An unknown field, or a field of another model, is reported with every other problem at once.

## Outputs

Each output is its own Parquet directory under `outputs.path`:
- `fit` (readable with `LogisticREFitIO.load`), `providers`, `summary`;
- `provider_tests/<method>`, `alpha_intervals`;
- `measures/<kind>`, `measure_intervals/<key>` (`indirect_ratio`, `indirect_rate`, `direct_ratio`, `direct_rate`);
- `profile`, `predictions` and `fitted`.

The run record, `run`, is written last. It holds the specification as given, the options used, software versions,
the data fingerprint, counts, the criterion, σ̂, the deviance, ℓ, AIC, BIC, the search's record, warnings, the outputs
written and each stage's time. A run without a run record did not finish.

The profile and the count tests are the slow outputs: every evaluation of the model's criterion is a Spark job (OI-99).
The other jobs refuse a `logistic-re` specification and name this one, and this job refuses theirs the same way.
