# Running logistic provider profiling as a Spark job

`pprof.spark.app.LogisticJob` fits the logistic fixed-effect model (docs/spec/logistic/) from a run
specification and writes the requested results as tables. It is the logistic counterpart of
[`CoxJob`](cox-job.md) and shares its run-specification format (version 1, with `model` set to
`logistic`). Specification: [docs/spec/logistic/job-runner.md](../spec/logistic/job-runner.md).

## Running it

```
spark-submit --class pprof.spark.app.LogisticJob \
  --jars pprof-spark-numerics_2.13.jar,pprof-spark-engine_2.13.jar \
  pprof-spark-app_2.13.jar --spec /path/to/run.json
```

`--spec <path>` reads the specification through Spark, so any file system Spark reaches works;
`--spec-json '<json>'` takes it inline. On Databricks, use a JAR task with the same class and
parameters. `CoxJob` refuses a logistic specification and `LogisticJob` a Cox one, naming the other.

## Run specification

```json
{
  "version": 1,
  "model": "logistic",
  "input": {"table": "main.profiling.readmissions"},
  "columns": {"outcome": "readmitted", "features": ["age", "comorbidity"], "provider": "facility",
              "rowId": "id", "cluster": "patient"},
  "fit": {"tol": 1e-8},
  "outputs": {
    "path": "/Volumes/main/profiling/results/2026-10",
    "fit": true,
    "providers": true,
    "covariateTests": {"methods": ["wald", "lr", "score"], "robust": true},
    "providerTests": {"method": "poibin_exact", "reference": "median", "level": 0.95},
    "measures": {"kinds": ["indirect", "direct"]},
    "measureTests": [{"measure": "direct_rate"}, {"measure": "indirect_ratio"}],
    "predictions": false
  }
}
```

| Field | Meaning |
|---|---|
| `input` | Exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | `outcome`, `features` and `provider` are required; `trials` (binomial counts), `rowId` and `cluster` (for robust variances) are optional |
| `fit` | `tol`, `maxIter`, `bound`, `backtrack`, `screen`, `minRecords`, `confidenceLevel`, `correlationThreshold`, `maxProvidersOnDriver` and `blocks`; omitted fields take the library defaults |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `providers`, `predictions` | The fitted model, the provider table, and the input rows with `linear_predictor` and `probability` |
| `outputs.covariateTests` | `methods` among `wald`, `lr`, `score` (default all three); `robust` Wald standard errors need `columns.cluster` |
| `outputs.providerTests` | `method` (`poibin_exact`, `score`, `wald`, `bootstrap_exact`), `reference` (`median`, `mean` or a number), `alternative`, `level`, `critical`, `nResample`, `seed` |
| `outputs.measures` | `kinds` (`indirect`, `direct`), `reference`, `extremeTrials` (R's extreme observations), `method` (`binned` or `exact` direct sums) |
| `outputs.measureTests` | A list of tests on a measure: `measure` (`direct_rate`, `direct_ratio`, `indirect_ratio`, `indirect_rate`, `gamma`), `nullValue`, `transform`, `reference`, `variance` (`model`, `robust`, `robust_fixed_beta`), `indirectVariance`, `alternative`, `level`, `critical`, `method` |

Every problem in a specification is reported at once, including fields that do not belong to a
logistic specification.

## Outputs

| Directory | Content |
|---|---|
| `fit` | The fitted model, readable with `LogisticFitIO.load` |
| `providers` | Parquet: provider, effect, variances (robust ones when clustered), counts, flags |
| `covariate_tests` | Parquet: feature, estimate, standard error, statistic, p-value, interval, method |
| `provider_tests` | Parquet: provider and pprof_py's provider-test columns, with observed, expected, trials, records |
| `measures/indirect`, `measures/direct` | Parquet: the standardized ratios and rates (rates in percent) |
| `measure_tests/<index>-<measure>` | Parquet: provider and the provider-test columns, one directory per requested test |
| `predictions` | Parquet: the input rows with the two prediction columns |
| `run` | The run record, one JSON line, written last: the specification, the fit options used, software versions, data fingerprint, convergence, counts (rows, trials, events, providers, excluded, degenerate, clusters), AUC, warnings, outputs and per-stage timings |

A run that fails part-way leaves the directories it wrote and no `run` record.
