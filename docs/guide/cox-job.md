# Running Cox provider profiling as a Spark job

`pprof.spark.app.CoxJob` runs a Cox fit and the provider workflow from a JSON run specification
(Phase 1d specification §5). It needs the `numerics`, `engine` and `app` JARs on the cluster;
Spark itself is provided.

```
spark-submit --class pprof.spark.app.CoxJob \
  --jars pprof-spark-numerics_2.13.jar,pprof-spark-engine_2.13.jar \
  pprof-spark-app_2.13.jar --spec /path/to/run.json
```

`--spec` reads the file through Spark, so any file system Spark reaches works; `--spec-json` takes
the specification as text. On Databricks, run it as a JAR task with the main class
`pprof.spark.app.CoxJob` and the same parameters, on a dedicated (single-user) cluster for
Classic Spark or a standard cluster, where Scala runs through Spark Connect.

## Run specification, version 1

```json
{
  "version": 1,
  "input": {"path": "/data/patients", "format": "parquet", "options": {}},
  "columns": {"time": "exit_day", "event": "died", "features": ["age", "diabetes"],
              "strata": "facility", "rowId": "patient_row", "entry": "entry_day",
              "weight": null, "offset": null, "cluster": null},
  "fit": {"ties": "breslow", "maxIterations": 20, "eps": 1e-9, "confidenceLevel": 0.95,
          "robust": false, "blocks": {"targetBlockBytes": 4194304}},
  "outputs": {"path": "/results/run-2026-10",
              "fit": true, "baseline": true, "residuals": true,
              "measures": {"provider": "facility", "kinds": ["indirect", "direct"]},
              "tests": {"provider": "facility", "method": "midp", "level": 0.95}}
}
```

| Field | Meaning |
|---|---|
| `input` | Exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | The roles of `CoxSpec`; `time`, `event` and `features` are required |
| `fit` | `CoxOptions`; omitted fields take the library defaults |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `baseline`, `residuals` | Write the fit (`CoxFitIO`, with the baseline when requested) and the residual table |
| `outputs.measures` | Standardized ratios per provider: `indirect` (SMR or SHR) and `direct` |
| `outputs.tests` | Provider tests: `midp` (default) or `exact`, at `level` |

A specification with problems fails before any work, listing every problem.

## Outputs

| Directory | Content |
|---|---|
| `fit` | The fitted model, readable with `CoxFitIO.load` and `CoxFitIO.loadBaseline` |
| `residuals` | Parquet: row identifier, martingale, score and dfbeta residuals |
| `measures/indirect`, `measures/direct` | Parquet: provider, ratio, observed, expected, and person-time or population |
| `tests` | Parquet: provider and pprof_py's provider-test columns |
| `run` | The run record, one JSON line: the specification, software versions, data fingerprint, convergence, warnings, outputs and per-stage timings |
