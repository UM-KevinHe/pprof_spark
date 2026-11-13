# Running fixed provider effects with random clusters as a Spark job

`pprof.spark.app.LogisticFERandomClusterJob` fits pprof_py's `LogisticFERandomClusterModel` from a JSON run
specification and writes the requested results as Parquet tables, never overwriting. The specification is
`docs/spec/logistic/fe-random-cluster-job.md` (D-67). The statistics are slices 4c-1 and 4c-2
(`docs/spec/logistic/fe-random-cluster.md`, `docs/spec/logistic/fe-random-cluster-he2013.md`). Every feature is
`Experimental` until it is scale-verified (D-28, D-50).

## Running it

```bash
spark-submit --class pprof.spark.app.LogisticFERandomClusterJob pprof-spark-app_2.13-<version>.jar --spec /path/to/run.json
```

`--spec-json '<json>'` takes the specification inline. The job prints the run record when it ends.

## Run specification

```json
{
  "version": 1,
  "model": "fe-random-cluster",
  "input": {"table": "main.profiling.dialysis"},
  "columns": {"outcome": "y_adj", "observed": "died", "features": ["age", "diabetes"], "provider": "facility",
              "cluster": "network", "rowId": "id"},
  "fit": {"estimator": "marginal", "stages": {"stage1": "/fits/stage1", "stage2": "/fits/three-stage"}},
  "outputs": {"path": "/results/dialysis-2026-11", "fit": true, "providers": true, "clusters": true,
              "summary": {"level": 0.95}, "providerTests": [{"method": "exact"}],
              "measures": {"kinds": ["indirect", "direct"]}, "fitted": true}
}
```

- `columns`:
  - `outcome` is the likelihood's outcome, in [0, 1];
  - `observed` is the outcome the tests count, defaulting to `outcome`;
  - `features`, `provider` and `cluster` are required, and `rowId` is optional.
- `fit`: `estimator` (`marginal` or `he2013`), `convergenceCriterion` (for `he2013`), `nNodes`, `maxIter`, `tol`,
  `bound`, `boundMode`, `path` and `blocks`, with exactly one source:
  - `given`: {`beta`, `sigma`, `start`}, the start one value per provider in key order;
  - `stages`: {`stage1`, `stage2`}, the paths of a saved `LogisticFE` fit and a saved three-stage fit.
- `outputs`:
  - `fit`, `providers`, `clusters`;
  - `summary`, which is stage 1's Wald table and needs the `stages` source;
  - `providerTests`: one test or a list with distinct methods (`exact`, `poibin_exact`, `resampling`);
  - `gammaIntervals` (option `gamma`), `measures`, `measureIntervals` (option `SM`), `fitted`.

## Outputs

Each output is its own Parquet directory under `outputs.path`: `fit`, `providers`, `clusters`, `summary`,
`provider_tests/<method>`, `gamma_intervals`, `measures/<kind>`, `measure_intervals/<key>` and `fitted`. The run record,
`run`, comes last, so a run without one did not finish.

A saved fit reloads with `LogisticFERandomClusterFitIO.load` without its data. `LogisticFERandomClusterFitIO.attach(df,
saved)` re-joins the training data and checks its rows and fingerprint, after which the tests and measures work.
