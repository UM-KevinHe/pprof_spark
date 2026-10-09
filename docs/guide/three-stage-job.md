# The three-stage job

`pprof.spark.app.ThreeStageJob` fits the three-stage model (slices 2f-1 to 2f-4) and writes the requested
outputs. It reads a run specification (version 1, `model` `three-stage`); `CoxJob` and `LogisticJob` point
three-stage specifications here, and this job points theirs back.

```
spark-submit --class pprof.spark.app.ThreeStageJob pprof-spark-app_2.13-<version>.jar --spec /path/spec.json
spark-submit --class pprof.spark.app.ThreeStageJob pprof-spark-app_2.13-<version>.jar --spec-json '{...}'
```

On Databricks, run it as a JAR task with the same class and parameters.

## Run specification

```json
{
  "version": 1,
  "model": "three-stage",
  "input": {"path": "/data/readmissions", "format": "parquet"},
  "columns": {"outcome": "Y", "features": ["age", "diabetes"], "provider": "fac", "cluster": "hosp", "rowId": "rid"},
  "fit": {
    "cutoff": 10,
    "stage1": {"tol": 1e-8, "maxIter": 10000, "bound": 10.0, "backtrack": true},
    "stage2": {"pirlsTolerance": 1e-12, "pirlsMaxIterations": 100, "gradientTolerance": 1e-7, "maxIterations": 200},
    "stage3": {"nNodes": 20, "maxIter": 10000, "tol": 1e-5, "bound": 10.0, "boundMode": "relative"}
  },
  "outputs": {
    "path": "/results/srr-2026",
    "fit": true, "providers": true, "clusters": true, "fitted": true,
    "tests": [{"method": "exact"}, {"method": "resampling", "nResample": 10000, "seed": 1}],
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "intervals": [{"option": "gamma"}, {"option": "SM", "kinds": ["indirect"], "measure": ["rate", "ratio"]}],
    "sensitivity": {"level": 0.95}
  }
}
```

`input` takes exactly one of `path` (with `format` and reader `options`) and `table`. Every `fit` field is
optional and defaults as shown (pprof_py's defaults; stage 1 does not screen covariates). Tests take `method`
(`exact`, `poibin_exact`, `resampling`), `reference` (`median`, `mean` or a number), `alternative`, `level`,
`critical`, `nResample` and `seed`; intervals take `option` (`gamma` or `SM`), `kinds`, `measure`,
`alternative`, `level`, `method` (`exact` or `poibin_exact`) and `reference`; sensitivity takes `level` (of
σ_c's profile interval), `method`, `alternative` and `testLevel`. Unknown fields and invalid values are
reported together, before anything runs.

## Outputs

Under `outputs.path`, as Parquet unless stated:

| Output | Content |
|---|---|
| `fit` | The fitted model (`ThreeStageFitIO`, format version 1), without the records |
| `providers` | Provider key, stage 3's `gamma`, `stage2_blup`, `stage3_start`, `held_at_bound` |
| `clusters` | Cluster key, `alpha_mean`, `alpha_var` (stage 3's posterior moments), `stage2_blup` |
| `tests/<i>-<method>` | Slice 2c's provider-test table for request i |
| `measures/indirect`, `measures/direct` | Ratio, rate, observed and expected per provider |
| `intervals/<i>-<table>` | `gamma_ci`, or the measure tables with `ci_ratio_*` or `ci_rate_*` |
| `sensitivity/sigma`, `sensitivity/flags`, `sensitivity/tests/<at>` | σ_c's interval; flags at the lower limit, estimate and upper limit with `stable`; the three test tables |
| `fitted` | The prepared records with stage 3's fitted probability |
| `run` | The run record (text, one JSON line), written last |

The run record holds the specification, the software, stage 1's data fingerprint, counts, stage 2's
parameters and convergence, stage 3's convergence, every output path and the timings. Existing outputs are
never overwritten.

## Reusing a saved fit

`ThreeStageFitIO.load(spark, path)` returns the saved fit without its records; `ThreeStageFitIO.attach(df,
saved)` re-prepares the training data (stage 1 again, deterministic) and refuses data whose stage 1
fingerprint differs from the saved one, so tests, measures, intervals and sensitivity can run again on the
saved stages 2 and 3.
