# Specification: logistic fixed-effect model — job runner (Phase 2e)

- Status: Approved by the maintainer, 2026-10-08 (D-37).
- Builds on: slices 2a to 2d ([plan.md](plan.md)) and the Cox job runner (Phase 1d, round 22;
  [docs/guide/cox-job.md](../../guide/cox-job.md)).
- Decision references: D-37. No statistical change: the job calls the library and writes its results.

## 1. Purpose

A `spark-submit` (or Databricks JAR task) entry point that fits the logistic fixed-effect model from
a declarative, versioned run specification and writes the requested result tables, the fitted model
and a run record, so that Python and SQL users and scheduled pipelines work through tables and job
parameters (PROJECT_CONTEXT §6.12).

## 2. Run specification

The Cox run specification (version 1) gains a top-level `model`: `cox` (the default, so existing
specifications keep their meaning) or `logistic`. A logistic specification:

```json
{
  "version": 1,
  "model": "logistic",
  "input": {"path": "/data/readmissions", "format": "parquet"},
  "columns": {"outcome": "readmitted", "features": ["age", "comorbidity"], "provider": "facility",
              "trials": null, "rowId": "id", "cluster": "patient"},
  "fit": {"tol": 1e-8, "maxIter": 10000, "blocks": {"targetBlockBytes": 4194304}},
  "outputs": {
    "path": "/results/run-2026-10",
    "fit": true,
    "providers": true,
    "covariateTests": {"methods": ["wald", "lr", "score"], "robust": false},
    "providerTests": {"method": "poibin_exact", "reference": "median", "alternative": "two_sided",
                      "level": 0.95, "nResample": 10000, "seed": 0},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median", "extremeTrials": 0,
                 "method": "binned"},
    "measureTests": [{"measure": "direct_rate", "transform": "auto", "variance": "model"}],
    "predictions": false
  }
}
```

| Section | Content |
|---|---|
| `input` | As Cox: exactly one of `path` (with `format`, default Parquet, and reader `options`) or `table` |
| `columns` | The roles of `LogisticSpec`: `outcome`, `features` and `provider` required; `trials`, `rowId`, `cluster` optional |
| `fit` | `LogisticOptions` (`tol`, `maxIter`, `bound`, `backtrack`, `screen`, `minRecords`, `confidenceLevel`, `correlationThreshold`, `maxProvidersOnDriver`, `blocks`); omitted fields take the library defaults |
| `outputs.path` | A directory that must not exist yet; results are never overwritten |
| `outputs.fit`, `providers` | The fit (`LogisticFitIO`) and the provider table |
| `outputs.covariateTests` | Any of `wald`, `lr`, `score`; `robust` Wald standard errors need `columns.cluster` |
| `outputs.providerTests` | Slice 2c's arguments; defaults as the library's |
| `outputs.measures` | Slice 2d's `measures` arguments |
| `outputs.measureTests` | A list of slice 2d's `test` arguments (`measure`, `nullValue`, `transform`, `reference`, `variance`, `indirectVariance`, `alternative`, `level`, `critical`, `method`) |
| `outputs.predictions` | The input rows with `linear_predictor` and `probability` |

Every problem in a specification is reported at once, as for Cox. A field that does not belong to
the chosen model is a problem (for example `columns.time` with `model` `logistic`).

## 3. Entry point and outputs

`pprof.spark.app.LogisticJob` takes `--spec <path>` (read through Spark) or `--spec-json <text>`, as
`CoxJob`. `CoxJob` refuses a logistic specification and `LogisticJob` a Cox one, each naming the
other entry point. `LogisticJob.run(spark, spec)` is the library form used by tests.

| Directory under `outputs.path` | Content |
|---|---|
| `fit` | The fitted model, readable with `LogisticFitIO.load` |
| `providers` | Parquet: the provider table (2a, with 2b's robust columns when clustered) |
| `covariate_tests` | Parquet: feature, estimate, standard error, statistic, p-value, interval, method; one row per covariate and method |
| `provider_tests` | Parquet: provider and pprof_py's provider-test columns, with observed, expected, trials and records |
| `measures/indirect`, `measures/direct` | Parquet: slice 2d's tables |
| `measure_tests/<index>-<measure>` | Parquet: provider and the provider-test columns, one directory per requested test |
| `predictions` | Parquet: the input rows with the two prediction columns |
| `run` | The run record, one JSON line: the specification as parsed (defaults filled in), software versions, data fingerprint, convergence (converged, iterations, criterion), warnings, excluded and degenerate provider counts, AUC, the outputs written and per-stage timings |

## 4. Behaviour

| Case | Behaviour |
|---|---|
| `outputs.path` exists | Fails before reading any data; nothing is written |
| A stage fails after others wrote | The job fails; directories already written stay, and the run record is not written, so an incomplete run is recognizable |
| Robust Wald tests or robust measure variances without `columns.cluster` | A specification problem, reported with the others |
| Spark Connect | The job uses the engine's Connect-compatible API; tests run under Classic Spark and Spark Connect |

## 5. Distributed plan

Each stage is the library call it names; the input is read once, and the fit's working set is built
by each stage that needs the training data (as the library does). Tables are written by Spark;
nothing n-scale reaches the driver.

## 6. Validation plan

`LogisticJobSuite`: on lfe-clustered with every output requested, each written table equals the
library's result bit for bit and the fit loads back bit for bit; the run record holds the parsed
specification, convergence, counts and every output; a second run with the same path fails rather
than overwriting; an invalid specification lists all its problems; `CoxJob` and `LogisticJob` refuse
each other's specifications; existing Cox specifications without `model` still parse; argument
handling. Classic Spark and Spark Connect (CI's Connect job already runs the `app` tests). A user guide,
`docs/guide/logistic-job.md`, as for Cox.

## 7. Implementation notes (round 41)

1. `LogisticRunSpec.parse`, `LogisticJob` (`main`, `specification`, `run`, `testsTable`); `RunSpec.parse`
   (Cox) now reads `model` and refuses `logistic`. Problems are reported in the Cox parser's format.
2. The run record holds the specification as given plus the fit options used (with defaults), rather
   than a re-serialized specification; output requests' defaults are those of the user guide.
3. An existing `outputs.path` fails at the first write (Spark's `errorifexists`), as `CoxJob` does,
   after the fit rather than before reading data: the Spark API that works under Spark Connect has no
   existence check. Nothing is overwritten either way.
4. User guide: [docs/guide/logistic-job.md](../../guide/logistic-job.md).
