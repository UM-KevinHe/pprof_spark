# Specification: fixed provider effects with random clusters — persistence, job runner and Python (slice 4c-3)

- Status: proposed with D-67 (round 131, 2026-11-05), awaiting the maintainer's approval.
- References: slices 4c-1 and 4c-2 ([fe-random-cluster.md](fe-random-cluster.md),
  [fe-random-cluster-he2013.md](fe-random-cluster-he2013.md)). It follows the three-stage model's persistence, a saved
  fit plus `attach` (spec 2f-4 §5), and slice 4b-3's job ([random-effect-job.md](random-effect-job.md)). No new
  statistics.

## 1. Purpose

The random-cluster model runs as a `spark-submit` job from a JSON run specification. A fit is saved and loaded bit
for bit, and Python saves and loads fits. Every output equals the library's result bit for bit.

## 2. The fit's fingerprint

A fit records how many rows it read and an order-free fingerprint of them. The fingerprint is the wrapping sum of a
64-bit hash of each row's values in the model's columns: provider, cluster, outcome, observed outcome, features and
row identifier, each double by its bit pattern. Re-attaching the training data checks both (API-3). The fingerprint
is computed in the pass that validates the input, so a fit costs no extra pass.

## 3. Persistence (`LogisticFERandomClusterFitIO`, format version 1)

As `ThreeStageFitIO`, a saved fit is everything but the records:

| Part | Content |
|---|---|
| `metadata` | One JSON line with a fixed schema: `formatVersion` (1), `kind` (`LogisticFERandomClusterFit`); the column roles and options, `tol` and `bound` as bit patterns; β and σ as bit patterns; stage 3's scalars (convergence, stall, iterations, the criterion, the log-likelihood, cells, bins); warnings; whether a stage-1 fit was given; software; rows; the fingerprint; the status |
| `providers` | Parquet: key, `gamma`, `held_at_bound` |
| `clusters` | Parquet: key, `alpha_mean`, `alpha_var` |

`LogisticFERandomClusterFitIO.load(spark, path)` returns the saved fit, whose tables and scalars need no data. It
refuses another `kind`, an unknown version, and tables whose lengths differ from the metadata's counts.
`LogisticFERandomClusterFitIO.attach(df, saved)` rebuilds the records and cells from `df` with the saved β, checks the
rows and the fingerprint, and gives a fit whose tests, intervals, measures and fitted values equal the original's
bit for bit. The stage-1 fit is not saved: a loaded fit's `summary` takes it as an argument, as pprof_py's does.

## 4. Run specification

```json
{
  "version": 1,
  "model": "fe-random-cluster",
  "input": {"table": "main.profiling.dialysis"},
  "columns": {"outcome": "y_adj", "observed": "died", "features": ["age", "diabetes"], "provider": "facility",
              "cluster": "network", "rowId": "id"},
  "fit": {"estimator": "marginal", "boundMode": "relative",
          "stages": {"stage1": "/fits/stage1", "stage2": "/fits/three-stage"}},
  "outputs": {
    "path": "/results/dialysis-2026-11",
    "fit": true,
    "providers": true,
    "clusters": true,
    "summary": {"level": 0.95},
    "providerTests": [{"method": "exact"}, {"method": "poibin_exact", "reference": 0.0}],
    "gammaIntervals": {"method": "exact", "level": 0.95},
    "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
    "measureIntervals": {"kinds": ["indirect"], "measures": ["ratio"], "method": "exact"},
    "fitted": true
  }
}
```

| Section | Content |
|---|---|
| `input` | As the other jobs |
| `columns` | As spec 4c-1 §2 |
| `fit` | Spec 4c-1 §3's options and he2013's `convergenceCriterion`, with exactly one source. `given` is {`beta`, `sigma`, `start`}, the start one value per provider in key order. `stages` is {`stage1`, `stage2`}: paths of a saved `LogisticFE` fit (`LogisticFitIO`) and a saved three-stage fit (`ThreeStageFitIO`), whose stage 2 gives σ and the start |
| `outputs.summary` | `level`, `null`, `alternative`; needs the `stages` source |
| `outputs.providerTests` | One test, or a list with distinct `method`s (`exact` by default, `poibin_exact`, `resampling`), each with `reference` (`median` by default), `alternative`, `level`, `critical`, `nResample` and `seed` |
| `outputs.gammaIntervals` | Option `gamma`: `method` (`exact` or `poibin_exact`), `alternative`, `level`, `reference` |
| `outputs.measures`, `measureIntervals` | As spec 4c-1 §5; the intervals take `method`, `alternative` and `level` |
| `outputs.fitted` | The input rows with `fitted` |

Every problem in a specification is reported at once. A field of another model counts as a problem, and so does a
specification with both sources or neither.

## 5. Entry point and outputs

`pprof.spark.app.LogisticFERandomClusterJob` takes `--spec <path>` or `--spec-json <text>`, as the other jobs, and
`LogisticFERandomClusterJob.run(spark, spec, json)` is the library form. The seven jobs refuse each other's
specifications, naming the right entry point.

| Directory under `outputs.path` | Content |
|---|---|
| `fit` | The saved fit (`LogisticFERandomClusterFitIO`) |
| `providers`, `clusters` | Parquet: §3's tables |
| `summary` | Parquet: stage 1's Wald table, as `LogisticREJob`'s summary |
| `provider_tests/<method>` | Parquet: slice 2c's table |
| `gamma_intervals` | Parquet: provider, gamma, gamma_lower, gamma_upper |
| `measures/<kind>`, `measure_intervals/<key>` | Parquet: spec 4c-1 §5's tables |
| `fitted` | Parquet: the input rows with `fitted` |
| `run` | The run record, written last: the specification as given, the options, the source, software, rows, the fingerprint, the estimator, σ, the log-likelihood, iterations, convergence, warnings, the outputs written and each stage's time |

As the other jobs: nothing is overwritten (an existing path is found at the first write, OI-63), a failed stage
leaves no run record, and nothing n-scale reaches the driver beyond what the fit's own paths allow (OI-99, OI-101).

## 6. Python access

`LogisticFERandomClusterModel.save(path)` saves the fit. `LogisticFERandomClusterModel.load(spark, path, df=None)`
loads it: with `df`, the training data, it re-attaches the records and every method works; without it, the tables
and scalars work, and the methods that need records fail and say so.

## 7. Validation plan

- `LogisticFERandomClusterFitIOSuite`:
  - frc-obs (`marginal`) and frc-text (`he2013`, `relative`) saved, loaded and re-attached; every field and table,
    and the tests, intervals and measures, equal the original's bit for bit;
  - the refusals: overwriting, another kind, an unknown version, short tables, and other data or the same data
    with one value changed (the fingerprint).
- `LogisticFERandomClusterJobSuite`:
  - with the given source and every output, each table equals the library's on the saved fit bit for bit;
  - with the stages source, from saved stage-1 and three-stage fits on ts-synthetic's prepared data, the job's
    γ̂ equals the three-stage fit's stage 3;
  - the run record; a second run to the same path; invalid specifications; the seven jobs' refusals; arguments.
- Python: save, then load with and without the data.
- Guides: `docs/guide/fe-random-cluster-job.md` and the Python guide's section.

## 8. Known discrepancies

None new.
