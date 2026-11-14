# Performance step 0: the baseline of `main`

- Status: prepared in round 150 (2026-11-13); since round 151, each point runs from `databricks/03_perf.py`. The
  maintainer runs it on the standard cluster, and the results go to `perf-results.md`.
- Plan: `docs/spec/performance/plan.md`, step 0 (D-75, approved 2026-11-13).
- Runner: `pprof.spark.bench.perf.PerfJob`, in the bench module. `PerfJobSuite` tests it on small points.

## The cluster

Every point runs on the standard cluster, the same cluster throughout:
- a 32 GB driver and 30 workers of 32 GB;
- Databricks Runtime 18 with a Classic Spark 4.1 session, not serverless;
- no autoscaling.

Each report records the executors, the total cores, the node types, Photon and adaptive execution as the cluster has
them.

## Building and running on Databricks

Nothing needs to be built or run on your computer.

1. Run `databricks/00_build.py` with the commit to measure. Since round 151 it builds four JARs: numerics, engine, app
   and bench.
2. Install the four JARs it prints as cluster libraries, uninstall older `pprof-spark-` JARs, and restart the cluster
   (`docs/guide/databricks.md`, step 3).
3. Run `databricks/03_perf.py` once per point:
   - choose the point, 1 to 13 as in the table below, or `custom` with its arguments;
   - set *Data folder* to an `s3://` folder the cluster can write to.

   The notebook sets `--path` to a subfolder named after the point, where the data are written once and reused. It
   calls `PerfJob.runLine` in the notebook's own session and prints the report line.

With a terminal and sbt, the same runner can be submitted directly; give each point its own `--path`:

```bash
spark-submit --class pprof.spark.bench.perf.PerfJob \
  --jars numerics/target/scala-2.13/pprof-spark-numerics_2.13-<version>.jar,engine/target/scala-2.13/pprof-spark-engine_2.13-<version>.jar \
  bench/target/scala-2.13/pprof-spark-bench_2.13-<version>.jar \
  --model cox --path <storage>/pprof-spark/perf/cox-n5e6-p180 --rows 5e6 --providers 8000 --features 180 \
  --check-partitions 401 --plans true
```

On Databricks, a spark-submit task takes the same arguments as its parameters.

## What a run does

1. **Data.** It generates the point's data under `--path` unless they are already there: Parquet rows in `data/` and
   a manifest line in `manifest/`.
   - Each row is a function of the seed and its identifier, so the data never depend on the cluster.
   - A second run reuses the data. It verifies their row count and checksum against the manifest, unless
     `--verify false`, which saves that pass.
2. **The fit.** It fits the model, timed, with a recorder attached.
3. **Truth.** It checks recovery of the generating coefficients: max |β̂ − β|/SE ≤ 4.5.
4. **R0.** With `--check-partitions k`, it fits again on the input repartitioned into k partitions by `id`. The two
   fits must agree bit for bit on:
   - the estimates, standard errors and log-likelihood;
   - for Cox, the information and covariance; for logistic, the covariance and provider effects;
   - the iteration count.
5. **Report.** It writes one line of JSON under `--report`, by default `<path>/reports/<time>/report`, and prints it.
   With `--plans true` it also writes the physical plan of each call site.

A fit that fails, such as one the driver budget refuses, is reported with its error.

## The report

| Field | Holds |
|---|---|
| `point`, `options` | The point and every argument given |
| `cluster` | Executors, total cores, the driver's heap, node types, Photon, adaptive execution, shuffle partitions |
| `data` | Whether this run generated the data, the seconds that took, the checksum, and the verification |
| `fit` | Seconds, success or the error, iterations, convergence, log-likelihood, and the block layout |
| `fit.recording.sites` | One entry per call site (see below) |
| `fit.recording` | Executor time no execution claims; the driver's heap peak, sampled every 50 ms and by its pools; the executors' heap peak from their heartbeats; the peak of cached blocks |
| `checks` | `truth`, and `partitions` with `--check-partitions` |

Each call site is a SQL execution's description, such as `collect at CoxPH.scala:507`. Its entry holds:
- executions, tasks and single-task stages;
- wall time, executor run time, CPU time and GC time;
- task-result bytes, the bytes sent to the driver, in total and for the largest execution;
- shuffle bytes, spills and peak task memory.

Stages are attributed to executions when they are submitted, so the map stages that adaptive execution runs as jobs of
their own are counted. These numbers are measurements of the run that wrote them.

## The points

The points are the baseline of `main`. Paths are examples. The expectations come from plan §2.2 and are arithmetic.

| # | Model | Rows | Features | Providers | Design | Extra arguments | Expected |
|---|---|---|---|---|---|---|---|
| 1 | cox | 5e6 | 180 | 8000 | simple | `--check-partitions 401 --plans true` | Fits; 434 blocks of 16 MB |
| 2 | logistic | 5e6 | 180 | 8000 | simple | `--check-partitions 401 --plans true` | As point 1 |
| 3 | cox | 2e7 | 180 | 8000 | simple | `--check-partitions 401` | Fits; 434 blocks of 64 MB |
| 4 | logistic | 2e7 | 180 | 8000 | simple | `--check-partitions 401` | As point 3 |
| 5 | cox | 2e7 | 100 | 8000 | simple | | Fits; 973 blocks of 16 MB |
| 6 | logistic | 2e7 | 100 | 8000 | simple | | As point 5 |
| 7 | cox | 5e7 | 180 | 8000 | simple | | Fits; 272 blocks of 256 MB |
| 8 | logistic | 5e7 | 180 | 8000 | simple | | As point 7 |
| 9 | cox | 1e8 | 180 | 8000 | simple | | The guard fails after validation (OI-103) |
| 10 | logistic | 1e8 | 180 | 8000 | simple | | As point 9 |
| 11 | cox | 5e6 | 180 | 8000 | correlated | `--design correlated` | Fits |
| 12 | logistic | 5e6 | 180 | 8000 | correlated | `--design correlated` | Fits |
| 13 | cox | 2e6 | 180 | 8000 | simple | `--strata none` | One stratum in one task |

`03_perf` gives each point its own data folder.
Generating a point costs one write; points 9 and 10 generate 100M rows. `--check-partitions` doubles a point's fit
time, so it is asked for at points 1 to 4 only.

What to send back:
- every report line;
- the plans files of points 1 and 2;
- the driver log of any run that fails other than by the guard.

## Results

`perf-results.md`.
