# Performance step 0: the baseline of `main`

- Status: prepared in round 150 (2026-11-13); since round 151, each point runs from `databricks/03_perf.py`. The
  maintainer runs it on the standard cluster, and the results go to `perf-results.md`.
- Plan: `docs/spec/performance/plan.md`, step 0 (D-75, approved 2026-11-13).
- Runner: `pprof.spark.bench.perf.PerfJob`, in the bench module. `PerfJobSuite` tests it on small points.

## The cluster

The production cluster as it is (D-76, proposed): Databricks Runtime 17.3 LTS with Photon, autoscaling. The maintainer
cannot change either, and dedicated (single-user) access mode is required.

Each report records:
- the executors, at the start and end of the fit and their range during it;
- the cores the cluster offered over the fit (core-seconds) and the utilization;
- the cache's peak, in memory and on disk;
- the node types, Photon and adaptive execution.

It records no storage path and no master address. Steps are compared on CPU per row, bytes to the driver, shuffle
bytes and memory peaks; wall time is read beside the core-seconds offered.

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

Each entry is a SQL execution's description with the operation tag of its jobs, such as `pprof.cox.evaluate` for
Cox's Newton passes or `pprof.logistic.pass-a` for SerBIN's first pass. Since round 156 every action of both models
runs under such a tag (`Operations`), and the description itself names the tagging helper's line. Each entry holds:
- executions, tasks and single-task stages;
- wall time, executor run time, CPU time and GC time;
- task-result bytes, the bytes sent to the driver, in total and for the largest execution;
- shuffle bytes, spills and peak task memory.

Stages are attributed to executions when they are submitted, so the map stages that adaptive execution runs as jobs of
their own are counted. These numbers are measurements of the run that wrote them.

## The points

The points began as the baseline of `main`. The "Expected" column below comes from plan §2.2, before step 1. Since
round 160, blocks stay at the target size and are reduced in groups. The report's `fit.reduction` gives the blocks, G
and R; at 180 features and 8,000 providers, about 8,000 blocks in 500 groups from 20M rows up (arithmetic).

The experiment for points 7 and 8, which ran out of executor memory on round 160 (OI-118), keeps the working set on
disk only. Add `--storage-level DISK_ONLY` to the notebook's "Extra arguments", and run point 7 and then point 8.
Measured since (round 161's build): with it, points 7, 8 and 9 fit. Point 10 failed late (OI-119). Rerun point 10 on
round 162's build, which reports which operation failed (`failed_executions` and `error` per entry). Use disk-only for
every point of 50M rows or more until D-77 makes it the default.

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

## Comparing storage levels (D-77 on hold)

`03_perf`'s widget 5 runs a point once per listed storage level, back to back on the same data and cluster, for
example `MEMORY_AND_DISK,DISK_ONLY`. If a report's cluster block shows `spark.memory.offHeap.enabled` as true, add
`OFF_HEAP`. The production cluster does: 6.5 GiB of off-heap memory per executor, as of 2026-11-19. Use
`MEMORY_AND_DISK,DISK_ONLY,OFF_HEAP` there. Run each line as point `custom`, with these arguments (no `--check-partitions`, which doubles a run):

| # | Arguments | Why |
|---|---|---|
| S1 | `--model logistic --rows 1e6 --features 180 --providers 8000` | Small: fits in memory comfortably |
| S2 | `--model logistic --rows 5e6 --features 180 --providers 8000` | Point 2's size |
| S3 | `--model logistic --rows 5e6 --features 20 --providers 8000` | Narrow: reading costs most relative to computing |
| S4 | `--model logistic --rows 2e7 --features 180 --providers 8000` | Point 4's size |
| S5 | `--model cox --rows 5e6 --features 180 --providers 8000` | Point 1's size |
| S6 | `--model cox --rows 2e7 --features 180 --providers 8000` | Point 3's size |

For each level, compare:
- the fit's seconds, against its core-seconds offered;
- each pass's wall time;
- `other_ms`, the executor time outside CPU and GC, where reading shows;
- `input_bytes`, the bytes read from the cached working set.

A level that fails at a size is itself a result.

The notebook's last cell shows one row per storage level, then each fit's operations, and ends with every report
line, one per fit. Send back all of them. Before round 167 the last cell printed only the last level's report; the
others are in the run cell's output, and in the point's `reports/` folders.

Two more points, with `OFF_HEAP` alone, against the disk-only results of round 161's build:

| # | Arguments | Against |
|---|---|---|
| S7 | `--model logistic --rows 5e7 --features 180 --providers 8000 --storage-level OFF_HEAP` | Disk-only: 1,167.6 s |
| S8 | `--model logistic --rows 1e8 --features 180 --providers 8000 --storage-level OFF_HEAP` | Disk-only: failed late (OI-119) |

At 50M rows the cache (about 19 GiB) may fit in the executors' combined off-heap memory, 6.5 GiB each.

## Results

`perf-results.md`.
