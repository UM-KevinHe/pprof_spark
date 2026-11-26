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
| `fit` | Seconds, success or the error, the storage level asked for (`storage_level`, since round 169) and the level used (`storage_level_used`, since round 170), iterations, convergence, log-likelihood, and the block layout |
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
and R, as the fit records them since round 174; at 180 features and 8,000 providers, about 8,000 blocks in 500 groups
from 20M rows up (arithmetic).

Points 7 and 8 ran out of executor memory on round 160's build, with the working set on the JVM heap (OI-118). With
`--storage-level DISK_ONLY` (round 161's build), points 7, 8 and 9 fit, and point 10 failed late (OI-119). Since
round 169 the default storage level is `OFF_HEAP` (D-77): off-heap memory where the cluster enables it, disk where it
is off, never the JVM heap. From that build on, no point needs a storage argument; on earlier builds, add
`--storage-level OFF_HEAP` to the notebook's "Extra arguments" for points of 50M rows or more.

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

## Comparing storage levels (D-77)

D-77, approved on 2026-11-19, rests on this comparison: `OFF_HEAP` is the default from round 169's build. Rerun it to
check the default on another cluster or build. `03_perf`'s widget 5 runs a point once per listed storage level, back
to back on the same data and cluster, for example `OFF_HEAP,DISK_ONLY,MEMORY_AND_DISK`. Where the cluster has no
off-heap memory, the engine runs `OFF_HEAP` as `DISK_ONLY` (round 170), and `fit.storage_level_used` says so. The
production cluster has 6.5 GiB of off-heap memory per executor, as of 2026-11-19, so there `storage_level_used`
should read `OFF_HEAP`: if it reads `DISK_ONLY`, the engine did not see that memory, and the report should go back. Run each line as point `custom`,
with these arguments (no `--check-partitions`, which doubles a run):

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
line, one per fit. Send back all of them. Since round 169 each report's `fit.storage_level` names the level asked
for, the default included, and since round 170 `fit.storage_level_used` names the level the engine used. Before round 167 the last cell printed only the last level's report; the
others are in the run cell's output, and in the point's `reports/` folders.

Two more points, with `OFF_HEAP` alone, against the disk-only results of round 161's build:

| # | Arguments | Against |
|---|---|---|
| S7 | `--model logistic --rows 5e7 --features 180 --providers 8000 --storage-level OFF_HEAP` | Disk-only: 1,167.6 s |
| S8 | `--model logistic --rows 1e8 --features 180 --providers 8000 --storage-level OFF_HEAP` | Disk-only: failed late (OI-119) |

S8 failed in the AUC pass (`logistic.auc`) on round 163's build. Round 173 made that pass leaner with the same result:
rerun S8 on round 173's build or later and send back its report, failed or not.

At 50M rows the cache (about 19 GiB) may fit in the executors' combined off-heap memory, 6.5 GiB each.

## Comparing the logistic Newton methods (D-83, step 5's exit)

Step 5's exit (plan §4) compares the profile-likelihood Newton (`--newton profile`) with SerBIN, the default, on step
2's exit points. It asks for two things:
- the passes and CPU per fit;
- converged estimates within T-coef of SerBIN's.

**The build.** Use round 199's build or later: its reports carry the estimates and standard errors the comparison
needs.

**Holding the cluster** (round 203). The production cluster autoscales, and its size cannot be fixed for these
runs. Set `03_perf`'s widget 7 to its maximum workers, 10, which adds `--hold-executors 10` to every run.
- **What the hold does.** Before each timed fit, PerfJob submits rounds of sleeping tasks until the autoscaler has
  attached that many executors, as statcompute's `hold_at` does. It asks once more if the count has fallen by the fit's
  start (statcompute's D-22). The report's `hold` records the target, the executors reached and before the fit, the
  rounds and the seconds.
- **Its limit.** The autoscaler can still remove workers during a fit, and `fit.recording.executors_min` and
  `executors_max` show it. `compare_newton.py` compares fit times only between fits that ran on one unchanged executor
  count, the same for all, and lists the others.
- **Why it is needed.** In round 201's runs, most of the first fit's time went to scaling from 1 to 10 workers:
  SerBIN, run first, had 3 to 7 executors where the profile method had 8 to 10. So the fit times, task times and GC
  were not comparable.
- **What it does not affect.** CPU per row per pass, the passes, the bytes read and the estimates do not depend on the
  executors (D-76), so they are compared whatever the hold achieved.

**The runs.** Set `03_perf`'s widget 6 to `serbin,profile,profile,serbin`: each method fits the point twice, once first
and once second, back to back on the same data and cluster. Run these points:

| # | Point | Size | Step 2's exit (round 185, SerBIN) |
|---|---|---|---|
| N1 | 2 | 5M rows × 180 features | Pass A 37.2 µs per row |
| N2 | 4 | 20M × 180 | Pass A 36.5 µs per row |
| N3 | 8 | 50M × 180 | Pass A 31.3 µs per row |
| N4 | 10 | 100M × 180 | Pass A 30.4 µs per row; fit 1,195.4 s |

- **R0 for both methods.** Points 2 and 4 refit on another partitioning (`--check-partitions 401`), which doubles
  each fit. R0 held for both methods in round 201. So to rerun them, choose point `custom` with
  `--model logistic --rows 5e6 --features 180 --providers 8000` (N1) or
  `--model logistic --rows 2e7 --features 180 --providers 8000` (N2).
- **What to expect, from the sandbox** (perf-results.md, round 199):
  - about half the passes: SerBIN makes two per iteration, plus one per eight rejected step lengths; the profile method
    one per iteration, plus one at its start;
  - about a quarter less pass CPU;
  - log-likelihoods equal to the last digits;
  - estimates far inside T-coef.
- **Failures are results too.** A profile fit that fails, or does not converge where SerBIN does, is itself a result.
- **Time.** At 100M rows step 2's SerBIN fit took 1,195 s, so N4 takes up to about 80 minutes for its four fits.

**What to send back.** Every report line the notebook prints at the end.

**The comparison.** It is made with
`python3 scripts/compare_newton.py <report lines> --baseline <step 2's report lines>`. For each point it prints the
fits, passes and pass CPU, and checks the profile estimates against SerBIN's (T-coef) and their standard errors
(T-var), with the values of `tolerances.conf`. `--self-test` checks the script's arithmetic.

## Results

`perf-results.md`.
