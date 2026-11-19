# Performance phase: results

Two kinds of numbers are kept here, in separate tables:
- **Cluster measurements** are runs of `PerfJob` (`perf-runbook.md`) on the standard cluster, entered from their
  report lines.
- **Sandbox measurements** come from the assistant's one-core sandbox, and say nothing about wall time on a cluster.

Estimates and arithmetic are never entered here; they live in `docs/spec/performance/plan.md`.

## Cluster measurements

The scale runs reported on 2026-11-13 used round 81's build (OI-103), so they are not entered.

### 2026-11-16: points 1 and 2 on a small test cluster (build `d0b9bd5`, round 151)

This was not the standard cluster:
- Databricks Runtime 17.3 with Photon (Spark 4.0.0);
- autoscaling, with 6 executors at the start of point 1 and 9 at point 2, each with 4 vCPUs on a 16 GB worker and a
  2.2 GB JVM heap, Photon holding the rest;
- a 32 GB driver with a 21 GB heap.

Both points: 5M rows, 180 features, 8,000 providers, simple design, data reused and verified.

| Point | Fit s | Iterations | SQL executions | Executor run s | CPU s | GC s | Tasks | To the driver: total, largest execution | Shuffle written | Driver heap peak (sampled) | Cached peak | Max \|z\| | R0 refit, 401 partitions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1, Cox | 202 | 3 | 8 | 3,857 | 3,071 | 476 | 1,539 | 229.7 MiB, 56.5 MiB | 2.00 GiB | 6.33 GiB | 6.04 GiB | 2.86 | Identical (200 s) |
| 2, logistic | 219 | 5 | 17 | 5,007 | 3,930 | 500 | 4,187 | 412.4 MiB, 57.1 MiB | 4.15 GiB | 8.31 GiB | 7.57 GiB | 2.87 | Identical (274 s) |

What these runs show, and what they could not:
- **Attribution failed.** Databricks describes every job by the running cell's code, so each run's executions fell
  under a single call site. Round 152 sets those descriptions aside during a run (OI-110); per-call-site figures need
  a rerun.
- **The largest execution matches the arithmetic.** 56.5 MiB for 439 blocks, against plan §2.2's 55 MiB for 434.
  Both models refit bit for bit on 401 partitions.
- **The executor heap figure is discarded.** It included the driver's heartbeat; round 152 excludes it.
- **Most of the CPU lies outside the kernels, by arithmetic.** At the study's rates, the kernels would take about
  1,020 CPU-seconds for Cox (4 evaluations at 51 µs a row) and about 1,130 for logistic, roughly a third of the CPU
  measured. CPU seconds on these vCPUs (hyperthreads) do not compare directly with the sandbox's, so this is an
  indication to confirm by attribution.

### 2026-11-19: S1 to S8 with off-heap storage (build `ada822d`, round 163)

All eight ran with `OFF_HEAP` alone, so the paired comparison with the other levels is still to run. The heaviest
pass is pass A for the logistic model and evaluation for Cox. "Outside CPU and GC" is `other_ms` over that pass's
executor run time.

| Run | Point | Fit s | Iterations | Executors, utilization | Blocks, G, R | Cache (all off-heap) | Heaviest pass: CPU µs per row | Outside CPU and GC | Max \|z\| |
|---|---|---|---|---|---|---|---|---|---|
| S1 | Logistic, 1M rows, 180 features | 151.7 | 5 | 2 to 6, 0.58 | 352, 1, 352 | 0.37 GiB | 88.4 | 8.9% | 2.91 |
| S2 | Logistic, 5M, 180 | 209.8 | 5 | 6 to 8, 0.66 | 1,756, 4, 439 | 1.85 GiB | 86.7 | 9.1% | 2.87 |
| S3 | Logistic, 5M, 20 | 18.7 | 4 | 8, 0.36 | 229, 1, 229 | 0.23 GiB | 2.6 | 14.2% | 1.67 |
| S4 | Logistic, 20M, 180 | 393.2 | 4 | 10, 0.90 | 7,021, 14, 502 | 7.40 GiB | 85.9 | 7.8% | 3.15 |
| S5 | Cox, 5M, 180 | 146.2 | 3 | 6 to 9, 0.65 | 1,756, 4, 439 | 1.88 GiB | 127.6 | 9.8% | 2.86 |
| S6 | Cox, 20M, 180 | 314.4 | 3 | 10, 0.89 | 7,021, 14, 502 | 7.50 GiB | 112.5 | 7.8% | 2.38 |
| S7 | Logistic, 50M, 180 | 1,218.9 | 4 | 6 to 10, 0.81 | 8,000, 16, 500 | 18.51 GiB | 83.3 | 11.1% | 2.90 |
| S8 | Logistic, 100M, 180 | Failed after 5,690.5 | — | 0 to 10, 0.76 | — | — | — | — | — |

**S8.** The recorder names the failing operation: `logistic.auc`, an executor lost to out-of-memory (exit code 52).
The cluster fell to no executors at one point, and every pass read tens of GiB of shuffle, recomputing lost
partitions (OI-119).

**Against earlier runs with the other levels.** These comparisons are not paired; the cores differed.

| Point | Off-heap | Earlier run | Notes |
|---|---|---|---|
| Cox, 20M, 180 (S6) | Fit 314.4 s; evaluation 9,002.7 CPU-s; 7.8% outside CPU and GC; GC 177.8 s | Round 160, `MEMORY_AND_DISK`: 1,474.8 s; 8,726.3 CPU-s; 10.5%; GC 561.4 s | 10 executors throughout against 1 to 9 |
| Logistic, 50M, 180 (S7) | Fit 1,218.9 s; pass A 16,666.1 CPU-s; 11.1% | Round 161, `DISK_ONLY`: 1,167.6 s; 16,721.7 CPU-s; 13.2% | Both with 6 to 10 executors |

Readings:
- **Off-heap held every working set up to 50M rows wholly in memory,** S1 and S3 included. On this cluster, an
  off-heap default would keep small data in memory: the case D-77's question was about.
- **Its time outside CPU and GC was lowest of the levels measured so far:** 7.8% to 11.1% on wide points, and 14.2% on
  the narrow S3, where reading weighs most against computing. The CPU per row matched the other levels.
- **What is still unmeasured is a paired run at small sizes.** S1 and S3 with
  `MEMORY_AND_DISK,DISK_ONLY,OFF_HEAP` in widget 5 take minutes.
- **Logistic at 100M rows fails in the AUC pass under any level** (OI-119).

### 2026-11-19: point 5 with off-heap storage (build `ada822d`, round 163)

Cox, 20M rows, 100 features, `--storage-level OFF_HEAP`.

| Run | Storage | Executors | Utilization | Fit s | Evaluation CPU s | Evaluation outside CPU and GC | Cache peak | Executor heap peak |
|---|---|---|---|---|---|---|---|---|
| Round 160 (`7ad4839`) | `MEMORY_AND_DISK` | 4 | 0.88 | 305.4 | 3,194.9 | Not recorded | — | 2.16 GB |
| Round 163 (`ada822d`) | `MEMORY_AND_DISK` | 1 to 10 | 0.39 | 994.3 | 3,296.2 | 12.9% | 0.98 GiB memory, 4.02 GiB disk | 2.14 GB |
| Round 163 (`ada822d`) | `OFF_HEAP` | 5 | 0.82 | 246.6 | 3,106.9 | 8.9% | 4.27 GiB off-heap memory, none on disk | 1.35 GB |

What the off-heap run showed:
- **It read the cache from off-heap memory.** It read 17.09 GiB over four evaluations, 4.27 GiB a pass, with 25.6 s of
  GC in evaluation against 45.8 s.
- **The results are bit-identical across storage levels:** log-likelihood −30,483,874.174662925 and max |z|
  2.4992479 in every run.
- **The runs are not paired.** The cluster's executors differed between them, so the fit times compare with care. The
  measures that do not depend on the cores (CPU, time outside CPU and GC, GC, heap peak) all favour off-heap here.

In the sandbox (round 165), with the same fit (logistic, 1M rows, 20 features) under `OFF_HEAP`:
- **With off-heap memory off,** as in Spark's default, the cache went wholly to disk (178 MiB) and the fit succeeded.
- **With off-heap memory on (1 GiB),** the cache stayed wholly in off-heap memory.
- **Both runs gave the same log-likelihood bits.**

So `OFF_HEAP` falls back to disk-only where a cluster has no off-heap memory.

### 2026-11-19: point 5 with the default storage level (build `ada822d`, round 163)

Cox, 20M rows, 100 features, `MEMORY_AND_DISK`.
- Fit 994.3 s; 3 iterations; executors 1 to 10; 11,680 core-seconds offered, utilization 0.39.
- 3,968 blocks, G = 3, R = 1,323; max |z| 2.50.

Measured:
- **Evaluation.** 4 executions, 769.0 s wall, 3,296.2 CPU-s (41.2 µs per row), GC 45.8 s. It read 19.96 GiB from the
  cached working set, 4.99 GiB per pass, and spent 493.1 s of executor time outside CPU and GC, 12.9% of its run time.
- **The cache peaked at 0.98 GiB in memory and 4.02 GiB on disk.** The cluster started this run with one executor, so
  four fifths of a "memory" working set were on disk anyway.
- **Against round 160's run of this point.** That run took 305.4 s, with four executors throughout and 3,194.9 CPU-s
  in evaluation. The CPU is the same within 3%; the wall time differs with the cores available (utilization 0.39
  here, 0.88 then).
- **Off-heap memory is enabled on this cluster,** at 6.50 GiB per executor (Photon's), so off-heap storage joins the
  comparison (OI-120).
- **An artifact.** Every operation showed `failed_executions` equal to its executions, with an empty error, although
  the fit succeeded. Databricks ends successful SQL executions with an empty message where Spark gives none. Round
  164 counts only non-blank messages.

### 2026-11-18: disk-only working sets at 50M and 100M rows (build `82fd37a`, round 161)

The same cluster; points 7 to 10 with `--storage-level DISK_ONLY`; 180 features and 8,000 providers. Every point has
8,000 blocks, one provider each, G = 16 and R = 500.

**Point 7, Cox, 50M rows: fits.** Fit 1,243.6 s; 3 iterations; executors 1 to 10; 31,458 core-seconds offered,
utilization 0.87; max |z| 2.86; 18.75 GiB cached on disk.

| Operation | Executions | Wall s | CPU s | CPU µs per row | GC s | Largest to the driver | Shuffle |
|---|---|---|---|---|---|---|---|
| `cox.evaluate` | 4 | 783.7 | 20,715.9 | 103.6 | 728.3 | 65.4 MiB | None |
| `cox.build` | 1 | 343.6 | 1,739.1 | 34.8 | 362.4 | 5.4 MiB | 18.28 GiB written and read once |
| `cox.validate`: counts, then distinct identifiers | 1 + 1 | 74.7 + 7.0 | 224.9 + 19.3 | 4.9 | 0.2 | 353 KiB | 218 MiB |
| `cox.summarize` | 1 | 30.3 | 309.1 | 6.2 | 118.9 | 2.2 MiB | None |

Evaluation takes 90.0% of the CPU, and GC 4.4% of executor run time.

**Point 8, logistic, 50M rows: fits.** Fit 1,167.6 s; 4 iterations; executors 6 to 10; utilization 0.84; max |z|
2.90; 18.51 GiB cached on disk.

| Operation | Executions | Wall s | CPU s | CPU µs per row | GC s | Largest to the driver |
|---|---|---|---|---|---|---|
| `logistic.pass-a` | 4 | 547.8 | 16,721.7 | 83.6 | 694.0 | 66.4 MiB |
| `logistic.variance` | 1 | 143.2 | 4,165.7 | 83.3 | 167.7 | 65.7 MiB |
| `logistic.summarize` | 1 | 140.0 | 3,920.5 | 78.4 | 155.0 | 65.8 MiB |
| `logistic.build` | 1 | 120.2 | 1,764.9 | 35.3 | 338.2 | 5.5 MiB; 18.16 GiB of shuffle |
| `logistic.pass-b` | 4 | 118.9 | 2,257.7 | 11.3 | 562.7 | 3.5 MiB |
| `logistic.auc` | 1 | 53.5 | 903.2 | 18.1 | 337.3 | 7.9 MiB; 446 MiB of shuffle |
| `logistic.provider-variance` | 1 | 22.8 | 413.8 | 8.3 | 130.5 | 3.0 MiB |
| `logistic.validate` | 1 + 1 | 15.4 + 1.9 | 240.6 + 15.8 | 5.1 | 0.4 | 365 KiB |

Passes A and B take 62.4% of the CPU, variance and summaries 26.6%.

**Point 9, Cox, 100M rows: fits.**
- Fit 1,677.5 s; 3 iterations; executors 6 to 10; utilization 0.90; max |z| 3.00.
- Blocks of 12,500 rows.
- Evaluation: 4 executions, 1,372.5 s wall, 40,687.7 CPU-s (101.7 µs per row), GC 4,159.9 s, largest to the driver
  65.8 MiB. It also read 34.44 GiB of shuffle, recomputing partitions lost with executors.
- Build: 219.5 s wall, 3,749.3 CPU-s, GC 2,900.1 s, 36.56 GiB of shuffle.
- Evaluation takes 89.2% of the CPU.

**Point 10, logistic, 100M rows: fails late** (OI-119). After 2,858.8 s an executor was lost to out-of-memory. By
then all of these had run:

| Operation | Wall s | Notes |
|---|---|---|
| Pass A, 4 executions | 1,428.8 | 82.9 µs per row; 30.01 GiB of shuffle read |
| Variance | 410.5 | 24.77 GiB read |
| Pass B, 4 executions | 300.5 | 22.74 GiB read |
| Summaries | 254.6 | |
| Build | 199.1 | GC 2,236 s |
| AUC | 180.8 | GC 1,226 s against 1,101 s of CPU; 6.54 GiB read |
| Provider variances | 42.5 | |

Executors fell to 3 during the run, and the passes recomputed lost partitions from the shuffle. The report cannot say
which job failed; round 162's recorder names the failing operation.

Readings:
- **Disk-only storage carries both models at 50M rows and Cox at 100M (OI-118).** Cox's build GC fell from 3,683 s
  to 362 s at 50M, and the 50M passes read no shuffle. D-77 proposes disk-only as the default.
- **Step 1's exit, as far as measured.**
  - The bytes to the driver per pass stay flat, at 65.4 to 66.4 MiB from 20M to 100M rows (500 to 502 groups).
  - Cox fits at 100M rows; R0 held at 20M; GC is 4.4% at 50M.
  - Logistic at 100M remains (OI-119).
- **Per-row cost is no worse on disk.** Cox evaluation costs 103.6 µs per row at 50M and 101.7 at 100M on disk,
  against 109.1 at 20M in memory.
- **The kernel passes still carry the CPU:** about 90% of Cox's, and 89% of the logistic fit's across passes A and B,
  variance and summaries. That is step 2's subject.

### 2026-11-18: step 1 at 20M and 50M rows (build `7ad4839`, round 160)

This was the same cluster as below: Runtime 17.3 with Photon, autoscaling, 2.2 GB executor heaps, 8,000 providers.
G and R follow the engine's rule (step1-reduction.md §2).

**Point 3, Cox, 20M rows, 180 features: fits (it ran out of memory on round 156).**
- Fit 1,474.8 s; 3 iterations; executors 1 to 9; 19,301 core-seconds offered, utilization 0.63.
- 7,021 blocks (2,500 to 5,000 rows each), G = 14, R = 502.
- Max |z| 2.38; refit on 401 partitions identical.

| Operation | Executions | Wall s | CPU s | CPU µs per row | GC s | Largest to the driver | Shuffle written |
|---|---|---|---|---|---|---|---|
| `cox.evaluate` | 4 | 1,011.0 | 8,726.3 | 109.1 | 561.4 | 65.6 MiB | 0 |
| `cox.build` | 1 | 360.0 | 744.7 | 37.2 | 382.9 | 5.0 MiB | 7.31 GiB |
| `cox.summarize` | 1 | 66.7 | 107.3 | 5.4 | 106.9 | 2.2 MiB | 0 |
| `cox.validate`: counts, then distinct identifiers | 1 + 1 | 29.2 + 3.3 | 90.1 + 8.0 | 4.9 | 0.1 | 146 KiB | 87 MiB, distinct identifiers only |

Evaluation takes 90.2% of the CPU.

**Point 5, Cox, 20M rows, 100 features: fits.**
- Fit 305.4 s; 4 executors; utilization 0.88.
- 3,968 blocks, G = 3, R = 1,323.
- Evaluation: 3,194.9 CPU-s, 39.9 µs per row, 85.9% of the CPU; largest to the driver 57.4 MiB.
- Build 48.3 s; validation 4.9 s, with no spill; max |z| 2.50.

**Point 6, logistic, 20M rows, 100 features: fits.**
- Fit 242.2 s; 4 iterations; 8 executors; utilization 0.71.
- 3,968 blocks, G = 3, R = 1,323.
- Pass A: 2,302.0 CPU-s, 28.8 µs per row per pass. Pass B: 463.2 CPU-s, 5.8 µs.
- Variance 29.0 µs per row; summaries 26.5; AUC 12.1; validation 2.9, with no spill.
- Passes A and B take 59.8% of the CPU, variance and summaries 24.0%. Max |z| 2.37.

**Points 7 and 8, Cox and logistic at 50M rows and 180 features: both fail.**

| | Cox (point 7) | Logistic (point 8) |
|---|---|---|
| Failed after | 370.7 s, an executor lost to out-of-memory | 1,278.0 s, `OutOfMemoryError: Java heap space` in pass A while a block was being decoded |
| Build: wall, CPU, GC | 282.8 s, 2,337 s, 3,683 s | 583.2 s, 2,697 s, 4,039 s |
| Build: shuffle written, read | 18.28 GiB, 24.90 GiB | 18.16 GiB, 33.60 GiB |
| Summaries | 4.39 GiB of shuffle read | 16.75 GiB of shuffle read; GC 2,108 s |
| Pass A | — | 13.89 GiB of shuffle read |

The passes read the shuffle again: cached partitions lost with their executors were recomputed.

Readings:
- **Step 1 works at 20M.** Both models fit at 20M rows with blocks at the target size. The largest pass sent at most
  65.6 MiB to the driver, and Cox evaluation took 109.1 µs of CPU per row at 180 features (140.6 at 5M on round 156).
- **Validation's fix (OI-109) is confirmed.** At 20M rows, Cox validation took 98 CPU-seconds and spilled nothing,
  against 1,045 CPU-seconds and 527 MiB spilled on round 156.
- **50M fails on executor heap (OI-118).** At 50M rows each provider has 6,250 rows. Each block therefore holds one
  provider, with a feature array of about 9 MB, more than twice the target, and building, caching, decoding and (for
  the logistic model) joining each make copies. In a 2.2 GB heap, arrays of that size are large objects for the
  garbage collector. GC exceeded CPU in both builds, executors died, and the passes recomputed lost partitions.

  This reading is a hypothesis the next runs test: the same points with the working set cached on disk only
  (`--storage-level DISK_ONLY`, runbook).

### 2026-11-17: points 1 to 4 by operation (build `7a14624`, round 156)

The production cluster as it is (D-76):
- Runtime 17.3 with Photon (Spark 4.0.0);
- autoscaling between 1 and 10 executors, each with 4 vCPUs on a 16 GB worker and a 2.2 GB JVM heap;
- a 32 GB driver.

All four points: 180 features, 8,000 providers, simple design. "CPU µs per row" is CPU per row per execution.

**Point 1, Cox, 5M rows.** Fit 779 s; 3 iterations; 7,154 core-seconds offered, utilization 0.61, executors 1 to 8;
max |z| 2.86; refit on 401 partitions identical.

| Operation | Executions | Wall s | CPU s | CPU µs per row | GC s | Largest to the driver | Shuffle written | Spilled |
|---|---|---|---|---|---|---|---|---|
| `cox.evaluate` | 4 | 520.2 | 2,812.3 | 140.6 | 315.3 | 56.5 MiB | 0 | 0 |
| `cox.build` | 1 | 140.7 | 169.9 | 34.0 | 253.8 | 1.8 MiB | 1.92 GiB | 0 |
| `cox.validate` | 1 | 86.3 | 269.0 | 53.8 | 0 | 60 KiB | 86.5 MiB | 121.0 MiB |
| `cox.summarize` | 1 | 28.3 | 34.7 | 6.9 | 58.9 | 1.0 MiB | 0 | 0 |
| `layout.group-sizes` | 1 | 0.6 | 0.3 | 0.1 | 0 | 88 KiB | 104 KiB | 0 |

**Point 2, logistic, 5M rows.** Fit 455 s; 5 iterations; 7,444 core-seconds offered, utilization 0.79, executors 2
to 7; max |z| 2.87; refit on 401 partitions identical.

| Operation | Executions | Wall s | CPU s | CPU µs per row | GC s | Largest to the driver | Shuffle written | Spilled |
|---|---|---|---|---|---|---|---|---|
| `logistic.pass-a` | 5 | 183.0 | 2,123.2 | 84.9 | 325.3 | 56.9 MiB | 265 KiB | 0 |
| `logistic.auc` | 1 | 61.6 | 195.8 | 39.2 | 263.8 | 4.1 MiB | 47.4 MiB, with 1.46 GiB read | 0 |
| `logistic.validate` | 1 | 48.3 | 476.5 | 95.3 | 0 | 256 KiB | 2.02 GiB | 1.43 GiB |
| `logistic.pass-b` | 5 | 46.0 | 275.3 | 11.0 | 252.6 | 1.2 MiB | 265 KiB | 0 |
| `logistic.summarize` | 1 | 39.5 | 418.4 | 83.7 | 60.3 | 56.0 MiB | 0 | 0 |
| `logistic.variance` | 1 | 37.0 | 427.3 | 85.5 | 59.9 | 56.3 MiB | 66 KiB | 0 |
| `logistic.build` | 1 | 29.6 | 152.1 | 30.4 | 171.8 | 1.9 MiB | 1.90 GiB | 0 |
| `logistic.provider-variance` | 1 | 7.4 | 41.2 | 8.2 | 47.6 | 1.3 MiB | 66 KiB | 0 |
| `layout.group-sizes` | 1 | 0.4 | 0.4 | 0.1 | 0 | 163 KiB | 202 KiB | 0 |

**Points 3 and 4, both models at 20M rows, failed in the working-set build.** Executors were lost to out-of-memory
(exit code 52) after validation (OI-115).

| Point | Failed after | Build before failing | Build's peak task memory | Validation |
|---|---|---|---|---|
| 3, Cox | 172 s | 7.67 GiB of shuffle written, 1.39 GiB read | 464 MiB | 63.3 s wall, 52.2 µs CPU per row, 346 MiB shuffle, 527 MiB spilled |
| 4, logistic | 262 s | 7.60 GiB of shuffle written | 392 MiB | 79.0 s wall, 63.6 µs per row, 8.12 GiB shuffle, 6.68 GiB spilled |

What these runs show:
- **The kernel passes dominate the CPU.**
  - Cox's evaluations take 85.6% of the fit's executor CPU.
  - SerBIN's passes A and B take 58.4% of the logistic fit's, and the co-moment and variance passes, which have the
    same p²-per-row work, another 20.6%.
- **Correction.** The 2026-11-16 note above estimated by arithmetic that the kernels took about a third of the CPU.
  That arithmetic used the sandbox's rates, and every kernel pass here costs about 2.8 times as much per row: 140.6
  against 51 µs for Cox's evaluation, 84.9 against 29.9 for pass A, 83.7 against 32.4 for the co-moments. Pass B
  costs about 4 times as much, 11.0 against 2.7. So steps 2 and 3 carry most of the possible gain.
- **Outside the passes.** More than a quarter of the wall time lies outside the Newton and SerBIN passes, so plan §3
  item 4 applies (step 0b):
  - Cox, 33%: build 18%, validation 11%, summaries 4%;
  - logistic, 49%: AUC 14%, validation 11%, summaries 9%, variance 8%, build 7%.

  Validation is fixed in round 157 (OI-109). Step 1 rebuilds the working set. The summary and variance passes belong
  to step 2. The AUC pass has an open item (OI-113).
- **Validation is costly.** Logistic validation shuffles 2.02 GiB and spills 1.43 GiB at 5M rows, and 8.12 and
  6.68 GiB at 20M (OI-109).
- **At 20M rows, block growth meets 2.2 GB executor heaps.** 64 MB blocks, held as rows and as arrays while they are
  built, exhaust executors in both models. Step 1's fixed small blocks are needed for any point past 5M on this
  cluster (OI-115).
- **GC.** It exceeds CPU in both builds and in the AUC pass.
- **Driver.** The driver's live heap after collections stays below 515 MiB, and executor heaps peak near 1.9 GB of
  their 2.2 GB.
- **Comparison with 2026-11-16.**
  - CPU: Cox 3,286 s against 3,071 s, logistic 4,110 s against 3,930 s, the same work within 7%.
  - Wall time: 779 s against 202 s for Cox, because the cluster began this run with one executor; D-76 reads wall
    time beside the 7,154 core-seconds offered.

### 2026-11-16: `02_scale`'s first look on the same kind of cluster (build `d0b9bd5`)

Wall time only. Default parallelism (cores) varied with autoscaling, so rows do not compare with one another. All rows
use 8,000 providers except the first, and every row recovered the truth.

| Model | Rows | Features | Cores | Iterations | Max \|z\| | Fit s | Follow-up s |
|---|---|---|---|---|---|---|---|
| Logistic | 1M | 10 (1,000 providers) | 4 | 4 | 1.86 | 31.8 | Provider tests 9.1 |
| Logistic | 1M | 100 | 24 | 5 | 2.71 | 133.6 | 27.7 |
| Logistic | 5M | 100 | 24 | 4 | 2.62 | 97.5 | 37.9 |
| Logistic | 5M | 180 | 20 | 5 | 2.73 | 361.4 | 115.1 |
| Logistic | 10M | 100 | 36 | 4 | 3.14 | 170.9 | 61.7 |
| Logistic | 20M | 100 | 24 | 4 | 2.88 | 744.7 | Not run |
| Cox | 1M | 100 | 40 | 3 | 2.62 | 23.3 | Baseline hazard 7.1 |
| Cox | 5M | 100 | 24 | 3 | 2.54 | 44.4 | 13.3 |
| Cox | 10M | 100 | 32 | 3 | 3.13 | 122.4 | 35.4 |
| Cox | 20M | 100 | 40 | 3 | 3.32 | 168.7 | 58.6 |
| Cox | 5M | 180 | 36 | 3 | 3.21 | 127.5 | 25.5 |
| Cox | 10M | 180 | 24 | 3 | 3.31 | 302.3 | 115.6 |
| SMR (stages 1 and 2) | 1M | 100 | 40 | 3 | 2.12 | 20.5 | Stage 2: 8.2 |
| SMR (stages 1 and 2) | 5M | 100 | 36 | 3 | 2.87 | 62.9 | Stage 2: 28.0 |

Three more rows are left out because they did not come from their own fits:
- logistic, 5M rows, 200 features;
- Cox, 50M rows, 100 features;
- Cox, 20M rows, 180 features.

Their fits did not complete, and `02_scale` appended a row computed from the previous run's fit. Each repeats the
preceding row's iteration count and max |z|. The logistic row's apparent failure to recover the truth (max |z| 10.5)
compared estimates for 100 features against the truth for 200. Round 152 makes the notebook refuse such a row
(OI-111). Why those three fits did not complete is not known.

## Sandbox measurements

### Round 149: one-core profile by call site

This was measured with `local[1]`, the `02_scale.py` design, 200,000 rows, 320 providers and 100 features. The full
tables are in plan §2.5 and round 149's evidence log.

| Model | Fit, s | Passes | Their CPU, s | Validation, wall s | Validation, CPU s | Working-set build, wall s |
|---|---|---|---|---|---|---|
| Cox (3 iterations) | 31.5 | 4 evaluations, 15.7 s wall | 12.7 | 5.5 | 1.6 | 7.1 |
| Logistic (4 iterations) | 48.9 | 4 pass A and 4 pass B, 13.5 s wall | 9.3 | 17.3 | 6.8 | 6.0 |

### Round 163: storage levels compared in the sandbox

This is a sandbox measurement: one core, local mode, the logistic model with the simple design. Each configuration
ran twice, alternating storage levels, each fit in a fresh JVM.

| Point | `MEMORY_AND_DISK`, fit s | `DISK_ONLY`, fit s | Pass A, wall s over all executions: memory, then disk |
|---|---|---|---|
| 200,000 rows, 180 features, 320 providers (5 iterations) | 113.8, 120.4 | 122.2, 121.7 | 40.1 and 37.2; 37.4 and 39.4 |
| 1,000,000 rows, 20 features, 1,000 providers (4 iterations) | 45.9, 46.0 | 47.0, 45.2 | 8.5 and 7.6; 8.4 and 7.6 |

- **No difference beyond run-to-run noise at either width.** Pass B, the lightest per row, was 0.5 to 1.7 s slower
  over five passes on disk at 180 features.
- **Caveat.** The working sets, about 0.3 and 0.16 GB, stayed in the operating system's page cache, so these runs
  measure Spark's serialization path and not device reads. The cluster comparison (runbook, "Comparing storage
  levels") measures both.

