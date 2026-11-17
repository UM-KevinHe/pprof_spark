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
