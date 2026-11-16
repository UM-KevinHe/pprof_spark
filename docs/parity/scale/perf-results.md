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
