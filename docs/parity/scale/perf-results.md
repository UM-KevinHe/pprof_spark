# Performance phase: results

Two kinds of numbers are kept here, in separate tables:
- **Cluster measurements** are runs of `PerfJob` (`perf-runbook.md`) on the standard cluster, entered from their
  report lines.
- **Sandbox measurements** come from the assistant's one-core sandbox, and say nothing about wall time on a cluster.

Estimates and arithmetic are never entered here; they live in `docs/spec/performance/plan.md`.

## Cluster measurements

The scale runs reported on 2026-11-13 used round 81's build (OI-103), so they are not entered.

### 2026-11-26: step 5's exit, SerBIN and the profile-likelihood Newton (build `91012fa`, round 200)

**The runs.** Points 2, 4, 8 and 10 (perf-runbook, "Comparing the logistic Newton methods"), with widget 6 set to
`serbin,profile`. Each point was fitted by SerBIN, then by the profile method, back to back, on the production cluster:
Spark 4.0, off-heap, autoscaling to 10 executors.

**The figures.** Computed from the reports' JSON with `scripts/compare_newton.py` and a breakdown script. The report
lines came pasted as text, and were transcribed into the sandbox for the computation. "Newton" means the iteration's
passes: pass A and pass B, or the profile pass. Task time is the executors' run time, GC and reading included.

| Point | Method | Iterations | Passes | Newton CPU | µs per row per pass | Newton task time | Newton reads | Fit CPU | Executors |
|---|---|---|---|---|---|---|---|---|---|
| 5M × 180 | SerBIN | 5 | 10 (5 A, 5 B) | 1,093 s | A 36.5, B 7.2 | 1,240 s | 20 GB | 1,793 s | 3 to 4 |
| | profile | 4 | 5 | 921 s | 36.9 | 1,041 s | 10 GB | 1,540 s | 8 |
| 20M × 180 | SerBIN | 4 | 8 (4 A, 4 B) | 3,618 s | A 37.8, B 7.4 | 4,311 s | 62 GB | 6,248 s | 3 to 7 |
| | profile | 4 | 5 | 3,721 s | 37.2 | 4,235 s | 40 GB | 6,245 s | 9 to 10 |
| 50M × 180 | SerBIN | 4 | 8 (4 A, 4 B) | 8,677 s | A 36.0, B 7.4 | 10,295 s | 159 GB | 14,811 s | 6 to 10 |
| | profile | 4 | 5 | 8,721 s | 34.9 | 10,033 s | 99 GB | 14,887 s | 10 |
| 100M × 180 | SerBIN | 4 | 8 (4 A, 4 B) | 16,048 s | A 31.9, B 8.2 | 22,018 s | 308 GB | 28,249 s | 5 to 10 |
| | profile | 4 | 5 | 16,020 s | 32.0 | 19,399 s | 194 GB | 27,891 s | 9 to 10 |

- **The same fit.** The log-likelihood is identical to the bit at all four points.
  - The estimates are within T-coef of SerBIN's: worst ratios 1.4·10⁻⁷, 2.9·10⁻⁶, 2.1·10⁻⁶ and 1.9·10⁻⁶; the largest
    |Δβ|/SE is 2.1·10⁻¹². The standard errors are within T-var, worst ratio at most 1.7·10⁻⁷.
  - R0 holds to the bit at 5M and 20M for both methods, and the truth checks pass.
- **Passes and reading.** Passes went from 10 to 5 and from 8 to 5, and the Newton passes read 37% to 50% less. Their
  GC and task times were lower too. But those depend on the executors a run had, and SerBIN's runs had fewer (below),
  so they are not attributed to the method (round 202).
- **CPU.**
  - 1.19 times less at 5M, where SerBIN took 5 iterations; within ±3% at 20M to 100M, where both took 4.
  - A profile pass costs what pass A does: the intercepts' solve in the tasks costs almost nothing. Pass B costs 7 to
    8 µs per row, so 5 profile passes cost about 4 pass A and 4 pass B.
  - The fifth profile pass is the trial at the converged β. It forms S and r, which nothing uses after the loop
    (OI-127).
- **Wall time is not compared.** SerBIN ran first in each pair, while the cluster scaled up: it had as few as 3
  executors at 5M and 20M. At 100M SerBIN's cache also spilled 22.8 GB to disk.
  - The maintainer found that most of the first runs' time went to scaling the workers from 1 to 10 (round 202).
  - The runbook now asks for a fixed cluster and the order `serbin,profile,profile,serbin`, and `compare_newton.py`
    compares fit times only on one fixed executor count.
  - CPU per row per pass is unaffected: pass A cost 36.5 µs per row on 3 to 4 executors here, against 37.2 at step 2's
    exit; the profile pass cost 36.9 on 8. The fits took 316.8 and 89.8 s,
  631.2 and 245.7, 794.0 and 506.4, and 1,547.0 and 997.6.
- **Against step 2's exit** (round 185). Pass A then took 37.2, 36.5, 31.3 and 30.4 µs per row; now 36.5, 37.8, 36.0
  and 31.9. That is up to 15% between runs (50M).

### 2026-11-25: step 4's exit, agreement with the sweep (build `0814d9a`, round 191)

The exit's second condition is agreement with the stratum sweep at a size both can run. Each point was run twice,
unstratified, on round 191's build, off-heap on Spark 4.0, with Breslow ties:
- as it is, the stratum aggregated over blocks;
- with a driver budget too small for the stratum's per-time totals, which keeps it in one block and sweeps it.

Every run had one executor.

| Point | Budget | Blocks | Fit s | Evaluation: CPU µs per row per pass | Utilization | ℓ | Max \|z\| |
|---|---|---|---|---|---|---|---|
| 1M rows, 20 features | default | 46 | 30.3 | 3.0 + 1.7 = 4.7 | 0.55 | −3812460.6236221213 | 1.939501825356209 |
| | 100,000 bytes | 1 | 20.5 | 0.95 | 0.22 | −3812460.6236221213 | 1.9395018253561744 |
| 250,000 rows, 180 features | default | 88 | 39.5 | 20.4 + 34.7 = 55.1 | 0.57 | −490209.9312356898 | 3.0966634634464305 |
| | 1,000,000 bytes | 1 | 32.7 | 17.8 | 0.25 | −490209.9312356898 | 3.0966634634464323 |

Readings:
- **Agreement.** At both points the log-likelihoods print identically, far inside T-fn. The truth checks' max |z|
  agree within 2·10⁻¹⁴, relative. This and the 20M-row fit complete step 4's exit.
- **The cost at these sizes (OI-126).** The aggregated evaluation needed 4.9 times the sweep's CPU per row at 20
  features and 3.1 times at 180, and the fits took 48% and 21% longer.
  - Each reduction group shuffles its strata's whole per-time totals, whatever the stratum's size: 13.7 MB per
    evaluation at 1M × 20, and 171 MB at 250,000 × 180.
  - At 20M rows the same cost was spread over 80 times the rows, and pass 1 was 16% of the evaluation.
- **Parallelism.** On the one executor, the aggregated runs used its cores (utilization 0.55 and 0.57); the sweep used
  one (0.22 and 0.25).
- **A first attempt** at the one-block runs used a budget of 1,000 bytes and stopped at once. The budget also bounds
  each reduction group's partial: 1,856 bytes at 20 features, 131,776 at 180. The budgets above were checked in the
  sandbox first.

### 2026-11-25: step 4's exit, the unstratified fit (build `0814d9a`, round 191)

The exit of plan §4's step 4 ran one Cox stratum of 20M rows on round 191's build: `--model cox --rows 2e7 --features
180 --providers 8000 --strata none`, Breslow ties, off-heap storage, Spark 4.0. Before step 4 that stratum was one block
of 29 GB of covariates, and the fit could not run.

**The fit.**
- 704.7 s, 3 iterations, converged, ℓ = −5.383551576712093·10⁷, max |z| 2.73.
- Executors 1 to 10, utilization 0.48 (the autoscaler began with one). The data were generated first, in 168.6 s.
- **Layout.** The stratum spread over 7,021 blocks of 2,849 rows, in 502 reduction groups of 14. The cache held
  7.55 GiB, all in memory.

| Operation | Wall s (share of fit) | CPU s | µs per row per pass | GC s | Shuffle | To the driver |
|---|---|---|---|---|---|---|
| Build | 248.6 (35.3%) | 907.3 | — | 123.8 | 7.36 GiB | — |
| Pass 1, 4 runs | 103.3 (14.7%) | 534.8 | 6.7 | 119.0 | 4.95 GiB, 1.24 per run | 5.3 MB per run |
| Pass 2, 4 runs | 287.2 (40.8%) | 2,822.8 | 35.3 | 137.7 | — | 68.1 MB per run |
| Summaries | 27.5 (3.9%) | 166.1 | — | 22.1 | — | — |
| Validation | 31.4 (4.5%) | 97.1 | — | 0.1 | — | — |
| Planning, the grid's two jobs | 2.0 | 3.9 | — | — | — | — |

Readings:
- **The exit's fit runs.** One stratum of 20M rows at 180 features is evaluated on every executor, from per-time
  aggregates over 7,021 blocks.
- **Cost per row.** An evaluation needs 42.0 µs of CPU per row, 6.7 in pass 1 and 35.3 in pass 2. That is 1.24
  times the stratified sweep at 20M rows (33.8 µs, step 3's exit).
- **Pass 1** is 16% of the evaluation's CPU on the cluster, far less than in the sandbox (OI-126). Its shuffle, 1.24
  GiB per evaluation, is what §7 of the specification estimated.
- **Memory.** Peak task memory was 256 MiB, in pass 1's fold. Executor heaps peaked at 1.79 GiB of 2.2, the driver's
  at 7.05 GB (0.56 GB after GC).
- **Not yet run.** Agreement with the stratum sweep on the cluster, at a size both can run. The sandbox found the
  log-likelihood's bits identical at 200,000 × 20 and 50,000 × 180 rows (round 190).

### 2026-11-23: step 3's exit (build `c3bbc66`, round 186)

The exit of plan §4's step 3 ran the Cox points of step 2's exit on round 186's build, with the weighted-Gram
information (D-80). All four used off-heap storage on Spark 4.0, with 180 features and 8,000 providers, and Breslow
ties. "Before" is step 2's exit, on round 184's build (2026-11-22, below). The shuffle read is the evaluations'.

| Run | Point | Fit s, before → now | Iterations | Executors, utilization | Evaluation: CPU µs per row, before → now | Ratio | GC, share of CPU | Outside CPU and GC | Shuffle read | Cache | Max \|z\| |
|---|---|---|---|---|---|---|---|---|---|---|---|
| S5 | Cox, 5M | 78.0 → 301.2 | 3 | 1, 0.86 | 55.5 → 31.6 | 1.76 | 1.9% | 10.4% | 0.00 GiB | 1.88 GiB | 2.86 |
| S6 | Cox, 20M | 278.7 → 928.9 | 3 | 1 to 5, 0.50 | 50.1 → 33.8 | 1.48 | 5.6% | 14.4% | 0.00 GiB | 9.97 GiB, 2.18 on disk | 2.38 |
| Point 7 | Cox, 50M | 467.4 → 438.6 | 3 | 7 to 8, 0.86 | 42.5 → 33.4 | 1.27 | 7.3% | 8.7% | 2.34 GiB | 21.18 GiB | 2.86 |
| Point 9 | Cox, 100M | 986.0 → 745.3 | 3 | 8 to 10, 0.89 | 48.4 → 32.2 | 1.50 | 13.0% | 8.9% | 5.78 GiB | 43.40 GiB, 0.37 on disk | 3.00 |

Readings:
- **Step 3's measurement.** Cox evaluation needs 31.6 to 33.8 µs of CPU per row per pass at every size from 5M to 100M
  rows. That is 1.27 to 1.76 times less than on round 184's build. The log-likelihoods print exactly as before, and
  every truth check passed.
- **Against the sandbox.** It measured 1.28 times with these data's few ties and no entry times (round 186). The
  cluster's gain is larger at 5M and 100M rows, and about the same at 50M.
- **Fits.**
  - Cox at 100M rows took 745.3 s against 986.0, 24% less; at 50M, 438.6 s against 467.4.
  - At 5M and 20M rows, wall times do not compare: S5 ran on one executor, and S6 started on one and reached five
    (utilization 0.50).
- **Rebuilt partitions (OI-123).** At 100M the evaluations read 5.78 GiB of shuffle against 22.96 on round 184's build.
  At 50M they read 2.34 GiB and wrote 1.20, so a map stage of the build ran again.
- **Where the time goes at 100M.** Evaluation is 66% of the fit, the build 25% and the summaries 6%.

### 2026-11-22: step 2's exit (build `2f084b5`, round 184)

The exit of plan §4's step 2 (D-76) ran the earlier off-heap points on round 184's build, whose kernels are round
182's. All runs used off-heap storage, the default, on Spark 4.0, with 180 features and 8,000 providers unless the
point says otherwise. "Before" means:
- S2–S7: round 163's build (`ada822d`), off-heap;
- S8: round 177's (`b92919c`), off-heap;
- points 7 and 9: round 161's (`82fd37a`), disk-only, so their ratios mix the storage level with the kernels.

The heaviest pass is pass A for the logistic model and the evaluation for Cox.

| Run | Point | Fit s, before → now | Iterations | Executors, utilization | Heaviest pass: CPU µs per row, before → now | Ratio | Outside CPU and GC | Cache | Max \|z\| |
|---|---|---|---|---|---|---|---|---|---|
| S2 | Logistic, 5M | 209.8 → 570.2 | 5 | 1 to 5, 0.43 | 86.7 → 37.2 | 2.33 | 15.2% | 1.89 GiB | 2.87 |
| S3 | Logistic, 5M, 20 features | 18.7 → 23.4 | 4 | 6, 0.38 | 2.6 → 1.6 | 1.61 | 19.6% | 0.27 GiB | 1.67 |
| S4 | Logistic, 20M | 393.2 → 267.6 | 4 | 6 to 10, 0.74 | 85.9 → 36.5 | 2.36 | 9.9% | 7.56 GiB | 3.15 |
| S7 | Logistic, 50M | 1,218.9 → 672.7 | 4 | 6 to 10, 0.70 | 83.3 → 31.3 | 2.66 | 9.2% | 18.90 GiB | 2.90 |
| S8 | Logistic, 100M | 3,934.9 → 1,195.4 | 4 | 6 to 10, 0.86 | 81.0 → 30.4 | 2.66 | 9.2% | 64.41 GiB, 10.22 on disk | 2.86 |
| S5 | Cox, 5M | 146.2 → 78.0 | 3 | 10, 0.59 | 127.6 → 55.5 | 2.30 | 13.2% | 1.88 GiB | 2.86 |
| S6 | Cox, 20M | 314.4 → 278.7 | 3 | 6 to 10, 0.65 | 112.5 → 50.1 | 2.25 | 11.4% | 7.52 GiB | 2.38 |
| Point 7 | Cox, 50M | 1,243.6 → 467.4 | 3 | 6 to 10, 0.82 | 103.6 → 42.5 | 2.44 | 9.1% | 18.77 GiB | 2.86 |
| Point 9 | Cox, 100M | 1,677.5 → 986.0 | 3 | 6 to 10, 0.89 | 101.7 → 48.4 | 2.10 | 10.8% | 61.39 GiB, 0.97 on disk | 3.00 |

S8 by operation, round 177's build against round 184's. CPU and GC are summed over executors; the shuffle is what
each pass read.

| Operation | Wall s | CPU s | GC s | Shuffle read |
|---|---|---|---|---|
| Pass A, 4 runs | 1,523.8 → 436.8 | 32,391.9 → 12,175.7 (2.66 times less) | 4,703.5 → 1,695.3 | 37.35 → 3.20 GiB |
| Build | 1,144.1 → 199.5 | 3,660.9 → 3,618.1 | 1,931.3 → 1,187.2 | its own shuffle |
| Pass B, 4 runs | 298.3 → 167.4 | 6,129.9 → 3,532.5 | 3,193.1 → 1,513.3 | 33.42 → 9.95 GiB |
| Variance pass | 354.7 → 147.1 | 8,008.3 → 3,172.0 (2.52 times less) | 746.4 → 461.1 | 0 → 0.07 GiB |
| Summaries | 327.5 → 121.2 | 7,554.2 → 3,564.8 (2.12 times less) | 809.1 → 361.5 | 5.96 GiB → none |
| Provider variances | 59.0 → 53.1 | 1,086.1 → 879.5 | 662.3 → 496.2 | 5.52 → 7.34 GiB |
| AUC pass | 55.4 → 37.3 | 561.1 → 496.9 | 491.2 → 311.9 | 1.63 → 0.76 GiB |

Readings:
- **Step 2's exit is met.** At 180 features the heaviest pass needs 2.25 to 2.66 times less CPU per row at every size.
  The sandbox measured 2.75 for pass A and 2.5 for Cox's evaluation (round 182). At 20 features it needs 1.61 times
  less.
- **Fits.** From 20M rows up, fits take 11% to 70% less time. At 5M rows the comparison is confounded. S2 began on one
  executor (utilization 0.43), and S3 is too small for its passes to dominate. CPU per row is the comparable measure.
- **100M rows.** Both models fit: logistic in 1,195.4 s and Cox in 986.0 s (OI-118 is closed). Against S8 on round
  177's build:
  - shuffle reads fell from 83.9 to 21.3 GiB;
  - pass A's time outside CPU and GC fell from 24.9% to 9.2%;
  - the cache's peak fell from 170.0 to 64.4 GiB, and its part on disk from 36.5 to 10.2 GiB.

  Partitions are still rebuilt (OI-123): Cox's evaluations read 23.0 GiB of shuffle, logistic's pass B 10.0 and the
  provider variances 7.3.
- **Where the time now goes, at 100M.**
  - Cox: evaluation 71% of the fit, build 21%.
  - Logistic: pass A 37%, build 17%, pass B 14%, the variance pass 12%, the summaries 10%.

  Step 3 is for Cox's evaluation.
- **Heaps.** Executor heaps peaked at 1.1 to 2.0 GiB of 2.2. The driver's peaked at 7.4 to 8.2 GB, and 0.45 to 0.76
  GB after GC.

### 2026-11-21: S8 on round 177's build (`b92919c`)

S8 again: the logistic model with 100M rows, 180 features and 8,000 providers, off-heap storage, on Spark 4.0.
Round 177's build has round 173's leaner AUC pass and D-77's off-heap default, but not round 178's records.

| Run | Point | Fit s | Iterations | Executors, utilization | Blocks, G, R | Cache | Heaviest pass: CPU µs per row | Outside CPU and GC | Max \|z\| |
|---|---|---|---|---|---|---|---|---|---|
| S8 | Logistic, 100M, 180 | 3,934.9 | 4 | 1 to 10, 0.76 | 8,000, 16, 500 | 170.0 GiB, 36.5 of it on disk | 81.0 | 24.9% | 2.86 |

By operation, with CPU and GC summed over executors:

| Operation | Runs | Wall s | Share of the fit | CPU s | GC, share of CPU | Shuffle read |
|---|---|---|---|---|---|---|
| Pass A | 4 | 1,523.8 | 38.7% | 32,391.9 | 14.5% | 37.35 GiB |
| Build | 1 | 1,144.1 | 29.1% | 3,660.9 | 52.8% | 38.07 GiB, its own shuffle |
| Variance pass | 1 | 354.7 | 9.0% | 8,008.3 | 9.3% | none |
| Summaries | 1 | 327.5 | 8.3% | 7,554.2 | 10.7% | 5.96 GiB |
| Pass B | 4 | 298.3 | 7.6% | 6,129.9 | 52.1% | 33.42 GiB |
| Validation | 2 | 167.4 | 4.3% | 528.6 | 0.1% | its own shuffle |
| Provider variances | 1 | 59.0 | 1.5% | 1,086.1 | 61.0% | 5.52 GiB |
| AUC pass | 1 | 55.4 | 1.4% | 561.1 | 87.5% | 1.63 GiB, its sort's 0.76 among it |

Readings:
- **The logistic model fits at 100M rows.** S8 on round 163's build failed in the AUC pass after 5,690.5 s. Round 173's
  AUC pass took 55.4 s here. OI-119 is closed.
- **Pass A carries the fit.** It takes 38.7% of the wall time, at 81.0 µs of CPU per row per pass (S7 at 50M: 83.3).
  Step 2's chunked sums (round D) are for it. The next heaviest per row are the variance pass at 80.1 µs and the
  summaries' co-moments at 75.5.
- **The passes recompute cached partitions.** These passes read shuffle output:
  - pass A, 37.35 GiB over its four runs;
  - pass B, 33.42 GiB;
  - the summaries, 5.96 GiB;
  - the provider variances, 5.52 GiB.

  Partitions of the cached working set were rebuilt from the build's shuffle. As a result, 24.9% of pass A's executor
  time was outside CPU and GC, against 11.1% for S7. Executors went from 1 to 10 during the fit, and 36.5 GiB of the
  170.0 GiB cache was on disk (OI-123).
- **GC.**
  - Its share of CPU: pass A 14.5%, pass B and the build about half, the provider variances 61.0%, the AUC pass 87.5%.
  - Round 178's build no longer joins γ to the blocks in any pass. In the sandbox, that cut the passes' GC by about
    three quarters at 180 features.
- **Twice S7's rows took 3.2 times its time** (1,218.9 s), the recomputation included.
- **Heaps.** The driver heap peaked at 8.58 GiB, 1.17 GiB after GC; executor heaps reached 2.01 GiB.

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

### 2026-11-19: storage levels paired, S1 to S6 (build `ada822d`, round 163)

Each point ran three times back to back on the same data and cluster, with widget 5 set to
`MEMORY_AND_DISK,DISK_ONLY,OFF_HEAP`, in that order. The off-heap runs are the lines recorded in the section below.

**Fit seconds**, with the change against off-heap in brackets:

| Run | Point | Executors (M, D, O) | `MEMORY_AND_DISK` | `DISK_ONLY` | `OFF_HEAP` |
|---|---|---|---|---|---|
| S1 | Logistic, 1M rows, 180 features | 4, 4, 4 | 117.5 (+43.6%) | 81.6 (−0.3%) | 81.8 |
| S2 | Logistic, 5M, 180 | 6, 6, 6 to 8 | 263.4 (+25.5%) | 212.4 (+1.2%) | 209.8 |
| S3 | Logistic, 5M, 20 | 8, 8, 8 | 20.6 (+10.3%) | 18.4 (−1.4%) | 18.7 |
| S4 | Logistic, 20M, 180 | 10, 10, 10 | 449.6 (+14.3%) | 399.6 (+1.6%) | 393.2 |
| S5 | Cox, 5M, 180 | 6, 6, 6 to 9 | 148.1 (+1.3%) | 153.0 (+4.6%) | 146.2 |
| S6 | Cox, 20M, 180 | 9, 9 to 10, 10 | 374.2 (+19.0%) | 330.5 (+5.1%) | 314.4 |

**The heaviest pass** (pass A, or Cox's evaluation): CPU-s; GC s; share of run time outside CPU and GC.

| Run | `MEMORY_AND_DISK` | `DISK_ONLY` | `OFF_HEAP` |
|---|---|---|---|
| S1 | 436.4; 6.9; 14.2% | 435.1; 9.4; 11.5% | 432.0; 8.4; 9.9% |
| S2 | 2,134.9; 17.1; 8.8% | 2,144.7; 31.4; 8.4% | 2,167.3; 28.6; 9.1% |
| S3 | 46.3; 2.4; 13.7% | 52.0; 2.9; 12.5% | 51.2; 2.5; 14.2% |
| S4 | 6,877.6; 460.2; 7.1% | 6,917.0; 191.1; 7.8% | 6,873.4; 185.6; 7.8% |
| S5 | 2,507.8; 11.7; 8.3% | 2,525.4; 18.9; 8.0% | 2,553.0; 23.7; 9.8% |
| S6 | 9,053.6; 615.1; 7.2% | 9,008.5; 180.5; 8.1% | 9,002.7; 177.8; 7.8% |

**GC over the whole fit at 20M rows:**
- S4: 1,537.9 s with memory-and-disk, 654.7 s with disk only, 573.1 s off-heap.
- S6: 947.1 s, 307.6 s and 303.4 s.

**The cache.** Memory-and-disk keeps blocks as objects in the JVM heap, at about 3.7 times their serialized size:
1.39 GiB at S1 against 0.37 GiB, and at 20M rows 9.69 GiB in memory plus 4.81 GiB on disk (S4). Disk-only and
off-heap hold the serialized 0.37 to 7.50 GiB, on disk or wholly in off-heap memory.

**Results.** All three levels gave the same log-likelihood bits at every point.

Readings:
- **The question D-77 was held for is answered.** On data that fit in memory (1M and 5M rows), keeping the working set
  on the JVM heap was not faster. It was the slowest level at four of the five small-to-mid points, and within 1.3%
  of off-heap at S5.
- **Off-heap was fastest, or within 1.4% of the fastest, at every point.** Disk-only stayed within 5.1% of off-heap.
- **Memory-and-disk paid in GC:** 2.7 to 3.1 times the other levels' at 20M rows, from a heap cache 3.7 times larger
  than the serialized one.
- **The kernels' CPU was the same at every level,** within 1.5% at 180 features. Storage changes what surrounds the
  kernels, not the kernels.
- **Caveat.** Memory-and-disk always ran first in its sweep and may carry first-run costs; S1's 44% likely includes
  some. Its extra GC at 20M is not an order effect.
- **The decision.** D-77 was approved on 2026-11-19: `OFF_HEAP` is the default storage level from round 169's build.

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
- **Logistic at 100M rows fails in the AUC pass under any level** (OI-119). On round 177's build it fits
  (2026-11-21, above).

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

### Round 199: logistic, SerBIN and the profile-likelihood Newton (step 5)

**Setup.** One core, Spark 4.1.0 in local mode, round 199's build. Each method ran twice, alternating
`--newton profile` with the default. The figures are medians; iteration CPU is the passes' executor CPU.

| Point | Method | Iterations | Passes | Iteration CPU | Fit |
|---|---|---|---|---|---|
| 200,000 × 180, 320 providers | SerBIN | 5 | 10 (5 A, 5 B) | 16.6 s | 92.4 s |
| | profile | 4 | 5 | 12.5 s | 79.7 s |
| 1,000,000 × 20, 1,000 providers | SerBIN | 4 | 8 (4 A, 4 B) | 5.3 s | 42.3 s |
| | profile | 4 | 5 | 3.9 s | 39.0 s |

- **Cost.** The iterations took 1.32 and 1.34 times less CPU, and the fits were 14% and 8% shorter. The profile
  method makes one pass per iteration, plus the pass at its start; no trial was rejected.
- **Where it comes from.** At 180 features a profile pass cost 12.5 µs per row, against pass A's 13.6 and pass B's
  3.8 (each method's first run). So the intercepts' solve costs little, and the gain comes from dropping pass B and,
  at 180 features, one iteration.
- **The same fit.** The log-likelihood is identical to the bit at both points. The largest |β̂ − β|/SE against the
  generating coefficients agrees within 3·10⁻¹⁴ relative.
- **Not compared here: the estimates themselves.** These reports did not carry them. PerfJob now writes `estimates`
  and `standard_errors`, so the cluster's exit can compare the methods within T-coef.

### Round 149: one-core profile by call site

This was measured with `local[1]`, the `02_scale.py` design, 200,000 rows, 320 providers and 100 features. The full
tables are in plan §2.5 and round 149's evidence log.

| Model | Fit, s | Passes | Their CPU, s | Validation, wall s | Validation, CPU s | Working-set build, wall s |
|---|---|---|---|---|---|---|
| Cox (3 iterations) | 31.5 | 4 evaluations, 15.7 s wall | 12.7 | 5.5 | 1.6 | 7.1 |
| Logistic (4 iterations) | 48.9 | 4 pass A and 4 pass B, 13.5 s wall | 9.3 | 17.3 | 6.8 | 6.0 |

### Round 190: Cox per-time aggregates, the evaluation (step 4's build)

This is a sandbox measurement: one core, local mode, with the working set on disk, the simple design, and one Cox
stratum, `--strata none`. PerfJob's times are whole days. Builds alternated, each fit in a fresh JVM:
- round 189's (`b89f0d2`), which sweeps the stratum in one block, as one task;
- round 190's, which spreads it over blocks of the default size and evaluates it from per-time aggregates.

| Point | Build | Fit s | Evaluation, 4 runs: CPU s (pass 1 + pass 2) | Planning jobs, wall s |
|---|---|---|---|---|
| Cox, 200,000 rows, 20 features, unstratified, Breslow | before | 16.7, 15.2 | 0.70, 0.73 | — |
| | after | 24.7, 24.7 | 1.17 + 0.53, 1.18 + 0.53 | 2.2, 2.1 |
| The same, Efron | before | 15.6, 14.7 | 0.79, 0.82 | — |
| | after | 23.7, 24.3 | 1.29 + 0.53, 1.32 + 0.53 | 1.9, 2.0 |
| Cox, 50,000 rows, 180 features, unstratified, Breslow | before | 31.5, 30.4 | 2.73, 2.60 | — |
| | after | 47.4, 44.8 | 2.99 + 2.95, 3.03 + 3.05 | 2.3, 2.0 |

- **The same results.** The log-likelihood's bits and the iteration counts match across builds at all three points.
- **On one core, the aggregated evaluation costs 2.2 to 2.4 times the sweep's CPU.**
  - Pass 1 sums every row into its stratum's per-time totals and shuffles them. At 180 features it costs about as much
    as the whole sweep.
  - Pass 2 costs about as much again.
  - Fits take 45% to 60% longer, and planning adds about 2 s.
- **What it buys.** The sweep is one task whatever the cluster's size, while the aggregated evaluation runs on every
  block's task. Step 4's exit measures that on the cluster.
- **Pass 1's cost (OI-126).**
  - Pass 1 adds rows with Neumaier sums per element. `per-time-aggregates.md` §5 describes plain sums per block, folded
    with Neumaier sums.
  - Rows without late entry still subtract at entry time 0, which no event time can see.

### Round 186: the Cox weighted-Gram information (step 3's kernel)

This is a sandbox measurement: one core, local mode, with the working set on disk, the simple design, and Cox stratified
by provider. PerfJob's times are whole days, so some deaths tie, and its data have no entry times. Builds alternated,
round 185's (`f18e528`, with round 182's Cox kernel) and round 186's, each fit in a fresh JVM.

| Point | Build | Fit s | Evaluation, 4 passes: CPU s (µs per row per pass) | GC s |
|---|---|---|---|---|
| Cox, 100,000 rows, 180 features, 160 strata, Breslow | before | 33.6, 35.0 | 6.46, 6.69 (16.4) | 0.19, 0.42 |
| | after | 31.9, 35.8 | 4.96, 5.32 (12.9) | 0.38, 0.28 |
| The same, Efron | before | 36.6, 34.8 | 7.88, 7.91 (19.7) | 0.17, 0.21 |
| | after | 33.4, 42.3 | 5.30, 6.15 (14.3) | 0.21, 0.26 |
| Cox, 1,000,000 rows, 20 features, 1,000 strata, Breslow | before | 28.6, 29.5 | 2.78, 2.80 (0.70) | 0.13, 0.14 |
| | after | 30.9, 30.4 | 3.34, 2.62 (0.75) | 0.14, 0.12 |

- **The same results.** The log-likelihood's bits and the iteration counts match across builds at all three points.
- **At 180 features.** Evaluation's CPU fell 1.28 times with Breslow ties and 1.38 times with Efron. PerfJob's data tie
  fewer deaths than the probe's and have no entry times. So the probe's larger gains for heavy ties and truncation,
  up to 2.4 times (`weighted-gram.md` §7), are not exercised here.
- **At 20 features.** No change beyond noise. The second pass's per-row work weighs about as much as the S₂ sweep it
  replaces.
- **Fit times.** These stayed within noise. One Efron fit, at 42.3 s, was slow in every operation, a sandbox outlier.

### Round 182: chunked compensated sums (step 2, round D)

This is a sandbox measurement: one core, local mode, with the working set on disk, the simple design, Cox stratified by
provider with Breslow ties. Builds alternated, round 181's (`cb6f8c1`) and round 182's, each fit in a fresh JVM. CPU
is summed over each operation's runs.

| Point | Build | Fit s | Heaviest pass: CPU s (µs per row per pass) | Variance pass CPU s | Summaries CPU s |
|---|---|---|---|---|---|
| Logistic, 200,000 rows, 180 features, 320 providers | before | 124.8, 127.0 | Pass A, 5 runs: 36.32, 36.59 (36.5) | 7.37, 7.26 | 7.47, 8.05 |
| | after | 94.9, 91.0 | 13.34, 13.18 (13.3) | 2.73, 2.74 | 2.98, 2.51 |
| Logistic, 1,000,000 rows, 20 features, 1,000 providers | before | 45.2, 44.7 | Pass A, 4 runs: 3.52, 3.42 (0.87) | 0.93, 0.96 | 1.12, 1.04 |
| | after | 42.6, 43.5 | 2.35, 2.30 (0.58) | 0.70, 0.67 | 0.76, 0.70 |
| Cox, 100,000 rows, 180 features, 160 strata | before | 54.4, 54.5 | Evaluation, 4 runs: 18.62, 18.90 (46.9) | — | 0.58, 0.59 |
| | after | 42.6, 42.2 | 7.47, 7.62 (18.9) | — | 0.73, 0.55 |
| Cox, 1,000,000 rows, 20 features, 1,000 strata | before | 31.8, 31.5 | Evaluation, 4 runs: 4.58, 4.51 (1.14) | — | 0.75, 0.71 |
| | after | 31.1, 28.8 | 3.00, 2.84 (0.73) | — | 0.72, 0.71 |

- **The same results.** The log-likelihood's bits and the iteration counts match across builds at all four points.
- **At 180 features.**
  - Logistic pass A's CPU fell 2.75 times, and the variance pass and summaries about 2.7 to 2.8 times.
  - Cox evaluation fell 2.5 times.
  - Fits ran 22% to 26% faster.
- **At 20 features.** These passes fell by about a third, and fits by about 5%, since the p² sums weigh less there.
- **Unchanged.** Pass B and the provider variances keep their sums, and their CPU did not change.
- **Against the study's estimate.** Its prototype ran pass A at 4.85 µs per row in its own harness. Here pass A
  costs 13.3 µs per row with Spark's overheads included. The cluster's measurement is step 2's exit (D-76).

### Round 180: Cox rows in records (step 2, round B)

This is a sandbox measurement: one core, local mode, with the working set on disk, and the Cox model stratified by
provider with Breslow ties and the simple design. Builds alternated, `main`'s at `86a7d9f` and round 180's, each fit
in a fresh JVM. Records hold 182 rows at 180 features and 1,638 at 20.

| Point | Build | Fit s | Evaluation, 4 passes: CPU s; GC s | Build: CPU s; GC s |
|---|---|---|---|---|
| 100,000 rows, 180 features, 160 strata | before | 55.0, 59.6 | 21.43, 21.13; 0.35, 0.54 | 2.74, 3.21; 0.36, 0.29 |
| | after | 57.7, 56.0 | 19.13, 19.23; 0.18, 0.19 | 2.95, 3.04; 0.25, 0.27 |
| 1,000,000 rows, 20 features, 1,000 strata | before | 32.7, 31.3 | 5.05, 5.14; 0.21, 0.19 | 3.83, 3.67; 0.20, 0.26 |
| | after | 32.7, 32.6 | 4.61, 4.52; 0.13, 0.11 | 4.01, 3.89; 0.31, 0.32 |

- **The same results.** The log-likelihood's bits and the iteration counts match across builds at both points.
- **The evaluation.** Its CPU fell by about 10% at both points, and its GC by between a third and a half. Breslow fits
  no longer zero the Efron buffers at each distinct time, and the Breslow term's means are computed once per time.
- **The build.** It took about 5% more CPU at 20 features (4.0 s against 3.7 to 3.8 s), and the same at 180. Fit times
  stayed within noise.
- **Caveat.** As for round 178, the sandbox's blocks are small, and memory pressure on the cluster's heaps is not
  visible here.

### Round 178: logistic rows in records (step 2, round A)

This is a sandbox measurement: one core, local mode, with the working set on disk, the logistic model and the simple
design. Builds alternated, `main`'s at `b92919c` and round 178's, each fit in a fresh JVM. Records hold 182 rows at
180 features and 1,638 at 20.

| Point | Build | Fit s | Pass A, 5 or 4 runs: CPU s; GC s | Variance pass: CPU s; GC s | Passes' peak task memory |
|---|---|---|---|---|---|
| 200,000 rows, 180 features, 320 providers | before | 134.2, 150.7 | 34.4, 37.4; 0.99, 1.21 | 6.87, 7.44; 0.15, 0.14 | 1.0 MB |
| | after | 142.8, 146.3 | 35.6, 38.0; 0.28, 0.29 | 7.19, 7.09; 0.04, 0.06 | none |
| 1,000,000 rows, 20 features, 1,000 providers | before | 60.6, 55.5 | 4.57, 4.46; 0.26, 0.49 | 1.20, 1.23; 0.07, 0.06 | 1.0 MB |
| | after | 56.5, 56.9 | 3.74, 3.89; 0.11, 0.15 | 0.99, 1.07; 0.03, 0.03 | none |

- **The same results.** The log-likelihood's bits and the iteration counts match across builds at both points.
- **Without the join.** The passes no longer join γ to the blocks.
  - Their GC fell by about three quarters at 180 features, and by half or more at 20.
  - Their peak task memory, the join's, fell from 1 MB to none.
  - At 20 features, pass A's CPU fell by about 15%. At 180 features the kernel dominates, and its CPU did not change.
- **Unchanged.** These stayed within noise:
  - the bytes each pass reads from the cache, the whole working set: about 297 MB and 187 MB;
  - the cache's size, 299 MB and 195 MB;
  - the build, pass B, the AUC pass and the summaries.

  Whole-fit times varied by up to 16 s between runs of the same build.
- **Caveat.** The sandbox's blocks are 4 MB, smaller than the cluster's one-provider blocks of 9 to 18 MB at 50M and
  100M rows. A one-core sandbox also cannot show memory pressure on 2.2 GB executor heaps. S8 on the cluster can.

### Round 173: the AUC pass before and after

This is a sandbox measurement: one core, local mode, where the working set is on disk; the logistic model with the
simple design. Builds alternated, `main`'s at `5adcc31` and round 173's, each fit in a fresh JVM. The figures are those
of the `logistic.auc` call site.

| Point | Build | Wall s | Run s | CPU s | GC s | Read from the cache |
|---|---|---|---|---|---|---|
| 200,000 rows, 180 features, 320 providers | before | 7.29, 7.09 | 5.39, 5.16 | 3.07, 2.87 | 0.87, 0.43 | 595 MB |
| | after | 6.69, 6.56 | 4.51, 4.25 | 2.75, 2.55 | 0.15, 0.15 | 301 MB |
| 1,000,000 rows, 20 features, 1,000 providers | before | 6.44, 5.75 | 4.99, 4.29 | 3.08, 2.82 | 0.46, 0.36 | 374 MB |
| | after | 6.50, 6.35 | 4.81, 4.60 | 3.29, 2.98 | 0.10, 0.14 | 203 MB |

- **The pass reads the working set once instead of twice,** and its GC falls by two thirds or more. At 180 features its
  wall time fell by about 8%; at 20 features, where the blocks are small and the cached scores cost as much as the
  second read saved, it did not change.
- **The results are the same:** the log-likelihood bits match across builds at each point, and the AUC is pinned by the
  golden runs and `LogisticAucSuite`. Whole-fit times varied by up to 10 s between runs of the same build.
- **Caveat.** A one-core sandbox cannot show the effect that matters for OI-119, memory pressure on 2.2 GB executor
  heaps with 18 MB blocks; S8 on the cluster can.

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

