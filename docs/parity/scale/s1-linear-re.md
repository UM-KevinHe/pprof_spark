# S1 scale smoke: linear random effects (part 4a)

- Status: prepared in round 108 (2026-10-22); to be run by the maintainer on his cluster, results below.
- Plan: `docs/spec/scale/plan.md` §3 (the checks) and §9 (S1: one point near 10⁷ rows with S0's checks, as part of a
  feature's definition of done); D-50, D-51 item 1.
- Runner: `pprof.spark.bench.S1LinearRE` (bench module), tested on a small point by `S1LinearRESuite`.

## The point

`bench.synthetic`'s linear data (plan §4): n = 10⁷ rows among m = 10⁴ providers of equal size, p = 20 features
(odd-numbered binary, even-numbered standard normal on a 1/64 grid, βₖ = (−1)ᵏ·0.5/√20), provider effects
γⱼ = 0 + 0.5·Zⱼ, residual SD 1, seed 79. The model is the default fit: REML, an intercept, no offset or weights.

## What the runner does

1. Generates the data under `--path` (Parquet, with the providers' true effects and a manifest), unless the manifest
   is already there, and verifies the data against the manifest's checksum.
2. Fits the model (fit A), then runs the provider tests (reference: the median), option `alpha` and the standardized
   measures on the training data.
3. Fits again with the driver budget at three quarters of the providers' summaries (fit B), so the search sums the
   providers across the cluster (spec 4a-1 §7 step 5); with m = 10⁴ and p = 20 the budget is about 1.5 MB, above the
   block partials (about 1 MB) and below the summaries (2 MB).
4. Fits again on the input repartitioned by `id` with `spark.sql.shuffle.partitions` doubled plus one (fit C, R0).
5. Writes one line of JSON under `--path/reports/<time>` and prints it.

| Report field | Content |
|---|---|
| `timesSeconds` | Each stage: generate, verify, fit, provider tests, alpha intervals, measures, the distributed fit, the R0 rerun |
| `driverPeakHeapMB`, `driverMaxHeapMB` | Per stage, the sum of the heap pools' peaks (an upper bound of the driver's peak heap); the driver's maximum heap |
| `fit` | Criterion, rows, providers, fixed effects, blocks, σ̂, θ̂, σ̂_α, the deviance, each fit's search path, evaluations and boundary, warnings |
| `checks` | Each check's value and verdict, below |
| `passed` | Whether every check passed |
| `stages`, `singleTaskStages` | The number of Spark stages, and the names of those that ran as one task |

| Check | Passes when |
|---|---|
| Data verified against the manifest | The row count and the checksum match |
| Distributed search identical to the driver's | Fits A and B agree bit for bit (estimates, covariance, σ̂, θ̂, deviance, ‖u‖², provider table) |
| R0 rerun identical | Fits A and C agree bit for bit |
| Search paths | A and C searched on the driver, B across the cluster |
| Fixed effects | Every \|(β̂ₖ − βₖ)/SE\| ≤ 4.5 |
| σ̂_α and σ̂ | \|z\| ≤ 4.5 against 0.5 and 1, with the large-sample SDs σ_γ/√(2m) and σ/√(2(n − m − p)) |
| Coverage | The 95% `alpha` intervals cover the true γⱼ at 0.95 ± 2.576·√(0.95·0.05/m) |

Beyond the report, S0's criteria ask for two judgments:
- that the driver's peak heap stays well below its memory (it should not depend on n);
- that no stage whose work grows with n runs as one task. The report lists the single-task stages by name; collecting
  small results and writing one-file outputs (the manifest, the report) are expected among them.

Timings are recorded; targets wait for S2 (plan §9).

## Running it

Build the jars from the commit under test, then submit the runner with the numerics and engine jars:

```bash
sbt numerics/package engine/package bench/package
spark-submit --class pprof.spark.bench.S1LinearRE \
  --jars numerics/target/scala-2.13/pprof-spark-numerics_2.13-<version>.jar,engine/target/scala-2.13/pprof-spark-engine_2.13-<version>.jar \
  bench/target/scala-2.13/pprof-spark-bench_2.13-<version>.jar \
  --path <a storage path for the point, e.g. dbfs:/pprof-spark/s1/linear-re-n1e7>
```

The cluster is a small multi-node cluster with a Classic Spark 4.1 session (Databricks Runtime 18, not serverless),
as S0's. Options: `--rows`, `--providers`, `--features`, `--seed` change the point (the defaults are the point above);
`--budget` sets fit B's driver budget in bytes; `--report <dir>` writes the report elsewhere. A second submission reuses
the generated data, and the data can be deleted after the run (plan §10).

What to send back: the report line; if a check fails or the run fails, the driver log; and, if a single-task stage
looks like row work, its name and time from the Spark UI.

## Results

Not yet run.
