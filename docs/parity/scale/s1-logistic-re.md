# S1 scale smoke: logistic random effects (part 4b)

- Status: prepared in round 122 (2026-11-05); to be run by the maintainer on his cluster, results below.
- Plan: `docs/spec/scale/plan.md` §3 (the checks) and §9 (S1: one point near 10⁷ rows with S0's checks); D-50,
  D-51 item 1. Part 4a's smoke is `s1-linear-re.md`; both wait for the maintainer's run (OI-97, OI-100).
- Runner: `pprof.spark.bench.S1LogisticRE` (bench module), tested on a small point by `S1LogisticRESuite`.

## The point

`bench.synthetic`'s logistic data (plan §4), as part 4a's point: n = 10⁷ rows among m = 10⁴ providers of equal size
and p = 20 features, with seed 79. Outcomes are Bernoulli, and the provider effects are γⱼ = −1 + 0.5·Zⱼ. The model is
the default fit: the Laplace approximation (nAGQ = 1), an intercept, no offset.

## What the runner does

1. Generates the data under `--path` unless the manifest is already there, and verifies them against it.
2. Fits the model (fit A). Then the Wald tests, option `alpha`, the indirect and direct measures, and their SM
   ratio intervals.
3. Fits again on the input repartitioned by `id`, with `spark.sql.shuffle.partitions` doubled plus one (fit C, R0).
4. With `--layout true`, fits again with blocks half the default size (fit B).
5. Writes one line of JSON under `--path/reports/<time>` and prints it.

The count tests (`poibin_exact`, `resampling`) and σ's profile are left out. At this size each takes some hundreds
of Spark jobs; S2 measures them (OI-99).

| Check | Passes when |
|---|---|
| Data verified against the manifest | The row count and the checksum match |
| The Laplace search converged | The projected gradient met its tolerance |
| R0 rerun identical | Fits A and C agree bit for bit: estimates, covariance, σ̂, the deviance, ‖u‖² and the provider table |
| Fixed effects | Every \|(β̂ₖ − βₖ)/SE\| ≤ 4.5, with the intercept's truth −1 |
| σ̂ | \|z\| ≤ 4.5 against 0.5, with σ_γ/√(2m) as its SD; an approximation, since the BLUPs of 1,000 records are precise |
| Coverage | The 95% `alpha` intervals cover γⱼ + 1 at 0.95 ± 2.576·√(0.95·0.05/m) |
| Half-size blocks (with `--layout true`) | Fits A and B agree within 10⁻¹⁰ relative (T-part) |

As for part 4a, beyond the report:
- the driver's peak heap should stay well below its memory;
- no stage whose work grows with n should run as one task. The report lists the single-task stages by name.

Timings are recorded; targets wait for S2. A fit at this size runs about 200 Spark jobs, one per evaluation (OI-99).

## Running it

```bash
sbt numerics/package engine/package bench/package
spark-submit --class pprof.spark.bench.S1LogisticRE \
  --jars numerics/target/scala-2.13/pprof-spark-numerics_2.13-<version>.jar,engine/target/scala-2.13/pprof-spark-engine_2.13-<version>.jar \
  bench/target/scala-2.13/pprof-spark-bench_2.13-<version>.jar \
  --path <a storage path for the point, e.g. dbfs:/pprof-spark/s1/logistic-re-n1e7> --layout true
```

Use a small multi-node cluster with a Classic Spark 4.1 session (Databricks Runtime 18, not serverless), as S0 and part
4a's smoke do. `--rows`, `--providers`, `--features` and `--seed` change the point. A second submission reuses the
generated data.

What to send back: the report line. If a check fails or the run fails, also the driver log. If a single-task stage
looks like row work, its name and time from the Spark UI.

## Results

Not yet run.
