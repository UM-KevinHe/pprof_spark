# Plan: the package-level scale test (D-28)

- Status: **Draft**, proposed in round 80 for the maintainer's approval (to become a decision when approved; D-49 went to the production-validation fixes).
- Decision references: D-04 (the design envelope; performance targets after the first benchmarks),
  D-14 (assistants never access the maintainer's Databricks workspace), D-28 (the maintainer runs the
  scale test once the package is done).
- Rules served: §9.8 (a feature becomes stable only when its scale test meets the target), PERF-1 (the
  Cox crossover analysis and decision guide, still owed), test layers T9 (cluster integration) and T10
  (scale and performance).
- Open items measured: OI-02, OI-03, OI-30, OI-38, OI-43, OI-45, OI-50, OI-57, OI-68, OI-70 (§8).

## 1. Purpose

Every feature of the package is parity-verified on small fixtures in CI and `Experimental` until its
scale test meets a target (§9.8). This plan defines that test and the kit that runs it. The test
answers four questions:

1. **Does each feature hold up on a real multi-node cluster at the sizes of the design envelope?**
   §2.3 plans for n = 10⁷ to 10⁹ rows, m = 10² to 10⁶ providers, p = 10 to 100 covariates, and up to
   10⁴ event times per stratum. Holding up means the same results as at small scale, within the
   cluster's resources.
2. **What should the performance targets be?** §2.3 and D-04 deliberately left them until the first
   benchmarks.
3. **What do the open "measure first" items actually cost?** Section 8 lists them, each with a proposed
   decision rule.
4. **Where is single-node pprof_py faster?** This is PERF-1's crossover analysis, with a decision guide
   for users.

It does not replace simulation studies of bias and coverage for features without an external reference
(T11), or validation on production data inside the approved environment (T12).

## 2. Principles

| Principle | Consequence |
|---|---|
| Synthetic data only | No protected data anywhere in the test; generators run on the cluster; results leave the workspace as aggregate tables |
| Deterministic generation | Every generated value is a function of (seed, row identifier, stream) through the counter-based SplitMix64 generator the bootstrap tests already use (STAT-2), so any point is identical on any cluster and partitioning and can be regenerated exactly |
| Measurement only | The kit calls the public API and the existing job runners; it adds no hooks to the engine and changes no statistical code; it observes through Spark listener events and the job runners' run records |
| Self-contained (D-14) | The notebooks in `databricks/` and [docs/guide/databricks.md](../../guide/databricks.md) (round 81) are all the maintainer needs: they build pprof_spark on his cluster, install it from workspace files and run the checks; assistants never touch the workspace, and nothing needs paid GitHub resources |
| Recorded context | Every result carries the software versions (`SoftwareInfo`), the Databricks Runtime build identifier (OI-06), the cluster shape and the Spark configuration |
| Two stages | A calibration run sets the targets (D-04); a verification run checks them (§9.8) |

## 3. Correctness at scale

No reference implementation can fit 10⁸ rows, so correctness at scale rests on five checks, each of
which can fail:

| Check | How | Passes when |
|---|---|---|
| Bitwise reproducibility (R0) on a real cluster | Selected points run twice, with a different `spark.sql.shuffle.partitions`, a different number of executors, and the input read in a different order | An order-independent hash of every output double is identical |
| Layout invariance (R1) | One point per model with half and with twice the default block size | Within T-part |
| Recovery of the generating parameters | Data come from known β and γ (section 4); for every coefficient, z = (β̂ − β)/SE | Every \|z\| ≤ 4.5 across the grid (with about 2,000 coefficients, the chance of a false alarm is about 1.4%); the 95% intervals of non-degenerate providers cover the true γⱼ at a rate within binomial limits of 95% |
| Sample parity with pprof_py on the cluster path | A sample of 10⁵ to 10⁶ rows of a grid point is fitted by pprof_py v0.7.0 on the driver and by pprof_spark across the executors | The fixtures' tolerance classes (T-coef, T-var, T-test, T-meas) |
| Guards | Points just beyond `maxStratumBytes`, `maxProvidersOnDriver` and the driver budget for partials | Each fails before any row shuffle, with the documented message |

Sample parity is the first T9 evidence: the same comparison as CI's, but with executors in separate
JVMs, real serialization and the Databricks runtime.

## 4. Synthetic data

The generators live in the `bench` module (the home of T10 workloads), run as a Spark job over
`spark.range(n)`, and write each grid point once as Parquet files of about 128 MB with a manifest:
generator, version, seed, parameters, true values, row counts and a checksum (a sum of row hashes) that
proves a regenerated table is identical. Model runs then read the tables, so generation time never
mixes with model time.

| Ingredient | Generation |
|---|---|
| Providers | Sizes from a named distribution: `equal`; `zipf` (size proportional to rank^−1.1, the largest about 1% of rows); `dominant` (one provider with 5% of rows, the rest equal). Provider j owns a contiguous range of row identifiers, from cumulative sizes computed on the driver (m ≤ 10⁶ values) |
| Covariates | p features: odd-numbered binary with P(1) = 0.3, even-numbered standard normal rounded to a 1/64 grid; true βₖ = (−1)ᵏ·0.5/√p, so the variance of η stays bounded as p grows |
| Normal draws | Box–Muller from two counter-based uniforms, with `StrictMath` |

| Model | Generating process | Options |
|---|---|---|
| Cox | Weibull baseline with shape 1.2 and a stratum scale λⱼ = λ̄·exp(0.3·Zⱼ); event time T = (−log U / (λⱼe^η))^(1/1.2) years; censoring C uniform on (0, 5) years plus administrative censoring at 5; time = ⌈365·min(T, C, 5)⌉ days, so ties occur at daily resolution and a stratum has at most 1,826 distinct times | Left truncation for a share of rows (entry = ⌊U·time⌋); integer weights 1 to 3; offsets 0.1·x₁; patient clusters of 1 to 5 rows splitting one follow-up into intervals |
| Logistic fixed effects | γⱼ = γ̄ + σ_γ·Zⱼ; y ~ Bernoulli(σ(γⱼ + xᵀβ)) | Binomial rows with 1 to 20 trials; patient clusters |
| Linear fixed effects | y = γⱼ + xᵀβ + σε, ε standard normal | — |
| Three-stage SRR | Provider × cluster cells as the three-stage specification describes | Second wave (section 12) |

## 5. The grid

Points are grouped in tiers so the maintainer can trim to budget: **P1** must run in Stage A, **P2**
should, **P3** could.

| Point | Tier | Models | n | m | p | Sizes | Purpose |
|---|---|---|---|---|---|---|---|
| C1 | P1 | Cox (stratified), logistic, linear | 10⁷ | 10⁴ | 20 | equal | Core path |
| C2 | P1 | Cox (stratified), logistic, linear | 10⁸ | 10⁵ | 20 | equal | Core path; most operations and checks |
| C3 | P2 | Cox (stratified), logistic, linear | 10⁹ | 10⁶ | 20 | equal | Top of the envelope |
| W1, W2, W3 | P1 (W3), P2 | Cox, logistic | 10⁸ | 10⁵ | 10, 50, 100 | equal | Width: compute O(n·p²), partial volume, the driver budget (OI-02) |
| S1 | P1 | Cox, logistic | 10⁸ | 10⁵ | 20 | zipf | Skew: stragglers, oversized groups |
| S2 | P2 | Cox, logistic | 10⁸ | 10⁵ | 20 | dominant | One group of 5 × 10⁶ rows |
| U1, U2 | P1, P2 | Cox, unstratified | 10⁶, 10⁷ | 1 | 20 | — | One task per pass: the default `maxStratumBytes` budget, then the budget raised (OI-03, OI-30) |
| T1 | P2 | Cox | 10⁸ | 10⁵ | 20 | equal | 30% left-truncated rows, Efron ties, weights, offsets |
| R1 | P2 | Cox | 10⁸ | 10⁵ | 20 | equal | Patient clusters and clustered robust variance (OI-45) |
| L1 | P1 | Logistic | 10⁸ | 10⁶ | 20 | equal | γ on the driver near the envelope's m (OI-50) |
| L2 | P2 | Logistic | 10⁸ | 10⁵ | 20 | zipf | Exact provider tests and their limits on skewed sizes (OI-57) |
| X1, X2, X3 | P1 | Cox (stratified), logistic | 10⁵, 10⁶, 10⁷ | n/10³ | 20 | equal | Crossover with pprof_py (PERF-1) |

Operations per point: every point fits; C2, S1 and L1 also run each model's downstream calls (Cox:
baseline, residuals, per-row robust variance, provider measures and tests; logistic: covariate Wald and
likelihood-ratio tests for two covariates, exact provider tests, indirect and direct standardization,
prediction; linear: provider tests and standardized differences); C2 also runs the three job runners
end to end, whose run records give per-stage timings.

Cluster shapes: C2 on 4, 16 and 64 workers (strong scaling); C1, C2 and C3 on workers in proportion to n
(weak scaling); X1 to X3 on 1, 4 and 16 workers, against pprof_py on the driver alone. R0 reruns: C2 and
S1 for Cox and logistic. R1 reruns: C2 with 2 MiB and 8 MiB blocks. Each timed configuration runs twice.

## 6. The runner

`ScaleJob` in the `bench` module, run on dedicated (single-user) compute from the scale notebook through the
Python package (or as a Databricks JAR task), takes a
plan file (the runs: data path, model, specification, options, Spark configuration, operations, repeats)
and a results path. For every run and operation it records:

| Measure | Source |
|---|---|
| Wall time, per operation and per phase (validation, sizes, build, summary, passes, inference, outputs) | Driver timers; stage names carry their call sites (for example `collect at CoxPH.scala:452`), which map to phases |
| Spark jobs, stages and tasks; task time median, 95th percentile and maximum, and the straggler ratio (maximum over median) per stage | A Spark listener |
| Shuffle bytes written and read; bytes returned to the driver; spilled bytes; peak execution memory per task; GC time | Task metrics |
| Driver heap: peak and after each operation | A sampling thread on the driver's memory bean |
| Layout, Newton steps, halvings, passes, convergence, warnings | The fit objects |
| Correctness | The output hash (R0), and z-scores and coverage against the manifest's true values |

Each row goes to a results table (Parquet, append-only, never overwriting) and, as JSON, to the driver
log. The listener needs a Classic session, which dedicated compute provides; the runbook says so.

## 7. Kernel benchmarks (OI-38, and L-01's cost question)

The S-05 harness (`DeterminismCost`, run by `bench.yml`) gains the production kernels on in-memory blocks
of 10⁵ rows at p = 10, 50 and 100 — the Cox risk-set sweep and the logistic passes A and B — with two
bench-only variants of each: plain sums, and a fused pairwise cascade. It runs on GitHub's runners and on
one Databricks worker. Together with the share of pass time spent in the kernel (from the listener), this
answers whether the Neumaier sums matter.

**Proposed rule:** keep Neumaier unless it costs more than 25% of a pass at p = 50. Above that, propose
the cascade as a behavioural change (NUM-2, with the fixtures re-compared).

## 8. What the measurements decide

| Item | Measured at | Proposed decision rule |
|---|---|---|
| OI-68 Cox: rebuilt working sets; tasks per pass that follow shuffle partitions | C2 job runner; C2 with shuffle partitions at the default and matched to cores | Rebuilds above 30% of a job runner's time → propose a reusable prepared working set; task mismatch above 20% of pass time → propose deterministic partitioning by block |
| OI-70 logistic: two jobs per pass; provider tests through the driver; rebuilds; no cap on a provider's block | C2, L1, S2 | Job overhead above 20% of pass time → ship γ once per step; driver memory or time of the provider-test round trip beyond the targets at m = 10⁶ → keep the table distributed |
| OI-38 Neumaier cost | Section 7 | As in section 7 |
| OI-02 large p | W3: bytes of partials per pass against the driver budget | The budget binds at p = 100 for n ≥ 10⁸ → propose reduction groups |
| OI-03, OI-30 TimeRange | U1, U2: single-task time and memory | Above the target at 10⁷ rows → schedule TimeRange |
| OI-43 prediction by stratum | Prediction for an unstratified fit at 10⁸ rows | A straggler ratio above 3 → bucket by time or broadcast the baseline |
| OI-45 large clusters | R1 | A straggler ratio above 3 → pre-aggregate within blocks |
| OI-50 γ on the driver | L1: driver heap and time per step at m = 10⁶ | Above the targets → table materialization of γ |
| OI-57 exact-test packing | L2: straggler ratio of the test pass | Above 3 → pack blocks by estimated cost |
| PERF-1 crossover | X1 to X3 | The crossover size per model and cluster shape; a decision guide in `docs/guide/` |
| §2.3 targets | Stage A as a whole | Set by the maintainer (section 9) |

## 9. Two stages

**Stage A, calibration.** Run tier P1, and P2 if the budget allows. The kit's report gives, per point and
operation, times, resources and the five checks. From it the maintainer sets targets per feature — for
example, a fit at C2 within a stated time on a stated cluster, a driver heap below a stated size, and
every check passing — and the decisions of section 8 are proposed.

**Stage B, verification.** Rerun the P1 points against the targets. A feature that meets them becomes
**scale-verified** in `docs/parity/matrix.md`; it becomes **stable**, losing its `Experimental` marker,
when the remaining conditions of §9.8 also hold.

## 10. The runbook

Round 81 delivered the workflow the runbook builds on: [docs/guide/databricks.md](../../guide/databricks.md)
and the notebooks `00_build` (builds a commit on the cluster's driver into a folder of workspace files),
`01_parity` (fixture parity and R0 on the cluster) and `02_scale` (a first look at scale: deterministic
synthetic data, timings, recovery of the true coefficients). The kit extends `02_scale` to the grid of
section 5 and the measurements of section 6, and the guide gains the steps for generating the grid's data
on S3, running the points, exporting the results table and, for failures, the driver log, and deleting the
generated data. Results come back to the repository as aggregate tables under `docs/parity/scale/`.

## 11. Deliverables and rounds

| Round | Content | Verified by |
|---|---|---|
| 81 (done) | The Databricks workflow: `databricks/00_build`, `01_parity` and `02_scale`, and docs/guide/databricks.md | Sandbox (local Spark); the maintainer's first run |
| 82 | Generators for the Cox, logistic and linear fixed-effect models, with manifests and a Python entry point; tests for determinism across partitionings, the distributions' moments against their true values, and parameter recovery at small n | Sandbox; CI |
| 83 | `ScaleJob` with Python access: the plan file, the listener, phase attribution, output hashes, recovery checks, the results table; a tiny local plan as a test | Sandbox; CI (Classic) |
| 84 | The kernel benchmarks of section 7; `02_scale` extended to the grid; a dry run of the whole P1 plan at n ≈ 10⁵ in local mode | Sandbox |
| — | Stage A on the maintainer's workspace | The maintainer |
| 85 and on | The report, `docs/parity/scale/`; proposed targets; the decisions of section 8; then Stage B | — |

## 12. Decisions requested

1. Approve this plan (D-49), or amend it.
2. The cluster budget: the worker type and the largest number of workers available. The grid assumes up
   to 64 workers for C2's strong scaling and enough for C3 at 10⁹ rows; with less, C3 moves to tier P3.
3. Trim or confirm the tiers of section 5.
4. The three-stage SRR model in a second wave, after Stage A shows the fixed-effect models' costs
   (proposed), or in the first.
5. Targets set from Stage A's report, as D-04 intended (proposed).
6. Results returned as aggregate tables under `docs/parity/scale/` (proposed).

Round 90's memo, [plan-decisions.md](plan-decisions.md), recommends an answer to each, in light of the
production validation.
