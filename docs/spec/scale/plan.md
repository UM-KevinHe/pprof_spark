# Plan: the package-level scale test (D-28)

- Status: **Draft**, proposed in round 80. D-50 (round 91) fixed its phases (section 9) and milestones (section
  11): S0 now, then S2 (Stage A) and S3 (Stage B) after inference parity and the random-effect models. The rest
  awaits the maintainer's approval (section 12), which takes the next free decision number.
- Decision references: D-04 (the design envelope; performance targets after the first benchmarks),
  D-14 (assistants never access the maintainer's Databricks workspace), D-28 (the maintainer runs the
  scale test once the package is done), D-50 (the phases of section 9 and the milestones of section 11).
- Rules served: §9.8 (a feature becomes stable only when its scale test meets the target), PERF-1 (the
  Cox crossover analysis and decision guide, still owed), test layers T9 (cluster integration) and T10
  (scale and performance).
- Open items measured: OI-02, OI-03, OI-29, OI-30, OI-38, OI-43, OI-45, OI-50, OI-57, OI-59, OI-68, OI-70,
  OI-76 (§8, §9.1).

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
| Self-contained (D-14, D-50) | The kit runs as a Spark application on any Spark 4.1 cluster, built from a commit of the repository; assistants never touch the maintainer's cluster, and nothing needs paid GitHub resources |
| Recorded context | Every result carries the software versions (`SoftwareInfo`), the runtime's build identifier where the platform has one (OI-06), the cluster shape and the Spark configuration |
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
JVMs and real serialization.

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
| Three-stage SRR | Provider × cluster cells as the three-stage specification describes | S0's A10 and A11 (section 9.1); the rest in a second wave (section 12) |

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

`ScaleJob` in the `bench` module runs as a Spark application, or from the Python package, on any Spark 4.1
cluster (D-50). It takes a plan file (the runs: data path, model, specification, options, Spark configuration,
operations, repeats) and a results path. For every run and operation it records:

| Measure | Source |
|---|---|
| Wall time, per operation and per phase (validation, sizes, build, summary, passes, inference, outputs) | Driver timers; stage names carry their call sites (for example `collect at CoxPH.scala:452`), which map to phases |
| Spark jobs, stages and tasks; task time median, 95th percentile and maximum, and the straggler ratio (maximum over median) per stage | A Spark listener |
| Shuffle bytes written and read; bytes returned to the driver; spilled bytes; peak execution memory per task; GC time | Task metrics |
| Driver heap: peak and after each operation | A sampling thread on the driver's memory bean |
| Layout, Newton steps, halvings, passes, convergence, warnings | The fit objects |
| Correctness | The output hash (R0), and z-scores and coverage against the manifest's true values |

Each row goes to a results table (Parquet, append-only, never overwriting) and, as JSON, to the driver
log. The listener needs a Classic session, not Spark Connect; the runbook says so.

## 7. Kernel benchmarks (OI-38, and L-01's cost question)

The S-05 harness (`DeterminismCost`, run by `bench.yml`) gains the production kernels on in-memory blocks
of 10⁵ rows at p = 10, 50 and 100 — the Cox risk-set sweep and the logistic passes A and B — with two
bench-only variants of each: plain sums, and a fused pairwise cascade. It runs on GitHub's runners and on
one worker of the test cluster. Together with the share of pass time spent in the kernel (from the listener), this
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

## 9. Phases (D-50)

D-50 runs the test in four phases. Architectural problems then surface before more models are built on them,
and targets wait until the implementation is complete enough to keep them.

| Phase | When | Content | Passes when |
|---|---|---|---|
| S0, architecture checks | Now (milestone M1) | The points of section 9.1, on a small multi-node cluster | The criteria of section 9.1 |
| S1, scale smoke | With each new model or feature | One point near 10⁷ rows with S0's checks, as part of the feature's definition of done | As S0 |
| S2, calibration (Stage A) | After D-50's P0 and P1: inference parity and the random-effect models | Tier P1 of section 5, and P2 if the budget allows; the kernel benchmarks of section 7; the crossover with pprof_py. The kit's report gives, per point and operation, times, resources and the five checks; from it the maintainer sets targets per feature (for example, a fit at C2 within a stated time on a stated cluster, a driver heap below a stated size, every check passing), and the decisions of section 8 are proposed | The targets are set |
| S3, verification (Stage B) | After S2's fixes | The P1 points against the targets | A feature that meets them becomes **scale-verified** in `docs/parity/matrix.md`, and **stable**, losing its `Experimental` marker, when the other conditions of §9.8 also hold |

Before S2, only two kinds of problem are fixed:
- Problems of complexity class: time growing faster than n, driver memory growing with n or m, a stage that
  cannot use more than one task, data expansion.
- Costs in the infrastructure every model shares: working sets, jobs per pass, provider tables.

Kernel tuning (OI-38), tuning parameters and absolute targets wait for S2.

### 9.1 S0: architecture checks

| Point | Models | n | m | p | Sizes | Measures |
|---|---|---|---|---|---|---|
| A1 to A3 | Cox (stratified), logistic, linear | 10⁶, 10⁷, 10⁸ | n/10³ | 20 | equal | Time and driver heap against n |
| A4, A5 | Cox (stratified), logistic, linear | 10⁷ | 10⁵, 10⁶ | 20 | equal | Driver heap and time against m (OI-29, OI-50) |
| A6, A7 | Cox, logistic | 10⁷ | 10⁴ | 50, 150 | equal | Partials and block growth against p (OI-02, L5) |
| A8 | Cox, logistic | 10⁷ | 10⁴ | 20 | dominant | One provider with 5% of rows (OI-30, OI-57) |
| A9 | Cox, unstratified | 10⁶ | 1 | 20 | — | One stratum: tasks per pass (OI-03, OI-30) |
| A10, A11 | Three-stage | 10⁶ | 10³, 10⁴ | 20 | equal | Stage 2's time and memory against the number of levels (OI-59) |
| A12 | Cox, logistic | 5 × 10⁶ | 5 × 10³ | 60 | equal | Time by phase, against pprof_py on one machine (OI-76) |

A2 and A12 also run each model's downstream operations, as C2 does (section 5). A2 reruns with a different
number of partitions and of executors (R0). The checks of section 3 apply to every point; sample parity runs
at A2.

S0 passes when:
- Time grows near-linearly in n: a log–log slope of at most 1.2 from A1 to A3, on a fixed cluster.
- The driver's peak heap does not grow with n, and at m = 10⁶ it stays below half of the driver's memory.
- No stage runs as a single task, except A9's by design, whose cost A9 measures.
- A2's outputs are bit-identical across partitionings.
- A12's time is attributed to phases. The attribution names which of OI-38, OI-68 and OI-70 explains
  production's ratio to pprof_py (OI-76).

S0's report, under `docs/parity/scale/`, lists each complexity problem found and the shared-infrastructure fix
proposed. Fixes follow the specification first (NN-2) and leave every parity test unchanged.

## 10. The runbook

The kit runs on any Spark 4.1 cluster with a Classic session (D-50), as a Spark application or from the Python
package, built from a commit of the repository. A runbook in `docs/guide/` gives the steps:
1. Generate a plan's data.
2. Run its points.
3. Export the results table and, for failures, the driver log.
4. Delete the generated data.

Round 81's notebooks (`databricks/`, [docs/guide/databricks.md](../../guide/databricks.md)) are one way to run
the kit in the maintainer's environment; they are outside this plan's deliverables. Results come back to the
repository as aggregate tables under `docs/parity/scale/`.

## 11. Deliverables and milestones

D-50's milestones order the work; this plan no longer assigns round numbers.

| Milestone | Content | Verified by |
|---|---|---|
| Done (round 81) | Notebooks for the maintainer's environment: `databricks/00_build`, `01_parity` and `02_scale` | The maintainer's runs |
| M1 | The generators of section 4 for the Cox, logistic, linear and three-stage models, with manifests, tests of determinism across partitionings and of the distributions' moments, and a Python entry point; `ScaleJob` with the measurements of section 6 (the listener, phase attribution, driver heap, output hashes, recovery checks, the results table) as a Spark application; S0's plan file and a local dry run of it at n ≈ 10⁵ | Sandbox; CI |
| M1, run by the maintainer | S0 on a small multi-node cluster (section 12, item 7) | The maintainer |
| M1 and M3 | S0's report under `docs/parity/scale/`; fixes for the complexity problems it finds | Rerunning S0 |
| With each feature from M2 on | S1's scale smoke | Rerunning S1 |
| Before M5 | The plan for section 5's full grid, the kernel benchmarks of section 7, and a dry run of tier P1 with node-hour estimates | Sandbox |
| M5 | S2 (Stage A) on the maintainer's cluster; its report; targets; the decisions of section 8; then S3 (Stage B) | The maintainer |

## 12. Decisions requested

1. Approve this plan, or amend it, under the next free decision number (D-49 went to the production-validation
   fixes, D-50 to the roadmap that fixed sections 9 and 11).
2. The cluster budget: the worker type and the largest number of workers available. The grid assumes up
   to 64 workers for C2's strong scaling and enough for C3 at 10⁹ rows; with less, C3 moves to tier P3.
3. Trim or confirm the tiers of section 5.
4. The three-stage SRR model in a second wave, after Stage A shows the fixed-effect models' costs
   (proposed), or in the first.
5. Targets set from Stage A's report, as D-04 intended (proposed).
6. Results returned as aggregate tables under `docs/parity/scale/` (proposed).
7. For S0: a small multi-node cluster, 4 to 8 workers of one type (preferably item 2's type), and one machine
   for pprof_py at A12's size.

Items 1 to 6 are needed before S2; item 7 before S0.

Round 90's memo, [plan-decisions.md](plan-decisions.md), recommends an answer to items 1 to 6, in light of
the production validation.
