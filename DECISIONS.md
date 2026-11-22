# Decisions

Sequentially numbered and never renumbered (PROJECT_CONTEXT §0). Status values: **Proposed**
(awaiting the maintainer), **Approved** (by the maintainer), **Approved (delegated)** (the
maintainer asked for the assistant's recommendation to be applied), **Rejected**,
**Superseded**, and **Delegated** (implementation choices under delegated authority, §11.6,
reversible by the maintainer). Architecture decisions also get an ADR in docs/adr/. Recorded
decisions take precedence over PROJECT_CONTEXT.md (§0), whose preface lists the amendments in
force.

## Phase 0 decisions (§16)

| ID | Decision | Status | Outcome or recommendation |
|---|---|---|---|
| D-01 | Supported compute tiers for v1 | Approved (delegated), 2026-10-06 | Classic Spark only; `engine` kept Connect-compatible (PLAT-2) and tested under Spark Connect (T8) |
| D-02 | Target and secondary runtimes | Approved (delegated), 2026-10-06 | Spark 4.1.0, Scala 2.13.16, Java 17 bytecode: one JAR for open-source Spark 4.1.x and Databricks Runtime 18 LTS; tested on open-source Spark only (D-14); amended by D-76 (2026-11-16): Spark 4.0.x and Databricks Runtime 17.3 LTS are supported too, and CI runs the JARs on PySpark 4.0.0 |
| D-03 | Python access in v1 | Superseded by D-31, 2026-10-07 | Was: none in v1; revisit after Phase 1d. Round 25 first recorded the maintainer's "Continue" as no change; he then chose py4j wrappers (D-31) |
| D-04 | Design envelope and performance targets | Approved (delegated), 2026-10-06 | §2.3 ranges as planning assumptions; targets set after the first benchmarks; OI-02 resolved before large-p work |
| D-05 | pprof_py pin and reference versions | Approved (delegated), 2026-10-05 | pprof_py v0.7.0, commit 9320766 (see below) |
| D-06 | Default Cox tie method | Approved (delegated), 2026-10-06 | Breslow, the reference default; every fit records its tie method |
| D-07 | Project license | Approved, 2026-10-05 | MIT, copyright holder Kevin He |
| D-08 | Artifact distribution | Approved (delegated), 2026-10-06 | GitHub Releases |
| D-09 | Tolerance calibration | Approved (delegated), 2026-10-06 | §8.4 values, an element-wise rule scaled by each quantity's largest element, calibrated against the Cox fixtures and enforced in CI (ADR-0005) |
| D-10 | Root package and artifact names | Approved, 2026-10-05 | Organization and root package `pprof.spark`; artifacts `pprof-spark-<module>_2.13` |

### D-05: what the pin is, and the choice

pprof_spark is a standalone Spark package: it does not depend on pprof_py at build time or at
run time. pprof_py matters only to the test suite. Parity tests (§9.2) compare pprof_spark's
results with pprof_py's results on the same synthetic data, and the pin fixes the exact pprof_py
version that produces those reference results. Test expectations therefore change only through
a deliberate re-pin (PAR-1), never because pprof_py moved on.

Chosen: pprof_py v0.7.0, its current release (tag object 9849f6c, commit
9320766e35e5b385d596a2b75418192098a124c4, 2026-10-03), recorded in reference/REFERENCE.lock. It
is a tagged release; its Cox model code is identical to v0.5.0; and it already contains the
provider-test and standardization changes that Phase 1d needs. The fixture pipeline establishes
which parts of its validation suite pass (OI-12).

## Decisions raised in rounds 1 to 3

### D-11: Phase 0 exit criterion and the first Cox slice (Approved, delegated, 2026-10-05)

(a) Phase 0 exits on a statistics-free platform skeleton: data contract, logical blocks, a toy
kernel, block-ordered reduction, a result table and persisted metadata, green in CI under
Classic Spark and under Spark Connect. The Databricks leg of the original proposal is dropped
(D-14). (b) The first Cox slice opens Phase 1a: stratified (StratumLocal), right-censored,
Breslow, with model-based variance; unstratified data is the one-stratum case while it fits in
one block, and TimeRange follows within Phase 1a or 1b. Stratified first exercises multi-block
reduction and bin packing, matches SMR stage 1, and avoids starting with the hardest layout
(OI-03).

### D-13: Spark Connect topology for test layer T8 (Approved, delegated, 2026-10-05; ADR-0002)

The T8 suites run through the Scala Spark Connect client against a Connect server started inside
the test JVM on the local Classic session, with the Connect client jar pinned to the end of Test
classpaths. Alternatives considered: a separate server JVM, and Databricks only. Evidence and
limits are in ADR-0002.

### D-14: No Databricks deployment work (Approved by the maintainer, 2026-10-05)

**Clarified by the maintainer on 2026-10-06:** "no Databricks work" means that AI assistants (Claude
Code included) must not access the maintainer's Databricks environment. It does not rule out running
pprof_spark on Databricks: the maintainer tests it there himself, starting with the Phase 1d scale
test (D-28). pprof_spark stays a standalone Apache Spark package, compatible with Databricks Runtime
18 LTS by construction (§5.3).

pprof_spark is developed and tested as a standalone Apache Spark package. No Databricks bundles,
workspace CI, job clusters or Unity Catalog artifacts until the maintainer reopens the topic.

- Phase 0 no longer requires a CI-deployed JAR running on a DBR job cluster (§4).
- Deferred: S-01 (runtime linkage on DBR), the Databricks legs of S-02, S-03 (table
  materialization on serverless), S-07 (envelope-scale block sizes on a cluster), test layers
  T9 and T10, the job-based Python access of §6.12, and the Databricks parts of D-01 to D-03
  and D-08. The `app` module targets plain Spark entry points.
- Kept: the build pins (Spark 4.1.0, Scala 2.13.16, Java 17 bytecode) and the linkage compile,
  which keep one JAR usable on open-source Spark 4.1.x and on DBR 18 LTS at no extra cost; and
  Spark Connect compatibility (PLAT-2), which any Spark Connect deployment needs.

### D-15: Deterministic summation algorithms (Delegated, 2026-10-05; ADR-0003)

Pairwise (cascade) summation with sequential leaves of at most 32 values within a block, and
Neumaier compensation for partials combined in block order, as specified in ADR-0003. These
algorithms fix the bits of every reduction; changing them is a behavioral change (NUM-2).

### D-16: Platform skeleton design (Delegated, 2026-10-05; ADR-0004)

The data contract, layout plan, working set, kernels, ordered reduction, result tables and
persistence format of ADR-0004, which every model family reuses.

### D-17: Reference fixtures and calibration (Approved, delegated, 2026-10-06; ADR-0005)

Exact synthetic inputs generated once (CSV), reference outputs from pprof_py v0.7.0 and R 4.3.3
with survival 3.5-8 as hexadecimal doubles (JSON), a checksummed manifest, stored negative
controls, and calibration enforced in CI by `FixturesSuite`.

### D-18: Deterministic mode stays the default (Approved, delegated, 2026-10-06; ADR-0006)

Spike S-05 measured the cost of `StrictMath` and ordered sums; in a Cox-shaped kernel, sums built
with a fused pairwise cascade make determinism nearly free. There is no fast mode for now.

### D-19: Codespaces development container (Approved, delegated, 2026-10-06; ADR-0007)

`.devcontainer/` provides JDK 21, the pinned sbt and Metals; CI remains the source of evidence.

### D-80: The Cox weighted-Gram information, performance step 3's specification (Accepted: approved by the maintainer, 2026-11-22)

Proposed in round 183 and approved as written on 2026-11-22: `docs/spec/cox/weighted-gram.md`.
- **The identity.** I(β) = Σᵢ ωᵢxᵢxᵢᵀ − Σₜ Σₛ aₜm̄ₜₛm̄ₜₛᵀ, with ωᵢ = rᵢ(Λᵢ − ψᵢ). Λᵢ is the cumulative hazard over row i's
  at-risk interval, and ψᵢ the Efron correction of tied events, as the martingale residuals compute them. ℓ and U are
  unchanged.
- **The algorithm.** The present sweep keeps S₀ and S₁ but no S₂, and adds the rank-one terms to a chunked sum M. A
  second, in-memory pass adds ωᵢxᵢxᵢᵀ to a chunked sum G, and I = G − M. Λᵢ is a compensated difference of the
  cumulative hazard's sum and compensation, so late entries keep their accuracy.
- **A new fixture case.** `lt-heavy-ties`: Efron ties with truncation, with entries on event times.
- **Validation.** Exact references; round 182's S₂ kernel as a test reference; the fixtures; the golden runs
  re-baselined under D-78 and D-79.
- **Rounds.** The fixture, then the kernel with the specification amendments, then the cluster's measurement.
- **Measured** in the sandbox probe at 180 features: 1.1 times faster for Breslow and 2.1 for Efron; 1.5 and 2.4 with
  40% of the rows truncated. Agreement with round 182's kernel was 3.0·10⁻¹⁶ to 4.5·10⁻¹⁶, scaled.

### D-79: How D-78 applies to the golden runs (Accepted: approved by the maintainer, 2026-11-21)

Proposed with step 2's design note (`docs/spec/performance/step2-chunks.md` §6). D-78
names log-likelihoods, scores and information at fixed β, converged estimates and covariances, and counts and flags.
The golden runs record nothing at a fixed β, and they record quantities D-78 does not name. Proposed:
- **Values at fixed points.** Before the first change of summation order, a bit-preserving round adds to each golden
  run the values the change touches, at two points: the fit's start (Cox: β = 0; logistic: SerBIN's start, every γ
  at logit(ȳ) and β = 0) and the run's recorded estimates.
  - Cox: the log-likelihood, score and information of one evaluation.
  - Logistic: pass A's Schur complement, right-hand side, log-likelihood and score term; the variance pass's Schur
    complement; and the merged co-moments.
  - A re-baselining round evaluates the new code at the previous manifest's points. These values are D-78's
    log-likelihoods, scores and information at fixed β, compared within 1·10⁻¹³ scaled; the logistic Schur terms
    stand for that model's information and score.
- **Converged outputs D-78 does not name.**
  - Standard errors, Cox's information at the estimates, and the provider effects and their two variances: within
    T-part's bound for parameters (1·10⁻¹⁰), as estimates and covariances are.
  - The converged log-likelihood: within T-part's bound for the objective (1·10⁻¹²).
  - Each is scaled by its largest element (D-09).
- **Counts and flags.**
  - `at_bound` is a flag, and must be identical.
  - The AUC is a ratio of integer counts from an exact sort. It must be identical, or any difference is explained in
    the evidence log and approved, as for iteration counts.

Approved as proposed on 2026-11-21, with step 2's design note. It applies from step 2's round C, which pins the fixed
points. Round 181 pinned them (step2-chunks.md §11).

### D-78: The re-baselining rule for changes of summation order, from step 2 onward (Accepted: approved by the maintainer, 2026-11-21)

Plan §6.3's rule, in force from performance step 2 onward. As §6.3 states it:

Some changes reorder floating-point sums without changing what is summed: chunked accumulation, the weighted-Gram
identity, per-time aggregates, and groups with G > 1. Such a change is admitted when the round that makes it shows
all four of the following:
1. **Fixtures.** Every fixture comparison passes in its tolerance class, with tolerances unchanged.
2. **Old manifest against new:**
   - log-likelihoods, scores and information at fixed β agree within 1·10⁻¹³ scaled, a tenth of T-fn;
   - converged estimates and covariances agree within T-part (1·10⁻¹⁰);
   - iteration counts, halving counts and flags are identical. A difference in counts is explained in the evidence
     log and approved.
3. **R0 for the new code.** Bits are identical across partition counts and repeated runs, with the one-ulp
   negative control.
4. **Re-baselining.** The new manifest, written with `PPROF_GOLDEN_WRITE`, replaces the old in the same commit, and the evidence log reports the
   largest difference for each quantity.

- **The approval.** The maintainer approved D-78 on 2026-11-21 as "plan §6.3's re-baselining rule for changes to
  summation order, adopted from step 2 onward", before its text was written. Round 177 records it as §6.3's rule,
  word for word.
- **D-75.** D-75 had approved the plan with this rule while §6.3 was still headed "proposed"; §6.3 now cites D-78.
  §6.3's paragraph on changes of iterates (step 5) is outside D-78 and stands as D-75 approved it.
- **Beyond §6.3.** How the rule applies to the golden runs' quantities that it does not name is proposed separately,
  as D-79.

### D-77: The working set is cached off-heap by default (Accepted: approved by the maintainer, 2026-11-19; amended at his direction, 2026-11-20)

- **The change.** `BlockOptions.storageLevel` defaults to `OFF_HEAP` instead of `MEMORY_AND_DISK`, for every model's
  working set:
  - where off-heap memory is enabled, as on Databricks clusters with Photon, the working set stays in memory outside
    the JVM heap;
  - where it is off, Spark's default, it falls back to disk (sandbox and `PerfJobSuite`, round 165);
  - it never takes the JVM heap.

  The first proposal, disk-only, was held at the maintainer's request (2026-11-18) until paired runs could show the
  cost for data that fit in memory.
- **Evidence: paired sweeps.** Round 163's build, 2026-11-19; `perf-results.md`, "storage levels paired". Each point
  ran under all three levels back to back on the same data.

  Fit seconds:

  | Point | `MEMORY_AND_DISK` | `DISK_ONLY` | `OFF_HEAP` |
  |---|---|---|---|
  | Logistic, 1M rows, 180 features | 117.5 | 81.6 | 81.8 |
  | Logistic, 5M, 180 | 263.4 | 212.4 | 209.8 |
  | Logistic, 5M, 20 | 20.6 | 18.4 | 18.7 |
  | Logistic, 20M, 180 | 449.6 | 399.6 | 393.2 |
  | Cox, 5M, 180 | 148.1 | 153.0 | 146.2 |
  | Cox, 20M, 180 | 374.2 | 330.5 | 314.4 |

  - Off-heap was fastest, or within 1.4% of the fastest, at every point.
  - Memory-and-disk was slowest at five of six, with 2.7 to 3.1 times the GC at 20M rows, and within 1.3% at the
    sixth.
  - At 50M rows memory-and-disk failed for both models (round 160's build), while disk-only and off-heap fit.
  - The log-likelihood bits matched across levels in all eighteen paired runs.
- **Why.** The production executors have 2.2 GB heaps. A heap cache stores blocks as objects at about 3.7 times
  their serialized size and competes with the passes for the heap.
- **Caveats.**
  - Memory-and-disk ran first in each sweep and may carry first-run costs.
  - On a shared cluster, off-heap storage shares memory with other work. Cached blocks may then be evicted to disk,
    which slows a fit but cannot change its result.
  - A fit can still choose any storage level.

Proposed in round 162 (disk-only), held in round 163, revised in round 168, and approved as revised on 2026-11-19.
Implemented in round 169:
- `BlockOptions.storageLevel` defaults to `BlockOptions.DefaultStorageLevel`, `OFF_HEAP`. The job specifications,
  `PythonApi` and the Python wrappers take their default from `BlockOptions()`. Fits saved by earlier builds keep the
  level they recorded.
- PROJECT_CONTEXT v2.11 §10.3, the Python guide, the runbook, `03_perf` and the Cox tutorials state the default.
  `PerfJob`'s report records the level each fit used (`fit.storage_level`).
- Evidence (assistant sandbox, local mode, where off-heap memory is off): the 40 golden runs are bit-identical under
  the disk fallback, and `PerfJobSuite` finds the default's cache wholly on disk, with the same log-likelihood bits as
  `MEMORY_AND_DISK`.
- Not foreseen: where off-heap memory is off, Spark logs three warnings for each cached partition as it falls back
  to disk, 684 lines in `GoldenRunsSuite` under CI's log settings. Results are unaffected. OI-121 holds the options.

Amendment (round 170, 2026-11-20, at the maintainer's direction; OI-121): where the session has no off-heap memory,
the engine persists at `DISK_ONLY` itself, so Spark no longer tries memory first and warns.
- **What counts as off-heap memory.** Spark sizes off-heap storage by `spark.memory.offHeap.size`, whatever
  `spark.memory.offHeap.enabled` says, and requires a positive size when the flag is on. A probe on Spark 4.0.0 and
  4.1.0 with the flag off and a size of 64 MB kept an off-heap cache in memory. The engine therefore counts off-heap
  memory as present when the flag is on or the size is positive, as the session's configuration shows them.
- **Records.** A fit records the level asked for, as before. `PerfJob`'s report also records the level used
  (`fit.storage_level_used`), which shows whether the engine saw a cluster's off-heap memory.

### D-76: Measuring on the production cluster as it is; Spark 4.0 supported (Accepted: approved by the maintainer, 2026-11-16)

The maintainer cannot change the cluster's autoscaling or runtime (2026-11-16). The cluster runs Databricks Runtime
17.3 LTS with Photon (Spark 4.0.0), autoscales, and gives its JVM executors about 2.2 GB of heap.
- **Where exits are measured.** D-75's exit measurements are taken on that cluster as it is.
- **What exits are judged on.** Autoscaling changes the cores between runs and during them, so a step's exit rests
  on measures that do not depend on them:
  - CPU per row per pass;
  - bytes to the driver per pass, and shuffle bytes;
  - the driver's and the tasks' memory peaks, and the share of GC;
  - the three checks of plan §6.4.

  Wall time is reported beside the core-seconds the cluster offered during the fit and the utilization, and is never
  compared on its own.
- **Memory.** With 2.2 GB heaps shared by four tasks, a task's working memory during a pass must stay within a few
  hundred megabytes. Step 1's fixed-size blocks, with block growth retired, are therefore also the memory fix, and
  step 1's exit adds peak task memory and the share of GC at 50M rows.
- **Runtimes, amending D-02.** Spark 4.0.x and Databricks Runtime 17.3 LTS are supported beside 4.1.x and Runtime 18.
  - The JARs stay compiled against 4.1.0.
  - CI's Python job also runs them on pyspark 4.0.0, so an API missing from 4.0 fails CI.

  Evidence: the maintainer's `01_parity` passes all 155 comparisons on Runtime 17.3 with round 151's build. In the
  sandbox, the 4.1-compiled classes pass `CoxPHSuite` (89), `LogisticFESuite` (6) and `PerfJobSuite` (8) on Spark
  4.0.0's jars (round 153).

Approved as proposed on 2026-11-16. Implemented in round 154: the CI leg on PySpark 4.0.0, the Python package's
requirement widened to `pyspark>=4.0,<4.2`, and the plan and guides updated.

### D-75: Performance phase plan for Cox and logistic fixed effects (Accepted: approved by the maintainer, 2026-11-13)

`docs/spec/performance/plan.md`, following the algorithmic feasibility study (`docs/reviews/algorithmic-feasibility-2026-11.md`).
- The plan: step 0, a harness and a baseline of `main`; step 1, executor-side reduction in fixed groups, retiring block
  growth; step 2, chunked compensated accumulation in both kernels; step 3, the Cox weighted-Gram information with Efron
  ties and entry times; step 4, Cox per-time aggregates for unstratified fits and oversized strata on gridded times;
  step 5, logistic profile-likelihood Newton; step 6, internal scaling and the wide-model question. Each step has an
  exit measurement on the standard cluster (32 GB driver, 30 workers of 32 GB), and plan §3 lists where it departs
  from the study.
- Golden runs, which NUM-2 relies on, are defined (plan §6.1): the Cox and logistic fixture cases and two multi-block
  synthetic cases per model, recorded bit for bit in a manifest that `GoldenRunsSuite` checks, with a one-ulp negative
  control.
- A change that reorders floating-point sums without changing what is summed is admitted under a re-baselining rule
  (plan §6.3): fixtures within their classes; the old and new manifests within 1·10⁻¹³ at fixed β and within T-part at
  convergence, with identical iteration counts and flags; R0 for the new code; and the new manifest committed in the
  same round. Changes of iterates are compared at convergence and registered as discrepancies.
- Phase 4 is paused at round 148 (plan §8).
- Profile-likelihood Newton is built as an option; making it the default is a later decision on step 5's measurements.

Approved as proposed on 2026-11-13. Amended by D-76 (2026-11-16): the exits are measured on the production cluster as
it is and judged on CPU per row, bytes and memory.

### D-74: Slice 4d-3 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/random-effects/cluster-terms-job.md`: persistence, the jobs, Python and 4c's stage 2 for the cluster-factor
fits, with no new statistics.
- `LinearREClusterFitIO` and `LogisticREClusterFitIO` are kinds of their own, each at format version 1. The logistic
  fit is saved without its records and re-attached with `attach`, as 4c-3's fit is.
- `LinearREJob` and `LogisticREJob` gain `columns.clusters`, with the outputs that apply and an `effects` output; the
  linear job refuses provider profiling, as pprof_py does.
- Python's `cluster_vars` for both models.
- The random-cluster model's stage 2 from a logistic cluster fit, by pprof_py's `_values_from_stages` rules.
- This amends D-68's "format version 2 of both random-effect fits": the cluster fits are separate types, so they get
  their own kinds, and the single-factor formats stay unchanged.

Approved as proposed on 2026-11-05.

### D-73: X-050, the linear cluster fit's summary degrees of freedom (Accepted: approved by the maintainer, 2026-11-05)

Spec 4d-1 §5 promised pprof_py's `summary`, `fixed_effects_table` and `conf_int` for a linear fit with cluster
factors, and round 144 implements them.
- `fixed_effects_table` and `conf_int` follow pprof_py exactly: Student t with max(n − p, 1) degrees of freedom.
- pprof_py's `summary` takes n − p − m degrees of freedom, with m = `len(coefficients_["alpha"])`. Its comment calls
  this "consistent with fixed_effect model": m is the provider count. With cluster factors, `coefficients_["alpha"]`
  becomes a dict keyed by factor, so m counts the factors (2 or 3) instead of the 30 providers. Round 144's probe
  matched the summary's limits to n − p − (factors) on lrc-one and lrc-two.
- Proposed (X-050, class B): the corrected behavior, n − p − (providers) as pprof_py intends, documented as a
  divergence. The tests assert that pprof_py's limits follow its own count from our estimates, and ours the
  corrected one. At these fixtures the two differ in the sixth significant digit of the t quantile. Bug-compatible
  behavior would be a one-line change if preferred.

Approved as proposed, with the corrected degrees of freedom, on 2026-11-05.

### D-72: Slice 4d-2 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-11-05)

1. Fixtures (round 140, `reference/fixtures/logistic_re_cluster_fixtures.py` and `logistic_re_cluster_r.R`):
   `fixtures/logistic-re-clusters` has six cases, each with 30 providers over 3,000 records: lgc-one, lgc-two,
   lgc-text, lgc-offset, lgc-nested and lgc-boundary (a cluster factor at σ̂ = 0). Each holds pprof_py's Laplace fit
   at its defaults and tightened. On the tightened fit it holds:
   - the summary, the tests (`wald`, `poibin_exact`, `resampling`, and `exact` with one cluster factor);
   - the measures, options `alpha` and `SM`, and every factor's profile limits.
   It also holds glmer's tightened Laplace fit with posterior SEs, and a negative control. pprof_py falls back from
   BOBYQA to Powell in stage 1, as in 4b's fixtures, because the pinned environment lacks nlopt; its warning is
   recorded.
2. `docs/parity/logistic-re-clusters-calibration.md` passes every check:
   - pprof_py's and glmer's optima agree within 0.008 of T-opt in σ, β, the BLUPs and the posterior SEs, and within
     0.63 of T-fn in the log-likelihood;
   - the measures satisfy their identity;
   - every factor's profile limits contain σ̂;
   - pprof_py's defaults stay within 0.005 of T-opt of its tightened fits;
   - the negative controls miss by at least 342.
   glmer's `deviance()` is not pprof_py's Laplace deviance, so the log-likelihoods are compared.
3. Classes for 4d-2's code:
   - σ̂, β̂, the BLUPs and the posterior SEs at T-opt against both references, with D-59's optimality check;
   - the log-likelihood at T-fn against both;
   - lockstep at pprof_py's tightened (σ, β): the tests at T-test, T-p, T-flag and T-coef (X-024's allowance for
     exact tails), `resampling` within 4.5 Monte Carlo standard errors, the measures and intervals at T-meas, and
     the profile limits at T-opt;
   - a fit without cluster factors equal to 4b's bit for bit.

Approved as proposed on 2026-11-05.

### D-71: Slice 4d-2 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/logistic/random-effect-clusters.md`: the logistic random-effect model with crossed cluster factors, as
pprof_py's `cluster_vars`.
- pprof_py's two stages, nAGQ = 0 then the Laplace fit, with a σ per factor.
- The PIRLS solves eliminate the provider block onto the cluster levels, as 4d-1 does. The posterior variances are
  σₖ²·diag(H⁻¹), and the driver's copy of the rows is used when they fit (OI-99).
- Every method:
  - `summary`;
  - `test` with `wald`, `poibin_exact` (the cluster BLUPs plugged in), `resampling` (cluster posteriors drawn,
    the row mixture at the floor) and `exact` (one cluster factor only);
  - the measures and intervals, as 4b-2, which pprof_py keeps provider-only;
  - `profile_sigma(var)` for any factor.
- A fit without cluster factors stays 4b's bit for bit.
- Fixtures: a new family, `fixtures/logistic-re-clusters`, from pprof_py and `glmer`.

Approved as proposed on 2026-11-05.

### D-70: Slice 4d-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-11-05)

1. Fixtures (round 136, `reference/fixtures/linear_re_cluster_fixtures.py` and `linear_re_cluster_r.R`): a new family,
   `fixtures/linear-re-clusters`, rather than new cases in `fixtures/linear-re`, whose generator and files are built
   around one factor. It has seven cases, each with 30 providers over 3,000 records: lrc-one, lrc-two (two cluster
   factors), lrc-text, lrc-weights, lrc-offset, lrc-nested and lrc-boundary (a cluster factor at θ = 0). Each holds
   pprof_py's REML and ML fits at its defaults and tightened, `lmer`'s REML and ML fits with tight tolerances, and a
   negative control.
2. `docs/parity/linear-re-clusters-calibration.md` passes every check:
   - pprof_py's tightened optima and lme4's agree within 0.004 of T-opt in θ, the standard errors and the BLUPs, and
     within 0.0023 of T-fn in the criterion;
   - pprof_py's defaults stay within 0.0012 of T-opt of its tightened fits;
   - the negative controls miss by at least 108.
3. Classes for 4d-1's code:
   - θ̂, β, σ̂, the standard errors, the BLUPs and the fitted values at T-opt against both references, with the
     optimality check of D-59 (the gradient at T-opt's scale, or the bound);
   - the criterion at T-fn against both, and at pprof_py's tightened θ against its objective (the kernel at fixed θ);
   - a fit without cluster factors equal to 4a-1's bit for bit.

Approved as proposed on 2026-11-05.

### D-69: Slice 4d-1 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/linear/random-effect-clusters.md`: the linear random-effect model with crossed cluster factors, as
pprof_py's `cluster_vars`, estimation only.
- θ per factor by profiled REML or ML, the criterion of pprof_py's `_objective_from_state`. The search is
  `BoundedQuasiNewton` with central-difference gradients from θ = 1; one factor keeps 4a-1's search.
- The system is solved by eliminating the provider block onto the cluster levels, with a dense Cholesky factor of
  the cluster block, from one pass of level sums and level-pair sums. The search then runs on the driver (OI-59
  beyond its budget).
- Outputs: β and its tables, θ̂ and σ̂θ̂ per factor, every factor's BLUPs, fitted values, and predictions over the
  chosen factors. Provider profiling is refused, as pprof_py refuses it.
- A fit without cluster factors stays 4a-1's bit for bit; fixtures come from pprof_py and `lmer`.

Approved as proposed on 2026-11-05.

### D-68: Part 4d plan, cluster terms (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/random-effects/cluster-terms-plan.md`: crossed cluster factors in the random-effect models, as pprof_py's
`cluster_vars` (OI-93). Round 134's probe agreed with lme4.
- 4d-1: the linear model with cluster factors, estimation only. pprof_py refuses provider profiling on such a fit, so
  pprof_spark gives θ per factor, β, σ, the BLUPs, `summary`, fitted values and predictions.
- 4d-2: the logistic model with cluster factors: the two-stage Laplace fit with a σ per factor, and every method,
  `exact` among them with one cluster factor.
- 4d-3: persistence version 2, the jobs and Python; and the random-cluster model's stage 2 from such a logistic fit.
- The core: the provider block eliminated onto the cluster levels, as the three-stage model's stage 2 does, from
  provider × cluster cell sums.
- Fixtures: new cases in `fixtures/linear-re` and `fixtures/logistic-re`, from pprof_py and lme4.

Approved as proposed on 2026-11-05.

### D-67: Slice 4c-3 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/logistic/fe-random-cluster-job.md`: persistence, the job runner and Python save and load for the
random-cluster model, with no new statistics.
- A fit records its rows and an order-free fingerprint, computed in the validation pass.
- `LogisticFERandomClusterFitIO` format version 1 saves everything but the records, as `ThreeStageFitIO` does:
  metadata with doubles as bit patterns, a providers table and a clusters table. `attach` re-joins the training data
  and checks it.
- `pprof.spark.app.LogisticFERandomClusterJob` runs `model` `fe-random-cluster`:
  - its source is either given values or the paths of a saved `LogisticFE` fit and a saved three-stage fit;
  - its outputs are every one of slices 4c-1 and 4c-2's, then a run record;
  - the seven jobs refuse each other's specifications.
- Python's `save`, and `load(spark, path, df=None)`.

Approved as proposed on 2026-11-05.

### D-66: Slice 4c-2 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-11-05)

1. `fixtures/fe-random-cluster` gains he2013 (round 128). For every case, pprof_py's `estimator="he2013"` runs under
   `max_delta_gamma` and `relative`, each at its defaults (`tol` 10⁻⁵) and at `tol` 10⁻¹² (the fixed point). The
   `max_delta_gamma` fixed point also carries its exact test and measures at the median, and a negative control.
2. `docs/parity/fe-random-cluster-calibration.md` passes every strict check:
   - the two criteria reach the same fixed point, within 9·10⁻⁵ of T-coef;
   - the fitted values use the last E-step's moments, within 3·10⁻⁸ of T-meas;
   - the negative controls miss by at least 1.9·10⁷.
   he2013 converges linearly: 43 to 88 iterations to the fixed point, and 5 to 39 at its defaults, which stop up to
   995 of T-coef from it. Its fixed point lies up to 2.5·10⁸ of T-coef from `marginal`'s maximum.
3. Classes for 4c-2's code, which runs the same iteration with the same arithmetic up to rounding:
   - the fixed point's γ̂ at T-coef, its moments and fitted values at T-meas, and its log-likelihood at T-fn;
   - the default runs: the same iteration counts as pprof_py, and γ̂ at T-coef;
   - the exact test and the measures on the fixed point at slice 4c-1's classes.

Approved as proposed on 2026-11-05.

### D-65: Slice 4c-2 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/logistic/fe-random-cluster-he2013.md`: pprof_py's `estimator="he2013"` for the random-cluster model of
slice 4c-1.
- The iteration of He et al. (2013), reproduced step for step: fixed Gauss–Hermite nodes of N(0, σ²); an E-step on
  the records, with the fractional outcome's mixture form; a Newton step for γ with He et al.'s second-order terms;
  the information floor and clipping; both convergence criteria.
- Its outputs are the fixed point's γ̂ and the last E-step's moments. The marginal log-likelihood is evaluated at the
  end.
- Distributed: two passes over the records in cells per iteration, with cell sums added on the driver in key order
  (OI-101 beyond the driver budget).
- Validation: pprof_py's he2013 fixtures at their defaults and at the fixed point.
- Proposed with it: X-049 (σ = 0 iterates where pprof_py raises).

Approved as proposed, with X-049, on 2026-11-05.

### D-64: Slice 4c-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-11-05)

1. Fixtures (round 124, `reference/fixtures/fe_random_cluster_fixtures.py`): `fixtures/fe-random-cluster`, five cases
   of crossed providers and clusters, from pprof_py's `LogisticFERandomClusterModel` with β, σ and the start given.
   - frc-base;
   - frc-obs, an adjusted outcome with `obs_var`;
   - frc-absolute, with a provider whose outcomes are all 0 held at the bound;
   - frc-text;
   - frc-small-sigma, σ = 0.05, since pprof_py cannot fit σ = 0 (X-005).
   Each holds pprof_py's default and tightened fits, and on the tightened fit its tests, intervals and measures,
   with negative controls.
2. `docs/parity/fe-random-cluster-calibration.md` passes every strict check:
   - pprof_py's tightened fits have largest scores of 10⁻¹³ or less, and its tables satisfy their definitions within
     2.5·10⁻⁸ of T-meas;
   - the negative controls miss by at least 1.8·10⁷.
   pprof_py's default fit lies within 0.009 of T-coef of its tightened one, because Newton's method overshoots its
   tolerance.
3. Classes for 4c-1's code, those of the three-stage model's stage 3 and its inference (D-40, D-43), against the
   tightened fit:
   - γ̂ at T-coef, the clusters' posterior moments and the fitted values at T-meas, the log-likelihood at T-fn;
   - the tests at T-test, T-p, T-flag and T-coef, with X-024's allowance for the exact tails;
   - the measures and intervals at T-meas;
   - `resampling` within 4.5 Monte Carlo standard errors.
   The three-stage fixtures check the stage sources, and σ = 0 is checked against `LogisticFE` with offset xβ.

Approved as proposed on 2026-11-05.

### D-63: Slice 4c-1 specification (Accepted: approved by the maintainer, 2026-11-05)

`docs/spec/logistic/fe-random-cluster.md`: pprof_py's `LogisticFERandomClusterModel` as a model of its own. It has
fixed provider effects and random cluster effects, with β and σ held fixed, and its statistics are the three-stage
model's stage 3 and stage-3 inference (specs 2f-2 and 2f-3).
- β, σ and the start are given, or taken from a `LogisticFE` fit and a three-stage stage-2 fit. A
  `LogisticRandomEffectModel` with cluster factors waits for part 4d.
- An optional observed outcome is counted by the tests.
- The estimator is `marginal`; `he2013` is slice 4c-2. `summary` is delegated to stage 1, and `test`,
  `calculate_confidence_intervals` and `calculate_standardized_measures` use the three-stage code.
- Validation: the three-stage fixtures, plus a new `fixtures/fe-random-cluster` family from pprof_py's model
  directly. No new discrepancies.

Approved as proposed on 2026-11-05.

### D-62: Slice 4b-3 specification (Accepted: approved by the maintainer, 2026-10-23)

`docs/spec/logistic/random-effect-job.md`: persistence, the job runner and Python save and load for the logistic
random-intercept model, as slice 4a-3 did for the linear one, with no new statistics.
- `LogisticREFitIO` format version 1: a `metadata` line with every double as its bit pattern, and the provider table
  in Parquet, saved and loaded bit for bit.
- `pprof.spark.app.LogisticREJob` runs a `logistic-re` specification with every output of slices 4b-1 and 4b-2: one or
  more provider-test methods, each to its own directory, the measures and intervals, the profile, predictions and
  fitted values, then a run record. The six jobs refuse each other's specifications by name.
- Python gains `save` and `load`; `LogisticRERunSpec` gains `parse`.

Approved as proposed on 2026-10-23.

### D-61: Slice 4b-2 fixtures, calibration and tolerance classes (Accepted: approved by the maintainer, 2026-10-23)

1. Fixtures (round 116), from pprof_py's tightened nAGQ = 1 fit:
   - `inference_pprof_py.json` in all ten `fixtures/logistic-re` cases: `summary`, twelve test variants, option
     `alpha`, the `SM` intervals for each alternative and at the size-weighted mean given as a number (X-046), the
     measures at three references, `profile_sigma`, and negative controls.
   - `r_lme4.json` gains R pprof's `logis_re` outputs on `glmer`'s tightened fit, in the seven cases R pprof can fit
     (no offset, an intercept, binary outcomes), and lme4's profile interval.
   - `REFERENCE.lock` pins R pprof's six files.
2. `docs/parity/logistic-re-inference-calibration.md` passes every strict check:
   - pprof_py's tables satisfy their definitions exactly;
   - R pprof's Wald tests, option `alpha` and direct measures and intervals agree within 0.034 of T-opt;
   - lme4's profile interval agrees within 0.37 of T-opt;
   - the negative controls miss by at least 4.3·10⁶.
3. The references are run to their precision:
   - R pprof on `glmer`'s tightened fit, because its default stops early on lgre-small-sigma and lgre-many;
   - lme4's profile at zeta steps of 0.02, because the default grid's spline interpolation moved lgre-base's upper
     limit by 1.8·10⁻⁴.
4. lgre-small-sigma is a boundary fit: lme4's σ̂ is 0 and pprof_py's 1.7·10⁻⁸. Its Wald tests are untested in
   pprof_spark (X-048); R pprof's z there are 0/0 and pprof_py's noise, so they are informational.
5. Classes for 4b-2's code. Lockstep, pprof_spark evaluated at pprof_py's tightened (σ, β):
   - `summary` and the Wald test at T-test, T-p, T-flag and T-coef;
   - `poibin_exact` at T-test and T-p, its inverted limits at T-coef, as slice 2c;
   - the measures and `SM` intervals at T-meas, with X-026's binned direct sums. X-045's one-sided indirect limits
     are compared on their finite side, and the `mean` reference through the size-weighted fixture (X-046);
   - `resampling` within 4.5 Monte Carlo standard errors of pprof_py's p-values;
   - `profile_sigma` at T-opt against pprof_py and lme4.
   End to end, from pprof_spark's own fit, at T-est (T-opt where σ̂ < 0.1).
6. `compare_outputs.py` compares regenerated resampling values at a Monte Carlo class (rtol 2·10⁻², atol 10⁻³);
   the family's other values stay at T-opt, and those at fixed parameters at 10⁻⁸.

Approved as proposed on 2026-10-23.

### D-60: Slice 4b-2 specification (Accepted: approved by the maintainer, 2026-10-23)

`docs/spec/logistic/random-effect-inference.md`: inference, measures and σ's profile for the logistic
random-intercept model.
- Scope: `summary` (Wald z); provider tests `wald`, `poibin_exact` and `resampling` in slice 2c's table; option
  `alpha` and the `SM` intervals for indirect and direct ratios and rates; the standardized measures; and
  `profile_sigma`, with β minimized by the bounded quasi-Newton and the limits found to 10⁻¹⁰.
- Excluded: `null_model` (OI-87), `funnel_limits` (OI-88) and `exact` (part 4d).
- Distributed: measures and intervals in one pass over the training data checked against the fingerprint, with the
  direct sums binned (X-026); count tests within provider-local blocks.
- Validation: lockstep at pprof_py's tightened estimates, at the tight classes; end to end at T-est; R pprof where its
  definitions match pprof_py's; lme4's profile interval at T-opt.
- Proposed with it: X-045 (one-sided indirect intervals), X-046 (the size-weighted mean in the intervals), X-047 (R
  pprof's informational differences) and X-048 (Wald tests untested at a boundary fit).

Approved as proposed, with X-045 to X-048, on 2026-10-23.

### D-59: Slice 4b-1, tighter agreement checks and the fixtures' fixed points (Accepted: approved by the maintainer, 2026-10-23)

The end-to-end tolerance for the fitted estimates, T-opt, allows about a hundred times more than the agreement
observed in round 111 (the maintainer's question, 2026-10-22). Proposed for slice 4b-1, in round 112:
1. An optimality check. pprof_spark's Laplace deviance at its optimum must not exceed the deviance at pprof_py's or
   lme4's tightened optimum by more than 10⁻¹⁰ relative, both evaluated by pprof_spark's objective. That objective
   matches pprof_py's to about 10⁻¹⁵ and lme4's to about 10⁻⁹ at fixed parameters. For nAGQ = 0, the criterion must
   not exceed pprof_py's at its optimum by more than 10⁻¹⁰ relative.
2. A class T-est (rtol 10⁻⁶, atol 10⁻⁹) for pprof_spark's estimates against pprof_py's tightened optimum. T-opt stays
   where σ̂ < 0.1, where the likelihood is flat in σ, and for every comparison with lme4.
3. The fixtures' fixed points move σ and at most one centred coefficient, by at most two standard errors. On
   lgre-shifted the deviance at fixed parameters then agrees with lme4 within 0.006 of T-coef, where it differed by
   3.3·10⁸ of T-coef when every coefficient moved at once, so that comparison becomes strict.
4. The fixture sweep runs the kernel on the driver: every case, both nAGQ, in seconds. The engine's distributed fit is
   checked against the kernel at T-part on three cases, with its fitted values, predictions and residuals against
   the references.
5. The fractional-outcome findings go into spec 4b-1 §12, and OI-98 and OI-99 are opened.

Approved as proposed on 2026-10-23.

### D-58: Slice 4b-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-10-22)

`fixtures/logistic-re` (round 110; `reference/fixtures/logistic_re_fixtures.py` and `logistic_re_r.R`): the ten cases
of spec 4b-1 §10 with nAGQ = 1 and nAGQ = 0, pprof_py and `glmer` at their defaults and with their tolerances
tightened. The fixtures also hold the Laplace deviance at four fixed points within two standard errors of the optimum
(with pprof_py's central differences there), the nAGQ = 0 criterion at three values of σ, and negative controls.

`docs/parity/logistic-re-calibration.md` passes every strict check: the tightened optima agree within 0.017 of T-opt,
the deviances at fixed parameters within 0.32 of T-coef, and the weakest control misses by 12.7. Proposed:
1. T-opt (D-42) for 4b-1's estimates, fitted values, predictions and residuals; T-coef for the deviance and the
   nAGQ = 0 criterion at fixed parameters, since both references stop their inner iterations at a relative
   tolerance.
2. lgre-fractional compared with pprof_py only: `glmer` fits another model to fractional outcomes (σ̂ = 0, β̂ about
   twice pprof_py's).
3. lgre-shifted compared by invariance against lgre-base, as D-53 did for lre-shifted.
4. Negative controls at the provider level: the largest provider's outcomes flipped, and the first row of every
   provider flipped. One row moves an optimizer-limited fit too little for T-opt.
5. X-044 (class B): pprof_py's predictions with the BLUPs ignore them under integer keys; pprof_spark adds them, as
   lme4 and pprof_py with text keys do.

Approved as proposed, with X-044, on 2026-10-22.

### D-57: Slice 4b-1 specification (Accepted: approved by the maintainer, 2026-10-22)

`docs/spec/logistic/random-effect-estimation.md`: estimation for the logistic random-intercept model (one provider
intercept, covariates, an offset; fractional outcomes as pprof_py). The Laplace (nAGQ = 1) optimum over (σ, β) by
`BoundedQuasiNewton` with the analytic gradient of §2.1, each evaluation one pass in which every provider's mode is
found by Newton's method on its own rows from 0. `stage2 = false` gives the nAGQ = 0 optimum: Newton over (β, u)
through the Schur complement at each σ, and Brent's minimization over σ, which also starts nAGQ = 1. The estimates
follow pprof_py's definitions, Var(β̂) as the information conditional on the modes. The section also covers the data
contract (X-038's and X-040's rules), the distributed plan, edge cases, the Python mapping and the validation plan with
`fixtures/logistic-re` at T-opt. Proposed with it: X-043 (the covariance's definition against `glmer`'s default
`vcov`).

Approved as proposed, with X-043, on 2026-10-22.

### D-56: Slice 4a-3 specification (Accepted: approved by the maintainer, 2026-10-22)

`docs/spec/linear/random-effect-job.md`: persistence, the job runner and Python save and load for the linear
random-intercept model, with no new statistics. `LinearREFitIO` format version 1 (a `metadata` line with every double
as its bit pattern, and the provider table in Parquet), saved and loaded bit for bit. `pprof.spark.app.LinearREJob`
runs a `linear-re` specification (`columns` with offset and weights; `fit` with `reml`, `includeIntercept`,
`thetaUpper`, `maxEvaluations` and `blocks`; every output of slices 4a-1 and 4a-2), and the five jobs refuse each
other's specifications by name. Python gains `save` and `load`. The facade's `linearREModel` moves to
`LinearRERunSpec`'s parser.

Approved as proposed on 2026-10-22.

### D-55: Slice 4a-2 fixtures, calibration and corrections (Accepted: approved by the maintainer, 2026-10-21)

`fixtures/linear-re` gains `inference_pprof_py.json` and inference sections in `r_lme4.json` (round 102): every case
under REML and lre-base and lre-weights under ML, at slice 4a-1's tight optima, with pprof_py's methods of spec 4a-2
§10, base R's tails and quantiles on lme4's tight fit, R pprof's tests on its own lmer fit and the negative
controls' z. `docs/parity/linear-re-inference-calibration.md`: every strict check passes, worst 0.0016 of T-coef,
6.2·10⁻⁴ of T-test, 0.0084 of T-p and 1.3·10⁻⁷ of T-meas, no flag differs, and the controls miss by at least
5.7·10⁵. Proposed:
1. the classes of spec 4a-2 §10 for its parity tests, with pprof_py's `summary` p-values compared within their
   rounding, 5·10⁻⁸ absolute (X-041), instead of above 10⁻⁶, where rounding alone exceeds T-p;
2. at a boundary fit pprof_spark follows pprof_py, which leaves every provider untested whatever γ₀ (z, p-value,
   flag and the test's limits NaN) while option `alpha` gives limits of 0; this amends spec 4a-2 §8;
3. X-042 corrected: R pprof labels its BLUPs in text order over values in numeric key order, so with integer keys
   `test.linear_re`, `confint.linear_re` (both options) and the direct differences pair one provider's BLUP with
   another's size or label, while its indirect differences are right; R pprof is a reference, at T-opt on its own
   default fit, for text keys only (lre-text: within 1.8·10⁻⁴ of T-opt);
4. `compare_outputs.py` compares the linear-re family's default runs at T-opt and the rest at 10⁻⁸ relative: two
   sandbox sessions regenerated the family with differences up to 1.1·10⁻⁷ in the default runs and 1.3·10⁻¹⁰ in the
   tight optima, beyond T-part (OI-96).

Approved as proposed on 2026-10-21.

### D-54: Slice 4a-2 specification (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/linear/random-effect-inference.md`: inference and standardized measures for the linear random-intercept
model, as pprof_py defines them. Covariates: `summary` (t with n − p − m degrees of freedom), `fixed_effects_table`
and `conf_int` (n − p). Provider tests: z = (b̂ⱼ − γ₀)/seⱼ with the conditional SD of X-037, the theoretical null and
slice 2c's table, γ₀ = 0 by default. Intervals: option `alpha`, and option `SM` with the indirect and direct
differences, whose observed values are sums of fitted values. The measures use the training data (API-3), and the
fixtures extend `fixtures/linear-re`. Proposed with it: X-041 (class B: unrounded upper-tail p-values in `summary`) and
X-042 (R pprof's `linear_re` inference: a reference for `test` and option `alpha` on unweighted fits only).

Approved as proposed, with X-041 and X-042, on 2026-10-21.

### D-53: Slice 4a-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-10-21)

`fixtures/linear-re` (round 96; `reference/fixtures/linear_re_fixtures.py` and `linear_re_r.R`): the nine cases of
spec 4a-1 §11 under REML and ML, with pprof_py and lme4 at their defaults and at their tight optima (the root in τ
of central differences of each reference's own deviance), the deviance at fixed θ, NumPy's summaries and negative
controls. `docs/parity/linear-re-calibration.md`: on the eight well-conditioned cases the two tight optima agree
within 7.4·10⁻⁴ of T-coef (β̂, BLUPs, fitted values, predictions), 3.7·10⁻⁴ of T-var (θ̂, σ̂, σ̂_α, Var(β̂), PWRSS,
log det H, ‖u‖²) and 1.3·10⁻³ of T-fn (the deviance, ℓ, AIC and BIC, and the deviance at fixed θ); every negative
control misses by at least 83 times its class. Proposed: these classes for 4a-1's parity tests, the conditional
variances against lme4's at T-var, and the default runs informational under T-opt. For lre-shifted, where both
references lose up to 4.2·10⁻⁷ in τ* and 6.2·10⁻⁸ in β̂ to conditioning, invariance against lre-base replaces the
references' agreement, and pprof_spark centres the features of the between part (spec 4a-1 §13 and §14).

Approved as proposed on 2026-10-21.

### D-52: Slice 4a-1 specification (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/linear/random-effect-estimation.md`: estimation for the linear random-intercept model (one provider
intercept, offsets, prior weights; REML by default, ML on request). lme4's profiled deviance and its derivative on
provider summaries and pooled within co-moments (§2); the root of the derivative in τ = θ² by Brent's method,
bracketed by a grid, with boundary fits at 0 and θ_upper (§3); the estimates, BLUPs and conditional variances
(X-037); the data contract; the distributed plan (two passes, then the search on the driver or by aggregations over
the provider table); outputs; edge cases from round 95's probes; the Python wrapper's estimation methods; and the
validation plan with `fixtures/linear-re`. Proposed with it: X-038 (class C: incomplete rows fail with counts),
X-039 (class C: aliased features fail and are named) and X-040 (class C: designs whose provider variance is not
identified, and n ≤ p, fail at validation).

Approved as proposed, with X-038 to X-040, on 2026-10-21.

### D-51: Phase 4 plan, the random-effect models (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/random-effects/plan.md` reads pprof_py v0.7.0's `LinearRandomEffectModel`, `LogisticRandomEffectModel`
and `LogisticFERandomClusterModel` with their inference and measures modules, lme4 1.1-35.1 and R pprof 1.0.3's
random-effect functions, maps them onto pprof_spark's code, and proposes (its §8):
1. parts 4a (linear), 4b (logistic) and 4c (fixed effects with random clusters) in the maintainer's order, then 4d
   (cluster terms, OI-93), each closing at parity-verified with its own gate and an S1 smoke;
2. 4a: one provider random intercept with offsets and prior weights;
3. 4b: one provider random intercept with covariates and offsets; the Laplace fit by default and nAGQ = 0 as an
   option; tests `wald`, `poibin_exact` and `resampling`; `profile_sigma`;
4. 4c: stage 3 as a model of its own on the three-stage code; `he2013` as slice 4c-2 (OI-94);
5. the theoretical null only, until P0 adds null models, the empirical null and funnel limits to every model;
6. references: pprof_py at default and tight settings, lme4 with tightened control, R pprof's `linear_re` and
   `logis_re` families for inference where they follow pprof_py;
7. estimation to the references' optima rather than their optimizer paths (class D, as X-030);
8. tolerance classes: the ordinary classes for the linear estimates against tight references, T-opt for the
   logistic ones, the lockstep classes for inference;
9. REML by default for the linear model, ML as an option;
10. X-037 (class B): the linear model's BLUP standard errors under prior weights, corrected to the fitted model's
    conditional variance, as lme4's.

Approved as proposed, with X-037, on 2026-10-21.

### D-50: The roadmap after Phase 3, and scale testing in phases (Accepted: approved by the maintainer, 2026-10-19; amended at his request, 2026-10-21)

A comparison of the two code bases found two things. pprof_py v0.7.0 is at `9320766`, also its `main`;
pprof_spark was at round 90.
- pprof_spark implements 4 of pprof_py's 29 public model classes: `CoxPH`, `LogisticFixedEffectModel`,
  `LogisticThreeStageModel` and `LinearFixedEffectModel`. Each is parity-verified, with inference gaps.
- Nothing has run at scale.

Approved:

1. **Priorities.** Each model starts with a specification (NN-2) and a fixture-based parity gate.
   - P0, essential:
     - Inference parity on the four models: `null_model` on every provider test (OI-87), the empirical null
       (OI-78), funnel-plot limits (OI-88), the logistic fixed-effect model's confidence intervals (OI-89),
       external standard populations (OI-58), and the provider tests' `interval` option and VIF screening
       (OI-90).
     - Fixes to the shared execution paths that S0 finds.
   - P1, essential: random-effect models: linear, then logistic, then fixed effects with random clusters.
   - P2: survival extensions: cause-specific Cox, time-varying Cox, Cox with provider effects at λ = 0, shared
     frailty, Fine–Gray once X-003's parity target is chosen, and discrete-time survival.
   - P3:
     - penalized and group-lasso models with cross-validation, provider-penalized variants and `CoxPHSelector`;
     - inter-unit reliability, after X-006;
     - results exported in pprof_py's shapes for its presentation layer;
     - the `Ban` algorithm;
     - method-name aliases.
2. **Scale testing in four phases** (`docs/spec/scale/plan.md` §9):
   - S0, architecture checks, now.
   - S1, a scale smoke with each new model or feature.
   - S2, the plan's Stage A, after P0 and P1.
   - S3, its Stage B.

   Before S2, fix only problems of complexity class and costs in shared infrastructure. Complexity-class
   problems are time growing faster than n, driver memory growing with n or m, a stage limited to one task,
   and data expansion. Kernel tuning (OI-38) and absolute targets wait for S2.
3. **Milestones:**
   - M1: data generators and measurement capture, S0 on the four models, and specifications for the empirical
     null and funnel limits.
   - M2: P0's inference parity.
   - M3: fixes for M1's complexity problems, alongside M2.
   - M4: P1.
   - M5: S2 and S3 on the essential set, and a first release with a wheel (OI-55).
   - M6: P2, then P3.
4. **Scope.** pprof_spark stays a standalone Spark package. The scale kit runs on any Spark 4.1 cluster, and
   deployment work is outside this roadmap (D-14).

The scale-test plan's other decisions (its §12) remain open and apply to S2 and S3; its approval takes the next
free decision number. PROJECT_CONTEXT §4's Later row now follows this order. PROJECT_CONTEXT itself is updated
when the next phase's plan is written.

Amendment (round 94, 2026-10-21, at the maintainer's request): M4 comes before M2. The random-effect models
(P1: linear, then logistic, then fixed effects with random clusters) are developed now, while the maintainer runs
his own scale tests; M1's remaining items (`ScaleJob`'s measurements, the three-stage generator, S0's plan file and
its dry run) continue alongside, and M2 follows M4. The new models' provider tests take the theoretical null, as
the existing models' do; P0 then adds null models, the empirical null and funnel limits to every model (OI-87,
OI-78, OI-88). Phase 4's plan: D-51.

### D-49: Fixes for the reported issues (Accepted: approved by the maintainer, 2026-10-12)

`docs/reviews/issues-2026-10.md` diagnoses ten issues the maintainer reported against round 81
(OI-73 to OI-81). Approved: its recommendations and their order (§3): L5 and C3, C1,
L2 to L4 and L6, L1, then C2's composite keys. New options keep today's defaults, except C3 (a stratum byte
budget, `maxStratumBytes` = 4 GiB, replaces the 1,000,000-row default of `maxStratumRows`, which becomes
unlimited) and L5 (blocks grow, doubling up to 256 MiB, where the block partials would exceed the driver budget;
plans that fit are unchanged). Fixes that change statistical behaviour come with specification addenda. The
scale-test plan, drafted in round 80 to become D-49, awaits its own decision.

Correction (round 85): L6's recommendation rested on a misreading of pprof_py, which refuses robust variance for
indirect measures too; pprof_spark keeps refusing it, and that recommendation is withdrawn.

### D-48: Close Phase 3 at parity-verified (Accepted: approved by the maintainer, 2026-10-11)

`docs/gates/phase-3.md`: slices 3a to 3c meet the parity gate in the sandbox (round 74: every linear engine suite
passes under Classic Spark on JDK 17, and none of the 3,544 tolerance ratios they compute exceeds 1; worst T-meas 0.887
on lin-shifted, inside the references' own spread of 0.504 there; T-fn 0.0496; T-test 7.3e-4); every discrepancy
affecting Phase 3 has an approved decision. Proposed: close Phase 3 at parity-verified once the maintainer reports CI
green for `main` at this round (rounds 61 to 73 have no CI report), on the terms of D-32 and D-44, accepting that the
within cross-products are verified against exact arithmetic (`LinearKernelSuite`) and end to end rather than against
the fixtures' NumPy values (spec 3a §11); every Phase 3 feature stays `Experimental` until the package-level scale
test (D-28). PROJECT_CONTEXT v2.6 then records Phase 3. Approved on 2026-10-11, with the substitute for 3a's
function-level check, the maintainer reporting CI green for every run since round 57.1.

### D-47: Slice 3c specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/job-and-python.md` specifies the linear job runner and the Python access, following the logistic
job runner (D-37) and the py4j wrappers (ADR-0009): a run specification with `model` `linear` (new `LinearRunSpec`,
entry point `LinearJob`; the four jobs refuse each other's specifications), one output directory per section with the
run record written last, and `pprof_spark.LinearFixedEffect` and `LinearFixedEffectModel` mapping pprof_py's methods,
with the data argument required exactly where the library needs the training data (API-3). No new statistics.
Approved on 2026-10-09.

### D-46: Slice 3b specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/provider-inference.md` specifies pprof_py's linear provider tests (t statistics mapped to z by
equal tail probability; slice 2c's flags, `critical` and columns; limits γ̂ ∓ q·se), intervals (`gamma`, and `SM` for
the indirect and direct differences) and standardized differences. The reference effect is computed on the driver
from the provider table (median, size-weighted mean or a number), and the measures from a pass over the training data
(API-3). Proposed with it: X-035 (class B: a provider whose t tail underflows keeps its test, through a log-scale z)
and X-036 (R pprof 1.0.3's linear inference serves as a reference only where it follows pprof_py's definitions).
R pprof's linear functions are pure R (OI-60 closed). Approved on 2026-10-09 with X-035 and X-036.

### D-45: Phase 3 plan and the slice 3a specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/plan.md` reads pprof_py v0.7.0's `LinearFixedEffectModel` and proposes slices 3a
(estimation and covariate inference), 3b (provider tests, intervals, standardized differences) and 3c (job
runner and Python), the distributed design (PROJECT_CONTEXT §7.5's pass over the within co-moments, then a
pass over the residuals; TSQR deferred, OI-61) and the tolerance classes.
`docs/spec/linear/fixed-effect-estimation.md` specifies slice 3a. Proposed with them: X-031 (class B:
covariate p-values from the upper tail), X-032 and X-033 (class C: aliased features and n − m − p ≤ 0 fail).
Out of scope: funnel limits, plots and empirical nulls (§4 "Later"). Approved on 2026-10-09 with X-031 to X-033.

### D-44: Close Phase 2 at parity-verified (Accepted: approved by the maintainer, 2026-10-09)

`docs/gates/phase-2.md`: slices 2a to 2f meet the parity gate. In round 60's sandbox every Phase 2 engine
suite passed (15 suites, 76 tests, Classic Spark, JDK 17) and none of the 10,042 tolerance ratios they
compute exceeds 1 (worst: T-coef 0.683, T-opt 0.331, T-meas 0.169, T-fn 0.142); every discrepancy affecting
Phase 2 has an approved decision; CI was green through round 57.1. Proposed: close Phase 2 at
parity-verified once the maintainer reports CI green for round 59, on the terms of D-32; every Phase 2
feature stays `Experimental` until the package-level scale test (D-28). PROJECT_CONTEXT §7.4's three-stage
description (OI-48) goes into the next revision. Approved on 2026-10-09, after the maintainer reported CI
green; PROJECT_CONTEXT v2.5 (round 61) folds in OI-48.

### D-43: Slice 2f-4 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-pipeline.md`: the three-stage pipeline (`ThreeStage.fit`); stage 3's provider
tests (`exact` by clustered Gauss–Hermite mixtures of Poisson-binomial counts, `poibin_exact`, `resampling`),
standardized measures (direct sums per cluster from offset-bin moments) and intervals; `sigma_sensitivity`
(σ_c's profile interval from the exact Laplace deviance, stage 3 refits at both ends with the σ = 0 limit);
persistence, the job runner (`model` `three-stage`) and Python; the distributed plan and validation (T-opt
for the profile limits). No new discrepancies: X-005, X-024, X-025, X-026 and X-030 apply as approved.
Approved as written; 2f-4a fixtures in round 54.

### D-42: The T-opt tolerance class (Accepted: approved by the maintainer, 2026-10-08)

For optimizer-limited estimates (three-stage stage 2's variance components, intercept and BLUPs; X-030):
rtol 2e-4, atol 1e-6 under the element-wise rule of D-09, added to `tolerances.conf`. Justification and
negative controls (NN-9): `docs/parity/three-stage-calibration.md` (round 51): pprof_py's tight run against
`glmer` at most 0.48 of it on integral outcomes, pprof_py's default run against its tight run at most
1.7e-4 of it, the controls at least 23.9 times outside. Approved as proposed.

### D-41: Slice 2f-3 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-stage2.md`: stage 2, the crossed random-intercept logistic GLMM with the
stage 1 offset (lme4's nAGQ = 1 Laplace deviance), on 2f-2's compressed cells: PIRLS converged from zero at
every evaluation, the joint system eliminated onto the smaller set of levels, and a bounded quasi-Newton
on (σₚ, σ_c, μ) to a gradient tolerance instead of pprof_py's Nelder–Mead (X-030, with a new
optimizer-limited tolerance class T-opt to be calibrated and approved under NN-9); outputs, the driver
plan (OI-59 for very large level sets) and validation. Approved with X-030 and OI-59 as proposed.

### D-40: Slice 2f-2 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-stage3.md`: stage 3 with β and σ fixed, as pprof_py's `marginal`
estimator (adaptive Gauss–Hermite quadrature at each cluster's posterior mode, projected Newton with a
sparse Hessian, Armijo line search, bounds), computed from per-cell offset-bin moments; the σ = 0 limit
(X-005); conjugate-gradient Newton steps and Newton-computed Gauss–Hermite rules (class D); outputs,
the distributed plan (compression, then the driver or cluster-local executors) and validation. Decision
to approve with it: X-029 (compressed cells for stage 3; class C). Approved as written; fixtures in
round 47.

### D-39: Slice 2f-1 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-preparation.md`: pprof_py's `glmm_data_prep` (screening at more than
`cutoff` records, `y_adj`, provider × cluster cells, `included`) and stage 1 (slice 2a on the included
cells without screening), the offset xβ̂, the outputs, the distributed plan and validation. Decision to
approve with it: X-028 (no dense provider × cluster table; class C). Approved as written; fixtures in
round 44.

### D-38: Slice 2f direction after the three-stage spike (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-spike.md` (OI-51): pprof_py's three-stage model is He et al. (2013)'s
(providers crossed with clusters, random cluster effects), as R's `glmm.fac.hosp`. Proposed: compress
the records once into per-cell offset-bin moments so that stages 2 and 3 iterate on cells, not
records; sub-slices 2f-1 (preparation and stage 1), 2f-2 (stage 3, `marginal`, given β and σ), 2f-3
(stage 2, the crossed random-intercept GLMM) and 2f-4 (pipeline, `sigma_sensitivity`, inference, job,
Python); X-005 (the σ = 0 limit, class B); fixtures without nlopt and an optimizer-limited tolerance
class for stage 2, to be calibrated. Approved as written.

### D-37: Slice 2e specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/job-runner.md`: a logistic job runner. The run specification (version 1) gains a
top-level `model` (`cox` by default, or `logistic`); `pprof.spark.app.LogisticJob` (`--spec` or
`--spec-json`) fits the model and writes the fit, the provider table, covariate tests, provider tests,
standardized measures, tests on measures and predictions as requested, never overwriting, with a run
record. No statistical change. Approved as written; implemented in round 41.

### D-36: Slice 2d specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/standardization.md`: indirect and direct standardized ratios and rates, their
standard errors (model-based, robust, β known; indirect variance at γ₀ or γ̂ⱼ), tests on a measure
(identity, logit and log scales), R's extreme observations, the distributed plan, and the decision
on OI-49: direct sums by binned Taylor expansions (bins of 0.1 on η, order 8), whose remainder is
below rounding. Decision to approve with it: X-026 (class C). Approved as written; fixtures and
calibration in round 38.

### D-35: Slice 2c specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/provider-tests.md`: provider tests on the logistic effects (exact Poisson-binomial
with mid-p tails and test-inversion limits, score, Wald, bootstrap), the reference effect, flags and
the result table, the exact algorithm (OI-52: the smaller tail by the exact recursion), the
distributed plan and validation. Decisions to approve with it: X-024 (correct pprof_py's upper
tails, class B) and X-025 (bootstrap parity distributional, class C). Approved as written; fixtures
and calibration in round 35.

### D-34: Slice 2b specification (Accepted: approved by the maintainer, 2026-10-07)

`docs/spec/logistic/covariate-inference.md`: Wald variants (null, sidedness, robust), likelihood-ratio
and score tests by refits on the working set, cluster-robust variances of β̂ and of the provider
effects (at the average case mix and with β known), prediction and AUC, with their distributed
plan and validation. Decisions to approve with it: X-022 (fail with counts for unknown providers;
no AUC for two outcome values other than 0 and 1; class C) and X-023 (clusters nested in providers,
following pprof_py). Approved as written; fixtures and calibration in round 32.

### D-33: Python wrappers' design, ADR-0009 (Accepted: approved by the maintainer, 2026-10-07)

`docs/adr/0009-py4j-wrappers.md`: a Java-friendly facade `pprof.spark.app.python.PythonApi` in `app`
(DataFrames, strings and numbers in; DataFrames, JSON summaries and opaque fit handles out), a
pure-Python package `pprof_spark` in `python/`, PySpark Classic sessions only, a wheel version-locked
to the JAR, and a CI job running the Python tests against PySpark 4.1.0. The round 29 spike reached
`LogisticFE` from PySpark through py4j. Approved; the Cox wrappers followed in round 30.

### D-32: Close slice 2a at parity-verified (Accepted: approved by the maintainer, 2026-10-07)

`docs/gates/phase-2a.md`: every requirement is met in the sandbox except CI for rounds 27 and 28,
the correlation warning and the scale test (D-28). Proposed: close slice 2a at parity-verified once
CI is green for rounds 27 and 28 and the correlation warning lands as decided under X-021, whose
recommended option computes it on the fitted rows (class C). The feature stays `Experimental`.
Approved with X-021 option (a); the warning landed in round 29, so the slice closes once CI is
green for rounds 27 to 29.

### D-31: Python access through py4j wrappers (Approved by the maintainer, 2026-10-07)

Revisiting D-03 after Phase 1d, the maintainer chose py4j wrappers: Python calls the Scala engine
through PySpark's JVM gateway; Python code never reimplements statistics (§6.12). Supersedes D-03.
Design (facade, packaging, CI, supported compute) in ADR-0009 before any wrapper code; schedule in
`docs/spec/logistic/plan.md` §6 (OI-53). Order approved by the maintainer on 2026-10-07: ADR-0009 once
the slice 2a estimator lands, then wrappers for the Cox API, then each logistic slice.

### D-30: Phase 2 plan and the first-slice specification (Accepted: approved by the maintainer, 2026-10-07)

`docs/spec/logistic/plan.md` (slices 2a to 2f with their references, fixtures, tolerance classes
and distributed design) and `docs/spec/logistic/fixed-effect-estimation.md` (slice 2a: SerBIN
estimation of the logistic fixed-effect model, screening, degenerate providers, model-based
variances, the Wald test for β). Discrepancy decisions to approve with it: X-004 resolved at
v0.7.0; X-016, X-017 and X-018 follow the reference, with degenerate providers flagged; X-019
fails at validation (class C); X-020 follows the behavior (class E). Approved as written, with the
recommended decisions; fixtures and calibration in round 26.

### D-29: Close Phase 1d at parity-verified (Accepted: signed off by the maintainer, 2026-10-07, with round 22's CI green)

`docs/gates/phase-1d.md`: every requirement is met except the end-to-end run at design-envelope
scale, which the maintainer runs on his Databricks once the whole package is done (D-28). Proposed:
close Phase 1d once CI for round 22 is green, on the terms of D-22, D-24 and D-26.

### D-28: Site for the Phase 1d scale test (Decided by the maintainer, 2026-10-06)

The maintainer runs the scale test himself on his Databricks workspace once the whole package is
done, not phase by phase (his preference, stated 2026-10-07). AI assistants never access that
environment; they prepare the JARs, run specifications, generator and instructions, and analyse
shared results. Phase gates therefore close at parity-verified, as Phases 1a to 1c did, and scale
verification happens once for the package.

### D-27: Phase 1d specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/provider-workflows.md`: indirect and direct standardized ratios (two-stage SMR and
SHR) with pprof_py's conventions, exact and mid-p provider tests with the theoretical null,
flags and result tables, a job runner, and scale validation. The empirical null stays deferred.

### D-26: Close Phase 1c at parity-verified (Accepted: signed off by the maintainer, 2026-10-06, after round 18's CI passed)

`docs/gates/phase-1c.md`: every requirement is met except the scale test. Proposed: close Phase 1c
on the terms of D-22 and D-24 once round 18's CI passes.

### D-25: Phase 1c specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/residuals-robust.md`: martingale, score and dfbeta residuals and robust variance,
per row or clustered, with X-015 (robust variance from score residuals, as R, where pprof_py's
Breslow counting-process kernel disagrees with R by 11%) and residual tables keyed by the row
identifier.

### D-24: Close Phase 1b at parity-verified (Accepted: signed off by the maintainer, 2026-10-06, after round 16.1's CI passed)

`docs/gates/phase-1b.md`: every requirement is met except the scale test. Proposed: close Phase 1b
on D-22's terms, with scale verification at the Phase 1d gate.

### D-23: Phase 1b specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/counting-process-baseline.md`: (start, stop] data and left truncation, per-stratum
baseline hazard and survival, and prediction, with X-014 (the baseline is reported at x = 0 and
offset 0; pprof_py's public baseline and R's `basehaz` include exp of the weighted mean offset).

### D-22: Close Phase 1a at parity-verified (Accepted, 2026-10-06)

`docs/gates/phase-1a.md`: every Phase 1a requirement of §9.8 is met except the scale test, which
needs a cluster (D-14) and targets (D-04). Proposed: close Phase 1a once CI confirms rounds 11 and
12, keep the Cox features `Experimental` at parity-verified, and move their scale verification to
the Phase 1d gate. Asked to sign off, the maintainer replied "Continue"; recorded as sign-off.

### D-21: Efron ties, case weights and offsets (Accepted in advance, 2026-10-06)

`docs/spec/cox/efron-weights-offsets.md`. The maintainer approved the round's specification and
changes in advance and asked for the work to continue. X-013 (zero-weight events under Efron) was
found afterwards; asked to confirm the recommended corrected behavior, the maintainer replied
"Continue", so it stands under the standing delegation. Implemented in round 11.

### D-20: Cox first-slice specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/first-slice.md`: stratified, right-censored, Breslow, model-based variance. It
includes three discrepancy decisions to approve with it: X-010 changed to test convergence before
halving (corrected behavior, superseding round 6's bug-compatible choice), X-011 (aliasing fails),
and X-012 (no events fails). Approved as written, with the three discrepancy decisions;
implemented in round 10.

### D-12: Build and test tooling baseline (Delegated; reversible)

sbt 1.12.15 (1.13.0 exists but has no patch release yet); sbt-scalafmt 2.6.2 with scalafmt
3.11.5; sbt-buildinfo 0.13.2; munit 1.2.0, the newest release built with Scala 2.13.16 (later
releases pull scala-library 2.13.17 or 2.13.18, OI-04). GitHub Actions: checkout v7, setup-java
v6 (Temurin), setup-sbt v1, upload-artifact v7, runner ubuntu-24.04. Deferred: OI-15. Round 172
adds cache v6 (its restore and save actions) for CI's dependency cache, in place of setup-java's
own sbt cache in `ci.yml`.
