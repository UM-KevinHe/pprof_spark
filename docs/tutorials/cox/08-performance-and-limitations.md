# 8. Performance considerations and limitations

[Tutorial home](README.md) · previous: [7. Validation and numerical accuracy](07-validation-and-numerical-accuracy.md) ·
next: [9. Worked examples and troubleshooting](09-worked-examples-and-troubleshooting.md)

No performance measurement of the Cox fit exists yet: there is no Cox benchmark in `bench`, and the
scale test waits for the maintainer's package-level Databricks run (D-28). Everything in this
chapter is therefore derived from the code and its formulas, or observed on toy data in local mode,
and says so. Treat the numbers as a model to be confirmed, not as measurements.

## 8.1 The cost of one fit

| Phase | Spark jobs | Data movement | Compute |
|---|---|---|---|
| Validation | 1 aggregation (3 jobs with a row identifier) | Partial aggregates only | O(n) |
| Stratum sizes | 1 query (2 jobs) | One count per stratum | O(n) |
| Working set | Broadcast, shuffle, materialization (5 jobs) | **Every row once**, plus the placement table | O(n log n) sort within blocks |
| Summary | 1 | B summaries | O(n·p) |
| Evaluations | 1 per evaluation: 1 at β = 0, 1 per iteration, 1 per halving | B partials to the driver each time | O(n·p²) each |
| Robust variance | 1 pass (per row), or a pass and 2 shuffles (clustered) | B or 256 partials; clustered: n p-vectors shuffled | O(n·p²) |
| Driver | — | — | O(p³) per iteration for Cholesky; O(B·p²) for the reduction |

A typical fit takes 3 to 6 iterations without halvings (the specification expects 4 to 6 passes in
all), so its cost is about one shuffle of the data plus 5 to 8 passes of the kernel, plus about 11
jobs of fixed overhead.

## 8.2 Compute and memory per pass

**The kernel.** For each row with positive weight, the sweep does p multiply-adds for η, one
`StrictMath.exp`, p compensated additions into S₁ and p(p+1)/2 into S₂ (chapter 4, section 4.3).
Event rows add p plain additions, and under Efron another p + p(p+1)/2. A truncated row is removed
with the same work again. Each event time adds O(p²) under Breslow and O(d·p²) under Efron. At
p = 100, that is 5,050 compensated additions per row per pass. A Neumaier addition costs about four
more floating-point operations than a plain one; OI-38 asks for that cost to be measured at p = 10 to
100 before large-p work.

**Block memory.** `targetRowsPerBlock` assumes 8·(p + 4) bytes per row
([BlockOptions.scala lines 29–30](../../../engine/src/main/scala/pprof/spark/engine/backend/BlockOptions.scala#L29-L30)),
called with p + 2 for Cox. A `CoxBlock` row actually holds p covariates, four more doubles (time,
weight, offset, entry), a long, a boolean, an int and a string reference, so blocks come out somewhat
larger than the 4 MiB target (review finding F-12).

**Build memory.** `CoxBlockBuilder.build` turns a block's rows into `CoxRow` objects, each with its
own covariate array, and sorts them before copying into primitive arrays
([CoxBlocks.scala lines 95–125](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxBlocks.scala#L95-L125)).
That transient footprint is several times the final arrays. For a regular block it is small; for an
oversized stratum, which always gets a block of its own whatever its size up to `maxStratumRows`, it
is not: a 1,000,000-row stratum with p = 10 makes final arrays of roughly 130 MB and a larger
transient peak in one task.

**Cache.** The working set is persisted at `MEMORY_AND_DISK`, and every pass decodes each block from
the cache into a `CoxBlock` before running the kernel (the `DeserializeToObject` step of the plan in
chapter 5). That costs O(n·p) per pass, small next to the O(n·p²) kernel unless p is small.

## 8.3 Parallelism and skew

**Parallelism is across strata.** A block is processed by one task, and a stratum never spans two
blocks. The useful parallelism of a pass is therefore the number of blocks that can run at once, and
the time of a pass is at least the time to sweep the largest stratum.

| Data | Blocks | Behavior |
|---|---|---|
| Many strata of moderate size (providers) | About n / T, balanced by bin packing | The intended case: every executor core busy |
| A few large strata (regions) | One per stratum | At most that many busy tasks; the largest stratum is the straggler |
| No strata | One | **One task does every pass.** Above `maxStratumBytes` (4 GiB of estimated arrays by default since round 83; 1,000,000 rows before) the fit fails; raising the cap keeps it to one task and one block in one executor's memory |

The project's answer for large strata, the TimeRange layout (§6.6), is not implemented (OI-03,
OI-30). It would split a stratum into time-contiguous blocks with a first pass computing each block's
risk-set offsets, at the cost of two passes per evaluation. Adaptive query execution's skew handling
does not apply to custom kernels and is not relied on.

**Tasks per pass are not blocks.** Every evaluation runs as many tasks as the persisted working set
has partitions, which is `spark.sql.shuffle.partitions` when it was built (observed: 1, 4 and 7 tasks
for the same one-block fit; chapter 5, section 5.7). Blocks are placed on partitions by hashing their
identifiers. With far fewer blocks than partitions most tasks are empty; with far more, each task runs
several blocks, unevenly. Until this is addressed (review finding F-06), setting
`spark.sql.shuffle.partitions` before a fit to a value suited to the expected number of blocks and
the cluster's cores is the available control; its effect has not been measured.

## 8.4 Driver traffic and the sizing limits of the default options

Every evaluation brings B partials of 1 + p + p(p+1)/2 doubles to the driver. Before building the
working set, `withWorkingSet` checks that B·8·(2 + p + p(p+1)/2) bytes fit in `driverBudgetBytes`
(64 MiB) and fails otherwise
([CoxPH.scala lines 333–337](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L333-L337)).
Combined with the rows per block, this caps the size of a fit at the default options. Computed from
the code's formulas, assuming full blocks:

| p | Rows per block T | Guarded bytes per partial | Largest B | Largest n ≈ B·T |
|---:|---:|---:|---:|---:|
| 1 | 104,857 | 32 | 2,097,152 | 2.2 × 10¹¹ |
| 10 | 37,449 | 536 | 125,203 | 4.7 × 10⁹ |
| 50 | 9,709 | 10,616 | 6,321 | 6.1 × 10⁷ |
| 100 | 5,041 | 41,216 | 1,628 | 8.2 × 10⁶ |
| 300 | 1,724 | 363,616 | 184 | 3.2 × 10⁵ |
| 1,000 | 522 | 4,012,016 | 16 | 8.4 × 10³ |

At p = 100 the defaults stop at about 8 million rows, below the design envelope of 10⁷ to 10⁹. Raising
`targetBlockBytes` raises T and the limit proportionally (64 MiB gives T = 80,659 at p = 100, so about
1.3 × 10⁸ rows), at the price of larger blocks per task; raising `driverBudgetBytes` raises B. Both
are recorded with the fit. The guard fails early, before any shuffle, with a message naming the two
options. At large p the partials themselves dominate, which is the open design question OI-02.
Separately, `maxGroupsOnDriver` (2,000,000) bounds the number of strata.

## 8.5 Fixed costs and repeated preparation

About 11 Spark jobs run before the first evaluation, whatever n (chapter 5, section 5.7). For small
data that overhead dominates, which is why single-node pprof_py is expected to be faster below some
crossover size (§3.7; the crossover analysis, PERF-1, is pending).

Every call that needs the training data rebuilds the working set, including the full shuffle,
because fitted models hold no data (ARCH-4) and every call verifies the fingerprint (API-3). A
`CoxJob` run that asks for every output prepares the data:

| Output | Preparations |
|---|---:|
| `CoxPH.fit` (robust variance reuses this one) | 1 |
| `CoxPH.baseline` | 1 |
| `CoxPH.residuals` | 1 |
| `CoxMeasures.standardized` | 1, or 2 when the provider is not the strata (a fingerprint check through the fit's layout, OI-46) |
| `CoxProviderTests.test`, which calls `standardized` again | 1 or 2 |
| **Total** | **5, or 7** |

Review finding F-05 proposes measuring this first and then, if it matters, letting a prepared working
set be reused across calls while keeping the fingerprint check.

## 8.6 What the design is good and less good at

| Situation | Fit |
|---|---|
| Provider-stratified models with thousands of strata, moderate p | Good: parallel across strata, one shuffle, no shuffle per iteration, deterministic |
| Two-stage SMR and SHR | Good: stage 1 is provider-stratified; the national baseline is aggregated by time |
| Unstratified fits, or a few huge strata | Poor: sequential within a stratum; capped by `maxStratumRows`; needs TimeRange |
| p in the hundreds | Constrained by the driver budget and the O(p²) per-row cost (OI-02, OI-38) |
| Many small fits | Fixed job overhead dominates; pprof_py is likely faster |
| Pipelines needing several outputs | Repeated preparation (F-05) |
| Databricks serverless | Not supported yet (D-14): the working set is always persisted; the table materialization of §6.5 does not exist |

## 8.7 Limitations, collected

Statistical:

- No exact tie methods; no merging of near-tied times (chapter 2, section 2.11).
- No detection of infinite coefficients (chapter 3, section 3.6; F-02).
- No covariate scaling and no per-stratum shift of η; extreme offsets or scale differences fail or
  mislead (F-03).
- Model-based variance treats weights as frequency weights; use robust variance for sampling weights.
- Per-row robust variance on counting-process data treats a subject's rows as independent (warned).
- No likelihood-ratio or score test output, no proportional-hazards diagnostics.
- Every feature is Experimental until the scale test.

Computational:

- One stratum is one task (TimeRange missing).
- Sizing limits at the default options for large p (section 8.4).
- Tasks per pass follow `spark.sql.shuffle.partitions`, not the blocks (F-06).
- Repeated preparation across calls (F-05).
- No measured performance, no cluster tests.
