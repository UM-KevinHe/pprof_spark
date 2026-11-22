# Performance step 2: chunked rows and chunked compensated sums

Status: design for plan step 2 (D-75, as amended by D-76), round 177. **Approved by the maintainer as written on
2026-11-21**, with D-79, which says how D-78 (plan §6.3) applies to the golden runs. Round A is built in round 178
(§11).

## 1. Why

- **Arrays that grow with n.** A block holds whole providers (logistic) or strata (Cox), so it is as large as its
  largest provider. With 8,000 providers and 180 features, a block's feature array is about 9 MB at 50M rows and
  18 MB at 100M (OI-118, OI-119). Every pass deserializes every block into new arrays of that size, in every task.
- **Spark's cache does not split them.** The cache serializer of Spark 4.0 and 4.1 closes a cached batch once its
  columns reach 4 MiB (`ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE`, 4,194,304 in both the 4.0.0 and 4.1.0 jars, read in
  round 177). Each such block is therefore a batch of its own.
  - A pass holds up to three block-sized copies at once: the batch, the decoded row and the deserialized arrays.
  - Each executor runs four tasks.
  - Under G1, JDK 17's default collector, a heap of about 2.2 GB has 1 MiB regions. Every array of 512 KiB or more
    is then a humongous allocation, placed in contiguous regions outside the young generation. Which collector the
    cluster's executors run is not recorded.
- **The kernels carry the CPU.** On the cluster, a Cox evaluation costs about 100 to 130 µs per row at 180
  features, about 90% of a fit's CPU (`perf-results.md`). Logistic pass A costs about 83 to 88 µs per row. The bulk
  is p(p+1)/2 Neumaier additions per row, each with a branch the JIT does not vectorize.
  - The study's prototype used plain sums over 256 rows, folded into Neumaier totals. It ran pass A at 4.85 µs per
    row against 29.9, in the sandbox (study §4.4).
- **Blocks that still grow.** The covariate tests, the provider tests and the standardized measures rebuild the
  working set with `BudgetedLayout`, which grows blocks up to 256 MB (§4).

## 2. Four rounds

| Round | Work | Bits |
|---|---|---|
| A | Logistic: chunked row storage (§3); the tests' and measures' layouts become the fit's (§4) | Unchanged; the manifest stays as it is (plan §6.2) |
| B | Cox: chunked row storage (§3); the Breslow buffer and the per-time means (§5.3) | Unchanged |
| C | The golden runs gain values at fixed points (§6), if D-79 is approved | Recorded values unchanged; new values added |
| D | Chunked compensated sums in both kernels and the co-moments (§5), with the specification and ADR amendments | Re-baselined under D-78 |

Storage is kept apart from summation. The accumulation chunks of §5 are defined on the canonical row order, not on
the stored records, so rounds A and B can be checked bit for bit against the present manifest.

## 3. Chunked row storage

- **The block stays.** Blocks are planned as now (`LayoutPlan.create` at the target size). They keep whole providers
  or strata in canonical order, and are folded in block order within reduction groups (step 1). Only their storage
  changes.
- **Chunk records.** A block is stored as consecutive records of at most C rows each. `LogisticChunk` and `CoxChunk`
  replace `LogisticBlock` and `CoxBlock` as the persisted types. A record carries:
  - its block's identifier, its own index, the block's record count, and the block row of its first row;
  - its rows' columns: row identifiers, outcomes and trials (logistic) or times, events, weights, offsets and entry
    times (Cox), cluster keys, and covariates row-major;
  - the providers or strata whose first row it holds;
  - for Cox, its slice of the entry order.
- **C.** C = max(1, ⌊256 KiB / 8p⌋) rows, fixed in code: 182 rows at 180 features and 1,310 at 25.
  - No array of a record reaches 512 KiB, and Spark's 4 MiB batches hold about 16 records.
  - C changes no result, so tests may set it lower (engine-private) to cross record boundaries in small data.
- **Building.** The builder buffers and sorts a block's rows as now, then emits records of C rows straight from the
  sorted rows. The block-sized arrays, and the encoder's single block-sized row, are never made.
- **Reading.** Every pass reads blocks through one adapter in its `mapPartitions`.
  - Consecutive records of one block form a view of the block, checked for order and count. Row r is in record
    ⌊r / C⌋, at position r mod C.
  - Kernels read rows through the view instead of flat arrays, with the same arithmetic in the same order.
  - The view goes to the group fold as a block does now, and its records are dropped after its partial.
- **Memory.** While its kernel runs, a task holds one block's records and Spark's current batch, about 4 MiB.
  - The records are whole providers or strata, as now, but in pieces under 512 KiB.
  - At the phase's goal of 100M rows and 8,000 providers, that is about 18 MB in about 70 pieces, against up to
    three 18 MB copies now.
  - A provider or stratum too large for an executor is not addressed here (OI-30; step 4 for unstratified Cox).
- **Unchanged.** Plans, groups, partition keys, persistence through `Storage.persistAt`, fit files and
  `LayoutSummary`. The covariate tests' reduced copies (`dropColumn`) are chunked too.

## 4. The tests' and measures' layouts

- **Today.**
  - `LogisticFE.covariateTests` and `LogisticFE.trainingWorkingSet` still plan with `BudgetedLayout`. The latter
    serves the provider tests (`LogisticProviderTests`) and the standardized measures (`LogisticStandardization`).
  - `predict` is a row-wise expression and builds no blocks. The "predictions" of round 176's record are therefore
    these callers.
  - Since round 160, the fit plans at the target size with reduction groups. Beyond the driver budget, these calls
    therefore build a different layout from their fit's, with blocks that grow with n, up to 256 MB.
    `BudgetedLayout`'s premise, that a fit and every later call find the same plan, no longer holds there.
- **The change.**
  - Both plan as the fit does, through one shared function: `LayoutPlan.create` at the target, then
    `ReductionGroups.plan`.
  - The score test's per-block partials (a packed p×p matrix and p values) fold in groups (`GroupFold`), as pass A's
    do.
  - The likelihood-ratio test's per-block log-likelihoods (one value per block) and the per-provider collects
    (O(m) values) need no change.
- **Bits.**
  - Where the fit's G is 1, as in every golden run and fixture, `BudgetedLayout` already returns the target-size
    plan, so nothing changes.
  - Beyond the budget, these results move to their fit's layout, a change under R1. No golden run or fixture covers
    that regime.
- **Elsewhere.** `BudgetedLayout` stays for the linear and random-effect models (plan §9).

## 5. Chunked compensated sums

### 5.1 The rule

A sum taken today with one Neumaier addition per term, in canonical order, becomes the following:
- The terms are split into chunks of L consecutive terms; the last chunk may be shorter.
- Within a chunk, the terms are added left to right in plain arithmetic, from +0.0.
- Each chunk's sum is added to a Neumaier total, in chunk order.

L = 256, the length the study measured. It is fixed in code (`Summation.ChunkLength`), not an option, so bits cannot
vary between runs; changing L later is itself a change under D-78. The plain sums run over packed arrays, in loops
the JIT can vectorize.

A chunk of positive terms is within (L − 1)u ≈ 2.8·10⁻¹⁴ of its exact sum, relative (u = 2⁻⁵³). The totals add about
2u. Typical errors are far smaller (§5.4).

### 5.2 What changes

Only the sums with p(p+1)/2 terms per row or per event time change, where the cost lies:

| Kernel | Sum | Terms | Chunks |
|---|---|---|---|
| Logistic pass A (`schur`), the variance pass (`varianceSchur`) and the score test (`scoreTerms`), all through `centeredInto` | The centred Schur complement S | One per row | Every L rows of the block, across its providers; the last chunk ends with the block |
| Logistic summaries (`Moments.centred`), which feed only the correlation warning | The centred co-moments | One per row | Every L rows of the block |
| Cox evaluation (`CoxStratum.add`) | The risk-set sums S₂ | One per row of positive weight added, and one per such row removed at its entry time, in sweep order | Every L terms of the stratum. A read at an event time takes the Neumaier total plus the pending chunk's plain sum, without ending the chunk (§5.4) |
| Cox evaluation | The stratum's information | One per event time, or per Efron step | Every L terms of the stratum; the last chunk ends with the stratum |

Everything else keeps its sums:
- the O(1) and O(p) sums of every kernel: the log-likelihood, the provider sums g, h and b, the scores, S₀ and S₁,
  and the Schur right-hand side;
- pass B, the provider variances, the robust passes, the AUC, the Cox baseline, residuals and measures, and `terms`.

So at fixed parameters, the log-likelihood and the scores keep their bits. Only S, S₂, the information and the
co-moments move.

### 5.3 Bit-preserving pieces, in round B

- **The Breslow buffer (OI-104).** `CoxStratum.add` zeroes the Efron buffers at every distinct time, Breslow fits
  included. Breslow fits will skip it.
- **The per-time means.** The information term divides S₁ₗ by S₀ for every pair (j, l). The p quotients will be
  computed once per time: same operands, same division, same bits.

### 5.4 Measured in the sandbox (round 177)

The probe, `step2-probe/Probe.scala`:
- runs the packed S₂ update, p(p+1)/2 terms per row, on 3,000 random rows at 180 features;
- uses one core and JDK 17, and takes the best of five runs after three warm-ups;
- takes today's Neumaier sum as its reference;
- reports "scaled" as the largest difference divided by the largest magnitude, after 95% of the rows are removed
  again, which is truncation's hazard.

| Summation | µs per row, no reads | µs per row, a read every 6 rows | Read error, scaled | After removals, scaled |
|---|---|---|---|---|
| Neumaier per term (today) | 25.9 to 27.0 | 28.2 to 31.3 | Reference | Reference |
| Branch-free TwoSum, with the same corrections | 18.7 to 19.5 | 22.9 to 24.0 | 0: the same bits | 0: the same bits |
| Chunks of 256, ended by each read | 7.9 | 14.6 to 14.9 | 2.1·10⁻¹⁶ | 4.7·10⁻¹⁵ |
| Chunks of 256; reads add the pending chunk | 7.9 | 10.0 | 1.1·10⁻¹⁵ | 7.3·10⁻¹⁶ |
| Plain sums, no compensation | 7.5 to 7.6 | 9.4 to 10.6 | 4.7·10⁻¹⁵ | 8.3·10⁻¹⁴ |

At 50 features (20,000 rows, a read every 6 rows):
- Times: Neumaier 2.32 µs per row; chunks with pending reads 0.92; chunks ended by reads 1.15; plain sums 0.89.
- After removals, plain sums differ by 2.4·10⁻¹³ scaled, beyond D-78's 10⁻¹³. Chunked sums differ by 3.6·10⁻¹⁶ with
  pending reads and 2.0·10⁻¹⁵ ended by reads.

Findings:
- **The branch-free form.** Today's corrections are exact rounding errors, so a branch-free TwoSum reproduces every
  bit. It gains only 1.2 to 1.4 times, and is not proposed.
- **Chunked sums without reads.** They run within 5% of plain sums and 3.3 to 3.4 times faster than today.
- **Chunked sums with reads.** Reads that add the pending chunk keep most of the gain: 2.8 to 3.1 times, against 1.9
  to 2.1 when reads end the chunk. They stay within 1.1·10⁻¹⁵ of today's reads, so §5.2 adopts them for Cox.
- **Compensation under truncation.** Without it, the difference after removals exceeds D-78's bound at 50 features.
- **L = 64.** It was no faster: 8.3 µs per row without reads, and 16.6 with pending reads. Its differences were 3·10⁻¹⁶
  to 1.1·10⁻¹⁵. L = 256 is kept.

These are sandbox measures of one update, not of whole passes; §7 lists the measures that decide.

### 5.5 Amended in round D, before its code (NN-2)

The specifications state today's sums. Round D amends them with its evidence:
- **Cox.**
  - `first-slice.md` §6: S₂ as running Neumaier sums; the per-time contributions as Neumaier sums in time order.
  - `counting-process-baseline.md`: removals as Neumaier additions of negated terms. Its bound on cancellation is
    restated with the probe's measurement and the heavily truncated fixture.
  - `efron-weights-offsets.md`: the running sums and the Efron terms.
- **Logistic.** `fixed-effect-estimation.md` §11: the provider sums keep Neumaier; the Schur complement's sums become
  chunked.
- **ADR-0006.** A second amendment, for chunked compensated sums. D-15's algorithms (ADR-0003) stay unchanged for
  the reductions they cover.
- **Discrepancy rows.** The rows for summation order stay in class D, within the tolerance classes by design.

## 6. Re-baselining under D-78

- **Condition 1, fixtures.** Round D runs every suite of numerics, engine, app and bench, and the Python tests. Every
  fixture comparison must pass with tolerances unchanged.
- **Condition 2, old manifest against new.** The manifest records only converged values, while D-78 asks for
  log-likelihoods, scores and information at fixed β.
  - D-79 proposes that round C first pin, in each golden run, the values §5 changes, at two fixed points: the fit's
    start (Cox: β = 0; logistic: SerBIN's start, every γ at logit(ȳ) and β = 0) and the run's recorded estimates.
  - For Cox, these are the log-likelihood, score and information of one evaluation (`CoxPH.evaluateAt`).
  - For the logistic model, they are pass A's S, right-hand side, log-likelihood and score term, the variance pass's
    S, and the merged co-moments, through an engine-private evaluation added in round C.
  - Round D evaluates the new code at the previous manifest's points. By §5.2, the log-likelihoods, scores and
    right-hand sides must be identical to the bit, and S, the information and the co-moments within 1·10⁻¹³
    scaled.
  - Converged values must agree within T-part, and counts and flags must be identical, as D-78 and D-79 say.
- **Condition 3, R0.**
  - The golden runs' test at 1 and 7 partitions and the one-ulp negative control, as now.
  - Rounds A and B add a test that records of a few rows give the same bits as records of C rows.
  - Round D adds numerics tests that cross several accumulation chunks, against compensated references.
- **Condition 4, the new manifest.** `GoldenRunsSuite` gains a comparison against a previous manifest, named by an
  environment variable. It reports the largest scaled difference for each quantity and checks D-78's bounds.
  - Round D runs it against the old manifest, then writes the new one with `PPROF_GOLDEN_WRITE` in the same commit.
  - The evidence log copies its report.
- **Coverage.** The golden runs' blocks hold tens to a few hundred rows. Each lies within one record, and their sums
  span one to a few accumulation chunks. The tests above cover longer runs.

## 7. Measurements

- **In the sandbox, every round:**
  - all the suites;
  - rounds A, B and D measured with `perf.sh` against the previous build: CPU per row per pass, GC, and task
    result bytes.
- **On the cluster (plan §4, D-76):**
  - CPU per row per pass, against step 0's runs;
  - peak task memory and the share of GC at 50M and 100M rows;
  - S8 (logistic, 100M rows, 180 features) on round A's build or later;
  - round D's re-baselining evidence.

## 8. Risks

- **Vectorization.** The gains depend on the JIT vectorizing the plain loops. The probe ran JDK 17 in the sandbox;
  the cluster's measurements decide.
- **Breadth.** Rounds A and B touch every Cox and logistic kernel that reads rows. They are to change no bit, and the
  40 golden runs, every fixture suite and the record-size test check that.
- **Photon.** The 4 MiB batch belongs to open-source Spark's cache serializer. If the cluster caches through
  another one, its batches may differ; the recorder's cache metrics will show it.
- **Large providers.** Whole providers and strata are still held while their kernel runs (§3).

## 9. Not in step 2

- **Step 3 replaces this Cox change.** The weighted-Gram information replaces the S₂ sweep in Cox's Newton
  evaluations. Step 2's Cox change is therefore an interim: it is measured on its own, and step 3 re-baselines
  again.
- **The later steps.** Per-time aggregates (step 4), profile-likelihood Newton (step 5) and internal scaling
  (step 6) follow their own designs.

## 10. For review

1. **The rounds.** The four rounds and their order, storage before summation.
2. **The chunks.** L = 256, and Cox reads that add the pending chunk (§5.4).
3. **Cox in step 2.** It is an interim before step 3, with its own re-baselining. It is included because step 3
   needs a specification and fixtures first (plan §4).
4. **D-79.** How D-78 applies to the golden runs (§6).

## 11. As built

**Round A, round 178.**
- **Records.** `LogisticChunk` is the persisted record; `LogisticBlock` is the view a pass reads, through
  `LogisticBlocks.assemble`, which checks record order and count. `RowRecords.default(p)` gives C, and tests can set it
  lower. The kernels read rows through `LogisticFE.RowsView`: `Rows` stores them flat, `ChunkedRows` in records.
- **Effects in the task.** Every logistic pass now takes each block's effects in the task, as the AUC pass has since
  round 173 (OI-105). The γ join copied every cached block once more per pass, and it could not attach effects to
  records without repeating them in each. The values and their order are unchanged. A pass's function carries m
  doubles; OI-122 records the size at very large m.
- **Fingerprints.** The tests and measures check the data's fingerprint by hashing rows, one value per block. Before,
  they built whole block summaries, with p×p co-moments per block.
- **Exact direct sums.** The exact direct standardization collects all m effects' sums per block, B × m values, so
  §4 should have listed it. Those sums now fold in groups too, and its driver guard counts R instead of B.
- **Evidence.** The golden runs are unchanged. Records of 7 rows give the fit, the covariate tests, the provider tests
  and the measures to the bit (`LogisticRecordsSuite`), and so does every kernel for records of any size (numerics
  `LogisticFESuite`).

**Round B, round 180.**
- **Records.** `CoxChunk` is the persisted record and `CoxBlock` the view a pass reads, through `CoxBlocks.assemble`,
  which checks record order and count. Each record carries its rows' positions of each stratum's entry order.
- **The kernels.** `CoxStratum`, `CoxResiduals` and `CoxMeasures` read rows through `CoxRowsView`: `CoxRows` stores
  them flat and `ChunkedCoxRows` in records. Their flat-array signatures remain as wrappers. `Moments.columnSumsRows`
  gives the column sums the same way.
- **§5.3's two pieces.** Breslow fits no longer zero the Efron buffers at each distinct time (OI-104). The Breslow
  term's means are computed once per time.
- **Evidence.** The golden runs are unchanged. Records of 7 rows give, to the bit (`CoxRecordsSuite`):
  - the fit, under both tie methods at two block sizes;
  - the baseline, residuals, reference point, both standardizations and the provider tests.

  So does every Cox kernel for records of any size (numerics `CoxRowsSuite`).

**Round C, round 181.**
- **The fixed points.** Each golden run now also records the values of D-79's two fixed points, at the fit's start and
  at its estimates:
  - Cox: the log-likelihood, score and information of one evaluation, through `CoxPH.evaluate` (now visible in its
    package);
  - logistic: pass A's Schur complement, right-hand side, log-likelihood and score term, and the variance pass's
    Schur complement and log-likelihood, through `LogisticFE.fixedPoints`;
  - logistic: also the data's merged co-moments.
- **Shared code.** `fixedPoints` lays the working set out and summarizes it as `fit` does. `fit`'s summaries and
  variance pass are now the helpers `summariesOf` and `variancePass`, which `fixedPoints` shares.
- **The manifest.** Every recorded value is unchanged: 40 runs, 338 lines added and none changed. The new values are
  bit-identical at 1 and 7 partitions.
- **Points.** A run's fixed points are evaluated at its fit's own estimates. Round D's comparison evaluates the new
  code at the previous manifest's estimates instead.

**Round D, round 182.**
- **Amendments first (NN-2).** The Cox specifications (`first-slice.md` §6, `counting-process-baseline.md`,
  `efron-weights-offsets.md` §5), the logistic specification (`fixed-effect-estimation.md` §11) and ADR-0006 state
  the chunked sums. Their discrepancy rows stay in class D.
- **The sums.** `Summation.ChunkLength` is 256 and `ChunkedVector` holds the rule of §5.1. It covers the logistic
  Schur complement (`centeredInto`, one term per row), the co-moments (`Moments.centredRows`), and Cox's S₂ (one term
  per row added or removed, read with the pending chunk) and information (one term per event time or Efron step).
  Each term's arithmetic is unchanged.
- **D-78's comparison.** `GoldenRunsSuite` compares with a previous manifest named by `PPROF_GOLDEN_BASELINE`,
  evaluating each run's estimates point at that manifest's estimates. Against round 181's manifest:
  - every fixed-point log-likelihood, score, right-hand side and score term, the AUC, and every count and flag matched
    to the bit;
  - the fixed points' information, Schur complements and co-moments moved by at most 9.0·10⁻¹⁶ scaled, against
    10⁻¹³;
  - converged values moved by at most 1.7·10⁻¹⁵, against T-part.
- **Tests.** The re-baselined manifest replaces round 181's. Numerics' `ChunkedSumsSuite` checks sums over many chunks
  against exact references, including a truncated Cox stratum and a 1,200-row logistic block.
