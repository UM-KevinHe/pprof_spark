# 4. From mathematics to code

[Tutorial home](README.md) · previous: [3. Estimation and inference](03-estimation-and-inference.md) ·
next: [5. Distributed execution](05-distributed-execution.md)

This chapter connects the formulas of chapters 2 and 3 to the source at `efb6329`. It starts with
the heart of the implementation, the risk-set sweep in `CoxStratum.add`, and then describes every
algorithmic component in the same format: the problem it solves, its formulation, inputs and
outputs, the sequence of calculations, the code, its assumptions and failure modes, and how it is
validated. Chapter 5 then shows how these components run on a cluster.

## 4.1 Where the statistics lives

| Concern | Module and file | Spark? |
|---|---|---|
| ℓ, U, I for one stratum | `numerics` · [kernels/CoxStratum.scala](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala) | No: pure functions on arrays |
| Residuals for one stratum | `numerics` · [kernels/CoxResiduals.scala](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxResiduals.scala) | No |
| National baseline and provider measures | `numerics` · kernels/CoxMeasures.scala | No |
| Newton–Raphson, Cholesky, compensated sums, normal distribution | `numerics` · Newton.scala, Cholesky.scala, Summation.scala, Normal.scala | No |
| Validation, layout, working set, passes, reduction, inference, results | `engine` · [cox/](../../../engine/src/main/scala/pprof/spark/engine/cox/), [layout/](../../../engine/src/main/scala/pprof/spark/engine/layout/), [backend/](../../../engine/src/main/scala/pprof/spark/engine/backend/) | Yes, Dataset API only |
| Job runner | `app` · [CoxJob.scala](../../../app/src/main/scala/pprof/spark/app/CoxJob.scala) | Yes |
| Python access | [python/pprof_spark/cox.py](../../../python/pprof_spark/cox.py) through py4j | Delegates to the JARs |

The rule (NN-6, ARCH-1) is that every numerical formula lives in a pure, Spark-free function with its
own unit tests, and the engine only moves data to those functions and combines their results. A
statistical error is therefore almost always in `numerics`, and a distribution error in `engine`.

## 4.2 Symbols and variables in `CoxStratum.add`

[`CoxStratum.add`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L37-L210)
computes one stratum's contributions to ℓ, U and I at a given β and adds them to a running total.

| Mathematics | Code | Notes |
|---|---|---|
| xᵢ − c (centered covariate) | `xc(j) = x(r * p + j) - center(j)` | Covariates are row-major in one array; `center` holds the column means from the summary pass |
| ηᵢ | `eta = linear + offset(r)` | `linear` = Σⱼ xcⱼβⱼ; offsets are not centered |
| rᵢ = wᵢe^{ηᵢ} | `rw = w * StrictMath.exp(eta)` | Kept in `riskScore` so that removal subtracts the identical value |
| S₀, S₁, S₂ | `s0`, `s1`, `s2` | `NeumaierSum`, `NeumaierVector`; S₂ as a packed upper triangle of p(p+1)/2 entries |
| d, d_w | `d`, `dw` | Events with positive weight only (X-013) |
| Σ_D wᵢηᵢ, Σ_D wᵢ(xᵢ − c) | `etaEvents`, `eventSum` | Plain sums, reset at each time |
| D₀, D₁, D₂ | `tied0`, `tied1`, `tied2` | Efron only; plain sums, reset at each time |
| m = d_w/d | `meanWeight` | Efron with d ≥ 2 |
| k/d, Aₖ, Bₖ/Aₖ, Cₖ | `f`, `a`, `means(j)`, `c` | The loop `step = 1 to d` |
| The stratum's ℓ, U, I | `stratum.value`, `stratum.score`, `stratum.information` | Neumaier totals, added to `totals` once per stratum (line 209) |

Packed storage: entry (i, j) with i ≤ j sits at index i·p − i(i−1)/2 + (j − i), row by row
(`Moments.packedIndex`). For p = 3 the six entries are (0,0), (0,1), (0,2), (1,1), (1,2), (2,2).

## 4.3 The risk-set sweep

Rows of a stratum arrive in **canonical order**: exit time descending, events before censored rows
at the same time, then row identifier (section 4.4, C3). `entryOrder` lists the same rows by entry
time descending. In outline:

```text
S0, S1, S2 <- 0                      running sums describing the current risk set
removal <- first row of entryOrder
for each distinct exit time t, from the latest to the earliest:
    for every row exiting at t (with positive weight):
        rw <- w * exp(eta);  S0 += rw;  S1 += rw * xc;  S2 += rw * xc * xc'     (joins)
        if it is an event: update d, dw, sum of w*eta, sum of w*xc, and D0, D1, D2 for Efron
    while the next row of entryOrder has entry >= t:
        S0 -= its rw;  S1 -= rw * xc;  S2 -= rw * xc * xc'                      (leaves)
    if d > 0: add the contributions at t to the stratum's l, U, I (Breslow or Efron)
add the stratum's l, U, I to the block's totals
```

Why this is correct and efficient:

- **The risk set at t is {i : aᵢ < t ≤ bᵢ}.** Sweeping exit times downward, a row joins the sums when
  the sweep reaches its exit time, so after the group at t has been added the sums hold every row with
  bᵢ ≥ t. Removing every row with aᵢ ≥ t then leaves exactly aᵢ < t. Because t only decreases, the
  removal pointer only moves forward.
- **A removed row has always been added.** If aᵢ ≥ t, then bᵢ > aᵢ ≥ t, so its exit time was
  processed earlier. A row entering exactly at t is removed before t's events count, matching
  "entering at t is not at risk at t".
- **Ties share one risk set.** All rows exiting at t, events and censored rows alike, join before
  any contribution at t is computed
  ([lines 76–131](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L76-L131)),
  so tied events see the same S₀ and a row censored at t counts as at risk at t.
- **Each row is added once and removed at most once**, so a pass over a stratum of nₛ rows costs
  O(nₛ·p²): the S₂ update dominates. A direct evaluation of each risk set would cost O(nₛ·K).
- **Without an entry column**, every entry is 0 and every time is positive, so nothing is removed
  and the sums are exactly those of right-censored data. The test "an entry column of zeros
  reproduces the right-censored fit bit for bit" checks this.
- **Zero-weight rows** are skipped both when joining and when leaving
  ([lines 78 and 134](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L132-L152)),
  so they are exactly absent (X-013).

The Breslow branch, also used for Efron when d = 1,
([lines 155–172](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L155-L172))
adds ℓ += Σwη − d_w·log S₀, U += Σw·xc − d_w·S₁/S₀ and I += d_w·(S₂/S₀ − (S₁/S₀)(S₁/S₀)ᵀ). The Efron
branch ([lines 173–206](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L173-L206))
adds the event terms once and then loops `step = 1 to d` with f = step/d,
a = (S₀ − D₀) + f·D₀ and the matching means and second moments, exactly the formulas of chapter 2,
section 2.9.

**Numerical choices in the kernel.**

- `StrictMath.exp` and `StrictMath.log` give the same bits on every JVM (§8.1); `java.lang.Math`
  could differ by one unit in the last place between JVMs and break bitwise reproducibility.
- The running sums are Neumaier-compensated. Adding and later subtracting the same terms can lose
  relative accuracy when the risk set becomes much smaller than what was removed (heavy
  truncation); the compensation term recovers the lost low-order bits. The per-time event sums are
  small and use plain sums.
- The contributions of a stratum's event times are accumulated in sweep order and added to the
  block's totals once, so the result depends only on the canonical order (R0).
- There is no shift of η by its maximum within the stratum: overflow of exp(η) makes ℓ non-finite,
  which step halving treats as a decrease (spec first-slice §6). Chapter 3 and review finding F-03
  show where this falls short.

## 4.4 The components

Each card below follows the same order: problem, formulation, inputs and outputs, steps, code,
assumptions and failure modes, validation.

### C1. Input validation

- **Problem.** Reject invalid input before any computation, report every problem at once, and never
  print a data value or drop a row silently (NN-7).
- **Formulation.** For each role, counts of nulls, non-finite values and out-of-range values;
  uniqueness of the row identifier as rows − nulls − distinct values; the number of events with
  positive weight.
- **Inputs and outputs.** A DataFrame and a `CoxSpec` → a `CoxInput`: the projected frame with
  internal columns (stratum key, time, event, a feature array, row id, weight, offset, entry,
  cluster), its row and event counts, and whether the stratum key is text.
- **Steps.** A schema check on the driver (types, missing columns); one aggregation job collecting
  all counts; problems turned into an `InvalidInputException`; the projection, with defaults for
  absent roles (row id 0, weight 1, offset 0, entry 0, cluster null).
- **Code.** [`CoxValidation.validate`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxInput.scala#L63-L159),
  [`valueCounts`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxInput.scala#L191-L256).
- **Assumptions and failure modes.** The schema is checked eagerly because Spark Connect resolves
  names late. Integer counts may use Spark's built-in aggregates (DIST-7 exempts them). The row
  identifier's distinct count adds two small exchanges to the job (chapter 5). Without a row
  identifier every row gets id 0 and the canonical order falls back to comparing values.
- **Validation.** `CoxPHSuite`: "invalid times, events, covariates, weights and offsets fail with
  counts, never values", "entry times must be finite and below the exit time, with counts reported",
  "no events, or only zero-weight events, fails (X-012)".

### C2. Layout planning

- **Problem.** Decide, deterministically and independently of the cluster, which strata go into which
  logical block (DIST-5).
- **Formulation.** With T = ⌊targetBlockBytes / (8·(p + 4))⌋ rows per block (104,857 for p = 1,
  37,449 for p = 10 at the 4 MiB default): strata with more than T rows get a block each, first, in
  key order; the others fill ⌈(their rows)/T⌉ shared blocks, largest stratum first, each into the
  currently least-loaded block (ties: lowest block identifier).
- **Inputs and outputs.** Stratum sizes → a `LayoutPlan` of placements (stratum key, group index in
  key order, rows, block identifier), the block count and the number of oversized strata.
- **Steps.** `GroupSizes.collectGroupSizes`: one job grouping by stratum and collecting the counts,
  guarded by `maxGroupsOnDriver` (2,000,000). `LayoutPlan.create` on the driver. Then two guards: no
  stratum above `maxStratumRows` (1,000,000), and B partials within `driverBudgetBytes` (64 MiB).
- **Code.** [GroupSizes.scala lines 17–26](../../../engine/src/main/scala/pprof/spark/engine/layout/GroupSizes.scala#L17-L26),
  [`LayoutPlan.create`](../../../engine/src/main/scala/pprof/spark/engine/layout/LayoutPlan.scala#L80-L110),
  [CoxPH.scala lines 322–337](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L322-L337),
  [`BlockOptions.targetRowsPerBlock`](../../../engine/src/main/scala/pprof/spark/engine/backend/BlockOptions.scala#L29-L30).
- **Assumptions and failure modes.** A stratum is never split: TimeRange, which would split one,
  does not exist (OI-03, OI-30), so an unstratified fit is a single block. Until round 77 the `maxStratumRows`
  message said TimeRange would arrive "in Phase 1b"; it now says TimeRange is not implemented
  (review finding F-04).
  The guards fail with messages naming the option to change.
- **Validation.** `LayoutPlanSuite`; `CoxPHSuite`: "a stratum larger than maxStratumRows fails and
  points to TimeRange" and "R1: another block layout stays within T-part".

### C3. Working set and canonical order

- **Problem.** Gather each block's rows into a single record of primitive arrays, in an order that
  depends only on the data, and keep it for every later pass.
- **Formulation.** Canonical order: group index ascending; exit time descending; events first; row
  identifier ascending; then covariates compared one by one; weight; offset; entry; cluster. Negative
  zeros become positive zeros first, because they compare equal and would otherwise leave their order
  to the input (DIST-6). Per stratum, `entryOrder` sorts row positions by entry time descending, ties
  by position.
- **Inputs and outputs.** The validated frame and the plan → a `CoxWorkingSet` wrapping a persisted
  `Dataset[CoxBlock]`. A `CoxBlock` holds `groupIndex` and `groupStart` (where each stratum starts)
  and the arrays `rowId`, `time`, `event`, `weight`, `offset`, `x` (row-major), `entry`,
  `entryOrder` and `cluster`.
- **Steps.** A broadcast join of the rows with the small placement table (stratum key → block, group
  index); conversion to `CoxRow`; `groupByKey(blockId)` and `mapGroups(CoxBlockBuilder.build)`: the
  one shuffle of row data; `persist` at `MEMORY_AND_DISK`; `count()` to materialize it and check the
  block count against the plan.
- **Code.** [`canonicalOrder`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxBlocks.scala#L53-L90),
  [`CoxBlockBuilder.build`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxBlocks.scala#L95-L154),
  [`CoxWorkingSet.build`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxBlocks.scala#L189-L223).
  The working set is released in a `finally` block
  ([CoxPH.scala line 364](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L364)).
- **Assumptions and failure modes.** The builder holds a whole block's rows as objects while it
  sorts, so an oversized stratum costs executor memory well beyond its arrays. Release is
  non-blocking.
- **Validation.** `CoxPHSuite`: "blocks hold rows in canonical order: time descending, events first,
  then row id" (including negative zeros) and "R0: row order and partitioning leave every bit
  unchanged, with weights and offsets".

### C4. Summary pass, centering and fingerprint

- **Problem.** Check that the blocks hold exactly the validated rows, compute the centering constants
  deterministically, and fingerprint the data so later calls can check they receive the same data
  (API-3).
- **Formulation.** Per block: rows; event rows; strata without a positive-weight event; column sums
  of the covariates (pairwise summation); and a fingerprint that is the sum of per-row hashes, so it
  does not depend on order. Center = column sums combined in block order with Neumaier sums, divided
  by n.
- **Inputs and outputs.** Blocks → `Prepared(center, rows, events, strataWithoutEvents, fingerprint)`.
- **Steps.** One job: `map(CoxKernel.summary)`, `collect`, sort by block identifier; compare the row
  and event counts with validation's; reduce the column sums.
- **Code.** [`CoxKernel.summary`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L79-L117),
  [CoxPH.scala lines 340–363](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L340-L363).
- **Assumptions and failure modes.** A count mismatch raises `IllegalStateException`: it would mean a
  defect in the build, not bad data. The center is the unweighted mean over all rows, zero-weight
  rows included; any constant would do (section 2.7).
- **Validation.** Every fit runs it. `CoxStratumSuite`: "centering the covariates leaves the results
  unchanged".

### C5. The risk-set kernel

- **Problem.** Compute one block's contributions to ℓ, U and I at a given β.
- **Formulation.** Chapter 2, sections 2.4 to 2.9, summed over the block's strata in key order.
- **Inputs and outputs.** A block, β, the center, the tie method → `CoxPartial(blockId, value,
  score, information)`, with the information packed.
- **Steps.** For each stratum of the block, `CoxStratum.add` (section 4.3) into one `CoxTotals`.
- **Code.** [`CoxKernel.partial`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L120-L148),
  [`CoxStratum.add`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L37-L210).
- **Assumptions and failure modes.** Rows of each stratum in canonical order and `entryOrder`
  consistent with them; entry below exit (validated). Overflow gives a non-finite value. A stratum
  without positive-weight events contributes exactly nothing.
- **Validation.** `CoxStratumSuite`: closed forms at β = 0 for both tie methods; score and information
  against finite differences with weights, offsets and truncation; Efron with untied events equals
  Breslow; a zero weight equals an absent row; centering invariance; strata without events; risk sets
  under truncation; entries at or below zero leave right-censored results unchanged bit for bit.
  `CoxPHSuite` function-level parity: ℓ, U and I at β = 0 and at the fixtures' fixed β, both tie
  methods, all six fixtures, against pprof_py and R under T-fn.

### C6. Deterministic reduction

- **Problem.** Combine the B block partials into ℓ, U, I so that the result does not depend on which
  task finished first.
- **Formulation.** Sort partials by block identifier and add them element by element with Neumaier
  sums.
- **Inputs and outputs.** B partials → an `Evaluation(value, gradient, information)`.
- **Steps.** One Spark job per evaluation: `map(CoxKernel.partial)` over the persisted blocks,
  `collect`, then the ordered reduction on the driver. β and the center reach the executors by
  closure capture (O(p) values).
- **Code.** [`CoxPH.evaluate`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L445-L465),
  [`OrderedReduction.reduce`](../../../engine/src/main/scala/pprof/spark/engine/backend/Blocks.scala#L81-L96),
  [`NeumaierSum`, `NeumaierVector`](../../../numerics/src/main/scala/pprof/spark/numerics/Summation.scala#L65-L123).
- **Assumptions and failure modes.** Partials travel to the driver: B·(1 + p + p(p+1)/2) doubles per
  pass, guarded at planning time (C2). Built-in SQL aggregates are not used for these sums, because
  their order depends on scheduling (DIST-7).
- **Validation.** The R0 tests (bitwise equality across row orders and partition counts); the probe
  adds partition counts 1 and 7 and a reversed, repartitioned input (chapter 5).

### C7. Newton iteration and step control

- **Problem.** Maximize ℓ.
- **Formulation and steps.** Chapter 3, sections 3.1 to 3.5.
- **Inputs and outputs.** An `evaluate` function, β⁽⁰⁾ = 0, `NewtonOptions` → `NewtonResult(beta,
  evaluation, initial, iterations, halvings, converged, message, log)`.
- **Code.** [`Newton.maximize`](../../../numerics/src/main/scala/pprof/spark/numerics/Newton.scala#L50-L108).
- **Assumptions and failure modes.** Chapter 3, section 3.6.
- **Validation.** `NewtonSuite`: one-step convergence on a quadratic; convergence tested before
  halving (X-010); halving of a lowering step; non-finite values; aliasing; the iteration cap; the
  relative-change formula. `CoxPHSuite` lockstep parity: iterates before the final one against
  pprof_py's under T-iter; default fits stop at pprof_py's iteration count.

### C8. Cholesky factorization and aliasing

- **Problem.** Solve I·Δ = U, invert I, and detect singularity.
- **Formulation.** I = LLᵀ, column by column. Column j is aliased when its pivot (the diagonal left
  after eliminating earlier columns) is not above `tolerance` × the largest diagonal of I; the column
  is skipped and elimination continues, so every aliased column is reported at once.
- **Code.** [`Cholesky.factor`](../../../numerics/src/main/scala/pprof/spark/numerics/Cholesky.scala#L19-L79),
  [`solve`](../../../numerics/src/main/scala/pprof/spark/numerics/Cholesky.scala#L98-L127),
  [`inversePacked`](../../../numerics/src/main/scala/pprof/spark/numerics/Cholesky.scala#L130-L169).
  `CoxPH.fit` turns `AliasedException` into `CoxAliasingException` with covariate names.
- **Assumptions and failure modes.** A NaN pivot counts as aliased, because the test is
  `!(pivot > threshold)`. The rule is relative to the largest diagonal, so badly scaled covariates
  can be reported as aliased (chapter 3, section 3.6).
- **Validation.** `CholeskySuite`: solve and inverse; the tolerance equals 2⁻³⁹ exactly; collinear
  and constant columns; pivots just below and above the threshold. `CoxPHSuite`: "collinear and
  constant covariates fail and are named (X-011)".

### C9. Inference

- **Problem.** Standard errors, Wald statistics, p-values, intervals.
- **Formulation and code.** Chapter 3, section 3.7;
  [CoxPH.scala lines 145–170](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L145-L170).
- **Validation.** `CoxPHSuite`: "Wald statistics, p-values and intervals (T-test, T-p, T-fn)" for
  every fixture and tie method; p-values below 1e-10 are compared on the log₁₀ scale (T-p).

### C10. Baseline hazard

- **Problem.** Λ̂₀ₛ(t) and Ŝ₀ₛ(t) per stratum at β̂.
- **Formulation.** Chapter 3, section 3.10.
- **Inputs and outputs.** The training data (fingerprint checked) and a `CoxFit` → a persisted
  DataFrame (stratum, time, hazard_increment, cumulative_hazard, survival), never collected (DIST-1).
- **Steps.** Rebuild the working set (C1–C4); per block and stratum, `CoxStratum.baseline` sweeps
  exactly like `add` but with S₀ only and uncentered covariates, then the increments are reversed to
  ascending time and accumulated with a Neumaier sum; the stratum keys are joined back through a
  broadcast of the placement table; the result is persisted and counted.
- **Code.** [`CoxStratum.baseline`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L218-L287),
  [`CoxKernel.baseline`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L153-L188),
  [`CoxPH.baseline`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L238-L268).
- **Assumptions and failure modes.** Fails when the data's fingerprint differs from the fit's.
  Times with only zero-weight events are not event times. The caller may unpersist the table.
- **Validation.** `CoxBaselineSuite`: against pprof_py's raw baseline and, after the X-014 factor,
  its public one, evaluated at pprof_py's estimates so that X-010 does not enter; against R's
  `basehaz`; R0 across row orders and partitions; API-3; persistence (format version 2).

### C11. Residuals

- **Problem.** Martingale, score and dfbeta residuals per row, including under truncation and Efron
  ties.
- **Formulation.** Chapter 3, section 3.12.
- **Steps.** `CoxResiduals.stratum` sweeps downward like `add`, recording for each event time the
  hazard increment h, its covariate-weighted counterpart g = h·x̄, and for Efron the dying rows'
  shares; running totals H and G over ascending event times then give each row its sums over
  (aᵢ, bᵢ] as differences, found by binary search for the last event time at or before bᵢ and aᵢ.
  The engine multiplies by wᵢV for dfbeta.
- **Code.** [`CoxResiduals.stratum`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxResiduals.scala#L19-L212),
  [`CoxKernel.residuals`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L193-L240),
  [`CoxPH.residuals`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L275-L304).
- **Assumptions and failure modes.** Needs `CoxSpec.rowId`, so results can be joined back to input
  rows; checks the fingerprint. The output stays distributed, keyed by row identifier.
- **Validation.** `CoxResidualsSuite`: against pprof_py and R at their estimates (T-res) for every
  fixture and tie method; weighted residuals sum to zero per stratum and to the score; API-3.

### C12. Robust variance

- **Problem.** B = Σ_c s_c s_cᵀ, then V B V.
- **Steps, per row.** One pass: each block adds (wᵢUᵢ)(wᵢUᵢ)ᵀ over its rows in canonical order;
  partials are collected and reduced in block order.
- **Steps, clustered.** Each row's wᵢUᵢ is emitted with its cluster key, block and position; one
  shuffle groups rows by cluster, and each cluster sums its rows in (block, position) order; each
  cluster is assigned one of 256 buckets by a fixed hash of its key; a second, much smaller shuffle
  groups clusters by bucket, and each bucket adds its clusters' s_c s_cᵀ in key order; the 256
  partials are collected and reduced in bucket order. Nothing depends on Spark's partitioning.
- **Code.** [`CoxPH.robustMeat`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L372-L412),
  [`CoxKernel.robustPerRow`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L293-L318),
  [`weightedScores`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L271-L290),
  [`clusterSum`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L321-L328),
  [`bucketOuter`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L331-L348),
  [`sandwich`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L415-L442).
- **Assumptions and failure modes.** Cluster keys are compared as strings. A cluster's rows meet in
  one task, so a very large cluster is a hot spot (OI-45). The specification described one shuffle
  until round 77 corrected it (review finding F-09).
- **Validation.** `CoxRobustSuite`: against R everywhere and pprof_py except where X-015 applies;
  clusters of one row equal per-row variance; renaming clusters changes nothing; R0 across row
  orders and partitions; the warning; null cluster keys; persistence (format version 3).

### C13. Prediction

- **Problem.** η, relative hazard, cumulative hazard and survival for any rows.
- **Steps.** η is a Spark column expression, xᵀβ̂ + o. For the cumulative hazard, the rows are
  unioned with the baseline table's steps, a window partitioned by stratum and ordered by (time,
  kind) carries the last cumulative hazard forward (`last(..., ignoreNulls = true)`), the baseline
  rows are filtered out, and the value is multiplied by exp(min(η, 700)). Ordering baseline rows
  before data rows at the same time makes the step function right-continuous.
- **Code.** [CoxPrediction.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPrediction.scala#L41-L72).
- **Assumptions and failure modes.** Rows with missing or non-finite inputs, or a stratum missing
  from the baseline, fail with counts. All rows of a stratum meet in one window partition (OI-43).
- **Validation.** `CoxBaselineSuite`: all four quantities on pprof_py's evaluation grid (T-base), and
  failures for unknown strata and invalid values.

### C14. Persistence

`CoxFitIO` (format version 3) writes a self-describing directory through Spark: metadata with every
double stored as its 64-bit pattern, because `Double.toString` differs between JDK 17 and 21 (§6.10),
and the baseline table as Parquet when given. Save then load is bitwise exact (PERS-1); a directory
with another format version fails with a message naming both (PERS-2). Tested in `CoxFitIOSuite`,
`CoxBaselineSuite` and `CoxRobustSuite`.

### C15. Provider measures and tests

`CoxMeasures.standardized` lays the training data out by provider (reusing the fit's layout when the
provider column is the stratum column), finds the largest η with one pass, sums exp(η − max η) by
exit and entry time in each block, shuffles those per-time partials by time and adds them in block
order, builds the national Breslow baseline on the driver (K-scale, guarded by `maxGroupsOnDriver`),
ships it to the executors by closure capture, and computes each provider's observed and expected
events, person-time and direct expectation within its block
([CoxMeasures.scala lines 33–119](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxMeasures.scala#L33-L119)).
Case weights are not used and the baselines are Breslow whatever the fit's ties, as in pprof_py.
`CoxProviderTests.test` calls `standardized` and adds the exact Poisson and mid-p tests per provider.
Specification: [provider-workflows.md](../../spec/cox/provider-workflows.md).

### C16. Job runner and Python

`CoxJob` reads a JSON run specification and calls `CoxPH.fit`, `CoxPH.baseline`, `CoxPH.residuals`,
`CoxMeasures.standardized` and `CoxProviderTests.test` as requested
([CoxJob.scala lines 60–82](../../../app/src/main/scala/pprof/spark/app/CoxJob.scala#L60-L82)); its
outputs equal the library's bit for bit (`CoxJobSuite`). Guide:
[docs/guide/cox-job.md](../../guide/cox-job.md). The Python `pprof_spark.CoxPH` delegates every
computation to the JARs through py4j and refuses Spark Connect sessions, pointing to the job runner
instead ([docs/guide/python.md](../../guide/python.md)).

## 4.5 What the code does not do

The README's status table lists the unimplemented features. Three are worth repeating here, because
they are easy to assume: there is no infinite-coefficient detection (chapter 3, section 3.6), no
covariate scaling or per-stratum η shift beyond centering (review finding F-03), and no splitting of
a large stratum across blocks (chapter 8).
