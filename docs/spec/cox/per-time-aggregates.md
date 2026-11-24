# Specification: Cox / per-time aggregates for strata larger than a block

- Status: **Approved** (2026-11-23, the maintainer) as D-81. The fixture is built in round 189, the evaluation in
  round 190 (§11).
- Decision references:
  - D-75: performance step 4;
  - D-78: the re-baselining rule, which names per-time aggregates among changes of summation order;
  - D-80: the weighted-Gram information, which this builds on.
- Parity matrix entry: the Cox rows keep their classes. Aggregated strata get no new estimand, only a new layout.

## 1. Purpose and scope

Performance step 4 (plan §4; study §4.2) evaluates a Cox stratum larger than one block from per-time aggregates,
summed over any number of blocks, when its times lie on a grid such as days.

**Today.** A stratum lives whole in one block (OI-30), so an unstratified fit, or a stratum larger than a block, is one
task's work. Its size is bounded by what one executor holds. An unstratified fit of 20M rows at 180 features carries
29 GB of covariates and cannot run.

**Unchanged.** The model, the objective, every output's definition, and the sweep of strata that fit in a block.

**What changes.** How a large stratum's ℓ, U and I are summed. D-78 governs that change.

**Scope.**
- The Newton evaluation, and every fit output it gives: estimates, information, covariance and log-likelihood.
- The baseline hazard, the residuals and the robust variance of aggregated strata, which need the same per-time arrays
  (§6).

Continuous times keep the sweep and OI-03's planned route.

## 2. When a stratum is aggregated

- **K_s.** The planner counts each stratum's distinct exit and entry times, K_s, in one Spark job per fit. It already
  has each stratum's rows.
- **The rule.** A stratum is aggregated when both of these hold:
  - its rows exceed one block, at the target block size;
  - its per-time totals fit the driver's budget, together with every other aggregated stratum's (§7):
    Σ_s K_s·w·8 bytes ≤ `driverBudgetBytes`, with w = p + 3 under Breslow and 2p + 4 under Efron.
- **Otherwise** the stratum keeps the sweep, whole in one block, as now. If it is then too large for its block, the fit
  refuses with a message naming the stratum, its rows and K_s.

At 180 features and 1,826 daily times, one stratum's totals are 2.7 MB under Breslow and 5.3 MB under Efron.

## 3. Layout

- **Splitting.** An aggregated stratum's rows are split over ⌈rows / target rows⌉ blocks by a fixed function of the
  row identifier, the low bits of its 64-bit mix. This takes no sort.
- **Rows in a block.** They keep canonical order: exit time descending, events first, then row identifier.
- **Small strata** keep their whole blocks, so one fit can mix both kinds.
- **Unchanged.** Reduction groups (step 1) and records (step 2) apply as they are.
- **Determinism.** Placement depends only on row identifiers and the plan, so permuted input gives the same bits
  (DIST-6), and so does a different partition count (R0).

## 4. Algorithm

Each Newton evaluation makes one or two passes.

**Notation**, per aggregated stratum:
- Its grid t₁ < … < t_K is the stratum's exit and entry times.
- Δ₀(t) = Σ_{exitᵢ = t} rᵢ − Σ_{entryᵢ = t} rᵢ, and Δ₁(t) likewise with rᵢxᵢ. Then S₀(t) = Σ_{s ≥ t} Δ₀(s) and
  S₁(t) = Σ_{s ≥ t} Δ₁(s), for the risk set entryᵢ < t ≤ exitᵢ.

**Pass 1, on the executors.** Small strata are swept as now. For each block of an aggregated stratum, its rows in
canonical order:
- Δ₀ and Δ₁ are added at the row's exit time, and subtracted at its entry time.
- Each event of positive weight adds to D(t) and d(t); under Efron it also adds to T₀(t) and T₁(t).
- The events' Σwη and Σwx are summed once per stratum, not per time.

A block's totals are folded within its group in block order (step 1). The groups' totals are then folded by a shuffle
keyed by stratum and time, in group order, as `CoxMeasures` folds its per-time totals. The driver collects
Σ_s K_s·w numbers.

**On the driver**, for each aggregated stratum:
- the suffix sums give S₀ and S₁ at its event times, as Neumaier sums in descending time;
- ℓ, U, the hazard increments h_t, the Efron corrections ψ_t and the rank-one sum M are computed with the formulas of
  first-slice §2, efron-weights-offsets §2 and weighted-gram.md §3;
- the cumulative hazards H keep their sums and compensations apart (weighted-gram.md §4).

**Pass 2, on the executors**, only if a stratum is aggregated:
- Each task gets every aggregated stratum's event times, Hˢ, Hᶜ and ψ: O(K) numbers in the pass's function.
- For each row, Λᵢ is the compensated difference and ωᵢ = rᵢ(Λᵢ − ψᵢ), and ωᵢxᵢxᵢᵀ is added to a chunked sum G.
- G folds per group, as evaluation partials do now, and the stratum's information is G − M.

**Step halving.** A halved candidate needs only ℓ, so pass 1 would do for it. That is OI-125, deferred to step 6.

## 5. Numerical analysis

- **Agreement.** The study measured 60,000 rows over 1,825 times, rows in random order: the aggregates agreed with the
  sorted sweep within 7·10⁻¹⁶ for ℓ, 8·10⁻¹⁵ for U and 6·10⁻¹⁵ for I.
- **The sums.** A block's per-time totals hold a few rows each, as plain sums. Folds across blocks and groups, and the
  suffix sums, are Neumaier sums.
- **Truncation.** S₀(t) nets exits against entries. Under heavy truncation that cancels as the sweep's removals do, and
  the Neumaier suffix sums bound it the same way (counting-process-baseline).
- **Late entries.** Pass 2's Λᵢ is weighted-gram.md's compensated difference.
- **Layout dependence.** An aggregated stratum's results depend on how many blocks it spans: R1, within T-part across
  layouts. They are bit-identical for a given layout: R0.

## 6. Outputs of aggregated strata

| Output | How |
|---|---|
| Estimates, information, covariance, ℓ | From the evaluation (§4) |
| Baseline hazard | From the driver's per-time hazards: the formulas of counting-process-baseline and efron-weights-offsets |
| Martingale and score residuals, robust variance | Per row, from the broadcast per-time arrays: Λᵢ and ψᵢ as in pass 2, the means m̄ for score residuals |
| Reference point | Unchanged: block sums, independent of layout |
| Standardized measures, provider tests | Unchanged: they lay out by provider |

Anything else that needs a stratum's rows in one block refuses an aggregated stratum, with a message, until it is
specified.

## 7. Memory, driver and shuffle

- **Driver.** The per-time totals are Σ_s K_s·w·8 bytes, and the budget of §2 bounds them. On the driver, M costs about
  K·p² multiply-adds, 30M at 1,826 times and 180 features.
- **Shuffle.** Pass 1 shuffles each group's totals once: R·Σ_s K_s·w·8 bytes, about 1.3 GB at 500 groups under Breslow,
  against the 29 GB a 20M-row pass reads.
- **Pass 2.** Its function carries O(Σ_s K_s) numbers: four arrays, about 58 KB at 1,826 times. Its partials are p×p per group, as
  now.
- **Executors.** Blocks are of the target size whatever the stratum's size.

## 8. Validation

- **Function level.** Aggregated against swept, on the same data at fixed β: ℓ, U and I within T-fn, Breslow and Efron,
  with ties, late entries, zero weights and offsets. A test option forces aggregation by setting a block size small
  enough to split every stratum.
- **Fixtures.** Every Cox case fit with aggregation forced, against the R and pprof_py references at their classes. A
  new case, unstratified and gridded with truncation, weights and offsets, holds about 5,000 rows over 60 days, so that
  it spans many blocks at test block sizes. It comes from the pinned reference environment (§9).
- **Golden runs.**
  - The existing runs keep their bits, because their strata fit in a block.
  - New runs pin aggregated fits of fixture cases at a forced small block size.
  - R0 is checked at 1 and 7 partitions, and DIST-6 with permuted input.
- **Exit, on the cluster** (plan §4):
  - an unstratified 20M-row fit using every worker;
  - agreement with the stratum sweep within T-fn, at a size both can run.

## 9. Rounds

1. **This specification** (round 188), for approval as D-81.
2. **The fixture.** The unstratified, gridded, truncated case is generated with the pinned environment and added to
   the Cox fixture suites, which the present sweep must pass. No bit of a fit changes.
3. **The build.**
   - The specification amendments first (NN-2).
   - Then the planner's time counts and stratum splitting, the two passes and the driver's per-time evaluation.
   - Then the outputs of §6, the tests of §8, new golden runs, and a sandbox measurement.
4. **The cluster's measurement** at step 4's exit.

## 10. For review

1. **The rule (§2).** Aggregate a stratum when it exceeds a block and its per-time totals fit the driver's budget.
   Otherwise refuse a stratum too large for its block.
2. **The layout (§3).** Rows split by row identifier, with no sort.
3. **The outputs (§6).** The baseline, residuals and robust variance are built in step 4; anything else refuses
   aggregated strata.
4. **Validation and rounds** (§8, §9).

## 11. As built

**Round 189, the fixture.**
- **Generation.** `lt-unstratified` (seed 8) holds 5,000 rows, 3 features, one stratum, times 1 to 60, weights,
  offsets and truncation. It was generated with the pinned environment: R 4.3.3 with survival 3.5-8, and pprof_py at
  `9320766`. In the same run, the 28 files of the seven existing Cox cases matched the committed manifest byte for byte.
- **What it holds.** Its grid has 60 times: 60 exit times and 50 entry times, all on the grid. It has 56 event times
  and 3,675 events; the largest tie has 401. 1,914 rows enter after time 0.
- **Calibration and suites.** `docs/parity/cox-calibration.md` gains its 22 rows, all within class. The case joins
  every Cox suite's list that holds the unstratified cases, and the golden runs. The present sweep passes it.

**Round 190, the evaluation.**
- **Planning.** One job counts the distinct exit and entry times of every stratum with more rows than a block
  (`cox.grid-counts`). Strata are taken in key order while their per-time totals fit `driverBudgetBytes`, and a
  second job collects their grids (`cox.grid`).
- **Layout.**
  - A split stratum takes ⌈rows / target⌉ blocks, ahead of the shared ones.
  - Its rows reach them by `xxhash64` of their content and identifier. That is §3's "row identifier", extended
    because identifiers are 0 without `rowId` or `rowKey`.
  - The layout summary gives split blocks the plan's even estimate of their rows.
- **Evaluation** (`CoxAggregation`).
  - Pass 1 (`cox.evaluate`) emits each reduction group's swept partial and its aggregated strata's totals, in records
    of 256 grid times. A shuffle folds those by stratum and record, in group order.
  - The driver evaluates each stratum (`CoxAggregates.evaluate`). Pass 2 (`cox.gram`) forms G.
  - The sum takes the swept partials in group order, then the aggregated strata in group order.
  - A fit without aggregated strata takes the earlier path, bit for bit.
- **The summary.** A split stratum counts once among strata without events.
- **Departures from §2 and §6, until round 191.**
  - A robust or clustered fit is not aggregated: it keeps today's whole-stratum layout and its guard, rather than
    refusing.
  - The baseline, residuals, reference point and measures build their working sets as before, with whole strata.
- **The budget.** The swept partials and the per-time totals are each bounded by `driverBudgetBytes`, so one pass
  may bring up to twice the budget to the driver.
- **A departure from §5 (OI-126).** Pass 1 adds each row to the per-time totals with Neumaier sums per element, where §5
  describes plain sums per block folded with Neumaier sums. That is more accurate and slower.
- **Measured** in the sandbox (`perf-results.md`, "Round 190"): the log-likelihood's bits match the one-block sweep. On
  one core the two passes cost 2.2 to 2.4 times its CPU.
