# Performance step 1: reduction in fixed groups

Status: design for plan step 1 (D-75, as amended by D-76), round 158. Rounds 159 and 160 wire it into Cox and the
logistic model; the step's exit is measured on the cluster after that.

## 1. Why

- **Measured (round 157).** At 20M rows and 180 features, both models fail in the working-set build. Blocks grown to
  64 MB exhaust executors with 2.2 GB heaps (OI-115).
- **The cause.** `BudgetedLayout` grows blocks until one partial per block fits the 64 MB driver budget. Block size,
  and with it task memory, therefore grows with n (plan §2.2). At 100M rows and 180 features, no growth fits.
- **The change.** Blocks stay at the target size (4 MB), and their partials are reduced in fixed groups on the
  executors. The driver receives one partial per group.

## 2. Groups

- **Blocks.** There are B blocks, planned as today at the target size (`LayoutPlan.create`, unchanged: whole strata or
  providers, bin-packed in key order).
- **At most M = ⌊budget / partial bytes⌋ groups.** A partial larger than the budget is refused, as today.
- **Blocks per group.**
  - G = 1 when B ≤ M; otherwise G = ⌈B / M⌉.
  - R = ⌈B / G⌉ ≤ M.
  - Group g holds blocks gG to min(B, (g + 1)G) − 1, consecutive and in order.
- **Reproducibility.** G and R depend only on the plan and the options, and are recorded with the fit.
- **Code.** `layout.ReductionGroups` (round 158).

## 3. Placing groups

- **One shuffle, as now.** Rows go into R partitions by a key k_g of their block's group. The key is chosen so that
  Spark's hash partitioning (the Murmur3 hash of the key modulo R, as `functions.hash` computes it) sends group g to
  partition g.
- **Finding the keys.** Spark computes them itself in one small job: for each partition, the least long in 0 to 32R
  that lands there. The range widens if a partition is missed (`layout.PartitionKeys`, round 158).
- **Building blocks.** Within a partition, rows are sorted by block. Blocks are built in a streaming `mapPartitions`
  with the present builder and canonical row order, holding one block's rows in memory at a time.
- **A group is never split.** Its key is a function of the group, whatever partitioning function runs; a different
  function could only unbalance groups over partitions. The build records how many partitions hold more than one
  group.
- **Persistence.** The working set is persisted as now.

## 4. Passes

- **On the executors.** Each pass is one `mapPartitions`. It computes each block's partial in block order and folds
  them per group with the present compensated reduction (`OrderedReduction`'s Neumaier sums), emitting one partial per
  group. It fails if a block arrives out of order or a group repeats.
- **On the driver.** The R group partials are folded in group order with the same reduction.
- **Per-provider vectors.** Δγ and the variances travel with their group's partial, in block order, m values in total
  per pass.
- **Constraints kept.** No pass shuffles, none uses an RDD, and everything works under Connect.

## 5. Properties

- **Driver bytes.** The bytes sent to the driver per pass stay within the budget for any n, since R ≤ M.
- **Task memory.** One block being built or folded (4 MB of arrays at the target), plus O(p²) accumulators,
  independent of n.
- **R0.** The fold order is block order within a group and group order across groups, both fixed by the plan, so the
  bits are identical for any input partitioning, executor count or core count.
- **NUM-2.**
  - With G = 1, the group fold of one block returns its partial exactly (a Neumaier fold of one value from zero), so
    the driver's fold is today's, bit for bit.
  - Every golden run has G = 1.
  - G > 1 arises only where today's layout grows blocks, so the blocks differ there anyway, and plan §6.3 governs. No
    golden run is affected.
- **Parallelism.** R tasks per pass, each folding G blocks. At 180 features and a 64 MB budget, M = 509. At 100M rows,
  B ≈ 34,700 blocks of 4 MB, G = 69 and R = 503.

## 6. What changes, and how it is tested

- **Growth retired.** `BudgetedLayout`'s growth is retired for Cox and the logistic model.
- **Records.** `LayoutSummary` gains G and R; files written before them read as G = 1.
- **Guard.** `DriverGuards` checks R · partial ≤ budget.
- **Tests.**
  - `ReductionGroupsSuite` (round 158).
  - R0 with G > 1: a tiny budget forcing groups, at partition counts 1 and 7.
  - The golden runs unchanged, and `OperationTagsSuite`.
  - The step's exit on the cluster: bytes to the driver flat across sizes; 20M, 50M and 100M rows fit; peak task
    memory and the share of GC at 50M.

## 7. Risks

- **Photon or adaptive execution changing `repartition(R, key)`.** Adaptive execution does not coalesce a repartition
  with an explicit number. A different hash would unbalance groups, not split them.
- **Groups of about 276 MB of cached blocks per task at 100M rows.** They stay within 2.2 GB heaps only because one
  block at a time is decoded. To be measured at 50M and 100M.
