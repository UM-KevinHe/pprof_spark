# ADR-0004: Platform skeleton

- Status: Accepted (delegated; D-16)
- Date: 2026-10-05
- Decision: D-11 defines the skeleton as the Phase 0 exit; D-16 records this design

## Context
D-11 makes a statistics-free skeleton the Phase 0 exit: data contract, logical blocks, a toy
kernel, block-ordered reduction, a result table and persisted metadata, green in CI under Classic
Spark and Spark Connect. Every model family will follow the same path, so the choices below are
the engine's backbone (§6.2–§6.10).

## Decision
- **Data contract** (`engine.data`): `InputSpec` names a group column (integral or string), numeric
  feature columns and an optional unique row-identifier column; names match exactly. Validation
  checks the schema before any job, then counts nulls, NaN and infinite values, duplicate row
  identifiers and empty input in one aggregation, and fails with every problem; messages carry
  column names and counts, never values (NN-7). Features become doubles.
- **Layout** (`engine.layout`): group sizes are collected under `BlockOptions.maxGroupsOnDriver`
  (DIST-1). `LayoutPlan` orders groups by key (numbers numerically, strings by unsigned UTF-8
  bytes, Spark's default collation), gives each group larger than the block target a block of
  its own, and fills `ceil(rows / target)` shared blocks largest group first into the least-loaded
  block, ties to the lowest block identifier. The block target comes from
  `BlockOptions.targetBlockBytes` (default 4 MB, DIST-4). The plan reaches executors as a
  driver-built table through a broadcast join; Spark's own partitioning never decides layout.
- **Working set** (`engine.backend`): one `groupByKey` on the block identifier and `mapGroups`
  build `BlockRecord`s: whole groups, rows in canonical order (group, row identifier, values;
  negative zeros normalized), primitive arrays. The set is persisted with an explicit storage
  level, materialized once with a block-count check, and released in `finally`.
- **Kernels** (`numerics.kernels`): pure functions over primitive arrays. The skeleton's kernel
  computes column sums and packed cross products with pairwise summation, per-group means, and an
  order-independent fingerprint (the wrapping sum of SplitMix64 hashes of rows) for §6.10.
- **Reduction**: one partial per block, collected under `BlockOptions.driverBudgetBytes` and
  combined in block order with Neumaier sums (ADR-0003), after checking that every block reported
  once and that the row total equals the validated input.
- **Results**: small results are an immutable summary with full metadata (software, input
  specification, options, layout, fingerprint). The m-scale group table is collected under the
  same guards and rebuilt on the driver, so it holds no reference to the training data (ARCH-4).
- **Persistence**: a self-describing directory written through Spark: `metadata` as one JSON
  record with every double stored as its 64-bit pattern (exact on any JDK, OI-07), and `groups` as
  Parquet. Saving never overwrites.

## Evidence (assistant sandbox, 2026-10-05; CI to confirm)
scalac 2.13.16 with the build's flags; OpenJDK 17.0.20 and 21.0.12; Classic and in-process Spark
Connect. All 34 engine tests, 29 numerics tests and 6 testkit tests pass in all four combinations.
`BlockMomentsSuite` shows bitwise-identical results across two input orders, 1 and 4 partitions
and both ANSI modes (R0); equality with a Spark-free evaluation of the same plan, kernels and
reduction; agreement within T-part between a one-block and a multi-block layout, with identical
group means and fingerprint (R1); exact sums for integer-valued data; guard messages naming the
option; and a bit-exact save and load (PERS-1).

## Consequences
- New model families reuse validation, layout, working set, reduction and persistence, and add
  kernels and specifications.
- Groups larger than a block are not split yet: TimeRange (OI-03, OI-30).
- m-scale results are bounded by the driver limits until distributed result tables exist
  (OI-29); per-iteration reduction volume at large p is OI-02.
