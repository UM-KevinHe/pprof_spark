# 6. Architecture and the end-to-end flow

[Tutorial home](README.md) · previous: [5. Distributed execution](05-distributed-execution.md) ·
next: [7. Validation and numerical accuracy](07-validation-and-numerical-accuracy.md)

This chapter is a map of the code: the modules and types involved in a Cox fit, the full path of
`CoxPH.fit` from the public call to the returned result, the design choices behind it, and where to
look when something goes wrong.

## 6.1 Modules

| Module | Depends on | Cox content at `efb6329` |
|---|---|---|
| `numerics` | Scala standard library only (ARCH-1) | Kernels `CoxStratum`, `CoxResiduals`, `CoxMeasures`; `Newton`, `Cholesky`, `Summation`, `Normal`, Poisson tests |
| `engine` | `numerics` and `spark-sql-api` only (ARCH-2) | `pprof.spark.engine.cox`: `CoxPH`, `CoxInput`, `CoxBlocks`, `CoxKernel`, `CoxPrediction`, `CoxMeasures`, `CoxProviderTests`, `CoxFitIO`; shared `layout` and `backend` |
| `app` | `engine` | `CoxJob` and its run specification; `python.PythonApi` |
| `python/` | The JARs, through py4j | `pprof_spark.CoxPH` |
| `testkit` | Spark (test scope) | Local session, fixture reader, tolerance classes |
| `ml` | `engine`, Spark ML | No code yet; PROJECT_CONTEXT §3.6 and §6.11 describe the adapters as planned |
| `bench` | `engine`, `testkit` | No Cox benchmark; `DeterminismCost` measures `StrictMath` in a Cox-shaped kernel (ADR-0006) |

`scripts/check-engine-api.sh` enforces on every push that `engine` uses no Classic-only Spark API and
that `numerics` and `engine` main code compute transcendental functions with `StrictMath`.

## 6.2 Types

| Type | File | Role |
|---|---|---|
| `CoxSpec` | [CoxInput.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxInput.scala#L20-L40) | Column roles; requires at least one feature and one role per column |
| `CoxOptions`, `Ties` | [CoxPH.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L24-L61) | Tie method, iteration control, aliasing tolerance, confidence level, `maxStratumRows`, `BlockOptions`, `robust` |
| `CoxInput` | CoxInput.scala | The validated frame with internal column names, counts, key type |
| `BlockOptions`, `DriverGuards` | [BlockOptions.scala](../../../engine/src/main/scala/pprof/spark/engine/backend/BlockOptions.scala) | Target block size, `maxGroupsOnDriver`, `driverBudgetBytes`, storage level |
| `GroupKey`, `GroupPlacement`, `LayoutPlan` | [LayoutPlan.scala](../../../engine/src/main/scala/pprof/spark/engine/layout/LayoutPlan.scala) | Keys ordered numerically or by UTF-8 bytes; the plan |
| `CoxRow`, `CoxBlock`, `CoxWorkingSet` | [CoxBlocks.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxBlocks.scala) | A row on its way into a block; the block; the persisted Dataset with `release()` |
| `Prepared` | CoxPH.scala, `private[cox]` | Plan, working set, center, counts, fingerprint for one call |
| `CoxBlockSummary`, `CoxPartial` and the other per-block records | [CoxKernel.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxKernel.scala#L13-L74) | What executors send back |
| `CoxTotals` | [CoxStratum.scala](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L5-L16) | Compensated ℓ, U and packed I |
| `Evaluation`, `NewtonOptions`, `NewtonResult`, `IterationRecord` | [Newton.scala](../../../numerics/src/main/scala/pprof/spark/numerics/Newton.scala#L3-L38) | The optimizer's contract |
| `CholeskyFactor` | [Cholesky.scala](../../../numerics/src/main/scala/pprof/spark/numerics/Cholesky.scala#L82-L174) | Solve, inverse, pivots, aliased columns |
| `CoxFit`, `CoxCoefficient` | [CoxPH.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L63-L111) | The immutable result (API-2) |
| `LayoutSummary` | engine/skeleton/BlockMoments.scala | Recorded layout: rows per block, block count, oversized strata, largest and smallest block |
| `SoftwareInfo` | engine/metadata/SoftwareInfo.scala | Versions and git SHA recorded with every fit (NN-11) |

`CoxFit` holds: the coefficient table (feature, estimate, standard error, z, p-value, lower, upper);
the covariance and the information at β̂ as packed upper triangles; ℓ(β̂) and ℓ(0); iterations,
halvings, convergence flag and message; observations, events (rows, as pprof_py counts them), strata
and strata without events; warnings; the per-iteration log of β, ℓ and halvings; the spec, options,
software information, layout summary and data fingerprint; the feature status ("experimental"); and,
for robust fits, the model-based covariance and the cluster count.

## 6.3 The end-to-end trace of `CoxPH.fit`

| # | Step | Where | Spark jobs | Code (CoxPH.scala unless noted) |
|---:|---|---|---|---|
| 1 | `fit(df, spec, options)` enters `withWorkingSet` | Driver | — | [L132–L133](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L132-L133), [L317](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L317) |
| 2 | Validate the schema and values; project internal columns | Driver and executors | 1 aggregation (3 jobs observed with a row identifier) | [L320](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L320); CoxInput.scala L63–L159 |
| 3 | Collect stratum sizes | Executors, then driver | 1 query (2 jobs observed) | [L323](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L322-L325); GroupSizes.scala L17–L26 |
| 4 | Plan blocks | Driver | — | LayoutPlan.scala L80–L110 |
| 5 | Guards: `maxStratumRows`, driver budget | Driver | — | [L326–L337](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L326-L337) |
| 6 | Build and persist the working set | Executors | Broadcast, shuffle, materialization (5 jobs observed) | [L338](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L338); CoxBlocks.scala L189–L223 |
| 7 | Summary pass; check counts | Executors, then driver | 1 | [L340–L350](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L340-L350) |
| 8 | Center = column means | Driver | — | [L351–L357](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L351-L357) |
| 9 | `Newton.maximize` from β = 0, each evaluation a pass | Driver loop, executor passes | 1 per evaluation | [L135–L144](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L135-L144), [L445–L465](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L445-L465) |
| 10 | Aliasing → `CoxAliasingException` with feature names | Driver | — | [L142–L146](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L142-L146) |
| 11 | Model-based covariance V = I(β̂)⁻¹ | Driver | — | [L145–L147](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L145-L147) |
| 12 | Robust variance, when requested | Both | 1 pass, or a pass and 2 shuffles | [L148–L155](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L148-L155) |
| 13 | Coefficient table | Driver | — | [L156–L170](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L156-L170) |
| 14 | Warnings, logged and kept | Driver | — | [L171–L181](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L171-L181) |
| 15 | Assemble `CoxFit` with metadata | Driver | — | [L182–L206](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L182-L206) |
| 16 | Release the working set, also on failure | Driver | — | [L364](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L364) |

## 6.4 Design choices and their reasons

| Choice | Reason | Cost |
|---|---|---|
| Pure numerical kernels in `numerics` (NN-6) | Unit tests without Spark in milliseconds; one implementation for tests and production; the formula is in one place | The engine must marshal data into arrays |
| Logical blocks, canonical order, ordered reduction (NN-4) | Bitwise reproducibility makes "no change in any bit" a usable test for refactoring (NUM-2) | Determinism constrains layouts and forbids Spark's aggregates for floating-point sums |
| Dataset API only (NN-5, DIST-3) | One engine for Classic Spark and Spark Connect | Each pass deserializes every block from the cache; RDD-style tricks are unavailable |
| Driver-side solves (§6.3) | p is small; Cholesky on the driver is exact, deterministic and cheap | B partials travel each pass; large p needs a different reduction (OI-02) |
| `StrictMath`, Neumaier sums, own Cholesky | Same bits on every JVM; accuracy for sums with cancellation; no native BLAS | A few extra flops per addition (OI-38) |
| Errors with counts, never values (NN-7) | Protected data never appear in logs or messages | Messages are less specific |
| Models hold no data (ARCH-4); data passed explicitly and fingerprinted (API-3) | Results are never silently recomputed from changed data | Each call that needs data rebuilds the working set |
| Fail rather than return pseudo-inverse or degenerate answers (X-011, X-012) | NN-10: degenerate outcomes are surfaced | Some pprof_py fits have no pprof_spark counterpart |
| Corrections of pprof_py behavior (X-010, X-013, X-015) | Reproducibility and agreement with R and with the reference's own documentation | Parity with pprof_py is within tolerance or explicitly divergent |

PROJECT_CONTEXT §6.9 describes capability traits (`HasCoefficients`, `SupportsResiduals` and so on)
through which inference and measures would be written generically; they are planned, not built
(v2.7 says so), and the Cox code uses its concrete types.

## 6.5 Where statistics and Spark are separated, and where they touch

Cleanly separated: the risk-set sweep, residuals, national baseline and provider kernels, the
optimizer, the factorization, summation and the normal distribution are all pure functions with no
Spark types. A statistical change normally touches `numerics` and its tests only.

Where the two concerns meet, and a change on one side can change results on the other:

- **The order of additions belongs to the engine.** The canonical order and the grouping of strata
  into blocks decide the order of floating-point additions, so a layout change changes the last bits
  of the result (R1 rather than R0).
- **Centering constants come from the engine's summary pass** and are passed into every kernel call.
- **The optimizer's cost model is Spark's.** `Newton.maximize` is pure, but each call to its
  `evaluate` function launches a Spark job, so every halving costs a pass over the data.
- **`CoxPH.fit` mixes orchestration and inference.** The sandwich product and the Wald calculations
  live in the engine object rather than in `numerics`.
- **Provider measures compute the national baseline on the driver** from collected per-time totals,
  with kernel functions, then ship it back in closures.

## 6.6 Where to look when something goes wrong

| Symptom | Start here | Then |
|---|---|---|
| A coefficient differs from R or pprof_py | Chapter 7's decision procedure: tie method, near-tie merging, weights, robust default (X-009), stopping rule (X-010) | Function-level parity with `CoxPH.evaluateAt` (test scope); then `CoxStratum.add` |
| A standard error differs | Robust or model-based? (`CoxFit.robust`, X-009, X-015) | `Cholesky.inversePacked`; `CoxPH.sandwich` |
| A baseline hazard differs | The X-014 factor exp(weighted mean offset); Efron increments | `CoxStratum.baseline`, `CoxKernel.baseline` |
| Iteration counts differ from pprof_py | X-010; ε; the halving schedule | `Newton.maximize` |
| Results change between runs or partition counts | The canonical order (`CoxBlockBuilder`), the reduction order (`CoxPH.evaluate`), any new floating-point SQL aggregate (DIST-7), iteration over hash maps (§13) | Rerun the R0 tests and the probe |
| Results change with the block size beyond T-part | Cancellation in running sums; a sum that is not compensated | `CoxStratum` sums; `OrderedReduction` |
| "covariates … are aliased" | A constant or collinear covariate; or a covariate on a very different scale | `Cholesky.factor`; rescale and refit |
| "the objective is not finite" | Extreme offsets or linear predictors | Chapter 3, section 3.6 |
| A validation error | The listed column and count | `CoxValidation.validate` |
| `DriverLimitExceededException` | Too many strata (`maxGroupsOnDriver`) or partials over `driverBudgetBytes` | Chapter 8's sizing table |
| "more than maxStratumRows" | A stratum too large for one block | Chapter 8, section 8.4 |
| An executor runs out of memory while building | The largest stratum's block | Chapter 8, section 8.2 |
| A fit is slow | Number of strata, largest stratum, `spark.sql.shuffle.partitions`, iterations and halvings, repeated rebuilds | Chapter 8 |
| A parity test fails | Which level fails tells you where: function level → kernel; lockstep → update logic; end to end → stopping or inference | Chapter 7 |
