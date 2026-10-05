# pprof_spark — Project Context

**Version** 2.0 · **Date** 2026-10-03 · **Supersedes** v1 (initial project brief)
**Status** Living document. Statements marked *(re-verify)* describe external platforms or the state of the reference implementation; re-check them before relying on them.
**Reference implementation** [`pprof_py`](https://github.com/UM-KevinHe/pprof_py) (MIT). The pinned commit and reference-tool versions are recorded in `reference/REFERENCE.lock`.

---

## 0. How to use this document

This document defines what `pprof_spark` is, the constraints it operates under, and the rules for building it. It is written for the maintainer, contributors, reviewers, and AI coding assistants, and is intended to be loaded as standing project context.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119 and RFC 8174 when, and only when, they appear in capitals. Normative requirements carry identifiers (for example `DIST-3`) so that pull requests, decisions, and reviews can cite them.

When sources disagree, precedence is: (1) approved statistical specifications in `docs/spec/` and recorded decisions in `DECISIONS.md`; (2) the behavior of the pinned reference implementation, and of R where the reference is validated against R; (3) this document; (4) existing code. A conflict between this document and an approved specification is resolved in favor of the specification, and this document is corrected in the same change.

Project state lives in a small set of files updated at the end of every working round: `STATUS.md` (current phase and gate status), `OPEN_ITEMS.md`, `DECISIONS.md` (sequentially numbered, never renumbered), `DISCREPANCIES.md` (the register defined in §3.5), `HANDOFF.md` (continuity between sessions), and `docs/adr/` (architecture decision records).

---

## 1. Non-negotiables

| ID | Rule |
|---|---|
| NN-1 | **No silent statistical change.** Any difference from the pinned reference in estimand, likelihood, parameterization, convention, default, or output definition is a registered discrepancy with an explicit, approved decision (§3.5). |
| NN-2 | **Specification before implementation.** No model feature is merged without an approved statistical specification (§7.1). |
| NN-3 | **Patient-level data never reach the driver.** Any driver materialization is explicit in the method name, size-guarded, and documented (§6.2). |
| NN-4 | **Deterministic, partition-invariant results.** For a fixed build, input, and configuration, results are bitwise identical regardless of cluster size or physical partitioning; changing layout parameters (such as block size) changes results only within a documented floating-point tolerance (§6.8, §8.2). |
| NN-5 | **The statistical engine is independent of Spark ML and restricted to Spark APIs available under both Classic Spark and Spark Connect.** Spark ML is an adapter layer (§5.4, §6.1). |
| NN-6 | **Numerical kernels are pure, Spark-free, deterministic functions** with their own unit tests (§6.4). |
| NN-7 | **Protected data never leave the approved environment.** PHI, PII, and CMS data never appear in GitHub, CI, Codespaces, logs, exception messages, or AI-assistant conversations (§12). |
| NN-8 | **Evidence over reasoning.** Every claim of correctness, parity, or performance cites recorded evidence: a test run, CI log, benchmark record, or validation report. |
| NN-9 | **Tolerances are never relaxed to make a test pass.** Changing a tolerance requires written justification, a negative control showing that the tolerance still detects real defects, and maintainer approval (§8.4). |
| NN-10 | **Degenerate outcomes are surfaced, never hidden.** Non-convergence, rank deficiency, infinite or boundary estimates, and degenerate providers or strata are reported in result status fields and warnings. |
| NN-11 | **Every fit records reproducibility metadata** (§6.10). |
| NN-12 | **Features are `Experimental` until they pass the parity gate** (§9.8); gate status is visible in documentation and in fit summaries. |
| NN-13 | **Build for the runtime actually deployed.** Spark line, Scala patch version, and JDK bytecode level are pinned to the target Databricks Runtime and verified on that runtime (§5). |

---

## 2. Mission, users, and success

### 2.1 Mission

`pprof_spark` is a Spark-native Scala package for large-scale healthcare provider profiling. It reimplements the validated statistical methods of `pprof_py` as distributed algorithms that preserve their statistical definitions and validated behavior. The objective is not to translate Python into Scala; it is to redesign each method around Spark's execution model, so that fitting, inference, standardization, and provider-level testing scale to national datasets without moving patient-level data to a single machine. The package should feel like a statistical library built for Spark.

### 2.2 Users and workloads

The primary users are statisticians and analysts who run national-scale provider profiling on Databricks — for example the CMS dialysis-facility measures that `pprof_py` already supports (two-stage SMR and SHR from Cox models; readmission measures from logistic models with provider effects) and transplant-center or hospital profiling — in settings where data volume, governance rules that keep patient-level data inside the governed environment, or production repeatability make single-node execution impractical. Secondary users are data engineers who run fitted models inside scheduled pipelines.

### 2.3 Design envelope

These are planning assumptions, to be confirmed in Phase 0 (decision D-04). They determine which layouts and thresholds must perform well.

| Symbol | Meaning | Planning range |
|---|---|---|
| n | Patient or patient-period rows | 10⁷ – 10⁹ |
| m | Providers (fixed effects, strata, or clusters) | 10² – 10⁶ |
| p | Covariates | 1 – 10³ (typically 10 – 100) |
| K | Distinct event times per stratum | Up to about 10⁴ (daily resolution over several years) |
| B | Logical blocks of the working set (§6.5) | 10¹ – 10⁵ |

### 2.4 Definition of success

For each in-scope model family, `pprof_spark` succeeds when it (a) passes the parity gate against the pinned reference (§9.8); (b) fits workloads at the top of the design envelope through distributed computation, without patient-level data on the driver, within the performance targets set in Phase 0; (c) produces deterministic, partition-invariant results (NN-4); (d) runs as a versioned, repeatable Databricks job that records complete reproducibility metadata; and (e) is measurably faster or more feasible than single-node `pprof_py` beyond a documented crossover point (§3.7).

### 2.5 Non-goals for the first releases

The first releases will not reproduce `pprof_py`'s plotting and presentation layer (`pprof_spark` emits tidy result tables that existing tools, including `pprof_py`'s plotting module, can render); reproduce pandas or NumPy idioms; support execution outside Spark as a user-facing mode; wrap or subclass Spark's built-in ML models; preserve Python method names where they are unidiomatic in Scala; implement exact-partial-likelihood ties; or parse model formulas (`pprof_py` also takes a numeric design matrix).

---

## 3. Relationship to pprof_py

### 3.1 A companion, not a port

`pprof_py` and `pprof_spark` are related implementations of the same methods, not one package with two language bindings. `pprof_py` remains optimized for local and medium-scale analysis with NumPy, SciPy, and numba, rich local result objects, publication-oriented presentation, and exploratory work. `pprof_spark` is optimized for very large datasets, distributed fitting, Databricks execution, Spark-native pipelines, and repeatable production runs. APIs differ where the execution model requires it; mathematical behavior does not.

### 3.2 The pinned reference

`pprof_py` is the reference implementation for every feature until that feature passes the parity gate. The reference is pinned, not tracked: `reference/REFERENCE.lock` records the `pprof_py` commit SHA and version, the Python, NumPy, and SciPy versions, and the version of every R package used to produce reference fixtures. At the time of writing, `pprof_py` documents validation against R 4.3.3 with `survival` 3.5.8, `glmnet` 4.1.8, and `lme4` 1.1.35.1 *(re-verify)*.

PAR-1: Re-pinning is a deliberate event. A dedicated pull request updates the lock file, regenerates fixtures, produces a diff report of every changed fixture value, and classifies each change under §3.5 before merge.

### 3.3 Inherited validation status

`pprof_py` reports *(re-verify at the pinned commit)*: Cox coefficients, standard errors, log-likelihood, baseline hazard, and martingale residuals matching `survival::coxph()` to 1e-8–1e-14 relative error across right-censored, left-truncated, stratified, offset, and weighted data, under both Breslow and Efron ties; robust and clustered variance validated against `coxph(robust = TRUE, cluster = ...)`; penalized regression validated against `glmnet`; linear random-effect models agreeing with `lme4::lmer` at about 1e-7–1e-9; logistic random-effect models agreeing with `glmer(nAGQ = 1)` at about 2e-5; and no external R reference for group lasso, provider-penalized, and discrete-time models.

PAR-2: This determines the parity target for each feature. Features validated against R are tested against both `pprof_py` fixtures and R fixtures. Features without an external reference are tested against `pprof_py` fixtures plus checks that depend on neither implementation: optimality (KKT) conditions for penalized fits, analytic special cases, and simulation studies of bias and coverage.

### 3.4 Known reference limitations and open items

*(Re-verify every row at the pinned commit and record the outcome in `DISCREPANCIES.md`.)*

| Reference behavior | Implication for pprof_spark |
|---|---|
| `CoxPH` defaults to Breslow ties; R's `coxph` defaults to Efron. | The `pprof_spark` default is Breslow, for parity with the reference. The tie method is recorded in every fit summary, and R fixtures always set `ties` explicitly. |
| `CoxPH` does not warn when `max_iter` is reached. | `pprof_spark` reports non-convergence (NN-10). No estimate changes. |
| `CoxPH(fit_intercept=True)` fits, but every `predict_*` method then raises. | `pprof_spark` rejects an intercept in Cox models at validation, because it is not identifiable (class C). |
| `ties="exact"` is not implemented. | Out of scope. |
| `FineGrayPH` on left-truncated data does not reproduce R's `finegray()` weights (about 3e-3 coefficient difference). | Choose the parity target before the competing-risks phase (class B). |
| The SerBIN fixed-effect solver can stop at a near-null fit when covariates are far from zero (open item C27). | `pprof_spark` centers covariates internally, which leaves the MLE unchanged; a dedicated fixture confirms the behavior, and cases where the reference stops early are class B. |
| `LogisticThreeStageModel.sigma_sensitivity()` fails when σ̂ = 0. | The three-stage specification defines behavior at the σ̂ = 0 boundary. |
| Some reference measures have unresolved discrepancies with internal R code (for example `IUR.fac` and `cal_SMR_pro_adj`). | Dependent features are not parity-gated until resolved. |
| The R-comparison suite has documented expected failures (`R_COMPATIBILITY.md`). | Only features with passing, documented validation are parity targets. |

### 3.5 Discrepancy protocol

PAR-3: Every observed difference between `pprof_spark` and the reference is classified before the change that reveals it can merge. Unclassified differences block merges. Crashes and nondeterminism are always defects to fix, never behaviors to match.

| Class | Definition | Required action | Approval |
|---|---|---|---|
| A — pprof_spark defect | `pprof_spark` differs from a correct reference. | Fix; add a regression test. | Maintainer review |
| B — Reference defect | The reference differs from its specification, from R, or from the mathematics. | Register with evidence; report upstream; decide between bug-compatible behavior (flagged) and corrected behavior (documented divergence). | Maintainer sign-off; methodology owner when an estimand is affected |
| C — Intentional divergence | A deliberate behavioral difference, for API or platform reasons, that leaves estimates for valid inputs unchanged. | Document; tests assert the new behavior. | Maintainer sign-off |
| D — Numerical noise | A difference explained by summation order or transcendental-function implementation, within its tolerance class. | Attach tolerance evidence. | Automatic within tolerance |
| E — Documentation mismatch | Reference documentation disagrees with reference behavior. | Follow the behavior; correct the documentation. | Maintainer review |

### 3.6 API freedom

The Scala API follows Spark and Scala conventions rather than Python's. Where Python writes `model.fit(X, duration=time, event=event)`, the Spark ML adapter is configured through typed parameters:

```scala
val model = new CoxPHEstimator()
  .setEntryCol("start").setDurationCol("stop").setEventCol("event")
  .setFeatureCols(Array("age", "diabetes", "bmi"))
  .setStrataCol("provider_id").setOffsetCol("offset").setWeightCol("weight")
  .setTies("breslow")
  .fit(data)
```

A migration guide maps each `pprof_py` call to its `pprof_spark` equivalent and lists the intentional differences.

### 3.7 When to use which package

`pprof_py` is fast on one machine: its README reports a 200,000-row, 3,000-stratum, 6-covariate Cox fit in about 1.8 seconds on one vCPU *(re-verify)*. Spark adds job-scheduling latency to every Newton iteration and a pass over the data per iteration, so for data that fit comfortably in one machine's memory — including a large Databricks driver node — `pprof_py` can be the faster tool. `pprof_spark` is justified when data exceed single-node memory or runtime budgets, when governance requires processing in place, or when the fit is part of a production Spark pipeline.

PERF-1: Phase 1 publishes a measured crossover analysis for Cox (runtime as a function of n, p, strata, and cluster size, for both packages), and the documentation includes a decision guide derived from it.

When a problem's sufficient statistics are small — the covariate-free stage-2 baseline fit of the SMR workflow, or event-time aggregates in general — aggregating in Spark and solving on the driver is often the best distributed design rather than a compromise; the engine supports it directly (§6.6).

---

## 4. Scope and roadmap

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 — Foundations and spikes | Repository, multi-module build, CI, test harness, fixture pipeline, numerics and backend skeletons, Databricks deployment path; decisions D-01–D-10 and spikes S-01–S-07 (§16). | CI green on the walking skeleton; a JAR deployed by CI runs on a DBR 18 LTS job cluster; every spike recorded as an ADR. |
| 1a — Cox estimation core | Right censoring, strata, offsets, case weights, Breslow and Efron ties; coefficients, information, model-based covariance, standard errors, Wald inference, log partial likelihood; convergence and step-halving; aliasing. | Parity gate passed for these features. |
| 1b — Counting process and baseline | (start, stop] data and left truncation; per-stratum baseline cumulative hazard and survival; prediction (linear predictor, relative hazard, cumulative hazard, survival). | Parity gate passed. |
| 1c — Residuals and robust inference | Martingale residuals (correct under left truncation and Efron ties), score and dfbeta residuals, robust sandwich and clustered variance. | Parity gate passed. |
| 1d — Provider workflows | Two-stage SMR and SHR; expected counts; O/E ratios; exact Poisson intervals and tests; flags; provider result tables; job-runner entry point. | Parity gate passed; end-to-end job on DBR 18 LTS at design-envelope scale. |
| 2 — Logistic provider models | Large-m fixed effects (SerBIN-type blocked Newton); provider tests (Wald, score, exact Poisson-binomial, bootstrap); direct and indirect standardization; then the three-stage SRR pipeline, including the stage-2 random-intercept variance estimation it needs. | Parity gate passed. |
| 3 — Linear fixed effects | Profile (within) estimation, standardization, inference. | Parity gate passed. |
| Later | Penalized models (elastic net, group lasso, provider-penalized), random and mixed effects, shared-frailty Cox, time-varying coefficients, discrete-time survival, competing risks (cause-specific, Fine-Gray), variable selection, inter-unit reliability, empirical-null calibration, funnel limits. | Each family enters through Phase-0-style spikes and its own gate. |

ROAD-1 (walking skeleton): The first merged vertical slice is an unstratified, right-censored, Breslow Cox model with model-based variance that passes through every layer: data contract, working set, kernel, Newton loop, fitted model, result tables, persistence, CI parity tests, and execution on DBR 18 LTS from the deployed JAR. Statistical breadth is then added behind the same skeleton, so that platform, build, and determinism risks surface before effort goes into breadth.

ROAD-2: Each phase ends with a gate review recorded in `STATUS.md`. A phase cannot close with open class A or B discrepancies affecting its features.

The mapping below covers `pprof_py`'s public model classes *(re-verify)*.

| pprof_py classes | Phase |
|---|---|
| `CoxPH` | 1a–1d |
| `LogisticFixedEffectModel`, `LogisticThreeStageModel` | 2 |
| `LinearFixedEffectModel` | 3 |
| `PenalizedLogistic(CV)`, `PenalizedLinear(CV)`, `PenalizedCoxPH(CV)`, `GroupLasso*`, `ProviderPenalized*` | Later |
| `LogisticRandomEffectModel`, `LogisticFERandomClusterModel`, `LinearRandomEffectModel`, `FrailtyCoxPH` | Later |
| `TimeVaryingCoxPH`, `DiscreteSurvival(CV)`, `ProviderPenalizedDiscreteSurvival(CV)` | Later |
| `CauseSpecificCoxPH`, `FineGrayPH`, `CoxPHSelector` | Later |

---

## 5. Platform targets and compatibility

### 5.1 Platform facts *(as of 2026-10-03; re-verify)*

| Item | Value |
|---|---|
| Databricks Runtime 18 LTS | Apache Spark 4.1.0; Scala 2.13.16; JDK 21 by default, with JDK 17 available as a fallback; supported until June 2029 |
| Databricks Runtime 17.3 LTS | Apache Spark 4.0.0; from Databricks Runtime 17 onward only Scala 2.13 is supported |
| Databricks Runtime 19 | Apache Spark 4.2.0; generally available but not yet LTS |
| Open-source Spark 4.1.x | Built with Scala 2.13.17; Spark ML on Spark Connect is generally available for the Python client |
| Open-source Spark 4.2.0 | Adds support for building and running on Java 25 |
| Serverless JAR tasks | Generally available since August 2026; a JAR must match the Scala, JDK, and Databricks Connect versions of its serverless environment |
| Declarative Automation Bundles | Current name of Databricks Asset Bundles, Databricks' recommended mechanism for CI/CD deployment |

### 5.2 Compute modes and support tiers

| Compute | Constraints relevant to this package | Tier |
|---|---|---|
| Classic, dedicated access mode | Full Spark API, Spark ML, caching, Spark UI. | Tier 1 — primary; all features |
| Classic, standard access mode (Unity Catalog) | Scala code cannot use `SparkContext`; RDD APIs are unsupported; JARs must be on the Unity Catalog allowlist; Databricks documents Spark ML on standard compute as Python-only. | Tier 2 — engine API from Scala notebooks or JAR tasks, after spike S-02; no Spark ML adapter |
| Serverless jobs | Spark Connect APIs only; no RDDs; DataFrame `cache`, `persist`, and `checkpoint` raise exceptions; custom code such as UDFs and `mapPartitions` may use at most 1 GB of memory; no Spark UI; JAR tasks run when versions match. | Tier 3 — engine through JAR tasks with table-materialized working sets, after spikes S-02 and S-03 |
| Serverless notebooks | Scala notebooks and JAR libraries are unsupported. | Not supported |

PLAT-1: Only Tier 1 gates releases until decision D-01 says otherwise.

PLAT-2: Engine code MUST remain Connect-compatible whatever the tier decision. This costs little and protects the package as Databricks moves more compute onto Spark Connect.

### 5.3 Version pinning and the Scala patch-skew hazard

The Scala 2.13 standard library is now only backward binary compatible (SIP-51): code compiled against a newer 2.13 patch release may call library methods that an older runtime lacks. A Scala issue (scala/bug#13181) reports exactly this failure, a `NoSuchMethodError`, when Spark 4.1.0 runs on a runtime that ships Scala 2.13.16. DBR 18 LTS ships Scala 2.13.16, while open-source Spark 4.1.x depends on scala-library 2.13.17. Since sbt 1.10, a build fails when `scalaVersion` is older than the scala-library on the dependency classpath, unless `allowUnsafeScalaLibUpgrade := true` demotes the failure to a warning.

PLAT-3: `scalaVersion` MUST equal the target runtime's Scala patch version (2.13.16 for DBR 18 LTS) and MUST NOT exceed it.

PLAT-4: Because open-source Spark 4.1.x needs scala-library 2.13.17 on the test classpath, CI keeps the compiler at the runtime's version and tolerates the newer test-time library. The linkage of every release candidate MUST then be verified on the actual runtime by an integration smoke test. Spike S-01 confirms this arrangement or replaces it, for example by compiling `engine` against Databricks Connect.

PLAT-5: Bytecode targets Java 17 (`-release 17`), so one JAR runs on JDK 17 and JDK 21; CI runs the tests on both.

PLAT-6: Spark and Scala dependencies are `provided`; the JAR bundles no Spark, Scala, or runtime-provided libraries. Third-party runtime dependencies are avoided and, when unavoidable, shaded.

PLAT-7: Only public Spark APIs are used. Package-private APIs (`private[spark]`, `private[ml]`) are never reached through package-placement tricks, and APIs that Spark marks unstable — including Spark ML's shared `Has*` parameter traits, which are documented as changeable between minor versions — are wrapped behind project-owned interfaces.

### 5.4 Compile-time API surface per module

| Module | Compiles against (`provided`) | What the restriction guarantees |
|---|---|---|
| `numerics` | Scala standard library only | Spark-free kernels with millisecond tests |
| `engine` | `spark-sql-api`, the shared Classic/Connect interface | Connect compatibility enforced by the compiler |
| `ml` | `spark-mllib` and `engine` | Spark ML adapters; Classic only |
| `app` | `engine`, plus `ml` where needed | Job-runner entry points |
| `testkit` | `spark-sql` (Classic), test scope | Shared test harness, data generators, fixture loaders |

For JARs on standard and serverless compute, Databricks documents two compile targets — Databricks Connect (recommended) or `spark-sql-api` — and for classic compute it allows any Spark artifact that matches the cluster's Spark version, marked `provided`. Since Spark 4, the Scala `SparkSession`, `Dataset`, and related types form a shared interface with Classic and Connect implementations. Code limited to that interface runs in either mode; Classic-only members such as `sparkContext` and RDD conversions are unavailable under Connect.

### 5.5 Spark 4 and Spark Connect behaviors that affect library code

| Behavior | Consequence | Rule |
|---|---|---|
| ANSI SQL mode is on by default in Spark 4 and on serverless. | Overflow, invalid casts, and division by zero raise errors instead of returning null. | Engine behavior is identical with ANSI on and off; CI tests both. |
| Spark Connect defers analysis and name resolution until execution. | Schema errors surface late. | Validate the input schema eagerly, once, at the start of `fit`; never inside loops. |
| Connect has no `SparkContext`, RDDs, broadcast variables, or accumulators. | Classic idioms are unavailable. | Small parameters reach kernels by closure capture; larger ones travel as small DataFrames joined with broadcast hints (§6.7). |
| Closures and UDF classes execute on the server. | Their classes must be on the server classpath. | The JAR is installed as a cluster or task library; closures never capture client-only objects. |
| The session time zone varies by compute; serverless defaults to UTC. | Timestamp arithmetic can change. | Durations come from DATE values or explicitly zoned computations; tests run under two time zones. |
| Serverless exposes no Spark UI and only client-side logs. | Operational visibility depends on the package itself. | Fit traces and warnings are first-class outputs (§6.13). |

### 5.6 Compatibility matrix

`docs/compatibility.md` maps every release to its Spark line, DBR LTS, Scala patch version, JDK levels, and tested tiers. Every release pull request updates it.

---

## 6. Architecture

### 6.1 Layering and modules

```
 Users:   Scala engine API       Spark ML pipelines        Databricks JAR jobs
               |                        |                          |
               |                  [ ml ] adapters            [ app ] run specs
               |                        |                          |
               +------------------------+--------------------------+
                                        |
                                        v
 [ engine ]  spark-sql-api only
     data contracts and validation
     working-set preparation: layout, logical blocks, canonical order, materialization
     execution backend: kernels, deterministic reduction
     statistical models -> inference -> provider measures -> diagnostics
     result tables (DataFrames), fit summaries, persistence
                                        |
                                        v
 [ numerics ]  pure Scala, no Spark
     dense linear algebra, summation, distributions, optimizers, risk-set kernels
```

| ID | Dependency rule |
|---|---|
| ARCH-1 | `numerics` depends only on the Scala standard library. |
| ARCH-2 | `engine` depends only on `numerics` and `spark-sql-api`. It MUST NOT reference `org.apache.spark.ml`, `org.apache.spark.rdd`, `SparkContext`, or other Classic-only members; a build-time import check enforces this in addition to the compile classpath. |
| ARCH-3 | `ml` contains no statistical algorithm: it validates parameters, converts columns, calls `engine`, and wraps results. |
| ARCH-4 | Fitted statistical models hold no `SparkSession` and no hidden references to training DataFrames (§6.9). |
| ARCH-5 | Composition is preferred to inheritance; inheritance is reserved for genuine behavioral contracts, such as the capability traits of §6.9. |

### 6.2 Scale taxonomy and data placement

| Scale | Examples | Placement |
|---|---|---|
| n (rows) | Patient or patient-period records, residuals, row-level predictions | Always distributed; never collected; returned as DataFrames. |
| m (providers, strata, clusters) | Provider effects, O/E tables, per-stratum baseline hazards | Distributed by default. MAY be collected for driver-side computation below a configurable size limit, only through explicitly named, size-guarded methods. |
| K (event times) | Per-stratum event-time grids | Small per stratum; the full strata-by-time table is distributed when large. |
| p (parameters) | Coefficients, score, information, covariance | Driver. |
| B (logical blocks) | Partial sufficient statistics, one per block | Driver, after deterministic reduction (§6.8). |

DIST-1: Methods that materialize data on the driver are named `collect…` or `toLocal…` and enforce a size guard that fails with an actionable message.

DIST-2: Default size limits are documented, configurable, and recorded in the fit summary.

### 6.3 Core computational pattern

The first model families are all fitted by Newton-type methods whose expensive step is a pass over n rows that produces low-dimensional sufficient statistics (log-likelihood, score, and information or its blocks) and whose cheap step is a small dense solve. The engine standardizes on this pattern: distributed sufficient statistics, driver-side small solves. Cox fits compute risk-set sums each iteration. Logistic fixed-effect fits compute a provider-blocked Hessian and reduce it through its Schur complement. Linear fixed-effect fits need one pass of within-provider co-moments and no iteration. Penalized fits (later phases) use a proximal-Newton or IRLS outer loop whose penalized quadratic subproblem is solved on the driver by coordinate descent. The target is one pass over the data per Newton iteration, plus at most one more when step-halving needs an extra objective evaluation.

### 6.4 Numerical core (`numerics`)

| Component | Content |
|---|---|
| Dense linear algebra | Project-owned Cholesky with a singularity tolerance and aliasing report, LDLᵀ, triangular solves, inversion through factorizations, packed symmetric storage. Deterministic, tested against NumPy and R, and independent of native BLAS. |
| Summation | Blocked (pairwise) summation within a block; Neumaier compensation across blocks. |
| Special functions and distributions | Normal, t, χ², gamma, Poisson (including exact intervals), Poisson-binomial (exact, plus documented approximations), log1p, expm1, log-sum-exp. |
| Optimization | Newton iterations with the reference's step control; convergence rules represented as data. |
| Risk-set kernels | Streaming accumulators over sorted primitive arrays for (start, stop] risk sets with Breslow and Efron tie groups. |

NUM-1: `numerics` functions are pure (no I/O, no global state), total (validated inputs or typed errors), and documented with formulas and specification references.

### 6.5 Working sets, logical blocks, and the execution backend

The engine reaches Spark through a narrow internal interface. A **working set** is the validated, encoded representation of a model's input under a layout (§6.6), organized into **logical blocks**. Every row is assigned to exactly one block by a deterministic function of the data and the layout parameters, never of cluster size or input partitioning, and each block is a single record holding primitive arrays (row-major features, times, weights, offsets) in canonical order. A **kernel** is a pure `numerics` function from a block and small parameters to a partial statistic. A **reduction** combines partials in block order (§6.8). Physical Spark partitions decide only where blocks execute; they never influence results.

| Materialization | Mechanism | Use |
|---|---|---|
| Persist | `Dataset.persist` with an explicit storage level, a materializing action, and `unpersist` in a `finally` block | Tier 1 default |
| Table | Blocks written to a session-scoped temporary location (Unity Catalog volume or managed table) and re-read each iteration | Serverless, where caching is unavailable; long fits that need fault tolerance |
| Recompute | No materialization; each pass rebuilds the blocks | Tiny data and tests |

DIST-3: The default backend uses Dataset APIs only: a one-time `groupByKey` on the block identifier builds a `Dataset[Block]`, and typed `mapPartitions` runs kernels over it. A Classic-only RDD backend MAY be added behind the same interface if profiling shows a material gain (an ADR with benchmark evidence), and it MUST produce bitwise-identical results. Dataset-first is chosen for Connect compatibility, not for Catalyst optimization, which does little for custom numerical loops; that is also why Spark's own iterative MLlib algorithms use RDDs internally.

DIST-4: Blocks hold roughly 1–16 MB of primitive arrays. This amortizes decoding costs, keeps kernels allocation-free, and stays well inside serverless memory limits for custom code. Block size is a recorded layout parameter.

### 6.6 Layouts, partitioning, and skew

| Layout | Block composition and order | Used by |
|---|---|---|
| StratumLocal | Whole strata bin-packed into blocks by size; rows sorted by time within each stratum | Stratified Cox with many strata, including stage 1 of SMR and SHR |
| TimeRange | A large stratum split into time-contiguous blocks; a two-pass scan gives each block its risk-set offsets | Unstratified Cox, or strata too large for one block |
| ProviderLocal | Whole providers bin-packed into blocks by size or estimated cost | Logistic and linear fixed effects; per-provider exact tests |
| EventGrid | Each block emits risk-set statistics aggregated onto the distinct event times (§7.3.3) | Covariate-free or small p·K problems, such as the stage-2 baseline fit |
| Any blocked layout | Reuse of an existing working set | Single-pass, row-separable computations: direct standardization, residuals, predictions |

DIST-5: The layout plan (block assignment, bin packing, splits) is computed on the driver from data summaries — strata count and size distribution, K, p, m — with deterministic tie-breaking, and applied through an explicit block-identifier column rather than Spark's sampling-based range partitioning or hash partitioning. The plan is recorded in the fit summary.

DIST-6 (canonical order): Within each block, rows are ordered by layout keys, then time, then a stable row identifier when one is supplied, and otherwise by full row content compared lexicographically. Results are therefore bitwise invariant to input row order.

Provider and stratum sizes are heavy-tailed. Largest-first bin packing balances blocks; per-provider computations whose cost grows faster than linearly, such as exact tests, are packed by estimated cost; strata that exceed the block budget are split and handled by the TimeRange technique. Adaptive query execution's skew handling does not apply to custom kernels and is not relied on.

### 6.7 Iteration management

Iterative fits keep the materialized working set fixed and vary only small parameters. Parameters of size O(p) reach kernels by closure capture. Parameters of size O(m), such as provider effects, travel as a small DataFrame keyed by block identifier and joined to the working set with a broadcast hint; this replicates them once per executor instead of serializing them into every task, and it works under Spark Connect. Because no iteration derives a DataFrame from the previous iteration's DataFrame, lineage does not grow and no checkpointing is needed. When per-row state must evolve, the engine uses table materialization rather than `localCheckpoint`, which loses data when executors are lost and is unavailable on serverless. Working sets are released in `finally` blocks.

### 6.8 Deterministic reduction and communication budget

Each kernel invocation returns one partial per logical block, keyed by block identifier. Partials are sorted by identifier and combined in that order with compensated summation. When B times the size of a partial exceeds the per-iteration driver budget (initially 64 MB), a two-level reduction groups partials by contiguous ranges of block identifiers, reduces each group in order on executors, and combines the group results in order on the driver. Symmetric matrices travel as packed upper triangles. Because the order is fixed by block identifiers rather than by task completion, results are bitwise identical across runs, cluster sizes, and physical partitionings (NN-4). An exploratory fast mode based on Spark SQL `Aggregator`s MAY exist; it is never the default and guarantees only agreement within tolerance.

DIST-7: Quantities under parity tolerance MUST NOT be computed with built-in floating-point SQL aggregates, whose summation order depends on task scheduling and on the execution engine (for example Photon versus the JVM). Integer counts are exempt.

DIST-8: Every floating-point reduction that follows a shuffle, such as summing dfbeta residuals within clusters, sorts its inputs by an explicit, data-derived key before summing.

### 6.9 Model contract, results, and schemas

Models declare capabilities as traits — `HasCoefficients`, `HasCovariance`, `HasProviderEffects`, `HasBaselineHazard`, `SupportsResiduals`, `SupportsRobustVariance`, `SupportsStandardization`, `SupportsProviderTests`, `SupportsPrediction` — and the inference and measures layers are written against capabilities rather than concrete classes. This mirrors `pprof_py`'s unified provider-model interface and lets a new family, such as Group Lasso, join testing and standardization without restructuring the core.

API-1: A support matrix is generated from the declared capabilities and enforced by tests.

API-2: Small results are immutable case classes: fit summary, coefficient table (names, estimates, standard errors, statistics, p-values, intervals), covariance, and convergence trace. Large results are DataFrames with documented, versioned schemas — stable column names, types, and nullability, plus a schema version — checked by tests.

API-3: Methods that compute from training data take the data explicitly (for example `fit.martingaleResiduals(df)`) and verify its fingerprint against the fit's, so results are never silently recomputed from a table that has changed since fitting.

### 6.10 Reproducibility metadata and persistence

| Recorded with every fit | Content |
|---|---|
| Software | Package version and git SHA; Spark, Scala, and JDK versions; Databricks Runtime version when available |
| Specification | Model, all parameters including defaults, tie method, convergence rule, reproducibility mode |
| Data | Input schema, row and group counts, fingerprint, and source table name and Delta version when available |
| Execution | Layout plan, block size, block count, materialization strategy, reduction mode |
| Outcome | Convergence status and trace, warnings, degeneracy reports, timings |

Models persist to a self-describing directory: `metadata.json` holding the metadata above, with numeric arrays in a round-trip-exact representation, plus Parquet files for m-scale tables such as baseline hazards and provider effects.

PERS-1: Save followed by load yields bitwise-identical parameters and predictions.

PERS-2: Models persisted by each release are kept as test fixtures; later releases load them or fail with a clear message under a documented deprecation policy.

PERS-3: Persistence uses only public Spark APIs; the Spark ML adapters implement `MLWritable` and `MLReadable` on top of this format.

### 6.11 Spark ML adapter layer (`ml`)

Each estimator (for example `CoxPHEstimator extends Estimator[CoxPHModel]`) validates parameters, converts inputs, calls the engine, and wraps the engine's fitted model; each model implements `transform`, `copy`, and persistence. Parameters are project-owned traits (`entryCol`, `durationCol`, `eventCol`, `strataCol`, `offsetCol`, `weightCol`, `clusterCol`, `providerCol`, `featureCols`, `ties`, `maxIter`, `tol`) with defaults and validators. The event column uses 1 for an event. Spark's AFT estimator calls its equivalent column `censorCol` even though 1 means the event occurred; this package does not copy that naming.

Features arrive as a list of numeric columns (preferred, because the names carry into coefficient tables) or as a vector column, which is converted with `vector_to_array` and named from its ML attribute metadata when present. `transformSchema` validates types eagerly. `transform` appends explicitly named outputs (linear predictor, relative hazard, expected count) instead of overloading `predictionCol`. Provider-level results are DataFrames returned by model methods, not rows forced through `transform`.

Categorical encoding is a parity hazard. `StringIndexer` orders categories by descending frequency by default and `RFormula` drops the last category after ordering, so the default reference level differs from R's; with `stringIndexerOrderType = "alphabetDesc"`, `RFormula` drops the same category as R. Parity tests therefore use numeric design matrices, as `pprof_py` does.

Hyperparameter tuning uses `CrossValidator` with `foldCol` and group-level folds (patient or provider); random row splits leak information across clustered records.

Spark ML on Spark Connect discovers estimators through Java's `ServiceLoader` and restricts which model attributes remote clients may call. Whether third-party models can be exposed that way is unverified (spike S-04); until it is, the `ml` module is Tier 1 only.

### 6.12 Application layer and Python access

The `app` module provides JAR-task entry points that read a declarative, versioned run specification (input tables, column mapping, model and options, output tables, reproducibility mode) and write results to Unity Catalog tables and model artifacts to a volume. This is the primary production interface: it works on every tier that runs JAR tasks, it is language-neutral because Python and SQL users work through tables and job parameters, and every run is auditable.

| Python access option | Mechanism | Runs on | Status |
|---|---|---|---|
| Job-based | Run specification plus JAR task | Tiers 1–3 | Planned for Phase 1d |
| py4j wrappers | PySpark `JavaEstimator` and `JavaModel` wrappers around `ml` classes | Tier 1 only | Decision D-03 |
| Spark Connect ML | Server-side registration of `ml` classes | Unverified | Spike S-04 |

Python-facing code never reimplements statistics.

### 6.13 Observability

Each iteration records its log-likelihood, largest absolute score component, step size, step-halvings, elapsed time, passes over the data, and bytes reduced, in the fit summary and in logs. Spark jobs carry descriptions or operation tags naming the model and iteration. Logs are structured, written through SLF4J, and never contain row-level values. Warnings are collected in the fit summary as well as logged, because serverless compute exposes only client-side logs.

---

## 7. Statistical specification

### 7.1 Specification first

STAT-1: Each feature has a specification in `docs/spec/<family>/<feature>.md`, approved before its implementation merges, with the following sections.

| Section | Content |
|---|---|
| Estimand and model | What is estimated, for which population, under which assumptions |
| Objective | Likelihood or loss, with formulas; penalty if any |
| Parameterization | Identifiability constraints, reference levels, centering |
| Data contract | Column types, valid ranges, coding, missing-data rule |
| Conventions | The value chosen for every applicable entry of §7.2 |
| Algorithm | Update equations, step control, convergence rule, initialization |
| Inference | Variance estimators, test statistics, intervals, sidedness |
| Outputs | Definition and schema of every returned quantity |
| Edge cases | Behavior for degenerate inputs and failure modes |
| Reference mapping | `pprof_py` functions and files at the pinned commit; R functions and options |
| Distributed plan | Layout, passes per iteration, communication volume, complexity |
| Validation plan | Fixtures, tolerance classes, metamorphic tests, negative controls |
| Known discrepancies | Entries from `DISCREPANCIES.md` |

### 7.2 Conventions registry

| Convention | What must be pinned | Default policy |
|---|---|---|
| Missing and invalid values | Null, NaN, and ±∞ in model columns; the reference's handling | Fail fast with counts; where the reference drops rows silently, drop them and report counts (class C) |
| Outcome coding | Accepted types and values | Reject anything else |
| Time representation and ties | Numeric type; tie rule; near-tie handling | Integer time units (for example days) recommended |
| Weights | Meaning (case or frequency versus sampling); zero and negative values | Negative weights rejected |
| Offsets | Where they enter; effect on baselines and predictions | Per reference |
| Group identifiers | Type, null handling, output ordering | Outputs sorted by identifier with documented collation |
| Centering and scaling | Internal (free) versus reported (pinned) | Reported values back-transformed to the reference's convention |
| Rank deficiency | Detection tolerance; reporting | Null coefficient plus flag, per reference |
| Convergence | Criterion, tolerance, iteration cap, step control | Per reference |
| Non-convergence | Status and warning; optional failure | NN-10 |
| Infinite or boundary estimates | Detection rule; reporting | Per reference |
| Variance estimators | Model-based, robust, clustered; degrees of freedom | Per reference |
| Tests | Sidedness, null distribution, reference value | Per reference |
| Intervals | Method and level | Per reference |
| Provider exclusions | Minimum size or expected-count rules | Parameters, never hard-coded |
| Flag boundaries | Strict or non-strict inequalities | Per reference; near-boundary cases reported (§8.4) |
| Randomness | Generator, seeding, partition invariance | §7.7 |
| Output naming and order | Column names, sort keys | Documented schemas |

### 7.3 Cox proportional hazards

#### 7.3.1 Definition

For stratum s with event-time set T_s, let row i have at-risk interval (a_i, b_i] (with a_i = −∞ for right-censored data), case weight w_i, offset o_i, and linear predictor η_i = x_iᵀβ + o_i. The risk set at time t is R_s(t) = {i ∈ s : a_i < t ≤ b_i}, the tied events at t form D_s(t), their number is k_s(t), and their total weight is d_s(t). With Breslow ties,

$$\ell(\beta)=\sum_{s}\sum_{t\in T_s}\Big[\sum_{i\in D_s(t)} w_i\,\eta_i\;-\;d_s(t)\,\log S_s(t)\Big],\qquad S_s(t)=\sum_{j\in R_s(t)} w_j\,e^{\eta_j}.$$

With Efron ties, the logarithmic term becomes

$$\bar w_s(t)\sum_{r=0}^{k_s(t)-1}\log\!\Big(S_s(t)-\frac{r}{k_s(t)}\,S^{D}_s(t)\Big),\qquad S^{D}_s(t)=\sum_{i\in D_s(t)} w_i\,e^{\eta_i},\qquad \bar w_s(t)=\frac{d_s(t)}{k_s(t)},$$

so the tied events share their mean weight, which is the `survival` package's convention.

#### 7.3.2 Conventions

| Topic | Requirement |
|---|---|
| Risk set | At risk at t if and only if a < t ≤ b: a row entering at t is not at risk at t; a row censored at t is. |
| Tie identification | Exact equality on the validated time representation. R's `coxph` merges near-ties by default (`timefix = TRUE`, through `aeqSurv`, with an `all.equal`-style tolerance of √ε). The specification records the reference's rule and `pprof_spark` replicates it; fixtures use integer-valued times or state `timefix` explicitly. |
| Default tie method | Breslow, the `pprof_py` default; R defaults to Efron. |
| Efron | Tied events share their mean weight; baseline-hazard increments follow the reference's Efron-adjusted form when ties are Efron. |
| Strata | Separate baseline hazards with shared coefficients; strata without events contribute nothing to ℓ but are represented in baseline tables. |
| Offsets | Added to η; baseline-hazard centering with offsets follows the reference (the `basehaz` offset-mean subtlety that `pprof_py` documents). |
| Centering | Internal centering, and per-stratum shifts of η for overflow safety, are free because ℓ is exactly invariant to them; reported baselines and linear predictors follow the reference's centering convention. |
| Convergence | Mirror the reference's rule. For orientation, `survival::coxph.control` stops when the relative change in log partial likelihood falls below `eps` = 1e-9, caps iterations at `iter.max` = 20, and halves steps that decrease ℓ. |
| Singularity | Cholesky with a singularity tolerance (`toler.chol` = ε^0.75 in `survival`); aliased covariates reported as null coefficients with a flag, per reference. |
| Infinite coefficients | Detection rule per reference (`toler.inf` in `survival`); always reported. |
| Weights in variance | Model-based variance treats case weights as frequency weights; robust and clustered variance follow the reference's dfbeta-based construction. |
| Intercept | Not identifiable; rejected at validation (§3.4). |
| Exact ties | Not supported. |

#### 7.3.3 Distributed algorithm

The partial likelihood is not a sum of independent row terms: each event time needs a sum over its risk set, which is a suffix sum over time within a stratum. The engine therefore turns risk sets into ordered scans over sorted blocks.

With many strata (StratumLocal), each stratum lies wholly within one block, rows are sorted once during preparation, and every Newton iteration sweeps each stratum from its latest time to its earliest. A row joins the running sums when the sweep reaches its exit time b and leaves them when the sweep reaches its entry time a. The sweep accumulates S, its gradient Σ w e^η x, and its second-moment matrix Σ w e^η x xᵀ, and closes Efron tie groups exactly. Each block emits its contributions to log-likelihood, score, and information; the driver solves. Each iteration costs O(n·p²) compute spread over executors, no shuffle, and O(B·p²) bytes reduced to the driver.

With few or very large strata (TimeRange), a large stratum is split into time-contiguous blocks whose boundaries never separate equal times, so tie groups stay whole. A first pass computes each block's risk-set totals, the driver turns them into exclusive suffix offsets, and a second pass completes each block's sweep from its offset.

When p is zero or p·K is small (EventGrid), each block aggregates its rows onto the stratum's distinct event times. A row's exit contribution goes to the largest event time not exceeding b, its entry contribution (subtracted) to the largest event time not exceeding a, and its event contribution to its own event time. Suffix sums over the event grid then give every risk-set sum exactly. This is the natural design for the covariate-free stage-2 baseline fit.

Running sums maintained by adding and subtracting can lose precision when large totals nearly cancel. Kernels use compensated accumulation, and the specification documents any periodic re-accumulation.

#### 7.3.4 Baseline hazard, prediction, residuals, and robust variance

Baseline increments come from the same risk-set sums at convergence and are stored per stratum as a distributed table (stratum, time, increment, cumulative hazard, survival). Row-level quantities — expected events e^η(Λ₀(b) − Λ₀(a)), martingale residuals, and score residuals — need cumulative quantities at each row's entry and exit times; they are computed during a sorted pass within StratumLocal blocks, or through a broadcast join with the per-stratum table when it is small. Score residuals use cumulative integrals of x̄(t) dΛ₀(t) evaluated at a and b, with the reference's Efron corrections. dfbeta residuals are score residuals multiplied by the inverse information. Robust variance sums outer products of dfbeta residuals with the reference's weighting. Clustered variance first sums them within clusters, which needs a shuffle on the cluster key when clusters cross strata (for example, patients who move between facilities) and follows DIST-8.

#### 7.3.5 Two-stage SMR and SHR

The reference workflow documented by `pprof_py` fits stage 1 as a Cox model stratified by provider, with offsets and weights, and computes each row's linear predictor including the offset. Stage 2 fits a covariate-free Cox model with that linear predictor as its offset; its baseline hazard gives each row's expected count, from which provider O/E ratios, intervals, tests, and flags follow. In `pprof_spark`, stage 1 uses StratumLocal blocks, stage 2 uses EventGrid, and expected counts come from a sorted pass or a broadcast join; no step collects patient-level data. Recurrent-event (SHR) data use the same counting-process machinery, with patient clusters for robust variance where the reference uses them. An illustrative engine-level sketch, with provisional names:

```scala
val spec = CoxSpec(
  entry = Some("start"), duration = "stop", event = "event",
  features = Seq("age", "diabetes", "bmi"),
  strata = Some("provider_id"), offset = Some("offset"), weight = Some("weight"),
  ties = Ties.Breslow)

val stage1 = CoxPH.fit(df, spec)                              // engine API; Connect-compatible
val withLp = stage1.withLinearPredictor(df, outputCol = "lp") // includes the stage-1 offset

val stage2 = CoxPH.fit(withLp,
  spec.copy(features = Nil, strata = None, offset = Some("lp")))
val smr = ProviderMeasures.indirect(withLp, stage2, providerCol = "provider_id") // DataFrame
```

### 7.4 Logistic fixed effects and the three-stage SRR

The fixed-effect model is logit π_ij = γ_j + x_ijᵀβ for patient i of provider j. For provider j let h_j = Σ_i w π(1 − π), b_j = Σ_i w π(1 − π) x_ij, and g_j = Σ_i w (y − π); over all rows let C = Σ w π(1 − π) x xᵀ and g_β = Σ w (y − π) x. Here w denotes case weights where the reference supports them and unit weights otherwise. Each Newton step eliminates the m provider effects through the Schur complement, the block elimination at the heart of SerBIN (Wu, Yang, Kang & He, 2022):

$$S = C-\sum_j \frac{b_j b_j^{\top}}{h_j},\qquad \Delta\beta = S^{-1}\Big(g_\beta-\sum_j \frac{b_j\,g_j}{h_j}\Big),\qquad \Delta\gamma_j=\frac{g_j-b_j^{\top}\Delta\beta}{h_j}.$$

Per-provider quantities are computed within ProviderLocal blocks. Each block emits its contributions to C, Σ b_j b_jᵀ/h_j, Σ b_j g_j/h_j, g_β, and the log-likelihood, and the driver solves the p×p system. The provider updates then use Δβ, on the driver when the per-provider table is under the size guard and otherwise in a distributed pass (§6.7). Covariates are centered internally; the MLE is invariant, and provider effects absorb the shift and are back-transformed. Centering is expected to avoid the reference's early-stopping behavior (§3.4), and a dedicated fixture confirms this.

Degenerate providers (all outcomes 0 or all 1, which imply infinite MLEs) and small providers follow the reference's rule exactly, whether exclusion, bounding, or flagging, as fixed in the specification.

Provider tests follow the unified contract of §7.6: Wald, score, exact Poisson-binomial, and bootstrap tests against the reference's reference value. Exact per-provider computations are independent across providers and run within ProviderLocal blocks; their cost grows faster than linearly in provider size, so bin packing uses estimated cost. FFT-based Poisson-binomial algorithms have absolute errors near machine epsilon, so extreme-tail p-values follow the absolute-tolerance rule of §8.4.

Indirect standardization (observed over expected, with expected counts computed at the reference provider effect) is a single pass over the data. Direct standardization evaluates every provider's effect over the whole population, which is O(n·m) work. It runs as a kernel over the working set, with the provider-effect vector shipped as in §6.7, producing per-block m-length partial sums, and it is never a cross join. Its cost is documented, and approximations are used only if the reference uses them.

The three-stage SRR pipeline (`LogisticThreeStageModel`) estimates β in stage 1 with fixed effects, the random-intercept variance σ² in stage 2 from a model with stage-1 offsets, and provider effects in stage 3 with β and σ held fixed. β is deliberately not updated in stage 3; this is a design choice of the pipeline, not a defect. Stage 2's per-provider marginal-likelihood contributions (Laplace or Gauss–Hermite, per the reference) are provider-local, and σ is optimized on the driver. The specification defines behavior at the σ̂ = 0 boundary (§3.4).

### 7.5 Linear fixed effects

The profile (within) estimator obtains β from within-provider co-moments and sets γ_j = ȳ_j − x̄_jᵀβ. One pass suffices: within each ProviderLocal block, per-provider means and co-moments are merged with the numerically stable pairwise updates of Chan, Golub, and LeVeque, and each block emits its within-provider cross-product totals. The "sum of squares minus correction" formula, which cancels catastrophically, is never used. When conditioning diagnostics indicate trouble, a TSQR (communication-avoiding QR) path solves the least-squares problem without forming cross-products. Degrees-of-freedom and variance conventions follow the reference.

### 7.6 Provider-level measures and inference

All model families share one test contract, mirroring `pprof_py`'s `test()`: a method (Wald, score, exact, bootstrap, or resampling, as the model supports), a reference value (for example 0, the median or mean provider effect, or a number, depending on the model), sidedness, and level. It returns a provider table with identifier, size, observed and expected values, measure, interval, statistic, p-value, flag, and the basis for the flag. Funnel limits, empirical-null calibration, and inter-unit reliability operate on m-scale tables and MAY run on the driver under the size guard of DIST-1, while still returning DataFrames. Methods whose R references are GPL-licensed are implemented from the published mathematics (§12).

### 7.7 Randomness, resampling, and cross-validation

STAT-2: Every random quantity is reproducible and partition-invariant. It comes from a counter-based or hash-based generator keyed on (seed, stable record or group identifier, replicate index), never from `rand(seed)`, whose output depends on partitioning. Fold assignment is a deterministic function of a stable group identifier (patient or provider) and a seed. For parity, fold identifiers and bootstrap replicate weights may be supplied as input columns, so tests can reuse exactly the folds and resamples that `pprof_py` used; without them, parity for resampling methods is distributional and verified statistically. Poisson-weight and multinomial bootstraps are different procedures, and the specification states which one the reference uses.

### 7.8 Design hooks for later families

| Family | Distributed design direction |
|---|---|
| Penalized (elastic net, group lasso) | Proximal-Newton or IRLS outer loop on distributed sufficient statistics; driver-side coordinate or block-coordinate descent; λ path with warm starts; KKT checks as validation. |
| Provider-penalized | Provider block kept distributed through the Schur-complement structure; only p-dimensional quantities on the driver. |
| Random and mixed effects | Per-provider conditional modes and Laplace or quadrature contributions computed provider-locally; variance components optimized on the driver. |
| Shared-frailty Cox | EM with a provider-local E-step and an M-step that reuses the Cox engine with offsets. |
| Discrete-time survival | Person-period rows generated lazily inside kernels, never materialized as an expanded table. |
| Competing risks | Cause-specific models reuse the Cox engine; Fine-Gray reuses the risk-set machinery for censoring weights. |
| Time-varying coefficients | Counting-process expansion with time-by-covariate terms, laid out like (start, stop] data. |

---

## 8. Numerical correctness and reproducibility

### 8.1 Floating-point model

Since Java 17, all floating-point arithmetic has strict IEEE 754 semantics (JEP 306): the same operations in the same order produce the same bits on any compliant JVM, and the JIT never fuses a multiply and an add unless `Math.fma` is called explicitly. Transcendental functions in `java.lang.Math` may differ by up to one ulp across implementations and intrinsics, whereas `StrictMath` is specified to reproduce the fdlibm algorithms bit for bit. Kernels therefore use `StrictMath` for `exp`, `log`, `log1p`, and `expm1` in deterministic mode; spike S-05 measures the cost. Small dense linear algebra uses the project's own routines rather than whatever native BLAS a runtime loads. Decimal input columns are converted to doubles explicitly, under the specification's rules.

### 8.2 Reproducibility levels

| Level | Definition | Requirement |
|---|---|---|
| R0 — bitwise | The same build, input, and configuration (including layout and block-size parameters) give identical bits on every run, on any cluster size or physical partitioning. | MUST in deterministic mode, the default |
| R1 — layout-invariant | Different block sizes or layouts agree within the T-part tolerance. | MUST |
| R2 — platform-invariant | CI local mode and Databricks, Classic and Connect, and JDK 17 and JDK 21 agree. | MUST within T-part; SHOULD be bitwise |
| R3 — cross-implementation | Results agree with `pprof_py` and R within the parity tolerance classes. | MUST for gated features |

NUM-2: Changes declared non-behavioral (refactors, performance work) MUST preserve R0 identity on the golden runs, demonstrated alongside a negative control showing that the comparison detects a deliberate one-ulp perturbation.

### 8.3 Stability techniques

Covariates are centered internally, and scaled when poorly conditioned, with reported quantities transformed back. Exponentials are formed after per-stratum or per-provider shifts, which are exact for the Cox partial likelihood, and log-sum-exp, log1p, and expm1 forms are used wherever they apply. Accumulations use blocked summation with compensation across blocks; co-moments use pairwise updates. Information matrices are explicitly symmetrized, factorized by Cholesky with the reference's singularity tolerance, and their condition numbers are reported. Step control follows the reference, and every fit records its convergence trace.

### 8.4 Tolerance policy

Tolerances live in one versioned file (`testkit/tolerances.conf`), tests refer to them by class rather than by literal value, and they change only under NN-9. The values below are initial defaults, to be calibrated in Phase 0 against negative controls (Breslow substituted for Efron, a one-day shift of a tied time, a dropped weight), each of which must fail.

| Class | What is compared | Initial default |
|---|---|---|
| T-fn | Objective, score, and information at fixed parameter values (relative, scaled by magnitude) | 1e-12 |
| T-iter | Lockstep iterates over the first five iterations from identical starts (relative) | 1e-10 |
| T-coef | Converged coefficients (absolute plus relative to the reference value) | atol 1e-10, rtol 1e-8 |
| T-var | Standard errors and covariance (relative) | 1e-7 |
| T-base | Baseline cumulative hazard and survival (relative) | 1e-8 |
| T-res | Residuals (absolute, scaled) | 1e-9 |
| T-meas | Expected counts, ratios, and rates (relative); observed counts exact | 1e-8 |
| T-test | Test statistics (relative) | 1e-8 |
| T-p | p-values: compare the statistics; when p < 1e-10, compare log₁₀ p (absolute 1e-6) or document the reference algorithm's accuracy limit | — |
| T-flag | Flags match exactly, except cases within 1e-8 (relative) of a threshold, which are reported rather than failed | — |
| T-part | Layout and platform invariance, R1 and R2 (relative) | 1e-12 objective; 1e-10 parameters |

Converged-estimate comparisons run both implementations to a tighter criterion than the production default, so that stopping rules do not masquerade as discrepancies; production-default behavior, including iteration counts, is compared separately.

---

## 9. Verification and validation

### 9.1 Test layers

| Layer | Scope | Where | When |
|---|---|---|---|
| T1 Numerics | Pure functions, linear algebra, distributions; finite-difference checks of score and information | CI, no Spark | Every pull request |
| T2 Kernels | Kernels on in-memory blocks | CI, no Spark | Every pull request |
| T3 Engine | Local-mode Classic Spark (`local[2]` to `local[4]`) with several partition counts and block sizes; ANSI on and off | CI | Every pull request |
| T4 Parity | Function-level, lockstep, and end-to-end parity with fixtures | CI | Every pull request touching an affected feature |
| T5 Metamorphic | Invariance properties on generated data (§9.4) | CI | Every pull request |
| T6 Edge cases | Degenerate inputs, validation errors, absence of row values in messages | CI | Every pull request |
| T7 Contracts | Parameter defaults, output schemas, persistence round trips, backward compatibility | CI | Every pull request |
| T8 Connect | Engine suite under local Spark Connect | CI | Nightly; every pull request once spike S-02 succeeds |
| T9 Databricks integration | DBR 18 LTS job clusters, multi-node, synthetic data; standard and serverless tiers once adopted | Databricks, through bundles | Merges to main, nightly, releases |
| T10 Scale and performance | Benchmark suite with regression thresholds | Databricks | Weekly and before releases |
| T11 Statistical validation | Simulation studies of bias, coverage, and type I error for features without an external reference | Databricks or CI | When the feature is released |
| T12 Production-data validation | Approved CMS data inside the approved environment, compared with `pprof_py` or production outputs; only aggregate results leave the environment | Restricted Databricks | Before production use |

### 9.2 Parity at three levels

Function-level parity compares objective, score, and information at the same fixed parameter values. It is the strongest test because no optimizer is involved. Lockstep parity starts both implementations from identical values and compares the first iterates, isolating update logic from stopping rules. End-to-end parity compares converged results and every reported output. A feature passes only when all three agree.

### 9.3 Reference fixtures

A dedicated workflow, triggered manually on each re-pin, installs the pinned `pprof_py` and R stack, generates fixtures, and opens a pull request with a diff report. Fixtures are Parquet files for inputs and outputs plus a JSON manifest that records the generator, seed, reference versions, the options passed to each tool (including `ties` and `timefix`), and checksums. Input data are generated once and shared by every implementation, never regenerated per language. Synthetic event times are integers unless a fixture specifically tests near-ties, because R's `timefix` documentation warns that simulated continuous times can be spuriously merged. Fixtures contain only synthetic data (NN-7), and large fixtures are stored with Git LFS.

The catalog spans tiny cases solvable by hand, edge cases, mid-size datasets covering every feature combination (mirroring `pprof_py`'s right-censored, left-truncated, stratified, offset, and weighted combinations under both tie methods), and scale generators that are themselves partition-invariant (§7.7).

### 9.4 Metamorphic and property tests

| Transformation | Applies to | Expected result |
|---|---|---|
| Permute input rows | All | Bitwise identical (DIST-6) |
| Change cluster size or physical partition count | All | Bitwise identical (R0) |
| Change block size or layout | All | Equal within T-part (R1) |
| Relabel providers or strata bijectively | All | Equal up to relabeling, within T-part |
| Translate a covariate, x → x + c | Cox; fixed-effect models | Coefficients unchanged; fixed effects absorb the shift (γ → γ − cβ) |
| Scale a covariate, x → c·x | Regression models | Coefficient divided by c; standard error divided by the absolute value of c |
| Duplicate a row versus doubling its weight | Case-weighted fits; for Cox, only Breslow ties or untied rows, because Efron treats a duplicated event as a tie | Equal estimates and model-based variance |
| Apply a strictly increasing transform to time | Cox | Coefficients and ℓ unchanged; baseline re-indexed |
| Split (a, b] at s into (a, s] without an event and (s, b] | Cox with time-fixed covariates | ℓ, coefficients, and model-based variance unchanged; robust variance unchanged when clustered by subject |
| Add zero-weight rows | Weighted models | Equal within T-part |
| Add a stratum without events | Cox | Coefficients unchanged |
| Shift all offsets in a stratum by a constant | Cox | Coefficients unchanged |

### 9.5 Edge-case catalog

| Case | Required behavior (per specification) |
|---|---|
| Stratum or provider without events | Contributes nothing to Cox ℓ and appears in degeneracy summaries; fixed-effect models follow the reference's rule |
| Provider with all outcomes 0 or all 1 | Reference's exclusion, bounding, or flag rule; never a silent infinity |
| Single-row strata or providers | Defined and tested behavior |
| Ties at every event time | Correct Breslow and Efron results |
| Zero weights | Rows retained, contributing nothing |
| Null, NaN, or infinite values | Policy of §7.2, with counts |
| Entry time not before exit time; negative times | Validation error with counts, without row values |
| Perfect separation; monotone likelihood | Detected and reported with non-convergence status |
| Collinear covariates | Aliasing reported per reference |
| Extreme covariate magnitudes | No overflow, thanks to shifts and centering |
| Empty input, or empty after validation | Clear validation error |

### 9.6 Distributed and platform tests

Every engine test runs with at least two physical partition counts, two block sizes, and one skewed provider-size distribution. A subset runs where executors are separate JVMs, on multi-node Databricks job clusters, because local mode shares one JVM between driver and executors and so hides singleton state initialized only on the driver, classpath differences, and some serialization paths. Tests that target Classic or Connect assert which implementation the session actually is: an upstream bug report (SPARK-58223) shows the session builder's `classic()` selector creating a Connect session in Spark 4.1.x. Integration jobs print the loaded package version and git SHA and fail if they differ from the commit under test, which guards against stale libraries on clusters.

### 9.7 Databricks integration and scale tests

Integration jobs are defined in the bundle (§11.5), run on fresh job clusters at the pinned DBR, generate synthetic data in place, and write machine-readable results (JUnit XML or JSON) to a volume for CI to collect. Scale tests sweep n, p, strata and provider counts, skew, tie density, and the fraction of left-truncated rows. They record time per iteration, iterations, passes, shuffle bytes, bytes reduced to the driver, and peak memory in a Delta table with regression thresholds.

### 9.8 Parity gate and traceability

`docs/parity/matrix.md` lists every feature with its specification section, reference functions, fixture identifiers, tests, tolerance classes, and status: planned, implemented, parity-verified, scale-verified, or stable. A feature becomes stable, and loses its `Experimental` marker, only when all three parity levels pass, its metamorphic and edge-case tests pass, its scale test meets the target, its documentation is complete, and the maintainer signs off. A CI check fails if any public API lacks a matrix entry.

---

## 10. Performance and scalability

### 10.1 Priorities

In order: correct statistical algorithm; distributed scalability; numerical stability; no driver bottlenecks; minimal shuffles; efficient block kernels; efficient serialization; no unnecessary caching; micro-optimization last. Clarity and statistical reproducibility are never traded for speed.

### 10.2 Cost model

Each algorithm's specification states its passes per iteration, shuffle volume (one-time and per iteration), bytes reduced to the driver per iteration, and compute complexity. For the core Cox layouts, preparation costs one shuffle of n rows to build blocks; each iteration costs O(n·p²) compute spread over executors, no shuffle, and O(B·p²) bytes reduced to the driver after packing.

### 10.3 Design rules

| Rule | Rationale |
|---|---|
| Shuffle while building the working set, not during iterations. | Iteration cost should be compute-bound. |
| Cache blocks of primitive arrays (DIST-4); no boxing or per-row allocation in inner loops. | JVM throughput and garbage-collection pressure. |
| Use built-in column expressions only for O(p) row-separable quantities that are not under parity tolerance; compute O(p²) statistics in kernels. | Whole-stage code generation degrades with very many aggregate expressions; determinism (DIST-7). |
| Choose the block size so that B stays within the driver budget (§6.8), and size physical partitions to keep all executor cores busy. | Driver traffic scales with B·p². |
| Bin-pack heavy-tailed groups by size or estimated cost; split oversized strata with the TimeRange technique. | Straggler tasks dominate wall time. |
| Avoid metadata actions (`count`, schema requests) inside loops. | Each is a job or, under Connect, a round trip. |
| Set storage levels explicitly; `unpersist` in `finally`. | Predictable memory. |
| Never rely on adaptive query execution for working-set layout. | Reproducibility (DIST-5). |
| Run direct standardization as a block kernel over a shipped provider-effect vector, never as a cross join. | O(n·m) work without O(n·m) data. |
| Respect `spark.driver.maxResultSize` and the package's own driver budget. | Driver stability. |

### 10.4 Benchmarks and regression tracking

The `bench` module defines reproducible synthetic workloads at three scales. A scheduled job runs them on a fixed cluster configuration and appends metrics to a Delta table. A change that regresses a tracked metric beyond its threshold needs a written justification in its pull request.

---

## 11. Development workflow and tooling

### 11.1 Environments

The maintainer cannot install Scala or Spark locally, but local-mode Spark remains the backbone of testing, because it runs inside CI runners with nothing installed on the maintainer's machine. Four environments cover development. GitHub Actions runs every build and test. GitHub Codespaces, if institutional policy permits, provides a browser-based development container with a JDK, sbt, and the Metals language server, in which `sbt test`, local Spark, and `git apply` work interactively. The GitHub web editor handles small changes. Databricks hosts integration and scale testing. Protected data never enter Codespaces (NN-7), and any Databricks Connect session from a Codespace uses synthetic-data workspaces only. Databricks Scala notebooks are suitable for exploration, not for package development.

### 11.2 Repository layout

```
pprof_spark/
  build.sbt, project/      sbt build and plugins
  numerics/                pure Scala kernels and linear algebra
  engine/                  distributed engine, models, inference, measures
  ml/                      Spark ML adapters
  app/                     Databricks job entry points and run specifications
  testkit/                 Spark fixtures, generators, tolerance assertions
  bench/                   benchmark workloads (not published)
  reference/               REFERENCE.lock; fixture generators (Python, R); manifests
  fixtures/                generated reference fixtures (synthetic only; Git LFS)
  databricks/              bundle definition (databricks.yml), job and cluster specs
  docs/                    spec/, adr/, parity/, compatibility.md, user and migration guides
  .devcontainer/           Codespaces definition
  .github/workflows/       CI, fixtures, Databricks integration, benchmarks, release
  STATUS.md  OPEN_ITEMS.md  DECISIONS.md  DISCREPANCIES.md  HANDOFF.md
```

### 11.3 Build configuration

The build uses sbt 1.x at version 1.11.7 or later, the minimum Databricks lists for building Scala JARs for serverless compute; migration to sbt 2 waits until the required plugins support it. The fragment below is illustrative and must be validated by spike S-01.

```scala
// Pinned to the deployed runtime, DBR 18 LTS (re-verify; see §5.1 and §5.3)
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / scalacOptions ++= Seq("-release", "17", "-deprecation", "-feature",
  "-unchecked", "-Xlint", "-Wunused:imports,privates,locals", "-Werror")
ThisBuild / javacOptions ++= Seq("--release", "17")
val sparkVersion = "4.1.0"

lazy val numerics = project                                   // no Spark dependency

lazy val engine = project.dependsOn(numerics).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql-api" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql"     % sparkVersion % Test),
  allowUnsafeScalaLibUpgrade := true,  // open-source Spark 4.1.x pulls scala-library 2.13.17 (SIP-51)
  Test / fork := true,
  Test / parallelExecution := false,
  Test / javaOptions ++= Seq(/* --add-opens flags copied from Spark's JavaModuleOptions */))

lazy val ml = project.dependsOn(engine).settings(
  libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided)
```

Tests fork a JVM that carries the `--add-opens` flags Spark needs on Java 17 and later, copied from the pinned Spark version's `JavaModuleOptions`. Suites run serially against a shared local session with the UI disabled, small shuffle-partition counts, and ANSI mode set explicitly. Formatting and linting use scalafmt and scalafix; `sbt-buildinfo` embeds the version and git SHA (NN-11); coverage uses scoverage; binary-compatibility checking with MiMa begins at 1.0.

### 11.4 CI/CD workflows

| Workflow | Trigger | Content | Gate |
|---|---|---|---|
| `ci.yml` | Pull requests and pushes | `actions/setup-java` (Temurin 21 and 17, sbt cache) and `sbt/setup-sbt`, needed because GitHub removed sbt from `ubuntu-latest` in December 2024; format check; compile with `-Werror`; layers T1–T7; coverage | Required |
| `connect.yml` | Nightly, later every pull request | Layer T8 | Required after spike S-02 |
| `fixtures.yml` | Manual, on re-pin | Pinned `pprof_py` and R stack; regenerated fixtures; diff-report pull request | Review |
| `databricks-it.yml` | Merges to main, nightly, release tags | Build the JAR; upload it to a Unity Catalog volume under an immutable name containing version and git SHA; `databricks bundle deploy`; run integration jobs (T9); collect results | Required for release |
| `bench.yml` | Weekly; manual | Benchmark jobs (T10); metrics to Delta; comparison report | Required before release |
| `release.yml` | Version tag | Build; publish artifacts; attach the validation report and compatibility entry | — |

Databricks authentication uses workload identity federation for GitHub Actions (OIDC, with `permissions: id-token: write` and `DATABRICKS_AUTH_TYPE: github-oidc`) and a least-privilege service principal limited to development workspaces and synthetic-data schemas. No long-lived tokens are stored in the repository or its secrets.

### 11.5 Databricks deployment

Deployment uses Declarative Automation Bundles. `databricks/databricks.yml` declares the JAR artifact, the job definitions (Lakeflow Jobs), job-cluster specifications pinned to the target DBR and dedicated access mode, and development, test, and production targets. JARs live in Unity Catalog volumes under immutable names, and a path is never overwritten. Standard-mode targets add the JAR path to the Unity Catalog allowlist. Serverless JAR tasks select an environment version whose Scala, JDK, and Databricks Connect versions match the build. Integration tests run on fresh job clusters rather than long-lived all-purpose clusters, whose installed libraries can stay stale until restart. Entry points obtain the session with `SparkSession.builder().getOrCreate()`; library code never stops the session or exits the JVM.

### 11.6 Working process

Work proceeds in rounds. Each round produces one self-contained change (a pull request or a `git apply`-ready diff, rehearsed on a fresh clone before delivery) and ends with updates to the state files of §0. Non-trivial work starts with a design sketch, and every behavior-changing proposal includes a numerical impact table. Decisions are numbered in `DECISIONS.md`, and architecture decisions also get an ADR. Statistical and design decisions, above all parity trade-offs and output-changing choices, require the maintainer's explicit approval; implementation and consistency work proceeds under delegated authority.

| Pull-request check | Evidence required |
|---|---|
| Specification | Section reference; specification approved |
| Statistical behavior | Either "no change", with R0 evidence (NUM-2), or a discrepancy identifier with approval |
| Tests | Layers added or updated; failing-test sets compared before and after, not just counts |
| Parity | Three-level parity results for affected features |
| Distribution | Layout-invariance results; no new driver materialization, or compliance with DIST-1 |
| Compatibility | `engine` still compiles against `spark-sql-api` only; Connect suite status |
| Data protection | No real data; no row values in logs or messages |
| Performance | Benchmark comparison if a kernel, layout, or reduction changed |
| Documentation | Specification, user guide, parity matrix, changelog |
| State files | `STATUS.md`, `OPEN_ITEMS.md`, `DECISIONS.md`, and `HANDOFF.md` updated |

### 11.7 Releases and versioning

Releases follow semantic versioning and stay at 0.x until the first parity-gated feature set; public APIs carry stability annotations, and MiMa enforces binary compatibility from 1.0. The compatibility matrix (§5.6) maps each release to its Spark line; if several Spark lines are ever supported at once, the Spark line becomes part of the artifact name. Every release ships a changelog and a validation report covering parity status per feature with tolerance evidence, metamorphic and edge-case results, a benchmark summary, and the tiers tested. Artifacts are published to GitHub Releases and the deployment volume; publication to a public Maven repository is decision D-08.

---

## 12. Data governance, privacy, security, and licensing

| Area | Rule |
|---|---|
| Protected data | PHI, PII, and CMS data stay in the approved Databricks environment. GitHub, CI runners, Codespaces, local machines, and AI assistants see only synthetic data. |
| Logs and errors | Messages identify problems by column names and counts, never by row values or patient identifiers; tests check this (T6). |
| Shared outputs | Provider-level outputs intended for sharing follow the applicable data use agreement's cell-size suppression rules; CMS policy prohibits displaying cell counts from 1 to 10. |
| Real-data validation | T12 runs only inside the approved environment; only aggregate comparison results leave it. |
| Credentials | OIDC federation and least-privilege service principals; no personal tokens in automation. |
| Dependencies | Kept minimal; updated through Scala Steward or Dependabot; every new dependency gets a license review. |
| Licensing | `pprof_py` is MIT-licensed. R's `survival` package is LGPL and EmpiNull is GPL-3: run them to produce reference outputs, but never port their source into this repository; implement from the published mathematics and the MIT-licensed reference. The project's own license is decision D-07. |

---

## 13. Coding standards

| Rule | Rationale |
|---|---|
| Immutable values, case classes, and sealed traits for closed concepts (tie methods, variance types, layouts). | Correctness and exhaustiveness checking. |
| Explicit types on public APIs and domain-specific names; one-letter names only for local notation that mirrors a formula in the specification. | Readability tied to specifications. |
| Small pure functions in `numerics`; no hidden global or mutable singleton state anywhere. | Determinism; local mode hides singleton bugs. |
| Kernel closures capture only local values, never `this` or a model object, and kernels are top-level functions. | Serialization size and correctness. |
| No `null` in Scala code except at Spark boundaries; `Option` for absence; typed validation errors. | Explicit failure modes. |
| No iteration over hash-based collections in numerical code; iterate over sorted keys. | Iteration order changes floating-point sums. |
| No `collect` outside size-guarded `collect…` methods. | NN-3. |
| `StrictMath` for transcendental functions in deterministic kernels. | R0 across platforms. |
| Scaladoc on public APIs, with formulas and specification references. | Traceability. |
| scalafmt formatting; scalafix lint rules, including import bans for `engine` (ARCH-2). | Mechanical enforcement. |

---

## 14. Documentation requirements

| Document | Content |
|---|---|
| Specification (per feature, §7.1) | Statistics, conventions, distributed plan, validation plan |
| User guide (per family) | Input schema, parameters, synthetic-data examples, outputs, limitations, performance guidance |
| Architecture documents and ADRs | Layers, backend, layouts, reduction; decisions with alternatives and evidence |
| Parity matrix and validation reports | Status per feature; evidence per release |
| Compatibility matrix | Spark, DBR, Scala, JDK, tiers |
| Migration guide | Mapping from `pprof_py` calls to `pprof_spark`; intentional differences |
| Example jobs | Bundle-deployable examples on synthetic data |

Each model's documentation explains both the statistics and the distributed implementation, including its relationship to `pprof_py`, its reference implementation, its validation results, and its limitations.

---

## 15. Working agreement for AI-assisted development

AI assistants working on this project follow every rule above, plus the following.

| Rule | Detail |
|---|---|
| Evidence | Never state that code compiles, passes, matches, or is faster without the run that shows it (NN-8). |
| Platform APIs | Check Spark, Databricks, and sbt APIs against documentation for the pinned versions, and mark anything unverified as unverified. |
| Execution location | For every code path, state whether it runs on the driver or on executors, and whether it is Connect-compatible. |
| Statistical impact | Give every behavioral difference a discrepancy class (§3.5); never present one as a refactor. |
| Tests with code | Every change includes tests at the appropriate layers. |
| Reviewable changes | One self-contained change per round; a design sketch first for non-trivial work. |
| Protected data | Never request, accept, or generate real patient data; use the synthetic generators. |
| Decision rights | Propose statistical and design decisions; do not make them. |
| State | Update the state files of §0 at the end of each round. |
| Context files | With Claude Code, keep a short `CLAUDE.md` (under about 200 lines) that points to this document, and keep area-specific conventions in path-scoped rule files. |

---

## 16. Phase 0: decisions and spikes

| ID | Decision | Why it matters | Default if undecided |
|---|---|---|---|
| D-01 | Supported compute tiers for v1 | Backend strategies and test matrix | Tier 1 only; engine kept Connect-compatible |
| D-02 | Target and secondary runtimes | Scala patch, JDK, Spark line | DBR 18 LTS; DBR 19 as a non-blocking canary |
| D-03 | Python access in v1 | Adoption by Python-first users | Job-based access; py4j wrappers deferred |
| D-04 | Design envelope and performance targets | Layout thresholds; success criteria | Ranges of §2.3; targets set after the first benchmarks |
| D-05 | `pprof_py` pin and reference versions | Parity baseline | Latest release whose validation suite passes for Cox features |
| D-06 | Default Cox tie method | Parity versus R familiarity | Breslow, the reference default |
| D-07 | Project license | Distribution and reuse | MIT, matching `pprof_py` |
| D-08 | Artifact distribution | How consumers install the package | GitHub Releases plus a Unity Catalog volume |
| D-09 | Tolerance calibration | Strictness of the gate | Initial values of §8.4 |
| D-10 | Root package and artifact names | Stability of public names | Decided before the first release |

| ID | Question | Method | Success criterion |
|---|---|---|---|
| S-01 | Can the JAR target DBR 18 LTS (Scala 2.13.16) while CI tests against open-source Spark 4.1.x? | Build per §11.3; deploy; run a kernel on a DBR 18 LTS job cluster; try compiling `engine` against Databricks Connect as the alternative | Passing smoke test on the runtime; ADR |
| S-02 | Does the Dataset-only backend run unchanged under Spark Connect? | Run the T3 suite in local Connect mode; run a JAR task on standard and serverless compute | Results within T-part, bitwise expected (R2) |
| S-03 | What does table materialization cost relative to persist? | Benchmark Cox iterations both ways on Tier 1, and table mode on serverless | Recorded ratio; layout guidance |
| S-04 | Can third-party Spark ML models be used through Spark Connect ML? | Register through `ServiceLoader` on a test server; call `fit`, `transform`, and custom attributes | Documented answer with limits |
| S-05 | What does deterministic mode cost (`StrictMath`, ordered reduction)? | Microbenchmarks and one Cox benchmark | Overhead recorded; default confirmed |
| S-06 | Is Codespaces usable under institutional policy? | Devcontainer with a JDK, sbt, and local Spark tests | Working environment, or a documented alternative |
| S-07 | Which block sizes and partition counts work at envelope scale? | Synthetic data at 10⁸–10⁹ rows | Defaults for DIST-4 and §10.3 |

---

## Appendix A. Changes from v1 and their rationale

| Area | v1 | v2 | Why |
|---|---|---|---|
| Platform | "Databricks"; "Spark 4.1.x initially" | Pinned to DBR 18 LTS (Spark 4.1.0, Scala 2.13.16, JDK 21 default), with support tiers by compute mode | Access modes and serverless impose hard API limits, and Scala patch skew is a real failure mode |
| Spark API stance | DataFrame/Dataset first because Spark guidance favors it | Engine restricted to the shared Classic/Connect API by its compile classpath; an RDD backend only as a measured Classic optimization | The binding constraint is Connect compatibility; Catalyst does little for numerical loops |
| Broadcasting | "Broadcast only small read-only objects" | Closure capture for O(p) parameters; broadcast-joined DataFrames for O(m) parameters | Broadcast variables require `SparkContext`, which Connect lacks |
| Spark ML | Integration layer | Separate `ml` module with project-owned parameters; Connect limits documented | Enforces the core's independence; shared parameter traits are unstable APIs |
| Local execution | "Not part of the development requirement" | Local-mode Spark in CI is mandatory; Codespaces optional | Kernel and invariance tests need Spark without a cluster |
| Partition invariance | Stability within tolerance | Logical blocks, deterministic reduction, canonical order, and reproducibility levels R0–R3 | Bitwise reproducibility across cluster sizes makes bit-identity a usable refactoring bar |
| Testing | Unit, reference, and distributed parity tests | Twelve layers; three parity levels; metamorphic tests; a tolerance policy with negative controls; fixture provenance | Catches defects that end-to-end comparisons miss |
| Reference governance | "pprof_py is the reference" | Pinned reference, inherited validation status, known limitations, discrepancy protocol | Defines parity even where the reference has gaps |
| Model hierarchy | Inheritance tree | Capability traits and composition | New families plug in without restructuring |
| Scale | Small versus large results | n, m, K, p, B taxonomy with placement rules | Precise rules for what may reach the driver |
| Statistics | List of behaviors to preserve | Conventions registry; Cox and logistic specifics (risk-set rule, `timefix`, Efron weights, offset subtlety, tie defaults, SerBIN block elimination) | Parity failures come from conventions, not headline methods |
| Randomness | Not covered | Partition-invariant generators; supplied folds and replicates for parity | `rand(seed)` depends on partitioning |
| Roadmap | Phases by family | Phase 0 with spikes; walking skeleton; Cox in four gated stages; SRR pipeline; mapping to `pprof_py` classes | Platform risks surface before breadth |
| Interfaces | Scala and Spark ML | Job-runner application layer and Python access options added | Production use and Python-first users |
| Governance | No PHI in CI | Logging rules, real-data validation inside the environment, licensing constraints | Leakage through logs and code provenance are realistic risks |
| CI/CD | GitHub → Actions → JAR → Databricks | Concrete workflows, OIDC, bundles, immutable artifacts, job clusters | Reproducible, secret-free deployment |
| Success | Qualitative | Measurable criteria and a crossover analysis against `pprof_py` | Spark is not automatically the faster tool |

## Appendix B. Glossary and notation

| Term | Meaning |
|---|---|
| n, m, p, K, B | Rows; providers or groups; covariates; distinct event times per stratum; logical blocks |
| Working set | Validated, laid-out, materialized input to a fit, organized into logical blocks |
| Logical block | Record holding a deterministic subset of rows as primitive arrays in canonical order |
| Kernel | Pure function computing a partial statistic from one block |
| Layout | Rule for composing and ordering blocks (§6.6) |
| Parity gate | Criteria for declaring a feature stable (§9.8) |
| Tier | Level of support for a Databricks compute mode (§5.2) |
| SMR, SHR, SRR | Standardized mortality, hospitalization, and readmission ratios |
| O/E | Observed over expected |
| SerBIN | Serial blockwise inversion Newton (Wu et al., 2022) |
| DBR | Databricks Runtime |
| Classic, Connect | Spark's in-process and client–server execution modes |

## Appendix C. References

| Methods reference | Relevance |
|---|---|
| He K, Kalbfleisch JD, Li Y, Li Y (2013). Evaluating hospital readmission rates in dialysis facilities; adjusting for hospital effects. *Lifetime Data Analysis* 19:490–512. | Foundation of the provider-profiling framework |
| He K (2019). Indirect and direct standardization for evaluating transplant centers. *Journal of Hospital Administration* 8(1):9–14. | Standardization definitions |
| Wu W, Yang Y, Kang J, He K (2022). Improving large-scale estimation and inference for profiling health care providers. *Statistics in Medicine* 41(15):2840–2853. | SerBIN; large-m fixed effects |
| Wu W, Kuriakose JP, Weng W, Burney RE, He K (2023). Test-specific funnel plots for healthcare provider profiling leveraging individual- and summary-level information. *Health Services and Outcomes Research Methodology* 23(1):45–58. | Funnel limits |
| Cox DR (1972). Regression models and life-tables. *Journal of the Royal Statistical Society, Series B* 34:187–220. | Proportional hazards model |
| Breslow N (1974). Covariance analysis of censored survival data. *Biometrics* 30:89–99. | Breslow ties and baseline hazard |
| Efron B (1977). The efficiency of Cox's likelihood function for censored data. *Journal of the American Statistical Association* 72:557–565. | Efron ties |
| Andersen PK, Gill RD (1982). Cox's regression model for counting processes: a large sample study. *Annals of Statistics* 10:1100–1120. | (start, stop] counting-process formulation |
| Lin DY, Wei LJ (1989). The robust inference for the Cox proportional hazards model. *Journal of the American Statistical Association* 84:1074–1078. | Robust (sandwich) variance |
| Therneau TM, Grambsch PM (2000). *Modeling Survival Data: Extending the Cox Model*. Springer. | Residuals and computation as implemented in `survival` |
| Fine JP, Gray RJ (1999). A proportional hazards model for the subdistribution of a competing risk. *Journal of the American Statistical Association* 94:496–509. | Fine-Gray model |
| Efron B (2004). Large-scale simultaneous hypothesis testing: the choice of a null hypothesis. *Journal of the American Statistical Association* 99:96–104. | Empirical null |
| Bates D, Mächler M, Bolker B, Walker S (2015). Fitting linear mixed-effects models using lme4. *Journal of Statistical Software* 67(1):1–48. | Random-effect reference |
| Chan TF, Golub GH, LeVeque RJ (1983). Algorithms for computing the sample variance: analysis and recommendations. *The American Statistician* 37:242–247. | Stable distributed co-moments |
| Higham NJ (2002). *Accuracy and Stability of Numerical Algorithms*, 2nd ed. SIAM. | Summation and factorization error |
| Demmel J, Grigori L, Hoemmen M, Langou J (2012). Communication-optimal parallel and sequential QR and LU factorizations. *SIAM Journal on Scientific Computing* 34(1):A206–A239. | TSQR |

| Platform and tooling source | Location |
|---|---|
| `pprof_py` README: models, validation, limitations, workflows | https://github.com/UM-KevinHe/pprof_py |
| Spark 4.1.0 release notes | https://spark.apache.org/releases/spark-release-4.1.0.html |
| Spark 4.2.0 release notes | https://spark.apache.org/releases/spark-release-4-2-0.html |
| Spark Connect overview | https://spark.apache.org/docs/latest/spark-connect-overview.html |
| Spark ML shared parameter traits (stability note) | https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/param/shared/HasTol.html |
| `RFormula` category ordering | https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/RFormula.html |
| SPARK-58223 (`classic()` builder selector) | https://github.com/apache/spark/pull/57379 |
| Databricks Runtime versions and support | https://docs.databricks.com/aws/en/release-notes/runtime |
| Databricks Runtime 18 LTS | https://docs.databricks.com/aws/en/release-notes/runtime/18 |
| Standard compute limitations | https://docs.databricks.com/aws/en/compute/standard-limitations |
| Serverless compute limitations | https://docs.databricks.com/aws/en/compute/serverless/limitations |
| Serverless release notes (JAR tasks) | https://docs.databricks.com/aws/en/release-notes/serverless |
| Databricks-compatible JARs | https://docs.databricks.com/aws/en/jobs/jar-create |
| Unity Catalog allowlist | https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist |
| Spark ML on Spark Connect in Databricks | https://docs.databricks.com/en/machine-learning/train-model/distributed-training/distributed-ml-for-spark-connect.html |
| GitHub Actions with Databricks | https://docs.databricks.com/aws/en/dev-tools/ci-cd/github |
| SIP-51 (Scala 2.13 library compatibility) | https://docs.scala-lang.org/sips/51.html |
| sbt `allowUnsafeScalaLibUpgrade` | https://github.com/sbt/sbt/pull/8012 |
| scala/bug#13181 | https://github.com/scala/bug/issues/13181 |
| `sbt/setup-sbt` | https://github.com/sbt/setup-sbt |
| `survival::coxph.control` | https://rdrr.io/cran/survival/man/coxph.control.html |
| `survival::aeqSurv` | https://search.r-project.org/CRAN/refmans/survival/html/aeqSurv.html |
