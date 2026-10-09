# pprof_spark — Project Context

**Version** 2.4 · **Date** 2026-10-09 · **Supersedes** v2.3 (2026-10-07)
**Status** Living document. Statements marked *(re-verify)* describe external platforms or the state of the reference implementation; re-check them before relying on them.
**Reference implementation** [`pprof_py`](https://github.com/UM-KevinHe/pprof_py) (MIT). Pinned: v0.7.0, commit `9320766` (D-05); the reference-tool versions are recorded in `reference/REFERENCE.lock`.


> **v2.4 folds in Phase 2's slices 2a to 2f** (D-30 to D-43): logistic fixed effects with covariate
> and provider inference, standardization, a job runner and Python access; the three-stage SRR model; the
> T-opt tolerance class (D-42); and the Python CI fix (round 57.1). v2.3 folded in Phases 1a to 1d.
>
> **v2.3 folds in Phases 1a to 1d** (D-20 to D-29 in `DECISIONS.md`) and the maintainer's
> clarification of D-14: AI assistants never access his Databricks environment, and he tests
> pprof_spark on Databricks himself, once the whole package is done (D-28). v2.2 folded in the
> decisions of Phase 0 (D-01 to D-19). Recorded decisions keep precedence over this document (§0).
---

## 0. How to use this document

This document defines what `pprof_spark` is, the constraints it operates under, and the rules for building it. It is written for the maintainer, contributors, reviewers, and AI coding assistants, and is intended to be loaded as standing project context.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119 and RFC 8174 when, and only when, they appear in capitals. Normative requirements carry identifiers (for example `DIST-3`) so that pull requests, decisions, and reviews can cite them.

When sources disagree, precedence is: (1) approved statistical specifications in `docs/spec/` and recorded decisions in `DECISIONS.md`; (2) the behavior of the pinned reference implementation, and of R where the reference is validated against R; (3) this document; (4) existing code. A conflict between this document and an approved specification is resolved in favor of the specification, and this document is corrected in the same change.

Project state lives in a small set of files updated at the end of every working round: `STATUS.md` (current phase and gate status), `OPEN_ITEMS.md`, `DECISIONS.md` (sequentially numbered, never renumbered), `DISCREPANCIES.md` (the register defined in §3.5), `HANDOFF.md` (continuity between sessions), `docs/adr/` (architecture decision records), and `docs/gates/` (phase gate reviews).

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
| NN-13 | **Build for the supported runtimes.** The Spark line, Scala patch version and JDK bytecode level are pinned (§5): one JAR for open-source Spark 4.1.x and, by construction, Databricks Runtime 18 LTS. A linkage compile checks the Scala pin on every push. |

---

## 2. Mission, users, and success

### 2.1 Mission

`pprof_spark` is a Spark-native Scala package for large-scale healthcare provider profiling. It reimplements the validated statistical methods of `pprof_py` as distributed algorithms that preserve their statistical definitions and validated behavior. The objective is not to translate Python into Scala; it is to redesign each method around Spark's execution model, so that fitting, inference, standardization, and provider-level testing scale to national datasets without moving patient-level data to a single machine. The package should feel like a statistical library built for Spark.

### 2.2 Users and workloads

The primary users are statisticians and analysts who run national-scale provider profiling on Spark clusters, Databricks among them — for example the CMS dialysis-facility measures that `pprof_py` already supports (two-stage SMR and SHR from Cox models; readmission measures from logistic models with provider effects) and transplant-center or hospital profiling — in settings where data volume, governance rules that keep patient-level data inside the governed environment, or production repeatability make single-node execution impractical. Secondary users are data engineers who run fitted models inside scheduled pipelines.

### 2.3 Design envelope

These are planning assumptions (D-04); performance targets are set after the first benchmarks. They determine which layouts and thresholds must perform well.

| Symbol | Meaning | Planning range |
|---|---|---|
| n | Patient or patient-period rows | 10⁷ – 10⁹ |
| m | Providers (fixed effects, strata, or clusters) | 10² – 10⁶ |
| p | Covariates | 1 – 10³ (typically 10 – 100) |
| K | Distinct event times per stratum | Up to about 10⁴ (daily resolution over several years) |
| B | Logical blocks of the working set (§6.5) | 10¹ – 10⁵ |

### 2.4 Definition of success

For each in-scope model family, `pprof_spark` succeeds when it (a) passes the parity gate against the pinned reference (§9.8); (b) fits workloads at the top of the design envelope through distributed computation, without patient-level data on the driver, within the performance targets set in Phase 0; (c) produces deterministic, partition-invariant results (NN-4); (d) runs as a versioned, repeatable Spark application that records complete reproducibility metadata; and (e) is measurably faster or more feasible than single-node `pprof_py` beyond a documented crossover point (§3.7).

### 2.5 Non-goals for the first releases

The first releases will not reproduce `pprof_py`'s plotting and presentation layer (`pprof_spark` emits tidy result tables that existing tools, including `pprof_py`'s plotting module, can render); reproduce pandas or NumPy idioms; support execution outside Spark as a user-facing mode; wrap or subclass Spark's built-in ML models; preserve Python method names where they are unidiomatic in Scala; implement exact-partial-likelihood ties; or parse model formulas (`pprof_py` also takes a numeric design matrix).

---

## 3. Relationship to pprof_py

### 3.1 A companion, not a port

`pprof_py` and `pprof_spark` are related implementations of the same methods, not one package with two language bindings. `pprof_py` remains optimized for local and medium-scale analysis with NumPy, SciPy, and numba, rich local result objects, publication-oriented presentation, and exploratory work. `pprof_spark` is optimized for very large datasets, distributed fitting, cluster execution, Spark-native pipelines, and repeatable production runs. APIs differ where the execution model requires it; mathematical behavior does not.

### 3.2 The pinned reference

`pprof_py` is the reference implementation for every feature until that feature passes the parity gate. The reference is pinned, not tracked: `reference/REFERENCE.lock` records the `pprof_py` commit SHA and version, the Python, NumPy, and SciPy versions, and the version of every R package used to produce reference fixtures. At the time of writing, `pprof_py` documents validation against R 4.3.3 with `survival` 3.5.8, `glmnet` 4.1.8, and `lme4` 1.1.35.1 *(re-verify)*.

PAR-1: Re-pinning is a deliberate event. A dedicated pull request updates the lock file, regenerates fixtures, produces a diff report of every changed fixture value, and classifies each change under §3.5 before merge.

Current pin (D-05): pprof_py v0.7.0, commit `9320766`, with R 4.3.3 and survival 3.5-8 from Ubuntu 24.04's archive and the Python packages of `reference/fixtures/requirements.txt`.

### 3.3 Inherited validation status

`pprof_py` reports *(re-verify at the pinned commit)*: Cox coefficients, standard errors, log-likelihood, baseline hazard, and martingale residuals matching `survival::coxph()` to 1e-8–1e-14 relative error across right-censored, left-truncated, stratified, offset, and weighted data, under both Breslow and Efron ties; robust and clustered variance validated against `coxph(robust = TRUE, cluster = ...)`; penalized regression validated against `glmnet`; linear random-effect models agreeing with `lme4::lmer` at about 1e-7–1e-9; logistic random-effect models agreeing with `glmer(nAGQ = 1)` at about 2e-5; and no external R reference for group lasso, provider-penalized, and discrete-time models.

PAR-2: This determines the parity target for each feature. Features validated against R are tested against both `pprof_py` fixtures and R fixtures. Features without an external reference are tested against `pprof_py` fixtures plus checks that depend on neither implementation: optimality (KKT) conditions for penalized fits, analytic special cases, and simulation studies of bias and coverage.

Verified at the pin for survival (2026-10-06): pprof_py's survival suite passes with its committed R results (275 passed, 1 skipped for an optional package), and R 4.3.3 with survival 3.5-8 regenerates those results byte for byte (X-007). The lme4 comparison script is not in pprof_py's repository (OI-13).

### 3.4 Known reference limitations and open items

*(Re-verify every row at the pinned commit and record the outcome in `DISCREPANCIES.md`.)*

| Reference behavior | Implication for pprof_spark |
|---|---|
| `CoxPH` defaults to Breslow ties; R's `coxph` defaults to Efron. | The `pprof_spark` default is Breslow, for parity with the reference. The tie method is recorded in every fit summary, and R fixtures always set `ties` explicitly. |
| `CoxPH` does not warn when `max_iter` is reached. | `pprof_spark` reports non-convergence (NN-10). No estimate changes. |
| `CoxPH(fit_intercept=True)` fits, but every `predict_*` method then raises. | `pprof_spark` rejects an intercept in Cox models at validation, because it is not identifiable (class C). |
| `ties="exact"` is not implemented. | Out of scope. |
| `FineGrayPH` on left-truncated data does not reproduce R's `finegray()` weights (about 3e-3 coefficient difference). | Choose the parity target before the competing-risks phase (class B). |
| The SerBIN fixed-effect solver can stop at a near-null fit when covariates are far from zero (open item C27). | `pprof_spark` centers covariates internally, which leaves the MLE unchanged; settled in slice 2a (`docs/spec/logistic/`, `DISCREPANCIES.md`). |
| `LogisticThreeStageModel.sigma_sensitivity()` fails when σ̂ = 0, and whenever σ_c's profile interval reaches 0 (`ZeroDivisionError` in the stage 3 refit; ts-synthetic and ts-shuffled). | `pprof_spark` takes the σ = 0 limit (X-005, class B). |
| pprof_py's exact Poisson-binomial upper tails are 1 − cdf from an FFT distribution, and a one-sided tail that is exactly 1 is a rounded sum. | `pprof_spark` computes the smaller tail directly; comparisons skip tails pprof_py cannot resolve (X-024, class B). |
| The three-stage stage 2 optimum is optimizer-limited (Nelder–Mead stopping on changes of 1e-7); lme4's `glmer` treats non-integer outcomes differently from pprof_py's continuous `y_adj`. | `pprof_spark` minimizes the exactly converged Laplace deviance and compares the optimum under T-opt (X-030, D-42); `glmer` is informational where `y_adj` is fractional. |
| Some reference measures have unresolved discrepancies with internal R code (for example `IUR.fac` and `cal_SMR_pro_adj`). | Dependent features are not parity-gated until resolved. |
| pprof_py's README reports 26 failures in a fresh R-comparison run; at v0.7.0 none of the survival tests fail (X-007, X-008). | No Cox feature is excluded; other families are checked when their phase begins. |
| `CoxPH` tests for a log-likelihood decrease, and halves the step, before testing convergence; R tests convergence first (X-010). | pprof_spark follows pprof_py (class B, bug-compatible, flagged): converged coefficients can differ from R by half a final step. |
| With non-integer case weights R's `coxph` reports a robust variance by default; pprof_py's default is model-based (X-009). | Fixtures and comparisons with R use `robust = FALSE`. |

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

`pprof_py` is fast on one machine: its README reports a 200,000-row, 3,000-stratum, 6-covariate Cox fit in about 1.8 seconds on one vCPU *(re-verify)*. Spark adds job-scheduling latency to every Newton iteration and a pass over the data per iteration, so for data that fit comfortably in one machine's memory — including a large Spark driver node — `pprof_py` can be the faster tool. `pprof_spark` is justified when data exceed single-node memory or runtime budgets, when governance requires processing in place, or when the fit is part of a production Spark pipeline.

PERF-1: Phase 1 publishes a measured crossover analysis for Cox (runtime as a function of n, p, strata, and cluster size, for both packages), and the documentation includes a decision guide derived from it.

When a problem's sufficient statistics are small — the covariate-free stage-2 baseline fit of the SMR workflow, or event-time aggregates in general — aggregating in Spark and solving on the driver is often the best distributed design rather than a compromise; the engine supports it directly (§6.6).

---

## 4. Scope and roadmap

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 — Foundations and spikes | Repository, multi-module build, CI, test harness, fixture pipeline, numerics and backend skeletons; decisions and spikes (§16). The Databricks deployment path is deferred (D-14). | CI green on the statistics-free platform skeleton under Classic Spark and Spark Connect (D-11); every spike recorded as an ADR; Phase 0 decisions resolved. Gate review: `docs/gates/phase-0.md`. |
| 1a — Cox estimation core | Right censoring, strata, offsets, case weights, Breslow and Efron ties; coefficients, information, model-based covariance, standard errors, Wald inference, log partial likelihood; convergence and step-halving; aliasing. | Parity gate passed for these features. |
| 1b — Counting process and baseline | (start, stop] data and left truncation; per-stratum baseline cumulative hazard and survival; prediction (linear predictor, relative hazard, cumulative hazard, survival). | Parity gate passed. |
| 1c — Residuals and robust inference | Martingale residuals (correct under left truncation and Efron ties), score and dfbeta residuals, robust sandwich and clustered variance. | Parity gate passed. |
| 1d — Provider workflows | Two-stage SMR and SHR; expected counts; O/E ratios; exact Poisson intervals and tests; flags; provider result tables; job-runner entry point. | Parity gate passed; end-to-end Spark application at design-envelope scale (run by the maintainer on his Databricks workspace once the whole package is done; D-28). |
| 2 — Logistic provider models | Large-m fixed effects (SerBIN-type blocked Newton); provider tests (Wald, score, exact Poisson-binomial, bootstrap); direct and indirect standardization; then the three-stage SRR pipeline, including the stage-2 random-intercept variance estimation it needs. | Parity gate passed. |
| 3 — Linear fixed effects | Profile (within) estimation, standardization, inference. | Parity gate passed. |
| Later | Penalized models (elastic net, group lasso, provider-penalized), random and mixed effects, shared-frailty Cox, time-varying coefficients, discrete-time survival, competing risks (cause-specific, Fine-Gray), variable selection, inter-unit reliability, empirical-null calibration, funnel limits. | Each family enters through Phase-0-style spikes and its own gate. |

ROAD-1 (walking skeleton, as amended by D-11): Phase 0 ends with a statistics-free platform skeleton (data contract, logical blocks, a toy kernel, block-ordered reduction, result tables and persistence) that is green in CI under Classic Spark and Spark Connect (ADR-0004). The first Cox slice opens Phase 1a: stratified (StratumLocal), right-censored, Breslow, with model-based variance, passing through every layer and all three parity levels. Statistical breadth is then added behind the same skeleton.

ROAD-2: Each phase ends with a gate review recorded in `STATUS.md`. A phase cannot close with open class A or B discrepancies affecting its features.

Status (2026-10-09): Phases 0 to 1d are closed at parity-verified. Phase 2's slices 2a to 2f are implemented with their parity tests (slice 2a closed, D-32); the three-stage Python wrapper and the Phase 2 gate review remain. Scale verification for every phase happens once, at package level (D-28).

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

### 5.1 Targets and platform facts

pprof_spark is a standalone Apache Spark package (D-14). It targets open-source Spark 4.1.x with Scala 2.13 and Java 17 or later, and keeps one JAR compatible with Databricks Runtime 18 LTS by construction (§5.3); the maintainer tests it on Databricks himself, and AI assistants never access his Databricks environment (D-14 as clarified, D-28).

| Item | Value *(as of 2026-10-06; re-verify)* |
|---|---|
| Open-source Spark 4.1.0 to 4.1.3 | Built with Scala 2.13.17; Spark 4.2.0 with 2.13.18 |
| Open-source Spark 4.0.x | Built with Scala 2.13.16 |
| Databricks Runtime 18 LTS | Apache Spark 4.1.0, Scala 2.13.16, JDK 21 by default; a rolling release whose dated updates change the runtime without changing its version |
| JDK | Java 17 bytecode; CI tests on JDK 17 and 21 |

### 5.2 Execution modes and support

| Mode | Status |
|---|---|
| Classic Spark, local or cluster | Supported (D-01); every test layer runs here |
| Spark Connect | `engine` stays Connect-compatible (PLAT-2), and its suites run against a local Connect server in CI (T8, ADR-0002); the `ml` adapters are Classic only |
| Databricks standard and serverless compute | Deferred (D-14, ADR-0008). The constraints recorded in v2.0 apply when this is reopened: no `SparkContext` or RDDs in standard access mode; no caching and at most 1 GB for custom code on serverless |

PLAT-1: Classic Spark gates releases (D-01).

PLAT-2: Engine code MUST remain Connect-compatible. This costs little, and every Spark Connect deployment needs it.

### 5.3 Version pinning and the Scala patch-skew hazard

The Scala 2.13 standard library is only backward binary compatible (SIP-51): code compiled against a newer 2.13 patch release can call library methods that an older runtime lacks, and then fails with `NoSuchMethodError` (scala/bug#13181 reports Spark 4.1.0 failing this way on a 2.13.16 runtime). Open-source Spark 4.1.x depends on scala-library 2.13.17, and the Spark Connect client also on scala-compiler 2.13.17. sbt keeps scala-library, scala-reflect and scala-compiler at one version and, since 1.10, fails when `scalaVersion` is older than the scala-library on the classpath, unless `allowUnsafeScalaLibUpgrade := true`. With that setting the newer library is on the compilation classpath, not only on the test classpath.

PLAT-3: `scalaVersion` is 2.13.16, the oldest Scala patch among the supported runtimes, and MUST NOT exceed it.

PLAT-4: Spark modules set `allowUnsafeScalaLibUpgrade := true`, so that tests run on open-source Spark 4.1.x. A linkage compile (`sbt -Dpprof.linkageCheck=true compile`, in CI on every push) forces scala-library, scala-reflect and scala-compiler to 2.13.16, so main code cannot link against newer library methods. `numerics` keeps its whole classpath at 2.13.16; munit 1.2.0 is the newest test framework release built with that version.

PLAT-5: Bytecode targets Java 17 (`-release 17`), so one JAR runs on JDK 17 and JDK 21; CI runs the tests on both.

PLAT-6: Spark and Scala dependencies are `provided`; the JAR bundles no Spark, Scala, or runtime-provided libraries. Third-party runtime dependencies are avoided and, when unavoidable, shaded.

PLAT-7: Only public Spark APIs are used. Package-private APIs (`private[spark]`, `private[ml]`) are never reached through package-placement tricks, and APIs that Spark marks unstable, including Spark ML's shared `Has*` parameter traits, which are documented as changeable between minor versions, are wrapped behind project-owned interfaces.

### 5.4 Compile-time API surface per module

| Module | Compiles against (`provided`) | What the restriction guarantees |
|---|---|---|
| `numerics` | Scala standard library only | Spark-free kernels with millisecond tests |
| `engine` | `spark-sql-api`, the shared Classic/Connect interface | Connect compatibility enforced by the compiler |
| `ml` | `spark-mllib` and `engine` | Spark ML adapters; Classic only |
| `app` | `engine`, plus `ml` where needed | Job-runner entry points |
| `testkit` | `spark-sql` (Classic), test scope | Shared test harness, data generators, fixture loaders |

Since Spark 4, the Scala `SparkSession`, `Dataset`, and related types form a shared interface with Classic and Connect implementations. Code limited to that interface runs in either mode; Classic-only members such as `sparkContext` and RDD conversions throw under Connect.

The `engine` compile classpath does not catch every Connect-incompatible call: spark-sql-api 4.1.0 depends on spark-connect-shims, which define placeholder `SparkContext`, `SparkConf` and `RDD` classes, and the shared interfaces declare `sparkContext`, `rdd` and `checkpoint`. `scripts/check-engine-api.sh`, with a self-test, therefore enforces ARCH-2 on every push. On Classic test classpaths the shims would shadow spark-core, so the build removes them there; the Connect client jar, which repackages Connect classes differently, goes last on test classpaths (ADR-0002).

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
 Users:   Scala engine API       Spark ML pipelines        Spark job entry points
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

DIST-4: Blocks hold roughly 1–16 MB of primitive arrays. This amortizes decoding costs, keeps kernels allocation-free, and stays well inside serverless memory limits for custom code. Block size is a recorded layout parameter. The default target is 4 MB (`BlockOptions.targetBlockBytes`, ADR-0004).

### 6.6 Layouts, partitioning, and skew

| Layout | Block composition and order | Used by |
|---|---|---|
| StratumLocal | Whole strata bin-packed into blocks by size; rows sorted by time within each stratum | Stratified Cox with many strata, including stage 1 of SMR and SHR |
| TimeRange | A large stratum split into time-contiguous blocks; a two-pass scan gives each block its risk-set offsets | Unstratified Cox, or strata too large for one block |
| ProviderLocal | Whole providers bin-packed into blocks by size or estimated cost | Logistic and linear fixed effects; per-provider exact tests |
| EventGrid | Each block emits risk-set statistics aggregated onto the distinct event times (§7.3.3) | Covariate-free or small p·K problems, such as the stage-2 baseline fit |
| Any blocked layout | Reuse of an existing working set | Single-pass, row-separable computations: direct standardization, residuals, predictions |

DIST-5: The layout plan (block assignment, bin packing, splits) is computed on the driver from data summaries — strata count and size distribution, K, p, m — with deterministic tie-breaking, and applied through an explicit block-identifier column rather than Spark's sampling-based range partitioning or hash partitioning. The plan is recorded in the fit summary.

DIST-6 (canonical order): Within each block, rows are ordered by layout keys, then time, then a stable row identifier when one is supplied, and otherwise by full row content compared lexicographically. Results are therefore bitwise invariant to input row order. Negative zeros are normalized to positive zeros before ordering, because the two compare equal (ADR-0004).

Provider and stratum sizes are heavy-tailed. Largest-first bin packing balances blocks; per-provider computations whose cost grows faster than linearly, such as exact tests, are packed by estimated cost; strata that exceed the block budget are split and handled by the TimeRange technique. Adaptive query execution's skew handling does not apply to custom kernels and is not relied on.

TimeRange has two consequences that its specification must state (OI-03). With left truncation, a row whose (a, b] spans a block boundary needs an entry record in the block that contains a, so such rows appear in two blocks. And because the block offsets depend on β, TimeRange takes two passes per Newton iteration. Until TimeRange exists, a group larger than a block gets a block of its own (ADR-0004, OI-30).

### 6.7 Iteration management

Iterative fits keep the materialized working set fixed and vary only small parameters. Parameters of size O(p) reach kernels by closure capture. Parameters of size O(m), such as provider effects, travel as a small DataFrame keyed by block identifier and joined to the working set with a broadcast hint; this replicates them once per executor instead of serializing them into every task, and it works under Spark Connect. Because no iteration derives a DataFrame from the previous iteration's DataFrame, lineage does not grow and no checkpointing is needed. When per-row state must evolve, the engine uses table materialization rather than `localCheckpoint`, which loses data when executors are lost and is unavailable on serverless. Working sets are released in `finally` blocks.

### 6.8 Deterministic reduction and communication budget

Each kernel invocation returns one partial per logical block, keyed by block identifier. Partials are sorted by identifier and combined in that order with compensated summation. When B times the size of a partial exceeds the per-iteration driver budget (initially 64 MB), a two-level reduction groups partials by contiguous ranges of block identifiers, reduces each group in order on executors, and combines the group results in order on the driver. Symmetric matrices travel as packed upper triangles. Because the order is fixed by block identifiers rather than by task completion, results are bitwise identical across runs, cluster sizes, and physical partitionings (NN-4). An exploratory fast mode based on Spark SQL `Aggregator`s MAY exist; it is never the default and guarantees only agreement within tolerance.

DIST-7: Quantities under parity tolerance MUST NOT be computed with built-in floating-point SQL aggregates, whose summation order depends on task scheduling and on the execution engine (for example Photon versus the JVM). Integer counts are exempt.

DIST-8: Every floating-point reduction that follows a shuffle, such as summing dfbeta residuals within clusters, sorts its inputs by an explicit, data-derived key before summing.

At large p the partials themselves become large: a packed p×p partial is about 4 MB at p = 1,000, so the driver budget admits few blocks, and the two-level reduction would shuffle every partial on every iteration. Fixed reduction groups of consecutive blocks, co-located when the working set is built, would keep the order fixed without that shuffle; the design is open (OI-02) and must be settled before large-p work (D-04).

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

Models persist to a self-describing directory: `metadata.json` holding the metadata above, with numeric arrays in a round-trip-exact representation, plus Parquet files for m-scale tables such as baseline hazards and provider effects. Doubles are stored as their 64-bit patterns, because `Double.toString` differs between JDK 17 and JDK 21, and everything is written through Spark, so any file system Spark reaches works (ADR-0004).

PERS-1: Save followed by load yields bitwise-identical parameters and predictions.

PERS-2: Models persisted by each release are kept as test fixtures; later releases load them or fail with a clear message under a documented deprecation policy.

PERS-3: Persistence uses only public Spark APIs; the Spark ML adapters implement `MLWritable` and `MLReadable` on top of this format.

### 6.11 Spark ML adapter layer (`ml`)

Each estimator (for example `CoxPHEstimator extends Estimator[CoxPHModel]`) validates parameters, converts inputs, calls the engine, and wraps the engine's fitted model; each model implements `transform`, `copy`, and persistence. Parameters are project-owned traits (`entryCol`, `durationCol`, `eventCol`, `strataCol`, `offsetCol`, `weightCol`, `clusterCol`, `providerCol`, `featureCols`, `ties`, `maxIter`, `tol`) with defaults and validators. The event column uses 1 for an event. Spark's AFT estimator calls its equivalent column `censorCol` even though 1 means the event occurred; this package does not copy that naming.

Features arrive as a list of numeric columns (preferred, because the names carry into coefficient tables) or as a vector column, which is converted with `vector_to_array` and named from its ML attribute metadata when present. `transformSchema` validates types eagerly. `transform` appends explicitly named outputs (linear predictor, relative hazard, expected count) instead of overloading `predictionCol`. Provider-level results are DataFrames returned by model methods, not rows forced through `transform`.

Categorical encoding is a parity hazard. `StringIndexer` orders categories by descending frequency by default and `RFormula` drops the last category after ordering, so the default reference level differs from R's; with `stringIndexerOrderType = "alphabetDesc"`, `RFormula` drops the same category as R. Parity tests therefore use numeric design matrices, as `pprof_py` does.

Hyperparameter tuning uses `CrossValidator` with `foldCol` and group-level folds (patient or provider); random row splits leak information across clustered records.

Spark ML on Spark Connect discovers estimators through Java's `ServiceLoader` and restricts which model attributes remote clients may call. Whether third-party models can be exposed that way is unverified (spike S-04, deferred in ADR-0008); the `ml` module is Classic only (D-01).

### 6.12 Application layer and Python access

The `app` module provides plain Spark entry points for `spark-submit` or a Databricks JAR task: `pprof.spark.app.CoxJob`, `LogisticJob` and `ThreeStageJob`. Each reads a declarative, versioned run specification (version 1, with a `model` field; input path or table, column roles, fit options, outputs) and writes result tables, the fitted model and a run record (`docs/guide/cox-job.md`, `logistic-job.md`, `three-stage-job.md`); each job points another model's specification to its own job. Python and SQL users can work through tables and job parameters, and every run is auditable. Databricks deployment automation stays out of scope (D-14).

Python access (D-31, D-33, ADR-0009): the `pprof_spark` package (`python/`) wraps the engine through py4j in PySpark Classic, delegating every computation to the JARs through `pprof.spark.app.python.PythonApi`. Cox and logistic wrappers exist; the three-stage wrapper is next. The `python` CI job tests them against the JARs each commit builds (green since round 57.1). Spark Connect ML registration (S-04) stays deferred. Python-facing code never reimplements statistics.

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
| Convergence | Mirror the reference's rule. For orientation, `survival::coxph.control` stops when the relative change in log partial likelihood falls below `eps` = 1e-9, caps iterations at `iter.max` = 20, and halves steps that decrease ℓ. pprof_py tests for a decrease (and halves the step) before testing convergence, whereas R tests convergence first; pprof_spark follows pprof_py (X-010, class B, bug-compatible). |
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

Since Java 17, all floating-point arithmetic has strict IEEE 754 semantics (JEP 306): the same operations in the same order produce the same bits on any compliant JVM, and the JIT never fuses a multiply and an add unless `Math.fma` is called explicitly. Transcendental functions in `java.lang.Math` may differ by up to one ulp across implementations and intrinsics, whereas `StrictMath` is specified to reproduce the fdlibm algorithms bit for bit. Kernels therefore use `StrictMath` for `exp`, `log`, `log1p`, `expm1`, `pow` and every other transcendental function, and `scripts/check-engine-api.sh` rejects `math.*` and `Math.*` transcendental calls in `numerics` and `engine` main code. Spike S-05 measured the cost (ADR-0006): 1.1 to 1.9 times per call, and almost nothing in a Cox-shaped kernel whose sums are fused into one pass over the rows. Small dense linear algebra uses the project's own routines rather than whatever native BLAS a runtime loads. Decimal input columns are converted to doubles explicitly, under the specification's rules.

### 8.2 Reproducibility levels

| Level | Definition | Requirement |
|---|---|---|
| R0 — bitwise | The same build, input, and configuration (including layout and block-size parameters) give identical bits on every run, on any cluster size or physical partitioning. | MUST in deterministic mode, the default |
| R1 — layout-invariant | Different block sizes or layouts agree within the T-part tolerance. | MUST |
| R2 — platform-invariant | CI local mode and a cluster, Classic and Connect, and JDK 17 and JDK 21 agree. | MUST within T-part; SHOULD be bitwise |
| R3 — cross-implementation | Results agree with `pprof_py` and R within the parity tolerance classes. | MUST for gated features |

NUM-2: Changes declared non-behavioral (refactors, performance work) MUST preserve R0 identity on the golden runs, demonstrated alongside a negative control showing that the comparison detects a deliberate one-ulp perturbation.

### 8.3 Stability techniques

Covariates are centered internally, and scaled when poorly conditioned, with reported quantities transformed back. Exponentials are formed after per-stratum or per-provider shifts, which are exact for the Cox partial likelihood, and log-sum-exp, log1p, and expm1 forms are used wherever they apply. Accumulations use blocked summation with compensation across blocks; co-moments use pairwise updates. Information matrices are explicitly symmetrized, factorized by Cholesky with the reference's singularity tolerance, and their condition numbers are reported. Step control follows the reference, and every fit records its convergence trace.

### 8.4 Tolerance policy

Tolerances live in one versioned file (`testkit/src/main/resources/tolerances.conf`), tests refer to them by class rather than by literal value, and they change only under NN-9. A scalar passes when |a − e| ≤ atol + rtol·|e|; element i of a vector or packed matrix passes when |aᵢ − eᵢ| ≤ atol + rtol·max(|eᵢ|, s), where s is the largest magnitude in the expected quantity (D-09). The values below were calibrated in Phase 0 against the Cox fixtures (`docs/parity/cox-calibration.md`): pprof_py and R agree within every class, and every negative control (the other tie method, a tied time shifted by a day, a dropped weight) misses by at least ten times its tolerance. `FixturesSuite` enforces both in CI.

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
| T-opt | Optimizer-limited estimates: the three-stage stage 2 SDs, intercept and BLUPs, σ's profile limits, and anything computed through them when both implementations run end to end (element-wise rule) | rtol 2e-4, atol 1e-6 (D-42) |
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
| T8 Connect | Engine suites through a Spark Connect client served in-process (ADR-0002) | CI | Every push and pull request |
| T9 Cluster integration | Multi-node cluster with separate executor JVMs, synthetic data | The maintainer's Databricks workspace, run by him once the whole package is done (D-28) | Deferred to then |
| T10 Scale and performance | Benchmark workloads in `bench`; regression thresholds once a cluster exists | GitHub runners by hand (`bench.yml`); a cluster later | By hand; before releases |
| T11 Statistical validation | Simulation studies of bias, coverage, and type I error for features without an external reference | CI or a cluster | When the feature is released |
| T12 Production-data validation | Approved CMS data inside the approved environment, compared with `pprof_py` or production outputs; only aggregate results leave the environment | The approved environment | Before production use |

### 9.2 Parity at three levels

Function-level parity compares objective, score, and information at the same fixed parameter values. It is the strongest test because no optimizer is involved. Lockstep parity starts both implementations from identical values and compares the first iterates, isolating update logic from stopping rules. End-to-end parity compares converged results and every reported output. A feature passes only when all three agree.

### 9.3 Reference fixtures

Fixtures are generated once from synthetic inputs and shared by every implementation (ADR-0005). Inputs are exact in text: integer event times, covariates on a 1/64 grid, offsets on a 1/16 grid and case weights on a 1/2 grid, stored as CSV. Outputs of the pinned pprof_py and of R are stored as JSON with every double as a hexadecimal floating-point string, which parses back bit for bit in Python, R and Java. `fixtures/manifest.json` records the generator, the reference versions, the options passed to each tool (including `ties`, `timefix`, and `robust = FALSE` for R) and a SHA-256 checksum of every file. Each case stores negative controls next to its outputs. Fixtures contain only synthetic data (NN-7); Git LFS is unnecessary while they stay small.

`reference/fixtures/generate.py` and `cox_survival.R` produce the fixtures, and `calibrate.py` checks the tolerances against them. `fixtures.yml`, run by hand on a re-pin (PAR-1), installs pprof_py at the pinned commit and R 4.3.3 with survival 3.5-8 from Ubuntu 24.04's archive, recomputes the outputs from the committed inputs, and compares them under T-part, because numpy's BLAS and numba compile for the host CPU. `FixturesSuite` checks the checksums, the pin, the exactness of the inputs and the calibration in every CI run.

The catalog (OI-32): Cox, six cases (estimation, left truncation and baselines, residuals and robust variance, provider measures and tests); logistic, six cases (fixed effects, covariate inference, provider tests, standardization) with Poisson-binomial tail references; three-stage, four cases (preparation and stages 1 to 3, inference and σ sensitivity) with Gauss–Hermite references. R tools: survival 3.5-8, sandwich 3.1-0, poibin 1.6 and lme4 1.1-35.1, pinned in `REFERENCE.lock`.

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

Every engine test runs with at least two physical partition counts, two block sizes, and one skewed provider-size distribution. A subset runs where executors are separate JVMs, on a multi-node cluster (deferred with T9), because local mode shares one JVM between driver and executors and so hides singleton state initialized only on the driver, classpath differences, and some serialization paths. Tests that target Classic or Connect assert which implementation the session actually is: an upstream bug report (SPARK-58223) shows the session builder's `classic()` selector creating a Connect session in Spark 4.1.x. Integration jobs print the loaded package version and git SHA and fail if they differ from the commit under test, which guards against stale libraries on clusters.

### 9.7 Cluster integration and scale tests

The maintainer runs these on his Databricks workspace once the whole package is done (D-28); assistants prepare the jobs, data generators and instructions and analyse the results he shares. There, integration jobs run on fresh clusters, generate synthetic data in place, and write machine-readable results for CI to collect. Scale tests sweep n, p, strata and provider counts, skew, tie density and the share of left-truncated rows, and record time per iteration, iterations, passes, shuffle bytes, bytes reduced to the driver and peak memory against regression thresholds.

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

The `bench` module holds reproducible workloads; `bench.yml` runs them by hand on GitHub's runners and shows the results on the run page. S-05's `DeterminismCost` is the first (ADR-0006). Runs on a fixed cluster with tracked regression thresholds wait for the package-level scale test on the maintainer's Databricks (D-28). A change that regresses a tracked metric beyond its threshold needs a written justification in its pull request.

---

## 11. Development workflow and tooling

### 11.1 Environments

The maintainer cannot install Scala or Spark locally, so CI is the build and test environment, and local-mode Spark inside CI runners is the backbone of testing. GitHub Actions runs every build and test and publishes failure details and test counts as annotations that are readable without signing in. A GitHub codespace (`.devcontainer/`, ADR-0007) adds interactive sbt, local Spark tests and `git apply` in the browser where policy allows. The AI assistant verifies each patch in its own sandbox before delivery: it compiles with the pinned Scala compiler and the build's flags, runs the suites on JDK 17 and 21 under a munit stand-in, and rehearses the patch on a fresh clone. That evidence is labeled as the sandbox's and does not replace CI. Protected data never enter any of these environments (NN-7).

### 11.2 Repository layout

```
pprof_spark/
  build.sbt, project/      sbt build, plugins, test logging configuration
  numerics/                pure Scala: summation, kernels, linear algebra
  engine/                  distributed engine: data contract, layout, backend, models, results
  ml/                      Spark ML adapters (Classic)
  app/                     Spark job entry points and run specifications
  testkit/                 test harness: sessions, tolerances, fixture reader
  bench/                   benchmark workloads (not published)
  reference/               REFERENCE.lock; fixture generators (Python, R); numerics references
  fixtures/                generated reference fixtures (synthetic only)
  scripts/                 source checks; CI failure reporting
  docs/                    PROJECT_CONTEXT.md, spec/, adr/, parity/, gates/, compatibility.md
  .devcontainer/           Codespaces definition
  .github/workflows/       ci.yml, fixtures.yml, bench.yml
  STATUS.md  OPEN_ITEMS.md  DECISIONS.md  DISCREPANCIES.md  HANDOFF.md  CLAUDE.md
```

### 11.3 Build configuration

The build uses sbt 1.12.15 (D-12); migration to sbt 2 waits until the plugins support it. The essentials of `build.sbt`, abbreviated:

```scala
val runtimeScalaVersion = "2.13.16"                        // PLAT-3
val sparkVersion = "4.1.0"
ThisBuild / scalaVersion := runtimeScalaVersion
ThisBuild / scalacOptions ++= Seq("-release", "17", "-encoding", "UTF-8", "-deprecation",
  "-feature", "-unchecked", "-Xlint", "-Wunused:imports,privates,locals", "-Werror")

// PLAT-4: `sbt -Dpprof.linkageCheck=true compile` forces the runtime's Scala modules.
ThisBuild / dependencyOverrides ++= (if (linkageCheck)
  Seq("scala-library", "scala-reflect", "scala-compiler").map("org.scala-lang" % _ % runtimeScalaVersion)
  else Nil)

lazy val sparkModuleSettings = Seq(
  allowUnsafeScalaLibUpgrade := true,                       // open-source Spark 4.1.x needs 2.13.17
  Test / fork := true,
  Test / parallelExecution := false,
  Test / dependencyClasspath ~= dropShimsAndPutConnectClientLast,   // OI-21, ADR-0002
  Test / javaOptions ++= sparkJavaModuleOptions ++ testProperties)  // Spark v4.1.0 JavaModuleOptions

lazy val numerics = project                                 // scala-library 2.13.16 only
lazy val engine   = project.dependsOn(numerics, testkit % "test->compile")   // spark-sql-api, provided
lazy val ml       = project.dependsOn(engine)               // spark-mllib, provided
lazy val app      = project.dependsOn(engine)
lazy val testkit  = project                                 // spark-sql, Connect server and client, munit
lazy val bench    = project.dependsOn(engine, testkit)
```

Tests fork a JVM with the `--add-opens` flags of the pinned Spark's `JavaModuleOptions`, a logging configuration that sends Spark's warnings to standard output, and system properties for the fixtures directory and the session mode (`pprof.test.sparkApi`). Suites run serially against a shared local session with the UI disabled, small shuffle-partition counts and ANSI mode set explicitly. Formatting uses scalafmt 3.11.5, and `sbt-buildinfo` embeds the version and git SHA (NN-11). scalafix, scoverage and MiMa (from 1.0) are not set up yet (OI-15).

### 11.4 CI workflows

| Workflow | Trigger | Content | Gate |
|---|---|---|---|
| `ci.yml` | Pull requests; pushes to main | Engine API rules with a self-test; scalafmt; the linkage compile (PLAT-3, PLAT-4); compilation with `-Werror` and tests on JDK 17 and 21 (Temurin, `sbt/setup-sbt`); the engine suites under Spark Connect (T8); failure details and per-module test counts published as annotations | Required |
| `ci.yml`, job `python` | Pull requests; pushes to main | The Python wrappers' tests in PySpark 4.1.0 (local Classic) against the JARs this commit builds, with absolute JAR paths; pytest's JUnit report feeds the counts and failure annotations | Required |
| `fixtures.yml` | By hand, on a re-pin | Pinned pprof_py and R stack; outputs recomputed from the committed inputs and compared under T-part; calibration | Review |
| `bench.yml` | By hand | Benchmark workloads on JDK 17 and 21; results on the run page | Informational |
| Release | Version tag | Build; publish to GitHub Releases (D-08); attach the validation report and compatibility entry | Not written yet |

Workflow logs need a signed-in user; annotations and job summaries do not, which lets the AI assistant read CI results from the public run page (OI-22). No secrets are stored; deployment credentials, such as OIDC federation and service principals, return with Databricks work.

### 11.5 Deployment

Databricks deployment (Declarative Automation Bundles, Unity Catalog volumes, OIDC federation, job clusters) is deferred by D-14 (ADR-0008); version 2.1 of this document describes the plan to start from when the maintainer reopens it. Releases go to GitHub Releases (D-08). Entry points obtain the session with `SparkSession.builder().getOrCreate()`; library code never stops the session or exits the JVM.

### 11.6 Working process

Work proceeds in rounds. Each round produces one self-contained change (a pull request or a `git apply`-ready diff, rehearsed on a fresh clone before delivery) and ends with updates to the state files of §0. Non-trivial work starts with a design sketch, and every behavior-changing proposal includes a numerical impact table. Decisions are numbered in `DECISIONS.md`, and architecture decisions also get an ADR. Statistical and design decisions, above all parity trade-offs and output-changing choices, require the maintainer's explicit approval; implementation and consistency work proceeds under delegated authority. The maintainer applies each round's patch to `main`, and the assistant reads the resulting CI run, through its public annotations, before building the next round.

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

Releases follow semantic versioning and stay at 0.x until the first parity-gated feature set; public APIs carry stability annotations, and MiMa enforces binary compatibility from 1.0. The compatibility matrix (§5.6) maps each release to its Spark line; if several Spark lines are ever supported at once, the Spark line becomes part of the artifact name. Every release ships a changelog and a validation report covering parity status per feature with tolerance evidence, metamorphic and edge-case results, a benchmark summary, and the tiers tested. Artifacts are published to GitHub Releases (D-08).

---

## 12. Data governance, privacy, security, and licensing

| Area | Rule |
|---|---|
| Protected data | PHI, PII, and CMS data stay in the approved environment. GitHub, CI runners, Codespaces, local machines, and AI assistants see only synthetic data. |
| Logs and errors | Messages identify problems by column names and counts, never by row values or patient identifiers; tests check this (T6). |
| Shared outputs | Provider-level outputs intended for sharing follow the applicable data use agreement's cell-size suppression rules; CMS policy prohibits displaying cell counts from 1 to 10. |
| Real-data validation | T12 runs only inside the approved environment; only aggregate comparison results leave it. |
| Credentials | No secrets in the repository or its workflows today. When deployment returns: OIDC federation and least-privilege service principals, never personal tokens. |
| Dependencies | Kept minimal; updated through Scala Steward or Dependabot; every new dependency gets a license review. |
| Licensing | pprof_spark is MIT-licensed, copyright Kevin He (D-07). `pprof_py` is MIT-licensed. R's `survival` (LGPL), `glmnet` and `lme4` (GPL), EmpiNull (GPL-3) and any other copyleft tool may be run to produce reference outputs, but their source is never ported into this repository; implement from the published mathematics and the MIT-licensed reference. |

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
| Example jobs | Runnable Spark examples on synthetic data |

Each model's documentation explains both the statistics and the distributed implementation, including its relationship to `pprof_py`, its reference implementation, its validation results, and its limitations.

---

## 15. Working agreement for AI-assisted development

AI assistants working on this project follow every rule above, plus the following.

| Rule | Detail |
|---|---|
| Evidence | Never state that code compiles, passes, matches, or is faster without the run that shows it (NN-8). |
| Platform APIs | Check Spark, sbt and other platform APIs against documentation for the pinned versions, and mark anything unverified as unverified. |
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

Phase 0's decisions and spikes are resolved. The outcomes are recorded in `DECISIONS.md` and `docs/adr/`, and the gate review in `docs/gates/phase-0.md`.

| ID | Decision | Outcome |
|---|---|---|
| D-01 | Supported compute tiers for v1 | Classic Spark; `engine` Connect-compatible and tested under Spark Connect |
| D-02 | Target and secondary runtimes | Spark 4.1.0, Scala 2.13.16, Java 17 bytecode: open-source Spark 4.1.x, and DBR 18 LTS by construction |
| D-03 | Python access in v1 | None; revisit after Phase 1d |
| D-04 | Design envelope and performance targets | §2.3 ranges as planning assumptions; targets after the first benchmarks; OI-02 before large-p work |
| D-05 | pprof_py pin | v0.7.0, commit `9320766` |
| D-06 | Default Cox tie method | Breslow |
| D-07 | Project license | MIT, copyright Kevin He |
| D-08 | Artifact distribution | GitHub Releases |
| D-09 | Tolerance calibration | The §8.4 values and rule, calibrated against the Cox fixtures and enforced in CI |
| D-10 | Root package and artifact names | `pprof.spark`; `pprof-spark-<module>_2.13` |
| D-11 to D-19 | Raised during Phase 0 | Phase 0 exit and first Cox slice; tooling baseline; Spark Connect test topology; no assistant access to Databricks (D-14, clarified 2026-10-07); summation; platform skeleton; fixtures; deterministic mode; Codespaces |
| D-20 to D-29 | Raised during Phases 1a to 1d | Cox specifications (D-20, D-21, D-23, D-25, D-27); phase gates closed at parity-verified (D-22, D-24, D-26, D-29); scale test on the maintainer's Databricks once the package is done (D-28) |
| D-30 to D-43 | Raised during Phase 2 | Phase 2 plan and slice specifications (D-30, D-34 to D-41, D-43); Python access through py4j (D-31, D-33, ADR-0009); slice 2a closed (D-32); the T-opt tolerance class (D-42) |

| ID | Question | Outcome |
|---|---|---|
| S-01 | Does the JAR run on DBR 18 LTS while CI tests open-source Spark? | Answered by the maintainer's package-level Databricks test (D-28); the linkage compile guards PLAT-3 meanwhile |
| S-02 | Does the Dataset backend run unchanged under Spark Connect? | Local part passed, bitwise identical to Classic (ADR-0002); Databricks legs deferred |
| S-03 | What does table materialization cost against persist? | Deferred to the package-level test (D-28) |
| S-04 | Can third-party Spark ML models be used through Spark Connect ML? | Deferred (D-01) |
| S-05 | What does deterministic mode cost? | Little; deterministic mode stays the default (ADR-0006) |
| S-06 | Is Codespaces usable? | Dev container provided; the maintainer's trial is pending (ADR-0007) |
| S-07 | Which block sizes work at envelope scale? | Deferred: needs a cluster (ADR-0008) |

## Appendix A. Changes and their rationale

### v2.3 to v2.4 (Phase 2)

| Area | v2.3 | v2.4 | Why |
|---|---|---|---|
| Python access | None in v1; revisit after Phase 1d | py4j wrappers in PySpark Classic, tested in CI | D-31, D-33, ADR-0009 |
| Application layer | `CoxJob` | `CoxJob`, `LogisticJob`, `ThreeStageJob`; run specifications carry `model` | Slices 2e and 2f-4 (D-37, D-43) |
| Tolerances (§8.4) | Ten classes | T-opt added for optimizer-limited estimates | X-030, D-42 |
| Reference limitations (§3.4) | Three-stage boundary to be specified | σ = 0 limit (X-005); exact-tail accuracy (X-024); optimizer-limited stage 2 (X-030) | Slices 2c and 2f |
| Fixtures (§9.3) | Cox | Cox, logistic and three-stage families; sandwich, poibin and lme4 pinned | OI-32 |
| CI (§11.4) | Scala jobs | The `python` job, with absolute JAR paths and pytest reports in the annotations | Round 57.1 |

### v2.2 to v2.3 (Phases 1a to 1d)

| Area | v2.2 | v2.3 | Why |
|---|---|---|---|
| Databricks | Not tested while D-14 stands | Tested by the maintainer himself; assistants never access his Databricks environment | D-14 clarified |
| Scale test | On a cluster to be chosen, per phase | Once, on the maintainer's Databricks, when the whole package is done; phase gates close at parity-verified | D-28, D-22, D-24, D-26, D-29 |
| Application layer | Planned | `CoxJob` and the version-1 run specification implemented | Phase 1d (D-27) |

### v2.0 to v2.2 (Phase 0)

| Area | v2.0 | v2.2 | Why |
|---|---|---|---|
| Platform | Pinned to DBR 18 LTS and verified on that runtime | Standalone Apache Spark 4.1 package; DBR 18 LTS compatible by construction but not tested | D-14 |
| Phase 0 exit | Walking Cox skeleton; a JAR on a DBR job cluster | Statistics-free platform skeleton, green under Classic Spark and Spark Connect | D-11: the Cox slice needs an approved specification (NN-2) |
| Scala skew (PLAT-4) | The newer library tolerated at test time | The newer library is on the compilation classpath too; a linkage compile forces 2.13.16 | sbt's documentation of the setting; review |
| Connect enforcement (§5.4) | The compile classpath enforces ARCH-2 | spark-connect-shims let incompatible calls compile; a source check enforces ARCH-2 | Review |
| T8 | Nightly, later on pull requests | In-process Spark Connect on every push and pull request | D-13, ADR-0002 |
| Fixtures (§9.3) | Parquet | CSV inputs on exact grids; JSON outputs as hexadecimal doubles | ADR-0005 |
| Tolerances (§8.4) | Initial values | An element-wise rule scaled by magnitude, calibrated and enforced in CI | D-09 |
| Licensing (§12) | survival and EmpiNull named | Every copyleft fixture tool; MIT for pprof_spark | Review; D-07 |
| Reference (§3) | 26 documented R-comparison failures | None at v0.7.0 for survival; two reference-vs-R differences registered | X-007 to X-010 |

### v1 to v2.0

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
