# Cox proportional hazards in pprof_spark: a tutorial

This tutorial explains how pprof_spark fits Cox proportional hazards models, from the statistics to
the Spark jobs. It covers the model and its partial likelihood, the estimation and inference
algorithms, how each formula maps onto the Scala source, and what Spark actually does when a model
is fitted. It is written for the people who maintain and review the package: statisticians who want
to see how the code realizes the mathematics, and engineers who want to know why the code is shaped
the way it is.

The tutorial describes the code at commit `efb6329` (round 75). Every implementation-specific claim
cites a file and a line range at that commit. The review and the probe runs were done at `9385836`
(round 69). Nothing the tutorial cites changed between the two: rounds 70 to 75 were Phase 3 work, no
Cox kernel, engine code, job runner, specification, fixture or test changed, and the shared
run-specification and Python entry points only gained the linear model. Round 77 then corrected the
text of one error message (`maxStratumRows`) without moving any line. Where the code and the project's own documents
disagree, the tutorial says so and follows the code; the disagreements are collected in
[review-findings.md](review-findings.md).

## Reading paths

| If you are | Read |
|---|---|
| A statistician new to survival analysis | Chapters 1, 2 and 3, then the worked examples in chapter 9 |
| A statistician reviewing the implementation | Chapters 2 and 3 quickly, then 4, 7 and the review findings |
| An engineer maintaining the engine | Chapters 4, 5, 6 and 8; chapter 3 for the optimizer |
| Debugging a wrong number or a failing test | Chapter 7's decision procedure, then chapter 9's troubleshooting table |
| Debugging a slow or failing Spark job | Chapters 5 and 8 |

| Chapter | File | What you learn |
|---|---|---|
| 1 | [01-survival-and-cox-fundamentals.md](01-survival-and-cox-fundamentals.md) | Survival data, censoring, truncation, hazards, the Cox model, strata, offsets, weights |
| 2 | [02-partial-likelihood-and-ties.md](02-partial-likelihood-and-ties.md) | Risk sets, the partial likelihood and why it ignores the baseline, score, information, Breslow and Efron ties |
| 3 | [03-estimation-and-inference.md](03-estimation-and-inference.md) | Newton–Raphson with pprof_spark's step control, failure modes, variance, tests, robust variance, baseline hazard, residuals |
| 4 | [04-from-mathematics-to-code.md](04-from-mathematics-to-code.md) | Each algorithmic component: problem, formula, inputs, steps, source, failure modes, validation |
| 5 | [05-distributed-execution.md](05-distributed-execution.md) | Logical blocks, the one shuffle, what each Spark job does, driver versus executors, determinism |
| 6 | [06-architecture-and-end-to-end-flow.md](06-architecture-and-end-to-end-flow.md) | Modules, types, the full trace of `CoxPH.fit`, design choices, where to look when something breaks |
| 7 | [07-validation-and-numerical-accuracy.md](07-validation-and-numerical-accuracy.md) | Fixtures, tolerances, the three parity levels, what is and is not demonstrated, telling a statistical discrepancy from a distributed defect |
| 8 | [08-performance-and-limitations.md](08-performance-and-limitations.md) | Cost model, sizing limits implied by the defaults, skew, fixed costs, limitations |
| 9 | [09-worked-examples-and-troubleshooting.md](09-worked-examples-and-troubleshooting.md) | One small data set followed through every calculation, in the kernel and in Spark; failure modes; troubleshooting |
| — | [review-findings.md](review-findings.md) | Discrepancies, weaknesses and gaps found while writing this tutorial |
| — | [probe/](probe/README.md) | The programs that produced the tutorial's numbers, to regenerate them when the code changes |

Further reading and a glossary are at the end of this page.

## Three kinds of statements

The tutorial separates three kinds of statements, because they carry different authority.

- **Theory** is established statistics: the Cox model, the partial likelihood, Breslow's and
  Efron's approximations, Newton's method, sandwich variance. Textbook references are at the end.
- **Implementation** describes what pprof_spark does at commit `efb6329`, with source references.
  When the code makes a choice that differs from the textbook or from R, the text says so and names
  the registered discrepancy (`X-…` in [DISCREPANCIES.md](../../../DISCREPANCIES.md)) or the
  decision (`D-…` in [DECISIONS.md](../../../DECISIONS.md)).
- **Review notes**, marked as such, record something this review found: a gap, a weakness, or a
  disagreement between the code and a document. Each one is listed in
  [review-findings.md](review-findings.md) with its evidence and a recommendation. A review note is a
  proposal, never a decision.

## Evidence used

| Source | What it establishes | How it is cited |
|---|---|---|
| Approved specifications, [docs/spec/cox/](../../spec/cox/) | What the code is meant to do | "spec first-slice §5" |
| Source code at `efb6329` | What the code does | File and line range |
| Reference fixtures, [fixtures/cox/](../../../fixtures/cox/) | pprof_py v0.7.0 and R 4.3.3 with survival 3.5-8 outputs, exact to the bit (hexadecimal doubles) | "R's stored value" |
| Test suites | What CI checks on every push | Suite and test name |
| CI runs | That the suites pass | Only as recorded in [STATUS.md](../../../STATUS.md) and [HANDOFF.md](../../../HANDOFF.md); this tutorial adds no CI evidence |
| The tutorial probe, [probe/](probe/README.md) | Numbers and Spark job traces quoted in chapters 3, 5 and 9 | "probe" |

The probe is sandbox evidence, not CI evidence (NN-8). It compiled `numerics` and `engine` with
the Scala 2.13.17 compiler shipped in the PySpark 4.1.0 source distribution, not the pinned 2.13.16;
without `-release 17`; and with a stand-in for the generated `BuildInfo` object. It ran on OpenJDK
21.0.10 against the Spark 4.1.0 jars in that distribution, in local mode (`local[4]`, one JVM). None
of these differences touches floating-point arithmetic, and the probe's kernel results agree with
R's stored values to rounding level, but the numbers are labelled as the probe's throughout.

## Feature status at `efb6329`

Every Cox feature is `Experimental` (NN-12) until the package-level scale test, which the
maintainer runs on his Databricks workspace once the whole package is done (D-28).

| Feature | Status | Where |
|---|---|---|
| Right-censored data, strata, Breslow ties, model-based variance | Implemented; parity-verified (D-22) | `CoxPH.fit`; spec [first-slice.md](../../spec/cox/first-slice.md) |
| Efron ties, case weights, offsets | Implemented; parity-verified (D-22) | spec [efron-weights-offsets.md](../../spec/cox/efron-weights-offsets.md) |
| (start, stop] data, left truncation; time-varying covariates by splitting rows | Implemented; parity-verified (D-24) | `CoxSpec.entry`; spec [counting-process-baseline.md](../../spec/cox/counting-process-baseline.md) §1 |
| Baseline cumulative hazard and survival per stratum | Implemented; parity-verified (D-24) | `CoxPH.baseline` |
| Prediction: linear predictor, relative hazard, cumulative hazard, survival | Implemented; parity-verified (D-24) | `CoxPrediction` |
| Martingale, score and dfbeta residuals | Implemented; parity-verified (D-26) | `CoxPH.residuals`; spec [residuals-robust.md](../../spec/cox/residuals-robust.md) |
| Robust (sandwich) variance, per row or clustered | Implemented; parity-verified (D-26) | `CoxOptions.robust`, `CoxSpec.cluster` |
| Two-stage SMR and SHR, indirect and direct standardization | Implemented; parity-verified (D-29) | `CoxMeasures`; spec [provider-workflows.md](../../spec/cox/provider-workflows.md) |
| Provider tests (exact Poisson, mid-p) | Implemented; parity-verified (D-29) | `CoxProviderTests` |
| Job runner and Python wrappers | Implemented; parity-verified | `pprof.spark.app.CoxJob`; `python/pprof_spark/cox.py` (Classic Spark only) |
| Recurrent events | Only through counting-process rows plus clustered robust variance, as the SHR workflow uses them; no recurrent-event model | — |
| Exact (discrete or averaged) partial likelihood for ties | Not implemented; a non-goal (§2.5) | — |
| R's `timefix` merging of near-equal times | Not implemented; times are compared exactly | spec first-slice §5 |
| Splitting one large stratum across blocks (TimeRange) | Not implemented (OI-03, OI-30) | — |
| Detection of infinite coefficients (monotone likelihood) | Not implemented; see [review-findings.md](review-findings.md) F-02 | — |
| Likelihood-ratio and score tests | Not reported; derivable from the reported log-likelihoods (chapter 3) | — |
| Proportional-hazards diagnostics (Schoenfeld residuals, `cox.zph`) | Not implemented | — |
| Penalized Cox, frailty, time-varying coefficients, competing risks | Not implemented; "Later" in §4 | — |
| Spark ML estimator (`CoxPHEstimator`) | Planned, not implemented: the `ml` module has no code (PROJECT_CONTEXT §3.6, §6.11) | — |
| An intercept | Rejected by design: not identifiable (X-001) | — |

## Notation

The tutorial follows the notation of the specifications.

| Symbol | Meaning |
|---|---|
| s | A stratum |
| i | A row (a subject, or one interval of a subject in counting-process data) |
| aᵢ, bᵢ | Entry and exit time; the row is at risk on (aᵢ, bᵢ]; aᵢ = 0 for right-censored data |
| δᵢ | Event indicator, 1 when the row ends with an event |
| wᵢ | Case weight, wᵢ ≥ 0 |
| oᵢ | Offset, a known term in the linear predictor |
| xᵢ | Covariate vector, length p |
| ηᵢ = xᵢᵀβ + oᵢ | Linear predictor |
| rᵢ = wᵢ·exp(ηᵢ) | Weighted relative risk ("risk score") |
| Rₛ(t) | Risk set: rows of stratum s with aᵢ < t ≤ bᵢ |
| Dₛ(t) | Tied events at t: rows of Rₛ(t) with bᵢ = t, δᵢ = 1 and wᵢ > 0 |
| d, d_w | The number of tied events and their total weight |
| S₀(t), S₁(t), S₂(t) | Σ rᵢ, Σ rᵢxᵢ and Σ rᵢxᵢxᵢᵀ over Rₛ(t) |
| D₀, D₁, D₂ | The same sums over Dₛ(t) (Efron only) |
| x̄(t) = S₁/S₀ | Risk-weighted mean covariate in the risk set |
| ℓ(β), U(β), I(β) | Log partial likelihood, score (gradient), information (negative Hessian) |
| n, m, p, K, B | Rows, strata or providers, covariates, distinct event times per stratum, logical blocks |

## Keeping this tutorial in sync

The tutorial is a snapshot of `efb6329`. When a change touches any of the files below, the chapters
that cite them need a look, and the probe should be rerun to refresh the quoted numbers:

```
numerics/src/main/scala/pprof/spark/numerics/{Newton,Cholesky,Summation,Normal}.scala
numerics/src/main/scala/pprof/spark/numerics/kernels/{CoxStratum,CoxResiduals,CoxMeasures}.scala
engine/src/main/scala/pprof/spark/engine/cox/*.scala
engine/src/main/scala/pprof/spark/engine/{backend,layout}/*.scala
```

`git diff efb6329 -- <those paths>` shows what moved. Line ranges cite `efb6329`; GitHub permalinks
of the form `https://github.com/UM-KevinHe/pprof_spark/blob/efb6329/<path>#L37-L210` keep pointing at
the cited lines after the code moves on. If a change is declared non-behavioral (NUM-2), the
probe's numbers must not change in a single bit.

## Glossary

| Term | Meaning |
|---|---|
| Aliasing | A covariate that is constant within strata or a linear combination of others, so the information matrix is singular; pprof_spark fails and names it (X-011) |
| AQE | Spark's adaptive query execution, which re-plans a query between stages; on by default in Spark 4 |
| Baseline hazard λ₀(t) | The hazard of a row with x = 0 and offset 0; left unspecified by the Cox model and estimated after the fit |
| Block (logical block) | One record of the working set holding whole strata as primitive arrays in canonical order; the unit of computation and of reduction order |
| Breslow ties | Approximation that gives every tied event the full risk set as its denominator |
| Canonical order | The deterministic row order inside a block: stratum, exit time descending, events first, then row identifier and values |
| Censoring (right) | The event time is only known to exceed the observed time |
| Cholesky factorization | I = LLᵀ with L lower triangular; used to solve Newton systems, invert I, and detect aliasing |
| Counting process | The view of each row as a process that jumps at its event, at risk on (a, b]; covers truncation and time-varying covariates |
| dfbeta | Approximate change in β̂ when a row is deleted: wᵢUᵢV |
| Driver | The JVM that runs the program's main thread: planning, Newton, small solves |
| Efron ties | Approximation that removes an average share of the tied events from the denominators |
| Executor | A JVM that runs tasks over partitions of distributed data |
| Fingerprint | An order-independent hash of the validated rows, recorded with a fit so that later calls can verify they received the same data (API-3) |
| Hazard ratio | exp(βⱼ): the multiplicative change in hazard per unit increase of covariate j |
| Information matrix | Negative Hessian of the log partial likelihood; its inverse is the model-based covariance |
| Job, stage, task | A Spark action launches a job; a job runs stages separated by shuffles; a stage runs one task per partition |
| Left truncation (delayed entry) | A row enters observation at a > 0 and contributes to risk sets only after a |
| Martingale residual | δᵢ − Êᵢ, observed minus expected events for a row |
| Monotone likelihood | Data in which ℓ keeps increasing as a coefficient goes to ±∞; the MLE is infinite |
| Neumaier summation | Compensated summation that tracks the rounding error of each addition; used for every running and reduced sum in the Cox code |
| Offset | A term added to η with its coefficient fixed at 1 |
| Partial likelihood | Product over event times of the probability that the observed row was the one to fail, given the risk set |
| Partition (physical) | A slice of a distributed data set processed by one task; it decides only where blocks run |
| Persist | Keep a computed data set in executor memory or disk so later jobs reuse it |
| R0, R1, R2, R3 | Reproducibility levels: bitwise per layout; within T-part across layouts; across platforms; across implementations (§8.2) |
| Risk set | The rows at risk at a time: entered before it and not yet exited |
| Score | Gradient of the log partial likelihood: observed minus expected covariates |
| Score residual | One row's contribution to the score, as a function of β̂ |
| Shuffle | Redistribution of rows across partitions by key; the expensive step of a Spark job |
| Strata | Groups with their own baseline hazards and shared coefficients |
| StratumLocal | The block layout used by every Cox fit: whole strata packed into blocks |
| StrictMath | Java's bit-reproducible transcendental functions, used for exp and log |
| Working set | The validated input laid out in blocks and persisted for the duration of a call |

## Further reading

Statistics, in suggested order:

- Kleinbaum DG, Klein M (2012). *Survival Analysis: A Self-Learning Text*, 3rd ed. Springer. Gentle
  introduction to censoring, hazards and the Cox model.
- Cox DR (1972). Regression models and life-tables. *JRSS B* 34:187–220. The model. Cox DR (1975).
  Partial likelihood. *Biometrika* 62:269–276. Why the partial likelihood works.
- Breslow N (1974). Covariance analysis of censored survival data. *Biometrics* 30:89–99. Tie
  handling and the baseline estimator as a profile likelihood.
- Efron B (1977). The efficiency of Cox's likelihood function for censored data. *JASA* 72:557–565.
- Kalbfleisch JD, Prentice RL (2002). *The Statistical Analysis of Failure Time Data*, 2nd ed.
  Wiley. The rigorous treatment, including exact tie methods.
- Therneau TM, Grambsch PM (2000). *Modeling Survival Data: Extending the Cox Model*. Springer. The
  computational side as implemented in R's `survival`: counting-process data, residuals, robust
  variance; the closest companion to this tutorial.
- Andersen PK, Gill RD (1982). Cox's regression model for counting processes. *Ann. Statist.*
  10:1100–1120. Lin DY, Wei LJ (1989). The robust inference for the Cox proportional hazards model.
  *JASA* 84:1074–1078.
- He K, Schaubel DE. *Tutorial for Standardized Measures of Survival Outcomes* (cited by pprof_py),
  and Wu W, Yang Y, Kang J, He K (2022), *Statistics in Medicine* 41:2840–2853, for the provider
  profiling workflows built on the Cox fit.

Numerics and Spark:

- Higham NJ (2002). *Accuracy and Stability of Numerical Algorithms*, 2nd ed. SIAM. Chapter 4 on
  summation (compensated sums), chapter 10 on Cholesky.
- Nocedal J, Wright SJ (2006). *Numerical Optimization*, 2nd ed. Springer. Newton's method,
  line searches and their convergence.
- The Apache Spark 4.1 documentation: the RDD programming guide (jobs, stages, shuffles,
  persistence) and the SQL performance tuning guide (adaptive query execution, shuffle partitions).
- Project documents: [PROJECT_CONTEXT.md](../../PROJECT_CONTEXT.md) §6 (architecture) and §7.3
  (Cox), [ADR-0003](../../adr/0003-deterministic-summation.md) (summation), and the Cox
  specifications in [docs/spec/cox/](../../spec/cox/).
