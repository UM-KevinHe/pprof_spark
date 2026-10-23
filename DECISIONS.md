# Decisions

Sequentially numbered and never renumbered (PROJECT_CONTEXT §0). Status values: **Proposed**
(awaiting the maintainer), **Approved** (by the maintainer), **Approved (delegated)** (the
maintainer asked for the assistant's recommendation to be applied), **Rejected**,
**Superseded**, and **Delegated** (implementation choices under delegated authority, §11.6,
reversible by the maintainer). Architecture decisions also get an ADR in docs/adr/. Recorded
decisions take precedence over PROJECT_CONTEXT.md (§0), whose preface lists the amendments in
force.

## Phase 0 decisions (§16)

| ID | Decision | Status | Outcome or recommendation |
|---|---|---|---|
| D-01 | Supported compute tiers for v1 | Approved (delegated), 2026-10-06 | Classic Spark only; `engine` kept Connect-compatible (PLAT-2) and tested under Spark Connect (T8) |
| D-02 | Target and secondary runtimes | Approved (delegated), 2026-10-06 | Spark 4.1.0, Scala 2.13.16, Java 17 bytecode: one JAR for open-source Spark 4.1.x and Databricks Runtime 18 LTS; tested on open-source Spark only (D-14) |
| D-03 | Python access in v1 | Superseded by D-31, 2026-10-07 | Was: none in v1; revisit after Phase 1d. Round 25 first recorded the maintainer's "Continue" as no change; he then chose py4j wrappers (D-31) |
| D-04 | Design envelope and performance targets | Approved (delegated), 2026-10-06 | §2.3 ranges as planning assumptions; targets set after the first benchmarks; OI-02 resolved before large-p work |
| D-05 | pprof_py pin and reference versions | Approved (delegated), 2026-10-05 | pprof_py v0.7.0, commit 9320766 (see below) |
| D-06 | Default Cox tie method | Approved (delegated), 2026-10-06 | Breslow, the reference default; every fit records its tie method |
| D-07 | Project license | Approved, 2026-10-05 | MIT, copyright holder Kevin He |
| D-08 | Artifact distribution | Approved (delegated), 2026-10-06 | GitHub Releases |
| D-09 | Tolerance calibration | Approved (delegated), 2026-10-06 | §8.4 values, an element-wise rule scaled by each quantity's largest element, calibrated against the Cox fixtures and enforced in CI (ADR-0005) |
| D-10 | Root package and artifact names | Approved, 2026-10-05 | Organization and root package `pprof.spark`; artifacts `pprof-spark-<module>_2.13` |

### D-05: what the pin is, and the choice

pprof_spark is a standalone Spark package: it does not depend on pprof_py at build time or at
run time. pprof_py matters only to the test suite. Parity tests (§9.2) compare pprof_spark's
results with pprof_py's results on the same synthetic data, and the pin fixes the exact pprof_py
version that produces those reference results. Test expectations therefore change only through
a deliberate re-pin (PAR-1), never because pprof_py moved on.

Chosen: pprof_py v0.7.0, its current release (tag object 9849f6c, commit
9320766e35e5b385d596a2b75418192098a124c4, 2026-10-03), recorded in reference/REFERENCE.lock. It
is a tagged release; its Cox model code is identical to v0.5.0; and it already contains the
provider-test and standardization changes that Phase 1d needs. The fixture pipeline establishes
which parts of its validation suite pass (OI-12).

## Decisions raised in rounds 1 to 3

### D-11: Phase 0 exit criterion and the first Cox slice (Approved, delegated, 2026-10-05)

(a) Phase 0 exits on a statistics-free platform skeleton: data contract, logical blocks, a toy
kernel, block-ordered reduction, a result table and persisted metadata, green in CI under
Classic Spark and under Spark Connect. The Databricks leg of the original proposal is dropped
(D-14). (b) The first Cox slice opens Phase 1a: stratified (StratumLocal), right-censored,
Breslow, with model-based variance; unstratified data is the one-stratum case while it fits in
one block, and TimeRange follows within Phase 1a or 1b. Stratified first exercises multi-block
reduction and bin packing, matches SMR stage 1, and avoids starting with the hardest layout
(OI-03).

### D-13: Spark Connect topology for test layer T8 (Approved, delegated, 2026-10-05; ADR-0002)

The T8 suites run through the Scala Spark Connect client against a Connect server started inside
the test JVM on the local Classic session, with the Connect client jar pinned to the end of Test
classpaths. Alternatives considered: a separate server JVM, and Databricks only. Evidence and
limits are in ADR-0002.

### D-14: No Databricks deployment work (Approved by the maintainer, 2026-10-05)

**Clarified by the maintainer on 2026-10-06:** "no Databricks work" means that AI assistants (Claude
Code included) must not access the maintainer's Databricks environment. It does not rule out running
pprof_spark on Databricks: the maintainer tests it there himself, starting with the Phase 1d scale
test (D-28). pprof_spark stays a standalone Apache Spark package, compatible with Databricks Runtime
18 LTS by construction (§5.3).

pprof_spark is developed and tested as a standalone Apache Spark package. No Databricks bundles,
workspace CI, job clusters or Unity Catalog artifacts until the maintainer reopens the topic.

- Phase 0 no longer requires a CI-deployed JAR running on a DBR job cluster (§4).
- Deferred: S-01 (runtime linkage on DBR), the Databricks legs of S-02, S-03 (table
  materialization on serverless), S-07 (envelope-scale block sizes on a cluster), test layers
  T9 and T10, the job-based Python access of §6.12, and the Databricks parts of D-01 to D-03
  and D-08. The `app` module targets plain Spark entry points.
- Kept: the build pins (Spark 4.1.0, Scala 2.13.16, Java 17 bytecode) and the linkage compile,
  which keep one JAR usable on open-source Spark 4.1.x and on DBR 18 LTS at no extra cost; and
  Spark Connect compatibility (PLAT-2), which any Spark Connect deployment needs.

### D-15: Deterministic summation algorithms (Delegated, 2026-10-05; ADR-0003)

Pairwise (cascade) summation with sequential leaves of at most 32 values within a block, and
Neumaier compensation for partials combined in block order, as specified in ADR-0003. These
algorithms fix the bits of every reduction; changing them is a behavioral change (NUM-2).

### D-16: Platform skeleton design (Delegated, 2026-10-05; ADR-0004)

The data contract, layout plan, working set, kernels, ordered reduction, result tables and
persistence format of ADR-0004, which every model family reuses.

### D-17: Reference fixtures and calibration (Approved, delegated, 2026-10-06; ADR-0005)

Exact synthetic inputs generated once (CSV), reference outputs from pprof_py v0.7.0 and R 4.3.3
with survival 3.5-8 as hexadecimal doubles (JSON), a checksummed manifest, stored negative
controls, and calibration enforced in CI by `FixturesSuite`.

### D-18: Deterministic mode stays the default (Approved, delegated, 2026-10-06; ADR-0006)

Spike S-05 measured the cost of `StrictMath` and ordered sums; in a Cox-shaped kernel, sums built
with a fused pairwise cascade make determinism nearly free. There is no fast mode for now.

### D-19: Codespaces development container (Approved, delegated, 2026-10-06; ADR-0007)

`.devcontainer/` provides JDK 21, the pinned sbt and Metals; CI remains the source of evidence.

### D-59: Slice 4b-1, tighter agreement checks and the fixtures' fixed points (Accepted: approved by the maintainer, 2026-10-23)

The end-to-end tolerance for the fitted estimates, T-opt, allows about a hundred times more than the agreement
observed in round 111 (the maintainer's question, 2026-10-22). Proposed for slice 4b-1, in round 112:
1. An optimality check. pprof_spark's Laplace deviance at its optimum must not exceed the deviance at pprof_py's or
   lme4's tightened optimum by more than 10⁻¹⁰ relative, both evaluated by pprof_spark's objective. That objective
   matches pprof_py's to about 10⁻¹⁵ and lme4's to about 10⁻⁹ at fixed parameters. For nAGQ = 0, the criterion must
   not exceed pprof_py's at its optimum by more than 10⁻¹⁰ relative.
2. A class T-est (rtol 10⁻⁶, atol 10⁻⁹) for pprof_spark's estimates against pprof_py's tightened optimum. T-opt stays
   where σ̂ < 0.1, where the likelihood is flat in σ, and for every comparison with lme4.
3. The fixtures' fixed points move σ and at most one centred coefficient, by at most two standard errors. On
   lgre-shifted the deviance at fixed parameters then agrees with lme4 within 0.006 of T-coef, where it differed by
   3.3·10⁸ of T-coef when every coefficient moved at once, so that comparison becomes strict.
4. The fixture sweep runs the kernel on the driver: every case, both nAGQ, in seconds. The engine's distributed fit is
   checked against the kernel at T-part on three cases, with its fitted values, predictions and residuals against
   the references.
5. The fractional-outcome findings go into spec 4b-1 §12, and OI-98 and OI-99 are opened.

Approved as proposed on 2026-10-23.

### D-58: Slice 4b-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-10-22)

`fixtures/logistic-re` (round 110; `reference/fixtures/logistic_re_fixtures.py` and `logistic_re_r.R`): the ten cases
of spec 4b-1 §10 with nAGQ = 1 and nAGQ = 0, pprof_py and `glmer` at their defaults and with their tolerances
tightened. The fixtures also hold the Laplace deviance at four fixed points within two standard errors of the optimum
(with pprof_py's central differences there), the nAGQ = 0 criterion at three values of σ, and negative controls.

`docs/parity/logistic-re-calibration.md` passes every strict check: the tightened optima agree within 0.017 of T-opt,
the deviances at fixed parameters within 0.32 of T-coef, and the weakest control misses by 12.7. Proposed:
1. T-opt (D-42) for 4b-1's estimates, fitted values, predictions and residuals; T-coef for the deviance and the
   nAGQ = 0 criterion at fixed parameters, since both references stop their inner iterations at a relative
   tolerance.
2. lgre-fractional compared with pprof_py only: `glmer` fits another model to fractional outcomes (σ̂ = 0, β̂ about
   twice pprof_py's).
3. lgre-shifted compared by invariance against lgre-base, as D-53 did for lre-shifted.
4. Negative controls at the provider level: the largest provider's outcomes flipped, and the first row of every
   provider flipped. One row moves an optimizer-limited fit too little for T-opt.
5. X-044 (class B): pprof_py's predictions with the BLUPs ignore them under integer keys; pprof_spark adds them, as
   lme4 and pprof_py with text keys do.

Approved as proposed, with X-044, on 2026-10-22.

### D-57: Slice 4b-1 specification (Accepted: approved by the maintainer, 2026-10-22)

`docs/spec/logistic/random-effect-estimation.md`: estimation for the logistic random-intercept model (one provider
intercept, covariates, an offset; fractional outcomes as pprof_py). The Laplace (nAGQ = 1) optimum over (σ, β) by
`BoundedQuasiNewton` with the analytic gradient of §2.1, each evaluation one pass in which every provider's mode is
found by Newton's method on its own rows from 0. `stage2 = false` gives the nAGQ = 0 optimum: Newton over (β, u)
through the Schur complement at each σ, and Brent's minimization over σ, which also starts nAGQ = 1. The estimates
follow pprof_py's definitions, Var(β̂) as the information conditional on the modes. The section also covers the data
contract (X-038's and X-040's rules), the distributed plan, edge cases, the Python mapping and the validation plan with
`fixtures/logistic-re` at T-opt. Proposed with it: X-043 (the covariance's definition against `glmer`'s default
`vcov`).

Approved as proposed, with X-043, on 2026-10-22.

### D-56: Slice 4a-3 specification (Accepted: approved by the maintainer, 2026-10-22)

`docs/spec/linear/random-effect-job.md`: persistence, the job runner and Python save and load for the linear
random-intercept model, with no new statistics. `LinearREFitIO` format version 1 (a `metadata` line with every double
as its bit pattern, and the provider table in Parquet), saved and loaded bit for bit. `pprof.spark.app.LinearREJob`
runs a `linear-re` specification (`columns` with offset and weights; `fit` with `reml`, `includeIntercept`,
`thetaUpper`, `maxEvaluations` and `blocks`; every output of slices 4a-1 and 4a-2), and the five jobs refuse each
other's specifications by name. Python gains `save` and `load`. The facade's `linearREModel` moves to
`LinearRERunSpec`'s parser.

Approved as proposed on 2026-10-22.

### D-55: Slice 4a-2 fixtures, calibration and corrections (Accepted: approved by the maintainer, 2026-10-21)

`fixtures/linear-re` gains `inference_pprof_py.json` and inference sections in `r_lme4.json` (round 102): every case
under REML and lre-base and lre-weights under ML, at slice 4a-1's tight optima, with pprof_py's methods of spec 4a-2
§10, base R's tails and quantiles on lme4's tight fit, R pprof's tests on its own lmer fit and the negative
controls' z. `docs/parity/linear-re-inference-calibration.md`: every strict check passes, worst 0.0016 of T-coef,
6.2·10⁻⁴ of T-test, 0.0084 of T-p and 1.3·10⁻⁷ of T-meas, no flag differs, and the controls miss by at least
5.7·10⁵. Proposed:
1. the classes of spec 4a-2 §10 for its parity tests, with pprof_py's `summary` p-values compared within their
   rounding, 5·10⁻⁸ absolute (X-041), instead of above 10⁻⁶, where rounding alone exceeds T-p;
2. at a boundary fit pprof_spark follows pprof_py, which leaves every provider untested whatever γ₀ (z, p-value,
   flag and the test's limits NaN) while option `alpha` gives limits of 0; this amends spec 4a-2 §8;
3. X-042 corrected: R pprof labels its BLUPs in text order over values in numeric key order, so with integer keys
   `test.linear_re`, `confint.linear_re` (both options) and the direct differences pair one provider's BLUP with
   another's size or label, while its indirect differences are right; R pprof is a reference, at T-opt on its own
   default fit, for text keys only (lre-text: within 1.8·10⁻⁴ of T-opt);
4. `compare_outputs.py` compares the linear-re family's default runs at T-opt and the rest at 10⁻⁸ relative: two
   sandbox sessions regenerated the family with differences up to 1.1·10⁻⁷ in the default runs and 1.3·10⁻¹⁰ in the
   tight optima, beyond T-part (OI-96).

Approved as proposed on 2026-10-21.

### D-54: Slice 4a-2 specification (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/linear/random-effect-inference.md`: inference and standardized measures for the linear random-intercept
model, as pprof_py defines them. Covariates: `summary` (t with n − p − m degrees of freedom), `fixed_effects_table`
and `conf_int` (n − p). Provider tests: z = (b̂ⱼ − γ₀)/seⱼ with the conditional SD of X-037, the theoretical null and
slice 2c's table, γ₀ = 0 by default. Intervals: option `alpha`, and option `SM` with the indirect and direct
differences, whose observed values are sums of fitted values. The measures use the training data (API-3), and the
fixtures extend `fixtures/linear-re`. Proposed with it: X-041 (class B: unrounded upper-tail p-values in `summary`) and
X-042 (R pprof's `linear_re` inference: a reference for `test` and option `alpha` on unweighted fits only).

Approved as proposed, with X-041 and X-042, on 2026-10-21.

### D-53: Slice 4a-1 fixtures and tolerance classes (Accepted: approved by the maintainer, 2026-10-21)

`fixtures/linear-re` (round 96; `reference/fixtures/linear_re_fixtures.py` and `linear_re_r.R`): the nine cases of
spec 4a-1 §11 under REML and ML, with pprof_py and lme4 at their defaults and at their tight optima (the root in τ
of central differences of each reference's own deviance), the deviance at fixed θ, NumPy's summaries and negative
controls. `docs/parity/linear-re-calibration.md`: on the eight well-conditioned cases the two tight optima agree
within 7.4·10⁻⁴ of T-coef (β̂, BLUPs, fitted values, predictions), 3.7·10⁻⁴ of T-var (θ̂, σ̂, σ̂_α, Var(β̂), PWRSS,
log det H, ‖u‖²) and 1.3·10⁻³ of T-fn (the deviance, ℓ, AIC and BIC, and the deviance at fixed θ); every negative
control misses by at least 83 times its class. Proposed: these classes for 4a-1's parity tests, the conditional
variances against lme4's at T-var, and the default runs informational under T-opt. For lre-shifted, where both
references lose up to 4.2·10⁻⁷ in τ* and 6.2·10⁻⁸ in β̂ to conditioning, invariance against lre-base replaces the
references' agreement, and pprof_spark centres the features of the between part (spec 4a-1 §13 and §14).

Approved as proposed on 2026-10-21.

### D-52: Slice 4a-1 specification (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/linear/random-effect-estimation.md`: estimation for the linear random-intercept model (one provider
intercept, offsets, prior weights; REML by default, ML on request). lme4's profiled deviance and its derivative on
provider summaries and pooled within co-moments (§2); the root of the derivative in τ = θ² by Brent's method,
bracketed by a grid, with boundary fits at 0 and θ_upper (§3); the estimates, BLUPs and conditional variances
(X-037); the data contract; the distributed plan (two passes, then the search on the driver or by aggregations over
the provider table); outputs; edge cases from round 95's probes; the Python wrapper's estimation methods; and the
validation plan with `fixtures/linear-re`. Proposed with it: X-038 (class C: incomplete rows fail with counts),
X-039 (class C: aliased features fail and are named) and X-040 (class C: designs whose provider variance is not
identified, and n ≤ p, fail at validation).

Approved as proposed, with X-038 to X-040, on 2026-10-21.

### D-51: Phase 4 plan, the random-effect models (Accepted: approved by the maintainer, 2026-10-21)

`docs/spec/random-effects/plan.md` reads pprof_py v0.7.0's `LinearRandomEffectModel`, `LogisticRandomEffectModel`
and `LogisticFERandomClusterModel` with their inference and measures modules, lme4 1.1-35.1 and R pprof 1.0.3's
random-effect functions, maps them onto pprof_spark's code, and proposes (its §8):
1. parts 4a (linear), 4b (logistic) and 4c (fixed effects with random clusters) in the maintainer's order, then 4d
   (cluster terms, OI-93), each closing at parity-verified with its own gate and an S1 smoke;
2. 4a: one provider random intercept with offsets and prior weights;
3. 4b: one provider random intercept with covariates and offsets; the Laplace fit by default and nAGQ = 0 as an
   option; tests `wald`, `poibin_exact` and `resampling`; `profile_sigma`;
4. 4c: stage 3 as a model of its own on the three-stage code; `he2013` as slice 4c-2 (OI-94);
5. the theoretical null only, until P0 adds null models, the empirical null and funnel limits to every model;
6. references: pprof_py at default and tight settings, lme4 with tightened control, R pprof's `linear_re` and
   `logis_re` families for inference where they follow pprof_py;
7. estimation to the references' optima rather than their optimizer paths (class D, as X-030);
8. tolerance classes: the ordinary classes for the linear estimates against tight references, T-opt for the
   logistic ones, the lockstep classes for inference;
9. REML by default for the linear model, ML as an option;
10. X-037 (class B): the linear model's BLUP standard errors under prior weights, corrected to the fitted model's
    conditional variance, as lme4's.

Approved as proposed, with X-037, on 2026-10-21.

### D-50: The roadmap after Phase 3, and scale testing in phases (Accepted: approved by the maintainer, 2026-10-19; amended at his request, 2026-10-21)

A comparison of the two code bases found two things. pprof_py v0.7.0 is at `9320766`, also its `main`;
pprof_spark was at round 90.
- pprof_spark implements 4 of pprof_py's 29 public model classes: `CoxPH`, `LogisticFixedEffectModel`,
  `LogisticThreeStageModel` and `LinearFixedEffectModel`. Each is parity-verified, with inference gaps.
- Nothing has run at scale.

Approved:

1. **Priorities.** Each model starts with a specification (NN-2) and a fixture-based parity gate.
   - P0, essential:
     - Inference parity on the four models: `null_model` on every provider test (OI-87), the empirical null
       (OI-78), funnel-plot limits (OI-88), the logistic fixed-effect model's confidence intervals (OI-89),
       external standard populations (OI-58), and the provider tests' `interval` option and VIF screening
       (OI-90).
     - Fixes to the shared execution paths that S0 finds.
   - P1, essential: random-effect models: linear, then logistic, then fixed effects with random clusters.
   - P2: survival extensions: cause-specific Cox, time-varying Cox, Cox with provider effects at λ = 0, shared
     frailty, Fine–Gray once X-003's parity target is chosen, and discrete-time survival.
   - P3:
     - penalized and group-lasso models with cross-validation, provider-penalized variants and `CoxPHSelector`;
     - inter-unit reliability, after X-006;
     - results exported in pprof_py's shapes for its presentation layer;
     - the `Ban` algorithm;
     - method-name aliases.
2. **Scale testing in four phases** (`docs/spec/scale/plan.md` §9):
   - S0, architecture checks, now.
   - S1, a scale smoke with each new model or feature.
   - S2, the plan's Stage A, after P0 and P1.
   - S3, its Stage B.

   Before S2, fix only problems of complexity class and costs in shared infrastructure. Complexity-class
   problems are time growing faster than n, driver memory growing with n or m, a stage limited to one task,
   and data expansion. Kernel tuning (OI-38) and absolute targets wait for S2.
3. **Milestones:**
   - M1: data generators and measurement capture, S0 on the four models, and specifications for the empirical
     null and funnel limits.
   - M2: P0's inference parity.
   - M3: fixes for M1's complexity problems, alongside M2.
   - M4: P1.
   - M5: S2 and S3 on the essential set, and a first release with a wheel (OI-55).
   - M6: P2, then P3.
4. **Scope.** pprof_spark stays a standalone Spark package. The scale kit runs on any Spark 4.1 cluster, and
   deployment work is outside this roadmap (D-14).

The scale-test plan's other decisions (its §12) remain open and apply to S2 and S3; its approval takes the next
free decision number. PROJECT_CONTEXT §4's Later row now follows this order. PROJECT_CONTEXT itself is updated
when the next phase's plan is written.

Amendment (round 94, 2026-10-21, at the maintainer's request): M4 comes before M2. The random-effect models
(P1: linear, then logistic, then fixed effects with random clusters) are developed now, while the maintainer runs
his own scale tests; M1's remaining items (`ScaleJob`'s measurements, the three-stage generator, S0's plan file and
its dry run) continue alongside, and M2 follows M4. The new models' provider tests take the theoretical null, as
the existing models' do; P0 then adds null models, the empirical null and funnel limits to every model (OI-87,
OI-78, OI-88). Phase 4's plan: D-51.

### D-49: Fixes for the reported issues (Accepted: approved by the maintainer, 2026-10-12)

`docs/reviews/issues-2026-10.md` diagnoses ten issues the maintainer reported against round 81
(OI-73 to OI-81). Approved: its recommendations and their order (§3): L5 and C3, C1,
L2 to L4 and L6, L1, then C2's composite keys. New options keep today's defaults, except C3 (a stratum byte
budget, `maxStratumBytes` = 4 GiB, replaces the 1,000,000-row default of `maxStratumRows`, which becomes
unlimited) and L5 (blocks grow, doubling up to 256 MiB, where the block partials would exceed the driver budget;
plans that fit are unchanged). Fixes that change statistical behaviour come with specification addenda. The
scale-test plan, drafted in round 80 to become D-49, awaits its own decision.

Correction (round 85): L6's recommendation rested on a misreading of pprof_py, which refuses robust variance for
indirect measures too; pprof_spark keeps refusing it, and that recommendation is withdrawn.

### D-48: Close Phase 3 at parity-verified (Accepted: approved by the maintainer, 2026-10-11)

`docs/gates/phase-3.md`: slices 3a to 3c meet the parity gate in the sandbox (round 74: every linear engine suite
passes under Classic Spark on JDK 17, and none of the 3,544 tolerance ratios they compute exceeds 1; worst T-meas 0.887
on lin-shifted, inside the references' own spread of 0.504 there; T-fn 0.0496; T-test 7.3e-4); every discrepancy
affecting Phase 3 has an approved decision. Proposed: close Phase 3 at parity-verified once the maintainer reports CI
green for `main` at this round (rounds 61 to 73 have no CI report), on the terms of D-32 and D-44, accepting that the
within cross-products are verified against exact arithmetic (`LinearKernelSuite`) and end to end rather than against
the fixtures' NumPy values (spec 3a §11); every Phase 3 feature stays `Experimental` until the package-level scale
test (D-28). PROJECT_CONTEXT v2.6 then records Phase 3. Approved on 2026-10-11, with the substitute for 3a's
function-level check, the maintainer reporting CI green for every run since round 57.1.

### D-47: Slice 3c specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/job-and-python.md` specifies the linear job runner and the Python access, following the logistic
job runner (D-37) and the py4j wrappers (ADR-0009): a run specification with `model` `linear` (new `LinearRunSpec`,
entry point `LinearJob`; the four jobs refuse each other's specifications), one output directory per section with the
run record written last, and `pprof_spark.LinearFixedEffect` and `LinearFixedEffectModel` mapping pprof_py's methods,
with the data argument required exactly where the library needs the training data (API-3). No new statistics.
Approved on 2026-10-09.

### D-46: Slice 3b specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/provider-inference.md` specifies pprof_py's linear provider tests (t statistics mapped to z by
equal tail probability; slice 2c's flags, `critical` and columns; limits γ̂ ∓ q·se), intervals (`gamma`, and `SM` for
the indirect and direct differences) and standardized differences. The reference effect is computed on the driver
from the provider table (median, size-weighted mean or a number), and the measures from a pass over the training data
(API-3). Proposed with it: X-035 (class B: a provider whose t tail underflows keeps its test, through a log-scale z)
and X-036 (R pprof 1.0.3's linear inference serves as a reference only where it follows pprof_py's definitions).
R pprof's linear functions are pure R (OI-60 closed). Approved on 2026-10-09 with X-035 and X-036.

### D-45: Phase 3 plan and the slice 3a specification (Accepted: approved by the maintainer, 2026-10-09)

`docs/spec/linear/plan.md` reads pprof_py v0.7.0's `LinearFixedEffectModel` and proposes slices 3a
(estimation and covariate inference), 3b (provider tests, intervals, standardized differences) and 3c (job
runner and Python), the distributed design (PROJECT_CONTEXT §7.5's pass over the within co-moments, then a
pass over the residuals; TSQR deferred, OI-61) and the tolerance classes.
`docs/spec/linear/fixed-effect-estimation.md` specifies slice 3a. Proposed with them: X-031 (class B:
covariate p-values from the upper tail), X-032 and X-033 (class C: aliased features and n − m − p ≤ 0 fail).
Out of scope: funnel limits, plots and empirical nulls (§4 "Later"). Approved on 2026-10-09 with X-031 to X-033.

### D-44: Close Phase 2 at parity-verified (Accepted: approved by the maintainer, 2026-10-09)

`docs/gates/phase-2.md`: slices 2a to 2f meet the parity gate. In round 60's sandbox every Phase 2 engine
suite passed (15 suites, 76 tests, Classic Spark, JDK 17) and none of the 10,042 tolerance ratios they
compute exceeds 1 (worst: T-coef 0.683, T-opt 0.331, T-meas 0.169, T-fn 0.142); every discrepancy affecting
Phase 2 has an approved decision; CI was green through round 57.1. Proposed: close Phase 2 at
parity-verified once the maintainer reports CI green for round 59, on the terms of D-32; every Phase 2
feature stays `Experimental` until the package-level scale test (D-28). PROJECT_CONTEXT §7.4's three-stage
description (OI-48) goes into the next revision. Approved on 2026-10-09, after the maintainer reported CI
green; PROJECT_CONTEXT v2.5 (round 61) folds in OI-48.

### D-43: Slice 2f-4 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-pipeline.md`: the three-stage pipeline (`ThreeStage.fit`); stage 3's provider
tests (`exact` by clustered Gauss–Hermite mixtures of Poisson-binomial counts, `poibin_exact`, `resampling`),
standardized measures (direct sums per cluster from offset-bin moments) and intervals; `sigma_sensitivity`
(σ_c's profile interval from the exact Laplace deviance, stage 3 refits at both ends with the σ = 0 limit);
persistence, the job runner (`model` `three-stage`) and Python; the distributed plan and validation (T-opt
for the profile limits). No new discrepancies: X-005, X-024, X-025, X-026 and X-030 apply as approved.
Approved as written; 2f-4a fixtures in round 54.

### D-42: The T-opt tolerance class (Accepted: approved by the maintainer, 2026-10-08)

For optimizer-limited estimates (three-stage stage 2's variance components, intercept and BLUPs; X-030):
rtol 2e-4, atol 1e-6 under the element-wise rule of D-09, added to `tolerances.conf`. Justification and
negative controls (NN-9): `docs/parity/three-stage-calibration.md` (round 51): pprof_py's tight run against
`glmer` at most 0.48 of it on integral outcomes, pprof_py's default run against its tight run at most
1.7e-4 of it, the controls at least 23.9 times outside. Approved as proposed.

### D-41: Slice 2f-3 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-stage2.md`: stage 2, the crossed random-intercept logistic GLMM with the
stage 1 offset (lme4's nAGQ = 1 Laplace deviance), on 2f-2's compressed cells: PIRLS converged from zero at
every evaluation, the joint system eliminated onto the smaller set of levels, and a bounded quasi-Newton
on (σₚ, σ_c, μ) to a gradient tolerance instead of pprof_py's Nelder–Mead (X-030, with a new
optimizer-limited tolerance class T-opt to be calibrated and approved under NN-9); outputs, the driver
plan (OI-59 for very large level sets) and validation. Approved with X-030 and OI-59 as proposed.

### D-40: Slice 2f-2 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-stage3.md`: stage 3 with β and σ fixed, as pprof_py's `marginal`
estimator (adaptive Gauss–Hermite quadrature at each cluster's posterior mode, projected Newton with a
sparse Hessian, Armijo line search, bounds), computed from per-cell offset-bin moments; the σ = 0 limit
(X-005); conjugate-gradient Newton steps and Newton-computed Gauss–Hermite rules (class D); outputs,
the distributed plan (compression, then the driver or cluster-local executors) and validation. Decision
to approve with it: X-029 (compressed cells for stage 3; class C). Approved as written; fixtures in
round 47.

### D-39: Slice 2f-1 specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-preparation.md`: pprof_py's `glmm_data_prep` (screening at more than
`cutoff` records, `y_adj`, provider × cluster cells, `included`) and stage 1 (slice 2a on the included
cells without screening), the offset xβ̂, the outputs, the distributed plan and validation. Decision to
approve with it: X-028 (no dense provider × cluster table; class C). Approved as written; fixtures in
round 44.

### D-38: Slice 2f direction after the three-stage spike (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/three-stage-spike.md` (OI-51): pprof_py's three-stage model is He et al. (2013)'s
(providers crossed with clusters, random cluster effects), as R's `glmm.fac.hosp`. Proposed: compress
the records once into per-cell offset-bin moments so that stages 2 and 3 iterate on cells, not
records; sub-slices 2f-1 (preparation and stage 1), 2f-2 (stage 3, `marginal`, given β and σ), 2f-3
(stage 2, the crossed random-intercept GLMM) and 2f-4 (pipeline, `sigma_sensitivity`, inference, job,
Python); X-005 (the σ = 0 limit, class B); fixtures without nlopt and an optimizer-limited tolerance
class for stage 2, to be calibrated. Approved as written.

### D-37: Slice 2e specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/job-runner.md`: a logistic job runner. The run specification (version 1) gains a
top-level `model` (`cox` by default, or `logistic`); `pprof.spark.app.LogisticJob` (`--spec` or
`--spec-json`) fits the model and writes the fit, the provider table, covariate tests, provider tests,
standardized measures, tests on measures and predictions as requested, never overwriting, with a run
record. No statistical change. Approved as written; implemented in round 41.

### D-36: Slice 2d specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/standardization.md`: indirect and direct standardized ratios and rates, their
standard errors (model-based, robust, β known; indirect variance at γ₀ or γ̂ⱼ), tests on a measure
(identity, logit and log scales), R's extreme observations, the distributed plan, and the decision
on OI-49: direct sums by binned Taylor expansions (bins of 0.1 on η, order 8), whose remainder is
below rounding. Decision to approve with it: X-026 (class C). Approved as written; fixtures and
calibration in round 38.

### D-35: Slice 2c specification (Accepted: approved by the maintainer, 2026-10-08)

`docs/spec/logistic/provider-tests.md`: provider tests on the logistic effects (exact Poisson-binomial
with mid-p tails and test-inversion limits, score, Wald, bootstrap), the reference effect, flags and
the result table, the exact algorithm (OI-52: the smaller tail by the exact recursion), the
distributed plan and validation. Decisions to approve with it: X-024 (correct pprof_py's upper
tails, class B) and X-025 (bootstrap parity distributional, class C). Approved as written; fixtures
and calibration in round 35.

### D-34: Slice 2b specification (Accepted: approved by the maintainer, 2026-10-07)

`docs/spec/logistic/covariate-inference.md`: Wald variants (null, sidedness, robust), likelihood-ratio
and score tests by refits on the working set, cluster-robust variances of β̂ and of the provider
effects (at the average case mix and with β known), prediction and AUC, with their distributed
plan and validation. Decisions to approve with it: X-022 (fail with counts for unknown providers;
no AUC for two outcome values other than 0 and 1; class C) and X-023 (clusters nested in providers,
following pprof_py). Approved as written; fixtures and calibration in round 32.

### D-33: Python wrappers' design, ADR-0009 (Accepted: approved by the maintainer, 2026-10-07)

`docs/adr/0009-py4j-wrappers.md`: a Java-friendly facade `pprof.spark.app.python.PythonApi` in `app`
(DataFrames, strings and numbers in; DataFrames, JSON summaries and opaque fit handles out), a
pure-Python package `pprof_spark` in `python/`, PySpark Classic sessions only, a wheel version-locked
to the JAR, and a CI job running the Python tests against PySpark 4.1.0. The round 29 spike reached
`LogisticFE` from PySpark through py4j. Approved; the Cox wrappers followed in round 30.

### D-32: Close slice 2a at parity-verified (Accepted: approved by the maintainer, 2026-10-07)

`docs/gates/phase-2a.md`: every requirement is met in the sandbox except CI for rounds 27 and 28,
the correlation warning and the scale test (D-28). Proposed: close slice 2a at parity-verified once
CI is green for rounds 27 and 28 and the correlation warning lands as decided under X-021, whose
recommended option computes it on the fitted rows (class C). The feature stays `Experimental`.
Approved with X-021 option (a); the warning landed in round 29, so the slice closes once CI is
green for rounds 27 to 29.

### D-31: Python access through py4j wrappers (Approved by the maintainer, 2026-10-07)

Revisiting D-03 after Phase 1d, the maintainer chose py4j wrappers: Python calls the Scala engine
through PySpark's JVM gateway; Python code never reimplements statistics (§6.12). Supersedes D-03.
Design (facade, packaging, CI, supported compute) in ADR-0009 before any wrapper code; schedule in
`docs/spec/logistic/plan.md` §6 (OI-53). Order approved by the maintainer on 2026-10-07: ADR-0009 once
the slice 2a estimator lands, then wrappers for the Cox API, then each logistic slice.

### D-30: Phase 2 plan and the first-slice specification (Accepted: approved by the maintainer, 2026-10-07)

`docs/spec/logistic/plan.md` (slices 2a to 2f with their references, fixtures, tolerance classes
and distributed design) and `docs/spec/logistic/fixed-effect-estimation.md` (slice 2a: SerBIN
estimation of the logistic fixed-effect model, screening, degenerate providers, model-based
variances, the Wald test for β). Discrepancy decisions to approve with it: X-004 resolved at
v0.7.0; X-016, X-017 and X-018 follow the reference, with degenerate providers flagged; X-019
fails at validation (class C); X-020 follows the behavior (class E). Approved as written, with the
recommended decisions; fixtures and calibration in round 26.

### D-29: Close Phase 1d at parity-verified (Accepted: signed off by the maintainer, 2026-10-07, with round 22's CI green)

`docs/gates/phase-1d.md`: every requirement is met except the end-to-end run at design-envelope
scale, which the maintainer runs on his Databricks once the whole package is done (D-28). Proposed:
close Phase 1d once CI for round 22 is green, on the terms of D-22, D-24 and D-26.

### D-28: Site for the Phase 1d scale test (Decided by the maintainer, 2026-10-06)

The maintainer runs the scale test himself on his Databricks workspace once the whole package is
done, not phase by phase (his preference, stated 2026-10-07). AI assistants never access that
environment; they prepare the JARs, run specifications, generator and instructions, and analyse
shared results. Phase gates therefore close at parity-verified, as Phases 1a to 1c did, and scale
verification happens once for the package.

### D-27: Phase 1d specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/provider-workflows.md`: indirect and direct standardized ratios (two-stage SMR and
SHR) with pprof_py's conventions, exact and mid-p provider tests with the theoretical null,
flags and result tables, a job runner, and scale validation. The empirical null stays deferred.

### D-26: Close Phase 1c at parity-verified (Accepted: signed off by the maintainer, 2026-10-06, after round 18's CI passed)

`docs/gates/phase-1c.md`: every requirement is met except the scale test. Proposed: close Phase 1c
on the terms of D-22 and D-24 once round 18's CI passes.

### D-25: Phase 1c specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/residuals-robust.md`: martingale, score and dfbeta residuals and robust variance,
per row or clustered, with X-015 (robust variance from score residuals, as R, where pprof_py's
Breslow counting-process kernel disagrees with R by 11%) and residual tables keyed by the row
identifier.

### D-24: Close Phase 1b at parity-verified (Accepted: signed off by the maintainer, 2026-10-06, after round 16.1's CI passed)

`docs/gates/phase-1b.md`: every requirement is met except the scale test. Proposed: close Phase 1b
on D-22's terms, with scale verification at the Phase 1d gate.

### D-23: Phase 1b specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/counting-process-baseline.md`: (start, stop] data and left truncation, per-stratum
baseline hazard and survival, and prediction, with X-014 (the baseline is reported at x = 0 and
offset 0; pprof_py's public baseline and R's `basehaz` include exp of the weighted mean offset).

### D-22: Close Phase 1a at parity-verified (Accepted, 2026-10-06)

`docs/gates/phase-1a.md`: every Phase 1a requirement of §9.8 is met except the scale test, which
needs a cluster (D-14) and targets (D-04). Proposed: close Phase 1a once CI confirms rounds 11 and
12, keep the Cox features `Experimental` at parity-verified, and move their scale verification to
the Phase 1d gate. Asked to sign off, the maintainer replied "Continue"; recorded as sign-off.

### D-21: Efron ties, case weights and offsets (Accepted in advance, 2026-10-06)

`docs/spec/cox/efron-weights-offsets.md`. The maintainer approved the round's specification and
changes in advance and asked for the work to continue. X-013 (zero-weight events under Efron) was
found afterwards; asked to confirm the recommended corrected behavior, the maintainer replied
"Continue", so it stands under the standing delegation. Implemented in round 11.

### D-20: Cox first-slice specification (Accepted: approved by the maintainer, 2026-10-06)

`docs/spec/cox/first-slice.md`: stratified, right-censored, Breslow, model-based variance. It
includes three discrepancy decisions to approve with it: X-010 changed to test convergence before
halving (corrected behavior, superseding round 6's bug-compatible choice), X-011 (aliasing fails),
and X-012 (no events fails). Approved as written, with the three discrepancy decisions;
implemented in round 10.

### D-12: Build and test tooling baseline (Delegated; reversible)

sbt 1.12.15 (1.13.0 exists but has no patch release yet); sbt-scalafmt 2.6.2 with scalafmt
3.11.5; sbt-buildinfo 0.13.2; munit 1.2.0, the newest release built with Scala 2.13.16 (later
releases pull scala-library 2.13.17 or 2.13.18, OI-04). GitHub Actions: checkout v7, setup-java
v6 (Temurin), setup-sbt v1, upload-artifact v7, runner ubuntu-24.04. Deferred: OI-15.
