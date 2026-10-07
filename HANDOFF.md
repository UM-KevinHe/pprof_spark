# Handoff

## Session handoff: start of Phase 2 (2026-10-07)

This section stands on its own for a new session. The round log below it is the detailed history.

### 1. The project

pprof_spark ([UM-KevinHe/pprof_spark](https://github.com/UM-KevinHe/pprof_spark), public, MIT,
maintainer Kevin He) reimplements the validated statistical methods of pprof_py, pinned at v0.7.0,
commit `9320766` (D-05), as distributed Spark algorithms that keep their statistical definitions
and validated behavior. Stack: Spark 4.1.0, Scala 2.13.16 (PLAT-3), Java 17 bytecode; CI on JDK 17
and 21, under Classic Spark and Spark Connect (T8). The authoritative context is
`docs/PROJECT_CONTEXT.md` v2.3; recorded decisions in `DECISIONS.md` take precedence over it.

### 2. Where things stand

| Phase | Status |
|---|---|
| 0, foundations | Closed (`docs/gates/phase-0.md`) |
| 1a, Cox estimation core | Closed (D-22) |
| 1b, counting process and baseline | Closed (D-24) |
| 1c, residuals and robust variance | Closed (D-26) |
| 1d, provider workflows | Closed (D-29) |
| 2, logistic provider models | Plan and 2a specification proposed (round 25, D-30) |

Every closed phase passed its parity gate at parity-verified; its features stay `Experimental`
until the package-level scale test, which the maintainer runs himself on his Databricks workspace
once the whole package is done (D-28). AI assistants never access his Databricks environment (D-14
as clarified). CI is green through round 22; rounds 23 and 24 changed documents only.

### 3. What exists

| Area | Content |
|---|---|
| `numerics` | Pairwise and Neumaier summation; `Normal`, `Cholesky` (R's aliasing rule), `Newton` (convergence tested before halving, X-010); `Gamma`, `Poisson`, `Brent`, `PoissonTests`; kernels `Moments`, `CoxStratum` (likelihood with ties, weights, offsets, entry times), `CoxResiduals`, `CoxMeasures` |
| `engine` | Validation with counts (`InputProblem`), layouts (`GroupSizes`, `LayoutPlan`), blocks and ordered reduction; `cox`: `CoxSpec`, `CoxOptions`, `CoxPH.fit`, `baseline`, `residuals`, `CoxFit`, `CoxFitIO` (format version 3), `CoxPrediction`, `CoxMeasures`, `CoxProviderTests` |
| `app` | `RunSpec` (JSON run specification, version 1) and `CoxJob` for `spark-submit` (`docs/guide/cox-job.md`) |
| `testkit` | Classic and Connect sessions, `SparkSuite` (two-minute test timeout), `Tolerances` (`tolerances.conf`, D-09), `Fixtures` |
| `bench` | `DeterminismCost` (ADR-0006) |
| Fixtures | `fixtures/cox`: six cases with `pprof_py.json` and `r_survival.json`, made by `reference/fixtures/generate.py` and `cox_survival.R`; `calibrate.py`; regeneration is byte-identical |
| Documents | `docs/spec/cox/` (five approved specifications), `docs/gates/`, `docs/parity/matrix.md`, `docs/adr/`, `STATUS.md`, `DECISIONS.md` (D-01 to D-29), `DISCREPANCIES.md` (X-001 to X-015), `OPEN_ITEMS.md` (to OI-47) |

### 4. How the work runs

- Rounds. Each round is one commit delivered as a patch (`git format-patch -1`), rehearsed by
  applying it to a fresh clone of `main` and checking that the tree matches, with an evidence log.
  The maintainer applies and pushes it and reports CI. CI is never recorded as green without his
  report (NN-8); the sandbox cannot read the GitHub API reliably (rate limits).
- Specification first (NN-2). Each feature gets `docs/spec/<family>/<feature>.md`, approved before
  code. Discrepancies with the reference (classes A to D, §3.4) are recommended, never assumed; the
  maintainer decides. He often answers "continue"; a direct question gets a direct answer.
- Records. Every round updates `STATUS.md`, this file's round log, the parity matrix and, as needed,
  `DECISIONS.md`, `DISCREPANCIES.md`, `OPEN_ITEMS.md` and the specifications.
- Parity. Function-level, lockstep and end-to-end tests against pprof_py and R fixtures, under the
  tolerance classes. When implementations' estimates differ slightly (X-010), downstream
  quantities are compared at each reference's own estimates.
- Reproducibility. R0: bitwise per layout (canonical row order, compensated sums, block-ordered
  reductions, StrictMath); R1: within T-part across layouts; R2: across JDKs and Classic and Connect.

### 5. Verifying in the sandbox

sbt cannot run in the sandbox (no Maven Central). The emulation compiles with the real Scala 2.13.16
compiler and the build's exact scalacOptions, against Spark 4.1.0 jars from the PySpark 4.1.0
distribution, and runs suites under a munit stand-in that enforces munit's timeouts. The scripts and
stand-ins are in `sandbox-tools.zip` (outside the repository), whose `SETUP.md` rebuilds the
environment. Typical use: `compile-only.sh` (about 2 minutes), then `run-suites.sh <jdk>
<classic|connect> "<modules>" "<engine suites>"`, `quick.sh` for one module, `app.sh` for the app
module; formatting with the scalafmt 3.11.5 native binary; `scripts/check-engine-api.sh`. A tool
call ends after about five minutes and background processes do not survive it, so split long runs:
CoxPHSuite and CoxBaselineSuite each take two to four minutes. Reference tools: Python 3.12.3 with
numpy 2.5.3, scipy 1.18.1, pandas 3.0.6, numba 0.68.0, mpmath 1.4.1 and pprof_py installed from a
clone at `9320766`; R 4.3.3 with survival 3.5-8.

### 6. Lessons that cost a round

- Spark Connect rejects an aggregate with two identically named columns; alias every column (OI-39).
- munit fails any test over 30 seconds by default; register one test per fixture case (OI-44).
- `/bin/sh` is dash: put bash syntax in script files. scalafmt reformats code, so scripted edits
  must match formatted text.
- Editing a file Spark saved needs its Hadoop `.crc` file deleted too.
- Jackson 2.20 deprecates `JsonNode.fields()`; with `-Werror` that fails the build.
- Residuals, baselines and measures move with the estimates; compare them at the reference's own.

### 7. Open questions to carry into Phase 2

- D-03 (Python access) was revisited in round 25: unchanged.
- OI-02 and OI-38 (large p), OI-03 (TimeRange for very large strata), OI-29 (distributed result
  tables at large m), OI-40 (CI check of the parity matrix), OI-41 (per-release model fixtures),
  OI-43, OI-45 and OI-46 (scale costs), OI-33 and OI-42 (reports to pprof_py), OI-13 (lme4
  comparison script not in pprof_py), which bears on Phase 2's random-intercept stage.

### 8. Phase 2 starting point

Scope (§4): large-m logistic fixed effects (SerBIN-type blocked Newton); provider tests (Wald,
score, exact Poisson-binomial, bootstrap); direct and indirect standardization; then the
three-stage SRR pipeline, including its stage-2 random-intercept variance estimation. pprof_py
classes: `LogisticFixedEffectModel` and `LogisticThreeStageModel`. First: read their
implementations at the pin, then propose a Phase 2 plan in slices with the first slice's
specification, fixtures and tolerance calibration, for approval before any code.

## Round log

## Round 25 (2026-10-07): Phase 2 plan and the first-slice specification

- New session. The sandbox was rebuilt from `sandbox-tools.zip` on a one-CPU, 3 GB machine
  (OpenJDK 17.0.20.1 and 21.0.10): `compile-only.sh` passes (123 s); numerics 60 and testkit 11
  pass on JDK 17 and 21 (Classic) and testkit under Spark Connect; five engine suites (29 tests)
  pass under Classic and Spark Connect. The zip lacks the optional `probe/Probe.scala`, so that
  informational step reports a missing file.
- D-03 revisited as planned after Phase 1d: the maintainer replied "Continue"; recorded as no
  change (no Python access in v1).
- Read pprof_py v0.7.0's logistic models (fixed effects, provider tests, standardization, the
  three-stage pipeline) and R pprof 1.0.3 (MIT). `docs/spec/logistic/plan.md` proposes slices 2a
  to 2f with references, fixtures, tolerance classes and the distributed design;
  `docs/spec/logistic/fixed-effect-estimation.md` specifies slice 2a (D-30, awaiting approval).
- Probes (sandbox, not fixtures): pprof_py's logistic suites pass at the pin (175 passed);
  pprof_py at tol 1e-13 agrees with `glm` within 1.4e-13 for estimates and variances, with equal
  ℓ; R pprof's SerBIN differs by 2.2e-10 (X-017); feature shifts change β̂ by 5.6e-15 (X-004
  resolved); 30 row orders vary β̂ by 2.2e-16; degenerate providers end at med(γ) ∓ 10 (X-018);
  edge cases in the specification's §9 (X-019); binomial rows equal expanded Bernoulli rows (X-020).
- PROJECT_CONTEXT §7.4's account of the three-stage model does not match the reference (OI-48).
  New open items OI-48 to OI-52. Documents only; no code changed.

## Round 23 (2026-10-07): Phase 1d gate review and PROJECT_CONTEXT v2.3

- `docs/gates/phase-1d.md`: all requirements met except the design-envelope run, which the
  maintainer does on his Databricks once the whole package is done (D-28). Measured worst ratios:
  indirect measures 1.2e-7 and direct 1.6e-6 of T-base; provider-test z 2.1e-6 of T-test, limits
  4.5e-7 of T-base; flags exact. D-29 proposes closing Phase 1d once CI for round 22 is green.
- CI results for rounds 20 to 22 were not reported; STATUS and the parity matrix now say so instead
  of "green".
- PROJECT_CONTEXT v2.3 (OI-47 closed): D-14's clarification, D-28 (scale test on the maintainer's
  Databricks at package completion), the implemented job runner (§6.12), T9 and §10.4 updated, D-20
  to D-29 in §16, and an Appendix A entry. Replace the claude.ai project copy with this file.
- README status updated for the Cox package as it stands.

## Round 22 (2026-10-07): the job runner (1d-3)

- `app` module (already declared in build.sbt, now with sources): `RunSpec.parse` reads a version-1
  JSON run specification with Jackson (from Spark's classpath), defaulting omitted fit options to
  the library's and reporting every problem at once; `CoxJob.main` (`--spec <path>` read through
  Spark, or `--spec-json <text>`) and `CoxJob.run` fit, then write the fit with its baseline
  (`CoxFitIO`), residuals, standardized measures and provider tests as Parquet, and a one-line JSON
  run record (specification, software, fingerprint, convergence, warnings, outputs, timings).
  Outputs are never overwritten.
- `CoxJobSuite` (3): on lt-weights-offset with Efron ties, every output equals the library's bit
  for bit, the run record is right and a second run fails rather than overwriting; an invalid
  specification lists ten problems; argument handling.
- CI: the Spark Connect job now runs `app/test` too.
- `docs/guide/cox-job.md`: usage with `spark-submit` and as a Databricks JAR task, the run
  specification and the outputs.
- Sandbox verification: the app module compiles at Scala 2.13.17 and, for linkage, 2.13.16; its suite
  passes under Classic and Spark Connect (JDK 17). The end-to-end test takes about 70 s here, within
  the two-minute limit for Spark suites. The engine and other modules are unchanged this round.

## Round 21 (2026-10-07): provider tests (1d-2)

The maintainer prefers to run the scale test on his Databricks once the whole package is done, not
per phase (D-28 updated): phase gates close at parity-verified and scale verification happens once.

- Fixtures: pprof_py's `CoxPH.test` for every case and tie method, mid-p and exact at level 0.95:
  estimate, z_raw, p_value, flag, limits (p-values down to 1.4e-25; flags +1, −1 and 0).
- `numerics`: `Brent.root` (zeroin); `Gamma.logGamma` (Lanczos g = 7) and `Gamma.regularized`
  (series and continued fraction, smaller tail direct); `Poisson.cdf`, `atLeast`, `pmf`,
  `chiSquareQuantile`; `PoissonTests.midpZ`, `exactP`, `exactLimits` (χ² below E = 100, Byar
  above), `midpLimits` (pprof_py's bracketing and tolerances) and `test`. `PoissonSuite` checks them
  against mpmath, scipy and pprof_py's own functions.
- `engine.cox.CoxProviderTests.test(df, fit, provider, method, level, providers)`: indirect
  measures, then a Scala UDF per provider; pprof_py's `PROVIDER_TEST_COLUMNS` plus observed,
  expected and person-time; columns pprof_py leaves empty for Poisson tests are NaN or null.
- `CoxProviderTestsSuite` (13): both methods for every case and tie method at pprof_py's estimates;
  the column list; level validation.
- Sandbox verification: JDK 17 Classic numerics 60, testkit 11, engine 215 (all suites); Spark
  Connect: CoxProviderTestsSuite (13), so the Scala UDF works there.

## Round 20 (2026-10-06): Phase 1d fixtures and standardized measures

The maintainer approved the Phase 1d specification (D-27) and settled the scale-test site: he runs
it himself on his Databricks workspace (D-28). D-14 is clarified: AI assistants never access his
Databricks environment; pprof_spark itself may run there (OI-47 tracks folding this into
PROJECT_CONTEXT).

- Fixtures: pprof_py's indirect and direct measures for every case and tie method, with the
  stratum as provider for stratified (two-stage) fits and id mod 10 for pooled fits. Σ Eⱼ = O holds
  to 2.3e-13. Regeneration byte-identical; calibration passes.
- `numerics.kernels.CoxMeasures`: per-time totals (Σ w by exit and by entry, events), the national
  baseline RS(t) = Σ_{exit ≥ t} w − Σ_{entry ≥ t} w and Λ₀, and per provider Oⱼ, Eⱼ, person-time and
  E⁽ʲ⁾ = Σ RS(t)/RSⱼ(t); w = exp(η − max η), no case weights, as pprof_py.
- `engine.cox.CoxMeasures.standardized(df, fit, provider, kinds, providers)`: a provider-local
  working set (the fit's own when the provider is its strata, after the fingerprint check; otherwise
  the fit's layout checks the fingerprint first, OI-46), time totals reduced by time in block order,
  the K-scale national baseline on the driver (guarded by `maxGroupsOnDriver`), provider tables
  distributed; providers without expected events are logged.
- `CoxMeasuresSuite` (15): every case and tie method against pprof_py at its estimates; Σ Eⱼ = O;
  the provider filter; the fingerprint checks.
- Sandbox verification: JDK 17 Classic numerics 54, testkit 11, engine 202 (all suites); Spark
  Connect: CoxMeasuresSuite (15). Not rerun on Spark Connect or JDK 21: the other suites.

## Round 19 (2026-10-06): Phase 1c closed; Phase 1d specification

- Round 18's CI passed and the maintainer closed Phase 1c (D-26).
- `docs/spec/cox/provider-workflows.md` (D-27, awaiting approval), read from pprof_py v0.7.0's
  `calculate_standardized_measures`, `cox_standardized_expectations`, `test`,
  `poisson_exact_test` and `poisson_midp_zscore`: indirect (Oⱼ/Eⱼ, national Breslow baseline with η
  as offset) and direct (E⁽ʲ⁾/O) ratios; Breslow baselines whatever the ties and no case weights,
  as pprof_py; exact Poisson tests (p clipped at 0.999; χ² limits below E = 100, Byar above) and
  mid-p tests (p floor 1e-6, limits by root finding) with the theoretical null; flags and
  `PROVIDER_TEST_COLUMNS`; a JSON-driven `spark-submit` job runner; a distributed plan (national
  baseline from time-keyed partials, K-scale on the driver; provider sums by keyed reduction;
  provider-local blocks for E⁽ʲ⁾); scale validation with synthetic data up to n = 10⁹.
- D-28 asks where the scale test runs: a university HPC cluster with Spark standalone, a managed
  cloud service, or one large machine.

## Round 18 (2026-10-06): robust variance and the Phase 1c gate review

- `CoxOptions.robust` and `CoxSpec.cluster` (integral or string, no nulls; it implies robust).
  `CoxPH.fit` computes B = Σ_c s_c s_cᵀ at β̂ from the residual kernel: per row from block partials
  in block order; clustered through one shuffle by cluster (sums in block and position order)
  and 256 MurmurHash3 buckets reduced in bucket order. V_rob = V B V, exactly symmetric; standard
  errors, z, p and intervals use it; `CoxFit.naiveCovariance` keeps V, and `robust` and
  `clusters` record the choice. Per-row robust variance on data with entry times records a
  warning. dfbeta residuals use the model-based covariance.
- Blocks carry the cluster key (canonical order and fingerprint include it).
- `CoxFitIO` format version 3: cluster column, robust flags, cluster count, model-based
  covariance; versions 1 and 2 load as model-based.
- `CoxRobustSuite` (17): per-row and clustered robust variance against R and pprof_py (X-015 cases
  against pprof_py's dfbeta sandwich) for six fixtures and both tie methods; clusters of one row;
  renamed clusters; R0 bitwise; the warning; validation; persistence.
- `docs/gates/phase-1c.md`: everything met except the scale test; D-26 proposes closing Phase 1c on
  the terms of D-22 and D-24 once CI passes.
- Sandbox verification: JDK 17 Classic numerics 54, testkit 11, engine 187 (all suites); Spark
  Connect: CoxRobustSuite and CoxFitIOSuite (21), CoxPHSuite (89). Not rerun on Spark Connect:
  CoxBaselineSuite and CoxResidualsSuite, which this round touched only through shared block code.

## Round 17 (2026-10-06): Phase 1c fixtures and residuals

Round 16.1's CI passed; the maintainer closed Phase 1b (D-24) and approved the Phase 1c
specification with X-015 (D-25).

- Fixtures: for six cases and both tie methods, pprof_py's martingale, score and dfbeta residuals,
  its naive, per-row robust and clustered robust covariances (clusters id mod 40), and sandwiches
  of its dfbeta residuals; R's residuals and robust variances (`cluster = id` and `id %% 40`).
  Fixtures grow from 0.9 MB to 3.0 MB; regeneration is byte-identical; calibration passes.
- Calibration findings: residuals of pprof_py and R differ by up to 2.1e-9 for Breslow, past
  T-res (atol 1e-9), because their tight estimates differ; at fixed estimates the computations
  agree, so residual parity is tested at each reference's own estimates and covariance. Robust
  variances: X-015 on both left-truncated Breslow cases (8% and 11%); the dfbeta sandwiches match
  R everywhere (2.3e-9 or better).
- `numerics`: `CoxResiduals.stratum`: one descending sweep for per-time h, g, the Efron dying share
  c₀ = Σₖ (k/d)·m/Aₖ and c₁, and the mean of the k-th means; ascending running totals; per-row
  differences over (entry, exit]. Zero-weight rows get the residuals of an at-risk row that never
  dies (X-013).
- `engine`: `CoxPH.residuals(df, fit)`: the row identifier, `martingale`, `score_<feature>`,
  `dfbeta_<feature>` (wᵢ·Uᵢ·V); needs `CoxSpec.rowId`; checks the fingerprint; distributed.
- Tests (`CoxResidualsSuite`, 15): martingale, score and dfbeta against pprof_py and R at their
  estimates for all six cases and both tie methods, including Efron with left truncation;
  weighted martingale residuals sum to zero per stratum and weighted score residuals to the
  score; API checks.
- Sandbox verification: numerics 54 and testkit 11 (JDK 17); engine non-Cox suites, CoxFitIOSuite
  and CoxResidualsSuite on JDK 17 Classic (53) and CoxResidualsSuite on Spark Connect (15; slowest
  test 36 s, within Spark suites' two minutes). CoxPHSuite and CoxBaselineSuite were not rerun:
  their code is unchanged and the fixture values they read are unchanged.

## Round 16.1 (2026-10-06): fix round 15's CI failure

- Round 15's Spark Connect job (run 37533252860) failed one test: `java.util.concurrent.
  TimeoutException: test timed out after 30 seconds`, from munit's default per-test limit, in a
  CoxBaselineSuite test that looped over twelve fits. The sandbox missed it because its munit
  stand-in did not enforce timeouts; it now does, as munit does (OI-44).
- Fix: the long Cox tests register one test per fixture case and tie method (CoxPHSuite 23 to 89
  tests, CoxBaselineSuite 6 to 28), and `SparkSuite` allows two minutes per test for whole-fit
  Spark tests. The slowest test in the sandbox's Spark Connect run took 33 s, the first of the
  suite including session start-up; the rest took at most 9 s.
- Engine tests now number 155 per job (Spark Connect included); numerics 54, testkit 11.
- Records corrected: rounds 11 to 14 are green; Phase 1a's closure (D-22) depended on rounds 11
  and 12 and stands; Phase 1b's CI evidence waits on this round.

## Round 16 (2026-10-06): Phase 1a closed, Phase 1b gate review, Phase 1c specification

- The maintainer confirmed CI green for rounds 11 to 15, which closes Phase 1a (D-22).
- `docs/gates/phase-1b.md`: everything met except the scale test; worst T-base ratios 3.2e-6 for
  the baseline and predictions against pprof_py (at its estimates) and 3.8e-8 against R's
  `basehaz`. D-24 proposes closing Phase 1b on Phase 1a's terms.
- `docs/spec/cox/residuals-robust.md` (D-25, awaiting approval): martingale, score and dfbeta
  residuals as R's agmart3 and agscore3 and pprof_py; robust variance V(Σ s_c s_cᵀ)V per row or
  clustered; residual tables keyed by the row identifier; a deterministic clustered reduction over
  256 hash buckets; fixtures and tests.
- Probe of pprof_py v0.7.0 against R (lt-weights-offset, 40 clusters): martingale, score and dfbeta
  residuals agree to 2e-11 (Breslow) and 8e-15 (Efron); robust variances agree to 3e-14 for Efron
  and to 3e-10 for right-censored Breslow, but pprof_py's Breslow robust variance on (start, stop]
  data is 11% off R's, while the sandwich of its own dfbeta residuals matches R to 7e-12 (X-015).
  R refuses per-row robust variance for (start, stop] data without `cluster` or `id`.

## Round 15 (2026-10-06): baseline hazard, prediction, and format version 2

- `numerics`: `CoxStratum.baseline` returns each stratum's event times and increments at β with
  uncentered covariates (x = 0, offset 0): d_w/S₀, or m·Σ 1/Aₖ for Efron with ties, with the same
  risk sets and entry-time removals as the likelihood.
- `CoxPH.baseline(df, fit)`: checks the data fingerprint (API-3), builds the table on the executors
  (stratum, time, hazard_increment, cumulative_hazard, survival) with a Neumaier running total,
  persists it and never collects it (DIST-1).
- `CoxPrediction`: linear predictor, relative hazard exp(min(η, 700)), and the cumulative hazard and
  survival at each row's own time through a union-and-window as-of join, right-continuous; Spark's
  EXP is StrictMath-based. Missing or non-finite inputs and strata without a baseline fail with
  counts (`InputProblem.UnknownStrata`).
- `CoxFitIO` format version 2: `hasBaseline` and the baseline table as Parquet; versions 1 and 2
  load. `loadBaseline` returns the table when one was saved.
- Tests (`CoxBaselineSuite`): baselines and predictions against pprof_py at pprof_py's own
  estimates (so X-010 does not enter) and against R's `basehaz` end to end, for six fixtures and
  both tie methods; steps between grid points; R0; the fingerprint check; validation; persistence.
- Sandbox verification, run in parts to fit the sandbox time limit: JDK 17 Classic, JDK 21 Classic
  and JDK 17 Spark Connect each pass numerics 54, testkit 11 and engine 67.

## Round 14 (2026-10-06): Phase 1b fixtures and fitting with entry times

The maintainer approved the Phase 1b specification with X-014 (D-23).

- Fixtures: two left-truncated cases (lt-stratified; lt-weights-offset with weights and offsets),
  entries in [0, exit) for about half the rows. For all six cases the generator now records the
  tight fit's raw baseline (stratum, time, cumulative hazard, survival), the public baseline,
  the weighted mean offset, linear predictors and relative hazards for three covariate profiles,
  and cumulative hazards and survival on two strata's grids; R records `basehaz(fit, centered =
  FALSE)`. pprof_py's public baseline agrees with R's at every event time within 0.046 of T-base,
  and equals the raw baseline times exp(weighted mean offset) exactly (X-014). Regeneration is
  byte-identical and calibration passes for all six cases.
- `numerics`: `CoxStratum` removes rows whose entry time is at or after the current event time,
  using a per-stratum entry order; with every entry below every event time the sums are exactly
  those of right-censored data. Tested against brute-force risk sets and finite differences.
- `engine.cox`: `CoxSpec.entry`; validation (finite entries below the exit; exits may then be zero
  or negative); entries in the blocks, canonical order, fingerprint and `CoxFitIO`.
- Tests: the Cox suite now covers six fixtures and both tie methods, plus an entry column of zeros
  (bitwise equal to right-censored fits), interval splitting, and a shift of every time to below
  zero (bitwise equal).
- Sandbox verification: JDK 17 Classic (numerics 54, testkit 11, engine 61), JDK 21 Classic (the
  same counts) and JDK 17 Spark Connect (engine 61) pass.

## Round 13 (2026-10-06): Phase 1a signed off; Phase 1b specification

- D-22 accepted: asked to sign off the Phase 1a gate, the maintainer replied "Continue". Phase 1a
  closes when CI confirms rounds 11 and 12 (not readable from the sandbox).
- `docs/spec/cox/counting-process-baseline.md` (D-23, awaiting approval): (start, stop] data and
  left truncation with the risk set a < t ≤ b; per-stratum baseline increments (Breslow d_w/S₀;
  Efron Σ m/Aₖ), cumulative hazard and survival exp(−Λ₀) at x = 0 and offset 0 (X-014);
  prediction of the linear predictor, relative hazard (exp clipped at 700, as pprof_py), cumulative
  hazard and survival as DataFrame columns, the baseline as a distributed table, an as-of join for
  times; fixtures, metamorphic tests and the distributed plan.
- Probes of pprof_py v0.7.0: baseline tables list each stratum's event times with cumulative
  hazard and survival; the public baseline is the raw one times exp(weighted mean offset); negative
  entry times are accepted; a small dataset with one zero-weight tied event raised "Empty risk
  set" (OI-42).

## Round 12 (2026-10-06): persistence, logging, and the Phase 1a gate review

- `CoxFitIO` saves a `CoxFit` as one JSON metadata record with doubles as 64-bit patterns and loads
  it bit for bit (PERS-1), through Spark's public API (PERS-3), never overwriting; another kind or
  format version fails with a message naming both versions (PERS-2). `Ties.fromName` added.
- `CoxFit` now carries the information matrix I(β̂) (a Phase 1a output, checked against the
  inverse of the fixtures' covariance under T-var) and its feature status, `experimental` (NN-12).
- `CoxPH` logs each fit warning through slf4j (OI-36): the iteration-cap tests show
  `WARN CoxPH$: the Cox fit did not converge: ...` in the test log.
- `docs/gates/phase-1a.md`: every requirement is met except the scale test (no cluster, D-14; no
  targets yet, D-04). D-22 proposes closing Phase 1a at parity-verified once CI confirms rounds 11
  and 12, keeping the features Experimental and moving scale verification to the Phase 1d gate.
- Sandbox verification: all suites pass on JDK 17 and 21 (Classic) and JDK 17 (Spark Connect):
  numerics 51, testkit 11, engine 57 each. A test that edits a saved file must also delete
  Hadoop's `.crc` checksum beside it.
- Round 11's CI result could not be read (the GitHub API was unavailable from the sandbox).

## Round 11 (2026-10-06): Efron ties, case weights and offsets

Round 10 passed CI (run 37502839703). The maintainer approved this round in advance (D-21) and,
asked to confirm X-013, replied "Continue", so the corrected behavior stands.

- Spec addendum `docs/spec/cox/efron-weights-offsets.md`: pprof_py's Efron formula (mean weight,
  k = 1 to d, the d = 1 shortcut), which R's coxfit6 shares; weights finite and non-negative;
  offsets finite; model-based variance; X-013, a zero weight equals an absent row under both
  methods (pprof_py still counts a zero-weight tied event under Efron; R rejects zero weights).
- `numerics`: `CoxStratum` replaces `CoxBreslow`, with weights, offsets and Efron. Unweighted
  Breslow fits are bitwise identical to round 10's.
- `engine.cox`: `CoxSpec.weight` and `CoxSpec.offset`, `Ties.Efron`, validation of both columns,
  weights and offsets in the blocks, canonical order and fingerprint.
- Tests: `CoxStratumSuite` (7) and `CoxPHSuite` (19), both tie methods on all four fixtures, plus
  metamorphic tests for weights and offsets and the X-013 equivalence.
- Spark Connect caught a defect: with no weight column, two validation counts shared an expression
  and Spark Connect rejected the duplicate column names (OI-39). Every count now has an alias.
- Sandbox verification: JDK 17 and 21 under Classic Spark and JDK 17 under Spark Connect pass
  (numerics 51, testkit 11, engine 53 each); the eight fixture fits are bitwise identical across
  the three. Worst ratios: T-coef 0.995 (Breslow) and 0.596 (Efron), both tiny-ties against
  pprof_py (X-010); T-fn 0.020; T-iter 2.3e-4; T-var 1.5e-3; T-test 1.6e-7.

## Round 10 (2026-10-06): the first Cox slice

The maintainer approved the Cox first-slice specification with its three discrepancy decisions
(D-20). This round implements it.

- `numerics`: `Normal` (upper tail by series and Laplace's continued fraction, quantile by Newton
  steps; within 2e-13 and 1e-14 of 160-digit mpmath references), `Cholesky` with R's aliasing
  rule (tolerance 2^-39), `Newton` with the approved step control (convergence tested on the full
  step before halving), and the `CoxBreslow` kernel (Neumaier running sums in canonical order).
  `NeumaierVector` gained `addAt` and `valueAt`; `add` keeps its arithmetic.
- `engine.cox`: `CoxSpec`, `CoxValidation` (one job; counts, never values; fails on no events),
  canonical `CoxBlock`s on StratumLocal layouts, `CoxKernel`, and `CoxPH.fit`, returning `CoxFit`
  with Wald inference, warnings, the iteration log and reproducibility metadata.
- Fixtures: the generator now records pprof_py's Newton iterates and z, p and interval outputs
  (OI-35 closed); regeneration is byte-identical and calibration still passes. R outputs are
  unchanged.
- Tests: 19 new `numerics` tests and 16 in `CoxPHSuite`: function-level, lockstep and end-to-end
  parity against pprof_py and R; inference; negative controls; R0 and R1; metamorphic relations;
  every edge case of the specification. Worst ratios: T-coef 0.995 (tiny-ties against pprof_py:
  the X-010 half step) and 1.2e-7 against R; T-fn 0.011; T-iter 2.1e-4; T-var 2.9e-4; T-test
  1.3e-7; T-part 1.6e-7. Scaling x1 by 4 and shifting x2 by 3 left the estimates bitwise unchanged.
- Spec amendment (implementation note, no statistical change): the normal quantile uses Newton
  steps instead of AS 241.
- New open items: OI-36 (log fit warnings), OI-37 (persist `CoxFit`), OI-38 (kernel cost).
- Sandbox verification: every suite passes on JDK 17 and 21 under Classic Spark and on JDK 17
  under Spark Connect (numerics 48, testkit 11, engine 50 tests each). The three fixture fits are
  bitwise identical across those three configurations (R2). One Spark Connect log line
  (`INVALID_CURSOR.DISCONNECTED`) appears during an existing BlockMoments test that passes; it
  is the server's timing-dependent notice of a detached result stream, not a failure.

## Round 9 (2026-10-06): Cox first-slice specification (Phase 1a starts)

Rounds 7 and 8 passed CI (runs 37484837090 and 37487451760), so Phase 0 is closed.

`docs/spec/cox/first-slice.md` specifies the first Cox slice (D-11): stratified, right-censored,
Breslow, model-based variance, with its data contract, conventions, algorithm, inference, outputs,
edge cases, reference mapping to pprof_py v0.7.0 and R, distributed plan and validation plan. It
awaits the maintainer's approval (D-20, NN-2); no Cox code is written before that.

Evidence gathered for it (assistant sandbox, pprof_py v0.7.0): the reference's behavior on ten edge
cases (it raises on invalid times, events and covariates; returns β̂ = 0 when there are no events;
returns pseudo-inverse estimates for collinear covariates) and a row-order experiment: under
pprof_py's step rule, 30 row orders of tiny-ties give 9 distinct coefficient vectors up to 1.05e-8
apart; testing convergence first, as R does, gives a spread of 2.3e-15. Hence the proposed change
to X-010. X-002 is resolved at v0.7.0 (CoxPH warns on non-convergence).

## Round 8 (2026-10-06): PROJECT_CONTEXT v2.2

PROJECT_CONTEXT.md v2.2 folds in decisions D-01 to D-19 and the corrections found in Phase 0, and
drops the v2.1 amendments preface. Main changes: the platform is a standalone Apache Spark 4.1
package (§5, NN-13, D-14); Phase 0's exit and ROAD-1 follow D-11; PLAT-4 and §5.4 describe the
compilation-classpath hazard, the linkage compile and the source check; §3 records the pin, the
verification at v0.7.0 and X-009 and X-010; §6 records the 4 MB block default, negative-zero
normalization, TimeRange's consequences, the reduction-volume question and bit-exact persistence;
§8.4 the calibrated tolerance rule; §9 the T8 cadence and the fixture format; §11 the actual
build, workflows and environments; §12 the licenses; §16 the outcomes; Appendix A the changes.
The claude.ai project copy of PROJECT_CONTEXT.md should be replaced with this file.
No code changed.

## Round 7 (2026-10-06): remaining spikes and the Phase 0 gate review

### CI result of round 6
Run 37477285324 on `main` (0be6e54): every job passed; tests counted engine 34, numerics 29 and
testkit 11, so `FixturesSuite` and the calibration checks pass in CI.

### What this round adds
- S-05 (ADR-0006, D-18): `bench` module with `DeterminismCost` and the manual `bench.yml`.
  Deterministic mode stays the default; multi-sum kernels use a fused pairwise cascade (OI-34).
- S-06 (ADR-0007, D-19): `.devcontainer/` for Codespaces; the maintainer's trial is pending.
- ADR-0008: S-01, S-02's Databricks legs, S-03, S-04 and S-07 deferred, with reasons.
- `docs/gates/phase-0.md`: the gate review. Phase 0 closes when this round's CI passes.

### Evidence (assistant sandbox)
`DeterminismCost` compiled with scalac 2.13.16 and the build's flags and ran twice on OpenJDK
17.0.20 and 21.0.12; the fused cascade reproduced the per-statistic pairwise bits (checked by the
program before timing). The dev container is configuration only (JSON parses; `bash -n` passes).
No test or main code of the other modules changed.

### Next
Round 8 opens Phase 1a: PROJECT_CONTEXT v2.2 (OI-27), then the Cox specification (approval needed,
NN-2).

## Round 6 (2026-10-06): Cox reference fixtures and tolerance calibration

### CI result of round 5
Run 37458038911 on `main` (7f31717): every job passed; tests counted engine 34, numerics 29 and
testkit 6. The first Phase 0 exit criterion (D-11) is met.

### What this round adds (ADR-0005, D-17)
- `reference/fixtures/`: `generate.py` (inputs once, outputs from pprof_py and R),
  `cox_survival.R`, `calibrate.py`, `compare_outputs.py`, `requirements.txt`.
- `fixtures/`: four Cox cases (tiny-ties, rc-unstratified, rc-stratified,
  rc-stratified-weights-offset) with Breslow and Efron outputs, function-level values, negative
  controls, and a checksummed manifest; `docs/parity/cox-calibration.md`.
- testkit: `Fixtures` (reader), the element-wise tolerance rule (`Tolerance.worstRatio`), and
  `FixturesSuite`, which checks checksums, the pin, exact inputs and the calibration in CI.
- `.github/workflows/fixtures.yml`, run by hand, recomputes outputs and compares them under T-part.
- Decisions D-01 to D-04, D-06, D-08 and D-09 approved with the recommendations; D-17; X-007
  resolved; X-008, X-009 and X-010 registered.

### Evidence (assistant sandbox)
Fixture generation is deterministic (byte-identical reruns). Calibration passes every check
(ADR-0005). pprof_py's survival suite passes at v0.7.0, and R regenerates its R results byte for
byte. All modules compile with scalac 2.13.16 and the build's flags; 74 tests (engine 34, numerics
29, testkit 11) pass on OpenJDK 17.0.20 and 21.0.12 under Classic Spark and Spark Connect.
Not verified: `fixtures.yml` on GitHub's runners.

### Next
Round 7: S-05 (cost of deterministic mode), S-06 (Codespaces), ADRs recording S-04 and the spikes
deferred by D-14, then the Phase 0 gate review. Phase 1a starts with the Cox specification.

## Round 5 (2026-10-05): platform skeleton (Phase 0 exit candidate, D-11)

### CI result of rounds 3 and 4
Run 37361715287 on `main` (08b378f): every job passed, the linkage compile included (OI-28
closed); tests on JDK 17 and 21 counted engine 9, numerics 25, testkit 6; T8 counted engine 9.

### What this round adds (ADR-0004, D-16)
- `engine.data`: `InputSpec` and `Validation` (data contract; counts, never values).
- `engine.layout`: `GroupKey`, `LayoutPlan` (key order, dedicated blocks for oversized groups,
  largest-first packing) and `GroupSizes.collectGroupSizes` (guarded).
- `engine.backend`: `BlockOptions` and driver guards, `BlockBuilder` (canonical order, negative
  zeros normalized), `WorkingSet` (broadcast-joined plan, `groupByKey` blocks, explicit
  persistence) and `OrderedReduction`.
- `numerics.kernels`: `Moments` (pairwise column sums and packed cross products) and
  `Fingerprint`.
- `engine.skeleton`: `BlockMoments.fit`, the statistics-free end-to-end computation, and
  `BlockMomentsIO` (bit-exact save and load).
- `SoftwareInfo.fromFields` for persisted metadata.

### Evidence (assistant sandbox; not CI)
All modules compiled with scalac 2.13.16 and the build's flags. 69 tests (engine 34, numerics 29,
testkit 6) pass on OpenJDK 17.0.20 and 21.0.12, under Classic Spark and under in-process Spark
Connect. The skeleton suite covers bitwise invariance (R0), agreement with a Spark-free
evaluation, layout invariance within T-part (R1), exact integer sums, guards, persistence
(PERS-1) and the result schema. scalafmt and the source checks pass.

### Next
Round 6, the fixture pipeline: reference generators for pprof_py v0.7.0 and R, manifests, the
R-comparison triage (OI-12) and tolerance calibration (D-09). Then S-05, S-06 and the Phase 0
gate review.

## Round 4 (2026-10-05): deterministic summation in `numerics`

Apply after round 3.

### What this round adds
- `pprof.spark.numerics.Summation.pairwise`, `NeumaierSum`, `Summation.neumaier` and
  `NeumaierVector`: the summation algorithms that fix the bits of every reduction (§6.4, §6.8),
  specified in ADR-0003 (D-15, delegated).
- `SummationSuite` pins results computed by an independent Python implementation,
  `reference/numerics/summation_reference.py` (standard library only), checks error bounds
  against the exactly rounded sum, and includes a negative control.
- `scripts/check-engine-api.sh` now also rejects `math.*` and `Math.*` transcendental functions in
  `numerics` and `engine` main code (§8.1); its self-test covers 16 violations.

### Evidence (assistant sandbox; not CI)
scalac 2.13.16 with the build's flags compiled `numerics` against scala-library 2.13.16 only, and
every other module as before. Tests: 40 of 40 on OpenJDK 17.0.20 and 21.0.12 (numerics 25,
testkit 6, engine 9), and the engine suites again under Spark Connect. The Python reference and
the Scala code agree bit for bit; scalafmt and the source check pass.

## Round 3 (2026-10-05): maintainer decisions and the round-2 CI fix

### Decisions recorded
D-05 (pprof_py v0.7.0, commit 9320766, pinned in reference/REFERENCE.lock), D-10 (`pprof.spark`),
D-11 (Phase 0 exits on the platform skeleton in CI), D-13 (in-process Connect for T8; ADR-0002
accepted) and D-14 (no Databricks deployment work). PROJECT_CONTEXT.md is v2.1, with a preface
listing the amendments in force; README, CLAUDE.md and docs/compatibility.md describe a
standalone Spark package.

### Round-2 CI result
Run 37351537662 on `main` (c89fcee): both test jobs passed (33 tests each: engine 9, numerics 18,
testkit 6) and the T8 job passed (9 engine tests through Spark Connect). The linkage compile
failed: spark-connect-client-jvm depends on scala-compiler 2.13.17, and sbt keeps scala-library,
scala-reflect and scala-compiler at one version, so forcing only the first two to 2.13.16 is
rejected (OI-28). Round 3 forces all three. The T8 job is now required.

### Evidence
The CI facts above come from the run's public annotations. The fix itself is not verified (sbt
cannot run in the sandbox); the POM of spark-connect-client-jvm 4.1.0 shows the scala-compiler
dependency.

## Round 2 (2026-10-05): Spark Connect locally (spike S-02, local part)

### CI result of round 1.1
Run 37325078166 on `main` (0c0bdd8): every job passed, including the tests on JDK 17 and 21,
which confirms the Classic test-classpath fix (OI-21).

### What this round adds
- Selectable test sessions: `-Dpprof.test.sparkApi=connect` runs Spark suites through the Scala
  Spark Connect client, served by a Connect server started inside the test JVM on the local
  Classic session (`TestSessions`, `LocalSparkConnect`).
- `SparkSuite.withSqlConf` works with either session; `SoftwareInfoSuite` expects the session
  API of the selected mode.
- build.sbt: testkit depends on the Connect server and client; Test classpaths put the client
  jar last (ADR-0002); the mode property reaches forked test JVMs.
- CI: a Spark Connect (T8) job runs the engine suites under Connect, informational until
  ADR-0002 is accepted; every test job publishes per-module test counts as a notice.
- ADR-0002 and D-13, both proposed.

### Evidence (assistant sandbox; not CI)
scalac 2.13.16 with the build's flags compiled every module with the Connect jars on the
classpaths. Classic: 33 of 33 tests pass on OpenJDK 17.0.20 and 21.0.12. Connect: 33 of 33 pass
on both JDKs, the 9 engine tests running through the Connect client; the backend-shape results
were bitwise identical across partition counts, input orders and ANSI modes, and equal to the
Spark-free evaluation, hence to Classic. Negative control: with the client jar first, 7 of 9
engine tests fail with the server's `NoSuchMethodError`. scalafmt, the API check and the workflow
YAML pass; the counts reporter was run on synthetic reports.
Not verified: sbt's resolution of spark-connect and spark-connect-client-jvm, the classpath order
inside sbt, and the new CI job.

### What to send back
The CI run link. Annotations now show per-module test counts for every job, plus failure details
when something fails.

### Next
Round 3 needs the Databricks prerequisites listed under round 1 below.

## Round 1.1 (2026-10-05): fix for the round-1 CI failure

### Round-1 CI result
Run 37318035767 on `main` (434ce01). The checks job passed: engine API rules, scalafmt through
sbt, and the linkage compile against scala-library 2.13.16, which also shows that sbt resolves
every dependency and loads both plugins. Both test jobs failed in step 5 with exit code 1 after
writing test reports. Workflow logs require signing in, so the failing lines were not read.

### Diagnosis: reproduced in the sandbox, not yet confirmed by CI
spark-sql-api depends on spark-connect-shims, whose placeholder `SparkConf`, `SparkContext` and
`RDD` classes shadow spark-core's real ones. Spark's Classic modules (spark-catalyst, spark-sql)
exclude the shims from their spark-sql-api dependency; `engine` declares spark-sql-api directly,
so its Test classpath carried both the shims and spark-core. With the shims first, 7 of 9 engine
tests fail with `NoSuchMethodError` on `SparkConf.set`; with spark-core first, all pass. This
matches the CI symptoms (compilation fine, the same failure on both JDKs, a fast failure), but
the classpath order in CI was not observed directly.

### Changes
- build.sbt: the Test classpaths of Spark modules drop spark-connect-shims; main code still
  compiles against them (ARCH-2).
- testkit: `LocalSpark.requireSparkCoreClasses()` fails fast, with an actionable message, when a
  placeholder `SparkConf` is on the classpath; `LocalSparkSuite` exercises it.
- Test logging goes to stdout, so sbt no longer reports Spark's warnings at error level.
- CI keeps the sbt output and, on failure, publishes failed tests and sbt `[error]` lines as
  annotations and a job summary (`scripts/ci_failure_report.py`); the public run page shows both.
- LICENSE: MIT, Copyright (c) 2026 Kevin He (D-07). The scripts regain their executable bit.

### Evidence (assistant sandbox; not CI)
The round-1 emulation on 434ce01 plus this change: scalac 2.13.16 with the build's flags compiled
every module, including the linkage compile against scala-library 2.13.16; 33 of 33 tests pass on
OpenJDK 17.0.20 and 21.0.12. Negative control: with the shims ahead of spark-core, the engine
tests fail with the guard's message instead of `NoSuchMethodError`. The failure reporter was run
on synthetic JUnit reports and logs; scalafmt and the API check pass. Not verified: the sbt
classpath filter itself (sbt cannot run in the sandbox) and the workflow changes.

### What to send back
The URL of the next CI run. If it fails, its annotations carry the failing tests and sbt errors,
readable without signing in.

## Round 1 (2026-10-03): repository bootstrap

Delivered as a patch; not merged. Decisions D-01 to D-11 are open, and round 1 used their
recommended options as reversible defaults.

### What the round adds
- An sbt multi-module build (`numerics`, `engine`, `ml`, `app`, `testkit`, `bench`) pinned to
  Spark 4.1.0, Scala 2.13.16 and `-release 17`. Spark modules accept the newer scala-library
  that open-source Spark 4.1.x needs (PLAT-4); `-Dpprof.linkageCheck=true` forces 2.13.16 for a
  compile-only linkage check (PLAT-3).
- CI (`.github/workflows/ci.yml`): engine API rules with a self-test, scalafmt, the linkage
  compile, and tests on JDK 17 and 21 with JUnit reports uploaded.
- `engine`: `SoftwareInfo` and `SparkApi`, the "Software" part of the NN-11 metadata, with a
  Classic/Connect classification for §9.6.
- `testkit`: the shared local Classic session, an ANSI on/off test helper, and the tolerance
  file with its loader.
- Tests: a StrictMath canary (17 golden bit patterns); tolerance rules; `SoftwareInfo`; and a
  Dataset-backend shape test (DIST-3) that requires bitwise-identical results across partition
  counts 1, 3 and 8 and two input orders, with ANSI on and off, plus a negative control.
- `scripts/check-engine-api.sh`, a PR template with the §11.6 checklist, state files, ADR and
  specification templates, and a proposed `reference/REFERENCE.lock`.

### Evidence (assistant sandbox) — not CI evidence
sbt could not run in the sandbox, which has no access to Maven Central. Emulation instead:
- Every source compiled with scalac 2.13.16 and the exact scalacOptions of build.sbt
  (`-Xlint`, `-Wunused`, `-Werror`, `-release 17`) against the Spark 4.1.0 jars of the PySpark
  4.1.0 distribution. `engine` main compiled against spark-sql-api and spark-connect-shims only,
  without spark-core. `engine` and `testkit` main also compiled against scala-library 2.13.16.
- Suites ran under a munit stand-in, in separate JVMs with the build's 22 Java module options:
  32 of 32 tests passed on OpenJDK 17.0.20 and on OpenJDK 21.0.12 (x86-64).
- scalafmt 3.11.5 (native binary) reports every file formatted; the API-check self-test detects
  13 of 13 seeded violations and accepts compliant code.
- The tests found two defects, both fixed: `Tolerance.accepts` accepted any finite value against
  an infinite expected value, and the first negative-control data did not distinguish reduction
  orders (now anchor blocks do so by construction).

Not verified: sbt dependency resolution, the sbt plugins, real munit, and the workflow file.
The first CI run is the evidence for those (OI-18).

### Applying the patch
Create an empty GitHub repository (no README, license or .gitignore), then:

```
git clone <repository-url> pprof_spark && cd pprof_spark
git commit --allow-empty -m "Initial commit" && git push -u origin HEAD:main
git checkout -b phase0/round1-bootstrap
git apply --index ../pprof_spark-round1.patch
git commit -m "Phase 0 round 1: repository bootstrap"
git push -u origin phase0/round1-bootstrap      # open a pull request; CI runs
```

### Next round (2): Databricks deployment path and S-01
Needs from the maintainer: the workspace URL; a Unity Catalog catalog and schema for synthetic
data plus a volume for artifacts; a service principal with a GitHub OIDC federation policy for
this repository, limited to development; permission to create DBR 18 LTS job clusters in
dedicated access mode; a GitHub environment for the deployment workflow.
Plan: bundle definition, JAR upload under an immutable name, a smoke job that prints
`SoftwareInfo`, runs the StrictMath canary and the backend shape test on a multi-node cluster,
and fails if the loaded git SHA differs from the commit under test (§9.6).
