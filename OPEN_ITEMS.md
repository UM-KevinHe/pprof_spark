# Open items

Updated 2026-10-06, round 15. Items come from the round-1 review of PROJECT_CONTEXT v2.0
and from the bootstrap work. "Doc fix" means PROJECT_CONTEXT.md is corrected once the related
decision is approved.

| ID | Item | Next step |
|---|---|---|
| OI-01 | Phase 0 exit requires the walking skeleton, which is a Cox feature gated by NN-2 | Closed by D-11 |
| OI-02 | Reduction volume: a packed p×p partial is about 4 MB at p = 10³, so the 64 MB budget admits about 16 partials; at n = 10⁹ with 16 MB blocks B ≈ 5×10⁵ (outside §2.3), about 2 TB of partials per iteration. EventGrid has the same shape with K-length partials | Propose reduction groups (G consecutive blocks co-located at build time, reduced in block order in the task, G recorded) in the layout ADR before S-07; resolve before D-04 |
| OI-03 | TimeRange: left-truncated rows crossing a block boundary need an entry record in a second block (contradicts §6.5 "exactly one block"); β-dependent offsets mean two passes per iteration (§6.3 targets one) | Cox specification (Phase 1b, with TimeRange); PROJECT_CONTEXT v2.2 §6.6 records the two consequences |
| OI-04 | Scala skew: sbt 1.12.15 documents that `allowUnsafeScalaLibUpgrade` puts the newer scala-library on the compilation classpath; PLAT-4 calls it a test-time library. The §11.3 fragment sets the flag on `engine` only | Done in round 1: linkage compile in CI, flag on every Spark module, munit pinned to 1.2.0. Doc fix done in v2.2 (§5.3, §11.3) |
| OI-05 | spark-sql-api 4.1.0 depends on spark-connect-shims, so `sparkContext`, `rdd` and `localCheckpoint` compile against it (sandbox probe, round 1) | Done in round 1: `scripts/check-engine-api.sh` with self-test. Later: semantic scalafix rule (OI-15). Doc fix done in v2.2 (§5.4) |
| OI-06 | DBR 18 ships dated updates under one version number; clusters pick them up on restart | Deferred by D-14 (no Databricks work); `SoftwareInfo` already records the identifiers that exist |
| OI-07 | `Double.toString` output differs between JDK 17 and 21, so `metadata.json` is not byte-stable across JDKs | Done for the skeleton's format (doubles stored as raw bits, ADR-0004); every persisted model keeps the rule |
| OI-08 | DIST-6 canonical order: −0.0 and +0.0 compare equal, so input order would decide their position | Done: `BlockBuilder` normalizes negative zeros before ordering (ADR-0004) |
| OI-09 | Tolerance semantics: "relative, scaled by magnitude" (T-fn) and "absolute, scaled" (T-res) | Defined for vectors by D-09 (scaled by the quantity's largest element); T-res is fixed with the residuals specification (Phase 1c) |
| OI-10 | §8.1 lists exp, log, log1p, expm1; `pow` is missing | Round 4: `scripts/check-engine-api.sh` bans `math`/`Math` transcendental functions in `numerics` and `engine` main code; doc fix done in v2.2 (§8.1) |
| OI-11 | §12 names survival (LGPL) and EmpiNull (GPL-3) only; glmnet and lme4 are GPL too | Done in v2.2 (§12) |
| OI-12 | pprof_py's R-comparison suite has 26 documented failures and does not run in upstream CI | Closed: see X-007 and X-008 |
| OI-13 | pprof_py's README says the lme4 comparison script is not in the repository | Relevant to PAR-2 for random-effect models (later phases) |
| OI-14 | Second CI time zone (§5.5) | Add with the first time-handling code (data contract) |
| OI-15 | Deferred tooling: scalafix (with a semantic ARCH-2 rule), scoverage, MiMa from 1.0, Scala Steward or Dependabot (keep the munit pin), parity-matrix CI check (§9.8) | Schedule per round |
| OI-16 | §8.4 names `testkit/tolerances.conf`; the file is the classpath resource `testkit/src/main/resources/tolerances.conf` | Done in v2.2 (§8.4) |
| OI-17 | `numerics` tests cannot use `testkit`, which brings Spark and scala-library 2.13.17 | Spark-free `testkit-core` when `numerics` needs tolerance assertions (round 4) |
| OI-18 | sbt evidence | Closed: run 37325078166 passed every job, tests included, on JDK 17 and 21 |
| OI-19 | Maintainer inputs for Databricks work | Closed by D-14 |
| OI-21 | spark-sql-api depends on spark-connect-shims; Spark's Classic modules (spark-catalyst, spark-sql) exclude it, but `engine` declares spark-sql-api directly, so its Test classpath held the shims and spark-core. With the shims first, 7 of 9 engine tests fail with `NoSuchMethodError` (sandbox) | Fixed in round 1.1 and confirmed by CI (run 37325078166). Remaining: treat any Classic run or assembly classpath built by sbt the same way |
| OI-22 | Workflow logs require signing in, so the assistant cannot read CI failures | Done: failure details since round 1.1; per-module test counts as a notice on every run since round 2 |
| OI-23 | The initial commit stored the scripts without the executable bit | Harmless (CI invokes them through bash and python); round 1.1 restores mode 100755 |
| OI-24 | The Connect client uber jar overlaps 1,228 spark-sql-api classes (4 differ) and 1,883 Connect-server classes (1,713 differ). In one JVM the client must come last: with it first, the in-process server fails and 7 of 9 engine tests fail (sandbox) | Pinned in build.sbt (ADR-0002); re-check on every Spark upgrade |
| OI-25 | T8 runs Classic and Connect in one JVM, so it cannot catch client-side use of Classic-only classes | Covered statically by ARCH-2 and the API check, at run time by the Databricks legs of S-02 (round 3); a separate server JVM is the fallback |
| OI-26 | T8 runs in ci.yml on every push and pull request; §11.4 plans a nightly `connect.yml` | Required since round 3 (ADR-0002 accepted); doc fix done in v2.2 (§9.1, §11.4) |
| OI-27 | PROJECT_CONTEXT still describes Databricks deployment and the original Phase 0 exit in its body | Closed: PROJECT_CONTEXT v2.2 (round 8) folds in D-01 to D-19 and the doc fixes; the claude.ai project copy should be replaced with `docs/PROJECT_CONTEXT.md` |
| OI-28 | Run 37351537662: the linkage compile failed because spark-connect-client-jvm depends on scala-compiler 2.13.17, and sbt keeps scala-library, scala-reflect and scala-compiler at one version | Closed: fixed in round 3, green in run 37361715287 |
| OI-29 | m-scale results (group sizes, group tables) are collected to the driver under `BlockOptions` guards | Distributed result tables for m beyond the driver budget, when a model needs them |
| OI-30 | Groups larger than a block get a block of their own; nothing splits them | TimeRange (OI-03) before the large-stratum Cox work |
| OI-31 | DIST-1's naming rule (`collect…` and `toLocal…` methods) is enforced by review only | Consider a source check once more models exist |
| OI-32 | Fixture families for later phases | Round 14 added left truncation (lt-stratified, lt-weights-offset), baseline hazards and predictions for all six cases; residuals, robust variance and provider measures remain |
| OI-33 | Upstream reports to pprof_py: the README's failure count (X-008) and the step-control order (X-010) | Maintainer |
| OI-34 | Kernels with several sums per row need a fused pairwise cascade: one walk over rows, the same addition tree as `Summation.pairwise` per statistic (ADR-0006) | Reusable form in `numerics` with the first Cox kernel |
| OI-35 | Lockstep parity needs pprof_py's Newton iterates in the fixtures | Closed: round 10 records `CoxPH(max_iter = k)`, k = 1 to 5, and pprof_py's z, p and interval outputs in the fixtures; `CoxPHSuite` uses them |
| OI-36 | Fit warnings (non-convergence) are collected in `CoxFit.warnings` but not yet logged | Closed in round 12: `CoxPH` logs each warning through slf4j (a dependency of Spark's common utilities, which spark-sql-api brings) |
| OI-37 | `CoxFit` has no persistence yet (PERS-1) | Closed in round 12: `CoxFitIO` saves and loads bit for bit under Classic and Connect |
| OI-39 | Spark Connect rejects an aggregate whose output has two columns with the same generated name; Classic Spark accepts it. Found in round 11, where two validation counts shared an expression (15 engine tests failed under T8 only) | Closed: `CoxValidation` aliases every count. Rule for new code: alias every column of a multi-column aggregate |
| OI-40 | §9.8: CI must fail when a public API has no parity-matrix entry; no such check exists | Add a check (for example over `engine` public classes against `docs/parity/matrix.md`) before the first release |
| OI-41 | PERS-2: models persisted by each release are kept as test fixtures | Start with release 0.1.0: save one `CoxFit` per tie method into `fixtures/models/` and load them in CI |
| OI-42 | pprof_py v0.7.0 raised "Empty risk set at an event time with positive event weight" on a five-row right-censored dataset with one zero-weight tied event (round 13 probe) | Reproduce, add to X-013's evidence, and report upstream with OI-33 |
| OI-43 | Prediction's as-of join partitions by stratum, so unstratified prediction puts every row in one window partition | Measure before Phase 1d; if needed, bucket by time range or broadcast a small baseline |
| OI-38 | Breslow kernel cost: Neumaier running sums on S₂ add about four flops per entry | Measure with the S-05 benchmark harness at p = 10 to 100 before large-p work (D-04) |
| OI-20 | Re-verification (§3.4, §5.1): Spark/Scala pairs, DBR 18 facts and pprof_py README claims checked 2026-10-03; §3.4 rows await the pin | DISCREPANCIES.md after D-05 |
