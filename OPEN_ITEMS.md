# Open items

Updated 2026-10-05, round 3. Items come from the round-1 review of PROJECT_CONTEXT v2.0
and from the bootstrap work. "Doc fix" means PROJECT_CONTEXT.md is corrected once the related
decision is approved.

| ID | Item | Next step |
|---|---|---|
| OI-01 | Phase 0 exit requires the walking skeleton, which is a Cox feature gated by NN-2 | Closed by D-11 |
| OI-02 | Reduction volume: a packed p×p partial is about 4 MB at p = 10³, so the 64 MB budget admits about 16 partials; at n = 10⁹ with 16 MB blocks B ≈ 5×10⁵ (outside §2.3), about 2 TB of partials per iteration. EventGrid has the same shape with K-length partials | Propose reduction groups (G consecutive blocks co-located at build time, reduced in block order in the task, G recorded) in the layout ADR before S-07; resolve before D-04 |
| OI-03 | TimeRange: left-truncated rows crossing a block boundary need an entry record in a second block (contradicts §6.5 "exactly one block"); β-dependent offsets mean two passes per iteration (§6.3 targets one) | Cox specification; doc fix |
| OI-04 | Scala skew: sbt 1.12.15 documents that `allowUnsafeScalaLibUpgrade` puts the newer scala-library on the compilation classpath; PLAT-4 calls it a test-time library. The §11.3 fragment sets the flag on `engine` only | Done in round 1: linkage compile in CI, flag on every Spark module, munit pinned to 1.2.0. Doc fix to PLAT-4 and §11.3 |
| OI-05 | spark-sql-api 4.1.0 depends on spark-connect-shims, so `sparkContext`, `rdd` and `localCheckpoint` compile against it (sandbox probe, round 1) | Done in round 1: `scripts/check-engine-api.sh` with self-test. Later: semantic scalafix rule (OI-15). Doc fix to §5.4 and ARCH-2 |
| OI-06 | DBR 18 ships dated updates under one version number; clusters pick them up on restart | Deferred by D-14 (no Databricks work); `SoftwareInfo` already records the identifiers that exist |
| OI-07 | `Double.toString` output differs between JDK 17 and 21, so `metadata.json` is not byte-stable across JDKs | Persistence specification: store raw bits or hex next to decimals |
| OI-08 | DIST-6 canonical order: −0.0 and +0.0 compare equal, so input order would decide their position | Normalize −0.0 to +0.0 at encoding (working-set round) |
| OI-09 | Tolerance semantics: "relative, scaled by magnitude" (T-fn) and "absolute, scaled" (T-res) are undefined | Define before D-09 calibration |
| OI-10 | §8.1 lists exp, log, log1p, expm1; `pow` is missing | Round 4: `scripts/check-engine-api.sh` bans `math`/`Math` transcendental functions in `numerics` and `engine` main code; doc fix remains (OI-27) |
| OI-11 | §12 names survival (LGPL) and EmpiNull (GPL-3) only; glmnet and lme4 are GPL too | Doc fix: "run, never port" applies to every copyleft fixture tool |
| OI-12 | pprof_py's R-comparison suite has 26 documented failures and does not run in upstream CI | Fixture round: run it at the candidate pin and map each failure to a feature (D-05) |
| OI-13 | pprof_py's README says the lme4 comparison script is not in the repository | Relevant to PAR-2 for random-effect models (later phases) |
| OI-14 | Second CI time zone (§5.5) | Add with the first time-handling code (data contract) |
| OI-15 | Deferred tooling: scalafix (with a semantic ARCH-2 rule), scoverage, MiMa from 1.0, Scala Steward or Dependabot (keep the munit pin), parity-matrix CI check (§9.8), Codespaces devcontainer (S-06) | Schedule per round |
| OI-16 | §8.4 names `testkit/tolerances.conf`; the file is the classpath resource `testkit/src/main/resources/tolerances.conf` | Doc fix |
| OI-17 | `numerics` tests cannot use `testkit`, which brings Spark and scala-library 2.13.17 | Spark-free `testkit-core` when `numerics` needs tolerance assertions (round 4) |
| OI-18 | sbt evidence | Closed: run 37325078166 passed every job, tests included, on JDK 17 and 21 |
| OI-19 | Maintainer inputs for Databricks work | Closed by D-14 |
| OI-21 | spark-sql-api depends on spark-connect-shims; Spark's Classic modules (spark-catalyst, spark-sql) exclude it, but `engine` declares spark-sql-api directly, so its Test classpath held the shims and spark-core. With the shims first, 7 of 9 engine tests fail with `NoSuchMethodError` (sandbox) | Fixed in round 1.1 and confirmed by CI (run 37325078166). Remaining: treat any Classic run or assembly classpath built by sbt the same way |
| OI-22 | Workflow logs require signing in, so the assistant cannot read CI failures | Done: failure details since round 1.1; per-module test counts as a notice on every run since round 2 |
| OI-23 | The initial commit stored the scripts without the executable bit | Harmless (CI invokes them through bash and python); round 1.1 restores mode 100755 |
| OI-24 | The Connect client uber jar overlaps 1,228 spark-sql-api classes (4 differ) and 1,883 Connect-server classes (1,713 differ). In one JVM the client must come last: with it first, the in-process server fails and 7 of 9 engine tests fail (sandbox) | Pinned in build.sbt (ADR-0002); re-check on every Spark upgrade |
| OI-25 | T8 runs Classic and Connect in one JVM, so it cannot catch client-side use of Classic-only classes | Covered statically by ARCH-2 and the API check, at run time by the Databricks legs of S-02 (round 3); a separate server JVM is the fallback |
| OI-26 | T8 runs in ci.yml on every push and pull request; §11.4 plans a nightly `connect.yml` | Required since round 3 (ADR-0002 accepted); doc fix to §11.4 |
| OI-27 | PROJECT_CONTEXT still describes Databricks deployment and the original Phase 0 exit in its body | v2.1 preface lists the amendments in force; full revision folding in D-05, D-07, D-10, D-11, D-13, D-14 and the doc fixes, at the Phase 0 gate review (round 7) |
| OI-28 | Run 37351537662: the linkage compile failed because spark-connect-client-jvm depends on scala-compiler 2.13.17, and sbt keeps scala-library, scala-reflect and scala-compiler at one version | Round 3 forces all three to 2.13.16 in linkage mode; confirm in CI |
| OI-20 | Re-verification (§3.4, §5.1): Spark/Scala pairs, DBR 18 facts and pprof_py README claims checked 2026-10-03; §3.4 rows await the pin | DISCREPANCIES.md after D-05 |
