# Handoff

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
