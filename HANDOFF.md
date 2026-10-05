# Handoff

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
