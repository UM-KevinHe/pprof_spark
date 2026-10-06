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
| D-03 | Python access in v1 | Approved (delegated), 2026-10-06 | None in v1; revisit after Phase 1d (job-based access was deferred by D-14) |
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

### D-12: Build and test tooling baseline (Delegated; reversible)

sbt 1.12.15 (1.13.0 exists but has no patch release yet); sbt-scalafmt 2.6.2 with scalafmt
3.11.5; sbt-buildinfo 0.13.2; munit 1.2.0, the newest release built with Scala 2.13.16 (later
releases pull scala-library 2.13.17 or 2.13.18, OI-04). GitHub Actions: checkout v7, setup-java
v6 (Temurin), setup-sbt v1, upload-artifact v7, runner ubuntu-24.04. Deferred: OI-15.
