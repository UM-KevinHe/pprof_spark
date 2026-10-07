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
