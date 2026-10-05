# Decisions

Sequentially numbered and never renumbered (PROJECT_CONTEXT §0). Status values: **Proposed**
(awaiting the maintainer), **Approved**, **Rejected**, **Superseded**, and **Delegated**
(implementation choices under delegated authority, §11.6, which the maintainer may reverse).
Architecture decisions also get an ADR in docs/adr/.

Round 1 used the recommended option of every proposed decision as a provisional default. Nothing
in the bootstrap is hard to reverse.

## Phase 0 decisions (§16)

| ID | Decision | Status | Recommendation |
|---|---|---|---|
| D-01 | Supported compute tiers for v1 | Proposed | Tier 1 only; `engine` kept Connect-compatible (PLAT-2) |
| D-02 | Target and secondary runtimes | Proposed | DBR 18 LTS, DBR 19 as a non-blocking canary; record the exact runtime build, because DBR 18 receives dated updates (OI-06) |
| D-03 | Python access in v1 | Proposed | Job-based access; py4j wrappers deferred |
| D-04 | Design envelope and performance targets | Proposed | §2.3 ranges once the reduction volume at large p is resolved (OI-02); targets after the first benchmarks |
| D-05 | pprof_py pin and reference versions | Proposed | v0.5.0, commit d3d92a1, by SHA (evidence below) |
| D-06 | Default Cox tie method | Proposed | Breslow, the reference default |
| D-07 | Project license | Approved 2026-10-05 | MIT, copyright holder Kevin He (LICENSE added in round 1.1) |
| D-08 | Artifact distribution | Proposed | GitHub Releases plus a Unity Catalog volume |
| D-09 | Tolerance calibration | Proposed | §8.4 initial values (`testkit/src/main/resources/tolerances.conf`), after the scaling rules are defined (OI-09) |
| D-10 | Root package and artifact names | Proposed | Provisionally organization and root package `pprof.spark`, artifacts `pprof-spark-<module>_2.13`; renaming is mechanical now and costly later |

### D-05 evidence (checked 2026-10-03)

- Tags are `v0.1-legacy` and `v0.5.0` (commit d3d92a1faedd4acc3c987084599f7fac324e5ca5,
  2026-09-29). `main` is 9320766e35e5b385d596a2b75418192098a124c4 (2026-10-03), pyproject version
  0.7.0. The "V0.6.0 release" commit (5c17633) and version 0.7.0 are untagged. No PyPI release.
- 37 commits separate v0.5.0 from `main`, mostly plotting. Nothing under `pprof_py/models/survival`
  changed. Changes touch `inference/survival/provider_tests.py`, `inference/standardized.py`, a
  new `inference/funnel.py`, logistic inference, and data preparation.
- The README at both commits documents Cox validation against R 4.3.3 and survival 3.5.8, says
  the R-comparison tests need generated data plus a symlink, and reports that a fresh run ends
  with 200 passed, 26 failed and 1 skipped, the failures being explained in R_COMPATIBILITY.md.
  The upstream CI workflow (added after v0.5.0) runs pytest without R.
- License: MIT, "Copyright (c) 2025 Kevin He".

Recommendation: pin v0.5.0 by SHA for Phase 0 to 1c, since the Cox model code is identical to
`main`; ask upstream to tag 0.6.0 and 0.7.0; re-pin under PAR-1 before Phase 1d. Whether the
validation suite passes for Cox features must be established by our fixture workflow (OI-12).

## Decisions raised in round 1

### D-11: Phase 0 exit criterion and the first Cox slice (Proposed)

Phase 0 exits on "CI green on the walking skeleton", but ROAD-1 defines the skeleton as a Cox
model, which NN-2 gates on an approved specification, a pinned reference and fixtures: Phase 1a
work.

Proposal: (a) Phase 0 exits on a statistics-free platform skeleton: data contract, logical
blocks, a toy kernel, block-ordered reduction, a result table and persisted metadata, run from
the CI-deployed JAR on a DBR 18 LTS job cluster. (b) The first Cox slice opens Phase 1a and is
stratified (StratumLocal), right-censored, Breslow, with model-based variance; unstratified data
is the one-stratum case while it fits in one block, and TimeRange follows within 1a or 1b.
Stratified first exercises multi-block reduction and bin packing, matches SMR stage 1, and avoids
starting with the hardest layout (OI-03).

### D-12: Build and test tooling baseline (Delegated; reversible)

sbt 1.12.15 (1.13.0 exists but has no patch release yet); sbt-scalafmt 2.6.2 with scalafmt
3.11.5; sbt-buildinfo 0.13.2; munit 1.2.0, the newest release built with Scala 2.13.16 (later
releases pull scala-library 2.13.17 or 2.13.18, OI-04). GitHub Actions: checkout v7, setup-java
v6 (Temurin), setup-sbt v1, upload-artifact v7, runner ubuntu-24.04. Deferred: OI-15.
