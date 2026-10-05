# Status

Updated 2026-10-05, round 3.

**Phase 0, foundations and spikes: in progress.** No phase gate has been passed.

## Phase 0 exit criteria (§4, as amended by D-11 and D-14)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the platform skeleton, under Classic Spark and Spark Connect | Not met | The bootstrap and the T8 harness pass in CI (run 37351537662); the skeleton is round 5 |
| Every spike recorded as an ADR | In progress | ADR-0002 (S-02, local part) accepted. S-01, S-03, S-07 and the Databricks legs of S-02 are deferred by D-14 and will be recorded as deferred; S-04, S-05 and S-06 remain |
| Phase 0 decisions resolved | In progress | D-05, D-07, D-10, D-11, D-13 and D-14 approved; D-01 to D-03 and D-08 partly deferred by D-14; D-04, D-06 and D-09 open |

## Rounds

| Round | Scope | Status |
|---|---|---|
| 1 | Repository bootstrap | Merged (434ce01); tests failed in CI (OI-21) |
| 1.1 | Classic test-classpath fix, public CI failure details, LICENSE | Merged (0c0bdd8); CI green, run 37325078166 |
| 2 | Engine suites under Spark Connect (layer T8), test counts, ADR-0002 | Merged (c89fcee). Run 37351537662: tests and T8 passed; the linkage compile failed (OI-28) |
| 3 | Maintainer decisions (D-05, D-10, D-11, D-13, D-14); reference pinned to pprof_py v0.7.0; T8 required; linkage fix | Delivered as a patch |
| 4 | `numerics`: deterministic summation and the StrictMath rule (ADR-0003) | Delivered as a patch, to apply after round 3 |
| 5 | Platform skeleton: working set, logical blocks, block-ordered reduction, result table, persisted metadata | Planned |
| 6 | Fixture pipeline: pprof_py v0.7.0 and R reference generators, manifests, R-comparison triage (OI-12); tolerance calibration (D-09) | Planned |
| 7 | S-05 (cost of deterministic mode), S-06 (Codespaces), deferred-spike ADRs, Phase 0 gate review | Planned |

## Decisions and spikes

See the table above and DECISIONS.md. S-02's local part is complete; the remaining spikes are
listed under the exit criteria.
