# Status

Updated 2026-10-06, round 6.

**Phase 0, foundations and spikes: in progress.** No phase gate has been passed.

## Phase 0 exit criteria (§4, as amended by D-11 and D-14)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the platform skeleton, under Classic Spark and Spark Connect | Met | Run 37458038911 (`main` at 7f31717): every job passed; the test jobs counted engine 34, numerics 29, testkit 6 |
| Every spike recorded as an ADR | In progress | ADR-0002 (S-02, local part) accepted. S-01, S-03, S-07 and the Databricks legs of S-02 are deferred by D-14 and will be recorded as deferred; S-04, S-05 and S-06 remain |
| Phase 0 decisions resolved | Met | D-01 to D-17 are approved or delegated (2026-10-06) |

## Rounds

| Round | Scope | Status |
|---|---|---|
| 1 | Repository bootstrap | Merged (434ce01); tests failed in CI (OI-21) |
| 1.1 | Classic test-classpath fix, public CI failure details, LICENSE | Merged (0c0bdd8); CI green, run 37325078166 |
| 2 | Engine suites under Spark Connect (layer T8), test counts, ADR-0002 | Merged (c89fcee). Run 37351537662: tests and T8 passed; the linkage compile failed (OI-28) |
| 3 | Maintainer decisions (D-05, D-10, D-11, D-13, D-14); reference pinned to pprof_py v0.7.0; T8 required; linkage fix | Merged (35f5a7c); CI green, run 37361715287 |
| 4 | `numerics`: deterministic summation and the StrictMath rule (ADR-0003) | Merged (08b378f); CI green, run 37361715287 (40 tests per JDK) |
| 5 | Platform skeleton: data contract, layout plan, working set, kernels, ordered reduction, result table, persistence (ADR-0004) | Merged (7f31717); CI green, run 37458038911 |
| 6 | Reference fixtures for Cox (pprof_py v0.7.0, R 4.3.3, survival 3.5-8), calibration, R-comparison triage (ADR-0005) | Delivered as a patch |
| 7 | S-05 (cost of deterministic mode), S-06 (Codespaces), deferred-spike ADRs, Phase 0 gate review | Planned |

## Decisions and spikes

See the table above and DECISIONS.md. S-02's local part is complete; the remaining spikes are
listed under the exit criteria.
