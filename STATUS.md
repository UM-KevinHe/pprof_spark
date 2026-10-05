# Status

Updated 2026-10-05, round 2.

**Phase 0, foundations and spikes: in progress.** No phase gate has been passed.

## Phase 0 exit criteria (PROJECT_CONTEXT §4)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the walking skeleton | Not met | The bootstrap is green (run 37325078166, `main` at 0c0bdd8: every job passed on JDK 17 and 21); the skeleton awaits D-11 |
| A JAR deployed by CI runs on a DBR 18 LTS job cluster | Not started | Round 3 (S-01), blocked on the Databricks prerequisites in HANDOFF.md |
| Every spike recorded as an ADR | In progress | ADR-0002 (S-02, local part) proposed |

## Rounds

| Round | Scope | Status |
|---|---|---|
| 1 | Repository bootstrap | Merged (434ce01); its tests failed in CI (OI-21) |
| 1.1 | Classic test-classpath fix, public CI failure details, LICENSE | Merged (0c0bdd8); CI green, run 37325078166 |
| 2 | S-02 local part: engine suites under Spark Connect (layer T8), ADR-0002; public test counts | Delivered as a patch; awaiting CI |
| 3 | Databricks deployment path: bundle, OIDC, Unity Catalog volume, smoke job on DBR 18 LTS (S-01); S-02 standard and serverless legs | Blocked on the prerequisites in HANDOFF.md |
| 4 | Cost of deterministic mode (S-05) | Planned |
| 5 | `numerics` core: summation, Cholesky with aliasing report, StrictMath lint rules | Planned |
| 6 | Fixture pipeline; pin the reference (D-05); calibrate tolerances (D-09) | Planned |
| 7 | Working set, StratumLocal layout, deterministic reduction; platform skeleton on DBR; S-03, S-07 | Planned |
| — | S-04 (Spark Connect ML) and S-06 (Codespaces) | Alongside later rounds |

## Decisions and spikes

D-07 is approved. D-01 to D-06, D-08 to D-11 and D-13 are proposed; D-12 is delegated. S-02 is in
progress (local part awaiting CI; Databricks legs open). The other spikes have not started.
