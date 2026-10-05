# Status

Updated 2026-10-05, round 1.1.

**Phase 0, foundations and spikes: in progress.** No phase gate has been passed.

## Phase 0 exit criteria (PROJECT_CONTEXT §4)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the walking skeleton | Not met | Run 37318035767 (round 1, `main` at 434ce01): checks job passed, both test jobs failed; round 1.1 awaits CI. Skeleton scope awaits D-11 |
| A JAR deployed by CI runs on a DBR 18 LTS job cluster | Not started | Round 2 (S-01) |
| Every spike recorded as an ADR | Not started | — |

## Rounds

| Round | Scope | Status |
|---|---|---|
| 1 | Repository bootstrap: build, CI, test harness, `SoftwareInfo`, platform canaries, state files | On `main` (434ce01). CI: API rules, formatting and linkage compile passed; tests failed on JDK 17 and 21 |
| 1.1 | Test-classpath fix (spark-connect-shims), fail-fast guard, public CI failure details, LICENSE | Delivered as a patch; awaiting CI |
| 2 | Databricks deployment path: bundle, OIDC, Unity Catalog volume, smoke job on DBR 18 LTS (S-01) | Planned; needs the prerequisites in HANDOFF.md |
| 3 | Spark Connect suite (S-02, layer T8); cost of deterministic mode (S-05) | Planned |
| 4 | `numerics` core: summation, Cholesky with aliasing report, StrictMath lint rules | Planned |
| 5 | Fixture pipeline; pin the reference (D-05); calibrate tolerances (D-09) | Planned |
| 6 | Working set, StratumLocal layout, deterministic reduction; platform skeleton on DBR; S-03, S-07 | Planned |
| — | S-04 (Spark Connect ML) and S-06 (Codespaces) | Alongside rounds 3–6 |

## Decisions and spikes

D-07 is approved (MIT, copyright holder Kevin He). D-01 to D-06 and D-08 to D-11 remain
proposed; round 1 used their recommended options as reversible defaults. D-12 records the
delegated tooling baseline. Spikes S-01 to S-07 have not started.
