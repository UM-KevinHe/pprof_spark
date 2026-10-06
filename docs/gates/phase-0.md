# Phase 0 gate review (ROAD-2)

- Date: 2026-10-06
- Recommendation: close Phase 0 when CI passes on round 7. On 2026-10-06 the maintainer delegated
  open decisions to the assistant's recommendations.

## Exit criteria (§4, as amended by D-11 and D-14)

| Criterion | Status | Evidence |
|---|---|---|
| CI green on the platform skeleton, under Classic Spark and Spark Connect | Met | Run 37458038911 (`main` at 7f31717); still green with the fixtures, run 37477285324 (0be6e54) |
| Every spike recorded as an ADR | Met with round 7 | S-02 local part: ADR-0002; S-05: ADR-0006; S-06: ADR-0007 (trial pending); S-01, S-02 Databricks legs, S-03, S-04, S-07: ADR-0008 |
| Phase 0 decisions resolved | Met | D-01 to D-19 in DECISIONS.md |

## Phase 0 scope

| Item | Outcome |
|---|---|
| Repository, multi-module build, CI | `ci.yml` (API rules, formatting, linkage compile, tests on JDK 17 and 21, Spark Connect), `fixtures.yml`, `bench.yml` |
| Test harness | Classic and Spark Connect sessions, calibrated tolerances, fixture reader, public CI failure details and test counts |
| Fixture pipeline | Cox fixtures from pprof_py v0.7.0 and R 4.3.3 with survival 3.5-8 (ADR-0005) |
| `numerics` skeleton | Deterministic summation (ADR-0003) and the moments kernel; Cholesky, distributions and step control arrive with the first Cox slice |
| Backend skeleton | Data contract, layout plan, working set, ordered reduction, results and persistence (ADR-0004) |
| Databricks deployment path | Deferred by D-14 |

## Discrepancies
No class A or B discrepancy affects Phase 0 features (ROAD-2). X-010 (class B, Cox step control) is
decided as bug-compatible and enters Phase 1a with the Cox specification.

## Carried into Phase 1
OI-02, OI-03 and OI-30 (layouts at scale), OI-15 (tooling), OI-27 (PROJECT_CONTEXT revision),
OI-29 (distributed result tables), OI-31 (DIST-1 naming check), OI-32 (fixture families), OI-33
(upstream reports), OI-34 (fused cascade).

## Phase 1a plan
1. PROJECT_CONTEXT v2.2: fold in the decisions and doc fixes (OI-27).
2. Cox specification for the first slice (D-11): stratified, right-censored, Breslow, model-based
   variance, mapped to pprof_py v0.7.0 and R, including X-010 (NN-2; approval required).
3. `numerics`: Cholesky with the reference's singularity rule, Newton step control as pprof_py
   does it (X-010), and the fused cascade (OI-34).
4. The Cox kernel on StratumLocal blocks, with function-level, lockstep and end-to-end parity
   against the fixtures; then Efron ties, case weights and offsets.
