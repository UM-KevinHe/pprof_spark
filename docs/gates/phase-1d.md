# Phase 1d gate review: provider workflows

- Date: 2026-10-07 (round 23). Reviewer: the assistant; sign-off: the maintainer (D-29).
- Exit criteria (§4): the parity gate passed for two-stage SMR and SHR, expected counts, O/E
  ratios, exact Poisson intervals and tests, flags, provider result tables and a job-runner entry
  point; and an end-to-end Spark application at design-envelope scale.
- Specification: [provider-workflows.md](../spec/cox/provider-workflows.md) (D-27). Guide:
  [cox-job.md](../guide/cox-job.md).

## Evidence

| Requirement | Status | Evidence |
|---|---|---|
| Standardized measures (1d-1) | Met | `CoxMeasuresSuite`: indirect and direct measures for six fixtures and both tie methods, two-stage and pooled, against pprof_py at its estimates; worst T-base ratios 1.2e-7 (indirect ratio) and 1.6e-6 (direct); person-time exact; Σⱼ Eⱼ = O |
| Provider tests (1d-2) | Met | `CoxProviderTestsSuite`: mid-p and exact for every fixture and tie method; flags equal pprof_py's; worst ratios 2.1e-6 of T-test for z and 4.5e-7 of T-base for limits; p-values under T-p, down to 1.4e-25. `PoissonSuite`: log-gamma, incomplete gamma, Poisson and χ² functions and the tests against mpmath, scipy and pprof_py |
| Job runner (1d-3) | Met | `CoxJobSuite`: every output of `CoxJob` equals the library's bit for bit, under Classic Spark and Spark Connect; invalid specifications list every problem; outputs are never overwritten |
| API-3, DIST-1, NN-10 | Met | Measures and tests check the data fingerprint; provider tables stay distributed; the national baseline is K-scale and guarded; providers without expected events are logged |
| Reproducibility | Met | Deterministic reductions (time-keyed partials in block order, provider-local blocks); Classic and Spark Connect agree |
| Documentation | Met | Specification, user guide, parity matrix; PROJECT_CONTEXT v2.3 |
| End-to-end at design-envelope scale (1d-4) | **Deferred** | The maintainer runs it on his Databricks workspace once the whole package is done (D-28) |
| CI | Met | Round 22 green, reported by the maintainer; it covers `main` with rounds 20 and 21 |

## Outcome

**Closed on 2026-10-07** (D-29): the maintainer reported round 22's CI green and signed off. With
Phases 1a to 1c, this completes Phase 1; every Cox feature is parity-verified and `Experimental`
until the package-level scale test (D-28).

## Open items carried forward

OI-46 (measures with a provider other than the strata need an extra shuffle for the fingerprint),
OI-43 (unstratified prediction), OI-45 (clustered robust variance), OI-03 (TimeRange for strata
above `maxStratumRows`), OI-02 and OI-38 (large p), OI-40 (CI check of the parity matrix), OI-41
(per-release model fixtures), OI-33 and OI-42 (upstream reports).

## Recommendation

Close Phase 1d at parity-verified once CI for round 22 is green, on the terms of D-22, D-24 and
D-26, with the scale test at the package level (D-28) (D-29, needs the maintainer's sign-off).
Phase 2 (logistic provider models) then starts with its specification.
