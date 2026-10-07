# Phase 1c gate review: residuals and robust variance

- Date: 2026-10-06 (round 18). Reviewer: the assistant; sign-off: the maintainer (D-26).
- Exit criterion (§13): the parity gate passed for martingale residuals (correct under left
  truncation and Efron ties), score and dfbeta residuals, and robust sandwich and clustered variance.
- Specification: [residuals-robust.md](../spec/cox/residuals-robust.md) (D-25, X-015).

## Evidence

| Requirement (§9.8) | Status | Evidence |
|---|---|---|
| Residual parity | Met | `CoxResidualsSuite`: martingale, score and dfbeta within T-res of pprof_py and R at each reference's estimates, six fixtures, both tie methods, including Efron with left truncation (round 17) |
| Robust variance parity | Met | `CoxRobustSuite`: per-row and clustered (40 clusters) within T-var of R everywhere and of pprof_py except Breslow on (start, stop] data, where the sandwich of pprof_py's dfbeta residuals is the reference (X-015) |
| Identities | Met | Weighted martingale residuals sum to zero per stratum, weighted score residuals to the score; clusters of one row equal per-row robust variance; renamed clusters change nothing |
| Reproducibility | Met | Clustered robust variance bitwise identical across row orders and partition counts (256 fixed buckets); Classic and Spark Connect |
| API-3, DIST-1, NN-10 | Met | Residuals check the fingerprint and stay distributed; per-row robust variance on data with entry times records a warning |
| Persistence | Met | Format version 3 keeps the robust and model-based covariances, the cluster column and the cluster count; versions 1 and 2 load as model-based |
| Documentation | Met | Specification, parity matrix, X-015 |
| Scale test | **Not met** | As in Phases 1a and 1b; the clustered variance shuffles one p-vector per row |
| CI | Met: round 18 green | Sandbox: JDK 17 Classic (numerics 54, testkit 11, engine 187) and Spark Connect for the robust, persistence and CoxPH suites |

## Outcome

**Closed on 2026-10-06** (D-26): round 18's CI passed and the maintainer signed off.

## Recommendation

Close Phase 1c at parity-verified on the terms of D-22 and D-24 once round 18's CI passes: the
features stay `Experimental`, and scale verification moves to the Phase 1d gate (D-26, needs the
maintainer's sign-off).
