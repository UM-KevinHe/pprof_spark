# Discrepancy register (PROJECT_CONTEXT §3.5)

No difference between pprof_spark and the reference has been observed: no model feature exists
yet. New entries take the next X-number and record the class (A to E), evidence, decision and
approval.

## Observed

| ID | Feature | Observation | Class | Evidence | Decision | Status |
|---|---|---|---|---|---|---|
| — | — | None yet | — | — | — | — |

## Known reference limitations to verify at the pinned commit (§3.4)

Classes are proposals. Each row is re-verified and finalized after D-05 pins the reference.

| ID | Reference behavior | Proposed handling | Proposed class |
|---|---|---|---|
| X-001 | `CoxPH(fit_intercept=True)` fits, but every `predict_*` method raises | Reject an intercept at validation | C |
| X-002 | `CoxPH` does not warn when `max_iter` is reached | Report non-convergence (NN-10); estimates unchanged | C |
| X-003 | `FineGrayPH` with left truncation differs from R `finegray()` (about 3e-3 in coefficients) | Choose the parity target before the competing-risks phase | B |
| X-004 | SerBIN can stop at a near-null fit when covariates are far from zero (upstream C27) | Center internally; confirm with a dedicated fixture | B |
| X-005 | `LogisticThreeStageModel.sigma_sensitivity()` fails when σ̂ = 0 | Define the boundary in the three-stage specification | B |
| X-006 | Unresolved differences with internal R code (`IUR.fac`, `cal_SMR_pro_adj`) | Not parity-gated until resolved | B (provisional) |
| X-007 | R-comparison suite: 26 documented failures | Only passing, documented features become parity targets (OI-12) | Triage |

Not discrepancies: `ties="exact"` is out of scope (§2.5); the Breslow default follows the
reference, every fit records its tie method, and R fixtures set `ties` explicitly.
