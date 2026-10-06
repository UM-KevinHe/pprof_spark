# Discrepancy register (PROJECT_CONTEXT §3.5)

No difference between pprof_spark and the reference has been observed: no model feature exists
yet. New entries take the next X-number and record the class (A to E), evidence, decision and
approval.

## Observed

| ID | Feature | Observation | Class | Evidence | Decision | Status |
|---|---|---|---|---|---|---|
| — | — | None yet | — | — | — | — |

## Registered during the fixture round (2026-10-06)

| ID | Observation | Evidence | Class | Decision |
|---|---|---|---|---|
| X-008 | pprof_py's README says a fresh R-comparison run ends with 26 failures; at v0.7.0 none of the survival tests fail | X-007 run | E | Follow the behavior; ask upstream to correct the README (OI-33) |
| X-009 | With non-integer case weights, R's coxph reports a robust variance by default; pprof_py's default, and pprof_spark's, is model-based | `fit$naive.var` equals pprof_py's covariance; the reported variance does not | — (not a pprof_spark difference) | Fixtures call coxph with `robust = FALSE`; the user guide will tell R users to do the same |
| X-010 | pprof_py tests for a log-likelihood decrease, and halves the step, before testing convergence; R's coxfit6 tests convergence first and keeps the full step. When a converged step lowers the log-likelihood at rounding level, the two return estimates half a step apart. R's later halvings also follow a different schedule | Iterate trace on tiny-ties: identical to 1e-16 for two iterations, then R's third step is twice pprof_py's; coxfit6.c lines 249-276. Converged coefficients differ by 1.05e-8 relative on tiny-ties (Breslow), at most 2.4e-9 elsewhere | B | Bug-compatible: pprof_spark follows pprof_py's step control, flagged in the Cox specification, and the difference is reported upstream (OI-33). Approved (delegated), 2026-10-06 |

## Known reference limitations to verify at the pinned commit (§3.4)

Classes are proposals. Each row is re-verified against the pinned reference, pprof_py v0.7.0
(commit 9320766), in the fixture round.

| ID | Reference behavior | Proposed handling | Proposed class |
|---|---|---|---|
| X-001 | `CoxPH(fit_intercept=True)` fits, but every `predict_*` method raises | Reject an intercept at validation | C |
| X-002 | `CoxPH` does not warn when `max_iter` is reached | Report non-convergence (NN-10); estimates unchanged | C |
| X-003 | `FineGrayPH` with left truncation differs from R `finegray()` (about 3e-3 in coefficients) | Choose the parity target before the competing-risks phase | B |
| X-004 | SerBIN can stop at a near-null fit when covariates are far from zero (upstream C27) | Center internally; confirm with a dedicated fixture | B |
| X-005 | `LogisticThreeStageModel.sigma_sensitivity()` fails when σ̂ = 0 | Define the boundary in the three-stage specification | B |
| X-006 | Unresolved differences with internal R code (`IUR.fac`, `cal_SMR_pro_adj`) | Not parity-gated until resolved | B (provisional) |
| X-007 | R-comparison suite: 26 documented failures | Resolved 2026-10-06: at v0.7.0 the survival suite passes with the committed R results (275 passed, 1 skipped for the optional lifelines package), and R 4.3.3 with survival 3.5-8 regenerates those results byte for byte; no Cox feature is excluded | — |

Not discrepancies: `ties="exact"` is out of scope (§2.5); the Breslow default follows the
reference, every fit records its tie method, and R fixtures set `ties` explicitly.
