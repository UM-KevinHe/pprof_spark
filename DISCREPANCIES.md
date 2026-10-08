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
| X-010 | pprof_py tests for a log-likelihood decrease, and halves the step, before testing convergence; R's coxfit6 tests convergence first and keeps the full step. When a converged step lowers the log-likelihood at rounding level, the two return estimates half a step apart. R's later halvings also follow a different schedule | Iterate trace on tiny-ties: identical to 1e-16 for two iterations, then R's third step is twice pprof_py's; coxfit6.c lines 249-276. Converged coefficients differ by 1.05e-8 relative on tiny-ties (Breslow), at most 2.4e-9 elsewhere | B | Corrected, approved 2026-10-06 (D-20), superseding round 6's bug-compatible choice: pprof_spark tests convergence on the full step before halving, because pprof_py's order makes its own results depend on row order by up to 1.05e-8 (9 distinct results over 30 row orders of tiny-ties), which conflicts with NN-4 and R1. Measured in round 10: tiny-ties coefficients differ from pprof_py's by 0.995 of T-coef and from R's by 1.2e-7 of it. Reported upstream (OI-33) |

| X-011 | Collinear or constant covariates: pprof_py returns pseudo-inverse estimates (silently for collinearity, with a NumPy warning for a constant column) | Probed at v0.7.0 | C | pprof_spark fails and names the aliased covariates (Cox specification §9). Approved 2026-10-06 (D-20) |
| X-012 | No events at all: pprof_py returns β̂ = 0, converged, with a NumPy warning | Probed at v0.7.0 | C | pprof_spark fails: the partial likelihood is constant. Approved 2026-10-06 (D-20) |
| X-013 | Efron ties: pprof_py counts a zero-weight event in the number of tied events d, so a zero weight is not the same as dropping the row (0.67% coefficient change on tiny-ties), contrary to its own validation message; R rejects zero weights | Probed at v0.7.0 and R 3.5-8 | B (corrected; accepted 2026-10-06, D-21) | pprof_spark counts only events with positive weight, so a zero weight equals dropping the row under both tie methods (D-21) |
| X-014 | Baseline hazard reference point: pprof_py's public `baseline_hazard_` and R's `basehaz(fit, centered = FALSE)` equal the hazard at x = 0 and offset 0 times exp(weighted mean offset); pprof_py's predictions use the raw baseline | Read in pprof_py v0.7.0's source and probed | C | Report the raw baseline and document the factor (Phase 1b specification §2). Approved 2026-10-06 (D-23) |
| X-015 | Robust variance, Breslow ties, (start, stop] data: pprof_py's differs from R's by 11% (lt-weights-offset, 40 clusters) while a sandwich of its own dfbeta residuals matches R to 7e-12; right-censored data and Efron agree | Probed in round 16 | B | Compute the sandwich from score residuals, as R does; report upstream. Approved 2026-10-06 (D-25). Round 17's fixtures show it on both left-truncated cases (8% and 11%) |

## Registered for Phase 2 (round 25, 2026-10-07)

| ID | Discrepancy | Evidence | Class | Decision |
|---|---|---|---|---|
| X-016 | Provider screening: pprof_py keeps providers with more than `cutoff` records (as R's `glmm.data.prep`); R pprof 1.0.3's `logis_fe` keeps those with at least `cutoff` (`included <- 1 * (prov.size.long >= cutoff)`) | Read in both sources; probe: pprof_py excludes a provider of 10 records | — (not a pprof_spark difference) | pprof_spark follows pprof_py; R comparisons are given pprof_py's screened rows. Approved 2026-10-07 (D-30) |
| X-017 | R pprof's SerBIN line search has no below-noise rule (pprof_py's C3), so near convergence it can shrink the last step towards zero and stop up to one step earlier | Read in both sources. Probe, 30 providers, tol = 1e-8: β̂ differs from pprof_py's by 2.2e-10, about the size of pprof_py's last step (3.4e-10), which fits the explanation; not traced step by step. With degenerate providers the two agree to 4e-16 | — (not a pprof_spark difference) | R SerBIN outputs are compared under T-coef; pprof_spark follows pprof_py's rule. Approved 2026-10-07 (D-30) |
| X-018 | Providers with no events or only events: pprof_py and R pprof hold their effects at med(γ) ∓ 10, where their records still enter the β score (about e^−10 each), so β̂ is not the maximum-likelihood estimate without them | Probe: both such providers end exactly 10 from the median; β̂ differs from `glm` without them by 3.5e-9 | — (the reference's bounding rule) | Follow the reference (§7.4) and flag those providers; `glm` comparisons use cases without them (approved 2026-10-07, D-30) |
| X-019 | Inputs pprof_py mishandles: non-integer outcomes or trials (accepted, then the outcome is truncated to an integer), no features (a broadcast error), no events or only events overall (an infinite start; β = 0 after one step with NaN warnings), features aliased with the provider effects or with each other (a `LinAlgError` from the Cholesky) | Probed at v0.7.0 | C | pprof_spark fails at validation, naming the problem or the aliased features (approved 2026-10-07, D-30) |
| X-020 | pprof_py's README says `fit(n_var=...)` still validates the response as 0/1; at v0.7.0 binomial counts with 0 ≤ y ≤ n are accepted and fitted | Probe: binomial rows give the expanded Bernoulli fit within 1.1e-16 | E | Follow the behavior; ask upstream to correct the README (OI-33). Approved 2026-10-07 (D-30) |
| X-021 | The correlation warning: pprof_py warns when two covariates' absolute Pearson correlation exceeds 0.9 over every input row, before screening. pprof_spark's deterministic kernels run on the working set, which holds the fitted providers only; over every row the co-moments would need built-in SQL aggregates, which are not partition-invariant (NN-4, DIST-7) and are discouraged for O(p²) statistics (§10.3), or another pass with its own shuffle | Read in pprof_py's `check_correlation`; spec §4 | C (a warning only; no estimate changes) | Proposed with D-32: compute the co-moments on the fitted rows in the working set's summary pass, and say so in the warning. Alternatives: Spark's built-in correlation over every row, or no warning. Approved 2026-10-07 (D-32, option a); implemented in round 29 |
| X-022 | pprof_py's `predict` locates providers with `np.searchsorted`: a key that was not fitted takes a neighbour's effect when it lies inside the fitted keys' range (key 0 gave provider 1's predictions) and raises `IndexError` above it; and its fit raises in `roc_auc_score` when the outcomes take two values other than 0 and 1 | Probed at v0.7.0 (round 31) | C | Approved 2026-10-07 (D-34): prediction fails with counts for rows of providers not in the fit; no AUC in the second case |
| X-023 | pprof_py's cluster-robust variances group observation identifiers within each provider, so a patient seen by two providers forms two clusters and correlation across providers is ignored | Read in `_compute_robust_variances`; the probe's 25 such patients; the dense sandwich and `vcovCL` with provider-patient clusters agree with it | — (reference convention) | Approved 2026-10-07 (D-34): follow pprof_py and document it; clustering across providers would need a shuffle by cluster and match neither reference |
| X-024 | pprof_py's exact upper tails are 1 − cdf + pmf/2 from `fast_poibin`'s FFT distribution, accurate only to about 1e-16 absolute: relative error 1.4e-5 at a tail of 6.4e-11 and none left at 1.3e-25 (500 rows); its lower tails are accurate | Round 34 probe against mpmath at 60 digits (spec 2c §3) | B | Approved 2026-10-08 (D-35): compute the smaller tail directly by the exact recursion; compare with mpmath where pprof_py's upper tail is below 1e-7. Round 36: one-sided z reads tails above one half from their small complements, which pprof_py and R read directly and lose the same way; on lfe-binomial, providers 20 and 27 show it (z 10.277 and 7.194 against pprof_py's 10.339 and 7.196) |
| X-025 | pprof_py's bootstrap test draws from numpy's generator; pprof_spark's is counter-based and partition-invariant (STAT-2), so the draws differ | Plan §3 | C | Approved 2026-10-08 (D-35): distributional parity only (p-values within four Monte Carlo standard errors of the exact test's) |

## Known reference limitations to verify at the pinned commit (§3.4)

Classes are proposals. Each row is re-verified against the pinned reference, pprof_py v0.7.0
(commit 9320766), in the fixture round.

| ID | Reference behavior | Proposed handling | Proposed class |
|---|---|---|---|
| X-001 | `CoxPH(fit_intercept=True)` fits, but every `predict_*` method raises | Reject an intercept at validation | C |
| X-002 | `CoxPH` does not warn when `max_iter` is reached | Resolved at v0.7.0: `CoxPH` warns; pprof_spark warns too | — |
| X-003 | `FineGrayPH` with left truncation differs from R `finegray()` (about 3e-3 in coefficients) | Choose the parity target before the competing-risks phase | B |
| X-004 | SerBIN can stop at a near-null fit when covariates are far from zero (upstream C27) | Resolved at v0.7.0: SerBIN uses the joint Newton direction as R does. Round 25 probe: shifting two features by +50 and −30 changes β̂ by 5.6e-15 and γ̂ + cᵀβ̂ by 4.7e-15, in the same five steps. The lfe-shifted fixture keeps it as a regression case (approved 2026-10-07, D-30) | — |
| X-005 | `LogisticThreeStageModel.sigma_sensitivity()` fails when σ̂ = 0 | Define the boundary in the three-stage specification | B |
| X-006 | Unresolved differences with internal R code (`IUR.fac`, `cal_SMR_pro_adj`) | Not parity-gated until resolved | B (provisional) |
| X-007 | R-comparison suite: 26 documented failures | Resolved 2026-10-06: at v0.7.0 the survival suite passes with the committed R results (275 passed, 1 skipped for the optional lifelines package), and R 4.3.3 with survival 3.5-8 regenerates those results byte for byte; no Cox feature is excluded | — |

Not discrepancies: `ties="exact"` is out of scope (§2.5); the Breslow default follows the
reference, every fit records its tie method, and R fixtures set `ties` explicitly.
