# Specification: logistic fixed-effect model — covariate tests, robust variance, prediction (Phase 2b)

- Status: Draft, awaiting approval (D-34). No 2b code is written before approval (NN-2).
- Builds on: [fixed-effect-estimation.md](fixed-effect-estimation.md) (2a, D-30); plan
  [plan.md](plan.md) (slice 2b).
- Decision references: D-34; discrepancies X-022 and X-023.
- Reference: pprof_py v0.7.0 (`9320766`): `inference/logistic/fixed_effect/covariates.py`
  (`summary`, `_compute_wald_beta`, `_fit_reduced`, `_compute_lr_beta`, `_compute_score_beta`,
  `_compute_robust_variances`), `models/logistic/fixed_effect.py` (`fit`: AUC; `predict`),
  `utils/metrics.py` (`roc_auc_score`). R 4.3.3: `glm`, `anova(..., test = "Rao")`, sandwich 3.1-0's
  `vcovCL` and `estfun`.

Probe results are sandbox evidence from round 31: lfe-base with a cluster column `patient = id
div 3` (384 provider-patient clusters; 25 patients seen by two providers), pprof_py at tol 1e-13
against R.

## 1. Estimands

For the model of 2a, at its estimates (γ̂, β̂):

| Quantity | Definition |
|---|---|
| Wald test of βⱼ = b₀ | z = (β̂ⱼ − b₀)/SEⱼ, SE from the model-based or the cluster-robust covariance; two-sided, `less` or `greater` |
| Likelihood-ratio test of βⱼ = 0 | 2(ℓ(γ̂, β̂) − ℓ(γ̃, β̃)), (γ̃, β̃) the fit without covariate j; χ²₁ |
| Score test of βⱼ = 0 | U²/I_eff at (γ̃, β̃): U the score for βⱼ, I_eff its information with every other parameter partialled out; χ²₁ |
| Cluster-robust Var(β̂) | The β block of the sandwich I⁻¹MI⁻¹, M the sum over clusters of outer products of the clusters' score sums |
| Robust Var(γ̂ⱼ + x̄ᵀβ̂) | The same sandwich for the provider effect at the average case mix (pprof_py C25) |
| Robust Var(γ̂ⱼ), β known | A0ⱼ/Iⱼ², the sandwich with β treated as known (R's `test_aoh`) |
| Prediction | πᵢ = 1/(1 + e^−ηᵢ), ηᵢ = γ̂ⱼ + xᵢᵀβ̂, for rows of fitted providers |
| AUC | The Mann–Whitney area under the ROC curve of the fitted π against 0/1 outcomes, ties counted as one half |

## 2. Objective and formulas

Clusters are identifiers within providers: rows of provider j with the same cluster key form one
cluster k (X-023). At the estimates, with π clipped to [1e-10, 1 − 1e-10] and no weight floor (as
the 2a variances), residual eᵢ = yᵢ − nᵢπᵢ, Iⱼ = Σᵢ nᵢπᵢ(1 − πᵢ) and x̄ⱼ = bⱼ/Iⱼ:

| Term | Per cluster k of provider j | Per provider j |
|---|---|---|
| Residual sum | Rₖ = Σᵢ∈ₖ eᵢ | A0ⱼ = Σₖ Rₖ² |
| Centred score | ũₖ = Σᵢ∈ₖ (xᵢ − x̄ⱼ)eᵢ | A1ⱼ = Σₖ Rₖ(Σᵢ∈ₖ xᵢeᵢ) |

M_eff = Σ over all clusters of ũₖũₖᵀ (which equals pprof_py's `B diag(A0/I²) Bᵀ − B diag(1/I) A1 −
A1ᵀ diag(1/I) Bᵀ + A2`), Var_rob(β̂) = S⁻¹M_effS⁻¹ with S⁻¹ the model-based Var(β̂) of 2a, and with
hⱼ = S⁻¹(x̄ⱼ − x̄):

Var_rob(γ̂ⱼ + x̄ᵀβ̂) = A0ⱼ/Iⱼ² + 2(A0ⱼ/Iⱼ)x̄ⱼᵀhⱼ + hⱼᵀM_effhⱼ − 2A1ⱼᵀhⱼ/Iⱼ.

A provider with Iⱼ < 1e-14 gets NaN robust variances and contributes nothing to M_eff (pprof_py).
At the reduced fit (γ̃, β̃ with β̃ⱼ = 0), with π clipped, the score test uses U = Σᵢ xᵢⱼeᵢ (uncentred,
as pprof_py and R's Rao score) and I_eff = 1/(S⁻¹)ⱼⱼ, S the 2a Schur complement over all p features
there: the information for βⱼ with the provider effects and the other coefficients partialled out
(pprof_py C32).

## 3. Parameterization and data contract

As 2a, plus an optional cluster column (integral or string, no nulls) naming, for example, the
patient. Prediction takes rows with the fit's features and provider column; their providers must
be fitted ones (X-022).

## 4. Conventions

| Convention | Value |
|---|---|
| Wald | `null` b₀ (default 0), `alternative` two-sided, `less` or `greater`, `level` 0.95; p two-sided 2Φ̄(abs(z)), `less` Φ(z), `greater` Φ̄(z); intervals β̂ ± z₁₋α/₂SE, or one-sided with an infinite bound; SE model-based or robust |
| LR and score | Null 0 only (pprof_py raises otherwise); at least two features, since the reduced model needs one (pprof_py raises); the refit starts from 2a's start and uses the fit's tol, maxIter, bound and backtrack; Wald intervals reported alongside, as pprof_py |
| Robust variance | Computed when the cluster column is given; clusters nested in providers (X-023); no small-sample factor (sandwich's `HC0` without cluster adjustment) |
| Prediction | Linear predictor and probability; rows of providers that were screened out or never in the fit fail with counts (X-022) |
| AUC | When the fitted rows' outcomes take exactly the values 0 and 1 (always for Bernoulli data, and for binomial rows whose outcomes happen to be 0 or 1, as pprof_py); otherwise none, including pprof_py's failing case of two outcome values other than 0 and 1 (X-022) |
| Accuracy | pprof_py's `score` (accuracy at 0.5) is not provided |

## 5. Algorithm and distributed plan

| Output | Passes | Driver | Executors |
|---|---|---|---|
| Wald variants | none | from the fit | — |
| Robust variances (at fit, with a cluster column) | 2 | M_eff (packed p×p) reduced in block order; Var_rob(β̂); the provider arrays collected under `maxProvidersOnDriver` | Pass R1: per provider x̄ⱼ, per cluster ũₖ, block partial of M_eff. Pass R2: per provider A0ⱼ, A1ⱼ, Iⱼ, x̄ⱼ and both robust variances, with M_eff, S⁻¹ and x̄ captured by closure |
| LR and score (`covariateTests(df, fit, ...)`, data explicit, fingerprint checked, API-3) | per covariate: a SerBIN refit on the same working set with the covariate dropped, then one pass at the reduced fit | ℓ difference; I_eff from the p×p system | The 2a kernels over the active covariates only; no new shuffle |
| Prediction (`predict(df, fit)`) | 1 | — | Provider effects joined by key with a broadcast hint (m rows); η and π as column expressions (Spark's `exp` is StrictMath-based) |
| AUC (at fit, outcomes 0 and 1) | 1 sort | Combines per-partition integer counts in partition order | (π, y) per row from the blocks, sorted by π (equal values share a partition); per partition: negatives, positives and twice the Mann–Whitney numerator as exact integers |

Within a provider, the robust kernels visit clusters in key order and rows in canonical order, so
the canonical order of 2a is unchanged and a fit is bitwise the same with or without a cluster
column. Blocks carry each row's cluster as its rank among the block's cluster keys. The AUC's
numerator is an integer below 2⁶³ for any n up to 10⁹, so it is exact; one division gives the
AUC. Determinism: R0 across partitionings; R1 within T-part.

## 6. Outputs

`LogisticFit` gains `robustCovariance` (packed, when clustered), `clusters` (count), `auc`
(optional) and, per provider, `robustVarCaseMix` and `robustVarFixedBeta`; the provider table gains
`robust_var_case_mix` and `robust_var_fixed_beta`. `LogisticFE.waldTests(fit, null, alternative,
level, robust)` and `LogisticFE.covariateTests(df, fit, method, covariates)` return coefficient
tables (feature, estimate, standard error, statistic, p-value, interval, method). `LogisticFE.predict`
returns the input with `linear_predictor` and `probability`. `LogisticFitIO` moves to format
version 3; versions 1 and 2 load without the new fields. The Python wrapper `LogisticFixedEffect`
(2a and 2b) follows in the implementation rounds (plan §6).

## 7. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| LR or score test with one feature | Raises | Fails with a message |
| LR or score test with a null other than 0 | Raises | Fails with a message |
| Robust Wald without a cluster column | Raises | Fails with a message |
| Provider with Iⱼ < 1e-14 | NaN robust variances; skipped in the meat | Same |
| Prediction for a provider not in the fit (probed) | Key inside the fitted range: another provider's effect, silently (key 0 gave provider 1's); above it: `IndexError` | Fails with counts (X-022) |
| Two outcome values other than 0 and 1 | The fit raises in `roc_auc_score` | No AUC (X-022) |
| A patient seen by two providers | Two clusters | Same (X-023) |

## 8. Reference mapping and probe agreement

| Element | pprof_py | R | Probe (relative to the largest magnitude) |
|---|---|---|---|
| LR statistic | `_compute_lr_beta` | 2(ℓ full − ℓ reduced) of `glm` | 4.3e-15 |
| Score statistic | `_compute_score_beta` (C32) | `anova(reduced, full, test = "Rao")` | 2.4e-13 |
| Robust Var(β̂) | `_compute_robust_variances` | `vcovCL(glm, cluster = interaction(provider, patient, drop = TRUE), type = "HC0", cadjust = FALSE)` | 3.6e-15 |
| Robust Var(γ̂ⱼ + x̄ᵀβ̂) | the same | `vcovCL` blocks, as for the case-mix variance of 2a | 2.1e-14 |
| Robust Var(γ̂ⱼ), β known | the same | A0ⱼ/Iⱼ² from `glm`'s residuals | 5.6e-16 |
| AUC | `roc_auc_score` | Mann–Whitney with `rank()` | exact |
| Prediction | `predict` | `fitted(glm)` | 4.0e-16 |

With unused interaction levels (`drop = FALSE`), `vcovCL` gave a different matrix (1.3e-2 in β̂'s
block, 0.63 in the case-mix variance) while a hand-built sandwich, `vcov(glm)` with the clustered
`estfun` meat, matched pprof_py; the fixtures use `drop = TRUE`, which agrees with the hand-built
one to 1.3e-16. pprof_py's default fit (tol 1e-8) gives the same LR statistics as its tight fit and
the score statistics and robust variances within 1.1e-15.

## 9. Validation plan

Fixtures (next round): every 2a case gains pprof_py's Wald variants (null 0.25 with `greater` and
`less`), LR and score statistics and p-values, AUC (Bernoulli cases) and predictions, and R's LR,
Rao score, AUC and predictions where `glm` applies (not lfe-degenerate). A new case, lfe-clustered,
has a patient column with repeated rows and patients seen by two providers, and adds pprof_py's
robust variances (both fits) and R's `vcovCL` and fixed-β variances. Tolerance classes: T-test
(statistics), T-p (p-values; pprof_py's below 1e-6 compared through z, as 2a), T-var (robust
variances), T-meas (AUC and predictions), T-part (layout invariance). Negative controls: a cluster
column shifted by one row, one outcome flipped, the wrong covariate dropped. Tests: function-level
(the meat and the efficient information at fixed parameters), end-to-end against both references,
bitwise equality of fits with and without a cluster column, invariance to row order and
partitioning, the edge cases of §7, Classic Spark and Spark Connect.

## 10. Known discrepancies

| ID | Summary | Class | Proposed decision (D-34) |
|---|---|---|---|
| X-022 | pprof_py's prediction maps unknown providers to neighbours or raises `IndexError`, and its fit raises on two outcome values other than 0 and 1 | C | Fail with counts for unknown providers; report no AUC in the second case |
| X-023 | Clusters are nested in providers: a patient seen by two providers counts as two clusters, and the sandwich ignores correlation across providers | — (reference convention) | Follow pprof_py and say so in the documentation; clustering across providers would need a shuffle by cluster and match neither reference |
