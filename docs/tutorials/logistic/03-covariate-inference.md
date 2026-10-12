# 3. Inference for covariates: variances, tests, robust variance, prediction, AUC

[Tutorial home](README.md) · previous: [2. SerBIN](02-serbin.md) · next: [4. Provider tests](04-provider-tests.md)

This chapter covers what slices 2a and 2b compute about β once SerBIN has converged: model-based
variances and Wald tests, likelihood-ratio and score tests by refitting, cluster-robust variances,
prediction and the AUC. Numbers are from the `lfe-base` fixture (30 providers, 1,075 records, three
features); chapter 9 follows the whole example.

## 3.1 Model-based variances and Wald tests

**Theory.** At the maximum, (γ̂, β̂) is approximately normal with covariance equal to the inverse
information. Chapter 2, section 2.9, derived the blocks of that inverse: Var(β̂) = S⁻¹, with S the
Schur complement at the estimates.

**Implementation.** After the iteration, one pass computes S at the estimates with π clipped to
[1e-10, 1 − 1e-10] and no weight floor (`varianceSchur`), the driver factors it by Cholesky and inverts
it, and the coefficient table follows: SE = √(S⁻¹)ⱼⱼ, z = β̂/SE, p = 2Φ̄(|z|), and β̂ ± z₁₋α/₂·SE
([engine LogisticFE.scala lines 154–172 and 239–251](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L239-L251)).
`LogisticFE.waldTests(fit, nullValue, alternative, level, robust)` reruns the test from the fit, with
no pass over the data, for another null value, a one-sided alternative or the robust covariance
([lines 358–410](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L358-L410)).

**lfe-base** (probe; pprof_py agrees to 1.8e-16 and `glm` to 3.2e-14 in Var(β̂)):

| | x1 | x2 | x3 |
|---|---:|---:|---:|
| β̂ | 0.49987 | −0.23778 | 0.86848 |
| SE | 0.07153 | 0.06874 | 0.13992 |
| z | 6.988 | −3.459 | 6.207 |

## 3.2 Likelihood-ratio tests

**Theory.** To test βⱼ = 0, refit the model without covariate j, keeping every provider effect free,
and compare: 2(ℓ(γ̂, β̂) − ℓ(γ̃, β̃)) is approximately χ²₁ under the null.

**Implementation** (`covariateTests(df, fit, "lr", covariates)`,
[lines 416–538](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L416-L538)).
The data must be the fit's: the call rebuilds the working set with the fit's provider plan and
checks the fingerprint (API-3). For each tested covariate it maps the persisted blocks to blocks
without that column (`dropColumn`), persists them, runs SerBIN from the original start with the fit's
options, and computes ℓ at the reduced estimates in one more pass. The p-value is computed as
2Φ̄(√statistic), which equals the χ²₁ upper tail; a statistic at or below zero gives 1. The test needs
at least two features (the reduced model needs one), as in pprof_py. Each tested covariate therefore
costs a complete SerBIN fit: a few Newton steps of two passes each.

**lfe-base**, from the fixture: the statistics are 52.538, 12.159 and 39.641 (p = 4.2e-13, 4.9e-4 and
3.1e-10), and pprof_py's and R's `anova` agree to every digit stored.

## 3.3 Score tests

**Theory.** The score (Rao) test needs only the reduced fit. At (γ̃, β̃) with β̃ⱼ = 0, the score for βⱼ
is U = Σᵢ xᵢⱼ(yᵢ − nᵢπᵢ), and its variance under the null is the information for βⱼ with every other
parameter, the provider effects included, partialled out. That efficient information is again a Schur
complement: with S the p × p Schur complement over all p features at the reduced fit,
I_eff = 1/(S⁻¹)ⱼⱼ. The statistic is U²/I_eff = U²·(S⁻¹)ⱼⱼ, approximately χ²₁.

**Implementation.** After the reduced fit, one pass over the full working set computes, at clipped π,
the centred S over all p features and the uncentred score Σ xᵢeᵢ (`scoreTerms`,
[kernels lines 392–418](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L392-L418)),
and the driver reads entry j of both (pprof_py C32).

**lfe-base**: 51.375, 12.103 and 39.400 (p = 7.6e-13, 5.0e-4, 3.5e-10), equal in pprof_py and R's Rao
test. The three tests agree closely, as they should when the log-likelihood is nearly quadratic in
βⱼ; Wald's z² (48.8, 12.0, 38.5) is a little smaller here.

## 3.4 Cluster-robust variances

**Theory.** The model-based variance assumes independent records. When records are grouped — several
admissions of one patient — the sandwich estimator replaces the score's variance with an empirical one
built from cluster sums of score contributions.

**Implementation** (specification [covariate-inference.md](../../spec/logistic/covariate-inference.md) §2).
Clusters are identifiers **within** providers: rows of provider j with the same cluster key form one
cluster k, so a patient seen by two providers counts as two clusters, as in pprof_py (X-023). At the
estimates, with π clipped and residuals eᵢ = yᵢ − nᵢπᵢ:

| Per cluster k of provider j | Per provider j |
|---|---|
| Rₖ = Σᵢ∈ₖ eᵢ | A0ⱼ = Σₖ Rₖ² |
| ũₖ = Σᵢ∈ₖ (xᵢ − x̄ⱼ)eᵢ | A1ⱼ = Σₖ Rₖ·Σᵢ∈ₖ xᵢeᵢ |

```math
M_{\text{eff}} = \sum_k \tilde u_k \tilde u_k^{\top}, \qquad
\operatorname{Var}_{\text{rob}}(\hat\beta) = S^{-1} M_{\text{eff}}\, S^{-1}.
```

ũₖ is cluster k's contribution to the score for β after the provider effect has been profiled out,
which is why it is centred at x̄ⱼ: M_eff is pprof_py's longer expression rearranged. The provider
effect at the average case mix gets the matching sandwich, and A0ⱼ/Iⱼ² is its variance with β treated
as known (R's `test_aoh`). A provider with Iⱼ < 1e-14 gets NaN robust variances and contributes nothing
to M_eff. No small-sample factor is applied (HC0).

Two extra passes run when the spec has a cluster column: one for M_eff (`robustMeat`), one for the
provider variances given M_eff, S⁻¹ and x̄, captured by closure (`robustProviders`)
([kernels lines 280–387](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L280-L387);
[engine lines 192–227](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L192-L227)).
Within a provider the kernels visit clusters in key order and rows in canonical order, and each block
carries a row's cluster as its rank among the block's keys, so a fit is bitwise the same with or
without a cluster column; the test "a cluster column leaves the fit bitwise unchanged and fills the
robust columns" checks it. `lfe-base` has no cluster column; the `lfe-clustered` fixture covers this
path against R's `vcovCL` (specification probe: 3.6e-15 for Var_rob(β̂)).

## 3.5 Prediction

`LogisticFE.predict(df, fit)` adds the linear predictor η = γ̂ⱼ + xᵀβ̂ and the probability σ(η) as Spark
column expressions, after joining the provider effects to the rows by key with a broadcast hint (an
m-row table). Rows of providers that were screened out or never in the fit fail with counts, where
pprof_py silently used a neighbouring provider's effect or raised `IndexError` (X-022)
([lines 594–634](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L594-L634)).

## 3.6 The AUC

When the outcomes are 0 and 1, the fit reports the area under the ROC curve of the fitted
probabilities: the Mann–Whitney probability that a random positive record has a higher π than a random
negative one, ties counting one half. The engine sorts (π, y) by π across partitions (equal values in
one partition), counts per partition the negatives, the positives and twice the Mann–Whitney numerator
as exact 64-bit integers, and combines the counts in partition order; one division gives the AUC, so
the result is exact for n up to 10⁹
([lines 636–686](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L636-L686)).
On `lfe-base` it is 0.71106393900924710, equal to pprof_py's and R's.

## 3.7 Where to look

| Quantity | Code |
|---|---|
| Variances at the estimates | [`varianceSchur`, `providerVariances`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L202-L250) |
| Coefficient table, warnings, AIC and BIC | [engine `fit`, lines 239–310](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L239-L310) |
| Wald variants | [`waldTests`](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L358-L410) |
| Likelihood-ratio and score tests | [`covariateTests`](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L416-L538), [`scoreTerms`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L392-L418) |
| Robust variances | [`robustMeat`, `robustProviders`, `sandwich`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L280-L439) |
| Prediction, AUC | [`predict`, `areaUnderCurve`](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L594-L686) |

Tests: `LogisticFESuite` (estimates, variances, Wald, ℓ, AIC and BIC against pprof_py, R SerBIN and
`glm`), `LogisticInferenceSuite` (tests, AUC, predictions and robust variances against pprof_py and R).
