# 1. The logistic provider model

[Tutorial home](README.md) · next: [2. SerBIN](02-serbin.md)

This chapter sets out the model that slices 2a to 2e fit and use: what it assumes, what its
parameters mean, its log-likelihood and derivatives, and the one structural fact that makes it
tractable for a million providers. It assumes ordinary logistic regression and no survival analysis.

## 1.1 The data

Provider profiling with a binary outcome asks whether a provider's patients experience an outcome —
readmission within 30 days, a hospital-acquired infection, death — more or less often than expected
given who those patients are. The data are records i of providers j, each with:

- an outcome: yᵢⱼ ∈ {0, 1} for one patient-episode, or, for aggregated records, yᵢⱼ events out of
  nᵢⱼ trials with the same covariates (binomial rows; Bernoulli rows are the case n = 1);
- covariates xᵢⱼ ∈ ℝᵖ describing the case mix: age, comorbidities, admission type.

A binomial record with n trials and y events contributes exactly what n Bernoulli records with the
same covariates contribute, so aggregating identical records changes nothing; the test "binomial
rows give the fit of the expanded Bernoulli rows within T-part" (`LogisticBehaviourSuite`) checks it.

## 1.2 The model

```math
\operatorname{logit} \pi_{ij} = \log\frac{\pi_{ij}}{1 - \pi_{ij}} = \gamma_j + x_{ij}^{\top}\beta ,
\qquad \pi_{ij} = \sigma(\eta_{ij}) = \frac{1}{1 + e^{-\eta_{ij}}} .
```

- **β**, shared by every provider, adjusts for case mix: exp(βₖ) is the odds ratio for one unit of
  covariate k, within any provider.
- **γⱼ**, one per provider, is the provider's effect on the log-odds: the log-odds of the outcome for
  a record with x = 0 treated by provider j. Differences are easier to read than levels:
  exp(γⱼ − γₗ) is the odds ratio between providers j and l for patients with identical covariates.
- There is **no intercept**: the provider effects carry it, and an intercept would not be
  identifiable alongside them.

Shifting a covariate by a constant, x → x + c, changes nothing that matters: β̂ is unchanged and every
γ̂ⱼ moves by −cᵀβ̂, so differences between providers are unchanged. The kernel test "shifting a
covariate leaves S, r and Δβ unchanged and moves Δγ by −cΔβ" checks the algorithmic counterpart.

## 1.3 Why fixed provider effects

A provider effect can be modelled as a fixed parameter, as here, or as a random draw from a
distribution, as in a random-intercept model. Fixed effects make no assumption about how provider
effects are distributed or about their independence from case mix, which matters when sicker
patients concentrate at particular providers; each γ̂ⱼ estimates that provider's own effect, which is
what profiling tests against a reference (chapter 4). Their cost is variance for small providers,
whose effects are estimated from few records. The model therefore screens out providers with too few
records (section 1.7), and inference reports each provider's standard error. Random effects, which
shrink small providers toward the average, appear in the three-stage model of slice 2f, for clusters
rather than providers. He, Kalbfleisch, Li and Li (2013) and Wu, Yang, Kang and He (2022) discuss
the choice.

## 1.4 The log-likelihood

With ηᵢⱼ = γⱼ + xᵢⱼᵀβ, the log-likelihood of independent binomial records is, up to the constant
Σ log C(n, y), which pprof_py omits and which is zero for Bernoulli data:

```math
\ell(\gamma, \beta) = \sum_{j}\sum_{i} \Big[\, y_{ij}\,\eta_{ij} - n_{ij}\log\big(1 + e^{\eta_{ij}}\big) \Big].
```

log(1 + e^η) overflows when computed directly for large η. The kernel evaluates it as
η + log1p(e^{−η}) for η > 0 and log1p(e^η) otherwise, numpy's `logaddexp(0, η)`, with `StrictMath`
([kernels/LogisticFE.scala lines 60–80](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L60-L80)).

## 1.5 Score and information

Each record's contribution depends on the parameters only through its η, and

```math
\frac{\partial \ell_{ij}}{\partial \eta_{ij}} = y_{ij} - n_{ij}\pi_{ij}, \qquad
-\frac{\partial^2 \ell_{ij}}{\partial \eta_{ij}^2} = n_{ij}\pi_{ij}(1 - \pi_{ij}) = q_{ij}.
```

Since η depends on γⱼ with coefficient 1 and on β through x, the chain rule gives every derivative:

| Quantity | Definition | Meaning |
|---|---|---|
| Score for γⱼ | gⱼ = Σᵢ (yᵢⱼ − nᵢⱼπᵢⱼ) | Provider j's observed minus expected events |
| Score for β | g_β = Σⱼ Σᵢ xᵢⱼ(yᵢⱼ − nᵢⱼπᵢⱼ) | Covariate-weighted residuals |
| Information for γⱼ | hⱼ = Σᵢ qᵢⱼ | Provider j's binomial variance |
| Cross information | bⱼ = Σᵢ qᵢⱼxᵢⱼ | A p-vector per provider |
| Information for β | C = Σⱼ Σᵢ qᵢⱼxᵢⱼxᵢⱼᵀ | p × p |

At the maximum every score is zero; in particular each fitted provider's expected events equal its
observed events, gⱼ = 0. That property is the basis of indirect standardization (chapter 5).

## 1.6 The structure that makes a million providers tractable

γⱼ appears only in provider j's records, so the second derivative with respect to two different
provider effects is zero. The information matrix of (γ, β) is **block-arrowhead**: diagonal in γ,
with a dense border for β.

```math
I(\gamma, \beta) =
\begin{pmatrix}
h_1 & & & b_1^{\top} \\
 & \ddots & & \vdots \\
 & & h_m & b_m^{\top} \\
b_1 & \cdots & b_m & C
\end{pmatrix}
```

For m = 10⁶ providers and p = 20 covariates the full matrix has about 10¹² entries, yet it is
described by m numbers hⱼ, m vectors bⱼ and one p × p matrix C. Chapter 2 shows how SerBIN solves the
Newton system in O(n·p² + m·p + p³) operations by eliminating the provider block exactly, instead of
the O((m + p)³) of a dense solve. The same structure decides the distributed design: everything about
provider j can be computed where provider j's records are, and only p- and p²-sized quantities need
to be combined across providers.

ℓ is concave in (γ, β), as for any logistic regression, because I is a sum of positive semidefinite
terms. It is strictly concave, with a unique finite maximum, when the design has full rank and every
provider has both events and non-events.

## 1.7 Screening

Providers with few records have unstable effects. pprof_py fits providers with **more than**
`cutoff` = 10 records and lists the others; pprof_spark does the same with `minRecords` = 10
(records are rows, also for binomial rows), lists the excluded providers with their record counts,
and warns how many there are. R pprof keeps providers with **at least** `cutoff` records, so the
two references differ at exactly 10 records (X-016; pprof_spark follows pprof_py). `screen = false`
fits every provider. Screening happens on the driver from the collected provider sizes, before the
working set is built, so excluded providers never enter a block
([engine LogisticFE.scala lines 69–76](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L69-L76)).

## 1.8 Degenerate providers and the bound

A provider whose records have no events (or only events) has no finite maximum-likelihood effect:
its log-likelihood keeps increasing as γⱼ → −∞ (or +∞). Both references keep such providers in the
fit and hold their effects at a **bound**: after every update, each γⱼ is clipped to
[med(γ) − 10, med(γ) + 10], where med is the median over fitted providers. Two consequences:

- their records still enter the β score, with weights near e^{−10}, so β̂ differs slightly from the
  MLE computed without them: by 3.5e-9 in the round-25 probe (X-018);
- the provider table flags them: `zero_events`, `all_events`, and `at_bound` when
  |γ̂ⱼ − med(γ̂)| ≥ bound − 0.1 or the provider is degenerate
  ([engine LogisticFE.scala lines 231–237](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L231-L237)).

The fit fails at validation when the whole input has no events or only events, since then no
provider has a finite effect (X-019).

## 1.9 Identifiability and aliasing

β is identified only by variation **within** providers: a covariate that is constant within every
provider is a linear combination of the provider indicators, so its coefficient cannot be separated
from the provider effects. Chapter 2 shows that the Newton system for β uses exactly the
within-provider variation, and that such a covariate makes its matrix singular. pprof_spark fails and
names the covariate (X-019); pprof_py raises a `LinAlgError`. Collinear covariates fail the same way.
A covariate with zero variance over all records fails earlier, at validation.

## 1.10 Inputs

A fit is described by a `LogisticSpec`, naming the outcome, the features and the provider column,
and optionally a trials column, a row identifier and a cluster column (chapter 3), and by
`LogisticOptions`, whose defaults follow pprof_py:

| Option | Default | Meaning |
|---|---|---|
| `tol` | 1e-8 | Convergence on ‖Δβ‖∞ |
| `maxIter` | 10,000 | Iteration cap (at most `maxIter` + 1 Newton steps) |
| `bound` | 10 | Half-width of the band around the median effect |
| `backtrack` | true | Armijo line search |
| `screen`, `minRecords` | true, 10 | Screening |
| `confidenceLevel` | 0.95 | Wald intervals |
| `correlationThreshold` | 0.9 | Warning for strongly correlated features (X-021) |
| `maxProvidersOnDriver` | 10⁷ | Guard on γ, which is held on the driver (OI-50) |
| `aliasTolerance` | 2⁻³⁹ | R's `toler.chol`, as for Cox |
| `blocks` | `BlockOptions()` | Block size, driver limits and storage level, as for Cox |

The data contract (specification [§4](../../spec/logistic/fixed-effect-estimation.md)): without a
trials column the outcome must be 0 or 1 (integral or boolean); with one, an integer with
0 ≤ y ≤ n and n ≥ 1; features numeric and finite, at least one, none with zero variance; the provider
column integral or string without nulls; the row identifier integral and unique. Every problem is
reported at once with column names and counts, never values. pprof_py accepts non-integer counts
and truncates them in its solver; pprof_spark rejects them (X-019).

## 1.11 Outputs

`LogisticFE.fit(df, spec, options)` returns a `LogisticFit` holding:

| Output | Content |
|---|---|
| Coefficients | β̂ with standard error, z, p-value and interval, per feature in input order |
| Covariance | Var(β̂), packed upper triangle; with a cluster column also the robust version (chapter 3) |
| Providers | Per fitted provider: records, trials, events, γ̂ⱼ, Var(γ̂ⱼ), Var(γ̂ⱼ + x̄ᵀβ̂), the degeneracy flags; as a DataFrame through `LogisticFE.providerTable` |
| Excluded providers | Key, record count and reason |
| Fit | ℓ(γ̂, β̂), AIC = 2(p + m) − 2ℓ, BIC = (p + m)·log(records) − 2ℓ, Newton steps, convergence, final criterion, last step length, warnings, a per-step trace |
| Extras | The AUC when the outcomes are 0 and 1; reproducibility metadata (NN-11); the data fingerprint (API-3) |

AIC and BIC count every fitted provider, degenerate ones included, as pprof_py does. Chapter 3
explains the variances and tests; chapter 4 the provider tests; chapter 5 the standardized measures.
