# The logistic fixed-effect provider model in pprof_spark: a tutorial

This tutorial explains how pprof_spark fits and uses the logistic fixed-effect provider model: the
model behind readmission-type measures, in which every provider has its own effect on the log-odds of
an outcome and shared coefficients adjust for case mix. It follows the
[Cox tutorial](../cox/README.md)'s approach and conventions: statistics first, then the algorithms,
the source and what Spark does; theory, implementation and review notes kept apart; and every
implementation claim tied to a file and a line range, here at commit `3e70739` (round 77).

## Scope

Slices 2a to 2e of Phase 2, all parity-verified and `Experimental` until the package-level scale test
(D-28): estimation by SerBIN (2a), covariate inference (2b), provider tests (2c), standardized measures
(2d), and the job runner and Python wrapper (2e). The three-stage SRR model (2f) is a different model,
with random effects and quadrature, and deserves its own tutorial.

## What this tutorial assumes from the Cox tutorial

Validation with counts and never values, bin packing of whole groups into logical blocks, the one
shuffle that builds a persisted working set, ordered compensated reduction and bitwise reproducibility
are explained once, in the Cox tutorial's chapters [4](../cox/04-from-mathematics-to-code.md) to
[7](../cox/07-validation-and-numerical-accuracy.md). This tutorial concentrates on what differs: a
parameter per provider, so that an m-vector lives on the driver and travels to the blocks on every
pass; two passes per Newton step and a line search that evaluates eight step lengths in one pass; and
provider-level inference, including direct standardization, whose O(n·m) cost pprof_spark removes with
binned Taylor sums.

## Chapters

| Chapter | File | What you learn |
|---|---|---|
| 1 | [01-logistic-provider-model.md](01-logistic-provider-model.md) | The model, its parameters, likelihood and derivatives; the arrowhead information; screening and degenerate providers |
| 2 | [02-serbin.md](02-serbin.md) | Newton's method by block elimination, the centred Schur complement, the line-search ladder, the bound, stopping, and variances from the block inverse |
| 3 | [03-covariate-inference.md](03-covariate-inference.md) | Wald, likelihood-ratio and score tests; cluster-robust variances; prediction; the AUC |
| 4 | [04-provider-tests.md](04-provider-tests.md) | Exact Poisson-binomial, score, Wald and bootstrap tests; exact tails; limits by inversion |
| 5 | [05-standardized-measures.md](05-standardized-measures.md) | Indirect and direct standardization, their tests, and binned Taylor sums |
| 6 | [06-distributed-execution.md](06-distributed-execution.md) | The ProviderLocal layout, the Spark jobs of a fit, γ by broadcast join, what the driver holds |
| 7 | [07-validation-and-numerical-accuracy.md](07-validation-and-numerical-accuracy.md) | Fixtures, what is compared, what the probe confirmed, why implementations differ, where the evidence stops |
| 8 | [08-performance-and-limitations.md](08-performance-and-limitations.md) | Cost of a fit and of the inference, driver limits, skew, limitations |
| 9 | [09-worked-examples-and-troubleshooting.md](09-worked-examples-and-troubleshooting.md) | `lfe-base` followed through every calculation against pprof_py and R; troubleshooting |
| — | [review-findings.md](review-findings.md) | Discrepancies, weaknesses and gaps found while writing (L-01 to L-06) |
| — | [probe/](probe/README.md) | The program that produced the tutorial's numbers |

## Evidence

The same kinds as in the Cox tutorial: approved specifications ([docs/spec/logistic/](../../spec/logistic/)),
the source, the reference fixtures ([fixtures/logistic/](../../../fixtures/logistic/), pprof_py v0.7.0 and
R 4.3.3 with R pprof 1.0.3, stored as hexadecimal doubles), the test suites, CI as recorded in
[STATUS.md](../../../STATUS.md), and the tutorial's probe. The probe is sandbox evidence (NN-8): `numerics`
and `engine` compiled with the Scala 2.13.17 compiler from the PySpark 4.1.0 source distribution, without
`-release 17` and with a stand-in `BuildInfo`, run on OpenJDK 21.0.10 in local mode (`local[4]`).

## Feature status

| Feature | Status |
|---|---|
| SerBIN estimation, screening, degenerate providers and the bound, model-based variances, Wald tests, AIC and BIC | Implemented; parity-verified |
| Likelihood-ratio and score tests for covariates; cluster-robust variances; prediction; AUC | Implemented; parity-verified |
| Exact, score, Wald and bootstrap provider tests; flags; exact limits | Implemented; parity-verified |
| Indirect and direct standardized measures, their standard errors and tests | Implemented; parity-verified; direct sums binned by default (X-026) |
| Job runner (`LogisticJob`) and Python wrapper (`LogisticFixedEffect`, Classic Spark only) | Implemented; parity-verified |
| Offsets and case weights | Not part of this model |
| Clusters across providers | Not supported: clusters are nested in providers (X-023) |
| External standard populations | Not implemented (OI-58) |
| A distributed γ beyond `maxProvidersOnDriver` | Not implemented (OI-50) |
| BAN, penalized and random-effect-only models | Out of Phase 2 (§4) |

## Notation

| Symbol | Meaning |
|---|---|
| j = 1, …, m | Fitted providers |
| i | A record of provider j |
| yᵢⱼ, nᵢⱼ | Events and trials; Bernoulli data have n = 1 |
| xᵢⱼ | Covariates (case mix), length p |
| γⱼ, β | Provider effects (log-odds) and shared coefficients |
| ηᵢⱼ = γⱼ + xᵢⱼᵀβ, πᵢⱼ = σ(ηᵢⱼ) | Linear predictor and probability |
| qᵢⱼ = nᵢⱼπᵢⱼ(1 − πᵢⱼ) | A record's weight in the information |
| gⱼ, hⱼ, bⱼ; g_β, C | Scores and information blocks |
| x̄ⱼ = bⱼ/hⱼ | Provider j's information-weighted mean covariate |
| S, r | Schur complement and its right-hand side |
| λ, v | Predicted gain of a Newton step; accepted step length |
| γ₀ | Reference effect: the median, a weighted mean, or a number |
| Oⱼ, Eⱼ, Dⱼ | Observed, expected (indirect) and direct-standardized events |

## Keeping this tutorial in sync

The chapters cite `3e70739`. When a change touches `numerics/.../kernels/LogisticFE.scala`,
`Serbin.scala`, `PoissonBinomial.scala`, `TaylorBins.scala`, or `engine/.../logistic/`, the chapters
citing them need a look and the probe should be rerun; a change declared non-behavioral (NUM-2) must
leave every printed number unchanged.

## Further reading

- Wu W, Yang Y, Kang J, He K (2022). Improving large-scale estimation and inference for profiling health
  care providers. *Statistics in Medicine* 41:2840–2853. SerBIN and the provider tests.
- He K, Kalbfleisch JD, Li Y, Li Y (2013). Evaluating hospital readmission rates in dialysis facilities;
  adjusting for hospital effects. *Lifetime Data Analysis* 19:490–512.
- Kalbfleisch JD, Wolfe RA (2013). On monitoring outcomes of medical providers. *Statistics in
  Biosciences* 5:286–302. Fixed versus random provider effects.
- Agresti A (2013). *Categorical Data Analysis*, 3rd ed. Wiley. Logistic regression, likelihood-ratio
  and score tests.
- Hong Y (2013). On computing the distribution function for the Poisson binomial distribution.
  *Computational Statistics and Data Analysis* 59:41–51. The FFT method pprof_py uses.
- Zeileis A, Köll S, Graham N (2020). Various versatile variances: an object-oriented implementation of
  clustered covariances in R. *Journal of Statistical Software* 95(1). The `vcovCL` reference.
