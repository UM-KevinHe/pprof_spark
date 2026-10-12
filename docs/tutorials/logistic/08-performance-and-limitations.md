# 8. Performance considerations and limitations

[Tutorial home](README.md) · previous: [7. Validation](07-validation-and-numerical-accuracy.md) ·
next: [9. Worked examples and troubleshooting](09-worked-examples-and-troubleshooting.md)

As for Cox, no performance measurement of the logistic model exists yet; the numbers below are derived
from the code or observed on `lfe-base` in local mode, and say which.

## 8.1 The cost of a fit

| Phase | Spark jobs (observed on lfe-base) | Compute |
|---|---|---|
| Preparation: validation, provider sizes, placement, build, summary | 11 | One shuffle of every row |
| Each Newton step | 4 when the full step is accepted, 2 more per rejected ladder | Pass A O(n·p²); pass B O(n·p) for Δγ plus 8 log-likelihoods |
| Variances | 4 | Two passes |
| Robust variances (with clusters) | 4 | Two passes |
| AUC | about 5 | One sort of (π, y) |

`lfe-base` took 5 Newton steps, 10 passes and 40 jobs in all. The passes themselves shuffle nothing.

## 8.2 The driver

The driver holds γ and Δγ (m doubles each), sorts m values for the median at every step, builds the
γ table twice per step, and receives B partials of p(p+1)/2 + p + 2 doubles per pass. Two guards apply:

- `maxProvidersOnDriver` = 10⁷ bounds m ([engine LogisticFE.scala lines 77–81](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L77-L81));
- B × 8 × (p(p+1)/2 + p + 2) bytes must fit in `driverBudgetBytes` = 64 MiB (lines 83–87), the same
  formula as Cox.

With rows per block T = ⌊4 MiB / (8(p + 4))⌋, the second guard caps a fit at the default options, if
blocks are full (computed from the code's formulas, not measured):

| p | Rows per block | Largest B | Largest n ≈ B·T |
|---:|---:|---:|---:|
| 3 | 74,898 | 762,600 | 5.7 × 10¹⁰ |
| 20 | 21,845 | 36,158 | 7.9 × 10⁸ |
| 50 | 9,709 | 6,321 | 6.1 × 10⁷ |
| 100 | 5,041 | 1,628 | 8.2 × 10⁶ |

Raising `targetBlockBytes` or `driverBudgetBytes` raises the cap, as for Cox.

## 8.3 Parallelism and skew

Work is parallel across providers: a block holds whole providers, so the time of a pass is at least the
time of the largest provider's block. A provider larger than the target gets a block of its own, and
unlike Cox's `maxStratumRows` there is no cap on its size: one very large provider becomes one large
block, built as row objects in one task (review finding L-06). As for Cox, the number of tasks per pass
is the build's `spark.sql.shuffle.partitions`, and blocks reach tasks by a hash of their identifiers.

## 8.4 After the fit

| Computation | Cost |
|---|---|
| Likelihood-ratio and score tests | A rebuild, then a complete SerBIN refit per tested covariate |
| Exact provider tests | O(nⱼ·min(oⱼ, nⱼ − oⱼ)) per tail evaluation, and roughly 40 to 80 evaluations for the two limits; a provider with 10,000 records and 2,000 events needs about 2 × 10⁷ operations per evaluation (estimate) |
| Bootstrap tests | 10,000 × nⱼ draws per provider |
| Direct standardization | One pass for bin moments, a shuffle by bin, then O(m × bins × 9) for the providers; the `exact` method is O(n·m) |
| Provider-test tables | m result rows collected to the driver and made a table again (L-02) |

Blocks are packed by records, not by the cost of exact tests, so a few large providers can make
stragglers (OI-57).

## 8.5 Repeated preparation

`covariateTests`, `LogisticProviderTests.test` and `LogisticStandardization` each rebuild the working
set and check the fingerprint, because a fit holds no data (ARCH-4, API-3). A `LogisticJob` asking for
every output pays the preparation once per output (L-04, as Cox's F-05).

## 8.6 Limitations

Statistical: no offsets or case weights in this model; clusters nested in providers only (X-023); no
external standard population (OI-58); the exact test is limited to 20,000 trials per provider;
provider effects are fixed effects, with no shrinkage for small providers beyond screening.

Computational: γ on the driver, bounded by `maxProvidersOnDriver` (OI-50); two Spark jobs per pass; no
cap on a single provider's block; task counts that follow shuffle partitions; repeated preparation; no
measured performance.
