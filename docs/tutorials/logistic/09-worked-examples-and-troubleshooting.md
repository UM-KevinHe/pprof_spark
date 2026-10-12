# 9. Worked example and troubleshooting

[Tutorial home](README.md) · previous: [8. Performance and limitations](08-performance-and-limitations.md)

This chapter follows the `lfe-base` fixture through every calculation of chapters 1 to 6, in the order
the code performs them, and compares each result with the stored outputs of pprof_py and R. The numbers
come from the [probe](probe/README.md), which runs the real kernels in memory and the real engine in
local Spark; the reference values come from
[pprof_py.json](../../../fixtures/logistic/lfe-base/pprof_py.json) and
[r_logistic.json](../../../fixtures/logistic/lfe-base/r_logistic.json).

## 9.1 The data

1,075 records of 30 providers (keys 1 to 30, 11 to 60 records each), 513 events, Bernoulli outcomes, and
three features: x1 and x2 continuous on a 1/64 grid, x3 binary
([input.csv](../../../fixtures/logistic/lfe-base/input.csv)). Every provider has more than 10 records, so
all 30 are fitted, and with p = 3 a block holds up to 74,898 rows: one block.

## 9.2 The start

ȳ = 513/1,075 = 0.47721, so every γⱼ starts at logit(0.47721) = −0.0912260047 and β at 0. There ℓ is
−744.01608789103620, bitwise equal to pprof_py's; every score, information block and the Schur
complement agree with pprof_py to 8.3e-16 or better, and so do they at the fixture's second point,
β = (0.375, −0.1875, 0.0625), where ℓ = −733.40509582151760, again bitwise equal.

## 9.3 The iterations

| Step | ℓ at the step's start | Step length v | ‖Δβ‖∞ (the criterion) | β after the step |
|---:|---:|---:|---:|---|
| 1 | −744.016088 | 1 | 7.676e-01 | (0.43377, −0.20643, 0.76764) |
| 2 | −665.289575 | 1 | 9.688e-02 | (0.49718, −0.23648, 0.86453) |
| 3 | −664.208259 | 1 | 3.951e-03 | (0.49986, −0.23778, 0.86848) |
| 4 | −664.206459269430 | 1 | 7.031e-06 | (0.49987, −0.23778, 0.86848) |
| 5 | −664.206459263065 | 1 | 2.406e-11 | (0.49987, −0.23778, 0.86848) |

Every full Newton step passed the Armijo test at the first of the eight step lengths, and the criterion
fell roughly quadratically until it was below 1e-8 after step 5: five steps, as pprof_py's. Each iterate
agrees with pprof_py's to 1.4e-15 or better.

## 9.4 The estimates and covariate inference

| | x1 | x2 | x3 |
|---|---:|---:|---:|
| β̂ | 0.49986733434839165 | −0.23778332739170122 | 0.86848473374438220 |
| SE | 0.071535 | 0.068736 | 0.139919 |
| Wald z | 6.988 | −3.459 | 6.207 |
| Likelihood ratio (fixture) | 52.538 | 12.159 | 39.641 |
| Score (fixture) | 51.375 | 12.103 | 39.400 |

β̂ is bitwise equal to R pprof's SerBIN at the default tolerance, and within 6.4e-17 of pprof_py's; a
tight fit (tol 1e-13, six steps) is within 6.4e-16 of `glm`. The variances agree with pprof_py to
2.0e-16 and with `glm` to 3.2e-14. ℓ̂ = −664.20645926306520, AIC = 2(3 + 30) − 2ℓ̂ = 1394.413, and the AUC
is 0.71106393900924710, equal to pprof_py's and R's. The likelihood-ratio and score statistics are the
fixture's (pprof_py and R agree to every digit stored); the probe did not recompute them.

## 9.5 Provider effects

The median effect is −0.364694. Provider 25 has the largest gap between its two variances:
γ̂ = −1.6105, Var(γ̂) = 0.1984, and Var(γ̂ + x̄ᵀβ̂) = 0.1910 at the average case mix (chapter 2,
section 2.9).

## 9.6 Provider tests

The exact two-sided test against the median flags 2 providers above and 4 below, with every flag equal
to pprof_py's and R pprof's (chapter 4, section 4.6). Provider 22 has 17 observed events against 28.196
expected at the median effect: z = −3.110, p = 0.0019, and the exact 95% interval for its effect is
[−1.890, −0.692] around γ̂ = −1.275.

## 9.7 Standardized measures

For the same provider, the indirect ratio is O/E = 17/28.196 = 0.603 (R's `SM_output.logis_fe`: 0.60292)
and R's direct rate is 30.49%, against a crude rate of 47.72% (chapter 5).

## 9.8 The same fit in Spark

`LogisticFE.fit` on the 1,075 rows as a DataFrame returned β̂, γ̂ and ℓ bitwise equal to the in-memory
calculation, in 40 Spark jobs: 11 to prepare, 20 for the 10 passes of the 5 Newton steps (a small job
broadcasting the γ table, then the pass), 4 for the variances and 5 for the AUC (chapter 6, section 6.4).
Laid out in four blocks of about 270 rows, β̂ and ℓ were bitwise equal to the one-block fit and γ̂ was
within 1.4e-16; and with `spark.sql.shuffle.partitions` at 1 and 7, on reversed and repartitioned input,
every bit was the same.

## 9.9 Troubleshooting

| Message or symptom | Likely cause | What to do |
|---|---|---|
| `InvalidInputException` with column names and counts | Invalid outcomes, trials, features, providers or row identifiers (chapter 1, section 1.10) | Fix the data; values are never printed (NN-7) |
| No providers fitted | Every provider has at most `minRecords` records | Lower `minRecords` or set `screen = false` |
| No outcome variation | No events, or only events, in the whole input | Check the outcome coding (X-019) |
| "… providers exceed maxProvidersOnDriver …" | More than 10⁷ fitted providers | Raise the limit if the driver has the memory; a distributed γ is OI-50 |
| `LogisticAliasingException` naming features | A feature constant within every provider, or collinear features | Remove or combine them (chapter 1, section 1.9) |
| "the SerBIN fit did not converge in … steps" | The step cap, or a fit dominated by degenerate providers | Read the trace; raise `maxIter` |
| "the line search shortened the last step …" | Convergence by a small step, not a small Newton step | Treat the estimates with care; refit with a smaller `tol` |
| "… providers have no events or only events …" | Degenerate providers held at the bound (X-018) | Expected; their effects are not estimates, and their Wald tests warn |
| "features … have an absolute correlation above 0.9" | Strongly correlated features (X-021) | Informational; consider dropping one |
| "robust Wald tests need a fit with a cluster column" | No cluster column in the spec | Refit with `cluster` |
| "… they need at least two features" | A likelihood-ratio or score test with one feature | Use the Wald test |
| "the data differ from the fit's (fingerprint) …" | Covariate tests, provider tests or measures given other data | Pass exactly the training data (API-3) |
| Prediction fails with counts of unknown providers | Rows of providers not in the fit (X-022) | Predict only for fitted providers |
| The exact test fails for a binomial provider | More than 20,000 trials in one provider | Use the score or bootstrap test |
| One task much slower than the others | A dominant provider, or exact tests on large providers (OI-57) | Chapter 8, sections 8.3 and 8.4 |
| Results differ between runs | An R0 violation | A defect: chapter 7, section 7.5 |
