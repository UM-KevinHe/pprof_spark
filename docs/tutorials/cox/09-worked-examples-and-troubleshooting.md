# 9. Worked examples and troubleshooting

[Tutorial home](README.md) · previous: [8. Performance and limitations](08-performance-and-limitations.md)

This chapter follows one small data set through every calculation the implementation performs, in
the order the code performs them, first inside the kernel and then inside Spark. It then distributes
a stratified version across two blocks, shows how the optimizer behaves on data that break it, and
ends with a troubleshooting table. All numbers come from the [probe](probe/README.md) unless they
are marked as hand calculations.

## 9.1 Example 1: tiny-ties, step by step

The data are the eight rows of chapter 1, section 1.1: one stratum, one covariate, Breslow ties
(the default), no weights, offsets or entry times.

**Step 1: validation and layout.** Eight rows, six events, no invalid values. One stratum of 8 rows
with p = 1: T = ⌊4,194,304 / (8·5)⌋ = 104,857 rows per block, so one block. The fit records
`LayoutSummary(104857, 1, 0, 8, 8)`: rows per block, blocks, oversized strata, largest and smallest
block.

**Step 2: canonical order.** Exit time descending, events first, then row identifier:

| Position | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|---|
| Row id | 7 | 6 | 4 | 5 | 3 | 2 | 0 | 1 |
| Time | 4 | 4 | 3 | 3 | 2 | 2 | 1 | 1 |
| Event | 1 | 0 | 1 | 1 | 1 | 0 | 1 | 1 |
| x1 | −0.5 | 0.25 | −1.0 | 0.75 | 0.0 | 1.0 | 0.5 | −0.25 |

With no entry column every entry is 0, so `entryOrder` is just 0 to 7 and no row is ever removed.

**Step 3: the summary pass.** Column sum 0.75, so the center is 0.75/8 = 0.09375. The centered
covariates xc = x1 − 0.09375 are −0.59375, 0.15625, −1.09375, 0.65625, −0.09375, 0.90625, 0.40625 and
−0.34375 in canonical order.

**Step 4: the first evaluation, at β = 0.** Every risk score is exp(0) = 1. This hand trace shows
what the kernel's running sums hold after each time group has joined (centered coordinates, exact
because every value is a short binary fraction):

| Time group | Rows joining | S₀ | S₁ (centered) | S₂ (centered) | Events | ℓ term | U term | I term |
|---|---|---:|---:|---:|---|---|---:|---:|
| t = 4 | 7, 6 | 2 | −0.4375 | 0.376953125 | 7 | −log 2 | −0.59375 − (−0.4375/2) = −0.375 | 0.376953125/2 − 0.21875² = 0.140625 |
| t = 3 | 4, 5 | 4 | −0.875 | 2.00390625 | 4, 5 | −2 log 4 | −0.4375 − 2(−0.875/4) = 0 | 2(2.00390625/4 − 0.21875²) = 0.90625 |
| t = 2 | 3, 2 | 6 | −0.0625 | 2.833984375 | 3 | −log 6 | −0.09375 − (−0.0625/6) = −0.083333 | 2.833984375/6 − 0.0104167² = 0.472222 |
| t = 1 | 0, 1 | 8 | 0 | 3.1171875 | 0, 1 | −2 log 8 | 0.0625 − 2·0 = 0.0625 | 2(3.1171875/8 − 0) = 0.779297 |

The U and I terms equal the uncentered ones of chapter 2, section 2.8, term by term, as the
invariance of section 2.7 requires; and at t = 1, where every row is at risk, S₁ is exactly zero,
because the center is the mean of the full risk set. The kernel adds the terms in this sweep order:
ℓ(0) = −9.4163784553874520, U(0) = −0.39583333333333330, I(0) = 2.2983940972222223, equal to R's
stored values in ℓ and I and one unit in the last place from R's U.

**Step 5: Newton.** U(0)/I(0) gives the first step. Three iterations:

| k | βₖ | ℓ(βₖ) | Relative change |
|---:|---|---|---|
| 1 | −0.17222169766783116 | −9.3824022346465930 | 3.6e-3 |
| 2 | −0.17151287926569106 | −9.3824016540412100 | 6.2e-8 |
| 3 | −0.17151288287930955 | −9.3824016540412100 | 0, converged |

**Step 6: inference.** The final evaluation's information, I(β̂) = 2.3112502578650234, is factored
on the driver: V = 0.43266625783905050, SE = 0.65777371324723100, z = −0.2607, p = 0.7943, interval
[−1.4607, 1.1177], hazard ratio 0.842 (interval 0.232 to 3.058).

**Step 7: after the fit.** `CoxPH.baseline` gives the increments 0.252590, 0.167888, 0.486149 and
0.488384 at t = 1 to 4 (chapter 3, section 3.10); `CoxPH.residuals` gives the martingale, score and
dfbeta residuals of section 3.12; per-row robust variance gives SE 0.3604.

**How it compares:**

| Quantity | pprof_spark | R survival 3.5-8 | pprof_py 0.7.0 |
|---|---|---|---|
| β̂ (Breslow) | −0.17151288287930955 | −0.17151288287930977 (1.3e-15) | −0.17151288107250043 (1.05e-8: X-010) |
| SE | 0.65777371324723100 | identical | 0.65777371323883560 (1.3e-11) |
| ℓ(β̂) | −9.3824016540412100 | identical | identical |
| ℓ(0) | −9.4163784553874520 | identical | −9.4163784553874540 (1.9e-16) |
| β̂ (Efron) | −0.17470774610210338 | −0.17470774610210324 (7.9e-16) | −0.17470774500205070 (6.3e-9: X-010) |
| Iterations, both methods | 3 | — | 3 |

## 9.2 The same fit in Spark

`CoxPH.fit` on the eight rows as a DataFrame returned the same β̂ and ℓ **bit for bit** as the
kernel-and-Newton calculation of section 9.1: one block means one partial per evaluation, and adding
a single partial with Neumaier sums returns it unchanged. Spark ran fifteen jobs (chapter 5, section
5.7): eleven to validate, plan, build the working set and summarize it, and four evaluations. The
shuffles moved a few hundred bytes; the evaluations moved none. The eight rows lived in one block,
so one task did each evaluation's real work while the other three tasks of each job found empty
partitions.

## 9.3 Example 2: three strata in two blocks

Adding two strata to tiny-ties (chapter 5, section 5.5) gives 16 rows in strata of 8, 5 and 3 rows.
With `targetBlockBytes = 400` the plan puts stratum 10 in block 0 and strata 20 and 30 in block 1.
At the converged β̂ = −0.370386, block 0 reports U = +0.4574 and block 1 U = −0.4574; the driver's
ordered sum is −1.2e-14. ℓ̂ = −13.959361073667552 and β̂ were bitwise identical with the one-block
layout, and stayed bitwise identical when the probe rebuilt the working set with 1 or 7 shuffle
partitions from reversed, repartitioned input.

Reading the numbers back to the formulas: each block's ℓ is the sum of its strata's ℓ terms, each a
sum over that stratum's event times exactly as in section 9.1; nothing crosses between strata except
β, which the driver chooses.

## 9.4 Example 3: data that break the optimizer

All from the probe, calling the real optimizer and kernel (chapter 3, section 3.6 for discussion):

| Data | Result |
|---|---|
| Six rows where the failing row always has the larger x (monotone likelihood) | Defaults: `converged = false` at the cap, β = 21.2, SE 23,205. With ε = 1e-11 and 100 iterations: `converged = true` at β = 27.2, SE 466,096, no warning |
| tiny-ties with every offset 700 or −720 | Correct: β̂ = −0.1715128829 |
| tiny-ties with every offset 710 | "the objective is not finite at the starting values" |
| tiny-ties with every offset −745 | 240 halvings, `converged = false`, β = −0.1519 (wrong) |
| tiny-ties with every offset −800 | "the objective is not finite at the starting values" |
| x1 × 10⁶ alongside a covariate in [−0.01, 0.01] | "columns 1 are aliased", though they are not collinear; × 10³ fits |
| Newton started at β = 8 (not reachable from `fit`) | Iteration 1 needs 3 halvings and iteration 2 four; converges to β̂ in 7 iterations |
| `maxIterations = 1` | `converged = false`, β = −0.1722, a warning |

## 9.5 Reproducing these numbers

[probe/README.md](probe/README.md) explains how to compile the modules and run the two probe
programs. Rerun them when the files listed in the README's "Keeping this tutorial in sync" change. A
non-behavioral change must leave every printed number unchanged in every digit.

## 9.6 Troubleshooting

| Message or symptom | Likely cause | What to do |
|---|---|---|
| `InvalidInputException` listing problems (`NonPositiveValues`, `NonBinaryValues`, `InvalidValues`, `NegativeValues`, `EntryNotBeforeExit`, `DuplicateRowIds`, `MissingColumn`, `UnsupportedType`, `EmptyInput`) | The listed column and count | Fix the data; counts, never values, are reported (NN-7) |
| `NoEvents` | No event with positive weight | Check the event coding (1 or true) and the weights |
| "the input has more than … groups, the limit set by BlockOptions.maxGroupsOnDriver" | More than 2,000,000 strata | Raise the limit if the driver has the memory |
| "stratum … has … rows, more than maxStratumRows …" | A stratum larger than 1,000,000 rows, often an unstratified fit | Raise `maxStratumRows` and accept one task for that stratum, or stratify; TimeRange, which would split it, is not implemented (OI-03) |
| "… block partials take about … bytes on the driver, more than BlockOptions.driverBudgetBytes" | Large p or very many blocks | Raise `targetBlockBytes` or `driverBudgetBytes` (chapter 8, section 8.4) |
| "covariates … are aliased" | A constant or collinear covariate, or one on a very different scale | Remove or combine; rescale covariates to comparable units |
| "the objective is not finite at the starting values" | Linear predictors or offsets beyond about ±709 | Center or rescale offsets; chapter 3, section 3.6 |
| "the objective is not finite after … step halvings" | Overflow along the Newton direction | As above; check for separation |
| "the Cox fit did not converge: reached … iterations …" | Slow convergence, or monotone likelihood | Look at the iteration log; a coefficient growing by about 1 per iteration with a huge SE means separation |
| A converged fit with SE many times the coefficient | Monotone likelihood not detected (F-02) | Treat the coefficient as infinite; examine the covariate's events |
| "robust variance treats each row as its own cluster, but the data have entry times" | Per-row robust variance on counting-process data | Set `CoxSpec.cluster` to the subject identifier |
| "the data differ from the data the model was fitted to (fingerprint mismatch, API-3)" | `baseline`, `residuals` or measures called with different data | Pass exactly the fitted DataFrame |
| "residuals need CoxSpec.rowId" | No row identifier in the spec | Refit with `rowId` |
| "more than … distinct entry and exit times" (measures) | Too many distinct times for the driver | Raise `maxGroupsOnDriver`, or coarsen times |
| "built … blocks; the plan has …" or "the blocks hold … rows and … events" | An internal inconsistency | A defect: report it with the layout summary |
| Results differ from R | Usually ties, near-tie merging or R's robust default | Chapter 7, sections 7.8 and 7.9 |
| Results differ between runs of pprof_spark | An R0 violation | A defect: chapter 7, section 7.9 |
| One executor busy, the others idle | An unstratified fit or one dominant stratum | Chapter 8, section 8.3 |
| Many empty tasks in every pass | `spark.sql.shuffle.partitions` far above the block count | Chapter 8, section 8.3 |
