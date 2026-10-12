# Review findings

[Tutorial home](README.md)

Writing the tutorial meant checking each claim against the code at `efb6329`. These are the
discrepancies, weaknesses and gaps that check found. None of them changes the implementation; the
tutorial documents behavior as it is. Each finding is a proposal for the maintainer, who decides
(§15). The actionable ones are registered as OI-64 to OI-68 in [OPEN_ITEMS.md](../../../OPEN_ITEMS.md).

| ID | Finding | Kind | Severity | Open item |
|---|---|---|---|---|
| F-01 | PROJECT_CONTEXT still calls the step control bug-compatible with pprof_py | Documentation | Medium: misleads readers about a parity-relevant behavior | OI-64 |
| F-02 | Monotone likelihood (infinite coefficients) is not detected | Robustness gap (NN-10) | Medium | OI-65 |
| F-03 | No η shift or covariate scaling; extreme offsets or scales fail or mislead; the context overstates the protection | Robustness gap and documentation | Low to medium | OI-66 |
| F-04 | One stratum is one task; unstratified fits are capped at 1,000,000 rows by default; the error message names a closed phase | Scalability limit and documentation | High for unstratified uses | OI-64 (message); OI-03, OI-30 |
| F-05 | Every call rebuilds the working set; a full `CoxJob` prepares the data 5 to 7 times | Performance | Unknown until measured | OI-68 |
| F-06 | Tasks per pass follow `spark.sql.shuffle.partitions`, not the blocks | Performance | Unknown until measured | OI-68 |
| F-07 | Tests do not exercise multi-block parity, shuffle-partition changes, skew, larger p or degenerate numerics | Test coverage | Medium | OI-67 |
| F-08 | ADR-0003 says compensated sums stay out of per-row loops; the Cox kernel uses them there | Documentation | Low | OI-64 |
| F-09 | The Phase 1c specification describes one shuffle for clustered robust variance; the code uses two | Documentation | Low | OI-64 |
| F-10 | Stale labels: a "draft" specification that is approved; a `CoxPHEstimator` and capability traits that do not exist | Documentation | Low | OI-64 |
| F-11 | One parity comparison passes with 0.5% of its tolerance to spare | Awareness | Low | — |
| F-12 | Minor efficiency and accounting items | Performance | Low | — |

## F-01. PROJECT_CONTEXT describes the corrected step control as bug-compatible

**What.** PROJECT_CONTEXT v2.6 §3.4 (row "`CoxPH` tests for a log-likelihood decrease, and halves the
step, before testing convergence") says "pprof_spark follows pprof_py (class B, bug-compatible,
flagged)", and §7.3.2 (row "Convergence") says the same.

**Evidence to the contrary.** The approved specification
([first-slice.md](../../spec/cox/first-slice.md) §5 and §10) requires convergence to be tested on the
full step before any halving; X-010 in DISCREPANCIES.md is "Corrected, approved 2026-10-06 (D-20),
superseding round 6's bug-compatible choice"; the code does it at
[Newton.scala line 74](../../../numerics/src/main/scala/pprof/spark/numerics/Newton.scala#L74); and
`NewtonSuite` tests it ("convergence is tested on the full step before any halving (X-010)"). The
probe reproduces the effect: on tiny-ties pprof_spark matches R to 1.3e-15 and differs from pprof_py
by exactly half the final step (chapter 3, section 3.4).

**Recommendation.** Correct both rows in the next revision of PROJECT_CONTEXT. §0 gives the
specification and decisions precedence and asks for the context to be corrected when they conflict.

## F-02. Monotone likelihood is not detected

**What.** When a covariate separates events from survivors at every event time, ℓ increases toward a
finite limit as the coefficient goes to infinity, and the maximum-likelihood estimate is infinite.
Nothing in the Cox code detects this (no "infinite", "monotone" or `toler.inf` handling anywhere in
`engine/…/cox` or `Newton.scala`), although PROJECT_CONTEXT §9.5 says "Perfect separation; monotone
likelihood: Detected and reported with non-convergence status" and §7.2 lists "Infinite or boundary
estimates: Detection rule; reporting".

**Evidence.** The probe's six-row example (chapter 3, section 3.6). With default options the fit
reaches the 20-iteration cap with β = 21.2 and SE 23,205, and is reported as not converged only
because the cap comes one iteration before the relative change in ℓ falls below 1e-9. With
ε = 1e-11 and 100 iterations it reports `converged = true` at β = 27.2 and SE 466,096, with no warning.
R's `coxph` warns in this situation. pprof_py's behavior was not probed.

**Recommendation.** A specification addendum (NN-2) defining a detection rule and its reporting:
for example R's approach of flagging coefficients that are still moving relative to their size at
convergence, or the joint signature of a growing |β̂| and a collapsing information. Probe pprof_py and
classify the difference before implementing.

## F-03. Extreme offsets, linear predictors and covariate scales

**What.** The fit kernel centers covariates by their column means and does nothing else: no shift of
η by its maximum within a stratum, no scaling of covariates, no condition estimate in the result.
PROJECT_CONTEXT §8.3 says covariates are "scaled when poorly conditioned", that exponentials are
"formed after per-stratum or per-provider shifts", and that condition numbers are reported. The
provider-measures path does shift by the global maximum η; the fit does not.

**Evidence** (probe, chapter 3, section 3.6). Adding the same offset to every row leaves the partial
likelihood unchanged, yet an offset of 710 fails with "the objective is not finite at the starting
values", −745 produces subnormal exponentials, 240 halvings and a wrong, non-converged β (−0.1519
instead of −0.1715), and −800 fails. Multiplying one covariate by 10⁶ next to a covariate of order
0.01 is reported as aliasing, because the Cholesky threshold is relative to the largest diagonal.

**Impact.** Typical data have |η| far below 700 and covariates in comparable units, so practical
impact is low; but the failures are reported as aliasing or non-finite objectives, which points
users in the wrong direction.

**Recommendation.** Either (a) shift each stratum's η by its maximum before exponentiating, which is
exact for ℓ, U and I but changes bits and so needs NUM-2 evidence and a re-comparison with the
fixtures; and scale covariates internally, or check the ratio of scales and say so in the aliasing
message; and record the Cholesky pivots or a condition estimate in `CoxFit`; or (b) amend §8.3 to
describe what is implemented and document the limits in the user guide.

## F-04. One stratum, one task; a stale message

**What.** StratumLocal keeps each stratum whole, and TimeRange, which would split large strata, does
not exist (OI-03, OI-30). An unstratified fit is therefore one block, swept by one task on every
pass, and it fails above `maxStratumRows` (1,000,000 rows by default). The failure message says "a
stratum must fit in one block until TimeRange layouts arrive in Phase 1b"
([CoxPH.scala lines 326–332](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L326-L332));
Phase 1b closed without TimeRange.

**Recommendation.** Correct the message now (it changes no result; `CoxPHSuite` only checks that it
mentions TimeRange). Document the limit in the user guide. Schedule TimeRange, or an event-grid
aggregation for small p·K, before unstratified fits at national scale are needed.

## F-05. Repeated preparation of the working set

**What.** `fit`, `baseline`, `residuals`, `evaluateAt`, `requireFittedData`, `CoxMeasures.standardized`
and, through it, `CoxProviderTests.test` each call `withWorkingSet`, which validates the data,
collects stratum sizes, plans, shuffles every row into blocks, persists them and runs a summary pass.
That follows from ARCH-4 and API-3. The probe saw `evaluateAt` repeat all eleven preparation jobs. A
`CoxJob` with every output prepares the data five times, or seven when the provider column is not the
stratum column (chapter 8, section 8.5).

**Recommendation.** Measure it in the package-level scale test. If it matters, let a job reuse one
prepared working set across calls while keeping the fingerprint check, for example through a
`Prepared` handle released in a `finally` block by the caller.

## F-06. Tasks per pass are decoupled from the blocks

**What.** The persisted working set keeps the partition count of the build's shuffle,
`spark.sql.shuffle.partitions`, and blocks reach partitions by a hash of their identifier. The probe
saw 1, 4 and 7 tasks per evaluation for the same one-block fit as the setting changed. The bin
packing balances blocks, not tasks.

**Recommendation.** Document the setting's role now. Consider repartitioning the working set
deterministically by block identifier into a partition count derived from B and the cluster, recorded
in the layout summary, and measure both.

## F-07. Test coverage

**What.** With the default 4 MiB block target, every fixture is a single block (74,898 rows per block
at p = 3), so the parity suites exercise single-block reduction only; the R1 test is the only one that
splits a fixture into several blocks, and it checks β̂ and ℓ within T-part. No test varies
`spark.sql.shuffle.partitions` or uses skewed stratum sizes, which PROJECT_CONTEXT §9.6 asks of every
engine test. Every multi-covariate fixture has p = 3. Monotone likelihood, extreme offsets, badly
scaled covariates, near-tied times and heavy truncation are untested.

**Recommendation.** The tests listed in chapter 7, section 7.10.

## F-08. ADR-0003's stated consequence no longer holds for Cox

**What.** ADR-0003 says Neumaier summation "is used where partials are combined across blocks, not
in per-row inner loops". The Cox specification later required compensated running sums, and
`CoxStratum.add` adds every row to S₀, S₁ and S₂ with Neumaier sums. OI-38 already tracks the cost.

**Recommendation.** Amend the ADR's consequences with a pointer to the Cox specification and OI-38.

## F-09. Clustered robust variance: one shuffle in the specification, two in the code

**What.** [residuals-robust.md](../../spec/cox/residuals-robust.md) §4 describes "one shuffle by
cluster" followed by bucketing. The code groups by cluster and then groups the cluster sums by bucket,
a second shuffle of one p-vector per cluster
([CoxPH.scala lines 390–406](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L390-L406)).
The result is unaffected.

**Recommendation.** Amend the specification's distributed plan.

## F-10. Stale labels

**What.** `docs/parity/matrix.md` lists the counting-process specification as "(draft, D-23)", but
it is approved (D-23). PROJECT_CONTEXT §3.6 shows a `CoxPHEstimator` and §6.9 capability traits;
neither exists at `efb6329` (the `ml` module has no Cox code).

**Recommendation.** Mark the matrix entry approved and the two context examples as planned.

## F-11. A comparison that passes with little to spare

**What.** On tiny-ties with Breslow ties, pprof_py and R differ by 0.995 of T-coef in the calibration,
and pprof_spark's tight and default estimates differ from pprof_py's by the same 0.995. The cause is
X-010's half step, understood and registered.

**Recommendation.** No change needed. A note in `cox-calibration.md` saying that this row is X-010
would let a future failure there be read correctly.

## F-12. Minor efficiency and accounting items

- Every evaluation computes U and I, including halving trials that only need ℓ.
- `targetRowsPerBlock` counts p + 4 doubles per row; a `CoxBlock` row holds about p + 6 values plus
  identifiers, so blocks exceed the nominal target.
- `CoxMeasures.standardized` evaluates its provider rows twice: once for the persisted tables and
  once to count providers with no expected events.
- The block builder holds a block's rows as objects while sorting, so an oversized stratum's build
  uses several times its final memory.

**Recommendation.** Address when profiling shows the need.
