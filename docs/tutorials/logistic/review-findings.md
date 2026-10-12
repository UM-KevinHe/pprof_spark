# Review findings

[Tutorial home](README.md)

Writing this tutorial meant checking each claim against the code at `3e70739` and against pprof_py
v0.7.0 and R pprof 1.0.3 where the claim concerned them. These are the discrepancies, weaknesses and
gaps found. None changes the implementation; each is a proposal for the maintainer, registered as
OI-69 to OI-71 in [OPEN_ITEMS.md](../../../OPEN_ITEMS.md).

| ID | Finding | Kind | Severity | Open item |
|---|---|---|---|---|
| L-01 | The specification, ADR-0006 and OI-34 call for a fused pairwise cascade in the kernels; the logistic kernels use Neumaier sums | Documentation; possible cost | Low | Resolved in round 80 (OI-69) |
| L-02 | Provider-test results pass through the driver as m rows before becoming a table again | Performance at large m | Low to medium | OI-70 |
| L-03 | Every pass costs two Spark jobs, one of them to broadcast the γ table | Performance | Unknown until measured | OI-70 |
| L-04 | Covariate tests, provider tests and measures each rebuild the working set | Performance | Unknown until measured | OI-70 |
| L-05 | No test varies shuffle partitions, skews provider sizes, or uses p above 3 or m above 500 | Test coverage | Medium | Resolved in round 79 (OI-71) |
| L-06 | A single provider's block has no size cap | Robustness at scale | Low | OI-70 |

## L-01. Summation in the kernels

**What.** The estimation specification ([§11](../../spec/logistic/fixed-effect-estimation.md)) says
provider sums run "with the fused pairwise cascade (OI-34)"; ADR-0006 chose that cascade for kernels with
several sums per row because its measurements put Neumaier summation at 1.5 to 1.7 times a plain loop and
the fused cascade at 0.99 to 1.19; OI-34, still open, plans a reusable form. The logistic kernels
instead keep a Neumaier sum per statistic
([kernels/LogisticFE.scala lines 594–618](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L594-L618)),
as the Cox kernel does by its own specification.

**What the references do** (asked by the maintainer; read in their sources for this review):

- pprof_py v0.7.0, `algorithms/logistic/fixed_effect.py`, `_compute_scores_and_info`: gⱼ and hⱼ by
  `np.bincount` with weights, plain sequential sums in row order; bⱼ by a sparse provider-indicator
  product; g_β and C by dense products `X.T @ residuals` and `X.T @ qX`, whose order of addition is the
  BLAS library's; ℓ by `np.sum`, numpy's pairwise summation. The weight floor `np.maximum(q, 1e-20)`.
- R pprof 1.0.3, `src/Fixed_effect.cpp`, `logis_BIN_fe_prov`: gⱼ, hⱼ and bⱼ by Armadillo's `sum` over each
  provider's contiguous rows, which accumulates in two interleaved plain partial sums and adds them
  (`arrayops::accumulate`, unless compiled with fast-math); g_β, C and the Schur system by matrix
  products, through BLAS; the weight floor only for exact zeros.

Neither reference compensates or cascades. pprof_spark's Neumaier sums are at least as accurate as
either, and every difference is class D: on `lfe-base` the function-level values agree with pprof_py to
8.3e-16 or better.

**Recommendation.** Record the choice: amend the specification's §11 and ADR-0006's consequences to say
the Cox and logistic kernels use Neumaier sums, and close or re-scope OI-34. Implementing the cascade
would change bits (NUM-2) and should wait for a measurement showing the kernels are compute-bound.

**Status.** Round 80 recorded the choice: the 2a specification's §11 and an amendment to ADR-0006 now say Neumaier, and OI-34 is closed. The cost question is measured by the scale-test kit's kernel benchmarks ([docs/spec/scale/plan.md](../../spec/scale/plan.md) §7, OI-38).

## L-02. Provider-test results through the driver

**What.** `LogisticProviderTests.test` computes each provider's row on the executors, collects the m rows
to the driver, recreates them as a Dataset, joins the provider keys and sorts
([lines 108–178](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticProviderTests.scala#L108-L178)).
The specification's "nothing n-scale reaches the driver" holds, but the m-scale table makes a round trip:
about 10⁶ rows of a dozen values at the top of the envelope, bounded only by `maxProvidersOnDriver`.

**Recommendation.** Keep the table distributed (join the keys by group index without collecting), or
state the round trip in the specification.

## L-03. Two Spark jobs per pass

**What.** Each pass builds the γ table on the driver and joins it with a broadcast hint, which Spark runs
as a separate job before the pass; the probe saw a one-task job per pass with one block and a four-task
job with four. A Newton step costs four jobs instead of two, plus 16m bytes of effects, which the
specification accepted (§11).

**Recommendation.** Measure in the package-level scale test (D-28) before considering alternatives such
as shipping both passes' γ once per step.

## L-04. Repeated preparation

**What.** `covariateTests`, `LogisticProviderTests.test` (except Wald) and `LogisticStandardization`
rebuild the working set and check the fingerprint (`trainingWorkingSet`): nine Spark jobs and a shuffle
of every row each time, as observed for the provider tests. Covariate tests then refit SerBIN once per
tested covariate. This is the logistic counterpart of the Cox tutorial's F-05.

**Recommendation.** As F-05: measure, then consider a reusable prepared working set within a job.

## L-05. Test coverage

**What.** The suites test R0 (reversed rows, `repartition(7)`) and R1 (`targetBlockBytes` = 4,096), which
is more than the Cox suites do. They do not vary `spark.sql.shuffle.partitions`, use skewed provider
sizes (§9.6), use p above 3, or approach the envelope's m (`lfe-many` has 500 providers); whether any
fixture exercises a rejected line-search ladder was not checked.

**Recommendation.** Add R0 at shuffle partitions 1, 4 and 7 (the probe's check); a synthetic case with a
heavy-tailed provider-size distribution and one oversized provider; a case with p around 20; a case that
forces backtracking; and a large-m smoke test of the driver-side γ.

**Status.** Round 79 added `LogisticLayoutSuite`: many-block parity with pprof_py on every fixture; bitwise R0 at `spark.sql.shuffle.partitions` 1, 4 and 7 on reversed, repartitioned input, with exact tests, robust variances and the AUC; a skewed provider-size distribution with an oversized provider; the eight-step ladder against a sequential search from a start that needs a second ladder pass; twenty features against the in-memory SerBIN; and twenty thousand providers with the `maxProvidersOnDriver` guard.

## L-06. No cap on a provider's block

**What.** Cox fails a stratum above `maxStratumRows`; the logistic fit has no counterpart. A provider
larger than the block target gets a block of its own whatever its size, built as row objects in one task.

**Recommendation.** Document the behaviour, or add a guard with an actionable message, as for Cox.
