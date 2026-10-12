# 7. Validation and numerical accuracy

[Tutorial home](README.md) · previous: [6. Distributed execution](06-distributed-execution.md) ·
next: [8. Performance and limitations](08-performance-and-limitations.md)

The logistic slices follow the Cox pattern (Cox tutorial, [chapter 7](../cox/07-validation-and-numerical-accuracy.md)):
exact reference fixtures, named tolerance classes, three levels of parity, negative controls,
invariance tests, and the bitwise reproducibility contract. This chapter lists what is specific to the
logistic model, what the tutorial's probe confirmed, and where the evidence stops.

## 7.1 Reference fixtures

The fixtures in [fixtures/logistic/](../../../fixtures/logistic/) are synthetic and exact in text
(features on a 1/64 grid or binary, integer outcomes and trials; ADR-0005):

| Case | Content |
|---|---|
| lfe-base | 30 providers of 11 to 60 records, 1,075 records, three features (two continuous, one binary), Bernoulli |
| lfe-degenerate | lfe-base's design plus providers with no events, with only events, and with 8 and 10 records |
| lfe-shifted | lfe-base with features shifted by +50 and −30 |
| lfe-binomial | 30 providers, rows with 1 to 20 trials |
| lfe-many | 500 providers of 11 to 30 records |
| lfe-clustered | A patient column with repeated rows and patients seen by two providers |
| poibin-tails.json | Poisson-binomial tails of fixed probability vectors from mpmath at 60 digits |

The references are pprof_py v0.7.0 (`9320766`), R 4.3.3's `glm` for the maximum-likelihood estimates,
R pprof 1.0.3's own SerBIN (`logis_BIN_fe_prov`), its `test.logis_fe` and `SM_output.logis_fe`,
sandwich's `vcovCL` for robust variances, `anova` for likelihood-ratio and Rao tests, and mpmath for
exact tails. Every double is stored as a hexadecimal string. The calibration
([logistic-calibration.md](../../parity/logistic-calibration.md)) shows the references agreeing within
every class and the negative controls (bound 5 instead of 10, trials dropped, one outcome flipped, the
reference shifted by 0.01, one-sided alternatives swapped, scales swapped) missing by at least ten
times.

## 7.2 What the tests compare

| Level | Comparison | Suite |
|---|---|---|
| Function | ℓ, gⱼ, hⱼ, bⱼ, g_β, C and S at the start and at fixed (γ, β), T-fn | `LogisticKernelParitySuite` |
| Lockstep | Iterates of steps 1 to 5 from the same start, T-iter | `LogisticKernelParitySuite` |
| End to end | Default fits (step count, β̂, γ̂, variances, Wald, ℓ, AIC, BIC) against pprof_py; tight fits against pprof_py, R SerBIN and `glm` | `LogisticFESuite` |
| Inference | Wald variants, LR and score tests, AUC, predictions, robust variances | `LogisticInferenceSuite` |
| Provider tests | Exact, score, Wald and bootstrap against pprof_py and R; tails against mpmath | `LogisticProviderTestsSuite` |
| Measures | Indirect and direct measures, standard errors and tests against pprof_py and R; binned against exact sums | `LogisticStandardizationSuite` |
| Behaviour | Binomial rows against expanded Bernoulli rows; invalid inputs; aliasing; degenerate and screened providers; `maxIter = 0`; the provider table; the correlation warning; R0 and R1 | `LogisticBehaviourSuite` |
| Kernels | Derivatives of ℓ; the centred identity; shift invariance; numpy's median and bound; convergence; a provider at the bound; aliasing; blocks in order | numerics `LogisticFESuite` |

On `lfe-shifted`, pprof_py forms S by subtracting C − Σbbᵀ/h and loses about three digits (1.38 of T-fn
against the centred S at the start), so the function-level test checks S there through C, b and h, the
iterates and the estimates (specification §14, note 3).

## 7.3 What the probe confirmed on lfe-base

| Quantity | Agreement (largest difference over the largest magnitude) |
|---|---|
| ℓ at the start and at the fixed parameters | Bitwise equal to pprof_py |
| Scores, information blocks and S at both points | 8.3e-16 or better against pprof_py |
| Iterates 1 to 5 | 1.4e-15 or better against pprof_py |
| Default fit | 5 steps, as pprof_py; β̂ 6.4e-17 and γ̂ 1.4e-16 from pprof_py; β̂ bitwise equal to R pprof's SerBIN |
| Tight fit (tol 1e-13) | 6 steps, as pprof_py; β̂ 6.4e-16 and γ̂ 5.5e-16 from `glm`; β̂ bitwise equal to R SerBIN |
| Var(β̂), Var(γ̂ⱼ), Var(γ̂ⱼ + x̄ᵀβ̂) | 2.0e-16 or better against pprof_py; 3.2e-14 or better against `glm` |
| AUC | Equal to pprof_py's and R's |
| Exact two-sided tests against the median | z 1.9e-15, p 7.0e-16, limits 2.5e-13 from pprof_py; z 1.1e-13 from R; every flag equal |
| The Spark fit against the in-memory kernels | β̂, γ̂ and ℓ bitwise equal |
| Four blocks against one | β̂ and ℓ bitwise equal; γ̂ within 1.4e-16 |
| Shuffle partitions 1, 4, 7; reversed, repartitioned input | Bitwise equal |

## 7.4 Why the implementations still differ in the last bits

| Source | pprof_spark | pprof_py | R pprof |
|---|---|---|---|
| Provider sums gⱼ, hⱼ, bⱼ | Neumaier-compensated sums in canonical row order | `np.bincount` with weights (plain sequential sums in row order); bⱼ by a sparse indicator product | Armadillo `sum` over each provider's contiguous rows: two interleaved plain partial sums (`arrayops::accumulate`) |
| g_β and C | Centred, compensated, per provider then per block in order | BLAS products (`X.T @ r`, `X.T @ qX`), order up to the BLAS library | BLAS products through Armadillo |
| S | Centred co-moments | C − Σbbᵀ/h | C − Σbbᵀ/h |
| ℓ | Compensated sum | numpy's pairwise `np.sum` | `Loglkd` |
| Weight floor | q ≥ 1e-20 everywhere | The same | Only exact zeros |
| Line search | pprof_py's, with its noise rule | — | No noise rule; can stop one step earlier (X-017) |

The table answers review finding L-01's question: neither reference uses compensated or pairwise-cascade
sums for provider statistics. All of these differences are class D, below every tolerance class; the
larger, intentional ones are registered: screening at exactly 10 records (X-016), degenerate providers
held at the bound (X-018), inputs pprof_py mishandles (X-019), unknown providers in prediction and the AUC
for non-binary outcomes (X-022), clusters nested in providers (X-023), exact tails (X-024), bootstrap
draws (X-025) and binned direct sums (X-026).

## 7.5 Telling a statistical discrepancy from a distributed defect

The Cox procedure applies (Cox tutorial, section 7.9), with logistic specifics:

1. **Check conventions**: screening (`minRecords`, X-016), the bound and degenerate providers (X-018),
   binomial trials, the reference γ₀, the variance chosen for a measure.
2. **Compare at fixed parameters.** `LogisticFE.terms` gives the uncentred ℓ, scores and information at
   any (γ, β), comparable with pprof_py's `_compute_scores_and_info`; agreement rules out the kernels.
3. **Compare iterates** from the same start (`InMemorySerbinPasses` with `maxIter = k − 1`) to isolate
   the update rule, the line search and the bound.
4. **Compare stopping**: tight fits remove the stopping rule's effect.
5. **A difference between runs of pprof_spark itself is a defect**, never noise: check the canonical
   order, the block-order reductions, the placement of Δγ by provider index, and any floating-point SQL
   aggregate.

## 7.6 Where the evidence stops

- No measurement of performance or scale, and no multi-node run (T9, T10, D-28).
- No test varies `spark.sql.shuffle.partitions` (the probe did) or uses skewed provider sizes, which
  §9.6 asks of every engine test; `lfe-many` has 500 providers, far from the 10⁶ of the envelope.
- Every fixture has p = 3.
- On `lfe-base` every full Newton step is accepted; whether a fixture exercises a rejected ladder of
  step lengths was not checked here.
- Exact provider tests on large providers (cost) and the 20,000-trial limit are covered by rules, not
  by data of that size.

Review finding L-05 proposes tests for these gaps.
