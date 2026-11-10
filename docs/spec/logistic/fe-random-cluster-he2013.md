# Specification: the `he2013` estimator for fixed provider effects with random clusters (slice 4c-2)

- Status: approved with D-65 (2026-11-05), with X-049; fixtures and calibration in round 128 (D-66, approved); code and Python in round 129.
- Reference: pprof_py v0.7.0 (`9320766`), `LogisticFERandomClusterModel(estimator="he2013")`
  (`models/logistic/fe_random_cluster.py`, the iteration after `gauss_hermite_normal`). pprof_py follows He et al.
  (2013) and R's `glmm.fac.hosp`, which is not on CRAN; pprof_py is the only executable reference (OI-94).
- Builds on slice 4c-1 ([fe-random-cluster.md](fe-random-cluster.md)): the same model, inputs, sources, options and
  inference. Only the estimator differs.

## 1. Estimand

`he2013` is not a maximum-likelihood estimator. It is the fixed point of He et al.'s alternating iteration, from the
given start, with Gauss–Hermite nodes fixed by σ. The fixed point depends on the start and on the node count, and
fixed nodes misstate the likelihood of large clusters (pprof_py's documentation says so). The estimator exists for
output comparable with R's `glmm.fac.hosp`. `marginal` (slice 4c-1) remains the default.

Round 127's probe on frc-base (σ = 0.7, the fixture's start) illustrates the difference:
- `he2013` converged in 22 iterations under `max_delta_gamma` and in 34 under `relative`, at `tol` 10⁻⁵;
- its γ̂ differs from `marginal`'s by up to 0.072;
- its marginal log-likelihood is −1535.559, against `marginal`'s −1535.504.

## 2. The iteration

The nodes and weights are those of the normal N(0, σ²) rule, as R's `statmod::gauss.quad.prob(n, "normal")`:
aₖ = √2·σ·tₖ and wₖ = vₖ/√π, from the physicists' Gauss–Hermite rule (tₖ, vₖ) with `nNodes` points. They stay
fixed throughout. Each iteration, from the current γ:

1. **E-step.** For each cluster h and node k, ℓₕₖ = Σᵢ∈ₕ log(yᵢ(1 − qᵢₖ) + (1 − yᵢ)qᵢₖ), with qᵢₖ =
   expit(−(aₖ + γⱼ₍ᵢ₎ + xᵢᵀβ)). For binary yᵢ this is the Bernoulli log-likelihood; for a fractional yᵢ it is the
   log of a mixture, as pprof_py computes it, and not `marginal`'s y log p + (1 − y) log(1 − p). The posterior
   moments of αₕ under the weights wₖ·exp(ℓₕₖ) give mₕ and vₕ.
2. **M-step.** For each provider, with p = expit(mₕ₍ᵢ₎ + γⱼ + xᵢᵀβ) and q = 1 − p:
   - the score is Σᵢ [yᵢ − p − ½vₕ₍ᵢ₎·pq(q − p)];
   - the information is Σᵢ [pq + ½vₕ₍ᵢ₎·pq(p² + q² − 4pq)], floored at 10⁻⁸, with a warning counting the floored
     provider-iterations.
   Then γⱼ ← γⱼ + score/information, clipped to the median of γ ± `bound` (`relative`) or to ±`bound` (`absolute`).
3. **Objective.** At the new γ, with the same mₕ and vₕ:
   Σᵢ [(mₕ₍ᵢ₎ + γⱼ + xᵢᵀβ)yᵢ + log q − ½vₕ₍ᵢ₎·pq − (mₕ₍ᵢ₎² + vₕ₍ᵢ₎)/(2σ²)].
4. **Convergence.** Under `max_delta_gamma` (the default), the criterion is the largest |Δγ|. Under `relative`, it
   is |obj_t − obj_{t−1}| / |obj_t − obj_1|, as R does. The first iteration's criterion is 1. A zero denominator
   counts as 0 when the numerator is 0 and as ∞ otherwise, with a warning.
   The loop runs while the iteration count is at most `maxIter` and the criterion is at least `tol`, so it can run
   `maxIter` + 1 iterations, as pprof_py does. A non-finite objective stops the loop without convergence.

The outputs are slice 4c-1's:
- γ̂;
- the last E-step's mₕ and vₕ, computed before the last γ update, as pprof_py keeps them;
- the fitted values expit(γ̂ⱼ + mₕ + xᵢᵀβ), mixing the last update's γ with those moments, as pprof_py's;
- the iterations, the final criterion, convergence, and warnings.
- The log-likelihood is the marginal one at γ̂ by adaptive quadrature, as `marginal` reports it, evaluated once at the
  end.

## 3. σ = 0

The nodes collapse to 0, so mₕ = vₕ = 0, and the objective's prior term is 0/0. pprof_py raises `ZeroDivisionError`
at σ = 0, as its `marginal` does (X-005); round 127's probe shows it under both criteria. pprof_spark drops the prior
term at σ = 0, its limit since mₕ = vₕ = 0, and iterates. The iteration is then Newton's method on the provider
effects with offset xβ (X-049).

## 4. Distributed algorithm

Each iteration takes two passes over the records, grouped in provider × cluster cells in canonical order, as stage 3
groups them:
- **The E-step pass** gives every cell's sums of log(y(1 − q) + (1 − y)q) at each node.
- **The M-step pass** gives every cell's score and information.

The driver adds the cells into clusters (H × `nNodes` values) and into providers (2m values), in key order. It forms
the moments and the step, and evaluates the objective from the next E-step pass, which runs at the same γ. Every sum
runs in canonical order with Neumaier's compensation.

The rows themselves enter the E-step, not compressed cells with offset bins (X-029), because the fractional
outcome's mixture is not a sum of Bernoulli terms. The cells' sums at all nodes reach the driver under its budget
(cells × `nNodes` doubles); a fit beyond it fails, naming OI-101.

## 5. Inference

The tests, intervals and measures are slice 4c-1's (the three-stage code), with he2013's γ̂ and moments. `summary`
is delegated to stage 1, as for `marginal`.

## 6. Validation plan

- `fixtures/fe-random-cluster` gains, for every case, pprof_py's he2013 fit under `max_delta_gamma` and `relative`.
  Each is run at its defaults and with `tol` 10⁻¹² (the fixed point). Each fit has its exact tests and measures.
- pprof_spark runs the same iteration with the same arithmetic. Proposed in the fixture round:
  - the fixed point's γ̂ and the moments at T-coef and T-meas;
  - the default runs' iteration counts equal pprof_py's, and their γ̂ at T-coef.
- σ = 0: the iteration converges, and every free provider's score with zero moments is 0 (X-049); pprof_py raises
  there.

## 7. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-049 | B | At σ = 0, pprof_spark's he2013 drops the objective's prior term (its limit) and iterates; pprof_py raises `ZeroDivisionError`, as for `marginal` (X-005) | Approved (D-65) |

## 8. Fixture findings (round 128)

- Every `fixtures/fe-random-cluster` case gains pprof_py's he2013 fits under both criteria, at the defaults and at
  `tol` 10⁻¹². The `max_delta_gamma` fixed point carries its exact test and measures at the median, and a negative
  control. Regenerating reproduces every file byte for byte.
- he2013 converges linearly. At `tol` 10⁻¹² it takes 43 to 88 iterations, and the two criteria's fixed points agree
  within 9·10⁻⁵ of T-coef. At the defaults it takes 5 to 39 iterations and stops up to 995 of T-coef from the fixed
  point, about 10⁻⁵ in γ, as a stopping rule on the change predicts for a linear rate.
- The fitted values follow §2: γ after the last update with the moments before it, within 3·10⁻⁸ of T-meas.
- The negative controls miss by at least 1.9·10⁷ of T-coef.

## 9. Implementation notes (round 129; no statistical change)

- `engine.logistic.He2013` groups the records into provider × cluster cells, sorted by the row identifier (or by
  their offset and outcome without one). Integral keys are read as Long, as `ThreeStage.compress` reads them.
- Each iteration takes three passes: the E-step's node sums, the M-step's score and information, and the objective
  at the new γ. Their cell sums come back sorted by provider and cluster and are added on the driver with Neumaier's
  compensation. The driver forms the moments, the step, the median clip and the criterion, in pprof_py's order.
- `log q` is −log(1 + e^η), the stable form; it differs from pprof_py's `log(plogis(−η))` only where that underflows.
- The result is a `ThreeStageStage3`, with `held` marking the providers at the bound, so `test`, `measures`,
  `intervals` and `fitted` work unchanged. The fit carries he2013's warnings: floored information, a zero
  denominator, non-convergence.
- `LogisticFERandomClusterOptions` takes `estimator` `he2013` and `convergenceCriterion`. The facade and Python
  pass them, and the Python summary reports the estimator and the warnings.
- Tests:
  - `LogisticFERandomClusterSuite` gains a he2013 test for every case (both criteria; the fixed point at D-66's
    classes; the default runs' iteration counts equal to pprof_py's, with γ̂ at T-coef; the exact test and
    measures) and the σ = 0 test of X-049;
  - `test_fe_random_cluster.py` gains a he2013 test on frc-text.
- The first run passed on string keys and failed on integral ones, which CSV reading made `Integer`, so the cells'
  keys are now cast to Long.
