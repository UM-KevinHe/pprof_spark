# Specification: three-stage model — stage 3 given β and σ (Phase 2f-2)

- Status: Approved by the maintainer, 2026-10-08 (D-40), with X-029 as recommended.
- Builds on: [three-stage-spike.md](three-stage-spike.md) (D-38), [three-stage-preparation.md](three-stage-preparation.md)
  (2f-1) and [standardization.md](standardization.md) §3 (binned Taylor sums, X-026).
- Decision references: D-40; X-005, X-029.
- Reference: pprof_py v0.7.0 (`9320766`): `models/logistic/fe_random_cluster.py`
  (`LogisticFERandomClusterModel` with `estimator = "marginal"`: `_cluster_modes`, `_marginal_terms`,
  `_adaptive_rule`, `_fit_marginal`).

Probe results are sandbox evidence from round 46.

## 1. Estimand

With β̂ (stage 1) and the cluster SD σ (stage 2) held fixed, the provider effects γ maximize the
marginal log-likelihood

ℓ(γ) = Σₕ log ∫ Πᵢ∈ₕ f(ỹᵢ | γⱼ₍ᵢ₎ + oᵢ + a) φ(a; 0, σ²) da, with log f(ỹ | η) = ỹη − log(1 + eᶯ),

where ỹ is `y_adj`, oᵢ = xᵢᵀβ̂ the stage 1 offset, and h runs over clusters. ℓ is concave in γ, so the
maximizer is unique.

## 2. Algorithm (pprof_py's `_fit_marginal`)

| Element | Rule |
|---|---|
| Quadrature | Adaptive Gauss–Hermite with `nNodes` (20) nodes: cluster h's effect is integrated at ĉₕ + √2·sₕ·tₖ, with log weights log wₖ + tₖ², at its posterior mode ĉₕ and scale sₕ |
| Cluster modes | Guarded Newton on Σᵢ∈ₕ log f − a²/(2σ²): steps clipped to ±1, stop when abs(step) < 1e-10 or after 100 steps, warm-started from the previous iteration's modes; sₕ = 1/√(curvature) at the mode |
| Score and Hessian | Score per provider Σᵢ (ỹᵢ − E_post[pᵢ]); negative Hessian: the posterior mean curvature of each cell on the diagonal, minus the posterior covariance of the cells' scores for providers sharing a cluster (sparse: providers interact only through shared clusters) |
| Bounds | `relative` (default): median(γ) ± `bound` (10); or `absolute`: ±`bound`. Providers at a bound with an outward score are held |
| Step | Newton on the free providers, then an Armijo line search on ℓ at the same nodes (c = 1e-4, halving; stalled below a step of 1e-10), clipping to the bounds |
| Convergence | max abs(score) over free providers below `tol` (1e-5), or `maxIter` (10,000), or a stall; non-convergence is a warning and a status |
| Start | Given (pprof_py: stage 2's provider effects plus its intercept), as in pprof_py; the length must match the providers |
| Outputs at the end | The cluster posterior means and variances, the log-likelihood at the final effects, fitted probabilities σ(γⱼ + E_post[aₕ] + oᵢ) |

## 3. Computing from compressed cells (X-029)

The records enter only through each cell (cluster, provider): its Σỹ, its Σỹ·o and the distribution
of its offsets. One pass groups each cell's offsets into bins of width 0.1 and records the moments
M_{c,b,k} = Σᵢ∈c,b (oᵢ − c_b)ᵏ/k!, k ≤ 8 (slice 2d's expansion). Every sum the algorithm needs at an
effect η = γⱼ + a, namely Σσ, Σσ′ and Σ log(1 + e^{o+η}), is then a sum over bins of the derivative
polynomials of σ at η + c_b (σ⁽ᵏ⁾ for the first two; log(1 + eˣ) and σ⁽ᵏ⁻¹⁾ for the third), with a
remainder below 1.2e-16 per record. Round 42 rebuilt pprof_py's stage 3 log-likelihood on its golden data
this way to 1.6e-16 relative.

## 4. Numerical choices (class D)

| Choice | pprof_py | pprof_spark |
|---|---|---|
| Newton step | `scipy.sparse.linalg.spsolve` (SuperLU) | Jacobi-preconditioned conjugate gradients on the free block to a relative residual of 1e-12, in a fixed order of operations (at most 2·m iterations); any iterate is an ascent direction, and convergence is judged on the score |
| Gauss–Hermite rule | numpy's `hermgauss` (Golub–Welsch) | Newton on the Hermite recurrence from asymptotic starts; checked against mpmath (50 digits) for 5 to 40 nodes |

Both change iterates only by the solvers' and rules' accuracy; converged effects agree under T-coef at a
tight tolerance.

## 5. σ = 0 (X-005)

At σ = 0 the cluster effects vanish and ℓ(γ) = Σᵢ log f(ỹᵢ | γⱼ + oᵢ): the same projected Newton with
score Σᵢ(ỹᵢ − σ(γⱼ + oᵢ)), a diagonal Hessian and no quadrature. pprof_py raises `ZeroDivisionError`
there; its estimates approach this limit as σ → 0 (round 46, golden data: largest difference from the
limit 1.1e-3, 1.2e-5 and 1.2e-7 at σ = 0.01, 0.001 and 0.0001, so of order σ²).

## 6. Outputs and API

`ThreeStage.stage3(prepared, beta, sigma, start, options)` on 2f-1's prepared records (with `y_adj`,
`stage1_offset`, provider and cluster) returns `ThreeStageStage3`: provider keys, γ, σ, β, converged,
iterations, criterion, log-likelihood, cluster keys with posterior means and variances, the held
providers, and the options; `fitted(prepared, stage3)` adds the fitted probability per record.
`ThreeStageOptions` gains `nNodes`, `maxIter`, `tol`, `bound` and `boundMode` for stage 3.

## 7. Distributed plan

| Step | Work |
|---|---|
| Compression | One pass over the records: per block, the moments of each (cell, bin) in canonical order; a shuffle by (cell, bin); partials summed in block order (compensated) |
| Small problems | When the compressed table fits the driver budget, it is collected and stage 3 runs on the driver |
| Large problems | Otherwise the compressed cells are grouped into cluster-local blocks; each iteration runs modes, node terms and cell scores per cluster on executors and emits provider scores, curvatures and the Hessian's cell-pair entries, reduced in block order; the driver solves |

The driver holds m-scale vectors and the sparse Hessian (entries for provider pairs sharing a cluster).
Results are bitwise invariant to partitioning (R0).

## 8. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| σ = 0 | `ZeroDivisionError` | The limit of §5 (X-005) |
| σ < 0, non-finite β or start, start of the wrong length | Errors or undefined | Fails with a message |
| A provider whose information is tiny | Newton information floored at 1e-8 (`he2013` only) | As pprof_py's `marginal`: the sparse system; a singular free block fails with a message |
| Line search stalls | Stops with the status | Same |

## 9. Validation plan

Fixtures (round 47, calibrated in [three-stage-calibration.md](../../parity/three-stage-calibration.md)), on ts-golden
and ts-synthetic: pprof_py's stage 3 with β, σ and the start from its
own pipeline, at `tol` 1e-5 and 1e-10: γ, iterations, criterion, log-likelihood, cluster posterior
means and variances, fitted probabilities; the first three Newton iterates from the same start
(lockstep); ℓ and the score at the start and at a fixed γ (function level); fits at σ = 1e-4 and the
σ = 0 limit (a numpy reference); Gauss–Hermite nodes and weights from mpmath. Tolerance classes:
T-fn (ℓ and score at fixed γ), T-iter (iterates), T-coef (γ, tight), T-meas (posterior moments,
fitted probabilities). Reference-independent checks: max abs(score) at the tight fit; ℓ at the fit
not below ℓ at perturbed effects. Negative controls: σ shifted by 0.01, one outcome flipped. R's
`glmm.fac.hosp` implements `he2013`, a different estimator, and is not a target for this slice.

Round 46 probes (golden data): stage 3 alone with pprof_py's β, σ = 0.422943 and stage 2's start
reproduces the pipeline's γ exactly, in 4 iterations at `tol` 1e-5 (0.04 s) and 5 at 1e-10; the two
differ by 9.0e-8, so parity uses the tight fits.

## 10. Known discrepancies

| ID | Summary | Class | Proposed decision (D-40) |
|---|---|---|---|
| X-029 | Stage 3's sums come from per-cell offset-bin moments (X-026's expansion) instead of pprof_py's sums over every record; remainder below 1.2e-16 per record | C | Use the compressed cells, which keeps every iteration independent of n |
| X-005 | pprof_py divides by zero at σ = 0 | B | Approved with D-38; the limit is specified in §5 |

## 11. Implementation notes (round 48; no statistical change)

1. numerics: `GaussHermite.rule` (Newton on the normalized Hermite recurrence, ascending, exact +0 middle
   node), `SparseSymmetric` (triplets summed in order; Jacobi-preconditioned conjugate gradients on an
   active block), `kernels.ThreeStageKernel` (compressed-cell sums, cluster modes, terms with the sparse
   negative Hessian, the σ = 0 limit, and the projected Newton of §2).
2. engine: `ThreeStage.compress` (one pass and a shuffle by cell; within a cell, records in offset order),
   `ThreeStage.stage3` (from prepared records, or from compressed cells) and `ThreeStage.marginal`
   (ℓ and score at given effects). The API takes σ and the start; β is the preparation's, through
   `stage1_offset`.
3. Cluster modes stop per cluster when that cluster's step falls below 1e-10; pprof_py iterates all
   clusters until the largest step does. The extra steps move a mode by less than 1e-10, within T-iter
   (the lockstep iterates agree).
4. At the tight fit both implementations' scores are rounding noise around zero, so the tests check
   optimality (max abs(score) ≤ 1e-9) there instead of a relative comparison.
5. This round runs stage 3 on the driver, with the compressed cells under the driver budget. The
   cluster-local executor path for larger tables (§7) and the per-record fitted probabilities follow in
   round 49.
