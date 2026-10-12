# 2. SerBIN: Newton's method by block elimination

[Tutorial home](README.md) · previous: [1. The logistic provider model](01-logistic-provider-model.md)

The logistic fixed-effect model has m + p parameters, and m can be a million. This chapter derives
how SerBIN (serial blockwise inversion Newton; Wu, Yang, Kang and He, 2022) takes exact Newton steps
anyway, how pprof_spark computes them, and how the step length, the bound and the stopping rule
work. Code references are to [Serbin.scala](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala),
the kernels in [kernels/LogisticFE.scala](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala),
and the engine's [LogisticFE.scala](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala).

## 2.1 The Newton system

Newton's method maximizes ℓ by solving, at the current (γ, β), the linear system "information times
step equals score" (Cox tutorial, chapter 3, section 3.1). With the arrowhead information of
chapter 1, H = diag(h₁, …, h_m) and B the m × p matrix whose rows are bⱼᵀ:

```math
\begin{pmatrix} H & B \\ B^{\top} & C \end{pmatrix}
\begin{pmatrix} \Delta\gamma \\ \Delta\beta \end{pmatrix}
=
\begin{pmatrix} g \\ g_\beta \end{pmatrix}.
```

Solved densely this costs O((m + p)³): impossible at m = 10⁶. Block elimination solves it exactly
for much less.

## 2.2 Eliminating the provider effects

The first block row says hⱼΔγⱼ + bⱼᵀΔβ = gⱼ for each provider, so

```math
\Delta\gamma_j = \frac{g_j - b_j^{\top}\Delta\beta}{h_j} = \frac{g_j}{h_j} - \bar x_j^{\top}\Delta\beta,
\qquad \bar x_j = \frac{b_j}{h_j}.
```

Substituting into the second block row, BᵀΔγ + CΔβ = g_β, leaves a p × p system for Δβ alone:

```math
S\,\Delta\beta = r, \qquad
S = C - \sum_{j} \frac{b_j b_j^{\top}}{h_j}, \qquad
r = g_\beta - \sum_{j} \frac{b_j\, g_j}{h_j}.
```

S is the **Schur complement** of H. The recipe is: form S and r (one pass over the data), solve the
p × p system on the driver, then compute every Δγⱼ from its own provider's sums. The cost is
O(n·p²) for the sums, O(p³) for the solve and O(m·p) for the provider steps, and the step is the
exact Newton step, not an approximation.

## 2.3 The centred form of S and r

Because x̄ⱼ = bⱼ/hⱼ is provider j's q-weighted mean covariate, the terms of S and r are provider-wise
centred co-moments:

```math
S = \sum_{j}\sum_{i} q_{ij}\,(x_{ij} - \bar x_j)(x_{ij} - \bar x_j)^{\top},
\qquad
r = \sum_{j}\sum_{i} (x_{ij} - \bar x_j)\,(y_{ij} - n_{ij}\pi_{ij}).
```

Expanding the first sum gives Σᵢ qxxᵀ − bⱼx̄ⱼᵀ − x̄ⱼbⱼᵀ + hⱼx̄ⱼx̄ⱼᵀ = Cⱼ − bⱼbⱼᵀ/hⱼ, and the second
gives g_β,ⱼ − x̄ⱼgⱼ, so these are the same quantities. They are better ones to compute:

- **No cancellation.** C − Σ bbᵀ/h subtracts large, nearly equal matrices when covariates are far
  from zero. pprof_py and R form it that way; the specification's probe found the two forms 4.2e-13
  apart at shifts of +50 and −30, and on the `lfe-shifted` fixture pprof_py's subtraction loses
  about three digits, so the parity tests check S there through C, b and h instead (specification
  §14, note 3).
- **No m × p intermediate.** pprof_py and R build a p × m matrix S⁻¹(bⱼ/hⱼ); the centred form needs
  only each provider's own x̄ⱼ.
- **A clear meaning.** S is the within-provider weighted covariance of the covariates. β is
  identified by variation within providers only, and a covariate constant within every provider
  makes S singular: that is the aliasing failure of chapter 1, section 1.9.

In the kernel, `ProviderSums.accumulate` makes one walk over a provider's rows for π, q, gⱼ, hⱼ and
bⱼ, and `centeredInto` makes a second for its centred contributions to S and r
([kernels/LogisticFE.scala lines 503–591](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L503-L591)).
Every sum is Neumaier-compensated and runs in canonical row order. The kernel test "the centred Schur
system equals C − Σ bbᵀ/h and g_β − Σ bg/h" checks the identity.

## 2.4 The predicted gain λ

Along the Newton direction, ℓ's directional derivative is gᵀΔγ + g_βᵀΔβ. Substituting Δγⱼ:

```math
\lambda = \sum_j g_j\Big(\frac{g_j}{h_j} - \bar x_j^{\top}\Delta\beta\Big) + g_\beta^{\top}\Delta\beta
= \sum_j \frac{g_j^2}{h_j} + r^{\top}\Delta\beta ,
```

which needs only Σ gⱼ²/hⱼ from the data pass and r and Δβ from the driver
([Serbin.scala lines 118–123](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L118-L123)).
Since S is positive definite, rᵀΔβ = rᵀS⁻¹r ≥ 0, so λ ≥ 0, and it is zero only at a stationary point.

## 2.5 Step length: Armijo backtracking, run as a ladder

A full Newton step can overshoot. With `backtrack = true` (the default), pprof_py accepts the first
step length v in 1, 0.6, 0.36, … for which

```math
\ell(\gamma + v\Delta\gamma,\ \beta + v\Delta\beta) - \ell(\gamma, \beta) \;\ge\; 0.01\cdot v\cdot\lambda ,
```

or for which the predicted gain 0.01·v·λ is positive but below 1e-12·(1 + |ℓ|): near convergence the
gain is below the rounding of ℓ, and insisting on it would shrink the step for nothing (pprof_py's
rule C3; R pprof has no such rule and can stop up to a step earlier, X-017).

Each evaluation of ℓ at a trial point is a pass over the data. A sequential search would launch a
Spark job per step length. pprof_spark instead evaluates **eight step lengths in one pass**,
v = 0.6⁰, …, 0.6⁷, and the driver scans them in order with the same acceptance rule, so it accepts
exactly the step length the sequential search would; if none of the eight passes, the next pass
evaluates 0.6⁸ to 0.6¹⁵
([Serbin.scala lines 126–156](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L126-L156)).
Each trial log-likelihood is computed by the same arithmetic as in a sequential search, so the
accepted value is the same to the bit. The trade-off is seven log-likelihood sums that are often
unused (the full step is usually accepted) against a job per rejected step length; the specification
chose the ladder for that reason (§11), without a measurement yet.

With `backtrack = false` the step is always the full Newton step.

## 2.6 The bound

After every update each γⱼ is clipped to [med(γ) − bound, med(γ) + bound], with bound = 10 and med
numpy's median over the fitted providers, the mean of the two middle values when m is even
([Serbin.scala lines 76–90 and 157–158](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L76-L90)).
The bound exists for degenerate providers, whose effects would otherwise drift toward ±∞ (chapter 1,
section 1.8). It applies to every provider, but a provider with both events and non-events is very
unlikely to sit 10 log-odds (an odds ratio of about 22,000) from the median. Computing the median
needs all m effects on the driver: one reason γ lives there (section 2.10).

## 2.7 Starting values and the weight floor

The iteration starts at γⱼ⁽⁰⁾ = logit(ȳ) for every provider, ȳ = Σy/Σn over the fitted records, and
β⁽⁰⁾ = 0 ([engine LogisticFE.scala lines 141–147](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L141-L147)).

During the iteration every record's weight q is raised to at least 1e-20
(`WeightFloor`, [kernels line 13 and line 570](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L565-L572)),
so hⱼ stays positive and gⱼ/hⱼ finite even for a provider whose fitted probabilities have reached 0 or
1 in double precision. pprof_py floors the same way; R raises only exact zeros. The variances after
convergence use a different guard: π clipped to [1e-10, 1 − 1e-10] and no floor (section 2.9).

## 2.8 Stopping

The criterion is the largest change in β made by the accepted step,

```math
\text{criterion} = \lVert \beta_{\text{new}} - \beta_{\text{old}} \rVert_\infty = v\,\lVert \Delta\beta \rVert_\infty ,
```

and with backtracking the loop runs while the step counter is at most `maxIter` and the criterion is
at least `tol` = 1e-8, so it takes at most `maxIter` + 1 steps, as both references do; the fit is
converged when the criterion is below `tol`. Without backtracking pprof_py's other loop applies
(counter below `maxIter`, criterion above `tol`, converged at criterion ≤ `tol`)
([Serbin.scala lines 107–111 and 171–172](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L107-L111)).

Three consequences are worth knowing:

- **γ is not in the criterion.** Provider effects can still be moving when β has settled; a
  degenerate provider usually stops at the bound before β converges.
- **A shortened step can look like convergence.** If the line search cut the last step (v < 1) while
  the Newton step for β was still at least `tol`, the small criterion reflects the line search, not
  the solution. The fit then carries pprof_py's second warning (`shortened`, line 181).
- **Non-convergence is flagged** (NN-10): when the cap ends the loop the fit says so and reports the
  number of steps.

Each step also re-factors S by Cholesky with R's aliasing rule and fails, naming the features, if a
pivot is not above 2⁻³⁹ times S's largest diagonal entry (lines 115–117), as for Cox.

## 2.9 Variances from the block inverse

The inverse of the arrowhead information gives the variances directly. For a matrix with blocks H,
B, C and Schur complement S, the inverse is

```math
\begin{pmatrix} H & B \\ B^{\top} & C \end{pmatrix}^{-1}
=
\begin{pmatrix}
H^{-1} + H^{-1}B\,S^{-1}B^{\top}H^{-1} & -H^{-1}B\,S^{-1} \\
-S^{-1}B^{\top}H^{-1} & S^{-1}
\end{pmatrix},
```

and row j of H⁻¹B is x̄ⱼᵀ. Hence, with everything evaluated at the estimates:

| Quantity | Formula |
|---|---|
| Var(β̂) | S⁻¹ |
| Var(γ̂ⱼ) | 1/hⱼ + x̄ⱼᵀS⁻¹x̄ⱼ (R's `logis_fe_var`) |
| Cov(γ̂ⱼ, β̂) | −x̄ⱼᵀS⁻¹ |
| Var(γ̂ⱼ + x̄ᵀβ̂) | 1/hⱼ + (x̄ⱼ − x̄)ᵀS⁻¹(x̄ⱼ − x̄), with x̄ = Σnx/Σn over the fitted records |

The last one, the variance of the provider effect at the average case mix (pprof_py's C34), is what
the provider tests use; it is usually smaller than Var(γ̂ⱼ), because x̄ⱼ − x̄ is smaller than x̄ⱼ when
covariates are not centred at zero. For these variances π is clipped to [1e-10, 1 − 1e-10] and q is
not floored, as in pprof_py: the kernels `varianceSchur` and `providerVariances`
([kernels lines 202–250](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L202-L250)),
run in two passes after the fit
([engine LogisticFE.scala lines 154–191](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L154-L191)).

## 2.10 One Newton step in Spark

The optimizer is pure Scala on the driver (`Serbin.fit`); it reaches the data through two functions
of the `SerbinPasses` interface, which the engine implements with Spark
([engine LogisticFE.scala lines 767–839](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L767-L839))
and the tests implement in memory (`InMemorySerbinPasses`, used for lockstep parity). Each step:

| Pass | Executors | Driver |
|---|---|---|
| A, `schur` | Per block: each provider's gⱼ, hⱼ, x̄ⱼ and centred contributions; the block's packed S, r, Σ gⱼ²/hⱼ and ℓ | Sort the B partials by block; Neumaier sums; Cholesky of S; Δβ; λ |
| B, `trial` | Per block: recompute each provider's sums (the same bits as pass A), Δγⱼ, and ℓ at eight step lengths | Place each block's Δγ into the m-vector; scan the ladder; update and bound γ; update β; test |

γ has to reach the blocks for both passes, and it changes every step, so it cannot live in the
persisted working set. `withGamma` builds a small table on the driver with one row per block holding
that block's providers' effects, and joins it to the cached blocks with a broadcast hint
(lines 780–787). That is the pattern of PROJECT_CONTEXT §6.7: O(m) parameters as a broadcast-joined
table, which Spark Connect supports, rather than a broadcast variable, which it does not. Pass B
returns Δγ, m doubles, to the driver. So every step moves about 16m bytes of provider effects plus
B small partials, and holds γ on the driver, guarded by `maxProvidersOnDriver` (OI-50 records the
alternative of a distributed γ).

Pass B recomputes each provider's sums rather than receiving them from pass A. Recomputing costs a
second walk over the rows but keeps the passes independent and stateless, and because the
arithmetic is the same, the values are the same to the bit. Chapter 6 traces the Spark jobs
of a complete fit.

## 2.11 Where SerBIN lives

| Piece | Code |
|---|---|
| Options and the iteration | [`SerbinOptions`, `Serbin.fit`](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L6-L184) |
| Line-search constants, noise rule, median, bound | [Serbin.scala lines 64–90](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L64-L90) |
| Pass A kernel | [`LogisticFE.schur`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L83-L100) |
| Pass B kernel | [`LogisticFE.trial`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L105-L143) |
| Per-provider sums; centred contributions | [`ProviderSums`, `centeredInto`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L503-L592) |
| Uncentred terms for function-level parity | [`LogisticFE.terms`](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/LogisticFE.scala#L161-L199) |
| Passes in Spark; γ per block by broadcast join | [`SparkPasses`](../../../engine/src/main/scala/pprof/spark/engine/logistic/LogisticFE.scala#L767-L839) |
| Passes in memory, for tests | [`InMemorySerbinPasses`](../../../numerics/src/main/scala/pprof/spark/numerics/Serbin.scala#L190-L224) |

The kernel tests in `numerics` (`LogisticFESuite`) check that the scores and information are the
derivatives of ℓ, the centred identity, shift invariance, numpy's median and bound, that SerBIN
reaches zero scores with default and tight fits agreeing, that a provider without events stops at
the bound, aliasing, and that blocks reduced in order give what one block gives. The engine's
`LogisticKernelParitySuite` compares ℓ, scores and information at fixed parameters (T-fn) and the
first five iterates (T-iter) with pprof_py; chapter 8 covers the rest.
