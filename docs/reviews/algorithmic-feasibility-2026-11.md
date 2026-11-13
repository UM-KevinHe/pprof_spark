# Algorithmic feasibility study: CoxPH and provider fixed-effect logistic regression in pprof_spark

> Committed in round 149 (2026-11-13) as written, at the start of the performance phase (D-75, `docs/spec/performance/plan.md`). Its raw output is `algorithmic-feasibility-2026-11-evidence.md` and its probes are in `feasibility-probes/`, both beside this file; they were written against `a28a224` (round 139) and are not part of the build. The plan records where later reading of the code departs from this study (plan §3).

Date: 2026-11-13. Sources read: pprof_spark at `a28a224` (main, round 139) and pprof_py at `94b5476`,
whose Cox and logistic kernels are unchanged since the pinned v0.7.0. Nothing was implemented in either
package. The experiments are prototypes on synthetic data; their output is in
`feasibility-evidence.md` and their source in `feasibility-probes.tar.gz`.

Each statement below is one of three kinds, and the text says which: read in the source, measured in a
run, or derived by arithmetic from measured rates.

## 1. Summary

Newton's method with an explicit information matrix is the right algorithm for both models at about 180
features, on this cluster. What has to change is how that matrix is computed and reduced, not the
optimizer. Every alternative optimizer I tested needs more passes over the data, more CPU, or both, and
the one-shot approximate estimators I tested end 0.2 to 2.3 standard errors away from the validated
estimate.

For Cox, one exact identity removes most of the work. The information can be written as a weighted
Gram matrix minus one rank-one term per event time, so no p×p arithmetic is done per event beyond that
term. A JVM prototype of it runs at 5.4 µs per row against 51 µs today and agrees with the engine to
4·10⁻¹⁶. The same identity gives a new exact algorithm for unstratified fits and oversized strata on
gridded times: two passes, no sort, any partitioning, and 2.7 MB to the driver where the present
formulation would need 238 MB.

For the logistic model, the Schur-complement method that SerBIN implements is already the
structure-exploiting algorithm; the econometrics literature reaches the same linear algebra by another
route. Two things improve on the present implementation without changing the estimator. Vectorizing
the accumulation gives 6.2 times on the dominant pass. Maximizing the profile likelihood, with each
provider's intercept solved inside the executor, needs one pass per iteration instead of two and
reached pprof_py's SerBIN optimum to 10⁻¹⁴ standard errors.

Communication is not the constraint here. An exact Newton pass returns at most 64 MB to the driver at
180 features, so the 32 GB driver is ample, and the methods designed to save communication solve a
problem this workload does not have. The constraints are CPU per pass, the number of passes, and
executor memory for the working set.

## 2. The target environment

The standard cluster has a 32 GB driver and 30 workers of 32 GB. The arithmetic below uses
8·n·(p + 3) bytes for a working set of doubles.

| Rows, at 180 features | Working set | Per worker (30) | pprof_py on the driver |
|---|---|---|---|
| 5 million | 7.3 GB | 0.24 GB | X alone is 7.2 GB, and each model holds a second n×p array |
| 20 million | 29 GB | 1.0 GB | At least 58 GB: not possible on 32 GB |
| 100 million | 146 GB | 4.9 GB | Not possible |

pprof_py's Cox fit keeps `X` and a centred copy (`coxph.py:200`), and its logistic fit keeps `X` and
`q·X` (`fixed_effect.py`, `_compute_scores_and_info`). On a 32 GB driver that puts its ceiling near 5
million rows at 180 features, by arithmetic and not by a run. On the standard cluster, then, the
distributed path is needed from about the size of the representative workload upwards.

What an exact Newton pass asks of the driver is small. A packed information matrix at p = 180 is 131 KB.
Reduced on the executors in a few hundred groups, a pass returns tens of megabytes whatever n is. The
driver then solves a 180×180 system.

## 3. The existing algorithms and their limits

| | pprof_spark | pprof_py |
|---|---|---|
| Cox optimizer | Newton with step halving; every candidate evaluated with ℓ, U and I (`Newton.scala`) | The same (`optimization.py`) |
| Cox evaluation | Per stratum, a sweep in descending time with running S₀, S₁, S₂; S₂ updated per row and a p×p term formed per event time (`CoxStratum.scala`) | The same sweep, compiled by numba, one thread, full p×p (`ties.py`) |
| Cox cost per evaluation | O((n + K)·p²) scalar compensated operations: 51 µs per row at p = 180 | O((n + K)·p²) vectorized: 17 µs per row |
| Logistic optimizer | SerBIN: Newton on (β, γ) through the Schur complement, an eight-step line-search ladder (`Serbin.scala`) | SerBIN by default; BAN available (`fixed_effect.py`) |
| Logistic evaluation | Pass A accumulates the centred Schur complement per element; pass B evaluates ℓ at eight step lengths; then a variance pass and a co-moment pass | One BLAS product XᵀQX, a sparse provider aggregation, the complement formed from uncentred blocks |
| Logistic cost per iteration | 33 µs per row at p = 180 | 2.3 µs per row on one thread, and BLAS uses every driver core |
| Data movement | One shuffle at build; one partial per block to the driver per pass | None; everything in driver memory |
| Limit | CPU per pass; all partials collected, so blocks must grow with n·p³ | Driver memory; one thread for Cox |

Here n is rows, p features, m providers and K the number of distinct event times summed over strata.
Three limits of pprof_spark are implementation choices, not properties of Newton's method: the
per-element compensated accumulation, the p×p work per event time, and the collection of every block's
partial. One limit is structural: an unstratified fit, or a stratum larger than a block, runs as one task.

pprof_py's own source records what BAN costs. The comment in `BanAlgorithm.fit` reports 19 iterations on
centred covariates, 705 with the covariates shifted by (+5, −3), and no convergence in 10,000 with
(+50, −30).

## 4. Candidate algorithms

### 4.1 Cox: the weighted-Gram identity (exact)

With rᵢ = wᵢ·exp(ηᵢ), S₀(t) = Σ rⱼ over the risk set and x̄(t) = S₁(t)/S₀(t), the Breslow information is

    I(β) = Σₜ dₜ [ S₂(t)/S₀(t) − x̄(t)·x̄(t)ᵀ ].

Exchanging the order of summation in the first term gives

    Σₜ dₜ·S₂(t)/S₀(t) = Σᵢ rᵢ·cᵢ·xᵢxᵢᵀ,   cᵢ = Σ dₜ/S₀(t) over the event times at which row i is at risk,

so that

    I(β) = Xᵀ·diag(rᵢcᵢ)·X − Σₜ dₜ·x̄(t)·x̄(t)ᵀ.

The weight cᵢ is the baseline cumulative hazard accumulated while row i is at risk, so it comes from one
extra O(n) sweep. With left truncation it is a difference of two cumulative values, at exit and at
entry. With Efron ties the increment at a time with k tied deaths becomes w̄·Σᵣ 1/Aᵣ for rows not dying
there and w̄·Σᵣ (1 − r/k)/Aᵣ for the tied deaths, where Aᵣ = S₀ − (r/k)·S₀ᴰ, and the second term gains
one rank-one matrix per r. These are the quantities the engine computes now, summed in another order.

The cost falls from (n + K)·p²/2 compensated operations with two divisions each to (n + K)·p²/2 plain
multiply-adds that the JIT vectorizes. The score and log-likelihood keep their present O(n·p) sweep.

Measured: 5.4 µs per row against 51 µs at p = 180; 0.77 against 4.67 at p = 50. Agreement with the
engine on its own blocks: log-likelihood identical, score 2·10⁻¹⁶, information 4·10⁻¹⁶ (scaled). The
prototype covers Breslow ties and rows without entry times; the Efron and truncated cases are derived
above and not run.

### 4.2 Cox: per-time aggregates for unstratified fits and oversized strata (exact, new)

The identity has a second consequence. S₂(t) is no longer needed per event time, so the only
time-indexed quantities are S₀(t) and S₁(t): p + 2 numbers per distinct time. When times lie on a grid,
as days do, a stratum of any size can be evaluated in two passes over rows in any order.

Pass 1 sums rᵢ, rᵢxᵢ and the events by distinct time, on the executors. The driver takes suffix sums
over the grid, which gives S₀(t), x̄(t), ℓ, U, the hazard increments and their prefix sums c(t). Pass 2
forms Xᵀ·diag(rᵢ·c(tᵢ))·X on the executors, and the driver subtracts Σₜ dₜx̄(t)x̄(t)ᵀ.

Measured on 60,000 rows with 1,825 distinct times, rows in random order: the result equals the sorted
sweep to 7·10⁻¹⁶ (ℓ), 8·10⁻¹⁵ (U) and 6·10⁻¹⁵ (I). Pass 1 returns 2.7 MB. Carrying S₂ per time, which
is why the project context limits this layout to small p·K, would need 238 MB.

This removes the one-task limit for unstratified fits and for strata larger than a block, without the
sorted time-range scans planned under OI-03. It needs no sort and no particular partitioning. It does
not help the provider-stratified fit, whose strata are small enough to sweep locally; there the
per-stratum grid would be as large as the stratum. For continuous times, where K approaches n, sorted
blocks with suffix offsets remain the route: three passes, with O(p) numbers per block to the driver.

### 4.3 Logistic: the Schur complement, the profile likelihood, and within-provider centring are one object

For logit πᵢⱼ = γⱼ + xᵢⱼᵀβ, with qᵢ = nᵢπᵢ(1 − πᵢ), the Schur complement that SerBIN solves is

    S = Σⱼ Σᵢ qᵢ·(xᵢ − x̄ⱼ)(xᵢ − x̄ⱼ)ᵀ,   x̄ⱼ = Σᵢ qᵢxᵢ / Σᵢ qᵢ over provider j.

This is also the negative Hessian of the profile log-likelihood ℓₚ(β) = max over γ of ℓ(β, γ), and it
is the normal matrix of a weighted least-squares step after centring each provider's covariates at
their weighted mean. Greene (2004) and the fixed-effect GLM literature (Stammann 2018; Bergé 2018)
arrive at the same elimination. So no undiscovered structure remains in the provider effects for an
exact method to exploit: the m unknowns already cost O(m) memory and no matrix larger than p×p.

What can change is the path. For fixed β each γⱼ solves the scalar equation Σᵢ (yᵢ − nᵢπᵢ) = 0 over its
own rows. Blocks hold whole providers, so that solve is local to a task. Newton on ℓₚ then needs one
pass per iteration: the pass solves the intercepts, and returns ℓₚ, its gradient Xᵀ(y − nπ) and S. The
present method needs two passes per iteration, ships γ to the executors each time, and evaluates eight
step lengths. Step control becomes ordinary halving on a concave function of p variables.

Measured against pprof_py's SerBIN on the same data: the profile Newton optimum agrees to 1·10⁻¹⁴
standard errors, in 4 iterations against SerBIN's 5, with about 2 inner steps per pass when warm
started. In the JVM the profile solve and gradient cost 0.65 µs per row, against 2.7 µs for pass B.

It is the same estimator and a different sequence of iterates. Lockstep parity with pprof_py would no
longer apply; converged parity would. The maintainer accepted this on 2026-11-13.

Providers with no events, or only events, need care, and I tested them against pprof_py on 30,000 rows
with 600 providers, 73 of them without events. Such a provider has no finite root, and pprof_py holds
it at its median-relative bound. At pprof_py's converged estimate the plain score in β is not zero
(4·10⁻⁴ here). What is zero is SerBIN's reduced right-hand side, to which the bounded providers
contribute. A profile method must therefore take its β step from that same reduced system, with
interior intercepts solved exactly and bounded ones held at the bound. Done that way it matched
pprof_py to 9·10⁻¹⁷ in β at a tolerance of 10⁻¹², in 5 outer iterations against SerBIN's 13. Treating
the bounded providers as constants and solving the plain score equation gave a β 1.4·10⁻⁶ away, or
2·10⁻⁵ standard errors: immaterial statistically, and far outside T-coef.

One visible difference remains at the default tolerance of 10⁻⁸. pprof_py stops when β has converged,
after 8 iterations here, before its zero-event providers have reached the bound. Their reported effects
were up to 2.4 short of it and depend on when the iteration stopped. The profile method reports the
bound itself, which is also what pprof_py reports at a tighter tolerance. β differed by 1.5·10⁻⁹ at
that default. The difference in the reported effects needs to be registered and approved.

The variance, which pprof_py computes with clipped probabilities, needs its own final pass, as now.

### 4.4 Cross-cutting exact changes

**Chunked compensated accumulation.** Plain sums over a fixed number of rows in a layout the JIT
vectorizes, each chunk folded into Neumaier totals. Measured: 4.85 µs per row against 29.9 for the
logistic pass A, agreement 1·10⁻¹⁶; it is also what the Cox prototype uses. The sums stay
deterministic.

**Reduction on the executors.** Partials folded in block order within fixed groups of blocks, one
partial per group to the driver. This is §6.8 of the project context, specified and not built. It makes
driver traffic independent of n and lets blocks stay small.

**Internal scaling.** Standardizing columns inside the kernel changed the condition number of the
information from 3.7·10⁵ to 34 on the correlated design, at no cost. Estimates are unchanged apart from
rounding. OI-66 already asks for it.

**Sparse and binary covariates.** A row with z non-zero covariates costs z²/2 instead of p²/2 in the
Gram term, exactly, provided only the dense columns are centred. The gain depends on the data and is
unmeasured here. It applies to Xᵀ·diag(w)·X, not to the rank-one terms.

**Binomial compression.** Rows of one provider with identical covariates can be merged into one row
with trials, exactly; the model already accepts trials. With 180 covariates few rows will coincide, so
this is for narrow models.

### 4.5 The full list

Cost is per pass; rounds are passes over the data per fit. "Same" under estimator means the maximum
likelihood estimate of the present model.

| Candidate | Cost per pass | Rounds per fit | To the driver per pass | Estimator | Assessment |
|---|---|---|---|---|---|
| Newton, explicit information, vectorized (4.1, 4.4) | O(n·p²), SIMD | 5 to 6 | Groups × p²/2 | Same, same iterates | Recommended |
| Per-time aggregates, Cox (4.2) | O(n·p) then O(n·p²) | 2 per evaluation | K·(p + 2) | Same | Recommended for unstratified fits and large strata |
| Sorted scans with suffix offsets, Cox | O(n·p²) | 3 per evaluation | Blocks × p | Same | For continuous times only |
| Profile-likelihood Newton, logistic (4.3) | O(n·p²) plus local solves | 5 to 6 | Groups × p²/2, and m | Same, other iterates | Recommended as a second step |
| SerBIN as now, vectorized | O(n·p²), then O(n·p) | 10 to 12 | The same | Same, same iterates | Recommended first |
| BAN (alternating) | O(n·p²) | 8 here; hundreds when coupled | The same | Same, other iterates | Rejected: dominated by SerBIN |
| Chord or Shamanskii: information reused | O(n·p) mostly | 8 to 12 | p | Same, other iterates | Small gain; needs a new stopping rule |
| Sub-sampled information, full gradient | O(n·p) + O(s·p²) | More than Newton | p²/2 | Same, other iterates | As chord, and not deterministic without care |
| L-BFGS on the profile or partial likelihood | O(n·p) | 30 to 65 here | p | Same, to a looser tolerance | Rejected at p = 180; for p above about 500 |
| L-BFGS on (β, γ) jointly | O(n·p) | 100 to 260 here | p + m | Same, to a looser tolerance | Rejected |
| Truncated Newton (Newton-CG, TRON) | O(n·p) per product | 15 to 65 here | p | Same, other iterates | Rejected at p = 180; for large p |
| Warm start from a pilot fit | As Newton | At most one fewer here | As Newton | Same | Rejected: no net saving measured |
| QR in place of normal equations (TSQR) | O(n·p²) | As Newton | p²/2 per block | Same | Only if conditioning defeats scaling |
| ADMM, DANE, GIANT, CoCoA | O(n·p²) locally | Tens | p | Same in the limit | Rejected: they save communication, which is not scarce |
| One-step from a pilot or an average | One Newton pass | 1 after the pilot | p²/2 | Approximate | Not for reported measures |
| Divide and conquer, averaged | Local fits | 1 | p or p²/2 | Approximate | Not for reported measures |
| Surrogate likelihood (CSL, ODAC) | Local fit, one gradient | 2 | p | Approximate | Not for reported measures |
| Optimal subsampling | Fit on a sample | n/a | n/a | Approximate | Not for reported measures |
| Mini-batch SGD, online updating | O(s·p) per step | Epochs | n/a | Another estimator | Not for reported measures |
| Conditional likelihood, logistic | O(nⱼ·dⱼ) per provider | As Newton | p²/2 | Another estimator | Rejected |
| Poisson or discrete-time form of Cox | GLM passes | As a GLM | p²/2 | Another estimator | Rejected |

## 5. Measurements

### 5.1 Passes and accuracy

Data: 60,000 rows, 96 providers of 625 rows, 180 features, in two designs. The first follows
`02_scale.py`: independent, similarly scaled features, and an information matrix with condition number
2. The second has correlated indicators, some rare, and continuous covariates with standard deviations
from 0.1 to 50; its condition number is 3.7·10⁵, or 34 after standardizing. Real covariate sets are
closer to the second. Deviations are the largest coordinate distance from the exact estimate, in
standard errors.

| Method | Logistic, simple | Logistic, correlated | Cox, simple | Cox, correlated |
|---|---|---|---|---|
| Newton: iterations (information passes) | 4 (5) | 4 (5) | 3 (4) | 5 (6) |
| pprof_py SerBIN: iterations | 5 | 5 | n/a | n/a |
| pprof_py BAN: iterations | 7 | 8 | n/a | n/a |
| Chord, information of the start only: gradient passes | 7 | 10 | 4 | 11 |
| Chord, information of the start and first iterate: gradient passes | 3 | 5 | 2 | 7 |
| L-BFGS, standardized: evaluations to 10⁻² / 10⁻⁴ / 10⁻⁶ | 5 / 6 / 8 | 21 / 32 / 49 | 5 / 7 / 9 | 27 / 42 / not in 65 |
| L-BFGS, unstandardized | 10 to 10⁻⁶ | 1.9 away after 501 | 13 to 10⁻⁶ | Diverged |
| L-BFGS on (β, γ), standardized: evaluations to 10⁻² / 10⁻⁴ / 10⁻⁶ | 27 / 76 / 100 | 102 / 172 / 256 | n/a | n/a |
| Newton-CG: gradient + Hessian-vector passes | 6 + 11 | 7 + 57 | 5 + 8 | 7 + 42 |
| Pilot on a tenth of providers, then Newton: full iterations | 4 | 5 | 3 | 4 |

Three results matter beyond the counts.

The chord method stopped early under the Cox rule. That rule tests the relative change in ℓ, which
suits a quadratically convergent method. With the information of the start reused, the correlated Cox
fit stopped 0.03 standard errors from the optimum, and 0.001 with one refresh. Any method that
converges linearly needs a different stopping rule, which is itself a change to validated behaviour.

First-order methods depend on preparation that Newton does not need. Without standardizing, L-BFGS did
not converge on the correlated design in 501 evaluations for the logistic model and overflowed for Cox.
Without profiling the intercepts out, it needed at least five times as many evaluations.

The pilot start saved nothing overall. The pilot estimate lay 8 standard errors from the full-data
optimum in its worst coordinate. It saved one full pass in one case of four, cost one in another, and
the pilot fit itself is not free.

### 5.2 Cost of a fit

CPU per row for one fit at p = 180, built from the JVM rates measured on one thread and the pass counts
of the correlated design. Each pass also pays about 1 µs per row to decode a block from the Dataset
cache. Preparation is excluded.

| Rate measured at p = 180 | µs per row |
|---|---|
| Cox evaluation today / weighted-Gram prototype | 51 / 5.4 |
| Cox gradient only / gradient and one Hessian-vector product | 0.75 / 1.6 |
| Logistic pass A today / chunked prototype | 29.9 / 4.85 |
| Logistic pass B today / profile solve and gradient | 2.7 / 0.65 |

| Algorithm | Logistic: µs per row | Logistic: rounds | Cox: µs per row | Cox: rounds |
|---|---|---|---|---|
| Today | 237 | 12 | 312 | 6 |
| Same iterates, vectorized | 60 | 12 | 38 | 6 |
| Profile-likelihood Newton | 39 | 6 | n/a | n/a |
| Chord, one refresh | 28 | 8 | 31 | 10 |
| L-BFGS, standardized, to 10⁻⁴ | 59 | 33 | 80 | 43 |
| Newton-CG | 150 | 65 | 128 | 50 |

At 20 million rows the vectorized Cox fit is about 760 CPU-seconds against 6,240 today, and the
profile-likelihood logistic fit about 780 against 4,740. These are arithmetic from one-thread rates and
say nothing yet about wall-clock time on the cluster.

The L-BFGS rows stop at 10⁻⁴ standard errors, where Newton's rows are converged to 10⁻¹⁰ or better, and
the Newton-CG row for the logistic model assumes a Hessian-vector pass costs twice a gradient pass.

The chord method saves a fifth to a quarter of the CPU once the information pass is cheap, at the price
of more rounds and a new stopping rule. I do not think that trade is worth making.

The crossover with first-order methods follows from the scaling. An information pass costs about
1.7·10⁻⁴·p² µs per row and a gradient pass with its decode about 10⁻²·p. With six information passes
against 45 to 100 gradient passes, the two families meet between roughly 400 and 1,000 features. The
representative workload is well below that. A wide-model mode is a separate project, and its natural
form is truncated Newton or L-BFGS on standardized, profiled objectives.

## 6. Exact, approximate and alternative methods

| Class | Methods | What changes for a validated measure |
|---|---|---|
| Exact, same iterates | Vectorized accumulation; weighted-Gram form; per-time aggregates; executor-side reduction; internal scaling; sparse Gram; binomial compression | Last bits only. Needs the NUM-2 evidence and fixtures re-compared under T-fn |
| Exact, other iterates | Profile-likelihood Newton; chord; sub-sampled information; L-BFGS; Newton-CG; warm starts; BAN; ADMM and its relatives | The optimum is the same, reached to the method's own tolerance. Iteration counts, traces and lockstep parity change; stopping rules may have to |
| Approximate | One-step estimators; divide and conquer; surrogate likelihood; subsampling | A different number, close in large samples |
| Another estimator | Mini-batch or online partial likelihood; conditional logistic likelihood; Poisson or discrete-time forms; bias-corrected fixed effects | A different statistical procedure |

Measured distance from the exact estimate, in standard errors, with 30 groups of providers:

| Method | Logistic, simple | Logistic, correlated | Cox, simple | Cox, correlated |
|---|---|---|---|---|
| Fit on a tenth of the providers | 8.6 | 7.6 | 8.9 | 8.4 |
| Divide and conquer, simple average | 1.6 | 2.1 | 2.1 | 2.3 |
| Divide and conquer, information-weighted | 0.48 | 0.80 | 0.50 | 0.82 |
| Surrogate likelihood, one round | 0.19 | 0.59 | 0.16 | 0.79 |
| One Newton step from the weighted average | 0.002 | 0.015 | 0.0006 | 0.063 |
| A second Newton step from a pilot | 0.002 | 0.008 | 0.0002 | 0.048 |
| Mini-batch SGD, 20 epochs | n/a | n/a | 1.3 | 1.7 |

A one-step estimator is the best of these and is still wrong in the second or third digit of a
standard error at 180 features, on data where the exact fit needs only five passes. Each further step
is a Newton pass, so iterating it to agreement is Newton's method with a worse start. The distributed
one-shot methods were designed for sites that cannot pool data. Here the data are pooled, and the exact
pass costs the same CPU as one of their local fits.

The SGD figures are indicative only. I tried three learning rates and one batch size, and its
objective is a modified partial likelihood, so it is not expected to reach the same estimate.

## 7. Recommendations

For Cox, in order:

| Rank | Change | Class | Evidence | Expected effect |
|---|---|---|---|---|
| 1 | Weighted-Gram information with chunked compensated accumulation | Exact, same iterates | 5.4 against 51 µs per row; agreement 4·10⁻¹⁶ | About 8 times less CPU per fit |
| 2 | Reduction on the executors in fixed groups; small blocks | Exact, same iterates | Driver bytes constant in n | Removes the n·p³ ceiling; fits the 32 GB driver at any n |
| 3 | Per-time aggregates for unstratified fits and strata above a block | Exact | Agreement 10⁻¹⁴ on 1,825 times; 2.7 MB to the driver | Removes the one-task limit on gridded times |
| 4 | Internal scaling; ℓ-only evaluation of halved steps | Exact | Condition number 3.7·10⁵ to 34 | Robustness; a small saving |
| 5 | Sparse Gram, if the covariates are mostly zeros | Exact | Not measured | Depends on the data |

For the logistic model, in order:

| Rank | Change | Class | Evidence | Expected effect |
|---|---|---|---|---|
| 1 | SerBIN's passes with chunked compensated accumulation, including the co-moment pass | Exact, same iterates | 4.85 against 29.9 µs per row; agreement 1·10⁻¹⁶ | About 4 times less CPU per fit |
| 2 | Reduction on the executors, as for Cox | Exact, same iterates | As for Cox | As for Cox |
| 3 | Profile-likelihood Newton, stepping through SerBIN's reduced system | Exact, other iterates | Same optimum as pprof_py's SerBIN to 10⁻¹⁴, and to 10⁻¹⁶ with providers at the bound; 6 rounds against 12 | A third less CPU again; half the rounds; no γ shipped |
| 4 | Internal scaling | Exact | As for Cox | Robustness |
| 5 | Binomial compression for narrow models | Exact | Not measured | Depends on the data |

Not recommended at this width: BAN, L-BFGS, Newton-CG, the chord method, pilot starts, the
communication-saving optimizers, and every approximate or alternative estimator for a reported measure.

Rank 3 for the logistic model is the only recommendation that changes iterates. It is a decision for
the maintainer, and it should follow rank 1, which keeps today's iterates and can be verified against
the existing fixtures at every level.

## 8. Development and benchmarking roadmap

Each step ends with a measurement on the standard cluster. No step depends on the 128 GB driver.

| Step | Work | Exit measurement on the standard cluster |
|---|---|---|
| 0 | Baseline on current main: 5 and 20 million rows, 100 and 180 features, 8,000 providers, both models | Time by call site, CPU-seconds, bytes to the driver per pass, driver heap peak |
| 1 | Specify and build reduction groups with plan-set partitioning | Driver bytes per pass no larger at 100 million rows than at 5 million; results bitwise equal across partition counts |
| 2 | Chunked accumulation in both kernels and the co-moment pass | CPU per row per pass against step 0; fixtures within T-fn; negative control |
| 3 | Specify the weighted-Gram form with Efron ties and entry times; fixtures; build | Cox evaluation CPU per row; parity at all three levels |
| 4 | Specify per-time aggregates for unstratified fits and oversized strata | An unstratified fit at 20 million rows using every worker; agreement with a stratum-local sweep |
| 5 | Specify profile-likelihood Newton, including bounded providers; fixtures for converged parity | Rounds and CPU per fit against step 2; converged estimates within T-coef of SerBIN's |
| 6 | Decide whether a wide-model mode is wanted; if so, prototype truncated Newton | Crossover width measured, not assumed |

The benchmark grid for steps 0 to 5 is rows in {5, 20, 50, 100} million, features in {50, 100, 180},
providers in {800, 8,000, 80,000}, on the standard cluster with a fixed number of workers. Each point
records wall time and executor CPU by call site, rounds, shuffle bytes, bytes returned to the driver
per pass, peak driver heap, and cached bytes per executor. Jobs should be attributed through SQL
execution identifiers; stage names do not carry call sites under adaptive execution.

Correctness at each point rests on three checks that need no reference at scale: bitwise equality
across partition counts, recovery of the generating coefficients within 4.5 standard errors, and
parity with pprof_py on a sample that fits the driver. pprof_py itself can be timed on the 32 GB driver
only up to about 5 million rows at 180 features, which is also the honest statement of where the two
packages meet on this cluster.

Step 0 comes first for a reason of its own. A Cox fit in pprof_spark costs about three times pprof_py's
CPU per row, spread over the workers, and nothing measured so far explains a 30-worker Cox run being
slower than one pprof_py thread. The baseline's times by call site will show where that time goes.

## 9. Limits of this study

The experiments ran on one core at 60,000 rows, in Python for the optimizers and in the JVM for the
kernel rates. Pass counts and accuracy carry over to larger n; wall-clock time on a cluster does not
follow from them. The designs are synthetic. Real covariates may be worse conditioned than the
correlated design, which would favour Newton further.

The Cox experiments use Breslow ties and right-censored rows. The profile-likelihood method was tested with
providers at the bound in Python only, without weights or trials. Sparse covariates, binomial compression and sorted scans for continuous times
are assessed by argument and not by a run. L-BFGS was SciPy's, with ten correction pairs.

## References

Entries marked † were checked against a publisher, PubMed or arXiv record on 2026-11-13. The others are
cited from memory and should be verified before being quoted elsewhere.

Andersen PK, Gill RD (1982). Cox's regression model for counting processes. *Annals of Statistics* 10:1100–1120.

Bergé L (2018). Efficient estimation of maximum likelihood models with multiple fixed-effects: the R package FENmlm. CREA Discussion Paper 2018-13.

Boyd S, Parikh N, Chu E, Peleato B, Eckstein J (2011). Distributed optimization and statistical learning via the alternating direction method of multipliers. *Foundations and Trends in Machine Learning* 3(1):1–122.

Breslow N (1974). Covariance analysis of censored survival data. *Biometrics* 30:89–99.

Chamberlain G (1980). Analysis of covariance with qualitative data. *Review of Economic Studies* 47:225–238.

Chen X, Xie M (2014). A split-and-conquer approach for analysis of extraordinarily large data. *Statistica Sinica* 24:1655–1684.

Dembo RS, Eisenstat SC, Steihaug T (1982). Inexact Newton methods. *SIAM Journal on Numerical Analysis* 19:400–408.

Demmel J, Grigori L, Hoemmen M, Langou J (2012). Communication-optimal parallel and sequential QR and LU factorizations. *SIAM Journal on Scientific Computing* 34:A206–A239.

† Duan R, Luo C, Schuemie MJ, et al. (2020). Learning from local to global: an efficient distributed algorithm for modeling time-to-event data. *JAMIA* 27(7):1028–1036.

Efron B (1977). The efficiency of Cox's likelihood function for censored data. *JASA* 72:557–565.

Gail MH, Lubin JH, Rubinstein LV (1981). Likelihood calculations for matched case-control studies and survival studies with tied death times. *Biometrika* 68:703–707.

Gaure S (2013). OLS with multiple high dimensional category variables. *Computational Statistics & Data Analysis* 66:8–18.

Greene W (2004). The behaviour of the maximum likelihood estimator of limited dependent variable models in the presence of fixed effects. *Econometrics Journal* 7:98–119.

He K, Kalbfleisch JD, Li Y, Li Y (2013). Evaluating hospital readmission rates in dialysis facilities; adjusting for hospital effects. *Lifetime Data Analysis* 19:490–512.

Huang C, Huo X (2019). A distributed one-step estimator. *Mathematical Programming* 174:41–76.

Jordan MI, Lee JD, Yang Y (2019). Communication-efficient distributed statistical inference. *JASA* 114:668–681.

Kelley CT (1995). *Iterative Methods for Linear and Nonlinear Equations*. SIAM.

† Keret N, Gorfine M (2023). Analyzing big EHR data: optimal Cox regression subsampling procedure with rare events. *JASA* 118(544):2262–2275.

Lin C-J, Weng RC, Keerthi SS (2008). Trust region Newton method for large-scale logistic regression. *JMLR* 9:627–650.

Liu DC, Nocedal J (1989). On the limited memory BFGS method for large scale optimization. *Mathematical Programming* 45:503–528.

† Lu C-L, Wang S, Ji Z, Wu Y, Xiong L, Jiang X, Ohno-Machado L (2015). WebDISCO: a web service for distributed Cox model learning without patient-level data sharing. *JAMIA* 22(6):1212–1219.

Mittal S, Madigan D, Burd RS, Suchard MA (2014). High-dimensional, massive sample-size Cox proportional hazards regression for survival analysis. *Biostatistics* 15:207–221.

Neyman J, Scott EL (1948). Consistent estimates based on partially consistent observations. *Econometrica* 16:1–32.

Roosta-Khorasani F, Mahoney MW (2019). Sub-sampled Newton methods. *Mathematical Programming* 174:293–326.

Shamir O, Srebro N, Zhang T (2014). Communication-efficient distributed optimization using an approximate Newton-type method. *ICML*.

Simon N, Friedman J, Hastie T, Tibshirani R (2011). Regularization paths for Cox's proportional hazards model via coordinate descent. *Journal of Statistical Software* 39(5).

Stammann A (2018). Fast and feasible estimation of generalized linear models with high-dimensional k-way fixed effects. arXiv:1707.01815.

† Tarkhan A, Simon N (2020). BigSurvSGD: big survival data analysis via stochastic gradient descent. arXiv:2003.00116.

Therneau TM, Grambsch PM (2000). *Modeling Survival Data: Extending the Cox Model*. Springer.

Wang H, Zhu R, Ma P (2018). Optimal subsampling for large sample logistic regression. *JASA* 113:829–844.

† Wang J, Zeng D, Lin D-Y (2024). Fitting the Cox proportional hazards model to big data. *Biometrics* 80(1).

Wang S, Roosta F, Xu P, Mahoney MW (2018). GIANT: globally improved approximate Newton method for distributed optimization. *NeurIPS*.

† Wang Y, Hong C, Palmer N, Di Q, Schwartz J, Kohane I, Cai T (2021). A fast divide-and-conquer sparse Cox regression. *Biostatistics* 22(2):381–401.

Wu J, Chen M-H, Schifano ED, Yan J (2021). Online updating of survival analysis. *Journal of Computational and Graphical Statistics* 30:1209–1223.

Wu W, Yang Y, Kang J, He K (2022). Improving large-scale estimation and inference for profiling health care providers. *Statistics in Medicine* 41(15):2840–2853.

Zhang Y, Duchi JC, Wainwright MJ (2013). Communication-efficient algorithms for statistical optimization. *JMLR* 14:3321–3363.
