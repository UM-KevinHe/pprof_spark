# Specification: Cox proportional hazards — first slice

- Status: **Approved on 2026-10-06 (D-20)**, with X-010 (corrected), X-011 and X-012. Implemented in
  round 10: `pprof.spark.engine.cox.CoxPH`, with kernels in `pprof.spark.numerics`.
- Scope (D-11): right-censored data, strata, Breslow ties, model-based variance. Efron ties, case
  weights and offsets follow in Phase 1a; left truncation, baseline hazards and TimeRange in 1b;
  residuals and robust variance in 1c.
- Reference: pprof_py v0.7.0, commit `9320766` (`CoxPH`), and R 4.3.3 `survival::coxph` 3.5-8.
- Fixtures: `fixtures/cox/{tiny-ties, rc-unstratified, rc-stratified}` (ADR-0005).
- Parity matrix entry: "Cox PH, first slice" in `docs/parity/matrix.md`.

## 1. Estimand and model

For subject i in stratum s with covariates xᵢ ∈ ℝᵖ, the hazard is
λᵢ(t) = λ₀ₛ(t) · exp(ηᵢ), ηᵢ = xᵢᵀβ, with an unspecified baseline hazard λ₀ₛ per stratum and one
coefficient vector β shared by all strata. The estimand is β, the log hazard ratios, with its
model-based covariance. There is no intercept: it is not identifiable from the partial likelihood
and would be absorbed by λ₀ₛ (X-001).

## 2. Objective

Each subject has an exit time bᵢ > 0 and an event indicator δᵢ ∈ {0, 1}; every subject enters at
time 0. At an event time t of stratum s, the risk set is Rₛ(t) = {i in s : bᵢ ≥ t} and the tied
events are Dₛ(t) = {i in s : bᵢ = t, δᵢ = 1}, with d = |Dₛ(t)|. With
S₀(t) = Σ_{Rₛ(t)} e^{ηᵢ}, S₁(t) = Σ_{Rₛ(t)} e^{ηᵢ} xᵢ, S₂(t) = Σ_{Rₛ(t)} e^{ηᵢ} xᵢxᵢᵀ, the Breslow
log partial likelihood, score and information are sums over strata and their distinct event times:

- ℓ(β) = Σₛ Σₜ [ Σ_{i ∈ Dₛ(t)} ηᵢ − d · log S₀(t) ]
- U(β) = Σₛ Σₜ [ Σ_{i ∈ Dₛ(t)} xᵢ − d · S₁(t)/S₀(t) ]
- I(β) = Σₛ Σₜ d · [ S₂(t)/S₀(t) − (S₁(t)/S₀(t))(S₁(t)/S₀(t))ᵀ ]

Strata without events contribute nothing to ℓ, U or I.

## 3. Parameterization

β is estimated on the scale of the input covariates. Covariates may be centered internally by
constants (for conditioning, as pprof_py and R do); centering shifts every ηᵢ of a stratum by the
same constant and leaves ℓ, U, I and β̂ unchanged mathematically. pprof_spark centers by the column
means, computed from per-block column sums combined in block order. Results are reported for the
uncentered covariates.

## 4. Data contract

| Column | Type | Rule |
|---|---|---|
| time | numeric | Finite and > 0 (pprof_py requires start < stop with start = 0) |
| event | integral 0/1 or boolean | Any other value fails (pprof_py rejects R's 1/2 coding) |
| features | numeric, at least one | Finite; Decimal converted to double explicitly |
| strata | integral or string, optional | Non-null; strata are ordered by key as in ADR-0004 |
| row identifier | integral, optional | Unique; fixes the canonical order (DIST-6) |

Validation runs before any fit (§5.5): every violation is reported at once with column names and
counts, never values (NN-7), and no row is dropped silently. This matches pprof_py, which raises
on each of these inputs (probed at v0.7.0).

## 5. Conventions (§7.2)

| Convention | Value in this slice |
|---|---|
| Event-time ties | Breslow; the result records the tie method |
| Interval and risk set | At risk at t when bᵢ ≥ t (entry at 0) |
| Near-equal times | Times are compared exactly; R's `timefix` cannot change integer or exactly repeated times, and fixtures use integer times |
| Starting values | β⁽⁰⁾ = 0, as pprof_py and R |
| Newton step | Solve I(β)·Δ = U(β) by Cholesky factorization of I |
| Step halving | While the new ℓ is not finite or is lower than the current ℓ, halve the step from the current β, at most 20 times, then accept — pprof_py's schedule |
| Convergence | Stop when abs(ℓ_new − ℓ_old) / abs(ℓ_old) < eps (a zero ℓ_old counts as 1), eps = 1e-9 by default, at most 20 iterations — pprof_py's formula. **The test is applied to the full Newton step before any halving**, as R's coxfit6 does; see §10 and X-010 |
| Aliasing | A Cholesky pivot below `tol × max diagonal`, tol = ε^0.75 (R's `toler.chol`), marks the covariate aliased; the fit fails and names the aliased covariates (X-011) |
| Non-convergence | Reported in the result and as a warning, never silent (NN-10); estimates are returned flagged |
| Null log-likelihood | ℓ(0), the first evaluation |
| Variance | Model-based: I(β̂)⁻¹ from the same Cholesky factor |
| Wald test and interval | z = β̂/SE, two-sided p = 2·Φ̄(abs(z)); interval β̂ ± z_{1−α/2}·SE, level 0.95 by default |

## 6. Algorithm

1. Validate (§4) and count rows and events per stratum (one aggregation and one guarded
   collection of stratum sizes, DIST-1).
2. Fail if there are no events at all (X-012).
3. Plan StratumLocal blocks (ADR-0004): whole strata, largest first into the least-loaded block. A
   stratum larger than the block target gets a block of its own; one larger than
   `maxStratumRows` fails with a message that TimeRange is needed (Phase 1b).
4. Build the working set once (one shuffle): per block, strata in key order; within a stratum rows
   in canonical order — exit time descending, then event before censoring, then row identifier,
   then values — with times, events and covariates in primitive arrays.
5. Newton iterations on the driver. Each evaluation of (ℓ, U, I) at a β is one pass over the
   working set: β travels in the closure; each block returns one partial; partials are combined
   in block order (ADR-0003). The first evaluation, at β = 0, also gives the null log-likelihood.
   Each halving costs one more pass.
6. After convergence, factor I(β̂) once more for the covariance.

Kernel per stratum (pure, in `numerics`): sweep rows in canonical order, keeping S₀, S₁ and the
packed S₂ as running Neumaier-compensated sums. At each distinct event time, add that time's rows
to the sums first (ties share one risk set), then add the time's contributions to ℓ, U and I. The
contributions of a stratum's event times are accumulated with Neumaier sums in time order, and
strata within a block in key order. e^{ηᵢ} uses `StrictMath.exp`. A non-finite e^{ηᵢ} or S₀ makes
the evaluation non-finite, which step halving then treats as a decrease.

## 7. Inference

- Covariance V = I(β̂)⁻¹ (packed, symmetric); SE = sqrt(diag V).
- z = β̂/SE; p = 2·Φ̄(|z|), with Φ̄ computed through a complementary error function accurate in
  relative terms deep into the tail, so that T-p's log10 comparison holds below 1e-10.
- Confidence interval β̂ ± q·SE with q = Φ⁻¹(1 − α/2), level in (0, 1). Implementation note
  (round 10): q comes from Newton steps on the upper tail, started from Abramowitz and Stegun
  26.2.23, and agrees with 160-digit references to 1e-14 in relative terms; this replaces the
  AS 241 named in the approved draft and changes no result beyond a unit in the last place.
- A negative or zero variance cannot occur after a successful Cholesky factorization; aliasing
  fails earlier (§5).

## 8. Outputs

Driver-side result (API-2), immutable, with the metadata of §6.10:

| Output | Content |
|---|---|
| Coefficients | β̂, SE, z, p, interval bounds, per feature, in input order |
| Covariance | Packed upper triangle of V |
| Fit | ℓ(β̂), ℓ(0), iterations, step halvings, converged flag and message |
| Counts | Observations, events, strata, strata without events |
| Conventions | Tie method, eps, iteration cap, halving cap, aliasing tolerance |
| Metadata | Software (`SoftwareInfo`), input specification, layout summary, data fingerprint, options |

A per-iteration log (β, ℓ, halvings) is kept in the result for lockstep parity and diagnostics.
Round 12 added the information matrix I(β̂) (packed) and the feature status (`experimental`,
NN-12) to the result, logs warnings through slf4j, and persists fits with `CoxFitIO` (§6.10).

## 9. Edge cases

| Case | pprof_py v0.7.0 (probed) | pprof_spark |
|---|---|---|
| Time ≤ 0, NaN or infinite; event not 0/1; non-finite covariate | Raises | Fails at validation with counts |
| Empty input | — | Fails at validation |
| No events at all | Returns β̂ = 0, converged, with a NumPy warning | Fails: the partial likelihood is constant (X-012) |
| A stratum without events | Contributes nothing | Same; counted in the result |
| A single-row stratum | Contributes nothing unless its row is an event, then only ℓ += η − η = 0 | Same |
| Collinear covariates | Returns a pseudo-inverse solution, converged, without a warning | Fails and names the aliased covariates (X-011) |
| Constant covariate | Pseudo-inverse with a NumPy warning | Fails: aliased with the baseline hazard (X-011) |
| Non-convergence within 20 iterations | Warns; returns flagged estimates | Same |
| Overflow of e^{η} | Step halving on non-finite ℓ | Same |
| Intercept requested | `fit_intercept=True` fits, then predictions raise | No intercept option (X-001) |

## 10. Reference mapping

| Element | pprof_py v0.7.0 | R survival 3.5-8 |
|---|---|---|
| Estimator | `models/survival/coxph.py`: `CoxPH(ties="breslow", max_iter=20, eps=1e-9)` | `coxph(..., ties = "breslow")` |
| ℓ, U, I | `algorithms/survival/cox_likelihood.py`: `cox_partial_likelihood`; Breslow kernel in `algorithms/survival/ties.py` | `coxfit6.c`, `coxph(init = β, control = coxph.control(iter.max = 0))` |
| Newton iteration | `algorithms/survival/optimization.py`: `newton_raphson` (LU solve, halving, convergence after halving) | `coxfit6.c` (Cholesky, convergence before halving, halving schedule (new + h·old)/(h + 1)) |
| Covariance | `utils/numerical.py`: `covariance_from_information` (`inv`, pseudo-inverse if singular) | `coxph(...)$var` with `robust = FALSE` |
| Wald and interval | `inference/survival/inference.py`: `wald_statistics`, `confidence_intervals` | `summary(coxph)` |
| Validation | `data/survival_validation.py`: `validate_fit_inputs` | `Surv()`, `model.frame` |

**Step control (X-010), recommended change.** Round 6 decided to follow pprof_py's order, which
halves a step whose ℓ fell before testing convergence. Measured since: under that rule, pprof_py
itself returns 9 different coefficient vectors for 30 row orders of `tiny-ties`, up to 1.05e-8
apart in relative terms, because the halving decision at convergence turns on rounding noise.
Testing convergence first, as R does, reduces the spread to 2.3e-15, rounding level. pprof_spark
must give layout-invariant results within T-part (1e-10, R1) and bitwise per layout (NN-4), so a
rule that amplifies rounding into 1e-8 differences is not acceptable. Recommended: test
convergence on the full step first and otherwise keep pprof_py's formula and halving schedule.
Effect: where pprof_py halves at convergence, pprof_spark's coefficients differ from pprof_py's by
half the final step (1.05e-8 relative on `tiny-ties`, 0.995 of T-coef) and agree with R's. On the
two larger fixtures both rules gave rounding-level spreads over 30 row orders at the default eps,
and pprof_py's and R's tight Breslow fits differ there by at most 2.4e-9 in relative terms.

## 11. Distributed plan

| Item | Plan |
|---|---|
| Layout | StratumLocal (§6.6): whole strata per block, ADR-0004's plan |
| Shuffles | One, to build the working set |
| Passes | One summary pass (counts, column sums for centering, fingerprint), then one per evaluation of (ℓ, U, I): 1 at β = 0, then one per iteration plus one per halving; typically 4 to 6 |
| Driver traffic per pass | B partials of 1 + p + p(p+1)/2 doubles, under `driverBudgetBytes` |
| Kernel cost | O(n·p²) per pass; O(n·p) memory per block |
| Driver work | Cholesky of a p×p matrix per iteration, O(p³) |
| Reproducibility | R0 bitwise per layout; R1 within T-part across layouts and partitionings; Classic and Connect identical (R2) |

## 12. Validation plan

| Level | Test | Class |
|---|---|---|
| Function | ℓ, U, I at β = 0 and at the fixtures' fixed β, against `pprof_py.json` and `r_survival.json` (Breslow) for the three fixtures | T-fn |
| Lockstep | Iterates before the final one, from β = 0, against pprof_py's iterates (to be added to the fixtures by running `CoxPH(max_iter = k)` for k = 1 to 5); the final iterate is covered by the next row because of X-010 | T-iter |
| End to end | Tight and default fits: β̂, SE, covariance, ℓ(β̂), ℓ(0), iteration counts, against pprof_py and R | T-coef, T-var, T-fn |
| Negative controls | The fixtures' Efron, shifted-tie results must not match pprof_spark's Breslow results | must exceed the class |
| Metamorphic | Translating a covariate leaves β̂ unchanged; scaling by c divides β̂ and SE by c; a strictly increasing time transform leaves β̂ and ℓ unchanged; row order and partition count leave every bit unchanged (R0); block size changes stay within T-part (R1) | as stated |
| Edge cases | Every row of §9 | exact behavior |
| Platforms | All of the above under Classic Spark and Spark Connect (T3, T8), JDK 17 and 21 | R2 |

## 13. Known discrepancies

| ID | Class | Summary | Decision |
|---|---|---|---|
| X-001 | C | No intercept; pprof_py accepts one, then fails at prediction | Approved with §3.4 |
| X-002 | — | §3.4 said `CoxPH` does not warn at `max_iter`; v0.7.0 does warn | Resolved; pprof_spark warns too |
| X-010 | B | Step-control order | Proposed: change from bug-compatible to corrected (§10) |
| X-011 | C | Aliased or constant covariates fail instead of returning pseudo-inverse estimates | Proposed |
| X-012 | C | No events at all fails instead of returning β̂ = 0 with a NumPy warning | Proposed |
| — | D | Summation order (Neumaier running sums), Cholesky instead of LU, `StrictMath` | Within T-fn, T-iter and T-coef by design |
