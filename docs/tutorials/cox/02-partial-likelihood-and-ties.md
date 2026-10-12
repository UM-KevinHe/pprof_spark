# 2. The partial likelihood, its derivatives, and tied events

[Tutorial home](README.md) · previous: [1. Fundamentals](01-survival-and-cox-fundamentals.md) ·
next: [3. Estimation and inference](03-estimation-and-inference.md)

This chapter derives everything the estimation code computes: the log partial likelihood ℓ(β), its
gradient U(β) and its negative Hessian I(β), first without ties and then with Breslow's and Efron's
treatments of tied event times. Every quantity is evaluated by hand on the tiny-ties data at β = 0
and compared with the kernel and with R.

## 2.1 Risk sets

The **risk set** R(t) at an event time t is the set of rows at risk just before t: those with
aᵢ < t ≤ bᵢ (in stratum s: Rₛ(t)). For tiny-ties every row enters at 0, so R(t) is every row with
bᵢ ≥ t:

| Event time t | Rows at risk | Rows with an event at t |
|---:|---|---|
| 1 | 0, 1, 2, 3, 4, 5, 6, 7 (8 rows) | 0, 1 |
| 2 | 2, 3, 4, 5, 6, 7 (6 rows) | 3 |
| 3 | 4, 5, 6, 7 (4 rows) | 4, 5 |
| 4 | 6, 7 (2 rows) | 7 |

Row 2 is censored at 2 and is still at risk at 2; row 6 is censored at 4 and at risk at 4. Risk sets
shrink as time passes. Every quantity in this chapter is a sum over a risk set, and the shape of
those sums — each event time needs the sum over all rows that exit *later* — is what drives the
distributed design in chapter 5.

## 2.2 From the hazard model to the partial likelihood

**Theory.** Suppose exactly one event happens at time t. Given that one of the rows in R(t) fails at
t, the probability that it is row i is its hazard over the total hazard of the risk set:

```math
P(\text{row } i \text{ fails at } t \mid \text{one failure in } R(t) \text{ at } t)
 = \frac{\lambda_0(t)\,e^{\eta_i}}{\sum_{j \in R(t)} \lambda_0(t)\,e^{\eta_j}}
 = \frac{e^{\eta_i}}{\sum_{j \in R(t)} e^{\eta_j}} .
```

The baseline hazard cancels. Multiplying these conditional probabilities over the K distinct event
times gives Cox's **partial likelihood**, and its logarithm:

```math
L(\beta) = \prod_{k=1}^{K} \frac{e^{\eta_{(k)}}}{\sum_{j \in R(t_k)} e^{\eta_j}}, \qquad
\ell(\beta) = \sum_{k=1}^{K} \Big[\eta_{(k)} - \log \sum_{j \in R(t_k)} e^{\eta_j}\Big],
```

where (k) is the row failing at tₖ. It is "partial" because it uses only the order in which rows
fail, not the spacing of the event times; the discarded part carries information mainly about λ₀.
Cox (1975) showed that maximizing it gives consistent, asymptotically normal estimates, and that
its derivatives can be used like those of an ordinary likelihood.

## 2.3 Why the baseline hazard can be ignored: the profile view

**Theory.** A second argument connects the partial likelihood to the baseline estimate the package
reports after the fit. Let the baseline put a hazard jump hₖ at each event time tₖ and nothing
elsewhere (the nonparametric maximum-likelihood choice). The full likelihood of the data is then,
for unweighted data,

```math
L(\beta, h) = \prod_{k} \Big[\prod_{i \in D(t_k)} h_k\, e^{\eta_i}\Big] \exp\Big\{-\sum_{k} h_k\, S_0(t_k)\Big\},
\qquad S_0(t) = \sum_{j \in R(t)} e^{\eta_j}.
```

Setting ∂ log L / ∂hₖ = dₖ/hₖ − S₀(tₖ) to zero gives ĥₖ = dₖ / S₀(tₖ). Substituting back,

```math
L(\beta, \hat h) = \prod_k d_k^{d_k} e^{-d_k} \;\times\; \prod_k \frac{\exp\big(\sum_{i \in D(t_k)} \eta_i\big)}{S_0(t_k)^{d_k}} ,
```

and the first factor does not depend on β. So the Breslow partial likelihood (section 2.9) is the
**profile likelihood** of β, and the maximizing jumps ĥₖ = dₖ/S₀(tₖ) are exactly the Breslow
baseline increments that `CoxStratum.baseline` computes at β̂ (section 3.10).

## 2.4 The general objective: weights, offsets, strata, truncation

**Implementation and theory agree here.** With case weights, offsets, strata and (a, b] intervals,
and Breslow's handling of ties, the log partial likelihood that pprof_spark maximizes is (spec
[first-slice.md](../../spec/cox/first-slice.md) §2 and
[efron-weights-offsets.md](../../spec/cox/efron-weights-offsets.md) §2):

```math
\ell(\beta) = \sum_{s} \sum_{t \in T_s} \Big[\sum_{i \in D_s(t)} w_i\,\eta_i \;-\; d_w(t)\,\log S_0(t)\Big],
\qquad S_0(t) = \sum_{j \in R_s(t)} w_j\, e^{\eta_j},
```

where Tₛ is the set of distinct event times of stratum s, Dₛ(t) the events at t with positive
weight, and d_w(t) their total weight. Strata simply add: each stratum has its own risk sets and
event times, and they share β. A stratum without events adds nothing.

## 2.5 The score

Differentiate log S₀: since ∂S₀/∂β = Σ wⱼ e^{ηⱼ} xⱼ = S₁,

```math
U(\beta) = \frac{\partial \ell}{\partial \beta}
 = \sum_{s} \sum_{t} \Big[\sum_{i \in D_s(t)} w_i\, x_i \;-\; d_w(t)\,\bar x(t)\Big],
\qquad \bar x(t) = \frac{S_1(t)}{S_0(t)} .
```

**Intuition.** Pick a row from the risk set with probability πⱼ = rⱼ/S₀(t), proportional to its
weighted relative risk. Then x̄(t) is the expected covariate of the failing row under the model, and
the score is the sum over events of **observed minus expected covariates**. At β̂ the two balance:
U(β̂) = 0. If high-x1 rows fail more often than the model predicts, U is positive and β increases.

## 2.6 The information matrix and concavity

Differentiate again: ∂x̄/∂βᵀ = S₂/S₀ − x̄x̄ᵀ, so

```math
I(\beta) = -\frac{\partial^2 \ell}{\partial \beta\, \partial \beta^{\top}}
 = \sum_{s} \sum_{t} d_w(t)\Big[\frac{S_2(t)}{S_0(t)} - \bar x(t)\,\bar x(t)^{\top}\Big].
```

The bracket is the covariance matrix of x under the same "pick a row from the risk set"
distribution. A covariance matrix is positive semidefinite, so I(β) is too, and **ℓ is concave**.
That is why Newton's method from β = 0 is reliable for Cox models (chapter 3). I(β) is singular
exactly when some combination of covariates has zero variance within every risk set that matters:
a covariate constant within strata, or a linear combination of others. pprof_spark detects that
during the Cholesky factorization and fails, naming the covariates (X-011, section 3.6).

## 2.7 Exact invariances the implementation relies on

These follow from the formulas and are used both by the code and by the tests.

| Transformation | Effect | Used for |
|---|---|---|
| Add a constant c to every η of a stratum | ℓ, U, I unchanged: e^c cancels from the ratio | No intercept (X-001); internal centering of covariates; the reason a per-stratum shift of η would be free (review finding F-03) |
| Translate a covariate, xⱼ → xⱼ + c | Adds cβⱼ to every η: β̂ unchanged | Centering; metamorphic test |
| Scale a covariate, xⱼ → c·xⱼ | β̂ⱼ → β̂ⱼ / c, SE likewise | Metamorphic test |
| Strictly increasing transform of time | Only the order of times matters: ℓ and β̂ unchanged | Metamorphic test (time² + 5) |
| Split (a, b] at s into (a, s] without event and (s, b] | Every risk set unchanged | Time-varying covariates; metamorphic test |
| Offset c·xⱼ | β̂ⱼ falls by c | Metamorphic test |
| Multiply every weight by c | β̂ unchanged; model-based SE divided by √c | Metamorphic test |

**Centering in pprof_spark.** The kernel subtracts the column means of the covariates before forming
η (`xc(j) = x(r * p + j) - center(j)` in
[CoxStratum.scala](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L76-L86)).
By the first two rows of the table this changes nothing mathematically, while keeping exp(η) away
from overflow when covariates have large means (age in years, say). The means are computed once,
deterministically, in the summary pass (chapter 4, component C4). Offsets are not centered.

## 2.8 Worked example at β = 0

At β = 0 every row has relative risk 1, so S₀(t) is the number at risk, x̄(t) is the plain mean of x1
over the risk set, and the bracket of I is the plain (population) variance. With exact fractions:

| t | n at risk | events, their x1 | x̄(t) | Var(t) | ℓ term | U term = Σx − d·x̄ | I term = d·Var |
|---:|---:|---|---:|---:|---|---:|---:|
| 1 | 8 | 0.5, −0.25 | 3/32 | 399/1024 | −2 log 8 | 1/16 | 399/512 |
| 2 | 6 | 0 | 1/12 | 17/36 | −log 6 | −1/12 | 17/36 |
| 3 | 4 | −1, 0.75 | −1/8 | 29/64 | −2 log 4 | 0 | 29/32 |
| 4 | 2 | −0.5 | −1/8 | 9/64 | −log 2 | −3/8 | 9/64 |

Totals: ℓ(0) = −(2 log 8 + log 6 + 2 log 4 + log 2) = −log 12288 ≈ −9.41637845538745, U(0) = −19/48 =
−0.3958333…, I(0) = 10591/4608 = 2.2983940972222….

What the code and R return for the same quantities
([probe](probe/README.md) output, Part A; R's values from
[r_survival.json](../../../fixtures/cox/tiny-ties/r_survival.json)):

| Quantity | pprof_spark kernel | R survival 3.5-8 | Relative difference |
|---|---|---|---|
| ℓ(0) | −9.4163784553874520 | −9.4163784553874520 | 0 |
| U(0) | −0.39583333333333330 | −0.39583333333333337 | 1.4e-16 |
| I(0) | 2.2983940972222223 | 2.2983940972222223 | 0 |

The kernel and R differ only in the last bit of U, and both are within one unit in the last place of
the exact −19/48. At β = 0.375, the fixture's second test point, the three quantities agree with R to
2.1e-16 or better.

The score is negative: at β = 0, the failing rows have lower x1, on average, than their risk sets
(row 7 at t = 4 contributes −0.375 on its own), so the fit will move β below zero.

## 2.9 Tied event times

**Why ties occur.** The model is in continuous time, where two events never coincide. Real data are
recorded in days or months, so events coincide often: §2.3 of the project context plans for up to
about 10⁴ distinct event times per stratum at daily resolution over several years, with many
patients sharing each day.

**Theory.** If d events are tied at t, the exact continuous-time contribution averages the partial
likelihood over all d! orders in which they could have occurred (the "exact" or "averaged" method),
or treats time as discrete (the "discrete" method, a conditional logistic likelihood). Both cost
combinatorially more than the approximations below, and neither is implemented: exact ties are a
non-goal (§2.5 of the project context). Two approximations are standard.

**Breslow** gives every tied event the full risk set as its denominator:

```math
\text{contribution at } t:\quad \sum_{i \in D(t)} w_i\,\eta_i \;-\; d_w \log S_0(t).
```

This is the formula of section 2.4, and it is what the profile likelihood of section 2.3 produces.
Its weakness is that after the first of the tied rows fails it is still counted in the denominator
for the others, so the denominators are too large and the estimates are biased toward zero when ties
are heavy relative to the size of the risk sets.

**Efron** corrects this on average. In whatever order the d tied events happened, by the time of the
k-th of them some of the tied rows had already failed; Efron removes, at every step, the *average*
share of the tied rows' risk. With D₀, D₁, D₂ the sums of r, rx and rxxᵀ over the tied events, a mean
event weight m = d_w/d, and for k = 1, …, d

```math
A_k = (S_0 - D_0) + \tfrac{k}{d}\,D_0,\qquad
B_k = (S_1 - D_1) + \tfrac{k}{d}\,D_1,\qquad
C_k = (S_2 - D_2) + \tfrac{k}{d}\,D_2,
```

the contributions at t are

```math
\ell \mathrel{+}= \sum_{i \in D(t)} w_i \eta_i - m \sum_{k=1}^{d} \log A_k, \qquad
U \mathrel{+}= \sum_{i \in D(t)} w_i x_i - m \sum_{k=1}^{d} \frac{B_k}{A_k}, \qquad
I \mathrel{+}= m \sum_{k=1}^{d} \Big[\frac{C_k}{A_k} - \frac{B_k}{A_k}\frac{B_k^{\top}}{A_k}\Big].
```

As k runs from 1 to d, the fraction of the tied mass removed, 1 − k/d, runs over (d − 1)/d, …, 1/d,
0: the same terms as the textbook form Σ_{r=0}^{d−1} log(S₀ − (r/d)D₀), in the opposite order. With
d = 1 the single term is A₁ = S₀, and Efron equals Breslow. The mean-weight form with m = d_w/d is the
convention of R's `survival` and of pprof_py, which the fixtures confirm.

## 2.10 Ties in the worked example

At β = 0 the tied rows have relative risk 1 each, so D₀ = d.

| t | S₀ | d | Breslow denominators | Efron denominators A₁, …, A_d |
|---:|---:|---:|---|---|
| 1 | 8 | 2 | 8, 8 | 6 + ½·2 = 7, then 8 |
| 3 | 4 | 2 | 4, 4 | 2 + ½·2 = 3, then 4 |

Hence Efron's ℓ(0) = −log(7·8·6·3·4·2) = −log 8064 ≈ −8.99516499031115, against Breslow's
−log 12288 ≈ −9.41637845538745. The tie corrections also change the derivatives:

| t | Breslow U term | Efron U term | Breslow I term | Efron I term |
|---:|---:|---:|---:|---:|
| 1 | 1/16 = 0.062500 | 15/224 = 0.066964 | 0.779297 | 0.814712 |
| 2 | −1/12 | −1/12 | 0.472222 | 0.472222 |
| 3 | 0 | 0 | 0.906250 | 0.802083 |
| 4 | −3/8 | −3/8 | 0.140625 | 0.140625 |
| Total | −19/48 = −0.395833 | −263/672 = −0.391369 | 2.298394 | 2.229643 |

Untied times (2 and 4) are identical under both methods. The kernel's Efron values at β = 0 agree
with R to 7.1e-16 (score) and 4.0e-16 (information), and its log-likelihood is bitwise equal to R's.
At convergence the Breslow estimate is β̂ = −0.171513 and the Efron estimate −0.174708 (chapter 9):
with these heavy ties, Efron's estimate is the one further from zero, as the theory predicts.

## 2.11 Implications for users and reviewers

- **Defaults differ from R.** pprof_py and pprof_spark default to Breslow (D-06); R's `coxph`
  defaults to Efron. Always state the tie method when comparing with R; the fixtures always pass
  `ties` explicitly. Swapping the method moves the fixtures' coefficients far outside the T-coef
  tolerance, which is why the parity tests use it as a negative control.
- **Replication.** Under Breslow an integer weight k equals k copies of the row. Under Efron it does
  not: copies of an event row are tied with each other and Efron counts them in d.
- **Zero weights.** d counts only events with positive weight, so a zero-weight row is the same as
  an absent row under both methods (X-013, corrected relative to pprof_py).
- **Baseline and residuals follow the tie method.** Under Efron the baseline increment at a tied
  time is Σₖ m/Aₖ rather than d_w/S₀, and martingale and score residuals carry per-tied-death
  corrections (chapter 3, sections 3.10 and 3.12).
- **Near-ties are not merged.** R's `coxph` merges times that differ by less than about √ε relative
  (`timefix = TRUE`). pprof_spark compares times exactly (spec first-slice §5), so times computed in
  floating point, such as days divided by 365.25, can split what should be one tie into several
  untied times. Integer time units avoid the problem; the fixtures use integer times so that both
  rules agree.

Where this happens in the code: the Breslow branch (also used for Efron when d = 1) is
[CoxStratum.scala lines 155–172](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L155-L172),
and the Efron loop over k is
[lines 173–206](../../../numerics/src/main/scala/pprof/spark/numerics/kernels/CoxStratum.scala#L173-L206).
Chapter 4 walks through both.
