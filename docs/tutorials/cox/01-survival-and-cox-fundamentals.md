# 1. Survival analysis and Cox fundamentals

[Tutorial home](README.md) · next: [2. Partial likelihood and ties](02-partial-likelihood-and-ties.md)

This chapter introduces the statistical objects the rest of the tutorial computes with. It assumes
basic statistics (likelihood, regression, the normal distribution) and no survival analysis.

## 1.1 Time-to-event data

Survival analysis studies the time T until an event: death, hospitalization, graft failure. Each
subject is followed from an origin (diagnosis, start of dialysis, transplant) until the event or
until follow-up stops. Two numbers describe what was observed:

- the observed time b = min(T, C), where C is the time follow-up stopped for another reason, and
- the event indicator δ = 1 if the event was observed (T ≤ C) and δ = 0 otherwise.

A row with δ = 0 is **censored**: we know only that T > b. Censoring is the reason ordinary
regression on b fails: treating censored times as event times underestimates survival, and dropping
censored rows discards information and biases toward short times.

The tutorial uses one small data set throughout, the `tiny-ties` fixture in
[fixtures/cox/tiny-ties/input.csv](../../../fixtures/cox/tiny-ties/input.csv). Read the times as
months and x1 as a standardized lab value:

| id | time b | event δ | x1 |
|---:|---:|---:|---:|
| 0 | 1 | 1 | 0.5 |
| 1 | 1 | 1 | −0.25 |
| 2 | 2 | 0 | 1.0 |
| 3 | 2 | 1 | 0.0 |
| 4 | 3 | 1 | −1.0 |
| 5 | 3 | 1 | 0.75 |
| 6 | 4 | 0 | 0.25 |
| 7 | 4 | 1 | −0.5 |

Six events and two censored rows. Two events share time 1 and two share time 3: these are **tied**
event times, which chapter 2 shows matter.

## 1.2 Censoring, truncation and the at-risk interval

**Independent censoring.** All the methods here assume that, given the covariates, censoring
carries no information about the event time: a subject censored at time c had the same future risk
as a subject still followed at c. The assumption cannot be checked from the data; it is a matter of
study design.

**Left truncation (delayed entry).** Sometimes a subject is observed only from a time a > 0. A
registry may enrol patients months after the start of dialysis; a patient who died before
enrolment never appears. Such a subject contributes information only about the interval after a:
including them in the analysis before a would treat them as having been at risk, and as having
survived, during time in which they could not have been observed to fail (the bias is sometimes
called immortal time bias).

**The at-risk interval.** Both situations are handled by giving each row an interval (a, b], open
on the left and closed on the right, during which it is **at risk**: under observation and able to
have the event. Right-censored data are the special case a = 0. In pprof_spark a row is at risk at
time t if and only if

```math
a_i < t \le b_i ,
```

so a row entering at t is not at risk at t, and a row censored at t is. This is the convention of R's
`survival` and of pprof_py (spec [counting-process-baseline.md](../../spec/cox/counting-process-baseline.md) §1).

**Counting-process rows.** Because each row is just an interval, one subject may contribute several
rows. Splitting (a, b] at s into (a, s] without an event and (s, b] with the original event leaves
every risk set unchanged, so the fit is unchanged; the test "splitting (a, b] at c into (a, c]
without an event and (c, b] leaves the fit unchanged" in
[CoxPHSuite.scala](../../../engine/src/test/scala/pprof/spark/engine/cox/CoxPHSuite.scala) checks
exactly this. Splitting is how time-varying covariates are represented (a new row whenever the
covariate changes) and how recurrent events are represented (one row per gap between events). Rows
of the same subject are then not independent, which matters for the variance (section 3.9).

## 1.3 Hazard, cumulative hazard and survival

The **survival function** S(t) = P(T > t) is the probability of being event-free at t. The
**hazard** is the instantaneous event rate among those still at risk:

```math
\lambda(t) = \lim_{\Delta \downarrow 0} \frac{P(t \le T < t + \Delta \mid T \ge t)}{\Delta}.
```

For a small Δ, λ(t)·Δ is approximately the probability that a subject who has survived to t has the
event in the next Δ. The **cumulative hazard** Λ(t) = ∫₀ᵗ λ(u) du accumulates it, and for a
continuous T

```math
S(t) = \exp\{-\Lambda(t)\}.
```

Example: a constant hazard of 0.1 per month gives Λ(10) = 1 and S(10) = e⁻¹ ≈ 0.368.

Hazards are the natural scale for censored data: at every time, the people still at risk are the
ones who can inform the event rate at that time, whatever happened to the others. Estimators built
from risk sets are therefore valid under independent censoring and under left truncation alike.

## 1.4 The Cox proportional hazards model

The Cox model relates the hazard of row i to its covariates:

```math
\lambda_i(t) = \lambda_0(t)\,\exp(\eta_i), \qquad \eta_i = x_i^{\top}\beta + o_i .
```

- λ₀(t) is the **baseline hazard**, the hazard of a row with x = 0 and offset 0. The model leaves it
  completely unspecified: any non-negative function of time. That is what makes the model
  **semiparametric** and robust to the shape of the hazard over time.
- β is a vector of p regression coefficients, the quantity pprof_spark estimates.
- oᵢ is an optional **offset** (section 1.7).

**Interpretation.** For two rows that differ by one unit in covariate j and are equal otherwise,

```math
\frac{\lambda_A(t)}{\lambda_B(t)} = \exp(\beta_j) \quad \text{for every } t .
```

exp(βⱼ) is the **hazard ratio** per unit of covariate j, and βⱼ its logarithm. In the tiny-ties fit
(chapter 9) β̂ = −0.1715, so the hazard ratio is exp(−0.1715) = 0.842: one more unit of x1 goes with
a 16% lower hazard. The 95% interval for β, [−1.461, 1.118], gives a hazard-ratio interval of
[0.232, 3.058], so eight rows cannot distinguish this from no effect (p = 0.79).

pprof_spark reports β̂ and its interval on the log scale (`CoxCoefficient.estimate`, `lower`,
`upper`); the hazard ratio and its interval are their exponentials.

## 1.5 The proportional hazards assumption

The model says the hazard ratio between any two covariate profiles is constant over time: the
hazard curves are proportional. If a treatment's benefit fades, or a risk factor matters mainly
early, the assumption fails and β̂ estimates a time-averaged log hazard ratio whose meaning depends
on the follow-up pattern.

The assumption is usually checked with scaled Schoenfeld residuals (R's `cox.zph`) or by plotting
log(−log Ŝ(t)) by group. **pprof_spark provides no proportional-hazards diagnostics**; check them on
a sample with R or pprof_py. Stratification (next section) is one remedy: a covariate that violates
proportionality can become a stratum, with its own baseline hazard.

## 1.6 Strata

A stratified model gives each stratum s its own baseline hazard and shares β:

```math
\lambda_i(t) = \lambda_{0s}(t)\,\exp(\eta_i) \qquad \text{for row } i \text{ in stratum } s .
```

Rows are compared only with rows of their own stratum, so β is estimated from within-stratum
contrasts. Provider profiling uses this directly: stage 1 of the two-stage SMR stratifies by
provider, so each facility has its own baseline and β adjusts only for patient characteristics
(spec [provider-workflows.md](../../spec/cox/provider-workflows.md)).

Strata matter twice in pprof_spark. Statistically, a stratum without events contributes nothing to
the fit (it is counted in `CoxFit.strataWithoutEvents`). Computationally, strata are the unit of
parallelism: chapter 5 shows that the engine distributes work across strata and never splits one.

## 1.7 Offsets

An offset is a term added to η with its coefficient fixed at 1. In the two-stage SMR, stage 2 uses
each row's stage-1 linear predictor as an offset, so the stage-1 risk adjustment is carried forward
without being re-estimated. In pprof_spark offsets enter η exactly as written and are not centered
(spec [efron-weights-offsets.md](../../spec/cox/efron-weights-offsets.md) §4).

A useful identity: an offset c·xⱼ is equivalent to moving c into the coefficient, so β̂ⱼ falls by
exactly c. The metamorphic test "weights scaled by 3 and an offset proportional to a covariate" in
`CoxPHSuite` checks this.

## 1.8 Case weights

A case weight wᵢ ≥ 0 multiplies row i's contribution to the likelihood. pprof_spark treats weights
as **frequency weights** in the likelihood and in the model-based variance, as pprof_py does:

- Under Breslow ties, an integer weight k is exactly equivalent to k copies of the row (tested in
  "under Breslow, an integer weight of 2 equals a duplicated row"). Under Efron it is not, because
  Efron's correction counts tied rows, not weights (chapter 2).
- A weight of 0 makes the row absent from the fit under both tie methods. pprof_py's Efron code
  still counts a zero-weight event among the tied events; pprof_spark does not, which is registered
  as X-013 and corrected (D-21). R rejects zero weights altogether.
- Negative or non-finite weights fail validation.
- Survey or inverse-probability weights are not frequency weights. Their model-based variance is
  wrong; use the robust variance (section 3.9). R's `coxph` switches to the robust variance by
  default when weights are not integers; pprof_py and pprof_spark do not (X-009).

## 1.9 Why there is no intercept

Adding a constant c to every ηᵢ multiplies every row's hazard by eᶜ, which is indistinguishable from
replacing λ₀(t) with eᶜλ₀(t). An intercept therefore cannot be estimated: the baseline absorbs it.
pprof_py accepts `fit_intercept=True` and then fails in prediction; pprof_spark has no intercept
option (X-001). The same invariance is what allows the engine to **center** covariates internally
without changing any result (chapter 2, section 2.7).

## 1.10 What pprof_spark expects as input

A fit is described by a `CoxSpec`
([CoxInput.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxInput.scala#L20-L40)),
which assigns roles to columns of a Spark DataFrame, and a `CoxOptions`
([CoxPH.scala](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxPH.scala#L43-L61)):

```scala
val spec = CoxSpec(
  time = "exit_day", event = "died", features = Seq("age", "diabetes"),
  strata = Some("facility"), rowId = Some("patient_row"),
  weight = None, offset = None, entry = Some("entry_day"), cluster = None)
val fit = CoxPH.fit(df, spec, CoxOptions(ties = Ties.Efron))
```

Validation
([`CoxValidation.validate`](../../../engine/src/main/scala/pprof/spark/engine/cox/CoxInput.scala#L63-L159))
runs one aggregation over the data and reports every problem at once, with column names and counts
and never values (NN-7). Nothing is dropped silently.

| Role | Accepted values | Rule |
|---|---|---|
| time (exit) | Any numeric type | Finite; positive when there is no entry column |
| event | Integral 0/1 or boolean | Anything else fails (pprof_py likewise rejects R's 1/2 coding) |
| features | Numeric, at least one | Finite; cast to double |
| strata | Integral or string | No nulls; ordered by key (integers numerically, strings by UTF-8 bytes) |
| rowId | Integral | No nulls, unique; needed for residuals; fixes the canonical row order |
| weight | Numeric | Finite and ≥ 0 |
| offset | Numeric | Finite |
| entry | Numeric | Finite and strictly below the exit time; may be zero or negative |
| cluster | Integral or string | No nulls; implies robust variance |

The fit fails before any computation when there is no event with a positive weight (X-012: the
partial likelihood would be constant), and later when covariates are aliased (X-011, section 3.6).
