# Specification: Cox / the weighted-Gram information

- Status: **Approved** (2026-11-22, the maintainer) as D-80. The fixture is built in round 184 (§13).
- Decision references:
  - D-75: performance step 3;
  - D-78: the re-baselining rule, which names the weighted-Gram identity among changes of summation order;
  - D-79: the golden runs' fixed points.
- Parity matrix entry: unchanged. The Cox information keeps T-fn at fixed β, and T-coef and T-var at convergence.

## 1. Purpose and scope

Performance step 3 (plan §4) computes the Cox information I(β) in the weighted-Gram form (study §4.1), with Breslow
and Efron ties, entry times, case weights and offsets.

**Unchanged.** The model, the objective, ℓ, U, the Newton iteration and every output stay as they are. Only how I is
summed changes. D-78 governs that change, and the golden runs are re-baselined under it.

**Scope.** `CoxStratum.add`, the kernel of every Newton evaluation. The baseline, residuals, measures and robust
variances keep their kernels.

**Why, after step 2.** Round 182 made the S₂ sweep's sums chunked; the sandbox measured Cox evaluation 2.5 times
faster. What remains costly is the p×p work that S₂ needs beyond one update per row:
- a p×p removal per truncated row;
- per event time, or per Efron step, a p×p read of S₂ with a division per element.

The weighted-Gram form has none of either (§7).

## 2. Notation

Per stratum, rows are in canonical order: exit time descending, events first (first-slice §6).
- **Covariates.** xᵢ are the centred covariates x − c, with c the fit's column means, as the kernel uses now.
- **Risk scores.** rᵢ = wᵢ·exp(xᵢᵀβ + oᵢ). Rows with wᵢ = 0 contribute nothing (X-013).
- **Event times.** t runs over the distinct exit times with at least one event of positive weight.
- **Risk set.** R(t) = {i : wᵢ > 0, entryᵢ < t ≤ exitᵢ} (counting-process-baseline §1).
- **Tied events.** E(t) = {i ∈ R(t) : exitᵢ = t, eventᵢ}, with d_t = |E(t)| and D_t = Σ_{E(t)} wᵢ.
- **Risk-set sums.** S_k(t) = Σ_{R(t)} rᵢ·xᵢ^{⊗k}. **Tied sums.** T_k(t) = Σ_{E(t)} rᵢ·xᵢ^{⊗k}, the code's D₀, D₁
  and D₂.
- **Steps.** A time has steps s, as the kernel defines them now:
  - Breslow, or Efron with d_t = 1: one step, with weight a_t = D_t, denominator A_{t1} = S₀ and mean
    m̄_{t1} = S₁/S₀.
  - Efron with d_t ≥ 2: steps s = 1, …, d_t, with f_s = s/d_t, weight a_t = D_t/d_t, denominator
    A_{ts} = (S₀ − T₀) + f_s·T₀ and mean m̄_{ts} = ((S₁ − T₁) + f_s·T₁)/A_{ts}.
- **Hazard increments.** These are exactly what the martingale residuals compute (`CoxResiduals`):
  h_t = Σ_s a_t/A_{ts} and c₀_t = Σ_s f_s·a_t/A_{ts}, so h_t = c₀_t for one step. ψ_t = h_t − c₀_t is zero for one
  step.

## 3. The identity

Today's information (first-slice §2; efron-weights-offsets §2) is

    I(β) = Σ_t Σ_s a_t · [ (S₂(t) − (1 − f_s)·T₂(t)) / A_{ts} − m̄_{ts}·m̄_{ts}ᵀ ],

with f_s = 1 for one step. Sum the first part over steps: it is h_t·S₂(t) − ψ_t·T₂(t). Then exchange the order of
summation over times and rows:

    Σ_t [ h_t·S₂(t) − ψ_t·T₂(t) ] = Σ_i rᵢ·(Λᵢ − ψᵢ)·xᵢxᵢᵀ.

Here Λᵢ = Σ h_t over the event times t ∈ (entryᵢ, exitᵢ], the cumulative hazard while row i is at risk.
ψᵢ = ψ_{exitᵢ} when i ∈ E(exitᵢ), and 0 otherwise. Hence

    I(β) = Σ_i ωᵢ·xᵢxᵢᵀ − Σ_t Σ_s a_t·m̄_{ts}·m̄_{ts}ᵀ,      ωᵢ = rᵢ·(Λᵢ − ψᵢ).

Λᵢ − ψᵢ is the hazard of row i's martingale residual Mᵢ = δᵢ − exp(ηᵢ)·(Λᵢ − ψᵢ), including its Efron correction. The
identity is exact in real arithmetic; in floating point it changes how I is summed (D-78). ℓ and U keep their present
sweep, so at fixed β they keep their bits.

## 4. Algorithm

Per stratum, in `CoxStratum.add`:

1. **The sweep** (pass 1). This is the present descending sweep, with S₀ and S₁ as running Neumaier sums; S₂ is no
   longer kept.
   - Rows are added at their exit time and removed at their entry, at O(p) each.
   - At each event time:
     - ℓ and U are added as now;
     - h_t and c₀_t are computed as the residuals compute them;
     - each step's rank-one term a_t·m̄_{ts}·m̄_{ts}ᵀ is added to a chunked compensated sum M, one term per step
       (step2-chunks.md §5.1).
   - Kept for pass 2: rᵢ, and per event time its time, h_t, ψ_t and d_t.
2. **Cumulative hazards.** H runs over the event times in ascending time as a Neumaier sum. Its sum and compensation
   are kept as two arrays, Hˢ and Hᶜ.
3. **The Gram** (pass 2). For each row of positive weight, in canonical order:
   - b and a are the last event times at or before exitᵢ and entryᵢ, found by binary search as the residuals find
     them;
   - Λᵢ = (Hˢ_b − Hˢ_a) + (Hᶜ_b − Hᶜ_a), with a missing index counting as zero;
   - ωᵢ = rᵢ·(Λᵢ − ψᵢ);
   - ωᵢ·xᵢxᵢᵀ is added to a chunked compensated sum G, one term per row, each term computed as (ωᵢ·xᵢⱼ)·xᵢₗ.
4. **The stratum's information** is G − M, one subtraction per element, added to the stratum's totals as now.

The stratum's rows are in memory as its records (step2-chunks.md §3), so pass 2 adds no Spark pass. Chunks of G run
over the stratum's rows of positive weight, chunks of M over its steps, and each last chunk ends with the stratum. With
the canonical order and a fixed L, R0 holds per layout.

## 5. Numerical analysis

- **Agreement.** Both forms sum products of the same quantities, grouped differently. The probe (§7) agreed with
  round 182's kernel within 3.0·10⁻¹⁶ to 4.5·10⁻¹⁶, scaled. That covers Breslow and Efron, with and without
  truncation.
- **The final difference.** I = G − M is one difference taken at the end, where today each time's term is a
  difference.
  - With centred covariates, M is small against G where strata's covariate means lie near the fit's centre.
  - Where a stratum's mean m̄ lies far from the centre, for instance strata by provider, G and M share a part near
    D·m̄m̄ᵀ, which cancels. Relative errors grow by about |m̄|²/var.
  - Today's per-time difference S₂/S₀ − m̄m̄ᵀ grows them by the same factor, so the accuracy class is unchanged.
    D-78's comparison measures it.
- **Removals.** No p×p term is removed, so counting-process-baseline's truncation hazard no longer touches I. S₀ and
  S₁ keep their Neumaier removals.
- **Late entries.** Λᵢ is a difference of two cumulative hazards. When row i enters late in a long stratum, the two
  are close, and a plain difference of their values would lose relative accuracy in proportion to H/Λᵢ. The
  compensated difference of §4 avoids this:
  - Hˢ_b − Hˢ_a is exact when the two lie within a factor of two (Sterbenz), and the compensations' difference
    restores what the sums dropped;
  - so Λᵢ keeps a relative accuracy near u however late the entry.

  The residuals keep their present Λ, so their outputs do not change.
- **Non-finite values.** A non-finite rᵢ or S₀ makes h, ω and I non-finite, as now, and step halving treats it as a
  decrease (first-slice §6).

## 6. Edge cases

| Case | Result |
|---|---|
| A stratum without events | No event times: Λᵢ = 0, and G = M = 0 |
| An entry equal to an event time | The row is outside R(t) at that time: the time is subtracted through a, as entryᵢ < t requires |
| An event of zero weight | Outside E(t) and the risk set, as now |
| Efron with d_t = 1 | One step, ψ_t = 0: the Breslow formula, as now |
| All rows entering at or below the first event time | Λᵢ is the plain cumulative hazard at exit; right-censored results are reproduced |

## 7. Cost, measured

The probe (`weighted-gram-probe/GramProbe.scala`, round 183) measured one stratum of 20,000 rows at 180 features,
with 4,000 distinct times (about three events per time). It ran in the sandbox on one core with JDK 17, taking the best
of five runs after three warm-ups.

| Ties | Truncated rows | Round 182 kernel, µs per row | Weighted Gram, µs per row | Ratio |
|---|---|---|---|---|
| Breslow | none | 14.19 | 12.80 | 1.11 |
| Efron | none | 34.66 | 16.26 | 2.13 |
| Breslow | 40% | 21.43 | 14.39 | 1.49 |
| Efron | 40% | 41.65 | 17.12 | 2.43 |

Per stratum, pass 1 costs O(n·p) plus O(K·p²) plain multiply-adds, where K counts steps. Pass 2 costs O(n·p²) plain
multiply-adds plus O(n·log K). Today the kernel costs O(n·p²) plus O(removals·p²) plus O(K·p²) with two divisions and a
compensated read per element. Memory is the same: G and M replace S₂ and the stratum's information, and the per-time
arrays are O(K).

## 8. Distributed plan

Unchanged. Each Newton evaluation is one Spark pass, with the same partials folded in reduction groups (step 1), no
shuffle, and the same bytes to the driver.

## 9. Validation plan

- **Numerics.**
  - Exact references: brute-force BigDecimal information under Breslow and Efron, over strata with ties, late entries
    (some on event times), zero weights and offsets, crossing chunks; within 10⁻¹³ scaled.
  - Against round 182's kernel: the S₂ kernel kept in the test tree as a reference, with random strata within 10⁻¹³
    scaled, and ℓ and U identical to the bit.
- **Fixtures.**
  - The six Cox cases under both tie methods, at their tolerance classes: T-fn at fixed β, T-coef and T-var.
  - One new case, `lt-heavy-ties`, as plan §4 asks ("fixtures for Efron ties with truncation"). The truncated cases
    now have integer times from 1 to 60, so a stratum seldom has two tied deaths. The new case has 600 rows, 3
    features, 10 strata, times from 1 to 8, weights, offsets, and half the rows entering late at an integer time, so
    that entries fall on event times.
  - Its references come from pprof_py at the pinned commit and R's `survival::coxph` with `ties = "efron"` and
    `"breslow"`, through `reference/fixtures/generate.py` as for the other cases (`REFERENCE.lock`).
- **Golden runs.** Re-baselined under D-78 and D-79:
  - the fixed points' ℓ and U identical to the bit;
  - their information within 10⁻¹³ scaled;
  - converged values within T-part;
  - counts identical;
  - R0 at 1 and 7 partitions.

## 10. Rounds

1. **This specification** (round 183), for approval as D-80.
2. **The fixture.** `lt-heavy-ties` is generated and added to the Cox fixture suites, and the present kernel must pass
   it. No bit of a fit changes. This needs the reference environment: pprof_py at the pinned commit, and R 4.3.3 with
   survival 3.5-8.
3. **The kernel.**
   - The specifications first (NN-2): first-slice §6 and efron-weights-offsets §5 for I's sums, and
     counting-process-baseline for removals that no longer touch I.
   - Then `CoxStratum.add` in the weighted-Gram form, with the numerics tests of §9.
   - The golden runs re-baselined under D-78, and the sandbox measured against round 182's build.
4. **The cluster's measurement** at step 3's exit (plan §4): Cox evaluation's CPU per row, and parity at all three
   levels.

## 11. Reference mapping

pprof_py and R compute the information time by time from risk-set sums. Their outputs are compared exactly as now. The
grouping differs, which is a class D discrepancy (first-slice's table: summation order, within the tolerance classes by
design).

## 12. Known discrepancies

None new. First-slice's summation-order row (class D) extends to the weighted-Gram form.

## 13. As built

**Round 184, the fixture.**
- **Generation.** `lt-heavy-ties` (seed 7, times 1 to 8) was generated with the pinned environment: R 4.3.3 with
  survival 3.5-8, pprof_py at `9320766`, and the requirements of `reference/fixtures/requirements.txt`. Regenerated in
  the same run, the 24 files of the six existing cases matched the committed manifest byte for byte.
- **Late entries.** The generator draws delayed entry for about half the rows, uniformly below exit, as for the other
  truncated cases. With times from 1 to 8, many draws are 0: 86 rows enter after time 0, 77 of them at an event time of
  their stratum.
- **Ties.** Of the case's 54 event times, 38 have two or more tied events; the largest has 38.
- **Calibration and suites.** `docs/parity/cox-calibration.md` gains its 22 rows, all within their classes. The case
  joins every Cox suite's list and the golden runs.
