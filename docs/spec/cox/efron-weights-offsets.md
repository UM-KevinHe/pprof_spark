# Specification addendum: Efron ties, case weights and offsets

- Status: **Approved in advance by the maintainer on 2026-10-06 (D-21)**, who asked for the work to
  continue. X-013 below was found afterwards; the maintainer was asked to confirm it and replied
  "Continue", so it stands as recommended under the standing delegation (reversible: one condition
  in the kernel). Implemented in round 11.
- Extends: [first-slice.md](first-slice.md). Everything not stated here is unchanged.
- Reference: pprof_py v0.7.0 (`EfronTies`, `_efron_accumulate_numba` and `BreslowTies` in
  `algorithms/survival/ties.py`), R 4.3.3 `survival::coxph` 3.5-8 (`coxfit6.c`).
- Fixtures: all four Cox cases for both tie methods; `rc-stratified-weights-offset` carries
  non-integer weights in [0.5, 2] and offsets in [−0.5, 0.5].

## 1. Model

ηᵢ = xᵢᵀβ + oᵢ, where the offset oᵢ is known and enters with coefficient 1. Each row carries a case
weight wᵢ ≥ 0. A row with wᵢ = 0 is treated as absent from the fit (X-013).

## 2. Objective

With rᵢ = wᵢ·e^{ηᵢ}, S₀(t) = Σ_{Rₛ(t)} rᵢ, S₁(t) = Σ_{Rₛ(t)} rᵢxᵢ and S₂(t) = Σ_{Rₛ(t)} rᵢxᵢxᵢᵀ. At an
event time with tied events Dₛ(t), let d be the number of tied events with positive weight,
d_w = Σ_{Dₛ(t)} wᵢ, and D₀, D₁, D₂ the same sums as S₀, S₁, S₂ taken over Dₛ(t) only.

- Event terms, both methods: ℓ += Σ_{Dₛ(t)} wᵢηᵢ and U += Σ_{Dₛ(t)} wᵢxᵢ.
- Breslow, or Efron with d = 1: ℓ −= d_w·log S₀; U −= d_w·S₁/S₀; I += d_w·(S₂/S₀ − (S₁/S₀)(S₁/S₀)ᵀ).
- Efron with d ≥ 2: with m = d_w/d and, for k = 1 to d, f = k/d, Aₖ = (S₀ − D₀) + f·D₀,
  Bₖ = (S₁ − D₁) + f·D₁ and Cₖ = (S₂ − D₂) + f·D₂: ℓ −= m·log Aₖ; U −= m·Bₖ/Aₖ;
  I += m·(Cₖ/Aₖ − (Bₖ/Aₖ)(Bₖ/Aₖ)ᵀ).

These are pprof_py's formulas (`meanwt`, the k = 1…d loop, and the d = 1 shortcut); R's coxfit6 uses
the same mean-weight form, and the fixtures agree to T-fn.

## 3. Data contract additions

| Column | Type | Rule |
|---|---|---|
| weight, optional | numeric | Finite and ≥ 0; a zero weight removes the row from the fit (X-013) |
| offset, optional | numeric | Finite |

As before, every violation is reported at once with counts, never values. The fit fails when no
event has a positive weight (X-012).

## 4. Conventions

| Convention | Value |
|---|---|
| Tie methods | Breslow (default, D-06) and Efron, as §2 |
| Case weights | Frequency-type in the likelihood; the variance stays model-based, I(β̂)⁻¹, as in pprof_py; R's default for non-integer weights is robust, so the fixtures use `robust = FALSE` (X-009) |
| Weights and replication | Under Breslow an integer weight k equals k copies of the row; under Efron it does not, because d counts rows (as in pprof_py and R) |
| Offsets | Added to η; not centered |
| Counts reported | Observations and events count rows, as pprof_py's `n_obs_` and `n_events_` do |

## 5. Algorithm

The kernel of first-slice §6 carries a weight and an offset per row. Within a time group it adds
rᵢ, rᵢxᵢ and rᵢxᵢxᵢᵀ to the running Neumaier sums; for events with positive weight it also adds them
to plain per-time sums D₀, D₁, D₂ and counts d. Contributions of §2 are added to the stratum's
Neumaier totals in sweep order, the Efron terms for k = 1 to d. Canonical order adds weight and
offset to the final tie-breakers, and the data fingerprint covers them.

## 6. Edge cases

| Case | pprof_py v0.7.0 (probed) | R 3.5-8 | pprof_spark |
|---|---|---|---|
| Negative or non-finite weight | Raises | Raises | Fails at validation with counts |
| Zero weight, Breslow | Equals dropping the row | Rejects any zero weight | Equals dropping the row |
| Zero-weight event tied with other events, Efron | Still counted in d: 0.67% coefficient change on tiny-ties against dropping the row | Rejects | Not counted: equals dropping the row (X-013) |
| Every event has zero weight | — | Rejects | Fails (X-012) |
| Non-finite offset | Raises | Raises | Fails at validation |

## 7. Validation plan

| Level | Test | Class |
|---|---|---|
| Function | ℓ, U, I at β = 0 and the fixed β, both methods, all four fixtures, against pprof_py and R | T-fn |
| Lockstep | Iterates before the final one, both methods, all four fixtures | T-iter |
| End to end | Tight and default fits, both methods, all four fixtures | T-coef, T-var, T-fn |
| Negative controls | Each method's estimates differ from the other method's beyond T-coef | — |
| Metamorphic | Scaling every weight by 3 leaves β̂ unchanged; an offset c·xⱼ lowers β̂ⱼ by c; a zero weight equals dropping the row under both methods; under Breslow, weight 2 equals a duplicated row | T-coef |
| Validation | Negative weights, non-finite offsets, all event weights zero | exact behavior |
| Reproducibility | R0 bitwise across row orders with weights and offsets; R2 across JDKs and Spark APIs | bitwise |

## 8. Known discrepancies

| ID | Class | Summary | Decision |
|---|---|---|---|
| X-013 | B | A zero-weight event tied with others still counts in pprof_py's Efron d, so a zero weight is not the same as dropping the row, contrary to pprof_py's own validation message ("it drops that observation"); R rejects zero weights | Corrected: d counts events with positive weight. Accepted 2026-10-06 (D-21) |
| X-009 | B | R reports robust variance for non-integer weights by default | Unchanged: model-based, as pprof_py |
