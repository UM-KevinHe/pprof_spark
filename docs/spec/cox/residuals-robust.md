# Specification: residuals and robust variance (Phase 1c)

- Status: **Approved on 2026-10-06 (D-25), including X-015.** Round 17 implements the residuals (§1, §3)
  and the fixtures (§5); round 18 implements robust variance (§2).
- Extends the Phase 1a and 1b specifications; everything not stated here is unchanged.
- Reference: pprof_py v0.7.0 (`inference/survival/residuals.py`: `martingale_residuals`,
  `score_residuals`, `dfbeta_residuals`; `inference/survival/robust.py`: `cluster_score_residuals`,
  `robust_covariance`; `CoxPH(robust=)` and `fit(cluster=)`), R 4.3.3 survival 3.5-8
  (`residuals.coxph` types martingale, score and dfbeta, from `agmart3.c` and `agscore3.c`;
  `coxph(robust = TRUE, cluster =)`).
- Scope (§13, Phase 1c): martingale residuals, correct under left truncation and Efron ties; score
  and dfbeta residuals; robust sandwich and clustered variance.

## 1. Residuals

At β̂, with rᵢ = exp(xᵢᵀβ̂ + oᵢ), the risk sets of Phase 1b, and the baseline increments dΛ₀(t):

| Residual | Definition |
|---|---|
| Martingale | Mᵢ = δᵢ − Êᵢ. Breslow: Êᵢ = rᵢ Σ_{t ∈ (aᵢ, bᵢ]} dΛ₀(t). Efron, at a time with d ≥ 2 tied events: a row at risk but not dying gains rᵢ Σₖ m/Aₖ, and each dying row gains rᵢ Σₖ (k/d)·m/Aₖ, k = 1 to d, the per-tied-death correction of R's agmart3 that pprof_py copies |
| Score | Uᵢ = ∫ (xᵢ − x̄(t)) dMᵢ(t), a p-vector, with Efron's per-tied-death means as in R's agscore3; not weighted, as R's default |
| dfbeta | wᵢ·Uᵢ V, with V the model-based covariance: the one-step change in β̂ when row i is dropped |

Weighted residuals sum to zero within each stratum's information: Σᵢ wᵢ Mᵢ = 0 per stratum and
Σᵢ wᵢ Uᵢ = U(β̂).

## 2. Robust variance

V_rob = V B V, B = Σ_c s_c s_cᵀ, s_c = Σ_{i ∈ c} wᵢ Uᵢ: the cluster sums of dfbeta, sandwiched. With
a cluster column, rows sharing a value form a cluster; with `robust = true` and no cluster column,
each row is its own cluster (pprof_py). R refuses per-row robust variance for (start, stop] data
without `cluster` or `id`; pprof_spark follows pprof_py and records a warning in the fit when entry
times are present without a cluster column, since a subject's rows then count as independent.

When robust variance is requested, standard errors, z, p-values and intervals use V_rob; the fit
also keeps V as `naiveCovariance`, as pprof_py keeps `naive_covariance_`.

## 3. Data contract and API

| Item | Rule |
|---|---|
| Cluster column, optional | Integral or string, no nulls; it implies `robust = true` (pprof_py) |
| `CoxOptions.robust` | Default false |
| `CoxPH.residuals(df, fit)` | A DataFrame keyed by the row identifier: martingale, score and dfbeta columns. It needs `CoxSpec.rowId`, so results join back to the input (pprof_py returns arrays in input order); it checks the data fingerprint (API-3) |

## 4. Algorithm and distributed plan

| Item | Plan |
|---|---|
| Residuals | One pass at β̂ over the StratumLocal blocks: a descending sweep forms the risk-set sums and per-time increments, and running totals over ascending event times give each row its sums over (aᵢ, bᵢ] as differences, as agmart3 and agscore3 do; O(n·p) per pass, no shuffle |
| Per-row robust B | Each block adds wᵢ² UᵢUᵢᵀ for its rows; partials combine in block order (ADR-0003) |
| Clustered robust B | One shuffle by cluster of (wᵢUᵢ, block, row position); each cluster sums its rows in that order with Neumaier sums; clusters fall into 256 buckets by a fixed hash of the key, each bucket adds its clusters' s_c s_cᵀ in key order, and the 256 partials combine in bucket order. The result does not depend on Spark's partitioning (R0) |
| Driver | B and V_rob, p×p |
| Output | The residual table stays distributed (DIST-1) |

## 5. Validation plan

| Level | Test | Class |
|---|---|---|
| Fixtures | For all six cases and both tie methods: pprof_py's and R's martingale, score and dfbeta residuals, and robust variances per row and with a cluster column (id mod 40) | — |
| Residuals | Against pprof_py and R | T-res |
| Robust variance | Against R everywhere; against pprof_py except where X-015 applies, where pprof_py's own dfbeta sandwich is the reference | T-var |
| Identities | Weighted residuals sum as §1 states; clusters of one row equal per-row robust variance; renaming clusters changes nothing | T-res, T-var |
| Reproducibility | R0 bitwise, including clustered variance across Spark partition counts; R1; R2 | bitwise or T-var |

## 6. Known discrepancies

| ID | Class | Summary | Decision |
|---|---|---|---|
| X-015 | B | pprof_py's robust variance with Breslow ties on (start, stop] data differs from R's by 11% (lt-weights-offset, 40 clusters), while a sandwich of pprof_py's own dfbeta residuals matches R to 7e-12; right-censored data and Efron ties agree to 3e-10 or better. The fault lies in pprof_py's counting-process cluster score kernel | Proposed: corrected, computing B from the score residuals as R does; report upstream (OI-33) |
| — | — | Per-row robust variance on (start, stop] data: R requires `cluster` or `id`; pprof_py and pprof_spark allow it, with a warning in pprof_spark | Follows pprof_py |
