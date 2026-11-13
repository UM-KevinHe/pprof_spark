# Plan: cluster terms in the random-effect models (part 4d)

- Status: approved with D-68 (2026-11-05). Slices 4d-1 and 4d-2 are written (rounds 135 to 144); slice 4d-3's
  specification was proposed in round 145 (D-74), which takes separate kinds for the cluster fits in place of §3's
  "format version 2".
- Parent: [plan.md](plan.md) (D-51), part 4d and §4.4; OI-93.
- References: pprof_py v0.7.0 (`9320766`) `LinearRandomEffectModel` and `LogisticRandomEffectModel` with
  `cluster_vars`; lme4 1.1-35.1, `lmer` and `glmer` with `(1 | provider) + (1 | cluster)`.

## 1. What pprof_py does with `cluster_vars`

`cluster_vars` adds crossed random intercepts, one per cluster factor, to the provider's. Round 134's probe used
3,000 records, 30 providers crossed with 8 clusters and one feature.

- **Linear model.** It fits θ for each factor, σ and β, and gives `summary`, the BLUPs of every factor, fitted
  values and predictions. Its provider-profiling methods (`test`, `calculate_standardized_measures`,
  `calculate_confidence_intervals`) refuse such a fit by name. Its θ matched `lmer`'s REML θ within 2·10⁻⁷, and β
  and σ to the printed digits.
- **Logistic model.** It fits a σ for each factor and β by Laplace, with pprof_py's two stages, nAGQ = 0 and then
  nAGQ = 1, and every method works:
  - `summary`;
  - `test` with `wald`, `poibin_exact`, `resampling`, and `exact`, which needs exactly one cluster factor (He et
    al.'s cluster mixture, as the three-stage model's);
  - the measures and the intervals.
  Its σ and β matched `glmer`'s Laplace fit within 4·10⁻⁵, the two optimizers' tolerances.
- **The random-cluster model of part 4c** takes its stage 2 from such a logistic fit, with the provider and one
  cluster factor. pprof_spark's slice 4c-1 takes the three-stage model's stage 2 until this part.

## 2. The computational core

With crossed intercepts, the penalized system's matrix is no longer diagonal. It has:
- a diagonal provider block;
- one diagonal block per cluster factor;
- off-diagonal blocks of provider × cluster sums.

The plan eliminates the provider block, the largest, as the three-stage model's stage 2 does (`ThreeStageGlmm`). It
then factors the Schur complement on the cluster levels, densely when they number a few thousand at most (the
driver budget decides; OI-59 beyond it).
- **The linear model's profiled deviance** needs, for each provider × cluster cell, the sums of the weights and of
  XᵀWX, XᵀWy and yᵀWy. One pass computes them; then the whole search runs on the driver, as 4a's search on
  provider sums does.
- **The logistic model's PIRLS** needs the same cell sums of the working weights and responses at every iteration,
  from the rows. They come from the blocks on the driver's copy (round 130) or from a pass. diag(H⁻¹) gives the
  posterior variances of the provider effects.
- **Several cluster factors** are crossed among themselves, so their union of levels is eliminated together.

## 3. Slices

| Slice | Content | References |
|---|---|---|
| 4d-1 | The linear model with cluster factors: REML and ML, θ per factor, the BLUPs of every factor, `summary`, fitted values and predictions; provider profiling refused as pprof_py refuses it | pprof_py; `lmer` |
| 4d-2 | The logistic model with cluster factors: the two-stage Laplace fit with a σ per factor; `summary`; `test` (`wald`, `poibin_exact`, `resampling`, and `exact` with one cluster factor); measures; intervals; σ's profile per factor if pprof_py offers it | pprof_py; `glmer` |
| 4d-3 | Persistence (format version 2 of both random-effect fits, with their factors), the jobs' `columns.clusters`, Python's `cluster_vars`; and the random-cluster model's stage 2 from such a logistic fit, as pprof_py takes it | pprof_py |

Each slice has a specification, then fixtures and calibration, then code and Python, as parts 4a to 4c had.

## 4. Fixtures

New cases join `fixtures/linear-re` and `fixtures/logistic-re`:
- one cluster factor;
- two cluster factors;
- text keys;
- prior weights (linear);
- offsets;
- a cluster factor whose σ reaches 0;
- clusters nested in providers.
Each case has pprof_py's fits at their defaults and tightened, and lme4's, as 4a and 4b had. Classes are proposed
from the calibration.

## 5. Points for the slices to settle

- Whether pprof_py's logistic `poibin_exact` and `resampling` condition on the cluster BLUPs (a plug-in) or integrate
  over them, and which fitted values its measures use.
- σ's profile per factor, and the boundary when one factor's σ is 0.
- The size of the cluster levels' Schur complement at the scale tests' points (OI-59).
- The persistence format's version 2 for fits that carry several factors, and reading version 1 fits unchanged.
