# Spike: the three-stage SRR model (OI-51) and a plan for slice 2f

- Status: Spike report; its direction and sub-slices approved by the maintainer, 2026-10-08 (D-38).
  Each sub-slice gets its own specification before code (NN-2); 2f-1: [three-stage-preparation.md](three-stage-preparation.md);
  2f-2: [three-stage-stage3.md](three-stage-stage3.md).
- Reference: pprof_py v0.7.0 (`9320766`): `models/logistic/three_stage.py`
  (`LogisticThreeStageModel`), `data/glmm_prep.py` (`glmm_data_prep`), `models/logistic/random_effect.py`
  (`LogisticRandomEffectModel`), `models/logistic/fe_random_cluster.py` (`LogisticFERandomClusterModel`),
  `tests/logistic/test_three_stage.py` with `tests/data/three_stage` (R's `glmer` and `glmm.fac.hosp`).
  R 4.3.3 with lme4 1.1-35.1.
- Evidence: sandbox probes of round 42 on pprof_py's own R golden data (2,637 records).

## 1. What pprof_py's three-stage model is

It is He et al. (2013)'s model as R's `glmm.fac.hosp`, not the model PROJECT_CONTEXT §7.4 describes
(OI-48): providers (dialysis facilities) are crossed with clusters (hospitals), and the hospitals carry
random effects.

| Step | pprof_py | Statistics |
|---|---|---|
| Preparation | `glmm_data_prep` (R's `glmm.data.prep`) | Providers with more than `cutoff` (10) records; `y_adj` = y raised by 0.01/size for providers without events and lowered by it for providers with only events; provider × cluster cells, `included` when a cell has more than `cutoff` records |
| Stage 1 | `LogisticFixedEffectModel` with one effect per included cell, raw outcome | β (slice 2a's SerBIN) |
| Stage 2 | `LogisticRandomEffectModel`: crossed random intercepts (provider, cluster), offset xβ̂, intercept only, outcome `y_adj` | lme4-style: PIRLS on the joint random-effect system with H = I + AᵀWA, Laplace deviance, an nAGQ = 0 stage then nAGQ = 1 by Nelder–Mead; gives σ (the cluster SD), the providers' BLUPs and the intercept |
| Stage 3 | `LogisticFERandomClusterModel`, `estimator = "marginal"` (default) | Fixed provider effects γ with β and σ held fixed, cluster effects integrated by adaptive Gauss–Hermite quadrature (20 nodes, centred at each cluster's posterior mode); projected Newton on the marginal likelihood with a sparse Hessian (providers interact through shared clusters), line search, bounds median(γ) ± 10, convergence max abs(score) below 1e-5. `estimator = "he2013"` reproduces R's `glmm.fac.hosp` |
| Inference | Stage 3's `test`, standardized measures, intervals; `summary` is stage 1's | To be read for sub-slice 2f-4 |
| `sigma_sensitivity` | Stage 3 refitted at both ends of σ's profile-likelihood interval (stage 2's `profile_sigma`) | Flags' stability under σ's uncertainty |

pprof_py's own parity tests: stage 2's σ within 1e-4 of `glmer`, stage 3's `he2013` effects within 2e-4
of `glmm.fac.hosp`, SRR within 2e-4.

## 2. Probe results

| Probe | Result |
|---|---|
| Golden data | 2,637 records, 40 providers, 12 clusters, 94 cells (61 included in stage 1); the whole fit 1.2 s; stage 3 converged in 4 iterations |
| Stage 2 against lme4 | σ̂: pprof_py 0.422943038; `glmer` (nAGQ = 1) 0.422935834 by default and 0.422935194 with bobyqa to 1e-12: differences of 7e-6 and 6e-7, the scale of derivative-free optimizers' stopping |
| Reference environment | pprof_py's stage 2 asks for bobyqa from nlopt and falls back to another optimizer when nlopt is absent (it is not in the fixture environment), so its σ̂ depends on an optional package |
| Compression | Stage 3's marginal log-likelihood rebuilt from 950 per-cell offset-bin moment rows (bins of 0.1 on xβ̂, order 8, slice 2d's expansion applied to log(1 + eˣ)) equals pprof_py's sum over every record to 1.6e-16 relative |
| σ̂ near 0 | With hospital labels shuffled, σ̂ = 0.086 and stage 3 converges, but `sigma_sensitivity` raises `ZeroDivisionError`: the profile interval reaches σ = 0 and stage 3 divides by σ² (X-005) |

## 3. Design direction

The records enter stages 2 and 3 only through each cell's events, Σ y·xβ̂, and the distribution of the
offset xβ̂, which is fixed after stage 1. One pass therefore compresses the records into per-cell
offset-bin moments (2d's binned Taylor expansion, remainder below rounding), and stages 2 and 3
iterate on cells × bins rather than on records: n leaves the iterations. The compressed table goes to
the driver when it fits the driver budget; otherwise cluster-local blocks of compressed cells run on
executors and emit provider scores and the sparse Hessian's entries. The driver solves the sparse
systems (providers × providers for stage 3; providers plus clusters for stage 2's PIRLS, with its
log-determinant). New numerics: a deterministic sparse symmetric positive-definite solver with
log-determinant, Gauss–Hermite nodes and weights, and stage 2's optimizer.

## 4. Proposed sub-slices

| Sub-slice | Scope | Main parity targets |
|---|---|---|
| 2f-1 | Preparation (screening, `y_adj`, cells, `included`; sparse cells instead of R's dense provider × cluster table) and stage 1 on the included cells | pprof_py and its R `glmm.data.prep` golden data; stage 1 as slice 2a |
| 2f-2 | The compressed cell table; stage 3 (`marginal`) given β and σ: adaptive quadrature per cluster, sparse projected Newton on the driver | pprof_py at a tightened tolerance; the marginal likelihood's optimality |
| 2f-3 | Stage 2: the crossed random-intercept logistic GLMM with offset (Laplace), on the compressed cells | pprof_py and `glmer` at tight settings, under a tolerance class for optimizer-limited quantities to be calibrated and approved (NN-9) |
| 2f-4 | The pipeline, `sigma_sensitivity` with σ's profile interval, stage 3's tests, measures and intervals, the job runner and Python | pprof_py; R's `glmm.fac.hosp` for SRR where `he2013` is offered |

## 5. Decisions to make (D-38)

1. Approve the direction (§3) and the sub-slices (§4); 2f-1's specification comes next.
2. X-005 (class B): at σ = 0 the cluster effects vanish and stage 3 is the fixed-effect model of slice
   2a with the offset; pprof_spark uses that limit instead of dividing by zero.
3. Stage 3's default is pprof_py's `marginal` estimator; `he2013` (R parity for SRR) is added in 2f-4
   only if needed.
4. Fixtures pin pprof_py's stage 2 without nlopt (the fixture environment's case), and stage 2's parity
   uses an optimizer-limited tolerance class to be calibrated with 2f-3's fixtures.
