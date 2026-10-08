# Specification: three-stage model — stage 2, the crossed random-intercept GLMM (Phase 2f-3)

- Status: Approved by the maintainer, 2026-10-08 (D-41), with X-030 and OI-59 as proposed; T-opt's values
  proposed in D-42.
- Builds on: [three-stage-spike.md](three-stage-spike.md) (D-38), 2f-1 (prepared records) and 2f-2
  (compressed cells, X-029).
- Decision references: D-41; X-030; OI-59.
- Reference: pprof_py v0.7.0 (`9320766`): `models/logistic/random_effect.py` (`LogisticRandomEffectModel`:
  `fit`, `_pirls`, `_build_H_C`, `_laplace_deviance`, `_logdet_H`, `_optimize_stage1`, `_optimize_stage2`,
  `get_random_effects`, `get_sigma`), as called by `LogisticThreeStageModel`; R 4.3.3 with lme4 1.1-35.1
  (`glmer`, `nAGQ = 1`).

Probe results are sandbox evidence from round 50 (`three_stage/raw.csv`, prepared as 2f-1).

## 1. Model and estimand

For record i of provider j and cluster h, logit pᵢ = μ + oᵢ + σₚuⱼ + σ_c v_h, with u and v independent
standard normal (crossed random intercepts), oᵢ the stage 1 offset and ỹᵢ (`y_adj`) the outcome. The
parameters σₚ ≥ 0, σ_c ≥ 0 and μ minimize the Laplace deviance

D(σ, μ) = −2 Σᵢ [ỹᵢη̂ᵢ − log(1 + e^η̂ᵢ)] + ‖û‖² + log|H|, H = I + AᵀWA, A = [σₚZₚ, σ_cZ_c],

where û maximizes the penalized log-likelihood at (σ, μ), η̂ the linear predictor there and
W = diag(max(p(1 − p), 1e-12)) (pprof_py's floor). This is lme4's nAGQ = 1 objective without the
saturated-model constant. Outputs for stage 3: σ_c (the cluster SD), μ and the provider BLUPs σₚûⱼ; stage
3 starts from σₚûⱼ + μ.

## 2. Algorithm

| Element | pprof_py | pprof_spark |
|---|---|---|
| PIRLS | Newton on u (on (u, μ) in its nAGQ = 0 stage) with the joint system, a step accepted when the penalized deviance does not rise by more than 1e-10, halving otherwise; stop when the relative change of the penalized deviance is below `tol_pirls` (1e-8), warm-started from the previous evaluation's u | The same Newton and step rule from u = 0 at every evaluation, to a relative change below 1e-12, so D is a function of (σ, μ) alone |
| Optimizer | nAGQ = 0 stage over σ (bobyqa with nlopt, Powell without), then Nelder–Mead over (σ, μ) with `xatol` = `fatol` = 1e-7, adaptive, at most 200 iterations | The same nAGQ = 0 start, then a bounded quasi-Newton on (σₚ, σ_c, μ) with central-difference gradients of D, to a projected gradient below 1e-8·max(1, D), bounds σ ≥ 0 (X-030) |
| Joint system | `splu` of H (and of the Schur system with μ in the nAGQ = 0 stage) | H eliminated onto the smaller set of levels: with D a diagonal block, the Schur complement S = D_small − Bᵀ D_large⁻¹ B, factorized by Cholesky; log det H = Σ log D_large + log det S |
| Sums over records | Every record, every evaluation | From 2f-2's compressed cells: a cell's linear predictor is μ + σₚuⱼ + σ_c v_h plus each record's offset, so Σỹη, Σσ, Σσ′ and Σ log(1 + eᶯ) per cell come from its Σỹ, Σỹ·o and offset-bin moments (X-029) |

At σₚ = 0 or σ_c = 0 the corresponding columns of A vanish, H is the identity on that block and its
log-determinant is 0, as in pprof_py (which then skips the off-diagonal block).

## 3. Why the optimizer differs (X-030)

D is very flat near its minimum. On the golden data, lme4's deviance function is 1.6e-8 lower at `glmer`'s
optimum than at pprof_py's, yet the two optima differ by 7.8e-6 in σ_c (0.4229352 against 0.4229430):
optimizers that stop on changes of D near 1e-7 leave σ̂ determined to about 1e-5. lme4's deviance function
is also accurate only to its own PIRLS tolerance: at (σₚ, σ_c, μ) = (0.5, 0.3, −0.2) it gives 2950.0784295
where pprof_py's tightly converged PIRLS gives 2950.0778371 (5.9e-4 apart), and 7.7e-6 apart at pprof_py's
optimum. pprof_py's default and tight (`tol_outer` 1e-12) runs agree to 5e-9 in σ_c and 5.6e-8 in σₚ.
pprof_spark therefore minimizes the exactly converged D to a gradient tolerance, compares D itself with
pprof_py at fixed parameters (T-fn), and compares the optimum under a new optimizer-limited tolerance class,
T-opt, to be calibrated against pprof_py and lme4 with the fixtures and approved under NN-9.

## 4. Outputs and API

`ThreeStage.stage2(cells, options)` on 2f-2's compressed cells returns `ThreeStageStage2`: σₚ, σ_c, μ, the
provider BLUPs (provider key order) and cluster BLUPs (cluster key order), D at the optimum, convergence,
iterations, evaluations and the projected gradient; `start` gives σₚûⱼ + μ for stage 3. `ThreeStage.laplace(
cells, sigmaProvider, sigmaCluster, mu)` gives D and û at fixed parameters. Options: PIRLS and optimizer
tolerances and limits.

## 5. Distributed plan

Stage 2 runs on the driver from the compressed cells collected under the driver budget (2f-2's driver path).
Each D evaluation is a PIRLS of a few Newton steps, each a pass over the cells and a Cholesky of the Schur
complement, whose size is the smaller set of levels: (number of levels)² doubles, guarded by the driver
budget. A sparse factorization for very large level sets, and a cluster-local executor path like 2f-2's,
are OI-59.

## 6. Edge cases

| Case | pprof_py v0.7.0 | pprof_spark |
|---|---|---|
| σ̂ on a boundary (0) | Allowed (lme4's parameterization); `sigma_sensitivity` then fails (X-005) | Allowed; the bounded optimizer reports which bound is active |
| One level in a factor | Fits, with that factor's σ unidentifiable from one level | Same, with a warning |
| y outside [0, 1], non-finite offsets | Raises | Fails with counts (2f-1's validation) |
| PIRLS or the Cholesky fails (non-finite, not positive definite) | Raises `LinAlgError` | Fails with a message naming the parameters |

## 7. Validation plan

Fixtures (round 51, calibrated in [three-stage-calibration.md](../../parity/three-stage-calibration.md)), on ts-golden,
ts-synthetic and ts-shuffled (hospital labels shuffled, σ_c near 0.09):
pprof_py's stage 2 at its default and tight (`tol_outer` 1e-12) settings (σ's, μ, BLUPs, D); D and û at
three fixed parameter points with its PIRLS converged to 1e-14 (function level); `glmer`'s optimum at
bobyqa `rhoend` 1e-12 (σ's, μ, BLUPs) and its deviance function at the same points (with the saturated
constant). Tolerance classes: T-fn for D at fixed parameters, T-coef for û there, T-opt (new) for the
optimum against pprof_py's tight run and against `glmer`. Reference-independent checks: the projected
gradient at pprof_spark's optimum, and D there not above either reference's D at its own optimum (by more
than 1e-9 relative). Negative controls: an offset shifted by 0.01, one outcome flipped.

## 8. Known discrepancies and open items

| ID | Summary | Class | Proposed decision (D-41) |
|---|---|---|---|
| X-030 | Stage 2's optimum: pprof_py stops Nelder–Mead on changes of D of 1e-7 and lme4 differs from it by 7.8e-6 in σ_c; pprof_spark minimizes the exactly converged Laplace deviance to a gradient tolerance | D (with a new tolerance class) | Accept; define T-opt from the fixtures' calibration, for approval under NN-9 |
| OI-59 | The Schur complement is dense on the smaller set of levels; at the envelope (thousands of hospitals and facilities) a sparse factorization or an executor path may be needed | — | Measure at package scale (D-28) |

## 9. Calibration findings (round 51)

1. T-opt (D-42): rtol 2e-4, atol 1e-6 under the element-wise rule. pprof_py's tight run against `glmer` is
   at most 0.48 of it (ts-shuffled's cluster BLUPs) on the cases with integral outcomes; its default run
   against its tight run at most 1.7e-4 of it; the controls (offsets + 0.01; the first five events
   flipped) at least 23.9 times outside. A single flipped outcome moved σ only 6 times T-opt on
   ts-shuffled, so the control flips five events.
2. lme4 with fractional outcomes: on ts-synthetic, whose providers without events or with only events
   get a fractional `y_adj`, `glmer`'s deviance function differs from pprof_py's by a parameter-dependent
   amount (0.262, 0.298 and 0.320 at the three points after its saturated constant) and its optimum by
   8.6e-4 in σₚ; tightening lme4's `tolPwrss` to 1e-13 changes this by less than 1e-5. lme4's binomial
   likelihood treats non-integer successes differently, so `glmer` is informational for such cases;
   pprof_py's continuous `y_adj` is the reference. On integral outcomes lme4's deviance function is within
   its PIRLS tolerance of pprof_py's (5.9e-4 at most at the fixed points).
