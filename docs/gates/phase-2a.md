# Slice 2a review: logistic fixed-effect estimation

- Date: 2026-10-07 (round 28). Reviewer: the assistant; sign-off: the maintainer (D-32, proposed).
- Scope: the plan's slice 2a ([plan.md](../spec/logistic/plan.md)); specification
  [fixed-effect-estimation.md](../spec/logistic/fixed-effect-estimation.md) (D-30). Phase 2's own
  gate closes after slice 2f (plan §2).

## Evidence

| Requirement | Status | Evidence |
|---|---|---|
| Specification approved (NN-2) | Met | D-30, with X-004 and X-016 to X-020 |
| Fixtures and calibration (§9.3, D-09) | Met | Round 26: five cases, pprof_py, `glm` and R pprof's SerBIN; every comparison within its class (worst 0.0253 of T-var), every negative control at least 1.6e3 outside; `FixturesSuite` checks both in CI |
| Function-level parity (T-fn) | Met, with one documented limit | `LogisticKernelParitySuite`: ℓ, gⱼ, g_β, hⱼ, bⱼ, C and S at two points for every case; on lfe-shifted, pprof_py's subtracted S is 1.38 of T-fn from the centred S, so S is checked there through C, b, h and the fits (spec §14) |
| Lockstep parity (T-iter) | Met | Iterates of steps 1 to 5 for every case |
| End-to-end parity | Met | `LogisticFESuite`: default fits with pprof_py's step counts exactly; β̂, γ̂ (T-coef), the three variances (T-var), ℓ, AIC, BIC (T-fn), Wald z (T-test); tight fits against pprof_py, `glm` and R SerBIN; p-values where pprof_py's are accurate (spec §14) |
| Metamorphic and edge cases (§9.4, §9.5) | Met, except the correlation warning | `LogisticBehaviourSuite`: bitwise invariance to row order and partitioning, block sizes within T-part, binomial against expanded Bernoulli rows, validation with counts, aliasing, degenerate and screened providers, the iteration cap; numerics `LogisticFESuite`; the correlation warning waits for X-021 |
| Persistence (PERS-1 to PERS-3) | Met | `LogisticFitIOSuite` (round 28): bitwise round trips with integral and text keys, no overwriting, other kinds and versions refused |
| Platforms | Met in the sandbox | Classic Spark and Spark Connect on JDK 17; Classic on JDK 21 |
| CI | **Pending** | Rounds 27 and 28 (NN-8) |
| Scale | **Deferred** | The maintainer's package-level test (D-28) |

## Proposal (D-32)

Close slice 2a at parity-verified once the maintainer reports CI green for rounds 27 and 28 and the
correlation warning lands as decided under X-021; the feature stays `Experimental` until the scale
test (D-28).
