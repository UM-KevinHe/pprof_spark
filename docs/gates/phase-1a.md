# Phase 1a gate review: Cox estimation core

- Date: 2026-10-06 (round 12). Reviewer: the assistant; sign-off: the maintainer (D-22).
- Exit criterion (§13): the parity gate passed for right censoring, strata, offsets, case weights,
  Breslow and Efron ties; coefficients, information, model-based covariance, standard errors, Wald
  inference, log partial likelihood; convergence and step halving; aliasing.
- Specifications: [first-slice.md](../spec/cox/first-slice.md) (D-20) and
  [efron-weights-offsets.md](../spec/cox/efron-weights-offsets.md) (D-21).

## Evidence

| Requirement (§9.8) | Status | Evidence |
|---|---|---|
| Function-level parity | Met | ℓ, U, I at β = 0 and the fixed β, both tie methods, four fixtures, against pprof_py and R; worst T-fn ratio 0.020 |
| Lockstep parity | Met | Iterates before the final one; worst T-iter ratio 2.3e-4 |
| End-to-end parity | Met | Tight and default fits; coefficients within T-coef of pprof_py (worst 0.995 Breslow and 0.596 Efron, both tiny-ties, from X-010) and within 1.2e-7 of it against R; variances 1.5e-3; information 1.5e-3 or less (round 12); z against R 1.6e-7 |
| Metamorphic and edge cases | Met | Monotone time maps, scaling, translation, weights times 3, offsets, zero weight against dropped row, weight 2 against duplicated row; validation errors, no events, aliasing, strata without events, iteration cap, oversized strata |
| Negative controls | Met | The other tie method and shifted ties lie more than 10 times outside T-coef |
| Reproducibility | Met | R0 bitwise across row orders and partitions; R1 within T-part across layouts; R2 bitwise across JDK 17 and 21 and Classic and Connect |
| Persistence | Met for PERS-1 and PERS-3; PERS-2 starts with the first release | `CoxFitIO` round-trips every bit under Classic and Connect, never overwrites, and names both format versions on a mismatch; per-release model fixtures begin with release 0.1.0 (OI-41) |
| Documentation | Met | Specifications, parity matrix, discrepancies X-001, X-002, X-009 to X-013 |
| Scale test | **Not met** | No cluster under D-14, and targets wait for benchmarks (D-04) |
| CI | Round 10 green (run 37502839703); rounds 11 and 12 to confirm | Sandbox: all suites pass on JDK 17 and 21 (Classic) and JDK 17 (Connect) |

## Gaps

- Scale verification. Under §9.8 a feature becomes stable only after its scale test, so every Cox
  feature stays `Experimental` (NN-12) at status parity-verified. Phase 1d's exit already requires
  an end-to-end run at design-envelope scale.
- The CI check that every public API has a parity-matrix entry (§9.8) does not exist yet (OI-40).
- Kernel cost at larger p (OI-38) and the reduction volume at large p (OI-02) are unmeasured.

## Outcome

Signed off on 2026-10-06 (D-22): the maintainer replied "Continue" to the sign-off question. **Closed
on 2026-10-06**: CI is green for rounds 11 to 14, including rounds 11 and 12 on which closure
depended. Round 15's Spark Connect job then failed on a test timeout, fixed in round 16.1. Earlier note: Phase 1a was to
close when CI confirmed rounds 11 and 12 (main at f619e9b and 094fd5b); CI as read in round 13:
not readable from the sandbox.

## Recommendation

Close Phase 1a when CI confirms rounds 11 and 12, with the Cox features parity-verified and
`Experimental`, and move their scale verification to the Phase 1d gate (D-22, needs the
maintainer's sign-off). Phase 1b (counting-process data, left truncation, baseline hazard and
prediction) then starts with a specification for approval.
