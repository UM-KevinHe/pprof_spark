# Specification: Logistic fixed effects / profile-likelihood Newton through SerBIN's reduced system

- Status: **Approved** (2026-11-25, the maintainer) as D-83. The fixture is built in round 197 (§12).
- Decision references:
  - D-75: performance step 5, in the class "other iterates";
  - plan §6.3: a change of iterates is compared at convergence. The maintainer accepted on 2026-11-13 that lockstep
    parity with pprof_py would no longer apply to this method, while converged parity would.
  - D-78 and D-79: the golden runs.
- Parity matrix entry: logistic fixed effects keep T-coef and T-var at convergence. Lockstep parity does not apply to
  this method (§9).

## 1. Purpose and scope

Plan §4's step 5 (study §4.3) fits the logistic fixed-effect model by Newton's method on β alone. Each provider's
intercept is solved inside its task, and every iteration needs one pass where SerBIN (fixed-effect-estimation.md §6)
needs two.

**The same estimator.** SerBIN's Schur complement S is the profile log-likelihood's negative Hessian, so the model,
the objective, the fixed point and every output are SerBIN's. Only the path to the fixed point changes, so the
iterates are other iterates (D-75).

**An option.** The method is chosen by `LogisticOptions.newton = "profile"`. SerBIN, `"serbin"`, stays the default
until the cluster's measurement (§10) and the maintainer decide otherwise.

**Scope.** The fit of fixed-effect logistic models. Providers added after fitting (§8a), tests, measures and the
variance pass consume the fitted γ and β as they do now.

## 2. Notation

As fixed-effect-estimation.md §2:
- for each provider j: the score gⱼ, the information hⱼ, the cross information bⱼ, and x̄ⱼ = bⱼ/hⱼ;
- the centred sums Sⱼ = Σᵢ qᵢⱼ(xᵢⱼ − x̄ⱼ)(xᵢⱼ − x̄ⱼ)ᵀ and rⱼ = Σᵢ (xᵢⱼ − x̄ⱼ)(yᵢⱼ − nᵢⱼπᵢⱼ);
- S = Σⱼ Sⱼ and r = Σⱼ rⱼ.

The weight floor of §5 applies: qᵢⱼ is raised to 10⁻²⁰. The bound is B = `bound` (10). A provider is **bounded** when
its intercept sits at mₖ ∓ B, where mₖ is a median (§3).

## 3. The iteration

**Starting values**, as SerBIN's: β⁽⁰⁾ = 0, γⱼ⁽⁰⁾ = logit(Ȳ), and m₀ = logit(Ȳ).

**The pass at β, with median m.** For each provider j, in its task, in canonical row order:

1. **The intercept.** γⱼ(β) maximizes the provider's log-likelihood over [m − B, m + B], warm-started from its last γⱼ.
   - With no events, γⱼ = m − B; with only events, γⱼ = m + B. Neither has a finite root.
   - Otherwise it is found by scalar Newton on gⱼ(γ) = 0, with the information hⱼ(γ). Each step is clipped to the
     interval and halved while the provider's log-likelihood falls. The solve stops when the step's size is at most
     10⁻¹²·max(1, |γ|), or after 100 steps, which is counted and warned of.
   - The provider's log-likelihood is concave in γ, so the result is the root when it lies inside the interval, and
     the nearer end when it does not.
2. **The provider's sums** at (γⱼ(β), β), as SerBIN's pass A computes them: hⱼ, bⱼ, x̄ⱼ, Sⱼ, rⱼ and ℓⱼ.

The pass returns:
- S, r and ℓₚ(β) = Σⱼ ℓⱼ, reduced as pass A's partials are;
- the solved γ, m numbers, for the next pass's warm starts and median.

**The driver**, after a pass at β:
- **The step.** Δβ = S⁻¹r, by Cholesky with SerBIN's aliasing rule, and λ = rᵀΔβ.
- **The trial.** The pass at β + vΔβ, from v = 1, gives ℓₚ(β + vΔβ). The trial is accepted when either holds:
  - ℓₚ(β + vΔβ) − ℓₚ(β) ≥ s·v·λ;
  - 0 < s·v·λ ≤ 10⁻¹²·(1 + |ℓₚ(β)|).

  Otherwise v ← t·v and the pass runs again, with s = 0.01 and t = 0.6, as SerBIN's line search (§5). With
  `backtrack = false`, v = 1.
- **The next iteration.** An accepted trial's pass is the next iteration's, so an iteration without a rejected trial is
  one pass. The median for the next pass is that of the accepted pass's γ, over the fitted providers (§5's median).
- **Convergence.** Stop when the accepted step satisfies ‖vΔβ‖∞ < `tol` (10⁻⁸), as SerBIN's. The iteration cap and
  the non-convergence warnings are SerBIN's, counting outer iterations.

**After convergence.** The fit's γ is the accepted pass's, at the final β. `at_bound` flags, as now, every provider
without events or with only events, and any within 0.1 of the bound: pprof_py's rule since its C35. The variance
pass is SerBIN's, with π clipped to [10⁻¹⁰, 1 − 10⁻¹⁰] (§5).

## 4. Why the β step is SerBIN's reduced system

Write g_β for the plain score in β. For an interior provider gⱼ = 0, so rⱼ = g_β,ⱼ: there, the reduced right-hand side
is the profile gradient. For a bounded provider gⱼ ≠ 0, and rⱼ = g_β,ⱼ − x̄ⱼgⱼ.

SerBIN converges where r = 0, not where g_β = 0. The study measured g_β at 4·10⁻⁴ at pprof_py's converged estimate,
with 73 of 600 providers without events.
- A profile method stepping on the plain score would converge elsewhere: β 1.4·10⁻⁶ away, 2·10⁻⁵ standard errors.
  That is immaterial statistically, and far outside T-coef.
- Stepping on r instead, with interior intercepts solved exactly and bounded ones held at the bound, reaches SerBIN's
  fixed point. It matched pprof_py within 9·10⁻¹⁷ in β at a tolerance of 10⁻¹², in 5 outer iterations against
  SerBIN's 13.

**The median.** The bound in a pass uses the median of the previous pass's γ, because a task cannot know the median of
intercepts it has not solved. At convergence the two medians agree, and the fixed point is SerBIN's:
- interior providers at their roots;
- bounded providers at med(γ) ∓ B;
- r = 0.

## 5. Differences from SerBIN at the default tolerance (X-051)

**Tight tolerance.** Both methods reach the same fixed point (§4).

**Default tolerance, 10⁻⁸.** SerBIN stops when β has converged. Its providers without events may then not yet have
reached the bound. In the study's case their reported effects were up to 2.4 short of it, depending on when SerBIN
stopped.
- This method reports the bound itself, which is also what SerBIN reports at a tighter tolerance.
- β differed by 1.5·10⁻⁹, within the tolerance.
- The variances computed at the effects differ too: Var(β̂), the standard errors, and the held providers'
  Var(γ̂ⱼ). This addendum to X-051 was confirmed by the maintainer in round 199.
- The numbers of iterations differ.

These differences are registered as X-051 in the discrepancy round (§10). Interior providers' effects and β agree
within the classes.

**Measured on `lfe-zero-events`** (round 198). Between pprof_py's default and tight fits:
- the 22 providers without events differ by 0.61 to 1.40;
- ℓ differs by 0.0064 (8·10⁻⁷ relative), and AIC and BIC by 0.013;
- β differs by 4.8·10⁻¹⁰, and the other providers' effects by 1.2·10⁻⁹.

R pprof's SerBIN stops short in the same way, by up to 1.3988. The `at_bound` flags agree, since they include every
provider without events.

## 6. Distributed plan

**Per pass.**
- The tasks receive β, the median and the previous γ: m numbers in the pass's function, as pass A receives γ now
  (OI-122).
- They return pass A's partials, reduced in groups (step 1), and the solved γ: m numbers, as pass B returns Δγ now.

**Per iteration.** One pass, plus one per rejected trial. There is no pass B and no Δγ. SerBIN takes two passes,
plus one per eight rejected step lengths.

**Compute per pass.**
- O(n·p²) for the centred sums, as pass A.
- O(n·p) per inner Newton step. The study measured about two inner steps per pass when warm-started, at 0.65 µs per
  row against pass B's 2.7.

**Layout.** ProviderLocal blocks, as now: a provider's rows are in one block, so its solve is local.

**Determinism.**
- The inner solves run in canonical row order with a fixed tolerance. Every sum is SerBIN's kernel's, so fits are
  bit-identical across partition counts and input orders for one layout (R0, DIST-6).
- Across layouts they agree within T-part (R1), as SerBIN's do.

## 7. Outputs

The fit's outputs are SerBIN's, with one difference: `iterations` counts outer iterations. The result also records the
total of inner Newton steps and the number of solves that reached the 100-step cap.

## 8. Edge cases

| Case | This method |
|---|---|
| A provider with no events, or only events | Held at m ∓ B from the first pass; `zero_events` or `all_events`, and `at_bound`, as now |
| A provider whose finite root lies beyond the bound | The nearer end of [m − B, m + B]; `at_bound` |
| An inner solve reaching 100 steps | Its last γ is used; the fit warns with the count |
| One provider | As SerBIN's: m is its γ |
| `backtrack = false` | v = 1 |
| Aliasing, screening, validation | As SerBIN's |

## 9. Validation plan

- **Function level.** At fixed β, the pass's S, r and ℓ equal SerBIN's pass A at (γ(β), β), to the bit: they come
  from the same kernel. Interior providers' gⱼ is at most 10⁻¹⁰·hⱼ.
- **Converged parity** (plan §6.3), on every logistic fixture case:
  - at the tight tolerance, against pprof_py's and R's references, within T-coef and T-var, β, γ and the variances
    included;
  - at the default tolerance, against pprof_py's default references: β, the interior providers' effects and
    `at_bound` within their classes. The degenerate providers' effects, ℓ, AIC and BIC are taken from the tight
    references instead, and iteration counts are not compared (X-051).
- **Lockstep parity.** It does not apply, because the iterates differ (plan §6.3). SerBIN's lockstep tests stay for
  SerBIN.
- **Golden runs.** New runs pin the method on the fixture cases and on a synthetic multi-block case, with R0 at 1 and
  7 partitions. SerBIN's runs keep their bits.
- **A new fixture**, `lfe-zero-events`, like the study's case:
  - 30,000 rows, 600 providers, about 12% without events;
  - pprof_py's references at the default and a tight tolerance, from the pinned environment.

  At the default tolerance pprof_py stops short of the bound there, which shows X-051.
- **The cluster's exit** (plan §4):
  - passes and CPU per fit against step 2;
  - converged estimates within T-coef of SerBIN's, on the same points.

## 10. Rounds

1. **This specification** (round 196), for approval as D-83.
2. **The fixture.** `lfe-zero-events`, from the pinned environment, added to the logistic fixture suites. SerBIN must
   pass it as it passes the others.
3. **The discrepancy.** X-051 is registered with the fixture's evidence, and the default-tolerance comparisons of §9
   are written to allow it, for this method only.
4. **The build.**
   - The specification amendments first (NN-2): fixed-effect-estimation.md §6, §11 and §14.
   - Then `LogisticOptions.newton`, persisted by `LogisticFitIO` as a new field that older fits read as `"serbin"`.
   - Then the pass, the driver's loop, the tests of §9, new golden runs, and a sandbox measurement.
5. **The cluster's measurement** at step 5's exit.

## 11. For review

1. **The method (§3).** Intercepts solved in the tasks within the previous pass's median ∓ B; the β step from S⁻¹r;
   SerBIN's line search and convergence on ℓₚ.
2. **The default.** SerBIN stays the default until the exit's measurement.
3. **X-051 (§5).** At the default tolerance, providers without events report the bound.
4. **Validation and rounds** (§9, §10).

## 12. As built

**Round 197, the fixture.**
- **The case.** `lfe-zero-events` follows the study's probe (`docs/reviews/feasibility-probes/bound_check.py`):
  - 500 providers of 55 rows and 100 of 25, 30,000 rows;
  - intercepts drawn around −3.5, so event rates are low (2,615 events) and 22 providers have no events by chance.
- **Generation.**
  - `logistic_fixtures.py` gained two case options: `intercept`, whose default −0.5 leaves every other case's bits
    unchanged, and `natural_degenerate`.
  - A natural degenerate provider is named in the case like a designated one, after the data are written.
  - R pprof's sources and `poibin` 1.6 come from their pinned commits. The 25 files of the six existing logistic
    cases regenerated byte for byte.
- **X-051 shows in it.**
  - pprof_py's default fit stops after 9 iterations, with the 22 providers 0.61 to 1.40 short of the bound.
  - Its tight fit takes 13 iterations and puts them on it.
  - β agrees between the two within 4.8·10⁻¹⁰, and the other providers' effects within 1.2·10⁻⁹.
- **Departs from §9.** §9 expected about 12% of providers without events; the case has 22 of 600.
- **Earlier designs that failed.** Designating 73 providers without events, at 20 to 80 rows or at 11, did not show
  X-051: pprof_py's default reached the bound. The study's low event rates are what leave them short.
- **The `at_bound` question, resolved in round 198.** pprof_py flags all 22 `at_bound` at the default tolerance,
  although they are short of the bound. Its `at_bound` includes every provider without events or with only events
  (its C35), and SerBIN's code already does the same. The specification's text named only the second clause and is
  corrected.
- **Calibration and suites.** `docs/parity/logistic-calibration.md` gains the case's 36 rows, all within class.
  Every logistic fixed-effect suite takes the case from the manifest. The twelve suites run in round 197 passed it
  with SerBIN. `LogisticProviderTestsMoreSuite` was not among them, and one of its assertions failed on the case
  until round 199 fixed it (below).

**Round 198, the discrepancy.** X-051 is registered, class C, as proposed, with `lfe-zero-events`' measurements, for
the maintainer's sign-off. The default-tolerance comparisons of §9 are stated as the build's tests will make them.

**Round 199, the build.**
- **Numerics.**
  - `kernels.LogisticFE.profile` solves a block's intercepts as §3 says. Its inner Newton runs on the provider's rows
    with xᵀβ computed once; the information is floored at 10⁻²⁰, steps are clipped and halved, and solves stop at
    10⁻¹²·max(1, |γ|) or after 100 steps. It then calls `schur` at the solved intercepts, so its sums are pass A's to
    the bit.
  - `ProfileNewton.fit` runs the loop over `ProfilePasses`, with SerBIN's Armijo test, noise level and stopping rule,
    and returns SerBIN's result type with the inner counts.
- **Two choices made in the build.**
  - **The settle.** A pass bounds intercepts by the previous pass's median. So after the last pass, the intercepts
    the bound held are set to the median of the last pass's intercepts ∓ the bound, as SerBIN's clip leaves them.
    Without that step they sat 10⁻⁹ (tight) to 6·10⁻⁹ (default) from that bound. Held intercepts are extremes, so the
    median does not move.
  - **The shrink cap.** One trial shrinks at most 60 times, then its last pass is accepted. SerBIN's ladder ends
    because λ > 0, which a zero r would not give.
- **Engine.**
  - `LogisticOptions.newton`, `"serbin"` or `"profile"`, validated.
  - `SparkProfilePasses` makes the `logistic.profile` pass. Its group partials carry pass A's sums and the blocks'
    intercepts.
  - `LogisticFit` gains `innerSteps` and `cappedSolves`. `LogisticFitIO`, at format 6, keeps them with the method;
    versions 1 to 5 read as SerBIN with none.
  - The run specification's `fit.newton`, Python's `newton` and PerfJob's `--newton` choose the method.
- **Tests.**
  - `ProfileNewtonSuite`: SerBIN's fixed point at 10⁻¹²; pass A's sums to the bit; interior scores within 10⁻¹⁰·hⱼ;
    the bound at the default tolerance; fewer passes.
  - `LogisticProfileSuite`, over every logistic case:
    - at 10⁻¹³ against pprof_py's tight fit;
    - at the default against its default fit, with X-051's handling;
    - R0 and DIST-6 to the bit;
    - persistence and validation.
  - New golden runs pin the method.
- **A test fixed** (found by round 199's full run).
  - `LogisticProviderTestsMoreSuite` compares exact-test z with R's. Where R's exact tails are unreliable (X-024),
    beyond |z| = 5.2, it then required our |z| to exceed 37, assuming such values are extreme.
  - On `lfe-zero-events`, provider 446 has z of 5.53 to 5.90 by the alternative. Ours, pprof_py's and R's agree to six
    digits or more there.
  - The assertion now requires |z| > 37 only where R's z is infinite. Where R's z is finite it requires z beyond the
    reliable tail, with R's sign. The test fits with SerBIN, whose fits round 199's golden runs show unchanged to the
    bit, so the failure had been on `main` since round 197.
