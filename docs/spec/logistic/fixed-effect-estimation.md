# Specification: logistic fixed-effect provider model — estimation (Phase 2a)

- Status: Draft, awaiting approval (D-30). No Phase 2 code is written before approval (NN-2).
- Decision references: D-30; discrepancies X-004, X-016 to X-020.
- Parity matrix entry: [docs/parity/matrix.md](../../parity/matrix.md).
- Plan: [plan.md](plan.md) (slice 2a).
- Reference: pprof_py v0.7.0 (`9320766`), `LogisticFixedEffectModel(algorithm="Serbin")`; R 4.3.3: `stats::glm`
  and R pprof 1.0.3 (MIT), `logis_BIN_fe_prov` in `src/Fixed_effect.cpp`.

Sections follow PROJECT_CONTEXT §7.1. Probe results are sandbox evidence from round 25 (pprof_py
v0.7.0, Python 3.12.3 with numpy 2.5.3; R 4.3.3; R pprof 1.0.3 compiled with `Rcpp::sourceCpp`).

## 1. Estimand and model

Record i of provider j has yᵢⱼ events out of nᵢⱼ trials (Bernoulli data: n = 1) and covariates xᵢⱼ:

logit πᵢⱼ = γⱼ + xᵢⱼᵀβ.

The estimands are β (p covariate effects shared by every provider), the provider effects γⱼ on the
log-odds scale, and their inverse-information variances. The estimate is the maximum-likelihood
estimate when every provider has events and non-events. A provider with no events or only events
has no finite estimate; the reference holds its effect at a bound (§5), and the result flags it.

## 2. Objective

ℓ(γ, β) = Σᵢⱼ [yᵢⱼ ηᵢⱼ − nᵢⱼ log(1 + e^ηᵢⱼ)], with ηᵢⱼ = γⱼ + xᵢⱼᵀβ.

This is the binomial log-likelihood without Σ log C(n, y), which pprof_py omits; the constant is
zero for Bernoulli data. With πᵢⱼ = 1/(1 + e^−ηᵢⱼ) and qᵢⱼ = nᵢⱼ πᵢⱼ(1 − πᵢⱼ):

| Quantity | Definition |
|---|---|
| Score for γⱼ | gⱼ = Σᵢ (yᵢⱼ − nᵢⱼ πᵢⱼ) |
| Score for β | g_β = Σᵢⱼ xᵢⱼ (yᵢⱼ − nᵢⱼ πᵢⱼ) |
| Information for γⱼ | hⱼ = Σᵢ qᵢⱼ |
| Cross information | bⱼ = Σᵢ qᵢⱼ xᵢⱼ (a p-vector per provider) |
| Information for β | C = Σᵢⱼ qᵢⱼ xᵢⱼ xᵢⱼᵀ |
| Schur complement | S = C − Σⱼ bⱼbⱼᵀ/hⱼ = Σⱼ Σᵢ qᵢⱼ (xᵢⱼ − x̄ⱼ)(xᵢⱼ − x̄ⱼ)ᵀ, with x̄ⱼ = bⱼ/hⱼ |

The information of (γ, β) is block-arrowhead: diagonal in γ, so the provider block is eliminated
exactly through S (SerBIN; Wu, Yang, Kang and He, 2022).

## 3. Parameterization

There is no intercept; the provider effects carry it. Estimates and variances are reported for the
covariates as given, without centering. Internally, S and its right-hand side are formed from
within-provider centered co-moments (§6), which equal the reference's quantities in exact
arithmetic and do not depend on the covariates' origin.

## 4. Data contract

| Column | Type | Rule |
|---|---|---|
| outcome | integral or boolean | Without trials: 0 or 1. With trials: an integer with 0 ≤ y ≤ n. Anything else fails with counts |
| trials | integral, optional | An integer ≥ 1. Without the column, n = 1 for every row |
| features | numeric, at least one | Finite; Decimal converted to double explicitly; no feature with zero variance |
| provider | integral or string | Non-null; providers are ordered by key as in ADR-0004 |
| row identifier | integral, optional | Unique; fixes the canonical order (DIST-6) |

Validation runs once, before screening, over every input row, in the reference's order: missing
values, outcome and trial rules, zero variance, then pairwise correlation (a warning when the
absolute correlation of two features exceeds 0.9). Messages carry column names and counts, never
values (T6). pprof_py accepts non-integer counts and its solver then truncates the outcome to an
integer; pprof_spark rejects them (X-019).

## 5. Conventions (§7.2)

| Convention | Value in this slice |
|---|---|
| Screening | Providers with more than `minRecords` = 10 records are fitted (pprof_py's `cutoff`); records are rows, also with trials. The others are listed with their record counts and the reason, and a warning gives their number. `screen = false` fits every provider. R pprof's `logis_fe` keeps providers with at least `cutoff` records (X-016) |
| Starting values | γⱼ⁽⁰⁾ = log(Ȳ/(1 − Ȳ)) with Ȳ = Σy/Σn over the fitted rows; β⁽⁰⁾ = 0 |
| Weight floor | qᵢⱼ is raised to 1e-20 where smaller, in every score and information sum of the iteration (pprof_py; R raises only exact zeros) |
| Newton step | The joint direction (Δγ, Δβ) by block elimination (§6), used as it is (pprof_py C27) |
| Line search | `backtrack = true` (default): with λ = gᵀ(Δγ, Δβ), s = 0.01 and t = 0.6, start at v = 1 and multiply v by t until ℓ(new) − ℓ(old) ≥ s·v·λ, or until 0 < s·v·λ ≤ 1e-12·(1 + abs(ℓ(old))), the predicted gain being below rounding (pprof_py C3). `backtrack = false`: v = 1 |
| Bound | After each update, every γⱼ is clipped to [med(γ) − bound, med(γ) + bound], bound = 10, med the median over fitted providers (the mean of the two middle values when their number is even) |
| Convergence | Stop when ‖β_new − β_old‖∞ < tol, tol = 1e-8 (pprof_py; R pprof's `logis_fe` defaults to tol = 1e-5 with `stop = "or"`) |
| Iteration cap | The loop runs while its counter is at most `maxIter` = 10000, so at most `maxIter` + 1 Newton steps, as in both references; warnings state the number of steps taken |
| Non-convergence | A warning and a flag (NN-10) when the cap ends the loop, and pprof_py's second warning when the line search shortened the last step (v < 1) while the Newton β step was at least tol |
| Degenerate providers | Providers with no events or only events are fitted with the others; their effects move towards ∓∞ until β converges or the bound holds them. The result flags `zero_events`, `all_events` and `at_bound` (abs(γⱼ − med(γ)) ≥ bound − 0.1, pprof_py's `at_bound`); β then differs from the MLE without those providers (X-018) |
| Aliasing | Cholesky of S with R's rule (a pivot below ε^0.75 times the largest diagonal entry): an aliased feature, collinear with others or constant within every provider, fails the fit and is named (X-019) |
| Variances | At the estimates, with π clipped to [1e-10, 1 − 1e-10]: Var(β̂) = S⁻¹; Var(γ̂ⱼ) = 1/hⱼ + x̄ⱼᵀS⁻¹x̄ⱼ (R's `logis_fe_var`); Var(γ̂ⱼ + x̄ᵀβ̂) = 1/hⱼ + (x̄ⱼ − x̄)ᵀS⁻¹(x̄ⱼ − x̄), with x̄ = Σnx/Σn over the fitted rows (pprof_py C34; the variance its provider tests use) |
| Wald test for β | z = β̂/SE, two-sided p = 2Φ̄(abs(z)), interval β̂ ± z₁₋α/₂·SE, level 0.95 by default |
| Information criteria | AIC = 2(p + m) − 2ℓ and BIC = (p + m)·log(records) − 2ℓ, with m every fitted provider, degenerate ones included |
| Provider order | By key: integers numerically, strings by code point (pprof_py's `np.unique`; Spark's binary order of UTF-8 strings is the same) |
| AUC | Not in this slice; slice 2b (plan.md) |

## 6. Algorithm

Iteration k, at (γ, β):

1. Per provider j, in canonical row order: gⱼ, hⱼ, bⱼ, then x̄ⱼ = bⱼ/hⱼ and the centered sums
   Sⱼ = Σᵢ qᵢⱼ(xᵢⱼ − x̄ⱼ)(xᵢⱼ − x̄ⱼ)ᵀ and rⱼ = Σᵢ (xᵢⱼ − x̄ⱼ)(yᵢⱼ − nᵢⱼπᵢⱼ); also ℓ.
2. S = Σⱼ Sⱼ and r = Σⱼ rⱼ (= g_β − Σⱼ bⱼgⱼ/hⱼ); Δβ = S⁻¹r by Cholesky.
3. Δγⱼ = gⱼ/hⱼ − x̄ⱼᵀΔβ.
4. λ = Σⱼ gⱼ²/hⱼ + rᵀΔβ, which equals Σⱼ gⱼΔγⱼ + g_βᵀΔβ.
5. Line search on ℓ(γ + vΔγ, β + vΔβ) (§5).
6. γ ← γ + vΔγ, then the bound; β ← β + vΔβ; stop when ‖vΔβ‖∞ < tol, taken as ‖β_new − β_old‖∞.

These are pprof_py's and R's equations rearranged: both form S as C minus a sum over providers
and use a p×m matrix S⁻¹(bⱼ/hⱼ). The centered form avoids the cancellation in C − Σ bⱼbⱼᵀ/hⱼ when
covariates are far from zero (probe: 4.2e-13 relative difference between the two forms at shifts
of +50 and −30) and needs no p×m matrix. The difference is rounding (class D). π uses StrictMath:
1/(1 + exp(−η)); log(1 + e^η) is η + log1p(e^−η) for η > 0 and log1p(e^η) otherwise, as numpy's
`logaddexp(0, η)`.

## 7. Inference

Model-based only in this slice: §5's variances and the Wald test for β. Covariate likelihood-ratio
and score tests, cluster-robust variances, provider tests and standardized measures are later
slices (plan.md).

## 8. Outputs

| Output | Content |
|---|---|
| Coefficients | β̂, SE, z, p, interval bounds, per feature, in input order |
| Covariance | Packed upper triangle of Var(β̂) |
| Provider table | DataFrame, one row per fitted provider: provider, records, trials, events, γ̂, Var(γ̂ⱼ), Var(γ̂ⱼ + x̄ᵀβ̂), `zero_events`, `all_events`, `at_bound`; schema versioned (API-2) |
| Excluded providers | Provider, records, reason |
| Fit | ℓ(γ̂, β̂), AIC, BIC, Newton steps, converged flag, final criterion, last step length, warnings, per-step trace (ℓ, v, ‖Δβ‖∞, passes over the data) |
| Counts | Records, trials, events; providers fitted, excluded, degenerate |
| Conventions | tol, maxIter, bound, backtrack, minRecords, screen, weight floor, line-search constants |
| Metadata | Software (`SoftwareInfo`), input specification, layout summary, data fingerprint, options (NN-11) |

The fit is persisted bit for bit like `CoxFit` (PERS-1), in a new kind with its own format version.

## 9. Edge cases

| Case | pprof_py v0.7.0 (probed in round 25 unless marked as read in the source) | pprof_spark |
|---|---|---|
| Missing value in a model column | Raises | Fails at validation with counts |
| Outcome not 0 or 1 without trials, or outside [0, n] | Raises | Fails at validation with counts |
| Non-integer outcome or trials | Read: accepted with trials; the solver truncates the outcome to an integer | Fails at validation (X-019) |
| Feature with zero variance | Raises | Fails and names the feature |
| Feature constant within every provider; collinear features | `LinAlgError` from the Cholesky (potrf info 4); collinear features not probed (a failed or near-singular Cholesky, depending on rounding) | Fails and names the aliased features (X-019) |
| No features | `ValueError` from a broadcast | Fails at validation (X-019) |
| No events, or only events, in the whole input | No events: infinite start; returns β = 0 after one step with NaN warnings (only events: not probed) | Fails at validation (X-019) |
| Providers with at most 10 records | Excluded with a warning and recorded | Same; listed in the result |
| Every provider excluded | Not probed | Fails at validation |
| Provider with no events or only events | Kept; its effect ends at med(γ) ∓ 10 (probe: exactly ∓10 after nine steps, β converged at step ten) | Same, flagged (X-018) |
| One provider | Fits (nine steps on 11 records) | Same |
| `max_iter = 0` | One step; warns "did not converge in 0 iterations" | One step; the warning says one step |
| `backtrack = false` | Full Newton steps | Same |
| String provider keys | Sorted by code point | Same |
| Binomial rows | Equal to the expanded Bernoulli rows (probe: 1.1e-16 in β) | Same; metamorphic test |
| Highly correlated features | Read: warning | Warning |
| Overflow of e^η | Read: stable log(1 + e^η) | Same |

## 10. Reference mapping

| Element | pprof_py v0.7.0 | R |
|---|---|---|
| Estimator | `models/logistic/fixed_effect.py`: `LogisticFixedEffectModel(algorithm="Serbin")`, `fit(max_iter=10000, tol=1e-8, bound=10.0, backtrack=True)` | `glm(y ~ 0 + factor(prov) + x, binomial, control = glm.control(epsilon = 1e-15))` for the MLE; R pprof `logis_BIN_fe_prov(tol = 1e-8, backtrack = TRUE, stop = "beta", bound = 10)` for the iteration |
| ℓ, score, information | `algorithms/logistic/fixed_effect.py`: `BaseAlgorithm._loglikelihood`, `SerbinAlgorithm._compute_scores_and_info` | `Loglkd` and the loop of `logis_BIN_fe_prov` |
| Newton step and line search | `SerbinAlgorithm._compute_deltas`, `_backtrack` (C3 noise rule, C27 direction), `_report_stop` | `logis_BIN_fe_prov` (no noise rule, X-017) |
| Validation and screening | `data/validation.py`: `validate_and_convert_inputs`; `data/preparation.py`: `DataPrep` | `logis_fe` (keeps at least `cutoff`, X-016) |
| Variances | `inference/logistic/fixed_effect/covariates.py`: `_estimate_variances` | `vcov(glm)`: Var(β̂), Var(γ̂ⱼ), and Var(γ̂ⱼ + x̄ᵀβ̂) from its blocks |
| Wald test for β | `_compute_wald_beta`, `summary(test_method="wald")` | `summary(glm)` |
| Information criteria | `fit` | `logLik(glm)`, `AIC(glm)` (Bernoulli) |
| Degenerate providers | `inference/standardized.py`: `at_bound`, `degenerate_providers` | — |

Probe agreement on a 30-provider Bernoulli case (sandbox): pprof_py at tol = 1e-13 against `glm`
differs by 1.1e-15 in β, 8.6e-16 in γ, 6.4e-15 in Var(β̂) and 1.4e-13 in both provider variances,
relative to the largest magnitude; ℓ and AIC are equal. R pprof's SerBIN at tol = 1e-8 differs from
pprof_py by 2.2e-10 in β, about the size of pprof_py's last step (3.4e-10; X-017); on a case with degenerate providers the two agree to 4e-16.

## 11. Distributed plan

**Layout.** ProviderLocal: whole providers bin-packed into blocks by record count, rows in
canonical order (provider key, then row identifier or row content); a provider larger than a block
gets a block of its own (ADR-0004, OI-30). Blocks hold outcome, trials and row-major features as
primitive arrays.

**Preparation.** One validation job (counts, never values); provider record counts through
`GroupSizes` (m-scale, guarded); screening on the driver; the correlation warning from the
`Moments` kernel's p×p co-moments; then the working set, built and persisted once.

**What reaches the driver.** p-scale and p²-scale quantities, and the provider effects γ: m doubles
(8 MB at m = 10⁶), guarded by `maxProvidersOnDriver` (default 10⁷, recorded in the result). Above
the guard the fit fails with an actionable message; a distributed γ (table materialization, §6.7)
is OI-50.

**Per Newton step.**

| Pass | Executors | Driver |
|---|---|---|
| A | γ of each block's providers arrives as a small DataFrame joined with a broadcast hint (§6.7); per provider gⱼ, hⱼ, bⱼ, Sⱼ, rⱼ; per block packed S, r, Σ gⱼ²/hⱼ and ℓ | Reduce in block order; Cholesky of S; Δβ; λ |
| B | Per provider Δγⱼ (recomputed sums, the same bits) and, per block, ℓ at the step lengths v = tᵏ, k = 0 to 7, in one pass | Collect Δγ in block order (m doubles, guarded); accept the first v that passes, which is what the sequential search accepts; if none passes, pass B again for k = 8 to 15; update, bound, convergence |

Two passes per step, plus one per eight rejected step lengths. Shipped per step: 16m bytes for γ
and Δγ, and B·(p(p+1)/2 + p + 11) doubles of partials, reduced in two levels when that exceeds the
driver budget (§6.8). Compute per step: O(n·p²) in pass A and O(n·p) in pass B; no shuffle after
preparation. A final pass at the estimates gives the provider table, distributed, with S⁻¹ captured
by closure (p² doubles).

**Determinism.** Provider sums in canonical row order with the fused pairwise cascade (OI-34);
partials in block order with compensated summation; the median is exact (a sort of m values on the
driver); every step length's ℓ is computed with the arithmetic of the sequential search. Hence R0
across partitionings and R1 across block sizes (T-part). Engine code uses Dataset APIs only and runs
under Spark Connect (PLAT-2).

## 12. Validation plan

**Fixtures** (`fixtures/logistic/`, synthetic, exact text inputs as ADR-0005: features on a 1/64
grid or binary, integer outcomes and trials; generated by the next round, after approval):

| Case | Content | References |
|---|---|---|
| lfe-base | 30 providers of 11 to 60 records, three features (two continuous, one binary), Bernoulli | pprof_py default and tight fits, iterates of steps 1 to 5, function-level values at fixed (γ, β); `glm`; R pprof SerBIN |
| lfe-degenerate | lfe-base's design plus providers with no events, with only events, and with 8 and 10 records | pprof_py; R pprof SerBIN on the screened rows; `glm` without the degenerate providers, for X-018 only |
| lfe-shifted | lfe-base with features shifted by +50 and −30 (C27) | pprof_py; `glm`; R pprof SerBIN |
| lfe-binomial | 30 providers, rows with 1 to 20 trials | pprof_py with `n_var`; `glm(cbind(y, n − y) ~ …)` |
| lfe-many | 500 providers of 11 to 30 records | pprof_py; `glm`; R pprof SerBIN |

**Tolerance classes.** T-fn (ℓ, scores and information blocks at fixed (γ, β)), T-iter (iterates of
steps 1 to 5 from the same start), T-coef (β̂, γ̂), T-var (Var(β̂), both provider variances), T-test
(Wald z), T-p, T-part. Converged estimates are compared at tight fits (pprof_py tol = 1e-13,
`glm` epsilon = 1e-15); production defaults (tol = 1e-8: steps, flags, warnings) are compared
separately. Calibration, as `docs/parity/cox-calibration.md`, runs on the generated fixtures before
they enter CI: pprof_py against both R references within every class, and negative controls (bound
5 instead of 10, trials dropped, one outcome flipped) at least ten times outside.

**Metamorphic tests.** Row permutation and partition count: bitwise (probe: pprof_py itself varies
by 2.2e-16 over 30 row orders, always five steps). Block size and layout: T-part. Relabelled
providers: equal up to relabelling. x → x + c: β̂ unchanged and γ̂ → γ̂ − cᵀβ̂ within T-part (probe:
5.6e-15 in pprof_py). x → c·x: β̂/c. Binomial rows against expanded Bernoulli rows: T-part. A
provider with at most 10 records added: the fit of the others unchanged within T-part.

**Edge-case tests:** every row of §9.

## 13. Known discrepancies

| ID | Summary | Class | Proposed decision (D-30) |
|---|---|---|---|
| X-004 | SerBIN stopped early with covariates far from zero (upstream C27) | — | Resolved at v0.7.0 (probe in §12); no internal centering is needed beyond §6's centered sums |
| X-016 | Screening keeps more than `cutoff` records (pprof_py) or at least `cutoff` (R pprof) | — | Follow pprof_py; R comparisons get pprof_py's screened rows |
| X-017 | R pprof's line search has no noise rule, so it can stop up to one step earlier | — | Not a pprof_spark difference; R SerBIN compared under T-coef |
| X-018 | Providers with no events or only events end at med(γ) ∓ 10, so β̂ is not the MLE without them | — | Follow the reference's bounding rule and flag those providers |
| X-019 | Inputs pprof_py mishandles: non-integer counts, no features, no or only events overall, aliased features | C | Fail at validation, naming the problem |
| X-020 | pprof_py's README says `n_var` responses are still validated as 0/1; binomial counts are accepted and fitted | E | Follow the behavior; report the README (OI-33) |
