# Specification: linear random effects — estimation (slice 4a-1)

- Status: approved with D-52 (2026-10-21), with X-038 to X-040; fixtures and calibration in round 96 (D-53, approved); the numerics kernel in round 97.
- Reference: pprof_py v0.7.0 (`9320766`), `LinearRandomEffectModel` (`models/linear/random_effect.py`), and
  lme4 1.1-35.1's `lmer`. Plan: [../random-effects/plan.md](../random-effects/plan.md) (D-51).
- Parity matrix: entries arrive with the code (`docs/parity/matrix.md`).

Inference (`summary`, tests, intervals, measures) is slice 4a-2's; persistence and the job runner are 4a-3's;
further grouping factors (`cluster_vars`) are part 4d's (OI-93).

## 1. Model and estimand

For record i of provider j(i):

    yᵢ = oᵢ + xᵢᵀβ + bⱼ₍ᵢ₎ + εᵢ,   bⱼ ~ N(0, σ²_α),   εᵢ ~ N(0, σ²/wᵢ),

independently, with offsets oᵢ (0 by default) and prior weights wᵢ > 0 (1 by default). x holds p ≥ 0 features,
the constant 1 included unless the fit has no intercept; a feature may vary within providers or only between
them. θ = σ_α/σ ≥ 0, and the search runs on τ = θ².

The estimand is lme4's for `y ~ x + offset(o) + (1 | provider)` with `weights = w`: θ̂ minimizing the profiled
REML deviance (the default) or ML deviance of §2 over [0, θ_upper], β̂ and σ̂ at θ̂, and the BLUPs (conditional
modes) b̂ⱼ.

## 2. Profiled deviance

### 2.1 Summaries

With ỹᵢ = yᵢ − oᵢ:

- per provider: the row count nⱼ, the weight total Wⱼ = Σwᵢ, and the weighted means x̄ⱼ and ȳⱼ of x and ỹ;
- pooled: the weighted within co-moments W_xx = Σᵢ wᵢ(xᵢ − x̄ⱼ)(xᵢ − x̄ⱼ)ᵀ and W_xy = Σᵢ wᵢ(xᵢ − x̄ⱼ)(ỹᵢ − ȳⱼ);
  a within solution β_w of W_xx·β = W_xy (§6); the within residual sum of squares
  RSS_w = Σᵢ wᵢ(ỹᵢ − ȳⱼ − (xᵢ − x̄ⱼ)ᵀβ_w)²; Σ log wᵢ; n, m and p.

### 2.2 Value

For τ ≥ 0, with hⱼ = 1 + τWⱼ and cⱼ = Wⱼ/hⱼ:

- A(τ) = W_xx + Σⱼ cⱼ·x̄ⱼx̄ⱼᵀ, which is pprof_py's X′WX − CH⁻¹C′ and lme4's R_XᵀR_X;
- β̂(τ) solves A(τ)·β = W_xy + Σⱼ cⱼ·x̄ⱼȳⱼ;
- PWRSS(τ) = RSS_w + (β̂ − β_w)ᵀW_xx(β̂ − β_w) + Σⱼ cⱼ(ȳⱼ − x̄ⱼᵀβ̂)², a sum of non-negative terms;
- ML: d(τ) = n·(1 + log(2π·PWRSS/n)) + Σⱼ log hⱼ − Σ log wᵢ;
- REML: d(τ) = (n − p)·(1 + log(2π·PWRSS/(n − p))) + Σⱼ log hⱼ + log det A(τ) − Σ log wᵢ.

As functions of θ = √τ these are pprof_py's `_objective_from_state` and lme4's deviance function (the deviance
under ML, the REML criterion under REML).

### 2.3 Derivative

With r̄ⱼ = ȳⱼ − x̄ⱼᵀβ̂, ν = n (ML) or n − p (REML) and G(τ) = Σⱼ cⱼ²·x̄ⱼx̄ⱼᵀ:

    d′(τ) = −ν·Σⱼ cⱼ²r̄ⱼ²/PWRSS + Σⱼ cⱼ − [REML only] tr(A⁻¹G).

PWRSS′ = −Σⱼ cⱼ²r̄ⱼ² by the envelope theorem (β̂ minimizes PWRSS at each τ), and (log det A)′ = −tr(A⁻¹G). In τ
the derivative at 0 is informative; in θ it is always 0 there.

## 3. Search (D-51 item 7)

- τ ∈ [0, τ_max] with τ_max = θ_upper² (∞ by default).
- Grid: τ = 0 and τ_k = 10ᵏ/W̃ for k = −8, …, 8, with W̃ the median of the Wⱼ; points above τ_max become τ_max.
  While d′ < 0 at the last point and it is below τ_max, the grid extends by decades.
- At the grid point with the smallest d (the smaller τ on ties):
  - τ = 0 with d′(0) ≥ 0: τ̂ = 0, a boundary fit;
  - τ_max with d′(τ_max) ≤ 0: τ̂ = τ_max, a boundary fit at the upper bound;
  - otherwise τ̂ is the root of d′ between that point's neighbours (from 0 to the first grid point when the
    smallest is τ = 0 with d′(0) < 0), by Brent's method (numerics `Brent.root`) to a relative tolerance of
    10⁻¹² in τ.
- Where d′ does not change sign between the neighbours (a deviance flat or not unimodal there), the bracket is
  subdivided once into ten points by the same rule; failing that, the fit takes the best point evaluated, with a
  warning.
- The fit records the evaluations, the bracket, |d′(τ̂)| and whether τ̂ lies on a boundary.

pprof_py's Powell search and `lmer`'s BOBYQA both start at θ = 1. Where the deviance is unimodal, all three reach
the same minimum, pprof_spark to the precision of d′ rather than of d. Where it has several local minima,
pprof_spark reports the best its grid finds, which a local search from θ = 1 may miss; no such case has been
observed.

## 4. Estimates and fit statistics at τ̂

| Quantity | Definition | pprof_py | lme4 |
|---|---|---|---|
| β̂ | β̂(τ̂) | `coefficients_["beta"]` | `fixef` |
| σ̂² | PWRSS(τ̂)/ν | `sigma_`² | `sigma`² |
| θ̂, σ̂_α | √τ̂ and θ̂σ̂ | `theta_`, `random_effect_sd_` | `getME(fit, "theta")`, `VarCorr` |
| Var(β̂) | σ̂²·A(τ̂)⁻¹ | `variances_["beta"]` | `vcov` |
| BLUP b̂ⱼ | τ̂Wⱼr̄ⱼ/(1 + τ̂Wⱼ) | `coefficients_["alpha"]` | `ranef` |
| Conditional variance of bⱼ | σ̂²τ̂/(1 + τ̂Wⱼ) (X-037) | none; its tests use nⱼ | `ranef(condVar = TRUE)` |
| Deviance | d(τ̂) | `objective_`, −2·`loglike_` | `deviance` (ML), `REMLcrit` (REML) |
| Log-likelihood ℓ | −d(τ̂)/2, the restricted one under REML | `loglike_` | `logLik` |
| AIC, BIC | d + 2k and d + k·log n, with k = p + 2 | `aic_`, `bic_` | `AIC`, `BIC` |
| PWRSS, log det H, ‖u‖² | PWRSS(τ̂), Σⱼ log hⱼ, Σⱼ b̂ⱼ²/τ̂ (0 when τ̂ = 0) | `pwrss_`, `ldL2_`, `ussq_` | `getME(fit, "devcomp")$cmp` |
| Fitted values, residuals, xβ | oᵢ + xᵢᵀβ̂ + b̂ⱼ, yᵢ − ŷᵢ, and oᵢ + xᵢᵀβ̂ | `fitted_`, `residuals_`, `xbeta_` | `fitted`, `residuals` |

## 5. Data contract

| Column | Type | Rule |
|---|---|---|
| outcome | numeric | Finite; Decimal converted to double explicitly |
| features | numeric, zero or more | Finite; aliasing is checked on the full design (§6, X-039), so a feature constant within providers is allowed |
| provider | integral or string | Non-null; providers ordered by key (ADR-0004) |
| offset | numeric, optional | Finite |
| weights | numeric, optional | Finite and positive, as pprof_py requires (lme4 accepts 0, and its deviance is then infinite) |
| row identifier | integral, optional | Unique; fixes the canonical order (DIST-6) |

Validation runs once, over every input row, before fitting: missing or non-finite values fail with counts per
column (X-038); weights that are not positive fail with their count; then the design must have m ≥ 2 and m < n,
as lme4 checks, and n > p (X-040). Messages carry column names and counts, never values (T6). No row is dropped.

## 6. Conventions

| Item | Convention |
|---|---|
| Criterion | REML by default (`reml`), ML on request, recorded with the fit (D-51 item 9) |
| Parameter | θ = σ_α/σ ∈ [0, θ_upper], with θ_upper = ∞ by default, searched as τ = θ² (§3) |
| Weights | Inverse residual-variance weights, as in pprof_py and lme4: σ̂ is the residual SD at unit weight |
| Aliasing | A pivoted Cholesky of X′WX = W_xx + Σⱼ Wⱼ·x̄ⱼx̄ⱼᵀ with R's rule, as `LinearFE`'s: an aliased feature fails the fit and is named (X-039). A feature constant within providers is identified by the between part, unlike in the fixed-effect model |
| Within solution β_w | A pivoted-Cholesky solution of W_xx·β = W_xy, with the coordinates of aliased pivots set to 0; every solution gives the same PWRSS |
| Boundary fits | τ̂ = 0 gives b̂ = 0, conditional variances of 0 and a warning, as lme4's singular-fit message; τ̂ = θ_upper² gives a warning |
| Provider order | Sorted by key, as the reference's categories |
| Provider sizes | The provider table carries both nⱼ and Wⱼ; pprof_py's `provider_sizes_` are the nⱼ |
| Prediction | Fixed effects by default, oᵢ + xᵢᵀβ̂, as pprof_py's `predict` (lme4's `predict` includes the random effects unless `re.form = NA`); with `use_re`, b̂ⱼ is added for providers in the fit and 0 for others, as in both references |

## 7. Distributed algorithm

1. Validation and the canonical order; ProviderLocal blocks, each provider in one block, as `LinearFE`.
2. Pass 1, per block: for each provider nⱼ, Wⱼ, x̄ⱼ, ȳⱼ and the weighted centred co-moments, by weighted pairwise
   updates in the canonical order (West's weighted form of Chan, Golub and LeVeque's). The block adds W_xx, W_xy,
   Σⱼ Wⱼ·x̄ⱼx̄ⱼᵀ, Σ log w and n over its providers (numerics `Summation`) and emits the totals, which the driver
   adds in block order (§6.8). The rows (key, nⱼ, Wⱼ, x̄ⱼ, ȳⱼ) form the distributed provider table.
3. Driver: the aliasing check on X′WX, and β_w.
4. Pass 2, per block, with β_w broadcast: RSS_w, emitted per block and added in block order. As in `LinearFE`,
   the residual sum of squares is never formed from the co-moments, where it cancels when the within fit is
   close.
5. The search of §3. On the driver when the provider table fits the driver budget (m(p + 3) doubles), each
   evaluation is a sweep over the providers in key order; otherwise each evaluation is two deterministic
   aggregations over the cached table: A(τ), the right-hand side and G(τ), then, with β̂(τ) broadcast,
   Σⱼ cⱼr̄ⱼ² and Σⱼ cⱼ²r̄ⱼ².
6. At τ̂: the estimates of §4; the provider table gains b̂ⱼ and its conditional variance.
7. Pass 3, on request: fitted values, residuals and xβ as columns of the training rows.

Cost: pass 1 is O(np²) and pass 2 O(np); the search is O(E·m·p²) for E evaluations (the grid's 18 points and the
root's iterations), with O(p²) bytes per block per evaluation on the distributed path. No shuffle beyond the
ProviderLocal layout.

## 8. Outputs

| Output | Content |
|---|---|
| Fit summary | β̂ with standard errors, Var(β̂) packed, σ̂, θ̂, σ̂_α, the criterion, n, m, p, the deviance, ℓ, AIC, BIC, PWRSS, log det H, ‖u‖², the search record (§3), warnings, column roles, software and the data fingerprint (NN-11) |
| Provider table | Key, nⱼ, Wⱼ, b̂ⱼ, its conditional variance and SD; distributed |
| Prediction | The input rows with the prediction, fixed effects only or with `use_re` |
| Fitted values | On request (pass 3) |

## 9. Edge cases (round 95 probes)

| Case | pprof_py | lme4 | pprof_spark |
|---|---|---|---|
| One provider | Fits: under REML θ̂ = 2.03 on a deviance flat in θ, under ML θ̂ = 1.4·10⁻⁸ | Fails: grouping factors must have more than one sampled level | Fails at validation (X-040) |
| One row per provider (m = n) | Fits (θ̂ = 0.65 under REML), though only σ²(1 + θ²) is identified | Fails: the levels must be fewer than the observations | Fails at validation (X-040) |
| An aliased feature (x₃ = 2x₁) | ML: x₁'s effect split arbitrarily (0.162 on x₁, 0.324 on x₃); REML: a linear-algebra error (the information matrix is not positive definite) | Drops the column with a message | Fails, naming the feature (X-039) |
| A feature constant within providers | Identified; θ̂ within 2.4·10⁻⁸ of lme4's | Identified | Identified (§6) |
| No provider variance in the data | θ̂ = 0.0604 under REML, interior in this sample, within 2.5·10⁻⁸ of lme4's | The same | The boundary rule of §3; a fixture with τ̂ = 0 (§11) |
| Missing values | Drops the incomplete rows (3 of 1,095 here) | Drops them (`na.omit`) | Fails with counts per column (X-038) |
| A zero weight | Fails: weights must be finite and positive | Fits, with an infinite deviance | Fails, as pprof_py |
| No intercept; an offset | Agrees with lme4 within 7·10⁻⁸ in θ̂ | — | Supported |
| Row order | — | — | Bitwise invariant (NN-4) |

## 10. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `reml`, `theta_upper` | The same options |
| `max_iter_outer` | The cap on the search's evaluations |
| `tol_outer`, `max_iter_inner`, `optimizer`, `verbose` | Not used by pprof_spark's search (D-51 item 7); the Python wrapper accepts them and warns when they differ from pprof_py's defaults; `verbose` maps to logging |
| `fit(X, y_var, x_vars, provider_var, cluster_vars, offset_var, weights_var, include_intercept, reml)` | The same roles; `cluster_vars` fails until part 4d (OI-93) |
| The attributes of §4; `get_random_effects`, `get_sigma`, `get_random_effect_sd`, `standard_errors`, `predict`, `fitted_values`, `residual_standard_error` | The fit summary, the provider table, prediction and pass 3 |
| `summary`, `fixed_effects_table`, `conf_int`, `test`, `calculate_confidence_intervals`, `calculate_standardized_measures` | Slice 4a-2 |

The Python wrapper of this slice, `pprof_spark.LinearRandomEffectModel`, takes pprof_py's constructor and `fit`
arguments and offers the estimation attributes and methods above, through a fit handle (ADR-0009).

## 11. Validation plan

Fixtures in `fixtures/linear-re`, each fitted under REML and ML:

| Case | Content |
|---|---|
| lre-base | 40 providers of 1 to 119 records and three features (lin-base's design) |
| lre-weights | Prior weights uniform on [0.5, 2] |
| lre-offset | An offset |
| lre-nointercept | No intercept |
| lre-provider-level | A feature constant within providers |
| lre-boundary | Data whose REML and ML optima lie at θ = 0 |
| lre-shifted | Features far from zero (means near 10⁴) and of unequal scales |
| lre-text | String provider keys |
| lre-many | About 2,000 providers, to exercise several blocks |

References: pprof_py at the pin, at its defaults and with `tol_outer` tightened; lme4's `lmer` at its defaults,
and each reference at its tight optimum: the root in τ of central differences of its own deviance function
(lme4's through `devFunOnly = TRUE`), with the fit evaluated there (§13).

- Function level: the summaries of §2.1 against NumPy at fixed data (T-fn); d(τ) at fixed τ against lme4's
  deviance function and pprof_py's `_objective_from_state` (T-fn); d′(τ) against central differences of the
  references' d.
- End to end, against the tight references: β̂, b̂ and predictions (T-coef); σ̂², θ̂, σ̂_α, Var(β̂) and the
  conditional variances (T-var); the deviance, ℓ, AIC and BIC (T-fn). Against the default runs: T-opt,
  informational in calibration. Calibration confirms the classes or proposes one under NN-9 (D-51 item 8).
- Metamorphic: bitwise invariance to row order and partitioning; block sizes within T-part; y + c, with an
  intercept, moves the intercept by c and nothing else; y·s scales β̂, b̂ and σ̂ by s and leaves θ̂ unchanged;
  weights·k leave β̂, b̂, σ̂_α, the conditional variances and the deviance unchanged, scale σ̂ by √k and θ̂ by
  1/√k; an offset o gives the fit of y − o.
- Recovery: σ̂_α on `bench.synthetic`'s linear data at large m, within its sampling error (seeded).
- The edge cases of §9; negative controls as D-09 requires; calibration as in the earlier slices.

## 12. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-037 | B | The BLUPs' conditional variances use the weight totals Wⱼ, where pprof_py's standard errors use the row counts nⱼ | Approved (D-51) |
| X-038 | C | Incomplete rows fail with counts per column instead of being dropped | Approved (D-52) |
| X-039 | C | Aliased features fail and are named, instead of an arbitrary split (ML), a linear-algebra error (REML) or lme4's dropped column | Approved (D-52) |
| X-040 | C | Designs whose provider variance is not identified (m < 2, m ≥ n), and n ≤ p, fail at validation, as lme4 refuses the first two; pprof_py fits them | Approved (D-52) |

## 13. Fixture findings (round 96)

- `fixtures/linear-re` (`reference/fixtures/linear_re_fixtures.py`, `linear_re_r.R`) holds the nine cases of §11
  under REML and ML: pprof_py and `lmer` at their defaults and at their tight optima, the deviance at θ = 0, 0.1,
  0.3, 1 and 3 and its central-difference slope in τ, NumPy's summaries of §2.1, and pprof_py's negative controls.
  Regenerating from the committed inputs reproduces every file byte for byte.
- A tight optimum is the root in τ of the Richardson-extrapolated central differences (h = 3·10⁻³τ) of the
  reference's own deviance function, or 0 where that deviance rises from 0; pprof_py is then evaluated by its `fit`
  at that θ, and lme4 by `mkMerMod`.
- An exact evaluation of §2's formulas (NumPy in double precision, outside the repository; the round's evidence
  log) reproduces both references' deviance at fixed θ within 2.6·10⁻¹⁵ relative on the eight well-conditioned
  cases. Its root of d′ lies within 2.3·10⁻¹⁰ of both references' tight τ*, which the difference scheme biases low
  by about 2·10⁻¹⁰, and its β̂ within 4·10⁻¹² of theirs.
- lre-shifted is lre-base's draws with x₁ + 10⁴, x₂ − 10⁴ and x₃/1024. There both references lose precision to the
  conditioning of the uncentred between part: up to 1.2·10⁻¹¹ in the REML deviance at fixed θ, 4.2·10⁻⁷ in τ* and
  6.2·10⁻⁸ in β̂. Its parity is invariance against lre-base, with the references' agreement informational (D-53):
  θ̂, σ̂, σ̂_α, b̂ and the fitted values unchanged; the slopes of x₁ and x₂ unchanged, x₃'s multiplied by 1024 and the
  intercept moved by 10⁴(β̂₂ − β̂₁); the ML deviance unchanged and the REML criterion lower by 2 log 1024.
- Calibration (`docs/parity/linear-re-calibration.md`): T-coef, T-var and T-fn as D-53 proposes. PWRSS, log det H
  and ‖u‖² move to first order with θ̂, unlike the deviance, so they take T-var.

## 14. Implementation note (round 96; no statistical change)

With an intercept, the features enter the between part centred at their weighted grand means, x̄ⱼ − x̄. The change
of variables leaves θ̂, σ̂, the BLUPs and the slopes unchanged and moves only the intercept, which is transformed
back; A(τ) stays as well conditioned as the data allow, so lre-shifted's invariance holds within the ordinary
classes.

## 15. Implementation notes (round 97, numerics; no statistical change)

- `numerics.kernels.LinearRE`: pass 1 `summaries` (two passes over each provider's rows, weighted means and then
  centred sums), `withinSolution` (through Cholesky's new `solveUnaliased`, which solves on the columns that are
  not aliased and leaves the others at 0), pass 2 `withinResiduals`; `Problem.evaluate(τ)` for d(τ), d′(τ) and β̂
  in the centred coordinates of §14; `fit` for the search of §3 and the estimates of §4; `aliased` for X-039's
  check on X′WX in centred coordinates.
- PWRSS uses the within gradient g = W_xy − W_xx·β_w: PWRSS = RSS_w − 2vᵀg + vᵀW_xx·v + Σⱼ cⱼr̄ⱼ² with v = β̂ − β_w.
  It is exact for any β_w, and g vanishes for an exact within solution, so a feature that Cholesky treats as
  within-aliased costs nothing.
- log(1 + τWⱼ) by `log1p`; Brent's root to machine precision (`xtol` 0); 18 to 33 evaluations on the fixtures.
- `LinearREKernelSuite`: the summaries against exact sums; the deviance, PWRSS, β̂ and the BLUPs against a dense
  solve of the mixed-model equations within 10⁻¹²; the slope against central differences; the minimum, the
  boundary fits, the shift and weight invariances, aliasing and the refused designs.
- Against `fixtures/linear-re`, by a harness outside the repository (the round's evidence log), the kernel agrees
  with both references within 0.0053 of T-coef, 0.0019 of T-var and 0.0024 of T-fn on the eight well-conditioned
  cases, its conditional variances with lme4's within 0.0019 of T-var, and lre-shifted's invariance against
  lre-base holds within 0.0025 of its classes. The committed parity suite comes with the engine.
