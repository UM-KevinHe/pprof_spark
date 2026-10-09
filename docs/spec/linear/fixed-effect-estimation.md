# Specification: linear fixed effects — estimation and covariate inference (Phase 3a)

- Status: approved with D-45 (2026-10-09).
- Reference: pprof_py v0.7.0 (`9320766`), `LinearFixedEffectModel` (`models/linear/fixed_effect.py`,
  `algorithms/linear/fixed_effect.py`, `inference/linear/fixed_effect.py`), and R's `lm` with a provider
  factor. Plan: [plan.md](plan.md).

## 1. Model and estimand

For record i of provider j, yᵢ = γⱼ + xᵢᵀβ + εᵢ with εᵢ independent N(0, σ²): one fixed intercept per
provider, no global intercept, p ≥ 1 features, no case weights, no offset. β̂ and γ̂ are the least-squares
(maximum-likelihood) estimates, unique when the within cross-product of §2 is positive definite.

## 2. Estimator

With nⱼ records per provider, means x̄ⱼ and ȳⱼ, and the within transformation x̃ᵢ = xᵢ − x̄ⱼ₍ᵢ₎,
ỹᵢ = yᵢ − ȳⱼ₍ᵢ₎:

- β̂ solves (Σᵢ x̃ᵢx̃ᵢᵀ) β = Σᵢ x̃ᵢỹᵢ;
- γ̂ⱼ = ȳⱼ − x̄ⱼᵀβ̂;
- the residuals are rᵢ = yᵢ − γ̂ⱼ₍ᵢ₎ − xᵢᵀβ̂, and RSS = Σᵢ rᵢ².

## 3. Variances and fit statistics

| Quantity | Definition |
|---|---|
| σ̂ | √(RSS/(n − m − p)), the reference's `sigma_` and R's `sigma` |
| Var(β̂) | σ̂²V with V = (Σᵢ x̃ᵢx̃ᵢᵀ)⁻¹ |
| Var(γ̂ⱼ), `complete` (default) | σ̂²(1/nⱼ + x̄ⱼᵀVx̄ⱼ), the exact variance (R's `vcov` for the provider factor without an intercept) |
| Var(γ̂ⱼ), `simplified` | σ̂²/nⱼ |
| Log-likelihood | ℓ = −(n/2)(log 2π + log(RSS/n) + 1) |
| AIC, BIC | −2ℓ + 2k and −2ℓ + k log n, with k = m + p + 1 |
| R² (`score`) | 1 − Σ(yᵢ − ŷᵢ)²/Σ(yᵢ − ȳ)² on the data given |

## 4. Inference on β (`summary`)

For feature k, t = (β̂ₖ − null)/se(β̂ₖ) on df = n − m − p degrees of freedom, with F the t distribution
function and F̄ = 1 − F its upper tail, evaluated directly (X-031):

| Alternative | p-value | Limits |
|---|---|---|
| `two_sided` | 2F̄(\|t\|) | β̂ₖ ∓ t₁₋α/₂ se |
| `greater` | F̄(t) | [β̂ₖ − t₁₋α se, ∞) |
| `less` | F(t) | (−∞, β̂ₖ + t₁₋α se] |

`level` (default 0.95) and `null` (default 0) as in the reference.

## 5. Data contract

| Column | Type | Rule |
|---|---|---|
| outcome | numeric | Finite; Decimal converted to double explicitly |
| features | numeric, at least one | Finite; a feature constant within every provider, a constant one included, is aliased (X-032) |
| provider | integral or string | Non-null; providers ordered by key (ADR-0004) |
| row identifier | integral, optional | Unique; fixes the canonical order (DIST-6) |

Validation runs once, over every input row, before fitting: missing or non-finite values, then the degrees
of freedom n − m − p ≥ 1 (X-033). Messages carry column names and counts, never values (T6). Every provider
is kept: the reference does not screen linear models.

## 6. Conventions

| Item | Convention |
|---|---|
| Provider order | Sorted by key, as the reference's `np.unique` |
| Variance option | `complete` (default) or `simplified`, recorded with the fit |
| Degrees of freedom | n − m − p for σ̂, the t tests and the limits |
| Aliasing | Cholesky of Σx̃x̃ᵀ with R's rule (a pivot below ε^0.75 times the largest diagonal entry, numerics `Cholesky`): an aliased feature, constant within every provider or collinear with others after demeaning, fails the fit and is named (X-032) |
| Condition | The fit records the ratio of the largest to the smallest Cholesky pivot; §7.5's TSQR path is deferred (OI-61) |
| Prediction | γ̂ⱼ + xᵀβ̂ for providers in the fit; rows of other providers fail with counts, as the reference raises |

## 7. Distributed algorithm (PROJECT_CONTEXT §7.5, plan §3)

1. Validation and the canonical order; ProviderLocal blocks (§6.6), each provider in one block.
2. Pass 1, per block: for each provider nⱼ, x̄ⱼ, ȳⱼ and the centred co-moments Σ(x − x̄ⱼ)(x − x̄ⱼ)ᵀ and
   Σ(x − x̄ⱼ)(y − ȳⱼ), by Chan, Golub and LeVeque's pairwise updates in the canonical order; the block adds
   them over its providers (numerics `Summation`) and emits the totals, which the driver adds in block
   order (§6.8). The rows (key, nⱼ, x̄ⱼ, ȳⱼ) form the distributed provider table.
3. Driver: the aliasing check and β̂ by Cholesky; V from the factor.
4. Pass 2, per block, with β̂ broadcast: γ̂ⱼ for its providers, the residuals and their sum of squares,
   emitted per block and added in block order. RSS gives σ̂, ℓ, AIC and BIC. The provider table gains γ̂ⱼ
   and qⱼ = 1/nⱼ + x̄ⱼᵀVx̄ⱼ, so Var(γ̂ⱼ) is σ̂²qⱼ (or σ̂²/nⱼ) without a further pass.
5. Results: the fit summary on the driver and the provider table, distributed.

RSS is never formed from the co-moments: ỹᵀỹ − β̂ᵀΣx̃ỹ cancels when the within fit is close.

## 8. Outputs

| Output | Content |
|---|---|
| Fit summary | β̂ with standard errors, V (packed), σ̂, n, m, p, df, RSS, ℓ, AIC, BIC, the variance option, the condition estimate, warnings, column roles, software and the data fingerprint (NN-11) |
| `summary` | Per feature: estimate, std_error, stat, p_value, ci_lower, ci_upper (the reference's columns) |
| Provider table | Key, nⱼ, γ̂ⱼ, its variance under the chosen option and its standard error; distributed |
| Prediction | The input rows with the prediction; fitted values and residuals on request, as columns of the training rows (the reference keeps them as n-vectors) |
| Persistence | `LinearFitIO`, format version 1: the summary as JSON with doubles as 64-bit patterns and the provider table in Parquet; never overwriting; a bit-for-bit round trip (as `LogisticFitIO`) |

## 9. Edge cases

| Case | pprof_py (round 62 probe) | pprof_spark |
|---|---|---|
| Providers with one record | γ̂ⱼ = yᵢ − xᵢᵀβ̂, residual 0, complete variance σ̂²(1 + x̄ⱼᵀVx̄ⱼ) | Same |
| A feature constant within every provider | An arbitrary coefficient (0.0081) with no warning; R's `lm`: NA | Fails, naming the feature (X-032) |
| Collinear features after demeaning | Not probed; a pseudo-inverse only when the solve raises on exact singularity | Fails, naming the features (X-032) |
| n − m − p ≤ 0 | σ̂ = ∞ with a NumPy divide warning | Fails at validation with the counts (X-033) |
| A perfect within fit (RSS = 0) | Not probed | σ̂ = 0 with a warning (NN-10); t statistics ±∞, NaN where the estimate equals the null |
| Missing or non-finite values | The shared validation | Fails with counts per column |
| String provider keys | Sorted (`np.unique`); `score` and array-input `predict` fail on them | Sorted by key (ADR-0004); R² and predictions for any key type (X-034) |
| Row order | Sums depend on it: β̂ moved by 7.8e-16 and γ̂ by 1.3e-15 when the probe's rows were shuffled | Bitwise invariant (NN-4) |

## 10. Reference mapping

| pprof_py | pprof_spark |
|---|---|
| `coefficients_["beta"]`, `coefficients_["gamma"]` | β̂ in the fit summary; γ̂ in the provider table |
| `variances_["beta"]`, `variances_["gamma"]` | σ̂²V in the fit summary; the provider table's variance |
| `sigma_`, `aic_`, `bic_` | σ̂, AIC, BIC |
| `summary(covariates, level, null, alternative)` | `summary` with the same arguments |
| `predict`, `score` | Prediction; R² |
| `fitted_`, `residuals_`, `xbeta_` | On request, as columns of the training rows |

## 11. Validation plan

Fixtures in `fixtures/linear`, from pprof_py at the pin and R's `lm`:

| Case | Content |
|---|---|
| lin-base | 40 providers of 1 to 119 records and three features (the probe's design) |
| lin-singletons | Many providers of one or two records |
| lin-shifted | Features far from zero (means near 10⁴) and of unequal scales |
| lin-text | String provider keys |
| lin-many | About 2,000 providers, to exercise several blocks |

- Function level: the within co-moments and the provider means against NumPy's demeaned cross-products
  at fixed data (T-fn). There is no lockstep level: the estimator does not iterate.
- End to end: β̂ and γ̂ (T-coef), both variance options (T-var), σ̂, ℓ, AIC and BIC (T-fn), t statistics
  (T-test), p-values (T-p: against R everywhere, against pprof_py where its value exceeds 1e-8, X-031),
  prediction and R².
- Metamorphic: bitwise invariance to row order and partitioning; block sizes within T-part; y + c moves
  every γ̂ⱼ by c and leaves β̂ and σ̂ unchanged; scaling a feature by s divides its coefficient by s;
  duplicating every record leaves β̂ and γ̂ unchanged.
- Edge cases of §9; negative controls as D-09 requires; calibration as in slices 2a to 2f.

## 12. Known discrepancies

| ID | Class | Summary | Status |
|---|---|---|---|
| X-031 | B | pprof_py's `summary` loses every p-value below about 1e-16 | Approved (D-45) |
| X-032 | C | Aliased features fail instead of returning an arbitrary coefficient | Approved (D-45) |
| X-033 | C | n − m − p ≤ 0 fails instead of returning σ̂ = ∞ | Approved (D-45) |
| X-034 | C | pprof_py's `score` rejects string provider keys; pprof_spark computes R² for any key type | Proposed (round 63) |

## 13. Fixture findings (round 63)

1. `fixtures/linear` holds the five cases of §11 (20 files), regenerated byte for byte from scratch and from
   the committed inputs. pprof_py agrees with R's `lm` inside the existing classes on every case; worst
   ratios: T-fn 0.11 (lin-shifted, rss), T-meas 0.003 (lin-shifted, predict), T-coef 8.9e-05 (lin-shifted, summary two_sided ci_lower), T-test 1.7e-05 (lin-shifted, summary greater stat), T-var 1.1e-06 (lin-shifted, var_gamma (simplified)), T-p 1.1e-06 (lin-text, summary two_sided p_value (1 of 3 above 1e-8 in pprof_py)). Every negative control misses by at least 138 (docs/parity/linear-calibration.md). No new
   tolerance class is needed.
2. X-031 shows in every case: pprof_py's `summary` returns p = 0 for features whose R p-value is positive.
   The calibration compares the t statistics under T-test and the p-values on the log₁₀ scale where
   pprof_py's exceed 1e-8, as §8.4's T-p allows.
3. X-034 (proposed): pprof_py's `score` takes arrays, and its array path converts provider keys to floats, so
   it fails on lin-text. The fixtures compute R² by `score`'s formula from `predict`'s DataFrame predictions;
   on the numeric-key cases the two agree to rounding.
