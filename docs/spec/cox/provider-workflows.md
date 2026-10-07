# Specification: provider workflows (Phase 1d)

- Status: **Approved on 2026-10-06 (D-27).** Round 20 implements 1d-1 (standardized measures) and
  its fixtures. Scale-test site settled (D-28): the maintainer runs it on his Databricks workspace.
- Scope (§4, Phase 1d): two-stage SMR and SHR; expected counts; O/E ratios; exact Poisson
  intervals and tests; flags; provider result tables; a job-runner entry point. Exit: the parity
  gate, and an end-to-end Spark application at design-envelope scale.
- Reference: pprof_py v0.7.0: `CoxPH.calculate_standardized_measures` and
  `cox_standardized_expectations` (`measures/survival/coxph.py`), `CoxPH.test`
  (`inference/survival/provider_tests.py`), `poisson_exact_test` (`inference/survival/inference.py`),
  `poisson_midp_zscore` (`inference/survival/empirical_null.py`), `PROVIDER_TEST_COLUMNS`
  (`inference/decision.py`). Definitions follow the "Tutorial for Standardized Measures of Survival
  Outcomes" (He and Schaubel), which pprof_py cites.
- Slices: 1d-1 standardized measures; 1d-2 provider tests and result tables; 1d-3 job runner;
  1d-4 scale validation.

## 1. Standardized measures (1d-1)

For row i of provider j with entry Bᵢ (0 without truncation), exit Xᵢ, event δᵢ and
wᵢ = exp(ηᵢ), ηᵢ = xᵢᵀβ̂ + oᵢ, using the data the model was fitted to (API-3):

| Quantity | Definition |
|---|---|
| National baseline Λ₀ | Breslow over all rows, with η as offset: increments d(t)/RS(t), RS(t) = Σ_{Bᵢ < t ≤ Xᵢ} wᵢ, d(t) the number of events at t |
| Provider baseline Λ₀ⱼ | The same over provider j's rows |
| Observed Oⱼ | Provider j's events |
| Indirect expected Eⱼ | Σ_{i ∈ j} wᵢ [Λ₀(Xᵢ) − Λ₀(Bᵢ)]; Σⱼ Eⱼ = O |
| Indirect ratio | Oⱼ / Eⱼ (SMR or SHR, depending on the event) |
| Direct expected E⁽ʲ⁾ | Σ_{all i} wᵢ [Λ₀ⱼ(Xᵢ) − Λ₀ⱼ(Bᵢ)], computed as the sum over provider j's events of RS(t)/RSⱼ(t) |
| Direct ratio | E⁽ʲ⁾ / O, with O all events |
| Person-time | Σ_{i ∈ j} (Xᵢ − Bᵢ) |

Conventions, as pprof_py: the baselines are Breslow whatever ties fitted β̂; case weights are not
used (each row counts once); a provider-stratified fit gives He and Schaubel's two-stage estimate,
an unstratified fit the pooled model's; a list of providers filters the output only; every
quantity is invariant to a common factor in wᵢ, which pprof_py removes with exp(η − max η) and
pprof_spark too. A provider with Eⱼ = 0 gets the ratio pprof_py gives (∞ or NaN), and a warning
counts such providers (NN-10).

## 2. Provider tests and result tables (1d-2)

Each provider's indirect ratio is tested against 1. Two methods, as pprof_py:

| Method | p-value and z | Interval for the ratio |
|---|---|---|
| `exact` | Two-sided Poisson: min(0.999, 2·P(X ≥ O)) when O/E > 1, else min(0.999, 2·P(X ≤ O)); z = sign(O − E)·Φ⁻¹(1 − p/2) | E < 100: χ²₍α/2, 2O₎/(2E) (0 when O = 0) to χ²₍1−α/2, 2(O+1)₎/(2E); E ≥ 100: Byar's cube-root approximation |
| `midp` (default) | P_min = 2F(O) − f(O), P_max = 2(1 − F(O − 1)) − f(O), p = max(1e-6, min(P_min, P_max)/2), z = Φ⁻¹(p) signed by the smaller tail; calibrated as (z − μ)/σ with the theoretical null μ = 0, σ = 1 | Where the calibrated two-sided p-value equals α, by root finding on the Poisson mean; 0 or ∞ where it stays above α |

F and f are the Poisson CDF and PMF at mean E. Flags: +1 when p < 1 − level and O > E, −1 when
p < 1 − level and O < E, otherwise 0. The result table has pprof_py's `PROVIDER_TEST_COLUMNS`
(estimate, se, null_value, transformed, se_transformed, null_transformed, z_raw, null_mean,
null_sd, null_group, z_adjusted, p_value, flag, ci_lower, ci_upper) plus observed, expected and
person_time, keyed by provider. The empirical null (`null_model`) is deferred with
"empirical-null calibration" (§4, Later); `null_model` other than the theoretical null fails.

Numerics added for this (StrictMath, deterministic, each tested against mpmath or scipy
references): Poisson CDF, survival function and PMF through the regularized incomplete gamma
function; χ² quantiles; Brent's root finding with fixed tolerances; Byar's limits.

## 3. API

| Call | Result |
|---|---|
| `CoxMeasures.standardized(df, fit, provider, kinds = Seq(Indirect), providers = None)` | One distributed DataFrame per kind: provider, ratio, observed, expected, and person_time (indirect) or n_pop (direct) |
| `CoxProviderTests.test(df, fit, provider, method = MidP, level = 0.95, providers = None)` | A distributed DataFrame with the columns of §2 |

`provider` names a column of `df` (integral or string, no nulls); it may equal `CoxSpec.strata`.
Both calls check the data fingerprint (API-3).

## 4. Distributed plan

| Step | Plan | Cost |
|---|---|---|
| Risk scores | wᵢ from η, with max η by a global max (order-free) | One pass |
| National baseline | Each block sums wᵢ by exit time and by entry time in canonical order; partials meet by time through one shuffle and add in block order; the K-scale result (distinct event times, about 10⁴, guarded by the driver budget) gives RS(t) and Λ₀ on the driver with Neumaier sums, then ships to executors | One shuffle of at most B·K partial sums |
| Eⱼ, Oⱼ, person-time | Per row, then summed by provider in (block, position) order through a keyed reduction | One shuffle of n small rows |
| E⁽ʲ⁾ | Provider-local blocks: the fit's blocks when it was stratified by provider, otherwise a provider layout built once; RSⱼ(t) at provider j's events in the kernel, RS(t) from the shipped table | One shuffle when a provider layout is needed |
| Tests | Row-wise over the provider table on executors | No shuffle |

m-scale tables stay distributed (DIST-1); results do not depend on Spark's partitioning (R0).

## 5. Job runner (1d-3)

An `app` module with a `spark-submit` entry point reads a versioned JSON run specification: input
(path and format, or table), column roles (`CoxSpec`), fit options, requested outputs (fit, baseline,
residuals, standardized measures, provider tests) and output locations. It writes each result as
Parquet, the fit through `CoxFitIO`, and a run record with the specification, `SoftwareInfo`, the
data fingerprint and timings. Classic Spark only (§6.12); no Databricks packaging (D-14).

## 6. Scale validation (1d-4)

A `bench` workload generates synthetic data deterministically on the cluster (seeded per block):
providers with heavy-tailed sizes, ties at daily resolution, delayed entry, p = 10 and 100, at
n = 10⁷, 10⁸ and 10⁹ with m = 10⁴ to 10⁶. It runs the job runner end to end and records wall time
per stage, passes, shuffle bytes, driver and executor memory, and checks R0 across reruns and R1
across block sizes. Targets are set from the first results (D-04). Known limits it will exercise:
strata above `maxStratumRows` (TimeRange, OI-03), unstratified prediction (OI-43), clustered robust
variance (OI-45), large p (OI-02, OI-38).

Site (D-28): the maintainer runs the scale test himself on his Databricks workspace, once the code
is ready; AI assistants never access that environment (D-14 as clarified). The assistant supplies
the JARs, run specifications, the data generator and instructions, and analyses the results the
maintainer shares. Databricks Runtime 18 LTS runs Spark 4.1.0 with Scala 2.13.16 and JDK 21, which
the linkage checks already target; a dedicated (single-user) cluster runs Classic Spark, and a
standard (shared) one runs Scala through Spark Connect, which the engine supports.

## 7. Validation plan

| Level | Test | Class |
|---|---|---|
| Fixtures | For the stratified cases with the stratum as provider, and for unstratified fits with a provider column: pprof_py's indirect and direct measures and its `test` output for `exact` and `midp` at level 0.95; R's `ppois` and `qchisq` for the Poisson numerics | — |
| Measures | Observed, expected, ratios and person-time against pprof_py | T-base |
| Tests | z and p-values, limits and flags against pprof_py | T-test, T-p, T-base, exact |
| Numerics | Poisson, χ² and Brent against mpmath | per function |
| Identities | Σⱼ Eⱼ = O; providers filter output only; a provider-stratified fit's direct and indirect ratios agree with a per-provider recomputation | T-base |
| Reproducibility | R0, R1, R2 | bitwise or T-base |
| End to end | The job runner on fixture data reproduces the library results bit for bit | bitwise |

## 8. Known discrepancies

None proposed. Any found while building the fixtures will be brought for decision.
