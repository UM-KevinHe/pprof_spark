# Python access (py4j wrappers)

The `pprof_spark` Python package calls the pprof_spark JARs in the Spark driver's JVM through
PySpark's py4j gateway (ADR-0009, D-31, D-33). Every statistic is the engine's; the package only
passes arguments and results, so a fit from Python equals the Scala library's bit for bit.

## Requirements

- A **Classic** PySpark 4.1 session with the pprof_spark JARs (`numerics`, `engine`, `app`) on its
  classpath, for example through `spark.jars`. On Databricks: classic compute in dedicated access
  mode with the JARs installed as libraries; standard (shared) access mode is expected to refuse
  py4j calls into library classes (to be checked on the maintainer's workspace, OI-53).
- **Spark Connect** clients have no JVM gateway: the package refuses them and points to the job
  runner ([cox-job.md](cox-job.md)).
- The JARs and the package must be the same version; the package checks it (`0.1.0-SNAPSHOT` in
  the JVM is `0.1.0.dev0` in Python).

Until releases ship a wheel with the JARs (OI-55), install from the repository:
`pip install -e python`, and build the JARs with `sbt numerics/package engine/package app/package`.

## Cox models

```python
from pyspark.sql import SparkSession
from pprof_spark import CoxPH, CoxPHModel

spark = (SparkSession.builder
         .config("spark.jars", "pprof-spark-numerics_2.13-0.1.0-SNAPSHOT.jar,"
                               "pprof-spark-engine_2.13-0.1.0-SNAPSHOT.jar,"
                               "pprof-spark-app_2.13-0.1.0-SNAPSHOT.jar")
         .getOrCreate())
df = spark.read.parquet("...")

fit = CoxPH("time", "event", ["age", "diabetes"], strata="facility", row_id="id",
            ties="breslow").fit(df)
fit.coefficients          # Coefficient(feature, estimate, standard_error, z, p_value, lower, upper)
fit.covariance, fit.log_likelihood, fit.converged, fit.warnings

baseline = fit.baseline(df)                                # per-stratum baseline hazard
fit.survival(df_with_baseline_strata, baseline, "time")    # cumulative hazard and survival
fit.residuals(df)                                          # martingale, score, dfbeta
fit.standardized_measures(df, "facility", ("indirect", "direct"))   # dict of DataFrames
fit.provider_tests(df, "facility", method="midp", level=0.95)

fit.save("/path/fit", baseline=baseline)
again = CoxPHModel.load(spark, "/path/fit")
```

`CoxPH`'s arguments are the `columns` and `fit` objects of the job runner's run specification, in
Python spelling; options left as `None` take the engine's defaults. Invalid inputs raise
`pyspark.errors.IllegalArgumentException` with the engine's message (counts, never values).
## Logistic fixed-effect models

```python
from pprof_spark import LogisticFixedEffect, LogisticFixedEffectModel

fit = LogisticFixedEffect("readmitted", ["age", "comorbidity"], "facility", row_id="id",
                          cluster="patient").fit(df)
fit.coefficients, fit.covariance, fit.robust_covariance, fit.auc, fit.excluded
fit.providers()                                    # effects, variances, flags per facility
fit.wald_tests(null=0.0, alternative="two_sided", robust=True)
fit.covariate_tests(df, "lr")                      # or "score": refits on the training data
fit.provider_tests(df, method="poibin_exact", reference="median")  # flags and limits per facility
fit.standardized_measures(df)["indirect"]          # O/E ratios and rates; also "direct"
fit.test_standardized(df, "direct_rate")           # tests on a measure (logit scale by default)
fit.predict(new_rows)                              # linear_predictor and probability
fit.save("/path/fit"); LogisticFixedEffectModel.load(spark, "/path/fit")
```

Its arguments follow `LogisticSpec` and `LogisticOptions` (docs/spec/logistic/).

## Three-stage SRR models

```python
from pprof_spark import ThreeStage, ThreeStageModel

fit = ThreeStage("readmitted", ["age", "comorbidity"], "facility", "hospital", row_id="id",
                 stage3={"nNodes": 20}).fit(df)
fit.sigma, fit.intercept, fit.stage1_coefficients, fit.excluded, fit.summary
fit.providers()                                    # gamma, stage 2's BLUP, stage 3's start, held_at_bound
fit.clusters()                                     # posterior mean and variance per hospital
fit.test(method="exact", reference="median")       # or poibin_exact, resampling (n_resample, seed)
fit.standardized_measures()["indirect"]            # O/E ratios and rates; also "direct"
fit.confidence_intervals()                         # SM: indirect_rate, indirect_ratio; option="gamma"
s = fit.sigma_sensitivity(level=0.95)              # s.lower, s.estimate, s.upper, s.flags, s.tests
fit.fitted()                                       # the prepared records with stage 3's probability
fit.save("/path/fit"); ThreeStageModel.load(spark, "/path/fit", df)
```

`ThreeStage`'s arguments are the job runner's `columns` and `fit` objects
([three-stage-job.md](three-stage-job.md)): `cutoff`, and `stage1`, `stage2` and `stage3` as dictionaries
of their keys; options left out take the engine's defaults. The fit keeps the prepared training records,
so its methods take no data. A saved fit does not hold them: `load` re-attaches it to the training data
and refuses data whose stage 1 fingerprint differs (docs/spec/logistic/three-stage-pipeline.md §12).

## Linear fixed-effect models

```python
from pprof_spark import LinearFixedEffect, LinearFixedEffectModel

fit = LinearFixedEffect("length_of_stay", ["age", "comorbidity"], "facility", row_id="id").fit(df)
fit.coefficients, fit.sigma, fit.r2, fit.aic, fit.bic, fit.degrees_of_freedom
fit.providers()                                      # records, gamma, variance, se
fit.summary(level=0.95, null=0.0)                    # t tests and intervals for the covariates
fit.test(reference="median")                         # provider tests: z_raw, p_value, flag, ci_lower, ci_upper
fit.confidence_intervals(option="gamma")["gamma_ci"] # effect intervals, from the fit alone
fit.standardized_measures(df)["indirect"]            # differences, with the training data (API-3)
fit.confidence_intervals(df, kinds=("indirect", "direct"))  # their intervals: indirect_ci, direct_ci
fit.predict(df), fit.score(df)
fit.save("/path/fit"); LinearFixedEffectModel.load(spark, "/path/fit")
```

`LinearFixedEffect`'s options are the job runner's `fit` object ([linear-job.md](linear-job.md)): `variance_option`
(`complete` or `simplified`) and `blocks`. The method names follow pprof_py's `LinearFixedEffectModel`
(docs/spec/linear/job-and-python.md §5).

## Linear random-effect models

```python
from pprof_spark import LinearRandomEffect

fit = LinearRandomEffect("length_of_stay", ["age", "comorbidity"], "facility", weights="w", row_id="id").fit(df)
fit.coefficients, fit.covariance, fit.sigma, fit.theta, fit.sigma_alpha, fit.criterion  # REML by default
fit.deviance, fit.loglik, fit.aic, fit.bic, fit.search      # the fit statistics and the search's record
fit.providers()                                  # records, weight, blup, its conditional variance and se (X-037)
fit.get_random_effects(), fit.get_sigma(), fit.get_random_effect_sd(), fit.standard_errors()
fit.predict(df), fit.predict(df, use_re=True)    # the fixed part; with the BLUPs (0 for providers not in the fit)
fit.fitted_values(df)                            # xbeta, fitted and residual on the training rows
fit.summary(), fit.fixed_effects_table(), fit.conf_int()   # the covariates (spec 4a-2 §2)
fit.test(), fit.test(reference="median")           # z tests on the BLUPs; reference 0 by default
fit.confidence_intervals(option="alpha")           # BLUP intervals, two-sided
fit.confidence_intervals(df), fit.standardized_measures(df)  # SM intervals and measures: the training data
fit.save("/fits/los-2026-10"); LinearRandomEffectModel.load(spark, "/fits/los-2026-10")  # bit for bit
```

`LinearRandomEffect` takes pprof_py's `LinearRandomEffectModel` arguments
(docs/spec/linear/random-effect-estimation.md §10): the `fit` roles as column names (`offset`, `weights`,
`include_intercept`), and `reml`, `theta_upper` and `max_iter_outer`, which caps the search's evaluations.
`tol_outer`, `max_iter_inner` and `optimizer` are accepted but not used, with a warning when they differ from pprof_py's
defaults; `cluster_vars` waits for part 4d (OI-93). As in pprof_py, `predict` adds an offset only when `offset_var`
names it. Rows with a missing value fail with counts per column (X-038) rather than being dropped. The measures and SM intervals read
the training data and check it against the fit (API-3). A saved fit also loads in Scala (`LinearREFitIO.load`), and
`LinearREJob` (docs/guide/linear-re-job.md) writes one with its other outputs.

## Logistic random-effect models

```python
from pprof_spark import LogisticRandomEffect

fit = LogisticRandomEffect("readmitted", ["age", "comorbidity"], "facility", row_id="id").fit(df)
fit.coefficients, fit.covariance, fit.sigma         # β̂ with standard errors, Var(β̂), the provider SD
fit.deviance, fit.loglik, fit.aic, fit.bic         # the Laplace deviance (−2ℓ) and the criteria, with p + 1 parameters
fit.providers()                                    # records, weight, blup, variance, se per provider
fit.predict(df, use_re=True)                       # probabilities with the BLUPs; type="link" for the linear predictor
fit.fitted_values(df)                              # μ̂, residuals, Pearson and deviance residuals
fit.summary(), fit.test()                          # Wald z for β; Wald tests of the providers, reference 0
fit.test(df, test_method="poibin_exact")            # exact tests on the training data; also "resampling"
fit.calculate_confidence_intervals(df, stdz=["indirect", "direct"])   # SM intervals; option="alpha" needs no data
fit.calculate_standardized_measures(df), fit.profile_sigma(df)          # ratios and rates; σ's profile interval
fit.save("/fits/readmissions-2026-10"); LogisticRandomEffectModel.load(spark, "/fits/readmissions-2026-10")
```

`LogisticRandomEffect` takes pprof_py's `LogisticRandomEffectModel` arguments
(docs/spec/logistic/random-effect-estimation.md). `stage2=True`, the default, fits the Laplace approximation
(lme4's nAGQ = 1); `stage2=False` fits nAGQ = 0. `sigma_upper` bounds σ, and `max_iter_outer` caps the quasi-Newton.
`max_iter_pirls`, `tol_pirls`, `tol_outer` and the two optimizer names are accepted but not used, because pprof_spark's
searches run to their own precision; a value other than pprof_py's default gives a warning. Predictions with
`use_re` add the BLUPs under integer provider keys as well, which pprof_py does not (X-044).

A fractional outcome, such as 3/8, counts as one observation with a Bernoulli-like likelihood, as in pprof_py. That
carries less information than eight trials would; spec §14.3 compares the two. A fit runs one Spark job per
evaluation of its criterion, about 200 in all (OI-99). The methods that read rows take the training data and check it against the fit (API-3). A saved fit also loads in
Scala (`LogisticREFitIO.load`), and `LogisticREJob` (docs/guide/logistic-re-job.md) writes one with its other outputs.

## Large data: strata, blocks and row identifiers

These options were added in round 83 (D-49).

- **One stratum is one task.** A Cox stratum is never split across tasks, so an unstratified fit holds
  every row in one task. `CoxPH(..., max_stratum_bytes=...)` caps a stratum's estimated arrays, 8 (p + 4)
  bytes a row, at 4 GiB by default: 10 million rows with 20 features (about 1.9 GB) pass; with 60
  features (about 5.1 GB) they do not, and the error names the option. Raise it when the executors have the memory;
  `max_stratum_rows`, unlimited by default, can cap rows as well.
- **Blocks and the driver budget.** Every pass collects one partial result per block to the driver, about
  8 (p(p+1)/2 + p + 2) bytes each, within `driverBudgetBytes` (64 MiB). When the default 4 MiB blocks would
  need more, the blocks now grow, doubling up to 256 MiB, until the partials fit; a fit that fitted before
  is unchanged. The growth is recorded in the fit's layout. `LogisticFixedEffect(..., blocks={...})` now
  takes the same block options as `CoxPH` and `LinearFixedEffect`: `targetBlockBytes`,
  `maxGroupsOnDriver`, `driverBudgetBytes`, `storageLevel`.
- **Row identifiers for counting-process data.** Residuals need a unique row identifier, and every later call
  checks that its data are the fit's through a fingerprint that includes it. Identifiers from
  `monotonically_increasing_id()` change whenever Spark recomputes the DataFrame, so later calls refuse the
  data. Give `CoxPH` the natural key instead: `row_id=["patient_id", "start"]` (round 87). It is hashed to a
  64-bit identifier, so nothing needs to be written out first, and residuals come back with those columns.
  Repeated keys are reported by name. The logistic and linear models take one column; for them,
  `df.withColumn("row_id", F.xxhash64("patient_id", "start"))` does the same.

## Cox baselines: reference points

`model.baseline(df)` is the baseline hazard at x = 0 and offset 0, the one every prediction here uses.
Other tools report it elsewhere, so `baseline` takes `reference=`:

- `"raw"` (default): x = 0, offset 0.
- `"mean_offset"`: at the weighted mean offset; pprof_py's `baseline_hazard_` and R's
  `basehaz(fit, centered = FALSE)`.
- `"means"`: also at R's covariate means; R's `survfit(fit)` default curve and `basehaz(fit)`.

R does not centre covariates whose values all lie in {−1, 0, 1}; it uses case-weighted means for
right-censored data and unweighted means for data with entry times. `model.reference_point(df)` returns
these means and the mean offset; R's `predict(fit, type = "lp")` equals this package's linear predictor minus
the sum of each coefficient times its mean. Expected values and residuals do not depend on the reference
point.

## Logistic tests on standardized measures: null values, null models, intervals and bounds

`model.test_standardized(df, measure, ...)` takes pprof_py's options (standardization specification §2a):

- `null_value`: `"reference"` (default; the measure at the reference provider effect), `"mean"` or `"median"`
  of the providers' estimates (the mean of the direct rates, not the rate at the mean effect), a number, or a
  callable given the estimates.
- `null_model`: `None` for N(0, 1), or `FixedNull(mean, sd)` (`from pprof_spark import FixedNull`; pprof_py's
  own `FixedNull` objects work too). z is calibrated to (z − mean)/sd before p-values, flags and intervals:
  `FixedNull(sd=1.5)` is a null whose z-scores are 1.5 times as spread.
- `interval`: `"inversion"` (default) or `"scale_only"`. `bounds`: `"auto"` (default), a `(lower, upper)` pair,
  or `None` to leave intervals unclipped.

Empirical nulls are not available yet. Indirect measures use `indirect_variance`; `variance="robust"` is refused
for them, as in pprof_py.

## Logistic models: providers added after fitting

A common workflow fits without the providers whose outcomes are all 0 or all 1, then adds them back with
fixed effects. `add_providers` does this, as pprof_py's does:

```python
model = LogisticFixedEffect("y", features, "provider", row_id="id").fit(df_without_extremes)
model.add_providers(zero_event_ids + all_event_ids,
                    [-17.0] * len(zero_event_ids) + [17.0] * len(all_event_ids),
                    se_gamma=0.01, group_sizes=sizes)
model.test_standardized(df_without_extremes, "direct_rate", null_value="mean")
```

The added providers enter the reference effect, the direct measures, the tests on standardized measures (and so
a mean null value), the provider tests, the provider table (`added` is true) and predictions. Their indirect
measures are missing, since they have no rows in the data. A provider already in the fit is replaced instead.
The model changes in place and is returned, and saved models keep the additions (logistic specification §8a).
