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

## Large data: strata, blocks and row identifiers

These options were added after production validation on about 5 million rows (round 83, D-49).

- **One stratum is one task.** A Cox stratum is never split across tasks, so an unstratified fit holds
  every row in one task. `CoxPH(..., max_stratum_bytes=...)` caps a stratum's estimated arrays, 8 (p + 4)
  bytes a row, at 4 GiB by default: 4.8 million rows with 57 features (about 2.3 GB) pass; with 142
  features they do not, and the error names the option. Raise it when the executors have the memory;
  `max_stratum_rows`, unlimited by default, can cap rows as well.
- **Blocks and the driver budget.** Every pass collects one partial result per block to the driver, about
  8 (p(p+1)/2 + p + 2) bytes each, within `driverBudgetBytes` (64 MiB). When the default 4 MiB blocks would
  need more, the blocks now grow, doubling up to 256 MiB, until the partials fit; a fit that fitted before
  is unchanged. The growth is recorded in the fit's layout. `LogisticFixedEffect(..., blocks={...})` now
  takes the same block options as `CoxPH` and `LinearFixedEffect`: `targetBlockBytes`,
  `maxGroupsOnDriver`, `driverBudgetBytes`, `storageLevel`.
- **Row identifiers for counting-process data.** Residuals need a unique `row_id`, and every later call
  checks that its data are the fit's through a fingerprint that includes the identifiers. Identifiers from
  `monotonically_increasing_id()` change whenever Spark recomputes the DataFrame, so later calls refuse
  the data. Derive them from natural keys instead, for example
  `df.withColumn("row_id", F.xxhash64("patient_id", "start"))`: deterministic, so nothing needs to be
  written out first. Validation reports any duplicate (two of 5 million rows collide with probability
  about 7 × 10⁻⁷).

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
  `FixedNull(sd=1.81)` is a null whose z-scores are 1.81 times as spread.
- `interval`: `"inversion"` (default) or `"scale_only"`. `bounds`: `"auto"` (default), a `(lower, upper)` pair,
  or `None` to leave intervals unclipped.

Empirical nulls are not available yet. Indirect measures use `indirect_variance`; `variance="robust"` is refused
for them, as in pprof_py.
