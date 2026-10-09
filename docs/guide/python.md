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
