# ADR-0009: Python access through py4j wrappers

- Status: Proposed
- Date: 2026-10-07
- Decisions: D-31 (py4j wrappers, approved), D-33 (this design, proposed)
- Spike: round 29, in the assistant's sandbox

## Context

D-31 chose py4j wrappers for Python access; Python code never reimplements statistics (§6.12).
py4j needs the driver JVM behind the Python process's gateway, so the wrappers serve PySpark Classic
sessions. The engine's Scala API uses `Seq`, `Option`, case classes and default arguments, which
py4j reaches only through Scala internals: the spike had to build `Seq` with `CollectionConverters`,
pass `Option.empty()`, and call the hidden default getter `LogisticFE$.MODULE$.fit$default$3()`.

## Options considered

| Option | Consequences |
|---|---|
| 1. Call the Scala API directly from Python | Works (spike), but every call builds Scala collections and options and names compiler-generated methods, which change with the Scala API |
| 2. A Java-friendly facade in the JVM (recommended) | One object with methods that take DataFrames, strings and numbers and return DataFrames, JSON summaries and opaque fit handles; options travel in the job runner's JSON format; Python holds handles as py4j objects, as `pyspark.ml` holds `_java_obj` |
| 3. Spark Connect ML registration | Would reach Connect clients, but needs the `ml` adapters and server-side registration (S-04, deferred) |
| 4. Reimplement in Python | Rejected by §6.12 |

## Decision (proposed, D-33)

- JVM side: `pprof.spark.app.python.PythonApi` in the `app` module, which already parses the JSON
  run specification. Methods take only Java types and `Dataset[Row]` and return DataFrames, JSON
  strings and opaque fit handles: for Cox, fit, baseline, prediction, residuals, measures, provider
  tests, save and load; for logistic models, each slice's methods as it lands.
- Python side: a pure-Python package `pprof_spark` in `python/` (`pyproject.toml`), with classes
  such as `CoxPH` and `LogisticFixedEffect` whose `fit(df, ...)` returns a fitted wrapper: its
  properties come from the JSON summary, its methods return `pyspark.sql.DataFrame`s. Errors need
  no mapping: PySpark already turns the JVM's `IllegalArgumentException` (and so pprof_spark's
  `InvalidInputException`) into `pyspark.errors.IllegalArgumentException` with the same message.
- Supported sessions: PySpark Classic (local, standalone, YARN, Kubernetes, and Databricks classic
  compute in dedicated access mode). Under Spark Connect, PySpark raises
  `JVM_ATTRIBUTE_NOT_SUPPORTED` for `spark._jvm`; the wrappers check first and point to the job
  runner. Databricks standard access mode is expected to refuse py4j calls into library classes;
  the maintainer checks it (OI-53).
- Packaging: one wheel per release, version-locked to the JAR, bundling it and exposing its path
  for `spark.jars`; on Databricks the JAR and the wheel are installed as libraries. At start the
  wrappers compare the JVM side's version (`BuildInfo`) with their own and fail on a mismatch.
- CI: a `python` job builds the JARs with sbt, installs PySpark 4.1.0 on Python 3.12 and runs the
  Python tests in local Classic mode; they compare wrapper results with the Scala library bit for
  bit on the fixtures (the same JVM code runs underneath).
- Order (approved by the maintainer): this ADR, then the Cox wrappers, then each logistic slice.

## Evidence

Round 29 spike: PySpark 4.1.0 (pip, `local[1]`, JDK 17) with a JAR of the `numerics` and `engine`
classes. Through py4j, `LogisticFE.fit` on lfe-base took 5 steps, converged, fitted 30 providers and
returned β within 5.6e-17 of pprof_py's default fit; `providerTable` came back as a PySpark
DataFrame (30 rows); a missing column surfaced as `pyspark.errors.IllegalArgumentException:
invalid input: column x2 is missing`. Spark Connect's refusal is read in PySpark 4.1.0's
`sql/connect/session.py`, not run.

## Consequences

Python users on Classic compute get the whole API without reimplemented statistics; Spark Connect
users keep the job runner. The facade is a second public surface to keep stable (API-2, and MiMa
from 1.0). Revisit Spark Connect ML (S-04) once the `ml` adapters exist.
