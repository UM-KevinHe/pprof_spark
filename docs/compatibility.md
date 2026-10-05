# Compatibility matrix (PROJECT_CONTEXT §5.6)

| Release | Spark (compile, `provided`) | Databricks Runtime | Scala | JDK (bytecode / tested) | Tiers tested |
|---|---|---|---|---|---|
| unreleased, 0.1.0-SNAPSHOT | 4.1.0 (`spark-sql-api`; `spark-sql` and `spark-mllib` where needed) | 18 LTS (target, not yet run) | 2.13.16 | 17 / 17 and 21 in CI | None yet |

Notes:
- Open-source Spark 4.1.0 to 4.1.3 are built with Scala 2.13.17 and 4.2.0 with 2.13.18; DBR 18
  ships 2.13.16. Hence PLAT-3 and the linkage check.
- DBR 18 receives dated updates under one version number, so record the runtime build
  identifier with any evidence gathered on Databricks (OI-06).
- Classic test classpaths exclude spark-connect-shims, as Spark's own Classic modules do (OI-21).
- Toolchain: sbt 1.12.15, scalafmt 3.11.5, munit 1.2.0, sbt-buildinfo 0.13.2.
