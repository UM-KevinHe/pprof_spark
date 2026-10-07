# pprof-spark (Python)

Python access to [pprof_spark](https://github.com/UM-KevinHe/pprof_spark) through py4j
(ADR-0009). Every statistic is computed by the pprof_spark JARs in the Spark driver's JVM; this
package only passes arguments and results. It needs a Classic PySpark session with the JARs on the
classpath (`spark.jars`); Spark Connect clients have no JVM gateway and use the job runner instead.
See `docs/guide/python.md` in the repository.
