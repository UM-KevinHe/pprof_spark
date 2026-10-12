# Tutorial probe

[Tutorial home](../README.md)

[LogisticProbe.scala](LogisticProbe.scala) produced the numbers quoted in the logistic tutorial. It is
documentation material: sbt does not compile it, CI does not run it, and no published JAR contains it.

- Part A builds the `lfe-base` block with the real `LogisticBlockBuilder` and runs the real kernels and
  `Serbin.fit` in memory (`InMemorySerbinPasses`): function-level values at pprof_py's two points, the
  first five iterates, default and tight fits, and the variances, each compared with pprof_py's and R's
  stored results.
- Part B starts a local Spark session configured like the test harness, fits `lfe-base` with
  `LogisticFE.fit`, runs the exact provider tests, refits with four blocks, checks bitwise invariance at
  `spark.sql.shuffle.partitions` 1, 4 and 7 on reversed, repartitioned input, and prints every Spark job
  with its stages, tasks and shuffle bytes.

It declares package `pprof.spark.engine.logistic`, as the engine's own tests do.

## Running

[run.sh](run.sh) compiles `numerics` and `engine` with plain `scalac`, then compiles and runs the probe
from the repository root. It needs Java 17 or later and a directory with the Spark 4.1.0 jars and
`scala-compiler`, `scala-library` and `scala-reflect`, such as `deps/jars` in the PySpark 4.1.0 source
distribution:

```
pip download pyspark==4.1.0 --no-deps -d /tmp/pyspark
tar -xzf /tmp/pyspark/pyspark-4.1.0.tar.gz -C /tmp/pyspark
JARS=/tmp/pyspark/pyspark-4.1.0/deps/jars docs/tutorials/logistic/probe/run.sh
```

The output goes to `$WORK/out/logistic-probe.txt` (`WORK` defaults to `/tmp/pprof-logistic-probe`). As
for the Cox probe, the script stands in for sbt-buildinfo's `BuildInfo` and uses the compiler found in
`JARS` (2.13.17), without `-release 17`; the fixture directory defaults to `fixtures/logistic` and can be
changed with `-Dpprof.fixtures=…`. Every number should be identical between runs; job identifiers within
a phase can interleave differently.

## The run quoted in the tutorial

Commit `3e70739`; scalac 2.13.17 from PySpark 4.1.0's jars; OpenJDK 21.0.10; Spark 4.1.0 in local mode
(`local[4]`, 4 shuffle partitions, ANSI on, adaptive query execution on). Sandbox evidence, not CI
evidence (NN-8).
