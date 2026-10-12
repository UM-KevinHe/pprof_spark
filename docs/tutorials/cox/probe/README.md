# Tutorial probe

[Tutorial home](../README.md)

Two small Scala programs produced the numbers quoted in the Cox tutorial. They are documentation
material: sbt does not compile them, CI does not run them, and no published JAR contains them.

- [TutorialProbe.scala](TutorialProbe.scala). Part A calls the real `CoxBlockBuilder`, `CoxKernel`,
  `Newton` and `Cholesky` on the tiny-ties fixture without Spark, and compares every quantity with
  R's and pprof_py's stored values. Part B starts a local Spark session configured like the test
  harness, fits tiny-ties and a three-stratum toy data set with `CoxPH.fit`, and prints the layout
  plan, the blocks, per-stratum and per-block partials, the physical plans, bitwise comparisons
  across shuffle-partition counts, and every Spark job with its stages, tasks and shuffle bytes.
- [EdgeProbe.scala](EdgeProbe.scala) runs the real kernel and optimizer on a monotone-likelihood data
  set, extreme offsets, badly scaled covariates, distant starting values and an iteration cap of 1.

Both declare package `pprof.spark.engine.cox`, so that they can call the package-private
`CoxPH.withWorkingSet` and `CoxPH.evaluateAt` as the engine's own tests do.

## Running

[run.sh](run.sh) compiles `numerics` and `engine` with plain `scalac`, then compiles and runs both
probes from the repository root. It needs Java 17 or later and a directory holding the Spark 4.1.0
jars together with `scala-compiler`, `scala-library` and `scala-reflect`; the `deps/jars` directory of
the PySpark 4.1.0 source distribution has all of them:

```
pip download pyspark==4.1.0 --no-deps -d /tmp/pyspark
tar -xzf /tmp/pyspark/pyspark-4.1.0.tar.gz -C /tmp/pyspark
JARS=/tmp/pyspark/pyspark-4.1.0/deps/jars docs/tutorials/cox/probe/run.sh
```

The outputs go to `$WORK/out/` (`WORK` defaults to `/tmp/pprof-cox-probe`). The script replaces
sbt-buildinfo's generated `BuildInfo` with fixed values and uses the Scala compiler found in `JARS`
(2.13.17 in PySpark 4.1.0), not the build's pinned 2.13.16, without `-release 17`: the probes check
arithmetic and Spark behavior, not the build. The fixture directory defaults to `fixtures/cox` and
can be changed with `-Dpprof.fixtures=…`.

Physical plans print lambda class names and plan identifiers that change from run to run; every
number should be identical between runs and between machines. After a change declared
non-behavioral (NUM-2), it must stay identical in every digit.

## The run quoted in the tutorial

Commit `9385836` (round 69), whose Cox sources are identical at `efb6329` (round 75); scalac 2.13.17 from PySpark 4.1.0's jars; OpenJDK 21.0.10; Spark 4.1.0 in local
mode (`local[4]`, 4 shuffle partitions, ANSI on, adaptive query execution on). This is sandbox
evidence, not CI evidence (NN-8).
