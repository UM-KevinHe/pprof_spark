package pprof.spark.engine.logistic

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LogisticFERandomClusterFitIO` (docs/spec/logistic/fe-random-cluster-job.md §3, §7): save, load and attach bit for
  * bit, with the same inference; the refusals.
  */
class LogisticFERandomClusterFitIOSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"fe-random-cluster/$name/input.csv").toString)

  private def setup(name: String, options: LogisticFERandomClusterOptions): (
      LogisticFERandomClusterSpec,
      LogisticFERandomClusterSource,
      LogisticFERandomClusterOptions
  ) = {
    val c = Fixtures.json(s"fe-random-cluster/$name/case.json")
    val values = Fixtures.json(s"fe-random-cluster/$name/pprof_py.json").get("given")
    val spec = LogisticFERandomClusterSpec(
      "y",
      Seq("x1", "x2"),
      "provider",
      "cluster",
      if (c.has("adjusted")) Some("obs") else None,
      Some("id")
    )
    val source = LogisticFERandomClusterSource.Given(
      Fixtures.doubles(values.get("beta")).toSeq,
      Fixtures.doubles(values.get("sigma")).head,
      Fixtures.doubles(values.get("start")).toSeq
    )
    (spec, source, options)
  }

  private def bits(v: Seq[Double]): Seq[Long] = v.map(java.lang.Double.doubleToRawLongBits)

  private def rows(df: DataFrame, order: String): Seq[String] =
    df.orderBy(order)
      .collect()
      .map(
        _.toSeq
          .map {
            case d: Double => java.lang.Long.toHexString(java.lang.Double.doubleToRawLongBits(d))
            case other     => String.valueOf(other)
          }
          .mkString("|")
      )
      .toSeq

  for (
    (name, options) <- Seq(
      "frc-obs" -> LogisticFERandomClusterOptions(),
      "frc-text" -> LogisticFERandomClusterOptions(
        estimator = "he2013",
        convergenceCriterion = "relative"
      )
    )
  ) {
    test(
      s"$name (${options.estimator}): saved, loaded and re-attached bit for bit, with the same inference"
    ) {
      val (spec, source, opts) = setup(name, options)
      val df = input(name)
      val fit = LogisticFERandomCluster.fit(df, spec, source, opts)
      val path = Files.createTempDirectory("pprof-frc-fit").resolve("fit").toString
      LogisticFERandomClusterFitIO.save(spark, fit, path)
      val saved = LogisticFERandomClusterFitIO.load(spark, path)
      val (a, b) = (fit.stage3, saved.stage3)
      assertEquals(b.providers, a.providers)
      assertEquals(b.clusters, a.clusters)
      Seq(a.gamma -> b.gamma, a.alphaMean -> b.alphaMean, a.alphaVar -> b.alphaVar).foreach {
        case (x, y) =>
          assertEquals(bits(y.toSeq), bits(x.toSeq))
      }
      assertEquals(
        bits(Seq(b.sigma, b.loglik, b.criterion)),
        bits(Seq(a.sigma, a.loglik, a.criterion))
      )
      assertEquals(
        (b.converged, b.stalled, b.iterations, b.held.toSeq, b.cells, b.bins),
        (a.converged, a.stalled, a.iterations, a.held.toSeq, a.cells, a.bins)
      )
      assertEquals(
        (saved.spec, saved.options, saved.warnings, saved.rows, saved.fingerprint, saved.software),
        (fit.spec, fit.options, fit.warnings, fit.rows, fit.fingerprint, fit.software)
      )
      assertEquals(bits(saved.beta), bits(fit.beta))
      val attached = LogisticFERandomClusterFitIO.attach(df, saved)
      val key = spec.provider
      assertEquals(
        rows(LogisticFERandomCluster.test(attached), key),
        rows(LogisticFERandomCluster.test(fit), key)
      )
      assertEquals(
        rows(LogisticFERandomCluster.test(attached, "poibin_exact"), key),
        rows(LogisticFERandomCluster.test(fit, "poibin_exact"), key)
      )
      assertEquals(
        rows(LogisticFERandomCluster.intervals(attached, "gamma")("gamma_ci"), key),
        rows(LogisticFERandomCluster.intervals(fit, "gamma")("gamma_ci"), key)
      )
      val (ma, mb) = (
        LogisticFERandomCluster.measures(fit, Seq("indirect", "direct")),
        LogisticFERandomCluster.measures(attached, Seq("indirect", "direct"))
      )
      Seq("indirect", "direct").foreach(k => assertEquals(rows(mb(k), key), rows(ma(k), key)))
      assertEquals(
        rows(LogisticFERandomCluster.fitted(attached), "id"),
        rows(LogisticFERandomCluster.fitted(fit), "id")
      )
    }
  }

  test(
    "refusals: overwriting, another kind, an unknown version, short tables, and other or changed data"
  ) {
    val (spec, source, options) = setup("frc-base", LogisticFERandomClusterOptions())
    val df = input("frc-base")
    val fit = LogisticFERandomCluster.fit(df, spec, source, options)
    val dir = Files.createTempDirectory("pprof-frc-fit")
    val path = dir.resolve("fit").toString
    LogisticFERandomClusterFitIO.save(spark, fit, path)
    intercept[Exception](LogisticFERandomClusterFitIO.save(spark, fit, path))
    def variant(name: String, from: String, to: String, providers: Int = -1): String = {
      val target = dir.resolve(name).toString
      spark.read
        .text(s"$path/metadata")
        .selectExpr(s"replace(value, '$from', '$to') as value")
        .write
        .text(s"$target/metadata")
      val p = spark.read.parquet(s"$path/providers")
      (if (providers >= 0) p.limit(providers) else p).write.parquet(s"$target/providers")
      spark.read.parquet(s"$path/clusters").write.parquet(s"$target/clusters")
      target
    }
    val kind = intercept[IllegalArgumentException](
      LogisticFERandomClusterFitIO.load(
        spark,
        variant("kind", "LogisticFERandomClusterFit", "OtherFit")
      )
    ).getMessage
    assert(kind.contains("OtherFit"), kind)
    val version = intercept[IllegalArgumentException](
      LogisticFERandomClusterFitIO.load(
        spark,
        variant("version", "\"formatVersion\":1", "\"formatVersion\":2")
      )
    ).getMessage
    assert(version.contains("format version 2"), version)
    val short = intercept[IllegalArgumentException](
      LogisticFERandomClusterFitIO.load(spark, variant("short", "\"kind\"", "\"kind\"", 5))
    ).getMessage
    assert(short.contains("5 providers"), short)
    val saved = LogisticFERandomClusterFitIO.load(spark, path)
    val other = intercept[IllegalArgumentException](
      LogisticFERandomClusterFitIO.attach(input("frc-text").drop("obs"), saved)
    ).getMessage
    assert(other.contains("not the fit's training data") || other.contains("differ"), other)
    val changed =
      df.withColumn("x1", when(col("id") === 7, col("x1") + lit(1.0 / 64)).otherwise(col("x1")))
    val message = intercept[IllegalArgumentException](
      LogisticFERandomClusterFitIO.attach(changed, saved)
    ).getMessage
    assert(message.contains("not the fit's training data"), message)
  }
}
