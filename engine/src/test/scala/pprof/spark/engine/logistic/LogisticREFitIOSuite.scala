package pprof.spark.engine.logistic

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.DataFrame
import pprof.spark.engine.linear.{LinearRE, LinearREFitIO, LinearRESpec}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LogisticREFitIO` (docs/spec/logistic/random-effect-job.md §2): a bit-for-bit round trip whose loaded fit gives the
  * same inference, never overwriting, other kinds and versions refused, a provider table of the wrong length refused.
  */
class LogisticREFitIOSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"logistic-re/$name/input.csv").toString)

  private def setup(name: String): LogisticRESpec = {
    val c = Fixtures.json(s"logistic-re/$name/case.json")
    LogisticRESpec(
      "y",
      c.get("features").elements().asScala.map(_.asText()).toSeq,
      "provider",
      if (c.has("offset")) Some("o") else None,
      Some("id")
    )
  }

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  private def table(df: DataFrame, order: String): Seq[String] =
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

  private def same(a: LogisticREFit, b: LogisticREFit): Unit = {
    assertEquals(a.coefficients.map(_.feature), b.coefficients.map(_.feature))
    assertEquals(bits(a.estimates), bits(b.estimates))
    assertEquals(bits(a.standardErrors), bits(b.standardErrors))
    assertEquals(bits(a.covariance), bits(b.covariance))
    assertEquals(
      bits(Seq(a.sigma, a.deviance, a.loglik, a.aic, a.bic, a.pwrss, a.logDetH, a.ussq)),
      bits(Seq(b.sigma, b.deviance, b.loglik, b.aic, b.bic, b.pwrss, b.logDetH, b.ussq))
    )
    assertEquals(
      (a.criterion, a.rows, a.providerCount, a.fingerprint, a.status),
      (b.criterion, b.rows, b.providerCount, b.fingerprint, b.status)
    )
    assertEquals(
      (
        a.search.stage1Evaluations,
        a.search.stage2Evaluations,
        a.search.converged,
        a.search.boundary
      ),
      (
        b.search.stage1Evaluations,
        b.search.stage2Evaluations,
        b.search.converged,
        b.search.boundary
      )
    )
    assertEquals(bits(Seq(a.search.projectedGradient)), bits(Seq(b.search.projectedGradient)))
    assertEquals(
      (a.warnings, a.spec, a.software, a.layout),
      (b.warnings, b.spec, b.software, b.layout)
    )
    assertEquals(
      (a.options.nagq1, a.options.includeIntercept, a.options.maxIterations, a.options.blocks),
      (b.options.nagq1, b.options.includeIntercept, b.options.maxIterations, b.options.blocks)
    )
    assertEquals(bits(Seq(a.options.sigmaUpper)), bits(Seq(b.options.sigmaUpper)))
    assertEquals(table(a.providers, a.spec.provider), table(b.providers, b.spec.provider))
  }

  for ((name, nagq1) <- Seq("lgre-offset" -> true, "lgre-text" -> false)) {
    test(s"$name: a fit saves and loads bit for bit, and the loaded fit gives the same inference") {
      val df = input(name)
      val fit = LogisticRE.fit(df, setup(name), LogisticREOptions(nagq1 = nagq1, sigmaUpper = 50.0))
      val path = Files.createTempDirectory("pprof-logistic-re-fit").resolve("fit").toString
      LogisticREFitIO.save(spark, fit, path)
      val loaded = LogisticREFitIO.load(spark, path)
      same(fit, loaded)
      val key = fit.spec.provider
      assertEquals(
        table(LogisticREInference.test(df, loaded), key),
        table(LogisticREInference.test(df, fit), key)
      )
      assertEquals(
        table(LogisticREInference.test(df, loaded, "poibin_exact"), key),
        table(LogisticREInference.test(df, fit, "poibin_exact"), key)
      )
      val (a, b) = (
        LogisticREInference.measures(df, fit, Seq("indirect", "direct")),
        LogisticREInference.measures(df, loaded, Seq("indirect", "direct"))
      )
      Seq("indirect", "direct").foreach(kind =>
        assertEquals(table(b(kind), key), table(a(kind), key))
      )
      assertEquals(
        table(LogisticRE.predict(df, loaded, useRe = true), "id"),
        table(LogisticRE.predict(df, fit, useRe = true), "id")
      )
    }
  }

  test(
    "nothing is overwritten; other kinds, unknown versions and a short provider table are refused"
  ) {
    val df = input("lgre-text")
    val fit =
      LogisticRE.fitAt(df, setup("lgre-text"), LogisticREOptions(), 0.5, Seq(-1.0, 0.7, -0.4))
    val dir = Files.createTempDirectory("pprof-logistic-re-fit")
    val path = dir.resolve("fit").toString
    LogisticREFitIO.save(spark, fit, path)
    intercept[Exception](LogisticREFitIO.save(spark, fit, path))
    val linear =
      LinearRE.fit(df, LinearRESpec("y", Seq("x1", "x2"), "provider", rowId = Some("id")))
    val other = dir.resolve("linear").toString
    LinearREFitIO.save(spark, linear, other)
    val kind = intercept[IllegalArgumentException](LogisticREFitIO.load(spark, other)).getMessage
    assert(kind.contains("LinearREFit") && kind.contains(LogisticREFitIO.Kind), kind)
    val newer = dir.resolve("newer").toString
    spark.read
      .text(s"$path/metadata")
      .selectExpr("replace(value, '\"formatVersion\":1', '\"formatVersion\":2') as value")
      .write
      .text(s"$newer/metadata")
    spark.read.parquet(s"$path/providers").write.parquet(s"$newer/providers")
    val version = intercept[IllegalArgumentException](LogisticREFitIO.load(spark, newer)).getMessage
    assert(version.contains("format version 2"), version)
    val short = dir.resolve("short").toString
    spark.read.text(s"$path/metadata").write.text(s"$short/metadata")
    spark.read.parquet(s"$path/providers").limit(5).write.parquet(s"$short/providers")
    val count = intercept[IllegalArgumentException](LogisticREFitIO.load(spark, short)).getMessage
    assert(count.contains("holds 5 providers"), count)
  }
}
