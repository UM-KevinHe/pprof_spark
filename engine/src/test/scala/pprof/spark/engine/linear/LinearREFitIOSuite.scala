package pprof.spark.engine.linear

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.DataFrame
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LinearREFitIO` (docs/spec/linear/random-effect-job.md §2): a bit-for-bit round trip whose loaded fit gives the
  * same inference, never overwriting, other kinds and versions refused, a provider table of the wrong length refused.
  */
class LinearREFitIOSuite extends SparkSuite {

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear-re/$name/input.csv").toString)

  private def setup(name: String): LinearRESpec = {
    val c = Fixtures.json(s"linear-re/$name/case.json")
    LinearRESpec(
      "y",
      c.get("features").elements().asScala.map(_.asText()).toSeq,
      "provider",
      offset = if (c.has("offset")) Some("o") else None,
      weights = if (c.has("weights")) Some("w") else None,
      rowId = Some("id")
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

  private def same(a: LinearREFit, b: LinearREFit): Unit = {
    assertEquals(a.coefficients.map(_.feature), b.coefficients.map(_.feature))
    assertEquals(bits(a.estimates), bits(b.estimates))
    assertEquals(bits(a.standardErrors), bits(b.standardErrors))
    assertEquals(bits(a.covariance), bits(b.covariance))
    assertEquals(
      bits(
        Seq(
          a.sigma,
          a.theta,
          a.sigmaAlpha,
          a.deviance,
          a.loglik,
          a.aic,
          a.bic,
          a.pwrss,
          a.logDetH,
          a.ussq
        )
      ),
      bits(
        Seq(
          b.sigma,
          b.theta,
          b.sigmaAlpha,
          b.deviance,
          b.loglik,
          b.aic,
          b.bic,
          b.pwrss,
          b.logDetH,
          b.ussq
        )
      )
    )
    assertEquals(
      (a.criterion, a.rows, a.providerCount, a.fingerprint, a.status),
      (b.criterion, b.rows, b.providerCount, b.fingerprint, b.status)
    )
    assertEquals(
      bits(Seq(a.search.lower, a.search.upper, a.search.slope)),
      bits(Seq(b.search.lower, b.search.upper, b.search.slope))
    )
    assertEquals(
      (a.search.evaluations, a.search.boundary, a.search.path),
      (b.search.evaluations, b.search.boundary, b.search.path)
    )
    assertEquals(
      (a.warnings, a.spec, a.software, a.layout),
      (b.warnings, b.spec, b.software, b.layout)
    )
    assertEquals(
      (a.options.reml, a.options.includeIntercept, a.options.maxEvaluations, a.options.blocks),
      (b.options.reml, b.options.includeIntercept, b.options.maxEvaluations, b.options.blocks)
    )
    assertEquals(bits(Seq(a.options.thetaUpper)), bits(Seq(b.options.thetaUpper)))
    assertEquals(table(a.providers, a.spec.provider), table(b.providers, b.spec.provider))
  }

  for ((name, reml) <- Seq("lre-offset" -> true, "lre-weights" -> false)) {
    test(s"$name: a fit saves and loads bit for bit, and the loaded fit gives the same inference") {
      val df = input(name)
      val fit = LinearRE.fit(df, setup(name), LinearREOptions(reml = reml, thetaUpper = 50.0))
      val path = Files.createTempDirectory("pprof-linear-re-fit").resolve("fit").toString
      LinearREFitIO.save(spark, fit, path)
      val loaded = LinearREFitIO.load(spark, path)
      same(fit, loaded)
      val key = fit.spec.provider
      assertEquals(
        table(LinearREInference.test(loaded), key),
        table(LinearREInference.test(fit), key)
      )
      assertEquals(
        table(LinearREInference.alphaIntervals(loaded), key),
        table(LinearREInference.alphaIntervals(fit), key)
      )
      val (a, b) = (LinearREInference.measures(df, fit), LinearREInference.measures(df, loaded))
      Seq("indirect", "direct").foreach(kind =>
        assertEquals(table(b(kind), key), table(a(kind), key))
      )
      assertEquals(
        table(LinearRE.predict(df, loaded, useRe = true), "id"),
        table(LinearRE.predict(df, fit, useRe = true), "id")
      )
    }
  }

  test(
    "nothing is overwritten; other kinds, unknown versions and a short provider table are refused"
  ) {
    val df = input("lre-base")
    val fit = LinearRE.fit(df, setup("lre-base"))
    val dir = Files.createTempDirectory("pprof-linear-re-fit")
    val path = dir.resolve("fit").toString
    LinearREFitIO.save(spark, fit, path)
    intercept[Exception](LinearREFitIO.save(spark, fit, path))
    val fixed = LinearFE.fit(df, LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id")))
    val other = dir.resolve("fixed").toString
    LinearFitIO.save(spark, fixed, other)
    val kind = intercept[IllegalArgumentException](LinearREFitIO.load(spark, other)).getMessage
    assert(kind.contains("LinearFit") && kind.contains(LinearREFitIO.Kind), kind)
    val newer = dir.resolve("newer").toString
    spark.read
      .text(s"$path/metadata")
      .selectExpr("replace(value, '\"formatVersion\":1', '\"formatVersion\":2') as value")
      .write
      .text(s"$newer/metadata")
    spark.read.parquet(s"$path/providers").write.parquet(s"$newer/providers")
    val version = intercept[IllegalArgumentException](LinearREFitIO.load(spark, newer)).getMessage
    assert(version.contains("format version 2"), version)
    val short = dir.resolve("short").toString
    spark.read.text(s"$path/metadata").write.text(s"$short/metadata")
    spark.read.parquet(s"$path/providers").limit(5).write.parquet(s"$short/providers")
    val count = intercept[IllegalArgumentException](LinearREFitIO.load(spark, short)).getMessage
    assert(count.contains("holds 5 providers"), count)
  }
}
