package pprof.spark.engine.linear

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LinearFitIO`: a bit-for-bit round trip, never overwriting, other kinds and versions refused (spec 3a §8). */
class LinearFitIOSuite extends SparkSuite {

  private val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear/$name/input.csv").toString)

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  private def table(fit: LinearFit): Seq[String] =
    fit.providers
      .orderBy(fit.spec.provider)
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

  private def same(a: LinearFit, b: LinearFit): Unit = {
    assertEquals(bits(a.estimates), bits(b.estimates))
    assertEquals(bits(a.standardErrors), bits(b.standardErrors))
    assertEquals(bits(a.covariance), bits(b.covariance))
    assertEquals(
      bits(Seq(a.sigma, a.rss, a.tss, a.loglik, a.aic, a.bic, a.condition)),
      bits(Seq(b.sigma, b.rss, b.tss, b.loglik, b.aic, b.bic, b.condition))
    )
    assertEquals(
      (a.rows, a.providerCount, a.degreesOfFreedom, a.fingerprint),
      (b.rows, b.providerCount, b.degreesOfFreedom, b.fingerprint)
    )
    assertEquals(
      (a.warnings, a.spec, a.options, a.software, a.layout, a.status),
      (b.warnings, b.spec, b.options, b.software, b.layout, b.status)
    )
    assertEquals(table(a), table(b))
  }

  private def fresh(name: String): String =
    Files.createTempDirectory("linear-fit").resolve(name).toString

  for (name <- Seq("lin-base", "lin-text")) {
    test(s"$name: a saved fit loads back bit for bit, and predicts and scores the same") {
      val df = input(name)
      val fit = LinearFE.fit(df, spec)
      val path = fresh("fit")
      LinearFitIO.save(spark, fit, path)
      val loaded = LinearFitIO.load(spark, path)
      same(fit, loaded)
      def predictions(f: LinearFit) =
        LinearFE
          .predict(df, f)
          .orderBy("id")
          .select("prediction")
          .collect()
          .map(_.getDouble(0))
          .toSeq
      assertEquals(bits(predictions(loaded)), bits(predictions(fit)))
      assertEquals(bits(Seq(LinearFE.score(df, loaded))), bits(Seq(LinearFE.score(df, fit))))
    }
  }

  test("saving never overwrites; other kinds and format versions are refused, naming both") {
    val fit = LinearFE.fit(input("lin-base"), spec)
    val path = fresh("fit")
    LinearFitIO.save(spark, fit, path)
    intercept[Exception](LinearFitIO.save(spark, fit, path))
    def metadata(json: String): String = {
      val dir =
        Files.createDirectories(Files.createTempDirectory("linear-fit").resolve("other/metadata"))
      Files.write(dir.resolve("part-00000.json"), json.getBytes(StandardCharsets.UTF_8))
      dir.getParent.toString
    }
    val logistic = intercept[IllegalArgumentException](
      LinearFitIO.load(
        spark,
        metadata("""{"formatVersion": 1, "kind": "pprof.spark.engine.logistic.LogisticFit"}""")
      )
    )
    assert(
      logistic.getMessage.contains("LogisticFit") && logistic.getMessage.contains(LinearFitIO.Kind),
      logistic.getMessage
    )
    val future = intercept[IllegalArgumentException](
      LinearFitIO.load(spark, metadata(s"""{"formatVersion": 2, "kind": "${LinearFitIO.Kind}"}"""))
    )
    assert(future.getMessage.contains("format version 2"), future.getMessage)
  }
}
