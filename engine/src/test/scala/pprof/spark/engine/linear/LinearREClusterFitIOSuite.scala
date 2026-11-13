package pprof.spark.engine.linear

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LinearREClusterFitIO` (spec 4d-3 §1.1): save and load bit for bit, the loaded fit's methods unchanged; refusals. */
class LinearREClusterFitIOSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(5, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear-re-clusters/$name/input.csv").toString)

  private def spec(name: String): LinearREClusterSpec = {
    val c = Fixtures.json(s"linear-re-clusters/$name/case.json")
    val clusters =
      (0 until c.get("cluster_vars").size).map(i => c.get("cluster_vars").get(i).asText())
    LinearREClusterSpec(
      "y",
      Seq("x1", "x2"),
      "provider",
      clusters,
      if (c.has("offset")) Some("o") else None,
      if (c.has("weights")) Some("w") else None,
      Some("id")
    )
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

  for (name <- Seq("lrc-two", "lrc-text")) {
    test(
      s"$name: a fit saves and loads bit for bit; the loaded fit's tables, fitted values and predictions are the same"
    ) {
      val df = input(name)
      val fit = LinearREClusters.fit(df, spec(name))
      val path = Files.createTempDirectory("pprof-lrc-fit").resolve("fit").toString
      LinearREClusterFitIO.save(spark, fit, path)
      val loaded = LinearREClusterFitIO.load(spark, path)
      assertEquals(loaded.coefficients.map(_.feature), fit.coefficients.map(_.feature))
      assertEquals(
        bits(loaded.estimates ++ loaded.standardErrors ++ loaded.covariance),
        bits(fit.estimates ++ fit.standardErrors ++ fit.covariance)
      )
      assertEquals(bits(loaded.theta :+ loaded.sigma), bits(fit.theta :+ fit.sigma))
      assertEquals(
        bits(
          Seq(loaded.deviance, loaded.loglik, loaded.aic, loaded.bic, loaded.pwrss, loaded.logDetH)
        ),
        bits(Seq(fit.deviance, fit.loglik, fit.aic, fit.bic, fit.pwrss, fit.logDetH))
      )
      assertEquals(
        (
          loaded.factors,
          loaded.criterion,
          loaded.rows,
          loaded.warnings,
          loaded.spec,
          loaded.options,
          loaded.software,
          loaded.fingerprint
        ),
        (
          fit.factors,
          fit.criterion,
          fit.rows,
          fit.warnings,
          fit.spec,
          fit.options,
          fit.software,
          fit.fingerprint
        )
      )
      assertEquals(
        (
          loaded.search.iterations,
          loaded.search.evaluations,
          loaded.search.converged,
          loaded.search.boundary
        ),
        (fit.search.iterations, fit.search.evaluations, fit.search.converged, fit.search.boundary)
      )
      fit.factors.foreach(f => assertEquals(rows(loaded.effects(f), f), rows(fit.effects(f), f)))
      assertEquals(LinearREClusters.summary(loaded), LinearREClusters.summary(fit))
      assertEquals(
        LinearREClusters.fixedEffectsTable(loaded),
        LinearREClusters.fixedEffectsTable(fit)
      )
      assertEquals(
        rows(LinearREClusters.fitted(df, loaded), "id"),
        rows(LinearREClusters.fitted(df, fit), "id")
      )
      assertEquals(
        rows(LinearREClusters.predict(df, loaded, Seq("c1")), "id"),
        rows(LinearREClusters.predict(df, fit, Seq("c1")), "id")
      )
    }
  }

  test("refusals: overwriting, another kind, an unknown version, a short effect table") {
    val fit = LinearREClusters.fit(input("lrc-one"), spec("lrc-one"))
    val dir = Files.createTempDirectory("pprof-lrc-fit")
    val path = dir.resolve("fit").toString
    LinearREClusterFitIO.save(spark, fit, path)
    intercept[Exception](LinearREClusterFitIO.save(spark, fit, path))
    def variant(name: String, from: String, to: String, short: Boolean = false): String = {
      val target = dir.resolve(name).toString
      spark.read
        .text(s"$path/metadata")
        .selectExpr(s"replace(value, '$from', '$to') as value")
        .write
        .text(s"$target/metadata")
      (0 until 2).foreach { k =>
        val t = spark.read.parquet(s"$path/effects/$k")
        (if (short && k == 1) t.limit(3) else t).write.parquet(s"$target/effects/$k")
      }
      target
    }
    val kind = intercept[IllegalArgumentException](
      LinearREClusterFitIO.load(spark, variant("kind", "LinearREClusterFit", "OtherFit"))
    ).getMessage
    assert(kind.contains("OtherFit"), kind)
    val version = intercept[IllegalArgumentException](
      LinearREClusterFitIO.load(
        spark,
        variant("version", "\"formatVersion\":1", "\"formatVersion\":2")
      )
    ).getMessage
    assert(version.contains("format version 2"), version)
    val short = intercept[IllegalArgumentException](
      LinearREClusterFitIO.load(spark, variant("short", "\"kind\"", "\"kind\"", short = true))
    ).getMessage
    assert(short.contains("holds 3 levels of c1"), short)
  }
}
