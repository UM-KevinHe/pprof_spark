package pprof.spark.engine.logistic

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LogisticREClusterFitIO` (spec 4d-3 §1.2): save, load and attach bit for bit, the inference unchanged; refusals. */
class LogisticREClusterFitIOSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"logistic-re-clusters/$name/input.csv").toString)

  private def spec(name: String): LogisticREClusterSpec = {
    val c = Fixtures.json(s"logistic-re-clusters/$name/case.json")
    val clusters =
      (0 until c.get("cluster_vars").size).map(i => c.get("cluster_vars").get(i).asText())
    LogisticREClusterSpec(
      "y",
      Seq("x1", "x2"),
      "provider",
      clusters,
      if (c.has("offset")) Some("o") else None,
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

  for (name <- Seq("lgc-one", "lgc-two")) {
    test(
      s"$name: saved, loaded and re-attached bit for bit; the tests, measures and intervals unchanged"
    ) {
      val df = input(name)
      val fit = LogisticREClusters.fit(df, spec(name))
      val path = Files.createTempDirectory("pprof-lgc-fit").resolve("fit").toString
      LogisticREClusterFitIO.save(spark, fit, path)
      val saved = LogisticREClusterFitIO.load(spark, path)
      assertEquals(
        bits(saved.sigma ++ saved.estimates ++ saved.covariance),
        bits(fit.sigma ++ fit.estimates ++ fit.covariance)
      )
      assertEquals(
        bits(Seq(saved.deviance, saved.loglik, saved.aic, saved.bic, saved.stage1Deviance)),
        bits(Seq(fit.deviance, fit.loglik, fit.aic, fit.bic, fit.stage1Deviance))
      )
      fit.factors.foreach(f => assertEquals(rows(saved.effects(f), f), rows(fit.effects(f), f)))
      val attached = LogisticREClusterFitIO.attach(df, saved)
      val methods =
        if (spec(name).clusters.size == 1) Seq("wald", "poibin_exact", "exact")
        else Seq("wald", "poibin_exact")
      methods.foreach(m =>
        assertEquals(
          rows(LogisticREClusters.test(attached, m), "provider"),
          rows(LogisticREClusters.test(fit, m), "provider")
        )
      )
      val (a, b) = (
        LogisticREClusters.measures(fit, Seq("indirect", "direct")),
        LogisticREClusters.measures(attached, Seq("indirect", "direct"))
      )
      Seq("indirect", "direct").foreach(k =>
        assertEquals(rows(b(k), "provider"), rows(a(k), "provider"))
      )
      assertEquals(
        rows(LogisticREClusters.alphaIntervals(attached), "provider"),
        rows(LogisticREClusters.alphaIntervals(fit), "provider")
      )
      assertEquals(
        rows(LogisticREClusters.fitted(df, attached), "id"),
        rows(LogisticREClusters.fitted(df, fit), "id")
      )
      if (name == "lgc-one")
        assertEquals(
          LogisticREClusters.profileSigma(attached, "c1"),
          LogisticREClusters.profileSigma(fit, "c1")
        )
    }
  }

  test(
    "refusals: overwriting, another kind, an unknown version, a short table, other and changed data"
  ) {
    val df = input("lgc-one")
    val fit = LogisticREClusters.fit(df, spec("lgc-one"))
    val dir = Files.createTempDirectory("pprof-lgc-fit")
    val path = dir.resolve("fit").toString
    LogisticREClusterFitIO.save(spark, fit, path)
    intercept[Exception](LogisticREClusterFitIO.save(spark, fit, path))
    def variant(name: String, from: String, to: String, short: Boolean = false): String = {
      val target = dir.resolve(name).toString
      spark.read
        .text(s"$path/metadata")
        .selectExpr(s"replace(value, '$from', '$to') as value")
        .write
        .text(s"$target/metadata")
      (0 until 2).foreach { k =>
        val t = spark.read.parquet(s"$path/effects/$k")
        (if (short && k == 0) t.limit(5) else t).write.parquet(s"$target/effects/$k")
      }
      target
    }
    val kind = intercept[IllegalArgumentException](
      LogisticREClusterFitIO.load(spark, variant("kind", "LogisticREClusterFit", "OtherFit"))
    ).getMessage
    assert(kind.contains("OtherFit"), kind)
    val version = intercept[IllegalArgumentException](
      LogisticREClusterFitIO.load(
        spark,
        variant("version", "\"formatVersion\":1", "\"formatVersion\":2")
      )
    ).getMessage
    assert(version.contains("format version 2"), version)
    val short = intercept[IllegalArgumentException](
      LogisticREClusterFitIO.load(spark, variant("short", "\"kind\"", "\"kind\"", short = true))
    ).getMessage
    assert(short.contains("holds 5 levels of provider"), short)
    val saved = LogisticREClusterFitIO.load(spark, path)
    val other = intercept[IllegalArgumentException](
      LogisticREClusterFitIO.attach(input("lgc-text"), saved)
    ).getMessage
    assert(other.contains("not the fit's training data"), other)
    val changed =
      df.withColumn("x1", when(col("id") === 11, col("x1") + lit(1.0 / 64)).otherwise(col("x1")))
    val message =
      intercept[IllegalArgumentException](LogisticREClusterFitIO.attach(changed, saved)).getMessage
    assert(message.contains("not the fit's training data"), message)
  }
}
