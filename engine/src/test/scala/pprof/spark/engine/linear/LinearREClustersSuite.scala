package pprof.spark.engine.linear

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, rand}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** The engine's cluster-factor fit (spec 4d-1, D-70) on every `fixtures/linear-re-clusters` case against pprof_py's
  * and lme4's tightened fits, REML and ML; invariance to row order and partitioning; fitted values; the refusals.
  */
class LinearREClustersSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear-re-clusters/$name/input.csv").toString)

  private def listed(node: JsonNode): Array[Double] =
    if (node.isArray) Fixtures.doubles(node) else Array(java.lang.Double.parseDouble(node.asText()))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val value = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(value <= 1.0, s"$what: $value of $cls")
  }

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

  for (
    name <- Seq(
      "lrc-one",
      "lrc-two",
      "lrc-text",
      "lrc-weights",
      "lrc-offset",
      "lrc-nested",
      "lrc-boundary"
    )
  ) {
    test(
      s"$name: REML and ML fits against pprof_py and lme4 (D-70), fitted values against pprof_py"
    ) {
      val df = input(name)
      val py = Fixtures.json(s"linear-re-clusters/$name/pprof_py.json")
      val r = Fixtures.json(s"linear-re-clusters/$name/r_lme4.json")
      for ((mode, reml) <- Seq("reml" -> true, "ml" -> false)) {
        val fit = LinearREClusters.fit(df, spec(name), LinearREClusterOptions(reml = reml))
        val (t, rr) = (py.get(s"${mode}_tight"), r.get(mode))
        check(
          "T-opt",
          s"$name $mode theta vs pprof_py",
          fit.theta,
          Fixtures.doubles(t.get("theta"))
        )
        check("T-opt", s"$name $mode theta vs lme4", fit.theta, listed(rr.get("theta")))
        check("T-opt", s"$name $mode beta vs lme4", fit.estimates, listed(rr.get("beta")))
        check("T-opt", s"$name $mode sigma vs lme4", Seq(fit.sigma), listed(rr.get("sigma")))
        check(
          "T-opt",
          s"$name $mode standard errors vs lme4",
          fit.standardErrors,
          listed(rr.get("se"))
        )
        check(
          "T-fn",
          s"$name $mode criterion vs lme4",
          Seq(fit.deviance),
          listed(rr.get("criterion"))
        )
        check(
          "T-fn",
          s"$name $mode criterion vs pprof_py",
          Seq(fit.deviance),
          Fixtures.doubles(t.get("objective"))
        )
        fit.factors.foreach { f =>
          val re = rr.get("random_effects").get(f)
          val byKey = (0 until re.get("keys").size)
            .map(i => re.get("keys").get(i).asText())
            .zip(listed(re.get("blup")))
            .toMap
          val ours = fit.effects(f).orderBy(f).collect().toSeq
          check(
            "T-opt",
            s"$name $mode BLUPs of $f",
            ours.map(_.getAs[Double]("blup")),
            ours.map(row => byKey(String.valueOf(row.get(0)))).toArray
          )
        }
        if (reml) {
          val fitted = LinearREClusters
            .fitted(df, fit)
            .orderBy("id")
            .limit(100)
            .select("fitted")
            .collect()
            .map(_.getDouble(0))
          check(
            "T-opt",
            s"$name fitted vs pprof_py",
            fitted.toSeq,
            Fixtures.doubles(t.get("fitted"))
          )
        }
      }
    }
  }

  test("invariant to row order and partitioning, bit for bit; predictions by chosen factors") {
    val df = input("lrc-two")
    val a = LinearREClusters.fit(df, spec("lrc-two"))
    val b = LinearREClusters.fit(df.orderBy(rand(7)).repartition(5, col("x1")), spec("lrc-two"))
    def bits(v: Seq[Double]) = v.map(java.lang.Double.doubleToRawLongBits)
    assertEquals(bits(b.theta), bits(a.theta))
    assertEquals(
      bits(b.estimates :+ b.sigma :+ b.deviance),
      bits(a.estimates :+ a.sigma :+ a.deviance)
    )
    a.factors.foreach(f =>
      assertEquals(
        b.effects(f).orderBy(f).collect().toSeq.map(_.toString),
        a.effects(f).orderBy(f).collect().toSeq.map(_.toString)
      )
    )
    val none = LinearREClusters.predict(df, a).orderBy("id").select("prediction", "xbeta").collect()
    none.foreach(row => assertEquals(row.getDouble(0), row.getDouble(1)))
    val withC1 = LinearREClusters
      .predict(df, a, Seq("c1"))
      .orderBy("id")
      .select("prediction")
      .collect()
      .map(_.getDouble(0))
    assert(withC1.zip(none).exists { case (x, y) => x != y.getDouble(0) })
    intercept[IllegalArgumentException](LinearREClusters.predict(df, a, Seq("other")))
  }

  test("refusals: a cluster column equal to the provider's, a missing column, a null key") {
    val df = input("lrc-one")
    intercept[IllegalArgumentException](
      LinearREClusters.fit(df, LinearREClusterSpec("y", Seq("x1"), "provider", Seq("provider")))
    )
    intercept[Exception](
      LinearREClusters.fit(df, LinearREClusterSpec("y", Seq("x1"), "provider", Seq("missing")))
    )
    val nulls = df.withColumn(
      "c1",
      org.apache.spark.sql.functions
        .when(col("id") === 3, org.apache.spark.sql.functions.lit(null))
        .otherwise(col("c1"))
    )
    intercept[Exception](
      LinearREClusters.fit(nulls, LinearREClusterSpec("y", Seq("x1"), "provider", Seq("c1")))
    )
  }
}
