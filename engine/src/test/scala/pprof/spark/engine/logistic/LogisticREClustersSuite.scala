package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, rand}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** The engine's logistic cluster-factor fit (spec 4d-2, D-72): fits against pprof_py and glmer; lockstep at pprof_py's
  * tightened (σ, β) for `summary` and every test method; `exact` refused with two cluster factors; invariance.
  */
class LogisticREClustersSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(8, "min")

  private val cases =
    Seq("lgc-one", "lgc-two", "lgc-text", "lgc-offset", "lgc-nested", "lgc-boundary")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"logistic-re-clusters/$name/input.csv").toString)

  private def listed(node: JsonNode): Array[Double] =
    if (node.isArray) Fixtures.doubles(node) else Array(java.lang.Double.parseDouble(node.asText()))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val keep = actual.indices.filter(i =>
      java.lang.Double.isFinite(expected(i)) || java.lang.Double.isFinite(actual(i))
    )
    keep
      .filterNot(i =>
        java.lang.Double.isFinite(expected(i)) && java.lang.Double.isFinite(actual(i))
      )
      .foreach { i =>
        assert(actual(i) == expected(i), s"$what: ${actual(i)} against ${expected(i)} at $i")
      }
    val finite = keep.filter(i =>
      java.lang.Double.isFinite(expected(i)) && java.lang.Double.isFinite(actual(i))
    )
    val value = Tolerances(cls).worstRatio(finite.map(actual).toArray, finite.map(expected).toArray)
    assert(value <= 1.0, s"$what: $value of $cls")
  }

  private def column(rows: Array[Row], name: String): Seq[Double] =
    rows.toSeq.map(r =>
      if (r.isNullAt(r.fieldIndex(name))) Double.NaN else r.getAs[Number](name).doubleValue
    )

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

  for (name <- Seq("lgc-one", "lgc-two", "lgc-boundary")) {
    test(s"$name: the engine's fit against pprof_py and glmer (D-72)") {
      val fit = LogisticREClusters.fit(input(name), spec(name))
      val py = Fixtures.json(s"logistic-re-clusters/$name/pprof_py.json").get("tight")
      val gl = Fixtures.json(s"logistic-re-clusters/$name/r_lme4.json")
      for ((who, ref) <- Seq("pprof_py" -> py, "glmer" -> gl)) {
        check("T-opt", s"$name sigma vs $who", fit.sigma, listed(ref.get("sigma")))
        check("T-opt", s"$name beta vs $who", fit.estimates, listed(ref.get("beta")))
        check("T-fn", s"$name log-likelihood vs $who", Seq(fit.loglik), listed(ref.get("loglik")))
        fit.factors.foreach { f =>
          val re = ref.get("random_effects").get(f)
          val keys = (0 until re.get("keys").size).map(i => re.get("keys").get(i).asText())
          val blup = keys.zip(listed(re.get("blup"))).toMap
          val se = keys.zip(listed(re.get("se"))).toMap
          val ours = fit.effects(f).orderBy(f).collect().toSeq
          check(
            "T-opt",
            s"$name $f BLUPs vs $who",
            ours.map(_.getAs[Double]("blup")),
            ours.map(r => blup(String.valueOf(r.get(0)))).toArray
          )
          check(
            "T-opt",
            s"$name $f posterior SEs vs $who",
            ours.map(_.getAs[Double]("se")),
            ours.map(r => se(String.valueOf(r.get(0)))).toArray
          )
        }
      }
    }
  }

  for (name <- cases) {
    test(
      s"$name: lockstep at pprof_py's tightened (sigma, beta): summary and every test method (D-72)"
    ) {
      val py = Fixtures.json(s"logistic-re-clusters/$name/pprof_py.json")
      val t = py.get("tight")
      val fit = LogisticREClusters.fitAt(
        input(name),
        spec(name),
        LogisticREClusterOptions(),
        listed(t.get("sigma")).toSeq,
        listed(t.get("beta")).toSeq
      )
      val inf = py.get("inference")
      val s = LogisticREClusters.summary(fit)
      val ps = inf.get("summary")
      check(
        "T-coef",
        s"$name summary standard errors",
        s.map(_.standardError),
        listed(ps.get("Std.Error"))
      )
      check("T-test", s"$name summary z", s.map(_.statistic), listed(ps.get("z value")))
      check(
        "T-coef",
        s"$name summary limits",
        s.map(_.lower) ++ s.map(_.upper),
        listed(ps.get("ci_lower")) ++ listed(ps.get("ci_upper"))
      )
      val methods =
        if (spec(name).clusters.size == 1) Seq("wald", "poibin_exact", "exact")
        else Seq("wald", "poibin_exact")
      methods.foreach { method =>
        val table = LogisticREClusters.test(fit, method).orderBy("provider").collect()
        val x = inf.get("test").get(method)
        check(
          "T-coef",
          s"$name $method estimate",
          column(table, "estimate"),
          listed(x.get("estimate"))
        )
        val (z, ez, p, ep) = (
          column(table, "z_raw"),
          listed(x.get("z_raw")),
          column(table, "p_value"),
          listed(x.get("p_value"))
        )
        val clear =
          z.indices.filter(i => method == "wald" || ep(i) >= 1e-6) // X-024 for the exact tails
        check("T-test", s"$name $method z", clear.map(z), clear.map(ez).toArray)
        p.zip(ep).foreach { case (a, b) =>
          assert(math.abs(a - b) <= 1e-14 + 1e-8 * b, s"$name $method p $a against $b")
        }
        check(
          "T-coef",
          s"$name $method limits",
          column(table, "ci_lower") ++ column(table, "ci_upper"),
          listed(x.get("ci_lower")) ++ listed(x.get("ci_upper"))
        )
        assertEquals(column(table, "flag"), listed(x.get("flag")).toSeq, s"$name $method flags")
      }
      val r = LogisticREClusters
        .test(fit, "resampling", nResample = 20000, seed = 7L)
        .orderBy("provider")
        .collect()
      column(r, "p_value").zip(listed(inf.get("test").get("resampling").get("p_value"))).foreach {
        case (a, b) =>
          val tail = math.min(b / 2.0, 0.5)
          val se = math.sqrt(8.0 * math.max(tail * (1.0 - tail), 1e-8) / 20000.0)
          assert(
            math.abs(a - b) <= 4.5 * se || math.abs(a - b) <= 1e-14 + 1e-8 * b,
            s"$name resampling p $a against $b"
          )
      }
      if (spec(name).clusters.size > 1) {
        val message =
          intercept[IllegalArgumentException](LogisticREClusters.test(fit, "exact")).getMessage
        assert(message.contains("exactly one cluster factor"), message)
      }
    }
  }

  test("invariant to row order and partitioning, bit for bit") {
    val df = input("lgc-one")
    val a = LogisticREClusters.fit(df, spec("lgc-one"))
    val b = LogisticREClusters.fit(df.orderBy(rand(5)).repartition(4, col("x1")), spec("lgc-one"))
    def bits(v: Seq[Double]) = v.map(java.lang.Double.doubleToRawLongBits)
    assertEquals(
      bits(b.sigma ++ b.estimates :+ b.deviance),
      bits(a.sigma ++ a.estimates :+ a.deviance)
    )
  }
}
