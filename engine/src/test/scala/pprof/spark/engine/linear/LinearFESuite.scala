package pprof.spark.engine.linear

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pprof.spark.engine.data.InvalidInputException
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** End-to-end parity of the distributed linear fit with pprof_py and R's `lm` on `fixtures/linear`
  * (docs/spec/linear/fixed-effect-estimation.md §11), one test per case, and its edge cases.
  */
class LinearFESuite extends SparkSuite {

  private val cases = Seq("lin-base", "lin-singletons", "lin-shifted", "lin-text", "lin-many")
  private val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear/$name/input.csv").toString)

  private def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  /** T-p (§8.4): above T-p.log10.threshold the p-values under T-test, below it log10 p within T-p.log10.atol. */
  private def checkP(what: String, actual: Seq[Double], expected: Array[Double]): Unit = {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    actual.zip(expected).zipWithIndex.foreach { case ((a, e), i) =>
      if (e >= threshold) assert(Tolerances("T-test").accepts(a, e), s"$what[$i]: $a against $e")
      else if (e == 0.0) assert(a < 1e-300, s"$what[$i]: $a against 0")
      else
        assert(
          math.abs(math.log10(a) - math.log10(e)) <= atol,
          s"$what[$i]: log10 of $a against $e"
        )
    }
  }

  private def column(table: DataFrame, name: String): Seq[Double] =
    table.orderBy(spec.provider).select(name).collect().map(_.getDouble(0)).toSeq

  for (name <- cases) {
    test(s"$name: the distributed fit matches pprof_py and R's lm") {
      val df = input(name)
      val py = Fixtures.json(s"linear/$name/pprof_py.json")
      val r = Fixtures.json(s"linear/$name/r_linear.json")
      val ref = py.get("complete")
      val fit = LinearFE.fit(df, spec)
      check("T-coef", "beta vs pprof_py", fit.estimates, d(ref.get("beta")))
      check("T-coef", "beta vs lm", fit.estimates, d(r.get("beta")))
      check("T-coef", "gamma vs pprof_py", column(fit.providers, "gamma"), d(ref.get("gamma")))
      check("T-coef", "gamma vs lm", column(fit.providers, "gamma"), d(r.get("gamma")))
      check("T-var", "var_beta", fit.covariance, d(ref.get("var_beta")))
      check("T-var", "var_gamma", column(fit.providers, "variance"), d(ref.get("var_gamma")))
      check("T-var", "var_gamma vs lm", column(fit.providers, "variance"), d(r.get("var_gamma")))
      Seq(
        "sigma" -> fit.sigma,
        "rss" -> fit.rss,
        "loglik" -> fit.loglik,
        "aic" -> fit.aic,
        "bic" -> fit.bic
      )
        .foreach { case (q, value) => check("T-fn", q, Seq(value), d(ref.get(q))) }
      check("T-fn", "R^2", Seq(fit.r2), d(py.get("score")))
      assertEquals(fit.degreesOfFreedom, ref.get("df").asLong)
      assertEquals(fit.providerCount, ref.get("providers").size)
      assertEquals(
        fit.providers
          .orderBy(spec.provider)
          .select(spec.provider)
          .collect()
          .map(_.get(0).toString)
          .toSeq,
        (0 until ref.get("providers").size).map(i => ref.get("providers").get(i).asText())
      )
      for ((alternative, nullValue) <- Seq("two_sided" -> 0.0, "greater" -> 0.25, "less" -> 0.25)) {
        val tests = LinearFE.summary(fit, 0.95, nullValue, alternative)
        val expected = py.get("summary").get(alternative)
        check("T-test", s"$alternative statistic", tests.map(_.statistic), d(expected.get("stat")))
        if (alternative != "less")
          check("T-coef", s"$alternative lower", tests.map(_.lower), d(expected.get("ci_lower")))
        if (alternative != "greater")
          check("T-coef", s"$alternative upper", tests.map(_.upper), d(expected.get("ci_upper")))
        val rp = d(
          r.get(Map("two_sided" -> "p", "greater" -> "greater_p", "less" -> "less_p")(alternative))
        )
        checkP(s"$alternative p vs lm", tests.map(_.pValue), rp)
        val pyP = d(expected.get("p_value"))
        val reliable = pyP.indices.filter(i => pyP(i) > 1e-8) // X-031
        checkP(
          s"$alternative p vs pprof_py",
          reliable.map(i => tests(i).pValue),
          reliable.map(pyP).toArray
        )
      }
      val predicted = LinearFE
        .predict(df.filter(col("id") < 100), fit)
        .orderBy("id")
        .select("prediction")
        .collect()
        .map(_.getDouble(0))
        .toSeq
      check("T-meas", "predictions", predicted, d(py.get("predict")))
      assertEquals(fit.status, "experimental")
    }
  }

  test("the simplified variance option gives sigma^2 / n per provider") {
    val py = Fixtures.json("linear/lin-base/pprof_py.json")
    val fit = LinearFE.fit(input("lin-base"), spec, LinearOptions(varianceOption = "simplified"))
    check(
      "T-var",
      "simplified var_gamma",
      column(fit.providers, "variance"),
      d(py.get("simplified").get("var_gamma"))
    )
  }

  test(
    "aliased features, missing degrees of freedom and unknown providers fail with names and counts"
  ) {
    val df = input("lin-base")
    val providerLevel = df.withColumn("x3", col("provider") * 0.125)
    val aliased = intercept[LinearAliasingException](LinearFE.fit(providerLevel, spec))
    assertEquals(aliased.features, Seq("x3"))
    val tiny = df.filter(col("id") < 3)
    val noDegrees = intercept[InvalidInputException](LinearFE.fit(tiny, spec))
    assert(noDegrees.getMessage.contains("X-033"), noDegrees.getMessage)
    val fit = LinearFE.fit(df, spec)
    val unknown = intercept[InvalidInputException](
      LinearFE.predict(df.withColumn("provider", col("provider") + 100000), fit)
    )
    assert(unknown.getMessage.contains("provider"), unknown.getMessage)
  }

  test("bitwise invariant to row order and partitioning") {
    val df = input("lin-base")
    val a = LinearFE.fit(df, spec)
    val b = LinearFE.fit(df.repartition(7).sortWithinPartitions(col("x2").desc), spec)
    assertEquals(
      a.estimates.map(java.lang.Double.doubleToLongBits),
      b.estimates.map(java.lang.Double.doubleToLongBits)
    )
    assertEquals(
      java.lang.Double.doubleToLongBits(a.sigma),
      java.lang.Double.doubleToLongBits(b.sigma)
    )
    assertEquals(
      column(a.providers, "gamma").map(java.lang.Double.doubleToLongBits),
      column(b.providers, "gamma").map(java.lang.Double.doubleToLongBits)
    )
    assertEquals(a.fingerprint, b.fingerprint)
  }
}
