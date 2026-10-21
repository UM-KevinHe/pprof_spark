package pprof.spark.engine.linear

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, rand, when}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.InvalidInputException
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** End-to-end parity of the distributed linear random-intercept fit with pprof_py and lme4 at their tight optima on
  * `fixtures/linear-re`, under D-53's classes (docs/spec/linear/random-effect-estimation.md §11): one test per case
  * and criterion, lre-shifted's invariance against lre-base, bitwise invariance to row order and partitioning, and
  * the data contract (§5).
  */
class LinearRESuite extends SparkSuite {

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear-re/$name/input.csv").toString)

  private def setup(name: String): (LinearRESpec, Boolean) = {
    val c = Fixtures.json(s"linear-re/$name/case.json")
    val features = c.get("features").elements().asScala.map(_.asText()).toSeq
    val spec = LinearRESpec(
      "y",
      features,
      "provider",
      offset = if (c.has("offset")) Some("o") else None,
      weights = if (c.has("weights")) Some("w") else None,
      rowId = Some("id")
    )
    (spec, !c.has("intercept") || c.get("intercept").asBoolean())
  }

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

  private def providerColumn(fit: LinearREFit, name: String): Seq[Double] =
    fit.providers.orderBy(fit.spec.provider).select(name).collect().map(_.getDouble(0)).toSeq

  private def firstHundred(frame: DataFrame, column: String): Seq[Double] =
    frame.filter(col("id") < 100).orderBy("id").select(column).collect().map(_.getDouble(0)).toSeq

  private val cases = Seq(
    "lre-base",
    "lre-weights",
    "lre-offset",
    "lre-nointercept",
    "lre-provider-level",
    "lre-boundary",
    "lre-text",
    "lre-many"
  )

  for (name <- cases; criterion <- Seq("reml", "ml")) {
    test(s"$name, $criterion: the distributed fit matches pprof_py's and lme4's tight optima") {
      val (spec, intercept) = setup(name)
      val df = input(name)
      val fit = LinearRE.fit(
        df,
        spec,
        LinearREOptions(reml = criterion == "reml", includeIntercept = intercept)
      )
      val fixed = firstHundred(LinearRE.predict(df, fit), "prediction")
      val withBlups = firstHundred(LinearRE.predict(df, fit, useRe = true), "prediction")
      val fitted = firstHundred(LinearRE.fitted(df, fit), "fitted")
      val blups = providerColumn(fit, "blup")
      for (reference <- Seq("pprof_py", "r_lme4")) {
        val t = Fixtures.json(s"linear-re/$name/$reference.json").get(criterion).get("tight")
        check("T-coef", s"$reference beta", fit.estimates, d(t.get("beta")))
        check("T-coef", s"$reference BLUPs", blups, d(t.get("blups")))
        Seq(
          "theta" -> fit.theta,
          "sigma" -> fit.sigma,
          "sigma_alpha" -> fit.sigmaAlpha,
          "pwrss" -> fit.pwrss,
          "ldL2" -> fit.logDetH,
          "ussq" -> fit.ussq
        ).foreach { case (q, v) => check("T-var", s"$reference $q", Seq(v), d(t.get(q))) }
        check("T-var", s"$reference var_beta", fit.covariance, d(t.get("var_beta")))
        Seq("deviance" -> fit.deviance, "loglik" -> fit.loglik, "aic" -> fit.aic, "bic" -> fit.bic)
          .foreach { case (q, v) =>
            check("T-fn", s"$reference $q", Seq(v), d(t.get(q)))
          }
        check(
          "T-coef",
          s"$reference predictions without the BLUPs",
          fixed,
          d(t.get("predict_fixed"))
        )
        check("T-coef", s"$reference predictions with the BLUPs", withBlups, d(t.get("predict_re")))
        check("T-coef", s"$reference fitted values", fitted, d(t.get("fitted")))
      }
      val lme4 = Fixtures.json(s"linear-re/$name/r_lme4.json").get(criterion).get("tight")
      check(
        "T-var",
        "conditional variances vs lme4",
        providerColumn(fit, "variance"),
        d(lme4.get("condvar"))
      )
      assertEquals(fit.search.boundary, if (name == "lre-boundary") Some("lower") else None)
    }
  }

  for (criterion <- Seq("reml", "ml")) {
    test(
      s"lre-shifted, $criterion: the fit is invariant against lre-base (spec 4a-1 section 13, D-53)"
    ) {
      val options = LinearREOptions(reml = criterion == "reml")
      val base = LinearRE.fit(input("lre-base"), setup("lre-base")._1, options)
      val shifted = LinearRE.fit(input("lre-shifted"), setup("lre-shifted")._1, options)
      check("T-var", "theta", Seq(shifted.theta), Array(base.theta))
      check("T-var", "sigma", Seq(shifted.sigma), Array(base.sigma))
      check(
        "T-coef",
        "BLUPs",
        providerColumn(shifted, "blup"),
        providerColumn(base, "blup").toArray
      )
      val (b, s) = (base.estimates, shifted.estimates)
      check("T-coef", "slopes", Seq(s(1), s(2), s(3) / 1024.0), Array(b(1), b(2), b(3)))
      check("T-coef", "intercept", Seq(s(0) - 1e4 * (b(2) - b(1))), Array(b(0)))
      val shift = if (criterion == "reml") -2.0 * math.log(1024.0) else 0.0
      check("T-fn", "deviance", Seq(shifted.deviance - shift), Array(base.deviance))
    }
  }

  test(
    "lre-many: a search over the distributed provider table gives the driver's bits (spec 4a-1 section 7, OI-95)"
  ) {
    val (spec, _) = setup("lre-many")
    val df = input("lre-many")
    val small = BlockOptions(targetBlockBytes = 65536L)
    val driver = LinearRE.fit(df, spec, LinearREOptions(blocks = small))
    val distributed =
      LinearRE.fit(df, spec, LinearREOptions(blocks = small.copy(driverBudgetBytes = 50000L)))
    assertEquals((driver.search.path, distributed.search.path), ("driver", "distributed"))
    assert(driver.layout.blockCount > 1, s"${driver.layout.blockCount} blocks")
    assertEquals(distributed.estimates, driver.estimates)
    assertEquals(distributed.covariance, driver.covariance)
    assertEquals(providerColumn(distributed, "blup"), providerColumn(driver, "blup"))
    assertEquals(providerColumn(distributed, "variance"), providerColumn(driver, "variance"))
    assertEquals(
      (distributed.deviance, distributed.theta, distributed.ussq),
      (driver.deviance, driver.theta, driver.ussq)
    )
    val t = Fixtures.json("linear-re/lre-many/r_lme4.json").get("reml").get("tight")
    check("T-coef", "beta vs lme4", distributed.estimates, d(t.get("beta")))
    check("T-coef", "BLUPs vs lme4", providerColumn(distributed, "blup"), d(t.get("blups")))
  }

  test("lre-base: the fit is bitwise invariant to row order and partitioning (NN-4)") {
    val (spec, _) = setup("lre-base")
    val df = input("lre-base")
    val a = LinearRE.fit(df, spec)
    val b = LinearRE.fit(df.orderBy(rand(7)).repartition(5), spec)
    assertEquals(b.estimates, a.estimates)
    assertEquals(b.covariance, a.covariance)
    assertEquals(providerColumn(b, "blup"), providerColumn(a, "blup"))
    assertEquals((b.deviance, b.theta, b.sigma), (a.deviance, a.theta, a.sigma))
    assertEquals(b.fingerprint, a.fingerprint)
  }

  test(
    "the data contract: incomplete rows, non-positive weights and unidentified designs fail (X-038, X-040)"
  ) {
    val (spec, _) = setup("lre-weights")
    val df = input("lre-weights")
    val holes = df.withColumn("x1", when(col("id") < 3, lit(null)).otherwise(col("x1")))
    val missing = intercept[InvalidInputException](LinearRE.fit(holes, spec))
    assert(missing.getMessage.contains("x1 has 3 null"), missing.getMessage)
    val zero = df.withColumn("w", when(col("id") === 5, lit(0.0)).otherwise(col("w")))
    val weights = intercept[InvalidInputException](LinearRE.fit(zero, spec))
    assert(weights.getMessage.contains("1 values that are not positive"), weights.getMessage)
    for (provider <- Seq(lit(1), col("id"))) {
      val design =
        intercept[InvalidInputException](LinearRE.fit(df.withColumn("provider", provider), spec))
      assert(design.getMessage.contains("X-040"), design.getMessage)
    }
  }

  test(
    "an aliased feature fails and is named; prediction gives providers not in the fit a BLUP of 0 (X-039)"
  ) {
    val (spec, _) = setup("lre-base")
    val df = input("lre-base")
    val aliased = intercept[LinearREAliasingException](
      LinearRE.fit(
        df.withColumn("x4", col("x1") * 2.0),
        spec.copy(features = spec.features :+ "x4")
      )
    )
    assertEquals(aliased.features, Seq("x4"))
    val fit = LinearRE.fit(df, spec)
    val stranger = df.filter(col("id") < 3).withColumn("provider", lit(999999))
    def predictions(useRe: Boolean): Seq[Double] =
      LinearRE
        .predict(stranger, fit, useRe)
        .orderBy("id")
        .select("prediction")
        .collect()
        .map(_.getDouble(0))
        .toSeq
    assertEquals(predictions(useRe = true), predictions(useRe = false))
  }
}
