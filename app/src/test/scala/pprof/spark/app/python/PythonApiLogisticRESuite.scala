package pprof.spark.app.python

import com.fasterxml.jackson.databind.ObjectMapper
import pprof.spark.engine.logistic.LogisticRESpec
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** The facade's logistic random-effect methods (spec 4b-1 §9): the model's JSON reports every problem at once and maps
  * the options; the fit is the model the JSON names, and the summary carries it bit for bit.
  */
class PythonApiLogisticRESuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(5, "min")

  test("logisticREModel reports every problem at once and maps the options") {
    val message = intercept[IllegalArgumentException](
      PythonApi.logisticREModel(
        """{"outcome": 3, "trials": "n"}""",
        """{"stage2": "yes", "blocks": {"bogus": 1}}"""
      )
    ).getMessage
    Seq(
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.trials does not belong to a logistic-re model",
      "fit.stage2 must be true or false",
      "fit.blocks.bogus does not belong to a logistic-re model"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val bounded = intercept[IllegalArgumentException](
      PythonApi.logisticREModel("""{"outcome": "y", "provider": "p"}""", """{"maxIterations": 5}""")
    ).getMessage
    assert(bounded.contains("maxIterations must be at least 20"), bounded)
    val (spec, options) = PythonApi.logisticREModel(
      """{"outcome": "y", "features": ["x1"], "provider": "p", "offset": "o"}""",
      """{"stage2": false, "sigmaUpper": 3}"""
    )
    assertEquals(spec, LogisticRESpec("y", Seq("x1"), "p", offset = Some("o")))
    assertEquals((options.nagq1, options.sigmaUpper, options.includeIntercept), (false, 3.0, true))
  }

  test("logisticREFit fits the model the JSON names, and the summary carries the fit bit for bit") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("logistic-re/lgre-text/input.csv").toString)
    val fit = PythonApi.logisticREFit(
      df,
      """{"outcome": "y", "features": ["x1", "x2"], "provider": "provider", "rowId": "id"}""",
      "{}"
    )
    assertEquals(fit.spec, LogisticRESpec("y", Seq("x1", "x2"), "provider", rowId = Some("id")))
    val expected = Fixtures.doubles(
      Fixtures.json("logistic-re/lgre-text/pprof_py.json").get("nagq1").get("tight").get("beta")
    )
    assert(Tolerances("T-est").worstRatio(fit.estimates.toArray, expected) <= 1.0)
    val summary = new ObjectMapper().readTree(PythonApi.logisticRESummary(fit))
    assertEquals(summary.get("criterion").asText(), "Laplace")
    assertEquals(summary.get("providers").asInt(), 30)
    assertEquals(java.lang.Double.parseDouble(summary.get("sigma").asText()), fit.sigma)
    val estimates = (0 until fit.coefficients.size).map(i =>
      java.lang.Double.parseDouble(summary.get("coefficients").get(i).get("estimate").asText())
    )
    assertEquals(
      estimates.map(java.lang.Double.doubleToRawLongBits),
      fit.estimates.map(java.lang.Double.doubleToRawLongBits)
    )
  }

  test(
    "the inference methods: JSON and frames as LogisticREInference gives them; a Wald test without data"
  ) {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("logistic-re/lgre-text/input.csv").toString)
    val fit = pprof.spark.engine.logistic.LogisticRE.fitAt(
      df,
      LogisticRESpec("y", Seq("x1", "x2"), "provider", rowId = Some("id")),
      pprof.spark.engine.logistic.LogisticREOptions(),
      0.5,
      Seq(-1.0, 0.7, -0.4)
    )
    val tests = new ObjectMapper().readTree(PythonApi.logisticRECovariateTests(fit, 0.9))
    val expected = pprof.spark.engine.logistic.LogisticREInference.summary(fit, 0.9)
    assertEquals(tests.size(), expected.size)
    assertEquals(
      java.lang.Double.parseDouble(tests.get(1).get("statistic").asText()),
      expected(1).statistic
    )
    val wald = PythonApi.logisticRETest(
      null,
      fit,
      "wald",
      "median",
      "two_sided",
      0.95,
      Double.NaN,
      "",
      10000,
      0L
    )
    assertEquals(wald.count(), 30L)
    val subset = PythonApi.logisticRETest(
      null,
      fit,
      "wald",
      "0.1",
      "greater",
      0.9,
      2.0,
      "[\"prov-1\", \"prov-2\"]",
      10000,
      0L
    )
    assertEquals(subset.count(), 2L)
    assertEquals(
      PythonApi.logisticREMeasures(df, fit, "[\"indirect\", \"direct\"]", "mean").keySet().size(),
      2
    )
    val intervals =
      PythonApi.logisticREIntervals(df, fit, "[\"direct\"]", "[\"ratio\"]", "median", "less", 0.9)
    assertEquals(intervals.keySet().toArray.toSeq, Seq("direct_ratio"))
  }

  test("logisticRESave and logisticRELoad round-trip; the loaded summary is the same") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("logistic-re/lgre-text/input.csv").toString)
    val fit = pprof.spark.engine.logistic.LogisticRE.fitAt(
      df,
      LogisticRESpec("y", Seq("x1", "x2"), "provider", rowId = Some("id")),
      pprof.spark.engine.logistic.LogisticREOptions(),
      0.5,
      Seq(-1.0, 0.7, -0.4)
    )
    val path =
      java.nio.file.Files.createTempDirectory("pprof-facade-logistic-re").resolve("fit").toString
    PythonApi.logisticRESave(spark, fit, path)
    assertEquals(
      PythonApi.logisticRESummary(PythonApi.logisticRELoad(spark, path)),
      PythonApi.logisticRESummary(fit)
    )
  }
}
