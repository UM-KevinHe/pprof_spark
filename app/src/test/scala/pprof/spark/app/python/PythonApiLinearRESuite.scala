package pprof.spark.app.python

import java.nio.file.Files

import pprof.spark.engine.linear.{LinearRE, LinearREOptions, LinearRESpec}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** The facade's linear random-effect methods (docs/spec/linear/random-effect-job.md §6, §7): the job runner's parser
  * reports every problem at once, the fit equals the library's bit for bit, and save and load round-trip.
  */
class PythonApiLinearRESuite extends SparkSuite {

  test("linearREModel reports every problem at once, as the job runner's parser") {
    val message = intercept[IllegalArgumentException](
      PythonApi.linearREModel(
        """{"outcome": 3, "trials": "n"}""",
        """{"reml": "yes", "maxEvaluations": 5, "blocks": {"bogus": 1}}"""
      )
    ).getMessage
    Seq(
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.trials does not belong to a linear-re specification",
      "fit.reml must be true or false",
      "fit.blocks.bogus does not belong to a linear-re specification"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val bounded = intercept[IllegalArgumentException](
      PythonApi.linearREModel("""{"outcome": "y", "provider": "p"}""", """{"maxEvaluations": 5}""")
    ).getMessage
    assert(bounded.contains("maxEvaluations must be at least 20"), bounded)
    val (spec, options) = PythonApi.linearREModel(
      """{"outcome": "y", "features": ["x1"], "provider": "p", "offset": "o"}""",
      """{"reml": false, "thetaUpper": 5}"""
    )
    assertEquals(spec, LinearRESpec("y", Seq("x1"), "p", offset = Some("o")))
    assertEquals((options.reml, options.thetaUpper, options.includeIntercept), (false, 5.0, true))
  }

  test("linearREFit equals the library's fit bit for bit; save and load round-trip") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("linear-re/lre-offset/input.csv").toString)
    val fit = PythonApi.linearREFit(
      df,
      """{"outcome": "y", "features": ["x1", "x2", "x3"], "provider": "provider", "offset": "o", "rowId": "id"}""",
      "{}"
    )
    val library = LinearRE.fit(
      df,
      LinearRESpec("y", Seq("x1", "x2", "x3"), "provider", offset = Some("o"), rowId = Some("id")),
      LinearREOptions()
    )
    assertEquals(
      fit.estimates.map(java.lang.Double.doubleToRawLongBits),
      library.estimates.map(java.lang.Double.doubleToRawLongBits)
    )
    val path = Files.createTempDirectory("pprof-facade-linear-re").resolve("fit").toString
    PythonApi.linearRESave(spark, fit, path)
    val loaded = PythonApi.linearRELoad(spark, path)
    assertEquals(PythonApi.linearRESummary(loaded), PythonApi.linearRESummary(fit))
  }
}
