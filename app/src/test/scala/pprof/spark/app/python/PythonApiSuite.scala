package pprof.spark.app.python

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import pprof.spark.engine.cox.{
  CoxMeasures,
  CoxOptions,
  CoxPH,
  CoxProviderTests,
  CoxSpec,
  Standardization,
  TestMethod,
  Ties
}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** The py4j facade (ADR-0009): every result equals the engine's bit for bit. */
class PythonApiSuite extends SparkSuite {

  private def frame(name: String): DataFrame = {
    val t = Fixtures.csv(s"cox/$name/input.csv")
    val rows = t.rows.map { r =>
      Row.fromSeq(t.columns.indices.map { i =>
        t.columns(i) match {
          case "id" | "stratum" => r(i).toLong
          case "event"          => r(i).toInt
          case _                => r(i)
        }
      })
    }
    val schema = StructType(t.columns.map {
      case c @ ("id" | "stratum") => StructField(c, LongType, nullable = false)
      case c @ "event"            => StructField(c, IntegerType, nullable = false)
      case c                      => StructField(c, DoubleType, nullable = false)
    })
    spark.createDataFrame(rows.asJava, schema)
  }

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  private val columns =
    """{"time": "time", "event": "event", "features": ["x1", "x2", "x3"], "strata": "stratum", "rowId": "id"}"""

  test("a fit through the facade equals the engine's, and its summary carries every bit") {
    val df = frame("rc-stratified")
    val fit =
      PythonApi.coxFit(df, columns, """{"ties": "efron", "eps": 1e-11, "maxIterations": 100}""")
    val direct = CoxPH.fit(
      df,
      CoxSpec("time", "event", Seq("x1", "x2", "x3"), Some("stratum"), Some("id")),
      CoxOptions(ties = Ties.Efron, eps = 1e-11, maxIterations = 100)
    )
    assertEquals(bits(fit.estimates), bits(direct.estimates))
    assertEquals(bits(fit.covariance), bits(direct.covariance))
    val summary = new ObjectMapper().readTree(PythonApi.coxSummary(fit))
    val estimates = summary
      .get("coefficients")
      .elements()
      .asScala
      .map(c => java.lang.Double.parseDouble(c.get("estimate").asText()))
      .toSeq
    assertEquals(bits(estimates), bits(fit.estimates))
    assertEquals(summary.get("ties").asText(), "efron")
    assertEquals(summary.get("fingerprint").asLong(), fit.fingerprint)
    assertEquals(summary.get("columns").get("strata").asText(), "stratum")
    assertEquals(PythonApi.version(), pprof.spark.engine.BuildInfo.version)
  }

  private def sorted(table: DataFrame): Seq[Row] = table.orderBy(table.columns.head).collect().toSeq

  test("measures and provider tests through the facade equal the engine's") {
    val df = frame("rc-stratified")
    val fit = PythonApi.coxFit(df, columns, "{}")
    val direct = Standardization.Direct
    assertEquals(
      sorted(PythonApi.coxMeasures(df, fit, "stratum", "direct")),
      sorted(CoxMeasures.standardized(df, fit, "stratum", Seq(direct))(direct))
    )
    assertEquals(
      sorted(PythonApi.coxProviderTests(df, fit, "stratum", "exact", 0.9)),
      sorted(CoxProviderTests.test(df, fit, "stratum", TestMethod.Exact, 0.9))
    )
  }

  test("invalid column roles or options are reported at once, in the job runner's words") {
    val failure = intercept[IllegalArgumentException](
      PythonApi.coxModel("""{"time": "time"}""", """{"ties": "exact"}""")
    )
    assert(
      failure.getMessage.contains("columns.event") && failure.getMessage.contains("fit.ties"),
      failure.getMessage
    )
    intercept[IllegalArgumentException](PythonApi.coxModel("[1]", "{}"))
    intercept[IllegalArgumentException](PythonApi.coxModel("{", "{}"))
    val tiny = frame("tiny-ties")
    val fit =
      PythonApi.coxFit(tiny, """{"time": "time", "event": "event", "features": ["x1"]}""", "{}")
    intercept[IllegalArgumentException](PythonApi.coxMeasures(tiny, fit, "stratum", "both"))
    intercept[IllegalArgumentException](
      PythonApi.coxProviderTests(tiny, fit, "stratum", "wald", 0.95)
    )
  }

  test(
    "a logistic fit through the facade equals the engine's; invalid roles are reported at once"
  ) {
    val f = Fixtures.csv("logistic/lfe-clustered/input.csv")
    val rows = f.rows.map { r =>
      Row.fromSeq(
        f.columns.indices.map(i =>
          if (Set("id", "provider", "patient", "y")(f.columns(i))) r(i).toLong else r(i)
        )
      )
    }
    val schema = StructType(f.columns.map { c =>
      StructField(
        c,
        if (Set("id", "provider", "patient", "y")(c)) LongType else DoubleType,
        nullable = false
      )
    })
    val df = spark.createDataFrame(rows.asJava, schema)
    val columns =
      """{"outcome": "y", "features": ["x1", "x2", "x3"], "provider": "provider", "rowId": "id", "cluster": "patient"}"""
    val viaFacade = PythonApi.logisticFit(df, columns, """{"tol": 1e-10}""")
    val direct = pprof.spark.engine.logistic.LogisticFE.fit(
      df,
      pprof.spark.engine.logistic
        .LogisticSpec("y", Seq("x1", "x2", "x3"), "provider", None, Some("id"), Some("patient")),
      pprof.spark.engine.logistic.LogisticOptions(tol = 1e-10)
    )
    assertEquals(bits(viaFacade.estimates), bits(direct.estimates))
    assertEquals(bits(viaFacade.robustCovariance.get), bits(direct.robustCovariance.get))
    val summary = new ObjectMapper().readTree(PythonApi.logisticSummary(viaFacade))
    assertEquals(java.lang.Double.parseDouble(summary.get("auc").asText()), direct.auc.get)
    assertEquals(summary.get("columns").get("cluster").asText(), "patient")
    val wald =
      new ObjectMapper().readTree(PythonApi.logisticWald(viaFacade, 0.0, "two_sided", 0.95, true))
    assertEquals(wald.get(0).get("method").asText(), "wald-robust")
    val failure = intercept[IllegalArgumentException](
      PythonApi.logisticModel(
        """{"outcome": 1, "colour": "red"}""",
        """{"tol": "small", "bound": -1}"""
      )
    )
    Seq("columns.outcome", "columns.colour", "columns.features", "columns.provider", "fit.tol")
      .foreach { key =>
        assert(failure.getMessage.contains(key), failure.getMessage)
      }
  }
}
