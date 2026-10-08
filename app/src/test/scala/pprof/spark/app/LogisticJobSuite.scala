package pprof.spark.app

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import pprof.spark.engine.logistic.{
  EffectReference,
  LogisticFE,
  LogisticFitIO,
  LogisticProviderTests,
  LogisticStandardization
}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** The logistic job runner against the library (Phase 2e specification §6). */
class LogisticJobSuite extends SparkSuite {

  private val integral = Set("id", "provider", "patient", "y")

  private def frame(): DataFrame = {
    val table = Fixtures.csv("logistic/lfe-clustered/input.csv")
    val rows = table.rows.map(r =>
      Row.fromSeq(
        table.columns.indices.map(i => if (integral(table.columns(i))) r(i).toLong else r(i))
      )
    )
    val schema = StructType(
      table.columns.map(c =>
        StructField(c, if (integral(c)) LongType else DoubleType, nullable = false)
      )
    )
    spark.createDataFrame(rows.asJava, schema)
  }

  private def specJson(input: String, output: String): String =
    s"""{
       |  "version": 1,
       |  "model": "logistic",
       |  "input": {"path": "$input", "format": "parquet"},
       |  "columns": {"outcome": "y", "features": ["x1", "x2", "x3"], "provider": "provider", "rowId": "id",
       |              "cluster": "patient"},
       |  "fit": {"tol": 1e-10},
       |  "outputs": {"path": "$output", "fit": true, "providers": true,
       |              "covariateTests": {"methods": ["wald", "lr"], "robust": true},
       |              "providerTests": {"method": "poibin_exact", "level": 0.9},
       |              "measures": {"kinds": ["indirect", "direct"], "extremeTrials": 20},
       |              "measureTests": [{"measure": "direct_rate"}, {"measure": "gamma", "variance": "robust"}],
       |              "predictions": true}
       |}""".stripMargin

  /** Rows as comparable values, doubles by their bits, in a fixed order. */
  private def canonical(df: DataFrame, keys: String*): Seq[Seq[Any]] =
    df.orderBy(keys.head, keys.tail: _*)
      .collect()
      .toSeq
      .map(_.toSeq.map {
        case d: Double => java.lang.Double.doubleToRawLongBits(d)
        case other     => other
      })

  private lazy val job = {
    val base = Files.createTempDirectory("pprof-logistic-job")
    val input = base.resolve("input").toString
    val output = base.resolve("output").toString
    frame().write.parquet(input)
    val json = specJson(input, output)
    val spec = LogisticRunSpec.parse(json)
    (input, output, json, spec, LogisticJob.run(spark, spec, json))
  }

  test("the fit, provider table and covariate tests equal the library's bit for bit") {
    val (input, output, _, spec, record) = job
    val df = spark.read.parquet(input)
    val fit = LogisticFE.fit(df, spec.columns, spec.options)
    val loaded = LogisticFitIO.load(spark, s"$output/fit")
    assertEquals(
      loaded.estimates.map(java.lang.Double.doubleToRawLongBits),
      fit.estimates.map(java.lang.Double.doubleToRawLongBits)
    )
    assertEquals(record.fingerprint, fit.fingerprint)
    assertEquals(
      canonical(spark.read.parquet(s"$output/providers"), "provider"),
      canonical(LogisticFE.providerTable(spark, fit), "provider")
    )
    val tests = LogisticFE.waldTests(fit, robust = true) ++ LogisticFE.covariateTests(df, fit, "lr")
    assertEquals(
      canonical(spark.read.parquet(s"$output/covariate_tests"), "method", "feature"),
      canonical(LogisticJob.testsTable(spark, tests), "method", "feature")
    )
  }

  test(
    "provider tests, measures, measure tests and predictions equal the library's; the run record"
  ) {
    val (input, output, json, spec, record) = job
    val df = spark.read.parquet(input)
    val fit = LogisticFitIO.load(spark, s"$output/fit")
    assertEquals(
      canonical(spark.read.parquet(s"$output/provider_tests"), "provider"),
      canonical(LogisticProviderTests.test(df, fit, level = 0.9), "provider")
    )
    val measures = LogisticStandardization.measures(df, fit, extremeTrials = 20.0)
    Seq("indirect", "direct").foreach { kind =>
      assertEquals(
        canonical(spark.read.parquet(s"$output/measures/$kind"), "provider"),
        canonical(measures(kind), "provider")
      )
    }
    assertEquals(
      canonical(spark.read.parquet(s"$output/measure_tests/0-direct_rate"), "provider"),
      canonical(LogisticStandardization.test(df, fit, "direct_rate"), "provider")
    )
    assertEquals(
      canonical(spark.read.parquet(s"$output/measure_tests/1-gamma"), "provider"),
      canonical(
        LogisticStandardization
          .test(df, fit, "gamma", variance = "robust", reference = EffectReference.Median),
        "provider"
      )
    )
    assertEquals(
      canonical(spark.read.parquet(s"$output/predictions"), "id"),
      canonical(LogisticFE.predict(df, fit), "id")
    )
    val written =
      new ObjectMapper().readTree(spark.read.text(s"$output/run").collect().head.getString(0))
    assertEquals(written.get("model").asText(), "logistic")
    assert(written.get("converged").asBoolean())
    assertEquals(written.get("outputs").size(), 9)
    assertEquals(record.outputs.size, 9)
    assert(written.get("clusters").asLong() > 0L && written.has("auc"))
    assertEquals(written.get("options").get("tol").asDouble(), 1e-10)
    intercept[Exception](LogisticJob.run(spark, spec, json))
  }

  test("an invalid specification lists every problem at once") {
    val failure = intercept[IllegalArgumentException](
      LogisticRunSpec.parse(
        """{"version": 1, "model": "logistic", "input": {"path": "a", "table": "b"},
          | "columns": {"outcome": 1, "features": ["x"], "time": "t"},
          | "fit": {"tol": "small", "maxIter": 2.5},
          | "outputs": {"covariateTests": {"methods": ["wald", "glm"], "robust": true},
          |             "providerTests": {"method": "midp", "level": 1.5},
          |             "measureTests": [{"measure": "odds", "variance": "robust"}]}}""".stripMargin
      )
    )
    Seq(
      "input needs exactly one of path and table",
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.time does not belong",
      "fit.tol must be a number",
      "fit.maxIter must be an integer",
      "outputs.path is required",
      "unknown method glm",
      "outputs.covariateTests.robust needs columns.cluster",
      "outputs.providerTests.method must be one of",
      "outputs.providerTests.level must lie in (0, 1)",
      "unknown measure odds",
      "variance robust needs columns.cluster"
    ).foreach(problem =>
      assert(failure.getMessage.contains(problem), s"missing: $problem in ${failure.getMessage}")
    )
  }

  test(
    "Cox and logistic jobs refuse each other's specifications; Cox specifications without a model still parse"
  ) {
    val cox =
      """{"version": 1, "input": {"path": "a"}, "columns": {"time": "t", "event": "e", "features": ["x"]},
                |  "outputs": {"path": "o"}}""".stripMargin
    assertEquals(RunSpec.parse(cox).columns.time, "t")
    assert(
      intercept[IllegalArgumentException](LogisticRunSpec.parse(cox)).getMessage.contains("CoxJob")
    )
    val logistic = cox.replace("\"version\": 1,", "\"version\": 1, \"model\": \"logistic\",")
    assert(
      intercept[IllegalArgumentException](RunSpec.parse(logistic)).getMessage
        .contains("LogisticJob")
    )
  }

  test("arguments: --spec reads a path and --spec-json takes the text") {
    assertEquals(LogisticJob.specification(Array("--spec", "/x"), path => s"read $path"), "read /x")
    assertEquals(LogisticJob.specification(Array("--spec-json", "{}"), _ => ""), "{}")
    intercept[IllegalArgumentException](LogisticJob.specification(Array("--other"), _ => ""))
  }
}
