package pprof.spark.app

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.cox.{
  CoxFitIO,
  CoxMeasures,
  CoxPH,
  CoxProviderTests,
  Standardization,
  TestMethod
}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** The job runner (Phase 1d specification §5, §7): its outputs equal the library's bit for bit. */
class CoxJobSuite extends SparkSuite {

  private val name = "lt-weights-offset"

  private def frame(): DataFrame = {
    val info = Fixtures.json(s"cox/$name/case.json")
    val features =
      (0 until info.get("features").size).map(i => info.get("features").get(i).asText())
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val names = Seq("id", "stratum", "time", "event", "weight", "offset", "entry") ++ features
    val index = names.map(table.columns.indexOf(_))
    val rows = table.rows.map { r =>
      Row.fromSeq(
        Seq[Any](r(index(0)).toLong, r(index(1)).toLong, r(index(2)), r(index(3)).toInt) ++
          index.drop(4).map(r(_))
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("stratum", LongType),
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("weight", DoubleType),
        StructField("offset", DoubleType),
        StructField("entry", DoubleType)
      ) ++ features.map(StructField(_, DoubleType))
    )
    spark.createDataFrame(rows.asJava, schema)
  }

  private def specJson(input: String, output: String): String =
    s"""{
       |  "version": 1,
       |  "input": {"path": "$input", "format": "parquet"},
       |  "columns": {"time": "time", "event": "event", "features": ["x1", "x2", "x3"],
       |              "strata": "stratum", "rowId": "id", "weight": "weight", "offset": "offset",
       |              "entry": "entry"},
       |  "fit": {"ties": "efron", "eps": 1e-10},
       |  "outputs": {"path": "$output", "fit": true, "baseline": true, "residuals": true,
       |              "measures": {"provider": "stratum", "kinds": ["indirect", "direct"]},
       |              "tests": {"provider": "stratum", "method": "exact", "level": 0.9}}
       |}""".stripMargin

  /** Rows as comparable values, doubles by their bits, in a fixed order. */
  private def canonical(df: DataFrame, key: String): Seq[Seq[Any]] =
    df.orderBy(key)
      .collect()
      .toSeq
      .map(_.toSeq.map {
        case d: Double => java.lang.Double.doubleToRawLongBits(d)
        case other     => other
      })

  test("the job's outputs equal the library's bit for bit") {
    val base = Files.createTempDirectory("pprof-job")
    val input = base.resolve("input").toString
    val output = base.resolve("output").toString
    frame().write.parquet(input)
    val json = specJson(input, output)
    val spec = RunSpec.parse(json)
    val record = CoxJob.run(spark, spec, json)

    val df = spark.read.parquet(input)
    val fit = CoxPH.fit(df, spec.columns, spec.options)
    assertEquals(CoxFitIO.load(spark, s"$output/fit"), fit)
    assertEquals(
      canonical(
        CoxFitIO.loadBaseline(spark, s"$output/fit").getOrElse(fail("no baseline")),
        "time"
      ),
      canonical(CoxPH.baseline(df, fit), "time")
    )
    assertEquals(
      canonical(spark.read.parquet(s"$output/residuals"), "id"),
      canonical(CoxPH.residuals(df, fit), "id")
    )
    val measures = CoxMeasures.standardized(
      df,
      fit,
      "stratum",
      Seq(Standardization.Indirect, Standardization.Direct)
    )
    for ((kind, table) <- measures)
      assertEquals(
        canonical(spark.read.parquet(s"$output/measures/${kind.name}"), "stratum"),
        canonical(table, "stratum")
      )
    assertEquals(
      canonical(spark.read.parquet(s"$output/tests"), "stratum"),
      canonical(CoxProviderTests.test(df, fit, "stratum", TestMethod.Exact, 0.9), "stratum")
    )

    val written =
      new ObjectMapper().readTree(spark.read.text(s"$output/run").collect().head.getString(0))
    assertEquals(written.get("fingerprint").asLong, fit.fingerprint)
    assertEquals(written.get("converged").asBoolean, true)
    assertEquals(written.get("specification").get("version").asInt, 1)
    assertEquals(record.outputs.size, 5)
    assert(written.get("timingsMillis").has("fit"))
    intercept[Exception](CoxJob.run(spark, spec, json))
  }

  test("an invalid specification lists every problem") {
    val e = intercept[IllegalArgumentException](
      RunSpec.parse(
        """{"version": 2, "input": {"path": "a", "table": "b"},
          | "columns": {"features": "x1"}, "fit": {"ties": "exact"},
          | "outputs": {"tests": {"method": "score", "level": 1.5}}}""".stripMargin
      )
    )
    for (
      expected <- Seq(
        "version 2 is not supported",
        "input needs exactly one of path and table",
        "columns.time is required",
        "columns.event is required",
        "columns.features must be a list of strings",
        "fit.ties must be breslow or efron",
        "outputs.tests.method must be midp or exact",
        "outputs.tests.level must lie in (0, 1)",
        "outputs.tests.provider is required",
        "outputs.path is required"
      )
    )
      assert(e.getMessage.contains(expected), s"missing '$expected' in ${e.getMessage}")
  }

  test("arguments: --spec reads a path and --spec-json takes the text") {
    assertEquals(CoxJob.specification(Array("--spec-json", "{}"), _ => fail("no read")), "{}")
    assertEquals(CoxJob.specification(Array("--spec", "/x"), path => s"read $path"), "read /x")
    intercept[IllegalArgumentException](CoxJob.specification(Array("--other"), identity))
  }
}
