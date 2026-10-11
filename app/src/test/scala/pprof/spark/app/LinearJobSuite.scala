package pprof.spark.app

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.linear._
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LinearJob` (docs/spec/linear/job-and-python.md §6): every output equals the library's result bit for bit, the
  * fit loads back, the run record lists everything; invalid specifications list every problem; the jobs refuse
  * each other's specifications; outputs are never overwritten.
  */
class LinearJobSuite extends SparkSuite {

  // The job and the library each fit and compute every output: allow four minutes (SparkSuite allows two), so slower
  // CI runners do not time out (OI-44).
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(4, "min")

  private val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))

  /** Every row as text, doubles as their 64-bit patterns, ordered by `order`. */
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

  /** Writes lin-base as Parquet and runs the job with `outputs`. */
  private def runJob(outputs: String): (String, DataFrame, RunRecord, String) = {
    val dir = Files.createTempDirectory("pprof-linear-job").toString
    val inputPath = s"$dir/input"
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("linear/lin-base/input.csv").toString)
      .write
      .parquet(inputPath)
    val json =
      s"""{"version": 1, "model": "linear", "input": {"path": "$inputPath"},
         |"columns": {"outcome": "y", "features": ["x1", "x2", "x3"], "provider": "provider", "rowId": "id"},
         |"fit": {"varianceOption": "complete"}, "outputs": {"path": "$dir/out", "fit": true, $outputs}}""".stripMargin
    val record = LinearJob.run(spark, LinearRunSpec.parse(json), json)
    (s"$dir/out", spark.read.parquet(inputPath), record, json)
  }

  test(
    "every output equals the library's bit for bit; the fit loads back; the run record lists them"
  ) {
    val (out, df, record, _) = runJob(
      """"providers": true, "summary": {"level": 0.9, "null": 0.25, "alternative": "greater"},
        |"providerTests": {"reference": "mean", "alternative": "two_sided", "level": 0.95, "critical": 2.5},
        |"effectIntervals": {"level": 0.9}, "measures": {"kinds": ["indirect", "direct"], "reference": "median"},
        |"measureIntervals": {"kinds": ["indirect", "direct"], "alternative": "less", "level": 0.95},
        |"predictions": true""".stripMargin
    )
    val fit = LinearFE.fit(df, spec)
    def read(name: String): DataFrame = spark.read.parquet(s"$out/$name")
    assertEquals(rows(read("providers"), "provider"), rows(fit.providers, "provider"))
    assertEquals(
      rows(read("summary"), "feature"),
      rows(LinearJob.summaryTable(spark, LinearFE.summary(fit, 0.9, 0.25, "greater")), "feature")
    )
    assertEquals(
      rows(read("provider_tests"), "provider"),
      rows(
        LinearProviderTests.test(fit, EffectReference.Mean, "two_sided", 0.95, Some(2.5)),
        "provider"
      )
    )
    assertEquals(
      rows(read("effect_intervals"), "provider"),
      rows(LinearProviderTests.gammaIntervals(fit, 0.9), "provider")
    )
    val measures = LinearStandardization.measures(df, fit)
    val intervals =
      LinearStandardization.intervals(df, fit, Seq("indirect", "direct"), alternative = "less")
    for (kind <- Seq("indirect", "direct")) {
      assertEquals(rows(read(s"measures/$kind"), "provider"), rows(measures(kind), "provider"))
      assertEquals(
        rows(read(s"measure_intervals/$kind"), "provider"),
        rows(intervals(kind), "provider")
      )
    }
    assertEquals(rows(read("predictions"), "id"), rows(LinearFE.predict(df, fit), "id"))
    val loaded = LinearFitIO.load(spark, s"$out/fit")
    assertEquals(
      loaded.estimates.map(java.lang.Double.doubleToRawLongBits),
      fit.estimates.map(java.lang.Double.doubleToRawLongBits)
    )
    assertEquals(rows(loaded.providers, "provider"), rows(fit.providers, "provider"))
    val written =
      new ObjectMapper().readTree(spark.read.text(s"$out/run").collect().head.getString(0))
    assertEquals(written.get("model").asText(), "linear")
    assertEquals(written.get("rows").asLong(), fit.rows)
    assertEquals(written.get("providers").asInt(), fit.providerCount)
    assertEquals(written.get("degreesOfFreedom").asLong(), fit.degreesOfFreedom)
    assertEquals(written.get("fitOptions").get("varianceOption").asText(), "complete")
    assertEquals(record.fingerprint, fit.fingerprint)
    val expected = Seq(
      "fit",
      "providers",
      "summary",
      "provider_tests",
      "effect_intervals",
      "measures/indirect",
      "measures/direct",
      "measure_intervals/indirect",
      "measure_intervals/direct",
      "predictions"
    ).map(n => s"$out/$n")
    assertEquals(record.outputs.toSet, expected.toSet)
  }

  test(
    "invalid linear specifications list every problem; the jobs refuse each other's; outputs are never overwritten"
  ) {
    val (_, _, _, json) = runJob(""""providers": true""")
    intercept[Exception](LinearJob.run(spark, LinearRunSpec.parse(json), json))
    val bad =
      """{"version": 1, "model": "linear", "input": {"path": "x", "table": "y"}, "columns": {"outcome": 3, "trials": "n"},
        |"fit": {"varianceOption": "full", "tol": 1e-8},
        |"outputs": {"providerTests": {"level": 2}, "measures": {"kinds": ["odd"]}, "extra": true}}""".stripMargin
    val message = intercept[IllegalArgumentException](LinearRunSpec.parse(bad)).getMessage
    Seq(
      "input needs exactly one",
      "columns.outcome must be a string",
      "columns.features is required",
      "columns.provider is required",
      "columns.trials does not belong",
      "fit.varianceOption must be one of",
      "fit.tol does not belong",
      "outputs.providerTests.level must lie",
      "unknown kind odd",
      "outputs.extra does not belong",
      "outputs.path is required"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val linear = """{"version": 1, "model": "linear"}"""
    assert(
      intercept[IllegalArgumentException](RunSpec.parse(linear)).getMessage.contains("LinearJob")
    )
    assert(
      intercept[IllegalArgumentException](LogisticRunSpec.parse(linear)).getMessage
        .contains("LinearJob")
    )
    assert(
      intercept[IllegalArgumentException](ThreeStageRunSpec.parse(linear)).getMessage
        .contains("LinearJob")
    )
    Seq("logistic" -> "LogisticJob", "three-stage" -> "ThreeStageJob", "cox" -> "CoxJob").foreach {
      case (model, job) =>
        val other = s"""{"version": 1, "model": "$model"}"""
        assert(
          intercept[IllegalArgumentException](LinearRunSpec.parse(other)).getMessage.contains(job),
          model
        )
    }
    assertEquals(LinearJob.specification(Array("--spec-json", "{}"), _ => "unused"), "{}")
    assertEquals(LinearJob.specification(Array("--spec", "p"), path => s"read $path"), "read p")
    intercept[IllegalArgumentException](LinearJob.specification(Array("--other"), _ => ""))
  }
}
