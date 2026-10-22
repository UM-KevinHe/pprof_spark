package pprof.spark.app

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.linear._
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LinearREJob` (docs/spec/linear/random-effect-job.md §7): every output equals the library's result bit for bit, the
  * fit loads back, the run record lists everything; invalid specifications list every problem; the five jobs refuse
  * each other's specifications; outputs are never overwritten.
  */
class LinearREJobSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(4, "min")

  private val spec =
    LinearRESpec("y", Seq("x1", "x2", "x3"), "provider", offset = Some("o"), rowId = Some("id"))

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

  /** Writes lre-offset as Parquet and runs the job with `outputs`. */
  private def runJob(outputs: String): (String, DataFrame, RunRecord, String) = {
    val dir = Files.createTempDirectory("pprof-linear-re-job").toString
    val inputPath = s"$dir/input"
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("linear-re/lre-offset/input.csv").toString)
      .write
      .parquet(inputPath)
    val json =
      s"""{"version": 1, "model": "linear-re", "input": {"path": "$inputPath"},
         |"columns": {"outcome": "y", "features": ["x1", "x2", "x3"], "provider": "provider", "offset": "o",
         |"rowId": "id"}, "fit": {"reml": true}, "outputs": {"path": "$dir/out", "fit": true, $outputs}}""".stripMargin
    val record = LinearREJob.run(spark, LinearRERunSpec.parse(json), json)
    (s"$dir/out", spark.read.parquet(inputPath), record, json)
  }

  test(
    "every output equals the library's bit for bit; the fit loads back; the run record lists them"
  ) {
    val (out, df, record, _) = runJob(
      """"providers": true, "summary": {"level": 0.9, "null": 0.25, "alternative": "greater"},
        |"fixedEffects": true, "confInt": {"level": 0.9},
        |"providerTests": {"reference": "median", "alternative": "two_sided", "level": 0.95, "critical": 2.5},
        |"alphaIntervals": {"level": 0.9}, "measures": {"kinds": ["indirect", "direct"], "reference": "mean"},
        |"measureIntervals": {"kinds": ["indirect", "direct"], "reference": 0.1, "alternative": "less", "level": 0.9},
        |"predictions": {"useRe": true}, "fitted": true""".stripMargin
    )
    val fit = LinearRE.fit(df, spec)
    def read(name: String): DataFrame = spark.read.parquet(s"$out/$name")
    assertEquals(rows(read("providers"), "provider"), rows(fit.providers, "provider"))
    assertEquals(
      rows(read("summary"), "feature"),
      rows(
        LinearJob.summaryTable(spark, LinearREInference.summary(fit, 0.9, 0.25, "greater")),
        "feature"
      )
    )
    assertEquals(
      rows(read("fixed_effects"), "feature"),
      rows(
        LinearREJob.fixedEffectsFrame(spark, LinearREInference.fixedEffectsTable(fit)),
        "feature"
      )
    )
    assertEquals(
      rows(read("conf_int"), "feature"),
      rows(LinearREJob.confIntFrame(spark, LinearREInference.confInt(fit, 0.9)), "feature")
    )
    assertEquals(
      rows(read("provider_tests"), "provider"),
      rows(
        LinearREInference.test(fit, EffectReference.Median, "two_sided", 0.95, Some(2.5)),
        "provider"
      )
    )
    assertEquals(
      rows(read("alpha_intervals"), "provider"),
      rows(LinearREInference.alphaIntervals(fit, 0.9), "provider")
    )
    val measures =
      LinearREInference.measures(df, fit, Seq("indirect", "direct"), EffectReference.Mean)
    val intervals =
      LinearREInference.smIntervals(
        df,
        fit,
        Seq("indirect", "direct"),
        EffectReference.Value(0.1),
        "less",
        0.9
      )
    Seq("indirect", "direct").foreach { kind =>
      assertEquals(rows(read(s"measures/$kind"), "provider"), rows(measures(kind), "provider"))
      assertEquals(
        rows(read(s"measure_intervals/$kind"), "provider"),
        rows(intervals(kind), "provider")
      )
    }
    assertEquals(
      rows(read("predictions"), "id"),
      rows(LinearRE.predict(df, fit, useRe = true), "id")
    )
    assertEquals(rows(read("fitted"), "id"), rows(LinearRE.fitted(df, fit), "id"))
    val loaded = LinearREFitIO.load(spark, s"$out/fit")
    assertEquals(
      loaded.estimates.map(java.lang.Double.doubleToRawLongBits),
      fit.estimates.map(java.lang.Double.doubleToRawLongBits)
    )
    assertEquals(rows(loaded.providers, "provider"), rows(fit.providers, "provider"))
    val written =
      new ObjectMapper().readTree(spark.read.text(s"$out/run").collect().head.getString(0))
    assertEquals(written.get("model").asText(), "linear-re")
    assertEquals(written.get("rows").asLong(), fit.rows)
    assertEquals(written.get("providers").asInt(), fit.providerCount)
    assertEquals(written.get("criterion").asText(), "REML")
    assertEquals(written.get("search").get("path").asText(), fit.search.path)
    assert(written.get("fitOptions").get("thetaUpper").isNull)
    assertEquals(record.fingerprint, fit.fingerprint)
    val expected = Seq(
      "fit",
      "providers",
      "summary",
      "fixed_effects",
      "conf_int",
      "provider_tests",
      "alpha_intervals"
    ) ++
      Seq("indirect", "direct").flatMap(k => Seq(s"measures/$k", s"measure_intervals/$k")) ++ Seq(
        "predictions",
        "fitted"
      )
    assertEquals(record.outputs.toSet, expected.map(n => s"$out/$n").toSet)
  }

  test("a second run to the same path fails rather than overwriting") {
    val (out, _, _, json) = runJob(""""providers": true""")
    val again = json.replaceAll("\"path\": \"[^\"]*/out\"", s"\"path\": \"$out\"")
    intercept[Exception](LinearREJob.run(spark, LinearRERunSpec.parse(again), again))
  }

  test(
    "an invalid specification lists every problem; the five jobs refuse each other's specifications"
  ) {
    val bad =
      """{"version": 2, "model": "linear-re", "input": {}, "columns": {"outcome": 3, "trials": "n"},
        |"fit": {"varianceOption": "complete", "maxEvaluations": 5, "reml": "yes"},
        |"outputs": {"providerTests": {"critical": -1}, "predictions": 3}}""".stripMargin
    val message = intercept[IllegalArgumentException](LinearRERunSpec.parse(bad)).getMessage
    Seq(
      "version 2 is not supported",
      "input needs exactly one of path and table",
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.trials does not belong to a linear-re specification",
      "fit.varianceOption does not belong to a linear-re specification",
      "fit.reml must be true or false",
      "maxEvaluations must be at least 20",
      "outputs.path is required",
      "outputs.providerTests.critical must be positive",
      "outputs.predictions must be true, false or an object with useRe"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val re =
      """{"version": 1, "model": "linear-re", "input": {"path": "x"}, "columns": {}, "outputs": {"path": "y"}}"""
    Seq[(String, String => Any)](
      "RunSpec" -> RunSpec.parse,
      "LogisticRunSpec" -> LogisticRunSpec.parse,
      "ThreeStageRunSpec" -> ThreeStageRunSpec.parse,
      "LinearRunSpec" -> LinearRunSpec.parse
    ).foreach { case (name, parse) =>
      val refused = intercept[IllegalArgumentException](parse(re)).getMessage
      assert(refused.contains("LinearREJob"), s"$name: $refused")
    }
    Seq(
      "cox" -> "CoxJob",
      "logistic" -> "LogisticJob",
      "three-stage" -> "ThreeStageJob",
      "linear" -> "LinearJob"
    )
      .foreach { case (model, job) =>
        val other = re.replace("\"linear-re\"", s"\"$model\"")
        assert(
          intercept[IllegalArgumentException](LinearRERunSpec.parse(other)).getMessage
            .contains(job),
          model
        )
      }
  }

  test("argument handling") {
    assertEquals(LinearREJob.specification(Array("--spec-json", "{}"), _ => "unused"), "{}")
    assertEquals(LinearREJob.specification(Array("--spec", "p"), path => s"read $path"), "read p")
    intercept[IllegalArgumentException](LinearREJob.specification(Array("--other"), _ => ""))
  }
}
