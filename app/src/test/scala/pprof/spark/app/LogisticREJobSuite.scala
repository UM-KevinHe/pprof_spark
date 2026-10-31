package pprof.spark.app

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.logistic._
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LogisticREJob` (docs/spec/logistic/random-effect-job.md §7): every output equals the library's result on the
  * job's own fit, bit for bit; the run record lists everything; invalid specifications list every problem; the six
  * jobs refuse each other's specifications; outputs are never overwritten.
  */
class LogisticREJobSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(8, "min")

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

  /** Writes lgre-text as Parquet and runs the job with `outputs`. */
  private def runJob(outputs: String): (String, DataFrame, RunRecord, String) = {
    val dir = Files.createTempDirectory("pprof-logistic-re-job").toString
    val inputPath = s"$dir/input"
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("logistic-re/lgre-text/input.csv").toString)
      .write
      .parquet(inputPath)
    val json =
      s"""{"version": 1, "model": "logistic-re", "input": {"path": "$inputPath"},
         |"columns": {"outcome": "y", "features": ["x1", "x2"], "provider": "provider", "rowId": "id"},
         |"fit": {"stage2": true}, "outputs": {"path": "$dir/out", "fit": true, $outputs}}""".stripMargin
    val record = LogisticREJob.run(spark, LogisticRERunSpec.parse(json), json)
    (s"$dir/out", spark.read.parquet(inputPath), record, json)
  }

  test(
    "every output equals the library's on the job's fit, bit for bit; the run record lists them"
  ) {
    val (out, df, record, _) = runJob(
      """"providers": true, "summary": {"level": 0.9},
        |"providerTests": [{"method": "wald", "reference": "median", "alternative": "greater"},
        |  {"method": "poibin_exact", "critical": 2.5}, {"method": "resampling", "nResample": 2000, "seed": 7}],
        |"alphaIntervals": {"level": 0.9}, "measures": {"kinds": ["indirect", "direct"], "reference": "mean"},
        |"measureIntervals": {"kinds": ["indirect", "direct"], "measures": ["ratio", "rate"], "reference": 0.1,
        |  "alternative": "less", "level": 0.9},
        |"profile": {"level": 0.9}, "predictions": {"useRe": true, "type": "link"}, "fitted": true""".stripMargin
    )
    val fit = LogisticREFitIO.load(spark, s"$out/fit")
    def read(name: String): DataFrame = spark.read.parquet(s"$out/$name")
    assertEquals(rows(read("providers"), "provider"), rows(fit.providers, "provider"))
    assertEquals(
      rows(read("summary"), "feature"),
      rows(LogisticREJob.summaryFrame(spark, LogisticREInference.summary(fit, 0.9)), "feature")
    )
    assertEquals(
      rows(read("provider_tests/wald"), "provider"),
      rows(LogisticREInference.test(df, fit, "wald", EffectReference.Median, "greater"), "provider")
    )
    assertEquals(
      rows(read("provider_tests/poibin_exact"), "provider"),
      rows(LogisticREInference.test(df, fit, "poibin_exact", critical = Some(2.5)), "provider")
    )
    assertEquals(
      rows(read("provider_tests/resampling"), "provider"),
      rows(LogisticREInference.test(df, fit, "resampling", nResample = 2000, seed = 7L), "provider")
    )
    assertEquals(
      rows(read("alpha_intervals"), "provider"),
      rows(LogisticREInference.alphaIntervals(fit, 0.9), "provider")
    )
    val measures =
      LogisticREInference.measures(df, fit, Seq("indirect", "direct"), EffectReference.Mean)
    Seq("indirect", "direct").foreach(kind =>
      assertEquals(rows(read(s"measures/$kind"), "provider"), rows(measures(kind), "provider"))
    )
    val intervals = LogisticREInference.smIntervals(
      df,
      fit,
      Seq("indirect", "direct"),
      Seq("ratio", "rate"),
      EffectReference.Value(0.1),
      "less",
      0.9
    )
    intervals.foreach { case (key, table) =>
      assertEquals(rows(read(s"measure_intervals/$key"), "provider"), rows(table, "provider"))
    }
    assertEquals(
      rows(read("profile"), "sigma"),
      rows(
        LogisticREJob.profileFrame(spark, LogisticREInference.profileSigma(df, fit, 0.9)),
        "sigma"
      )
    )
    assertEquals(
      rows(read("predictions"), "id"),
      rows(LogisticRE.predict(df, fit, useRe = true, `type` = "link"), "id")
    )
    assertEquals(rows(read("fitted"), "id"), rows(LogisticRE.fitted(df, fit), "id"))
    val written =
      new ObjectMapper().readTree(spark.read.text(s"$out/run").collect().head.getString(0))
    assertEquals(written.get("model").asText(), "logistic-re")
    assertEquals(written.get("providers").asInt(), 30)
    assertEquals(written.get("criterion").asText(), "Laplace")
    assert(written.get("fitOptions").get("sigmaUpper").isNull)
    assertEquals(record.fingerprint, fit.fingerprint)
    val expected =
      Seq("fit", "providers", "summary", "alpha_intervals", "profile", "predictions", "fitted") ++
        Seq("wald", "poibin_exact", "resampling").map(m => s"provider_tests/$m") ++
        Seq("indirect", "direct").map(k => s"measures/$k") ++
        Seq("indirect_ratio", "indirect_rate", "direct_ratio", "direct_rate").map(k =>
          s"measure_intervals/$k"
        )
    assertEquals(record.outputs.toSet, expected.map(n => s"$out/$n").toSet)
  }

  test("a second run to the same path fails rather than overwriting") {
    val (out, _, _, json) = runJob(""""providers": true""")
    val again = json.replaceAll("\"path\": \"[^\"]*/out\"", s"\"path\": \"$out\"")
    intercept[Exception](LogisticREJob.run(spark, LogisticRERunSpec.parse(again), again))
  }

  test(
    "an invalid specification lists every problem; the six jobs refuse each other's specifications"
  ) {
    val bad =
      """{"version": 2, "model": "logistic-re", "input": {}, "columns": {"outcome": 3, "weights": "w"},
        |"fit": {"reml": true, "stage2": false, "maxIterations": 5},
        |"outputs": {"providerTests": [{"method": "exact"}, {"method": "wald", "critical": -1}, {"method": "wald"}],
        |"profile": {"level": 0.9}, "predictions": 3}}""".stripMargin
    val message = intercept[IllegalArgumentException](LogisticRERunSpec.parse(bad)).getMessage
    Seq(
      "version 2 is not supported",
      "input needs exactly one of path and table",
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.weights does not belong to a logistic-re specification",
      "fit.reml does not belong to a logistic-re specification",
      "maxIterations must be at least 20",
      "outputs.path is required",
      "method exact needs a cluster factor",
      "critical must be positive",
      "lists method wald twice",
      "outputs.profile needs the Laplace fit",
      "outputs.predictions must be true, false or an object"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val re =
      """{"version": 1, "model": "logistic-re", "input": {"path": "x"}, "columns": {}, "outputs": {"path": "y"}}"""
    Seq[(String, String => Any)](
      "RunSpec" -> RunSpec.parse,
      "LogisticRunSpec" -> LogisticRunSpec.parse,
      "ThreeStageRunSpec" -> ThreeStageRunSpec.parse,
      "LinearRunSpec" -> LinearRunSpec.parse,
      "LinearRERunSpec" -> LinearRERunSpec.parse
    ).foreach { case (name, parse) =>
      val refused = intercept[IllegalArgumentException](parse(re)).getMessage
      assert(refused.contains("LogisticREJob"), s"$name: $refused")
    }
    Seq(
      "cox" -> "CoxJob",
      "logistic" -> "LogisticJob",
      "three-stage" -> "ThreeStageJob",
      "linear" -> "LinearJob",
      "linear-re" -> "LinearREJob"
    ).foreach { case (model, job) =>
      val other = re.replace("\"logistic-re\"", s"\"$model\"")
      assert(
        intercept[IllegalArgumentException](LogisticRERunSpec.parse(other)).getMessage
          .contains(job),
        model
      )
    }
  }

  test("argument handling") {
    assertEquals(LogisticREJob.specification(Array("--spec-json", "{}"), _ => "unused"), "{}")
    assertEquals(LogisticREJob.specification(Array("--spec", "p"), path => s"read $path"), "read p")
    intercept[IllegalArgumentException](LogisticREJob.specification(Array("--other"), _ => ""))
  }
}
