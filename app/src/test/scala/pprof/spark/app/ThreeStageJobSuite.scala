package pprof.spark.app

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pprof.spark.engine.logistic._
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** The three-stage job (docs/spec/logistic/three-stage-pipeline.md §5). */
class ThreeStageJobSuite extends SparkSuite {

  // Each test runs the whole three-stage pipeline as a job and again through the library: allow four minutes
  // (SparkSuite allows two), so slower CI runners do not time out (OI-44).
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(4, "min")

  private val spec = ThreeStageSpec("Y", Seq("x1", "x2", "x3"), "fac", "hosp", Some("rid"))

  private def bits(df: DataFrame, order: String, column: String): Seq[Long] =
    df.orderBy(order)
      .select(column)
      .collect()
      .toSeq
      .map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  /** Writes ts-synthetic as Parquet and runs the job with `outputs`. */
  private def runJob(outputs: String): (String, DataFrame, RunRecord, String) = {
    val dir = Files.createTempDirectory("pprof-three-stage-job").toString
    val inputPath = s"$dir/input"
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("three-stage/ts-synthetic/input.csv").toString)
      .write
      .parquet(inputPath)
    val json =
      s"""{"version": 1, "model": "three-stage", "input": {"path": "$inputPath"},
         |"columns": {"outcome": "Y", "features": ["x1", "x2", "x3"], "provider": "fac", "cluster": "hosp", "rowId": "rid"},
         |"fit": {"cutoff": 10}, "outputs": {"path": "$dir/out", "fit": true, $outputs}}""".stripMargin
    val record = ThreeStageJob.run(spark, ThreeStageRunSpec.parse(json), json)
    (s"$dir/out", spark.read.parquet(inputPath), record, json)
  }

  test("the job's tables equal the library's on the saved fit, which round-trips and re-attaches") {
    val (out, df, record, _) = runJob(
      """"providers": true, "clusters": true, "tests": [{"method": "poibin_exact"}], "measures": {"kinds": ["indirect", "direct"]},
        |"fitted": true""".stripMargin
    )
    val saved = ThreeStageFitIO.load(spark, s"$out/fit")
    val fit = ThreeStageFitIO.attach(df, saved)
    val records = fit.preparation.data
    assertEquals(
      bits(spark.read.parquet(s"$out/providers"), "fac", "gamma"),
      bits(saved.stage3.gamma.toSeq)
    )
    assertEquals(
      bits(spark.read.parquet(s"$out/clusters"), "hosp", "alpha_mean"),
      bits(saved.stage3.alphaMean.toSeq)
    )
    val exact = ThreeStagePipeline.test(records, spec, fit.stage3, "poibin_exact")
    assertEquals(
      bits(spark.read.parquet(s"$out/tests/0-poibin_exact"), "fac", "z_raw"),
      bits(exact, "fac", "z_raw")
    )
    val measures = ThreeStagePipeline.measures(records, fit.cells, spec, fit.stage3)
    assertEquals(
      bits(spark.read.parquet(s"$out/measures/direct"), "fac", "direct_ratio"),
      bits(measures("direct"), "fac", "direct_ratio")
    )
    assertEquals(
      bits(spark.read.parquet(s"$out/measures/indirect"), "fac", "expected"),
      bits(measures("indirect"), "fac", "expected")
    )
    assertEquals(
      bits(spark.read.parquet(s"$out/tests/0-poibin_exact"), "fac", "ci_lower"),
      bits(exact, "fac", "ci_lower")
    )
    assertEquals(
      bits(spark.read.parquet(s"$out/fitted"), "rid", "fitted"),
      bits(ThreeStage.fitted(records, spec, fit.stage3), "rid", "fitted")
    )
    val refit = ThreeStage.stage3(
      fit.cells,
      saved.stage2.sigmaCluster,
      saved.stage2.start,
      saved.options.stage3
    )
    assertEquals(bits(refit.gamma.toSeq), bits(saved.stage3.gamma.toSeq))
    intercept[IllegalArgumentException](ThreeStageFitIO.attach(df.filter(col("rid") =!= 1), saved))
    assert(record.json.contains("\"model\":\"three-stage\""), record.json)
    assertEquals(record.outputs.size, 7)
  }

  test("the job's sensitivity, resampling and measure intervals agree with the library") {
    val (out, df, _, _) = runJob(
      """"tests": [{"method": "resampling", "nResample": 2000, "seed": 4}],
        |"intervals": [{"option": "SM", "kinds": ["indirect"], "measure": ["rate"]}], "sensitivity": {}""".stripMargin
    )
    val saved = ThreeStageFitIO.load(spark, s"$out/fit")
    val fit = ThreeStageFitIO.attach(df, saved)
    val records = fit.preparation.data
    assertEquals(
      bits(spark.read.parquet(s"$out/tests/0-resampling"), "fac", "p_value"),
      bits(
        ThreeStagePipeline
          .test(records, spec, fit.stage3, "resampling", nResample = 2000, seed = 4L),
        "fac",
        "p_value"
      )
    )
    assert(spark.read.parquet(s"$out/intervals/0-indirect_rate").columns.contains("ci_rate_upper"))
    val sigma = spark.read.parquet(s"$out/sensitivity/sigma").collect().head
    assertEquals(bits(Seq(sigma.getAs[Double]("estimate"))), bits(Seq(saved.stage2.sigmaCluster)))
    assertEquals(sigma.getAs[Double]("lower"), 0.0)
    assert(sigma.getAs[Double]("estimate") < sigma.getAs[Double]("upper"))
    val flags = spark.read.parquet(s"$out/sensitivity/flags").orderBy("fac").collect()
    val estimate = spark.read
      .parquet(s"$out/sensitivity/tests/estimate")
      .orderBy("fac")
      .select("flag")
      .collect()
      .map(_.getInt(0))
      .toSeq
    assertEquals(flags.map(_.getAs[Int]("estimate")).toSeq, estimate)
    assert(flags.forall { r =>
      r.getAs[Boolean]("stable") == (r.getAs[Int]("lower") == r.getAs[Int]("estimate") && r
        .getAs[Int]("estimate") == r.getAs[Int]("upper"))
    })
  }

  test(
    "invalid three-stage specifications list every problem; the other jobs point here; outputs are never overwritten"
  ) {
    val (_, _, _, json) = runJob(""""providers": true""")
    intercept[Exception](ThreeStageJob.run(spark, ThreeStageRunSpec.parse(json), json))
    val bad =
      """{"version": 1, "model": "three-stage", "input": {"path": "x", "table": "y"}, "columns": {"outcome": 3},
        |"fit": {"stage3": {"boundMode": "loose", "nodes": 3}},
        |"outputs": {"tests": [{"method": "wald", "level": 2}], "measures": {"kinds": ["odd"]}, "extra": true}}""".stripMargin
    val message = intercept[IllegalArgumentException](ThreeStageRunSpec.parse(bad)).getMessage
    Seq(
      "input needs exactly one",
      "columns.outcome must be a string",
      "columns.features is required",
      "columns.provider is required",
      "fit.stage3.nodes does not belong",
      "fit.stage3.boundMode must be one of",
      "outputs.tests[0].method must be one of",
      "outputs.tests[0].level must lie",
      "unknown kind odd",
      "outputs.extra does not belong",
      "outputs.path is required"
    )
      .foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val threeStage = """{"version": 1, "model": "three-stage"}"""
    assert(
      intercept[IllegalArgumentException](RunSpec.parse(threeStage)).getMessage
        .contains("ThreeStageJob")
    )
    assert(
      intercept[IllegalArgumentException](LogisticRunSpec.parse(threeStage)).getMessage
        .contains("ThreeStageJob")
    )
    assert(
      intercept[IllegalArgumentException](
        ThreeStageRunSpec.parse("""{"version": 1, "model": "logistic"}""")
      ).getMessage.contains("LogisticJob")
    )
  }
}
