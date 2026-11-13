package pprof.spark.app

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.logistic._
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `LogisticFERandomClusterJob` (docs/spec/logistic/fe-random-cluster-job.md §7): every output equals the library's on
  * the saved fit; the stages source reproduces a three-stage fit's stage 3; refusals and arguments.
  */
class LogisticFERandomClusterJobSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

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

  private def hexes(values: Array[Double]): String = values.map(_.toString).mkString("[", ", ", "]")

  test(
    "the given source with every output: each table equals the library's on the saved fit, bit for bit"
  ) {
    val dir = Files.createTempDirectory("pprof-frc-job").toString
    val inputPath = s"$dir/input"
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("fe-random-cluster/frc-base/input.csv").toString)
      .write
      .parquet(inputPath)
    val g = Fixtures.json("fe-random-cluster/frc-base/pprof_py.json").get("given")
    val (beta, sigma, start) = (
      Fixtures.doubles(g.get("beta")),
      Fixtures.doubles(g.get("sigma")).head,
      Fixtures.doubles(g.get("start"))
    )
    val json =
      s"""{"version": 1, "model": "fe-random-cluster", "input": {"path": "$inputPath"},
         |"columns": {"outcome": "y", "features": ["x1", "x2"], "provider": "provider", "cluster": "cluster", "rowId": "id"},
         |"fit": {"given": {"beta": ${hexes(beta)}, "sigma": $sigma, "start": ${hexes(start)}}},
         |"outputs": {"path": "$dir/out", "fit": true, "providers": true, "clusters": true,
         |  "providerTests": [{"method": "exact"}, {"method": "poibin_exact", "reference": 0.0, "alternative": "greater"}],
         |  "gammaIntervals": {"method": "poibin_exact", "level": 0.9},
         |  "measures": {"kinds": ["indirect", "direct"], "reference": "mean"},
         |  "measureIntervals": {"kinds": ["indirect"], "measures": ["ratio", "rate"], "alternative": "less"},
         |  "fitted": true}}""".stripMargin
    val record =
      LogisticFERandomClusterJob.run(spark, LogisticFERandomClusterRunSpec.parse(json), json)
    val df = spark.read.parquet(inputPath)
    val fit = LogisticFERandomClusterFitIO.attach(
      df,
      LogisticFERandomClusterFitIO.load(spark, s"$dir/out/fit")
    )
    def read(name: String) = spark.read.parquet(s"$dir/out/$name")
    val api = pprof.spark.app.python.PythonApi
    assertEquals(
      rows(read("providers"), "provider"),
      rows(api.feRandomClusterProviders(spark, fit), "provider")
    )
    assertEquals(
      rows(read("clusters"), "cluster"),
      rows(api.feRandomClusterClusters(spark, fit), "cluster")
    )
    assertEquals(
      rows(read("provider_tests/exact"), "provider"),
      rows(LogisticFERandomCluster.test(fit), "provider")
    )
    assertEquals(
      rows(read("provider_tests/poibin_exact"), "provider"),
      rows(
        LogisticFERandomCluster.test(fit, "poibin_exact", EffectReference.Value(0.0), "greater"),
        "provider"
      )
    )
    assertEquals(
      rows(read("gamma_intervals"), "provider"),
      rows(
        LogisticFERandomCluster.intervals(fit, "gamma", level = 0.9, method = "poibin_exact")(
          "gamma_ci"
        ),
        "provider"
      )
    )
    val measures =
      LogisticFERandomCluster.measures(fit, Seq("indirect", "direct"), EffectReference.Mean)
    Seq("indirect", "direct").foreach(k =>
      assertEquals(rows(read(s"measures/$k"), "provider"), rows(measures(k), "provider"))
    )
    LogisticFERandomCluster
      .intervals(fit, "SM", Seq("indirect"), Seq("ratio", "rate"), "less")
      .foreach { case (k, t) =>
        assertEquals(rows(read(s"measure_intervals/$k"), "provider"), rows(t, "provider"))
      }
    assertEquals(rows(read("fitted"), "id"), rows(LogisticFERandomCluster.fitted(fit), "id"))
    val written =
      new ObjectMapper().readTree(spark.read.text(s"$dir/out/run").collect().head.getString(0))
    assertEquals(
      (
        written.get("model").asText(),
        written.get("source").asText(),
        written.get("providers").asInt()
      ),
      ("fe-random-cluster", "given", 30)
    )
    assertEquals(record.fingerprint, fit.fingerprint)
    val expected = Seq(
      "fit",
      "providers",
      "clusters",
      "provider_tests/exact",
      "provider_tests/poibin_exact",
      "gamma_intervals",
      "measures/indirect",
      "measures/direct",
      "measure_intervals/indirect_ratio",
      "measure_intervals/indirect_rate",
      "fitted"
    )
    assertEquals(record.outputs.toSet, expected.map(n => s"$dir/out/$n").toSet)
    intercept[Exception](
      LogisticFERandomClusterJob.run(spark, LogisticFERandomClusterRunSpec.parse(json), json)
    )
  }

  test(
    "the stages source: saved stage 1 and three-stage fits give the three-stage fit's own stage 3, with a summary"
  ) {
    val dir = Files.createTempDirectory("pprof-frc-stages").toString
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("three-stage/ts-synthetic/input.csv").toString)
    val three = ThreeStagePipeline.fit(
      raw,
      ThreeStageSpec("Y", Seq("x1", "x2", "x3"), "fac", "hosp", Some("rid"))
    )
    LogisticFitIO.save(spark, three.preparation.stage1, s"$dir/stage1")
    ThreeStageFitIO.save(spark, three, s"$dir/three")
    three.preparation.data.write.parquet(s"$dir/input")
    val json =
      s"""{"version": 1, "model": "fe-random-cluster", "input": {"path": "$dir/input"},
         |"columns": {"outcome": "y_adj", "observed": "Y", "features": ["x1", "x2", "x3"], "provider": "fac", "cluster": "hosp", "rowId": "rid"},
         |"fit": {"stages": {"stage1": "$dir/stage1", "stage2": "$dir/three"}},
         |"outputs": {"path": "$dir/out", "fit": true, "summary": {"level": 0.9}}}""".stripMargin
    LogisticFERandomClusterJob.run(spark, LogisticFERandomClusterRunSpec.parse(json), json)
    val saved = LogisticFERandomClusterFitIO.load(spark, s"$dir/out/fit")
    assertEquals(
      saved.stage3.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits),
      three.stage3.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits)
    )
    assert(saved.withStage1)
    assertEquals(spark.read.parquet(s"$dir/out/summary").count(), 3L)
  }

  test(
    "an invalid specification lists every problem; the seven jobs refuse each other's specifications; arguments"
  ) {
    val bad =
      """{"version": 2, "model": "fe-random-cluster", "input": {}, "columns": {"outcome": 3, "weights": "w"},
        |"fit": {"estimator": "other", "given": {"beta": [0.1]}, "stages": {"stage1": "a"}},
        |"outputs": {"summary": {}, "providerTests": [{"method": "wald"}, {"method": "exact"}, {"method": "exact", "critical": -1}]}}""".stripMargin
    val message =
      intercept[IllegalArgumentException](LogisticFERandomClusterRunSpec.parse(bad)).getMessage
    Seq(
      "version 2 is not supported",
      "input needs exactly one of path and table",
      "columns.outcome must be a string",
      "columns.provider is required",
      "columns.cluster is required",
      "columns.weights does not belong to a fe-random-cluster specification",
      "estimator must be marginal or he2013",
      "fit needs exactly one source",
      "outputs.path is required",
      "outputs.summary needs the stages source",
      "method must be one of exact, poibin_exact, resampling, got wald",
      "critical must be positive",
      "lists method exact twice"
    ).foreach(p => assert(message.contains(p), s"missing '$p' in: $message"))
    val ours =
      """{"version": 1, "model": "fe-random-cluster", "input": {"path": "x"}, "columns": {}, "outputs": {"path": "y"}}"""
    Seq[(String, String => Any)](
      "RunSpec" -> RunSpec.parse,
      "LogisticRunSpec" -> LogisticRunSpec.parse,
      "ThreeStageRunSpec" -> ThreeStageRunSpec.parse,
      "LinearRunSpec" -> LinearRunSpec.parse,
      "LinearRERunSpec" -> LinearRERunSpec.parse,
      "LogisticRERunSpec" -> LogisticRERunSpec.parse
    ).foreach { case (name, parse) =>
      val refused = intercept[IllegalArgumentException](parse(ours)).getMessage
      assert(refused.contains("LogisticFERandomClusterJob"), s"$name: $refused")
    }
    Seq(
      "cox" -> "CoxJob",
      "logistic" -> "LogisticJob",
      "three-stage" -> "ThreeStageJob",
      "linear" -> "LinearJob",
      "linear-re" -> "LinearREJob",
      "logistic-re" -> "LogisticREJob"
    ).foreach { case (model, job) =>
      val other = ours.replace("\"fe-random-cluster\"", s"\"$model\"")
      assert(
        intercept[IllegalArgumentException](LogisticFERandomClusterRunSpec.parse(other)).getMessage
          .contains(job),
        model
      )
    }
    assertEquals(
      LogisticFERandomClusterJob.specification(Array("--spec-json", "{}"), _ => "unused"),
      "{}"
    )
    assertEquals(
      LogisticFERandomClusterJob.specification(Array("--spec", "p"), path => s"read $path"),
      "read p"
    )
    intercept[IllegalArgumentException](
      LogisticFERandomClusterJob.specification(Array("--other"), _ => "")
    )
  }
}
