package pprof.spark.bench.perf

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import pprof.spark.bench.synthetic.Synthetic
import pprof.spark.testkit.SparkSuite

/** The performance runner on small points (docs/parity/scale/perf-runbook.md): the generator, the recorder and the
  * report, with the checks that need no reference.
  */
class PerfJobSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(8, "min")

  test("the generator gives the same rows for any partitioning, for both models and designs") {
    for (model <- Seq("cox", "logistic"); design <- Seq("simple", "correlated")) {
      val d = PerfDesign(model, design, n = 3000L, m = 30, p = 6, seed = 5L)
      val one = Generator.rows(spark, d, 1)
      assertEquals(one.count(), 3000L)
      assertEquals(Synthetic.checksum(one), Synthetic.checksum(Generator.rows(spark, d, 4)), d.name)
    }
  }

  test(
    "the correlated design: indicators at their prevalence, covariates at their offsets and scales"
  ) {
    val d = PerfDesign("logistic", "correlated", n = 40000L, m = 40, p = 9, seed = 11L)
    val parameters = new PerfParameters(d)
    val rows = Generator.rows(spark, d, 2).collect()
    def column(j: Int): Array[Double] = rows.map(_.getDouble(2 + j))
    val n = rows.length.toDouble
    (0 until 6).foreach { j =>
      val pi = parameters.prevalence(j)
      val mean = column(j).sum / n
      assert(
        math.abs(mean - pi) < 5.0 * math.sqrt(pi * (1.0 - pi) / n),
        s"x${j + 1}: $mean against $pi"
      )
    }
    (0 until 3).foreach { c =>
      val xs = column(6 + c)
      val mean = xs.sum / n
      val sd = math.sqrt(xs.map(v => (v - mean) * (v - mean)).sum / (n - 1.0))
      assert(
        math.abs(mean - parameters.offset(c)) < 5.0 * parameters.scale(c) / math.sqrt(n),
        s"x${7 + c}: mean $mean against ${parameters.offset(c)}"
      )
      assert(
        math.abs(sd / parameters.scale(c) - 1.0) < 0.05,
        s"x${7 + c}: sd $sd against ${parameters.scale(c)}"
      )
    }
    val y = rows.map(_.getInt(2 + d.p))
    assert(y.contains(0) && y.contains(1))
  }

  test(
    "a small point for each model: recorded by call site, the truth recovered, an R0 refit identical, data reused"
  ) {
    for (model <- Seq("cox", "logistic")) {
      val path = Files.createTempDirectory(s"pprof-perf-$model").resolve("point").toString
      val options = Map(
        "model" -> model,
        "path" -> path,
        "rows" -> "4000",
        "providers" -> "40",
        "features" -> "4",
        "check-partitions" -> "3",
        "plans" -> "true"
      )
      val result = PerfJob.run(spark, options)
      println(s"perf report ($model): ${result.report}")
      val report = new ObjectMapper().readTree(result.report)
      assert(report.get("data").get("generated").asBoolean())
      val fit = report.get("fit")
      assert(fit.get("succeeded").asBoolean(), result.report)
      assert(report.get("checks").get("truth").get("passed").asBoolean(), result.report)
      assert(report.get("checks").get("partitions").get("identical").asBoolean(), result.report)
      val sites = fit.get("recording").get("sites").elements().asScala.toVector
      val source = if (model == "cox") "CoxPH.scala" else "LogisticFE.scala"
      val passes = sites.filter(_.get("site").asText().contains(source))
      assert(
        passes.exists(s => s.get("executions").asInt() >= 2 && s.get("result_bytes").asLong() > 0L),
        result.report
      )
      assert(sites.exists(_.get("shuffle_write_bytes").asLong() > 0L), result.report)
      assert(result.plans.contains("HashAggregate"), result.plans.take(400))
      val again = new ObjectMapper().readTree(
        PerfJob.run(spark, options - "check-partitions" - "plans").report
      )
      assert(!again.get("data").get("generated").asBoolean())
      assert(again.get("data").get("verified").asBoolean())
      assert(again.get("fit").get("succeeded").asBoolean())
    }
  }

  test("a fit that the driver budget refuses is reported with its error, not thrown") {
    val path = Files.createTempDirectory("pprof-perf-budget").resolve("point").toString
    val options = Map(
      "model" -> "cox",
      "path" -> path,
      "rows" -> "2000",
      "providers" -> "20",
      "features" -> "3",
      "budget-bytes" -> "50"
    )
    val report = new ObjectMapper().readTree(PerfJob.run(spark, options).report)
    val fit = report.get("fit")
    assert(!fit.get("succeeded").asBoolean())
    assert(fit.get("error").asText().contains("DriverLimitExceededException"), fit.toString)
    assert(!report.get("checks").has("truth"))
  }

  test(
    "arguments: --model and --path are required, unknown ones are refused, counts may be written 5e6"
  ) {
    val options = PerfJob.arguments(Array("--model", "cox", "--path", "p", "--rows", "5e6"))
    assertEquals(PerfJob.design(options).n, 5000000L)
    intercept[IllegalArgumentException](PerfJob.arguments(Array("--model", "cox")))
    intercept[IllegalArgumentException](
      PerfJob.arguments(Array("--model", "cox", "--path", "p", "--layout", "1"))
    )
    intercept[IllegalArgumentException](
      PerfJob.design(Map("model" -> "logistic", "strata" -> "none"))
    )
  }

  test(
    "only operation tags group call sites; Spark's own session, execution and broadcast tags are dropped"
  ) {
    val session = "spark-session-0b1f2c3d-4e5f-6071-8293-a4b5c6d7e8f9"
    assertEquals(Recorder.operationTag(session), None)
    assertEquals(Recorder.operationTag(s"$session-execution-root-id-12"), None)
    assertEquals(Recorder.operationTag("broadcast exchange (runId 8d0e0b1f-2c3d)"), None)
    assertEquals(Recorder.operationTag("pprof.cox.evaluate"), Some("pprof.cox.evaluate"))
    assertEquals(
      Recorder.operationTag(s"$session-thread-7-pprof.cox.evaluate"),
      Some("pprof.cox.evaluate")
    )
  }
}
