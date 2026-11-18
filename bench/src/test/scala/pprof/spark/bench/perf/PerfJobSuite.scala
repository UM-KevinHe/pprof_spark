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
      val operation = if (model == "cox") "pprof.cox.evaluate" else "pprof.logistic.pass-a"
      val passes = sites.filter(_.get("tags").asText().contains(operation))
      assert(
        passes.exists(s => s.get("executions").asInt() >= 2 && s.get("result_bytes").asLong() > 0L),
        result.report
      )
      assert(sites.exists(_.get("shuffle_write_bytes").asLong() > 0L), result.report)
      assert(sites.exists(_.get("input_bytes").asLong() > 0L), result.report)
      assert(sites.forall(_.get("other_ms").asLong() >= 0L), result.report)
      assert(report.get("cluster").has("spark.memory.offHeap.enabled"), result.report)
      val recording = fit.get("recording")
      val utilization = recording.get("utilization").asDouble()
      assert(recording.get("core_seconds_available").asDouble() > 0.0, result.report)
      assert(utilization > 0.0 && utilization <= 1.25, result.report)
      assert(
        recording.get("executors_max").asInt() >= recording.get("executors_min").asInt(),
        result.report
      )
      assert(
        recording.get("cached_memory_peak_bytes").asLong() + recording
          .get("cached_disk_peak_bytes")
          .asLong() > 0L
      )
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

  test(
    "runLine, the notebook's entry point: the report returned, and written with the plans under the point"
  ) {
    val path = Files.createTempDirectory("pprof-perf-line").resolve("point").toString
    val line = s"--model logistic --path $path --rows 3000 --providers 30 --features 3 --plans true"
    val text = PerfJob.runLine(spark, s"  $line ")
    assert(new ObjectMapper().readTree(text).get("fit").get("succeeded").asBoolean(), text)
    val reports = new java.io.File(s"$path/reports").listFiles().toVector
    assertEquals(reports.size, 1)
    assertEquals(spark.read.text(s"${reports.head.getPath}/report").head().getString(0), text)
    assert(spark.read.text(s"${reports.head.getPath}/plans").count() > 0L)
  }

  test(
    "a notebook's job descriptions are set aside during a run, so executions keep their call sites, then restored"
  ) {
    val path = Files.createTempDirectory("pprof-perf-notebook").resolve("point").toString
    val context = spark.sparkContext
    val cell = "started = time.time()\nreport = runner.runLine(spark._jsparkSession, ...)"
    context.setJobDescription(cell)
    context.setLocalProperty("callSite.short", "cell 7")
    context.setLocalProperty("callSite.long", "cell 7, line 2")
    try {
      val options =
        Map(
          "model" -> "cox",
          "path" -> path,
          "rows" -> "2000",
          "providers" -> "20",
          "features" -> "3",
          "verify" -> "false"
        )
      val report = new ObjectMapper().readTree(PerfJob.run(spark, options).report)
      val sites = report
        .get("fit")
        .get("recording")
        .get("sites")
        .elements()
        .asScala
        .map(_.get("site").asText())
        .toVector
      assert(sites.forall(_.matches("""\w+ at \w+\.scala:\d+""")), sites.mkString("; "))
      val tags = report
        .get("fit")
        .get("recording")
        .get("sites")
        .elements()
        .asScala
        .map(_.get("tags").asText())
      assert(tags.exists(_.contains("pprof.cox.evaluate")), tags.mkString("; "))
      assert(
        !sites.exists(s => s.contains("runLine") || s.contains("cell 7")),
        sites.mkString("; ")
      )
      assertEquals(context.getLocalProperty("spark.job.description"), cell)
      assertEquals(context.getLocalProperty("callSite.short"), "cell 7")
      assertEquals(context.getLocalProperty("callSite.long"), "cell 7, line 2")
      assert(
        !report
          .get("cluster")
          .has("spark.master") && report.get("cluster").get("master").asText() == "local"
      )
      assert(!report.get("options").has("path"), report.get("options").toString)
      assert(report.get("fit").get("recording").has("driver_heap_after_gc_peak_bytes"))
    } finally {
      context.setJobDescription(null)
      context.setLocalProperty("callSite.short", null)
      context.setLocalProperty("callSite.long", null)
    }
  }

  test("a storage level is passed to the working set, and the reduction groups are reported") {
    val path = Files.createTempDirectory("pprof-perf-storage").resolve("point").toString
    val options = Map(
      "model" -> "cox",
      "path" -> path,
      "rows" -> "2000",
      "providers" -> "20",
      "features" -> "3",
      "storage-level" -> "DISK_ONLY",
      "block-bytes" -> "2048",
      "budget-bytes" -> "300",
      "verify" -> "false"
    )
    val report = new ObjectMapper().readTree(PerfJob.run(spark, options).report)
    assertEquals(report.get("options").get("storage-level").asText(), "DISK_ONLY")
    val fit = report.get("fit")
    assert(fit.get("succeeded").asBoolean(), fit.toString)
    val reduction = fit.get("reduction")
    val (blocks, perGroup, groups) =
      (
        reduction.get("blocks").asInt(),
        reduction.get("blocks_per_group").asInt(),
        reduction.get("groups").asInt()
      )
    assert(
      perGroup > 1 && groups * perGroup >= blocks && (groups - 1) * perGroup < blocks,
      reduction.toString
    )
  }

  test(
    "a failed execution is counted against its operation, with its error, and addresses are redacted"
  ) {
    val recorder = Recorder.attach(spark)
    try {
      val from = recorder.mark()
      intercept[Exception](
        spark
          .range(10)
          .map((x: java.lang.Long) =>
            if (x == 5L) throw new IllegalStateException("boom") else x.longValue
          )(
            org.apache.spark.sql.Encoders.scalaLong
          )
          .collect()
      )
      val failed = recorder.summary(from).sites.filter(_.failures > 0)
      assertEquals(failed.size, 1, failed.toString)
      assert(failed.head.error.contains("boom"), failed.head.error)
    } finally recorder.detach()
    assertEquals(
      PerfJob.redact("Lost task 28.3 (192.0.2.10 executor 94) on ip-192-0-2-10 at 1.5.0"),
      "Lost task 28.3 (<address> executor 94) on <address> at 1.5.0"
    )
  }
}
