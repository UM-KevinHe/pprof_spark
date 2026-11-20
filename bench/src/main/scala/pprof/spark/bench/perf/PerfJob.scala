package pprof.spark.bench.perf

import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.col

import pprof.spark.engine.BuildInfo
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.cox.{CoxOptions, CoxPH, CoxSpec, Ties}
import pprof.spark.engine.layout.ReductionGroups
import pprof.spark.engine.logistic.{LogisticFE, LogisticOptions, LogisticSpec}

/** The performance phase's runner (docs/parity/scale/perf-runbook.md; docs/spec/performance/plan.md, step 0). It
  * prepares one point's data, fits `CoxPH` or `LogisticFE` with a [[Recorder]] attached, and reports one line of JSON:
  * the point, the cluster, the fit's outcome, its call sites with their times, bytes to the driver and shuffle bytes,
  * the memory peaks, and the checks that need no reference: recovery of the generating coefficients within 4.5
  * standard errors and, with `--check-partitions`, a refit on another input partitioning that must agree bit for bit
  * (R0). A fit that fails, as one the driver budget refuses, is reported with its error.
  */
object PerfJob {

  val Usage: String =
    "usage: PerfJob --model cox|logistic --path <dir> [--rows n] [--providers m] [--features p] " +
      "[--design simple|correlated] [--seed s] [--strata provider|none] [--ties breslow|efron] " +
      "[--block-bytes b] [--budget-bytes b] [--storage-level level] [--partitions k] [--check-partitions k] " +
      "[--verify true|false] " +
      "[--plans true|false] [--report <dir>]"

  private val Known: Set[String] = Set(
    "model",
    "path",
    "rows",
    "providers",
    "features",
    "design",
    "seed",
    "strata",
    "ties",
    "block-bytes",
    "budget-bytes",
    "storage-level",
    "partitions",
    "check-partitions",
    "verify",
    "plans",
    "report"
  )

  /** A message without network addresses or host names built from them: the maintainer's records keep none. */
  def redact(message: String): String =
    message
      .replaceAll("""\b\d{1,3}(\.\d{1,3}){3}\b""", "<address>")
      .replaceAll("""\bip-\d{1,3}(-\d{1,3}){3}\b""", "<address>")

  /** Options that name storage locations, which the report leaves out. */
  private val Private: Set[String] = Set("path", "report")

  /** The local properties by which a notebook describes every job with the running cell's code. They are set aside
    * during a run, so that Spark describes each SQL execution by its call site in pprof_spark, and then restored.
    */
  private val Described: Seq[String] =
    Seq("spark.job.description", "callSite.short", "callSite.long")

  private def executors(spark: SparkSession): Int =
    math.max(0, spark.sparkContext.getExecutorMemoryStatus.size - 1)

  /** The report line, and the physical plans by call site when `--plans true`. */
  final case class Result(report: String, plans: String)

  /** What a fit returns that the report and the R0 check use. */
  final case class Outcome(
      estimates: Vector[Double],
      errors: Vector[Double],
      loglik: Double,
      iterations: Int,
      converged: Boolean,
      layout: String,
      matrices: Vector[Double],
      blocks: Int
  )

  def main(args: Array[String]): Unit =
    println(runArgs(SparkSession.builder().appName("pprof-spark perf").getOrCreate(), args))

  /** Runs one point from `--name value` arguments in an existing session, writes its report (and, with `--plans
    * true`, its plans) under `--report`, by default `<path>/reports/<time>`, and returns the report line.
    */
  def runArgs(spark: SparkSession, args: Array[String]): String = {
    val options = arguments(args)
    val path = options("path").stripSuffix("/")
    val result = run(spark, options)
    val target = options.getOrElse("report", s"$path/reports/${System.currentTimeMillis}")
    spark
      .createDataset(Seq(result.report))(Encoders.STRING)
      .coalesce(1)
      .write
      .text(s"$target/report")
    if (result.plans.nonEmpty)
      spark
        .createDataset(Seq(result.plans))(Encoders.STRING)
        .coalesce(1)
        .write
        .text(s"$target/plans")
    result.report
  }

  /** [[runArgs]] with the arguments on one line, separated by spaces: the entry point `databricks/03_perf.py` calls
    * through py4j. Values cannot contain spaces.
    */
  def runLine(spark: SparkSession, line: String): String = runArgs(spark, line.trim.split("\\s+"))

  /** `--name value` pairs; `--model` and `--path` are required. */
  def arguments(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, Usage)
    val pairs = args.toSeq.grouped(2).map(pair => pair(0) -> pair(1)).toSeq
    val unknown = pairs.map(_._1).filterNot(k => k.startsWith("--") && Known(k.stripPrefix("--")))
    require(unknown.isEmpty, s"unknown arguments ${unknown.mkString(", ")}; $Usage")
    val map = pairs.map { case (k, v) => k.stripPrefix("--") -> v }.toMap
    require(map.contains("model") && map.contains("path"), Usage)
    map
  }

  /** A count written as an integer or in scientific notation, such as 5e6. */
  private def count(text: String): Long = BigDecimal(text).toLongExact

  def design(options: Map[String, String]): PerfDesign = {
    val d = PerfDesign(options("model"))
    val strata = options.getOrElse("strata", "provider")
    require(
      strata == "provider" || strata == "none",
      s"--strata must be provider or none, got $strata"
    )
    PerfDesign(
      d.model,
      options.getOrElse("design", d.design),
      options.get("rows").fold(d.n)(count),
      options.get("providers").fold(d.m)(v => count(v).toInt),
      options.get("features").fold(d.p)(_.toInt),
      options.get("seed").fold(d.seed)(_.toLong),
      strata == "provider"
    )
  }

  /** The working set's options: block size, driver budget and storage level, defaults unless given. */
  private def blockOptions(options: Map[String, String]): BlockOptions = {
    val defaults = BlockOptions()
    BlockOptions(
      targetBlockBytes = options.get("block-bytes").fold(defaults.targetBlockBytes)(count),
      driverBudgetBytes = options.get("budget-bytes").fold(defaults.driverBudgetBytes)(count),
      storageLevel = options.getOrElse("storage-level", defaults.storageLevel)
    )
  }

  private def fit(data: DataFrame, d: PerfDesign, options: Map[String, String]): Outcome = {
    val blocks = blockOptions(options)
    if (d.model == "cox") {
      val spec = CoxSpec(
        "time",
        "event",
        d.features,
        strata = if (d.stratified) Some("provider") else None,
        rowId = Some("id")
      )
      val ties = Ties.fromName(options.getOrElse("ties", "breslow"))
      val f = CoxPH.fit(data, spec, CoxOptions(ties = ties, blocks = blocks))
      Outcome(
        f.estimates,
        f.standardErrors,
        f.logLikelihood,
        f.iterations,
        f.converged,
        String.valueOf(f.layout),
        f.information ++ f.covariance,
        f.layout.blockCount
      )
    } else {
      val spec = LogisticSpec("y", d.features, "provider", rowId = Some("id"))
      val f = LogisticFE.fit(data, spec, LogisticOptions(blocks = blocks))
      Outcome(
        f.coefficients.map(_.estimate),
        f.coefficients.map(_.standardError),
        f.loglik,
        f.iterations,
        f.converged,
        String.valueOf(f.layout),
        f.covariance ++ f.providers.gamma.toVector,
        f.layout.blockCount
      )
    }
  }

  private def timed(body: => Outcome): (Either[String, Outcome], Double) = {
    val start = System.nanoTime()
    val outcome =
      try Right(body)
      catch { case NonFatal(e) => Left(s"${e.getClass.getName}: ${e.getMessage}") }
    (outcome, (System.nanoTime() - start) / 1e9)
  }

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  private def identical(a: Outcome, b: Outcome): Boolean =
    bits(a.estimates) == bits(b.estimates) && bits(a.errors) == bits(b.errors) &&
      bits(Seq(a.loglik)) == bits(Seq(b.loglik)) && bits(a.matrices) == bits(b.matrices) &&
      a.iterations == b.iterations

  def run(spark: SparkSession, options: Map[String, String]): Result = {
    val d = design(options)
    val path = options("path").stripSuffix("/")
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("runner", "pprof_spark.bench.perf.PerfJob").put("version", Generator.Version)
    root
      .put("commit", BuildInfo.gitSha)
      .put("pprof_spark", BuildInfo.version)
      .put("spark", spark.version)
    cluster(spark, root.putObject("cluster"))
    root
      .putObject("point")
      .put("name", d.name)
      .put("model", d.model)
      .put("design", d.design)
      .put("n", d.n)
      .put("m", d.m)
      .put("p", d.p)
      .put("seed", d.seed)
      .put("stratified", d.stratified)
    val supplied = root.putObject("options")
    options.toSeq.sorted.filterNot { case (k, _) => Private(k) }.foreach { case (k, v) =>
      supplied.put(k, v)
    }
    val prepared =
      Generator.prepare(spark, d, path, verify = options.get("verify").forall(_ == "true"))
    val data = root
      .putObject("data")
      .put("generated", prepared.generated)
      .put("seconds", prepared.seconds)
      .put("checksum", prepared.checksum)
    prepared.verified.fold(data.putNull("verified"))(v => data.put("verified", v))
    val input =
      options
        .get("partitions")
        .fold(prepared.data)(k => prepared.data.repartition(k.toInt, col("id")))
    val truth = new PerfParameters(d).beta
    val recorder = Recorder.attach(spark)
    val context = spark.sparkContext
    val described = Described.map(key => key -> context.getLocalProperty(key))
    described.foreach { case (key, _) => context.setLocalProperty(key, null) }
    try {
      val from = recorder.mark()
      val (first, seconds) = timed(fit(input, d, options))
      val window = recorder.summary(from)
      val node = root
        .putObject("fit")
        .put("seconds", seconds)
        .put("succeeded", first.isRight)
        .put("storage_level", blockOptions(options).storageLevel)
        .put("storage_level_used", blockOptions(options).storageLevelNameIn(spark))
        .put("executors_at_end", executors(spark))
      first match {
        case Left(error) => node.put("error", redact(error))
        case Right(o)    =>
          node
            .put("iterations", o.iterations)
            .put("converged", o.converged)
            .put("loglik", o.loglik)
            .put("layout", o.layout)
          // The reduction groups the engine forms (docs/spec/performance/step1-reduction.md §2), recomputed from the
          // block count with the engine's own rule and the fit's options.
          val partial = 8L * (2L + d.p + d.p.toLong * (d.p + 1) / 2)
          val groups = ReductionGroups.plan(o.blocks, partial, blockOptions(options))
          node
            .putObject("reduction")
            .put("blocks", o.blocks)
            .put("blocks_per_group", groups.blocksPerGroup)
            .put("groups", groups.groups)
      }
      recording(node.putObject("recording"), window)
      val checks = root.putObject("checks")
      first.foreach { o =>
        val z = o.estimates.indices.map(k => math.abs(o.estimates(k) - truth(k)) / o.errors(k)).max
        checks.putObject("truth").put("max_abs_z", z).put("passed", z <= 4.5)
        options.get("check-partitions").foreach { k =>
          val again = prepared.data.repartition(k.toInt, col("id"))
          val (second, refit) = timed(fit(again, d, options))
          val check = checks
            .putObject("partitions")
            .put("partitions", k.toInt)
            .put("seconds", refit)
            .put("identical", second.exists(identical(o, _)))
          second.left.foreach(error => check.put("error", error))
        }
      }
      val plans =
        if (!options.get("plans").contains("true")) ""
        else window.sites.map(s => s"== ${s.site} [${s.tags}]\n${s.plan}").mkString("\n\n")
      Result(mapper.writeValueAsString(root), plans)
    } finally {
      described.foreach { case (key, value) => context.setLocalProperty(key, value) }
      recorder.detach()
    }
  }

  private def cluster(spark: SparkSession, node: ObjectNode): Unit = {
    val context = spark.sparkContext
    node.put("executors", executors(spark))
    node.put("master", context.master.takeWhile(c => c != ':' && c != '['))
    node.put("default_parallelism", context.defaultParallelism)
    node.put("driver_max_heap_bytes", Runtime.getRuntime.maxMemory)
    Seq(
      "spark.executor.memory",
      "spark.executor.cores",
      "spark.driver.memory",
      "spark.sql.shuffle.partitions",
      "spark.sql.adaptive.enabled",
      "spark.memory.fraction",
      "spark.memory.storageFraction",
      "spark.memory.offHeap.enabled",
      "spark.memory.offHeap.size",
      "spark.databricks.photon.enabled",
      "spark.databricks.clusterUsageTags.clusterWorkers",
      "spark.databricks.clusterUsageTags.clusterNodeType",
      "spark.databricks.clusterUsageTags.driverNodeType",
      "spark.databricks.clusterUsageTags.sparkVersion"
    ).foreach { key =>
      node.put(key, spark.conf.getOption(key).orElse(context.getConf.getOption(key)).getOrElse(""))
    }
  }

  private def recording(node: ObjectNode, w: WindowSummary): Unit = {
    val run = w.sites.map(_.runMs).sum + w.unattributedRunMs
    val sites = node.putArray("sites")
    w.sites.foreach { s =>
      sites
        .addObject()
        .put("site", s.site)
        .put("tags", s.tags)
        .put("executions", s.executions)
        .put("wall_ms", s.wallMs)
        .put("run_ms", s.runMs)
        .put("cpu_ms", s.cpuMs)
        .put("gc_ms", s.gcMs)
        .put("tasks", s.tasks)
        .put("result_bytes", s.resultBytes)
        .put("max_result_bytes_per_execution", s.maxResultBytes)
        .put("shuffle_write_bytes", s.shuffleWriteBytes)
        .put("shuffle_read_bytes", s.shuffleReadBytes)
        .put("input_bytes", s.inputBytes)
        .put("other_ms", math.max(0L, s.runMs - s.cpuMs - s.gcMs))
        .put("spilled_bytes", s.spilledBytes)
        .put("peak_task_memory_bytes", s.peakTaskMemory)
        .put("single_task_stages", s.singleTaskStages)
        .put("failed_executions", s.failures)
      if (s.failures > 0)
        sites.get(sites.size - 1).asInstanceOf[ObjectNode].put("error", redact(s.error))
    }
    node
      .putObject("unattributed")
      .put("tasks", w.unattributedTasks)
      .put("cpu_ms", w.unattributedCpuMs)
      .put("run_ms", w.unattributedRunMs)
    node
      .put("driver_heap_peak_bytes", w.driverHeapPeakBytes)
      .put("driver_heap_pool_peak_bytes", w.driverHeapPoolPeakBytes)
      .put("driver_heap_after_gc_peak_bytes", w.driverHeapAfterGcPeakBytes)
      .put("driver_max_heap_bytes", w.driverMaxHeapBytes)
      .put("executor_heap_peak_bytes", w.executorHeapPeakBytes)
      .put("cached_peak_bytes", w.cachedPeakBytes)
      .put("cached_memory_peak_bytes", w.cachedMemoryPeakBytes)
      .put("cached_disk_peak_bytes", w.cachedDiskPeakBytes)
      .put("core_seconds_available", w.coreSecondsAvailable)
      .put(
        "utilization",
        if (w.coreSecondsAvailable > 0.0) run / 1000.0 / w.coreSecondsAvailable else 0.0
      )
      .put("executors_min", w.executorsMin)
      .put("executors_max", w.executorsMax)
  }
}
