package pprof.spark.bench

import java.lang.management.{ManagementFactory, MemoryType}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import pprof.spark.bench.synthetic.{Design, Model, Sizes, Synthetic}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.linear.{
  LinearRE,
  LinearREFit,
  LinearREInference,
  LinearREOptions,
  LinearRESpec
}
import pprof.spark.engine.logistic.EffectReference

/** The S1 scale smoke of part 4a, linear random effects (docs/parity/scale/s1-linear-re.md; the scale-test plan's §3 and
  * §9). One point of `bench.synthetic`'s linear data, 10⁷ rows by default, is fitted three times: with the search on
  * the driver; with the providers' sums distributed, under a driver budget below their summaries; and again with the
  * input repartitioned under another `spark.sql.shuffle.partitions` (R0). The downstream operations follow the first
  * fit. The report, one line of JSON, holds the timings, the driver's peak heap, the stages that ran as one task, the
  * bitwise comparisons and the recovery of the generating parameters, each check with its verdict.
  */
object S1LinearRE {

  final case class Point(n: Long = 10000000L, m: Int = 10000, p: Int = 20, seed: Long = 79L)

  val Usage: String =
    "usage: S1LinearRE --path <dir> [--rows n] [--providers m] [--features p] [--seed s] [--budget bytes] [--report <dir>]"

  def main(args: Array[String]): Unit = {
    val options = arguments(args)
    val spark = SparkSession.builder().appName("pprof-spark S1 linear random effects").getOrCreate()
    val d = Point()
    val point = Point(
      options.get("rows").fold(d.n)(_.toLong),
      options.get("providers").fold(d.m)(_.toInt),
      options.get("features").fold(d.p)(_.toInt),
      options.get("seed").fold(d.seed)(_.toLong)
    )
    val path = options("path").stripSuffix("/")
    val report = run(spark, path, point, options.get("budget").map(_.toLong))
    val target = options.getOrElse("report", s"$path/reports/${System.currentTimeMillis}")
    spark.createDataset(Seq(report))(Encoders.STRING).coalesce(1).write.text(target)
    println(report)
  }

  /** `--name value` pairs; `--path` is required. */
  def arguments(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, Usage)
    val pairs = args.toSeq.grouped(2).map(pair => pair(0) -> pair(1)).toSeq
    val known =
      Set("--path", "--rows", "--providers", "--features", "--seed", "--budget", "--report")
    val unknown = pairs.map(_._1).filterNot(known)
    require(unknown.isEmpty, s"unknown arguments ${unknown.mkString(", ")}; $Usage")
    val map = pairs.map { case (k, v) => k.stripPrefix("--") -> v }.toMap
    require(map.contains("path"), Usage)
    map
  }

  /** The stages that ran, and those with a single task (scale-test plan §9.1). */
  private final class Stages extends SparkListener {
    val count = new AtomicInteger()
    val single = new ConcurrentLinkedQueue[String]()
    override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
      count.incrementAndGet()
      if (event.stageInfo.numTasks == 1) single.add(event.stageInfo.name)
    }
  }

  private def heapPools =
    ManagementFactory.getMemoryPoolMXBeans.asScala.filter(_.getType == MemoryType.HEAP)

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  /** Whether two fits agree bit for bit: estimates, covariance, σ̂, θ̂, the deviance, ‖u‖² and the provider table. */
  private def identical(x: LinearREFit, y: LinearREFit): Boolean =
    bits(x.estimates) == bits(y.estimates) && bits(x.covariance) == bits(y.covariance) &&
      bits(Seq(x.sigma, x.theta, x.deviance, x.ussq)) == bits(
        Seq(y.sigma, y.theta, y.deviance, y.ussq)
      ) &&
      Synthetic.checksum(x.providers) == Synthetic.checksum(y.providers)

  /** Runs the smoke on the data under `path`, generating them first unless their manifest is there. */
  def run(spark: SparkSession, path: String, point: Point, budget: Option[Long] = None): String = {
    val design = Design(Model.Linear(), point.n, point.m, point.p, Sizes.Equal, point.seed)
    val truth = Model.Linear()
    val mapper = new ObjectMapper()
    val report = mapper.createObjectNode()
    val times = report.putObject("timesSeconds")
    val heaps = report.putObject("driverPeakHeapMB")
    def measured[A](stage: String)(body: => A): A = {
      heapPools.foreach(_.resetPeakUsage())
      val start = System.nanoTime()
      val result = body
      times.put(stage, (System.nanoTime() - start) / 1e9)
      heaps.put(stage, heapPools.map(_.getPeakUsage.getUsed).sum / 1048576.0)
      result
    }
    val stages = new Stages
    spark.sparkContext.addSparkListener(stages)
    try {
      val manifestPath = new org.apache.hadoop.fs.Path(s"$path/manifest")
      val existing =
        manifestPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(manifestPath)
      if (!existing) measured("generate")(Synthetic.write(spark, design, path))
      report.put("generated", !existing)
      report.put("driverMaxHeapMB", Runtime.getRuntime.maxMemory / 1048576.0)
      report.put("defaultParallelism", spark.sparkContext.defaultParallelism)
      val verified = measured("verify")(Synthetic.verify(spark, path))
      report.set("manifest", mapper.readTree(Synthetic.manifest(spark, path)))
      val df = spark.read.parquet(s"$path/data")
      val spec = LinearRESpec("y", design.features, "provider", rowId = Some("id"))
      val a = measured("fit")(LinearRE.fit(df, spec))
      measured("provider tests")(LinearREInference.test(a, EffectReference.Median).count())
      val alpha = measured("alpha intervals") {
        val table = LinearREInference.alphaIntervals(a).cache()
        table.count()
        table
      }
      measured("measures")(LinearREInference.measures(df, a).values.foreach(_.count()))
      val limit = budget.getOrElse(8L * point.m * (a.coefficients.size + 4L) * 3L / 4L)
      val b = measured("fit, distributed search")(
        LinearRE.fit(df, spec, LinearREOptions(blocks = BlockOptions(driverBudgetBytes = limit)))
      )
      val shuffle = spark.conf.get("spark.sql.shuffle.partitions")
      val c =
        try {
          spark.conf.set("spark.sql.shuffle.partitions", (shuffle.toInt * 2 + 1).toString)
          val reordered = df.repartition(spark.sparkContext.defaultParallelism * 2 + 1, col("id"))
          measured("fit, R0 rerun")(LinearRE.fit(reordered, spec))
        } finally spark.conf.set("spark.sql.shuffle.partitions", shuffle)
      val fit = report.putObject("fit")
      fit.put("criterion", a.criterion)
      fit.put("rows", a.rows)
      fit.put("providers", a.providerCount)
      fit.put("fixedEffects", a.coefficients.size)
      fit.put("blocks", a.layout.blockCount)
      Seq(
        "sigma" -> a.sigma,
        "theta" -> a.theta,
        "sigmaAlpha" -> a.sigmaAlpha,
        "deviance" -> a.deviance
      )
        .foreach { case (k, v) => fit.put(k, v) }
      Seq("driver" -> a, "distributed" -> b, "r0" -> c).foreach { case (name, f) =>
        val s = fit.putObject(s"search ($name)")
        s.put("path", f.search.path)
        s.put("evaluations", f.search.evaluations)
        f.search.boundary.foreach(v => s.put("boundary", v))
      }
      val warnings = fit.putArray("warnings")
      a.warnings.foreach(w => warnings.add(w))
      val expected = 0.0 +: design.beta
      val z = a.coefficients.indices.map(i => (a.estimates(i) - expected(i)) / a.standardErrors(i))
      val sigmaAlphaZ = (a.sigmaAlpha - truth.gammaSd) / (truth.gammaSd / math.sqrt(2.0 * point.m))
      val sigmaZ = (a.sigma - truth.sigma) / (truth.sigma / math.sqrt(
        2.0 * (point.n - point.m - a.coefficients.size)
      ))
      val covered = alpha
        .join(
          spark.read.parquet(s"$path/providers").select(col("provider"), col("effect")),
          Seq("provider")
        )
        .filter(col("alpha_lower") <= col("effect") && col("effect") <= col("alpha_upper"))
        .count()
      val coverage = covered.toDouble / point.m
      val coverageLimit = 2.576 * math.sqrt(0.95 * 0.05 / point.m)
      val checks = report.putObject("checks")
      def check(name: String, value: Double, passed: Boolean): Boolean = {
        val node = checks.putObject(name)
        node.put("value", value)
        node.put("passed", passed)
        passed
      }
      val verdicts = Seq(
        check("data verified against the manifest", if (verified) 1.0 else 0.0, verified),
        check(
          "distributed search identical to the driver's",
          if (identical(a, b)) 1.0 else 0.0,
          identical(a, b)
        ),
        check("R0 rerun identical", if (identical(a, c)) 1.0 else 0.0, identical(a, c)),
        check(
          "search paths driver, distributed, driver",
          0.0,
          (a.search.path, b.search.path, c.search.path) ==
            (("driver", "distributed", "driver"))
        ),
        check(
          "max |z| of the fixed effects (at most 4.5)",
          z.map(math.abs).max,
          z.forall(v => math.abs(v) <= 4.5)
        ),
        check(
          "z of sigma_alpha against 0.5 (at most 4.5)",
          sigmaAlphaZ,
          math.abs(sigmaAlphaZ) <= 4.5
        ),
        check("z of sigma against 1 (at most 4.5)", sigmaZ, math.abs(sigmaZ) <= 4.5),
        check(
          f"coverage of the alpha intervals (0.95 within $coverageLimit%.4f)",
          coverage,
          math.abs(coverage - 0.95) <= coverageLimit
        )
      )
      report.put("passed", verdicts.forall(identity))
      alpha.unpersist()
      Thread.sleep(2000L) // let the listener bus deliver the last stages
      report.put("stages", stages.count.get)
      val single = report.putArray("singleTaskStages")
      stages.single.asScala.toSeq.distinct.foreach(name => single.add(name))
      mapper.writeValueAsString(report)
    } finally spark.sparkContext.removeSparkListener(stages)
  }
}
