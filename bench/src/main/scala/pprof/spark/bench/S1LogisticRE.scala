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
import pprof.spark.engine.logistic.{
  LogisticRE,
  LogisticREFit,
  LogisticREInference,
  LogisticREOptions,
  LogisticRESpec
}

/** The S1 scale smoke of part 4b, logistic random effects (docs/parity/scale/s1-logistic-re.md; the scale-test plan's
  * §3 and §9). One point of `bench.synthetic`'s logistic data, 10⁷ rows by default, is fitted (the Laplace fit), then
  * fitted again on the input repartitioned under another `spark.sql.shuffle.partitions` (R0), and, with `--layout`,
  * again with blocks half the size. The Wald tests, option `alpha`, the measures and the SM intervals follow the
  * first fit; the count tests and the profile, each some hundreds of Spark jobs at this size, are left to S2 (OI-99).
  * The report, one line of JSON, holds the timings, the driver's peak heap, the stages that ran as one task, and every
  * check with its verdict.
  */
object S1LogisticRE {

  final case class Point(n: Long = 10000000L, m: Int = 10000, p: Int = 20, seed: Long = 79L)

  val Usage: String =
    "usage: S1LogisticRE --path <dir> [--rows n] [--providers m] [--features p] [--seed s] [--layout true] [--report <dir>]"

  def main(args: Array[String]): Unit = {
    val options = arguments(args)
    val spark =
      SparkSession.builder().appName("pprof-spark S1 logistic random effects").getOrCreate()
    val d = Point()
    val point = Point(
      options.get("rows").fold(d.n)(_.toLong),
      options.get("providers").fold(d.m)(_.toInt),
      options.get("features").fold(d.p)(_.toInt),
      options.get("seed").fold(d.seed)(_.toLong)
    )
    val path = options("path").stripSuffix("/")
    val report = run(spark, path, point, options.get("layout").contains("true"))
    val target = options.getOrElse("report", s"$path/reports/${System.currentTimeMillis}")
    spark.createDataset(Seq(report))(Encoders.STRING).coalesce(1).write.text(target)
    println(report)
  }

  /** `--name value` pairs; `--path` is required. */
  def arguments(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, Usage)
    val pairs = args.toSeq.grouped(2).map(pair => pair(0) -> pair(1)).toSeq
    val known =
      Set("--path", "--rows", "--providers", "--features", "--seed", "--layout", "--report")
    val unknown = pairs.map(_._1).filterNot(known)
    require(unknown.isEmpty, s"unknown arguments ${unknown.mkString(", ")}; $Usage")
    val map = pairs.map { case (k, v) => k.stripPrefix("--") -> v }.toMap
    require(map.contains("path"), Usage)
    map
  }

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

  /** Whether two fits agree bit for bit: estimates, covariance, σ̂, the deviance, ‖u‖² and the provider table. */
  private def identical(x: LogisticREFit, y: LogisticREFit): Boolean =
    bits(x.estimates) == bits(y.estimates) && bits(x.covariance) == bits(y.covariance) &&
      bits(Seq(x.sigma, x.deviance, x.ussq)) == bits(Seq(y.sigma, y.deviance, y.ussq)) &&
      Synthetic.checksum(x.providers) == Synthetic.checksum(y.providers)

  /** The largest relative difference of two fits' estimates, σ̂ and deviance (another block layout, T-part). */
  private def apart(x: LogisticREFit, y: LogisticREFit): Double =
    ((x.estimates :+ x.sigma :+ x.deviance) zip (y.estimates :+ y.sigma :+ y.deviance)).map {
      case (a, b) =>
        math.abs(a - b) / math.max(math.abs(b), 1e-300)
    }.max

  /** Runs the smoke on the data under `path`, generating them first unless their manifest is there. */
  def run(spark: SparkSession, path: String, point: Point, layout: Boolean = false): String = {
    val truth = Model.Logistic()
    val design = Design(truth, point.n, point.m, point.p, Sizes.Equal, point.seed)
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
      val spec = LogisticRESpec("y", design.features, "provider", rowId = Some("id"))
      val a = measured("fit")(LogisticRE.fit(df, spec))
      measured("Wald tests")(LogisticREInference.test(df, a).count())
      val alpha = measured("alpha intervals") {
        val table = LogisticREInference.alphaIntervals(a).cache()
        table.count()
        table
      }
      measured("measures")(
        LogisticREInference.measures(df, a, Seq("indirect", "direct")).values.foreach(_.count())
      )
      measured("SM intervals")(
        LogisticREInference
          .smIntervals(df, a, Seq("indirect", "direct"), Seq("ratio"))
          .values
          .foreach(_.count())
      )
      val shuffle = spark.conf.get("spark.sql.shuffle.partitions")
      val c =
        try {
          spark.conf.set("spark.sql.shuffle.partitions", (shuffle.toInt * 2 + 1).toString)
          val reordered = df.repartition(spark.sparkContext.defaultParallelism * 2 + 1, col("id"))
          measured("fit, R0 rerun")(LogisticRE.fit(reordered, spec))
        } finally spark.conf.set("spark.sql.shuffle.partitions", shuffle)
      val b =
        if (!layout) None
        else {
          val half = BlockOptions().targetBlockBytes / 2
          Some(
            measured("fit, half-size blocks")(
              LogisticRE.fit(
                df,
                spec,
                LogisticREOptions(blocks = BlockOptions(targetBlockBytes = half))
              )
            )
          )
        }
      val fit = report.putObject("fit")
      fit.put("criterion", a.criterion)
      fit.put("rows", a.rows)
      fit.put("providers", a.providerCount)
      fit.put("fixedEffects", a.coefficients.size)
      fit.put("blocks", a.layout.blockCount)
      Seq("sigma" -> a.sigma, "deviance" -> a.deviance, "aic" -> a.aic).foreach { case (k, v) =>
        fit.put(k, v)
      }
      val search = fit.putObject("search")
      search.put("stage1Evaluations", a.search.stage1Evaluations)
      search.put("stage2Evaluations", a.search.stage2Evaluations)
      search.put("projectedGradient", a.search.projectedGradient)
      search.put("converged", a.search.converged)
      a.search.boundary.foreach(v => search.put("boundary", v))
      val warnings = fit.putArray("warnings")
      a.warnings.foreach(w => warnings.add(w))
      val expected = truth.gammaMean +: design.beta
      val z = a.coefficients.indices.map(i => (a.estimates(i) - expected(i)) / a.standardErrors(i))
      // σ̂'s large-sample SD with precise BLUPs (1,000 rows a provider), an approximation for the smoke
      val sigmaZ = (a.sigma - truth.gammaSd) / (truth.gammaSd / math.sqrt(2.0 * point.m))
      val covered = alpha
        .join(
          spark.read.parquet(s"$path/providers").select(col("provider"), col("effect")),
          Seq("provider")
        )
        .filter(
          col("alpha_lower") <= col("effect") - truth.gammaMean && col(
            "effect"
          ) - truth.gammaMean <= col("alpha_upper")
        )
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
        check("the Laplace search converged", a.search.projectedGradient, a.search.converged),
        check("R0 rerun identical", if (identical(a, c)) 1.0 else 0.0, identical(a, c)),
        check(
          "max |z| of the fixed effects (at most 4.5)",
          z.map(math.abs).max,
          z.forall(v => math.abs(v) <= 4.5)
        ),
        check("z of sigma against 0.5 (at most 4.5)", sigmaZ, math.abs(sigmaZ) <= 4.5),
        check(
          f"coverage of the alpha intervals (0.95 within $coverageLimit%.4f)",
          coverage,
          math.abs(coverage - 0.95) <= coverageLimit
        )
      ) ++ b.toSeq.map(f =>
        check("half-size blocks within 1e-10 relative (T-part)", apart(a, f), apart(a, f) <= 1e-10)
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
