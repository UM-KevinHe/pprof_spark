package pprof.spark.engine.logistic

// Tutorial probe: documentation material for docs/tutorials/logistic, not compiled by sbt. It lives in
// package pprof.spark.engine.logistic, as the engine's own tests do.

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.numerics.{Cholesky, InMemorySerbinPasses, Serbin, SerbinOptions, SerbinResult}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}

final case class LJob(id: Int, phase: String, stageIds: Seq[Int])
final case class LStage(tasks: Int, shuffleWrite: Long, shuffleRead: Long)

class LRecorder extends SparkListener {
  val jobs = new ConcurrentLinkedQueue[LJob]()
  val stages = new ConcurrentHashMap[Int, LStage]()
  override def onJobStart(e: SparkListenerJobStart): Unit = {
    val phase = Option(e.properties).flatMap(p => Option(p.getProperty("pprof.phase"))).getOrElse("-")
    jobs.add(LJob(e.jobId, phase, e.stageIds.toVector))
  }
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val i = e.stageInfo
    val m = Option(i.taskMetrics)
    stages.put(
      i.stageId,
      LStage(
        i.numTasks,
        m.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(0L),
        m.map(_.shuffleReadMetrics.totalBytesRead).getOrElse(0L)
      )
    )
  }
}

object LogisticProbe {
  val dir: String = sys.props.getOrElse("pprof.fixtures", "fixtures/logistic") + "/lfe-base"
  val mapper = new ObjectMapper()
  val p = 3

  def json(name: String): JsonNode = mapper.readTree(new java.io.File(s"$dir/$name.json"))
  def num(n: JsonNode): Double =
    if (n == null || n.isNull) Double.NaN
    else if (n.isTextual) java.lang.Double.parseDouble(n.asText())
    else if (n.isBoolean) (if (n.asBoolean()) 1.0 else 0.0)
    else n.asDouble()
  def arr(n: JsonNode): Array[Double] = (0 until n.size).map(i => num(n.get(i))).toArray
  def at(root: JsonNode, path: String): JsonNode =
    path.split("\\.").foldLeft(root)((n, k) => if (n == null) null else n.get(k))
  def g(d: Double): String = f"$d%.17g"
  def e(d: Double): String = f"$d%.1e"

  /** max |a − e| over the largest magnitude of e: the scale of the tolerance rule (D-09). */
  def rel(a: Array[Double], x: Array[Double]): Double = {
    require(a.length == x.length, s"length ${a.length} against ${x.length}")
    val s = math.max(x.map(math.abs).max, 1e-300)
    a.indices.map(i => math.abs(a(i) - x(i))).max / s
  }

  // id, provider, y, (x1, x2, x3), in input.csv order
  lazy val records: Vector[(Long, Long, Int, Array[Double])] = {
    val src = scala.io.Source.fromFile(s"$dir/input.csv")
    try
      src
        .getLines()
        .drop(1)
        .map { l =>
          val f = l.split(",")
          (f(0).toLong, f(1).toLong, f(2).toInt, Array(f(3).toDouble, f(4).toDouble, f(5).toDouble))
        }
        .toVector
    finally src.close()
  }
  lazy val keys: Vector[Long] = records.map(_._2).distinct.sorted
  lazy val m: Int = keys.size

  def block(): LogisticBlock = {
    val index = keys.zipWithIndex.toMap
    val rows = records.map { case (id, prov, y, x) => LogisticRow(0, index(prov), id, y.toDouble, 1.0, x, "") }
    LogisticBlockBuilder.build(0, rows.iterator, p)
  }

  var inMemory: SerbinResult = _
  var inMemoryLoglik: Double = Double.NaN

  def partA(): Unit = {
    println("=" * 100)
    println("PART A: the real block builder, kernels and SerBIN on lfe-base, in memory")
    val py = json("pprof_py")
    val rr = json("r_logistic")
    val b = block()
    val rows = b.rows
    val sizes = keys.map(k => records.count(_._2 == k))
    println(s"rows ${records.size}, providers $m (keys ${keys.head} to ${keys.last}), records per provider ${sizes.min} to ${sizes.max}, events ${records.map(_._3).sum}")
    val ybar = records.map(_._3).sum.toDouble / records.size
    val start = Array.fill(m)(StrictMath.log(ybar / (1.0 - ybar)))
    println(s"start: gamma_j = logit(${g(ybar)}) = ${g(start(0))}; matches pprof_py's start (rel ${e(rel(start, arr(at(py, "function.start.gamma"))))})")
    for (point <- Seq("start", "fixed")) {
      val ref = at(py, s"function.$point")
      val gamma = arr(ref.get("gamma"))
      val beta = arr(ref.get("beta"))
      val t = Kernel.terms(rows, gamma, beta)
      val s = Kernel.schur(rows, gamma, beta)
      val bg = arr(ref.get("info_beta_gamma"))
      val transposed = Array.tabulate(m * p)(i => t.crossInfo((i % m) * p + i / m))
      val crossRel = math.min(rel(t.crossInfo, bg), rel(transposed, bg))
      println(
        s"function level at $point (beta ${beta.map(g).mkString(", ")}): loglik ${g(t.loglik)} (pprof_py ${g(num(ref.get("loglik").get(0)))}); " +
          s"rel diff: loglik ${e(rel(Array(t.loglik), arr(ref.get("loglik"))))}, score_gamma ${e(rel(t.scoreGamma, arr(ref.get("score_gamma"))))}, " +
          s"info_gamma ${e(rel(t.infoGamma, arr(ref.get("info_gamma"))))}, info_beta_gamma ${e(crossRel)}, score_beta ${e(rel(t.scoreBeta, arr(ref.get("score_beta"))))}, " +
          s"info_beta ${e(rel(t.infoBeta, arr(ref.get("info_beta"))))}, schur (centred) ${e(rel(s.packed, arr(ref.get("schur"))))}"
      )
    }
    val passes = new InMemorySerbinPasses(Seq(rows))
    for (k <- 1 to 5) {
      val r = Serbin.fit(start, new Array[Double](p), passes, SerbinOptions(maxIter = k - 1))
      val ref = py.get("iterates").get(k - 1)
      println(
        f"iterate $k: beta ${r.beta.map(x => f"$x%.10f").mkString(", ")}  v ${r.lastStep}%.4f (pprof_py v ${num(ref.get("step"))}%.4f)  rel diff beta ${e(rel(r.beta, arr(ref.get("beta"))))} gamma ${e(rel(r.gamma, arr(ref.get("gamma"))))}"
      )
    }
    val default = Serbin.fit(start, new Array[Double](p), passes, SerbinOptions())
    inMemory = default
    println(s"default fit: ${default.iterations} steps (pprof_py ${py.get("default").get("iterations").asInt}), converged ${default.converged}, criterion ${e(default.criterion)}, last v ${default.lastStep}, shortened ${default.shortened}")
    default.trace.foreach(s => println(f"  step ${s.iteration}%2d: loglik at start ${g(s.loglik)}  v ${s.step}%.4f  ||dbeta||inf ${s.betaChange}%.3e  step lengths evaluated ${s.evaluated}"))
    println(s"  beta ${default.beta.map(g).mkString(", ")}")
    println(s"  rel diff: beta vs pprof_py default ${e(rel(default.beta, arr(at(py, "default.beta"))))}, gamma ${e(rel(default.gamma, arr(at(py, "default.gamma"))))}; vs R SerBIN default beta ${e(rel(default.beta, arr(at(rr, "serbin_default.beta"))))}")
    val tight = Serbin.fit(start, new Array[Double](p), passes, SerbinOptions(tol = 1e-13))
    println(s"tight fit (tol 1e-13): ${tight.iterations} steps (pprof_py ${py.get("tight").get("iterations").asInt}); beta ${tight.beta.map(g).mkString(", ")}")
    println(
      s"  rel diff beta: pprof_py ${e(rel(tight.beta, arr(at(py, "tight.beta"))))}, R SerBIN ${e(rel(tight.beta, arr(at(rr, "serbin_tight.beta"))))}, glm ${e(rel(tight.beta, arr(at(rr, "glm.beta"))))}; " +
        s"gamma: pprof_py ${e(rel(tight.gamma, arr(at(py, "tight.gamma"))))}, R SerBIN ${e(rel(tight.gamma, arr(at(rr, "serbin_tight.gamma"))))}, glm ${e(rel(tight.gamma, arr(at(rr, "glm.gamma"))))}"
    )
    for ((label, fit, refs) <- Seq(("default", default, Seq("pprof_py" -> at(py, "default"))), ("tight", tight, Seq("pprof_py" -> at(py, "tight"), "glm" -> at(rr, "glm"))))) {
      val vs = Kernel.varianceSchur(rows, fit.gamma, fit.beta)
      val cov = Cholesky.factor(vs, p).inversePacked
      val xbar = Array.tabulate(p)(j => records.map(_._4(j)).sum / records.size)
      val pv = Kernel.providerVariances(rows, fit.gamma, fit.beta, cov, xbar)
      val ll = Kernel.loglik(rows, fit.gamma, fit.beta)
      if (label == "default") inMemoryLoglik = ll
      val se = (0 until p).map(j => math.sqrt(cov(Seq(0, 3, 5)(j))))
      println(f"$label variances: se(beta) ${se.map(g).mkString(", ")}; loglik ${g(ll)}; AIC ${g(2.0 * (p + m) - 2.0 * ll)}; z ${(0 until p).map(j => f"${fit.beta(j) / se(j)}%.6f").mkString(", ")}")
      refs.foreach { case (name, ref) =>
        println(
          s"  vs $name: var_beta ${e(rel(cov, arr(ref.get("var_beta"))))}, var_gamma ${e(rel(pv.varGamma, arr(ref.get("var_gamma"))))}, " +
            s"var_case_mix ${e(rel(pv.varCaseMix, arr(ref.get("var_case_mix"))))}, loglik ${e(rel(Array(ll), arr(ref.get("loglik"))))}"
        )
      }
      if (label == "default") {
        val k = (0 until m).maxBy(j => pv.varGamma(j) - pv.varCaseMix(j))
        println(f"  provider ${keys(k)}: gamma ${fit.gamma(k)}%.6f, var_gamma ${pv.varGamma(k)}%.6f, var_case_mix ${pv.varCaseMix(k)}%.6f; median gamma ${Serbin.median(fit.gamma)}%.6f")
      }
    }
  }

  def partB(): Unit = {
    println("=" * 100)
    println("PART B: the real engine in local Spark (local[4], 4 shuffle partitions, AQE and ANSI on)")
    val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
      .builder()
      .master("local[4]")
      .appName("pprof-logistic-probe")
      .config("spark.ui.enabled", "false")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .config("spark.sql.ansi.enabled", "true")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    val sc = spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession].sparkContext
    sc.setLogLevel("ERROR")
    val rec = new LRecorder
    sc.addSparkListener(rec)
    val order = scala.collection.mutable.ArrayBuffer.empty[String]
    def phase[A](name: String)(body: => A): A = {
      order += name
      sc.setLocalProperty("pprof.phase", name)
      try body
      finally sc.setLocalProperty("pprof.phase", null)
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("provider", LongType, nullable = false),
        StructField("y", IntegerType, nullable = false),
        StructField("x1", DoubleType, nullable = false),
        StructField("x2", DoubleType, nullable = false),
        StructField("x3", DoubleType, nullable = false)
      )
    )
    def frame(rows: Seq[(Long, Long, Int, Array[Double])]): DataFrame =
      spark.createDataFrame(rows.map { case (id, prov, y, x) => Row(id, prov, y, x(0), x(1), x(2)) }.asJava, schema)
    val df = frame(records)
    val spec = LogisticSpec("y", Seq("x1", "x2", "x3"), "provider", rowId = Some("id"))
    val fit = phase("1: LogisticFE.fit(lfe-base, defaults)")(LogisticFE.fit(df, spec))
    def bits(a: Seq[Double]) = a.map(java.lang.Double.doubleToLongBits)
    println(s"engine fit: ${fit.iterations} steps, converged ${fit.converged}, layout ${fit.layout}, AUC ${fit.auc.map(g)}, warnings ${fit.warnings.size}")
    println(
      s"  bitwise equal to Part A's in-memory SerBIN: beta ${bits(fit.estimates) == bits(inMemory.beta.toSeq)}, gamma ${bits(fit.providers.gamma.toSeq) == bits(inMemory.gamma.toSeq)}, loglik ${bits(Seq(fit.loglik)) == bits(Seq(inMemoryLoglik))}"
    )
    val passes = fit.trace.map(s => 1 + (s.evaluated + 7) / 8).sum
    println(s"  Newton passes: ${fit.trace.size} steps, ${passes} passes (pass A plus one pass B per eight step lengths)")
    val py = json("pprof_py")
    val rr = json("r_logistic")
    val tests = phase("2: LogisticProviderTests.test(exact, two-sided, median)")(LogisticProviderTests.test(df, fit).collect())
    val z = tests.map(_.getAs[Double]("z_raw"))
    val pv = tests.map(_.getAs[Double]("p_value"))
    val flag = tests.map(_.getAs[Int]("flag").toDouble)
    val lo = tests.map(_.getAs[Double]("ci_lower"))
    val hi = tests.map(_.getAs[Double]("ci_upper"))
    val ref = at(py, "provider_tests.exact_two_sided_median")
    val rt = at(rr, "r_tests_exact_two_sided")
    println(
      s"exact provider tests: ${tests.length} providers, flags +1: ${flag.count(_ > 0)}, -1: ${flag.count(_ < 0)}; rel diff vs pprof_py: z ${e(rel(z, arr(ref.get("z"))))}, p ${e(rel(pv, arr(ref.get("p"))))}, " +
        s"limits ${e(rel(lo, arr(ref.get("ci_lower"))))} / ${e(rel(hi, arr(ref.get("ci_upper"))))}, flags equal ${flag.sameElements(arr(ref.get("flag")))}; vs R: z ${e(rel(z, arr(rt.get("z"))))}, flags equal ${flag.sameElements(arr(rt.get("flag")))}"
    )
    val j = (0 until tests.length).maxBy(k => math.abs(z(k)))
    println(
      f"  largest |z|: provider ${tests(j).get(0)}, observed ${tests(j).getAs[Double]("observed")}%.0f, expected ${tests(j).getAs[Double]("expected")}%.4f, z ${z(j)}%.6f, p ${pv(j)}%.3e, interval [${lo(j)}%.4f, ${hi(j)}%.4f], gamma ${tests(j).getAs[Double]("estimate")}%.4f"
    )
    val four = LogisticOptions(blocks = BlockOptions(targetBlockBytes = 16800L))
    val fit4 = phase("3: LogisticFE.fit(lfe-base, targetBlockBytes = 16800)")(LogisticFE.fit(df, spec, four))
    println(s"four-block layout ${fit4.layout}: beta bitwise equal to one block ${bits(fit4.estimates) == bits(fit.estimates)}, max rel diff beta ${e(rel(fit4.estimates.toArray, fit.estimates.toArray))}, gamma ${e(rel(fit4.providers.gamma, fit.providers.gamma))}, loglik ${e(rel(Array(fit4.loglik), Array(fit.loglik)))}, steps ${fit4.iterations}")
    phase("4: R0 probes") {
      spark.conf.set("spark.sql.shuffle.partitions", "1")
      val a = LogisticFE.fit(df, spec, four)
      spark.conf.set("spark.sql.shuffle.partitions", "7")
      val b = LogisticFE.fit(frame(records.reverse).repartition(3), spec, four)
      spark.conf.set("spark.sql.shuffle.partitions", "4")
      def all(f: LogisticFit) = bits(f.estimates ++ f.covariance ++ f.providers.gamma.toSeq :+ f.loglik)
      println(s"  R0 bitwise (four blocks): shuffle.partitions 1 vs 4: ${all(a) == all(fit4)}; reversed + repartition(3) at 7: ${all(b) == all(fit4)}")
    }
    Thread.sleep(2500)
    println("=" * 100)
    println("Spark jobs by phase (stages run/skipped, tasks, shuffle bytes)")
    val jobs = rec.jobs.asScala.toVector.sortBy(_.id)
    order.foreach { ph =>
      val js = jobs.filter(_.phase == ph)
      println(s"[$ph] ${js.size} jobs")
      if (!ph.startsWith("4")) js.foreach { jb =>
        val done = jb.stageIds.flatMap(s => Option(rec.stages.get(s)))
        println(
          f"  job ${jb.id}%3d  stages ${done.size}/${jb.stageIds.size - done.size}  tasks ${done.map(_.tasks).sum}%3d  shuffle write ${done.map(_.shuffleWrite).sum}%7d B  read ${done.map(_.shuffleRead).sum}%7d B"
        )
      }
    }
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    partA()
    partB()
  }
}
