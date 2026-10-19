package pprof.spark.engine.cox

// Tutorial probe: documentation material for docs/tutorials/cox, not compiled by sbt. It lives in package
// pprof.spark.engine.cox so that it can call the private[cox] members CoxPH.withWorkingSet,
// Prepared and CoxPH.evaluateAt exactly as the engine does.

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobStart,
  SparkListenerStageCompleted
}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.numerics.{Cholesky, Evaluation, Newton, NewtonOptions, NewtonResult, Normal}
import pprof.spark.numerics.kernels.{CoxStratum, CoxTotals, Moments}

final case class JobRec(id: Int, phase: String, site: String, stageIds: Seq[Int])
final case class StageRec(id: Int, name: String, tasks: Int, shuffleWrite: Long, shuffleRead: Long)

class Recorder extends SparkListener {
  val jobs = new ConcurrentLinkedQueue[JobRec]()
  val stages = new ConcurrentHashMap[Int, StageRec]()
  override def onJobStart(e: SparkListenerJobStart): Unit = {
    val props = Option(e.properties)
    val phase = props.flatMap(p => Option(p.getProperty("pprof.phase"))).getOrElse("-")
    val site = props.flatMap(p => Option(p.getProperty("callSite.short"))).getOrElse("?")
    jobs.add(JobRec(e.jobId, phase, site, e.stageIds.toVector))
  }
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val i = e.stageInfo
    val m = Option(i.taskMetrics)
    stages.put(
      i.stageId,
      StageRec(
        i.stageId,
        i.name,
        i.numTasks,
        m.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(0L),
        m.map(_.shuffleReadMetrics.totalBytesRead).getOrElse(0L)
      )
    )
  }
}

object TutorialProbe {
  val fixtures: String = sys.props.getOrElse("pprof.fixtures", "fixtures/cox") + "/tiny-ties"
  val mapper = new ObjectMapper()

  def hx(d: Double): String = java.lang.Double.toHexString(d)
  def g(d: Double): String = f"$d%.17g"
  def rel(a: Double, e: Double): String = {
    val r = if (e == 0.0) math.abs(a) else math.abs(a - e) / math.abs(e)
    f"$r%.2e"
  }

  def json(source: String): JsonNode = mapper.readTree(new java.io.File(s"$fixtures/$source.json"))
  def hexes(node: JsonNode): Array[Double] =
    (0 until node.size).map(i => java.lang.Double.parseDouble(node.get(i).asText())).toArray

  lazy val rRef: JsonNode = json("r_survival")
  lazy val pyRef: JsonNode = json("pprof_py")
  def rv(ties: String, point: String, key: String): Array[Double] =
    hexes(rRef.get(ties).get(point).get(key))
  def pv(ties: String, point: String, key: String): Array[Double] =
    hexes(pyRef.get(ties).get(point).get(key))

  // id, stratum, time, event, x1 (input.csv order)
  lazy val tinyRows: Vector[(Long, Long, Double, Int, Double)] = {
    val src = scala.io.Source.fromFile(s"$fixtures/input.csv")
    try
      src
        .getLines()
        .drop(1)
        .map { line =>
          val f = line.split(",")
          (f(0).toLong, f(1).toLong, f(2).toDouble, f(3).toInt, f(6).toDouble)
        }
        .toVector
    finally src.close()
  }

  var kernelBits: Map[String, Long] = Map.empty

  def partA(): Unit = {
    println("=" * 100)
    println("PART A: the real CoxBlockBuilder / CoxKernel / Newton on tiny-ties, without Spark")
    val rows = tinyRows.map { case (i, _, t, e, x) =>
      CoxRow(0, 0, i, t, e == 1, 1.0, 0.0, Array(x))
    }
    val block = CoxBlockBuilder.build(0, rows.iterator, 1)
    println(
      "canonical order (id:time:event:x) = " + block.rowId.indices
        .map(r =>
          s"${block.rowId(r)}:${block.time(r)}:${if (block.event(r)) 1 else 0}:${block.x(r)}"
        )
        .mkString("  ")
    )
    println(
      s"entryOrder = ${block.entryOrder.mkString(",")}  groupStart = ${block.groupStart.mkString(",")}"
    )
    val center = Array(Moments.columnSums(block.x, 1, 0, block.rowCount)(0) / block.rowCount)
    println(s"center (column mean) = ${center(0)}")
    for (ties <- Seq("breslow", "efron")) {
      val efron = ties == "efron"
      def ev(b: Array[Double]): Evaluation = {
        val p = CoxKernel.partial(block, b, center, efron)
        Evaluation(p.value, p.score, p.information)
      }
      println("-" * 100)
      println(s"[$ties] function level")
      for ((point, b) <- Seq("beta_zero" -> 0.0, "beta_fixed" -> 0.375)) {
        val e = ev(Array(b))
        val names = Seq("loglik", "score", "information")
        val ours = Seq(e.value, e.gradient(0), e.information(0))
        names.zip(ours).foreach { case (k, a) =>
          val r = rv(ties, point, k)(0)
          println(f"  beta=$b%-6s $k%-12s kernel ${g(a)}  R ${g(r)}  rel.diff ${rel(a, r)}")
        }
      }
      def runNewton(label: String, o: NewtonOptions): NewtonResult = {
        val res = Newton.maximize(ev, Array(0.0), o)
        println(s"[$ties] Newton ($label: eps=${o.eps}, maxIterations=${o.maxIterations})")
        println(
          f"  it 0  beta ${g(0.0)}  loglik ${g(res.initial.value)}  U ${g(res.initial.gradient(0))}  I ${g(res.initial.information(0))}"
        )
        var previous = res.initial.value
        var previousBeta = 0.0
        res.log.foreach { r =>
          val change = math.abs(r.value - previous) / math.abs(previous)
          println(
            f"  it ${r.iteration}  beta ${g(r.beta(0))}  loglik ${g(r.value)}  step ${g(r.beta(0) - previousBeta)}  halvings ${r.halvings}  relchange ${change}%.3e"
          )
          previous = r.value
          previousBeta = r.beta(0)
        }
        println(
          s"  converged=${res.converged} iterations=${res.iterations} halvings=${res.halvings}  beta hex ${hx(res.beta(0))}"
        )
        res
      }
      val default = runNewton("default", NewtonOptions())
      kernelBits += (s"$ties.default.beta" -> java.lang.Double.doubleToLongBits(default.beta(0)))
      kernelBits += (s"$ties.default.loglik" -> java.lang.Double.doubleToLongBits(
        default.evaluation.value
      ))
      val pyDefault = pv(ties, "default", "coef")(0)
      val pyIterations = pyRef.get(ties).get("default").get("iterations").asInt()
      val rDefault = rv(ties, "default", "coef")(0)
      println(
        s"  default beta vs pprof_py ${g(pyDefault)} (rel ${rel(default.beta(0), pyDefault)}, pprof_py iterations $pyIterations); vs R ${g(rDefault)} (rel ${rel(default.beta(0), rDefault)})"
      )
      val tight = runNewton("tight", NewtonOptions(maxIterations = 100, eps = 1e-11))
      val b = tight.beta(0)
      val factor = Cholesky.factor(tight.evaluation.information, 1)
      val v = factor.inversePacked(0)
      val se = math.sqrt(v)
      val z = b / se
      val q = Normal.upperQuantile(0.025)
      println(
        f"[$ties] tight inference: beta ${g(b)}  I(beta) ${g(tight.evaluation.information(0))}  var ${g(v)}  se ${g(se)}"
      )
      println(
        f"  z ${g(z)}  p ${g(Normal.twoSidedPValue(z))}  q ${g(q)}  CI [${g(b - q * se)}, ${g(b + q * se)}]  HR ${g(math.exp(b))}"
      )
      for (
        (k, a) <- Seq(
          "coef" -> b,
          "se" -> se,
          "loglik" -> tight.evaluation.value,
          "loglik_null" -> tight.initial.value
        )
      ) {
        val r = rv(ties, "tight", k)(0)
        val py = pv(ties, "tight", k)(0)
        println(
          f"  $k%-12s ours ${g(a)}  R ${g(r)} (rel ${rel(a, r)})  pprof_py ${g(py)} (rel ${rel(a, py)})"
        )
      }
      val base = CoxKernel.baseline(block, Array(b), efron)
      val rBase = rv(ties, "tight", "basehaz_hazard")
      base.zip(rBase).foreach { case (row, r) =>
        println(
          f"  baseline t=${row.time}%-4s dLambda ${g(row.increment)}  Lambda ${g(row.cumulativeHazard)}  S0 ${g(row.survival)}  R Lambda ${g(r)} (rel ${rel(row.cumulativeHazard, r)})"
        )
      }
      val res = CoxKernel.residuals(block, Array(b), center, efron, Array(v)).sortBy(_.rowId)
      val rm = rv(ties, "tight", "martingale")
      val rs = rv(ties, "tight", "score_1")
      val rd = rv(ties, "tight", "dfbeta_1")
      res.foreach { row =>
        val i = row.rowId.toInt
        println(
          f"  resid id $i  martingale ${g(row.martingale)} (R rel ${rel(row.martingale, rm(i))})  score ${g(row.score(0))} (R rel ${rel(row.score(0), rs(i))})  dfbeta ${g(row.dfbeta(0))} (R rel ${rel(row.dfbeta(0), rd(i))})"
        )
      }
      println(
        f"  sum of martingale ${g(res.map(_.martingale).sum)}  sum of score ${g(res.map(_.score(0)).sum)}  U(beta) ${g(tight.evaluation.gradient(0))}"
      )
      val meat = CoxKernel.robustPerRow(block, Array(b), center, efron).outer(0)
      val robust = v * meat * v
      val rRobust = rv(ties, "tight", "robust_per_row")(0)
      println(
        f"  robust per row: B ${g(meat)}  V B V ${g(robust)}  robust se ${g(math.sqrt(robust))}  R ${g(rRobust)} (rel ${rel(robust, rRobust)})"
      )
    }
  }

  val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("stratum", LongType, nullable = false),
      StructField("time", DoubleType, nullable = false),
      StructField("event", IntegerType, nullable = false),
      StructField("x1", DoubleType, nullable = false)
    )
  )

  def frame(spark: SparkSession, rows: Seq[(Long, Long, Double, Int, Double)]): DataFrame =
    spark.createDataFrame(rows.map { case (i, s, t, e, x) => Row(i, s, t, e, x) }.asJava, schema)

  def shortPlan(s: String): String =
    s.replaceAll("#\\d+L?", "")
      .replaceAll("\\[[^\\[\\]]{40,}\\]", "[...]")
      .replaceAll("\\([^()]{60,}\\)", "(...)")
      .linesIterator
      .map(_.take(150))
      .mkString("\n")

  def partB(): Unit = {
    println("=" * 100)
    println(
      "PART B: the real engine in local Spark (local[4], 4 shuffle partitions, ANSI on, as testkit)"
    )
    val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
      .builder()
      .master("local[4]")
      .appName("pprof-tutorial-probe")
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
    println(s"Spark ${spark.version}; AQE ${spark.conf.get("spark.sql.adaptive.enabled")}")
    val rec = new Recorder
    sc.addSparkListener(rec)
    val order = scala.collection.mutable.ArrayBuffer.empty[String]
    def phase[A](name: String)(body: => A): A = {
      order += name
      sc.setLocalProperty("pprof.phase", name)
      try body
      finally sc.setLocalProperty("pprof.phase", null)
    }

    val tiny = frame(spark, tinyRows)
    val tinySpec = CoxSpec("time", "event", Seq("x1"), rowId = Some("id"))
    val fit1 =
      phase("1: CoxPH.fit(tiny-ties, Breslow, defaults)")(CoxPH.fit(tiny, tinySpec, CoxOptions()))
    println(
      s"tiny-ties engine fit: beta ${g(fit1.estimates(0))} se ${g(fit1.standardErrors(0))} loglik ${g(fit1.logLikelihood)} iterations ${fit1.iterations} halvings ${fit1.halvings} converged ${fit1.converged}"
    )
    println(s"  layout ${fit1.layout}")
    println(
      s"  bitwise equal to Part A (single-block kernel + Newton): beta ${java.lang.Double.doubleToLongBits(fit1.estimates(0)) == kernelBits("breslow.default.beta")}, loglik ${java.lang.Double.doubleToLongBits(fit1.logLikelihood) == kernelBits("breslow.default.loglik")}"
    )
    println(
      s"  iteration log: ${fit1.iterationLog.map(r => s"(${r.iteration}, ${g(r.beta(0))}, ${g(r.value)}, h=${r.halvings})").mkString(" ")}"
    )

    val toyRows: Vector[(Long, Long, Double, Int, Double)] =
      tinyRows.map { case (i, _, t, e, x) => (i, 10L, t, e, x) } ++ Vector(
        (100L, 20L, 1.0, 1, 0.25),
        (101L, 20L, 2.0, 0, -0.5),
        (102L, 20L, 2.0, 1, 0.5),
        (103L, 20L, 3.0, 1, -0.25),
        (104L, 20L, 5.0, 0, 1.0),
        (200L, 30L, 2.0, 1, -0.75),
        (201L, 30L, 3.0, 0, 0.5),
        (202L, 30L, 4.0, 1, 0.0)
      )
    val toy = frame(spark, toyRows)
    val toySpec = CoxSpec("time", "event", Seq("x1"), strata = Some("stratum"), rowId = Some("id"))
    val twoBlocks = CoxOptions(blocks = BlockOptions(targetBlockBytes = 400L))
    val fitOne =
      phase("2: CoxPH.fit(toy, 3 strata, default blocks)")(CoxPH.fit(toy, toySpec, CoxOptions()))
    val fitTwo =
      phase("3: CoxPH.fit(toy, 3 strata, targetBlockBytes=400)")(CoxPH.fit(toy, toySpec, twoBlocks))
    println(s"toy default layout ${fitOne.layout}")
    println(s"toy small-block layout ${fitTwo.layout}")
    println(
      s"  toy beta: one block ${g(fitOne.estimates(0))} [${hx(fitOne.estimates(0))}]; two blocks ${g(fitTwo.estimates(0))} [${hx(fitTwo.estimates(0))}]; rel ${rel(fitTwo.estimates(0), fitOne.estimates(0))}"
    )
    println(
      s"  toy loglik: one block ${g(fitOne.logLikelihood)}; two blocks ${g(fitTwo.logLikelihood)}; rel ${rel(fitTwo.logLikelihood, fitOne.logLikelihood)}; iterations ${fitOne.iterations}/${fitTwo.iterations}"
    )

    phase("4: inspect the two-block working set (probe only)") {
      CoxPH.withWorkingSet(toy, toySpec, twoBlocks) { prepared =>
        println(
          s"plan: targetRowsPerBlock ${prepared.plan.targetRowsPerBlock}, blockCount ${prepared.plan.blockCount}, oversized ${prepared.plan.oversizedGroups}"
        )
        prepared.plan.placements.foreach(pl =>
          println(
            s"  stratum key ${pl.key} -> groupIndex ${pl.groupIndex}, rows ${pl.rows}, block ${pl.blockId}"
          )
        )
        println(
          s"center ${g(prepared.center(0))}; rows ${prepared.rows}; events ${prepared.events}; fingerprint ${prepared.fingerprint}"
        )
        val blocks = prepared.workingSet.blocks.collect().sortBy(_.blockId)
        val beta = fitTwo.estimates.toArray
        val c = prepared.center.clone()
        blocks.foreach { b =>
          println(
            s"  block ${b.blockId}: groups ${b.groupIndex.mkString(",")} starts ${b.groupStart.mkString(",")} rowIds ${b.rowId.mkString(",")}"
          )
          (0 until b.groupCount).foreach { k =>
            val t = new CoxTotals(1)
            CoxStratum.add(
              b.time,
              b.entry,
              b.entryOrder,
              b.event,
              b.weight,
              b.offset,
              b.x,
              1,
              b.groupStart(k),
              b.groupEnd(k),
              beta,
              c,
              false,
              t
            )
            println(
              f"    group ${b.groupIndex(k)}: loglik ${g(t.value.value)}  U ${g(t.score.values(0))}  I ${g(t.information.values(0))}"
            )
          }
          val part = CoxKernel.partial(b, beta, c, false)
          println(
            f"    partial(block ${part.blockId}) at beta-hat: loglik ${g(part.value)}  U ${g(part.score(0))}  I ${g(part.information(0))}"
          )
        }
        val evaluation = CoxPH.evaluateAt(toy, toySpec, beta, twoBlocks)
        println(
          f"  driver reduction in block order: loglik ${g(evaluation.value)}  U ${g(evaluation.gradient(0))}  I ${g(evaluation.information(0))}"
        )
        val cached =
          prepared.workingSet.blocks.asInstanceOf[org.apache.spark.sql.classic.Dataset[CoxBlock]]
        println("-- physical plan of the persisted working set (built once, one shuffle):")
        println(shortPlan(cached.queryExecution.executedPlan.toString))
        val evalDs = prepared.workingSet.blocks
          .map((b: CoxBlock) => CoxKernel.partial(b, beta, c, false))(Encoders.product[CoxPartial])
          .asInstanceOf[org.apache.spark.sql.classic.Dataset[CoxPartial]]
        println("-- physical plan of one evaluation pass (per Newton step):")
        println(shortPlan(evalDs.queryExecution.executedPlan.toString))
      }
    }

    phase("5: R0 probes (shuffle partitions 1 and 7, reversed and repartitioned input)") {
      spark.conf.set("spark.sql.shuffle.partitions", "1")
      val a = CoxPH.fit(toy, toySpec, twoBlocks)
      spark.conf.set("spark.sql.shuffle.partitions", "7")
      val b = CoxPH.fit(frame(spark, toyRows.reverse).repartition(3), toySpec, twoBlocks)
      spark.conf.set("spark.sql.shuffle.partitions", "4")
      def bits(f: CoxFit) =
        (f.estimates ++ f.covariance :+ f.logLikelihood).map(java.lang.Double.doubleToLongBits)
      println(
        s"  R0 bitwise: shuffle.partitions=1 vs 4: ${bits(a) == bits(fitTwo)}; reversed+repartition(3) with 7: ${bits(b) == bits(fitTwo)}"
      )
    }

    Thread.sleep(2500)
    println("=" * 100)
    println(
      "Spark jobs by phase (call site = first engine frame; stages run/skipped; tasks; shuffle bytes)"
    )
    val all = rec.jobs.asScala.toVector.sortBy(_.id)
    order.foreach { ph =>
      println(s"[$ph]")
      all.filter(_.phase == ph).foreach { j =>
        val done = j.stageIds.flatMap(s => Option(rec.stages.get(s)))
        val tasks = done.map(_.tasks).sum
        val w = done.map(_.shuffleWrite).sum
        val r = done.map(_.shuffleRead).sum
        println(
          f"  job ${j.id}%3d  ${j.site}%-44s stages ${done.size}/${j.stageIds.size - done.size}  tasks $tasks%3d  shuffle write $w%6d B  read $r%6d B"
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
