package pprof.spark.engine

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.SplittableRandom

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  DataType,
  DoubleType,
  IntegerType,
  LongType,
  StructField,
  StructType
}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.cox.{CoxFixtures, CoxGoldenPoints, CoxOptions, CoxPH, CoxSpec, Ties}
import pprof.spark.engine.logistic.{
  LogisticFE,
  LogisticFixtures,
  LogisticGoldenPoints,
  LogisticOptions,
  LogisticSpec
}
import pprof.spark.numerics.Evaluation
import pprof.spark.testkit.SparkSuite

/** NUM-2's golden runs (docs/spec/performance/plan.md §6.1, D-75). Every Cox and logistic fixture case, at default
  * and tight options, two multi-block synthetic cases per model, and two unstratified Cox cases spread over many
  * blocks and evaluated from per-time aggregates (D-81), are recorded as the IEEE-754 bit patterns of
  * their outputs in `golden/runs.json`. Any difference fails: a change meant to preserve bits must leave the manifest
  * as it is, and one that reorders sums replaces it under the re-baselining rule of plan §6.3 (D-78). Each run also
  * records, under D-79, the values step 2's chunked sums touch at the fit's start and at its estimates
  * (docs/spec/performance/step2-chunks.md §6). With the environment variable PPROF_GOLDEN_WRITE set to a path, the
  * suite writes the manifest there instead of comparing.
  */
class GoldenRunsSuite extends SparkSuite {
  import GoldenRunsSuite._

  override val munitTimeout: Duration = Duration(20, "min")

  /** With PPROF_GOLDEN_BASELINE set to a previous manifest (and no write), each run's estimates point is evaluated at
    * that manifest's estimates, for D-78's comparison (D-79).
    */
  private lazy val baseline: Option[Map[String, Run]] =
    if (sys.env.get(WriteVariable).exists(_.nonEmpty)) None
    else
      sys.env.get(BaselineVariable).filter(_.nonEmpty).map { path =>
        parse(new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8))
          .map(r => r.name -> r)
          .toMap
      }

  private lazy val synthetic: Vector[(Run, Run)] =
    Vector(
      coxA(),
      coxB(),
      coxAggregated("lt-unstratified", Ties.Efron),
      coxAggregated("rc-unstratified", Ties.Breslow),
      logisticA(),
      logisticA(LogisticOptions.ProfileMethod),
      logisticB()
    )
  private lazy val computed: Vector[Run] = coxFixtures ++ logisticFixtures ++ synthetic.map(_._1)

  test("every golden run matches its recorded bits (NUM-2)") {
    sys.env.get(WriteVariable).filter(_.nonEmpty) match {
      case Some(path) =>
        Files.write(Paths.get(path), render(computed).getBytes(StandardCharsets.UTF_8))
        println(s"$WriteVariable: wrote ${computed.size} golden runs to $path")
      case None if baseline.nonEmpty =>
        val (lines, violations) = underD78(baseline.get.values.toVector, computed)
        println(
          s"$BaselineVariable: D-78's comparison over ${computed.size} runs:\n" + lines.mkString(
            "\n"
          )
        )
        assert(violations.isEmpty, violations.mkString("\n"))
      case None =>
        val problems = compare(load(), computed)
        assert(
          problems.isEmpty,
          s"${problems.size} differences from $Resource (plan §6.3):\n" + problems
            .take(25)
            .mkString("\n")
        )
    }
  }

  test("the multi-block runs are bit-identical at 1 and 7 partitions (R0)") {
    synthetic.foreach { case (one, seven) =>
      val problems = compare(Vector(one), Vector(seven))
      assert(problems.isEmpty, s"${one.name}:\n" + problems.mkString("\n"))
    }
  }

  test("D-78's comparison holds values within their bounds and flags the rest (D-79)") {
    val run = computed.find(_.doubles.contains("at_start_information")).get
    def scaledBy(key: String, factor: Double) =
      run.copy(doubles = run.doubles.updated(key, run.doubles(key).map(v => v * (1.0 + factor))))
    assert(underD78(Vector(run), Vector(scaledBy("at_start_information", 1e-15)))._2.isEmpty)
    assert(underD78(Vector(run), Vector(scaledBy("at_start_information", 1e-12)))._2.nonEmpty)
    assert(underD78(Vector(run), Vector(scaledBy("estimates", 1e-12)))._2.isEmpty)
    assert(underD78(Vector(run), Vector(scaledBy("estimates", 1e-9)))._2.nonEmpty)
    assert(underD78(Vector(run), Vector(scaledBy("at_start_score", 1e-14)))._2.nonEmpty)
    val counted =
      run.copy(integers = run.integers.updated("iterations", run.integers("iterations").map(_ + 1)))
    assert(underD78(Vector(run), Vector(counted))._2.nonEmpty)
  }

  test("the comparison reports a one-ulp change (negative control)") {
    val run = computed.head
    val estimates = run.doubles("estimates")
    val perturbed =
      run.copy(doubles =
        run.doubles.updated("estimates", estimates.updated(0, math.nextUp(estimates(0))))
      )
    val problems = compare(Vector(run), Vector(perturbed))
    assertEquals(problems.size, 1, problems.mkString("\n"))
    assert(
      problems.head.contains("estimates") && problems.head.contains("by up to 1 ulp"),
      problems.head
    )
  }

  private def coxRun(name: String, df: DataFrame, spec: CoxSpec, options: CoxOptions): Run = {
    val f = CoxPH.fit(df, spec, options)
    val atEstimates =
      baseline
        .flatMap(_.get(name))
        .map(_.doubles("estimates").toArray)
        .getOrElse(f.estimates.toArray)
    val points = CoxGoldenPoints.evaluate(
      df,
      spec,
      options,
      Seq(new Array[Double](spec.features.size), atEstimates)
    )
    def at(prefix: String, e: Evaluation): Map[String, Vector[Double]] =
      Map(
        s"${prefix}_loglik" -> Vector(e.value),
        s"${prefix}_score" -> e.gradient.toVector,
        s"${prefix}_information" -> e.information.toVector
      )
    Run(
      name,
      Map(
        "loglik" -> Vector(f.logLikelihood),
        "estimates" -> f.estimates.toVector,
        "standard_errors" -> f.standardErrors.toVector,
        "information" -> f.information.toVector,
        "covariance" -> f.covariance.toVector
      ) ++ at("at_start", points(0)) ++ at("at_estimates", points(1)),
      Map(
        "iterations" -> Vector(f.iterations.toLong),
        "halvings" -> Vector(f.halvings.toLong),
        "converged" -> Vector(if (f.converged) 1L else 0L)
      )
    )
  }

  private def logisticRun(
      name: String,
      df: DataFrame,
      spec: LogisticSpec,
      options: LogisticOptions
  ): Run = {
    val f = LogisticFE.fit(df, spec, options)
    val previous = baseline.flatMap(_.get(name))
    Run(
      name,
      Map(
        "loglik" -> Vector(f.loglik),
        "auc" -> f.auc.toVector,
        "estimates" -> f.coefficients.map(_.estimate),
        "standard_errors" -> f.coefficients.map(_.standardError),
        "covariance" -> f.covariance.toVector,
        "gamma" -> f.providers.gamma.toVector,
        "var_gamma" -> f.providers.varGamma.toVector,
        "var_case_mix" -> f.providers.varCaseMix.toVector
      ) ++ LogisticGoldenPoints.values(
        df,
        spec,
        options,
        previous.map(_.doubles("gamma").toArray).getOrElse(f.providers.gamma.toArray),
        previous
          .map(_.doubles("estimates").toArray)
          .getOrElse(f.coefficients.map(_.estimate).toArray)
      ),
      Map(
        "iterations" -> Vector(f.iterations.toLong),
        "converged" -> Vector(if (f.converged) 1L else 0L),
        "at_bound" -> f.providers.atBound.toVector.map(b => if (b) 1L else 0L)
      )
    )
  }

  private def coxFixtures: Vector[Run] =
    (for {
      name <- CoxFixtures.Cases
      ties <- CoxFixtures.Methods
      (label, options) <- Seq(
        "default" -> CoxOptions(ties = ties),
        "tight" -> CoxOptions(ties = ties, eps = 1e-11, maxIterations = 100)
      )
    } yield coxRun(
      s"cox/$name/${ties.name}/$label",
      CoxFixtures.frame(spark, name),
      CoxFixtures.spec(name),
      options
    )).toVector

  private def logisticFixtures: Vector[Run] =
    (for {
      name <- LogisticFixtures.Cases
      (label, options) <- Seq(
        "default" -> LogisticOptions(),
        "tight" -> LogisticOptions(tol = 1e-13),
        // The profile-likelihood Newton (D-83).
        "profile-default" -> LogisticOptions(newton = LogisticOptions.ProfileMethod),
        "profile-tight" -> LogisticOptions(tol = 1e-13, newton = LogisticOptions.ProfileMethod)
      )
    } yield logisticRun(
      s"logistic/$name/$label",
      LogisticFixtures.frame(spark, name),
      LogisticFixtures.spec(name),
      options
    )).toVector

  private def frame(rows: Seq[Row], columns: Seq[(String, DataType)]): DataFrame =
    spark.createDataFrame(
      rows.asJava,
      StructType(columns.map { case (n, t) => StructField(n, t, nullable = false) })
    )

  private def features(p: Int): Seq[String] = (1 to p).map(k => s"x$k")

  /** 1,500 rows in 12 strata, interleaved by identifier; weights, offsets, entry times, and times on a grid of
    * thirds, so Efron's ties; blocks of 8 KB.
    */
  private def coxA(): (Run, Run) = {
    val rows = (0 until 1500).map { i =>
      val rng = new SplittableRandom(mix(11L, i.toLong))
      val x = Array.fill(3)(normal(rng))
      val offset = 0.2 * normal(rng)
      val weight = 0.5 + rng.nextDouble()
      val eta = 0.4 * x(0) - 0.3 * x(1) + 0.2 * x(2) + offset
      val event = -StrictMath.log(1.0 - rng.nextDouble()) / (0.1 * StrictMath.exp(eta))
      val censor = 10.0 * rng.nextDouble()
      val time = math.max(1.0, math.ceil(math.min(event, censor) * 3.0)) / 3.0
      val entry = math.min(2.0 * rng.nextDouble(), time / 2.0)
      Row.fromSeq(
        Seq[Any](
          i.toLong,
          (i % 12).toLong,
          time,
          if (event <= censor) 1 else 0,
          weight,
          offset,
          entry
        ) ++ x.toSeq
      )
    }
    val columns = Seq(
      "id" -> LongType,
      "stratum" -> LongType,
      "time" -> DoubleType,
      "event" -> IntegerType,
      "weight" -> DoubleType,
      "offset" -> DoubleType,
      "entry" -> DoubleType
    ) ++ features(3).map(_ -> DoubleType)
    val spec = CoxSpec(
      "time",
      "event",
      features(3),
      strata = Some("stratum"),
      rowId = Some("id"),
      weight = Some("weight"),
      offset = Some("offset"),
      entry = Some("entry")
    )
    val options = CoxOptions(ties = Ties.Efron, blocks = BlockOptions(targetBlockBytes = 8192L))
    val df = frame(rows, columns)
    (
      coxRun("cox/synthetic-a", df.repartition(1), spec, options),
      coxRun("cox/synthetic-a", df.repartition(7, col("id")), spec, options)
    )
  }

  /** An unstratified fixture case on blocks of 2 KB, 36 rows each at p = 3: its one stratum spreads over many blocks
    * and is evaluated from per-time aggregates (docs/spec/cox/per-time-aggregates.md, D-81).
    */
  private def coxAggregated(name: String, ties: Ties): (Run, Run) = {
    val df = CoxFixtures.frame(spark, name)
    val spec = CoxFixtures.spec(name)
    val options = CoxOptions(
      ties = ties,
      blocks = BlockOptions(targetBlockBytes = 2048L),
      aggregateAboveBytes = 0L
    )
    (
      coxRun(s"cox/aggregated-$name", df.repartition(1), spec, options),
      coxRun(s"cox/aggregated-$name", df.repartition(7), spec, options)
    )
  }

  /** 2,000 rows in 40 strata, two continuous and two binary features, continuous times, Breslow; blocks of 16 KB. */
  private def coxB(): (Run, Run) = {
    val rows = (0 until 2000).map { i =>
      val rng = new SplittableRandom(mix(12L, i.toLong))
      val x = Array(
        normal(rng),
        normal(rng),
        if (rng.nextDouble() < 0.3) 1.0 else 0.0,
        if (rng.nextDouble() < 0.5) 1.0 else 0.0
      )
      val eta = 0.3 * x(0) + 0.5 * x(1) - 0.4 * x(2) + 0.2 * x(3)
      val event = -StrictMath.log(1.0 - rng.nextDouble()) / (0.2 * StrictMath.exp(eta))
      val censor = 8.0 * rng.nextDouble()
      val time = math.max(1e-3, math.min(event, censor))
      Row.fromSeq(
        Seq[Any](i.toLong, (i % 40).toLong, time, if (event <= censor) 1 else 0) ++ x.toSeq
      )
    }
    val columns =
      Seq("id" -> LongType, "stratum" -> LongType, "time" -> DoubleType, "event" -> IntegerType) ++
        features(4).map(_ -> DoubleType)
    val spec = CoxSpec("time", "event", features(4), strata = Some("stratum"), rowId = Some("id"))
    val options = CoxOptions(ties = Ties.Breslow, blocks = BlockOptions(targetBlockBytes = 16384L))
    val df = frame(rows, columns)
    (
      coxRun("cox/synthetic-b", df.repartition(1), spec, options),
      coxRun("cox/synthetic-b", df.repartition(7, col("id")), spec, options)
    )
  }

  /** 2,400 rows of binomial outcomes with 1 to 5 trials, 30 providers; blocks of 4 KB. */
  private def logisticA(newton: String = LogisticOptions.SerbinMethod): (Run, Run) = {
    val rows = (0 until 2400).map { i =>
      val rng = new SplittableRandom(mix(21L, i.toLong))
      val provider = (i % 30).toLong
      val effect = -0.5 + 0.8 * normal(new SplittableRandom(mix(121L, provider)))
      val x = Array.fill(3)(normal(rng))
      val probability = expit(effect + 0.5 * x(0) - 0.3 * x(1) + 0.2 * x(2))
      val trials = 1 + rng.nextInt(5)
      val y = (0 until trials).count(_ => rng.nextDouble() < probability)
      Row.fromSeq(Seq[Any](i.toLong, provider, y, trials) ++ x.toSeq)
    }
    val columns =
      Seq("id" -> LongType, "provider" -> LongType, "y" -> IntegerType, "n" -> IntegerType) ++
        features(3).map(_ -> DoubleType)
    val spec = LogisticSpec("y", features(3), "provider", trials = Some("n"), rowId = Some("id"))
    val options = LogisticOptions(blocks = BlockOptions(targetBlockBytes = 4096L), newton = newton)
    val df = frame(rows, columns)
    val name =
      if (newton == LogisticOptions.SerbinMethod) "logistic/synthetic-a"
      else s"logistic/synthetic-a-$newton"
    (
      logisticRun(name, df.repartition(1), spec, options),
      logisticRun(name, df.repartition(7, col("id")), spec, options)
    )
  }

  /** 3,000 Bernoulli rows, 50 providers, one with no events (at the bound) and one of 8 rows (excluded); blocks
    * of 4 KB.
    */
  private def logisticB(): (Run, Run) = {
    val rows = (0 until 3000).map { i =>
      val rng = new SplittableRandom(mix(22L, i.toLong))
      val provider = if (i < 8) 99L else (i % 50).toLong
      val effect = -0.3 + 0.6 * normal(new SplittableRandom(mix(122L, provider)))
      val x = Array(
        normal(rng),
        if (rng.nextDouble() < 0.4) 1.0 else 0.0,
        normal(rng),
        if (rng.nextDouble() < 0.2) 1.0 else 0.0
      )
      val probability = expit(effect + 0.4 * x(0) - 0.6 * x(1) + 0.3 * x(2) + 0.5 * x(3))
      val y = if (provider == 7L) 0 else if (rng.nextDouble() < probability) 1 else 0
      Row.fromSeq(Seq[Any](i.toLong, provider, y) ++ x.toSeq)
    }
    val columns =
      Seq("id" -> LongType, "provider" -> LongType, "y" -> IntegerType) ++ features(4).map(
        _ -> DoubleType
      )
    val spec = LogisticSpec("y", features(4), "provider", rowId = Some("id"))
    val options = LogisticOptions(blocks = BlockOptions(targetBlockBytes = 4096L))
    val df = frame(rows, columns)
    (
      logisticRun("logistic/synthetic-b", df.repartition(1), spec, options),
      logisticRun("logistic/synthetic-b", df.repartition(7, col("id")), spec, options)
    )
  }
}

object GoldenRunsSuite {

  val WriteVariable: String = "PPROF_GOLDEN_WRITE"
  val BaselineVariable: String = "PPROF_GOLDEN_BASELINE"

  /** D-78's bound for a quantity, scaled by its largest element (D-09), as D-79 maps it; None means bit-identical.
    * Converged values are within T-part: 1e-12 for the log-likelihood and 1e-10 for parameters and their variances.
    * The fixed points' information, Schur complements and the co-moments are within 1e-13. Their log-likelihoods,
    * scores, right-hand sides and score terms, and the AUC, are unchanged by step 2 and must be identical.
    */
  def bound(key: String): Option[Double] = key match {
    case "loglik" => Some(1e-12)
    case "estimates" | "standard_errors" | "information" | "covariance" | "gamma" | "var_gamma" |
        "var_case_mix" =>
      Some(1e-10)
    case k if k.endsWith("_information") || k.endsWith("_schur") || k == "comoments" => Some(1e-13)
    case _                                                                           => None
  }

  /** D-78's comparison of the computed runs with a previous manifest (D-79): for each quantity, the largest scaled
    * difference over the runs and the number of runs that match to the bit, then the violations of its bound. Counts
    * and flags must match.
    */
  def underD78(previous: Vector[Run], computed: Vector[Run]): (Vector[String], Vector[String]) = {
    val p = previous.map(r => r.name -> r).toMap
    val c = computed.map(r => r.name -> r).toMap
    val names = (p.keySet intersect c.keySet).toVector.sorted
    val unmatched =
      ((p.keySet ++ c.keySet) -- names).toVector.sorted.map(n => s"$n: in one manifest only")
    val keys = names.flatMap(n => p(n).doubles.keySet ++ c(n).doubles.keySet).distinct.sorted
    val doubles = keys.map { k =>
      val rows = names.flatMap { n =>
        (p(n).doubles.get(k), c(n).doubles.get(k)) match {
          case (Some(a), Some(b)) if a.length == b.length =>
            val scale = math.max(a.map(math.abs).foldLeft(0.0)(math.max), Double.MinPositiveValue)
            val difference =
              a.indices.map(i => math.abs(a(i) - b(i))).foldLeft(0.0)(math.max) / scale
            val identical = a.indices.forall(i =>
              java.lang.Double.doubleToRawLongBits(a(i)) == java.lang.Double.doubleToRawLongBits(
                b(i)
              )
            )
            Some((n, difference, identical))
          case (None, None) => None
          case _            => Some((n, Double.PositiveInfinity, false))
        }
      }
      val (at, largest, _) = rows.maxBy(_._2)
      val same = rows.count(_._3)
      val violated = bound(k) match {
        case Some(b) => largest > b
        case None    => same < rows.size
      }
      val limit = bound(k).fold("bit-identical")(b => f"bound $b%.0e")
      (
        f"$k: largest scaled difference $largest%.3e ($at); $limit; $same of ${rows.size} runs identical",
        violated
      )
    }
    val integers = names.flatMap { n =>
      (p(n).integers.keySet ++ c(n).integers.keySet).toVector.sorted.flatMap { k =>
        if (p(n).integers.get(k) == c(n).integers.get(k)) None
        else Some(s"$n $k: ${p(n).integers.get(k)} before, ${c(n).integers.get(k)} now")
      }
    }
    val lines =
      doubles.map(_._1) ++ Vector(s"counts and flags: ${integers.size} differences") ++ integers
    (lines, unmatched ++ doubles.filter(_._2).map(_._1) ++ integers)
  }
  val Resource: String = "/golden/runs.json"

  /** One golden run: its floating-point outputs and its counts and flags. */
  final case class Run(
      name: String,
      doubles: Map[String, Vector[Double]],
      integers: Map[String, Vector[Long]]
  )

  def mix(seed: Long, i: Long): Long = {
    var z = seed * 0x9e3779b97f4a7c15L + i
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }

  def normal(rng: SplittableRandom): Double =
    math.sqrt(-2.0 * StrictMath.log(1.0 - rng.nextDouble())) * StrictMath.cos(
      2.0 * math.Pi * rng.nextDouble()
    )

  def expit(eta: Double): Double = 1.0 / (1.0 + StrictMath.exp(-eta))

  def hex(v: Double): String = f"${java.lang.Double.doubleToRawLongBits(v)}%016x"

  def unhex(s: String): Double =
    java.lang.Double.longBitsToDouble(java.lang.Long.parseUnsignedLong(s, 16))

  /** The manifest: runs by name, quantities by name, one line each, doubles as 16 hexadecimal digits. */
  def render(runs: Vector[Run]): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("format", 1)
    root.put(
      "description",
      "NUM-2's golden runs (docs/spec/performance/plan.md §6.1): IEEE-754 bits in hexadecimal"
    )
    val all = root.putObject("runs")
    runs.sortBy(_.name).foreach { run =>
      val node = all.putObject(run.name)
      val doubles = node.putObject("doubles")
      run.doubles.toSeq.sortBy(_._1).foreach { case (k, vs) =>
        val a = doubles.putArray(k)
        vs.foreach(v => a.add(hex(v)))
      }
      val integers = node.putObject("integers")
      run.integers.toSeq.sortBy(_._1).foreach { case (k, vs) =>
        val a = integers.putArray(k)
        vs.foreach(v => a.add(v))
      }
    }
    val printer =
      new DefaultPrettyPrinter().withArrayIndenter(DefaultPrettyPrinter.NopIndenter.instance)
    mapper.writer(printer).writeValueAsString(root) + "\n"
  }

  def parse(text: String): Vector[Run] =
    new ObjectMapper()
      .readTree(text)
      .get("runs")
      .properties()
      .asScala
      .toVector
      .map { e =>
        val doubles = e.getValue.get("doubles").properties().asScala.toVector.map { q =>
          q.getKey -> q.getValue.elements().asScala.map(n => unhex(n.asText())).toVector
        }
        val integers = e.getValue.get("integers").properties().asScala.toVector.map { q =>
          q.getKey -> q.getValue.elements().asScala.map(_.asLong()).toVector
        }
        Run(e.getKey, doubles.toMap, integers.toMap)
      }

  def load(): Vector[Run] = {
    val stream = Option(getClass.getResourceAsStream(Resource)).getOrElse(
      throw new IllegalStateException(
        s"no $Resource on the test classpath: write it with $WriteVariable"
      )
    )
    try parse(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
    finally stream.close()
  }

  private def ordered(v: Double): Long = {
    val bits = java.lang.Double.doubleToRawLongBits(v)
    if (bits < 0L) Long.MinValue - bits else bits
  }

  def ulps(a: Double, b: Double): BigInt = (BigInt(ordered(a)) - BigInt(ordered(b))).abs

  /** Every difference between the recorded and the computed runs, one line per run and quantity. */
  def compare(recorded: Vector[Run], computed: Vector[Run]): Vector[String] = {
    val r = recorded.map(x => x.name -> x).toMap
    val c = computed.map(x => x.name -> x).toMap
    val missing = (r.keySet -- c.keySet).toVector.sorted.map(n => s"$n: recorded but not computed")
    val extra = (c.keySet -- r.keySet).toVector.sorted.map(n => s"$n: computed but not recorded")
    val differences = (r.keySet intersect c.keySet).toVector.sorted.flatMap { n =>
      val (a, b) = (r(n), c(n))
      val doubles = (a.doubles.keySet ++ b.doubles.keySet).toVector.sorted.flatMap { k =>
        (a.doubles.get(k), b.doubles.get(k)) match {
          case (Some(x), Some(y)) if x.length != y.length =>
            Some(s"$n $k: ${x.length} values recorded, ${y.length} computed")
          case (Some(x), Some(y)) =>
            val differing = x.indices.filter(i =>
              java.lang.Double.doubleToRawLongBits(x(i)) !=
                java.lang.Double.doubleToRawLongBits(y(i))
            )
            if (differing.isEmpty) None
            else {
              val most = differing.map(i => ulps(x(i), y(i))).max
              val relative = differing.map { i =>
                math.abs(x(i) - y(i)) / math.max(
                  math.max(math.abs(x(i)), math.abs(y(i))),
                  Double.MinPositiveValue
                )
              }.max
              Some(
                f"$n $k: ${differing.size} of ${x.length} values differ, by up to $most ulp, relative $relative%.3e"
              )
            }
          case _ => Some(s"$n $k: present on one side only")
        }
      }
      val integers = (a.integers.keySet ++ b.integers.keySet).toVector.sorted.flatMap { k =>
        (a.integers.get(k), b.integers.get(k)) match {
          case (Some(x), Some(y)) if x == y => None
          case (x, y)                       =>
            Some(
              s"$n $k: recorded ${x.fold("none")(_.take(12).mkString(","))}, computed ${y.fold("none")(_.take(12).mkString(","))}"
            )
        }
      }
      doubles ++ integers
    }
    missing ++ extra ++ differences
  }
}
