package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{broadcast, col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import pprof.spark.engine.cox.CoxProviderTests
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{Normal, PoissonBinomial, Serbin}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}

/** The reference effect γ₀ of the provider tests (Phase 2c specification §2). */
sealed trait EffectReference

object EffectReference {

  /** numpy's median of the fitted effects, degenerate providers included. */
  case object Median extends EffectReference

  /** The fitted effects' average weighted by records. */
  case object Mean extends EffectReference

  final case class Value(value: Double) extends EffectReference
}

/** One provider's test, with its p-value and flag. */
final case class LogisticProviderTestRow(
    groupIndex: Int,
    estimate: Double,
    se: Double,
    zRaw: Double,
    pValue: Double,
    flag: Int,
    lower: Double,
    upper: Double,
    observed: Double,
    expected: Double,
    trials: Double,
    records: Long
)

/** Provider tests of the logistic fixed-effect model (docs/spec/logistic/provider-tests.md), as pprof_py's
  * `test` with the theoretical null: exact Poisson-binomial (default), score, Wald and bootstrap.
  */
object LogisticProviderTests {

  private val log = LoggerFactory.getLogger(getClass)

  val Methods: Seq[String] = Seq("poibin_exact", "score", "wald", "bootstrap_exact")
  val Alternatives: Seq[String] = Seq("two_sided", "greater", "less")

  /** pprof_py's `PROVIDER_TEST_COLUMNS`, then observed, expected, trials and records. */
  val Columns: Seq[String] =
    CoxProviderTests.Columns
      .takeWhile(_ != "observed") ++ Seq("observed", "expected", "trials", "records")

  def reference(fit: LogisticFit, reference: EffectReference): Double = reference match {
    case EffectReference.Median => Serbin.median(fit.providers.gamma)
    case EffectReference.Mean   =>
      val providers = fit.providers
      var weighted = 0.0
      var total = 0.0
      providers.gamma.indices.foreach { k =>
        weighted += providers.records(k) * providers.gamma(k)
        total += providers.records(k).toDouble
      }
      weighted / total
    case EffectReference.Value(v) => v
  }

  /** One row per tested provider of the fit (all, or `providers`), with the fit's provider column and
    * [[Columns]]. Except for `wald`, `df` must be the fit's training data (API-3).
    */
  def test(
      df: DataFrame,
      fit: LogisticFit,
      method: String = "poibin_exact",
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None,
      nResample: Int = 10000,
      seed: Long = 0L
  ): DataFrame = {
    require(
      Methods.contains(method),
      s"method must be one of ${Methods.mkString(", ")}, got $method"
    )
    require(
      Alternatives.contains(alternative),
      s"alternative must be one of ${Alternatives.mkString(", ")}"
    )
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    require(nResample > 0, s"nResample must be positive, got $nResample")
    val spark = df.sparkSession
    val g0 = this.reference(fit, reference)
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      Normal.upperQuantile(if (alternative == "two_sided") alpha / 2.0 else alpha)
    )
    val keys = fit.providers.keys
    val wanted = providers.map(_.map(_.toString).toSet)
    val tested =
      keys.indices.filter(k => wanted.forall(_.contains(GroupKey.value(keys(k)).toString))).toSet
    val rows: Dataset[LogisticProviderTestRow] =
      if (method == "wald")
        spark.createDataset(waldRows(fit, g0, alternative, c, tested))(Encoders.product)
      else {
        val (workingSet, providersOfBlock) = LogisticFE.trainingWorkingSet(df, fit)
        val p = fit.spec.features.size
        val beta = fit.estimates.toArray
        val testedArray = (0 until keys.length).map(tested.contains).toArray
        try
          spark.createDataset(
            LogisticFE
              .withEffects(workingSet.blocks, providersOfBlock, p, fit.providers.gamma)
              .flatMap { (block: LogisticBlock) =>
                (0 until block.groupIndex.length).iterator
                  .filter(k => testedArray(block.groupIndex(k)))
                  .map(k =>
                    providerRow(block, k, beta, g0, method, alternative, c, nResample, seed)
                  )
              }(Encoders.product[LogisticProviderTestRow])
              .collect()
              .toSeq
          )(Encoders.product)
        finally workingSet.release()
      }
    if (method == "wald") {
      val degenerate = tested.count(k => fit.providers.zeroEvents(k) || fit.providers.allEvents(k))
      if (degenerate > 0)
        log.warn(
          s"$degenerate providers have no events or only events, so their effects have no finite estimate; " +
            "their Wald statistics and intervals are unreliable (the exact test handles them)"
        )
    }
    val text = keys.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val keyFrame = spark.createDataFrame(
      keys.indices.map(k => Row(k, GroupKey.value(keys(k)))).asJava,
      StructType(
        Seq(
          StructField("groupIndex", IntegerType, nullable = false),
          StructField(fit.spec.provider, if (text) StringType else LongType, nullable = false)
        )
      )
    )
    val wald = method == "wald"
    val nan = lit(Double.NaN)
    rows
      .toDF()
      .join(broadcast(keyFrame), Seq("groupIndex"))
      .orderBy("groupIndex")
      .select(
        col(fit.spec.provider),
        col("estimate"),
        col("se"),
        lit(g0).as("null_value"),
        (if (wald) col("estimate") else nan).as("transformed"),
        (if (wald) col("se") else nan).as("se_transformed"),
        lit(g0).as("null_transformed"),
        col("zRaw").as("z_raw"),
        lit(0.0).as("null_mean"),
        lit(1.0).as("null_sd"),
        lit(null).cast(IntegerType).as("null_group"),
        col("zRaw").as("z_adjusted"),
        col("pValue").as("p_value"),
        col("flag"),
        col("lower").as("ci_lower"),
        col("upper").as("ci_upper"),
        col("observed"),
        col("expected"),
        col("trials"),
        col("records")
      )
  }

  /** p-value and flag of z under the theoretical null, at the critical value c. */
  private[logistic] def decide(z: Double, alternative: String, c: Double): (Double, Int) =
    alternative match {
      case "two_sided" => (Normal.twoSidedPValue(z), if (z > c) 1 else if (z < -c) -1 else 0)
      case "greater"   => (Normal.upperTail(z), if (z > c) 1 else 0)
      case _           => (Normal.upperTail(-z), if (z < -c) -1 else 0)
    }

  private def waldRows(
      fit: LogisticFit,
      g0: Double,
      alternative: String,
      c: Double,
      tested: Set[Int]
  ) = {
    val pr = fit.providers
    (0 until pr.size).filter(tested.contains).map { k =>
      val se = math.sqrt(pr.varCaseMix(k))
      val z = (pr.gamma(k) - g0) / se
      val (p, flag) = decide(z, alternative, c)
      val lower = if (alternative == "less") Double.NegativeInfinity else pr.gamma(k) - c * se
      val upper = if (alternative == "greater") Double.PositiveInfinity else pr.gamma(k) + c * se
      LogisticProviderTestRow(
        k,
        pr.gamma(k),
        se,
        z,
        p,
        flag,
        lower,
        upper,
        pr.events(k),
        Double.NaN,
        pr.trials(k),
        pr.records(k)
      )
    }
  }

  private def providerRow(
      block: LogisticBlock,
      k: Int,
      beta: Array[Double],
      g0: Double,
      method: String,
      alternative: String,
      c: Double,
      nResample: Int,
      seed: Long
  ): LogisticProviderTestRow = {
    val rows = block.rows
    val from = rows.starts(k)
    val until = rows.end(k)
    val linear = Array.tabulate(until - from)(i => Kernel.eta(rows, from + i, 0.0, beta))
    val trials = Array.tabulate(until - from)(i => rows.n(from + i).toInt)
    val observed = (from until until).map(r => rows.y(r)).sum
    val totalTrials = trials.foldLeft(0L)(_ + _)
    def probabilities(g: Double) = linear.map(eta => Kernel.sigmoid(g + eta))
    val atNull = probabilities(g0).map(PoissonBinomial.clip)
    var expected = 0.0
    var variance = 0.0
    atNull.indices.foreach { i =>
      expected += trials(i) * atNull(i)
      variance += trials(i) * atNull(i) * (1.0 - atNull(i))
    }
    val estimate = block.gamma(k)
    val (z, lower, upper) = method match {
      case "score" =>
        (
          if (variance >= 1e-14) (observed - expected) / math.sqrt(variance) else 0.0,
          Double.NaN,
          Double.NaN
        )
      case "bootstrap_exact" =>
        val t = PoissonBinomial.bootstrapTails(
          atNull,
          trials,
          observed.toInt,
          nResample,
          seed,
          block.groupIndex(k).toLong
        )
        (zOf(t, alternative, 0.5 / nResample), Double.NaN, Double.NaN)
      case _ =>
        if (trials.exists(_ != 1) && totalTrials > PoissonBinomial.MaxTrials)
          throw new IllegalArgumentException(
            s"the exact test would need $totalTrials trials for one provider (more than " +
              s"${PoissonBinomial.MaxTrials}); use the score or bootstrap test for large binomial counts"
          )
        def zAt(g: Double) =
          zOf(
            PoissonBinomial.tails(probabilities(g), trials, observed.toInt),
            alternative,
            PoissonBinomial.TailFloor
          )
        val lo =
          if (alternative == "less") Double.NegativeInfinity
          else PoissonBinomial.invertDecreasing(zAt, estimate, c)
        val hi =
          if (alternative == "greater") Double.PositiveInfinity
          else PoissonBinomial.invertDecreasing(zAt, estimate, -c)
        (zAt(g0), lo, hi)
    }
    val (p, flag) = decide(z, alternative, c)
    LogisticProviderTestRow(
      block.groupIndex(k),
      estimate,
      Double.NaN,
      z,
      p,
      flag,
      lower,
      upper,
      observed,
      expected,
      totalTrials.toDouble,
      (until - from).toLong
    )
  }

  private def zOf(t: PoissonBinomial.Tails, alternative: String, floor: Double): Double =
    PoissonBinomial.z(t, alternative, floor)
}
