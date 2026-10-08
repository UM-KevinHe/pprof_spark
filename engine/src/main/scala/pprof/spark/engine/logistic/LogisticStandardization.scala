package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.DriverGuards
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{NeumaierSum, NeumaierVector, Normal, TaylorBins}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}

/** One block's bin moments (Phase 2d specification §4). */
final case class LogisticBinPartial(bin: Long, blockId: Int, moments: Array[Double])

final case class LogisticBin(bin: Long, moments: Array[Double])

/** A chunk of provider effects, and the direct sums at them. */
final case class LogisticEffectChunk(from: Int, gammas: Array[Double])

final case class LogisticEffectSums(from: Int, direct: Array[Double], variance: Array[Double])

/** One block's indirect sums per provider: Eⱼ at γ₀ and Var(Oⱼ). */
final case class LogisticIndirectPartial(
    blockId: Int,
    expected: Array[Double],
    variance: Array[Double]
)

/** One block's exact direct sums for every effect requested. */
final case class LogisticDirectPartial(blockId: Int, direct: Array[Double], variance: Array[Double])

/** Standardized measures of the logistic fixed-effect model (docs/spec/logistic/standardization.md). */
object LogisticStandardization {

  val Measures: Seq[String] =
    Seq("direct_rate", "direct_ratio", "indirect_ratio", "indirect_rate", "gamma")
  val Methods: Seq[String] = Seq("binned", "exact")

  /** Estimates, standard errors and the reference value of one measure, in provider order. */
  final case class Measure(
      name: String,
      estimate: Array[Double],
      se: Array[Double],
      referenceValue: Double
  )

  private def text(fit: LogisticFit): Boolean =
    fit.providers.keys.headOption.exists(_.isInstanceOf[GroupKey.Text])

  /** Eⱼ (at γ₀) and Var(Oⱼ) (at γ₀ or at γ̂ⱼ) for every provider, from one provider-local pass. */
  private def indirect(
      df: DataFrame,
      fit: LogisticFit,
      g0: Double,
      varianceAt: String
  ): (Array[Double], Array[Double]) = {
    require(
      varianceAt == "null" || varianceAt == "fitted",
      s"indirectVariance must be null or fitted, got $varianceAt"
    )
    val (workingSet, providersOfBlock) = LogisticFE.trainingWorkingSet(df, fit)
    try {
      val beta = fit.estimates.toArray
      val fitted = varianceAt == "fitted"
      val m = fit.providers.size
      val expected = new Array[Double](m)
      val variance = new Array[Double](m)
      LogisticFE
        .withEffects(workingSet.blocks, providersOfBlock, beta.length, fit.providers.gamma)
        .map { (block: LogisticBlock) =>
          val rows = block.rows
          val e = new Array[Double](rows.providers)
          val v = new Array[Double](rows.providers)
          (0 until rows.providers).foreach { k =>
            val es = new NeumaierSum
            val vs = new NeumaierSum
            (rows.starts(k) until rows.end(k)).foreach { r =>
              val eta = Kernel.eta(rows, r, 0.0, beta)
              val p0 = Kernel.sigmoid(g0 + eta)
              val pv = if (fitted) Kernel.sigmoid(block.gamma(k) + eta) else p0
              es.add(rows.n(r) * p0)
              vs.add(rows.n(r) * pv * (1.0 - pv))
            }
            e(k) = es.value
            v(k) = vs.value
          }
          LogisticIndirectPartial(block.blockId, e, v)
        }(Encoders.product[LogisticIndirectPartial])
        .collect()
        .foreach { part =>
          val ids = providersOfBlock(part.blockId)
          ids.indices.foreach { i =>
            expected(ids(i)) = part.expected(i)
            variance(ids(i)) = part.variance(i)
          }
        }
      (expected, variance)
    } finally workingSet.release()
  }

  /** Σᵢ wᵢσ(γ + ηᵢ) and Σᵢ wᵢσ′(γ + ηᵢ) over the fitted rows for each γ in `effects`. */
  private def direct(
      df: DataFrame,
      fit: LogisticFit,
      effects: Array[Double],
      method: String
  ): (Array[Double], Array[Double]) = {
    require(Methods.contains(method), s"method must be binned or exact, got $method")
    val spark = df.sparkSession
    val (workingSet, _) = LogisticFE.trainingWorkingSet(df, fit)
    val beta = fit.estimates.toArray
    try
      if (method == "binned") {
        val bins = workingSet.blocks
          .flatMap { (block: LogisticBlock) =>
            val rows = block.rows
            val acc = scala.collection.mutable.TreeMap.empty[Long, NeumaierVector]
            (0 until rows.rows).foreach { r =>
              val eta = Kernel.eta(rows, r, 0.0, beta)
              val b = TaylorBins.bin(eta)
              acc
                .getOrElseUpdate(b, new NeumaierVector(TaylorBins.Order + 1))
                .add(TaylorBins.rowMoments(eta, rows.n(r), b))
            }
            acc.iterator.map { case (b, v) => LogisticBinPartial(b, block.blockId, v.values) }
          }(Encoders.product[LogisticBinPartial])
          .groupByKey((p: LogisticBinPartial) => p.bin)(Encoders.scalaLong)
          .mapGroups { (b: Long, parts: Iterator[LogisticBinPartial]) =>
            val sum = new NeumaierVector(TaylorBins.Order + 1)
            parts.toArray.sortBy(_.blockId).foreach(p => sum.add(p.moments))
            LogisticBin(b, sum.values)
          }(Encoders.product[LogisticBin])
          .collect()
          .sortBy(_.bin)
        val keys = bins.map(_.bin)
        val moments = bins.map(_.moments)
        val chunk = 10000
        val results = spark
          .createDataset(
            effects.indices
              .grouped(chunk)
              .map(c => LogisticEffectChunk(c.head, c.map(effects).toArray))
              .toSeq
          )(Encoders.product[LogisticEffectChunk])
          .map { (c: LogisticEffectChunk) =>
            val pairs = c.gammas.map(g => TaylorBins.sums(g, keys, moments))
            LogisticEffectSums(c.from, pairs.map(_._1), pairs.map(_._2))
          }(Encoders.product[LogisticEffectSums])
          .collect()
        val d = new Array[Double](effects.length)
        val s = new Array[Double](effects.length)
        results.foreach { r =>
          r.direct.indices.foreach { i =>
            d(r.from + i) = r.direct(i)
            s(r.from + i) = r.variance(i)
          }
        }
        (d, s)
      } else {
        DriverGuards.requireWithinBudget(
          s"${workingSet.plan.blockCount} exact direct partials of ${effects.length} effects",
          workingSet.plan.blockCount.toLong * effects.length * 16L,
          fit.options.blocks
        )
        val gammas = effects.clone()
        val parts = workingSet.blocks
          .map { (block: LogisticBlock) =>
            val rows = block.rows
            val ds = Array.fill(gammas.length)(new NeumaierSum)
            val vs = Array.fill(gammas.length)(new NeumaierSum)
            (0 until rows.rows).foreach { r =>
              val eta = Kernel.eta(rows, r, 0.0, beta)
              gammas.indices.foreach { j =>
                val p = Kernel.sigmoid(gammas(j) + eta)
                ds(j).add(rows.n(r) * p)
                vs(j).add(rows.n(r) * p * (1.0 - p))
              }
            }
            LogisticDirectPartial(block.blockId, ds.map(_.value), vs.map(_.value))
          }(Encoders.product[LogisticDirectPartial])
          .collect()
          .sortBy(_.blockId)
        val d = new NeumaierVector(effects.length)
        val s = new NeumaierVector(effects.length)
        parts.foreach { p =>
          d.add(p.direct)
          s.add(p.variance)
        }
        (d.values, s.values)
      }
    finally workingSet.release()
  }

  private def gammaSe(fit: LogisticFit, variance: String): Array[Double] = variance match {
    case "model"                        => fit.providers.varCaseMix.map(math.sqrt)
    case "robust" | "robust_fixed_beta" =>
      val v =
        if (variance == "robust") fit.providers.robustVarCaseMix
        else fit.providers.robustVarFixedBeta
      require(v.nonEmpty, "robust variances need a fit with a cluster column")
      v.map(math.sqrt)
    case other =>
      throw new IllegalArgumentException(
        s"variance must be model, robust or robust_fixed_beta, got $other"
      )
  }

  /** pprof_py's `standardized_measure` (specification §1). */
  def measure(
      df: DataFrame,
      fit: LogisticFit,
      name: String,
      reference: EffectReference = EffectReference.Median,
      variance: String = "model",
      indirectVariance: String = "null",
      method: String = "binned"
  ): Measure = {
    require(
      Measures.contains(name),
      s"measure must be one of ${Measures.mkString(", ")}, got $name"
    )
    if (variance != "model" && name.startsWith("indirect"))
      throw new IllegalArgumentException(
        "indirect measures use the variance of the observed count (indirectVariance); robust variances apply " +
          "to direct measures and the provider effect"
      )
    val g0 = LogisticProviderTests.reference(fit, reference)
    val gamma = fit.providers.gamma
    name match {
      case "gamma" => Measure(name, gamma.clone(), gammaSe(fit, variance), g0)
      case "direct_rate" | "direct_ratio" =>
        val se = gammaSe(fit, variance)
        val (d, s) = direct(df, fit, gamma :+ g0, method)
        val denominator = if (name == "direct_rate") fit.trials else fit.events
        val m = gamma.length
        Measure(
          name,
          Array.tabulate(m)(j => d(j) / denominator),
          Array.tabulate(m)(j => s(j) / denominator * se(j)),
          d(m) / denominator
        )
      case _ =>
        val (expected, varianceO) = indirect(df, fit, g0, indirectVariance)
        val observed = fit.providers.events
        val ratio = expected.indices
          .map(j => if (expected(j) > 1e-10) observed(j) / expected(j) else Double.NaN)
          .toArray
        val se = expected.indices
          .map(j => if (expected(j) > 1e-10) math.sqrt(varianceO(j)) / expected(j) else Double.NaN)
          .toArray
        if (name == "indirect_ratio") Measure(name, ratio, se, 1.0)
        else {
          val rate = fit.events / fit.trials
          Measure(name, ratio.map(_ * rate), se.map(_ * rate), rate)
        }
    }
  }

  /** pprof_py's `calculate_standardized_measures`: rates in percent clipped to [0, 100]; `extremeTrials`
    * adds R's extreme observations (η = 0) to the direct sums and the population.
    */
  def measures(
      df: DataFrame,
      fit: LogisticFit,
      kinds: Seq[String] = Seq("indirect", "direct"),
      reference: EffectReference = EffectReference.Median,
      providers: Option[Seq[Any]] = None,
      extremeTrials: Double = 0.0,
      method: String = "binned"
  ): Map[String, DataFrame] = {
    require(
      kinds.nonEmpty && kinds.forall(k => k == "indirect" || k == "direct"),
      "kinds must be indirect and/or direct"
    )
    val spark = df.sparkSession
    val keys = fit.providers.keys
    val wanted = providers.map(_.map(_.toString).toSet)
    val selected =
      keys.indices.filter(k => wanted.forall(_.contains(GroupKey.value(keys(k)).toString)))
    val observed = fit.providers.events
    def frame(columns: Seq[String], values: Int => Seq[Any]): DataFrame = {
      val schema = StructType(
        StructField(fit.spec.provider, if (text(fit)) StringType else LongType, nullable = false) +:
          columns.map(StructField(_, DoubleType, nullable = false))
      )
      spark.createDataFrame(
        selected.map(k => Row.fromSeq(GroupKey.value(keys(k)) +: values(k))).asJava,
        schema
      )
    }
    kinds.map {
      case "indirect" =>
        val g0 = LogisticProviderTests.reference(fit, reference)
        val (expected, _) = indirect(df, fit, g0, "null")
        val populationRate = observed.sum / fit.trials * 100.0
        "indirect" -> frame(
          Seq("indirect_ratio", "indirect_rate", "observed", "expected"),
          k => {
            val ratio = observed(k) / expected(k)
            Seq(
              ratio,
              math.min(math.max(ratio * populationRate, 0.0), 100.0),
              observed(k),
              expected(k)
            )
          }
        )
      case _ =>
        val (d, _) = direct(df, fit, fit.providers.gamma, method)
        val total = fit.events
        val population = fit.trials + extremeTrials
        "direct" -> frame(
          Seq("direct_ratio", "direct_rate", "observed", "expected", "n_pop"),
          k => {
            val preds = d(k) + (if (extremeTrials > 0.0)
                                  extremeTrials * Kernel.sigmoid(fit.providers.gamma(k))
                                else 0.0)
            Seq(
              preds / total,
              math.min(math.max(preds / population * 100.0, 0.0), 100.0),
              total,
              preds,
              population
            )
          }
        )
    }.toMap
  }

  /** pprof_py's `test_standardized` with the theoretical null: z on the working scale (`auto`: logit for
    * the direct rate, log for the direct ratio, identity otherwise), the null at the reference value
    * unless given, p-values, flags and intervals as specified (§2).
    */
  def test(
      df: DataFrame,
      fit: LogisticFit,
      name: String = "direct_rate",
      nullValue: Option[Double] = None,
      transform: String = "auto",
      reference: EffectReference = EffectReference.Median,
      variance: String = "model",
      indirectVariance: String = "null",
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None,
      method: String = "binned"
  ): DataFrame = {
    require(
      LogisticProviderTests.Alternatives.contains(alternative),
      s"unknown alternative $alternative"
    )
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val scale = transform match {
      case "auto" =>
        if (name == "direct_rate") "logit" else if (name == "direct_ratio") "log" else "identity"
      case "identity" | "logit" | "log" => transform
      case other                        =>
        throw new IllegalArgumentException(
          s"transform must be auto, identity, logit or log, got $other"
        )
    }
    def forward(x: Double): Double = scale match {
      case "logit" => StrictMath.log(x / (1.0 - x))
      case "log"   => StrictMath.log(x)
      case _       => x
    }
    def seForward(x: Double, se: Double): Double = scale match {
      case "logit" => se / (x * (1.0 - x))
      case "log"   => se / x
      case _       => se
    }
    def inverse(t: Double): Double = scale match {
      case "logit" => 1.0 / (1.0 + StrictMath.exp(-t))
      case "log"   => StrictMath.exp(t)
      case _       => t
    }
    val m = measure(df, fit, name, reference, variance, indirectVariance, method)
    val nv = nullValue.getOrElse(m.referenceValue)
    val nt = forward(nv)
    require(
      java.lang.Double.isFinite(nt),
      s"the null value $nv lies outside the domain of the $scale scale"
    )
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      Normal.upperQuantile(if (alternative == "two_sided") alpha / 2.0 else alpha)
    )
    val bounds =
      if (scale == "identity" && name.endsWith("_rate")) Some((0.0, 1.0))
      else if (scale == "identity" && name.endsWith("_ratio")) Some((0.0, Double.PositiveInfinity))
      else None
    val keys = fit.providers.keys
    val wanted = providers.map(_.map(_.toString).toSet)
    val rows =
      keys.indices.filter(k => wanted.forall(_.contains(GroupKey.value(keys(k)).toString))).map {
        k =>
          val t = forward(m.estimate(k))
          val st = seForward(m.estimate(k), m.se(k))
          val raw = (t - nt) / st
          val testable = java.lang.Double.isFinite(t) && java.lang.Double.isFinite(
            st
          ) && st > 0.0 && java.lang.Double.isFinite(raw)
          val z = if (testable) raw else Double.NaN
          val (p, flag) =
            if (!testable) (Double.NaN, null)
            else {
              val (pValue, _) = LogisticProviderTests.decide(z, alternative, c)
              val significant = critical match {
                case None    => pValue < alpha
                case Some(v) =>
                  if (alternative == "less") z < -v
                  else if (alternative == "greater") z > v
                  else math.abs(z) > v
              }
              val up = significant && z > 0.0 && alternative != "less"
              val down = significant && z < 0.0 && alternative != "greater"
              (pValue, Integer.valueOf(if (up) 1 else if (down) -1 else 0))
            }
          var lo = inverse(if (alternative == "less") Double.NegativeInfinity else t - c * st)
          var hi = inverse(if (alternative == "greater") Double.PositiveInfinity else t + c * st)
          if (!testable) {
            lo = Double.NaN
            hi = Double.NaN
          }
          bounds.foreach { case (a, b) =>
            lo = math.min(math.max(lo, a), b)
            hi = math.min(math.max(hi, a), b)
          }
          Row(
            GroupKey.value(keys(k)),
            m.estimate(k),
            m.se(k),
            nv,
            t,
            st,
            nt,
            z,
            0.0,
            1.0,
            null,
            z,
            p,
            flag,
            lo,
            hi
          )
      }
    val doubles = Seq(
      "estimate",
      "se",
      "null_value",
      "transformed",
      "se_transformed",
      "null_transformed",
      "z_raw",
      "null_mean",
      "null_sd"
    )
    val schema = StructType(
      Seq(
        StructField(fit.spec.provider, if (text(fit)) StringType else LongType, nullable = false)
      ) ++
        doubles.map(StructField(_, DoubleType, nullable = false)) ++
        Seq(
          StructField("null_group", IntegerType, nullable = true),
          StructField("z_adjusted", DoubleType, nullable = false),
          StructField("p_value", DoubleType, nullable = false),
          StructField("flag", IntegerType, nullable = true),
          StructField("ci_lower", DoubleType, nullable = false),
          StructField("ci_upper", DoubleType, nullable = false)
        )
    )
    df.sparkSession.createDataFrame(rows.asJava, schema)
  }

  /** The measure as a table: provider, estimate, se and the reference value. */
  def measureTable(spark: SparkSession, fit: LogisticFit, m: Measure): DataFrame = {
    val keys = fit.providers.keys
    spark.createDataFrame(
      keys.indices
        .map(k => Row(GroupKey.value(keys(k)), m.estimate(k), m.se(k), m.referenceValue))
        .asJava,
      StructType(
        Seq(
          StructField(fit.spec.provider, if (text(fit)) StringType else LongType, nullable = false),
          StructField("estimate", DoubleType, nullable = false),
          StructField("se", DoubleType, nullable = false),
          StructField("reference_value", DoubleType, nullable = false)
        )
      )
    )
  }
}
