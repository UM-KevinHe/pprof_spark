package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.DriverGuards
import pprof.spark.engine.linear.LinearBlock
import pprof.spark.numerics.{
  BoundedQuasiNewton,
  Brent,
  NeumaierSum,
  Normal,
  PoissonBinomial,
  Summation,
  TaylorBins
}

/** A fixed effect's Wald test (spec 4b-2 §2). */
final case class LogisticRETest(
    feature: String,
    estimate: Double,
    standardError: Double,
    statistic: Double,
    pValue: Double,
    lower: Double,
    upper: Double
)

/** One provider's count test (spec 4b-2 §3), by group index. */
final case class LogisticRECountRow(
    groupIndex: Int,
    records: Long,
    estimate: Double,
    zRaw: Double,
    pValue: Double,
    flag: Option[Int],
    lower: Double,
    upper: Double
)

/** One block's measure sums (spec 4b-2 §7): per provider Oⱼ and Eⱼ, and the bins' Taylor moments of η over its rows. */
final case class LogisticREMeasurePartial(
    blockId: Int,
    groupIndex: Array[Int],
    records: Array[Long],
    observed: Array[Double],
    expected: Array[Double],
    bins: Array[Long],
    moments: Array[Double]
)

/** σ's profile interval (spec 4b-2 §6). */
final case class LogisticREProfile(sigma: Double, lower: Double, upper: Double, evaluations: Int)

/** Inference, measures and σ's profile of the logistic random-intercept model
  * (docs/spec/logistic/random-effect-inference.md): `summary`, the provider tests `wald`, `poibin_exact` and
  * `resampling`, option `alpha`, the `SM` intervals, the standardized measures and `profileSigma`. Everything that
  * reads the rows takes the training data, checked against the fit's fingerprint (API-3).
  */
object LogisticREInference {

  val Alternatives: Seq[String] = Seq("two_sided", "greater", "less")
  val Methods: Seq[String] = Seq("wald", "poibin_exact", "resampling")
  val Kinds: Seq[String] = Seq("indirect", "direct")
  val Measures: Seq[String] = Seq("ratio", "rate")

  private def checkLevel(level: Double): Unit =
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
  private def checkAlternative(alternative: String): Unit =
    require(
      Alternatives.contains(alternative),
      s"alternative must be one of ${Alternatives.mkString(", ")}"
    )

  /** `summary(level)` (§2): z on the normal, p = 2Φ̄(|z|), limits β̂ ∓ Φ⁻¹(1 − α/2)·se; NaN where se = 0. */
  def summary(fit: LogisticREFit, level: Double = 0.95): Vector[LogisticRETest] = {
    checkLevel(level)
    val q = Normal.upperQuantile((1.0 - level) / 2.0)
    fit.coefficients.map { c =>
      if (c.standardError > 0.0) {
        val z = c.estimate / c.standardError
        LogisticRETest(
          c.feature,
          c.estimate,
          c.standardError,
          z,
          Normal.twoSidedPValue(z),
          c.estimate - q * c.standardError,
          c.estimate + q * c.standardError
        )
      } else
        LogisticRETest(
          c.feature,
          c.estimate,
          c.standardError,
          Double.NaN,
          Double.NaN,
          Double.NaN,
          Double.NaN
        )
    }
  }

  private final case class Effects(
      keys: Array[Any],
      records: Array[Long],
      blups: Array[Double],
      se: Array[Double]
  )

  /** The provider table in key order, on the driver under its budget. */
  private def effects(fit: LogisticREFit): Effects = {
    DriverGuards.requireWithinBudget(
      s"${fit.providerCount} providers' effects",
      32L * fit.providerCount,
      fit.options.blocks
    )
    val rows = fit.providers
      .orderBy(fit.spec.provider)
      .select(fit.spec.provider, "records", "blup", "se")
      .collect()
    Effects(
      rows.map(_.get(0)),
      rows.map(_.getLong(1)),
      rows.map(_.getDouble(2)),
      rows.map(_.getDouble(3))
    )
  }

  /** γ₀ (§1): a number, the median of the BLUPs, or their mean weighted by nⱼ (X-046). */
  def reference(fit: LogisticREFit, reference: EffectReference): Double = reference match {
    case EffectReference.Value(value) => value
    case kind                         =>
      val e = effects(fit)
      if (kind == EffectReference.Median) {
        val sorted = e.blups.sorted
        val m = sorted.length
        if (m % 2 == 1) sorted(m / 2) else 0.5 * (sorted(m / 2 - 1) + sorted(m / 2))
      } else {
        val s = new NeumaierSum
        e.blups.indices.foreach(j => s.add(e.records(j).toDouble * e.blups(j)))
        s.value / e.records.sum.toDouble
      }
  }

  private def decide(z: Double, alternative: String, c: Double): (Double, Option[Int]) =
    if (z.isNaN) (Double.NaN, None)
    else
      alternative match {
        case "two_sided" =>
          (Normal.twoSidedPValue(z), Some(if (z > c) 1 else if (z < -c) -1 else 0))
        case "greater" => (Normal.upperTail(z), Some(if (z > c) 1 else 0))
        case _         => (Normal.upperTail(-z), Some(if (z < -c) -1 else 0))
      }

  /** Provider tests (§3) in slice 2c's table, ordered by key, with `records`. `exact` needs a cluster factor (part 4d);
    * a boundary fit leaves every provider untested by `wald` (X-048).
    */
  def test(
      df: DataFrame,
      fit: LogisticREFit,
      method: String = "wald",
      reference: EffectReference = EffectReference.Value(0.0),
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None,
      nResample: Int = 10000,
      seed: Long = 0L
  ): DataFrame = {
    require(
      method != "exact",
      "test_method 'exact' needs a cluster factor besides the provider (part 4d, OI-93)"
    )
    require(
      Methods.contains(method),
      s"method must be one of ${Methods.mkString(", ")}, got $method"
    )
    checkAlternative(alternative)
    checkLevel(level)
    require(nResample > 0, s"nResample must be positive, got $nResample")
    critical.foreach(c => require(c > 0.0, s"the critical value must be positive, got $c"))
    val g0 = this.reference(fit, reference)
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      Normal.upperQuantile(if (alternative == "two_sided") alpha / 2.0 else alpha)
    )
    val e = effects(fit)
    val m = e.blups.length
    val rows: Seq[LogisticRECountRow] =
      if (method == "wald") {
        val boundary = e.se.forall(_ == 0.0)
        (0 until m).map { j =>
          if (boundary)
            LogisticRECountRow(
              j,
              e.records(j),
              e.blups(j),
              Double.NaN,
              Double.NaN,
              None,
              Double.NaN,
              Double.NaN
            )
          else {
            val se = math.max(e.se(j), 1e-15)
            val z = (e.blups(j) - g0) / se
            val (p, flag) = decide(z, alternative, c)
            val lower = if (alternative == "less") Double.NegativeInfinity else e.blups(j) - c * se
            val upper =
              if (alternative == "greater") Double.PositiveInfinity else e.blups(j) + c * se
            LogisticRECountRow(j, e.records(j), e.blups(j), z, p, flag, lower, upper)
          }
        }
      } else {
        val t = LogisticRE.training(df, fit)
        try {
          val beta = fit.estimates.toArray
          val blups = e.blups
          val p = t.p
          t.workingSet.blocks
            .flatMap { (block: LinearBlock) =>
              (0 until block.groupStart.length).iterator.map { g =>
                countRow(block, g, beta, p, blups, g0, method, alternative, c, nResample, seed)
              }
            }(Encoders.product[LogisticRECountRow])
            .collect()
            .toSeq
        } finally t.workingSet.release()
      }
    val wald = method == "wald"
    val spark = fit.providers.sparkSession // `df` is read only by the count tests
    val wanted = providers.map(_.map(_.toString).toSet)
    val kept = rows
      .filter(r => wanted.forall(_.contains(e.keys(r.groupIndex).toString)))
      .sortBy(_.groupIndex)
    val text = fit.providers.schema(fit.spec.provider).dataType == StringType
    val schema = StructType(
      Seq(
        StructField(fit.spec.provider, if (text) StringType else LongType, nullable = false),
        StructField("records", LongType, nullable = false),
        StructField("estimate", DoubleType, nullable = false),
        StructField("se", DoubleType, nullable = false),
        StructField("null_value", DoubleType, nullable = false),
        StructField("transformed", DoubleType, nullable = false),
        StructField("se_transformed", DoubleType, nullable = false),
        StructField("null_transformed", DoubleType, nullable = false),
        StructField("z_raw", DoubleType, nullable = false),
        StructField("null_mean", DoubleType, nullable = false),
        StructField("null_sd", DoubleType, nullable = false),
        StructField("null_group", IntegerType, nullable = true),
        StructField("z_adjusted", DoubleType, nullable = false),
        StructField("p_value", DoubleType, nullable = false),
        StructField("flag", IntegerType, nullable = true),
        StructField("ci_lower", DoubleType, nullable = false),
        StructField("ci_upper", DoubleType, nullable = false)
      )
    )
    val out = kept.map { r =>
      val se = if (wald) e.se(r.groupIndex) else Double.NaN
      Row(
        if (text) e.keys(r.groupIndex).toString
        else e.keys(r.groupIndex).asInstanceOf[Number].longValue,
        r.records,
        r.estimate,
        se,
        g0,
        if (wald) r.estimate else Double.NaN,
        se,
        g0,
        r.zRaw,
        0.0,
        1.0,
        null,
        r.zRaw,
        r.pValue,
        r.flag.map(Int.box).orNull,
        r.lower,
        r.upper
      )
    }
    spark.createDataFrame(out.asJava, schema)
  }

  /** One provider's `poibin_exact` or `resampling` test on its rows (§3): Oⱼ rounded to the nearest integer, halves
    * to even, as `poibin_tails`; probabilities expit(g + ηᵢ); the exact test inverted from the BLUP, as slice 2c.
    */
  private def countRow(
      block: LinearBlock,
      g: Int,
      beta: Array[Double],
      p: Int,
      blups: Array[Double],
      g0: Double,
      method: String,
      alternative: String,
      c: Double,
      nResample: Int,
      seed: Long
  ): LogisticRECountRow = {
    val q = block.p + 1
    val from = block.groupStart(g)
    val until = if (g + 1 < block.groupStart.length) block.groupStart(g + 1) else block.rowId.length
    val eta = Array.tabulate(until - from) { i =>
      val r = from + i
      val s = new NeumaierSum
      var k = 0
      while (k < p) {
        s.add(block.values(r * q + k) * beta(k))
        k += 1
      }
      s.add(block.values(r * q + p))
      s.value
    }
    val total = new NeumaierSum
    (from until until).foreach(r => total.add(block.values(r * q + block.p)))
    val observed = StrictMath.rint(total.value).toInt
    val trials = Array.fill(eta.length)(1)
    def probabilities(gamma: Double) = eta.map(x => 1.0 / (1.0 + StrictMath.exp(-(gamma + x))))
    val index = block.groupIndex(g)
    val estimate = blups(index)
    val (z, lower, upper) =
      if (method == "resampling") {
        // pprof_py's count_test: a tail at the Monte Carlo floor 0.5/nResample is replaced by the exact tail
        val floor = 0.5 / nResample
        val t = PoissonBinomial.bootstrapTails(
          probabilities(g0),
          trials,
          observed,
          nResample,
          seed,
          index.toLong
        )
        val tail = alternative match {
          case "two_sided" => math.min(t.upperMid, t.lowerMid)
          case "greater"   => t.atLeast
          case _           => t.atMost
        }
        val z =
          if (tail <= floor)
            PoissonBinomial.z(
              PoissonBinomial.tails(probabilities(g0), trials, observed),
              alternative,
              PoissonBinomial.TailFloor
            )
          else PoissonBinomial.z(t, alternative, floor)
        (z, Double.NaN, Double.NaN)
      } else {
        def zAt(gamma: Double) =
          PoissonBinomial.z(
            PoissonBinomial.tails(probabilities(gamma), trials, observed),
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
    val (pValue, flag) = decide(z, alternative, c)
    LogisticRECountRow(index, eta.length.toLong, estimate, z, pValue, flag, lower, upper)
  }

  /** Option `alpha` (§4): b̂ⱼ ∓ Φ⁻¹(1 − α/2)·seⱼ, two-sided only; columns key, alpha, alpha_lower, alpha_upper. */
  def alphaIntervals(fit: LogisticREFit, level: Double = 0.95): DataFrame = {
    checkLevel(level)
    val q = Normal.upperQuantile((1.0 - level) / 2.0)
    fit.providers
      .select(
        col(fit.spec.provider),
        col("blup").as("alpha"),
        (col("blup") - lit(q) * col("se")).as("alpha_lower"),
        (col("blup") + lit(q) * col("se")).as("alpha_upper")
      )
      .orderBy(fit.spec.provider)
  }

  /** The training pass of §7: per provider Oⱼ and Eⱼ at γ₀, and the Taylor moments of η over every record, in key
    * order and bin order on the driver.
    */
  private final case class Sums(
      records: Array[Long],
      observed: Array[Double],
      expected: Array[Double],
      bins: Array[Long],
      moments: Array[Array[Double]],
      n: Long
  )

  private def sums(df: DataFrame, fit: LogisticREFit, g0: Double): Sums = {
    val t = LogisticRE.training(df, fit)
    try {
      val beta = fit.estimates.toArray
      val p = t.p
      val parts = t.workingSet.blocks
        .map((block: LinearBlock) => measurePartial(block, beta, p, g0))(
          Encoders.product[LogisticREMeasurePartial]
        )
        .collect()
        .sortBy(_.blockId)
      val m = t.m
      val records = new Array[Long](m)
      val observed = new Array[Double](m)
      val expected = new Array[Double](m)
      parts.foreach { b =>
        b.groupIndex.indices.foreach { g =>
          records(b.groupIndex(g)) = b.records(g)
          observed(b.groupIndex(g)) = b.observed(g)
          expected(b.groupIndex(g)) = b.expected(g)
        }
      }
      val width = TaylorBins.Order + 1
      val merged = scala.collection.mutable.TreeMap.empty[Long, Array[NeumaierSum]]
      parts.foreach { b =>
        b.bins.indices.foreach { i =>
          val acc = merged.getOrElseUpdate(b.bins(i), Array.fill(width)(new NeumaierSum))
          (0 until width).foreach(k => acc(k).add(b.moments(i * width + k)))
        }
      }
      Sums(
        records,
        observed,
        expected,
        merged.keys.toArray,
        merged.values.map(_.map(_.value)).toArray,
        t.rows
      )
    } finally t.workingSet.release()
  }

  private def measurePartial(
      block: LinearBlock,
      beta: Array[Double],
      p: Int,
      g0: Double
  ): LogisticREMeasurePartial = {
    val q = block.p + 1
    val groups = block.groupStart.length
    val records = new Array[Long](groups)
    val observed = new Array[Double](groups)
    val expected = new Array[Double](groups)
    val bins = scala.collection.mutable.TreeMap.empty[Long, Array[NeumaierSum]]
    var g = 0
    while (g < groups) {
      val from = block.groupStart(g)
      val until = if (g + 1 < groups) block.groupStart(g + 1) else block.rowId.length
      val o = new NeumaierSum
      val ex = new NeumaierSum
      var r = from
      while (r < until) {
        val s = new NeumaierSum
        var k = 0
        while (k < p) {
          s.add(block.values(r * q + k) * beta(k))
          k += 1
        }
        s.add(block.values(r * q + p))
        val eta = s.value
        o.add(block.values(r * q + block.p))
        ex.add(1.0 / (1.0 + StrictMath.exp(-(g0 + eta))))
        val b = TaylorBins.bin(eta)
        val acc = bins.getOrElseUpdate(b, Array.fill(TaylorBins.Order + 1)(new NeumaierSum))
        val row = TaylorBins.rowMoments(eta, 1.0, b)
        row.indices.foreach(k => acc(k).add(row(k)))
        r += 1
      }
      records(g) = (until - from).toLong
      observed(g) = o.value
      expected(g) = ex.value
      g += 1
    }
    LogisticREMeasurePartial(
      block.blockId,
      block.groupIndex,
      records,
      observed,
      expected,
      bins.keys.toArray,
      bins.values.flatMap(_.map(_.value)).toArray
    )
  }

  /** Σᵢ expit(s + ηᵢ) over every record from the bins (X-026): 0 at s = −∞ and n at s = ∞. */
  private def direct(s: Double, z: Sums): Double =
    if (s == Double.NegativeInfinity) 0.0
    else if (s == Double.PositiveInfinity) z.n.toDouble
    else TaylorBins.sums(s, z.bins, z.moments)._1

  private def frame(
      spark: SparkSession,
      fit: LogisticREFit,
      keys: Array[Any],
      columns: Seq[(String, Array[Double])]
  ): DataFrame = {
    val text = fit.providers.schema(fit.spec.provider).dataType == StringType
    val schema = StructType(
      StructField(fit.spec.provider, if (text) StringType else LongType, nullable = false) +:
        columns.map { case (name, _) => StructField(name, DoubleType, nullable = false) }
    )
    val rows = keys.indices.map { j =>
      val key: Any = if (text) keys(j).toString else keys(j).asInstanceOf[Number].longValue
      Row.fromSeq(key +: columns.map(_._2(j)))
    }
    spark.createDataFrame(rows.asJava, schema)
  }

  /** `calculate_standardized_measures` (§5): tables `indirect` and `direct`, ordered by key. */
  def measures(
      df: DataFrame,
      fit: LogisticREFit,
      kinds: Seq[String] = Seq("indirect"),
      reference: EffectReference = EffectReference.Median
  ): Map[String, DataFrame] = {
    require(
      kinds.nonEmpty && kinds.forall(Kinds.contains),
      s"kinds must be indirect and/or direct, got ${kinds.mkString(", ")}"
    )
    val g0 = this.reference(fit, reference)
    val e = effects(fit)
    val z = sums(df, fit, g0)
    val n = z.n.toDouble
    val total = Summation.pairwise(z.observed)
    val rate = 100.0 * total / n
    val m = e.blups.length
    def clip(x: Double) = if (x.isNaN) x else math.min(math.max(x, 0.0), 100.0)
    kinds.map {
      case "indirect" =>
        val ratio = Array.tabulate(m)(j =>
          if (z.expected(j) > 0.0) z.observed(j) / z.expected(j) else Double.NaN
        )
        "indirect" -> frame(
          df.sparkSession,
          fit,
          e.keys,
          Seq(
            "indirect_ratio" -> ratio,
            "indirect_rate" -> ratio.map(r => clip(r * rate)),
            "observed" -> z.observed,
            "expected" -> z.expected
          )
        )
      case _ =>
        val expected = e.blups.map(b => direct(b, z))
        "direct" -> frame(
          df.sparkSession,
          fit,
          e.keys,
          Seq(
            "direct_ratio" -> expected.map(d => if (total > 0.0) d / total else Double.NaN),
            "direct_rate" -> expected.map(d => clip(100.0 * d / n)),
            "observed" -> Array.fill(m)(total),
            "expected" -> expected
          )
        )
    }.toMap
  }

  /** `calculate_confidence_intervals(option = "SM")` (§4): tables keyed `indirect_ratio`, `indirect_rate`,
    * `direct_ratio` and `direct_rate`, each with `value`, `lower` and `upper`; one-sided indirect limits (X-045).
    */
  def smIntervals(
      df: DataFrame,
      fit: LogisticREFit,
      kinds: Seq[String] = Seq("indirect"),
      measures: Seq[String] = Seq("rate", "ratio"),
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95
  ): Map[String, DataFrame] = {
    require(
      kinds.nonEmpty && kinds.forall(Kinds.contains),
      s"kinds must be indirect and/or direct, got ${kinds.mkString(", ")}"
    )
    require(
      measures.nonEmpty && measures.forall(Measures.contains),
      s"measures must be ratio and/or rate"
    )
    checkAlternative(alternative)
    checkLevel(level)
    val g0 = this.reference(fit, reference)
    val e = effects(fit)
    val z = sums(df, fit, g0)
    val n = z.n.toDouble
    val total = Summation.pairwise(z.observed)
    val rate = 100.0 * total / n
    val m = e.blups.length
    val alpha = 1.0 - level
    val c =
      if (alternative == "two_sided") Normal.upperQuantile(alpha / 2.0)
      else Normal.upperQuantile(alpha)
    val lower = Array.tabulate(m)(j =>
      if (alternative == "less") Double.NegativeInfinity else e.blups(j) - c * e.se(j)
    )
    val upper = Array.tabulate(m)(j =>
      if (alternative == "greater") Double.PositiveInfinity else e.blups(j) + c * e.se(j)
    )
    def clip(x: Double) = if (x.isNaN) x else math.min(math.max(x, 0.0), 100.0)
    def table(key: String, value: Array[Double], lo: Array[Double], hi: Array[Double]) =
      key -> frame(
        df.sparkSession,
        fit,
        e.keys,
        Seq("value" -> value, "lower" -> lo, "upper" -> hi)
      )
    val out = Seq.newBuilder[(String, DataFrame)]
    if (kinds.contains("indirect")) {
      val ratio = Array.tabulate(m)(j =>
        if (z.expected(j) > 0.0) z.observed(j) / z.expected(j) else Double.NaN
      )
      def limit(j: Int, sign: Double): Double =
        if (z.observed(j) > 0.0 && z.expected(j) > 0.0)
          StrictMath.exp(StrictMath.log(ratio(j)) + sign * c * e.se(j))
        else Double.NaN
      val lo = Array.tabulate(m)(j => if (alternative == "less") 0.0 else limit(j, -1.0))
      val hi = Array.tabulate(m)(j =>
        if (alternative == "greater") Double.PositiveInfinity else limit(j, 1.0)
      )
      if (measures.contains("ratio")) out += table("indirect_ratio", ratio, lo, hi)
      if (measures.contains("rate"))
        out += table(
          "indirect_rate",
          ratio.map(r => clip(r * rate)),
          lo.map(r => clip(r * rate)),
          hi.map(r => clip(r * rate))
        )
    }
    if (kinds.contains("direct")) {
      val d = e.blups.map(b => direct(b, z))
      val dl = lower.map(s => direct(s, z))
      val du = upper.map(s => direct(s, z))
      def ratio(x: Array[Double]) = x.map(v => if (total > 0.0) v / total else Double.NaN)
      def pct(x: Array[Double]) = x.map(v => clip(100.0 * v / n))
      if (measures.contains("ratio")) out += table("direct_ratio", ratio(d), ratio(dl), ratio(du))
      if (measures.contains("rate")) out += table("direct_rate", pct(d), pct(dl), pct(du))
    }
    out.result().toMap
  }

  /** `profile_sigma(level)` (§6): the σ where minβ D(σ, β) − D_min ≤ χ²₁(level), by Brent's method to 10⁻¹⁰;
    * nAGQ = 1 fits only.
    */
  def profileSigma(df: DataFrame, fit: LogisticREFit, level: Double = 0.95): LogisticREProfile = {
    checkLevel(level)
    require(fit.options.nagq1, "profile_sigma needs the Laplace (stage2 = true) fit")
    val t = LogisticRE.training(df, fit)
    try {
      val q = Normal.upperQuantile((1.0 - level) / 2.0)
      val critical = q * q
      var beta = LogisticRE.centredOf(fit.estimates.toArray, t.centre, fit.options.includeIntercept)
      var evaluations = 0
      def profiled(sigma: Double): Double = {
        val r = BoundedQuasiNewton.minimizeWithGradient(
          x => {
            val s = t.blocks.laplace(sigma, x)
            (s.deviance, s.gradient.drop(1))
          },
          beta,
          Array.fill(beta.length)(Double.NegativeInfinity),
          BoundedQuasiNewton.Options(
            gradientTolerance = 1e-10,
            maxIterations = fit.options.maxIterations
          )
        )
        evaluations += r.evaluations
        beta = r.x
        r.value
      }
      val estimate = fit.sigma
      val minimum = math.min(fit.deviance, profiled(estimate))
      def excess(sigma: Double): Double = profiled(sigma) - minimum - critical
      var hi = estimate + math.max(0.1, estimate)
      while (excess(hi) < 0.0) {
        require(hi <= 1e3, "no upper profile limit for sigma below 1000")
        hi *= 2.0
      }
      val upper = Brent.root(excess, estimate, hi, 1e-10)
      val lower =
        if (estimate <= 0.0 || excess(0.0) <= 0.0) 0.0 else Brent.root(excess, 0.0, estimate, 1e-10)
      LogisticREProfile(estimate, lower, upper, evaluations)
    } finally t.workingSet.release()
  }
}
