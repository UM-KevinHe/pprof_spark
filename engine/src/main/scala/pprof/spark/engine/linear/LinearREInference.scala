package pprof.spark.engine.linear

import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import pprof.spark.engine.backend.{DriverGuards, PlanFrames}
import pprof.spark.engine.data.Validation
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.numerics.{NeumaierSum, Normal, StudentT, Summation}

/** One fixed effect of pprof_py's `fixed_effects_table` (spec 4a-2 §2). */
final case class LinearREFixedEffect(
    feature: String,
    estimate: Double,
    standardError: Double,
    statistic: Double,
    degreesOfFreedom: Double,
    pValue: Double
)

/** One provider's BLUP and its conditional SD, keyed as text. */
final case class LinearREEffectRow(key: String, records: Long, blup: Double, se: Double)

/** One block's totals for the measures: rows, Σ ηᵢ with ηᵢ = oᵢ + xᵢᵀβ̂, and its share of the fingerprint. */
final case class LinearREStandardTotals(blockId: Int, rows: Long, eta: Double, fingerprint: Long)

/** One provider's Σ ηᵢ and its row count. */
final case class LinearREStandardRow(groupIndex: Int, records: Long, eta: Double)

/** Inference and standardized measures of the linear random-intercept model (docs/spec/linear/random-effect-inference.md):
  * covariate tables from the fit's summary, provider tests and `alpha` intervals from its provider table, and the
  * measures and `SM` intervals from one pass over the training data, checked against the fit's fingerprint (API-3).
  */
object LinearREInference {

  val Alternatives: Seq[String] = Seq("two_sided", "greater", "less")
  val Kinds: Seq[String] = Seq("indirect", "direct")

  /** pprof_py's `summary` (§2): t with n − p − m degrees of freedom, tails computed directly and unrounded (X-041). */
  def summary(
      fit: LinearREFit,
      level: Double = 0.95,
      nullValue: Double = 0.0,
      alternative: String = "two_sided"
  ): Vector[LinearTest] = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    require(Alternatives.contains(alternative), s"unknown alternative $alternative")
    val df = (fit.rows - fit.coefficients.size - fit.providerCount).toDouble
    fit.coefficients.map { c =>
      val t = (c.estimate - nullValue) / c.standardError
      alternative match {
        case "two_sided" =>
          val q = StudentT.upperQuantile((1.0 - level) / 2.0, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.twoSided(t, df),
            c.estimate - q * c.standardError,
            c.estimate + q * c.standardError
          )
        case "greater" =>
          val q = StudentT.upperQuantile(1.0 - level, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.upperTail(t, df),
            c.estimate - q * c.standardError,
            Double.PositiveInfinity
          )
        case _ =>
          val q = StudentT.upperQuantile(1.0 - level, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.cdf(t, df),
            Double.NegativeInfinity,
            c.estimate + q * c.standardError
          )
      }
    }
  }

  /** pprof_py's `fixed_effects_table` (§2): t = β̂/se (NaN when se = 0) with max(n − p, 1) degrees of freedom. */
  def fixedEffectsTable(fit: LinearREFit): Vector[LinearREFixedEffect] = {
    val df = math.max(fit.rows - fit.coefficients.size, 1L).toDouble
    fit.coefficients.map { c =>
      val t = if (c.standardError == 0.0) Double.NaN else c.estimate / c.standardError
      LinearREFixedEffect(c.feature, c.estimate, c.standardError, t, df, StudentT.twoSided(t, df))
    }
  }

  /** pprof_py's `conf_int` (§2): β̂ ∓ t·se with max(n − p, 1) degrees of freedom; (feature, lower, upper). */
  def confInt(fit: LinearREFit, level: Double = 0.95): Vector[(String, Double, Double)] = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val q = StudentT.upperQuantile(
      (1.0 - level) / 2.0,
      math.max(fit.rows - fit.coefficients.size, 1L).toDouble
    )
    fit.coefficients.map(c =>
      (c.feature, c.estimate - q * c.standardError, c.estimate + q * c.standardError)
    )
  }

  /** γ₀ (§1) on the driver: the provider table's records and BLUPs in key order, guarded by `DriverGuards`. */
  def reference(fit: LinearREFit, reference: EffectReference): Double = reference match {
    case EffectReference.Value(value) => value
    case kind                         =>
      DriverGuards.requireWithinBudget(
        s"${fit.providerCount} BLUPs",
        16L * fit.providerCount,
        fit.options.blocks
      )
      val rows = fit.providers.orderBy(fit.spec.provider).select("records", "blup").collect()
      if (kind == EffectReference.Median) {
        val sorted = rows.map(_.getDouble(1)).sorted
        val m = sorted.length
        if (m % 2 == 1) sorted(m / 2) else 0.5 * (sorted(m / 2 - 1) + sorted(m / 2))
      } else {
        val weighted = new NeumaierSum
        rows.foreach(row => weighted.add(row.getLong(0).toDouble * row.getDouble(1)))
        weighted.value / rows.iterator.map(_.getLong(0)).sum.toDouble
      }
  }

  /** Provider tests (§3): z = (b̂ⱼ − γ₀)/seⱼ against the normal, with slice 2c's columns, ordered by key. At a boundary
    * fit (seⱼ = 0) every provider is untested, whatever γ₀, as pprof_py (D-55).
    */
  def test(
      fit: LinearREFit,
      reference: EffectReference = EffectReference.Value(0.0),
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None
  ): DataFrame = {
    require(Alternatives.contains(alternative), s"unknown alternative $alternative")
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    critical.foreach(c => require(c > 0.0, s"the critical value must be positive, got $c"))
    val g0 = this.reference(fit, reference)
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      if (alternative == "two_sided") Normal.upperQuantile(alpha / 2.0)
      else Normal.upperQuantile(alpha)
    )
    val wanted = providers.map(_.map(_.toString).toSet)
    val rows = effects(fit).flatMap { (e: LinearREEffectRow) =>
      if (wanted.exists(w => !w.contains(e.key))) Iterator.empty
      else Iterator.single(row(e, g0, alternative, c))
    }(Encoders.product[LinearTestRow])
    rows
      .toDF()
      .select(
        keyColumn(fit),
        col("records"),
        col("estimate"),
        col("se"),
        lit(g0).as("null_value"),
        col("estimate").as("transformed"),
        col("se").as("se_transformed"),
        lit(g0).as("null_transformed"),
        col("z").as("z_raw"),
        lit(0.0).as("null_mean"),
        lit(1.0).as("null_sd"),
        lit(null).cast(IntegerType).as("null_group"),
        col("z").as("z_adjusted"),
        col("pValue").as("p_value"),
        col("flag"),
        col("lower").as("ci_lower"),
        col("upper").as("ci_upper")
      )
      .orderBy(fit.spec.provider)
  }

  /** Option `alpha` (§4): b̂ⱼ ∓ Φ⁻¹(1 − α/2)·seⱼ, two-sided only; columns key, alpha, alpha_lower, alpha_upper. */
  def alphaIntervals(
      fit: LinearREFit,
      level: Double = 0.95,
      providers: Option[Seq[Any]] = None
  ): DataFrame = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val q = Normal.upperQuantile((1.0 - level) / 2.0)
    val key = fit.spec.provider
    val all = fit.providers.select(
      col(key),
      col("blup").as("alpha"),
      (col("blup") - lit(q) * col("se")).as("alpha_lower"),
      (col("blup") + lit(q) * col("se")).as("alpha_upper")
    )
    providers
      .fold(all)(wanted => all.filter(col(key).cast(StringType).isin(wanted.map(_.toString): _*)))
      .orderBy(key)
  }

  /** pprof_py's `calculate_standardized_measures` (§5): a table per kind, ordered by key. */
  def measures(
      df: DataFrame,
      fit: LinearREFit,
      kinds: Seq[String] = Kinds,
      reference: EffectReference = EffectReference.Median,
      providers: Option[Seq[Any]] = None
  ): Map[String, DataFrame] = compute(df, fit, kinds, reference, None, providers)

  /** pprof_py's `calculate_confidence_intervals(option = "SM")` (§4): the measures with `lower` and `upper`. */
  def smIntervals(
      df: DataFrame,
      fit: LinearREFit,
      kinds: Seq[String] = Seq("indirect"),
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      providers: Option[Seq[Any]] = None
  ): Map[String, DataFrame] = {
    require(Alternatives.contains(alternative), s"unknown alternative $alternative")
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    compute(df, fit, kinds, reference, Some((alternative, level)), providers)
  }

  private def compute(
      df: DataFrame,
      fit: LinearREFit,
      kinds: Seq[String],
      reference: EffectReference,
      bounds: Option[(String, Double)],
      providers: Option[Seq[Any]]
  ): Map[String, DataFrame] = {
    require(
      kinds.nonEmpty && kinds.forall(Kinds.contains),
      s"kinds must be indirect and/or direct, got ${kinds.mkString(", ")}"
    )
    val spec = fit.spec
    val g0 = this.reference(fit, reference)
    val beta = fit.estimates.toArray
    val (workingSet, plan, input) = LinearRE.trainingWorkingSet(df, fit)
    try {
      val totals = workingSet.blocks
        .map((block: LinearBlock) => blockTotals(block, beta))(
          Encoders.product[LinearREStandardTotals]
        )
        .collect()
        .sortBy(_.blockId)
      val rows = totals.iterator.map(_.rows).sum
      if (totals.iterator.map(_.fingerprint).sum != fit.fingerprint || rows != fit.rows)
        throw new IllegalArgumentException(
          "the data differ from the fit's (fingerprint); standardized measures need the training data (API-3)"
        )
      val n = rows.toDouble
      val etaTotal = Summation.pairwise(totals.map(_.eta))
      DriverGuards.requireWithinBudget(
        s"${fit.providerCount} BLUPs",
        16L * fit.providerCount,
        fit.options.blocks
      )
      val fittedBlups = new NeumaierSum
      fit.providers.orderBy(spec.provider).select("records", "blup").collect().foreach { row =>
        fittedBlups.add(row.getLong(0).toDouble * row.getDouble(1))
      }
      val observedTotal = etaTotal + fittedBlups.value // Σᵢ ŷᵢ = Σᵢ ηᵢ + Σⱼ nⱼb̂ⱼ
      val sums = workingSet.blocks.flatMap((block: LinearBlock) => providerSums(block, beta))(
        Encoders.product[LinearREStandardRow]
      )
      val keys = PlanFrames
        .placements(df.sparkSession, plan, input.providerKeyIsText)
        .select(
          col(PlanFrames.GroupIndexColumn).as("groupIndex"),
          col(Validation.GroupColumn).as(spec.provider)
        )
      val joined = sums
        .toDF()
        .join(keys, Seq("groupIndex"))
        .join(fit.providers.select(col(spec.provider), col("blup"), col("se")), Seq(spec.provider))
      val table = providers.fold(joined)(wanted =>
        joined.filter(col(spec.provider).cast(StringType).isin(wanted.map(_.toString): _*))
      )
      val (lower, upper) = limits(bounds)
      val records = col("records").cast(DoubleType)
      val tables = kinds.map {
        case "indirect" =>
          val observed = col("eta") + records * col("blup")
          val expected = col("eta") + records * lit(g0)
          val withBounds = bounds.toSeq.flatMap(_ =>
            Seq(
              ((records * lower + col("eta") - expected) / records).as("lower"),
              ((records * upper + col("eta") - expected) / records).as("upper")
            )
          )
          "indirect" -> table.select(
            Seq(
              col(spec.provider),
              ((observed - expected) / records).as("indirect_difference"),
              observed.as("observed"),
              expected.as("expected")
            ) ++ withBounds: _*
          )
        case _ =>
          val expected = lit(etaTotal) + lit(n) * col("blup")
          val withBounds = bounds.toSeq.flatMap(_ =>
            Seq(
              ((lit(etaTotal) + lit(n) * lower - lit(observedTotal)) / lit(n)).as("lower"),
              ((lit(etaTotal) + lit(n) * upper - lit(observedTotal)) / lit(n)).as("upper")
            )
          )
          "direct" -> table.select(
            Seq(
              col(spec.provider),
              ((expected - lit(observedTotal)) / lit(n)).as("direct_difference"),
              lit(observedTotal).as("observed"),
              expected.as("expected")
            ) ++ withBounds: _*
          )
      }
      tables.map { case (kind, t) =>
        val persisted = t.orderBy(spec.provider).persist(fit.options.blocks.resolvedStorageLevel)
        persisted.count()
        kind -> persisted
      }.toMap
    } finally workingSet.release()
  }

  /** The BLUP's limits b̂ⱼ ∓ c·seⱼ at c = Φ⁻¹(1 − α/2), or Φ⁻¹(1 − α) and open on the other side (§4). */
  private def limits(bounds: Option[(String, Double)]): (Column, Column) = bounds match {
    case None                       => (lit(Double.NaN), lit(Double.NaN))
    case Some((alternative, level)) =>
      val q =
        if (alternative == "two_sided") Normal.upperQuantile((1.0 - level) / 2.0)
        else Normal.upperQuantile(1.0 - level)
      val below = col("blup") - lit(q) * col("se")
      val above = col("blup") + lit(q) * col("se")
      alternative match {
        case "two_sided" => (below, above)
        case "greater"   => (below, lit(Double.PositiveInfinity))
        case _           => (lit(Double.NegativeInfinity), above)
      }
  }

  /** p-value, flag and limits of one provider; undefined (NaN, no flag) when its se is 0 (D-55). */
  private[linear] def row(
      e: LinearREEffectRow,
      g0: Double,
      alternative: String,
      c: Double
  ): LinearTestRow =
    if (e.se == 0.0 || e.se.isNaN)
      LinearTestRow(
        e.key,
        e.records,
        e.blup,
        e.se,
        Double.NaN,
        Double.NaN,
        None,
        Double.NaN,
        Double.NaN
      )
    else {
      val z = (e.blup - g0) / e.se
      val (p, flag) =
        if (z.isNaN) (Double.NaN, None)
        else
          alternative match {
            case "two_sided" =>
              (Normal.twoSidedPValue(z), Some(if (z > c) 1 else if (z < -c) -1 else 0))
            case "greater" => (Normal.upperTail(z), Some(if (z > c) 1 else 0))
            case _         => (Normal.upperTail(-z), Some(if (z < -c) -1 else 0))
          }
      val (lower, upper) = alternative match {
        case "two_sided" => (e.blup - c * e.se, e.blup + c * e.se)
        case "greater"   => (e.blup - c * e.se, Double.PositiveInfinity)
        case _           => (Double.NegativeInfinity, e.blup + c * e.se)
      }
      LinearTestRow(e.key, e.records, e.blup, e.se, z, p, flag, lower, upper)
    }

  private def effects(fit: LinearREFit) =
    fit.providers
      .select(
        col(fit.spec.provider).cast(StringType).as("key"),
        col("records"),
        col("blup"),
        col("se")
      )
      .as[LinearREEffectRow](Encoders.product[LinearREEffectRow])

  private def keyColumn(fit: LinearREFit): Column = {
    val text = fit.providers.schema(fit.spec.provider).dataType == StringType
    (if (text) col("key") else col("key").cast(LongType)).as(fit.spec.provider)
  }

  /** ηᵢ = oᵢ + xᵢᵀβ̂ from a block row: the features, then w, then the offset (the working set's layout). */
  private def eta(block: LinearBlock, r: Int, beta: Array[Double]): Double = {
    val q = block.p + 1
    val p = block.p - 2
    val sum = new NeumaierSum
    var k = 0
    while (k < p) {
      sum.add(block.values(r * q + k) * beta(k))
      k += 1
    }
    sum.add(block.values(r * q + p + 1))
    sum.value
  }

  private def blockTotals(block: LinearBlock, beta: Array[Double]): LinearREStandardTotals = {
    val total = new NeumaierSum
    var r = 0
    while (r < block.rowId.length) {
      total.add(eta(block, r, beta))
      r += 1
    }
    LinearREStandardTotals(
      block.blockId,
      block.rowId.length.toLong,
      total.value,
      LinearFE.fingerprintOf(block)
    )
  }

  private def providerSums(
      block: LinearBlock,
      beta: Array[Double]
  ): Iterator[LinearREStandardRow] = {
    val rows = block.rows
    (0 until rows.groups).iterator.map { g =>
      val sum = new NeumaierSum
      var r = rows.groupStart(g)
      while (r < rows.until(g)) {
        sum.add(eta(block, r, beta))
        r += 1
      }
      LinearREStandardRow(
        block.groupIndex(g),
        (rows.until(g) - rows.groupStart(g)).toLong,
        sum.value
      )
    }
  }
}
