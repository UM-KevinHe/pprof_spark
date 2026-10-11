package pprof.spark.engine.linear

import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, StringType}
import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.data.Validation
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.numerics.{NeumaierSum, StudentT, Summation}

/** One block's totals: rows, Σ xᵢᵀβ̂, Σ (γ₀ + xᵢᵀβ̂) and its share of the fingerprint. */
final case class LinearStandardTotals(
    blockId: Int,
    rows: Long,
    xbeta: Double,
    observed: Double,
    fingerprint: Long
)

/** One provider's sums: Oⱼ = Σ yᵢ, Eⱼ = Σ (γ₀ + xᵢᵀβ̂) and Sⱼ = Σ xᵢᵀβ̂. */
final case class LinearStandardRow(
    groupIndex: Int,
    records: Long,
    observed: Double,
    expected: Double,
    xbeta: Double
)

/** Standardized differences and their intervals (docs/spec/linear/provider-inference.md §3 to §6): one pass over
  * ProviderLocal blocks of the training data, checked against the fit's fingerprint (API-3), with the provider sums
  * in canonical order and the totals reduced in block order; the limits come from the fit's provider table, so they
  * agree with `LinearProviderTests`.
  */
object LinearStandardization {

  val Kinds: Seq[String] = Seq("indirect", "direct")

  /** pprof_py's `calculate_standardized_measures`: a table per kind, ordered by key. */
  def measures(
      df: DataFrame,
      fit: LinearFit,
      kinds: Seq[String] = Kinds,
      reference: EffectReference = EffectReference.Median,
      providers: Option[Seq[Any]] = None
  ): Map[String, DataFrame] = compute(df, fit, kinds, reference, None, providers)

  /** pprof_py's `calculate_confidence_intervals(option = "SM")`: the measures with `lower` and `upper`. */
  def intervals(
      df: DataFrame,
      fit: LinearFit,
      kinds: Seq[String] = Seq("indirect"),
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      providers: Option[Seq[Any]] = None
  ): Map[String, DataFrame] = {
    require(
      LinearProviderTests.Alternatives.contains(alternative),
      s"unknown alternative $alternative"
    )
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    compute(df, fit, kinds, reference, Some((alternative, level)), providers)
  }

  private def compute(
      df: DataFrame,
      fit: LinearFit,
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
    val g0 = LinearProviderTests.reference(fit, reference)
    val beta = fit.estimates.toArray
    val (workingSet, plan, input) = LinearFE.trainingWorkingSet(df, fit)
    try {
      val totals = workingSet.blocks
        .map((block: LinearBlock) => blockTotals(block, beta, g0))(
          Encoders.product[LinearStandardTotals]
        )
        .collect()
        .sortBy(_.blockId)
      val rows = totals.iterator.map(_.rows).sum
      if (totals.iterator.map(_.fingerprint).sum != fit.fingerprint || rows != fit.rows)
        throw new IllegalArgumentException(
          "the data differ from the fit's (fingerprint); standardized measures need the training data (API-3)"
        )
      val n = rows.toDouble
      val xbetaTotal = Summation.pairwise(totals.map(_.xbeta))
      val observedTotal = Summation.pairwise(totals.map(_.observed))
      val sums = workingSet.blocks.flatMap((block: LinearBlock) => providerSums(block, beta, g0))(
        Encoders.product[LinearStandardRow]
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
        .join(fit.providers.select(col(spec.provider), col("gamma"), col("se")), Seq(spec.provider))
      val table = providers.fold(joined)(wanted =>
        joined.filter(col(spec.provider).cast(StringType).isin(wanted.map(_.toString): _*))
      )
      val (lower, upper) = limits(fit, bounds)
      val records = col("records").cast(DoubleType)
      val tables = kinds.map {
        case "indirect" =>
          val withBounds = bounds.toSeq.flatMap(_ =>
            Seq(
              ((records * lower + col("xbeta") - col("expected")) / records).as("lower"),
              ((records * upper + col("xbeta") - col("expected")) / records).as("upper")
            )
          )
          "indirect" -> table.select(
            Seq(
              col(spec.provider),
              ((col("observed") - col("expected")) / records).as("indirect_difference"),
              col("observed"),
              col("expected")
            ) ++ withBounds: _*
          )
        case _ =>
          val expected = col("gamma") * lit(n) + lit(xbetaTotal)
          val withBounds = bounds.toSeq.flatMap(_ =>
            Seq((lower - lit(g0)).as("lower"), (upper - lit(g0)).as("upper"))
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

  /** The effect's limits γ̂ⱼ ∓ q·seⱼ from the provider table, open on the other side when one-sided (§3). */
  private def limits(fit: LinearFit, bounds: Option[(String, Double)]): (Column, Column) =
    bounds match {
      case None                       => (lit(Double.NaN), lit(Double.NaN))
      case Some((alternative, level)) =>
        val df = fit.degreesOfFreedom.toDouble
        val q =
          if (alternative == "two_sided") StudentT.upperQuantile((1.0 - level) / 2.0, df)
          else StudentT.upperQuantile(1.0 - level, df)
        val below = col("gamma") - lit(q) * col("se")
        val above = col("gamma") + lit(q) * col("se")
        alternative match {
          case "two_sided" => (below, above)
          case "greater"   => (below, lit(Double.PositiveInfinity))
          case _           => (lit(Double.NegativeInfinity), above)
        }
    }

  private def xbeta(block: LinearBlock, r: Int, beta: Array[Double]): Double = {
    val q = block.p + 1
    val sum = new NeumaierSum
    var k = 0
    while (k < block.p) {
      sum.add(block.values(r * q + k) * beta(k))
      k += 1
    }
    sum.value
  }

  private def blockTotals(
      block: LinearBlock,
      beta: Array[Double],
      g0: Double
  ): LinearStandardTotals = {
    val xb = new NeumaierSum
    val observed = new NeumaierSum
    var r = 0
    while (r < block.rowId.length) {
      val v = xbeta(block, r, beta)
      xb.add(v)
      observed.add(g0 + v)
      r += 1
    }
    LinearStandardTotals(
      block.blockId,
      block.rowId.length.toLong,
      xb.value,
      observed.value,
      LinearFE.fingerprintOf(block)
    )
  }

  private def providerSums(
      block: LinearBlock,
      beta: Array[Double],
      g0: Double
  ): Iterator[LinearStandardRow] = {
    val rows = block.rows
    val q = block.p + 1
    (0 until rows.groups).iterator.map { g =>
      val observed = new NeumaierSum
      val expected = new NeumaierSum
      val xb = new NeumaierSum
      var r = rows.groupStart(g)
      while (r < rows.until(g)) {
        val v = xbeta(block, r, beta)
        observed.add(block.values(r * q + block.p))
        expected.add(g0 + v)
        xb.add(v)
        r += 1
      }
      LinearStandardRow(
        block.groupIndex(g),
        (rows.until(g) - rows.groupStart(g)).toLong,
        observed.value,
        expected.value,
        xb.value
      )
    }
  }
}
