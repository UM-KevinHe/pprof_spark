package pprof.spark.engine.linear

import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import pprof.spark.engine.backend.DriverGuards
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.numerics.{NeumaierSum, Normal, StudentT}

/** One provider's effect and standard error, keyed as text. */
final case class LinearEffectRow(key: String, records: Long, gamma: Double, se: Double)

/** One provider's test (docs/spec/linear/provider-inference.md §2). */
final case class LinearTestRow(
    key: String,
    records: Long,
    estimate: Double,
    se: Double,
    z: Double,
    pValue: Double,
    flag: Option[Int],
    lower: Double,
    upper: Double
)

/** Provider tests and effect intervals of the linear model (spec 3b §1 to §3), from the fit alone. */
object LinearProviderTests {

  val Alternatives: Seq[String] = Seq("two_sided", "greater", "less")

  /** γ₀ (§1) on the driver: the provider table's records and effects in key order, guarded by `DriverGuards`. */
  def reference(fit: LinearFit, reference: EffectReference): Double = reference match {
    case EffectReference.Value(value) => value
    case kind                         =>
      DriverGuards.requireWithinBudget(
        s"${fit.providerCount} provider effects",
        16L * fit.providerCount,
        fit.options.blocks
      )
      val rows = fit.providers.orderBy(fit.spec.provider).select("records", "gamma").collect()
      if (kind == EffectReference.Median) median(rows.map(_.getDouble(1)))
      else {
        val weighted = new NeumaierSum
        rows.foreach(row => weighted.add(row.getLong(0).toDouble * row.getDouble(1)))
        weighted.value / rows.iterator.map(_.getLong(0)).sum.toDouble
      }
  }

  /** One row per provider with the columns of slice 2c (§2), ordered by key. */
  def test(
      fit: LinearFit,
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None
  ): DataFrame = {
    require(Alternatives.contains(alternative), s"unknown alternative $alternative")
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    critical.foreach(c => require(c > 0.0, s"the critical value must be positive, got $c"))
    val g0 = this.reference(fit, reference)
    val df = fit.degreesOfFreedom.toDouble
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      if (alternative == "two_sided") Normal.upperQuantile(alpha / 2.0)
      else Normal.upperQuantile(alpha)
    )
    val q = StudentT.upperQuantile(Normal.upperTail(c), df)
    val wanted = providers.map(_.map(_.toString).toSet)
    val rows = effects(fit).flatMap { (e: LinearEffectRow) =>
      if (wanted.exists(w => !w.contains(e.key))) Iterator.empty
      else Iterator.single(row(e, g0, df, alternative, c, q))
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

  /** Effect intervals γ̂ⱼ ∓ t₁₋α/₂,ν·seⱼ, two-sided only (§3, option `gamma`); columns key, gamma, lower, upper. */
  def gammaIntervals(
      fit: LinearFit,
      level: Double = 0.95,
      providers: Option[Seq[Any]] = None
  ): DataFrame = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val q = StudentT.upperQuantile((1.0 - level) / 2.0, fit.degreesOfFreedom.toDouble)
    val key = fit.spec.provider
    val all = fit.providers.select(
      col(key),
      col("gamma"),
      (col("gamma") - lit(q) * col("se")).as("lower"),
      (col("gamma") + lit(q) * col("se")).as("upper")
    )
    providers
      .fold(all)(wanted => all.filter(col(key).cast(StringType).isin(wanted.map(_.toString): _*)))
      .orderBy(key)
  }

  /** p-value, flag and limits of one provider: t, then z by equal tail probability (finite for any finite t). */
  private[linear] def row(
      e: LinearEffectRow,
      g0: Double,
      df: Double,
      alternative: String,
      c: Double,
      q: Double
  ): LinearTestRow = {
    val t = (e.gamma - g0) / e.se
    val z = StudentT.toZ(t, df)
    val (p, flag) =
      if (t.isNaN) (Double.NaN, None)
      else
        alternative match {
          case "two_sided" =>
            (StudentT.twoSided(t, df), Some(if (z > c) 1 else if (z < -c) -1 else 0))
          case "greater" => (StudentT.upperTail(t, df), Some(if (z > c) 1 else 0))
          case _         => (StudentT.cdf(t, df), Some(if (z < -c) -1 else 0))
        }
    val (lower, upper) = alternative match {
      case "two_sided" => (e.gamma - q * e.se, e.gamma + q * e.se)
      case "greater"   => (e.gamma - q * e.se, Double.PositiveInfinity)
      case _           => (Double.NegativeInfinity, e.gamma + q * e.se)
    }
    LinearTestRow(e.key, e.records, e.gamma, e.se, z, p, flag, lower, upper)
  }

  private def effects(fit: LinearFit) =
    fit.providers
      .select(
        col(fit.spec.provider).cast(StringType).as("key"),
        col("records"),
        col("gamma"),
        col("se")
      )
      .as[LinearEffectRow](Encoders.product[LinearEffectRow])

  private def keyColumn(fit: LinearFit): Column = {
    val text = fit.providers.schema(fit.spec.provider).dataType == StringType
    (if (text) col("key") else col("key").cast(LongType)).as(fit.spec.provider)
  }

  private def median(values: Array[Double]): Double = {
    val sorted = values.sorted
    val m = sorted.length
    if (m % 2 == 1) sorted(m / 2) else 0.5 * (sorted(m / 2 - 1) + sorted(m / 2))
  }
}
