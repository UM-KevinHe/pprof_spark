package pprof.spark.engine.logistic

/** The null value of a test on a standardized measure (pprof_py's `null_value`; standardization
  * specification §2a, D-49).
  */
sealed abstract class NullValue extends Product with Serializable

object NullValue {

  /** The measure at the reference provider effect (the median or mean of γ̂): pprof_py's default. */
  case object Reference extends NullValue

  /** The mean of the providers' finite estimates. */
  case object Mean extends NullValue

  /** The median of the providers' finite estimates (the mean of the middle two for an even count). */
  case object Median extends NullValue

  /** A given value on the measure's scale. */
  final case class Value(value: Double) extends NullValue

  /** `reference`, `mean`, `median`, or `value` with its number. */
  def of(mode: String, value: Double): NullValue = mode match {
    case "reference" => Reference
    case "mean"      => Mean
    case "median"    => Median
    case "value"     => Value(value)
    case other       =>
      throw new IllegalArgumentException(
        s"null value must be reference, mean, median or a number, got $other"
      )
  }
}

/** The null distribution of z (pprof_py's `null_model`): z is calibrated to (z - mean) / sd before
  * p-values, flags and intervals (standardization specification §2a, D-49).
  */
sealed abstract class NullModel extends Product with Serializable {
  def mean: Double
  def sd: Double
}

object NullModel {

  /** N(0, 1): pprof_py's `TheoreticalNull`, the default. */
  case object Theoretical extends NullModel {
    val mean: Double = 0.0
    val sd: Double = 1.0
  }

  /** N(mean, sd²): pprof_py's `FixedNull`. */
  final case class Fixed(mean: Double, sd: Double) extends NullModel {
    require(
      java.lang.Double.isFinite(mean) && java.lang.Double.isFinite(sd) && sd > 0.0,
      s"a fixed null needs a finite mean and a positive finite sd, got $mean and $sd"
    )
  }
}

/** Where confidence limits are clipped (pprof_py's `bounds`). */
sealed abstract class IntervalBounds extends Product with Serializable

object IntervalBounds {

  /** [0, 1] for rates and [0, ∞) for ratios on the identity scale, otherwise none: pprof_py's `auto`. */
  case object Auto extends IntervalBounds

  /** No clipping: pprof_py's `None`. */
  case object Unbounded extends IntervalBounds

  /** Clipping to [lower, upper]. */
  final case class Fixed(lower: Double, upper: Double) extends IntervalBounds {
    require(!(lower > upper), s"bounds must have lower <= upper, got ($lower, $upper)")
  }
}
