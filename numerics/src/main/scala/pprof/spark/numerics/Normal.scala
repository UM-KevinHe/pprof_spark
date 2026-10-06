package pprof.spark.numerics

/** The standard normal distribution, for Wald tests and confidence intervals (Cox specification
  * §7).
  *
  * The upper tail uses the power series of Φ(z) − ½ below z = 1.5 and Laplace's continued fraction
  * for the Mills ratio above it, evaluated with the modified Lentz method, so it keeps its relative
  * accuracy deep into the tail: p-values far below 1e-10 are compared on the log scale (§8.4).
  * Quantiles refine a rational starting value (Abramowitz and Stegun 26.2.23) with Newton steps on
  * the upper tail. Everything uses StrictMath (§8.1), so results are the same on every JVM.
  */
object Normal {

  private val InvSqrtTwoPi = 0.3989422804014327
  private val SeriesCut = 1.5
  private val Underflow = 38.5

  /** The density φ(z). */
  def density(z: Double): Double = InvSqrtTwoPi * StrictMath.exp(-0.5 * z * z)

  /** The upper tail P(Z > z). */
  def upperTail(z: Double): Double =
    if (z.isNaN) Double.NaN
    else if (z < 0.0) 1.0 - upperTail(-z)
    else if (z < SeriesCut) 0.5 - density(z) * series(z)
    else if (z > Underflow) 0.0
    else density(z) * millsRatio(z)

  /** The two-sided p-value 2·P(Z > |z|) of a Wald statistic. */
  def twoSidedPValue(z: Double): Double =
    if (z.isNaN) Double.NaN else math.min(1.0, 2.0 * upperTail(math.abs(z)))

  /** The z with P(Z > z) = a, for a in (0, 1). */
  def upperQuantile(a: Double): Double = {
    require(a > 0.0 && a < 1.0, s"an upper-tail probability must lie in (0, 1); got $a")
    if (a > 0.5) -upperQuantile(1.0 - a)
    else if (a == 0.5) 0.0
    else {
      val t = math.sqrt(-2.0 * StrictMath.log(a))
      var z = t - (2.515517 + 0.802853 * t + 0.010328 * t * t) /
        (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t)
      var k = 0
      var done = false
      while (!done && k < 50) {
        val step = (upperTail(z) - a) / density(z)
        z += step
        k += 1
        done = math.abs(step) <= 1e-15 * math.abs(z)
      }
      z
    }
  }

  /** Σ z^(2n+1) / (1·3·5···(2n+1)), so that Φ(z) − ½ = φ(z) times this sum. */
  private def series(z: Double): Double = {
    var term = z
    var total = z
    var n = 0
    var done = false
    while (!done) {
      n += 1
      term *= z * z / (2 * n + 1)
      val next = total + term
      done = next == total
      total = next
    }
    total
  }

  /** The Mills ratio 1/(z + 1/(z + 2/(z + 3/(z + ...)))), for z ≥ 1.5. */
  private def millsRatio(z: Double): Double = {
    val tiny = 1e-300
    var f = z
    var c = f
    var d = 0.0
    var n = 0
    var done = false
    while (!done && n < 5000) {
      n += 1
      d = z + n * d
      d = 1.0 / (if (d != 0.0) d else tiny)
      c = z + n / c
      if (c == 0.0) c = tiny
      val delta = c * d
      f *= delta
      done = math.abs(delta - 1.0) < 1e-16
    }
    1.0 / f
  }
}
