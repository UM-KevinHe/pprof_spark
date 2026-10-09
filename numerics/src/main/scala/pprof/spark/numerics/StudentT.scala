package pprof.spark.numerics

/** Student's t distribution for the linear model's inference (docs/spec/linear/fixed-effect-estimation.md §4).
  * Tails are evaluated directly in the tail asked for (X-031). The two-sided tail P(|T| > |t|) is the regularized
  * incomplete beta function I_x(ν/2, 1/2), x = ν/(ν + t²), computed exactly in every regime, as R's `pt` does
  * through `pbeta`: by Didonato and Morris's asymptotic expansion for large ν/2 (TOMS 708's BGRAT) when
  * |log x| ≤ 1, where the continued fraction would cancel, and otherwise by Lentz's continued fraction (or its
  * complement). log B(ν/2, 1/2) comes from Stirling's series for log Γ(a + 1/2) − log Γ(a). Quantiles invert the
  * upper tail by bisection. Pure and deterministic.
  */
object StudentT {

  private val Epsilon = 1e-16
  private val Tiny = 1e-300
  private val MaxIterations = 10000000

  /** P(|T| > |t|) for T with `df` degrees of freedom. */
  def twoSided(t: Double, df: Double): Double = {
    requireDegrees(df)
    if (t.isNaN) Double.NaN
    else {
      val t2 = t * t
      if (t2 == 0.0) 1.0
      else if (t2.isInfinite) 0.0
      else {
        val a = df / 2.0
        val logX = -StrictMath.log1p(t2 / df) // log x, x = ν / (ν + t²)
        if (a >= 15.0 && logX >= -1.0) asymptotic(a, logX) else fraction(t2, df)
      }
    }
  }

  /** I_x(a, 1/2) by the continued fraction, where it converges without cancellation. */
  private def fraction(t2: Double, df: Double): Double = {
    val a = df / 2.0
    val b = 0.5
    val x = 1.0 / (1.0 + t2 / df) // ν / (ν + t²)
    val y = 1.0 / (1.0 + df / t2) // t² / (ν + t²), computed without cancellation
    val logFront = -a * StrictMath.log1p(t2 / df) + b * StrictMath.log(y) - logBeta(a)
    if (x < (a + 1.0) / (a + b + 2.0)) StrictMath.exp(logFront) * continuedFraction(a, b, x) / a
    else 1.0 - StrictMath.exp(logFront) * continuedFraction(b, a, y) / b
  }

  /** P(T > t). */
  def upperTail(t: Double, df: Double): Double = {
    val p = twoSided(t, df)
    if (t.isNaN) Double.NaN else if (t >= 0.0) 0.5 * p else 1.0 - 0.5 * p
  }

  /** P(T ≤ t). */
  def cdf(t: Double, df: Double): Double = upperTail(-t, df)

  /** The t with P(T > t) = `upper`, for 0 < upper < 1. */
  def upperQuantile(upper: Double, df: Double): Double = {
    requireDegrees(df)
    require(
      upper > 0.0 && upper < 1.0,
      s"the tail probability must lie strictly between 0 and 1, got $upper"
    )
    if (upper == 0.5) 0.0
    else if (upper > 0.5) -upperQuantile(1.0 - upper, df)
    else {
      var lo = 0.0
      var hi = 1.0
      while (upperTail(hi, df) > upper) hi *= 2.0
      var i = 0
      while (i < 2000 && hi - lo > 2.0 * Math.ulp(hi)) {
        val mid = lo + 0.5 * (hi - lo)
        if (upperTail(mid, df) > upper) lo = mid else hi = mid
        i += 1
      }
      lo + 0.5 * (hi - lo)
    }
  }

  private val Terms = 30

  /** (2k + 1)! for k = 0 until Terms. */
  private val OddFactorials: Array[Double] = {
    val out = new Array[Double](Terms + 1)
    var f = 1.0
    var k = 1
    out(0) = 1.0
    while (k <= Terms) {
      f *= (2.0 * k) * (2.0 * k + 1.0)
      out(k) = f
      k += 1
    }
    out
  }

  /** Didonato and Morris's (1992) asymptotic expansion of I_x(a, b) for large a and small b (TOMS 708's BGRAT,
    * as Boost's `beta_small_b_large_a_series`), with b = 1/2: Q(1/2, u) = 2Φ̄(√(2u)), u = −(a − 1/4) log x.
    */
  private def asymptotic(a: Double, logX: Double): Double = {
    val b = 0.5
    val bm1 = b - 1.0
    val big = a + bm1 / 2.0
    val u = -big * logX
    val h =
      StrictMath.exp(b * StrictMath.log(u) - u - 0.5 * StrictMath.log(math.Pi)) // u^b e^-u / Γ(b)
    val prefix = h * StrictMath.exp(gammaRatio(a)) / math.sqrt(big)
    if (!(prefix > 0.0) || !(h > 0.0)) 0.0
    else {
      var j = 2.0 * Normal.upperTail(math.sqrt(2.0 * u)) / h // Q(b, u) / h
      var total = prefix * j
      val p = new Array[Double](Terms)
      p(0) = 1.0
      val lx2 = (logX / 2.0) * (logX / 2.0)
      var lxp = 1.0
      val t4 = 4.0 * big * big
      var b2n = b
      var n = 1
      var done = false
      while (!done && n < Terms) {
        var s = 0.0
        var m = 1
        while (m < n) {
          s += (m * b - n) * p(n - m) / OddFactorials(m)
          m += 1
        }
        p(n) = s / n + bm1 / OddFactorials(n)
        j = (b2n * (b2n + 1.0) * j + (u + b2n + 1.0) * lxp) / t4
        lxp *= lx2
        b2n += 2.0
        val r = prefix * p(n) * j
        total += r
        done = math.abs(r) < Epsilon * math.abs(total)
        n += 1
      }
      total
    }
  }

  /** log B(a, 1/2) = log Γ(1/2) − (log Γ(a + 1/2) − log Γ(a)). */
  private[numerics] def logBeta(a: Double): Double = 0.5 * StrictMath.log(math.Pi) - gammaRatio(a)

  /** log Γ(a + 1/2) − log Γ(a): Stirling's series for a ≥ 30, shifted up for smaller a. */
  private[numerics] def gammaRatio(a: Double): Double =
    if (a >= 30.0) {
      def series(z: Double): Double = {
        val z2 = z * z
        (1.0 / 12.0 - (1.0 / 360.0 - (1.0 / 1260.0 - (1.0 / 1680.0 - 1.0 / (1188.0 * z2)) / z2) / z2) / z2) / z
      }
      (a * StrictMath.log1p(0.5 / a) - 0.5) + 0.5 * StrictMath.log(a) + (series(a + 0.5) - series(
        a
      ))
    } else {
      val shift = math.ceil(30.0 - a).toInt
      var correction = 0.0
      var i = 0
      while (i < shift) {
        correction += StrictMath.log1p(0.5 / (a + i))
        i += 1
      }
      gammaRatio(a + shift) - correction
    }

  /** Lentz's evaluation of the incomplete beta continued fraction (Numerical Recipes' betacf). */
  private def continuedFraction(a: Double, b: Double, x: Double): Double = {
    val qab = a + b
    val qap = a + 1.0
    val qam = a - 1.0
    var c = 1.0
    var d = 1.0 - qab * x / qap
    if (math.abs(d) < Tiny) d = Tiny
    d = 1.0 / d
    var h = d
    var m = 1
    var done = false
    while (!done) {
      if (m > MaxIterations)
        throw new ArithmeticException(
          s"the incomplete beta continued fraction did not converge (a = $a, b = $b)"
        )
      val m2 = 2.0 * m
      var aa = m * (b - m) * x / ((qam + m2) * (a + m2))
      d = 1.0 + aa * d
      if (math.abs(d) < Tiny) d = Tiny
      c = 1.0 + aa / c
      if (math.abs(c) < Tiny) c = Tiny
      d = 1.0 / d
      h *= d * c
      aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
      d = 1.0 + aa * d
      if (math.abs(d) < Tiny) d = Tiny
      c = 1.0 + aa / c
      if (math.abs(c) < Tiny) c = Tiny
      d = 1.0 / d
      val delta = d * c
      h *= delta
      done = math.abs(delta - 1.0) < Epsilon
      m += 1
    }
    h
  }

  private def requireDegrees(df: Double): Unit =
    require(df > 0.0 && !df.isInfinite, s"degrees of freedom must be positive and finite, got $df")
}
