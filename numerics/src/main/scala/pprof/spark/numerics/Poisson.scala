package pprof.spark.numerics

/** Brent's root finder (Brent 1973, "zeroin"): bracketing, deterministic, with an absolute tolerance
  * `xtol` plus 4ε relative.
  */
object Brent {

  def root(
      f: Double => Double,
      a0: Double,
      b0: Double,
      xtol: Double,
      maxIterations: Int = 200
  ): Double = {
    var a = a0
    var b = b0
    var fa = f(a)
    var fb = f(b)
    if (fa == 0.0) return a
    if (fb == 0.0) return b
    require(fa * fb < 0.0, s"the root is not bracketed by [$a0, $b0]")
    var c = a
    var fc = fa
    var d = b - a
    var e = d
    var iteration = 0
    while (iteration < maxIterations) {
      if (fb * fc > 0.0) {
        c = a; fc = fa; d = b - a; e = d
      }
      if (math.abs(fc) < math.abs(fb)) {
        a = b; b = c; c = a
        fa = fb; fb = fc; fc = fa
      }
      val tol = 2.0 * Ulp * math.abs(b) + 0.5 * xtol
      val m = 0.5 * (c - b)
      if (math.abs(m) <= tol || fb == 0.0) return b
      if (math.abs(e) < tol || math.abs(fa) <= math.abs(fb)) {
        d = m; e = m
      } else {
        var p = 0.0
        var q = 0.0
        val s = fb / fa
        if (a == c) {
          p = 2.0 * m * s
          q = 1.0 - s
        } else {
          val qa = fa / fc
          val r = fb / fc
          p = s * (2.0 * m * qa * (qa - r) - (b - a) * (r - 1.0))
          q = (qa - 1.0) * (r - 1.0) * (s - 1.0)
        }
        if (p > 0.0) q = -q else p = -p
        if (2.0 * p < math.min(3.0 * m * q - math.abs(tol * q), math.abs(e * q))) {
          e = d; d = p / q
        } else {
          d = m; e = m
        }
      }
      a = b
      fa = fb
      b += (if (math.abs(d) > tol) d else if (m > 0.0) tol else -tol)
      fb = f(b)
      iteration += 1
    }
    b
  }

  private val Ulp = 2.220446049250313e-16
}

/** Log-gamma and the regularized incomplete gamma functions, with StrictMath (§8.1). */
object Gamma {

  private val Lanczos = Array(
    0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
    -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6,
    1.5056327351493116e-7
  )
  private val HalfLogTwoPi = 0.9189385332046728
  private val Epsilon = 1e-16
  private val Tiny = 1e-300

  /** log Γ(x) for x > 0 (Lanczos, g = 7). */
  def logGamma(x: Double): Double = {
    require(x > 0.0, s"logGamma needs x > 0, got $x")
    val z = x - 1.0
    var a = Lanczos(0)
    var i = 1
    while (i < Lanczos.length) {
      a += Lanczos(i) / (z + i)
      i += 1
    }
    val t = z + 7.5
    HalfLogTwoPi + (z + 0.5) * StrictMath.log(t) - t + StrictMath.log(a)
  }

  /** (P(a, x), Q(a, x)), the lower and upper regularized incomplete gamma functions, a > 0, x ≥ 0;
    * the smaller of the two is computed directly, so tails keep their relative accuracy.
    */
  def regularized(a: Double, x: Double): (Double, Double) = {
    require(a > 0.0 && x >= 0.0, s"need a > 0 and x >= 0, got a = $a, x = $x")
    if (x == 0.0) (0.0, 1.0)
    else {
      val front = StrictMath.exp(-x + a * StrictMath.log(x) - logGamma(a))
      if (x < a + 1.0) {
        var ap = a
        var term = 1.0 / a
        var sum = term
        var n = 0
        while (math.abs(term) >= math.abs(sum) * Epsilon && n < 10000) {
          ap += 1.0
          term *= x / ap
          sum += term
          n += 1
        }
        val p = sum * front
        (p, 1.0 - p)
      } else {
        var b = x + 1.0 - a
        var c = 1.0 / Tiny
        var d = 1.0 / b
        var h = d
        var i = 1
        var done = false
        while (!done && i < 10000) {
          val an = -i * (i - a)
          b += 2.0
          d = an * d + b
          if (math.abs(d) < Tiny) d = Tiny
          c = b + an / c
          if (math.abs(c) < Tiny) c = Tiny
          d = 1.0 / d
          val delta = d * c
          h *= delta
          done = math.abs(delta - 1.0) < Epsilon
          i += 1
        }
        val q = front * h
        (1.0 - q, q)
      }
    }
  }
}

/** The Poisson distribution and χ² quantiles for provider tests (Phase 1d specification §2). */
object Poisson {

  /** P(X ≤ k) for X ~ Poisson(mu). */
  def cdf(k: Double, mu: Double): Double =
    if (k < 0.0) 0.0 else Gamma.regularized(math.floor(k) + 1.0, mu)._2

  /** P(X ≥ k). */
  def atLeast(k: Double, mu: Double): Double =
    if (k <= 0.0) 1.0 else Gamma.regularized(math.ceil(k), mu)._1

  /** P(X = k) for integral k ≥ 0. */
  def pmf(k: Double, mu: Double): Double =
    if (k < 0.0) 0.0
    else if (mu == 0.0) (if (k == 0.0) 1.0 else 0.0)
    else StrictMath.exp(k * StrictMath.log(mu) - mu - Gamma.logGamma(k + 1.0))

  /** The p-quantile of χ² with `df` degrees of freedom. */
  def chiSquareQuantile(p: Double, df: Double): Double = {
    require(p > 0.0 && p < 1.0 && df > 0.0, s"need 0 < p < 1 and df > 0, got p = $p, df = $df")
    val a = df / 2.0
    val f = (y: Double) => Gamma.regularized(a, y)._1 - p
    var hi = math.max(1.0, a)
    while (f(hi) <= 0.0) hi *= 2.0
    2.0 * Brent.root(f, 0.0, hi, 1e-300)
  }
}

/** One provider's test against a ratio of 1 (Phase 1d specification §2). */
final case class PoissonTestResult(
    estimate: Double,
    zRaw: Double,
    pValue: Double,
    flag: Int,
    ciLower: Double,
    ciUpper: Double
)

/** Exact and mid-p Poisson tests of observed against expected events, as pprof_py's
  * `poisson_exact_test`, `poisson_midp_zscore` and `CoxPH.test` with the theoretical null.
  */
object PoissonTests {

  /** The mid-p z-statistic: p = max(1e-6, min(P_min, P_max)/2), z = Φ⁻¹(p) signed by the smaller tail. */
  def midpZ(observed: Double, expected: Double): Double = {
    val f = Poisson.pmf(observed, expected)
    val low = 2.0 * Poisson.cdf(observed, expected) - f
    val high = 2.0 * Poisson.atLeast(observed, expected) - f
    val p = math.max(1e-6, math.min(low, high) / 2.0)
    val z = if (p >= 0.5) 0.0 else -Normal.upperQuantile(p)
    if (low <= high) z else -z
  }

  /** The two-sided exact Poisson p-value, at most 0.999. */
  def exactP(observed: Double, expected: Double): Double =
    if (observed / expected > 1.0) math.min(0.999, 2.0 * Poisson.atLeast(observed, expected))
    else math.min(0.999, 2.0 * Poisson.cdf(observed, expected))

  /** Limits for the ratio: χ²-based below 100 expected events, Byar's cube-root approximation above. */
  def exactLimits(observed: Double, expected: Double, alpha: Double): (Double, Double) =
    if (expected < 100.0) {
      val lower =
        if (observed > 0.0) Poisson.chiSquareQuantile(alpha / 2.0, 2.0 * observed) / 2.0 / expected
        else 0.0
      val upper =
        Poisson.chiSquareQuantile(1.0 - alpha / 2.0, 2.0 * (observed + 1.0)) / 2.0 / expected
      (lower, upper)
    } else {
      val z = Normal.upperQuantile(alpha / 2.0)
      def cube(v: Double) = v * v * v
      val lower =
        if (observed > 0.0)
          (observed / expected) * cube(
            1.0 - 1.0 / (9.0 * observed) - z / (3.0 * math.sqrt(observed))
          )
        else 0.0
      val o1 = observed + 1.0
      (lower, (o1 / expected) * cube(1.0 - 1.0 / (9.0 * o1) + z / (3.0 * math.sqrt(o1))))
    }

  /** Limits for the Poisson mean from inverting the calibrated two-sided mid-p test (pprof_py's
    * `_midp_limits`): 0 or ∞ on a side where the calibrated p-value stays at or above `alpha`.
    */
  def midpLimits(
      observed: Double,
      expected: Double,
      alpha: Double,
      mu: Double = 0.0,
      sd: Double = 1.0
  ): (Double, Double) = {
    val z = (t: Double) => midpZ(observed, t)
    val excess = (t: Double) => 2.0 * Normal.upperTail(math.abs(z(t) - mu) / sd) - alpha
    val scale = math.max(expected, 1.0)
    val loEnd = 1e-10 * scale
    var hiEnd = 10.0 * (observed + expected + 10.0)
    while (z(hiEnd) > math.max(mu, -4.75) && hiEnd < 1e12) hiEnd *= 10.0
    val mode =
      if (z(loEnd) <= mu) loEnd
      else if (z(hiEnd) >= mu) hiEnd
      else Brent.root(t => z(t) - mu, loEnd, hiEnd, 1e-12 * scale)
    val lower = if (excess(loEnd) >= 0.0) 0.0 else Brent.root(excess, loEnd, mode, 1e-10 * scale)
    val upper =
      if (excess(hiEnd) >= 0.0) Double.PositiveInfinity
      else Brent.root(excess, mode, hiEnd, 1e-10 * scale)
    (lower, upper)
  }

  /** The test of one provider: `midp` (default in pprof_py) or `exact`, at `level`. */
  def test(observed: Double, expected: Double, method: String, level: Double): PoissonTestResult = {
    require(level > 0.0 && level < 1.0, s"level must lie in (0, 1), got $level")
    val alpha = 1.0 - level
    val (z, lower, upper) = method match {
      case "midp" =>
        val (lo, hi) = midpLimits(observed, expected, alpha)
        (midpZ(observed, expected), lo / expected, hi / expected)
      case "exact" =>
        val p = exactP(observed, expected)
        val magnitude = if (p <= 0.0) Double.PositiveInfinity else Normal.upperQuantile(p / 2.0)
        val (lo, hi) = exactLimits(observed, expected, alpha)
        (math.signum(observed - expected) * magnitude, lo, hi)
      case other =>
        throw new IllegalArgumentException(s"unknown test method $other; expected midp or exact")
    }
    val p = if (z.isNaN) Double.NaN else 2.0 * Normal.upperTail(math.abs(z))
    val flag = if (p < alpha) math.signum(z).toInt else 0
    PoissonTestResult(observed / expected, z, p, flag, lower, upper)
  }
}
