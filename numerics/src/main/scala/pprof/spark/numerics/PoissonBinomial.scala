package pprof.spark.numerics

/** Exact Poisson-binomial tails and the pieces of the logistic provider tests (Phase 2c specification,
  * docs/spec/logistic/provider-tests.md §2 and §3).
  */
object PoissonBinomial {

  /** Probabilities are clipped to [1e-10, 1 − 1e-10] (pprof_py's `_poibin_probs`). */
  val ProbabilityClip: Double = 1e-10

  /** Exact tails below this are raised to it, and abs(z) is capped at its normal quantile. */
  val TailFloor: Double = 1e-300

  /** The exact test expands binomial rows into their trials up to this many per provider (pprof_py). */
  val MaxTrials: Long = 20000L

  /** Tails at the observed count: mid-p upper and lower (two-sided tests), P(X ≥ o), P(X ≤ o), and the
    * strict tails P(X > o) and P(X < o), so that a tail near 1 can be read from its small complement.
    */
  final case class Tails(
      upperMid: Double,
      lowerMid: Double,
      atLeast: Double,
      atMost: Double,
      above: Double,
      below: Double
  )

  def clip(p: Double): Double = math.min(math.max(p, ProbabilityClip), 1.0 - ProbabilityClip)

  /** Tails at `observed` of the sum of independent Bernoulli(πᵢ), row i repeated `trials(i)` times. The
    * smaller tail comes from the exact recursion truncated at the observed count, over the events when
    * the count is at or below its expectation and over the non-events above it; the other tail is one
    * minus it. All terms are non-negative, so both tails keep relative accuracy (X-024).
    */
  def tails(probabilities: Array[Double], trials: Array[Int], observed: Int): Tails = {
    require(probabilities.length == trials.length, "one trial count per probability")
    val p = probabilities.map(clip)
    val total = trials.foldLeft(0L)(_ + _)
    require(
      observed >= 0 && observed <= total,
      s"the observed count $observed lies outside [0, $total]"
    )
    val expected = new NeumaierSum
    p.indices.foreach(i => expected.add(trials(i) * p(i)))
    if (observed <= expected.value) {
      val pmf = truncated(p, trials, observed)
      val below = sum(pmf, observed)
      val at = pmf(observed)
      val lowerMid = below + 0.5 * at
      Tails(
        1.0 - lowerMid,
        lowerMid,
        if (observed > 0) 1.0 - below else 1.0,
        below + at,
        1.0 - below - at,
        below
      )
    } else {
      val count = (total - observed).toInt
      val pmf = truncated(p.map(1.0 - _), trials, count)
      val above = sum(pmf, count)
      val at = pmf(count)
      val upperMid = above + 0.5 * at
      Tails(upperMid, 1.0 - upperMid, above + at, 1.0 - above, above, 1.0 - above - at)
    }
  }

  /** P(S = k) for k = 0 to `cap`, S the sum of the trials' Bernoulli outcomes. */
  private def truncated(p: Array[Double], trials: Array[Int], cap: Int): Array[Double] = {
    val pmf = new Array[Double](cap + 1)
    pmf(0) = 1.0
    var reached = 0
    var i = 0
    while (i < p.length) {
      var t = 0
      while (t < trials(i)) {
        val q = 1.0 - p(i)
        reached = math.min(reached + 1, cap)
        var k = reached
        while (k >= 1) {
          pmf(k) = pmf(k) * q + pmf(k - 1) * p(i)
          k -= 1
        }
        pmf(0) *= q
        t += 1
      }
      i += 1
    }
    pmf
  }

  private def sum(values: Array[Double], until: Int): Double = {
    val s = new NeumaierSum
    var k = 0
    while (k < until) {
      s.add(values(k))
      k += 1
    }
    s.value
  }

  /** The z-statistic whose normal tails reproduce the tails (pprof_py's `z_from_tails`): two-sided
    * tests take the smaller mid-p tail, one-sided ones P(X ≥ o) or P(X ≤ o), read from the small
    * complement when the tail exceeds one half (X-024); tails are raised to `floor` and abs(z) is
    * capped at its normal quantile.
    */
  def z(t: Tails, alternative: String, floor: Double): Double = {
    def tail(x: Double) = math.min(math.max(x, floor), 1.0)
    val raw = alternative match {
      case "two_sided" =>
        val u = tail(t.upperMid)
        val l = tail(t.lowerMid)
        if (u <= l) inverse(u) else -inverse(l)
      case "greater" => if (t.atLeast <= 0.5) inverse(tail(t.atLeast)) else -inverse(tail(t.below))
      case "less"    => if (t.atMost <= 0.5) -inverse(tail(t.atMost)) else inverse(tail(t.above))
      case other     => throw new IllegalArgumentException(s"unknown alternative $other")
    }
    val cap = Normal.upperQuantile(floor)
    math.min(math.max(raw, -cap), cap)
  }

  /** Φ̄⁻¹(t), with Φ̄⁻¹(1) = −∞. */
  private def inverse(t: Double): Double =
    if (t >= 1.0) Double.NegativeInfinity else Normal.upperQuantile(t)

  /** Solves z(g) = target for z decreasing in g, from `start` outward (pprof_py's `invert_decreasing`):
    * steps of 0.5 growing by 1.6 until the sign changes, then Brent's method to 1e-10; ±∞ when the
    * target lies beyond `span`.
    */
  def invertDecreasing(
      z: Double => Double,
      start: Double,
      target: Double,
      span: Double = 40.0
  ): Double =
    if (!java.lang.Double.isFinite(target)) Double.NaN
    else {
      var a = start
      var fa = z(a) - target
      if (fa == 0.0) a
      else {
        val direction = if (fa > 0.0) 1.0 else -1.0
        var step = 0.5
        var result = Double.NaN
        while (result.isNaN && math.abs(a - start) < span) {
          val b = a + direction * step
          val fb = z(b) - target
          if (fb == 0.0) result = b
          else if (math.signum(fb) != math.signum(fa)) {
            val (lo, hi) = if (a < b) (a, b) else (b, a)
            result = Brent.root(g => z(g) - target, lo, hi, 1e-10)
          } else {
            a = b
            fa = fb
            step *= 1.6
          }
        }
        if (!result.isNaN) result
        else if (direction > 0.0) Double.PositiveInfinity
        else Double.NegativeInfinity
      }
    }

  /** A uniform in [0, 1) determined by (seed, provider, replicate, trial) alone (STAT-2, X-025). */
  def uniform(seed: Long, provider: Long, replicate: Int, trial: Long): Double = {
    val h = mix(mix(mix(mix(seed + 0x9e3779b97f4a7c15L) ^ provider) ^ replicate.toLong) ^ trial)
    (h >>> 11).toDouble * (1.0 / (1L << 53).toDouble)
  }

  /** SplitMix64's finalizer. */
  private def mix(x: Long): Long = {
    var z = x + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }

  /** Bootstrap tails at `observed` from `replicates` simulated counts (pprof_py's `bootstrap_tails`). */
  def bootstrapTails(
      probabilities: Array[Double],
      trials: Array[Int],
      observed: Int,
      replicates: Int,
      seed: Long,
      provider: Long
  ): Tails = {
    val p = probabilities.map(clip)
    var ge = 0L
    var gt = 0L
    var le = 0L
    var lt = 0L
    var r = 0
    while (r < replicates) {
      var count = 0L
      var trial = 0L
      var i = 0
      while (i < p.length) {
        var t = 0
        while (t < trials(i)) {
          if (uniform(seed, provider, r, trial) < p(i)) count += 1L
          trial += 1L
          t += 1
        }
        i += 1
      }
      if (count >= observed) ge += 1L
      if (count > observed) gt += 1L
      if (count <= observed) le += 1L
      if (count < observed) lt += 1L
      r += 1
    }
    val n = replicates.toDouble
    Tails((ge + gt) / 2.0 / n, (le + lt) / 2.0 / n, ge / n, le / n, gt / n, lt / n)
  }
}
