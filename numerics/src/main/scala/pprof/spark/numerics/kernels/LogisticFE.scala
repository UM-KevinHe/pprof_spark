package pprof.spark.numerics.kernels

import pprof.spark.numerics.NeumaierSum

/** Kernels of the logistic fixed-effect model over the rows of one block (Phase 2a specification
  * §2 and §6). Provider `k` holds rows `starts(k)` until `end(k)`, with events `y`, trials `n` and
  * covariates `x` (row-major, `p` per row) in canonical order. Every sum runs in that order with
  * Neumaier compensation, so a result depends only on the block's contents (R0).
  */
object LogisticFE {

  /** Floor of nπ(1 − π) in the iteration's scores and information (pprof_py). */
  val WeightFloor: Double = 1e-20

  /** π is clipped to [1e-10, 1 − 1e-10] for the variances (pprof_py's `_estimate_variances`). */
  val VarianceClip: Double = 1e-10

  /** One block's rows, grouped by provider; `cluster(r)` ranks row r's cluster key in the block (empty
    * without a cluster column; Phase 2b §5).
    */
  final case class Rows(
      starts: Array[Int],
      y: Array[Double],
      n: Array[Double],
      x: Array[Double],
      p: Int,
      cluster: Array[Int] = Array.emptyIntArray
  ) {
    def providers: Int = starts.length
    def rows: Int = y.length
    def end(k: Int): Int = if (k + 1 < starts.length) starts(k + 1) else y.length
  }

  /** The block's share of the Schur system: S packed, r = g_β − Σ bⱼgⱼ/hⱼ, Σ gⱼ²/hⱼ and ℓ. */
  final case class Schur(
      packed: Array[Double],
      rhs: Array[Double],
      scoreTerm: Double,
      loglik: Double
  )

  /** Δγ for each provider of the block, and ℓ at each trial step length. */
  final case class Trial(deltaGamma: Array[Double], loglik: Array[Double])

  /** The uncentered terms of §2, for function-level parity: ℓ, gⱼ, hⱼ, bⱼ (provider-major), g_β
    * and C packed.
    */
  final case class Terms(
      loglik: Double,
      scoreGamma: Array[Double],
      infoGamma: Array[Double],
      crossInfo: Array[Double],
      scoreBeta: Array[Double],
      infoBeta: Array[Double]
  )

  /** Per provider of the block: Var(γ̂ⱼ) and Var(γ̂ⱼ + x̄ᵀβ̂). */
  final case class ProviderVariances(varGamma: Array[Double], varCaseMix: Array[Double])

  def sigmoid(eta: Double): Double = 1.0 / (1.0 + StrictMath.exp(-eta))

  /** log(1 + e^η) without overflow, as numpy's `logaddexp(0, η)`. */
  def log1pExp(eta: Double): Double =
    if (eta > 0.0) eta + StrictMath.log1p(StrictMath.exp(-eta))
    else StrictMath.log1p(StrictMath.exp(eta))

  /** η = γ + xᵀβ for row `r`. */
  def eta(rows: Rows, r: Int, gamma: Double, beta: Array[Double]): Double = {
    var dot = 0.0
    var j = 0
    val base = r * rows.p
    while (j < rows.p) {
      dot += rows.x(base + j) * beta(j)
      j += 1
    }
    gamma + dot
  }

  /** The row's log-likelihood term, y·η − n·log(1 + e^η). */
  def rowLoglik(y: Double, n: Double, eta: Double): Double = y * eta - n * log1pExp(eta)

  /** Pass A of the specification (§6, steps 1 and 2) at (γ, β); `gamma` holds the block's providers. */
  def schur(rows: Rows, gamma: Array[Double], beta: Array[Double]): Schur = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val s = new Accumulator(Moments.packedLength(p))
    val r = new Accumulator(p)
    val scoreTerm = new NeumaierSum
    val loglik = new NeumaierSum
    val provider = new ProviderSums(p)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = true, loglik)
      scoreTerm.add(provider.g * provider.g / provider.h)
      centeredInto(rows, k, provider.xbar, pi, q, s, r)
      k += 1
    }
    Schur(s.values, r.values, scoreTerm.value, loglik.value)
  }

  /** Pass B (§6, steps 3 and 5): Δγⱼ = gⱼ/hⱼ − x̄ⱼᵀΔβ for the block's providers and ℓ at
    * (γ + vΔγ, β + vΔβ) for each step length v.
    */
  def trial(
      rows: Rows,
      gamma: Array[Double],
      beta: Array[Double],
      deltaBeta: Array[Double],
      steps: Array[Double]
  ): Trial = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    val deltaGamma = new Array[Double](rows.providers)
    val betas = steps.map(v => Array.tabulate(p)(j => beta(j) + v * deltaBeta(j)))
    val logliks = Array.fill(steps.length)(new NeumaierSum)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = true, unused)
      var shift = 0.0
      var j = 0
      while (j < p) {
        shift += provider.xbar(j) * deltaBeta(j)
        j += 1
      }
      deltaGamma(k) = provider.g / provider.h - shift
      var t = 0
      while (t < steps.length) {
        val gammaT = gamma(k) + steps(t) * deltaGamma(k)
        var row = rows.starts(k)
        while (row < rows.end(k)) {
          logliks(t).add(rowLoglik(rows.y(row), rows.n(row), eta(rows, row, gammaT, betas(t))))
          row += 1
        }
        t += 1
      }
      k += 1
    }
    Trial(deltaGamma, logliks.map(_.value))
  }

  /** ℓ at (γ, β) over the block. */
  def loglik(rows: Rows, gamma: Array[Double], beta: Array[Double]): Double = {
    val sum = new NeumaierSum
    var k = 0
    while (k < rows.providers) {
      var r = rows.starts(k)
      while (r < rows.end(k)) {
        sum.add(rowLoglik(rows.y(r), rows.n(r), eta(rows, r, gamma(k), beta)))
        r += 1
      }
      k += 1
    }
    sum.value
  }

  /** The uncentered terms of §2 at (γ, β), with the iteration's weight floor. */
  def terms(rows: Rows, gamma: Array[Double], beta: Array[Double]): Terms = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val loglik = new NeumaierSum
    val provider = new ProviderSums(p)
    val scoreGamma = new Array[Double](rows.providers)
    val infoGamma = new Array[Double](rows.providers)
    val cross = new Array[Double](rows.providers * p)
    val scoreBeta = new Accumulator(p)
    val infoBeta = new Accumulator(Moments.packedLength(p))
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = true, loglik)
      scoreGamma(k) = provider.g
      infoGamma(k) = provider.h
      System.arraycopy(provider.b, 0, cross, k * p, p)
      var r = rows.starts(k)
      while (r < rows.end(k)) {
        val residual = rows.y(r) - rows.n(r) * pi(r)
        var i = 0
        var index = 0
        while (i < p) {
          val xi = rows.x(r * p + i)
          scoreBeta.add(i, xi * residual)
          var j = i
          while (j < p) {
            infoBeta.add(index, q(r) * xi * rows.x(r * p + j))
            index += 1
            j += 1
          }
          i += 1
        }
        r += 1
      }
      k += 1
    }
    Terms(loglik.value, scoreGamma, infoGamma, cross, scoreBeta.values, infoBeta.values)
  }

  /** S at the estimates with π clipped and no floor, for Var(β̂) = S⁻¹ (§5, Variances). */
  def varianceSchur(rows: Rows, gamma: Array[Double], beta: Array[Double]): Array[Double] = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val s = new Accumulator(Moments.packedLength(p))
    val r = new Accumulator(p)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = false, unused)
      centeredInto(rows, k, provider.xbar, pi, q, s, r)
      k += 1
    }
    s.values
  }

  /** Var(γ̂ⱼ) = 1/hⱼ + x̄ⱼᵀVx̄ⱼ and Var(γ̂ⱼ + x̄ᵀβ̂) = 1/hⱼ + (x̄ⱼ − x̄)ᵀV(x̄ⱼ − x̄), with π clipped,
    * V = Var(β̂) packed and x̄ the trials-weighted mean covariate row of the fitted rows.
    */
  def providerVariances(
      rows: Rows,
      gamma: Array[Double],
      beta: Array[Double],
      covariance: Array[Double],
      xbar: Array[Double]
  ): ProviderVariances = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    val varGamma = new Array[Double](rows.providers)
    val varCaseMix = new Array[Double](rows.providers)
    val centered = new Array[Double](p)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = false, unused)
      var j = 0
      while (j < p) {
        centered(j) = provider.xbar(j) - xbar(j)
        j += 1
      }
      varGamma(k) = 1.0 / provider.h + quadratic(covariance, provider.xbar, p)
      varCaseMix(k) = 1.0 / provider.h + quadratic(covariance, centered, p)
      k += 1
    }
    ProviderVariances(varGamma, varCaseMix)
  }

  /** Below this information a provider gets NaN robust variances and no share of the meat (pprof_py). */
  val RobustFloor: Double = 1e-14

  /** The block's share of the robust meat M_eff (packed) and its number of clusters (Phase 2b §2). */
  final case class Meat(packed: Array[Double], clusters: Long)

  /** Per provider of the block: Var_rob(γ̂ⱼ + x̄ᵀβ̂) and A0ⱼ/Iⱼ² (Phase 2b §2). */
  final case class RobustVariances(caseMix: Array[Double], fixedBeta: Array[Double])

  /** The score test's terms at clipped π: the centred S (packed) and the uncentred score Σ x e. */
  final case class ScoreTerms(packed: Array[Double], score: Array[Double])

  /** The fitted probability of every row, unclipped (AUC, prediction). */
  def fitted(rows: Rows, gamma: Array[Double], beta: Array[Double]): Array[Double] = {
    val out = new Array[Double](rows.rows)
    var k = 0
    while (k < rows.providers) {
      var r = rows.starts(k)
      while (r < rows.end(k)) {
        out(r) = sigmoid(eta(rows, r, gamma(k), beta))
        r += 1
      }
      k += 1
    }
    out
  }

  /** Pass R1 at the estimates, π clipped: Σ over clusters of ũₖũₖᵀ, ũₖ = Σᵢ∈ₖ (xᵢ − x̄ⱼ)eᵢ. */
  def robustMeat(rows: Rows, gamma: Array[Double], beta: Array[Double]): Meat = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    val meat = new Accumulator(Moments.packedLength(p))
    val u = new Array[Double](p)
    var clusters = 0L
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = false, unused)
      if (provider.h >= RobustFloor) {
        val order = byCluster(rows, k)
        var i = 0
        while (i < order.length) {
          java.util.Arrays.fill(u, 0.0)
          val cluster = rows.cluster(order(i))
          while (i < order.length && rows.cluster(order(i)) == cluster) {
            val r = order(i)
            val e = rows.y(r) - rows.n(r) * pi(r)
            var j = 0
            while (j < p) {
              u(j) += (rows.x(r * p + j) - provider.xbar(j)) * e
              j += 1
            }
            i += 1
          }
          addOuter(meat, u, p)
          clusters += 1L
        }
      }
      k += 1
    }
    Meat(meat.values, clusters)
  }

  /** Pass R2: per provider Var_rob(γ̂ⱼ + x̄ᵀβ̂) and A0ⱼ/Iⱼ², given M_eff, S⁻¹ = Var(β̂) (both packed) and
    * the average case mix x̄ (Phase 2b §2).
    */
  def robustProviders(
      rows: Rows,
      gamma: Array[Double],
      beta: Array[Double],
      meat: Array[Double],
      covariance: Array[Double],
      xbar: Array[Double]
  ): RobustVariances = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    val caseMix = new Array[Double](rows.providers)
    val fixedBeta = new Array[Double](rows.providers)
    val a1 = new Array[Double](p)
    val clusterScore = new Array[Double](p)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = false, unused)
      val h = provider.h
      if (h < RobustFloor) {
        caseMix(k) = Double.NaN
        fixedBeta(k) = Double.NaN
      } else {
        var a0 = 0.0
        java.util.Arrays.fill(a1, 0.0)
        val order = byCluster(rows, k)
        var i = 0
        while (i < order.length) {
          var residual = 0.0
          java.util.Arrays.fill(clusterScore, 0.0)
          val cluster = rows.cluster(order(i))
          while (i < order.length && rows.cluster(order(i)) == cluster) {
            val r = order(i)
            val e = rows.y(r) - rows.n(r) * pi(r)
            residual += e
            var j = 0
            while (j < p) {
              clusterScore(j) += rows.x(r * p + j) * e
              j += 1
            }
            i += 1
          }
          a0 += residual * residual
          var j = 0
          while (j < p) {
            a1(j) += residual * clusterScore(j)
            j += 1
          }
        }
        val d = Array.tabulate(p)(j => provider.xbar(j) - xbar(j))
        val hv = packedTimes(covariance, d, p)
        var xh = 0.0
        var a1h = 0.0
        var j = 0
        while (j < p) {
          xh += provider.xbar(j) * hv(j)
          a1h += a1(j) * hv(j)
          j += 1
        }
        caseMix(k) = a0 / (h * h) + 2.0 * (a0 / h) * xh + quadratic(meat, hv, p) - 2.0 * a1h / h
        fixedBeta(k) = a0 / (h * h)
      }
      k += 1
    }
    RobustVariances(caseMix, fixedBeta)
  }

  /** At clipped π: the centred Schur complement over all p features and the uncentred score Σ xᵢeᵢ;
    * the score test reads entry j of both at the fit without feature j (Phase 2b §2).
    */
  def scoreTerms(rows: Rows, gamma: Array[Double], beta: Array[Double]): ScoreTerms = {
    val p = rows.p
    val pi = new Array[Double](rows.rows)
    val q = new Array[Double](rows.rows)
    val s = new Accumulator(Moments.packedLength(p))
    val r = new Accumulator(p)
    val score = new Accumulator(p)
    val unused = new NeumaierSum
    val provider = new ProviderSums(p)
    var k = 0
    while (k < rows.providers) {
      provider.accumulate(rows, k, gamma(k), beta, pi, q, floored = false, unused)
      centeredInto(rows, k, provider.xbar, pi, q, s, r)
      var row = rows.starts(k)
      while (row < rows.end(k)) {
        val e = rows.y(row) - rows.n(row) * pi(row)
        var j = 0
        while (j < p) {
          score.add(j, rows.x(row * p + j) * e)
          j += 1
        }
        row += 1
      }
      k += 1
    }
    ScoreTerms(s.values, score.values)
  }

  /** A M A for symmetric A and M packed row by row; the result packed (the robust Var(β̂)). */
  def sandwich(bread: Array[Double], meat: Array[Double], p: Int): Array[Double] = {
    val a = unpack(bread, p)
    val m = unpack(meat, p)
    val am =
      Array.tabulate(p, p)((i, j) => (0 until p).foldLeft(0.0)((acc, l) => acc + a(i)(l) * m(l)(j)))
    val out = new Array[Double](Moments.packedLength(p))
    var index = 0
    var i = 0
    while (i < p) {
      var j = i
      while (j < p) {
        out(index) = (0 until p).foldLeft(0.0)((acc, l) => acc + am(i)(l) * a(l)(j))
        index += 1
        j += 1
      }
      i += 1
    }
    out
  }

  /** Mv for M symmetric, packed row by row. */
  def packedTimes(packed: Array[Double], v: Array[Double], p: Int): Array[Double] = {
    val m = unpack(packed, p)
    Array.tabulate(p)(i => (0 until p).foldLeft(0.0)((acc, j) => acc + m(i)(j) * v(j)))
  }

  private def unpack(packed: Array[Double], p: Int): Array[Array[Double]] = {
    val m = Array.ofDim[Double](p, p)
    var index = 0
    var i = 0
    while (i < p) {
      var j = i
      while (j < p) {
        m(i)(j) = packed(index)
        m(j)(i) = packed(index)
        index += 1
        j += 1
      }
      i += 1
    }
    m
  }

  private def addOuter(acc: Accumulator, u: Array[Double], p: Int): Unit = {
    var i = 0
    var index = 0
    while (i < p) {
      var j = i
      while (j < p) {
        acc.add(index, u(i) * u(j))
        index += 1
        j += 1
      }
      i += 1
    }
  }

  /** Provider k's rows by cluster: clusters by rank, rows in canonical order within (a stable sort). */
  private def byCluster(rows: Rows, k: Int): Array[Int] = {
    require(rows.cluster.length == rows.rows, "robust variances need every row's cluster")
    (rows.starts(k) until rows.end(k)).toArray.sortBy(r => rows.cluster(r))
  }

  /** vᵀMv for M symmetric, packed row by row. */
  def quadratic(packed: Array[Double], v: Array[Double], p: Int): Double = {
    var sum = 0.0
    var i = 0
    var index = 0
    while (i < p) {
      var j = i
      while (j < p) {
        val term = packed(index) * v(i) * v(j)
        sum += (if (i == j) term else 2.0 * term)
        index += 1
        j += 1
      }
      i += 1
    }
    sum
  }

  /** Adds Σ q(x − x̄)(x − x̄)ᵀ to `s` and Σ (x − x̄)(y − nπ) to `r` over provider k's rows. */
  private def centeredInto(
      rows: Rows,
      k: Int,
      xbar: Array[Double],
      pi: Array[Double],
      q: Array[Double],
      s: Accumulator,
      r: Accumulator
  ): Unit = {
    val p = rows.p
    val d = new Array[Double](p)
    var row = rows.starts(k)
    while (row < rows.end(k)) {
      var j = 0
      while (j < p) {
        d(j) = rows.x(row * p + j) - xbar(j)
        j += 1
      }
      val residual = rows.y(row) - rows.n(row) * pi(row)
      var i = 0
      var index = 0
      while (i < p) {
        r.add(i, d(i) * residual)
        j = i
        while (j < p) {
          s.add(index, q(row) * d(i) * d(j))
          index += 1
          j += 1
        }
        i += 1
      }
      row += 1
    }
  }

  /** gⱼ, hⱼ, bⱼ and x̄ⱼ = bⱼ/hⱼ of one provider; also fills π and the weights of its rows and adds
    * their ℓ terms to `loglik`.
    */
  private final class ProviderSums(p: Int) {
    private val gSum = new NeumaierSum
    private val hSum = new NeumaierSum
    private val bSum = new Accumulator(p)
    val b: Array[Double] = new Array[Double](p)
    val xbar: Array[Double] = new Array[Double](p)
    var g: Double = 0.0
    var h: Double = 0.0

    def accumulate(
        rows: Rows,
        k: Int,
        gamma: Double,
        beta: Array[Double],
        pi: Array[Double],
        q: Array[Double],
        floored: Boolean,
        loglik: NeumaierSum
    ): Unit = {
      gSum.reset()
      hSum.reset()
      bSum.reset()
      var r = rows.starts(k)
      while (r < rows.end(k)) {
        val e = eta(rows, r, gamma, beta)
        val prob =
          if (floored) sigmoid(e)
          else math.min(math.max(sigmoid(e), VarianceClip), 1.0 - VarianceClip)
        val raw = rows.n(r) * prob * (1.0 - prob)
        val weight = if (floored) math.max(raw, WeightFloor) else raw
        pi(r) = prob
        q(r) = weight
        gSum.add(rows.y(r) - rows.n(r) * prob)
        hSum.add(weight)
        loglik.add(rowLoglik(rows.y(r), rows.n(r), e))
        var j = 0
        while (j < p) {
          bSum.add(j, weight * rows.x(r * p + j))
          j += 1
        }
        r += 1
      }
      g = gSum.value
      h = hSum.value
      var j = 0
      while (j < p) {
        b(j) = bSum.value(j)
        xbar(j) = b(j) / h
        j += 1
      }
    }
  }

  /** A resettable vector of Neumaier sums. */
  private final class Accumulator(length: Int) {
    private val sums = new Array[Double](length)
    private val compensations = new Array[Double](length)

    def add(i: Int, x: Double): Unit = {
      val s = sums(i)
      val t = s + x
      if (java.lang.Double.isFinite(t)) {
        if (math.abs(s) >= math.abs(x)) compensations(i) += (s - t) + x
        else compensations(i) += (x - t) + s
      }
      sums(i) = t
    }

    def value(i: Int): Double =
      if (java.lang.Double.isFinite(sums(i))) sums(i) + compensations(i) else sums(i)

    def values: Array[Double] = Array.tabulate(length)(value)

    def reset(): Unit = {
      java.util.Arrays.fill(sums, 0.0)
      java.util.Arrays.fill(compensations, 0.0)
    }
  }
}
