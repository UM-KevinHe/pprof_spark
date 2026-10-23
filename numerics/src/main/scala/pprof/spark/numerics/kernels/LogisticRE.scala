package pprof.spark.numerics.kernels

import pprof.spark.numerics.{BoundedQuasiNewton, Cholesky, NeumaierSum, NeumaierVector, Summation}

/** Kernels of the logistic random-intercept model (docs/spec/logistic/random-effect-estimation.md §2 and §3). A
  * block's rows are grouped by provider in canonical order; `values` holds each row's p features (the intercept's
  * constant first when the model has one), then its offset, then its outcome y ∈ [0, 1], row-major. Features enter
  * centred at `centre` (0 for the intercept), which moves only the intercept. Pure and deterministic: sums run in row
  * and provider order with Neumaier's compensation, and every provider's mode is found from 0 at every evaluation.
  */
object LogisticRE {

  final case class Rows(groupStart: Array[Int], values: Array[Double], p: Int) {
    require(p >= 0, s"the number of features must be non-negative, got $p")
    require(values.length % (p + 2) == 0, s"${values.length} values are not rows of ${p + 2}")
    def count: Int = values.length / (p + 2)
    def groups: Int = groupStart.length
    def until(g: Int): Int = if (g + 1 < groups) groupStart(g + 1) else count
  }

  private def log1pExp(t: Double): Double =
    if (t > 0.0) t + StrictMath.log1p(StrictMath.exp(-t)) else StrictMath.log1p(StrictMath.exp(t))

  private def expit(t: Double): Double =
    if (t >= 0.0) 1.0 / (1.0 + StrictMath.exp(-t))
    else {
      val e = StrictMath.exp(t)
      e / (1.0 + e)
    }

  /** oᵢ + (xᵢ − c)ᵀβ for every row of provider g. */
  private def fixedParts(
      rows: Rows,
      g: Int,
      beta: Array[Double],
      centre: Array[Double]
  ): Array[Double] = {
    val p = rows.p
    val q = p + 2
    val from = rows.groupStart(g)
    Array.tabulate(rows.until(g) - from) { k =>
      val r = from + k
      val sum = new NeumaierSum
      sum.add(rows.values(r * q + p))
      var j = 0
      while (j < p) {
        sum.add((rows.values(r * q + j) - centre(j)) * beta(j))
        j += 1
      }
      sum.value
    }
  }

  /** Provider g's mode u* at (σ, β): the root of σΣ(y − μ) − u, decreasing in u, by Newton's method from 0 kept inside
    * the bracket where the function changes sign (§3), to |Δu| ≤ 10⁻¹²(1 + |u|).
    */
  private def mode(rows: Rows, g: Int, sigma: Double, eta0: Array[Double]): Double =
    if (sigma == 0.0) 0.0
    else {
      val p = rows.p
      val q = p + 2
      val from = rows.groupStart(g)
      def score(u: Double): (Double, Double) = {
        val residual = new NeumaierSum
        val weight = new NeumaierSum
        var k = 0
        while (k < eta0.length) {
          val mu = expit(eta0(k) + sigma * u)
          residual.add(rows.values((from + k) * q + p + 1) - mu)
          weight.add(mu * (1.0 - mu))
          k += 1
        }
        (sigma * residual.value - u, 1.0 + sigma * sigma * weight.value)
      }
      val (g0, _) = score(0.0)
      val reach = math.abs(sigma) * eta0.length + 1.0
      var lower = if (g0 > 0.0) 0.0 else -reach
      var upper = if (g0 > 0.0) reach else 0.0
      var u = 0.0
      var iterations = 0
      var done = g0 == 0.0
      while (!done && iterations < 200) {
        val (value, slope) = score(u)
        if (value == 0.0) done = true // at the root: the bracket must not move it
        else {
          if (value > 0.0) lower = u else upper = u
          var next = u + value / slope
          if (!(next > lower && next < upper)) next = 0.5 * (lower + upper)
          done = math.abs(next - u) <= 1e-12 * (1.0 + math.abs(u))
          u = next
        }
        iterations += 1
      }
      u
    }

  /** A block's shares of the Laplace deviance D(σ, β) and of its gradient (§2, §2.1). */
  final case class LaplaceSums(deviance: Double, gradient: Array[Double])

  def laplace(
      rows: Rows,
      sigma: Double,
      beta: Array[Double],
      centre: Array[Double]
  ): LaplaceSums = {
    val p = rows.p
    val q = p + 2
    val deviance = new NeumaierSum
    val gradient = new NeumaierVector(p + 1)
    var g = 0
    while (g < rows.groups) {
      val eta0 = fixedParts(rows, g, beta, centre)
      val u = mode(rows, g, sigma, eta0)
      val loglik = new NeumaierSum
      val sr = new NeumaierSum
      val w = new NeumaierSum
      val v = new NeumaierSum
      val srx = new NeumaierVector(p)
      val swx = new NeumaierVector(p)
      val svx = new NeumaierVector(p)
      val from = rows.groupStart(g)
      var k = 0
      while (k < eta0.length) {
        val r = from + k
        val eta = eta0(k) + sigma * u
        val y = rows.values(r * q + p + 1)
        val mu = expit(eta)
        val wi = mu * (1.0 - mu)
        val ri = y - mu
        val vi = wi * (1.0 - 2.0 * mu)
        loglik.add(y * eta - log1pExp(eta))
        sr.add(ri)
        w.add(wi)
        v.add(vi)
        var j = 0
        while (j < p) {
          val x = rows.values(r * q + j) - centre(j)
          srx.addAt(j, ri * x)
          swx.addAt(j, wi * x)
          svx.addAt(j, vi * x)
          j += 1
        }
        k += 1
      }
      val h = 1.0 + sigma * sigma * w.value
      deviance.add(-2.0 * loglik.value + u * u + StrictMath.log(h))
      val duSigma = (sr.value - sigma * u * w.value) / h
      gradient.addAt(
        0,
        -2.0 * u * sr.value + (2.0 * sigma * w.value + sigma * sigma * v.value * (u + sigma * duSigma)) / h
      )
      var j = 0
      while (j < p) {
        val duBeta = -sigma * swx.valueAt(j) / h
        gradient.addAt(
          j + 1,
          -2.0 * srx.valueAt(j) + sigma * sigma * (svx.valueAt(j) + sigma * v.value * duBeta) / h
        )
        j += 1
      }
      g += 1
    }
    LaplaceSums(deviance.value, gradient.values)
  }

  def combineLaplace(parts: Seq[LaplaceSums]): LaplaceSums =
    if (parts.length == 1) parts.head
    else
      LaplaceSums(
        Summation.pairwise(parts.map(_.deviance).toArray),
        Array.tabulate(parts.head.gradient.length)(i =>
          Summation.pairwise(parts.map(_.gradient(i)).toArray)
        )
      )

  /** A block's sums for the nAGQ = 0 Newton step at (σ, β, u) (§3): per provider Wⱼ, Σ wx and Σ r; for the block
    * X′WX (packed), Σ rx, the penalized deviance Σⱼ(−2Σℓ + uⱼ²) and Σⱼ log hⱼ.
    */
  final case class Stage1Sums(
      weights: Array[Double],
      wx: Array[Double],
      residuals: Array[Double],
      xwx: Array[Double],
      rx: Array[Double],
      penalized: Double,
      logH: Double
  )

  def stage1(
      rows: Rows,
      sigma: Double,
      beta: Array[Double],
      u: Array[Double],
      centre: Array[Double]
  ): Stage1Sums = {
    val p = rows.p
    val q = p + 2
    val m = rows.groups
    val weights = new Array[Double](m)
    val wx = new Array[Double](m * p)
    val residuals = new Array[Double](m)
    val xwx = new NeumaierVector(Moments.packedLength(p))
    val rx = new NeumaierVector(p)
    val penalized = new NeumaierSum
    val logH = new NeumaierSum
    val x = new Array[Double](p)
    var g = 0
    while (g < m) {
      val eta0 = fixedParts(rows, g, beta, centre)
      val loglik = new NeumaierSum
      val w = new NeumaierSum
      val sr = new NeumaierSum
      val swx = new NeumaierVector(p)
      val from = rows.groupStart(g)
      var k = 0
      while (k < eta0.length) {
        val r = from + k
        val eta = eta0(k) + sigma * u(g)
        val y = rows.values(r * q + p + 1)
        val mu = expit(eta)
        val wi = mu * (1.0 - mu)
        loglik.add(y * eta - log1pExp(eta))
        w.add(wi)
        sr.add(y - mu)
        var j = 0
        while (j < p) {
          x(j) = rows.values(r * q + j) - centre(j)
          j += 1
        }
        j = 0
        while (j < p) {
          swx.addAt(j, wi * x(j))
          rx.addAt(j, (y - mu) * x(j))
          var l = j
          while (l < p) {
            xwx.addAt(Moments.packedIndex(j, l, p), wi * x(j) * x(l))
            l += 1
          }
          j += 1
        }
        k += 1
      }
      weights(g) = w.value
      residuals(g) = sr.value
      System.arraycopy(swx.values, 0, wx, g * p, p)
      penalized.add(-2.0 * loglik.value + u(g) * u(g))
      logH.add(StrictMath.log1p(sigma * sigma * w.value))
      g += 1
    }
    Stage1Sums(weights, wx, residuals, xwx.values, rx.values, penalized.value, logH.value)
  }

  /** Each provider's mode and Wⱼ at (σ, β), for the estimates (§4). */
  def modes(
      rows: Rows,
      sigma: Double,
      beta: Array[Double],
      centre: Array[Double]
  ): (Array[Double], Array[Double]) = {
    val u = new Array[Double](rows.groups)
    val w = new Array[Double](rows.groups)
    var g = 0
    while (g < rows.groups) {
      val eta0 = fixedParts(rows, g, beta, centre)
      u(g) = mode(rows, g, sigma, eta0)
      val weight = new NeumaierSum
      var k = 0
      while (k < eta0.length) {
        val mu = expit(eta0(k) + sigma * u(g))
        weight.add(mu * (1.0 - mu))
        k += 1
      }
      w(g) = weight.value
      g += 1
    }
    (u, w)
  }

  /** How a fit's blocks are evaluated: each block's sums where its rows are, combined in block order (§7). */
  trait Blocks {
    def laplace(sigma: Double, beta: Array[Double]): LaplaceSums

    /** The nAGQ = 0 sums with `u` in providers' key order; per-provider values come back in that order. */
    def stage1(sigma: Double, beta: Array[Double], u: Array[Double]): Stage1Sums

    def modes(sigma: Double, beta: Array[Double]): (Array[Double], Array[Double])
  }

  /** Blocks held on the driver, in block order; each holds whole providers, in key order. */
  final class LocalBlocks(blocks: Seq[Rows], p: Int, centre: Array[Double]) extends Blocks {
    private val offsets = blocks.scanLeft(0)(_ + _.groups).toArray

    def laplace(sigma: Double, beta: Array[Double]): LaplaceSums =
      combineLaplace(blocks.map(b => LogisticRE.laplace(b, sigma, beta, centre)))

    def stage1(sigma: Double, beta: Array[Double], u: Array[Double]): Stage1Sums =
      combineStage1(
        blocks.indices.map { i =>
          LogisticRE.stage1(blocks(i), sigma, beta, u.slice(offsets(i), offsets(i + 1)), centre)
        },
        p
      )

    def modes(sigma: Double, beta: Array[Double]): (Array[Double], Array[Double]) = {
      val parts = blocks.map(b => LogisticRE.modes(b, sigma, beta, centre))
      (parts.flatMap(_._1).toArray, parts.flatMap(_._2).toArray)
    }
  }

  /** Blocks' nAGQ = 0 sums: per-provider values concatenated in block order, totals added pairwise. */
  def combineStage1(parts: Seq[Stage1Sums], p: Int): Stage1Sums =
    if (parts.length == 1) parts.head
    else
      Stage1Sums(
        parts.flatMap(_.weights).toArray,
        parts.flatMap(_.wx).toArray,
        parts.flatMap(_.residuals).toArray,
        Array.tabulate(Moments.packedLength(p))(i =>
          Summation.pairwise(parts.map(_.xwx(i)).toArray)
        ),
        Array.tabulate(p)(i => Summation.pairwise(parts.map(_.rx(i)).toArray)),
        Summation.pairwise(parts.map(_.penalized).toArray),
        Summation.pairwise(parts.map(_.logH).toArray)
      )

  /** The Schur complement S = X′WX − Σⱼ bⱼbⱼᵀ/hⱼ with bⱼ = σΣ wx, packed: the information on β given the modes. */
  private def schur(s: Stage1Sums, sigma: Double, p: Int): Array[Double] = {
    val out = s.xwx.clone()
    val m = s.weights.length
    var g = 0
    while (g < m) {
      val h = 1.0 + sigma * sigma * s.weights(g)
      var j = 0
      while (j < p) {
        var l = j
        while (l < p) {
          out(Moments.packedIndex(j, l, p)) -= sigma * sigma * s.wx(g * p + j) * s.wx(g * p + l) / h
          l += 1
        }
        j += 1
      }
      g += 1
    }
    out
  }

  /** The nAGQ = 0 solution at σ (§3): Newton over (β, u) through the Schur complement with step halving, to a
    * relative change of 10⁻¹² in the penalized deviance; returns (β, u, D₀(σ), iterations).
    */
  def stage1Solve(
      blocks: Blocks,
      p: Int,
      sigma: Double,
      beta0: Array[Double],
      u0: Array[Double],
      maxIterations: Int = 100
  ): (Array[Double], Array[Double], Double, Int) = {
    var beta = beta0.clone()
    var u = u0.clone()
    var s = blocks.stage1(sigma, beta, u)
    var iterations = 0
    var done = false
    while (!done && iterations < maxIterations) {
      val m = u.length
      val h = Array.tabulate(m)(g => 1.0 + sigma * sigma * s.weights(g))
      val gu = Array.tabulate(m)(g => sigma * s.residuals(g) - u(g))
      val rhs = Array.tabulate(p) { j =>
        val sum = new NeumaierSum
        sum.add(s.rx(j))
        var g = 0
        while (g < m) {
          sum.add(-sigma * s.wx(g * p + j) * gu(g) / h(g))
          g += 1
        }
        sum.value
      }
      val step =
        if (p == 0) new Array[Double](0) else Cholesky.factor(schur(s, sigma, p), p).solve(rhs)
      val du = Array.tabulate(m) { g =>
        var b = 0.0
        var j = 0
        while (j < p) {
          b += sigma * s.wx(g * p + j) * step(j)
          j += 1
        }
        (gu(g) - b) / h(g)
      }
      var t = 1.0
      var accepted = false
      var tries = 0
      while (!accepted && tries < 30) {
        val trialBeta = Array.tabulate(p)(j => beta(j) + t * step(j))
        val trialU = Array.tabulate(m)(g => u(g) + t * du(g))
        val trial = blocks.stage1(sigma, trialBeta, trialU)
        if (trial.penalized <= s.penalized + 1e-10 * math.abs(s.penalized)) {
          val change =
            math.abs(s.penalized - trial.penalized) / math.max(math.abs(trial.penalized), 1e-12)
          beta = trialBeta
          u = trialU
          s = trial
          accepted = true
          done = change < 1e-12
        } else {
          t *= 0.5
          tries += 1
        }
      }
      if (!accepted) done = true
      iterations += 1
    }
    (beta, u, s.penalized + s.logH, iterations)
  }

  /** Brent's minimization of f on [a, b] to `tolerance` (golden sections and parabolic steps). */
  private def minimizeScalar(
      f: Double => Double,
      a0: Double,
      b0: Double,
      tolerance: Double
  ): (Double, Double) = {
    val golden = 0.3819660112501051
    var a = a0
    var b = b0
    var x = a + golden * (b - a)
    var w = x
    var v = x
    var fx = f(x)
    var fw = fx
    var fv = fx
    var d = 0.0
    var e = 0.0
    var iterations = 0
    var done = false
    while (!done && iterations < 200) {
      val middle = 0.5 * (a + b)
      val tol1 = tolerance * math.abs(x) + 1e-15
      val tol2 = 2.0 * tol1
      if (math.abs(x - middle) <= tol2 - 0.5 * (b - a)) done = true
      else {
        var golden_ = true
        if (math.abs(e) > tol1) {
          val r = (x - w) * (fx - fv)
          var q = (x - v) * (fx - fw)
          var p = (x - v) * q - (x - w) * r
          q = 2.0 * (q - r)
          if (q > 0.0) p = -p else q = -q
          if (math.abs(p) < math.abs(0.5 * q * e) && p > q * (a - x) && p < q * (b - x)) {
            e = d
            d = p / q
            val trial = x + d
            if (trial - a < tol2 || b - trial < tol2) d = if (middle >= x) tol1 else -tol1
            golden_ = false
          }
        }
        if (golden_) {
          e = if (x >= middle) a - x else b - x
          d = golden * e
        }
        val trial = if (math.abs(d) >= tol1) x + d else x + (if (d > 0.0) tol1 else -tol1)
        val ft = f(trial)
        if (ft <= fx) {
          if (trial >= x) a = x else b = x
          v = w
          fv = fw
          w = x
          fw = fx
          x = trial
          fx = ft
        } else {
          if (trial < x) a = trial else b = trial
          if (ft <= fw || w == x) {
            v = w
            fv = fw
            w = trial
            fw = ft
          } else if (ft <= fv || v == x || v == w) {
            v = trial
            fv = ft
          }
        }
        iterations += 1
      }
    }
    (x, fx)
  }

  /** The fit at the estimate (§4), in the original coordinates. */
  final case class Fit(
      sigma: Double,
      beta: Array[Double],
      covariance: Array[Double],
      modes: Array[Double],
      blups: Array[Double],
      weights: Array[Double],
      conditionalVariances: Array[Double],
      deviance: Double,
      logLikelihood: Double,
      aic: Double,
      bic: Double,
      pwrss: Double,
      logDetH: Double,
      ussq: Double,
      nagq: Int,
      converged: Boolean,
      projectedGradient: Double,
      stage1Evaluations: Int,
      stage2Evaluations: Int,
      boundary: Option[String],
      warnings: Vector[String]
  )

  /** Fits the model (§3): nAGQ = 0 by a grid and Brent's minimization over σ, then, when `nagq1`, the Laplace
    * optimum over (σ, β) by the bounded quasi-Newton with the analytic gradient; the estimates of §4.
    */
  def fit(
      blocks: Blocks,
      rows: Long,
      providers: Int,
      p: Int,
      intercept: Boolean,
      centre: Array[Double],
      outcomeMean: Double,
      nagq1: Boolean = true,
      sigmaUpper: Double = Double.PositiveInfinity,
      maxIterations: Int = 200
  ): Fit = {
    require(providers >= 2, s"the provider SD needs at least two providers, got $providers (X-040)")
    require(rows > p, s"the rows ($rows) must outnumber the fixed effects ($p)")
    require(sigmaUpper > 0.0, s"sigmaUpper must be positive, got $sigmaUpper")
    val warnings = Vector.newBuilder[String]
    val start = Array.tabulate(p) { j =>
      if (j == 0 && intercept) {
        val ybar = math.min(math.max(outcomeMean, 1e-6), 1.0 - 1e-6)
        StrictMath.log(ybar / (1.0 - ybar))
      } else 0.0
    }
    var beta = start
    var u = new Array[Double](providers)
    var stage1Evaluations = 0
    def d0(sigma: Double): Double = {
      val (b, uu, value, _) = stage1Solve(blocks, p, sigma, beta, u)
      beta = b
      u = uu
      stage1Evaluations += 1
      value
    }
    val grid = (0.0 +: (-4 to 2).map(k => StrictMath.pow(10.0, k.toDouble) / 4.0))
      .map(math.min(_, sigmaUpper))
      .distinct
    val values = grid.map(d0)
    val best = values.indices.minBy(values)
    val lowerBound = if (best == 0) 0.0 else grid(best - 1)
    val upperBound = if (best == grid.length - 1) grid(best) else grid(best + 1)
    val (sigma0, _) =
      if (upperBound > lowerBound) minimizeScalar(d0, lowerBound, upperBound, 1e-10)
      else (grid(best), values(best))
    val (beta0, u0, value0, _) = stage1Solve(blocks, p, sigma0, beta, u)
    stage1Evaluations += 1
    var sigma = sigma0
    beta = beta0
    u = u0
    var deviance = value0
    var converged = true
    var projected = 0.0
    var stage2Evaluations = 0
    if (nagq1) {
      val lower = Array.tabulate(p + 1)(i => if (i == 0) 0.0 else Double.NegativeInfinity)
      val result = BoundedQuasiNewton.minimizeWithGradient(
        x => {
          val s = blocks.laplace(math.min(x(0), sigmaUpper), x.drop(1))
          (s.deviance, s.gradient)
        },
        sigma +: beta,
        lower,
        BoundedQuasiNewton.Options(gradientTolerance = 1e-8, maxIterations = maxIterations)
      )
      stage2Evaluations = result.evaluations
      sigma = math.min(result.x(0), sigmaUpper)
      beta = result.x.drop(1)
      deviance = result.value
      converged = result.converged
      projected = result.projectedGradient
      if (!converged)
        warnings += "the Laplace optimum was not reached within the iteration cap; separation of the outcomes by " +
          "the features is a likely cause"
    }
    val boundary =
      if (sigma == 0.0) Some("lower") else if (sigma >= sigmaUpper) Some("upper") else None
    if (boundary.contains("lower")) warnings += "boundary fit: sigma = 0, so the BLUPs are 0"
    if (boundary.contains("upper")) warnings += s"boundary fit at sigmaUpper = $sigmaUpper"
    val (modes, weights) =
      if (nagq1) blocks.modes(sigma, beta)
      else {
        val s = blocks.stage1(sigma, beta, u)
        (u, s.weights)
      }
    val sums = blocks.stage1(sigma, beta, modes)
    val information = if (p == 0) None else Some(Cholesky.factor(schur(sums, sigma, p), p))
    val centred = information.fold(new Array[Double](0))(_.inversePacked)
    val covariance =
      if (!intercept || p == 0) centred
      else
        Array.tabulate(Moments.packedLength(p)) { index =>
          val (i, k) = (0 until p).flatMap(a => (a until p).map(b => (a, b)))(index)
          def at(a: Int, b: Int): Double =
            centred(Moments.packedIndex(math.min(a, b), math.max(a, b), p))
          def t(a: Int, b: Int): Double = if (a == b) 1.0 else if (a == 0) -centre(b) else 0.0
          val s = new NeumaierSum
          for (a <- 0 until p; b <- 0 until p) s.add(t(i, a) * at(a, b) * t(k, b))
          s.value
        }
    val original = beta.clone()
    if (intercept && p > 0) {
      var shift = 0.0
      var k = 1
      while (k < p) {
        shift += centre(k) * beta(k)
        k += 1
      }
      original(0) = beta(0) - shift
    }
    val k = (p + 1).toDouble
    val ussq = modes.map(v => v * v).sum
    Fit(
      sigma = sigma,
      beta = original,
      covariance = covariance,
      modes = modes,
      blups = modes.map(_ * sigma),
      weights = weights,
      conditionalVariances = weights.map(w => sigma * sigma / (1.0 + sigma * sigma * w)),
      deviance = deviance,
      logLikelihood = -0.5 * deviance,
      aic = deviance + 2.0 * k,
      bic = deviance + k * StrictMath.log(rows.toDouble),
      pwrss = sums.penalized,
      logDetH = sums.logH,
      ussq = ussq,
      nagq = if (nagq1) 1 else 0,
      converged = converged,
      projectedGradient = projected,
      stage1Evaluations = stage1Evaluations,
      stage2Evaluations = stage2Evaluations,
      boundary = boundary,
      warnings = warnings.result()
    )
  }
}
