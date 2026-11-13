package pprof.spark.numerics.kernels

import pprof.spark.numerics.{BoundedQuasiNewton, NeumaierSum}

/** The logistic random-effect model with crossed cluster factors (docs/spec/logistic/random-effect-clusters.md, slice
  * 4d-2): PIRLS for the spherical modes with the provider block eliminated onto the cluster levels (`CrossedSystem`),
  * lme4's Laplace and nAGQ = 0 deviances, and pprof_py's two-stage search, on rows held in memory.
  */
object LogisticREClusters {

  /** The rows: features (n × p, row-major), outcomes, offsets and each factor's level index (factor 0 the provider). */
  final case class Rows(
      n: Int,
      p: Int,
      x: Array[Double],
      y: Array[Double],
      offset: Array[Double],
      index: Array[Array[Int]],
      levels: Array[Int]
  ) {
    val factors: Int = levels.length
    val offsets: Array[Int] = levels.scanLeft(0)(_ + _)
    val q: Int = offsets.last

    /** For each factor pair (k < l): its distinct level pairs and each row's pair. */
    val pairs: Array[(Int, Int, Array[Int], Array[Int], Array[Int])] =
      (for (k <- 0 until factors; l <- k + 1 until factors) yield {
        val keys = scala.collection.mutable.TreeMap.empty[(Int, Int), Int]
        (0 until n).foreach(i => keys.getOrElseUpdate((index(k)(i), index(l)(i)), 0))
        val ordered = keys.keys.toArray
        val position = ordered.zipWithIndex.toMap
        (
          k,
          l,
          ordered.map(_._1),
          ordered.map(_._2),
          Array.tabulate(n)(i => position((index(k)(i), index(l)(i))))
        )
      }).toArray
  }

  final case class Modes(
      beta: Array[Double],
      u: Array[Double],
      eta: Array[Double],
      logLik: Double,
      logDetH: Double,
      deviance: Double
  )

  final case class Fit(
      sigma: Array[Double],
      beta: Array[Double],
      u: Array[Double],
      blups: Array[Double],
      posteriorVariance: Array[Double],
      covariance: Array[Double],
      deviance: Double,
      logLik: Double,
      stage1Deviance: Double,
      iterations: Int,
      evaluations: Int,
      projectedGradient: Double,
      converged: Boolean
  )

  private def log1pExp(t: Double): Double =
    if (t > 0.0) t + StrictMath.log1p(StrictMath.exp(-t)) else StrictMath.log1p(StrictMath.exp(t))
  private def expit(t: Double): Double =
    if (t >= 0.0) 1.0 / (1.0 + StrictMath.exp(-t)) else { val e = StrictMath.exp(t); e / (1.0 + e) }

  private def eta(
      rows: Rows,
      sigma: Array[Double],
      beta: Array[Double],
      u: Array[Double]
  ): Array[Double] =
    Array.tabulate(rows.n) { i =>
      val s = new NeumaierSum
      s.add(rows.offset(i))
      var j = 0
      while (j < rows.p) { s.add(rows.x(i * rows.p + j) * beta(j)); j += 1 }
      var k = 0
      while (k < rows.factors) { s.add(sigma(k) * u(rows.offsets(k) + rows.index(k)(i))); k += 1 }
      s.value
    }

  private def logLik(rows: Rows, e: Array[Double]): Double = {
    val s = new NeumaierSum
    var i = 0
    while (i < rows.n) { s.add(rows.y(i) * e(i) - log1pExp(e(i))); i += 1 }
    s.value
  }

  private def ussq(u: Array[Double]): Double = {
    val s = new NeumaierSum; u.foreach(v => s.add(v * v)); s.value
  }

  /** H = I + ΛZᵀWZΛ at the weights `w`. */
  private def system(rows: Rows, sigma: Array[Double], w: Array[Double]): CrossedSystem = {
    val diag = Array.fill(rows.q)(new NeumaierSum)
    (0 until rows.q).foreach(z => diag(z).add(1.0))
    var i = 0
    while (i < rows.n) {
      var k = 0
      while (k < rows.factors) {
        diag(rows.offsets(k) + rows.index(k)(i)).add(sigma(k) * sigma(k) * w(i)); k += 1
      }
      i += 1
    }
    val pairs = rows.pairs.toSeq.flatMap { case (k, l, a, b, rowPair) =>
      val sums = Array.fill(a.length)(new NeumaierSum)
      (0 until rows.n).foreach(r => sums(rowPair(r)).add(w(r)))
      a.indices.map(e => (k, l, a(e), b(e), sigma(k) * sigma(l) * sums(e).value))
    }
    new CrossedSystem(rows.levels, diag.map(_.value), pairs)
  }

  /** Λ Zᵀ v by level. */
  private def lambdaZt(rows: Rows, sigma: Array[Double], v: Array[Double]): Array[NeumaierSum] = {
    val out = Array.fill(rows.q)(new NeumaierSum)
    var i = 0
    while (i < rows.n) {
      var k = 0
      while (k < rows.factors) {
        out(rows.offsets(k) + rows.index(k)(i)).add(sigma(k) * v(i)); k += 1
      }
      i += 1
    }
    out
  }

  /** The modes at (σ, β) by PIRLS (nAGQ = 1), or of (β, u) jointly (`joint`, nAGQ = 0), with step halving. */
  def modes(
      rows: Rows,
      sigma: Array[Double],
      beta0: Array[Double],
      u0: Array[Double],
      joint: Boolean,
      tolerance: Double = 1e-13,
      maxIterations: Int = 200
  ): Modes = {
    var beta = beta0.clone()
    var u = u0.clone()
    var e = eta(rows, sigma, beta, u)
    var pdev = -2.0 * logLik(rows, e) + ussq(u)
    var iteration = 0
    var done = false
    while (!done && iteration < maxIterations) {
      iteration += 1
      val mu = e.map(expit)
      val w = mu.map(m => m * (1.0 - m))
      val resid = Array.tabulate(rows.n)(i => rows.y(i) - mu(i))
      val h = system(rows, sigma, w)
      val gu = {
        val z = lambdaZt(rows, sigma, resid); Array.tabulate(rows.q)(j => z(j).value - u(j))
      }
      val (dBeta, dU) =
        if (!joint || rows.p == 0) (new Array[Double](rows.p), h.solve(gu))
        else {
          val p = rows.p
          val gb = Array.tabulate(p) { j =>
            val s = new NeumaierSum;
            (0 until rows.n).foreach(i => s.add(rows.x(i * p + j) * resid(i))); s.value
          }
          val xwx = Array.tabulate(p * p) { z =>
            val s = new NeumaierSum;
            (0 until rows.n).foreach(i =>
              s.add(w(i) * rows.x(i * p + z / p) * rows.x(i * p + z % p))
            );
            s.value
          }
          val c = Array.tabulate(p)(j => {
            val z = lambdaZt(rows, sigma, Array.tabulate(rows.n)(i => w(i) * rows.x(i * p + j)));
            z.map(_.value)
          })
          val hInvC = c.map(h.solve)
          val hInvG = h.solve(gu)
          def dot(a: Array[Double], b: Array[Double]): Double = {
            val s = new NeumaierSum; a.indices.foreach(z => s.add(a(z) * b(z))); s.value
          }
          val s = Array.tabulate(p * p)(z => xwx(z) - dot(c(z / p), hInvC(z % p)))
          val rhs = Array.tabulate(p)(j => gb(j) - dot(c(j), hInvG))
          val db = solveDense(s, p, rhs)
          val du =
            h.solve(Array.tabulate(rows.q)(z => gu(z) - (0 until p).map(j => c(j)(z) * db(j)).sum))
          (db, du)
        }
      var step = 1.0
      var accepted = false
      while (!accepted && step > 1e-10) {
        val b1 = Array.tabulate(rows.p)(j => beta(j) + step * dBeta(j))
        val u1 = Array.tabulate(rows.q)(j => u(j) + step * dU(j))
        val e1 = eta(rows, sigma, b1, u1)
        val pdev1 = -2.0 * logLik(rows, e1) + ussq(u1)
        if (pdev1 <= pdev + 1e-12 * math.abs(pdev)) {
          val change = math.abs(pdev - pdev1)
          beta = b1; u = u1; e = e1
          done = change <= tolerance * (math.abs(pdev1) + tolerance)
          pdev = pdev1
          accepted = true
        } else step *= 0.5
      }
      if (!accepted) done = true
    }
    val mu = e.map(expit)
    val h = system(rows, sigma, mu.map(m => m * (1.0 - m)))
    val ll = logLik(rows, e)
    Modes(beta, u, e, ll, h.logDet, -2.0 * ll + ussq(u) + h.logDet)
  }

  private def solveDense(a: Array[Double], n: Int, b: Array[Double]): Array[Double] = {
    val l = new Array[Double](n * n)
    for (j <- 0 until n) {
      val s = new NeumaierSum; s.add(a(j * n + j));
      (0 until j).foreach(k => s.add(-l(j * n + k) * l(j * n + k)))
      if (!(s.value > 0.0))
        throw new ArithmeticException(s"the β system is not positive definite at $j")
      l(j * n + j) = StrictMath.sqrt(s.value)
      for (i <- j + 1 until n) {
        val t = new NeumaierSum; t.add(a(i * n + j));
        (0 until j).foreach(k => t.add(-l(i * n + k) * l(j * n + k)))
        l(i * n + j) = t.value / l(j * n + j)
      }
    }
    val y = new Array[Double](n)
    for (i <- 0 until n) {
      val s = new NeumaierSum; s.add(b(i)); (0 until i).foreach(k => s.add(-l(i * n + k) * y(k)));
      y(i) = s.value / l(i * n + i)
    }
    val x = new Array[Double](n)
    for (i <- (0 until n).reverse) {
      val s = new NeumaierSum; s.add(y(i));
      (i + 1 until n).foreach(k => s.add(-l(k * n + i) * x(k))); x(i) = s.value / l(i * n + i)
    }
    x
  }

  /** The Laplace deviance at (σ, β), its modes found from `u0`. */
  def laplace(rows: Rows, sigma: Array[Double], beta: Array[Double], u0: Array[Double]): Modes =
    modes(rows, sigma, beta, u0, joint = false)

  /** pprof_py's two stages (§1, §4): σ by the nAGQ = 0 deviance from σ = 1, then (σ, β) by the Laplace deviance. */
  def fit(
      rows: Rows,
      beta0: Array[Double],
      sigmaUpper: Double = Double.PositiveInfinity,
      maxIterations: Int = 500
  ): Fit = {
    val k = rows.factors
    val p = rows.p
    def clampSigma(s: Array[Double]): Array[Double] =
      s.map(v => math.min(math.max(v, 0.0), sigmaUpper))
    var u = new Array[Double](rows.q)
    var beta = beta0.clone()
    val stage1 = BoundedQuasiNewton.minimize(
      s => {
        val m = modes(rows, clampSigma(s), beta, u, joint = true)
        m.deviance
      },
      Array.fill(k)(1.0),
      Array.fill(k)(0.0),
      BoundedQuasiNewton.Options(gradientTolerance = 1e-7, maxIterations = maxIterations)
    )
    val s1 = clampSigma(stage1.x)
    val m1 = modes(rows, s1, beta, u, joint = true)
    beta = m1.beta
    u = m1.u
    val start = s1 ++ beta
    var lastU = u
    val stage2 = BoundedQuasiNewton.minimize(
      x => {
        val m = laplace(rows, clampSigma(x.take(k)), x.drop(k), lastU)
        lastU = m.u
        m.deviance
      },
      start,
      Array.fill(k)(0.0) ++ Array.fill(p)(Double.NegativeInfinity),
      BoundedQuasiNewton.Options(gradientTolerance = 1e-9, maxIterations = maxIterations)
    )
    val sigma = clampSigma(stage2.x.take(k))
    val betaHat = stage2.x.drop(k)
    val m2 = laplace(rows, sigma, betaHat, lastU)
    // posterior variances σₖ²·diag(H⁻¹) and Var(β̂) from the β system conditional on the modes (X-043's rule)
    val mu = m2.eta.map(expit)
    val w = mu.map(m => m * (1.0 - m))
    val h = system(rows, sigma, w)
    val inv = h.inverseDiagonal
    val posterior = Array.tabulate(rows.q) { z =>
      val f = rows.offsets.lastIndexWhere(_ <= z).min(k - 1)
      sigma(f) * sigma(f) * inv(z)
    }
    val blups = Array.tabulate(rows.q) { z =>
      val f = rows.offsets.lastIndexWhere(_ <= z).min(k - 1); sigma(f) * m2.u(z)
    }
    val covariance =
      if (p == 0) Array.empty[Double]
      else {
        val xwx = Array.tabulate(p * p) { z =>
          val s = new NeumaierSum;
          (0 until rows.n).foreach(i =>
            s.add(w(i) * rows.x(i * p + z / p) * rows.x(i * p + z % p))
          );
          s.value
        }
        val c = Array.tabulate(p)(j =>
          lambdaZt(rows, sigma, Array.tabulate(rows.n)(i => w(i) * rows.x(i * p + j))).map(_.value)
        )
        val hInvC = c.map(h.solve)
        def dot(a: Array[Double], b: Array[Double]): Double = {
          val s = new NeumaierSum; a.indices.foreach(z => s.add(a(z) * b(z))); s.value
        }
        val s = Array.tabulate(p * p)(z => xwx(z) - dot(c(z / p), hInvC(z % p)))
        val out = new Array[Double](p * p)
        (0 until p).foreach { j =>
          val col = solveDense(s, p, Array.tabulate(p)(z => if (z == j) 1.0 else 0.0))
          (0 until p).foreach(r => out(r * p + j) = col(r))
        }
        out
      }
    Fit(
      sigma,
      betaHat,
      m2.u,
      blups,
      posterior,
      covariance,
      m2.deviance,
      -0.5 * m2.deviance,
      m1.deviance,
      stage1.iterations + stage2.iterations,
      stage1.evaluations + stage2.evaluations,
      stage2.projectedGradient,
      stage2.converged
    )
  }
}
