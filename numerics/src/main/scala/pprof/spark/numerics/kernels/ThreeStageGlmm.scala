package pprof.spark.numerics.kernels

import pprof.spark.numerics.{BoundedQuasiNewton, NeumaierSum, TaylorBins}
import pprof.spark.numerics.kernels.ThreeStageKernel.Cell

/** Stage 2 of the three-stage model on compressed cells (Phase 2f-3 specification): the crossed
  * random-intercept logistic GLMM with the stage 1 offset, its Laplace deviance by PIRLS from u = 0, the
  * joint system eliminated onto the smaller set of levels, and the bounded quasi-Newton optimum (X-030).
  */
object ThreeStageGlmm {

  final case class Options(
      pirlsTolerance: Double = 1e-12,
      pirlsMaxIterations: Int = 100,
      optimizer: BoundedQuasiNewton.Options = BoundedQuasiNewton.Options()
  )

  /** The Laplace deviance at (σₚ, σ_c, μ), with û (providers, then clusters) and its parts. */
  final case class Evaluation(
      deviance: Double,
      u: Array[Double],
      pwrss: Double,
      logDet: Double,
      iterations: Int,
      converged: Boolean
  )

  final case class Fit(
      sigmaProvider: Double,
      sigmaCluster: Double,
      mu: Double,
      u: Array[Double],
      deviance: Double,
      converged: Boolean,
      iterations: Int,
      evaluations: Int,
      projectedGradient: Double
  )

  private val Stride = TaylorBins.Order + 1

  private final case class CellTerms(s1: Array[Double], s2: Array[Double], loglik: Double)

  private def cellTerms(
      cells: Array[Cell],
      m: Int,
      sp: Double,
      sc: Double,
      mu: Double,
      u: Array[Double]
  ): CellTerms = {
    val s1 = new Array[Double](cells.length)
    val s2 = new Array[Double](cells.length)
    val loglik = new NeumaierSum
    cells.indices.foreach { i =>
      val c = cells(i)
      val eta = mu + sp * u(c.provider) + sc * u(m + c.cluster)
      val (a, b, soft) = ThreeStageKernel.sums(c, eta)
      s1(i) = a
      s2(i) = b
      loglik.add(c.yOffset + eta * c.events - soft)
    }
    CellTerms(s1, s2, loglik.value)
  }

  private def pwrss(loglik: Double, u: Array[Double]): Double =
    -2.0 * loglik + u.map(v => v * v).sum

  /** Solves H x = r and gives log|H| for H = I + AᵀWA by eliminating the larger (diagonal) block. */
  private def solve(
      cells: Array[Cell],
      m: Int,
      h: Int,
      sp: Double,
      sc: Double,
      s2: Array[Double],
      r: Array[Double]
  ): (Array[Double], Double) = {
    val dp = Array.fill(m)(1.0)
    val dc = Array.fill(h)(1.0)
    cells.indices.foreach { i =>
      dp(cells(i).provider) += sp * sp * s2(i)
      dc(cells(i).cluster) += sc * sc * s2(i)
    }
    val providersLarger = m >= h
    val (dLarge, dSmall) = if (providersLarger) (dp, dc) else (dc, dp)
    val k = dSmall.length
    def large(c: Cell) = if (providersLarger) c.provider else c.cluster
    def small(c: Cell) = if (providersLarger) c.cluster else c.provider
    val rLarge = if (providersLarger) r.take(m) else r.drop(m)
    val rSmall = if (providersLarger) r.drop(m) else r.take(m)
    // S = D_small − Bᵀ D_large⁻¹ B, with B's entries σₚσ_c w per cell; a cell couples one level of each side
    val schur = Array.tabulate(k, k)((i, j) => if (i == j) dSmall(i) else 0.0)
    val byLarge = cells.indices.groupBy(i => large(cells(i))).toSeq.sortBy(_._1)
    byLarge.foreach { case (l, members) =>
      val ms = members.sorted
      ms.foreach { a =>
        ms.foreach { b =>
          schur(small(cells(a)))(small(cells(b))) -= (sp * sc * s2(a)) * (sp * sc * s2(b)) / dLarge(
            l
          )
        }
      }
    }
    val rhs = rSmall.clone()
    cells.indices.foreach(i =>
      rhs(small(cells(i))) -= sp * sc * s2(i) * rLarge(large(cells(i))) / dLarge(large(cells(i)))
    )
    // dense Cholesky of S
    val l = Array.ofDim[Double](k, k)
    var logDet = dLarge.map(StrictMath.log).sum
    var j = 0
    while (j < k) {
      var d = schur(j)(j)
      var p = 0
      while (p < j) {
        d -= l(j)(p) * l(j)(p)
        p += 1
      }
      if (!(d > 0.0))
        throw new ArithmeticException(
          s"the random-effect system is not positive definite (sigma $sp, $sc)"
        )
      l(j)(j) = StrictMath.sqrt(d)
      logDet += StrictMath.log(d)
      var i = j + 1
      while (i < k) {
        var v = schur(i)(j)
        p = 0
        while (p < j) {
          v -= l(i)(p) * l(j)(p)
          p += 1
        }
        l(i)(j) = v / l(j)(j)
        i += 1
      }
      j += 1
    }
    val z = new Array[Double](k)
    (0 until k).foreach(i => z(i) = (rhs(i) - (0 until i).map(p => l(i)(p) * z(p)).sum) / l(i)(i))
    val xSmall = new Array[Double](k)
    (k - 1 to 0 by -1).foreach(i =>
      xSmall(i) = (z(i) - (i + 1 until k).map(p => l(p)(i) * xSmall(p)).sum) / l(i)(i)
    )
    val xLarge = rLarge.indices.map(i => rLarge(i) / dLarge(i)).toArray
    cells.indices.foreach(i =>
      xLarge(large(cells(i))) -= sp * sc * s2(i) * xSmall(small(cells(i))) / dLarge(large(cells(i)))
    )
    val x = if (providersLarger) xLarge ++ xSmall else xSmall ++ xLarge
    (x, logDet)
  }

  /** PIRLS from u = 0 (pprof_py's step rule: accept a step when the penalized deviance does not rise by
    * more than 1e-10, halving otherwise), then the Laplace deviance at û.
    */
  def evaluate(
      cells: Array[Cell],
      m: Int,
      h: Int,
      sigmaProvider: Double,
      sigmaCluster: Double,
      mu: Double,
      options: Options = Options()
  ): Evaluation = {
    val (sp, sc) = (sigmaProvider, sigmaCluster)
    var u = new Array[Double](m + h)
    var terms = cellTerms(cells, m, sp, sc, mu, u)
    var current = pwrss(terms.loglik, u)
    var iterations = 0
    var converged = false
    var stalled = false
    while (!converged && !stalled && iterations < options.pirlsMaxIterations) {
      val r = new Array[Double](m + h)
      cells.indices.foreach { i =>
        val resid = cells(i).events - terms.s1(i)
        r(cells(i).provider) += sp * resid
        r(m + cells(i).cluster) += sc * resid
      }
      (0 until m + h).foreach(i => r(i) -= u(i))
      val (step, _) = solve(cells, m, h, sp, sc, terms.s2, r)
      var t = 1.0
      var accepted = false
      var trial = u
      var trialTerms = terms
      var trialValue = current
      var halvings = 0
      while (!accepted && halvings <= 30) {
        trial = Array.tabulate(m + h)(i => u(i) + t * step(i))
        trialTerms = cellTerms(cells, m, sp, sc, mu, trial)
        trialValue = pwrss(trialTerms.loglik, trial)
        if (!trialValue.isNaN && trialValue <= current + 1e-10) accepted = true
        else {
          t *= 0.5
          halvings += 1
        }
      }
      if (!accepted) stalled = true
      else {
        val change = math.abs(current - trialValue) / math.max(math.abs(trialValue), 1e-300)
        u = trial
        terms = trialTerms
        current = trialValue
        iterations += 1
        converged = change < options.pirlsTolerance
      }
    }
    val (_, logDet) = solve(cells, m, h, sp, sc, terms.s2, new Array[Double](m + h))
    Evaluation(current + logDet, u, current, logDet, iterations, converged)
  }

  /** The optimum of the Laplace deviance over (σₚ, σ_c, μ) with σ ≥ 0, from (1, 1, μ₀) with μ₀ the logit of
    * the mean outcome minus the mean offset.
    */
  def fit(cells: Array[Cell], m: Int, h: Int, options: Options = Options()): Fit = {
    val records = cells
      .map(c =>
        (0 until c.bins.length)
          .map(b => c.moments(b * (Stride)))
          .sum
      )
      .sum
    val events = cells.map(_.events).sum
    val offsetSum = cells.map { c =>
      (0 until c.bins.length)
        .map(b => c.moments(b * Stride + 1) + TaylorBins.centre(c.bins(b)) * c.moments(b * Stride))
        .sum
    }.sum
    val mean = events / records
    val mu0 = StrictMath.log(mean / (1.0 - mean)) - offsetSum / records
    val r = BoundedQuasiNewton.minimize(
      x => evaluate(cells, m, h, x(0), x(1), x(2), options).deviance,
      Array(1.0, 1.0, mu0),
      Array(0.0, 0.0, Double.NegativeInfinity),
      options.optimizer
    )
    val at = evaluate(cells, m, h, r.x(0), r.x(1), r.x(2), options)
    Fit(
      r.x(0),
      r.x(1),
      r.x(2),
      at.u,
      at.deviance,
      r.converged,
      r.iterations,
      r.evaluations,
      r.projectedGradient
    )
  }
}
