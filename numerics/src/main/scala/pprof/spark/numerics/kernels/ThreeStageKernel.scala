package pprof.spark.numerics.kernels

import pprof.spark.numerics.{GaussHermite, NeumaierSum, Serbin, SparseSymmetric, TaylorBins}

/** Stage 3 of the three-stage model on compressed cells (Phase 2f-2 specification §2 to §5, X-029):
  * pprof_py's `marginal` estimator (adaptive Gauss–Hermite quadrature at each cluster's posterior mode,
  * projected Newton with a sparse Hessian and an Armijo line search), with every sum over records taken
  * from each cell's offset-bin moments, and the σ = 0 limit (X-005).
  */
object ThreeStageKernel {

  /** One (cluster, provider) cell: Σỹ, Σỹ·o and its offset bins (ascending) with Order + 1 moments each. */
  final case class Cell(
      cluster: Int,
      provider: Int,
      events: Double,
      yOffset: Double,
      bins: Array[Long],
      moments: Array[Double]
  )

  final case class Options(
      nNodes: Int = 20,
      maxIter: Int = 10000,
      tol: Double = 1e-5,
      bound: Double = 10.0,
      relativeBound: Boolean = true
  )

  final case class Result(
      gamma: Array[Double],
      converged: Boolean,
      stalled: Boolean,
      iterations: Int,
      criterion: Double,
      loglik: Double,
      alphaMean: Array[Double],
      alphaVar: Array[Double],
      held: Array[Boolean]
  )

  private val K = TaylorBins.Order
  private val Sqrt2 = StrictMath.sqrt(2.0)
  private val LogSqrt2Pi = 0.5 * StrictMath.log(2.0 * StrictMath.PI)

  private def sigmoid(x: Double): Double = 1.0 / (1.0 + StrictMath.exp(-x))

  private def softplus(x: Double): Double =
    if (x > 0.0) x + StrictMath.log1p(StrictMath.exp(-x)) else StrictMath.log1p(StrictMath.exp(x))

  /** Σσ, Σσ′ and Σ log(1 + e^(o+η)) over the cell's records, at effect η (remainder below rounding). */
  def sums(cell: Cell, eta: Double): (Double, Double, Double) = {
    val first = new NeumaierSum
    val second = new NeumaierSum
    val soft = new NeumaierSum
    var b = 0
    while (b < cell.bins.length) {
      val x = eta + TaylorBins.centre(cell.bins(b))
      val s = sigmoid(x)
      var k = 0
      while (k <= K) {
        val m = cell.moments(b * (K + 1) + k)
        first.add(TaylorBins.derivative(k, s) * m)
        second.add(TaylorBins.derivative(k + 1, s) * m)
        soft.add((if (k == 0) softplus(x) else TaylorBins.derivative(k - 1, s)) * m)
        k += 1
      }
      b += 1
    }
    (first.value, second.value, soft.value)
  }

  /** Cells grouped by cluster (cell indices in input order within each cluster). */
  def byCluster(cells: Array[Cell], clusters: Int): Array[Array[Int]] = {
    val lists = Array.fill(clusters)(Array.newBuilder[Int])
    cells.indices.foreach(i => lists(cells(i).cluster) += i)
    lists.map(_.result())
  }

  /** Each cluster's posterior mode and scale of its effect (pprof_py's `_cluster_modes`, stopping per
    * cluster when its step is below 1e-10).
    */
  def modes(
      cells: Array[Cell],
      groups: Array[Array[Int]],
      gamma: Array[Double],
      sigma: Double,
      start: Array[Double]
  ): (Array[Double], Array[Double]) = {
    val center = start.clone()
    val scale = new Array[Double](groups.length)
    val prior = 1.0 / (sigma * sigma)
    groups.indices.foreach { h =>
      var a = center(h)
      var step = Double.PositiveInfinity
      var count = 0
      while (count < 100 && math.abs(step) >= 1e-10) {
        var grad = -a * prior
        var curv = prior
        groups(h).foreach { c =>
          val (s1, s2, _) = sums(cells(c), gamma(cells(c).provider) + a)
          grad += cells(c).events - s1
          curv += s2
        }
        step = math.max(-1.0, math.min(1.0, grad / curv))
        a += step
        count += 1
      }
      var curv = prior
      groups(h).foreach(c => curv += sums(cells(c), gamma(cells(c).provider) + a)._2)
      center(h) = a
      scale(h) = 1.0 / StrictMath.sqrt(curv)
    }
    (center, scale)
  }

  /** The adaptive-quadrature marginal log-likelihood, its score per provider, the posterior node weights
    * and node values per cluster and, when `hessian`, the negative Hessian (pprof_py's `_marginal_terms`).
    */
  final case class Terms(
      loglik: Double,
      score: Array[Double],
      post: Array[Array[Double]],
      nodes: Array[Array[Double]],
      negativeHessian: Option[SparseSymmetric]
  )

  def terms(
      cells: Array[Cell],
      groups: Array[Array[Int]],
      providers: Int,
      gamma: Array[Double],
      sigma: Double,
      center: Array[Double],
      scale: Array[Double],
      rule: GaussHermite.Rule,
      logWeights: Array[Double],
      hessian: Boolean
  ): Terms = {
    val n = rule.nodes.length
    val score = new Array[Double](providers)
    val loglik = new NeumaierSum
    val post = new Array[Array[Double]](groups.length)
    val nodes = new Array[Array[Double]](groups.length)
    val rows = Array.newBuilder[Int]
    val cols = Array.newBuilder[Int]
    val vals = Array.newBuilder[Double]
    groups.indices.foreach { h =>
      val a = Array.tabulate(n)(k => center(h) + Sqrt2 * scale(h) * rule.nodes(k))
      val cellCount = groups(h).length
      val s = Array.ofDim[Double](cellCount, n)
      val v = Array.ofDim[Double](cellCount, n)
      val logPost = new Array[Double](n)
      var k = 0
      while (k < n) {
        var ll = 0.0
        var i = 0
        while (i < cellCount) {
          val cell = cells(groups(h)(i))
          val eta = gamma(cell.provider) + a(k)
          val (s1, s2, soft) = sums(cell, eta)
          ll += cell.yOffset + eta * cell.events - soft
          s(i)(k) = cell.events - s1
          v(i)(k) = s2
          i += 1
        }
        logPost(k) = ll + logWeights(k) - a(k) * a(k) / (2.0 * sigma * sigma)
        k += 1
      }
      val peak = logPost.max
      val logZ = StrictMath.log(logPost.map(lp => StrictMath.exp(lp - peak)).sum) + peak
      val p = logPost.map(lp => StrictMath.exp(lp - logZ))
      loglik.add(logZ + StrictMath.log(Sqrt2 * scale(h)) - (LogSqrt2Pi + StrictMath.log(sigma)))
      post(h) = p
      nodes(h) = a
      val mean = Array.tabulate(cellCount)(i =>
        (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * s(i)(kk))
      )
      var i = 0
      while (i < cellCount) {
        score(cells(groups(h)(i)).provider) += mean(i)
        i += 1
      }
      if (hessian) {
        var x = 0
        while (x < cellCount) {
          var y = 0
          while (y < cellCount) {
            val cov =
              (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * s(x)(kk) * s(y)(kk)) - mean(
                x
              ) * mean(y)
            rows += cells(groups(h)(x)).provider
            cols += cells(groups(h)(y)).provider
            vals += -cov
            y += 1
          }
          rows += cells(groups(h)(x)).provider
          cols += cells(groups(h)(x)).provider
          vals += (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * v(x)(kk))
          x += 1
        }
      }
    }
    val matrix =
      if (hessian)
        Some(SparseSymmetric.fromTriplets(providers, rows.result(), cols.result(), vals.result()))
      else None
    Terms(loglik.value, score, post, nodes, matrix)
  }

  /** The σ = 0 limit: no cluster effects (X-005). */
  def limitTerms(
      cells: Array[Cell],
      providers: Int,
      gamma: Array[Double],
      hessian: Boolean
  ): Terms = {
    val score = new Array[Double](providers)
    val curvature = new Array[Double](providers)
    val loglik = new NeumaierSum
    cells.foreach { cell =>
      val (s1, s2, soft) = sums(cell, gamma(cell.provider))
      loglik.add(cell.yOffset + gamma(cell.provider) * cell.events - soft)
      score(cell.provider) += cell.events - s1
      curvature(cell.provider) += s2
    }
    val matrix =
      if (hessian)
        Some(
          SparseSymmetric.fromTriplets(
            providers,
            Array.range(0, providers),
            Array.range(0, providers),
            curvature
          )
        )
      else None
    Terms(loglik.value, score, Array.empty, Array.empty, matrix)
  }

  /** pprof_py's `_fit_marginal` on compressed cells, from `start`; σ = 0 takes the limit. */
  def fit(
      cells: Array[Cell],
      providers: Int,
      clusters: Int,
      sigma: Double,
      start: Array[Double],
      options: Options
  ): Result = {
    require(sigma >= 0.0 && !sigma.isInfinite, s"sigma must be finite and non-negative, got $sigma")
    require(
      start.length == providers && start.forall(v => !v.isNaN && !v.isInfinite),
      "the start needs one finite effect per provider"
    )
    val groups = byCluster(cells, clusters)
    val rule = GaussHermite.rule(options.nNodes)
    val logWeights = rule.adaptiveLogWeights
    def bounds(g: Array[Double]): (Double, Double) =
      if (options.relativeBound) {
        val c = Serbin.median(g)
        (c - options.bound, c + options.bound)
      } else (-options.bound, options.bound)
    def clip(g: Array[Double]): Array[Double] = {
      val (lo, hi) = bounds(g)
      g.map(x => math.min(math.max(x, lo), hi))
    }
    var gamma = clip(start)
    var center = new Array[Double](clusters)
    var scale = new Array[Double](clusters)
    var iterations = 0
    var criterion = Double.PositiveInfinity
    var stalled = false
    var done = false
    var last: Terms = null
    var held = new Array[Boolean](providers)
    while (!done) {
      if (sigma > 0.0) {
        val (c, s) = modes(cells, groups, gamma, sigma, center)
        center = c
        scale = s
      }
      val current =
        if (sigma > 0.0)
          terms(
            cells,
            groups,
            providers,
            gamma,
            sigma,
            center,
            scale,
            rule,
            logWeights,
            hessian = true
          )
        else limitTerms(cells, providers, gamma, hessian = true)
      last = current
      val (lo, hi) = bounds(gamma)
      held = Array.tabulate(providers)(j =>
        (gamma(j) <= lo + 1e-12 && current.score(j) < 0.0) || (gamma(j) >= hi - 1e-12 && current
          .score(j) > 0.0)
      )
      val free = held.map(!_)
      criterion = (0 until providers)
        .filter(free)
        .map(j => math.abs(current.score(j)))
        .foldLeft(0.0)(math.max)
      if (criterion < options.tol || iterations >= options.maxIter || stalled) done = true
      else {
        val (step, _) = current.negativeHessian.get.solve(current.score, free)
        val slope = (0 until providers).filter(free).map(j => current.score(j) * step(j)).sum
        var t = 1.0
        var trial = gamma
        var accepted = false
        while (!accepted && !stalled) {
          trial = clip(Array.tabulate(providers)(j => gamma(j) + t * step(j)))
          val value =
            if (sigma > 0.0)
              terms(
                cells,
                groups,
                providers,
                trial,
                sigma,
                center,
                scale,
                rule,
                logWeights,
                hessian = false
              ).loglik
            else limitTerms(cells, providers, trial, hessian = false).loglik
          if (value >= current.loglik + 1e-4 * t * slope) accepted = true
          else {
            t *= 0.5
            if (t < 1e-10) stalled = true
          }
        }
        if (!stalled) {
          gamma = trial
          iterations += 1
        }
      }
    }
    val (alphaMean, alphaVar) =
      if (sigma > 0.0)
        (
          last.post.indices
            .map(h => last.post(h).indices.map(k => last.post(h)(k) * last.nodes(h)(k)).sum)
            .toArray,
          last.post.indices.map { h =>
            val mean = last.post(h).indices.map(k => last.post(h)(k) * last.nodes(h)(k)).sum
            last
              .post(h)
              .indices
              .map(k => last.post(h)(k) * last.nodes(h)(k) * last.nodes(h)(k))
              .sum - mean * mean
          }.toArray
        )
      else (new Array[Double](clusters), new Array[Double](clusters))
    Result(
      gamma,
      criterion < options.tol,
      stalled,
      iterations,
      criterion,
      last.loglik,
      alphaMean,
      alphaVar,
      held
    )
  }
}
