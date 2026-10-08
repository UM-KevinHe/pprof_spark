package pprof.spark.numerics.kernels

import pprof.spark.numerics.{GaussHermite, NeumaierSum, Serbin, SparseSymmetric, TaylorBins}

/** Stage 3 of the three-stage model on compressed cells (Phase 2f-2 specification §2 to §5, X-029):
  * pprof_py's `marginal` estimator (adaptive Gauss–Hermite quadrature at each cluster's posterior mode,
  * projected Newton with a sparse Hessian and an Armijo line search), with every sum over records taken
  * from each cell's offset-bin moments, and the σ = 0 limit (X-005). Work is per cluster
  * ([[clusterTerms]], [[clusterValue]]); [[Passes]] runs it in memory or on executors, and assembly in
  * cluster order makes both bitwise identical.
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

  /** One cluster's contribution at γ: its mode and scale, ℓ term, score contributions (provider, value) in
    * cell order, negative-Hessian triplets, and the posterior mean and variance of its effect.
    */
  final case class ClusterTerms(
      cluster: Int,
      center: Double,
      scale: Double,
      loglik: Double,
      scoreProviders: Array[Int],
      scoreValues: Array[Double],
      rows: Array[Int],
      cols: Array[Int],
      values: Array[Double],
      alphaMean: Double,
      alphaVar: Double
  )

  /** ℓ, score, posterior moments per cluster and the negative Hessian (when requested), assembled. */
  final case class Terms(
      loglik: Double,
      score: Array[Double],
      center: Array[Double],
      scale: Array[Double],
      alphaMean: Array[Double],
      alphaVar: Array[Double],
      negativeHessian: Option[SparseSymmetric]
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

  /** The posterior mode and scale of one cluster's effect (pprof_py's `_cluster_modes`, stopping when this
    * cluster's step falls below 1e-10).
    */
  def clusterMode(
      cells: Array[Cell],
      gamma: Array[Double],
      sigma: Double,
      start: Double
  ): (Double, Double) = {
    val prior = 1.0 / (sigma * sigma)
    var a = start
    var step = Double.PositiveInfinity
    var count = 0
    while (count < 100 && math.abs(step) >= 1e-10) {
      var grad = -a * prior
      var curv = prior
      cells.foreach { c =>
        val (s1, s2, _) = sums(c, gamma(c.provider) + a)
        grad += c.events - s1
        curv += s2
      }
      step = math.max(-1.0, math.min(1.0, grad / curv))
      a += step
      count += 1
    }
    var curv = prior
    cells.foreach(c => curv += sums(c, gamma(c.provider) + a)._2)
    (a, 1.0 / StrictMath.sqrt(curv))
  }

  /** One cluster's terms (pprof_py's `_marginal_terms`); the mode is found from `start` unless `fixed`
    * gives the centre and scale; σ = 0 gives the limit's terms (no quadrature).
    */
  def clusterTerms(
      cluster: Int,
      cells: Array[Cell],
      gamma: Array[Double],
      sigma: Double,
      start: Double,
      fixed: Option[(Double, Double)],
      rule: GaussHermite.Rule,
      logWeights: Array[Double],
      hessian: Boolean
  ): ClusterTerms = {
    val rows = Array.newBuilder[Int]
    val cols = Array.newBuilder[Int]
    val vals = Array.newBuilder[Double]
    if (sigma == 0.0) {
      var ll = 0.0
      val scores = new Array[Double](cells.length)
      cells.indices.foreach { i =>
        val c = cells(i)
        val (s1, s2, soft) = sums(c, gamma(c.provider))
        ll += c.yOffset + gamma(c.provider) * c.events - soft
        scores(i) = c.events - s1
        if (hessian) {
          rows += c.provider
          cols += c.provider
          vals += s2
        }
      }
      return ClusterTerms(
        cluster,
        0.0,
        0.0,
        ll,
        cells.map(_.provider),
        scores,
        rows.result(),
        cols.result(),
        vals.result(),
        0.0,
        0.0
      )
    }
    val (center, scale) = fixed.getOrElse(clusterMode(cells, gamma, sigma, start))
    val n = rule.nodes.length
    val a = Array.tabulate(n)(k => center + Sqrt2 * scale * rule.nodes(k))
    val s = Array.ofDim[Double](cells.length, n)
    val v = Array.ofDim[Double](cells.length, n)
    val logPost = new Array[Double](n)
    var k = 0
    while (k < n) {
      var ll = 0.0
      var i = 0
      while (i < cells.length) {
        val cell = cells(i)
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
    val loglik = logZ + StrictMath.log(Sqrt2 * scale) - (LogSqrt2Pi + StrictMath.log(sigma))
    val mean = Array.tabulate(cells.length)(i =>
      (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * s(i)(kk))
    )
    if (hessian) {
      var x = 0
      while (x < cells.length) {
        var y = 0
        while (y < cells.length) {
          val cov =
            (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * s(x)(kk) * s(y)(kk)) - mean(
              x
            ) * mean(y)
          rows += cells(x).provider
          cols += cells(y).provider
          vals += -cov
          y += 1
        }
        rows += cells(x).provider
        cols += cells(x).provider
        vals += (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * v(x)(kk))
        x += 1
      }
    }
    val alphaMean = (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * a(kk))
    val alphaVar =
      (0 until n).foldLeft(0.0)((acc, kk) => acc + p(kk) * a(kk) * a(kk)) - alphaMean * alphaMean
    ClusterTerms(
      cluster,
      center,
      scale,
      loglik,
      cells.map(_.provider),
      mean,
      rows.result(),
      cols.result(),
      vals.result(),
      alphaMean,
      alphaVar
    )
  }

  /** Assembles clusters' terms in cluster order (identical for any split of the clusters). */
  def assemble(parts: Seq[ClusterTerms], providers: Int, clusters: Int, hessian: Boolean): Terms = {
    val ordered = parts.sortBy(_.cluster)
    val loglik = new NeumaierSum
    val score = new Array[Double](providers)
    val center = new Array[Double](clusters)
    val scale = new Array[Double](clusters)
    val alphaMean = new Array[Double](clusters)
    val alphaVar = new Array[Double](clusters)
    ordered.foreach { t =>
      loglik.add(t.loglik)
      t.scoreProviders.indices.foreach(i => score(t.scoreProviders(i)) += t.scoreValues(i))
      center(t.cluster) = t.center
      scale(t.cluster) = t.scale
      alphaMean(t.cluster) = t.alphaMean
      alphaVar(t.cluster) = t.alphaVar
    }
    val matrix =
      if (hessian)
        Some(
          SparseSymmetric.fromTriplets(
            providers,
            ordered.flatMap(_.rows).toArray,
            ordered.flatMap(_.cols).toArray,
            ordered.flatMap(_.values).toArray
          )
        )
      else None
    Terms(loglik.value, score, center, scale, alphaMean, alphaVar, matrix)
  }

  /** Where the per-cluster work runs: in memory, or on executors (the engine's implementation). */
  trait Passes {
    def providers: Int
    def clusters: Int

    /** Terms with the negative Hessian at γ, each cluster's mode found from `start`. */
    def full(gamma: Array[Double], start: Array[Double]): Terms

    /** ℓ at γ with the given centres and scales. */
    def value(gamma: Array[Double], center: Array[Double], scale: Array[Double]): Double
  }

  /** Cells grouped by cluster (input order within each cluster). */
  def byCluster(cells: Array[Cell], clusters: Int): Array[Array[Cell]] = {
    val lists = Array.fill(clusters)(Array.newBuilder[Cell])
    cells.foreach(c => lists(c.cluster) += c)
    lists.map(_.result())
  }

  final class InMemoryPasses(
      cells: Array[Cell],
      val providers: Int,
      val clusters: Int,
      sigma: Double,
      nNodes: Int
  ) extends Passes {
    private val groups = byCluster(cells, clusters)
    private val rule = GaussHermite.rule(nNodes)
    private val logWeights = rule.adaptiveLogWeights

    def full(gamma: Array[Double], start: Array[Double]): Terms =
      assemble(
        groups.indices.map(h =>
          clusterTerms(h, groups(h), gamma, sigma, start(h), None, rule, logWeights, hessian = true)
        ),
        providers,
        clusters,
        hessian = true
      )

    def value(gamma: Array[Double], center: Array[Double], scale: Array[Double]): Double =
      assemble(
        groups.indices.map(h =>
          clusterTerms(
            h,
            groups(h),
            gamma,
            sigma,
            0.0,
            Some((center(h), scale(h))),
            rule,
            logWeights,
            hessian = false
          )
        ),
        providers,
        clusters,
        hessian = false
      ).loglik
  }

  def fit(
      cells: Array[Cell],
      providers: Int,
      clusters: Int,
      sigma: Double,
      start: Array[Double],
      options: Options
  ): Result =
    fit(
      new InMemoryPasses(cells, providers, clusters, sigma, options.nNodes),
      sigma,
      start,
      options
    )

  /** pprof_py's `_fit_marginal` from `start`; σ = 0 takes the limit. */
  def fit(passes: Passes, sigma: Double, start: Array[Double], options: Options): Result = {
    val providers = passes.providers
    require(sigma >= 0.0 && !sigma.isInfinite, s"sigma must be finite and non-negative, got $sigma")
    require(
      start.length == providers && start.forall(v => !v.isNaN && !v.isInfinite),
      "the start needs one finite effect per provider"
    )
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
    var center = new Array[Double](passes.clusters)
    var iterations = 0
    var criterion = Double.PositiveInfinity
    var stalled = false
    var done = false
    var last: Terms = null
    var held = new Array[Boolean](providers)
    while (!done) {
      val current = passes.full(gamma, center)
      center = current.center
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
          if (
            passes.value(trial, current.center, current.scale) >= current.loglik + 1e-4 * t * slope
          ) accepted = true
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
    Result(
      gamma,
      criterion < options.tol,
      stalled,
      iterations,
      criterion,
      last.loglik,
      last.alphaMean,
      last.alphaVar,
      held
    )
  }
}
