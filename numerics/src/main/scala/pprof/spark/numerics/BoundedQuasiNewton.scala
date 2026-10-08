package pprof.spark.numerics

/** Minimization with lower bounds by a projected BFGS with central-difference gradients (forward
  * differences at an active bound) and an Armijo line search (Phase 2f-3 specification §2, X-030).
  */
object BoundedQuasiNewton {

  final case class Options(
      gradientTolerance: Double = 1e-7,
      maxIterations: Int = 200,
      relativeStep: Double = 1e-4
  )

  final case class Result(
      x: Array[Double],
      value: Double,
      projectedGradient: Double,
      converged: Boolean,
      iterations: Int,
      evaluations: Int
  )

  def minimize(
      f: Array[Double] => Double,
      start: Array[Double],
      lower: Array[Double],
      options: Options = Options()
  ): Result = {
    val n = start.length
    var evaluations = 0
    def value(x: Array[Double]): Double = {
      evaluations += 1
      f(x)
    }
    def project(x: Array[Double]): Array[Double] = Array.tabulate(n)(i => math.max(x(i), lower(i)))
    def gradient(x: Array[Double], fx: Double): Array[Double] = Array.tabulate(n) { i =>
      val h = options.relativeStep * math.max(1.0, math.abs(x(i)))
      if (x(i) - h < lower(i)) {
        val up = x.clone()
        up(i) += h
        (value(up) - fx) / h
      } else {
        val up = x.clone()
        val down = x.clone()
        up(i) += h
        down(i) -= h
        (value(up) - value(down)) / (2.0 * h)
      }
    }
    def projectedNorm(x: Array[Double], g: Array[Double]): Double =
      (0 until n).map(i => if (x(i) <= lower(i) && g(i) > 0.0) 0.0 else math.abs(g(i))).max
    var x = project(start)
    var fx = value(x)
    var g = gradient(x, fx)
    var inverse = Array.tabulate(n, n)((i, j) => if (i == j) 1.0 else 0.0)
    var iterations = 0
    var converged = projectedNorm(x, g) <= options.gradientTolerance * math.max(1.0, math.abs(fx))
    var stuck = false
    while (!converged && !stuck && iterations < options.maxIterations) {
      val free = Array.tabulate(n)(i => !(x(i) <= lower(i) && g(i) > 0.0))
      val direction = Array.tabulate(n)(i =>
        if (!free(i)) 0.0 else -(0 until n).filter(free).map(j => inverse(i)(j) * g(j)).sum
      )
      val slope0 = (0 until n).map(i => direction(i) * g(i)).sum
      val d = if (slope0 < 0.0) direction else Array.tabulate(n)(i => if (free(i)) -g(i) else 0.0)
      var t = 1.0
      var accepted = false
      var next = x
      var fNext = fx
      while (!accepted && t > 1e-12) {
        next = project(Array.tabulate(n)(i => x(i) + t * d(i)))
        fNext = value(next)
        val decrease = (0 until n).map(i => g(i) * (next(i) - x(i))).sum
        if (fNext <= fx + 1e-4 * decrease) accepted = true else t *= 0.5
      }
      if (!accepted) stuck = true
      else {
        val gNext = gradient(next, fNext)
        val s = Array.tabulate(n)(i => next(i) - x(i))
        val y = Array.tabulate(n)(i => gNext(i) - g(i))
        val sy = (0 until n).map(i => s(i) * y(i)).sum
        if (
          sy > 1e-14 * math.sqrt(
            (0 until n).map(i => s(i) * s(i)).sum * (0 until n).map(i => y(i) * y(i)).sum
          )
        ) {
          val hy = Array.tabulate(n)(i => (0 until n).map(j => inverse(i)(j) * y(j)).sum)
          val yhy = (0 until n).map(i => y(i) * hy(i)).sum
          inverse = Array.tabulate(n, n)((i, j) =>
            inverse(i)(j) + (sy + yhy) * s(i) * s(j) / (sy * sy) - (hy(i) * s(j) + s(i) * hy(
              j
            )) / sy
          )
        }
        val change = math.abs(fx - fNext)
        x = next
        fx = fNext
        g = gNext
        iterations += 1
        converged =
          projectedNorm(x, g) <= options.gradientTolerance * math.max(1.0, math.abs(fx)) ||
            (change <= 1e-15 * math.max(1.0, math.abs(fx)) && s.map(math.abs).max <= 1e-10)
      }
    }
    Result(x, fx, projectedNorm(x, g), converged, iterations, evaluations)
  }
}
