package pprof.spark.numerics.kernels

import pprof.spark.numerics.{Brent, Cholesky, CholeskyFactor, NeumaierSum, NeumaierVector}

/** Kernels of the linear random-intercept model (docs/spec/linear/random-effect-estimation.md §2 to §4, §7 and
  * §14). A block's rows are grouped by provider in canonical order; `groupStart` gives each provider's first row,
  * and `values` holds each row's p features (the intercept's constant first when the model has one), then
  * ỹ = y − offset, then the prior weight w, row-major. Pure and deterministic: every sum runs in row and provider
  * order with Neumaier's compensation.
  */
object LinearRE {

  final case class Rows(groupStart: Array[Int], values: Array[Double], p: Int) {
    require(p >= 0, s"the number of features must be non-negative, got $p")
    require(values.length % (p + 2) == 0, s"${values.length} values are not rows of ${p + 2}")
    def count: Int = values.length / (p + 2)
    def groups: Int = groupStart.length
    def until(g: Int): Int = if (g + 1 < groups) groupStart(g + 1) else count
  }

  /** One provider: its rows nⱼ, its weight total Wⱼ and the weighted means x̄ⱼ and ȳⱼ (of ỹ). */
  final case class Provider(rows: Long, weight: Double, xMean: Array[Double], yMean: Double)

  /** A block's totals: rows, Σ log w, and the weighted within co-moments Σ w(x − x̄ⱼ)(x − x̄ⱼ)ᵀ (packed) and
    * Σ w(x − x̄ⱼ)(ỹ − ȳⱼ).
    */
  final case class Within(rows: Long, logWeights: Double, xx: Array[Double], xy: Array[Double])

  /** Pass 1: each provider's summary, by two passes over its rows (weighted means, then centred sums), and the
    * block's totals, added over its providers in order.
    */
  def summaries(rows: Rows): (Array[Provider], Within) = {
    val p = rows.p
    val q = p + 2
    val xx = new NeumaierVector(Moments.packedLength(p))
    val xy = new NeumaierVector(p)
    val logWeights = new NeumaierSum
    val providers = new Array[Provider](rows.groups)
    val deviation = new Array[Double](p + 1)
    var g = 0
    while (g < rows.groups) {
      val from = rows.groupStart(g)
      val until = rows.until(g)
      val weight = new NeumaierSum
      val sums = new NeumaierVector(p + 1)
      var r = from
      while (r < until) {
        val w = rows.values(r * q + p + 1)
        weight.add(w)
        logWeights.add(math.log(w))
        var k = 0
        while (k <= p) {
          sums.addAt(k, w * rows.values(r * q + k))
          k += 1
        }
        r += 1
      }
      val total = weight.value
      val mean = Array.tabulate(p + 1)(k => sums.valueAt(k) / total)
      r = from
      while (r < until) {
        val w = rows.values(r * q + p + 1)
        var k = 0
        while (k <= p) {
          deviation(k) = rows.values(r * q + k) - mean(k)
          k += 1
        }
        var i = 0
        while (i < p) {
          var j = i
          while (j < p) {
            xx.addAt(Moments.packedIndex(i, j, p), w * deviation(i) * deviation(j))
            j += 1
          }
          xy.addAt(i, w * deviation(i) * deviation(p))
          i += 1
        }
        r += 1
      }
      providers(g) = Provider((until - from).toLong, total, mean.take(p), mean(p))
      g += 1
    }
    (providers, Within(rows.count.toLong, logWeights.value, xx.values, xy.values))
  }

  /** A solution β_w of W_xx·β = W_xy (§6): the columns Cholesky finds aliased, the intercept and features constant
    * within providers among them, at 0.
    */
  def withinSolution(xx: Array[Double], xy: Array[Double], p: Int): Array[Double] =
    if (p == 0) new Array[Double](0) else Cholesky.factor(xx, p).solveUnaliased(xy)

  /** Pass 2: Σ w(ỹ − ȳⱼ − (x − x̄ⱼ)ᵀβ_w)² over the block's rows, with its providers from pass 1. */
  def withinResiduals(rows: Rows, providers: Array[Provider], betaWithin: Array[Double]): Double = {
    val p = rows.p
    val q = p + 2
    val sum = new NeumaierSum
    var g = 0
    while (g < rows.groups) {
      val provider = providers(g)
      var r = rows.groupStart(g)
      val until = rows.until(g)
      while (r < until) {
        var e = rows.values(r * q + p) - provider.yMean
        var k = 0
        while (k < p) {
          e -= (rows.values(r * q + k) - provider.xMean(k)) * betaWithin(k)
          k += 1
        }
        sum.add(rows.values(r * q + p + 1) * e * e)
        r += 1
      }
      g += 1
    }
    sum.value
  }

  /** The fit's data: the provider table in key order (nⱼ, Wⱼ, x̄ⱼ row-major m × p, ȳⱼ), the pooled totals, and
    * the within solution with its residual sum of squares (pass 2). `intercept` when feature 0 is the constant.
    */
  final case class Data(
      rowCounts: Array[Long],
      weights: Array[Double],
      xMeans: Array[Double],
      yMeans: Array[Double],
      p: Int,
      intercept: Boolean,
      rows: Long,
      logWeights: Double,
      xx: Array[Double],
      xy: Array[Double],
      betaWithin: Array[Double],
      withinResidual: Double
  ) {
    def m: Int = weights.length
  }

  /** The deviance and its slope in τ (§2.2, §2.3), with β̂ in the centred coordinates of §14. */
  final case class Evaluation(
      tau: Double,
      deviance: Double,
      slope: Double,
      beta: Array[Double],
      factor: Option[CholeskyFactor],
      pwrss: Double,
      logDetH: Double,
      residualMeans: Array[Double]
  )

  /** With an intercept, the weighted grand means of the features (0 for the intercept) and of ỹ (§14); zeros
    * otherwise.
    */
  private def centres(data: Data): (Array[Double], Double) = {
    val p = data.p
    if (!data.intercept) (new Array[Double](p), 0.0)
    else {
      val total = new NeumaierSum
      val xs = new NeumaierVector(p)
      val ys = new NeumaierSum
      var j = 0
      while (j < data.m) {
        val w = data.weights(j)
        total.add(w)
        var k = 1
        while (k < p) {
          xs.addAt(k, w * data.xMeans(j * p + k))
          k += 1
        }
        ys.add(w * data.yMeans(j))
        j += 1
      }
      (
        Array.tabulate(p)(k => if (k == 0) 0.0 else xs.valueAt(k) / total.value),
        ys.value / total.value
      )
    }
  }

  /** X′WX in the centred coordinates of §14: W_xx + Σⱼ Wⱼ(x̄ⱼ − x̄)(x̄ⱼ − x̄)ᵀ, packed. */
  private def crossProducts(data: Data, centreX: Array[Double]): Array[Double] = {
    val p = data.p
    val out = new NeumaierVector(Moments.packedLength(p))
    var index = 0
    while (index < data.xx.length) {
      out.addAt(index, data.xx(index))
      index += 1
    }
    var j = 0
    while (j < data.m) {
      var i = 0
      while (i < p) {
        val zi = data.xMeans(j * p + i) - centreX(i)
        var k = i
        while (k < p) {
          out.addAt(
            Moments.packedIndex(i, k, p),
            data.weights(j) * zi * (data.xMeans(j * p + k) - centreX(k))
          )
          k += 1
        }
        i += 1
      }
      j += 1
    }
    out.values
  }

  /** The columns of X′WX, in the centred coordinates of §14, that Cholesky finds aliased (X-039). */
  def aliased(data: Data): Vector[Int] =
    if (data.p == 0) Vector.empty
    else Cholesky.factor(crossProducts(data, centres(data)._1), data.p).aliased

  /** The profiled criterion of one fit: REML or ML on the data's summaries. */
  final class Problem(val data: Data, val reml: Boolean) {
    private val p = data.p
    private val m = data.m
    val nu: Double = (if (reml) data.rows - p else data.rows).toDouble
    require(nu > 0.0, s"the criterion needs n > p, got n = ${data.rows} and p = $p")

    private val centred = centres(data)
    val centreX: Array[Double] = centred._1
    val centreY: Double = centred._2
    private val z: Array[Double] = Array.tabulate(m * p)(i => data.xMeans(i) - centreX(i % p))
    private val y: Array[Double] = Array.tabulate(m)(j => data.yMeans(j) - centreY)
    private val gradient: Array[Double] = Array.tabulate(p) { i =>
      var s = data.xy(i)
      var k = 0
      while (k < p) {
        s -= data.xx(Moments.packedIndex(math.min(i, k), math.max(i, k), p)) * data.betaWithin(k)
        k += 1
      }
      s
    }

    def evaluate(tau: Double): Evaluation = {
      require(tau >= 0.0, s"tau must be non-negative, got $tau")
      val length = Moments.packedLength(p)
      val a = new NeumaierVector(length)
      val g = new NeumaierVector(length)
      val rhs = new NeumaierVector(p)
      val logH = new NeumaierSum
      val sumC = new NeumaierSum
      var j = 0
      while (j < m) {
        val w = data.weights(j)
        val c = w / (1.0 + tau * w)
        logH.add(math.log1p(tau * w))
        sumC.add(c)
        var i = 0
        while (i < p) {
          val zi = z(j * p + i)
          var k = i
          while (k < p) {
            val index = Moments.packedIndex(i, k, p)
            val product = zi * z(j * p + k)
            a.addAt(index, c * product)
            g.addAt(index, c * c * product)
            k += 1
          }
          rhs.addAt(i, c * zi * y(j))
          i += 1
        }
        j += 1
      }
      val factor =
        if (p == 0) None
        else Some(Cholesky.factor(Array.tabulate(length)(i => data.xx(i) + a.valueAt(i)), p))
      factor.foreach(f =>
        require(f.isFullRank, s"A(tau) is aliased in columns ${f.aliased.mkString(", ")}")
      )
      val beta = factor.fold(new Array[Double](0))(
        _.solve(Array.tabulate(p)(i => data.xy(i) + rhs.valueAt(i)))
      )
      val residualMeans = new Array[Double](m)
      val between = new NeumaierSum
      val betweenSlope = new NeumaierSum
      j = 0
      while (j < m) {
        var r = y(j)
        var k = 0
        while (k < p) {
          r -= z(j * p + k) * beta(k)
          k += 1
        }
        residualMeans(j) = r
        val c = data.weights(j) / (1.0 + tau * data.weights(j))
        between.add(c * r * r)
        betweenSlope.add(c * c * r * r)
        j += 1
      }
      val v = Array.tabulate(p)(k => beta(k) - data.betaWithin(k))
      val quadratic = new NeumaierSum
      val cross = new NeumaierSum
      var i = 0
      while (i < p) {
        cross.add(v(i) * gradient(i))
        var k = 0
        while (k < p) {
          quadratic.add(
            v(i) * data.xx(Moments.packedIndex(math.min(i, k), math.max(i, k), p)) * v(k)
          )
          k += 1
        }
        i += 1
      }
      val pwrss =
        data.withinResidual - 2.0 * cross.value + math.max(quadratic.value, 0.0) + between.value
      require(pwrss > 0.0, "the penalized residual sum of squares is 0: the data fit exactly")
      var deviance =
        nu * (1.0 + math.log(2.0 * math.Pi * pwrss / nu)) + logH.value - data.logWeights
      var slope = -nu * betweenSlope.value / pwrss + sumC.value
      if (reml) factor.foreach { f =>
        deviance += f.pivots.map(math.log).sum
        val inverse = f.inversePacked
        val trace = new NeumaierSum
        var r = 0
        while (r < p) {
          var c = 0
          while (c < p) {
            val index = Moments.packedIndex(math.min(r, c), math.max(r, c), p)
            trace.add(inverse(index) * g.valueAt(index))
            c += 1
          }
          r += 1
        }
        slope -= trace.value
      }
      Evaluation(tau, deviance, slope, beta, factor, pwrss, logH.value, residualMeans)
    }

    /** β̂ in the original coordinates: the intercept moved back from the centred coordinates (§14). */
    def originalBeta(beta: Array[Double]): Array[Double] = {
      val out = beta.clone()
      if (data.intercept) {
        var shift = centreY
        var k = 1
        while (k < p) {
          shift -= centreX(k) * beta(k)
          k += 1
        }
        out(0) = beta(0) + shift
      }
      out
    }

    /** T·V·Tᵀ for a packed V in the centred coordinates, with T the identity but T(0, k) = −x̄ₖ (§14). */
    def originalCovariance(packed: Array[Double]): Array[Double] = {
      if (!data.intercept) packed
      else {
        def at(i: Int, k: Int): Double = packed(
          Moments.packedIndex(math.min(i, k), math.max(i, k), p)
        )
        def t(i: Int, k: Int): Double = if (i == k) 1.0 else if (i == 0) -centreX(k) else 0.0
        val out = new Array[Double](Moments.packedLength(p))
        var i = 0
        while (i < p) {
          var k = i
          while (k < p) {
            val s = new NeumaierSum
            var a = 0
            while (a < p) {
              var b = 0
              while (b < p) {
                s.add(t(i, a) * at(a, b) * t(k, b))
                b += 1
              }
              a += 1
            }
            out(Moments.packedIndex(i, k, p)) = s.value
            k += 1
          }
          i += 1
        }
        out
      }
    }
  }

  /** The search's record (§3): its evaluations, the bracket of the root, |d′(τ̂)| and the boundary, if any. */
  final case class Search(
      evaluations: Int,
      lower: Double,
      upper: Double,
      slope: Double,
      boundary: Option[String],
      warnings: Vector[String]
  )

  /** The fit at τ̂ (§4), in the original coordinates. */
  final case class Fit(
      tau: Double,
      theta: Double,
      sigma: Double,
      sigmaAlpha: Double,
      beta: Array[Double],
      covariance: Array[Double],
      blups: Array[Double],
      conditionalVariances: Array[Double],
      deviance: Double,
      logLikelihood: Double,
      aic: Double,
      bic: Double,
      pwrss: Double,
      logDetH: Double,
      ussq: Double,
      reml: Boolean,
      search: Search
  )

  private def median(values: Array[Double]): Double = {
    val sorted = values.sorted
    val n = sorted.length
    if (n % 2 == 1) sorted(n / 2) else 0.5 * (sorted(n / 2 - 1) + sorted(n / 2))
  }

  /** Fits the model (§3, §4): the grid, the boundary rules, Brent's root of d′, the estimates at τ̂. */
  def fit(
      data: Data,
      reml: Boolean,
      thetaUpper: Double = Double.PositiveInfinity,
      maxEvaluations: Int = 200
  ): Fit = {
    require(
      data.m >= 2,
      s"the provider variance needs at least two providers, got ${data.m} (X-040)"
    )
    require(
      data.m < data.rows,
      s"the providers (${data.m}) must be fewer than the rows (${data.rows}) (X-040)"
    )
    require(
      data.rows > data.p,
      s"the rows (${data.rows}) must outnumber the fixed effects (${data.p}) (X-040)"
    )
    require(thetaUpper > 0.0, s"thetaUpper must be positive, got $thetaUpper")
    val problem = new Problem(data, reml)
    val tauMax = thetaUpper * thetaUpper
    val scale = median(data.weights)
    var count = 0
    def evaluate(tau: Double): Evaluation = {
      count += 1
      if (count > maxEvaluations)
        throw new IllegalStateException(s"the search exceeded $maxEvaluations evaluations")
      problem.evaluate(tau)
    }
    val grid = scala.collection.mutable.ArrayBuffer(evaluate(0.0))
    for (k <- -8 to 8) {
      val tau = math.min(math.pow(10.0, k.toDouble) / scale, tauMax)
      if (tau > grid.last.tau) grid += evaluate(tau)
    }
    var extensions = 0
    while (grid.last.slope < 0.0 && grid.last.tau < tauMax && extensions < 30) {
      grid += evaluate(math.min(grid.last.tau * 10.0, tauMax))
      extensions += 1
    }
    if (grid.last.slope < 0.0 && grid.last.tau < tauMax)
      throw new IllegalStateException(
        "the deviance falls as theta grows: the provider variance is not identified"
      )
    var best = 0
    var i = 1
    while (i < grid.length) {
      if (grid(i).deviance < grid(best).deviance) best = i
      i += 1
    }
    val warnings = Vector.newBuilder[String]
    val (chosen, lower, upper, boundary) =
      if (best == 0 && grid(0).slope >= 0.0) (grid(0), 0.0, 0.0, Some("lower"))
      else if (grid(best).tau == tauMax && grid(best).slope <= 0.0)
        (grid(best), tauMax, tauMax, Some("upper"))
      else {
        val candidates =
          if (best == 0) Seq((grid(0), grid(1)))
          else if (best == grid.length - 1) Seq((grid(best - 1), grid(best)))
          else if (grid(best).slope < 0.0)
            Seq((grid(best), grid(best + 1)), (grid(best - 1), grid(best)))
          else Seq((grid(best - 1), grid(best)), (grid(best), grid(best + 1)))
        val bracket = candidates.find { case (l, u) => l.slope < 0.0 && u.slope > 0.0 }.orElse {
          val (l, u) = (candidates.head._1, candidates.last._2)
          val points = (1 to 9).map { s =>
            if (l.tau == 0.0) u.tau * s / 10.0 else l.tau * math.pow(u.tau / l.tau, s / 10.0)
          }
          val fine = (l +: points.map(evaluate)) :+ u
          fine.sliding(2).map(pair => (pair(0), pair(1))).find { case (a, b) =>
            a.slope < 0.0 && b.slope > 0.0
          }
        }
        bracket match {
          case Some((l, u)) =>
            val root = Brent.root(t => evaluate(t).slope, l.tau, u.tau, 0.0)
            (evaluate(root), l.tau, u.tau, None)
          case None =>
            warnings += "the derivative of the deviance does not change sign near its smallest grid value; " +
              "the fit takes the best point evaluated"
            (grid(best), grid(best).tau, grid(best).tau, None)
        }
      }
    if (boundary.contains("lower")) warnings += "boundary fit: theta = 0, so the BLUPs are 0"
    if (boundary.contains("upper")) warnings += s"boundary fit at thetaUpper = $thetaUpper"
    val tau = chosen.tau
    val sigma2 = chosen.pwrss / problem.nu
    val theta = math.sqrt(tau)
    val sigma = math.sqrt(sigma2)
    val covariance = chosen.factor.fold(new Array[Double](0)) { f =>
      problem.originalCovariance(f.inversePacked.map(_ * sigma2))
    }
    val blups = Array.tabulate(data.m) { j =>
      val w = data.weights(j)
      tau * w * chosen.residualMeans(j) / (1.0 + tau * w)
    }
    val conditional = Array.tabulate(data.m)(j => sigma2 * tau / (1.0 + tau * data.weights(j)))
    val ussq = if (tau > 0.0) blups.map(b => b * b).sum / tau else 0.0
    val k = (data.p + 2).toDouble
    Fit(
      tau = tau,
      theta = theta,
      sigma = sigma,
      sigmaAlpha = theta * sigma,
      beta = problem.originalBeta(chosen.beta),
      covariance = covariance,
      blups = blups,
      conditionalVariances = conditional,
      deviance = chosen.deviance,
      logLikelihood = -0.5 * chosen.deviance,
      aic = chosen.deviance + 2.0 * k,
      bic = chosen.deviance + k * math.log(data.rows.toDouble),
      pwrss = chosen.pwrss,
      logDetH = chosen.logDetH,
      ussq = ussq,
      reml = reml,
      search = Search(count, lower, upper, math.abs(chosen.slope), boundary, warnings.result())
    )
  }
}
