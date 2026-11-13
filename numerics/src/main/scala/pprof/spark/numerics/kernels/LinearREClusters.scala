package pprof.spark.numerics.kernels

import pprof.spark.numerics.{BoundedQuasiNewton, NeumaierSum}

/** The linear random-effect model with crossed cluster factors (docs/spec/linear/random-effect-clusters.md, slice
  * 4d-1): pprof_py's profiled REML/ML criterion for θ per factor, evaluated from sufficient statistics by eliminating
  * the provider block (factor 0) onto the cluster levels, whose dense block is factored by Cholesky's method.
  */
object LinearREClusters {

  /** One factor's level sums: Σw, Σw·x (levels × p, row-major) and Σw·(y − o − ȳ). */
  final case class Factor(levels: Int, weight: Array[Double], wx: Array[Double], wy: Array[Double])

  /** The weight totals of the level pairs that share records, for factors `k` < `l`. */
  final case class Pair(k: Int, l: Int, a: Array[Int], b: Array[Int], w: Array[Double])

  /** The model's sufficient statistics; y is centred by `yCentre` (added back to the intercept, `intercept` ≥ 0). */
  final case class Data(
      n: Long,
      p: Int,
      factors: Array[Factor],
      pairs: Array[Pair],
      xx: Array[Double],
      xy: Array[Double],
      yy: Double,
      logWeights: Double,
      yCentre: Double,
      intercept: Int
  )

  final case class Evaluation(
      criterion: Double,
      beta: Array[Double],
      u: Array[Double],
      sigma2: Double,
      pwrss: Double,
      logDetH: Double,
      logDetS: Double,
      sInverse: Array[Double]
  )

  final case class Fit(
      theta: Array[Double],
      evaluation: Evaluation,
      iterations: Int,
      evaluations: Int,
      projectedGradient: Double,
      converged: Boolean
  )

  private def cholesky(a: Array[Double], n: Int): Array[Double] = {
    val l = new Array[Double](n * n)
    var j = 0
    while (j < n) {
      val s = new NeumaierSum
      s.add(a(j * n + j))
      var k = 0
      while (k < j) { s.add(-l(j * n + k) * l(j * n + k)); k += 1 }
      val pivot = s.value
      if (!(pivot > 0.0))
        throw new ArithmeticException(s"matrix not positive definite at pivot $j ($pivot)")
      val root = StrictMath.sqrt(pivot)
      l(j * n + j) = root
      var i = j + 1
      while (i < n) {
        val t = new NeumaierSum
        t.add(a(i * n + j))
        k = 0
        while (k < j) { t.add(-l(i * n + k) * l(j * n + k)); k += 1 }
        l(i * n + j) = t.value / root
        i += 1
      }
      j += 1
    }
    l
  }

  private def solve(l: Array[Double], n: Int, b: Array[Double]): Array[Double] = {
    val y = new Array[Double](n)
    var i = 0
    while (i < n) {
      val s = new NeumaierSum
      s.add(b(i))
      var k = 0
      while (k < i) { s.add(-l(i * n + k) * y(k)); k += 1 }
      y(i) = s.value / l(i * n + i)
      i += 1
    }
    val x = new Array[Double](n)
    i = n - 1
    while (i >= 0) {
      val s = new NeumaierSum
      s.add(y(i))
      var k = i + 1
      while (k < n) { s.add(-l(k * n + i) * x(k)); k += 1 }
      x(i) = s.value / l(i * n + i)
      i -= 1
    }
    x
  }

  /** The criterion and its solution at θ (one value per factor, factor 0 the provider). */
  def evaluate(data: Data, theta: Array[Double], reml: Boolean): Evaluation = {
    val f = data.factors
    val p = data.p
    val levels = f.map(_.levels)
    val offsets = levels.scanLeft(0)(_ + _) // each factor's first index in u
    val q = offsets.last
    val m = levels(0)
    val cluster = q - m // the cluster levels, eliminated last
    // the provider block's diagonal
    val d0 = Array.tabulate(m)(i => 1.0 + theta(0) * theta(0) * f(0).weight(i))
    // provider–cluster couplings, by provider: (cluster index in the dense block, value)
    val coupling = Array.fill(m)(scala.collection.mutable.ArrayBuffer.empty[(Int, Double)])
    val dense = new Array[Double](cluster * cluster)
    var k = 1
    while (k < f.length) {
      var i = 0
      while (i < levels(k)) {
        val c = offsets(k) - m + i
        dense(c * cluster + c) = 1.0 + theta(k) * theta(k) * f(k).weight(i)
        i += 1
      }
      k += 1
    }
    data.pairs.foreach { pair =>
      val scale = theta(pair.k) * theta(pair.l)
      if (scale != 0.0) {
        var e = 0
        while (e < pair.a.length) {
          val v = scale * pair.w(e)
          if (pair.k == 0) coupling(pair.a(e)) += ((offsets(pair.l) - m + pair.b(e), v))
          else {
            val r = offsets(pair.k) - m + pair.a(e)
            val c = offsets(pair.l) - m + pair.b(e)
            dense(r * cluster + c) += v
            dense(c * cluster + r) += v
          }
          e += 1
        }
      }
    }
    // the Schur complement on the cluster levels
    var i = 0
    while (i < m) {
      val row = coupling(i)
      var a = 0
      while (a < row.length) {
        var b = 0
        while (b < row.length) {
          dense(row(a)._1 * cluster + row(b)._1) -= row(a)._2 * row(b)._2 / d0(i)
          b += 1
        }
        a += 1
      }
      i += 1
    }
    val chol = if (cluster > 0) cholesky(dense, cluster) else Array.empty[Double]
    val logDetSum = new NeumaierSum
    d0.foreach(v => logDetSum.add(StrictMath.log(v)))
    i = 0
    while (i < cluster) { logDetSum.add(2.0 * StrictMath.log(chol(i * cluster + i))); i += 1 }
    val logDetH = logDetSum.value
    // H⁻¹r by block elimination
    def hSolve(r: Array[Double]): Array[Double] = {
      val t = Array.tabulate(cluster)(c => r(m + c))
      var j = 0
      while (j < m) {
        coupling(j).foreach { case (c, v) => t(c) -= v * r(j) / d0(j) }
        j += 1
      }
      val xc = if (cluster > 0) solve(chol, cluster, t) else Array.empty[Double]
      val x = new Array[Double](q)
      j = 0
      while (j < m) {
        val s = new NeumaierSum
        s.add(r(j))
        coupling(j).foreach { case (c, v) => s.add(-v * xc(c)) }
        x(j) = s.value / d0(j)
        j += 1
      }
      Array.copy(xc, 0, x, m, cluster)
      x
    }
    // C = XᵀWZΛ (p × q) and d = ΛZᵀW(y − o − ȳ)
    val cT = Array.ofDim[Double](p, q) // column j of Cᵀ, as rows by feature
    val dVec = new Array[Double](q)
    k = 0
    while (k < f.length) {
      var l = 0
      while (l < levels(k)) {
        val index = offsets(k) + l
        dVec(index) = theta(k) * f(k).wy(l)
        var j = 0
        while (j < p) { cT(j)(index) = theta(k) * f(k).wx(l * p + j); j += 1 }
        l += 1
      }
      k += 1
    }
    val hInvD = hSolve(dVec)
    val hInvCt = cT.map(hSolve) // p vectors of length q
    def dot(a: Array[Double], b: Array[Double]): Double = {
      val s = new NeumaierSum
      var z = 0
      while (z < a.length) { s.add(a(z) * b(z)); z += 1 }
      s.value
    }
    val sMatrix = Array.tabulate(p * p)(z => data.xx(z) - dot(cT(z / p), hInvCt(z % p)))
    (0 until p).foreach(r =>
      (r + 1 until p).foreach { c =>
        val v = 0.5 * (sMatrix(r * p + c) + sMatrix(c * p + r)); sMatrix(r * p + c) = v;
        sMatrix(c * p + r) = v
      }
    )
    val sChol = if (p > 0) cholesky(sMatrix, p) else Array.empty[Double]
    val rhs = Array.tabulate(p)(j => data.xy(j) - dot(cT(j), hInvD))
    val beta = if (p > 0) solve(sChol, p, rhs) else Array.empty[Double]
    val dMinus = Array.tabulate(q)(z => dVec(z) - (0 until p).map(j => cT(j)(z) * beta(j)).sum)
    val u = hSolve(dMinus)
    val pw = new NeumaierSum
    pw.add(data.yy)
    var j = 0
    while (j < p) { pw.add(-beta(j) * data.xy(j)); j += 1 }
    pw.add(-dot(u, dVec))
    val pwrss = pw.value
    val logDetS = {
      val s = new NeumaierSum; j = 0;
      while (j < p) { s.add(2.0 * StrictMath.log(sChol(j * p + j))); j += 1 }; s.value
    }
    val dof = if (reml) (data.n - p).toDouble else data.n.toDouble
    val sigma2 = math.max(pwrss / dof, java.lang.Double.MIN_NORMAL)
    val criterion =
      if (reml)
        dof * (StrictMath.log(
          2.0 * StrictMath.PI * sigma2
        ) + 1.0) + logDetH + logDetS - data.logWeights
      else dof * (StrictMath.log(2.0 * StrictMath.PI * sigma2) + 1.0) + logDetH - data.logWeights
    val sInverse = new Array[Double](p * p)
    j = 0
    while (j < p) {
      val e = Array.tabulate(p)(z => if (z == j) 1.0 else 0.0)
      val col = solve(sChol, p, e)
      var r = 0
      while (r < p) { sInverse(r * p + j) = col(r); r += 1 }
      j += 1
    }
    if (data.intercept >= 0) beta(data.intercept) += data.yCentre
    Evaluation(criterion, beta, u, sigma2, pwrss, logDetH, logDetS, sInverse)
  }

  /** θ̂ by the bounded quasi-Newton search from θ = 1 for every factor (§4), within [0, `thetaUpper`]. */
  def fit(
      data: Data,
      reml: Boolean,
      thetaUpper: Double = Double.PositiveInfinity,
      maxIterations: Int = 200
  ): Fit = {
    val factors = data.factors.length
    def clamp(x: Array[Double]): Array[Double] = x.map(v => math.min(math.max(v, 0.0), thetaUpper))
    val result = BoundedQuasiNewton.minimize(
      x => evaluate(data, clamp(x), reml).criterion,
      Array.fill(factors)(1.0),
      Array.fill(factors)(0.0),
      BoundedQuasiNewton.Options(gradientTolerance = 1e-9, maxIterations = maxIterations)
    )
    val theta = clamp(result.x)
    Fit(
      theta,
      evaluate(data, theta, reml),
      result.iterations,
      result.evaluations,
      result.projectedGradient,
      result.converged
    )
  }
}
