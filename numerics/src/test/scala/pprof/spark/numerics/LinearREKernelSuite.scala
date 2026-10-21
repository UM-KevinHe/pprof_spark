package pprof.spark.numerics

import pprof.spark.numerics.LinearREKernelSuite.Dense
import pprof.spark.numerics.kernels.{LinearRE, Moments}

/** The linear random-effect kernels (docs/spec/linear/random-effect-estimation.md §2 to §4 and §14): the summaries
  * against exact sums, the profiled deviance and the fit against a dense solve of the mixed-model equations, the
  * slope against central differences, the search's optimum and boundaries, invariances and the design checks.
  */
class LinearREKernelSuite extends munit.FunSuite {

  // Five providers of 1, 3, 4, 6 and 9 rows; features on a 1/64 grid, weights on a 1/64 grid, outcomes on 1/1024.
  private val sizes = Array(1, 3, 4, 6, 9)
  private val n = sizes.sum
  private val starts = sizes.scanLeft(0)(_ + _).init
  private val provider = sizes.zipWithIndex.flatMap { case (s, j) => Array.fill(s)(j) }
  private val rng = new scala.util.Random(97)
  private val effects = Array(0.4, -0.3, 0.8, -0.6, 0.1)
  private val x = Array.fill(n)(Array(rng.nextInt(129) / 64.0 - 1.0, rng.nextInt(129) / 64.0 - 1.0))
  private val w = Array.fill(n)((32 + rng.nextInt(97)) / 64.0)
  private val noise = Array.fill(n)(rng.nextGaussian())
  private val y = Array.tabulate(n) { i =>
    val mean = 1.0 + 0.8 * x(i)(0) - 0.5 * x(i)(1) + effects(provider(i))
    math.rint((mean + noise(i) / math.sqrt(w(i))) * 1024) / 1024
  }

  private def rows(
      xs: Array[Array[Double]],
      ys: Array[Double],
      ws: Array[Double],
      intercept: Boolean
  ) = {
    val p = xs(0).length + (if (intercept) 1 else 0)
    val values = (0 until n).toArray.flatMap { i =>
      (if (intercept) Array(1.0) else Array.empty[Double]) ++ xs(i) ++ Array(ys(i), ws(i))
    }
    LinearRE.Rows(starts, values, p)
  }

  private def data(r: LinearRE.Rows, intercept: Boolean): LinearRE.Data = {
    val (providers, within) = LinearRE.summaries(r)
    val bw = LinearRE.withinSolution(within.xx, within.xy, r.p)
    LinearRE.Data(
      providers.map(_.rows),
      providers.map(_.weight),
      providers.flatMap(_.xMean),
      providers.map(_.yMean),
      r.p,
      intercept,
      within.rows,
      within.logWeights,
      within.xx,
      within.xy,
      bw,
      LinearRE.withinResiduals(r, providers, bw)
    )
  }

  private val base = data(rows(x, y, w, intercept = true), intercept = true)

  private def relative(a: Double, e: Double): Double =
    if (a == e) 0.0 else math.abs(a - e) / math.abs(e)

  private def dense(tau: Double, reml: Boolean): Dense = {
    val m = sizes.length
    val p = 3
    val d = m + p
    val theta = math.sqrt(tau)
    val design = Array.tabulate(n)(i => Array(1.0, x(i)(0), x(i)(1)))
    val full = Array.ofDim[Double](d, d)
    val rhs = new Array[Double](d)
    for (i <- 0 until n) {
      val col = Array.tabulate(d)(k =>
        if (k < m) (if (provider(i) == k) theta else 0.0) else design(i)(k - m)
      )
      for (a <- 0 until d; b <- 0 until d) full(a)(b) += w(i) * col(a) * col(b)
      for (a <- 0 until d) rhs(a) += w(i) * col(a) * y(i)
    }
    for (k <- 0 until m) full(k)(k) += 1.0
    val packed = for (a <- 0 until d; b <- a until d) yield full(a)(b)
    val factor = Cholesky.factor(packed.toArray, d, 0.0)
    val solution = factor.solve(rhs)
    val u = solution.take(m)
    val beta = solution.drop(m)
    val pwrss = (0 until n).map { i =>
      val e = y(i) - (0 until p).map(k => design(i)(k) * beta(k)).sum - theta * u(provider(i))
      w(i) * e * e
    }.sum + u.map(v => v * v).sum
    val logFull = factor.pivots.map(math.log).sum
    val logH =
      (0 until m).map(k => math.log(full(k)(k))).sum // H is diagonal: providers do not share rows
    val nu = if (reml) n - p else n
    val dev = nu * (1 + math.log(2 * math.Pi * pwrss / nu)) + logH - w.map(math.log).sum +
      (if (reml) logFull - logH else 0.0)
    Dense(dev, beta, u.map(_ * theta), pwrss)
  }

  test(
    "the summaries equal exact weighted sums: weight totals, means and the pooled within co-moments"
  ) {
    val r = rows(x, y, w, intercept = true)
    val (providers, within) = LinearRE.summaries(r)
    for (g <- sizes.indices) {
      val range = starts(g) until starts(g) + sizes(g)
      val total = range.map(i => BigDecimal(w(i))).sum
      assertEquals(providers(g).weight, total.toDouble)
      val mean = range.map(i => BigDecimal(w(i)) * BigDecimal(y(i))).sum / total
      assert(relative(providers(g).yMean, mean.toDouble) <= 1e-15, s"provider $g")
    }
    for (a <- 0 until 2; b <- a until 2) {
      val exact = sizes.indices.map { g =>
        val range = starts(g) until starts(g) + sizes(g)
        val total = range.map(i => BigDecimal(w(i))).sum
        def mean(k: Int) = range.map(i => BigDecimal(w(i)) * BigDecimal(x(i)(k))).sum / total
        range
          .map(i =>
            BigDecimal(w(i)) * (BigDecimal(x(i)(a)) - mean(a)) * (BigDecimal(x(i)(b)) - mean(b))
          )
          .sum
      }.sum
      assert(
        relative(within.xx(Moments.packedIndex(a + 1, b + 1, 3)), exact.toDouble) <= 1e-14,
        s"xx($a, $b)"
      )
    }
    assertEquals(within.xx(Moments.packedIndex(0, 0, 3)), 0.0)
  }

  test(
    "the profiled deviance and the fit equal a dense solve of the mixed-model equations, REML and ML"
  ) {
    for (reml <- Seq(true, false)) {
      val problem = new LinearRE.Problem(base, reml)
      for (tau <- Seq(0.0, 0.05, 0.4, 3.0)) {
        val e = dense(tau, reml)
        val a = problem.evaluate(tau)
        assert(
          relative(a.deviance, e.deviance) <= 1e-12,
          s"reml=$reml tau=$tau: ${a.deviance} vs ${e.deviance}"
        )
        assert(relative(a.pwrss, e.pwrss) <= 1e-12, s"pwrss reml=$reml tau=$tau")
        problem.originalBeta(a.beta).zip(e.beta).foreach { case (b, eb) =>
          assert(math.abs(b - eb) <= 1e-12)
        }
      }
      val fit = LinearRE.fit(base, reml)
      val e = dense(fit.tau, reml)
      assert(relative(fit.deviance, e.deviance) <= 1e-12)
      fit.blups.zip(e.blups).foreach { case (b, eb) =>
        assert(math.abs(b - eb) <= 1e-12, s"BLUP $b vs $eb")
      }
      fit.beta.zip(e.beta).foreach { case (b, eb) => assert(math.abs(b - eb) <= 1e-12) }
    }
  }

  test("the slope in tau equals central differences of the deviance") {
    for (reml <- Seq(true, false); tau <- Seq(0.02, 0.3, 2.0)) {
      val problem = new LinearRE.Problem(base, reml)
      val h = 1e-5 * tau
      val difference =
        (problem.evaluate(tau + h).deviance - problem.evaluate(tau - h).deviance) / (2 * h)
      val slope = problem.evaluate(tau).slope
      assert(
        math.abs(slope - difference) <= 1e-6 * math.max(1.0, math.abs(slope)),
        s"$slope vs $difference"
      )
    }
  }

  test(
    "the search finds the minimum: the slope vanishes at tau-hat, and the deviance rises on either side"
  ) {
    for (reml <- Seq(true, false)) {
      val fit = LinearRE.fit(base, reml)
      val problem = new LinearRE.Problem(base, reml)
      assert(fit.tau > 0.0 && fit.search.boundary.isEmpty)
      assert(fit.search.slope <= 1e-9 * base.weights.sum, s"slope ${fit.search.slope}")
      assert(problem.evaluate(fit.tau * 1.001).deviance > fit.deviance)
      assert(problem.evaluate(fit.tau * 0.999).deviance > fit.deviance)
      assertEquals(fit.sigmaAlpha, fit.theta * fit.sigma)
      assert(fit.search.evaluations <= 60, s"${fit.search.evaluations} evaluations")
    }
  }

  test(
    "no between-provider variation gives a boundary fit at theta = 0; thetaUpper bounds the search"
  ) {
    val flat = Array.tabulate(n) { i =>
      val range = starts(provider(i)) until starts(provider(i)) + sizes(provider(i))
      val mean = range.map(k => w(k) * noise(k) / math.sqrt(w(k))).sum / range.map(w(_)).sum
      1.0 + 0.8 * x(i)(0) - 0.5 * x(i)(1) + noise(i) / math.sqrt(w(i)) - mean
    }
    val fit = LinearRE.fit(data(rows(x, flat, w, intercept = true), intercept = true), reml = true)
    assertEquals(fit.tau, 0.0)
    assertEquals(fit.search.boundary, Some("lower"))
    assert(
      fit.blups.forall(_ == 0.0) && fit.conditionalVariances.forall(_ == 0.0) && fit.ussq == 0.0
    )
    val free = LinearRE.fit(base, reml = true)
    val bound = 0.1 * free.theta
    val capped = LinearRE.fit(base, reml = true, thetaUpper = bound)
    assertEquals(capped.search.boundary, Some("upper"))
    assertEquals(capped.tau, bound * bound)
  }

  test(
    "shifting a feature moves only the intercept; scaling the weights by 4 halves theta and doubles sigma"
  ) {
    val shifted = x.map(r => Array(r(0) + 4096.0, r(1)))
    for (reml <- Seq(true, false)) {
      val a = LinearRE.fit(base, reml)
      val b = LinearRE.fit(data(rows(shifted, y, w, intercept = true), intercept = true), reml)
      assert(relative(b.tau, a.tau) <= 1e-10, s"tau ${b.tau} vs ${a.tau}")
      assert(math.abs(b.beta(0) + 4096.0 * b.beta(1) - a.beta(0)) <= 1e-9)
      assert(relative(b.beta(1), a.beta(1)) <= 1e-10 && relative(b.beta(2), a.beta(2)) <= 1e-10)
      b.blups.zip(a.blups).foreach { case (u, v) => assert(math.abs(u - v) <= 1e-11) }
      assert(relative(b.deviance, a.deviance) <= 1e-12)
      val c =
        LinearRE.fit(data(rows(x, y, w.map(_ * 4.0), intercept = true), intercept = true), reml)
      assert(relative(c.theta, a.theta / 2.0) <= 1e-10 && relative(c.sigma, a.sigma * 2.0) <= 1e-10)
      c.beta.zip(a.beta).foreach { case (u, v) => assert(math.abs(u - v) <= 1e-11) }
      assert(relative(c.deviance, a.deviance) <= 1e-12)
    }
  }

  test(
    "an aliased feature is reported; a feature constant within providers is identified; designs refused"
  ) {
    val duplicated = x.map(r => Array(r(0), r(1), 2.0 * r(0)))
    assertEquals(
      LinearRE.aliased(data(rows(duplicated, y, w, intercept = true), intercept = true)),
      Vector(3)
    )
    val level = Array(0.25, -0.5, 0.75, 0.125, -0.25)
    val withLevel = x.indices.map(i => Array(x(i)(0), x(i)(1), level(provider(i)))).toArray
    val d = data(rows(withLevel, y, w, intercept = true), intercept = true)
    assertEquals(LinearRE.aliased(d), Vector.empty[Int])
    assertEquals(d.betaWithin(3), 0.0)
    assert(LinearRE.fit(d, reml = true).tau > 0.0)
    val one = base.copy(
      rowCounts = Array(n.toLong),
      weights = Array(w.sum),
      xMeans = base.xMeans.take(3),
      yMeans = base.yMeans.take(1)
    )
    intercept[IllegalArgumentException](LinearRE.fit(one, reml = true))
    val each = base.copy(rows = sizes.length.toLong)
    intercept[IllegalArgumentException](LinearRE.fit(each, reml = false))
  }
}

object LinearREKernelSuite {

  /** The dense mixed-model solve at τ: (u, β) from the full penalized normal equations, ordered u first. */
  final case class Dense(deviance: Double, beta: Array[Double], blups: Array[Double], pwrss: Double)
}
