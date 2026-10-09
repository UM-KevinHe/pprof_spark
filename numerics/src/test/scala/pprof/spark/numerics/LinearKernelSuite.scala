package pprof.spark.numerics

import pprof.spark.numerics.kernels.{LinearFE, Moments}

/** The linear fixed-effect kernels against exact rational arithmetic (BigDecimal), and Cholesky's pivots
  * (docs/spec/linear/fixed-effect-estimation.md §7).
  */
class LinearKernelSuite extends munit.FunSuite {

  private val p = 2
  // Three providers of 1, 4 and 7 rows; features and outcomes on dyadic grids, so BigDecimal is exact.
  private val sizes = Array(1, 4, 7)
  private val data: Array[Array[Double]] = {
    val rng = new scala.util.Random(64)
    Array.fill(sizes.sum)(
      Array(rng.nextInt(129) / 64.0 - 1.0, rng.nextInt(2049) / 1024.0, rng.nextInt(4097) / 256.0)
    )
  }
  private val starts = sizes.scanLeft(0)(_ + _).init
  private val rows = LinearFE.Rows(starts, data.flatten, p)

  private def exactMeans(g: Int): Array[BigDecimal] = {
    val range = starts(g) until starts(g) + sizes(g)
    Array.tabulate(p + 1)(j => range.map(r => BigDecimal(data(r)(j))).sum / BigDecimal(sizes(g)))
  }

  private def close(actual: Double, expected: BigDecimal, tolerance: Double, what: String): Unit = {
    val scale = expected.abs.max(BigDecimal(1e-300))
    assert(
      ((BigDecimal(actual) - expected).abs / scale).toDouble <= tolerance,
      s"$what: $actual against $expected"
    )
  }

  test("the within cross-products equal the exact centred sums over providers") {
    val within = LinearFE.within(rows)
    assertEquals(within.rows, sizes.sum.toLong)
    for (i <- 0 until p; j <- i until p) {
      val exact = sizes.indices.map { g =>
        val m = exactMeans(g)
        (starts(g) until starts(g) + sizes(g))
          .map(r => (BigDecimal(data(r)(i)) - m(i)) * (BigDecimal(data(r)(j)) - m(j)))
          .sum
      }.sum
      close(within.xx(Moments.packedIndex(i, j, p)), exact, 1e-15, s"xx($i, $j)")
    }
    (0 until p).foreach { i =>
      val exact = sizes.indices.map { g =>
        val m = exactMeans(g)
        (starts(g) until starts(g) + sizes(g))
          .map(r => (BigDecimal(data(r)(i)) - m(i)) * (BigDecimal(data(r)(p)) - m(p)))
          .sum
      }.sum
      close(within.xy(i), exact, 1e-15, s"xy($i)")
    }
    close(within.ySum, data.map(r => BigDecimal(r(p))).sum, 1e-16, "sum of y")
  }

  test(
    "effects, their variance factors and the sums of squares equal exact arithmetic at given β and V"
  ) {
    val beta = Array(0.75, -1.25)
    val v = Array(0.5, -0.125, 0.25)
    val providers = LinearFE.providers(rows, beta, v)
    providers.indices.foreach { g =>
      val m = exactMeans(g)
      val gamma = m(p) - m(0) * beta(0) - m(1) * beta(1)
      val q =
        BigDecimal(1) / sizes(g) + m(0) * m(0) * v(0) + 2 * m(0) * m(1) * v(1) + m(1) * m(1) * v(2)
      assertEquals(providers(g).rows, sizes(g).toLong)
      close(providers(g).gamma, gamma, 1e-15, s"gamma $g")
      close(providers(g).q, q, 1e-15, s"q $g")
    }
    val gammas = providers.map(_.gamma)
    val mean = data.map(_(p)).sum / data.length
    val s = LinearFE.sums(rows, beta, gammas, mean)
    val rss = data.indices.map { r =>
      val g = starts.lastIndexWhere(_ <= r)
      val e = BigDecimal(data(r)(p)) - BigDecimal(gammas(g)) - data(r)(0) * BigDecimal(
        beta(0)
      ) - data(r)(1) * BigDecimal(beta(1))
      e * e
    }.sum
    close(s.rss, rss, 1e-14, "rss")
    close(s.tss, data.map(r => (BigDecimal(r(p)) - BigDecimal(mean)).pow(2)).sum, 1e-14, "tss")
  }

  test(
    "an exact within fit is recovered: β by Cholesky of the within cross-products, residuals near zero"
  ) {
    val truth = Array(0.5, -2.0)
    val effects = Array(1.5, -0.25, 3.0)
    val exact = data.indices.map { r =>
      val g = starts.lastIndexWhere(_ <= r)
      Array(data(r)(0), data(r)(1), effects(g) + truth(0) * data(r)(0) + truth(1) * data(r)(1))
    }.toArray
    val exactRows = LinearFE.Rows(starts, exact.flatten, p)
    val within = LinearFE.within(exactRows)
    val factor = Cholesky.factor(within.xx, p)
    val beta = factor.solve(within.xy)
    beta.indices.foreach(k =>
      assert(math.abs(beta(k) - truth(k)) <= 1e-13, s"beta $k = ${beta(k)}")
    )
    val providers = LinearFE.providers(exactRows, beta, factor.inversePacked)
    providers.indices
      .drop(1)
      .foreach(g => assert(math.abs(providers(g).gamma - effects(g)) <= 1e-12, s"gamma $g"))
    assert(LinearFE.sums(exactRows, beta, providers.map(_.gamma), 0.0).rss <= 1e-24)
  }

  test("Cholesky's pivots: the diagonal of a diagonal matrix; zero for an aliased column") {
    assertEquals(Cholesky.factor(Array(4.0, 0.0, 9.0), 2).pivots.toSeq, Seq(4.0, 9.0))
    val aliased = Cholesky.factor(Array(1.0, 2.0, 4.0), 2)
    assertEquals(aliased.aliased, Vector(1))
    assertEquals(aliased.pivots.toSeq, Seq(1.0, 0.0))
  }
}
