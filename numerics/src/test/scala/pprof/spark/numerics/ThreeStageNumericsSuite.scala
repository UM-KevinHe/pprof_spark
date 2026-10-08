package pprof.spark.numerics

import pprof.spark.numerics.kernels.ThreeStageKernel

/** Gauss–Hermite rules, the sparse solver and stage 3's compressed sums (Phase 2f-2 specification §3, §4). */
class ThreeStageNumericsSuite extends munit.FunSuite {

  private def near(a: Double, b: Double, tolerance: Double, what: String): Unit =
    assert(math.abs(a - b) <= tolerance, s"$what: $a against $b")

  test("Gauss–Hermite rules integrate polynomials exactly and are symmetric") {
    val sqrtPi = StrictMath.sqrt(StrictMath.PI)
    Seq(1, 2, 5, 10, 20, 21, 40).foreach { n =>
      val rule = GaussHermite.rule(n)
      near(rule.weights.sum, sqrtPi, 1e-14 * sqrtPi, s"$n: Σw")
      if (n >= 2) // n nodes integrate exactly up to degree 2n − 1
        near(
          rule.nodes.indices.map(k => rule.weights(k) * rule.nodes(k) * rule.nodes(k)).sum,
          sqrtPi / 2,
          1e-13,
          s"$n: Σwt²"
        )
      rule.nodes.indices.foreach(k =>
        near(rule.nodes(k), -rule.nodes(n - 1 - k), 0.0, s"$n: symmetry")
      )
      if (n % 2 == 1)
        assert(
          java.lang.Double.doubleToRawLongBits(rule.nodes(n / 2)) == 0L,
          "the middle node is +0.0"
        )
      assert(rule.nodes.sliding(2).forall(p => p.length < 2 || p(0) < p(1)), "ascending")
    }
  }

  test("conjugate gradients solve a sparse symmetric positive-definite block") {
    val random = new scala.util.Random(3)
    val n = 40
    val rows = Array.newBuilder[Int]
    val cols = Array.newBuilder[Int]
    val vals = Array.newBuilder[Double]
    (0 until n).foreach { i =>
      rows += i; cols += i; vals += 4.0 + random.nextDouble()
      if (i + 3 < n) {
        val v = random.nextDouble() - 0.5
        rows += i; cols += i + 3; vals += v
        rows += i + 3; cols += i; vals += v
      }
    }
    val a = SparseSymmetric.fromTriplets(n, rows.result(), cols.result(), vals.result())
    val active = Array.tabulate(n)(i => i % 7 != 0)
    val b = Array.fill(n)(random.nextGaussian())
    val (x, converged) = a.solve(b, active)
    assert(converged)
    val r = a.times(x, active)
    (0 until n).filter(active).foreach(i => near(r(i), b(i), 1e-10, s"row $i"))
    (0 until n).filterNot(active).foreach(i => assertEquals(x(i), 0.0))
  }

  test("compressed cell sums equal sums over the records to rounding") {
    val random = new scala.util.Random(11)
    val offsets = Array.fill(300)(random.nextGaussian() * 1.7)
    val y = Array.fill(300)(if (random.nextDouble() < 0.3) 1.0 else 0.0)
    val bins = scala.collection.mutable.TreeMap.empty[Long, NeumaierVector]
    offsets.foreach { o =>
      val b = TaylorBins.bin(o)
      bins
        .getOrElseUpdate(b, new NeumaierVector(TaylorBins.Order + 1))
        .add(TaylorBins.rowMoments(o, 1.0, b))
    }
    val cell = ThreeStageKernel.Cell(
      0,
      0,
      y.sum,
      y.indices.map(i => y(i) * offsets(i)).sum,
      bins.keys.toArray,
      bins.values.flatMap(_.values).toArray
    )
    Seq(-3.0, -0.7, 0.0, 1.9).foreach { eta =>
      val (s1, s2, soft) = ThreeStageKernel.sums(cell, eta)
      val p = offsets.map(o => 1.0 / (1.0 + StrictMath.exp(-(o + eta))))
      near(s1, p.sum, 1e-13 * p.sum, s"Σσ at $eta")
      near(s2, p.map(v => v * (1 - v)).sum, 1e-13 * p.sum, s"Σσ′ at $eta")
      val direct = offsets.map(o => StrictMath.log1p(StrictMath.exp(o + eta))).sum
      near(soft, direct, 1e-13 * direct, s"Σ log(1 + e) at $eta")
    }
  }

  test("the bounded quasi-Newton finds interior and boundary minima") {
    val rosenbrock =
      (x: Array[Double]) => 100.0 * math.pow(x(1) - x(0) * x(0), 2) + math.pow(1.0 - x(0), 2)
    val free = BoundedQuasiNewton.minimize(
      rosenbrock,
      Array(-1.2, 1.0),
      Array(Double.NegativeInfinity, Double.NegativeInfinity)
    )
    assert(
      free.converged && math.abs(free.x(0) - 1.0) < 1e-4 && math.abs(free.x(1) - 1.0) < 1e-4,
      free.x.mkString(",")
    )
    val bowl = (x: Array[Double]) => math.pow(x(0) + 1.0, 2) + math.pow(x(1) - 2.0, 2)
    val bounded =
      BoundedQuasiNewton.minimize(bowl, Array(3.0, 3.0), Array(0.0, Double.NegativeInfinity))
    assert(
      bounded.converged && bounded.x(0) == 0.0 && math.abs(bounded.x(1) - 2.0) < 1e-6,
      bounded.x.mkString(",")
    )
  }
}
