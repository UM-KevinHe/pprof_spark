package pprof.spark.numerics

/** Binned Taylor sums of the logistic function (Phase 2d specification §3, X-026). */
class TaylorBinsSuite extends munit.FunSuite {

  private def sigma(x: Double): Double = 1.0 / (1.0 + StrictMath.exp(-x))

  private def near(a: Double, b: Double, tolerance: Double): Unit =
    assert(math.abs(a - b) <= tolerance, s"$a against $b")

  test("the derivative polynomials give the logistic function's derivatives") {
    Seq(-3.0, -0.4, 0.0, 1.7).foreach { x =>
      val s = sigma(x)
      near(TaylorBins.derivative(0, s), s, 0.0)
      near(TaylorBins.derivative(1, s), s * (1 - s), 1e-16)
      near(TaylorBins.derivative(2, s), s * (1 - s) * (1 - 2 * s), 1e-16)
      val h = 1e-4
      val numeric =
        (TaylorBins.derivative(4, sigma(x + h)) - TaylorBins.derivative(4, sigma(x - h))) / (2 * h)
      near(TaylorBins.derivative(5, s), numeric, 1e-6)
    }
  }

  test("binned sums equal direct sums to rounding over a wide spread of η and weights") {
    val random = new scala.util.Random(17)
    val etas = Array.fill(5000)(random.nextGaussian() * 2.5)
    val weights = Array.fill(5000)((1 + random.nextInt(3)).toDouble)
    val bins = scala.collection.mutable.TreeMap.empty[Long, NeumaierVector]
    etas.indices.foreach { i =>
      val b = TaylorBins.bin(etas(i))
      bins
        .getOrElseUpdate(b, new NeumaierVector(TaylorBins.Order + 1))
        .add(TaylorBins.rowMoments(etas(i), weights(i), b))
    }
    val keys = bins.keys.toArray
    val moments = bins.values.map(_.values).toArray
    Seq(-6.0, -1.2, 0.0, 2.3).foreach { g =>
      val (d, s) = TaylorBins.sums(g, keys, moments)
      val exactD = new NeumaierSum
      val exactS = new NeumaierSum
      etas.indices.foreach { i =>
        val p = sigma(g + etas(i))
        exactD.add(weights(i) * p)
        exactS.add(weights(i) * p * (1 - p))
      }
      assert(
        math.abs(d - exactD.value) <= 1e-13 * exactD.value,
        s"direct at $g: $d vs ${exactD.value}"
      )
      assert(
        math.abs(s - exactS.value) <= 1e-13 * exactS.value,
        s"variance at $g: $s vs ${exactS.value}"
      )
    }
  }
}
