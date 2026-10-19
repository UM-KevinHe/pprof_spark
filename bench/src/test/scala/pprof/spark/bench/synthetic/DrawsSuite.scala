package pprof.spark.bench.synthetic

/** The counter-based draws behind every synthetic value (scale-test plan §4). */
class DrawsSuite extends munit.FunSuite {

  private def correlation(a: Array[Double], b: Array[Double]): Double = {
    val (ma, mb) = (a.sum / a.length, b.sum / b.length)
    var (sab, saa, sbb) = (0.0, 0.0, 0.0)
    a.indices.foreach { i =>
      sab += (a(i) - ma) * (b(i) - mb); saa += (a(i) - ma) * (a(i) - ma);
      sbb += (b(i) - mb) * (b(i) - mb)
    }
    sab / math.sqrt(saa * sbb)
  }

  test("the bits are SplitMix64's, as an independent implementation computes them") {
    assertEquals(Draws.bits(0L, 0L, 0, 0), -4090968392165319293L)
    assertEquals(Draws.bits(79L, 12345L, 7, 3), 7985377396730132236L)
    assertEquals(
      Draws.uniform(79L, 12345L, 2),
      java.lang.Double.parseDouble("0x1.814af56556bfep-3")
    )
    assert(math.abs(Draws.normal(79L, 12345L, 1002) - -0.457157236078445) <= 1e-15)
  }

  test("uniforms lie strictly inside (0, 1), with the uniform distribution's mean and variance") {
    val n = 100000
    val u = Array.tabulate(n)(i => Draws.uniform(79L, i.toLong, 2))
    assert(u.forall(x => x > 0.0 && x < 1.0))
    val mean = u.sum / n
    val variance = u.map(x => (x - mean) * (x - mean)).sum / (n - 1)
    assert(math.abs(mean - 0.5) <= 4.5 * math.sqrt(1.0 / 12.0 / n), s"mean $mean")
    assert(
      math.abs(variance - 1.0 / 12.0) <= 4.5 * math.sqrt(1.0 / 180.0 / n),
      s"variance $variance"
    )
  }

  test("other streams, draws, seeds and neighbouring identifiers give uncorrelated uniforms") {
    val n = 100000
    val base = Array.tabulate(n)(i => Draws.uniform(79L, i.toLong, 2))
    Seq(
      "stream" -> Array.tabulate(n)(i => Draws.uniform(79L, i.toLong, 3)),
      "draw" -> Array.tabulate(n)(i => Draws.uniform(79L, i.toLong, 2, 1)),
      "seed" -> Array.tabulate(n)(i => Draws.uniform(80L, i.toLong, 2)),
      "neighbour" -> Array.tabulate(n)(i => Draws.uniform(79L, i.toLong + 1L, 2))
    ).foreach { case (what, other) =>
      val r = correlation(base, other)
      assert(math.abs(r) <= 4.5 / math.sqrt(n.toDouble), s"$what: correlation $r")
    }
  }

  test("normals have mean 0, variance 1 and the normal distribution's two-sided 5% tail") {
    val n = 200000
    val z = Array.tabulate(n)(i => Draws.normal(79L, i.toLong, 1002))
    val mean = z.sum / n
    val variance = z.map(x => (x - mean) * (x - mean)).sum / (n - 1)
    val tail = z.count(x => math.abs(x) > 1.959963984540054).toDouble / n
    assert(math.abs(mean) <= 4.5 / math.sqrt(n.toDouble), s"mean $mean")
    assert(math.abs(variance - 1.0) <= 4.5 * math.sqrt(2.0 / n), s"variance $variance")
    assert(math.abs(tail - 0.05) <= 4.5 * math.sqrt(0.05 * 0.95 / n), s"tail $tail")
  }
}
