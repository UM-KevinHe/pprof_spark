package pprof.spark.numerics

/** Exact Poisson-binomial tails, tails to z, test inversion and the bootstrap of the logistic provider
  * tests (Phase 2c specification §2 and §3). Tails against mpmath are checked in the engine's
  * `LogisticProviderTestsSuite` with the fixtures.
  */
class PoissonBinomialSuite extends munit.FunSuite {

  private val probabilities = Array(0.12, 0.47, 0.83, 0.05, 0.6, 0.31)
  private val trials = Array(1, 2, 1, 3, 1, 1)

  /** The distribution of the count by enumerating every outcome of every trial. */
  private def bruteForce(p: Array[Double], n: Array[Int]): Array[Double] = {
    val expanded = p.indices.flatMap(i => Seq.fill(n(i))(PoissonBinomial.clip(p(i)))).toArray
    val pmf = new Array[Double](expanded.length + 1)
    (0 until (1 << expanded.length)).foreach { mask =>
      var probability = 1.0
      var events = 0
      expanded.indices.foreach { t =>
        if ((mask >> t & 1) == 1) {
          probability *= expanded(t)
          events += 1
        } else probability *= 1.0 - expanded(t)
      }
      pmf(events) += probability
    }
    pmf
  }

  private def close(actual: Double, expected: Double, what: String): Unit =
    assert(
      math.abs(actual - expected) <= 1e-13 * expected + 1e-300,
      s"$what: $actual against $expected"
    )

  test("the recursion's tails equal enumeration at every count, with binomial trials") {
    val pmf = bruteForce(probabilities, trials)
    pmf.indices.foreach { o =>
      val t = PoissonBinomial.tails(probabilities, trials, o)
      val below = pmf.take(o).sum
      val above = pmf.drop(o + 1).sum
      close(t.below, below, s"P(X < $o)")
      close(t.above, above, s"P(X > $o)")
      close(t.atLeast, above + pmf(o), s"P(X >= $o)")
      close(t.atMost, below + pmf(o), s"P(X <= $o)")
      close(t.upperMid, above + pmf(o) / 2, s"upper mid-p at $o")
      close(t.lowerMid, below + pmf(o) / 2, s"lower mid-p at $o")
      assert(math.abs(t.upperMid + t.lowerMid - 1.0) <= 1e-15)
    }
  }

  test("binomial trials give the tails of the repeated probabilities, bit for bit") {
    val expanded = probabilities.indices.flatMap(i => Seq.fill(trials(i))(probabilities(i))).toArray
    (0 to trials.sum).foreach { o =>
      assertEquals(
        PoissonBinomial.tails(probabilities, trials, o),
        PoissonBinomial.tails(expanded, Array.fill(expanded.length)(1), o)
      )
    }
    intercept[IllegalArgumentException](
      PoissonBinomial.tails(probabilities, trials, trials.sum + 1)
    )
  }

  test("z reproduces the tails, reads near-one tails from their complements and is capped") {
    val two = PoissonBinomial.Tails(0.025, 0.975, 0.03, 0.98, 0.02, 0.97)
    assert(
      math.abs(
        PoissonBinomial.z(two, "two_sided", PoissonBinomial.TailFloor) - 1.959963984540054
      ) < 1e-12
    )
    val nearOne = PoissonBinomial.Tails(1e-20, 1.0, 1e-20, 1.0, 1e-20, 1.0 - 1e-20)
    val less = PoissonBinomial.z(nearOne, "less", PoissonBinomial.TailFloor)
    assert(math.abs(less - Normal.upperQuantile(1e-20)) < 1e-9, s"less: $less")
    val greater = PoissonBinomial.z(nearOne, "greater", PoissonBinomial.TailFloor)
    assertEquals(greater, PoissonBinomial.z(nearOne, "two_sided", PoissonBinomial.TailFloor))
    val zero = PoissonBinomial.Tails(0.0, 1.0, 0.0, 1.0, 0.0, 1.0)
    assertEquals(
      PoissonBinomial.z(zero, "two_sided", PoissonBinomial.TailFloor),
      Normal.upperQuantile(1e-300)
    )
    intercept[IllegalArgumentException](PoissonBinomial.z(two, "both", PoissonBinomial.TailFloor))
  }

  test("inversion solves z(g) = target from the start outward, with ±∞ beyond the span") {
    val z = (g: Double) => 3.0 - g
    assert(math.abs(PoissonBinomial.invertDecreasing(z, 0.0, 1.0) - 2.0) < 1e-9)
    assert(math.abs(PoissonBinomial.invertDecreasing(z, 0.0, 7.5) - -4.5) < 1e-9)
    assertEquals(PoissonBinomial.invertDecreasing(z, 0.0, -100.0), Double.PositiveInfinity)
    assertEquals(PoissonBinomial.invertDecreasing(z, 0.0, 100.0), Double.NegativeInfinity)
    assert(PoissonBinomial.invertDecreasing(z, 0.0, Double.NaN).isNaN)
  }

  test(
    "the bootstrap is a function of (seed, provider, replicate, trial) and near the exact tails"
  ) {
    val a = PoissonBinomial.bootstrapTails(probabilities, trials, 3, 20000, 11L, 4L)
    assertEquals(PoissonBinomial.bootstrapTails(probabilities, trials, 3, 20000, 11L, 4L), a)
    assert(PoissonBinomial.bootstrapTails(probabilities, trials, 3, 20000, 11L, 5L) != a)
    val exact = PoissonBinomial.tails(probabilities, trials, 3)
    Seq(a.upperMid -> exact.upperMid, a.atLeast -> exact.atLeast, a.atMost -> exact.atMost)
      .foreach { case (simulated, e) =>
        assert(
          math.abs(simulated - e) <= 4.0 * math.sqrt(e * (1.0 - e) / 20000.0) + 1.0 / 20000.0,
          s"$simulated vs $e"
        )
      }
    val u = PoissonBinomial.uniform(1L, 2L, 3, 4L)
    assert(u >= 0.0 && u < 1.0)
  }
}
