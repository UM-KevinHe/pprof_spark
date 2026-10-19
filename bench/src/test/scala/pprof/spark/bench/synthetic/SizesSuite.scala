package pprof.spark.bench.synthetic

/** How rows divide among providers (scale-test plan §4). */
class SizesSuite extends munit.FunSuite {

  test("equal sizes differ by at most one, the larger first, and sum to n") {
    assertEquals(Sizes.Equal.sizes(103L, 10).toSeq, Seq.fill(3)(11L) ++ Seq.fill(7)(10L))
  }

  test(
    "a dominant provider has its share of the rows, rounded, and the others share the rest equally"
  ) {
    val s = Sizes.Dominant(0.05).sizes(10000L, 11)
    assertEquals(s.toSeq, 500L +: Seq.fill(10)(950L))
  }

  test("Zipf sizes fall with rank, give every provider a row, sum to n and follow rank^−exponent") {
    val (n, m) = (1000000L, 1000)
    val s = Sizes.Zipf(1.1).sizes(n, m)
    assertEquals(s.sum, n)
    assert(s.forall(_ >= 1L) && s.sliding(2).forall(w => w(0) >= w(1)))
    val weights = (1 to m).map(k => StrictMath.pow(k.toDouble, -1.1))
    val total = weights.sum
    s.indices.foreach { j =>
      val exact = 1.0 + (n - m).toDouble * weights(j) / total
      assert(math.abs(s(j) - exact) < 1.0, s"provider $j: ${s(j)} rows for $exact")
    }
  }

  test("each row belongs to the provider whose range holds it") {
    val starts = Sizes.starts(Sizes.Zipf(0.7).sizes(5000L, 37))
    assertEquals((starts.length, starts.last), (38, 5000L))
    (0L until 5000L).foreach { id =>
      val j = Sizes.providerOf(starts, id)
      assert(starts(j) <= id && id < starts(j + 1), s"row $id in provider $j")
    }
    intercept[IllegalArgumentException](Sizes.providerOf(starts, 5000L))
  }

  test("sizes parse from text, and impossible designs fail clearly") {
    assertEquals(Sizes.parse("equal"), Sizes.Equal)
    assertEquals(Sizes.parse("dominant:0.05"), Sizes.Dominant(0.05))
    assertEquals(Sizes.parse("zipf:0.7"), Sizes.Zipf(0.7))
    intercept[IllegalArgumentException](Sizes.parse("lognormal"))
    intercept[IllegalArgumentException](Sizes.Equal.sizes(5L, 6))
    intercept[IllegalArgumentException](Sizes.Dominant(0.05).sizes(10L, 1))
    intercept[IllegalArgumentException](Design(Model.Linear(), n = 10L, m = 3, p = 0))
  }
}
