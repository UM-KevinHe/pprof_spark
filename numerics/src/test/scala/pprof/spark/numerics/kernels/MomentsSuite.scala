package pprof.spark.numerics.kernels

class MomentsSuite extends munit.FunSuite {

  private val p = 3

  /** Five rows of small integers, so every sum below is exact. */
  private val values = Array[Double](1, 2, 3, 4, 5, 6, -7, 8, 9, 10, -11, 12, 13, 14, 15)
  private val rows = values.grouped(p).toSeq

  test("packed indices enumerate the upper triangle row by row") {
    val pairs = for (i <- 0 until 4; j <- i until 4) yield (i, j)
    assertEquals(pairs.map { case (i, j) => Moments.packedIndex(i, j, 4) }, 0 until 10)
    assertEquals(Moments.packedLength(4), 10)
    intercept[IllegalArgumentException](Moments.packedIndex(2, 1, 4))
  }

  test("column sums and cross products are exact for small integers") {
    val sums = (0 until p).map(j => rows.map(_(j)).sum)
    val cross = for (i <- 0 until p; j <- i until p) yield rows.map(r => r(i) * r(j)).sum
    assertEquals(Moments.columnSums(values, p, 0, 5).toSeq, sums)
    assertEquals(Moments.crossProducts(values, p, 0, 5).toSeq, cross)
  }

  test("row ranges select rows") {
    val sums = (0 until p).map(j => rows.slice(1, 3).map(_(j)).sum)
    assertEquals(Moments.columnSums(values, p, 1, 3).toSeq, sums)
    assertEquals(Moments.columnSums(values, p, 2, 2).toSeq, Seq(0.0, 0.0, 0.0))
    intercept[IllegalArgumentException](Moments.columnSums(values, p, 0, 6))
    intercept[IllegalArgumentException](Moments.crossProducts(values, p, 3, 2))
  }

  test("row fingerprints see every bit of every field") {
    val base = Fingerprint.row(1, 7L, values, 3, p)
    val changed = values.clone()
    changed(4) = Math.nextUp(changed(4))
    assertNotEquals(Fingerprint.row(1, 7L, changed, 3, p), base, "one ulp in a value")
    assertNotEquals(Fingerprint.row(2, 7L, values, 3, p), base, "group index")
    assertNotEquals(Fingerprint.row(1, 8L, values, 3, p), base, "row identifier")
    assertEquals(Fingerprint.row(1, 7L, values.clone(), 3, p), base)
  }

  test("centred co-moments merge exactly as a direct computation, and give correlations (X-021)") {
    val random = new scala.util.Random(17L)
    val p = 3
    val rows = 40
    val values = Array.tabulate(rows * p) { i =>
      val r = i / p
      if (i % p == 2) 2.0 * r / 7.0 + 100.0
      else math.rint(random.nextGaussian() * 64) / 64 + 100.0 * (i % p)
    }
    val whole = Moments.centred(values, p, 0, rows)
    val merged =
      Moments.merge(Moments.centred(values, p, 0, 13), Moments.centred(values, p, 13, rows))
    assertEquals(merged.count, rows.toLong)
    whole.comoments.indices.foreach { k =>
      val scale = whole.comoments.map(math.abs).max
      assert(math.abs(merged.comoments(k) - whole.comoments(k)) <= 1e-12 * scale, s"co-moment $k")
    }
    val r = Moments.correlations(whole)
    assert(math.abs(r(Moments.packedIndex(0, 0, p)) - 1.0) <= 1e-15)
    assert(math.abs(r(Moments.packedIndex(0, 1, p))) < 0.9)
    assertEquals(
      Moments.merge(whole, Moments.Centred(0L, new Array[Double](p), new Array[Double](6))),
      whole
    )
  }
}
