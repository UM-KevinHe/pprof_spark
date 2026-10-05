package pprof.spark.numerics

class SummationSuite extends munit.FunSuite {
  import SummationSuite._

  test("Neumaier recovers what sequential summation loses") {
    val xs = Seq(1.0, 1e100, 1.0, -1e100)
    assertEquals(bits(xs.foldLeft(0.0)(_ + _)), bits(0.0))
    assertEquals(bits(Summation.neumaier(xs)), bits(2.0))
  }

  test("pairwise and Neumaier match the independent Python reference bit for bit") {
    assertEquals(bits(Summation.pairwise(Data)), PairwiseBits, "pairwise")
    assertEquals(bits(Summation.neumaier(Data)), NeumaierBits, "neumaier")
  }

  test("errors stay within their documented bounds against the exactly rounded sum") {
    val exact = java.lang.Double.longBitsToDouble(ExactBits)
    val magnitude = java.lang.Double.longBitsToDouble(AbsSumBits)
    val u = Math.ulp(1.0) / 2
    val depth = Iterator.from(0).find(d => (Summation.PairwiseLeafSize.toLong << d) >= Size).get
    val pairwiseBound = (Summation.PairwiseLeafSize + depth) * u * magnitude
    val neumaierBound = 2 * u * math.abs(exact) + 2 * Size * u * u * magnitude
    assert(math.abs(Summation.pairwise(Data) - exact) <= pairwiseBound)
    assert(math.abs(Summation.neumaier(Data) - exact) <= neumaierBound)
  }

  test("negative control: this data distinguishes summation order and leaf size") {
    assertNotEquals(bits(Data.foldLeft(0.0)(_ + _)), PairwiseBits, "sequential summation")
    assertNotEquals(bits(pairwiseWithLeaf(Data, 0, Data.length, 16)), PairwiseBits, "leaf size 16")
  }

  test("non-finite values follow IEEE arithmetic") {
    assertEquals(
      bits(Summation.neumaier(Seq(1.0, Double.PositiveInfinity))),
      bits(Double.PositiveInfinity)
    )
    assert(Summation.neumaier(Seq(Double.PositiveInfinity, Double.NegativeInfinity)).isNaN)
    assert(Summation.neumaier(Seq(Double.NaN, 1.0)).isNaN)
    assertEquals(
      bits(Summation.neumaier(Seq(Double.MaxValue, Double.MaxValue))),
      bits(Double.PositiveInfinity)
    )
    assertEquals(
      bits(Summation.pairwise(Array(1.0, Double.PositiveInfinity))),
      bits(Double.PositiveInfinity)
    )
  }

  test("empty, single-value and sub-range sums") {
    assertEquals(bits(Summation.pairwise(Array.empty[Double])), bits(0.0))
    assertEquals(bits(Summation.pairwise(Array(-0.0))), bits(-0.0))
    assertEquals(bits(Summation.neumaier(Seq.empty[Double])), bits(0.0))
    assertEquals(
      bits(Summation.pairwise(Data, 123, 4567)),
      bits(Summation.pairwise(Data.slice(123, 4567)))
    )
    intercept[IllegalArgumentException](Summation.pairwise(Data, 5, 3))
    intercept[IllegalArgumentException](Summation.pairwise(Data, 0, Data.length + 1))
  }

  test("NeumaierVector sums each element exactly like NeumaierSum") {
    val vectors = (0 until 50).map(k => Array.tabulate(7)(j => Data((k * 7 + j) * 13 % Size)))
    val sums = new NeumaierVector(7)
    vectors.foreach(sums.add)
    val expected = (0 until 7).map(j => bits(Summation.neumaier(vectors.map(_(j)))))
    assertEquals(sums.values.map(bits).toSeq, expected)
    intercept[IllegalArgumentException](sums.add(new Array[Double](6)))
  }
}

object SummationSuite {
  val Size: Int = 10000

  /** `x(i) = (h(i) / 2^32 - 1/2) * 2^e(i)` with integer hashes `h` and `e`, so the data set is
    * exactly the same in reference/numerics/summation_reference.py, which produced the constants.
    */
  val Data: Array[Double] = Array.tabulate(Size) { i =>
    val h = (i.toLong * 2654435761L) & 0xffffffffL
    val e = ((i.toLong * 37L) % 81L).toInt - 40
    Math.scalb(h.toDouble / 4294967296.0 - 0.5, e)
  }

  val PairwiseBits: Long = 0x4252d6a3dda2570dL
  val NeumaierBits: Long = 0x4252d6a3dda2571aL
  val ExactBits: Long = 0x4252d6a3dda2571aL // math.fsum: the exactly rounded sum
  val AbsSumBits: Long = 0x42cf1fe905ec0a51L // math.fsum of absolute values

  def bits(x: Double): Long = java.lang.Double.doubleToRawLongBits(x)

  /** The pairwise rule with another leaf size, for the negative control only. */
  def pairwiseWithLeaf(xs: Array[Double], from: Int, until: Int, leaf: Int): Double = {
    val n = until - from
    if (n == 0) 0.0
    else if (n <= leaf) (from + 1 until until).foldLeft(xs(from))((acc, i) => acc + xs(i))
    else {
      val mid = from + n / 2
      pairwiseWithLeaf(xs, from, mid, leaf) + pairwiseWithLeaf(xs, mid, until, leaf)
    }
  }
}
