package pprof.spark.numerics

/** Deterministic floating-point summation (PROJECT_CONTEXT §6.4 and §6.8; ADR-0003).
  *
  * Results depend only on the values and their order, never on threads, partitions or timing, so
  * they are reproducible bit for bit (NN-4, level R0). The algorithms are part of that contract:
  * changing [[Summation.PairwiseLeafSize]], the split rule or the compensation formula changes
  * results, and is a behavioral change that needs R0 evidence and a negative control (NUM-2).
  *
  *  - [[Summation.pairwise]] sums within a block: cascade summation whose error grows like
  *    `O(eps * log n)` times the sum of magnitudes.
  *  - [[NeumaierSum]] and [[NeumaierVector]] combine per-block partials in block order (Neumaier,
  *    1974): the error is about `2 * eps * |sum|` plus second-order terms, whatever the number of
  *    partials.
  */
object Summation {

  /** Longest range that [[pairwise]] sums sequentially instead of splitting. */
  val PairwiseLeafSize: Int = 32

  /** Pairwise sum of `xs(from)` to `xs(until - 1)`. Ranges of at most [[PairwiseLeafSize]] values
    * are summed left to right starting from their first value; longer ranges are split at
    * `from + (until - from) / 2` and the two halves added. An empty range sums to `+0.0`.
    */
  def pairwise(xs: Array[Double], from: Int, until: Int): Double = {
    require(
      0 <= from && from <= until && until <= xs.length,
      s"invalid range [$from, $until) for ${xs.length} values"
    )
    sumRange(xs, from, until)
  }

  /** Pairwise sum of every value of `xs`; see the range overload. */
  def pairwise(xs: Array[Double]): Double = sumRange(xs, 0, xs.length)

  /** Neumaier-compensated sum of `xs` in iteration order; `+0.0` when empty. */
  def neumaier(xs: IterableOnce[Double]): Double = {
    val sum = new NeumaierSum
    xs.iterator.foreach(sum.add)
    sum.value
  }

  private def sumRange(xs: Array[Double], from: Int, until: Int): Double = {
    val n = until - from
    if (n == 0) 0.0
    else if (n <= PairwiseLeafSize) {
      var acc = xs(from)
      var i = from + 1
      while (i < until) {
        acc += xs(i)
        i += 1
      }
      acc
    } else {
      val mid = from + n / 2
      sumRange(xs, from, mid) + sumRange(xs, mid, until)
    }
  }
}

/** A Neumaier-compensated running sum. Once the running sum becomes infinite or NaN,
  * compensation stops and IEEE arithmetic decides the result. Mutable: confine an instance to one
  * thread and one ordered reduction.
  */
final class NeumaierSum {
  private var sum = 0.0
  private var compensation = 0.0

  def add(x: Double): Unit = {
    val t = sum + x
    if (java.lang.Double.isFinite(t)) {
      if (math.abs(sum) >= math.abs(x)) compensation += (sum - t) + x
      else compensation += (x - t) + sum
    }
    sum = t
  }

  def value: Double = if (java.lang.Double.isFinite(sum)) sum + compensation else sum

  /** Starts again from zero. */
  def reset(): Unit = {
    sum = 0.0
    compensation = 0.0
  }
}

/** Element-wise Neumaier sums of equal-length vectors, such as score vectors or packed symmetric
  * information matrices reduced in block order (§6.8). Each element behaves exactly like a
  * [[NeumaierSum]]. Mutable: confine an instance to one thread and one ordered reduction.
  */
final class NeumaierVector(val length: Int) {
  require(length >= 0, s"length must be non-negative, got $length")

  private val sums = new Array[Double](length)
  private val compensations = new Array[Double](length)

  def add(xs: Array[Double]): Unit = {
    require(xs.length == length, s"expected $length values, got ${xs.length}")
    var i = 0
    while (i < length) {
      addAt(i, xs(i))
      i += 1
    }
  }

  /** Adds `x` to element `i` alone, as running sums in a kernel do. */
  def addAt(i: Int, x: Double): Unit = {
    val s = sums(i)
    val t = s + x
    if (java.lang.Double.isFinite(t)) {
      if (math.abs(s) >= math.abs(x)) compensations(i) += (s - t) + x
      else compensations(i) += (x - t) + s
    }
    sums(i) = t
  }

  /** The current sum of element `i`. */
  def valueAt(i: Int): Double =
    if (java.lang.Double.isFinite(sums(i))) sums(i) + compensations(i) else sums(i)

  /** The current sums, as a new array. */
  def values: Array[Double] = Array.tabulate(length)(valueAt)
}
