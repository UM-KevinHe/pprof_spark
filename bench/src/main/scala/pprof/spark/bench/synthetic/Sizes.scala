package pprof.spark.bench.synthetic

/** How the rows divide among providers (scale-test plan §4). Provider j owns the contiguous row identifiers from
  * `starts(j)` up to `starts(j + 1)`, computed on the driver; every provider has at least one row.
  */
sealed abstract class Sizes extends Product with Serializable {

  /** The m sizes, each at least 1, summing to n. */
  def sizes(n: Long, m: Int): Array[Long]

  /** The distribution and its parameters, for the manifest. */
  def describe: Seq[(String, Any)]
}

object Sizes {

  /** n / m rows each; the first n mod m providers have one more. */
  case object Equal extends Sizes {
    def sizes(n: Long, m: Int): Array[Long] = {
      check(n, m)
      Array.tabulate(m)(j => n / m + (if (j < n % m) 1L else 0L))
    }
    def describe: Seq[(String, Any)] = Seq("distribution" -> "equal")
  }

  /** Provider 0 has `share` of the rows, rounded; the others divide the rest equally. */
  final case class Dominant(share: Double) extends Sizes {
    require(share > 0.0 && share < 1.0, s"the dominant share must lie in (0, 1), got $share")
    def sizes(n: Long, m: Int): Array[Long] = {
      check(n, m)
      require(m >= 2, "a dominant provider needs at least two providers")
      val first = math.max(1L, math.round(share * n))
      require(
        n - first >= m - 1L,
        s"$n rows leave less than one row for each of the other ${m - 1} providers"
      )
      first +: Equal.sizes(n - first, m - 1)
    }
    def describe: Seq[(String, Any)] = Seq("distribution" -> "dominant", "share" -> share)
  }

  /** Sizes proportional to rank^−exponent, provider 0 having rank 1. Every provider has one row; the other n − m
    * rows are shared in proportion to the weights, by largest remainders, ties going to the lower rank.
    */
  final case class Zipf(exponent: Double) extends Sizes {
    require(
      exponent > 0.0 && !exponent.isInfinite,
      s"the exponent must be positive and finite, got $exponent"
    )
    def sizes(n: Long, m: Int): Array[Long] = {
      check(n, m)
      val weights = Array.tabulate(m)(j => StrictMath.pow(j + 1.0, -exponent))
      var total = 0.0
      weights.foreach(w => total += w)
      val rest = n - m
      val exact = weights.map(w => w / total * rest.toDouble)
      val base = exact.map(x => math.floor(x).toLong)
      val left = (rest - base.sum).toInt
      val order = (0 until m).sortBy(j => (-(exact(j) - base(j).toDouble), j))
      order.take(left).foreach(j => base(j) += 1L)
      base.map(_ + 1L)
    }
    def describe: Seq[(String, Any)] = Seq("distribution" -> "zipf", "exponent" -> exponent)
  }

  /** The first identifier of each provider, then n: m + 1 values. */
  def starts(sizes: Array[Long]): Array[Long] = sizes.scanLeft(0L)(_ + _)

  /** The provider owning row `id`: the largest j with starts(j) ≤ id. */
  def providerOf(starts: Array[Long], id: Long): Int = {
    require(
      id >= 0L && id < starts(starts.length - 1),
      s"row $id lies outside [0, ${starts(starts.length - 1)})"
    )
    var lo = 0
    var hi = starts.length - 2
    while (lo < hi) {
      val mid = (lo + hi + 1) >>> 1
      if (starts(mid) <= id) lo = mid else hi = mid - 1
    }
    lo
  }

  /** Parses `equal`, `dominant:<share>` or `zipf:<exponent>`. */
  def parse(text: String): Sizes = text.split(":", 2) match {
    case Array("equal")           => Equal
    case Array("dominant", share) => Dominant(share.toDouble)
    case Array("zipf", exponent)  => Zipf(exponent.toDouble)
    case _                        =>
      throw new IllegalArgumentException(
        s"sizes must be equal, dominant:<share> or zipf:<exponent>, got $text"
      )
  }

  private def check(n: Long, m: Int): Unit = {
    require(m >= 1, s"at least one provider is required, got $m")
    require(n >= m.toLong, s"$n rows cannot give each of $m providers a row")
  }
}
