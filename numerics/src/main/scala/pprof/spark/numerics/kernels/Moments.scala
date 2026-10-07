package pprof.spark.numerics.kernels

import pprof.spark.numerics.{NeumaierVector, Summation}

/** Column sums and cross products of rows stored row-major in a primitive array: the toy kernel of
  * the platform skeleton (D-11). Every sum runs over the rows in their stored, canonical order with
  * [[Summation.pairwise]], so results depend only on the rows and their order (ADR-0003).
  */
object Moments {

  /** Entries in the packed upper triangle of a `p` by `p` symmetric matrix. */
  def packedLength(p: Int): Int = p * (p + 1) / 2

  /** Position of entry `(i, j)`, `i <= j`, in the row-by-row packed upper triangle. */
  def packedIndex(i: Int, j: Int, p: Int): Int = {
    require(
      0 <= i && i <= j && j < p,
      s"($i, $j) is outside the upper triangle of a $p by $p matrix"
    )
    i * p - i * (i - 1) / 2 + (j - i)
  }

  /** Sums of each of the `p` columns over rows `from` to `until - 1`. */
  def columnSums(values: Array[Double], p: Int, from: Int, until: Int): Array[Double] = {
    checkRows(values, p, from, until)
    val column = new Array[Double](until - from)
    val sums = new Array[Double](p)
    var j = 0
    while (j < p) {
      var r = 0
      while (r < column.length) {
        column(r) = values((from + r) * p + j)
        r += 1
      }
      sums(j) = Summation.pairwise(column, 0, column.length)
      j += 1
    }
    sums
  }

  /** Packed sums of `x_i * x_j`, `i <= j`, over rows `from` to `until - 1`. */
  def crossProducts(values: Array[Double], p: Int, from: Int, until: Int): Array[Double] = {
    checkRows(values, p, from, until)
    val products = new Array[Double](until - from)
    val packed = new Array[Double](packedLength(p))
    var k = 0
    var i = 0
    while (i < p) {
      var j = i
      while (j < p) {
        var r = 0
        while (r < products.length) {
          val base = (from + r) * p
          products(r) = values(base + i) * values(base + j)
          r += 1
        }
        packed(k) = Summation.pairwise(products, 0, products.length)
        k += 1
        j += 1
      }
      i += 1
    }
    packed
  }

  /** Count, mean and centred co-moments Σ(x − x̄)(x − x̄)ᵀ (packed) of a set of rows. */
  final case class Centred(count: Long, mean: Array[Double], comoments: Array[Double])

  /** [[Centred]] of rows `from` until `until`: the mean, then the centred sums, both compensated, so
    * nothing cancels (Chan, Golub and LeVeque, 1983).
    */
  def centred(values: Array[Double], p: Int, from: Int, until: Int): Centred = {
    checkRows(values, p, from, until)
    val n = until - from
    val sums = new NeumaierVector(p)
    var r = from
    while (r < until) {
      var j = 0
      while (j < p) {
        sums.addAt(j, values(r * p + j))
        j += 1
      }
      r += 1
    }
    val mean = if (n == 0) new Array[Double](p) else sums.values.map(_ / n)
    val m2 = new NeumaierVector(packedLength(p))
    val d = new Array[Double](p)
    r = from
    while (r < until) {
      var j = 0
      while (j < p) {
        d(j) = values(r * p + j) - mean(j)
        j += 1
      }
      var i = 0
      var index = 0
      while (i < p) {
        j = i
        while (j < p) {
          m2.addAt(index, d(i) * d(j))
          index += 1
          j += 1
        }
        i += 1
      }
      r += 1
    }
    Centred(n.toLong, mean, m2.values)
  }

  /** The [[Centred]] moments of the union of two disjoint sets of rows (Chan, Golub and LeVeque). */
  def merge(a: Centred, b: Centred): Centred =
    if (a.count == 0L) b
    else if (b.count == 0L) a
    else {
      val p = a.mean.length
      val n = a.count + b.count
      val delta = Array.tabulate(p)(j => b.mean(j) - a.mean(j))
      val weight = a.count.toDouble * b.count.toDouble / n.toDouble
      val mean = Array.tabulate(p)(j => a.mean(j) + delta(j) * (b.count.toDouble / n.toDouble))
      val comoments = new Array[Double](packedLength(p))
      var i = 0
      var index = 0
      while (i < p) {
        var j = i
        while (j < p) {
          comoments(index) = a.comoments(index) + b.comoments(index) + delta(i) * delta(j) * weight
          index += 1
          j += 1
        }
        i += 1
      }
      Centred(n, mean, comoments)
    }

  /** Pearson correlations (packed; NaN where a column does not vary). */
  def correlations(c: Centred): Array[Double] = {
    val p = c.mean.length
    val out = new Array[Double](packedLength(p))
    var i = 0
    var index = 0
    while (i < p) {
      var j = i
      while (j < p) {
        out(index) = c.comoments(index) / math.sqrt(
          c.comoments(packedIndex(i, i, p)) * c.comoments(packedIndex(j, j, p))
        )
        index += 1
        j += 1
      }
      i += 1
    }
    out
  }

  private def checkRows(values: Array[Double], p: Int, from: Int, until: Int): Unit =
    require(
      p > 0 && 0 <= from && from <= until && until.toLong * p <= values.length,
      s"rows [$from, $until) of $p values each do not fit in ${values.length} values"
    )
}

/** An order-independent fingerprint of a data set (§6.10): the wrapping sum of a 64-bit hash of
  * every row. The same rows give the same fingerprint whatever their order, layout or partitioning.
  */
object Fingerprint {

  /** Hash of one row: its group index, row identifier and `p` values starting at `offset`. */
  def row(groupIndex: Int, rowId: Long, values: Array[Double], offset: Int, p: Int): Long = {
    var h = mix(groupIndex.toLong ^ 0x9e3779b97f4a7c15L)
    h = mix(h ^ rowId)
    var j = 0
    while (j < p) {
      h = mix(h ^ java.lang.Double.doubleToLongBits(values(offset + j)))
      j += 1
    }
    h
  }

  /** The SplitMix64 finalizer (Steele, Lea and Flood, 2014). */
  def mix(x: Long): Long = {
    var z = x
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }
}
