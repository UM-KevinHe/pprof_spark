package pprof.spark.numerics.kernels

import pprof.spark.numerics.Summation

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
