package pprof.spark.numerics

import pprof.spark.numerics.kernels.Moments

/** Cholesky factorization of a symmetric matrix with R's aliasing rule (Cox specification §5).
  *
  * Column j is aliased when its pivot, the diagonal entry left after the earlier columns are
  * eliminated, is not above `tolerance` times the largest diagonal entry; R's `coxph` uses the same
  * rule with `toler.chol`. An aliased column is left at zero and elimination continues, so every
  * aliased column is reported at once. Loops run in a fixed order, so results are bitwise
  * reproducible.
  */
object Cholesky {

  /** R's `toler.chol` default, `.Machine$double.eps^0.75`, which is exactly 2^-39. */
  val DefaultTolerance: Double = 1.8189894035458565e-12

  /** Factors the symmetric matrix whose upper triangle is `packed`, row by row (§6.8). */
  def factor(
      packed: Array[Double],
      p: Int,
      tolerance: Double = DefaultTolerance
  ): CholeskyFactor = {
    require(p >= 1, s"dimension must be at least 1, got $p")
    require(
      packed.length == Moments.packedLength(p),
      s"expected ${Moments.packedLength(p)} packed values for dimension $p, got ${packed.length}"
    )
    require(tolerance >= 0.0, s"tolerance must be non-negative, got $tolerance")
    val a = new Array[Double](p * p)
    var i = 0
    var k = 0
    while (i < p) {
      var j = i
      while (j < p) {
        a(i * p + j) = packed(k)
        a(j * p + i) = packed(k)
        k += 1
        j += 1
      }
      i += 1
    }
    var maxDiagonal = 0.0
    i = 0
    while (i < p) {
      if (a(i * p + i) > maxDiagonal) maxDiagonal = a(i * p + i)
      i += 1
    }
    val threshold = tolerance * maxDiagonal
    val lower = new Array[Double](p * p)
    val aliased = Vector.newBuilder[Int]
    var j = 0
    while (j < p) {
      var pivot = a(j * p + j)
      k = 0
      while (k < j) {
        pivot -= lower(j * p + k) * lower(j * p + k)
        k += 1
      }
      if (!(pivot > threshold)) aliased += j
      else {
        val root = math.sqrt(pivot)
        lower(j * p + j) = root
        i = j + 1
        while (i < p) {
          var s = a(i * p + j)
          k = 0
          while (k < j) {
            s -= lower(i * p + k) * lower(j * p + k)
            k += 1
          }
          lower(i * p + j) = s / root
          i += 1
        }
      }
      j += 1
    }
    new CholeskyFactor(p, lower, aliased.result())
  }
}

/** A lower-triangular factor L with A = L·Lᵀ, and the columns found aliased. */
final class CholeskyFactor private[numerics] (
    val dimension: Int,
    lower: Array[Double],
    val aliased: Vector[Int]
) {

  def isFullRank: Boolean = aliased.isEmpty

  /** The pivots, the factor's diagonal squared, in column order; 0 for an aliased column. Their range gives a
    * condition estimate.
    */
  def pivots: Array[Double] =
    Array.tabulate(dimension)(j => lower(j * dimension + j) * lower(j * dimension + j))

  /** Solves A·x = b. */
  def solve(b: Array[Double]): Array[Double] = {
    requireFullRank()
    val p = dimension
    require(b.length == p, s"expected $p values, got ${b.length}")
    val y = new Array[Double](p)
    var i = 0
    while (i < p) {
      var s = b(i)
      var k = 0
      while (k < i) {
        s -= lower(i * p + k) * y(k)
        k += 1
      }
      y(i) = s / lower(i * p + i)
      i += 1
    }
    val x = new Array[Double](p)
    i = p - 1
    while (i >= 0) {
      var s = y(i)
      var k = i + 1
      while (k < p) {
        s -= lower(k * p + i) * x(k)
        k += 1
      }
      x(i) = s / lower(i * p + i)
      i -= 1
    }
    x
  }

  /** A⁻¹ as a packed upper triangle, computed as (L⁻¹)ᵀ·L⁻¹ so that it is exactly symmetric. */
  def inversePacked: Array[Double] = {
    requireFullRank()
    val p = dimension
    val inv = new Array[Double](p * p)
    var j = 0
    while (j < p) {
      inv(j * p + j) = 1.0 / lower(j * p + j)
      var i = j + 1
      while (i < p) {
        var s = 0.0
        var k = j
        while (k < i) {
          s -= lower(i * p + k) * inv(k * p + j)
          k += 1
        }
        inv(i * p + j) = s / lower(i * p + i)
        i += 1
      }
      j += 1
    }
    val out = new Array[Double](Moments.packedLength(p))
    var index = 0
    var r = 0
    while (r < p) {
      var c = r
      while (c < p) {
        var s = 0.0
        var k = c
        while (k < p) {
          s += inv(k * p + r) * inv(k * p + c)
          k += 1
        }
        out(index) = s
        index += 1
        c += 1
      }
      r += 1
    }
    out
  }

  private def requireFullRank(): Unit =
    if (!isFullRank)
      throw new IllegalStateException(s"the matrix is aliased in columns ${aliased.mkString(", ")}")
}
