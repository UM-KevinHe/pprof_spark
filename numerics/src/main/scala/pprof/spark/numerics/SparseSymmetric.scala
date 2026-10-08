package pprof.spark.numerics

/** A symmetric matrix in coordinate form, summed into rows with sorted columns, and Jacobi-preconditioned
  * conjugate gradients on a subset of its rows and columns (Phase 2f-2 specification §4).
  */
final class SparseSymmetric private (
    val size: Int,
    rowStart: Array[Int],
    columns: Array[Int],
    values: Array[Double]
) {

  def diagonal: Array[Double] = {
    val out = new Array[Double](size)
    var r = 0
    while (r < size) {
      var k = rowStart(r)
      while (k < rowStart(r + 1)) {
        if (columns(k) == r) out(r) += values(k)
        k += 1
      }
      r += 1
    }
    out
  }

  /** y = A x restricted to `active` rows and columns (inactive entries of x are ignored, of y zero). */
  def times(x: Array[Double], active: Array[Boolean]): Array[Double] = {
    val y = new Array[Double](size)
    var r = 0
    while (r < size) {
      if (active(r)) {
        var sum = 0.0
        var k = rowStart(r)
        while (k < rowStart(r + 1)) {
          val c = columns(k)
          if (active(c)) sum += values(k) * x(c)
          k += 1
        }
        y(r) = sum
      }
      r += 1
    }
    y
  }

  /** Solves A x = b on the `active` block by Jacobi-preconditioned conjugate gradients from x = 0, to a
    * relative residual of `tolerance` or `maxIterations`; returns x (zero outside the block) and whether
    * the tolerance was reached.
    */
  def solve(
      b: Array[Double],
      active: Array[Boolean],
      tolerance: Double = 1e-12,
      maxIterations: Int = -1
  ): (Array[Double], Boolean) = {
    val limit = if (maxIterations > 0) maxIterations else math.max(2 * active.count(identity), 1)
    val d = diagonal
    val x = new Array[Double](size)
    val r = Array.tabulate(size)(i => if (active(i)) b(i) else 0.0)
    def dot(a: Array[Double], c: Array[Double]): Double = {
      var s = 0.0
      var i = 0
      while (i < size) {
        if (active(i)) s += a(i) * c(i)
        i += 1
      }
      s
    }
    val z = Array.tabulate(size)(i => if (active(i) && d(i) > 0.0) r(i) / d(i) else r(i))
    val p = z.clone()
    var rz = dot(r, z)
    val norm0 = math.sqrt(dot(r, r))
    if (norm0 == 0.0) return (x, true)
    var iteration = 0
    var converged = false
    while (!converged && iteration < limit) {
      val ap = times(p, active)
      val pap = dot(p, ap)
      if (!(pap > 0.0)) return (x, false)
      val alpha = rz / pap
      var i = 0
      while (i < size) {
        if (active(i)) {
          x(i) += alpha * p(i)
          r(i) -= alpha * ap(i)
        }
        i += 1
      }
      converged = math.sqrt(dot(r, r)) <= tolerance * norm0
      if (!converged) {
        i = 0
        while (i < size) {
          if (active(i)) z(i) = if (d(i) > 0.0) r(i) / d(i) else r(i)
          i += 1
        }
        val rzNew = dot(r, z)
        val beta = rzNew / rz
        rz = rzNew
        i = 0
        while (i < size) {
          if (active(i)) p(i) = z(i) + beta * p(i)
          i += 1
        }
      }
      iteration += 1
    }
    (x, converged)
  }
}

object SparseSymmetric {

  /** Builds the matrix from (row, column, value) triplets; duplicates are summed in triplet order. */
  def fromTriplets(
      size: Int,
      rows: Array[Int],
      cols: Array[Int],
      values: Array[Double]
  ): SparseSymmetric = {
    val order = rows.indices.sortBy(k => (rows(k), cols(k))).toArray
    val starts = new Array[Int](size + 1)
    val outColumns = Array.newBuilder[Int]
    val outValues = Array.newBuilder[Double]
    var count = 0
    var k = 0
    var row = 0
    while (k < order.length) {
      val r = rows(order(k))
      val c = cols(order(k))
      while (row < r) {
        row += 1
        starts(row) = count
      }
      var sum = 0.0
      while (k < order.length && rows(order(k)) == r && cols(order(k)) == c) {
        sum += values(order(k))
        k += 1
      }
      outColumns += c
      outValues += sum
      count += 1
    }
    while (row < size) {
      row += 1
      starts(row) = count
    }
    new SparseSymmetric(size, starts, outColumns.result(), outValues.result())
  }
}
