package pprof.spark.numerics.kernels

import pprof.spark.numerics.NeumaierSum

/** A symmetric positive-definite system over crossed factors' levels (slice 4d-2): a diagonal provider block (factor
  * 0), the cluster levels' block with its cross-factor entries, and provider–cluster couplings. The provider block is
  * eliminated onto the cluster levels, whose Schur complement is factored by Cholesky's method; every sum is
  * compensated. Built from each level's diagonal value and the off-diagonal values of the level pairs.
  */
final class CrossedSystem(
    levels: Array[Int],
    diagonal: Array[Double],
    pairs: Seq[(Int, Int, Int, Int, Double)]
) {

  private val offsets = levels.scanLeft(0)(_ + _)
  val size: Int = offsets.last
  private val m = levels(0)
  private val cluster = size - m
  private val d0 = Array.tabulate(m)(i => diagonal(i))
  private val coupling = Array.fill(m)(scala.collection.mutable.ArrayBuffer.empty[(Int, Double)])
  private val dense = new Array[Double](cluster * cluster)

  (0 until cluster).foreach(c => dense(c * cluster + c) = diagonal(m + c))
  pairs.foreach { case (k, l, a, b, v) =>
    if (v != 0.0) {
      if (k == 0) coupling(a) += ((offsets(l) - m + b, v))
      else {
        val r = offsets(k) - m + a
        val c = offsets(l) - m + b
        dense(r * cluster + c) += v
        dense(c * cluster + r) += v
      }
    }
  }
  (0 until m).foreach { i =>
    val row = coupling(i)
    row.foreach { case (ca, va) =>
      row.foreach { case (cb, vb) => dense(ca * cluster + cb) -= va * vb / d0(i) }
    }
  }

  private val chol: Array[Double] = {
    val l = new Array[Double](cluster * cluster)
    var j = 0
    while (j < cluster) {
      val s = new NeumaierSum
      s.add(dense(j * cluster + j))
      var k = 0
      while (k < j) { s.add(-l(j * cluster + k) * l(j * cluster + k)); k += 1 }
      val pivot = s.value
      if (!(pivot > 0.0))
        throw new ArithmeticException(s"the cluster block is not positive definite at $j ($pivot)")
      val root = StrictMath.sqrt(pivot)
      l(j * cluster + j) = root
      var i = j + 1
      while (i < cluster) {
        val t = new NeumaierSum
        t.add(dense(i * cluster + j))
        k = 0
        while (k < j) { t.add(-l(i * cluster + k) * l(j * cluster + k)); k += 1 }
        l(i * cluster + j) = t.value / root
        i += 1
      }
      j += 1
    }
    l
  }

  /** log det of the whole system. */
  val logDet: Double = {
    val s = new NeumaierSum
    d0.foreach(v => s.add(StrictMath.log(v)))
    (0 until cluster).foreach(i => s.add(2.0 * StrictMath.log(chol(i * cluster + i))))
    s.value
  }

  private def denseSolve(b: Array[Double]): Array[Double] = {
    val y = new Array[Double](cluster)
    var i = 0
    while (i < cluster) {
      val s = new NeumaierSum
      s.add(b(i))
      var k = 0
      while (k < i) { s.add(-chol(i * cluster + k) * y(k)); k += 1 }
      y(i) = s.value / chol(i * cluster + i)
      i += 1
    }
    val x = new Array[Double](cluster)
    i = cluster - 1
    while (i >= 0) {
      val s = new NeumaierSum
      s.add(y(i))
      var k = i + 1
      while (k < cluster) { s.add(-chol(k * cluster + i) * x(k)); k += 1 }
      x(i) = s.value / chol(i * cluster + i)
      i -= 1
    }
    x
  }

  /** The system's solution for `r`, by block elimination. */
  def solve(r: Array[Double]): Array[Double] = {
    val t = Array.tabulate(cluster)(c => r(m + c))
    (0 until m).foreach(j => coupling(j).foreach { case (c, v) => t(c) -= v * r(j) / d0(j) })
    val xc = denseSolve(t)
    val x = new Array[Double](size)
    (0 until m).foreach { j =>
      val s = new NeumaierSum
      s.add(r(j))
      coupling(j).foreach { case (c, v) => s.add(-v * xc(c)) }
      x(j) = s.value / d0(j)
    }
    Array.copy(xc, 0, x, m, cluster)
    x
  }

  /** The diagonal of the inverse: 1/D₀ⱼ + D₀ⱼ⁻²·eⱼᵀA⁻¹eⱼ for providers, diag(A⁻¹) for cluster levels. */
  def inverseDiagonal: Array[Double] = {
    val aInv = Array.tabulate(cluster)(c =>
      denseSolve(Array.tabulate(cluster)(z => if (z == c) 1.0 else 0.0))
    )
    val out = new Array[Double](size)
    (0 until m).foreach { j =>
      val s = new NeumaierSum
      s.add(1.0 / d0(j))
      coupling(j).foreach { case (ca, va) =>
        coupling(j).foreach { case (cb, vb) => s.add(va * vb * aInv(ca)(cb) / (d0(j) * d0(j))) }
      }
      out(j) = s.value
    }
    (0 until cluster).foreach(c => out(m + c) = aInv(c)(c))
    out
  }
}
