package pprof.spark.numerics.kernels

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}

/** Kernels of the linear fixed-effect model (docs/spec/linear/fixed-effect-estimation.md §2 and §7). A block's rows
  * are grouped by provider in canonical order; `groupStart` gives each provider's first row, and `values` holds
  * each row's p features and then its outcome, row-major. Pure and deterministic: every sum runs in row and
  * provider order with Neumaier's compensation.
  */
object LinearFE {

  final case class Rows(groupStart: Array[Int], values: Array[Double], p: Int) {
    require(p >= 1, s"at least one feature, got $p")
    require(values.length % (p + 1) == 0, s"${values.length} values are not rows of ${p + 1}")
    def count: Int = values.length / (p + 1)
    def groups: Int = groupStart.length
    def until(g: Int): Int = if (g + 1 < groups) groupStart(g + 1) else count
  }

  /** A block's totals: rows, Σ x̃x̃ᵀ (packed p by p), Σ x̃ỹ and Σ y, with x̃ and ỹ centred within providers. */
  final case class Within(rows: Long, xx: Array[Double], xy: Array[Double], ySum: Double)

  /** Pass 1: each provider's centred co-moments (two passes over its rows, `Moments.centred`), added over the
    * block's providers in order.
    */
  def within(rows: Rows): Within = {
    val p = rows.p
    val q = p + 1
    val xx = new NeumaierVector(Moments.packedLength(p))
    val xy = new NeumaierVector(p)
    val ySum = new NeumaierSum
    var g = 0
    while (g < rows.groups) {
      val c = Moments.centred(rows.values, q, rows.groupStart(g), rows.until(g))
      var i = 0
      while (i < p) {
        var j = i
        while (j < p) {
          xx.addAt(Moments.packedIndex(i, j, p), c.comoments(Moments.packedIndex(i, j, q)))
          j += 1
        }
        xy.addAt(i, c.comoments(Moments.packedIndex(i, p, q)))
        i += 1
      }
      g += 1
    }
    var r = 0
    while (r < rows.count) {
      ySum.add(rows.values(r * q + p))
      r += 1
    }
    Within(rows.count.toLong, xx.values, xy.values, ySum.value)
  }

  /** One provider: its rows, γ̂ⱼ = ȳⱼ − x̄ⱼᵀβ̂ and qⱼ = 1/nⱼ + x̄ⱼᵀVx̄ⱼ, so Var(γ̂ⱼ) = σ̂²qⱼ. */
  final case class Provider(rows: Long, gamma: Double, q: Double)

  /** Each provider of the block, in order; `covariance` is V = (Σ x̃x̃ᵀ)⁻¹, packed. */
  def providers(rows: Rows, beta: Array[Double], covariance: Array[Double]): Array[Provider] = {
    val p = rows.p
    require(
      beta.length == p && covariance.length == Moments.packedLength(p),
      "β and V must match the features"
    )
    Array.tabulate(rows.groups) { g =>
      val mean = means(rows, g)
      val fitted = new NeumaierSum
      var k = 0
      while (k < p) {
        fitted.add(mean(k) * beta(k))
        k += 1
      }
      val quadratic = new NeumaierSum
      var i = 0
      while (i < p) {
        var j = i
        while (j < p) {
          val term = covariance(Moments.packedIndex(i, j, p)) * mean(i) * mean(j)
          quadratic.add(if (i == j) term else 2.0 * term)
          j += 1
        }
        i += 1
      }
      val n = (rows.until(g) - rows.groupStart(g)).toLong
      Provider(n, mean(p) - fitted.value, 1.0 / n + quadratic.value)
    }
  }

  /** A block's residual sum of squares Σ (y − γ̂ⱼ − xᵀβ̂)² and total sum of squares Σ (y − mean)². */
  final case class Sums(rss: Double, tss: Double)

  def sums(rows: Rows, beta: Array[Double], gamma: Array[Double], mean: Double): Sums = {
    val p = rows.p
    val q = p + 1
    require(gamma.length == rows.groups, s"${gamma.length} effects for ${rows.groups} providers")
    val rss = new NeumaierSum
    val tss = new NeumaierSum
    var g = 0
    while (g < rows.groups) {
      var r = rows.groupStart(g)
      while (r < rows.until(g)) {
        val prediction = new NeumaierSum
        prediction.add(gamma(g))
        var k = 0
        while (k < p) {
          prediction.add(rows.values(r * q + k) * beta(k))
          k += 1
        }
        val y = rows.values(r * q + p)
        val residual = y - prediction.value
        rss.add(residual * residual)
        tss.add((y - mean) * (y - mean))
        r += 1
      }
      g += 1
    }
    Sums(rss.value, tss.value)
  }

  /** Provider g's means of the features and the outcome, as `Moments.centred` computes them. */
  private def means(rows: Rows, g: Int): Array[Double] = {
    val q = rows.p + 1
    val sums = new NeumaierVector(q)
    var r = rows.groupStart(g)
    while (r < rows.until(g)) {
      var j = 0
      while (j < q) {
        sums.addAt(j, rows.values(r * q + j))
        j += 1
      }
      r += 1
    }
    val n = (rows.until(g) - rows.groupStart(g)).toDouble
    sums.values.map(_ / n)
  }
}
