package pprof.spark.numerics.kernels

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}

/** Compensated totals of log partial likelihood, score and packed information (§6.8). */
final class CoxTotals(val p: Int) {
  val value = new NeumaierSum
  val score = new NeumaierVector(p)
  val information = new NeumaierVector(Moments.packedLength(p))

  def add(other: CoxTotals): Unit = {
    value.add(other.value.value)
    score.add(other.score.values)
    information.add(other.information.values)
  }
}

/** The log partial likelihood, score and information of one stratum, with Breslow or Efron ties,
  * case weights and offsets (Cox specification §2 and §6; addendum efron-weights-offsets.md §2
  * and §5).
  *
  * Rows `[from, until)` of a block hold one stratum in canonical order: exit time descending, then
  * events before censored rows, then row identifier. Covariates are row-major and are centered by
  * `center` before use; offsets are not centered. A row with zero weight is skipped, so it is the
  * same as an absent row (X-013). The sweep adds every row of an exit time to the running risk-set
  * sums before that time's events contribute, so tied events share one risk set. Running sums and
  * the stratum's contributions are Neumaier sums in sweep order; the per-time event sums are plain
  * sums in the same order. The stratum total is added to `totals` once, so the result depends only
  * on the canonical order (R0).
  */
object CoxStratum {

  def add(
      time: Array[Double],
      event: Array[Boolean],
      weight: Array[Double],
      offset: Array[Double],
      x: Array[Double],
      p: Int,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      totals: CoxTotals
  ): Unit = {
    require(beta.length == p && center.length == p && totals.p == p, "dimension mismatch")
    val q = Moments.packedLength(p)
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val s2 = new NeumaierVector(q)
    val stratum = new CoxTotals(p)
    val xc = new Array[Double](p)
    val eventSum = new Array[Double](p)
    val tied1 = new Array[Double](p)
    val tied2 = new Array[Double](q)
    val means = new Array[Double](p)
    var r = from
    while (r < until) {
      val t = time(r)
      var d = 0
      var dw = 0.0
      var etaEvents = 0.0
      var tied0 = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      java.util.Arrays.fill(tied1, 0.0)
      java.util.Arrays.fill(tied2, 0.0)
      while (r < until && time(r) == t) {
        val w = weight(r)
        if (w > 0.0) {
          var j = 0
          var linear = 0.0
          while (j < p) {
            xc(j) = x(r * p + j) - center(j)
            linear += xc(j) * beta(j)
            j += 1
          }
          val eta = linear + offset(r)
          val rw = w * StrictMath.exp(eta)
          s0.add(rw)
          j = 0
          var k = 0
          while (j < p) {
            val wx = rw * xc(j)
            s1.addAt(j, wx)
            var l = j
            while (l < p) {
              s2.addAt(k, wx * xc(l))
              k += 1
              l += 1
            }
            j += 1
          }
          if (event(r)) {
            d += 1
            dw += w
            etaEvents += w * eta
            j = 0
            while (j < p) {
              eventSum(j) += w * xc(j)
              j += 1
            }
            if (efron) {
              tied0 += rw
              j = 0
              k = 0
              while (j < p) {
                val wx = rw * xc(j)
                tied1(j) += wx
                var l = j
                while (l < p) {
                  tied2(k) += wx * xc(l)
                  k += 1
                  l += 1
                }
                j += 1
              }
            }
          }
        }
        r += 1
      }
      if (d > 0) {
        val total0 = s0.value
        if (!efron || d == 1) {
          stratum.value.add(etaEvents - dw * StrictMath.log(total0))
          var j = 0
          var k = 0
          while (j < p) {
            val mj = s1.valueAt(j) / total0
            stratum.score.addAt(j, eventSum(j) - dw * mj)
            var l = j
            while (l < p) {
              stratum.information.addAt(
                k,
                dw * (s2.valueAt(k) / total0 - mj * (s1.valueAt(l) / total0))
              )
              k += 1
              l += 1
            }
            j += 1
          }
        } else {
          val meanWeight = dw / d
          stratum.value.add(etaEvents)
          var j = 0
          while (j < p) {
            stratum.score.addAt(j, eventSum(j))
            j += 1
          }
          var step = 1
          while (step <= d) {
            val f = step.toDouble / d
            val a = (total0 - tied0) + f * tied0
            stratum.value.add(-meanWeight * StrictMath.log(a))
            j = 0
            while (j < p) {
              means(j) = ((s1.valueAt(j) - tied1(j)) + f * tied1(j)) / a
              stratum.score.addAt(j, -meanWeight * means(j))
              j += 1
            }
            j = 0
            var k = 0
            while (j < p) {
              var l = j
              while (l < p) {
                val c = (s2.valueAt(k) - tied2(k)) + f * tied2(k)
                stratum.information.addAt(k, meanWeight * (c / a - means(j) * means(l)))
                k += 1
                l += 1
              }
              j += 1
            }
            step += 1
          }
        }
      }
    }
    totals.add(stratum)
  }
}
