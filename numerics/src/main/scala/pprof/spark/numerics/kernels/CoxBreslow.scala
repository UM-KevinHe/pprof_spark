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

/** The Breslow log partial likelihood, score and information of one stratum (Cox specification
  * §2 and §6).
  *
  * Rows `[from, until)` of a block hold one stratum in canonical order: exit time descending, then
  * events before censored rows, then row identifier. Covariates are row-major and are centered by
  * `center` before use, which leaves the results unchanged mathematically. The sweep adds every row
  * of an exit time to the running risk-set sums S₀, S₁ and S₂ before that time's events contribute,
  * so tied events share one risk set. Running sums and the stratum's contributions are Neumaier
  * sums in sweep order, and the stratum total is added to `totals` once, so the result depends
  * only on the canonical order (R0).
  */
object CoxBreslow {

  def addStratum(
      time: Array[Double],
      event: Array[Boolean],
      x: Array[Double],
      p: Int,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double],
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
    var r = from
    while (r < until) {
      val t = time(r)
      var d = 0
      var etaEvents = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      while (r < until && time(r) == t) {
        var j = 0
        var eta = 0.0
        while (j < p) {
          xc(j) = x(r * p + j) - center(j)
          eta += xc(j) * beta(j)
          j += 1
        }
        val w = StrictMath.exp(eta)
        s0.add(w)
        j = 0
        var k = 0
        while (j < p) {
          val wx = w * xc(j)
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
          etaEvents += eta
          j = 0
          while (j < p) {
            eventSum(j) += xc(j)
            j += 1
          }
        }
        r += 1
      }
      if (d > 0) {
        val total0 = s0.value
        stratum.value.add(etaEvents - d * StrictMath.log(total0))
        var j = 0
        var k = 0
        while (j < p) {
          val mj = s1.valueAt(j) / total0
          stratum.score.addAt(j, eventSum(j) - d * mj)
          var l = j
          while (l < p) {
            stratum.information.addAt(
              k,
              d * (s2.valueAt(k) / total0 - mj * (s1.valueAt(l) / total0))
            )
            k += 1
            l += 1
          }
          j += 1
        }
      }
    }
    totals.add(stratum)
  }
}
