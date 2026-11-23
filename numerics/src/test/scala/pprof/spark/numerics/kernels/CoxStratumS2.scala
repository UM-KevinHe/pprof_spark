package pprof.spark.numerics.kernels

import pprof.spark.numerics.{ChunkedVector, NeumaierSum, NeumaierVector}

/** Round 182's Cox kernel, which keeps S₂ as a running chunked sum, kept as a reference for the weighted-Gram form
  * (docs/spec/cox/weighted-gram.md §9, D-80): its ℓ and U must match to the bit, and its information within 1e-13.
  */
object CoxStratumS2 {

  def add(
      rows: CoxRowsView,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      totals: CoxTotals
  ): Unit = {
    val p = rows.p
    require(beta.length == p && center.length == p && totals.p == p, "dimension mismatch")
    val q = Moments.packedLength(p)
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val s2 = new ChunkedVector(q)
    val p2 = s2.pending
    val info = new ChunkedVector(q)
    val ip = info.pending
    val stratum = new CoxTotals(p)
    val xc = new Array[Double](p)
    val eventSum = new Array[Double](p)
    val tied1 = new Array[Double](p)
    val tied2 = new Array[Double](q)
    val means = new Array[Double](p)
    val riskScore = new Array[Double](until - from)
    var removal = from
    var r = from
    while (r < until) {
      val t = rows.timeAt(r)
      var d = 0
      var dw = 0.0
      var etaEvents = 0.0
      var tied0 = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      if (efron) {
        java.util.Arrays.fill(tied1, 0.0)
        java.util.Arrays.fill(tied2, 0.0)
      }
      while (r < until && rows.timeAt(r) == t) {
        val w = rows.weightAt(r)
        if (w > 0.0) {
          val xs = rows.xs(r)
          val base = rows.xBase(r)
          var j = 0
          var linear = 0.0
          while (j < p) {
            xc(j) = xs(base + j) - center(j)
            linear += xc(j) * beta(j)
            j += 1
          }
          val eta = linear + rows.offsetAt(r)
          val rw = w * StrictMath.exp(eta)
          riskScore(r - from) = rw
          s0.add(rw)
          j = 0
          var k = 0
          while (j < p) {
            val wx = rw * xc(j)
            s1.addAt(j, wx)
            val off = k - j
            var l = j
            while (l < p) {
              p2(off + l) += wx * xc(l)
              l += 1
            }
            k += p - j
            j += 1
          }
          s2.endTerm()
          if (rows.eventAt(r)) {
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
      while (removal < until && rows.entryAt(rows.entryOrderAt(removal)) >= t) {
        val i = rows.entryOrderAt(removal)
        if (rows.weightAt(i) > 0.0) {
          val rw = riskScore(i - from)
          s0.add(-rw)
          val xs = rows.xs(i)
          val base = rows.xBase(i)
          var j = 0
          var k = 0
          while (j < p) {
            val wx = rw * (xs(base + j) - center(j))
            s1.addAt(j, -wx)
            val off = k - j
            var l = j
            while (l < p) {
              p2(off + l) += -(wx * (xs(base + l) - center(l)))
              l += 1
            }
            k += p - j
            j += 1
          }
          s2.endTerm()
        }
        removal += 1
      }
      if (d > 0) {
        val total0 = s0.value
        if (!efron || d == 1) {
          stratum.value.add(etaEvents - dw * StrictMath.log(total0))
          var j = 0
          while (j < p) {
            means(j) = s1.valueAt(j) / total0
            j += 1
          }
          j = 0
          var k = 0
          while (j < p) {
            val mj = means(j)
            stratum.score.addAt(j, eventSum(j) - dw * mj)
            var l = j
            while (l < p) {
              ip(k) += dw * (s2.readAt(k) / total0 - mj * means(l))
              k += 1
              l += 1
            }
            j += 1
          }
          info.endTerm()
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
                val c = (s2.readAt(k) - tied2(k)) + f * tied2(k)
                ip(k) += meanWeight * (c / a - means(j) * means(l))
                k += 1
                l += 1
              }
              j += 1
            }
            info.endTerm()
            step += 1
          }
        }
      }
    }
    stratum.information.add(info.values)
    totals.add(stratum)
  }
}
