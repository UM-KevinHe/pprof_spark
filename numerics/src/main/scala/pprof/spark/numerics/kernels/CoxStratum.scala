package pprof.spark.numerics.kernels

import pprof.spark.numerics.ChunkedVector

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
  * sums, then removes every row whose entry time is at or after that time (rows are at risk on
  * (entry, exit], Phase 1b specification §1 and §6), before that time's events contribute, so tied
  * events share one risk set. `entryOrder` lists the stratum's row indices by entry time descending;
  * removals subtract from S₀ and S₁ with the same Neumaier sums. With every entry below every exit
  * time of an event nothing is removed, and the sums are exactly those of right-censored data. The
  * running sums and the stratum's contributions to ℓ and U are Neumaier sums in sweep order; the
  * per-time event sums are plain sums in the same order. The information is computed in the
  * weighted-Gram form (docs/spec/cox/weighted-gram.md, D-80): the sweep adds each event time's
  * rank-one terms to a chunked compensated sum M, and a second pass over the stratum's rows adds
  * each row's Gram term to a chunked compensated sum G, weighted by its risk score times its
  * cumulative hazard while at risk, less the Efron correction of a tied event. The stratum total is added to `totals` once, so the result depends only
  * on the canonical order (R0).
  */
object CoxStratum {

  def add(
      time: Array[Double],
      entry: Array[Double],
      entryOrder: Array[Int],
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
  ): Unit = add(
    CoxRows(time, entry, entryOrder, event, weight, offset, x, p),
    from,
    until,
    beta,
    center,
    efron,
    totals
  )

  /** As above, with the rows read through a view (docs/spec/performance/step2-chunks.md §3). */
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
    val subtractive = new ChunkedVector(q)
    val mp = subtractive.pending
    val stratum = new CoxTotals(p)
    val xc = new Array[Double](p)
    val eventSum = new Array[Double](p)
    val tied1 = new Array[Double](p)
    val means = new Array[Double](p)
    val riskScore = new Array[Double](until - from)
    val times = Array.newBuilder[Double]
    val hazards = Array.newBuilder[Double]
    val corrections = Array.newBuilder[Double]

    // One step's rank-one term a·m̄m̄ᵀ, added to M (weighted-gram.md §4).
    def rankOne(a: Double): Unit = {
      var j = 0
      var k = 0
      while (j < p) {
        val am = a * means(j)
        val off = k - j
        var l = j
        while (l < p) {
          mp(off + l) += am * means(l)
          l += 1
        }
        k += p - j
        j += 1
      }
      subtractive.endTerm()
    }

    var removal = from
    var r = from
    while (r < until) {
      val t = rows.timeAt(r)
      var d = 0
      var dw = 0.0
      var etaEvents = 0.0
      var tied0 = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      if (efron) java.util.Arrays.fill(tied1, 0.0)
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
          while (j < p) {
            s1.addAt(j, rw * xc(j))
            j += 1
          }
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
              while (j < p) {
                tied1(j) += rw * xc(j)
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
          while (j < p) {
            s1.addAt(j, -(rw * (xs(base + j) - center(j))))
            j += 1
          }
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
          while (j < p) {
            stratum.score.addAt(j, eventSum(j) - dw * means(j))
            j += 1
          }
          rankOne(dw)
          times += t
          hazards += dw / total0
          corrections += 0.0
        } else {
          val meanWeight = dw / d
          stratum.value.add(etaEvents)
          var j = 0
          while (j < p) {
            stratum.score.addAt(j, eventSum(j))
            j += 1
          }
          var hazard = 0.0
          var dying = 0.0
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
            rankOne(meanWeight)
            val increment = meanWeight / a
            hazard += increment
            dying += f * increment
            step += 1
          }
          times += t
          hazards += hazard
          corrections += hazard - dying
        }
      }
    }
    // Cumulative hazards over the event times in ascending time, their sums and compensations apart (§4).
    val ts = times.result().reverse
    val hs = hazards.result().reverse
    val psi = corrections.result().reverse
    val events = ts.length
    val hSum = new Array[Double](events)
    val hComp = new Array[Double](events)
    var sum = 0.0
    var compensation = 0.0
    var e = 0
    while (e < events) {
      val h = hs(e)
      val next = sum + h
      if (java.lang.Double.isFinite(next)) {
        if (math.abs(sum) >= math.abs(h)) compensation += (sum - next) + h
        else compensation += (h - next) + sum
      }
      sum = next
      hSum(e) = sum
      hComp(e) = compensation
      e += 1
    }
    def lastAtOrBefore(v: Double): Int = {
      var lo = 0
      var hi = events - 1
      var found = -1
      while (lo <= hi) {
        val mid = (lo + hi) >>> 1
        if (ts(mid) <= v) {
          found = mid
          lo = mid + 1
        } else hi = mid - 1
      }
      found
    }
    val gram = new ChunkedVector(q)
    val gp = gram.pending
    r = from
    while (r < until) {
      if (rows.weightAt(r) > 0.0) {
        val b = lastAtOrBefore(rows.timeAt(r))
        val a = lastAtOrBefore(rows.entryAt(r))
        val lambda = ((if (b >= 0) hSum(b) else 0.0) - (if (a >= 0) hSum(a) else 0.0)) +
          ((if (b >= 0) hComp(b) else 0.0) - (if (a >= 0) hComp(a) else 0.0))
        val hazard = if (rows.eventAt(r) && b >= 0) lambda - psi(b) else lambda
        if (hazard != 0.0) {
          val omega = riskScore(r - from) * hazard
          val xs = rows.xs(r)
          val base = rows.xBase(r)
          var j = 0
          while (j < p) {
            xc(j) = xs(base + j) - center(j)
            j += 1
          }
          j = 0
          var k = 0
          while (j < p) {
            val wx = omega * xc(j)
            val off = k - j
            var l = j
            while (l < p) {
              gp(off + l) += wx * xc(l)
              l += 1
            }
            k += p - j
            j += 1
          }
        }
        gram.endTerm()
      }
      r += 1
    }
    val g = gram.values
    val m = subtractive.values
    val information = new Array[Double](q)
    var k = 0
    while (k < q) {
      information(k) = g(k) - m(k)
      k += 1
    }
    stratum.information.add(information)
    totals.add(stratum)
  }

  /** Baseline hazard increments at the stratum's event times, in ascending time (Phase 1b
    * specification §2), at `beta` with uncentered covariates, so they refer to x = 0 and offset 0:
    * d_w/S₀ for Breslow fits and for Efron fits with one event at the time, and m·Σₖ 1/Aₖ for Efron
    * fits with tied events (pprof_py's `_efron_baseline_numba`). Risk sets, removals at entry times
    * and zero weights follow [[add]].
    */
  def baseline(
      time: Array[Double],
      entry: Array[Double],
      entryOrder: Array[Int],
      event: Array[Boolean],
      weight: Array[Double],
      offset: Array[Double],
      x: Array[Double],
      p: Int,
      from: Int,
      until: Int,
      beta: Array[Double],
      efron: Boolean
  ): (Array[Double], Array[Double]) = baseline(
    CoxRows(time, entry, entryOrder, event, weight, offset, x, p),
    from,
    until,
    beta,
    efron
  )

  /** As above, with the rows read through a view (step2-chunks.md §3). */
  def baseline(
      rows: CoxRowsView,
      from: Int,
      until: Int,
      beta: Array[Double],
      efron: Boolean
  ): (Array[Double], Array[Double]) = {
    val p = rows.p
    require(beta.length == p, "dimension mismatch")
    val s0 = new NeumaierSum
    val riskScore = new Array[Double](until - from)
    val times = Array.newBuilder[Double]
    val increments = Array.newBuilder[Double]
    var removal = from
    var r = from
    while (r < until) {
      val t = rows.timeAt(r)
      var d = 0
      var dw = 0.0
      var tied0 = 0.0
      while (r < until && rows.timeAt(r) == t) {
        val w = rows.weightAt(r)
        if (w > 0.0) {
          val xs = rows.xs(r)
          val base = rows.xBase(r)
          var j = 0
          var linear = 0.0
          while (j < p) {
            linear += xs(base + j) * beta(j)
            j += 1
          }
          val rw = w * StrictMath.exp(linear + rows.offsetAt(r))
          riskScore(r - from) = rw
          s0.add(rw)
          if (rows.eventAt(r)) {
            d += 1
            dw += w
            tied0 += rw
          }
        }
        r += 1
      }
      while (removal < until && rows.entryAt(rows.entryOrderAt(removal)) >= t) {
        val i = rows.entryOrderAt(removal)
        if (rows.weightAt(i) > 0.0) s0.add(-riskScore(i - from))
        removal += 1
      }
      if (d > 0) {
        val total0 = s0.value
        val increment =
          if (!efron || d == 1) dw / total0
          else {
            var sum = 0.0
            var k = 1
            while (k <= d) {
              sum += 1.0 / ((total0 - tied0) + (k.toDouble / d) * tied0)
              k += 1
            }
            (dw / d) * sum
          }
        times += t
        increments += increment
      }
    }
    (times.result().reverse, increments.result().reverse)
  }
}
