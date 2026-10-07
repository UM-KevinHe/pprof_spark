package pprof.spark.numerics.kernels

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}

/** Martingale and score residuals of one stratum (Phase 1c specification §1), as R's agmart3 and
  * agscore3 and pprof_py compute them, under left truncation and Efron ties.
  *
  * A descending sweep, with the risk sets, entry-time removals and zero weights of
  * [[CoxStratum.add]], records per event time the hazard h, its covariate-weighted counterpart g,
  * and for Efron with d tied events the dying rows' share c₀ = Σₖ (k/d)·m/Aₖ, c₁ and the mean of the
  * k-th means. Ascending running totals H and G then give each row its sums over (entry, exit] as
  * differences. A row dying at a tied Efron time uses c₀ and c₁ there instead of h and g. Rows with
  * zero weight are absent from the fit (X-013); they get the residuals of an at-risk row that never
  * dies. Covariates are centered by `center`, which leaves the residuals unchanged.
  */
object CoxResiduals {

  /** Residuals of rows `[from, until)`: martingale (one per row) and score (row-major, p per row). */
  def stratum(
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
      efron: Boolean
  ): (Array[Double], Array[Double]) = {
    val n = until - from
    val risk = new Array[Double](n)
    var r = from
    while (r < until) {
      var j = 0
      var linear = 0.0
      while (j < p) {
        linear += (x(r * p + j) - center(j)) * beta(j)
        j += 1
      }
      risk(r - from) = StrictMath.exp(linear + offset(r))
      r += 1
    }
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val times = Array.newBuilder[Double]
    val tied = Array.newBuilder[Int]
    val h = Array.newBuilder[Double]
    val c0 = Array.newBuilder[Double]
    val g = Array.newBuilder[Array[Double]]
    val c1 = Array.newBuilder[Array[Double]]
    val means = Array.newBuilder[Array[Double]]
    val d1 = new Array[Double](p)
    var removal = from
    r = from
    while (r < until) {
      val t = time(r)
      var d = 0
      var dw = 0.0
      var d0 = 0.0
      java.util.Arrays.fill(d1, 0.0)
      while (r < until && time(r) == t) {
        val w = weight(r)
        if (w > 0.0) {
          val rw = w * risk(r - from)
          s0.add(rw)
          var j = 0
          while (j < p) {
            s1.addAt(j, rw * (x(r * p + j) - center(j)))
            j += 1
          }
          if (event(r)) {
            d += 1
            dw += w
            d0 += rw
            j = 0
            while (j < p) {
              d1(j) += rw * (x(r * p + j) - center(j))
              j += 1
            }
          }
        }
        r += 1
      }
      while (removal < until && entry(entryOrder(removal)) >= t) {
        val i = entryOrder(removal)
        if (weight(i) > 0.0) {
          val rw = weight(i) * risk(i - from)
          s0.add(-rw)
          var j = 0
          while (j < p) {
            s1.addAt(j, -(rw * (x(i * p + j) - center(j))))
            j += 1
          }
        }
        removal += 1
      }
      if (d > 0) {
        val total0 = s0.value
        val total1 = s1.values
        val gt = new Array[Double](p)
        val ct = new Array[Double](p)
        val mt = new Array[Double](p)
        if (!efron || d == 1) {
          val hazard = dw / total0
          var j = 0
          while (j < p) {
            mt(j) = total1(j) / total0
            gt(j) = hazard * mt(j)
            ct(j) = gt(j)
            j += 1
          }
          h += hazard
          c0 += hazard
        } else {
          val m = dw / d
          var hazard = 0.0
          var dying = 0.0
          var k = 1
          while (k <= d) {
            val f = k.toDouble / d
            val a = (total0 - d0) + f * d0
            val step = m / a
            hazard += step
            dying += f * step
            var j = 0
            while (j < p) {
              val mean = ((total1(j) - d1(j)) + f * d1(j)) / a
              gt(j) += step * mean
              ct(j) += f * step * mean
              mt(j) += mean / d
              j += 1
            }
            k += 1
          }
          h += hazard
          c0 += dying
        }
        times += t
        tied += d
        g += gt
        c1 += ct
        means += mt
      }
    }
    // Ascending order and running totals H and G over event times.
    val ts = times.result().reverse
    val ds = tied.result().reverse
    val hs = h.result().reverse
    val c0s = c0.result().reverse
    val gs = g.result().reverse
    val c1s = c1.result().reverse
    val ms = means.result().reverse
    val k = ts.length
    val hTotal = new Array[Double](k)
    val gTotal = new Array[Double](k * p)
    val hSum = new NeumaierSum
    val gSum = new NeumaierVector(p)
    var e = 0
    while (e < k) {
      hSum.add(hs(e))
      gSum.add(gs(e))
      hTotal(e) = hSum.value
      var j = 0
      while (j < p) {
        gTotal(e * p + j) = gSum.valueAt(j)
        j += 1
      }
      e += 1
    }
    def lastAtOrBefore(v: Double): Int = {
      var lo = 0
      var hi = k - 1
      var found = -1
      while (lo <= hi) {
        val mid = (lo + hi) >>> 1
        if (ts(mid) <= v) { found = mid; lo = mid + 1 }
        else hi = mid - 1
      }
      found
    }
    val martingale = new Array[Double](n)
    val score = new Array[Double](n * p)
    r = from
    while (r < until) {
      val i = r - from
      val kb = lastAtOrBefore(time(r))
      val ka = lastAtOrBefore(entry(r))
      val lambda = (if (kb >= 0) hTotal(kb) else 0.0) - (if (ka >= 0) hTotal(ka) else 0.0)
      val dies = event(r) && weight(r) > 0.0
      val corrected = dies && efron && ds(kb) >= 2
      val rr = risk(i)
      martingale(i) =
        (if (dies) 1.0 else 0.0) - rr * lambda + (if (corrected) rr * (hs(kb) - c0s(kb)) else 0.0)
      var j = 0
      while (j < p) {
        val xc = x(r * p + j) - center(j)
        val gamma =
          (if (kb >= 0) gTotal(kb * p + j) else 0.0) - (if (ka >= 0) gTotal(ka * p + j) else 0.0)
        var u = -rr * (xc * lambda - gamma)
        if (dies) u += xc - ms(kb)(j)
        if (corrected) u += rr * (xc * (hs(kb) - c0s(kb)) - (gs(kb)(j) - c1s(kb)(j)))
        score(i * p + j) = u
        j += 1
      }
      r += 1
    }
    (martingale, score)
  }
}
