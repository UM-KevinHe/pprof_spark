package pprof.spark.numerics.kernels

import pprof.spark.numerics.{ChunkedVector, NeumaierSum, NeumaierVector}

/** Per-time totals of one Cox stratum on its grid of times (docs/spec/cox/per-time-aggregates.md §4, D-81). The grid
  * holds every exit and entry time of the stratum, ascending. At each grid time t the totals hold Δ₀(t) and Δ₁(t), the
  * risk scores rᵢ and rᵢxᵢ of rows exiting at t less those of rows entering at t; D(t) and d(t), the weight and count
  * of events of positive weight exiting at t; and under Efron T₀(t) and T₁(t), those events' rᵢ and rᵢxᵢ. The
  * events' Σwη and Σwx are kept once for the stratum. Rows of zero weight add nothing (X-013). Every total is a Neumaier
  * sum, so totals of blocks merged in a fixed order give the same bits whatever the partitioning (R0).
  */
final class CoxTimeTotals(val grid: Array[Double], val p: Int, val efron: Boolean) {
  require(grid.nonEmpty, "the grid must have at least one time")
  require(
    (1 until grid.length).forall(k => grid(k - 1) < grid(k)),
    "the grid must be strictly ascending"
  )
  require(p >= 0, "p must not be negative")

  val times: Int = grid.length
  private val width = p + 1

  /** Δ₀ at k·(p + 1) and Δ₁ at k·(p + 1) + 1 + j, for grid time k. */
  val net = new NeumaierVector(times * width)

  /** D(t), the weight of events of positive weight at each grid time. */
  val eventWeight = new NeumaierVector(times)

  /** d(t), the number of events of positive weight at each grid time. */
  val eventCount = new Array[Long](times)

  /** Under Efron, T₀ and T₁ laid out as `net`; empty under Breslow. */
  val tied = new NeumaierVector(if (efron) times * width else 0)

  /** The stratum's events: Σwη at 0, then Σwx at 1 + j. */
  val events = new NeumaierVector(1 + p)

  /** The position of `t` on the grid; a time off the grid is an error of planning. */
  def indexOf(t: Double): Int = {
    val k = java.util.Arrays.binarySearch(grid, t)
    if (k < 0)
      throw new IllegalArgumentException(
        s"time $t is not on the stratum's grid of ${grid.length} times"
      )
    k
  }

  /** Adds rows [from, until), all of this stratum, with the risk scores of the sweep (first-slice §6). */
  def add(
      rows: CoxRowsView,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double]
  ): Unit = {
    require(rows.p == p && beta.length == p && center.length == p, "dimension mismatch")
    val xc = new Array[Double](p)
    var r = from
    while (r < until) {
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
        val k = indexOf(rows.timeAt(r))
        val exit = k * width
        val entry = indexOf(rows.entryAt(r)) * width
        net.addAt(exit, rw)
        net.addAt(entry, -rw)
        j = 0
        while (j < p) {
          val v = rw * xc(j)
          net.addAt(exit + 1 + j, v)
          net.addAt(entry + 1 + j, -v)
          j += 1
        }
        if (rows.eventAt(r)) {
          eventWeight.addAt(k, w)
          eventCount(k) += 1
          events.addAt(0, w * eta)
          j = 0
          while (j < p) {
            events.addAt(1 + j, w * xc(j))
            j += 1
          }
          if (efron) {
            tied.addAt(exit, rw)
            j = 0
            while (j < p) {
              tied.addAt(exit + 1 + j, rw * xc(j))
              j += 1
            }
          }
        }
      }
      r += 1
    }
  }

  /** Adds another part's totals, by their values: parts merged in a fixed order give fixed bits. */
  def merge(other: CoxTimeTotals): Unit = {
    require(
      other.p == p && other.efron == efron && java.util.Arrays.equals(other.grid, grid),
      "totals of different strata or options"
    )
    addValues(
      other.net.values,
      other.eventWeight.values,
      other.eventCount,
      other.tied.values,
      other.events.values
    )
  }

  /** Adds totals given as values, as they arrive from other tasks. */
  def addValues(
      netValues: Array[Double],
      eventWeightValues: Array[Double],
      eventCounts: Array[Long],
      tiedValues: Array[Double],
      eventValues: Array[Double]
  ): Unit = {
    require(
      netValues.length == net.length && eventWeightValues.length == times && eventCounts.length == times &&
        tiedValues.length == tied.length && eventValues.length == 1 + p,
      "totals of a different shape"
    )
    net.add(netValues)
    eventWeight.add(eventWeightValues)
    var k = 0
    while (k < times) {
      eventCount(k) += eventCounts(k)
      k += 1
    }
    tied.add(tiedValues)
    events.add(eventValues)
  }
}

/** A stratum's contribution at β from its per-time totals: ℓ, U, the rank-one sum M of weighted-gram.md §3, and what
  * pass 2 needs, the event times ascending with their cumulative hazards' sums and compensations and Efron's
  * corrections ψ.
  */
final case class CoxAggregateEvaluation(
    value: Double,
    score: Array[Double],
    subtractive: Array[Double],
    eventTimes: Array[Double],
    hazardSums: Array[Double],
    hazardCompensations: Array[Double],
    corrections: Array[Double]
)

/** A stratum's residual tables (Phase 1c specification §1) from its per-time totals, as `CoxResiduals` records them in
  * its sweep: per event time, ascending, the time, the tied count, the hazard h and Efron's c₀, and p-vectors g, c₁ and
  * the mean, row-major; then the running totals H and G, Neumaier sums in ascending time.
  */
final case class CoxResidualTables(
    p: Int,
    eventTimes: Array[Double],
    tied: Array[Long],
    hazard: Array[Double],
    dying: Array[Double],
    g: Array[Double],
    c1: Array[Double],
    means: Array[Double],
    hTotal: Array[Double],
    gTotal: Array[Double]
)

object CoxAggregates {

  /** The driver's step (per-time-aggregates.md §4): suffix sums of the totals give S₀ and S₁ at each event time, as
    * Neumaier sums in descending time; ℓ, U and M follow first-slice §2, efron-weights-offsets §2 and weighted-gram.md
    * §3; the hazards are those of the residuals, and their cumulative sums keep sum and compensation apart.
    */
  def evaluate(totals: CoxTimeTotals): CoxAggregateEvaluation = {
    val p = totals.p
    val q = Moments.packedLength(p)
    val width = p + 1
    val net = totals.net.values
    val eventWeight = totals.eventWeight.values
    val tied = totals.tied.values
    val events = totals.events.values
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val value = new NeumaierSum
    value.add(events(0))
    val score = new NeumaierVector(p)
    var j = 0
    while (j < p) {
      score.addAt(j, events(1 + j))
      j += 1
    }
    val subtractive = new ChunkedVector(q)
    val mp = subtractive.pending
    val means = new Array[Double](p)
    def rankOne(a: Double): Unit = {
      var i = 0
      var k = 0
      while (i < p) {
        val am = a * means(i)
        val off = k - i
        var l = i
        while (l < p) {
          mp(off + l) += am * means(l)
          l += 1
        }
        k += p - i
        i += 1
      }
      subtractive.endTerm()
    }
    val times = Array.newBuilder[Double]
    val hazards = Array.newBuilder[Double]
    val corrections = Array.newBuilder[Double]
    var k = totals.times - 1
    while (k >= 0) {
      val base = k * width
      s0.add(net(base))
      j = 0
      while (j < p) {
        s1.addAt(j, net(base + 1 + j))
        j += 1
      }
      val d = totals.eventCount(k)
      if (d > 0) {
        val total0 = s0.value
        val dw = eventWeight(k)
        if (!totals.efron || d == 1) {
          value.add(-dw * StrictMath.log(total0))
          j = 0
          while (j < p) {
            means(j) = s1.valueAt(j) / total0
            score.addAt(j, -dw * means(j))
            j += 1
          }
          rankOne(dw)
          times += totals.grid(k)
          hazards += dw / total0
          corrections += 0.0
        } else {
          val tied0 = tied(base)
          val mean = dw / d
          var hazard = 0.0
          var dying = 0.0
          var step = 1L
          while (step <= d) {
            val f = step.toDouble / d
            val a = (total0 - tied0) + f * tied0
            value.add(-mean * StrictMath.log(a))
            j = 0
            while (j < p) {
              means(j) = ((s1.valueAt(j) - tied(base + 1 + j)) + f * tied(base + 1 + j)) / a
              score.addAt(j, -mean * means(j))
              j += 1
            }
            rankOne(mean)
            val increment = mean / a
            hazard += increment
            dying += f * increment
            step += 1
          }
          times += totals.grid(k)
          hazards += hazard
          corrections += hazard - dying
        }
      }
      k -= 1
    }
    val ts = times.result().reverse
    val hs = hazards.result().reverse
    val events_ = ts.length
    val hSum = new Array[Double](events_)
    val hComp = new Array[Double](events_)
    var sum = 0.0
    var compensation = 0.0
    var e = 0
    while (e < events_) {
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
    CoxAggregateEvaluation(
      value.value,
      score.values,
      subtractive.values,
      ts,
      hSum,
      hComp,
      corrections.result().reverse
    )
  }

  /** Pass 2 (§4): adds ωᵢxᵢxᵢᵀ for rows [from, until) of the stratum to `gram`, with ωᵢ = rᵢ(Λᵢ − ψᵢ), Λᵢ the
    * compensated difference of the cumulative hazards at the row's exit and entry (weighted-gram.md §4). A row whose
    * hazard is exactly zero ends its term without adding, so 0 × ∞ cannot make NaN.
    */
  def gram(
      rows: CoxRowsView,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double],
      evaluation: CoxAggregateEvaluation,
      gram: ChunkedVector
  ): Unit = {
    val p = rows.p
    require(
      beta.length == p && center.length == p && gram.length == Moments.packedLength(p),
      "dimension mismatch"
    )
    val ts = evaluation.eventTimes
    val hSum = evaluation.hazardSums
    val hComp = evaluation.hazardCompensations
    val psi = evaluation.corrections
    val events = ts.length
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
    val xc = new Array[Double](p)
    val gp = gram.pending
    var r = from
    while (r < until) {
      val w = rows.weightAt(r)
      if (w > 0.0) {
        val b = lastAtOrBefore(rows.timeAt(r))
        val a = lastAtOrBefore(rows.entryAt(r))
        val lambda = ((if (b >= 0) hSum(b) else 0.0) - (if (a >= 0) hSum(a) else 0.0)) +
          ((if (b >= 0) hComp(b) else 0.0) - (if (a >= 0) hComp(a) else 0.0))
        val hazard = if (rows.eventAt(r) && b >= 0) lambda - psi(b) else lambda
        if (hazard != 0.0) {
          val xs = rows.xs(r)
          val base = rows.xBase(r)
          var j = 0
          var linear = 0.0
          while (j < p) {
            xc(j) = xs(base + j) - center(j)
            linear += xc(j) * beta(j)
            j += 1
          }
          val omega = w * StrictMath.exp(linear + rows.offsetAt(r)) * hazard
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
  }

  /** The baseline hazard's event times and increments (§6), as `CoxStratum.baseline` computes them, from totals
    * accumulated with the centre at zero: Breslow D/S₀, Efron (D/d)·Σₖ 1/((S₀ − T₀) + (k/d)·T₀).
    */
  def baseline(totals: CoxTimeTotals): (Array[Double], Array[Double]) = {
    val width = totals.p + 1
    val net = totals.net.values
    val eventWeight = totals.eventWeight.values
    val tied = totals.tied.values
    val s0 = new NeumaierSum
    val times = Array.newBuilder[Double]
    val increments = Array.newBuilder[Double]
    var k = totals.times - 1
    while (k >= 0) {
      s0.add(net(k * width))
      val d = totals.eventCount(k)
      if (d > 0) {
        val total0 = s0.value
        val dw = eventWeight(k)
        val increment =
          if (!totals.efron || d == 1) dw / total0
          else {
            val tied0 = tied(k * width)
            var sum = 0.0
            var i = 1L
            while (i <= d) {
              sum += 1.0 / ((total0 - tied0) + (i.toDouble / d) * tied0)
              i += 1
            }
            (dw / d) * sum
          }
        times += totals.grid(k)
        increments += increment
      }
      k -= 1
    }
    (times.result().reverse, increments.result().reverse)
  }

  /** The residual tables of a stratum (§6), with the arithmetic of `CoxResiduals.stratum` on S₀, S₁, T₀ and T₁ taken
    * from the totals' suffix sums.
    */
  def residualTables(totals: CoxTimeTotals): CoxResidualTables = {
    val p = totals.p
    val width = p + 1
    val net = totals.net.values
    val eventWeight = totals.eventWeight.values
    val tied = totals.tied.values
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val times = Array.newBuilder[Double]
    val counts = Array.newBuilder[Long]
    val h = Array.newBuilder[Double]
    val c0 = Array.newBuilder[Double]
    val g = Array.newBuilder[Array[Double]]
    val c1 = Array.newBuilder[Array[Double]]
    val means = Array.newBuilder[Array[Double]]
    var k = totals.times - 1
    while (k >= 0) {
      val base = k * width
      s0.add(net(base))
      var j = 0
      while (j < p) {
        s1.addAt(j, net(base + 1 + j))
        j += 1
      }
      val d = totals.eventCount(k)
      if (d > 0) {
        val total0 = s0.value
        val total1 = s1.values
        val dw = eventWeight(k)
        val gt = new Array[Double](p)
        val ct = new Array[Double](p)
        val mt = new Array[Double](p)
        if (!totals.efron || d == 1) {
          val hazard = dw / total0
          j = 0
          while (j < p) {
            mt(j) = total1(j) / total0
            gt(j) = hazard * mt(j)
            ct(j) = gt(j)
            j += 1
          }
          h += hazard
          c0 += hazard
        } else {
          val d0 = tied(base)
          val m = dw / d
          var hazard = 0.0
          var dying = 0.0
          var i = 1L
          while (i <= d) {
            val f = i.toDouble / d
            val a = (total0 - d0) + f * d0
            val step = m / a
            hazard += step
            dying += f * step
            j = 0
            while (j < p) {
              val mean = ((total1(j) - tied(base + 1 + j)) + f * tied(base + 1 + j)) / a
              gt(j) += step * mean
              ct(j) += f * step * mean
              mt(j) += mean / d
              j += 1
            }
            i += 1
          }
          h += hazard
          c0 += dying
        }
        times += totals.grid(k)
        counts += d
        g += gt
        c1 += ct
        means += mt
      }
      k -= 1
    }
    val ts = times.result().reverse
    val hs = h.result().reverse
    val gs = g.result().reverse
    val events = ts.length
    val hTotal = new Array[Double](events)
    val gTotal = new Array[Double](events * p)
    val hSum = new NeumaierSum
    val gSum = new NeumaierVector(p)
    var e = 0
    while (e < events) {
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
    def flat(rows: Array[Array[Double]]): Array[Double] = Array.concat(rows.toSeq: _*)
    CoxResidualTables(
      p,
      ts,
      counts.result().reverse,
      hs,
      c0.result().reverse,
      flat(gs),
      flat(c1.result().reverse),
      flat(means.result().reverse),
      hTotal,
      gTotal
    )
  }

  /** Martingale (one per row) and score (row-major, p per row) residuals of rows [from, until) of a stratum, from its
    * tables, with the per-row arithmetic of `CoxResiduals.stratum`.
    */
  def residuals(
      rows: CoxRowsView,
      from: Int,
      until: Int,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      tables: CoxResidualTables
  ): (Array[Double], Array[Double]) = {
    val p = rows.p
    require(tables.p == p && beta.length == p && center.length == p, "dimension mismatch")
    val n = until - from
    val ts = tables.eventTimes
    val events = ts.length
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
    val martingale = new Array[Double](n)
    val score = new Array[Double](n * p)
    var r = from
    while (r < until) {
      val i = r - from
      val xs = rows.xs(r)
      val base = rows.xBase(r)
      var j = 0
      var linear = 0.0
      while (j < p) {
        linear += (xs(base + j) - center(j)) * beta(j)
        j += 1
      }
      val rr = StrictMath.exp(linear + rows.offsetAt(r))
      val kb = lastAtOrBefore(rows.timeAt(r))
      val ka = lastAtOrBefore(rows.entryAt(r))
      val lambda =
        (if (kb >= 0) tables.hTotal(kb) else 0.0) - (if (ka >= 0) tables.hTotal(ka) else 0.0)
      val dies = rows.eventAt(r) && rows.weightAt(r) > 0.0
      val corrected = dies && efron && tables.tied(kb) >= 2
      martingale(i) = (if (dies) 1.0 else 0.0) - rr * lambda +
        (if (corrected) rr * (tables.hazard(kb) - tables.dying(kb)) else 0.0)
      j = 0
      while (j < p) {
        val xc = xs(base + j) - center(j)
        val gamma = (if (kb >= 0) tables.gTotal(kb * p + j) else 0.0) -
          (if (ka >= 0) tables.gTotal(ka * p + j) else 0.0)
        var u = -rr * (xc * lambda - gamma)
        if (dies) u += xc - tables.means(kb * p + j)
        if (corrected)
          u += rr * (xc * (tables.hazard(kb) - tables.dying(kb)) - (tables.g(kb * p + j) - tables
            .c1(kb * p + j)))
        score(i * p + j) = u
        j += 1
      }
      r += 1
    }
    (martingale, score)
  }
}
