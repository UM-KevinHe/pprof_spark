package pprof.spark.numerics.kernels

import pprof.spark.numerics.NeumaierSum

/** Kernels for indirect and direct standardized measures (Phase 1d specification §1 and §4), as
  * pprof_py's `cox_standardized_expectations`. Risk scores are w = exp(η − max η) with η = xᵀβ + o
  * uncentered; case weights are not used, so each row counts once. All sums run in canonical order
  * with Neumaier compensation.
  */
object CoxMeasures {

  /** Per distinct time of rows `[from, until)`: Σ w by exit time, Σ w by entry time, and the number
    * of events at that exit time, in ascending time.
    */
  def timeTotals(
      time: Array[Double],
      entry: Array[Double],
      event: Array[Boolean],
      offset: Array[Double],
      x: Array[Double],
      p: Int,
      from: Int,
      until: Int,
      beta: Array[Double],
      maxEta: Double
  ): (Array[Double], Array[Double], Array[Double], Array[Long]) = {
    val totals =
      scala.collection.mutable.TreeMap.empty[Double, (NeumaierSum, NeumaierSum, Array[Long])]
    def slot(t: Double) =
      totals.getOrElseUpdate(t + 0.0, (new NeumaierSum, new NeumaierSum, Array(0L)))
    var r = from
    while (r < until) {
      val w = StrictMath.exp(eta(x, p, r, beta, offset) - maxEta)
      val exit = slot(time(r))
      exit._1.add(w)
      if (event(r)) exit._3(0) += 1
      slot(entry(r))._2.add(w)
      r += 1
    }
    val keys = totals.keysIterator.toArray
    (
      keys,
      keys.map(totals(_)._1.value),
      keys.map(totals(_)._2.value),
      keys.map(totals(_)._3(0))
    )
  }

  /** National baseline at the distinct event times, from all rows' time totals in ascending time:
    * RS(t) = Σ_{exit ≥ t} w − Σ_{entry ≥ t} w and Λ₀(t) = Σ_{s ≤ t} d(s)/RS(s). Returns the event
    * times, Λ₀ and RS at them.
    */
  def national(
      times: Array[Double],
      exitSums: Array[Double],
      entrySums: Array[Double],
      events: Array[Long]
  ): (Array[Double], Array[Double], Array[Double]) = {
    val k = times.length
    val exitTail = new Array[Double](k)
    val entryTail = new Array[Double](k)
    val a = new NeumaierSum
    val b = new NeumaierSum
    var i = k - 1
    while (i >= 0) {
      a.add(exitSums(i))
      b.add(entrySums(i))
      exitTail(i) = a.value
      entryTail(i) = b.value
      i -= 1
    }
    val eventTimes = Array.newBuilder[Double]
    val cumulative = Array.newBuilder[Double]
    val riskSets = Array.newBuilder[Double]
    val total = new NeumaierSum
    i = 0
    while (i < k) {
      if (events(i) > 0) {
        val rs = exitTail(i) - entryTail(i)
        total.add(events(i) / rs)
        eventTimes += times(i)
        cumulative += total.value
        riskSets += rs
      }
      i += 1
    }
    (eventTimes.result(), cumulative.result(), riskSets.result())
  }

  /** One provider's observed events, indirect expected Eⱼ = Σ w [Λ₀(exit) − Λ₀(entry)], person-time
    * Σ (exit − entry), and direct expected E⁽ʲ⁾ = Σ over its events of RS(t)/RSⱼ(t). Rows
    * `[from, until)` are the provider's, in canonical order with `entryOrder` as for [[CoxStratum]].
    */
  def provider(
      time: Array[Double],
      entry: Array[Double],
      entryOrder: Array[Int],
      event: Array[Boolean],
      offset: Array[Double],
      x: Array[Double],
      p: Int,
      from: Int,
      until: Int,
      beta: Array[Double],
      maxEta: Double,
      eventTimes: Array[Double],
      cumulative: Array[Double],
      riskSets: Array[Double]
  ): (Long, Double, Double, Double) = {
    def at(v: Double): Int = {
      var lo = 0
      var hi = eventTimes.length - 1
      var found = -1
      while (lo <= hi) {
        val mid = (lo + hi) >>> 1
        if (eventTimes(mid) <= v) { found = mid; lo = mid + 1 }
        else hi = mid - 1
      }
      found
    }
    val w =
      Array.tabulate(until - from)(i => StrictMath.exp(eta(x, p, from + i, beta, offset) - maxEta))
    val expected = new NeumaierSum
    val personTime = new NeumaierSum
    var observed = 0L
    var r = from
    while (r < until) {
      val ke = at(time(r))
      val ks = at(entry(r))
      val lambda = (if (ke >= 0) cumulative(ke) else 0.0) - (if (ks >= 0) cumulative(ks) else 0.0)
      expected.add(w(r - from) * lambda)
      personTime.add(time(r) - entry(r))
      if (event(r)) observed += 1
      r += 1
    }
    val own = new NeumaierSum
    val direct = new NeumaierSum
    var removal = from
    r = from
    while (r < until) {
      val t = time(r)
      var d = 0L
      while (r < until && time(r) == t) {
        own.add(w(r - from))
        if (event(r)) d += 1
        r += 1
      }
      while (removal < until && entry(entryOrder(removal)) >= t) {
        own.add(-w(entryOrder(removal) - from))
        removal += 1
      }
      if (d > 0) {
        val k = at(t)
        direct.add(d * (riskSets(k) / own.value))
      }
    }
    (observed, expected.value, personTime.value, direct.value)
  }

  /** η = xᵀβ + o of row r, uncentered (pprof_py's `X @ coef + offset`). */
  def eta(x: Array[Double], p: Int, r: Int, beta: Array[Double], offset: Array[Double]): Double = {
    var s = 0.0
    var j = 0
    while (j < p) {
      s += x(r * p + j) * beta(j)
      j += 1
    }
    s + offset(r)
  }

}
