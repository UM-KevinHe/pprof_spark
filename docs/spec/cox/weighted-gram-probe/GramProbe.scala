// Sandbox probe for docs/spec/cox/weighted-gram.md (round 183): the weighted-Gram information of one stratum against
// round 182's kernel (`CoxStratum.add`), Breslow and Efron, with and without truncation. Not part of the build: compile
// against the numerics classes and run as GramProbe <rows> <features> <distinct times> <truncated fraction>.
import java.util.SplittableRandom

import pprof.spark.numerics.{ChunkedVector, NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.{CoxRows, CoxStratum, CoxTotals, Moments}

object GramProbe {

  /** The information I = Σᵢ ωᵢ xᵢxᵢᵀ − Σₜ Σₖ aₜₖ m̄ₜₖm̄ₜₖᵀ of rows [0, n) (weighted-gram.md §6). */
  def gram(rows: CoxRows, beta: Array[Double], center: Array[Double], efron: Boolean): Array[Double] = {
    val n = rows.rows
    val p = rows.p
    val q = Moments.packedLength(p)
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val subtractive = new ChunkedVector(q)
    val sp = subtractive.pending
    val risk = new Array[Double](n)
    val xc = new Array[Double](p)
    val d1 = new Array[Double](p)
    val m = new Array[Double](p)
    val times = Array.newBuilder[Double]
    val hs = Array.newBuilder[Double]
    val psis = Array.newBuilder[Double]
    val ds = Array.newBuilder[Int]
    var removal = 0
    var r = 0
    def rankOne(a: Double): Unit = {
      var j = 0
      var k = 0
      while (j < p) {
        val am = a * m(j)
        val off = k - j
        var l = j
        while (l < p) { sp(off + l) += am * m(l); l += 1 }
        k += p - j
        j += 1
      }
      subtractive.endTerm()
    }
    while (r < n) {
      val t = rows.timeAt(r)
      var d = 0
      var dw = 0.0
      var d0 = 0.0
      java.util.Arrays.fill(d1, 0.0)
      while (r < n && rows.timeAt(r) == t) {
        val w = rows.weightAt(r)
        if (w > 0.0) {
          val xs = rows.xs(r); val base = rows.xBase(r)
          var j = 0; var linear = 0.0
          while (j < p) { xc(j) = xs(base + j) - center(j); linear += xc(j) * beta(j); j += 1 }
          val rw = w * StrictMath.exp(linear + rows.offsetAt(r))
          risk(r) = rw
          s0.add(rw)
          j = 0
          while (j < p) { s1.addAt(j, rw * xc(j)); j += 1 }
          if (rows.eventAt(r)) {
            d += 1; dw += w; d0 += rw
            j = 0
            while (j < p) { d1(j) += rw * xc(j); j += 1 }
          }
        }
        r += 1
      }
      while (removal < n && rows.entryAt(rows.entryOrderAt(removal)) >= t) {
        val i = rows.entryOrderAt(removal)
        if (rows.weightAt(i) > 0.0) {
          s0.add(-risk(i))
          val xs = rows.xs(i); val base = rows.xBase(i)
          var j = 0
          while (j < p) { s1.addAt(j, -(risk(i) * (xs(base + j) - center(j)))); j += 1 }
        }
        removal += 1
      }
      if (d > 0) {
        val total0 = s0.value
        val total1 = s1.values
        if (!efron || d == 1) {
          var j = 0
          while (j < p) { m(j) = total1(j) / total0; j += 1 }
          rankOne(dw)
          times += t; hs += dw / total0; psis += 0.0; ds += d
        } else {
          val mean = dw / d
          var h = 0.0
          var dying = 0.0
          var k = 1
          while (k <= d) {
            val f = k.toDouble / d
            val a = (total0 - d0) + f * d0
            val step = mean / a
            h += step
            dying += f * step
            var j = 0
            while (j < p) { m(j) = ((total1(j) - d1(j)) + f * d1(j)) / a; j += 1 }
            rankOne(mean)
            k += 1
          }
          times += t; hs += h; psis += h - dying; ds += d
        }
      }
    }
    val ts = times.result().reverse
    val h = hs.result().reverse
    val psi = psis.result().reverse
    val tied = ds.result().reverse
    val total = new Array[Double](ts.length)
    val running = new NeumaierSum
    var e = 0
    while (e < ts.length) { running.add(h(e)); total(e) = running.value; e += 1 }
    def lastAtOrBefore(v: Double): Int = {
      var (lo, hi, found) = (0, ts.length - 1, -1)
      while (lo <= hi) { val mid = (lo + hi) >>> 1; if (ts(mid) <= v) { found = mid; lo = mid + 1 } else hi = mid - 1 }
      found
    }
    val gram = new ChunkedVector(q)
    val gp = gram.pending
    r = 0
    while (r < n) {
      if (rows.weightAt(r) > 0.0) {
        val kb = lastAtOrBefore(rows.timeAt(r))
        val ka = lastAtOrBefore(rows.entryAt(r))
        val lambda = (if (kb >= 0) total(kb) else 0.0) - (if (ka >= 0) total(ka) else 0.0)
        val corrected = efron && rows.eventAt(r) && kb >= 0 && tied(kb) >= 2
        val omega = risk(r) * (if (corrected) lambda - psi(kb) else lambda)
        val xs = rows.xs(r); val base = rows.xBase(r)
        var j = 0
        while (j < p) { xc(j) = xs(base + j) - center(j); j += 1 }
        j = 0
        var k = 0
        while (j < p) {
          val wx = omega * xc(j)
          val off = k - j
          var l = j
          while (l < p) { gp(off + l) += wx * xc(l); l += 1 }
          k += p - j
          j += 1
        }
        gram.endTerm()
      }
      r += 1
    }
    val g = gram.values
    val s = subtractive.values
    Array.tabulate(q)(k => g(k) - s(k))
  }

  def main(args: Array[String]): Unit = {
    val (n, p, distinct, truncated) = (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toDouble)
    val rng = new SplittableRandom(29L)
    val raw = Array.fill(n) {
      val t = 1.0 + rng.nextInt(distinct)
      val late = rng.nextDouble() < truncated
      (t, rng.nextDouble() < 0.6, if (rng.nextDouble() < 0.03) 0.0 else 0.5 + rng.nextDouble(),
       if (late) (rng.nextInt(t.toInt)).toDouble else 0.0, Array.fill(p)(rng.nextGaussian()))
    }.sortBy(r => (-r._1, !r._2))
    val rows = CoxRows(raw.map(_._1), raw.map(_._4), (0 until n).sortBy(i => (-raw(i)._4, i)).toArray, raw.map(_._2),
      raw.map(_._3), Array.fill(n)(0.0), raw.flatMap(_._5), p)
    val beta = Array.tabulate(p)(j => 0.02 * ((j % 7) - 3))
    val center = new Array[Double](p)
    println(s"rows=$n features=$p distinct times=$distinct truncated=$truncated java=${System.getProperty("java.version")}")
    for (efron <- Seq(false, true)) {
      def kernel(): Array[Double] = { val t = new CoxTotals(p); CoxStratum.add(rows, 0, n, beta, center, efron, t); t.information.values }
      def best(f: () => Array[Double]): (Double, Array[Double]) = {
        var out: Array[Double] = null; var bestNs = Long.MaxValue
        for (i <- 0 until 8) { val t0 = System.nanoTime(); out = f(); val ns = System.nanoTime() - t0; if (i >= 3) bestNs = math.min(bestNs, ns) }
        (bestNs / 1e3 / n, out)
      }
      val (kt, ki) = best(() => kernel())
      val (gt, gi) = best(() => gram(rows, beta, center, efron))
      val scaled = ki.indices.map(i => math.abs(ki(i) - gi(i))).max / ki.map(math.abs).max
      println(f"${if (efron) "efron  " else "breslow"} round 182 kernel ${kt}%7.2f us/row   weighted Gram ${gt}%7.2f us/row   ratio ${kt / gt}%5.2f   information scaled difference $scaled%.2e")
    }
  }
}
