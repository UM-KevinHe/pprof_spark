// Sandbox probe for docs/spec/performance/step2-chunks.md §5.4 (round 177): the packed S2 update under five
// summations, against today's Neumaier sums. Not part of the build. Compile with Scala 2.13 and run as
// Probe <features> <rows> <read every k rows, 0 for none> <fraction of rows removed again> <chunk length L>.

import java.util.SplittableRandom

/** Sandbox probe for step 2's note: the packed S2 update (p(p+1)/2 sums per row) under five summations. */
object Probe {
  var L = 256
  final class Data(val p: Int, val n: Int, seed: Long) {
    val q: Int = p * (p + 1) / 2
    val x: Array[Double] = { val g = new SplittableRandom(seed); Array.fill(n * p)(g.nextGaussian()) }
    val r: Array[Double] = { val g = new SplittableRandom(seed + 1); Array.fill(n)(math.exp(0.5 * g.nextGaussian())) }
  }
  final class Acc(val q: Int, val kind: Int) {
    val sums = new Array[Double](q); val comps = new Array[Double](q); val pend = new Array[Double](q)
    var pending = 0
    def flush(): Unit = if ((kind == 3 || kind == 5) && pending > 0) {
      var k = 0
      while (k < q) {
        val v = pend(k); val s = sums(k); val t = s + v
        if (java.lang.Double.isFinite(t)) { if (math.abs(s) >= math.abs(v)) comps(k) += (s - t) + v else comps(k) += (v - t) + s }
        sums(k) = t; pend(k) = 0.0; k += 1
      }
      pending = 0
    }
    def value(k: Int): Double = if (java.lang.Double.isFinite(sums(k))) sums(k) + comps(k) else sums(k)
  }
  def rowNeumaier(sums: Array[Double], comps: Array[Double], x: Array[Double], base: Int, p: Int, w: Double): Unit = {
    var j = 0; var start = 0
    while (j < p) {
      val wx = w * x(base + j); val off = start - j; var l = j
      while (l < p) {
        val v = wx * x(base + l); val idx = off + l; val s = sums(idx); val t = s + v
        if (java.lang.Double.isFinite(t)) { if (math.abs(s) >= math.abs(v)) comps(idx) += (s - t) + v else comps(idx) += (v - t) + s }
        sums(idx) = t; l += 1
      }
      start += p - j; j += 1
    }
  }
  def rowTwoSum(sums: Array[Double], comps: Array[Double], x: Array[Double], base: Int, p: Int, w: Double): Unit = {
    var j = 0; var start = 0
    while (j < p) {
      val wx = w * x(base + j); val off = start - j; var l = j
      while (l < p) {
        val v = wx * x(base + l); val idx = off + l; val s = sums(idx); val t = s + v
        val bp = t - s; val ap = t - bp
        comps(idx) += (s - ap) + (v - bp)
        sums(idx) = t; l += 1
      }
      start += p - j; j += 1
    }
  }
  def rowSelect(sums: Array[Double], comps: Array[Double], x: Array[Double], base: Int, p: Int, w: Double): Unit = {
    var j = 0; var start = 0
    while (j < p) {
      val wx = w * x(base + j); val off = start - j; var l = j
      while (l < p) {
        val v = wx * x(base + l); val idx = off + l; val s = sums(idx); val t = s + v
        val swap = math.abs(s) < math.abs(v); val big = if (swap) v else s; val small = if (swap) s else v
        comps(idx) += (big - t) + small
        sums(idx) = t; l += 1
      }
      start += p - j; j += 1
    }
  }
  def rowPlain(acc: Array[Double], x: Array[Double], base: Int, p: Int, w: Double): Unit = {
    var j = 0; var start = 0
    while (j < p) {
      val wx = w * x(base + j); val off = start - j; var l = j
      while (l < p) { acc(off + l) += wx * x(base + l); l += 1 }
      start += p - j; j += 1
    }
  }
  def addRow(a: Acc, d: Data, i: Int, w: Double): Unit = a.kind match {
    case 0 => rowNeumaier(a.sums, a.comps, d.x, i * d.p, d.p, w)
    case 1 => rowTwoSum(a.sums, a.comps, d.x, i * d.p, d.p, w)
    case 2 => rowSelect(a.sums, a.comps, d.x, i * d.p, d.p, w)
    case 3 | 5 => rowPlain(a.pend, d.x, i * d.p, d.p, w); a.pending += 1; if (a.pending >= L) a.flush()
    case _ => rowPlain(a.sums, d.x, i * d.p, d.p, w)
  }
  def run(d: Data, kind: Int, readEvery: Int, removeFrac: Double): (Long, Double, Array[Double], Array[Double]) = {
    val a = new Acc(d.q, kind); var checksum = 0.0
    val t0 = System.nanoTime(); var i = 0
    while (i < d.n) {
      addRow(a, d, i, d.r(i))
      if (readEvery > 0 && (i + 1) % readEvery == 0) { if (kind != 5) a.flush(); var k = 0; while (k < d.q) { checksum += (if (kind == 5) a.value(k) + a.pend(k) else a.value(k)); k += 1 } }
      i += 1
    }
    a.flush(); val t1 = System.nanoTime()
    val afterAdd = Array.tabulate(d.q)(a.value)
    i = 0; while (i < (removeFrac * d.n).toInt) { addRow(a, d, i, -d.r(i)); i += 1 }
    a.flush()
    (t1 - t0, checksum, afterAdd, Array.tabulate(d.q)(a.value))
  }
  def readError(d: Data, kind: Int, readEvery: Int): Double = {
    if (readEvery == 0) return 0.0
    val a = new Acc(d.q, kind); val ref = new Acc(d.q, 0); var worst = 0.0; var i = 0
    while (i < d.n) {
      addRow(a, d, i, d.r(i)); addRow(ref, d, i, d.r(i))
      if ((i + 1) % readEvery == 0) {
        if (kind != 5) a.flush()
        var scale = 0.0; var k = 0
        while (k < d.q) { scale = math.max(scale, math.abs(ref.value(k))); k += 1 }
        k = 0
        while (k < d.q) { val v = if (kind == 5) a.value(k) + a.pend(k) else a.value(k); worst = math.max(worst, math.abs(v - ref.value(k)) / scale); k += 1 }
      }
      i += 1
    }
    worst
  }
  def main(args: Array[String]): Unit = {
    val (p, n, readEvery, removeFrac) = (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toDouble)
    val d = new Data(p, n, 42L)
    L = args(4).toInt
    val names = Array("neumaier", "twosum", "select", s"chunk$L", "plain", s"pending$L")
    val res = names.indices.map { kind =>
      var best = Long.MaxValue; var last: (Long, Double, Array[Double], Array[Double]) = null
      for (rep <- 0 until 8) { val r = run(d, kind, readEvery, removeFrac); if (rep >= 3) best = math.min(best, r._1); last = r }
      (best, last)
    }
    val ref = res(0)._2
    def bits(a: Array[Double], b: Array[Double]) = a.indices.count(k => java.lang.Double.doubleToRawLongBits(a(k)) != java.lang.Double.doubleToRawLongBits(b(k)))
    def scaled(a: Array[Double], b: Array[Double]) = a.indices.map(k => math.abs(a(k) - b(k))).max / b.map(math.abs).max
    println(s"p=$p n=$n readEvery=$readEvery removeFrac=$removeFrac java=${System.getProperty("java.version")}")
    names.indices.foreach { kind =>
      val (best, r) = res(kind)
      val reads = f"${readError(d, kind, readEvery)}%.1e"
      println(f"${names(kind)}%-9s ${best / 1e3 / n}%7.2f us/row  reads $reads%-7s bits differ ${bits(r._3, ref._3)}%5d/${d.q} after adds, ${bits(r._4, ref._4)}%5d after removals; scaled ${scaled(r._3, ref._3)}%.1e, after removals ${scaled(r._4, ref._4)}%.1e")
    }
  }
}
