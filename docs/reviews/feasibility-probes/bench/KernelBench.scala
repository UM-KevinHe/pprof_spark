package bench

import java.io.{BufferedOutputStream, DataOutputStream, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder}

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.{CoxStratum, CoxTotals, LogisticFE, Moments}

/** Deterministic synthetic data shaped like databricks/02_scale.py's generator. */
final class Data(val p: Int, val groups: Int, val perGroup: Int, seed: Long) {
  val n: Int = groups * perGroup
  private def mix(z0: Long): Long = {
    var z = z0 + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }
  private def unif(key: Long, stream: Long): Double =
    ((mix(mix(key + seed) + stream * 0x100000001b3L) >>> 11).toDouble + 0.5) / 9007199254740992.0
  val beta: Array[Double] = Array.tabulate(p)(k0 => (if ((k0 + 1) % 2 == 0) 1.0 else -1.0) * 0.5 / math.sqrt(p.toDouble))
  val x = new Array[Double](n * p)
  val time = new Array[Double](n)
  val event = new Array[Boolean](n)
  val y = new Array[Double](n)
  val groupStart: Array[Int] = Array.tabulate(groups)(_ * perGroup)
  val groupOf = new Array[Int](n)
  locally {
    var g = 0
    while (g < groups) {
      val effect = (math.floor(unif(g.toLong, 900001L) * 201.0) - 100.0) / 200.0
      val xs = new Array[Double](perGroup * p)
      val ts = new Array[Double](perGroup)
      val es = new Array[Boolean](perGroup)
      val ys = new Array[Double](perGroup)
      var r = 0
      while (r < perGroup) {
        val id = g.toLong * perGroup + r
        var lin = 0.0
        var k = 0
        while (k < p) {
          val u = unif(id, k + 1L)
          val v = if ((k + 1) % 2 == 1) (if (u < 0.3) 1.0 else 0.0) else (math.floor(u * 129.0) - 64.0) / 64.0
          xs(r * p + k) = v
          lin += beta(k) * v
          k += 1
        }
        val hazard = 0.2 * math.exp(effect + lin)
        val ev = -math.log(unif(id, 700001L)) / hazard
        val ce = 5.0 * unif(id, 700002L)
        ts(r) = math.max(1.0, math.ceil(365.0 * math.min(ev, math.min(ce, 5.0))))
        es(r) = ev <= math.min(ce, 5.0)
        ys(r) = if (unif(id, 700003L) < 1.0 / (1.0 + math.exp(-(effect + lin)))) 1.0 else 0.0
        r += 1
      }
      // canonical Cox order: time descending, events first, row id ascending
      val order = (0 until perGroup).toArray.sortWith { (a, b) =>
        if (ts(a) != ts(b)) ts(a) > ts(b) else if (es(a) != es(b)) es(a) else a < b
      }
      r = 0
      while (r < perGroup) {
        val src = order(r)
        val dst = g * perGroup + r
        System.arraycopy(xs, src * p, x, dst * p, p)
        time(dst) = ts(src); event(dst) = es(src); y(dst) = ys(src); groupOf(dst) = g
        r += 1
      }
      g += 1
    }
  }
  val weight: Array[Double] = Array.fill(n)(1.0)
  val offset: Array[Double] = new Array[Double](n)
  val entry: Array[Double] = new Array[Double](n)
  val entryOrder: Array[Int] = Array.tabulate(n)(identity)
  val center: Array[Double] = {
    val c = new Array[Double](p)
    var r = 0
    while (r < n) { var k = 0; while (k < p) { c(k) += x(r * p + k); k += 1 }; r += 1 }
    c.map(_ / n)
  }
  def events: Int = event.count(identity)
  def distinctEventTimes: Int = {
    var c = 0; var r = 0
    while (r < n) {
      if (event(r) && (r == groupStart(groupOf(r)) || !(time(r - 1) == time(r) && event(r - 1) && groupOf(r - 1) == groupOf(r)))) c += 1
      r += 1
    }
    c
  }
  def dump(dir: String): Unit = {
    def doubles(name: String, a: Array[Double]): Unit = {
      val out = new FileOutputStream(s"$dir/$name")
      val ch = out.getChannel
      val buf = ByteBuffer.allocate(1 << 20).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < a.length) {
        if (!buf.hasRemaining) { buf.flip(); ch.write(buf); buf.clear() }
        buf.putDouble(a(i)); i += 1
      }
      buf.flip(); ch.write(buf); out.close()
    }
    doubles("x.f64", x); doubles("time.f64", time); doubles("y.f64", y)
    doubles("event.f64", event.map(b => if (b) 1.0 else 0.0))
    doubles("group.f64", groupOf.map(_.toDouble))
    val meta = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(s"$dir/meta.txt")))
    meta.writeBytes(s"$n $p $groups $perGroup\n"); meta.close()
  }
}

/** Prototype kernels for the review: identical statistics, different accumulation. */
object Proto {

  /** Same sweep as CoxStratum.add (Breslow), plain sums in row arrays. Returns (value, score, info rows). */
  final class CoxAcc(val p: Int) {
    var value = 0.0
    val score = new Array[Double](p)
    val info: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
  }

  def coxPlain(d: Data, from: Int, until: Int, beta: Array[Double], acc: CoxAcc): Unit = {
    val p = d.p
    var s0 = 0.0
    val s1 = new Array[Double](p)
    val s2 = Array.fill(p)(new Array[Double](p))
    val xc = new Array[Double](p)
    val eventSum = new Array[Double](p)
    val mean = new Array[Double](p)
    var r = from
    while (r < until) {
      val t = d.time(r)
      var dn = 0; var dw = 0.0; var etaEvents = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      while (r < until && d.time(r) == t) {
        val w = d.weight(r)
        var j = 0; var linear = 0.0
        while (j < p) { xc(j) = d.x(r * p + j) - d.center(j); linear += xc(j) * beta(j); j += 1 }
        val eta = linear + d.offset(r)
        val rw = w * StrictMath.exp(eta)
        s0 += rw
        j = 0
        while (j < p) {
          val wx = rw * xc(j)
          s1(j) += wx
          val row = s2(j)
          var l = j
          while (l < p) { row(l) += wx * xc(l); l += 1 }
          j += 1
        }
        if (d.event(r)) {
          dn += 1; dw += w; etaEvents += w * eta
          j = 0
          while (j < p) { eventSum(j) += w * xc(j); j += 1 }
        }
        r += 1
      }
      if (dn > 0) {
        acc.value += etaEvents - dw * StrictMath.log(s0)
        var j = 0
        while (j < p) { mean(j) = s1(j) / s0; acc.score(j) += eventSum(j) - dw * mean(j); j += 1 }
        j = 0
        while (j < p) {
          val mj = mean(j)
          val row = s2(j); val out = acc.info(j)
          var l = j
          while (l < p) { out(l) += dw * (row(l) / s0 - mj * mean(l)); l += 1 }
          j += 1
        }
      }
    }
  }

  /** Weighted-Gram form: I = sum_i r_i c_i x_i x_i' - sum_t d_t xbar_t xbar_t', with
    * c_i = sum of d_t/S0(t) over the event times at which row i is at risk. Right-censored rows
    * (entry below every event time), Breslow. `chunk` > 0 folds plain chunk sums into Neumaier totals.
    */
  final class GramAcc(val p: Int, val chunk: Int) {
    val value = new NeumaierSum
    val score = new NeumaierVector(p)
    val a: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
    val b: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
    val total: Array[NeumaierVector] = Array.fill(p)(new NeumaierVector(p))
    private var pending = 0
    def tick(): Unit = if (chunk > 0) { pending += 1; if (pending >= chunk) flush() }
    def flush(): Unit = if (chunk > 0) {
      var j = 0
      while (j < p) {
        val aj = a(j); val bj = b(j); val tj = total(j)
        var l = j
        while (l < p) { tj.addAt(l, aj(l) - bj(l)); aj(l) = 0.0; bj(l) = 0.0; l += 1 }
        j += 1
      }
      pending = 0
    }
    def info(j: Int, l: Int): Double = if (chunk > 0) total(j).valueAt(l) else a(j)(l) - b(j)(l)
  }

  def coxGram(d: Data, from: Int, until: Int, beta: Array[Double], acc: GramAcc): Unit = {
    val p = d.p
    val n = until - from
    val s0 = new NeumaierSum
    val s1 = new NeumaierVector(p)
    val xc = new Array[Double](p)
    val eventSum = new Array[Double](p)
    val mean = new Array[Double](p)
    val risk = new Array[Double](n)
    val atExit = new Array[Double](n)
    var cum = 0.0 // sum of d_t/S0(t) over the event times already swept (later times)
    var r = from
    while (r < until) {
      val t = d.time(r)
      var dn = 0; var dw = 0.0; var etaEvents = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      while (r < until && d.time(r) == t) {
        val w = d.weight(r)
        var j = 0; var linear = 0.0
        while (j < p) { xc(j) = d.x(r * p + j) - d.center(j); linear += xc(j) * beta(j); j += 1 }
        val eta = linear + d.offset(r)
        val rw = w * StrictMath.exp(eta)
        risk(r - from) = rw
        atExit(r - from) = cum
        s0.add(rw)
        j = 0
        while (j < p) { s1.addAt(j, rw * xc(j)); j += 1 }
        if (d.event(r)) {
          dn += 1; dw += w; etaEvents += w * eta
          j = 0
          while (j < p) { eventSum(j) += w * xc(j); j += 1 }
        }
        r += 1
      }
      if (dn > 0) {
        val total0 = s0.value
        acc.value.add(etaEvents - dw * StrictMath.log(total0))
        var j = 0
        while (j < p) { mean(j) = s1.valueAt(j) / total0; acc.score.addAt(j, eventSum(j) - dw * mean(j)); j += 1 }
        j = 0
        while (j < p) {
          val wm = dw * mean(j)
          val row = acc.b(j)
          var l = j
          while (l < p) { row(l) += wm * mean(l); l += 1 }
          j += 1
        }
        acc.tick()
        cum += dw / total0
      }
    }
    // second sweep: each row's weight r_i * (cum_end - cum_at_exit)
    r = from
    while (r < until) {
      val c = risk(r - from) * (cum - atExit(r - from))
      if (c != 0.0) {
        var j = 0
        while (j < p) { xc(j) = d.x(r * p + j) - d.center(j); j += 1 }
        j = 0
        while (j < p) {
          val wx = c * xc(j)
          val row = acc.a(j)
          var l = j
          while (l < p) { row(l) += wx * xc(l); l += 1 }
          j += 1
        }
        acc.tick()
      }
      r += 1
    }
  }

  /** Pass A of SerBIN with the production algebra, the Schur sums accumulated plainly in row arrays;
    * `chunk` > 0 folds chunk sums into Neumaier totals.
    */
  final class SchurAcc(val p: Int, val chunk: Int) {
    val s: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
    val total: Array[NeumaierVector] = Array.fill(p)(new NeumaierVector(p))
    val rhs = new NeumaierVector(p)
    val scoreTerm = new NeumaierSum
    val loglik = new NeumaierSum
    private var pending = 0
    def tick(): Unit = if (chunk > 0) { pending += 1; if (pending >= chunk) flush() }
    def flush(): Unit = if (chunk > 0) {
      var j = 0
      while (j < p) {
        val sj = s(j); val tj = total(j)
        var l = j
        while (l < p) { tj.addAt(l, sj(l)); sj(l) = 0.0; l += 1 }
        j += 1
      }
      pending = 0
    }
    def at(j: Int, l: Int): Double = if (chunk > 0) total(j).valueAt(l) else s(j)(l)
  }

  def schur(d: Data, gamma: Array[Double], beta: Array[Double], acc: SchurAcc): Unit = {
    val p = d.p
    val pi = new Array[Double](d.perGroup)
    val q = new Array[Double](d.perGroup)
    val xbar = new Array[Double](p)
    val dd = new Array[Double](p)
    val bSum = new NeumaierVector(p)
    var k = 0
    while (k < d.groups) {
      val from = d.groupStart(k); val until = from + d.perGroup
      val g = new NeumaierSum; val h = new NeumaierSum
      var j = 0
      val b = new NeumaierVector(p)
      var r = from
      while (r < until) {
        var dot = 0.0
        j = 0
        while (j < p) { dot += d.x(r * p + j) * beta(j); j += 1 }
        val e = gamma(k) + dot
        val prob = LogisticFE.sigmoid(e)
        val weight = math.max(prob * (1.0 - prob), LogisticFE.WeightFloor)
        pi(r - from) = prob; q(r - from) = weight
        g.add(d.y(r) - prob); h.add(weight)
        acc.loglik.add(LogisticFE.rowLoglik(d.y(r), 1.0, e))
        j = 0
        while (j < p) { b.addAt(j, weight * d.x(r * p + j)); j += 1 }
        r += 1
      }
      val hv = h.value
      acc.scoreTerm.add(g.value * g.value / hv)
      j = 0
      while (j < p) { xbar(j) = b.valueAt(j) / hv; j += 1 }
      r = from
      while (r < until) {
        j = 0
        while (j < p) { dd(j) = d.x(r * p + j) - xbar(j); j += 1 }
        val residual = d.y(r) - pi(r - from)
        val w = q(r - from)
        j = 0
        while (j < p) {
          acc.rhs.addAt(j, dd(j) * residual)
          val wd = w * dd(j)
          val row = acc.s(j)
          var l = j
          while (l < p) { row(l) += wd * dd(l); l += 1 }
          j += 1
        }
        acc.tick()
        r += 1
      }
      k += 1
    }
    val _ = bSum
  }
}

object KernelBench {
  private def best(reps: Int, warm: Int)(f: () => Unit): Double = {
    var b = Long.MaxValue; var t = 0
    while (t < reps + warm) { val t0 = System.nanoTime(); f(); val dt = System.nanoTime() - t0; if (t >= warm && dt < b) b = dt; t += 1 }
    b / 1e9
  }
  private def maxScaled(a: Array[Double], b: Array[Double]): Double = {
    val s = b.iterator.map(math.abs).max
    a.indices.iterator.map(i => math.abs(a(i) - b(i))).max / s
  }

  def main(args: Array[String]): Unit = {
    val p = args(0).toInt; val groups = args(1).toInt; val perGroup = args(2).toInt
    val d = new Data(p, groups, perGroup, 79L)
    if (args.length > 3 && args(3) != "-") d.dump(args(3))
    val chunks: Seq[Int] = if (args.length > 4) args(4).split(",").map(_.toInt).toSeq else Seq(0, 64)
    val q = Moments.packedLength(p)
    println(s"n=${d.n} p=$p groups=$groups perGroup=$perGroup events=${d.events} distinctEventTimes=${d.distinctEventTimes} java=${System.getProperty("java.version")}")
    val beta = d.beta
    def line(name: String, sec: Double, extra: String = ""): Unit =
      println(f"$name%-34s ${sec * 1e3}%9.1f ms  ${sec * 1e6 / d.n}%8.2f us/row  $extra")

    // ---- Cox -------------------------------------------------------------------------------
    var ref: CoxTotals = null
    val tProd = best(3, 2) { () =>
      val totals = new CoxTotals(p)
      var g = 0
      while (g < groups) {
        CoxStratum.add(d.time, d.entry, d.entryOrder, d.event, d.weight, d.offset, d.x, p, d.groupStart(g), d.groupStart(g) + perGroup, beta, d.center, false, totals)
        g += 1
      }
      ref = totals
    }
    line("cox production (Neumaier)", tProd)
    val refInfo = ref.information.values; val refScore = ref.score.values; val refValue = ref.value.value

    var plain: Proto.CoxAcc = null
    val tPlain = best(3, 2) { () =>
      val acc = new Proto.CoxAcc(p)
      var g = 0
      while (g < groups) { Proto.coxPlain(d, d.groupStart(g), d.groupStart(g) + perGroup, beta, acc); g += 1 }
      plain = acc
    }
    def packRows(f: (Int, Int) => Double): Array[Double] = {
      val out = new Array[Double](q); var k = 0; var j = 0
      while (j < p) { var l = j; while (l < p) { out(k) = f(j, l); k += 1; l += 1 }; j += 1 }
      out
    }
    line("cox same sweep, plain sums", tPlain,
      f"x${tProd / tPlain}%.1f  dInfo ${maxScaled(packRows((j, l) => plain.info(j)(l)), refInfo)}%.1e dScore ${maxScaled(plain.score, refScore)}%.1e dValue ${math.abs(plain.value - refValue) / math.abs(refValue)}%.1e")

    for (chunk <- chunks) {
      var gram: Proto.GramAcc = null
      val t = best(3, 2) { () =>
        val acc = new Proto.GramAcc(p, chunk)
        var g = 0
        while (g < groups) { Proto.coxGram(d, d.groupStart(g), d.groupStart(g) + perGroup, beta, acc); g += 1 }
        acc.flush()
        gram = acc
      }
      line(if (chunk == 0) "cox weighted Gram, plain sums" else s"cox weighted Gram, chunk $chunk + Neumaier", t,
        f"x${tProd / t}%.1f  dInfo ${maxScaled(packRows((j, l) => gram.info(j, l)), refInfo)}%.1e dScore ${maxScaled(gram.score.values, refScore)}%.1e dValue ${math.abs(gram.value.value - refValue) / math.abs(refValue)}%.1e")
    }

    // ---- Logistic --------------------------------------------------------------------------
    val ybar = d.y.sum / d.n
    val gamma = Array.fill(groups)(StrictMath.log(ybar / (1.0 - ybar)))
    val rows = LogisticFE.Rows(d.groupStart, d.y, Array.fill(d.n)(1.0), d.x, p)
    var sRef: LogisticFE.Schur = null
    val tL = best(3, 2) { () => sRef = LogisticFE.schur(rows, gamma, beta) }
    line("logistic pass A production", tL)
    val steps = Array.tabulate(8)(i => math.pow(0.6, i.toDouble))
    val db = beta.map(_ * 0.01)
    val tB = best(3, 2) { () => LogisticFE.trial(rows, gamma, beta, db, steps); () }
    line("logistic pass B production", tB)
    for (chunk <- chunks) {
      var acc: Proto.SchurAcc = null
      val t = best(3, 2) { () =>
        val a = new Proto.SchurAcc(p, chunk)
        Proto.schur(d, gamma, beta, a)
        a.flush()
        acc = a
      }
      line(if (chunk == 0) "logistic pass A, plain sums" else s"logistic pass A, chunk $chunk + Neumaier", t,
        f"x${tL / t}%.1f  dS ${maxScaled(packRows((j, l) => acc.at(j, l)), sRef.packed)}%.1e dRhs ${maxScaled(acc.rhs.values, sRef.rhs)}%.1e dLoglik ${math.abs(acc.loglik.value - sRef.loglik) / math.abs(sRef.loglik)}%.1e")
    }
    val tM = best(3, 2) { () => Moments.centred(d.x, p, 0, d.n); () }
    line("summary co-moments (Moments.centred)", tM)
  }
}
