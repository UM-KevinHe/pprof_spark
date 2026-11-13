package bench

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.LogisticFE

/** The cost of passes that need no p x p work: gradient-only and Hessian-vector passes. */
object GradBench {
  private def best(f: () => Unit): Double = {
    var b = Long.MaxValue; var t = 0
    while (t < 6) { val t0 = System.nanoTime(); f(); val dt = System.nanoTime() - t0; if (t >= 3 && dt < b) b = dt; t += 1 }
    b / 1e9
  }

  /** Log partial likelihood and score of one stratum (Breslow, rows without entry times); with `v`, also I v. */
  def coxSweep(d: Data, from: Int, until: Int, beta: Array[Double], v: Array[Double], score: NeumaierVector, hv: NeumaierVector): Double = {
    val p = d.p
    val s0 = new NeumaierSum; val s1 = new NeumaierVector(p)
    val sa = new NeumaierSum; val sxa = new NeumaierVector(p)
    val xc = new Array[Double](p); val eventSum = new Array[Double](p)
    var value = 0.0
    var r = from
    while (r < until) {
      val t = d.time(r)
      var dn = 0; var dw = 0.0; var etaEvents = 0.0
      java.util.Arrays.fill(eventSum, 0.0)
      while (r < until && d.time(r) == t) {
        var j = 0; var linear = 0.0; var a = 0.0
        while (j < p) { xc(j) = d.x(r * p + j) - d.center(j); linear += xc(j) * beta(j); j += 1 }
        val rw = StrictMath.exp(linear)
        s0.add(rw)
        if (v != null) {
          j = 0
          while (j < p) { a += xc(j) * v(j); j += 1 }
          sa.add(rw * a)
          j = 0
          while (j < p) { s1.addAt(j, rw * xc(j)); sxa.addAt(j, rw * a * xc(j)); j += 1 }
        } else {
          j = 0
          while (j < p) { s1.addAt(j, rw * xc(j)); j += 1 }
        }
        if (d.event(r)) {
          dn += 1; dw += 1.0; etaEvents += linear
          j = 0
          while (j < p) { eventSum(j) += xc(j); j += 1 }
        }
        r += 1
      }
      if (dn > 0) {
        val total0 = s0.value
        value += etaEvents - dw * StrictMath.log(total0)
        val abar = if (v != null) sa.value / total0 else 0.0
        var j = 0
        while (j < p) {
          val mean = s1.valueAt(j) / total0
          score.addAt(j, eventSum(j) - dw * mean)
          if (v != null) hv.addAt(j, dw * (sxa.valueAt(j) / total0 - mean * abar))
          j += 1
        }
      }
    }
    value
  }

  /** Profile gradient of the logistic model for the providers of a block: each intercept solved, then X'(y - pi). */
  def logisticProfile(d: Data, beta: Array[Double], gamma: Array[Double], grad: NeumaierVector): (Double, Int) = {
    val p = d.p
    val eta0 = new Array[Double](d.perGroup)
    var value = 0.0; var inner = 0
    var k = 0
    while (k < d.groups) {
      val from = d.groupStart(k)
      var i = 0
      while (i < d.perGroup) {
        var dot = 0.0; var j = 0
        while (j < p) { dot += d.x((from + i) * p + j) * beta(j); j += 1 }
        eta0(i) = dot; i += 1
      }
      var g = gamma(k); var step = 1.0
      while (math.abs(step) > 1e-11 && inner < 100000000) {
        var num = 0.0; var den = 0.0
        i = 0
        while (i < d.perGroup) { val pi = LogisticFE.sigmoid(g + eta0(i)); num += d.y(from + i) - pi; den += pi * (1.0 - pi); i += 1 }
        step = num / den; g += step; inner += 1
      }
      gamma(k) = g
      i = 0
      while (i < d.perGroup) {
        val e = g + eta0(i); val res = d.y(from + i) - LogisticFE.sigmoid(e)
        value += LogisticFE.rowLoglik(d.y(from + i), 1.0, e)
        var j = 0
        while (j < p) { grad.addAt(j, d.x((from + i) * p + j) * res); j += 1 }
        i += 1
      }
      k += 1
    }
    (value, inner)
  }

  def main(args: Array[String]): Unit = {
    val p = args(0).toInt; val groups = args(1).toInt; val perGroup = args(2).toInt
    val d = new Data(p, groups, perGroup, 79L)
    val beta = d.beta; val v = Array.tabulate(p)(j => if (j % 3 == 0) 0.1 else -0.05)
    println(s"n=${d.n} p=$p java=${System.getProperty("java.version")}")
    def line(name: String, sec: Double): Unit = println(f"$name%-46s ${sec * 1e3}%8.1f ms ${sec * 1e6 / d.n}%7.2f us/row")
    line("cox: log-likelihood and score only", best { () =>
      val score = new NeumaierVector(p); var g = 0
      while (g < groups) { coxSweep(d, d.groupStart(g), d.groupStart(g) + perGroup, beta, null, score, null); g += 1 }
    })
    line("cox: score and one Hessian-vector product", best { () =>
      val score = new NeumaierVector(p); val hv = new NeumaierVector(p); var g = 0
      while (g < groups) { coxSweep(d, d.groupStart(g), d.groupStart(g) + perGroup, beta, v, score, hv); g += 1 }
    })
    val ybar = d.y.sum / d.n
    var inner = 0
    line("logistic: profile likelihood and gradient", best { () =>
      val gamma = Array.fill(groups)(StrictMath.log(ybar / (1.0 - ybar)))
      inner = logisticProfile(d, beta, gamma, new NeumaierVector(p))._2
    })
    println(f"  inner Newton steps per provider, from the common start: ${inner.toDouble / groups}%.1f")
  }
}
