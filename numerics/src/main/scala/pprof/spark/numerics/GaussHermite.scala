package pprof.spark.numerics

/** Gauss–Hermite rules for the weight e^(−t²) (Phase 2f-2 specification §4): nodes by Newton's method on the
  * normalized Hermite recurrence from asymptotic starting values (as Numerical Recipes' `gauher`), in
  * ascending order, symmetric, with exact zero as the middle node of an odd rule.
  */
object GaussHermite {

  final case class Rule(nodes: Array[Double], weights: Array[Double]) {

    /** log wₖ + tₖ², the adaptive rule's log weights (pprof_py's `_adaptive_rule`). */
    def adaptiveLogWeights: Array[Double] =
      nodes.indices.map(k => StrictMath.log(weights(k)) + nodes(k) * nodes(k)).toArray
  }

  def rule(n: Int): Rule = {
    require(n >= 1 && n <= 200, s"Gauss–Hermite rules have 1 to 200 nodes, got $n")
    val x = new Array[Double](n)
    val w = new Array[Double](n)
    val piM4 = StrictMath.pow(StrictMath.PI, -0.25)
    val m = (n + 1) / 2
    var z = 0.0
    var i = 0
    while (i < m) {
      z = i match {
        case 0 => StrictMath.sqrt(2.0 * n + 1.0) - 1.85575 * StrictMath.pow(2.0 * n + 1.0, -0.16667)
        case 1 => z - 1.14 * StrictMath.pow(n.toDouble, 0.426) / z
        case 2 => 1.86 * z - 0.86 * x(0)
        case 3 => 1.91 * z - 0.91 * x(1)
        case _ => 2.0 * z - x(i - 2)
      }
      var derivative = 0.0
      var iteration = 0
      var done = false
      while (!done) {
        var p1 = piM4
        var p2 = 0.0
        var j = 1
        while (j <= n) {
          val p3 = p2
          p2 = p1
          p1 = z * StrictMath.sqrt(2.0 / j) * p2 - StrictMath.sqrt((j - 1.0) / j) * p3
          j += 1
        }
        derivative = StrictMath.sqrt(2.0 * n) * p2
        val previous = z
        z = previous - p1 / derivative
        iteration += 1
        done = math.abs(z - previous) <= 1e-15 * math.max(1.0, math.abs(z)) || iteration >= 100
      }
      if (n % 2 == 1 && i == m - 1) z = 0.0
      x(i) = z
      x(n - 1 - i) = -z
      w(i) = 2.0 / (derivative * derivative)
      w(n - 1 - i) = w(i)
      i += 1
    }
    if (n % 2 == 1) {
      // the middle node's weight from the recurrence at exactly zero
      var p1 = piM4
      var p2 = 0.0
      var j = 1
      while (j <= n) {
        val p3 = p2
        p2 = p1
        p1 = -StrictMath.sqrt((j - 1.0) / j) * p3
        j += 1
      }
      val d = StrictMath.sqrt(2.0 * n) * p2
      w(m - 1) = 2.0 / (d * d)
    }
    if (n % 2 == 1) x(m - 1) = 0.0 // not −0.0 from the mirrored assignment
    val order = (0 until n).sortBy(k => x(k)).toArray
    Rule(order.map(x), order.map(w))
  }
}
