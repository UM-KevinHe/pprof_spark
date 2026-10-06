package pprof.spark.numerics

import munit.FunSuite
import pprof.spark.numerics.kernels.{CoxBreslow, CoxTotals}

class CoxBreslowSuite extends FunSuite {

  // One stratum in canonical order: exit time descending, events first within a time.
  private val time = Array(4.0, 3.0, 3.0, 2.0, 2.0, 1.0)
  private val event = Array(false, true, true, true, false, true)
  private val x = Array(0.5, 1.0, -1.0, 0.0, 2.0, -0.5, 0.0, 1.5, 1.0, 1.0, -0.5, -2.0)
  private val zero = Array(0.0, 0.0)

  private def evaluate(
      beta: Array[Double],
      center: Array[Double] = zero,
      events: Array[Boolean] = event
  ) = {
    val totals = new CoxTotals(2)
    CoxBreslow.addStratum(time, events, x, 2, 0, time.length, beta, center, totals)
    (totals.value.value, totals.score.values, totals.information.values)
  }

  test("at beta = 0 the log partial likelihood is minus the sum of d times log risk-set size") {
    val expected = -(2.0 * StrictMath.log(3.0) + StrictMath.log(5.0) + StrictMath.log(6.0))
    val value = evaluate(zero)._1
    assert(math.abs(value - expected) <= 1e-15 * math.abs(expected), s"$value vs $expected")
  }

  test("score and information match finite differences of the log partial likelihood") {
    val beta = Array(0.3, -0.2)
    val h = 1e-5
    val (_, score, information) = evaluate(beta)
    def shifted(j: Int, s: Double) =
      beta.indices.map(i => if (i == j) beta(i) + s else beta(i)).toArray
    for (j <- 0 until 2) {
      val numeric = (evaluate(shifted(j, h))._1 - evaluate(shifted(j, -h))._1) / (2 * h)
      assert(math.abs(numeric - score(j)) <= 1e-8, s"score $j: ${score(j)} vs $numeric")
    }
    var k = 0
    for (j <- 0 until 2; l <- j until 2) {
      val numeric = -(evaluate(shifted(l, h))._2(j) - evaluate(shifted(l, -h))._2(j)) / (2 * h)
      assert(
        math.abs(numeric - information(k)) <= 1e-7,
        s"information $k: ${information(k)} vs $numeric"
      )
      k += 1
    }
  }

  test("centering the covariates leaves the results unchanged") {
    val beta = Array(0.4, 0.1)
    val center = Array(
      x.indices.filter(_ % 2 == 0).map(x(_)).sum / 6,
      x.indices.filter(_ % 2 == 1).map(x(_)).sum / 6
    )
    val (v0, u0, i0) = evaluate(beta)
    val (v1, u1, i1) = evaluate(beta, center)
    assert(math.abs(v0 - v1) <= 1e-13 * math.abs(v0))
    for (k <- u0.indices) assert(math.abs(u0(k) - u1(k)) <= 1e-13, s"score $k")
    for (k <- i0.indices) assert(math.abs(i0(k) - i1(k)) <= 1e-13, s"information $k")
  }

  test("a stratum without events contributes nothing") {
    val (v, u, i) = evaluate(Array(0.4, 0.1), events = Array.fill(6)(false))
    assertEquals(v, 0.0)
    assert(u.forall(_ == 0.0) && i.forall(_ == 0.0))
  }
}
