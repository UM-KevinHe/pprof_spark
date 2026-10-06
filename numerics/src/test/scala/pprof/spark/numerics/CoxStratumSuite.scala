package pprof.spark.numerics

import munit.FunSuite
import pprof.spark.numerics.kernels.{CoxStratum, CoxTotals}

class CoxStratumSuite extends FunSuite {

  // One stratum in canonical order: exit time descending, events first within a time.
  private val time = Array(4.0, 3.0, 3.0, 3.0, 2.0, 2.0, 1.0)
  private val event = Array(false, true, true, true, true, false, true)
  private val x = Array(0.5, 1.0, -1.0, 0.0, 2.0, -0.5, 0.25, 0.75, 0.0, 1.5, 1.0, 1.0, -0.5, -2.0)
  private val zero = Array(0.0, 0.0)
  private val ones = Array.fill(time.length)(1.0)
  private val noOffset = Array.fill(time.length)(0.0)
  private val weights = Array(1.5, 0.5, 2.0, 1.0, 0.75, 1.25, 1.0)
  private val offsets = Array(0.1, -0.2, 0.0, 0.3, -0.1, 0.2, 0.05)

  private def evaluate(
      beta: Array[Double],
      efron: Boolean,
      w: Array[Double] = ones,
      o: Array[Double] = noOffset,
      events: Array[Boolean] = event,
      center: Array[Double] = zero
  ) = {
    val totals = new CoxTotals(2)
    CoxStratum.add(time, events, w, o, x, 2, 0, time.length, beta, center, efron, totals)
    (totals.value.value, totals.score.values, totals.information.values)
  }

  test("Breslow at beta = 0 is minus the sum of d times log risk-set size") {
    // t = 3: 4 at risk, 3 events; t = 2: 6 at risk, 1 event; t = 1: 7 at risk, 1 event
    val expected = -(3.0 * StrictMath.log(4.0) + StrictMath.log(6.0) + StrictMath.log(7.0))
    val value = evaluate(zero, efron = false)._1
    assert(math.abs(value - expected) <= 1e-15 * math.abs(expected), s"$value vs $expected")
  }

  test("Efron at beta = 0 interpolates the tied events' share of the risk set") {
    // t = 3: denominators 4 - 0, 4 - 1, 4 - 2 for k = 3, 2, 1 of 3 tied events
    val expected = -(StrictMath.log(4.0) + StrictMath.log(3.0) + StrictMath.log(2.0) +
      StrictMath.log(6.0) + StrictMath.log(7.0))
    val value = evaluate(zero, efron = true)._1
    assert(math.abs(value - expected) <= 1e-15 * math.abs(expected), s"$value vs $expected")
  }

  test("score and information match finite differences, both methods, weights and offsets") {
    val beta = Array(0.3, -0.2)
    val h = 1e-5
    for (efron <- Seq(false, true)) {
      def at(b: Array[Double]) = evaluate(b, efron, weights, offsets)
      val (_, score, information) = at(beta)
      def shifted(j: Int, s: Double) =
        beta.indices.map(i => if (i == j) beta(i) + s else beta(i)).toArray
      for (j <- 0 until 2) {
        val numeric = (at(shifted(j, h))._1 - at(shifted(j, -h))._1) / (2 * h)
        assert(
          math.abs(numeric - score(j)) <= 1e-8,
          s"efron=$efron score $j: ${score(j)} vs $numeric"
        )
      }
      var k = 0
      for (j <- 0 until 2; l <- j until 2) {
        val numeric = -(at(shifted(l, h))._2(j) - at(shifted(l, -h))._2(j)) / (2 * h)
        assert(math.abs(numeric - information(k)) <= 1e-7, s"efron=$efron information $k")
        k += 1
      }
    }
  }

  test("Efron with untied events equals Breslow") {
    val untied = Array(false, true, false, false, true, false, true)
    assertEquals(
      evaluate(Array(0.4, 0.1), efron = true, events = untied)._1,
      evaluate(Array(0.4, 0.1), efron = false, events = untied)._1
    )
  }

  test("a zero weight is the same as an absent row, under both methods (X-013)") {
    for (efron <- Seq(false, true)) {
      val w = weights.updated(2, 0.0)
      val (v0, u0, i0) = evaluate(Array(0.4, 0.1), efron, w, offsets)
      val keep = time.indices.filter(_ != 2)
      val totals = new CoxTotals(2)
      CoxStratum.add(
        keep.map(time).toArray,
        keep.map(event).toArray,
        keep.map(weights).toArray,
        keep.map(offsets).toArray,
        keep.flatMap(r => Seq(x(2 * r), x(2 * r + 1))).toArray,
        2,
        0,
        keep.size,
        Array(0.4, 0.1),
        zero,
        efron,
        totals
      )
      assertEquals(v0, totals.value.value, s"efron=$efron")
      assertEquals(u0.toSeq, totals.score.values.toSeq)
      assertEquals(i0.toSeq, totals.information.values.toSeq)
    }
  }

  test("centering the covariates leaves the results unchanged") {
    val beta = Array(0.4, 0.1)
    val center = Array(0.3, 0.2)
    for (efron <- Seq(false, true)) {
      val (v0, u0, i0) = evaluate(beta, efron, weights, offsets)
      val (v1, u1, i1) = evaluate(beta, efron, weights, offsets, center = center)
      assert(math.abs(v0 - v1) <= 1e-13 * math.abs(v0))
      for (k <- u0.indices) assert(math.abs(u0(k) - u1(k)) <= 1e-13, s"score $k")
      for (k <- i0.indices) assert(math.abs(i0(k) - i1(k)) <= 1e-13, s"information $k")
    }
  }

  test("a stratum without events contributes nothing") {
    val (v, u, i) = evaluate(Array(0.4, 0.1), efron = true, events = Array.fill(7)(false))
    assertEquals(v, 0.0)
    assert(u.forall(_ == 0.0) && i.forall(_ == 0.0))
  }
}
