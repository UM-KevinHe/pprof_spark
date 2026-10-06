package pprof.spark.numerics

import munit.FunSuite

class NewtonSuite extends FunSuite {

  private val m = Array(1.5, -0.5)

  /** −½(β − m)ᵀA(β − m) − 1 with A = [[2, 0.5], [0.5, 1]]. */
  private def quadratic(b: Array[Double]): Evaluation = {
    val d0 = b(0) - m(0)
    val d1 = b(1) - m(1)
    val value = -0.5 * (2.0 * d0 * d0 + d0 * d1 + d1 * d1) - 1.0
    Evaluation(value, Array(-(2.0 * d0 + 0.5 * d1), -(0.5 * d0 + d1)), Array(2.0, 0.5, 1.0))
  }

  private def scalar(value: Double, gradient: Double) =
    Evaluation(value, Array(gradient), Array(1.0))

  test("a concave quadratic is maximized in one step and declared converged on the next") {
    val r = Newton.maximize(quadratic, Array(0.0, 0.0), NewtonOptions())
    assert(r.converged)
    assertEquals(r.iterations, 2)
    assertEquals(r.halvings, 0)
    assert(
      math.abs(r.beta(0) - 1.5) <= 1e-14 && math.abs(r.beta(1) + 0.5) <= 1e-14,
      r.beta.toString
    )
    assertEquals(r.log.map(_.iteration), Vector(1, 2))
    assertEquals(r.initial.value, quadratic(Array(0.0, 0.0)).value)
  }

  test("convergence is tested on the full step before any halving (X-010)") {
    val e = (b: Array[Double]) => if (b(0) == 0.0) scalar(-1.0, 1.0) else scalar(-1.0 - 1e-12, 0.0)
    val r = Newton.maximize(e, Array(0.0), NewtonOptions())
    assert(r.converged)
    assertEquals((r.iterations, r.halvings, r.beta), (1, 0, Vector(1.0)))
  }

  test("a step that lowers the objective by more than eps is halved") {
    val e = (b: Array[Double]) =>
      if (b(0) == 0.0) scalar(-1.0, 1.0)
      else if (b(0) == 1.0) scalar(-2.0, 0.0)
      else scalar(-0.75, 0.0)
    val r = Newton.maximize(e, Array(0.0), NewtonOptions())
    assert(r.converged)
    assertEquals((r.iterations, r.halvings, r.beta), (2, 1, Vector(0.5)))
    assertEquals(r.log.map(_.halvings), Vector(1, 0))
  }

  test("a non-finite value is halved like a decrease, and an endless one fails") {
    val e = (b: Array[Double]) =>
      if (b(0) == 0.0) scalar(-1.0, 1.0)
      else if (b(0) == 1.0) scalar(Double.NegativeInfinity, 0.0)
      else scalar(-0.5, 0.0)
    val r = Newton.maximize(e, Array(0.0), NewtonOptions())
    assertEquals((r.converged, r.halvings, r.beta), (true, 1, Vector(0.5)))
    val never =
      (b: Array[Double]) => if (b(0) == 0.0) scalar(-1.0, 1.0) else scalar(Double.NaN, 0.0)
    intercept[ArithmeticException](
      Newton.maximize(never, Array(0.0), NewtonOptions(maxHalvings = 3))
    )
  }

  test("a singular information matrix fails and names the aliased columns (X-011)") {
    val e = (_: Array[Double]) => Evaluation(-1.0, Array(1.0, 1.0), Array(1.0, 1.0, 1.0))
    val error = intercept[AliasedException](Newton.maximize(e, Array(0.0, 0.0), NewtonOptions()))
    assertEquals(error.columns, Vector(1))
  }

  test("stopping at the iteration cap is reported, not silent") {
    val r = Newton.maximize(quadratic, Array(0.0, 0.0), NewtonOptions(maxIterations = 1))
    assert(!r.converged)
    assertEquals(r.iterations, 1)
    assert(r.message.contains("1 iterations"), r.message)
  }

  test("the relative change uses the current value, with zero counting as one") {
    assertEquals(Newton.relativeChange(-2.0, -4.0), 0.5)
    assertEquals(Newton.relativeChange(0.25, 0.0), 0.25)
  }
}
