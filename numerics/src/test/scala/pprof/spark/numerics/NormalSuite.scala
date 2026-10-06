package pprof.spark.numerics

import munit.FunSuite

/** References computed with mpmath at 160 significant digits and rounded to the nearest double. */
class NormalSuite extends FunSuite {

  private val tails = Seq(
    (0.0, 0.5),
    (0.25, 0.4012936743170763),
    (0.5, 0.3085375387259869),
    (1.0, 0.15865525393145705),
    (1.4999, 0.0668201539998336),
    (1.5, 0.06680720126885807),
    (1.96, 0.024997895148220435),
    (2.5, 0.006209665325776135),
    (3.0, 0.0013498980316300946),
    (5.0, 2.866515718791939e-07),
    (8.0, 6.220960574271784e-16),
    (10.0, 7.619853024160525e-24),
    (15.0, 3.670966199312751e-51),
    (20.0, 2.7536241186062337e-89),
    (30.0, 4.906713927148187e-198),
    (37.0, 5.725571222524577e-300),
    (-0.5, 0.6914624612740131),
    (-1.0, 0.8413447460685429),
    (-3.0, 0.9986501019683699),
    (-8.0, 0.9999999999999993)
  )

  private val quantiles = Seq(
    (0.25, 0.6744897501960817),
    (0.05, 1.6448536269514726),
    (0.025, 1.9599639845400543),
    (0.005, 2.575829303548901),
    (0.001, 3.0902323061678136),
    (1e-06, 4.753424308822899),
    (1e-10, 6.361340902404057),
    (1e-15, 7.941345326170997),
    (1e-30, 11.464024688443615),
    (1e-100, 21.273453560965326),
    (0.75, -0.6744897501960817),
    (0.975, -1.9599639845400538)
  )

  private def relative(a: Double, b: Double): Double = math.abs(a - b) / math.abs(b)

  test("the upper tail keeps its relative accuracy from the centre to z = 37") {
    for ((z, expected) <- tails) {
      val error = relative(Normal.upperTail(z), expected)
      assert(error <= 2e-13, s"upper tail at $z: relative error $error")
    }
  }

  test("upper-tail quantiles are accurate to a few units in the last place") {
    for ((a, expected) <- quantiles) {
      val error = relative(Normal.upperQuantile(a), expected)
      assert(error <= 1e-14, s"upper quantile at $a: relative error $error")
    }
    assertEquals(Normal.upperQuantile(0.5), 0.0)
  }

  test("quantiles invert the upper tail deep into the tail") {
    for (a <- Seq(1e-50, 1e-150, 1e-300)) {
      val error = relative(Normal.upperTail(Normal.upperQuantile(a)), a)
      assert(error <= 1e-12, s"round trip at $a: relative error $error")
    }
  }

  test("two-sided p-values are symmetric and capped at 1") {
    assertEquals(Normal.twoSidedPValue(2.5), Normal.twoSidedPValue(-2.5))
    assertEquals(Normal.twoSidedPValue(0.0), 1.0)
    assert(Normal.twoSidedPValue(Double.NaN).isNaN)
    assertEquals(Normal.upperTail(40.0), 0.0)
    assertEquals(Normal.upperTail(Double.NegativeInfinity), 1.0)
    intercept[IllegalArgumentException](Normal.upperQuantile(0.0))
  }
}
