package pprof.spark.testkit

class TolerancesSuite extends munit.FunSuite {

  test("every tolerance class and rule parameter of §8.4 is defined") {
    Tolerances.Classes.foreach(cls => Tolerances(cls))
    Tolerances.Parameters.foreach(key => Tolerances.number(key))
  }

  test("the file defines nothing beyond the known classes and parameters") {
    val known =
      Tolerances.Classes.flatMap(cls => Seq(s"$cls.rtol", s"$cls.atol")) ++ Tolerances.Parameters
    assertEquals(Tolerances.entries.keySet.diff(known.toSet), Set.empty[String])
  }

  test("acceptance rule is atol + rtol * |expected|") {
    val t = Tolerance("example", rtol = 1e-8, atol = 1e-10)
    assert(t.accepts(1.0 + 5e-9, 1.0))
    assert(!t.accepts(1.0 + 2e-8, 1.0))
    assert(t.accepts(5e-11, 0.0))
    assert(!t.accepts(2e-10, 0.0))
  }

  test("NaN is never accepted; an infinity only matches the same infinity") {
    val t = Tolerance("example", rtol = 1.0, atol = 1.0)
    assert(!t.accepts(Double.NaN, Double.NaN))
    assert(!t.accepts(Double.NaN, 0.0))
    assert(t.accepts(Double.PositiveInfinity, Double.PositiveInfinity))
    assert(!t.accepts(Double.PositiveInfinity, Double.NegativeInfinity))
    assert(!t.accepts(1e308, Double.PositiveInfinity))
    assert(!t.accepts(Double.PositiveInfinity, 1e308))
  }

  test("vectors are scaled by their largest element") {
    val t = Tolerance("example", rtol = 1e-8, atol = 0.0)
    val expected = Array(100.0, 1e-12, -50.0)
    assert(
      t.acceptsAll(Array(100.0 + 5e-7, 5e-7, -50.0), expected),
      "near-zero element, large scale"
    )
    assert(!t.acceptsAll(Array(100.0 + 2e-6, 1e-12, -50.0), expected))
    assertEquals(t.worstRatio(expected, expected), 0.0)
    assertEquals(t.worstRatio(Array(Double.NaN, 0.0, 0.0), expected), Double.PositiveInfinity)
    intercept[IllegalArgumentException](t.worstRatio(Array(1.0), expected))
  }

  test("negative and non-finite tolerances are rejected") {
    intercept[IllegalArgumentException](Tolerance("bad", rtol = -1.0, atol = 0.0))
    intercept[IllegalArgumentException](Tolerance("bad", rtol = 0.0, atol = Double.NaN))
    intercept[IllegalArgumentException](
      Tolerance("bad", rtol = Double.PositiveInfinity, atol = 0.0)
    )
  }
}
