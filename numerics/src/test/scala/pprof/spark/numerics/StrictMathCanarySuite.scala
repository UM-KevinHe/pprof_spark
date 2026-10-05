package pprof.spark.numerics

/** Platform canary for §8.1 and reproducibility level R2.
  *
  * Deterministic kernels use `StrictMath`, which is specified to reproduce the fdlibm algorithms
  * bit for bit. The golden bit patterns below were computed on OpenJDK 17.0.20 and 21.0.12
  * (x86-64) and agree on both. Any JVM or architecture that disagrees, such as a Databricks node
  * or an ARM runner, fails here loudly instead of silently changing fitted results.
  */
class StrictMathCanarySuite extends munit.FunSuite {

  private def bits(x: Double): Long = java.lang.Double.doubleToRawLongBits(x)

  private val exp = StrictMath.exp _
  private val log = StrictMath.log _
  private val log1p = StrictMath.log1p _
  private val expm1 = StrictMath.expm1 _

  private val cases: Seq[(String, Double, Double => Double, Long)] = Seq(
    ("exp", 1.0, exp, 0x4005bf0a8b14576aL),
    ("exp", -0.5, exp, 0x3fe368b2fc6f960aL),
    ("exp", 1e-4, exp, 0x3ff00068dce3484eL),
    ("exp", 20.5, exp, 0x41c7d6c4f0bcdd5cL),
    ("exp", -700.25, exp, 0x00caf5fe9a485c8eL),
    ("log", 2.0, log, 0x3fe62e42fefa39efL),
    ("log", 0.1, log, 0xc0026bb1bbb55515L),
    ("log", 1e-300, log, 0xc085963447f87fb5L),
    ("log", 12345.678, log, 0x4022d79559791e31L),
    ("log1p", 1e-10, log1p, 0x3ddb7cdfd9d1d693L),
    ("log1p", -0.5, log1p, 0xbfe62e42fefa39efL),
    ("log1p", 3.0, log1p, 0x3ff62e42fefa39efL),
    ("expm1", 1e-10, expm1, 0x3ddb7cdfd9dda4e3L),
    ("expm1", -1.5, expm1, 0xbfe8dc1e236d28f9L),
    ("expm1", 0.75, expm1, 0x3ff1df3b68cfb9f0L),
    ("pow(2.5, x)", 0.3, (x: Double) => StrictMath.pow(2.5, x), 0x3ff50fe6c94a6e58L),
    ("pow(10, x)", -2.5, (x: Double) => StrictMath.pow(10.0, x), 0x3f69e7c6e43390b7L)
  )

  cases.foreach { case (function, x, f, expected) =>
    test(s"StrictMath $function at x = $x keeps its fdlibm bits") {
      val actual = bits(f(x))
      assertEquals(actual, expected, f"got 0x$actual%016x")
    }
  }

  test("fdlibm exp(1) is one ulp above the correctly rounded e, so implementations differ") {
    assertEquals(bits(StrictMath.exp(1.0)) - bits(Math.E), 1L)
  }
}
