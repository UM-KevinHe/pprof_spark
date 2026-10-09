package pprof.spark.numerics

/** Student's t tails and quantiles against mpmath at 40 digits, from 1 to 10⁹ degrees of freedom
  * (docs/spec/linear/fixed-effect-estimation.md §4, X-031): every regime is exact, and tails are evaluated
  * directly, so far tails keep their relative accuracy. t = 7.0 and 7.4 straddle the switch between the
  * continued fraction and the asymptotic expansion at 30 degrees of freedom.
  */
class StudentTSuite extends munit.FunSuite {

  private def h(hex: String): Double = java.lang.Double.parseDouble(hex)

  private def relative(actual: Double, expected: Double): Double =
    if (expected == 0.0) math.abs(actual) else math.abs(actual - expected) / math.abs(expected)

  private val tails = Seq(
    (h("0x1.0000000000000p-1"), h("0x1.0000000000000p+0"), h("0x1.68dfd7131067cp-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.0000000000000p+0"), h("0x1.338d16470e780p-2")),
    (h("0x1.8000000000000p+1"), h("0x1.0000000000000p+0"), h("0x1.a37f5c4c419efp-3")),
    (h("0x1.c000000000000p+2"), h("0x1.0000000000000p+0"), h("0x1.72028ecef9843p-4")),
    (h("0x1.d99999999999ap+2"), h("0x1.0000000000000p+0"), h("0x1.5e41805f007c1p-4")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.0000000000000p+0"), h("0x1.c2c7f956fc3f8p-5")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.0000000000000p+0"), h("0x1.facb460a98550p-7")),
    (h("0x1.0000000000000p-1"), h("0x1.0000000000000p+1"), h("0x1.5555555555555p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.0000000000000p+1"), h("0x1.83307a8e79370p-3")),
    (h("0x1.8000000000000p+1"), h("0x1.0000000000000p+1"), h("0x1.8707522ff1e39p-4")),
    (h("0x1.c000000000000p+2"), h("0x1.0000000000000p+1"), h("0x1.4477bfece8970p-6")),
    (h("0x1.d99999999999ap+2"), h("0x1.0000000000000p+1"), h("0x1.233e17ca2620bp-6")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.0000000000000p+1"), h("0x1.e69be83202d67p-8")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.0000000000000p+1"), h("0x1.35490925bedf9p-11")),
    (h("0x1.0000000000000p-1"), h("0x1.4000000000000p+2"), h("0x1.46cf1c158b010p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.4000000000000p+2"), h("0x1.b77392743d9a5p-4")),
    (h("0x1.8000000000000p+1"), h("0x1.4000000000000p+2"), h("0x1.ed256556a4a28p-6")),
    (h("0x1.c000000000000p+2"), h("0x1.4000000000000p+2"), h("0x1.e0a3c4be86bdcp-11")),
    (h("0x1.d99999999999ap+2"), h("0x1.4000000000000p+2"), h("0x1.73c7c63cc8a52p-11")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.4000000000000p+2"), h("0x1.675e763f9686cp-14")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.4000000000000p+2"), h("0x1.571a68256d838p-23")),
    (h("0x1.0000000000000p-1"), h("0x1.e000000000000p+4"), h("0x1.3dcf67dbb6834p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.e000000000000p+4"), h("0x1.e621d9a50a88ap-5")),
    (h("0x1.8000000000000p+1"), h("0x1.e000000000000p+4"), h("0x1.613c97637aa78p-8")),
    (h("0x1.c000000000000p+2"), h("0x1.e000000000000p+4"), h("0x1.7cf63a17c202cp-24")),
    (h("0x1.d99999999999ap+2"), h("0x1.e000000000000p+4"), h("0x1.0442ee385521bp-25")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.e000000000000p+4"), h("0x1.9e15498a29182p-40")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.e000000000000p+4"), h("0x1.782387bf32abbp-91")),
    (h("0x1.0000000000000p-1"), h("0x1.9000000000000p+6"), h("0x1.3c813ee0fd9ffp-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.9000000000000p+6"), h("0x1.b05d60e92d303p-5")),
    (h("0x1.8000000000000p+1"), h("0x1.9000000000000p+6"), h("0x1.beaea9e4ef169p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.9000000000000p+6"), h("0x1.480bef1c5d551p-32")),
    (h("0x1.d99999999999ap+2"), h("0x1.9000000000000p+6"), h("0x1.7c7dbea9bdea4p-35")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.9000000000000p+6"), h("0x1.9745ec7c4f335p-65")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.9000000000000p+6"), h("0x1.1b90d54ce6722p-212")),
    (h("0x1.0000000000000p-1"), h("0x1.1e40000000000p+11"), h("0x1.3bf78fb34cfb4p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.1e40000000000p+11"), h("0x1.9a8ec05f0d70cp-5")),
    (h("0x1.8000000000000p+1"), h("0x1.1e40000000000p+11"), h("0x1.65aefadc73c64p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.1e40000000000p+11"), h("0x1.d763fd706570dp-39")),
    (h("0x1.d99999999999ap+2"), h("0x1.1e40000000000p+11"), h("0x1.ac3f05cb47955p-43")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.1e40000000000p+11"), h("0x1.bc69435ad7da1p-98")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.1e40000000000p+11"), h("0x1.1af58950f32ffp-920")),
    (h("0x1.0000000000000p-1"), h("0x1.57c0000000000p+13"), h("0x1.3bf29353beca2p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.57c0000000000p+13"), h("0x1.99c5a37b46f30p-5")),
    (h("0x1.8000000000000p+1"), h("0x1.57c0000000000p+13"), h("0x1.62a9072bcd0e9p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.57c0000000000p+13"), h("0x1.7d37583422e94p-39")),
    (h("0x1.d99999999999ap+2"), h("0x1.57c0000000000p+13"), h("0x1.490609f01242cp-43")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.57c0000000000p+13"), h("0x1.91f573c841c48p-100")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.57c0000000000p+13"), h("0x0.0p+0")),
    (h("0x1.0000000000000p-1"), h("0x1.86a0000000000p+16"), h("0x1.3bf168a45533ep-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.86a0000000000p+16"), h("0x1.99969638874a2p-5")),
    (h("0x1.8000000000000p+1"), h("0x1.86a0000000000p+16"), h("0x1.61f46e5cc44f0p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.86a0000000000p+16"), h("0x1.6a7dc6768dc05p-39")),
    (h("0x1.d99999999999ap+2"), h("0x1.86a0000000000p+16"), h("0x1.350d04acc6104p-43")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.86a0000000000p+16"), h("0x1.182dc92cb2497p-100")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.86a0000000000p+16"), h("0x0.0p+0")),
    (h("0x1.0000000000000p-1"), h("0x1.e848000000000p+19"), h("0x1.3bf1476abbdadp-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.e848000000000p+19"), h("0x1.99915a66a2865p-5")),
    (h("0x1.8000000000000p+1"), h("0x1.e848000000000p+19"), h("0x1.61e05aa361588p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.e848000000000p+19"), h("0x1.6875d2d955d45p-39")),
    (h("0x1.d99999999999ap+2"), h("0x1.e848000000000p+19"), h("0x1.32e606397c233p-43")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.e848000000000p+19"), h("0x1.0d113f039307bp-100")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.e848000000000p+19"), h("0x0.0p+0")),
    (h("0x1.0000000000000p-1"), h("0x1.dcd6500000000p+29"), h("0x1.3bf143ba9c612p-1")),
    (h("0x1.f5c28f5c28f5cp+0"), h("0x1.dcd6500000000p+29"), h("0x1.9990c5ae756bfp-5")),
    (h("0x1.8000000000000p+1"), h("0x1.dcd6500000000p+29"), h("0x1.61de202a8aff8p-9")),
    (h("0x1.c000000000000p+2"), h("0x1.dcd6500000000p+29"), h("0x1.683c4534227b1p-39")),
    (h("0x1.d99999999999ap+2"), h("0x1.dcd6500000000p+29"), h("0x1.32a9143c584edp-43")),
    (h("0x1.714af4f0d844dp+3"), h("0x1.dcd6500000000p+29"), h("0x1.0bdc44e25c0fbp-100")),
    (h("0x1.493b98c7e2824p+5"), h("0x1.dcd6500000000p+29"), h("0x0.0p+0"))
  )

  private val quantiles = Seq(
    (h("0x1.999999999999ap-6"), h("0x1.0000000000000p+0"), h("0x1.96993aacc4d24p+3")),
    (h("0x1.999999999999ap-5"), h("0x1.0000000000000p+0"), h("0x1.9414813ba662bp+2")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.0000000000000p+0"), h("0x1.36d978b737d36p+18")),
    (h("0x1.19799812dea11p-40"), h("0x1.0000000000000p+0"), h("0x1.2872fda39f29ap+38")),
    (h("0x1.999999999999ap-6"), h("0x1.4000000000000p+2"), h("0x1.4908d359dff3ap+1")),
    (h("0x1.999999999999ap-5"), h("0x1.4000000000000p+2"), h("0x1.01ed1ae7a9632p+1")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.4000000000000p+2"), h("0x1.8c5623429dc29p+4")),
    (h("0x1.19799812dea11p-40"), h("0x1.4000000000000p+2"), h("0x1.89f4fb4d8966fp+8")),
    (h("0x1.999999999999ap-6"), h("0x1.e000000000000p+4"), h("0x1.05692f10aaeeap+1")),
    (h("0x1.999999999999ap-5"), h("0x1.e000000000000p+4"), h("0x1.b27fb080b375ap+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.e000000000000p+4"), h("0x1.77c06205cbd3bp+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.e000000000000p+4"), h("0x1.6cb6018600a90p+3")),
    (h("0x1.999999999999ap-6"), h("0x1.1e40000000000p+11"), h("0x1.f604201d17262p+0")),
    (h("0x1.999999999999ap-5"), h("0x1.1e40000000000p+11"), h("0x1.a540c0b71df03p+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.1e40000000000p+11"), h("0x1.31012a6260909p+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.1e40000000000p+11"), h("0x1.c4b32d78efd7dp+2")),
    (h("0x1.999999999999ap-6"), h("0x1.57c0000000000p+13"), h("0x1.f5ce55b429f1fp+0")),
    (h("0x1.999999999999ap-5"), h("0x1.57c0000000000p+13"), h("0x1.a51e34d6eff13p+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.57c0000000000p+13"), h("0x1.3061e3003ba00p+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.57c0000000000p+13"), h("0x1.c2b959746ec8cp+2")),
    (h("0x1.999999999999ap-6"), h("0x1.86a0000000000p+16"), h("0x1.f5c1c1206c545p+0")),
    (h("0x1.999999999999ap-5"), h("0x1.86a0000000000p+16"), h("0x1.a516203c75c88p+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.86a0000000000p+16"), h("0x1.303cb2b4ef756p+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.86a0000000000p+16"), h("0x1.c24387d7321bdp+2")),
    (h("0x1.999999999999ap-6"), h("0x1.e848000000000p+19"), h("0x1.f5c05aebc6e01p+0")),
    (h("0x1.999999999999ap-5"), h("0x1.e848000000000p+19"), h("0x1.a5153a270220ep+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.e848000000000p+19"), h("0x1.30389032bd291p+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.e848000000000p+19"), h("0x1.c23670079dae4p+2")),
    (h("0x1.999999999999ap-6"), h("0x1.dcd6500000000p+29"), h("0x1.f5c03329204f5p+0")),
    (h("0x1.999999999999ap-5"), h("0x1.dcd6500000000p+29"), h("0x1.a515209d0212bp+0")),
    (h("0x1.0c6f7a0b5ed8dp-20"), h("0x1.dcd6500000000p+29"), h("0x1.30381ab5b55b2p+2")),
    (h("0x1.19799812dea11p-40"), h("0x1.dcd6500000000p+29"), h("0x1.c234fc04ce737p+2"))
  )

  test("two-sided tails match mpmath to 1e-12 relative at every degree of freedom") {
    val worst = tails.map { case (t, df, expected) =>
      val actual = StudentT.twoSided(t, df)
      assert(relative(actual, expected) <= 1e-12, s"t = $t, df = $df: $actual against $expected")
      assertEquals(StudentT.twoSided(-t, df), actual)
      relative(actual, expected)
    }.max
    println(s"StudentT two-sided tails: worst relative error $worst over ${tails.size} cases")
  }

  test(
    "upper tails, the distribution function and quantiles are consistent; quantiles match mpmath"
  ) {
    tails.foreach { case (t, df, _) =>
      val upper = StudentT.upperTail(t, df)
      assertEquals(upper, 0.5 * StudentT.twoSided(t, df))
      assertEquals(StudentT.cdf(-t, df), upper)
      assert(math.abs(StudentT.upperTail(-t, df) + upper - 1.0) <= 2e-16, s"t = $t, df = $df")
    }
    val worst = quantiles.map { case (upper, df, expected) =>
      val actual = StudentT.upperQuantile(upper, df)
      assert(
        relative(actual, expected) <= 1e-12,
        s"upper $upper, df $df: $actual against $expected"
      )
      // Symmetry against the exact complement (1 - (1 - upper) need not equal upper in floating point).
      assertEquals(
        StudentT.upperQuantile(1.0 - upper, df),
        -StudentT.upperQuantile(1.0 - (1.0 - upper), df)
      )
      relative(actual, expected)
    }.max
    println(s"StudentT quantiles: worst relative error $worst over ${quantiles.size} cases")
  }

  test("edge cases: t = 0 and infinite t, NaN, and invalid arguments") {
    assertEquals(StudentT.twoSided(0.0, 7.0), 1.0)
    assertEquals(StudentT.upperTail(0.0, 7.0), 0.5)
    assertEquals(StudentT.twoSided(Double.PositiveInfinity, 7.0), 0.0)
    assertEquals(StudentT.upperTail(Double.NegativeInfinity, 7.0), 1.0)
    assert(StudentT.twoSided(Double.NaN, 7.0).isNaN)
    assertEquals(StudentT.upperQuantile(0.5, 3.0), 0.0)
    intercept[IllegalArgumentException](StudentT.twoSided(1.0, 0.0))
    intercept[IllegalArgumentException](StudentT.upperQuantile(0.0, 3.0))
  }
}
