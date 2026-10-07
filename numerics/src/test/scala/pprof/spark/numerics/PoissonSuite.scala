package pprof.spark.numerics

import munit.FunSuite

/** References: mpmath at 50 digits for the special functions; scipy for χ² quantiles; pprof_py
  * v0.7.0's `poisson_midp_zscore`, `poisson_exact_test` and `_midp_limits` for the tests.
  */
class PoissonSuite extends FunSuite {

  private def close(a: Double, b: Double, rel: Double, abs: Double = 0.0): Boolean =
    (a == b) || math.abs(a - b) <= math.max(abs, rel * math.abs(b))

  test("log-gamma") {
    for (
      (x, expected) <- Seq(
        (1.0, 0.0),
        (2.0, 0.0),
        (3.5, 1.2009736023470743),
        (10.0, 12.801827480081469),
        (100.5, 361.4355404677776),
        (10000.0, 82099.71749644238)
      )
    )
      assert(
        close(Gamma.logGamma(x), expected, 1e-14, 1e-14),
        s"logGamma($x) = ${Gamma.logGamma(x)} vs $expected"
      )
  }

  test("regularized incomplete gamma, both tails") {
    for (
      (a, x, p, q) <- Seq(
        (1.0, 0.5, 0.3934693402873666, 0.6065306597126334),
        (5.0, 2.0, 0.05265301734371116, 0.9473469826562888),
        (5.0, 10.0, 0.970747311923039, 0.029252688076961072),
        (20.0, 15.0, 0.12478121503252482, 0.8752187849674752),
        (100.0, 90.0, 0.15822098918643016, 0.8417790108135699),
        (100.0, 150.0, 0.9999940754596646, 5.924540335483916e-06),
        (1000.0, 950.0, 0.05505468623073803, 0.9449453137692619),
        (3.0, 40.0, 0.9999999999999964, 3.572865928700226e-15)
      )
    ) {
      val (gp, gq) = Gamma.regularized(a, x)
      assert(close(gp, p, 1e-12, 1e-300), s"P($a, $x) = $gp vs $p")
      assert(close(gq, q, 1e-12, 1e-300), s"Q($a, $x) = $gq vs $q")
    }
  }

  test("Poisson CDF and PMF") {
    for (
      (k, mu, cdf, pmf) <- Seq(
        (0, 3.0, 0.049787068367863944, 0.049787068367863944),
        (3, 3.0, 0.6472318887822313, 0.22404180765538775),
        (18, 17.256833, 0.6314418892508666, 0.09213218402094052),
        (40, 19.5, 0.9999854622612286, 1.663401118717431e-05),
        (5, 30.0, 2.2573487463962842e-08, 1.8949186511901354e-08),
        (150, 120.0, 0.9964480348947647, 0.0010114761989159756)
      )
    ) {
      assert(close(Poisson.cdf(k.toDouble, mu), cdf, 1e-12), s"cdf($k, $mu)")
      assert(close(Poisson.pmf(k.toDouble, mu), pmf, 1e-12), s"pmf($k, $mu)")
      assert(
        close(Poisson.atLeast(k.toDouble + 1, mu), 1.0 - cdf, 1e-9, 1e-15),
        s"atLeast(${k + 1}, $mu)"
      )
    }
  }

  test("chi-square quantiles") {
    for (
      (p, df, expected) <- Seq(
        (0.025, 2.0, 0.05063561596857975),
        (0.975, 2.0, 7.377758908227871),
        (0.025, 36.0, 21.335881560799056),
        (0.975, 38.0, 56.895520535055965),
        (0.005, 400.0, 330.9027503436506),
        (0.995, 402.0, 478.7883569022797)
      )
    ) assert(close(Poisson.chiSquareQuantile(p, df), expected, 1e-12), s"chi2($p, $df)")
  }

  test("mid-p z, exact p-values and limits, and mid-p limits match pprof_py") {
    for (
      (o, e, z) <- Seq(
        (0.0, 19.509996, -4.753424308822899),
        (18.0, 17.256833, 0.21566561524685843),
        (18.0, 16.13747, 0.49202201776687954),
        (40.0, 19.5, 4.07654664876376),
        (3.0, 12.0, -2.9873526343405876),
        (150.0, 120.0, 2.6472303011867426),
        (5.0, 0.8, 3.1567002786007095)
      )
    )
      assert(
        close(PoissonTests.midpZ(o, e), z, 1e-12, 1e-12),
        s"midpZ($o, $e) = ${PoissonTests.midpZ(o, e)} vs $z"
      )
    for (
      (o, e, p, lo, hi) <- Seq(
        (0.0, 19.509996, 6.7289358951422186e-09, 0.0, 0.18907638187695863),
        (18.0, 17.256833, 0.9213805895401475, 0.6181864760700604, 1.6484925285843575),
        (18.0, 16.13747, 0.7070922012342222, 0.6610664980569771, 1.762838925031494),
        (40.0, 19.5, 6.234349991695914e-05, 1.4654659713737932, 2.7932639468709777),
        (3.0, 12.0, 0.004583582415582846, 0.05155601024130011, 0.7306060891451936),
        (150.0, 120.0, 0.009126882608302472, 1.057955612634368, 1.466817985682498),
        (5.0, 0.8, 0.00282262029177347, 2.0293579876480257, 14.585415099153337)
      )
    ) {
      assert(close(PoissonTests.exactP(o, e), p, 1e-12, 1e-300), s"exactP($o, $e)")
      val (l, h) = PoissonTests.exactLimits(o, e, 0.05)
      assert(
        close(l, lo, 1e-12) && close(h, hi, 1e-12),
        s"exact limits ($o, $e): ($l, $h) vs ($lo, $hi)"
      )
    }
    for (
      (o, e, lo, hi) <- Seq(
        (0.0, 19.509996, 0.0, 2.9957322735617655),
        (18.0, 17.256833, 11.003212744930497, 27.896686481629494),
        (18.0, 16.13747, 11.003212744930496, 27.896686481755808),
        (40.0, 19.5, 28.963911429152514, 53.932910432033815),
        (3.0, 12.0, 0.7630962977974831, 8.164469208176186),
        (150.0, 120.0, 127.39701764324415, 175.4979589298425),
        (5.0, 0.8, 1.8319948085284168, 11.082415664992144)
      )
    ) {
      val (l, h) = PoissonTests.midpLimits(o, e, 0.05)
      assert(
        close(l, lo, 1e-9, 1e-12) && close(h, hi, 1e-9),
        s"mid-p limits ($o, $e): ($l, $h) vs ($lo, $hi)"
      )
    }
  }

  test("Brent finds a bracketed root to its tolerance") {
    val root = Brent.root(x => x * x * x - 2.0, 0.0, 2.0, 1e-15)
    assert(math.abs(root - StrictMath.cbrt(2.0)) <= 1e-14, root.toString)
    intercept[IllegalArgumentException](Brent.root(x => x * x + 1.0, -1.0, 1.0, 1e-12))
  }
}
