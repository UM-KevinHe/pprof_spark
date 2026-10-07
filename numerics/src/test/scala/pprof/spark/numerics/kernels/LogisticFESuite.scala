package pprof.spark.numerics.kernels

import pprof.spark.numerics.{AliasedException, InMemorySerbinPasses, Serbin, SerbinOptions}

/** The logistic fixed-effect kernels and the SerBIN iteration on in-memory data (Phase 2a
  * specification §2, §5 and §6).
  */
class LogisticFESuite extends munit.FunSuite {

  /** Providers of the given sizes; covariates on a 1/64 grid; outcomes from a fixed generator. */
  private def data(sizes: Seq[Int], p: Int, trials: Int = 1, seed: Long = 7L): LogisticFE.Rows = {
    val random = new scala.util.Random(seed)
    val starts = sizes.scanLeft(0)(_ + _).init.toArray
    val total = sizes.sum
    val x = Array.fill(total * p)(math.rint(random.nextGaussian() * 64.0) / 64.0)
    val n = Array.fill(total)((1 + random.nextInt(trials)).toDouble)
    val y = n.map(t => (0 until t.toInt).count(_ => random.nextDouble() < 0.4).toDouble)
    LogisticFE.Rows(starts, y, n, x, p)
  }

  private def close(actual: Double, expected: Double, relative: Double, what: String): Unit =
    assert(
      math.abs(actual - expected) <= relative * math.max(1.0, math.abs(expected)),
      s"$what: $actual against $expected"
    )

  private val gamma = Array(-0.75, 0.25, -0.4)
  private val beta = Array(0.3, -0.2)

  test("scores and information are the derivatives of the log-likelihood") {
    for (trials <- Seq(1, 6)) {
      val rows = data(Seq(5, 9, 7), 2, trials)
      val t = LogisticFE.terms(rows, gamma, beta)
      val h = 1e-6
      def ll(g: Array[Double], b: Array[Double]) = LogisticFE.loglik(rows, g, b)
      def bump(v: Array[Double], i: Int, d: Double) = v.updated(i, v(i) + d)
      close(t.loglik, ll(gamma, beta), 1e-14, "loglik")
      gamma.indices.foreach { k =>
        val numeric = (ll(bump(gamma, k, h), beta) - ll(bump(gamma, k, -h), beta)) / (2 * h)
        close(t.scoreGamma(k), numeric, 1e-7, s"score for provider $k")
        val curvature = (LogisticFE.terms(rows, bump(gamma, k, h), beta).scoreGamma(k) -
          LogisticFE.terms(rows, bump(gamma, k, -h), beta).scoreGamma(k)) / (2 * h)
        close(t.infoGamma(k), -curvature, 1e-6, s"information for provider $k")
        beta.indices.foreach { j =>
          val cross = (LogisticFE.terms(rows, gamma, bump(beta, j, h)).scoreGamma(k) -
            LogisticFE.terms(rows, gamma, bump(beta, j, -h)).scoreGamma(k)) / (2 * h)
          close(t.crossInfo(k * 2 + j), -cross, 1e-6, s"cross information ($k, $j)")
        }
      }
      beta.indices.foreach { j =>
        val numeric = (ll(gamma, bump(beta, j, h)) - ll(gamma, bump(beta, j, -h))) / (2 * h)
        close(t.scoreBeta(j), numeric, 1e-7, s"score for beta $j")
      }
      val c01 = (LogisticFE.terms(rows, gamma, bump(beta, 1, h)).scoreBeta(0) -
        LogisticFE.terms(rows, gamma, bump(beta, 1, -h)).scoreBeta(0)) / (2 * h)
      close(t.infoBeta(1), -c01, 1e-6, "information C(0, 1)")
    }
  }

  test("the centred Schur system equals C − Σ bbᵀ/h and g_β − Σ bg/h") {
    val rows = data(Seq(5, 9, 7), 2, trials = 4)
    val t = LogisticFE.terms(rows, gamma, beta)
    val s = LogisticFE.schur(rows, gamma, beta)
    val expected = Array.tabulate(3) { index =>
      val (i, j) = Seq((0, 0), (0, 1), (1, 1))(index)
      t.infoBeta(index) - gamma.indices
        .map(k => t.crossInfo(k * 2 + i) * t.crossInfo(k * 2 + j) / t.infoGamma(k))
        .sum
    }
    expected.indices.foreach(i => close(s.packed(i), expected(i), 1e-12, s"S($i)"))
    beta.indices.foreach { j =>
      val rhs = t.scoreBeta(j) - gamma.indices
        .map(k => t.crossInfo(k * 2 + j) * t.scoreGamma(k) / t.infoGamma(k))
        .sum
      close(s.rhs(j), rhs, 1e-12, s"r($j)")
    }
    close(
      s.scoreTerm,
      gamma.indices.map(k => t.scoreGamma(k) * t.scoreGamma(k) / t.infoGamma(k)).sum,
      1e-12,
      "Σ g²/h"
    )
    close(s.loglik, t.loglik, 1e-14, "loglik")
  }

  test(
    "the block-eliminated step solves the joint Newton system, and λ is its directional derivative"
  ) {
    val rows = data(Seq(6, 4, 8), 2)
    val t = LogisticFE.terms(rows, gamma, beta)
    val s = LogisticFE.schur(rows, gamma, beta)
    val factor = pprof.spark.numerics.Cholesky.factor(s.packed, 2)
    val deltaBeta = factor.solve(s.rhs)
    val trial = LogisticFE.trial(rows, gamma, beta, deltaBeta, Array.emptyDoubleArray)
    // The joint system: hₖΔγₖ + bₖᵀΔβ = gₖ and Σ bₖΔγₖ + CΔβ = g_β.
    gamma.indices.foreach { k =>
      val lhs = t.infoGamma(k) * trial
        .deltaGamma(k) + (0 until 2).map(j => t.crossInfo(k * 2 + j) * deltaBeta(j)).sum
      close(lhs, t.scoreGamma(k), 1e-12, s"provider equation $k")
    }
    val c = Array(Array(t.infoBeta(0), t.infoBeta(1)), Array(t.infoBeta(1), t.infoBeta(2)))
    (0 until 2).foreach { j =>
      val lhs = gamma.indices.map(k => t.crossInfo(k * 2 + j) * trial.deltaGamma(k)).sum +
        (0 until 2).map(i => c(j)(i) * deltaBeta(i)).sum
      close(lhs, t.scoreBeta(j), 1e-12, s"covariate equation $j")
    }
    val lambda = gamma.indices.map(k => t.scoreGamma(k) * trial.deltaGamma(k)).sum +
      (0 until 2).map(j => t.scoreBeta(j) * deltaBeta(j)).sum
    close(s.scoreTerm + s.rhs(0) * deltaBeta(0) + s.rhs(1) * deltaBeta(1), lambda, 1e-12, "lambda")
  }

  test("shifting a covariate leaves S, r and Δβ unchanged and moves Δγ by −cΔβ") {
    val rows = data(Seq(6, 4, 8), 2)
    val shifted = rows.copy(x = rows.x.zipWithIndex.map { case (v, i) =>
      if (i % 2 == 0) v + 50.0 else v
    })
    val gammaShifted = gamma.map(_ - 50.0 * beta(0))
    val a = LogisticFE.schur(rows, gamma, beta)
    val b = LogisticFE.schur(shifted, gammaShifted, beta)
    a.packed.indices.foreach(i => close(b.packed(i), a.packed(i), 1e-11, s"S($i)"))
    a.rhs.indices.foreach(i => close(b.rhs(i), a.rhs(i), 1e-11, s"r($i)"))
    val deltaBeta = Array(0.01, -0.02)
    val ta = LogisticFE.trial(rows, gamma, beta, deltaBeta, Array.emptyDoubleArray)
    val tb = LogisticFE.trial(shifted, gammaShifted, beta, deltaBeta, Array.emptyDoubleArray)
    gamma.indices.foreach(k =>
      close(tb.deltaGamma(k), ta.deltaGamma(k) - 50.0 * deltaBeta(0), 1e-11, s"Δγ($k)")
    )
  }

  test("median and bound follow numpy") {
    assertEquals(Serbin.median(Array(3.0, -1.0, 2.0)), 2.0)
    assertEquals(Serbin.median(Array(4.0, -1.0, 2.0, 0.5)), 1.25)
    assertEquals(
      Serbin.bounded(Array(-30.0, 0.0, 1.0, 25.0), 10.0).toSeq,
      Seq(-9.5, 0.0, 1.0, 10.5)
    )
    assert(
      Serbin.belowNoise(1e-13, 100.0) && !Serbin.belowNoise(0.0, 1.0) && !Serbin.belowNoise(
        1e-9,
        1.0
      )
    )
  }

  test("SerBIN reaches the maximum: zero scores, and the default and tight fits agree") {
    val rows = data(Seq(12, 15, 20, 11), 2, trials = 3, seed = 11L)
    val passes = new InMemorySerbinPasses(Seq(rows))
    val start = Array.fill(4)(-0.4)
    val fit = Serbin.fit(start, Array(0.0, 0.0), passes, SerbinOptions(tol = 1e-13))
    assert(fit.converged, fit.toString)
    val t = LogisticFE.terms(rows, fit.gamma, fit.beta)
    (t.scoreGamma ++ t.scoreBeta).foreach(score => assert(math.abs(score) < 1e-9, s"score $score"))
    val default = Serbin.fit(start, Array(0.0, 0.0), passes, SerbinOptions())
    fit.beta.indices.foreach(j => close(default.beta(j), fit.beta(j), 1e-7, s"beta $j"))
    assertEquals(fit.trace.size, fit.iterations)
  }

  test(
    "the iteration cap allows maxIter + 1 steps with backtracking and maxIter without, as pprof_py"
  ) {
    val rows = data(Seq(12, 15, 20), 2, seed = 3L)
    val passes = new InMemorySerbinPasses(Seq(rows))
    val start = Array.fill(3)(-0.4)
    assertEquals(
      Serbin.fit(start, Array(0.0, 0.0), passes, SerbinOptions(maxIter = 0)).iterations,
      1
    )
    assertEquals(
      Serbin.fit(start, Array(0.0, 0.0), passes, SerbinOptions(maxIter = 1)).iterations,
      2
    )
    val noSearch =
      Serbin.fit(start, Array(0.0, 0.0), passes, SerbinOptions(maxIter = 1, backtrack = false))
    assertEquals(noSearch.iterations, 1)
    assert(!noSearch.converged)
  }

  test("a provider without events stops at the bound below the median") {
    val base = data(Seq(12, 15, 20, 14, 18), 2, seed = 5L)
    val rows = base.copy(y = base.y.zipWithIndex.map { case (v, r) => if (r < 12) 0.0 else v })
    val passes = new InMemorySerbinPasses(Seq(rows))
    val fit = Serbin.fit(Array.fill(5)(-0.4), Array(0.0, 0.0), passes, SerbinOptions(bound = 3.0))
    assert(fit.converged, fit.toString)
    assertEquals(fit.gamma(0), Serbin.median(fit.gamma) - 3.0)
    assert(fit.gamma.drop(1).forall(g => math.abs(g - Serbin.median(fit.gamma)) < 3.0))
  }

  test("a covariate constant within every provider is aliased with the provider effects (X-019)") {
    val base = data(Seq(12, 15, 20), 2, seed = 9L)
    val providerLevel = base.copy(x = base.x.zipWithIndex.map { case (v, i) =>
      if (i % 2 == 1) base.starts.lastIndexWhere(_ <= i / 2).toDouble else v
    })
    val failure = intercept[AliasedException] {
      Serbin.fit(
        Array.fill(3)(-0.4),
        Array(0.0, 0.0),
        new InMemorySerbinPasses(Seq(providerLevel)),
        SerbinOptions()
      )
    }
    assertEquals(failure.columns, Vector(1))
  }

  test("blocks in order give what one block gives, within rounding") {
    val rows = data(Seq(12, 15, 20, 11), 2, seed = 13L)
    val split = Seq(
      LogisticFE.Rows(Array(0, 12), rows.y.take(27), rows.n.take(27), rows.x.take(54), 2),
      LogisticFE.Rows(Array(0, 20), rows.y.drop(27), rows.n.drop(27), rows.x.drop(54), 2)
    )
    val one = Serbin.fit(
      Array.fill(4)(-0.4),
      Array(0.0, 0.0),
      new InMemorySerbinPasses(Seq(rows)),
      SerbinOptions()
    )
    val two = Serbin.fit(
      Array.fill(4)(-0.4),
      Array(0.0, 0.0),
      new InMemorySerbinPasses(split),
      SerbinOptions()
    )
    assertEquals(two.iterations, one.iterations)
    one.beta.indices.foreach(j => close(two.beta(j), one.beta(j), 1e-12, s"beta $j"))
    one.gamma.indices.foreach(k => close(two.gamma(k), one.gamma(k), 1e-12, s"gamma $k"))
  }
}
