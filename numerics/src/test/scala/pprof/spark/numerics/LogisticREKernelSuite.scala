package pprof.spark.numerics

import pprof.spark.numerics.kernels.LogisticRE

/** The logistic random-intercept kernels (docs/spec/logistic/random-effect-estimation.md §2, §3): the modes solve their
  * equations, the analytic gradient agrees with central differences, the nAGQ = 0 Newton reaches the joint optimum, and
  * the fits are optimal, invariant to shifting a feature, bounded and refused where the specification says.
  */
class LogisticREKernelSuite extends munit.FunSuite {

  private def close(actual: Double, expected: Double, tolerance: Double): Unit =
    assert(math.abs(actual - expected) <= tolerance, s"$actual vs $expected (tolerance $tolerance)")

  /** m providers of 10 to 50 rows; x₁ on a 1/64 grid, x₂ binary, provider effects of SD 0.7, from a fixed LCG. */
  private def data(
      m: Int = 12,
      shift: Double = 0.0,
      seed: Long = 7L
  ): (Seq[LogisticRE.Rows], Array[Double], Double, Long) = {
    var state = seed
    def next(): Double = {
      state = state * 6364136223846793005L + 1442695040888963407L
      ((state >>> 11).toDouble + 0.5) / (1L << 53).toDouble
    }
    def normal(): Double = math.sqrt(-2.0 * math.log(next())) * math.cos(2.0 * math.Pi * next())
    val providers = (0 until m).map { j =>
      val size = 10 + (j * 7) % 41
      val a = 0.7 * normal()
      (0 until size).map { _ =>
        val x1 = math.rint(normal() * 64.0) / 64.0
        val x2 = if (next() < 0.3) 1.0 else 0.0
        val eta = -0.5 + 0.8 * x1 - 0.6 * x2 + a
        val y = if (next() < 1.0 / (1.0 + math.exp(-eta))) 1.0 else 0.0
        Array(1.0, x1 + shift, x2, 0.0, y)
      }
    }
    val rows = providers.flatten
    val centre = Array(0.0, rows.map(_(1)).sum / rows.length, rows.map(_(2)).sum / rows.length)
    // two blocks: the first half of the providers, then the rest
    val halves = Seq(providers.take(m / 2), providers.drop(m / 2)).map { group =>
      LogisticRE.Rows(
        group.scanLeft(0)(_ + _.length).dropRight(1).toArray,
        group.flatten.flatten.toArray,
        3
      )
    }
    (halves, centre, rows.map(_(4)).sum / rows.length, rows.length.toLong)
  }

  private def local(rows: Seq[LogisticRE.Rows], centre: Array[Double]) =
    new LogisticRE.LocalBlocks(rows, 3, centre)

  test(
    "the Laplace deviance equals the nAGQ = 0 sums at the modes, and every mode solves its equation"
  ) {
    val (rows, centre, _, _) = data()
    val blocks = local(rows, centre)
    val beta = Array(-0.4, 0.7, -0.5)
    val (u, _) = blocks.modes(0.8, beta)
    val s = blocks.stage1(0.8, beta, u)
    close(blocks.laplace(0.8, beta).deviance, s.penalized + s.logH, 1e-10)
    u.indices.foreach(g => assert(math.abs(0.8 * s.residuals(g) - u(g)) < 1e-11, s"provider $g"))
    assertEquals(blocks.modes(0.0, beta)._1.toSeq, Seq.fill(u.length)(0.0))
  }

  test("the analytic gradient agrees with central differences of the Laplace deviance") {
    val (rows, centre, _, _) = data()
    val blocks = local(rows, centre)
    for (
      point <- Seq(
        Array(0.9, -0.3, 0.6, -0.4),
        Array(0.2, -0.6, 1.0, 0.1),
        Array(1.7, 0.2, 0.3, -0.9)
      )
    ) {
      val g = blocks.laplace(point(0), point.drop(1)).gradient
      point.indices.foreach { k =>
        val h = 1e-5 * math.max(1.0, math.abs(point(k)))
        def at(t: Double) = {
          val x = point.clone(); x(k) += t; blocks.laplace(x(0), x.drop(1)).deviance
        }
        val cd = (8.0 * (at(h) - at(-h)) - (at(2 * h) - at(-2 * h))) / (12.0 * h)
        assert(
          math.abs(g(k) - cd) <= 1e-6 * math.max(1.0, math.abs(cd)),
          s"coordinate $k at ${point.toSeq}: ${g(k)} vs $cd"
        )
      }
    }
  }

  test("the nAGQ = 0 Newton reaches the joint optimum of the penalized deviance") {
    val (rows, centre, _, _) = data()
    val blocks = local(rows, centre)
    val (beta, u, value, iterations) =
      LogisticRE.stage1Solve(blocks, 3, 0.6, new Array[Double](3), new Array[Double](12))
    val s = blocks.stage1(0.6, beta, u)
    s.rx.foreach(r => assert(math.abs(r) < 1e-8, s"score in beta $r"))
    u.indices.foreach(g =>
      assert(math.abs(0.6 * s.residuals(g) - u(g)) < 1e-8, s"score of provider $g")
    )
    close(value, s.penalized + s.logH, 1e-9)
    assert(iterations < 30, s"$iterations iterations")
  }

  test("nAGQ = 1 and 0 fits are optimal, and shifting a feature moves only the intercept") {
    val (rows, centre, ybar, n) = data()
    for (nagq1 <- Seq(true, false)) {
      val fit = LogisticRE.fit(local(rows, centre), n, 12, 3, intercept = true, centre, ybar, nagq1)
      assert(
        fit.converged && fit.sigma > 0.0 && fit.boundary.isEmpty,
        s"nagq1 $nagq1: ${fit.warnings}"
      )
      assertEquals(fit.blups.length, 12)
      close(fit.logLikelihood, -0.5 * fit.deviance, 1e-12)
      close(fit.aic, fit.deviance + 8.0, 1e-9)
      if (nagq1) {
        assert(fit.projectedGradient <= 1e-8 * math.max(1.0, fit.deviance))
        val betaC = fit.beta.clone()
        betaC(0) += centre(1) * fit.beta(1) + centre(2) * fit.beta(2)
        val d = (s: Double) => local(rows, centre).laplace(s, betaC).deviance
        assert(
          d(fit.sigma * 1.01) >= fit.deviance - 1e-9 && d(fit.sigma * 0.99) >= fit.deviance - 1e-9
        )
      } else {
        val betaStart = new Array[Double](3)
        val d0 = (s: Double) =>
          LogisticRE.stage1Solve(local(rows, centre), 3, s, betaStart, new Array[Double](12))._3
        assert(
          d0(fit.sigma * 1.01) >= fit.deviance - 1e-9 && d0(fit.sigma * 0.99) >= fit.deviance - 1e-9
        )
      }
      val (shiftedRows, shiftedCentre, _, _) = data(shift = 1000.0)
      val shifted = LogisticRE.fit(
        local(shiftedRows, shiftedCentre),
        n,
        12,
        3,
        intercept = true,
        shiftedCentre,
        ybar,
        nagq1
      )
      close(shifted.sigma, fit.sigma, 1e-6 * fit.sigma)
      close(shifted.beta(1), fit.beta(1), 1e-6 * math.abs(fit.beta(1)))
      close(
        shifted.beta(0) + 1000.0 * shifted.beta(1),
        fit.beta(0),
        1e-6 * math.abs(fit.beta(0)) + 1e-9
      )
      close(shifted.deviance, fit.deviance, 1e-9 * fit.deviance)
    }
  }

  test("an upper bound on sigma holds, and too few providers or rows are refused") {
    val (rows, centre, ybar, n) = data()
    val bounded = LogisticRE.fit(
      local(rows, centre),
      n,
      12,
      3,
      intercept = true,
      centre,
      ybar,
      sigmaUpper = 0.05
    )
    assertEquals(bounded.sigma, 0.05)
    assertEquals(bounded.boundary, Some("upper"))
    intercept[IllegalArgumentException](
      LogisticRE.fit(local(rows, centre), n, 1, 3, intercept = true, centre, ybar)
    )
    intercept[IllegalArgumentException](
      LogisticRE.fit(local(rows, centre), 3L, 12, 3, intercept = true, centre, ybar)
    )
  }
}
