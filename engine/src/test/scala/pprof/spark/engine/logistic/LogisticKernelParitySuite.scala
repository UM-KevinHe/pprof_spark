package pprof.spark.engine.logistic

import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.numerics.{InMemorySerbinPasses, Serbin, SerbinOptions}
import pprof.spark.numerics.kernels.{Moments, LogisticFE => Kernel}

/** Function-level and lockstep parity of the kernels and the SerBIN iteration with pprof_py, on
  * in-memory blocks without Spark (Phase 2a specification §12, §9.2).
  */
class LogisticKernelParitySuite extends munit.FunSuite {

  private def start(rows: Kernel.Rows): Array[Double] = {
    val yBar = rows.y.sum / rows.n.sum
    Array.fill(rows.providers)(StrictMath.log(yBar / (1.0 - yBar)))
  }

  for (name <- Cases) {
    test(s"$name: log-likelihood, scores and information at fixed parameters (T-fn)") {
      val rows = block(name)
      val p = rows.p
      Seq("start", "fixed").foreach { point =>
        val ref = json(name, "pprof_py").get("function").get(point)
        val gamma = doubles(ref, "gamma")
        val beta = doubles(ref, "beta")
        val t = Kernel.terms(rows, gamma, beta)
        check("T-fn", s"$point loglik", Seq(t.loglik), doubles(ref, "loglik"))
        check("T-fn", s"$point score_gamma", t.scoreGamma.toSeq, doubles(ref, "score_gamma"))
        check("T-fn", s"$point score_beta", t.scoreBeta.toSeq, doubles(ref, "score_beta"))
        check("T-fn", s"$point info_gamma", t.infoGamma.toSeq, doubles(ref, "info_gamma"))
        val cross = Array.tabulate(p * rows.providers)(i =>
          t.crossInfo((i % rows.providers) * p + i / rows.providers)
        )
        check("T-fn", s"$point info_beta_gamma", cross.toSeq, doubles(ref, "info_beta_gamma"))
        check("T-fn", s"$point info_beta", t.infoBeta.toSeq, doubles(ref, "info_beta"))
        // pprof_py forms S = C − Σ bbᵀ/h by subtraction, which cancels about three digits when the
        // covariates are far from zero (lfe-shifted: 1.38 of T-fn against the centred S, round 27).
        // There S is checked through C, b and h above, and through the iterates and estimates.
        if (name != "lfe-shifted")
          check(
            "T-fn",
            s"$point schur",
            Kernel.schur(rows, gamma, beta).packed.toSeq,
            doubles(ref, "schur")
          )
      }
    }

    test(s"$name: iterates of steps 1 to 5 from the same start (T-iter)") {
      val rows = block(name)
      val passes = new InMemorySerbinPasses(Seq(rows))
      val iterates = json(name, "pprof_py").get("iterates")
      (0 until iterates.size).foreach { k =>
        val ref = iterates.get(k)
        val fit =
          Serbin.fit(start(rows), new Array[Double](rows.p), passes, SerbinOptions(maxIter = k))
        assertEquals(fit.iterations, ref.get("steps").asInt, s"step ${k + 1}")
        check("T-iter", s"step ${k + 1} beta", fit.beta.toSeq, doubles(ref, "beta"))
        check("T-iter", s"step ${k + 1} gamma", fit.gamma.toSeq, doubles(ref, "gamma"))
      }
    }

    test(s"$name: default and tight fits, variances and log-likelihood in memory") {
      val rows = block(name)
      val passes = new InMemorySerbinPasses(Seq(rows))
      val py = json(name, "pprof_py")
      Seq("default" -> SerbinOptions(), "tight" -> SerbinOptions(tol = 1e-13)).foreach {
        case (label, options) =>
          val ref = py.get(label)
          val fit = Serbin.fit(start(rows), new Array[Double](rows.p), passes, options)
          if (label == "default") assertEquals(fit.iterations, ref.get("iterations").asInt)
          check("T-coef", s"$label beta", fit.beta.toSeq, doubles(ref, "beta"))
          check("T-coef", s"$label gamma", fit.gamma.toSeq, doubles(ref, "gamma"))
          val factor = pprof.spark.numerics.Cholesky
            .factor(Kernel.varianceSchur(rows, fit.gamma, fit.beta), rows.p)
          val covariance = factor.inversePacked
          check("T-var", s"$label var_beta", covariance.toSeq, doubles(ref, "var_beta"))
          val xbar = Array.tabulate(rows.p)(j =>
            rows.y.indices.map(r => rows.n(r) * rows.x(r * rows.p + j)).sum / rows.n.sum
          )
          val v = Kernel.providerVariances(rows, fit.gamma, fit.beta, covariance, xbar)
          check("T-var", s"$label var_gamma", v.varGamma.toSeq, doubles(ref, "var_gamma"))
          check("T-var", s"$label var_case_mix", v.varCaseMix.toSeq, doubles(ref, "var_case_mix"))
          check(
            "T-fn",
            s"$label loglik",
            Seq(Kernel.loglik(rows, fit.gamma, fit.beta)),
            doubles(ref, "loglik")
          )
          assertEquals(Moments.packedLength(rows.p), covariance.length)
      }
    }
  }
}
