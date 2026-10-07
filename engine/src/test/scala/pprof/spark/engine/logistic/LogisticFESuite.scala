package pprof.spark.engine.logistic

import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.SparkSuite

/** End-to-end parity of the distributed fit with pprof_py's default and tight fits, R's `glm` and R
  * pprof's SerBIN (Phase 2a specification §12), one test per fixture case.
  */
class LogisticFESuite extends SparkSuite {

  for (name <- Cases) {
    test(s"$name: the distributed fit matches pprof_py and R") {
      val df = frame(spark, name)
      val py = json(name, "pprof_py")
      val r = json(name, "r_logistic")
      val default = LogisticFE.fit(df, spec(name))
      val ref = py.get("default")
      assertEquals(default.iterations, ref.get("iterations").asInt)
      assert(default.converged)
      check("T-coef", "beta", default.estimates, doubles(ref, "beta"))
      check("T-coef", "gamma", default.providers.gamma.toSeq, doubles(ref, "gamma"))
      check("T-var", "var_beta", default.covariance, doubles(ref, "var_beta"))
      check("T-var", "var_gamma", default.providers.varGamma.toSeq, doubles(ref, "var_gamma"))
      check(
        "T-var",
        "var_case_mix",
        default.providers.varCaseMix.toSeq,
        doubles(ref, "var_case_mix")
      )
      check("T-var", "standard errors", default.standardErrors, doubles(ref, "se"))
      check("T-test", "Wald z", default.coefficients.map(_.z), doubles(ref, "z"))
      checkPValues("Wald", default.coefficients.map(_.pValue), doubles(ref, "p"))
      check("T-coef", "interval lower", default.coefficients.map(_.lower), doubles(ref, "ci_lower"))
      check("T-coef", "interval upper", default.coefficients.map(_.upper), doubles(ref, "ci_upper"))
      check("T-fn", "loglik", Seq(default.loglik), doubles(ref, "loglik"))
      check("T-fn", "AIC", Seq(default.aic), doubles(ref, "aic"))
      check("T-fn", "BIC", Seq(default.bic), doubles(ref, "bic"))
      assertEquals(default.providers.atBound.toSeq, booleans(ref, "at_bound"))
      assertEquals(
        default.providers.keys.toSeq.map(k =>
          pprof.spark.engine.layout.GroupKey.value(k).toString.toLong
        ),
        ints(ref, "providers")
      )
      assertEquals(
        default.excluded.map(e => pprof.spark.engine.layout.GroupKey.value(e._1).toString.toLong),
        ints(ref, "excluded").toVector
      )

      val tight = LogisticFE.fit(df, spec(name), LogisticOptions(tol = 1e-13))
      val pyTight = py.get("tight")
      check("T-coef", "tight beta vs pprof_py", tight.estimates, doubles(pyTight, "beta"))
      check(
        "T-coef",
        "tight gamma vs pprof_py",
        tight.providers.gamma.toSeq,
        doubles(pyTight, "gamma")
      )
      check("T-var", "tight var_beta vs pprof_py", tight.covariance, doubles(pyTight, "var_beta"))
      val serbin = r.get("serbin_tight")
      check("T-coef", "tight beta vs R SerBIN", tight.estimates, doubles(serbin, "beta"))
      check(
        "T-coef",
        "tight gamma vs R SerBIN",
        tight.providers.gamma.toSeq,
        doubles(serbin, "gamma")
      )
      if (r.has("glm")) {
        val glm = r.get("glm")
        check("T-coef", "beta vs glm", tight.estimates, doubles(glm, "beta"))
        check("T-coef", "gamma vs glm", tight.providers.gamma.toSeq, doubles(glm, "gamma"))
        check("T-var", "var_beta vs glm", tight.covariance, doubles(glm, "var_beta"))
        check(
          "T-var",
          "var_gamma vs glm",
          tight.providers.varGamma.toSeq,
          doubles(glm, "var_gamma")
        )
        check(
          "T-var",
          "var_case_mix vs glm",
          tight.providers.varCaseMix.toSeq,
          doubles(glm, "var_case_mix")
        )
        check("T-fn", "loglik vs glm", Seq(tight.loglik), doubles(glm, "loglik"))
      }
    }
  }
}
