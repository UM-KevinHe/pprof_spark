package pprof.spark.engine.logistic

import org.apache.spark.sql.functions.{col, lit}
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.SparkSuite

/** Slice 2b against pprof_py's default fit and R (docs/spec/logistic/covariate-inference.md §9): Wald
  * variants, likelihood-ratio and score tests, AUC, predictions and cluster-robust variances.
  */
class LogisticInferenceSuite extends SparkSuite {

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  for (name <- Cases) {
    test(s"$name: tests, AUC, predictions and robust variances match pprof_py and R") {
      val df = frame(spark, name)
      val fit = LogisticFE.fit(df, spec(name))
      val py = json(name, "pprof_py").get("inference").get("default")
      val r = json(name, "r_logistic")
      Seq("greater", "less").foreach { side =>
        val ref = py.get(s"wald_$side")
        val tests = LogisticFE.waldTests(fit, 0.25, side)
        check("T-test", s"Wald $side z", tests.map(_.statistic), doubles(ref, "stat"))
        checkPValues(s"Wald $side", tests.map(_.pValue), doubles(ref, "p"))
        val finite = if (side == "greater") "ci_lower" else "ci_upper"
        check(
          "T-coef",
          s"Wald $side $finite",
          tests.map(t => if (side == "greater") t.lower else t.upper),
          doubles(ref, finite)
        )
      }
      val lr = LogisticFE.covariateTests(df, fit, "lr")
      val score = LogisticFE.covariateTests(df, fit, "score")
      check("T-test", "LR", lr.map(_.statistic), doubles(py.get("lr"), "stat"))
      checkPValues("LR", lr.map(_.pValue), doubles(py.get("lr"), "p"))
      check("T-test", "score", score.map(_.statistic), doubles(py.get("score"), "stat"))
      checkPValues("score", score.map(_.pValue), doubles(py.get("score"), "p"))
      val auc = doubles(py, "auc")
      assertEquals(fit.auc.isDefined, auc.nonEmpty)
      fit.auc.foreach(a => check("T-meas", "AUC", Seq(a), auc))
      val ids = ints(py, "predict_ids")
      val predicted = LogisticFE
        .predict(df.filter(col("id").isin(ids: _*)), fit)
        .orderBy("id")
        .select("probability")
        .collect()
        .map(_.getDouble(0))
        .toSeq
      check("T-meas", "predictions", predicted, doubles(py, "predict"))
      if (py.has("robust")) {
        val robust = py.get("robust")
        check("T-var", "robust var_beta", fit.robustCovariance.get, doubles(robust, "var_beta"))
        check(
          "T-var",
          "robust var_case_mix",
          fit.providers.robustVarCaseMix.toSeq,
          doubles(robust, "var_case_mix")
        )
        check(
          "T-var",
          "robust var_fixed_beta",
          fit.providers.robustVarFixedBeta.toSeq,
          doubles(robust, "var_fixed_beta")
        )
        val wald = LogisticFE.waldTests(fit, robust = true)
        check(
          "T-test",
          "robust Wald z",
          wald.map(_.statistic),
          doubles(py.get("wald_robust"), "stat")
        )
        check(
          "T-var",
          "robust standard errors",
          wald.map(_.standardError),
          doubles(py.get("wald_robust"), "se")
        )
      } else assert(fit.robustCovariance.isEmpty && fit.providers.robustVarCaseMix.isEmpty)
      // R's references come from tight fits; pprof_py's default and tight tests agree to 3e-16 (round 32).
      if (r.has("glm_tests")) {
        val t = r.get("glm_tests")
        check("T-test", "LR vs glm", lr.map(_.statistic), doubles(t, "lr"))
        check("T-test", "score vs Rao", score.map(_.statistic), doubles(t, "rao"))
        check("T-meas", "predictions vs glm", predicted, doubles(t, "predict"))
        if (t.has("auc")) check("T-meas", "AUC vs Mann-Whitney", fit.auc.toSeq, doubles(t, "auc"))
      }
      if (r.has("glm_robust")) {
        val g = r.get("glm_robust")
        check(
          "T-var",
          "robust var_beta vs vcovCL",
          fit.robustCovariance.get,
          doubles(g, "var_beta")
        )
        check(
          "T-var",
          "robust var_case_mix vs vcovCL",
          fit.providers.robustVarCaseMix.toSeq,
          doubles(g, "var_case_mix")
        )
        check(
          "T-var",
          "robust var_fixed_beta vs R",
          fit.providers.robustVarFixedBeta.toSeq,
          doubles(g, "var_fixed_beta")
        )
      }
    }
  }

  test("a cluster column leaves the fit bitwise unchanged and fills the robust columns") {
    val name = "lfe-clustered"
    val clustered = LogisticFE.fit(frame(spark, name), spec(name))
    val plain = LogisticFE.fit(frame(spark, name), spec(name).copy(cluster = None))
    assertEquals(bits(clustered.estimates), bits(plain.estimates))
    assertEquals(bits(clustered.providers.gamma.toSeq), bits(plain.providers.gamma.toSeq))
    assertEquals(bits(clustered.covariance), bits(plain.covariance))
    assertEquals(clustered.fingerprint, plain.fingerprint)
    val withRobust = LogisticFE.providerTable(spark, clustered)
    assert(withRobust.filter(col("robust_var_case_mix").isNull).count() == 0L)
    assert(
      LogisticFE
        .providerTable(spark, plain)
        .filter(col("robust_var_case_mix").isNotNull)
        .count() == 0L
    )
  }

  test(
    "unknown providers, one feature, robust tests without clusters and other data fail clearly (X-022)"
  ) {
    val name = "lfe-degenerate"
    val df = frame(spark, name)
    val fit = LogisticFE.fit(df, spec(name))
    val screened = df.filter(col("provider") === 23 || col("provider") === 24)
    val failure = intercept[InvalidInputException](LogisticFE.predict(screened, fit))
    assertEquals(failure.problems, Seq(InputProblem.UnknownProviders("provider", screened.count())))
    intercept[IllegalArgumentException](LogisticFE.waldTests(fit, robust = true))
    intercept[IllegalArgumentException](
      LogisticFE.covariateTests(df.withColumn("x1", col("x1") + lit(1.0)), fit, "lr")
    )
    val one = LogisticFE.fit(df, spec(name).copy(features = Seq("x1")))
    intercept[IllegalArgumentException](LogisticFE.covariateTests(df, one, "score"))
    assert(fit.auc.isDefined)
  }
}
