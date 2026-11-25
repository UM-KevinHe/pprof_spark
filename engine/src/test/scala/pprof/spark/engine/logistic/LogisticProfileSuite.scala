package pprof.spark.engine.logistic

import java.nio.file.Files

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.SparkSuite

/** The profile-likelihood Newton (docs/spec/logistic/profile-newton.md §9, D-83) on every logistic fixture case: at a
  * tight tolerance against pprof_py's tight fit; at the default tolerance against its default fit, with X-051's
  * handling for providers held at the bound; bit-identical across partitions and input order; persisted.
  */
class LogisticProfileSuite extends SparkSuite {

  private val Profile = LogisticOptions(newton = LogisticOptions.ProfileMethod)

  private def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)

  for (name <- Cases)
    test(
      s"$name: the profile fit matches pprof_py's tight fit, and its default fit apart from X-051"
    ) {
      val df = frame(spark, name)
      val py = json(name, "pprof_py")
      val tightRef = py.get("tight")
      val defaultRef = py.get("default")

      val tight = LogisticFE.fit(df, spec(name), Profile.copy(tol = 1e-13))
      assert(tight.converged, tight.warnings.mkString("; "))
      check("T-coef", "tight beta", tight.estimates, doubles(tightRef, "beta"))
      check("T-coef", "tight gamma", tight.providers.gamma.toSeq, doubles(tightRef, "gamma"))
      check("T-var", "tight var_beta", tight.covariance, doubles(tightRef, "var_beta"))
      check("T-var", "tight standard errors", tight.standardErrors, doubles(tightRef, "se"))
      check("T-fn", "tight loglik", Seq(tight.loglik), doubles(tightRef, "loglik"))
      check("T-fn", "tight AIC", Seq(tight.aic), doubles(tightRef, "aic"))
      check("T-fn", "tight BIC", Seq(tight.bic), doubles(tightRef, "bic"))
      assertEquals(tight.providers.atBound.toSeq, booleans(tightRef, "at_bound"))

      val default = LogisticFE.fit(df, spec(name), Profile)
      assert(default.converged, default.warnings.mkString("; "))
      assertEquals(default.cappedSolves, 0)
      assert(default.innerSteps > 0L)
      // X-051: β, the interior effects and at_bound as pprof_py's default fit; the effects held at the bound, and what is
      // computed at them (ℓ, AIC, BIC, the variances of β), as its tight fit; iteration counts not compared.
      check("T-coef", "default beta", default.estimates, doubles(defaultRef, "beta"))
      check("T-var", "default var_beta vs tight", default.covariance, doubles(tightRef, "var_beta"))
      check(
        "T-var",
        "default standard errors vs tight",
        default.standardErrors,
        doubles(tightRef, "se")
      )
      val atBound = booleans(defaultRef, "at_bound")
      assertEquals(default.providers.atBound.toSeq, atBound)
      val gamma = default.providers.gamma.toSeq
      val interior = atBound.indices.filterNot(atBound)
      val held = atBound.indices.filter(atBound)
      check(
        "T-coef",
        "default interior gamma",
        interior.map(gamma),
        interior.map(doubles(defaultRef, "gamma")).toArray
      )
      if (held.nonEmpty)
        check(
          "T-coef",
          "default held gamma vs tight",
          held.map(gamma),
          held.map(doubles(tightRef, "gamma")).toArray
        )
      check("T-fn", "default loglik vs tight", Seq(default.loglik), doubles(tightRef, "loglik"))
      check("T-fn", "default AIC vs tight", Seq(default.aic), doubles(tightRef, "aic"))
      check("T-fn", "default BIC vs tight", Seq(default.bic), doubles(tightRef, "bic"))
    }

  test("a profile fit is bit-identical across shuffle partitions and input order (R0, DIST-6)") {
    val name = "lfe-zero-events"
    val options = Profile.copy(blocks = BlockOptions(targetBlockBytes = 64L * 1024))
    val fit = LogisticFE.fit(frame(spark, name), spec(name), options)
    assert(fit.layout.blockCount > 4, s"${fit.layout}")
    val other = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
      LogisticFE.fit(frame(spark, name, reverse = true).repartition(5), spec(name), options)
    )
    assertEquals(
      bits(other.estimates ++ other.covariance ++ other.providers.gamma.toSeq :+ other.loglik),
      bits(fit.estimates ++ fit.covariance ++ fit.providers.gamma.toSeq :+ fit.loglik)
    )
    assertEquals((other.iterations, other.innerSteps), (fit.iterations, fit.innerSteps))
  }

  test("a profile fit persists its method and inner counts; the method is validated") {
    val name = "lfe-degenerate"
    val fit = LogisticFE.fit(frame(spark, name), spec(name), Profile)
    val path = Files.createTempDirectory("pprof-logistic-profile").resolve("model").toString
    LogisticFitIO.save(spark, fit, path)
    val loaded = LogisticFitIO.load(spark, path)
    assertEquals(loaded.options.newton, LogisticOptions.ProfileMethod)
    assertEquals((loaded.innerSteps, loaded.cappedSolves), (fit.innerSteps, fit.cappedSolves))
    assertEquals(bits(loaded.estimates), bits(fit.estimates))
    intercept[IllegalArgumentException](LogisticOptions(newton = "lbfgs"))
  }
}
