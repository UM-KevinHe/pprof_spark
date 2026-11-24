package pprof.spark.engine.cox

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.cox.CoxFixtures._
import pprof.spark.testkit.SparkSuite

/** Strata larger than a block, evaluated from per-time aggregates over many blocks (docs/spec/cox/per-time-aggregates.md
  * §8, D-81): the unstratified cases against pprof_py and R, against the swept evaluation, to the bit across partitions
  * and input order, and the fallback to one block when the per-time totals exceed the driver's budget.
  */
class CoxAggregationSuite extends SparkSuite {

  /** 36 rows per block at p = 3: rc-unstratified spans 12 blocks and lt-unstratified 139. */
  private val tiny = BlockOptions(targetBlockBytes = 2048L)

  private val unstratified = Seq("rc-unstratified", "lt-unstratified")

  private def scaled(a: Array[Double], b: Array[Double]): Double =
    a.indices.map(i => math.abs(a(i) - b(i))).max / math.max(
      b.map(math.abs).max,
      Double.MinPositiveValue
    )

  for (name <- unstratified; ties <- Methods)
    test(
      s"aggregated: function-level values and tight fits match pprof_py and R: $name, ${ties.name}"
    ) {
      val options = CoxOptions(ties = ties, blocks = tiny)
      for (point <- Seq("beta_zero", "beta_fixed")) {
        val beta = doubles(reference(name, "pprof_py", ties).get(point), "beta")
        val e = CoxPH.evaluateAt(frame(spark, name), spec(name), beta, options)
        for (source <- Seq("pprof_py", "r_survival")) {
          val node = reference(name, source, ties).get(point)
          val what = s"$name ${ties.name} $point vs $source, aggregated"
          check("T-fn", s"$what loglik", Seq(e.value), doubles(node, "loglik"))
          check("T-fn", s"$what score", e.gradient.toSeq, doubles(node, "score"))
          check("T-fn", s"$what information", e.information.toSeq, doubles(node, "information"))
        }
      }
      val fit = CoxPH.fit(frame(spark, name), spec(name), tight(ties).copy(blocks = tiny))
      assert(fit.converged, fit.message)
      assertEquals(fit.layout.oversizedGroups, 1)
      assert(fit.layout.blockCount > 10, s"expected the stratum over many blocks: ${fit.layout}")
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source, ties).get("tight")
        val what = s"$name ${ties.name} vs $source, aggregated over ${fit.layout.blockCount} blocks"
        check("T-coef", s"$what coefficients", fit.estimates, doubles(node, "coef"))
        check("T-var", s"$what standard errors", fit.standardErrors, doubles(node, "se"))
        check("T-var", s"$what covariance", fit.covariance, doubles(node, "covariance"))
        check("T-fn", s"$what loglik", Seq(fit.logLikelihood), doubles(node, "loglik"))
      }
    }

  for (name <- unstratified; ties <- Methods)
    test(s"aggregated and swept evaluations agree within 1e-12, scaled: $name, ${ties.name}") {
      val beta = doubles(reference(name, "pprof_py", ties).get("beta_fixed"), "beta")
      val swept = CoxPH.evaluateAt(frame(spark, name), spec(name), beta, CoxOptions(ties = ties))
      val aggregated =
        CoxPH.evaluateAt(
          frame(spark, name),
          spec(name),
          beta,
          CoxOptions(ties = ties, blocks = tiny)
        )
      val dv = math.abs(aggregated.value - swept.value) / math.abs(swept.value)
      assert(dv <= 1e-12, s"loglik scaled difference $dv")
      val du = scaled(aggregated.gradient, swept.gradient)
      assert(du <= 1e-12, s"score scaled difference $du")
      val di = scaled(aggregated.information, swept.information)
      assert(di <= 1e-12, s"information scaled difference $di")
    }

  test(
    "an aggregated fit is reproducible to the bit across shuffle partitions and input order (R0, DIST-6)"
  ) {
    val name = "lt-unstratified"
    val options = CoxOptions(ties = Ties.Efron, blocks = tiny)
    val fit = CoxPH.fit(frame(spark, name), spec(name), options)
    val other = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
      CoxPH.fit(frame(spark, name, reverse = true).repartition(5), spec(name), options)
    )
    assertEquals(
      bits(other.estimates ++ other.covariance ++ other.information :+ other.logLikelihood),
      bits(fit.estimates ++ fit.covariance ++ fit.information :+ fit.logLikelihood)
    )
  }

  test("a stratum whose per-time totals exceed the driver's budget keeps one block, as before") {
    val name = "lt-unstratified"
    // 60 grid times at w = p + 3 = 6 numbers take 2,880 bytes, more than this budget.
    val fallback = CoxPH.fit(
      frame(spark, name),
      spec(name),
      CoxOptions(blocks = tiny.copy(driverBudgetBytes = 2048L))
    )
    assertEquals(fallback.layout.blockCount, 1)
    val default = CoxPH.fit(frame(spark, name), spec(name))
    assertEquals(
      bits(fallback.estimates ++ fallback.covariance :+ fallback.logLikelihood),
      bits(default.estimates ++ default.covariance :+ default.logLikelihood)
    )
  }
}
