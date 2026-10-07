package pprof.spark.engine.logistic

import org.apache.spark.sql.functions.{col, lit, when}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.{SparkSuite, Tolerances}

/** Invariance, edge cases and outputs of the distributed fit (Phase 2a specification §9, §11, §12). */
class LogisticBehaviourSuite extends SparkSuite {

  private def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)

  private def problems(f: => Any): Seq[InputProblem] = intercept[InvalidInputException](f).problems

  test(
    "row order and partitioning leave the fit bitwise unchanged; block sizes change it within T-part"
  ) {
    val name = "lfe-base"
    val a = LogisticFE.fit(frame(spark, name), spec(name))
    val b = LogisticFE.fit(frame(spark, name, reverse = true).repartition(7), spec(name))
    assertEquals(bits(b.estimates), bits(a.estimates))
    assertEquals(bits(b.providers.gamma.toSeq), bits(a.providers.gamma.toSeq))
    assertEquals(bits(b.providers.varCaseMix.toSeq), bits(a.providers.varCaseMix.toSeq))
    assertEquals(b.fingerprint, a.fingerprint)
    val small = LogisticFE.fit(
      frame(spark, name),
      spec(name),
      LogisticOptions(blocks = BlockOptions(targetBlockBytes = 4096))
    )
    assert(small.layout.blockCount > a.layout.blockCount, s"${small.layout.blockCount} blocks")
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(small.estimates.toArray, a.estimates.toArray) <= 1.0
    )
    assert(
      Tolerances("T-part.parameters").worstRatio(small.providers.gamma, a.providers.gamma) <= 1.0
    )
  }

  test("binomial rows give the fit of the expanded Bernoulli rows within T-part") {
    val name = "lfe-binomial"
    val aggregated = LogisticFE.fit(frame(spark, name), spec(name))
    val expanded = frame(spark, name)
      .selectExpr("*", "explode(sequence(1, n)) as k")
      .withColumn("y", when(col("k") <= col("y"), 1).otherwise(0))
      .drop("n", "k")
      .withColumn("id", lit(0L))
    val bernoulli = LogisticFE.fit(expanded, LogisticSpec("y", Features, "provider"))
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(bernoulli.estimates.toArray, aggregated.estimates.toArray) <= 1.0
    )
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(bernoulli.providers.gamma, aggregated.providers.gamma) <= 1.0
    )
  }

  test("invalid inputs fail at validation with counts, never values (X-019)") {
    val df = frame(spark, "lfe-base")
    assertEquals(
      problems(LogisticFE.fit(df.withColumn("y", lit(2)), spec("lfe-base")))
        .map(_.getClass.getSimpleName),
      Seq("NonBinaryValues")
    )
    assertEquals(
      problems(LogisticFE.fit(df.withColumn("x2", lit(1.5)), spec("lfe-base"))),
      Seq(InputProblem.ConstantFeature("x2"))
    )
    assertEquals(
      problems(LogisticFE.fit(df, spec("lfe-base").copy(features = Seq.empty))),
      Seq(InputProblem.NoFeatures)
    )
    assert(
      problems(LogisticFE.fit(df.withColumn("y", lit(0)), spec("lfe-base"))).head
        .isInstanceOf[InputProblem.NoOutcomeVariation]
    )
    assertEquals(
      problems(LogisticFE.fit(df, spec("lfe-base"), LogisticOptions(minRecords = 1000L))),
      Seq(InputProblem.NoProvidersFitted("provider", 1000L))
    )
    assert(
      problems(LogisticFE.fit(df.withColumn("x1", lit(1.0).cast("string")), spec("lfe-base"))).head
        .isInstanceOf[InputProblem.UnsupportedType]
    )
    val binomial = frame(spark, "lfe-binomial")
    val over =
      problems(LogisticFE.fit(binomial.withColumn("y", col("n") + 1), spec("lfe-binomial")))
    assertEquals(over, Seq(InputProblem.EventsExceedTrials("y", "n", binomial.count())))
    assertEquals(
      problems(
        LogisticFE
          .fit(binomial.withColumn("n", lit(0)).withColumn("y", lit(0)), spec("lfe-binomial"))
      ),
      Seq(InputProblem.NonPositiveValues("n", binomial.count()))
    )
  }

  test("a feature constant within every provider is aliased and named (X-019)") {
    val df = frame(spark, "lfe-base").withColumn("x4", (col("provider") % 3).cast("double"))
    val failure = intercept[LogisticAliasingException] {
      LogisticFE.fit(df, spec("lfe-base").copy(features = Features :+ "x4"))
    }
    assertEquals(failure.features, Seq("x4"))
  }

  test("degenerate and screened providers, maxIter = 0, and screening off") {
    val name = "lfe-degenerate"
    val fit = LogisticFE.fit(frame(spark, name), spec(name))
    assertEquals(fit.excluded.map(_._2), Vector(8L, 10L))
    assertEquals(fit.degenerateProviders, 2)
    assert(
      fit.warnings.exists(_.contains("X-018")) && fit.warnings.exists(_.contains("not fitted"))
    )
    val one = LogisticFE.fit(frame(spark, name), spec(name), LogisticOptions(maxIter = 0))
    assertEquals(one.iterations, 1)
    assert(!one.converged && one.warnings.exists(_.contains("did not converge in 1 steps")))
    val all = LogisticFE.fit(frame(spark, name), spec(name), LogisticOptions(screen = false))
    assertEquals(all.providers.size, 24)
    assert(all.excluded.isEmpty)
  }

  test("the provider table lists every fitted provider with its effect and variances") {
    val name = "lfe-degenerate"
    val fit = LogisticFE.fit(frame(spark, name), spec(name))
    val table = LogisticFE.providerTable(spark, fit)
    assertEquals(table.columns.toSeq, LogisticFE.ProviderColumns)
    val rows = table.orderBy("provider").collect().toSeq
    assertEquals(rows.size, fit.providers.size)
    val first = rows.head
    assertEquals(first.getAs[Long]("provider"), 1L)
    assertEquals(first.getAs[Double]("gamma"), fit.providers.gamma(0))
    assertEquals(rows.count(_.getAs[Boolean]("zero_events")), 1)
    assertEquals(rows.count(_.getAs[Boolean]("all_events")), 1)
  }
}
