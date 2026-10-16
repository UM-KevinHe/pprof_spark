package pprof.spark.engine.logistic

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.numerics.Serbin
import pprof.spark.testkit.SparkSuite

/** Providers added after fitting (pprof_py's `add_providers`; specification §8a; production finding
  * L1), as R's PPPW step uses it: providers left out of the fit are added back with fixed effects, and
  * every output that lists providers includes them as pprof_py v0.7.0 does.
  */
class LogisticAddProvidersSuite extends SparkSuite {

  private lazy val all = frame(spark, "lfe-base")
  private lazy val keys =
    all.select("provider").distinct().orderBy("provider").collect().map(_.getLong(0))
  private lazy val d0 = keys(0)
  private lazy val d1 = keys(1)
  private lazy val df = all.filter(!col("provider").isin(d0, d1))
  private lazy val fit = LogisticFE.fit(df, spec("lfe-base"))
  private lazy val replaced = fit.providers.keys(5)
  private lazy val extended = fit.withProviders(
    Seq(GroupKey.Integral(d0), GroupKey.Integral(d1), replaced),
    Seq(-17.0, 17.0, 0.5),
    Seq(0.01, 0.01, 0.01),
    Seq(11L, 12L, 40L)
  )

  private def rows(table: DataFrame): Array[Row] = table.orderBy("provider").collect()
  private def provider(table: Array[Row], key: Long): Row =
    table.find(_.getAs[Long]("provider") == key).get
  private def replacedKey: Long = GroupKey.value(replaced).asInstanceOf[Long]

  test(
    "the view: every provider in key order, added and replaced ones marked, sizes as pprof_py's"
  ) {
    val view = LogisticProviderView.of(extended)
    assertEquals(view.size, fit.providers.size + 2)
    assertEquals(view.keys.toSeq, view.keys.toSeq.sorted(GroupKey.ordering))
    val at = view.keys.zipWithIndex.toMap
    val (a0, a1, r) = (at(GroupKey.Integral(d0)), at(GroupKey.Integral(d1)), at(replaced))
    assertEquals(Seq(view.gamma(a0), view.gamma(a1), view.gamma(r)), Seq(-17.0, 17.0, 0.5))
    assertEquals(Seq(view.records(a0), view.records(a1)), Seq(11L, 12L))
    assertEquals(view.records(r), fit.providers.records(fit.providers.keys.indexOf(replaced)))
    assertEquals(view.varCaseMix(a0), 0.01 * 0.01)
    assertEquals((view.fitted(a0), view.added.count(identity)), (-1, 3))
    assert(LogisticProviderView.of(fit).gamma eq fit.providers.gamma)
    // A later call replaces an earlier addition, which keeps its group size.
    val again = extended.withProviders(Seq(GroupKey.Integral(d0)), Seq(-16.0), Seq(0.02), Seq(99L))
    val v2 = LogisticProviderView.of(again)
    val i0 = v2.keys.indexOf(GroupKey.Integral(d0))
    assertEquals((v2.gamma(i0), v2.records(i0)), (-16.0, 11L))
    intercept[IllegalArgumentException](
      fit.withProviders(Seq(GroupKey.Text("x")), Seq(1.0), Seq(0.01), Seq(1L))
    )
    intercept[IllegalArgumentException](
      fit.withProviders(Seq(GroupKey.Integral(d0)), Seq(1.0), Seq(0.0), Seq(1L))
    )
  }

  test("the reference effect: the median of every effect; the mean weighted by records and sizes") {
    val view = LogisticProviderView.of(extended)
    assertEquals(
      LogisticProviderTests.reference(extended, EffectReference.Median),
      Serbin.median(view.gamma)
    )
    val mean =
      view.gamma.indices.map(k => view.records(k) * view.gamma(k)).sum / view.records.sum.toDouble
    assert(
      math.abs(LogisticProviderTests.reference(extended, EffectReference.Mean) - mean) <= 1e-14
    )
  }

  test("measures: direct ones for every provider; indirect ones missing where there are no rows") {
    val view = LogisticProviderView.of(extended)
    val (a0, a1, r) = (
      view.keys.indexOf(GroupKey.Integral(d0)),
      view.keys.indexOf(GroupKey.Integral(d1)),
      view.keys.indexOf(replaced)
    )
    val gamma = LogisticStandardization.measure(df, extended, "gamma")
    assertEquals((gamma.estimate(a0), gamma.se(a1)), (-17.0, 0.01))
    val direct = LogisticStandardization.measure(df, extended, "direct_rate")
    assert(direct.estimate(a0) < 1e-6 && direct.estimate(a1) > 1.0 - 1e-6)
    val indirect = LogisticStandardization.measure(df, extended, "indirect_ratio")
    assert(indirect.estimate(a0).isNaN && indirect.estimate(a1).isNaN)
    assert(java.lang.Double.isFinite(indirect.estimate(r)))
    val tables = LogisticStandardization.measures(df, extended)
    val row = provider(rows(tables("indirect")), d0)
    assert(row.getAs[Double]("indirect_ratio").isNaN)
    assertEquals((row.getAs[Double]("observed"), row.getAs[Double]("expected")), (0.0, 0.0))
    assertEquals(rows(tables("direct")).length, view.size)
    // The measure as a table: every provider, each with its own value.
    val table = rows(LogisticStandardization.measureTable(spark, extended, direct))
    assertEquals(table.length, view.size)
    assertEquals(provider(table, d0).getAs[Double]("estimate"), direct.estimate(a0))
    assertEquals(provider(table, replacedKey).getAs[Double]("estimate"), direct.estimate(r))
  }

  test("tests on a standardized measure include the added providers, in a null value of mean") {
    val estimates = LogisticStandardization.measure(df, extended, "direct_rate").estimate
    val finite = estimates.filter(java.lang.Double.isFinite)
    val table = rows(LogisticStandardization.test(df, extended, "direct_rate", NullValue.Mean))
    assertEquals(table.length, LogisticProviderView.of(extended).size)
    assert(math.abs(table.head.getAs[Double]("null_value") - finite.sum / finite.length) <= 1e-15)
    assertEquals(provider(table, d0).getAs[Int]("flag"), -1)
    assertEquals(provider(table, d1).getAs[Int]("flag"), 1)
  }

  test(
    "provider tests: an added provider has z 0 and p 1 in the data-based tests, a Wald test of its own"
  ) {
    val g0 = LogisticProviderTests.reference(extended, EffectReference.Median)
    for (method <- Seq("poibin_exact", "score", "wald")) {
      val table = rows(LogisticProviderTests.test(df, extended, method))
      assertEquals(table.length, LogisticProviderView.of(extended).size, method)
      val a0 = provider(table, d0)
      assertEquals(provider(table, replacedKey).getAs[Double]("estimate"), 0.5, method)
      if (method == "wald") {
        assert(math.abs(a0.getAs[Double]("z_raw") - (-17.0 - g0) / 0.01) <= 1e-9, method)
        assertEquals(a0.getAs[Int]("flag"), -1)
      } else {
        assertEquals(
          (a0.getAs[Double]("z_raw"), a0.getAs[Double]("p_value"), a0.getAs[Int]("flag")),
          (0.0, 1.0, 0),
          method
        )
        val lower = a0.getAs[Double]("ci_lower")
        if (method == "poibin_exact") assertEquals(lower, Double.NegativeInfinity)
        else assert(lower.isNaN, method)
      }
    }
  }

  test("the provider table, predictions and saved models carry the added providers") {
    val table = rows(LogisticFE.providerTable(spark, extended))
    assertEquals(table.length, LogisticProviderView.of(extended).size)
    val a1 = provider(table, d1)
    assert(a1.getAs[Boolean]("added"))
    assertEquals(
      (a1.getAs[Long]("records"), a1.getAs[Double]("trials"), a1.getAs[Double]("gamma")),
      (12L, 0.0, 17.0)
    )
    assert(!rows(LogisticFE.providerTable(spark, fit)).exists(_.getAs[Boolean]("added")))
    val rowsOfAdded = all.filter(col("provider") === d1)
    intercept[Exception](LogisticFE.predict(rowsOfAdded, fit).count())
    val predicted = LogisticFE.predict(rowsOfAdded, extended).collect()
    assert(predicted.nonEmpty)
    predicted.foreach { r =>
      val dot =
        Seq("x1", "x2", "x3").zip(fit.estimates).map { case (x, b) => r.getAs[Double](x) * b }.sum
      assert(math.abs(r.getAs[Double]("linear_predictor") - (17.0 + dot)) <= 1e-12)
    }
    val directory = Files.createTempDirectory("pprof-logistic-added")
    LogisticFitIO.save(spark, extended, directory.resolve("extended").toString)
    val loaded = LogisticFitIO.load(spark, directory.resolve("extended").toString)
    assertEquals(loaded.added.keys.toSeq, extended.added.keys.toSeq)
    assertEquals(
      loaded.added.gamma.map(java.lang.Double.doubleToLongBits).toSeq,
      extended.added.gamma.map(java.lang.Double.doubleToLongBits).toSeq
    )
    assertEquals(loaded.added.seGamma.toSeq, extended.added.seGamma.toSeq)
    assertEquals(loaded.added.sizes.toSeq, extended.added.sizes.toSeq)
    LogisticFitIO.save(spark, fit, directory.resolve("plain").toString)
    assertEquals(LogisticFitIO.load(spark, directory.resolve("plain").toString).added.size, 0)
  }
}
