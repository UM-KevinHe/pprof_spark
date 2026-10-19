package pprof.spark.bench.synthetic

import java.nio.file.Files

import org.apache.spark.sql.functions.{avg, countDistinct, max, min, stddev}

import pprof.spark.engine.cox.{CoxPH, CoxSpec}
import pprof.spark.engine.linear.{LinearFE, LinearSpec}
import pprof.spark.engine.logistic.{LogisticFE, LogisticSpec}
import pprof.spark.testkit.SparkSuite

/** The synthetic data of the scale tests (scale-test plan §4): deterministic across partitionings, distributed as
  * designed, and fitted back to their true coefficients by the models they were generated for.
  */
class SyntheticSuite extends SparkSuite {

  private def small(model: Model): Design = Design(model, n = 20000L, m = 20, p = 4)

  private def within(what: String, estimate: Double, se: Double, truth: Double): Unit =
    assert(
      math.abs(estimate - truth) <= 4.5 * se,
      s"$what: estimate $estimate, standard error $se, truth $truth"
    )

  test("the rows are the same for any number of partitions") {
    Seq(Model.Cox(), Model.Logistic(maxTrials = 5), Model.Linear()).foreach { model =>
      val design = Design(model, n = 5000L, m = 17, p = 5, sizes = Sizes.Zipf(0.7))
      val (one, seven) = (Synthetic.rows(spark, design, 1), Synthetic.rows(spark, design, 7))
      assertEquals(Synthetic.checksum(one), Synthetic.checksum(seven), model.name)
      assertEquals(
        one.orderBy("id").collect().toSeq,
        seven.orderBy("id").collect().toSeq,
        model.name
      )
    }
  }

  test("providers own their designed ranges, and the features follow their distributions") {
    val design = Design(Model.Linear(), n = 40000L, m = 13, p = 4, sizes = Sizes.Dominant(0.05))
    val df = Synthetic.rows(spark, design, 3)
    val counts =
      df.groupBy("provider").count().orderBy("provider").collect().map(_.getLong(1)).toSeq
    assertEquals(counts, design.sizes.sizes(design.n, design.m).toSeq)
    val s = df.agg(avg("x1"), avg("x3"), avg("x2"), avg("x4"), stddev("x2"), stddev("x4")).head()
    val n = design.n.toDouble
    Seq(0, 1).foreach(i =>
      assert(math.abs(s.getDouble(i) - 0.3) <= 4.5 * math.sqrt(0.21 / n), s"binary $i")
    )
    Seq(2, 3).foreach(i =>
      assert(math.abs(s.getDouble(i)) <= 4.5 / math.sqrt(n), s"normal mean $i")
    )
    Seq(4, 5).foreach(i => assert(math.abs(s.getDouble(i) - 1.0) <= 0.02, s"normal sd $i"))
    assert(
      df.select("x2")
        .collect()
        .forall(r => r.getDouble(0) * 64.0 == math.rint(r.getDouble(0) * 64.0))
    )
  }

  test(
    "the logistic fixed-effect model recovers the true coefficients, from single trials and from counts"
  ) {
    val bernoulli = small(Model.Logistic())
    val fit = LogisticFE.fit(
      Synthetic.rows(spark, bernoulli, 4),
      LogisticSpec("y", bernoulli.features, "provider", rowId = Some("id"))
    )
    fit.coefficients.zip(bernoulli.beta).foreach { case (c, b) =>
      within(c.feature, c.estimate, c.standardError, b)
    }
    val counts = Design(Model.Logistic(maxTrials = 20), n = 5000L, m = 20, p = 4)
    val binomial = LogisticFE.fit(
      Synthetic.rows(spark, counts, 4),
      LogisticSpec("y", counts.features, "provider", trials = Some("n"), rowId = Some("id"))
    )
    binomial.coefficients.zip(counts.beta).foreach { case (c, b) =>
      within(s"binomial ${c.feature}", c.estimate, c.standardError, b)
    }
  }

  test("the linear fixed-effect model recovers the true coefficients and σ") {
    val design = small(Model.Linear())
    val fit = LinearFE.fit(
      Synthetic.rows(spark, design, 4),
      LinearSpec("y", design.features, "provider", Some("id"))
    )
    fit.coefficients.zip(design.beta).foreach { case (c, b) =>
      within(c.feature, c.estimate, c.standardError, b)
    }
    assert(math.abs(fit.sigma - 1.0) <= 0.03, s"sigma ${fit.sigma}")
  }

  test(
    "Cox, stratified by provider, recovers the true coefficients from whole-day times with ties"
  ) {
    val design = small(Model.Cox())
    val df = Synthetic.rows(spark, design, 4)
    val fit = CoxPH.fit(
      df,
      CoxSpec("time", "event", design.features, strata = Some("provider"), rowId = Some("id"))
    )
    fit.coefficients.zip(design.beta).foreach { case (c, b) =>
      within(c.feature, c.estimate, c.standardError, b)
    }
    val s = df.agg(min("time"), max("time"), avg("event"), countDistinct("time")).head()
    assert(
      s.getDouble(0) >= 1.0 && s.getDouble(1) <= 1825.0,
      s"times from ${s.getDouble(0)} to ${s.getDouble(1)}"
    )
    assert(s.getDouble(2) > 0.1 && s.getDouble(2) < 0.9, s"event share ${s.getDouble(2)}")
    assert(s.getLong(3) < design.n, "no tied times")
  }

  test("written data carry a manifest that verifies, and a regeneration matches its checksum") {
    val design = Design(Model.Logistic(), n = 3000L, m = 7, p = 3)
    val path = Files.createTempDirectory("pprof-synthetic").resolve("point").toString
    val manifest = Synthetic.write(spark, design, path, partitions = 2)
    assertEquals(manifest.rows, 3000L)
    assert(Synthetic.verify(spark, path))
    assertEquals(Synthetic.checksum(Synthetic.rows(spark, design, 5)), manifest.checksum)
    assertEquals(
      Synthetic.checksum(spark.read.parquet(s"$path/providers")),
      manifest.providersChecksum
    )
    val json = Manifest.parse(Synthetic.manifest(spark, path))
    assertEquals(json.get("version").asInt, Synthetic.Version)
    assertEquals(json.get("model").asText, "logistic")
    assertEquals((0 until 3).map(k => json.get("beta").get(k).asDouble), design.beta)
    intercept[Exception](Synthetic.write(spark, design, path))
  }
}
