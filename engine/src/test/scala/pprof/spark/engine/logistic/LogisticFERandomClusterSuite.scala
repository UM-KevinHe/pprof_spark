package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, exp, lit, sum}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Slice 4c-1 (docs/spec/logistic/fe-random-cluster.md §8, D-64): every case of `fixtures/fe-random-cluster` against
  * pprof_py's tightened fit at the three-stage model's classes; σ = 0 against its score equations; the refusals.
  */
class LogisticFERandomClusterSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(6, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"fe-random-cluster/$name/input.csv").toString)

  private def h(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val keep = actual.indices.filter(i =>
      java.lang.Double.isFinite(expected(i)) || java.lang.Double.isFinite(actual(i))
    )
    keep
      .filterNot(i =>
        java.lang.Double.isFinite(expected(i)) && java.lang.Double.isFinite(actual(i))
      )
      .foreach { i =>
        assert(actual(i) == expected(i), s"$what: ${actual(i)} against ${expected(i)} at $i")
      }
    val finite = keep.filter(i =>
      java.lang.Double.isFinite(expected(i)) && java.lang.Double.isFinite(actual(i))
    )
    val value = Tolerances(cls).worstRatio(finite.map(actual).toArray, finite.map(expected).toArray)
    assert(value <= 1.0, s"$what: $value of $cls")
  }

  private def column(rows: Array[Row], name: String): Seq[Double] =
    rows.toSeq.map(r =>
      if (r.isNullAt(r.fieldIndex(name))) Double.NaN else r.getAs[Number](name).doubleValue
    )

  private def setup(name: String): (
      LogisticFERandomClusterSpec,
      LogisticFERandomClusterSource,
      LogisticFERandomClusterOptions
  ) = {
    val c = Fixtures.json(s"fe-random-cluster/$name/case.json")
    val values = Fixtures.json(s"fe-random-cluster/$name/pprof_py.json").get("given")
    val spec = LogisticFERandomClusterSpec(
      "y",
      Seq("x1", "x2"),
      "provider",
      "cluster",
      if (c.has("adjusted")) Some("obs") else None,
      Some("id")
    )
    val source = LogisticFERandomClusterSource.Given(
      h(values.get("beta")).toSeq,
      h(values.get("sigma")).head,
      h(values.get("start")).toSeq
    )
    val mode = if (c.has("bound_mode")) c.get("bound_mode").asText() else "relative"
    (spec, source, LogisticFERandomClusterOptions(boundMode = mode))
  }

  private def tests(
      name: String,
      fit: LogisticFERandomClusterFit,
      key: String,
      inf: JsonNode
  ): Unit = {
    for (
      (label, method, reference, alternative) <- Seq(
        ("exact_median", "exact", EffectReference.Median, "two_sided"),
        ("exact_zero", "exact", EffectReference.Value(0.0), "two_sided"),
        ("exact_greater", "exact", EffectReference.Median, "greater"),
        ("poibin_median", "poibin_exact", EffectReference.Median, "two_sided")
      )
    ) {
      val t =
        LogisticFERandomCluster.test(fit, method, reference, alternative).orderBy(key).collect()
      val x = inf.get("test").get(label)
      val (z, ez, p, ep) =
        (column(t, "z_raw"), h(x.get("z_raw")), column(t, "p_value"), h(x.get("p_value")))
      val clear =
        z.indices.filter(i => ep(i) >= 1e-6) // X-024: pprof_py's FFT tails are exact to about 1e-16
      check("T-test", s"$name $label z", clear.map(z), clear.map(ez).toArray)
      p.zip(ep).foreach { case (a, b) =>
        assert(math.abs(a - b) <= 1e-14 + 1e-8 * b, s"$name $label p $a against $b")
      }
      check(
        "T-coef",
        s"$name $label limits",
        column(t, "ci_lower") ++ column(t, "ci_upper"),
        h(x.get("ci_lower")) ++ h(x.get("ci_upper"))
      )
    }
    val r = LogisticFERandomCluster
      .test(fit, "resampling", nResample = 20000, seed = 7L)
      .orderBy(key)
      .collect()
    column(r, "p_value").zip(h(inf.get("test").get("resampling_median").get("p_value"))).foreach {
      case (a, b) =>
        val tail = math.min(b / 2.0, 0.5)
        val se = math.sqrt(8.0 * math.max(tail * (1.0 - tail), 1e-8) / 20000.0)
        assert(
          math.abs(a - b) <= 4.5 * se || math.abs(a - b) <= 1e-14 + 1e-8 * b,
          s"$name resampling p $a against $b"
        )
    }
  }

  for (name <- Seq("frc-base", "frc-obs", "frc-absolute", "frc-text", "frc-small-sigma")) {
    test(
      s"$name: the fit, its tests, intervals and measures against pprof_py's tightened fit (D-64)"
    ) {
      val (spec, source, options) = setup(name)
      val df = input(name)
      val fit = LogisticFERandomCluster.fit(df, spec, source, options)
      val py = Fixtures.json(s"fe-random-cluster/$name/pprof_py.json")
      val t = py.get("tight")
      assert(fit.stage3.converged, s"$name: not converged")
      check("T-coef", s"$name gamma", fit.gamma.toSeq, h(t.get("gamma")))
      check("T-meas", s"$name alpha mean", fit.stage3.alphaMean.toSeq, h(t.get("alpha_mean")))
      check("T-meas", s"$name alpha var", fit.stage3.alphaVar.toSeq, h(t.get("alpha_var")))
      check("T-fn", s"$name loglik", Seq(fit.stage3.loglik), h(t.get("loglik")))
      val fitted = LogisticFERandomCluster
        .fitted(fit)
        .orderBy("id")
        .limit(100)
        .select("fitted")
        .collect()
        .map(_.getDouble(0))
      check("T-meas", s"$name fitted", fitted.toSeq, h(t.get("fitted")))
      val inf = py.get("inference")
      tests(name, fit, "provider", inf)
      val gamma = LogisticFERandomCluster
        .intervals(fit, option = "gamma")("gamma_ci")
        .orderBy("provider")
        .collect()
      check(
        "T-coef",
        s"$name option gamma",
        column(gamma, "gamma_lower") ++ column(gamma, "gamma_upper"),
        h(inf.get("gamma_ci").get("lower")) ++ h(inf.get("gamma_ci").get("upper"))
      )
      for ((alternative, key) <- Seq("two_sided" -> "sm_two_sided", "greater" -> "sm_greater")) {
        val sm = LogisticFERandomCluster.intervals(
          fit,
          "SM",
          Seq("indirect", "direct"),
          Seq("ratio", "rate"),
          alternative
        )
        for (q <- Seq("indirect_ratio", "indirect_rate", "direct_ratio", "direct_rate")) {
          val rows = sm(q).orderBy("provider").collect()
          val x = inf.get(key).get(q)
          val (kind, measure) = (q.takeWhile(_ != '_'), q.dropWhile(_ != '_').drop(1))
          check("T-meas", s"$name SM $alternative $q", column(rows, q), h(x.get("value")))
          check(
            "T-meas",
            s"$name SM $alternative $q lower",
            column(rows, s"ci_${measure}_lower"),
            h(x.get("lower"))
          )
          check(
            "T-meas",
            s"$name SM $alternative $q upper",
            column(rows, s"ci_${measure}_upper"),
            h(x.get("upper"))
          )
          assert(kind.nonEmpty)
        }
      }
      for (
        (label, reference) <- Seq(
          "median" -> EffectReference.Median,
          "mean" -> EffectReference.Mean,
          "zero" -> EffectReference.Value(0.0)
        )
      ) {
        val me = LogisticFERandomCluster.measures(fit, Seq("indirect", "direct"), reference)
        for (kind <- Seq("indirect", "direct")) {
          val rows = me(kind).orderBy("provider").collect()
          val x = inf.get(s"measures_$label").get(kind)
          x.fieldNames().forEachRemaining { q =>
            if (rows.head.schema.fieldNames.contains(q))
              check("T-meas", s"$name measures $label $kind $q", column(rows, q), h(x.get(q)))
          }
        }
      }
    }
  }

  test("sigma = 0: the clusters' effects vanish and every free provider's score equation holds") {
    val (spec, LogisticFERandomClusterSource.Given(beta, _, start), options) =
      setup("frc-base"): @unchecked
    val df = input("frc-base")
    val fit = LogisticFERandomCluster.fit(
      df,
      spec,
      LogisticFERandomClusterSource.Given(beta, 0.0, start),
      options
    )
    assert(fit.stage3.alphaMean.forall(_ == 0.0) && fit.stage3.alphaVar.forall(_ == 0.0))
    val gamma = fit.providers
      .map(k => pprof.spark.engine.layout.GroupKey.value(k).toString)
      .zip(fit.gamma)
      .toMap
    val keyed =
      spark.createDataFrame(gamma.toSeq.map { case (k, g) => (k.toLong, g) }).toDF("provider", "g")
    val eta = col("g") + col("x1") * lit(beta(0)) + col("x2") * lit(beta(1))
    val scores = df
      .join(keyed, "provider")
      .groupBy("provider")
      .agg(sum(col("y") - lit(1.0) / (lit(1.0) + exp(-eta))).as("score"))
      .collect()
    scores.foreach(r =>
      assert(math.abs(r.getDouble(1)) < 1e-4, s"provider ${r.get(0)}: score ${r.getDouble(1)}")
    )
  }

  test("the sources and options are checked") {
    val (spec, source, options) = setup("frc-base")
    val df = input("frc-base")
    val LogisticFERandomClusterSource.Given(beta, sigma, start) = source: @unchecked
    intercept[IllegalArgumentException](
      LogisticFERandomCluster.fit(
        df,
        spec,
        LogisticFERandomClusterSource.Given(beta.take(1), sigma, start),
        options
      )
    )
    intercept[IllegalArgumentException](
      LogisticFERandomCluster.fit(
        df,
        spec,
        LogisticFERandomClusterSource.Given(beta, sigma, start.take(3)),
        options
      )
    )
    intercept[IllegalArgumentException](LogisticFERandomClusterOptions(estimator = "other"))
    val fit = LogisticFERandomCluster.fit(df, spec, source, options)
    intercept[IllegalArgumentException](LogisticFERandomCluster.summary(fit))
  }

  test(
    "the stage sources: a three-stage fit's stage 1 and stage 2 give its own stage 3, bit for bit"
  ) {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve("three-stage/ts-synthetic/input.csv").toString)
    val three = ThreeStagePipeline.fit(
      df,
      ThreeStageSpec("Y", Seq("x1", "x2", "x3"), "fac", "hosp", Some("rid"))
    )
    val spec = LogisticFERandomClusterSpec(
      "y_adj",
      Seq("x1", "x2", "x3"),
      "fac",
      "hosp",
      Some("Y"),
      Some("rid")
    )
    val fit = LogisticFERandomCluster.fit(
      three.preparation.data,
      spec,
      LogisticFERandomClusterSource.Stages(three.preparation.stage1, three.stage2)
    )
    assertEquals(fit.providers, three.stage3.providers)
    assertEquals(
      fit.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits),
      three.stage3.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits)
    )
    assertEquals(fit.sigma, three.stage2.sigmaCluster)
    assertEquals(
      LogisticFERandomCluster.summary(fit).map(_.statistic),
      LogisticFE.waldTests(three.preparation.stage1).map(_.statistic)
    )
  }

  for (name <- Seq("frc-base", "frc-obs", "frc-absolute", "frc-text", "frc-small-sigma")) {
    test(
      s"$name: he2013 under both criteria, at the defaults and the fixed point, against pprof_py (D-66)"
    ) {
      val (spec, source, options) = setup(name)
      val df = input(name)
      val he = Fixtures.json(s"fe-random-cluster/$name/pprof_py.json").get("he2013")
      for (criterion <- Seq("max_delta_gamma", "relative")) {
        val expected = he.get(criterion)
        val fixed = LogisticFERandomCluster.fit(
          df,
          spec,
          source,
          options.copy(estimator = "he2013", convergenceCriterion = criterion, tol = 1e-12)
        )
        val t = expected.get("tight")
        assert(fixed.stage3.converged, s"$name $criterion: ${fixed.warnings}")
        check("T-coef", s"$name $criterion fixed point gamma", fixed.gamma.toSeq, h(t.get("gamma")))
        check(
          "T-meas",
          s"$name $criterion alpha mean",
          fixed.stage3.alphaMean.toSeq,
          h(t.get("alpha_mean"))
        )
        check(
          "T-meas",
          s"$name $criterion alpha var",
          fixed.stage3.alphaVar.toSeq,
          h(t.get("alpha_var"))
        )
        check("T-fn", s"$name $criterion loglik", Seq(fixed.stage3.loglik), h(t.get("loglik")))
        val fitted = LogisticFERandomCluster
          .fitted(fixed)
          .orderBy("id")
          .limit(100)
          .select("fitted")
          .collect()
          .map(_.getDouble(0))
        check("T-meas", s"$name $criterion fitted", fitted.toSeq, h(t.get("fitted")))
        val default = LogisticFERandomCluster.fit(
          df,
          spec,
          source,
          options.copy(estimator = "he2013", convergenceCriterion = criterion)
        )
        val d = expected.get("default")
        assertEquals(
          default.stage3.iterations,
          d.get("iterations").asInt(),
          s"$name $criterion default iterations"
        )
        check("T-coef", s"$name $criterion default gamma", default.gamma.toSeq, h(d.get("gamma")))
        if (criterion == "max_delta_gamma") {
          val inf = he.get("inference")
          val tbl = LogisticFERandomCluster.test(fixed).orderBy("provider").collect()
          val x = inf.get("exact_median")
          val (z, ez, ep) = (column(tbl, "z_raw"), h(x.get("z_raw")), h(x.get("p_value")))
          val clear = z.indices.filter(i => ep(i) >= 1e-6) // X-024
          check("T-test", s"$name he2013 exact z", clear.map(z), clear.map(ez).toArray)
          val me = LogisticFERandomCluster.measures(fixed, Seq("indirect", "direct"))
          for (kind <- Seq("indirect", "direct")) {
            val rows = me(kind).orderBy("provider").collect()
            val mx = inf.get("measures_median").get(kind)
            mx.fieldNames().forEachRemaining { q =>
              if (rows.head.schema.fieldNames.contains(q))
                check("T-meas", s"$name he2013 measures $kind $q", column(rows, q), h(mx.get(q)))
            }
          }
        }
      }
    }
  }

  test(
    "he2013 at sigma = 0 (X-049): it converges with zero moments, and every free provider's score equation holds"
  ) {
    val (spec, LogisticFERandomClusterSource.Given(beta, _, start), options) =
      setup("frc-base"): @unchecked
    val df = input("frc-base")
    val fit = LogisticFERandomCluster.fit(
      df,
      spec,
      LogisticFERandomClusterSource.Given(beta, 0.0, start),
      options.copy(estimator = "he2013", tol = 1e-12)
    )
    assert(
      fit.stage3.converged && fit.stage3.alphaMean.forall(_ == 0.0) && fit.stage3.alphaVar.forall(
        _ == 0.0
      )
    )
    val gamma = fit.providers
      .map(k => pprof.spark.engine.layout.GroupKey.value(k).toString)
      .zip(fit.gamma)
      .toMap
    val keyed =
      spark.createDataFrame(gamma.toSeq.map { case (k, g) => (k.toLong, g) }).toDF("provider", "g")
    val eta = col("g") + col("x1") * lit(beta(0)) + col("x2") * lit(beta(1))
    val scores = df
      .join(keyed, "provider")
      .groupBy("provider")
      .agg(sum(col("y") - lit(1.0) / (lit(1.0) + exp(-eta))).as("score"))
      .collect()
    scores.foreach(r =>
      assert(math.abs(r.getDouble(1)) < 1e-8, s"provider ${r.get(0)}: score ${r.getDouble(1)}")
    )
    intercept[IllegalArgumentException](
      options.copy(estimator = "he2013", convergenceCriterion = "other")
    )
  }

  test(
    "he2013 on the driver's copy of the cells and on the executors gives the same bits (OI-99)"
  ) {
    val (spec, source, options) = setup("frc-text")
    val df = input("frc-text")
    def fit(path: String) =
      LogisticFERandomCluster.fit(df, spec, source, options.copy(estimator = "he2013", path = path))
    val (driver, executors) = (fit("driver"), fit("executors"))
    def bits(v: Seq[Double]) = v.map(java.lang.Double.doubleToRawLongBits)
    assertEquals(bits(driver.gamma.toSeq), bits(executors.gamma.toSeq))
    assertEquals(bits(driver.stage3.alphaMean.toSeq), bits(executors.stage3.alphaMean.toSeq))
    assertEquals(bits(driver.stage3.alphaVar.toSeq), bits(executors.stage3.alphaVar.toSeq))
    assertEquals(driver.stage3.iterations, executors.stage3.iterations)
  }
}
