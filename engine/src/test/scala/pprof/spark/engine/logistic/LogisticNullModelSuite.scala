package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}

import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.numerics.Normal
import pprof.spark.testkit.SparkSuite

/** Null models, intervals, bounds and null values of tests on standardized measures (standardization
  * specification §2a; issues L2, L3 and L4, D-49), against pprof_py's formulas.
  */
class LogisticNullModelSuite extends SparkSuite {

  private lazy val df = frame(spark, "lfe-base")
  private lazy val fit = LogisticFE.fit(df, spec("lfe-base"))

  private def rows(table: DataFrame): Array[Row] = table.orderBy("provider").collect()
  private def d(r: Row, column: String): Double = r.getAs[Double](column)
  private def bits(x: Double): Long = java.lang.Double.doubleToLongBits(x)

  private val doubleColumns = Seq(
    "estimate",
    "se",
    "null_value",
    "transformed",
    "se_transformed",
    "null_transformed",
    "z_raw",
    "null_mean",
    "null_sd",
    "z_adjusted",
    "p_value",
    "ci_lower",
    "ci_upper"
  )

  test(
    "the defaults, and a fixed null of mean 0 and sd 1, reproduce the theoretical null bit for bit"
  ) {
    for (measure <- Seq("direct_rate", "indirect_ratio")) {
      val a = rows(LogisticStandardization.test(df, fit, measure))
      val b = rows(
        LogisticStandardization.test(
          df,
          fit,
          measure,
          NullValue.Reference,
          nullModel = NullModel.Fixed(0.0, 1.0),
          interval = "inversion",
          bounds = IntervalBounds.Auto
        )
      )
      for ((x, y) <- a.zip(b); c <- doubleColumns)
        assertEquals(bits(d(y, c)), bits(d(x, c)), s"$measure $c")
      a.zip(b).foreach { case (x, y) => assertEquals(y.getAs[Any]("flag"), x.getAs[Any]("flag")) }
    }
  }

  for (measure <- Seq("direct_rate", "indirect_ratio"); interval <- Seq("inversion", "scale_only"))
    test(
      s"a fixed null calibrates z, p-values, flags and intervals as pprof_py's: $measure, $interval"
    ) {
      val (mu, sigma) = (0.25, 1.5)
      val theoretical = rows(LogisticStandardization.test(df, fit, measure))
      val fixed = rows(
        LogisticStandardization.test(
          df,
          fit,
          measure,
          nullModel = NullModel.Fixed(mu, sigma),
          interval = interval
        )
      )
      // As the engine: alpha = 1 - level, c the upper alpha/2 quantile (both a hair off 0.05 and 1.96).
      val alpha = 1.0 - 0.95
      val c = Normal.upperQuantile(alpha / 2.0)
      val (upper, lower) =
        if (interval == "inversion") (mu + c * sigma, mu - c * sigma) else (c * sigma, -c * sigma)
      // direct_rate tests on the logit scale; indirect_ratio on the identity scale, clipped to [0, ∞).
      val inverse: Double => Double =
        if (measure == "direct_rate") x => 1.0 / (1.0 + StrictMath.exp(-x))
        else x => math.max(x, 0.0)
      var tested = 0
      theoretical.zip(fixed).foreach { case (t0, f) =>
        val z = d(t0, "z_raw")
        assertEquals(bits(d(f, "z_raw")), bits(z))
        if (!z.isNaN) {
          tested += 1
          val zc = (z - mu) / sigma
          assertEquals(bits(d(f, "z_adjusted")), bits(zc))
          assertEquals(d(f, "null_mean"), mu)
          assertEquals(d(f, "null_sd"), sigma)
          val p = Normal.twoSidedPValue(zc)
          assertEquals(bits(d(f, "p_value")), bits(p))
          assertEquals(f.getAs[Int]("flag"), if (p < alpha) (if (zc > 0.0) 1 else -1) else 0)
          val (t, st) = (d(t0, "transformed"), d(t0, "se_transformed"))
          assertEquals(bits(d(f, "ci_lower")), bits(inverse(t - upper * st)))
          assertEquals(bits(d(f, "ci_upper")), bits(inverse(t - lower * st)))
        }
      }
      assert(tested > 0)
    }

  test("bounds: auto clips identity-scale rates to [0, 1], none leaves them, a pair clips to it") {
    def run(bounds: IntervalBounds) =
      rows(
        LogisticStandardization.test(
          df,
          fit,
          "direct_rate",
          transform = "identity",
          bounds = bounds
        )
      )
    val auto = run(IntervalBounds.Auto)
    val none = run(IntervalBounds.Unbounded)
    val pair = run(IntervalBounds.Fixed(0.05, 0.6))
    for (((a, n), q) <- auto.zip(none).zip(pair); column <- Seq("ci_lower", "ci_upper")) {
      val raw = d(n, column)
      if (!raw.isNaN) {
        assertEquals(bits(d(a, column)), bits(math.min(math.max(raw, 0.0), 1.0)))
        assertEquals(bits(d(q, column)), bits(math.min(math.max(raw, 0.05), 0.6)))
      }
    }
    intercept[IllegalArgumentException](IntervalBounds.Fixed(0.6, 0.05))
    intercept[IllegalArgumentException](NullModel.Fixed(0.0, 0.0))
    intercept[IllegalArgumentException](
      LogisticStandardization.test(df, fit, "direct_rate", interval = "wald")
    )
  }

  test(
    "null values: the mean and median of the providers' estimates, not the rate at the mean effect"
  ) {
    val estimates = LogisticStandardization.measure(df, fit, "direct_rate").estimate
    val finite = estimates.filter(java.lang.Double.isFinite).sorted
    val n = finite.length
    val mean = finite.sum / n
    val median = if (n % 2 == 1) finite(n / 2) else (finite(n / 2 - 1) + finite(n / 2)) / 2.0
    val byMean = rows(LogisticStandardization.test(df, fit, "direct_rate", NullValue.Mean))
    val byMedian = rows(LogisticStandardization.test(df, fit, "direct_rate", NullValue.Median))
    byMean.foreach(r => assert(math.abs(d(r, "null_value") - mean) <= 1e-15 * mean))
    byMedian.foreach(r => assertEquals(bits(d(r, "null_value")), bits(median)))
    val atMeanEffect =
      rows(LogisticStandardization.test(df, fit, "direct_rate", reference = EffectReference.Mean))
    assert(d(atMeanEffect.head, "null_value") != d(byMean.head, "null_value"))
    rows(LogisticStandardization.test(df, fit, "direct_rate", NullValue.Value(0.2)))
      .foreach(r => assertEquals(d(r, "null_value"), 0.2))
  }

  test("variance robust is refused for indirect measures, as pprof_py v0.7.0 refuses it (L6)") {
    val clustered = frame(spark, "lfe-clustered")
    val robustFit = LogisticFE.fit(clustered, spec("lfe-clustered"))
    val e = intercept[IllegalArgumentException](
      LogisticStandardization.test(clustered, robustFit, "indirect_ratio", variance = "robust")
    )
    assert(e.getMessage.contains("indirectVariance"), e.getMessage)
  }

  private def nullValueOf(o: JsonNode): NullValue = o.get("null_value").asText() match {
    case "value" => NullValue.Value(doubles(o, "null_number")(0))
    case mode    => NullValue.of(mode, Double.NaN)
  }

  private def nullModelOf(o: JsonNode): NullModel = o.get("null_model").asText() match {
    case "fixed" =>
      val p = doubles(o, "null_parameters")
      NullModel.Fixed(p(0), p(1))
    case _ => NullModel.Theoretical
  }

  private def boundsOf(o: JsonNode): IntervalBounds = o.get("bounds").asText() match {
    case "none"  => IntervalBounds.Unbounded
    case "fixed" =>
      val b = doubles(o, "bounds_values")
      IntervalBounds.Fixed(b(0), b(1))
    case _ => IntervalBounds.Auto
  }

  // OI-84 (round 88): pprof_py's own outputs for these options, from the fixtures, which also record the options.
  for (name <- Seq("lfe-base", "lfe-clustered"); measure <- Seq("direct_rate", "indirect_ratio"))
    test(
      s"pprof_py's test_standardized with null values, a fixed null, intervals and bounds: $name, $measure"
    ) {
      val df = frame(spark, name)
      val fit = LogisticFE.fit(df, spec(name))
      val ref = json(name, "pprof_py").get("standardized_test_options")
      val providers = ints(ref, "providers")
      val labels = ref.fieldNames().asScala.toSeq.filter { label =>
        label != "providers" && ref.get(label).get("options").get("measure").asText() == measure
      }
      assert(labels.size >= 8, s"$name $measure: ${labels.size} sets of options")
      labels.foreach { label =>
        val r = ref.get(label)
        val o = r.get("options")
        val table = LogisticStandardization.test(
          df,
          fit,
          measure,
          nullValueOf(o),
          o.get("transform").asText(),
          variance = o.get("variance").asText(),
          nullModel = nullModelOf(o),
          interval = o.get("interval").asText(),
          bounds = boundsOf(o)
        )
        checkTestTable(s"$name $label", table, r, providers)
        val first = table.orderBy("provider").head()
        check(
          "T-meas",
          s"$name $label null value",
          Seq(first.getAs[Double]("null_value")),
          doubles(r, "null_value")
        )
        assertEquals(
          (first.getAs[Double]("null_mean"), first.getAs[Double]("null_sd")),
          (doubles(r, "null_mean")(0), doubles(r, "null_sd")(0)),
          s"$name $label"
        )
      }
    }
}
