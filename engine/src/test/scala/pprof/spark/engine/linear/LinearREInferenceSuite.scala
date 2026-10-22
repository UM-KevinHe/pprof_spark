package pprof.spark.engine.linear

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Slice 4a-2 end to end (docs/spec/linear/random-effect-inference.md §10): from pprof_spark's own fit, its covariate
  * tables, provider tests, intervals and measures against base R on lme4's tight fit and against pprof_py at its
  * tight optimum, under D-55's classes. Weighted fits are compared with lme4's conditional variances (X-037); the
  * boundary fit leaves every provider untested (D-55); lre-shifted is informational (D-53).
  */
class LinearREInferenceSuite extends SparkSuite {

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear-re/$name/input.csv").toString)

  private def setup(name: String): (LinearRESpec, Boolean) = {
    val c = Fixtures.json(s"linear-re/$name/case.json")
    val features = c.get("features").elements().asScala.map(_.asText()).toSeq
    val spec = LinearRESpec(
      "y",
      features,
      "provider",
      offset = if (c.has("offset")) Some("o") else None,
      weights = if (c.has("weights")) Some("w") else None,
      rowId = Some("id")
    )
    (spec, !c.has("intercept") || c.get("intercept").asBoolean())
  }

  private def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  /** T-p: relative T-test at or above the log threshold, log10 within its atol below it, 0 against 0. */
  private def pCheck(what: String, actual: Seq[Double], expected: Array[Double]): Unit = {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    val rtol = Tolerances.number("T-test.rtol")
    actual.zip(expected).foreach { case (x, y) =>
      val r =
        if (y >= threshold) math.abs(x - y) / (rtol * math.abs(y))
        else if (y == 0.0) (if (x < 1e-300) 0.0 else Double.PositiveInfinity)
        else if (x > 0.0) math.abs(math.log10(x) - math.log10(y)) / atol
        else Double.PositiveInfinity
      assert(r <= 1.0, s"$what: p $x against $y is $r of T-p")
    }
  }

  /** Flags equal, except within T-flag's band of the critical value. */
  private def flagCheck(
      what: String,
      actual: Seq[Double],
      expected: Array[Double],
      z: Array[Double],
      c: Double
  ): Unit = {
    val band = Tolerances.number("T-flag.boundary.rtol") * c
    actual.indices.foreach { i =>
      assert(
        actual(i) == expected(i) || math.abs(math.abs(z(i)) - c) <= band,
        s"$what: flag ${actual(i)} against ${expected(i)} at z ${z(i)}"
      )
    }
  }

  private def column(rows: Array[Row], name: String): Seq[Double] =
    rows.toSeq.map(row =>
      if (row.isNullAt(row.fieldIndex(name))) Double.NaN else row.getAs[Number](name).doubleValue
    )

  private val Critical = 1.959963984540054

  private val cases = Seq(
    "lre-base" -> "reml",
    "lre-base" -> "ml",
    "lre-weights" -> "reml",
    "lre-weights" -> "ml",
    "lre-offset" -> "reml",
    "lre-nointercept" -> "reml",
    "lre-provider-level" -> "reml",
    "lre-boundary" -> "reml",
    "lre-text" -> "reml",
    "lre-many" -> "reml"
  )

  for ((name, criterion) <- cases) {
    test(s"$name, $criterion: inference and measures match the references (spec 4a-2 section 10)") {
      val (spec, intercept) = setup(name)
      val df = input(name)
      val fit = LinearRE.fit(
        df,
        spec,
        LinearREOptions(reml = criterion == "reml", includeIntercept = intercept)
      )
      val py = Fixtures.json(s"linear-re/$name/inference_pprof_py.json").get(criterion)
      val r = Fixtures.json(s"linear-re/$name/r_lme4.json").get(criterion).get("inference")
      val weighted = spec.weights.nonEmpty
      val boundary = name == "lre-boundary"

      for (
        (alternative, lower, upper) <- Seq(
          ("two_sided", Some("summary_lower_two_sided"), Some("summary_upper_two_sided")),
          ("greater", Some("summary_lower_greater"), None),
          ("less", None, Some("summary_upper_less"))
        )
      ) {
        val s = LinearREInference.summary(fit, 0.9, 0.0, alternative)
        check("T-test", s"summary $alternative t", s.map(_.statistic), d(r.get("summary_stat")))
        pCheck(s"summary $alternative p", s.map(_.pValue), d(r.get(s"summary_p_$alternative")))
        lower.foreach(k =>
          check("T-coef", s"summary $alternative lower", s.map(_.lower), d(r.get(k)))
        )
        upper.foreach(k =>
          check("T-coef", s"summary $alternative upper", s.map(_.upper), d(r.get(k)))
        )
        val rounded =
          d(py.get(s"summary_$alternative").get("p_value")) // X-041: pprof_py rounds to 7 decimals
        s.map(_.pValue).zip(rounded).foreach { case (x, y) =>
          assert(math.abs(x - y) <= 5e-8, s"$x vs $y")
        }
      }
      val table = LinearREInference.fixedEffectsTable(fit)
      check(
        "T-test",
        "fixed_effects_table t",
        table.map(_.statistic),
        d(py.get("fixed_effects_table").get("t"))
      )
      pCheck("fixed_effects_table p", table.map(_.pValue), d(r.get("fet_p")))
      val bounds = LinearREInference.confInt(fit, 0.95)
      check("T-coef", "conf_int lower", bounds.map(_._2), d(r.get("conf_lower")))
      check("T-coef", "conf_int upper", bounds.map(_._3), d(r.get("conf_upper")))

      for (
        (label, reference) <- Seq(
          "zero" -> EffectReference.Value(0.0),
          "median" -> EffectReference.Median
        )
      ) {
        val t = LinearREInference.test(fit, reference).collect()
        if (boundary) {
          assert(
            column(t, "z_raw").forall(_.isNaN) && column(t, "p_value").forall(_.isNaN),
            "boundary: untested"
          )
          assert(
            column(t, "flag").forall(_.isNaN) && column(t, "ci_lower").forall(_.isNaN),
            "boundary: no flags"
          )
        } else {
          val z = d(r.get(s"test_${label}_z"))
          check("T-test", s"test $label z", column(t, "z_raw"), z)
          pCheck(s"test $label p", column(t, "p_value"), d(r.get(s"test_${label}_p")))
          flagCheck(
            s"test $label flags",
            column(t, "flag"),
            d(r.get(s"test_${label}_flag")),
            z,
            Critical
          )
          check(
            "T-coef",
            s"test $label lower",
            column(t, "ci_lower"),
            d(r.get(s"test_${label}_lower"))
          )
          check(
            "T-coef",
            s"test $label upper",
            column(t, "ci_upper"),
            d(r.get(s"test_${label}_upper"))
          )
        }
      }
      if (!weighted && !boundary) {
        val variants = Seq(
          ("mean", EffectReference.Mean, "two_sided", None, None),
          ("given", EffectReference.Value(0.1), "two_sided", None, None),
          ("greater", EffectReference.Median, "greater", None, None),
          ("less", EffectReference.Median, "less", None, None),
          ("critical", EffectReference.Value(0.0), "two_sided", Some(2.5), None),
          (
            "subset",
            EffectReference.Value(0.0),
            "two_sided",
            None,
            Some(py.get("test").get("subset").get("providers"))
          )
        )
        for ((label, reference, alternative, critical, subset) <- variants) {
          val wanted = subset.map(_.elements().asScala.map(_.asText()).toSeq)
          val t =
            LinearREInference.test(fit, reference, alternative, 0.95, critical, wanted).collect()
          val e = py.get("test").get(label)
          val c =
            critical.getOrElse(if (alternative == "two_sided") Critical else 1.6448536269514722)
          check("T-test", s"test $label z", column(t, "z_raw"), d(e.get("z_raw")))
          pCheck(s"test $label p", column(t, "p_value"), d(e.get("p_value")))
          flagCheck(s"test $label flags", column(t, "flag"), d(e.get("flag")), d(e.get("z_raw")), c)
          check("T-coef", s"test $label lower", column(t, "ci_lower"), d(e.get("ci_lower")))
          check("T-coef", s"test $label upper", column(t, "ci_upper"), d(e.get("ci_upper")))
        }
        val alpha = LinearREInference.alphaIntervals(fit).collect()
        check(
          "T-coef",
          "alpha lower",
          column(alpha, "alpha_lower"),
          d(py.get("alpha_ci").get("alpha_lower"))
        )
        check(
          "T-coef",
          "alpha upper",
          column(alpha, "alpha_upper"),
          d(py.get("alpha_ci").get("alpha_upper"))
        )
      }

      for (alternative <- Seq("two_sided", "greater")) {
        val sm = LinearREInference.smIntervals(
          df,
          fit,
          Seq("indirect", "direct"),
          EffectReference.Median,
          alternative
        )
        for (kind <- Seq("indirect", "direct")) {
          val rows = sm(kind).collect()
          val e = py.get(s"sm_ci_$alternative").get(kind)
          for (q <- Seq(s"${kind}_difference", "observed", "expected"))
            check("T-meas", s"SM $alternative $kind $q", column(rows, q), d(e.get(q)))
          if (!weighted && !boundary)
            for (q <- Seq("lower", "upper"))
              check("T-meas", s"SM $alternative $kind $q", column(rows, q), d(e.get(q)))
        }
      }
      for (
        (label, reference) <- Seq(
          "median" -> EffectReference.Median,
          "mean" -> EffectReference.Mean,
          "zero" -> EffectReference.Value(0.0)
        )
      ) {
        val m = LinearREInference.measures(df, fit, Seq("indirect", "direct"), reference)
        for (
          kind <- Seq("indirect", "direct"); q <- Seq(s"${kind}_difference", "observed", "expected")
        )
          check(
            "T-meas",
            s"measures $label $kind $q",
            column(m(kind).collect(), q),
            d(py.get(s"measures_$label").get(kind).get(q))
          )
      }
    }
  }

  test("measures refuse data other than the fit's (API-3)") {
    val (spec, _) = setup("lre-base")
    val df = input("lre-base")
    val fit = LinearRE.fit(df, spec)
    val other = df.withColumn("x1", df("x1") + 1.0)
    intercept[IllegalArgumentException](LinearREInference.measures(other, fit))
  }
}
