package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Slice 4b-2 (docs/spec/logistic/random-effect-inference.md §10, D-61): pprof_spark's inference evaluated at pprof_py's
  * tightened (σ, β) (`fitAt`) against `inference_pprof_py.json` at T-test, T-p, T-flag, T-coef and T-meas; `resampling`
  * within 4.5 Monte Carlo standard errors; `profileSigma` at T-opt against pprof_py and lme4.
  */
class LogisticREInferenceSuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(15, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"logistic-re/$name/input.csv").toString)

  private def setup(name: String): (LogisticRESpec, LogisticREOptions) = {
    val c = Fixtures.json(s"logistic-re/$name/case.json")
    val features = c.get("features").elements().asScala.map(_.asText()).toSeq
    val spec = LogisticRESpec(
      "y",
      features,
      "provider",
      if (c.has("offset")) Some("o") else None,
      Some("id")
    )
    (
      spec,
      LogisticREOptions(includeIntercept = !c.has("intercept") || c.get("intercept").asBoolean())
    )
  }

  private def h(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val value = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(value <= 1.0, s"$what: $value of $cls")
  }

  /** T-p: relative T-test at or above the log threshold, log10 within its atol below it. */
  private def pCheck(what: String, actual: Seq[Double], expected: Array[Double]): Unit = {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    val rtol = Tolerances.number("T-test.rtol")
    actual.zip(expected).foreach { case (x, y) =>
      val ok =
        if (y >= threshold) math.abs(x - y) <= rtol * math.abs(y)
        else if (y == 0.0) x < 1e-300
        else x > 0.0 && math.abs(math.log10(x) - math.log10(y)) <= atol
      assert(ok, s"$what: p $x against $y")
    }
  }

  private def column(rows: Array[Row], name: String): Seq[Double] =
    rows.toSeq.map(r =>
      if (r.isNullAt(r.fieldIndex(name))) Double.NaN else r.getAs[Number](name).doubleValue
    )

  private def flags(
      what: String,
      actual: Seq[Double],
      expected: Array[Double],
      z: Array[Double],
      c: Double
  ): Unit = {
    val band = Tolerances.number("T-flag.boundary.rtol") * c
    actual.indices.foreach { i =>
      val same = actual(i) == expected(i) || (actual(i).isNaN && expected(i).isNaN)
      assert(
        same || math.abs(math.abs(z(i)) - c) <= band,
        s"$what: flag ${actual(i)} against ${expected(i)}"
      )
    }
  }

  private val Two = 1.959963984540054

  private def lockstep(name: String, full: Boolean): Unit = {
    val (spec, options) = setup(name)
    val df = input(name)
    val inf = Fixtures.json(s"logistic-re/$name/inference_pprof_py.json")
    val fit = LogisticRE.fitAt(
      df,
      spec,
      options,
      h(inf.get("estimates").get("sigma")).head,
      h(inf.get("estimates").get("beta")).toSeq
    )
    val s = LogisticREInference.summary(fit, 0.9)
    val e = inf.get("summary")
    check("T-coef", s"$name summary se", s.map(_.standardError), h(e.get("se")))
    check("T-test", s"$name summary z", s.map(_.statistic), h(e.get("z")))
    pCheck(s"$name summary p", s.map(_.pValue), h(e.get("p")))
    check(
      "T-coef",
      s"$name summary limits",
      s.map(_.lower) ++ s.map(_.upper),
      h(e.get("lower")) ++ h(e.get("upper"))
    )
    val variants: Seq[(String, String, EffectReference, String, Option[Double])] = Seq(
      ("wald_zero", "wald", EffectReference.Value(0.0), "two_sided", None),
      ("poibin_zero", "poibin_exact", EffectReference.Value(0.0), "two_sided", None)
    ) ++ (if (!full) Nil
          else
            Seq(
              ("wald_median", "wald", EffectReference.Median, "two_sided", None),
              ("wald_mean", "wald", EffectReference.Mean, "two_sided", None),
              ("wald_given", "wald", EffectReference.Value(0.1), "two_sided", None),
              ("wald_greater", "wald", EffectReference.Median, "greater", None),
              ("wald_less", "wald", EffectReference.Median, "less", None),
              ("wald_critical", "wald", EffectReference.Value(0.0), "two_sided", Some(2.5)),
              ("poibin_median", "poibin_exact", EffectReference.Median, "two_sided", None),
              ("poibin_greater", "poibin_exact", EffectReference.Median, "greater", None)
            ))
    for ((label, method, reference, alternative, critical) <- variants) {
      val t =
        LogisticREInference.test(df, fit, method, reference, alternative, 0.95, critical).collect()
      val x = inf.get("test").get(label)
      val c = critical.getOrElse(if (alternative == "two_sided") Two else 1.6448536269514722)
      check("T-coef", s"$name $label estimate", column(t, "estimate"), h(x.get("estimate")))
      if (method == "wald") {
        check("T-test", s"$name $label z", column(t, "z_raw"), h(x.get("z_raw")))
        pCheck(s"$name $label p", column(t, "p_value"), h(x.get("p_value")))
      } else { // X-024: pprof_py's FFT tails are exact to about 1e-16 absolute, so z is compared where p >= 1e-6
        val (z, ez) = (column(t, "z_raw"), h(x.get("z_raw")))
        val (pv, ep) = (column(t, "p_value"), h(x.get("p_value")))
        val clear = z.indices.filter(i => ep(i) >= 1e-6)
        check("T-test", s"$name $label z", clear.map(z), clear.map(ez).toArray)
        pv.zip(ep).foreach { case (a, b) =>
          assert(math.abs(a - b) <= 1e-14 + 1e-8 * b, s"$name $label p $a against $b")
        }
      }
      flags(s"$name $label", column(t, "flag"), h(x.get("flag")), h(x.get("z_raw")), c)
      val (lo, hi) = (column(t, "ci_lower"), column(t, "ci_upper"))
      val (elo, ehi) = (h(x.get("ci_lower")), h(x.get("ci_upper")))
      val finite = lo.indices.filter(i =>
        java.lang.Double.isFinite(elo(i)) && java.lang.Double.isFinite(ehi(i))
      )
      check(
        "T-coef",
        s"$name $label limits",
        finite.map(lo) ++ finite.map(hi),
        (finite.map(elo) ++ finite.map(ehi)).toArray
      )
    }
    val alpha = LogisticREInference.alphaIntervals(fit).orderBy(spec.provider).collect()
    check(
      "T-coef",
      s"$name alpha",
      column(alpha, "alpha_lower") ++ column(alpha, "alpha_upper"),
      h(inf.get("alpha_ci").get("lower")) ++ h(inf.get("alpha_ci").get("upper"))
    )
    val measures =
      LogisticREInference.measures(df, fit, Seq("indirect", "direct"), EffectReference.Median)
    for (
      kind <- Seq("indirect", "direct");
      q <- Seq(s"${kind}_ratio", s"${kind}_rate", "observed", "expected")
    ) {
      val rows = measures(kind).orderBy(spec.provider).collect()
      check(
        "T-meas",
        s"$name measures $kind $q",
        column(rows, q),
        h(inf.get("measures_median").get(kind).get(q))
      )
    }
    if (full)
      for (
        (alternative, key) <- Seq(
          "two_sided" -> "sm_two_sided",
          "greater" -> "sm_greater",
          "less" -> "sm_less"
        )
      ) {
        val sm = LogisticREInference.smIntervals(
          df,
          fit,
          Seq("indirect", "direct"),
          Seq("ratio", "rate"),
          EffectReference.Median,
          alternative
        )
        for (q <- Seq("indirect_ratio", "indirect_rate", "direct_ratio", "direct_rate")) {
          val rows = sm(q).orderBy(spec.provider).collect()
          val x = inf.get(key).get(q)
          check(
            "T-meas",
            s"$name SM $alternative $q value",
            column(rows, "value"),
            h(x.get("value"))
          )
          // X-045: one-sided indirect intervals are compared on their finite side
          val sides =
            if (q.startsWith("indirect") && alternative == "greater") Seq("lower")
            else if (q.startsWith("indirect") && alternative == "less") Seq("upper")
            else Seq("lower", "upper")
          for (side <- sides) {
            val (a, b) = (column(rows, side), h(x.get(side)))
            val keep = a.indices.filter(i => !b(i).isNaN)
            check("T-meas", s"$name SM $alternative $q $side", keep.map(a), keep.map(b).toArray)
          }
        }
      }
  }

  for (
    (name, full) <- Seq(
      "lgre-base" -> true,
      "lgre-text" -> true,
      "lgre-offset" -> true,
      "lgre-extreme" -> false,
      "lgre-many" -> false,
      "lgre-nointercept" -> false,
      "lgre-provider-level" -> false
    )
  ) {
    test(s"$name: lockstep at pprof_py's tightened estimates (D-61)")(lockstep(name, full))
  }

  test(
    "lgre-base: resampling within 4.5 Monte Carlo SEs of pprof_py's, and the profile interval at T-opt"
  ) {
    val (spec, options) = setup("lgre-base")
    val df = input("lgre-base")
    val inf = Fixtures.json("logistic-re/lgre-base/inference_pprof_py.json")
    val fit = LogisticRE.fitAt(
      df,
      spec,
      options,
      h(inf.get("estimates").get("sigma")).head,
      h(inf.get("estimates").get("beta")).toSeq
    )
    val rows =
      LogisticREInference.test(df, fit, "resampling", nResample = 20000, seed = 7L).collect()
    val ours = column(rows, "p_value")
    val theirs = h(inf.get("test").get("resampling_zero").get("p_value"))
    ours.zip(theirs).foreach {
      case (a, b) => // at the floor both take the exact tail (X-024's allowance)
        // the two-sided p is twice a simulated tail t: two runs differ with SD sqrt(2 * 4t(1 - t) / n)
        val tail = math.min(b / 2.0, 0.5)
        val se = math.sqrt(8.0 * math.max(tail * (1.0 - tail), 1e-8) / 20000.0)
        assert(
          math.abs(a - b) <= 4.5 * se || math.abs(a - b) <= 1e-14 + 1e-8 * b,
          s"resampling p $a against $b"
        )
    }
    val profile = LogisticREInference.profileSigma(df, LogisticRE.fit(df, spec, options), 0.95)
    val py = inf.get("profile_sigma")
    check(
      "T-opt",
      "profile against pprof_py",
      Seq(profile.lower, profile.upper),
      h(py.get("lower")) ++ h(py.get("upper"))
    )
    val lme4 =
      h(Fixtures.json("logistic-re/lgre-base/r_lme4.json").get("profile").get("lme4").get("limits"))
    check("T-opt", "profile against lme4", Seq(profile.lower, profile.upper), lme4)
  }

  test("exact needs a cluster factor, and the measures refuse other data (API-3)") {
    val (spec, options) = setup("lgre-text")
    val df = input("lgre-text")
    val fit = LogisticRE.fitAt(df, spec, options, 0.5, Seq(-1.0, 0.7, -0.4))
    intercept[IllegalArgumentException](LogisticREInference.test(df, fit, "exact"))
    val other = df.withColumn("x1", df("x1") + 1.0)
    intercept[IllegalArgumentException](LogisticREInference.measures(other, fit))
  }
}
